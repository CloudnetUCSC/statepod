package network

import (
	"context"
	"fmt"
	"net"
	"os/exec"
)

const (
	bridgeName         = "sp0"
	defaultSubnet      = "10.10.0.0/24"
	defaultGateway     = "10.10.0.1"
	defaultContainerIP = "10.10.0.10"
)

func runCmd(ctx context.Context, name string, args ...string) error {

	cmd := exec.CommandContext(ctx, name, args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%s %v failed: %s", name, args, out)
	}
	return nil
}

func EnsureBridge(ctx context.Context, gateway string) error {
	if gateway == "" {
		gateway = defaultGateway
	}

	if err := runCmd(ctx, "ip", "link", "show", bridgeName); err == nil {
		return nil
	}

	if err := runCmd(ctx, "sudo", "ip", "link", "add", "name", bridgeName, "type", "bridge"); err != nil {
		return err
	}

	if err := runCmd(ctx, "sudo", "ip", "addr", "add", gateway+"/24", "dev", bridgeName); err != nil {
		return err
	}

	return runCmd(ctx, "sudo", "ip", "link", "set", bridgeName, "up")
}

func ensureNAT(ctx context.Context, subnet string) error {
	return runCmd(
		ctx,
		"sudo", "iptables",
		"-t", "nat",
		"-C", "POSTROUTING",
		"-s", subnet,
		"-j", "MASQUERADE",
	)
}

func SetupNAT(ctx context.Context, subnet string) {
	if subnet == "" {
		subnet = defaultSubnet
	}
	err := ensureNAT(ctx, subnet)
	if err == nil {
		return
	}

	runCmd(
		ctx,
		"sudo", "iptables",
		"-t", "nat",
		"-A", "POSTROUTING",
		"-s", subnet,
		"-j", "MASQUERADE",
	)
}

// SetupContainerNetwork attaches the (restored or freshly created) container
// with the given PID to the bridge: veth pair, address, default route inside
// its netns. Everything here needs the PID, so it cannot be done ahead of the
// restore; everything else (bridge, NAT, forwarding, DNAT) is PID-independent
// and is staged earlier by the receiver. The steps run as ONE
// privileged shell (one sudo/PAM round instead of eight: ~40 ms each on the
// testbed VMs, on the migration's critical path).
// The first step brings the loopback interface up: CRIU restores the network
// namespace with `lo` DOWN (found 2026-10-05: a restored PostgreSQL listened but
// every 127.0.0.1 connection inside the container failed; Redis was unaffected
// because its clients use the container IP), so any workload that talks to
// localhost needs it. Idempotent for a freshly started container.
func SetupContainerNetwork(ctx context.Context, pid int, containerIP, gateway string) error {
	if containerIP == "" {
		containerIP = defaultContainerIP
	}
	if gateway == "" {
		gateway = defaultGateway
	}
	if net.ParseIP(containerIP) == nil || net.ParseIP(gateway) == nil {
		return fmt.Errorf("invalid container IP %q or gateway %q", containerIP, gateway)
	}

	hostVeth := fmt.Sprintf("veth%d", pid)
	contVeth := fmt.Sprintf("eth%d", pid)
	script := fmt.Sprintf(`set -e
nsenter -t %[4]d -n ip link set lo up
ip link add %[1]s type veth peer name %[2]s
ip link set %[1]s master %[3]s
ip link set %[1]s up
ip link set %[2]s netns %[4]d
nsenter -t %[4]d -n ip addr add %[5]s/24 dev %[2]s
nsenter -t %[4]d -n ip link set %[2]s up
nsenter -t %[4]d -n ip route add default via %[6]s
`, hostVeth, contVeth, bridgeName, pid, containerIP, gateway)
	return runCmd(ctx, "sudo", "sh", "-c", script)
}

// setupContainerNetworkStepwise is the original one-command-per-step version,
// kept only as documentation of what the script above does.
func setupContainerNetworkStepwise(ctx context.Context, pid int, containerIP, gateway string) error {
	if containerIP == "" {
		containerIP = defaultContainerIP
	}
	if gateway == "" {
		gateway = defaultGateway
	}

	hostVeth := fmt.Sprintf("veth%d", pid)
	contVeth := fmt.Sprintf("eth%d", pid)

	if err := runCmd(ctx,
		"sudo", "ip", "link", "add",
		hostVeth,
		"type", "veth",
		"peer", "name", contVeth); err != nil {
		return err
	}

	if err := runCmd(ctx,
		"sudo", "ip", "link", "set",
		hostVeth,
		"master",
		bridgeName); err != nil {
		return err
	}

	if err := runCmd(ctx,
		"sudo", "ip", "link", "set",
		hostVeth,
		"up"); err != nil {
		return err
	}

	if err := runCmd(ctx,
		"sudo", "ip", "link", "set",
		contVeth,
		"netns",
		fmt.Sprint(pid)); err != nil {
		return err
	}

	if err := runCmd(ctx,
		"sudo", "nsenter",
		"-t", fmt.Sprint(pid),
		"-n",
		"ip", "addr", "add",
		containerIP+"/24",
		"dev", contVeth); err != nil {
		return err
	}

	if err := runCmd(ctx,
		"sudo", "nsenter",
		"-t", fmt.Sprint(pid),
		"-n",
		"ip", "link", "set",
		contVeth,
		"up"); err != nil {
		return err
	}

	return runCmd(ctx,
		"sudo", "nsenter",
		"-t", fmt.Sprint(pid),
		"-n",
		"ip", "route", "add",
		"default",
		"via",
		gateway)
}

// knownServicePorts enumerates every in-container port a statepod workload
// can listen on (container.Config.ServicePort: Redis 6379, Node 3000). A
// hostPort DNAT rule from an earlier container occupying the same hostPort
// (e.g. a prior rep's Redis container before this rep's Node container) is
// stale once the workload changes, since iptables matches the first rule in
// chain order regardless of which container is actually running now.
// Ports of the other workloads statepod is evaluated on (PostgreSQL
// 5432, MariaDB 3306, JVM/HTTP services 8080/8000, a generic app 5000, the
// synthetic knob workload 9000) are listed too, so a rep that switches
// workload cannot inherit a stale DNAT rule from the previous one.
var knownServicePorts = []int{6379, 3000, 5432, 3306, 8080, 8000, 5000, 9000}

func SetupPortForwarding(ctx context.Context, containerIP string, hostPort, containerPort int) error {
	// Enable IP forwarding
	if err := runCmd(ctx, "sudo", "sysctl", "-w", "net.ipv4.ip_forward=1"); err != nil {
		return err
	}

	// Drop any stale DNAT rule left at this hostPort by a previous container
	// that forwarded to a different in-container port. Best-effort: the rule
	// may not exist, so errors are ignored.
	for _, staleContainerPort := range knownServicePorts {
		if staleContainerPort == containerPort {
			continue
		}
		runCmd(ctx, "sudo", "iptables", "-t", "nat", "-D", "PREROUTING",
			"-p", "tcp", "--dport", fmt.Sprint(hostPort),
			"-j", "DNAT", "--to-destination", fmt.Sprintf("%s:%d", containerIP, staleContainerPort))
		runCmd(ctx, "sudo", "iptables", "-t", "nat", "-D", "OUTPUT",
			"-p", "tcp", "-d", "127.0.0.1", "--dport", fmt.Sprint(hostPort),
			"-j", "DNAT", "--to-destination", fmt.Sprintf("%s:%d", containerIP, staleContainerPort))
	}

	// Add FORWARD rules idempotently, at the TOP of the chain (-I, not -A):
	// Docker installs its own FORWARD rules (DOCKER-USER/DOCKER-FORWARD) plus
	// ufw adds a catch-all REJECT, both ahead of anything appended at the
	// bottom. An -A rule here would never fire for genuinely routed traffic
	// (only for locally-originated packets, e.g. an SSH -L tunnel terminating
	// on this host, which skip FORWARD altogether) — appended rules looked
	// fine over a tunnel and silently dropped every real external connection.
	if err := runCmd(ctx, "sudo", "iptables", "-C", "FORWARD", "-i", bridgeName, "-j", "ACCEPT"); err != nil {
		if err := runCmd(ctx, "sudo", "iptables", "-I", "FORWARD", "1", "-i", bridgeName, "-j", "ACCEPT"); err != nil {
			return err
		}
	}
	if err := runCmd(ctx, "sudo", "iptables", "-C", "FORWARD", "-o", bridgeName, "-j", "ACCEPT"); err != nil {
		if err := runCmd(ctx, "sudo", "iptables", "-I", "FORWARD", "1", "-o", bridgeName, "-j", "ACCEPT"); err != nil {
			return err
		}
	}

	// Add DNAT rule for port forwarding (external traffic)
	dnatArgs := []string{
		"iptables", "-t", "nat", "-C", "PREROUTING",
		"-p", "tcp", "--dport", fmt.Sprint(hostPort),
		"-j", "DNAT", "--to-destination", fmt.Sprintf("%s:%d", containerIP, containerPort),
	}

	if err := runCmd(ctx, "sudo", dnatArgs...); err != nil {
		dnatArgs[3] = "-A"
		if err := runCmd(ctx, "sudo", dnatArgs...); err != nil {
			return err
		}
	}

	// Add OUTPUT rule for local traffic (processes running on host hitting 127.0.0.1)
	outputDnatArgs := []string{
		"iptables", "-t", "nat", "-C", "OUTPUT",
		"-p", "tcp", "-d", "127.0.0.1", "--dport", fmt.Sprint(hostPort),
		"-j", "DNAT", "--to-destination", fmt.Sprintf("%s:%d", containerIP, containerPort),
	}

	if err := runCmd(ctx, "sudo", outputDnatArgs...); err != nil {
		outputDnatArgs[3] = "-A"
		if err := runCmd(ctx, "sudo", outputDnatArgs...); err != nil {
			return err
		}
	}

	// The OUTPUT DNAT above leaves locally-generated packets with source
	// 127.0.0.1: the kernel treats them as martians on the bridge and the
	// container would reply to 127.0.0.1 anyway. route_localnet lets 127/8
	// route via the bridge; MASQUERADE rewrites the source so replies come
	// back through conntrack. Without these, 127.0.0.1:<hostPort> hangs.
	if err := runCmd(ctx, "sudo", "sysctl", "-w",
		fmt.Sprintf("net.ipv4.conf.%s.route_localnet=1", bridgeName)); err != nil {
		return err
	}

	masqArgs := []string{
		"iptables", "-t", "nat", "-C", "POSTROUTING",
		"-s", "127.0.0.1", "-d", containerIP,
		"-p", "tcp", "--dport", fmt.Sprint(containerPort),
		"-j", "MASQUERADE",
	}

	if err := runCmd(ctx, "sudo", masqArgs...); err != nil {
		masqArgs[3] = "-A"
		if err := runCmd(ctx, "sudo", masqArgs...); err != nil {
			return err
		}
	}

	return nil
}
