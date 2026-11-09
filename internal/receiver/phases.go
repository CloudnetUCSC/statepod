package receiver

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/CloudnetUCSC/statepod/internal/container"
	"github.com/CloudnetUCSC/statepod/internal/network"
)

// Destination-side phase timeline. Handlers run on separate
// goroutines, so this is mutex-guarded; the receiver is one process per
// migration, so a package-level log is enough. Every key carries the "dst_"
// prefix — these timestamps are on the destination's clock and must never be
// subtracted from "src_*" ones (see migration.MigrationMetrics.Phases).
var dstPhases = struct {
	sync.Mutex
	m map[string]time.Time
}{m: map[string]time.Time{}}

// markDst records the current time under "dst_<name>" (last write wins).
func markDst(name string) {
	dstPhases.Lock()
	dstPhases.m["dst_"+name] = time.Now()
	dstPhases.Unlock()
}

// hostNet guards the one-time, PID-independent host network staging.
var hostNet struct {
	sync.Mutex
	done bool
}

// prestageHostNetwork does everything about the destination's networking that
// does not need the restored container's PID: the bridge, NAT, IP forwarding,
// FORWARD rules and the host-port DNAT (which only names the container IP).
// All of it is idempotent. Called at the first contact of a migration (the
// manifest request / config stream), so by the time the container is restored
// only the veth attach remains on the critical path. Safe to call
// repeatedly and concurrently: later callers block until the first finishes.
func prestageHostNetwork(ctx context.Context, cfg *container.Config) {
	hostNet.Lock()
	defer hostNet.Unlock()
	if hostNet.done {
		return
	}
	markDst("hostnet_prestage_start")
	if err := network.EnsureBridge(ctx, cfg.Gateway); err != nil {
		fmt.Printf("Warning: host network prestage (bridge) failed: %v\n", err)
		return
	}
	network.SetupNAT(ctx, cfg.Subnet)
	if cfg.HostPort >= 0 {
		if err := network.SetupPortForwarding(ctx, cfg.ContainerIP, cfg.RedisHostPort(), cfg.ServicePort()); err != nil {
			fmt.Printf("Warning: Failed to setup port forwarding: %v\n", err)
		}
	}
	hostNet.done = true
	markDst("hostnet_prestage_end")
}

// snapshotDst returns a copy of the destination timeline for merging into the
// metrics JSON.
func snapshotDst() map[string]time.Time {
	dstPhases.Lock()
	defer dstPhases.Unlock()
	out := make(map[string]time.Time, len(dstPhases.m))
	for k, v := range dstPhases.m {
		out[k] = v
	}
	return out
}
