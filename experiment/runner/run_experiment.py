#!/usr/bin/env python3
"""Unattended migration experiment runner.

Reproduces the manual source.sh/destination.sh flow over SSH and lands every
run in a self-contained results directory:

    <results_root>/<YYYY-MM-DD_HHMMSS>_<name>/
        manifest.json            environment + parameters (kernel, criu/runc/git versions,
                                 netem, pre/post iperf3 link probe)
        migration_metrics.json   statepod's metrics (fetched from destination)
        probe.log                client-side availability probe (downtime measurement)
        load_client.log          per-request SLA client (--load-client)
        contention.log           background WAN-saturating bulk transfer (--contend)
        source_migrate.log       stdout/stderr of `statepod migrate` on the source
        destination_receiver.log stdout/stderr of the receiver on the destination

The availability probe (probe.sh) runs on the source VM and fails over from the
source's host-forwarded port to the destination's when the source stops
answering, giving the client-observed downtime without a third VM or HAProxy
(design: statepod/docs/testbed.md).

With --load-client, load_client.py runs alongside the probe for the same
window (steady-state through migration through, for synth-postcopy, the M5
drain) issuing real per-request GET/PING traffic and logging one JSON line
per request. It is off by default: an opt-in, not a probe.sh replacement --
the probe stays the cheap default downtime signal every run gets.

Usage:
    ./run_experiment.py --config hosts.yaml --name wan_20mbit_redis_precopy
    ./run_experiment.py --config hosts.yaml --name quick --mode sync --reps 3
    ./run_experiment.py --config hosts.yaml --name cloud --ssh-key ~/.ssh/statepod.key

Stdlib only (ssh/scp via subprocess) so it runs anywhere without pip installs.
Requires: passwordless SSH + sudo on all hosts, NTP-synced source/destination.
"""

import argparse
import datetime
import json
import math
import re
import subprocess
import sys
import time
from pathlib import Path

# dataset-size variants (key-count scaling): "medium" keeps the
# key count every prior result was seeded with (5000 + canary = 5001) so old
# result dirs stay comparable to a --dataset medium (default) run.
import faults  # noqa: E402  (fault injection)
import workloads  # noqa: E402  (per-workload logic lives in workloads.py)
from workloads import DATASET_SIZES  # noqa: E402

# --- tiny YAML subset parser (key: value, 2-space nesting) to avoid a pyyaml dep ---


def load_config(path):
    cfg, stack = {}, [(0, {})]
    stack[0] = (-1, cfg)
    for raw in Path(path).read_text().splitlines():
        line = raw.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip())
        key, _, value = line.strip().partition(":")
        raw_value = value.strip()
        value = raw_value.strip("'\"")
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]
        if raw_value == "":  # no value at all -> nested section ("" stays a string)
            child = {}
            parent[key] = child
            stack.append((indent, child))
        else:
            parent[key] = value
    # ssh_config: an ssh config file (relative to this config) for every ssh/scp call, so a
    # profile run from a server carries its own aliases without touching ~/.ssh/config.
    if cfg.get("ssh_config"):
        f = (Path(path).resolve().parent / Path(cfg["ssh_config"]).expanduser()).resolve()
        if not f.is_file():
            sys.exit(f"ERROR: ssh_config not found: {f}")
        if str(f) not in SSH_OPTS:
            SSH_OPTS.extend(["-F", str(f)])
    return cfg


# --- ssh helpers ---

# Extra args for every ssh/scp call; main() appends ["-i", <key>] when a key is
# configured (--ssh-key flag or ssh_key: in hosts.yaml).
SSH_OPTS = ["-o", "BatchMode=yes"]


SSH_TRANSPORT_ERR = re.compile(
    r"ssh: connect to host|Connection (closed|reset|timed out|refused)|kex_exchange_identification|"
    r"banner exchange|Broken pipe|Operation timed out|No route to host", re.I)


def ssh(target, cmd, check=True, capture=True, timeout=None):
    """Run cmd on target (user@host). Returns CompletedProcess."""
    full = ["ssh", *SSH_OPTS, target, cmd]
    # A flaky link (home Wi-Fi: 2.4 s handshakes with occasional stalls) must not
    # kill a long unattended campaign: retry SSH-LEVEL failures only -- a timeout or
    # ssh's own exit 255 (connection lost/refused) -- never a remote command that ran
    # and failed. The callers' commands are idempotent (setup, probes, cleanup, reads);
    # the migrate command itself does not go through here.
    attempts = 3
    for attempt in range(1, attempts + 1):
        try:
            res = subprocess.run(full, check=False, timeout=timeout,
                                 capture_output=capture, text=True)
        except subprocess.TimeoutExpired:
            if attempt == attempts:
                raise
            print(f"  [ssh {target}] timed out after {timeout}s, retrying ({attempt}/{attempts - 1})", flush=True)
            continue
        # 255 is ssh's own failure code but ALSO what a remote command (runc exec on a
        # missing container, ...) can exit with, so require ssh's transport message too.
        if res.returncode == 255 and attempt < attempts and SSH_TRANSPORT_ERR.search(res.stderr or ""):
            print(f"  [ssh {target}] connection error, retrying ({attempt}/{attempts - 1})", flush=True)
            time.sleep(3)
            continue
        if check and res.returncode != 0:
            raise subprocess.CalledProcessError(res.returncode, full, res.stdout, res.stderr)
        return res


def ssh_bg(target, cmd, logfile):
    """Run cmd on target in the background, teeing output to a local logfile."""
    full = ["ssh", *SSH_OPTS, target, cmd]
    log = open(logfile, "w")
    return subprocess.Popen(full, stdout=log, stderr=subprocess.STDOUT, text=True)


# Background clients (probe, load client, lost-write client) write their log to a
# FILE ON THE VM and the runner copies it back after the run. Streaming it over
# the ssh session (the old way) lost every line produced while the session
# stalled behind a heavy transfer through QEMU's NAT -- the lost-write client's
# log ended 0.3 s before the migration window and the run "passed" without ever
# seeing it (2026-10-04).
CLIENT_LOGS = {}  # local log name -> remote path, per run


def ssh_bg_file(target, cmd, remote_path, local_name):
    """Run cmd in the background on target with stdout -> remote_path."""
    CLIENT_LOGS[local_name] = (target, remote_path)
    full = ["ssh", *SSH_OPTS, target, f"rm -f {remote_path}; {cmd} > {remote_path} 2>&1"]
    return subprocess.Popen(full, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, text=True)


def fetch_client_logs(run_dir):
    """Copy every background client's log back into the run directory."""
    for local_name, (target, remote_path) in list(CLIENT_LOGS.items()):
        try:
            scp_from(target, remote_path, run_dir / local_name)
        except Exception as e:
            (run_dir / local_name).write_text(f"# could not fetch {remote_path}: {e}\n")
    CLIENT_LOGS.clear()


def scp_from(target, remote_path, local_path):
    # as_posix(): keep local paths forward-slashed so scp never
    # mistakes a path for a host:path spec.
    subprocess.run(["scp", *SSH_OPTS,
                    f"{target}:{remote_path}", Path(local_path).as_posix()],
                   check=True, capture_output=True, text=True)


def scp_to(target, local_path, remote_path):
    subprocess.run(["scp", *SSH_OPTS,
                    Path(local_path).as_posix(), f"{target}:{remote_path}"],
                   check=True, capture_output=True, text=True)


def try_out(target, cmd):
    try:
        return ssh(target, cmd, check=False).stdout.strip()
    except Exception:
        return "unavailable"


def local_provenance():
    """Provenance of the engine code the VMs were synced to.

    test.sh resets each VM to the last *pushed* commit and applies unpushed
    work as a patch, so the VM's own `git rev-parse` names only the base. The
    authoritative record is the local HEAD plus a hash of the uncommitted
    diff; per-host binary hashes (host_provenance) prove what actually ran.
    """
    import hashlib
    root = Path(__file__).resolve().parent.parent.parent

    def git(*a):
        r = subprocess.run(["git", "-C", str(root), *a], capture_output=True, text=True)
        return r.stdout if r.returncode == 0 else ""

    # Untracked files are part of the synced tree too (test.sh does `add -N`).
    untracked = [f for f in git("ls-files", "--others", "--exclude-standard").splitlines()
                 if not f.startswith("experiment/results/")]
    h = hashlib.sha256(git("diff", "--binary", "HEAD", "--", ".", ":(exclude)experiment/results")
                       .replace("\r\n", "\n").encode())
    for f in sorted(untracked):
        try:
            h.update(f.encode() + (root / f).read_bytes().replace(b"\r\n", b"\n"))
        except OSError:
            pass
    return {
        "local_head": git("rev-parse", "HEAD").strip(),
        "origin_main": git("rev-parse", "origin/main").strip(),
        "dirty": bool(git("status", "--porcelain", "--", ".", ":(exclude)experiment/results").strip()),
        "uncommitted_diff_sha256": h.hexdigest()[:16],
    }


def host_provenance(target, statepod_bin):
    """What actually ran on one host: binary hash, base commit, CPU."""
    return {
        "statepod_bin_sha256": try_out(target, f"sha256sum {statepod_bin} | cut -c1-16"),
        "cpu_model": try_out(target, "grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs"),
        "cpu_flags_sha256": try_out(target, "grep -m1 '^flags' /proc/cpuinfo | sha256sum | cut -c1-16"),
    }


def dest_public_ip(cfg):
    """IP the source VM uses to reach the destination (migration + probes).

    With ~/.ssh/config aliases the ssh target carries no usable IP, so
    destination.public_ip must be set explicitly in hosts.yaml."""
    ip = cfg["destination"].get("public_ip", "")
    if ip:
        return ip
    return cfg["destination"]["ssh"].split("@")[-1].split(":")[0]


def tunnel_enabled(cfg):
    return bool(cfg.get("source", {}).get("tunnel", {}).get("enabled"))


def migrate_dest_addr(cfg):
    """Address the source's migrate/burst --dest dials.

    Direct WAN normally (dest_public_ip). When source.tunnel.enabled, the
    migration transport port is egress-filtered on this source host (e.g. a
    network that allows only a few outbound ports), so an SSH -L
    tunnel from the source guest itself to the destination carries it
    instead (start_migrate_tunnel/stop_migrate_tunnel below) -- matching the
    manual technique in experiment/CLOUD_MIGRATION.md. Only the migrate
    transport is tunneled: 6380 (canary/probe/load_client, all via
    dest_public_ip directly) is already reachable and untouched."""
    if tunnel_enabled(cfg):
        return "127.0.0.1"
    return dest_public_ip(cfg)


def _migrate_tunnel_forward_spec(cfg):
    port = cfg["source"].get("migrate_listen_port", "8080")
    return f"127.0.0.1:{port}:127.0.0.1:{port}"


def start_migrate_tunnel(cfg):
    """Open the SSH -L tunnel from the source guest to the destination,
    carrying migrate_listen_port. No-op unless
    source.tunnel.enabled. Only 8080 is tunneled -- the rsync-daemon
    transport (--mode precopy-rsync/sync-rsync, port 8730) is a distinct
    baseline this tunnel does not carry; main() refuses those modes on a
    tunneled profile rather than silently substituting a different
    transport."""
    t = cfg.get("source", {}).get("tunnel", {})
    if not t.get("enabled"):
        return
    stop_migrate_tunnel(cfg)  # clear any stale tunnel left by a prior crashed run
    fwd = _migrate_tunnel_forward_spec(cfg)
    key_opt = f"-i {t['key']} " if t.get("key") else ""
    # source.tunnel.also_ports: more ports through the same tunnel, e.g. "8730" so the
    # filesystem pre-copy rounds (rsync daemon) are encrypted too (lab D4)
    extra = "".join(f"-L 127.0.0.1:{p}:127.0.0.1:{p} " for p in str(t.get("also_ports", "")).split(",") if p.strip())
    cmd = (f"ssh -o StrictHostKeyChecking=accept-new -o ExitOnForwardFailure=yes "
           f"{key_opt}-f -N -L {fwd} {extra}{t['user']}@{t['host']}")
    result = ssh(cfg["source"]["ssh"], cmd, check=False, timeout=20)
    if result.returncode != 0:
        raise RuntimeError(f"migrate tunnel failed to establish ({fwd} -> "
                            f"{t['host']}): {result.stderr.strip()}")
    print(f"  migrate tunnel up: source guest 127.0.0.1:{fwd.split(':')[1]} "
          f"-> {t['host']} (via ssh)")


def stop_migrate_tunnel(cfg):
    if not tunnel_enabled(cfg):
        return
    fwd = _migrate_tunnel_forward_spec(cfg)
    ssh(cfg["source"]["ssh"], f"pkill -f 'L {fwd}' >/dev/null 2>&1 || true", check=False)


# --- experiment steps ---


def collect_manifest(cfg, args):
    src, dst = cfg["source"]["ssh"], cfg["destination"]["ssh"]
    return {
        "name": args.name,
        "timestamp": datetime.datetime.now().astimezone().isoformat(),
        "mode": args.mode,
        "workload": args.workload,
        "deterministic": getattr(args, "deterministic", False),
        "dataset": getattr(args, "dataset", "medium"),
        "dataset_keys": DATASET_SIZES.get(getattr(args, "dataset", "medium")),
        "wl_param": dict(kv.split("=", 1) for kv in getattr(args, "wl_param", []) if "=" in kv),
        "no_volume_sync": getattr(args, "no_volume_sync", False),
        "aslr_on": getattr(args, "aslr_on", False),
        # None = not passed explicitly, the statepod binary's own per-mode
        # default applies (0 for synth-precopy; 4 for precopy/criu-incr/
        # precopy-rsync, preserving the historical Baseline 2/3 shape).
        "precopy_rounds": getattr(args, "precopy_rounds", None),
        # None = not passed explicitly, the statepod binary's own default (3)
        # applies. synth-precopy only; 0 disables the zstd stage.
        "zstd_level": getattr(args, "zstd_level", None),
        "netem": args.netem or "unshaped",
        "migrate_env": list(getattr(args, "migrate_env", []) or []),
        "meta": dict(kv.split("=", 1) for kv in getattr(args, "meta", []) if "=" in kv),
        # tunneled (SSH -L) vs direct-WAN
        # migration transport. These are not comparable timing/bandwidth
        # numbers -- keep them apart in any aggregation/figure.
        "transport_tunnel": tunnel_enabled(cfg),
        "repetition": None,  # filled per rep
        "source": {
            "host": src,
            "kernel": try_out(src, "uname -r"),
            "criu": try_out(src, "criu --version | head -1"),
            "runc": try_out(src, "runc --version | head -1"),
            "statepod_git": try_out(src, "cd ~/statepod 2>/dev/null && git rev-parse --short HEAD"),
            **host_provenance(src, cfg["source"]["statepod"]),
        },
        "destination": {
            "host": dst,
            "kernel": try_out(dst, "uname -r"),
            "criu": try_out(dst, "criu --version | head -1"),
            "runc": try_out(dst, "runc --version | head -1"),
            **host_provenance(dst, cfg["destination"]["statepod"]),
        },
        "code_provenance": local_provenance(),
        "clock_skew_check": {
            "source_date": try_out(src, "date -u +%s.%N"),
            "destination_date": try_out(dst, "date -u +%s.%N"),
        },
        "client_probe": {
            "runs_on": src,
            "source_endpoint": f"127.0.0.1:{cfg.get('client', {}).get('probe_port', '6380')}",
            "dest_endpoint": (dest_public_ip(cfg) + ":"
                              + cfg.get("client", {}).get("probe_port", "6380")),
            "interval_s": cfg.get("client", {}).get("probe_interval", "0.05"),
            "timeout_s": cfg.get("client", {}).get("probe_timeout", "1.0"),
        },
        "load_client": {
            "enabled": getattr(args, "load_client", False),
            "rate_rps": cfg.get("client", {}).get("load_rate", "20"),
            "timeout_s": cfg.get("client", {}).get("load_timeout", "1.0"),
        },
        "contention": {
            "enabled": getattr(args, "contend", False),
            "port": cfg.get("experiment", {}).get("contend_port", "8090"),
            "streams": cfg.get("experiment", {}).get("contend_streams", "2"),
        },
    }


def netem_at_host_taps(cfg):
    return (cfg.get("netem") or {}).get("at") == "host_taps"


def parse_netem_spec(netem):
    """'rate 2mbit delay 200ms loss 0.1%' -> {rate_mbps, rtt_ms, loss_pct}. `delay` is the ADDED
    ROUND-TRIP time, the same meaning it had on the source-egress shaper. Jitter is refused
    rather than silently dropped: netem jitter reorders packets, which needs its own decision."""
    toks = (netem or "").split()
    out = {"rate_mbps": parse_netem_rate_mbps(netem), "rtt_ms": None, "loss_pct": None}
    for i, t in enumerate(toks):
        if t == "delay":
            out["rtt_ms"] = float(re.match(r"([0-9.]+)ms$", toks[i + 1]).group(1))
            if i + 2 < len(toks) and re.match(r"[0-9.]+(ms|us)$", toks[i + 2]):
                raise SystemExit(f"netem jitter is not supported with netem.at=host_taps: {netem!r}")
        elif t == "loss":
            out["loss_pct"] = float(toks[i + 1].rstrip("%"))
        elif t not in ("rate",) and not re.match(r"[0-9.]+([kmg]?bit|ms|%)$", t):
            raise SystemExit(f"unsupported netem token {t!r} in {netem!r} (netem.at=host_taps)")
    return out


def host_tap_plan(cfg, netem):
    """Emulated WAN as a middlebox on the host side of the VMs (lab testbed), shaping only
    VM-to-VM traffic: control traffic (ssh, harvest) is never shaped. Both directions are shaped
    on ONE host (one emulator box): data direction source -> destination (the on-prem uplink) on
    the device that carries it off the source host (fwd_dev, the VXLAN to the peer server):
    half the RTT + rate + loss; reverse direction on the source VM's tap (rev_dev): the other
    half of the RTT. Queue: netem's limit counts the packets in its delay line too, so
    limit = delay line (BDP/2) + buffer_bdp BDPs of buffer (default 1, the classic buffer = BDP
    rule), at least min_limit packets (the one emulator parameter calibrated against the real
    path on the vanilla baseline: netem_calibrate.py). Returns [(host, dev, spec, match_src,
    match_dst)]; host "local" = this machine."""
    n = cfg["netem"]
    p = parse_netem_spec(netem)
    mtu = int(n.get("mtu", 1500))
    half = f"delay {p['rtt_ms'] / 2:g}ms " if p["rtt_ms"] else ""
    fwd = half
    if p["loss_pct"]:
        fwd += f"loss {p['loss_pct']:g}% "
    if p["rate_mbps"]:
        bdp_pkts = p["rate_mbps"] * 1e6 * (p["rtt_ms"] or 1.0) / 1000 / 8 / mtu
        limit = max(int(n.get("min_limit", 100)), math.ceil((0.5 + float(n.get("buffer_bdp", 1.0))) * bdp_pkts))
        fwd += f"rate {p['rate_mbps']:g}mbit limit {limit}"
    else:
        fwd += "limit 10000"
    plan = [(n["fwd_host"], n["fwd_dev"], fwd.strip(), n["src_ip"], n["dst_ip"])]
    if p["rtt_ms"]:
        plan.append((n["rev_host"], n["rev_dev"], f"{half}limit 10000", n["dst_ip"], n["src_ip"]))
    return plan


def on_host(host, cmd, check=True):
    """Run a root command on a testbed host: over ssh, or here when host is "local"
    (the runner itself runs on the emulator host)."""
    if host == "local":
        r = subprocess.run(["bash", "-c", cmd], capture_output=True, text=True)
        if check and r.returncode != 0:
            raise RuntimeError(f"local command failed ({r.returncode}): {cmd}: {r.stderr.strip()}")
        return r
    return ssh(host, cmd, check=check)


def apply_netem(cfg, netem):
    if netem and netem_at_host_taps(cfg):
        applied = []
        for host, dev, spec, msrc, mdst in host_tap_plan(cfg, netem):
            on_host(host, f"tc qdisc del dev {dev} root 2>/dev/null; "
                          f"tc qdisc add dev {dev} root handle 1: prio bands 2 priomap {' '.join(['1'] * 16)} && "
                          f"tc qdisc add dev {dev} parent 1:1 handle 10: netem {spec} && "
                          f"tc filter add dev {dev} parent 1: protocol ip prio 1 u32 "
                          f"match ip src {msrc}/32 match ip dst {mdst}/32 flowid 1:1")
            applied.append({"host": host, "dev": dev, "netem": spec, "match": f"{msrc}->{mdst}"})
            print(f"  netem on {host}:{dev} ({msrc}->{mdst}): {spec}")
        return applied
    iface = cfg["source"].get("wan_iface", "")
    if not netem or not iface:
        return None
    src = cfg["source"]["ssh"]
    ssh(src, f"sudo tc qdisc replace dev {iface} root netem {netem}")
    print(f"  netem applied on {iface}: {netem}")
    return [{"host": src, "iface": iface, "netem": netem}]


def clear_netem(cfg):
    if netem_at_host_taps(cfg):
        n = cfg["netem"]
        for host, dev in ((n["fwd_host"], n["fwd_dev"]), (n["rev_host"], n["rev_dev"])):
            on_host(host, f"tc qdisc del dev {dev} root 2>/dev/null || true", check=False)
        return
    iface = cfg["source"].get("wan_iface", "")
    if not iface:
        return
    ssh(cfg["source"]["ssh"], f"sudo tc qdisc del dev {iface} root || true", check=False)


def parse_netem_rate_mbps(netem):
    """Bandwidth cap in Mbit/s from a tc netem spec ('rate 5mbit delay 100ms'),
    or None if the spec sets no rate (a cap is what netem was told,
    never what was measured -- the probe below measures)."""
    import re
    m = re.search(r"\brate\s+([0-9]*\.?[0-9]+)\s*([kmg]?)(?:bit|bps)", netem or "", re.I)
    if not m:
        return None
    v = float(m.group(1))
    return v * {"": 1e-6, "k": 1e-3, "m": 1.0, "g": 1e3}[m.group(2).lower()]


def iperf_probe(cfg, label, cap_mbps=None, duration=None):
    """One-shot iperf3 bandwidth probe, source -> destination.

    Starts a single-use iperf3 server on the destination, runs the client on
    the source (same direction as migration traffic, so it passes through any
    netem shaping), and returns the parsed result. Never raises — a failed
    probe is recorded in the manifest, not fatal to the run.
    """
    exp = cfg.get("experiment", {})
    port = exp.get("iperf_port", "5201")
    # Port the source dials; differs from the server port when a host relay sits
    # in between and cannot bind `port` (e.g. a reserved port range).
    client_port = exp.get("iperf_client_port", port)
    duration = int(duration if duration is not None else exp.get("iperf_duration", "5"))
    # Seconds of TCP slow-start skipped (iperf3 -O) and excluded from the reported
    # rates. At ~150 ms RTT a 3 s probe reads 19-22 Mb/s under a 30 Mb/s cap
    # (measured 2026-10-05) purely from ramp-up; 2 s omitted + 3 s measured reads
    # 28.9. The probe runs duration+omit seconds; default 0 keeps old probes
    # comparable.
    omit = int(exp.get("iperf_omit", "0"))
    src, dst = cfg["source"]["ssh"], cfg["destination"]["ssh"]
    # experiment.iperf_host: probe the LINK at this address instead of dest_public_ip. The lab's
    # channel profiles (WireGuard, SSH tunnel) set it to the destination's lab address,
    # so the validity gate checks the emulated link the same way for every channel and the
    # encryption overhead shows in the migration results, not in the gate.
    iperf_host = exp.get("iperf_host", "")
    dest_ip = iperf_host or dest_public_ip(cfg)
    result = {
        "label": label,
        "timestamp": datetime.datetime.now().astimezone().isoformat(),
        "duration_s": duration,
        "omit_s": omit,
        "port": port,
        "cap_mbps": cap_mbps,
        "host": dest_ip,
    }
    if tunnel_enabled(cfg) and not iperf_host:
        # 5201 is egress-filtered on a tunneled source profile same
        # as 8080 was. Tunneling the probe too would measure SSH's own
        # throughput ceiling, not the real WAN path -- record "unavailable"
        # rather than a number that looks real but measures the wrong thing.
        result["error"] = "unavailable (egress-filtered, tunneled source profile)"
        return result
    try:
        ssh(dst, f"pkill -x iperf3 >/dev/null 2>&1; "
                 f"nohup iperf3 -s -1 -p {port} >/dev/null 2>&1 & sleep 0.5",
            check=False, timeout=15)
        # With a cap, offer exactly the cap: 3 s at the cap measures whether the
        # path can deliver it, at ~1/20 of the data of a full-speed probe.
        # experiment.iperf_pacing: "iperf" (default, -b: iperf3 paces in user space)
        # or "fq" (--fq-rate: the kernel paces the socket). On the lab's emulated
        # 200 ms path -b read 0.67-0.70 Mb/s for a 2 Mb/s cap in 5 of 5 probes while
        # --fq-rate read 1.908 in 5 of 5 (probe_variants.py, 2026-10-26).
        pacing = exp.get("iperf_pacing", "iperf")
        result["pacing"] = pacing
        rate = (f" --fq-rate {cap_mbps:g}M" if pacing == "fq" else f" -b {cap_mbps:g}M") if cap_mbps else ""
        omit_arg = f" -O {omit}" if omit else ""
        out = ssh(src, f"iperf3 -c {dest_ip} -p {client_port} -t {duration + omit}{omit_arg}{rate} -J",
                  check=False, timeout=duration + omit + 30)
        data = json.loads(out.stdout) if out.stdout.strip() else {}
        if not data or "error" in data:
            result["error"] = data.get("error") or out.stderr.strip() or "no output"
            return result
        sent = data["end"]["sum_sent"]
        recv = data["end"]["sum_received"]
        result.update({
            "send_mbps": round(sent["bits_per_second"] / 1e6, 2),
            "recv_mbps": round(recv["bits_per_second"] / 1e6, 2),
            "retransmits": sent.get("retransmits"),
        })
    except Exception as e:  # ssh timeout, missing iperf3, malformed JSON, ...
        result["error"] = f"probe failed: {e}"
    finally:
        ssh(dst, "pkill -x iperf3 >/dev/null 2>&1 || true", check=False, timeout=15)
    return result


# --- container lifecycle (replaces manual source.sh / destination.sh) ---


def wait_redis(target, cli_args, attempts=30):
    """Poll redis-cli until PONG (1 s between attempts).

    `timeout 3`: a DNAT misconfiguration makes redis-cli hang rather than
    fail, which would otherwise blow up the ssh() timeout as an exception."""
    for _ in range(attempts):
        try:
            out = ssh(target, f"timeout 3 redis-cli {cli_args} ping",
                      check=False, timeout=15)
        except subprocess.TimeoutExpired:
            continue
        if out.stdout.strip() == "PONG":
            return True
        time.sleep(1)
    return False


def wait_http(target, host, port, attempts=30):
    """Poll the Node HTTP server until it answers 200 (1 s between attempts)."""
    for _ in range(attempts):
        try:
            out = ssh(target, f"curl -s -o /dev/null -w '%{{http_code}}' "
                              f"--max-time 3 http://{host}:{port}/",
                      check=False, timeout=15)
        except subprocess.TimeoutExpired:
            continue
        if out.stdout.strip() == "200":
            return True
        time.sleep(1)
    return False


LAST_GUARD = {}  # destination memory reading taken after cleanup, before the run (copied to the manifest)


class MemoryGuardError(RuntimeError):
    """The destination VM does not have the memory this run needs (exit code 75)."""


# A small destination (~1 GB RAM, no swap) does not fail cleanly when a run overshoots:
# the kernel OOM-kills statepod and the VM can wedge until a console reboot. So: look before, look after.
def dest_mem_state(cfg):
    """MemAvailable / Shmem (kB) and the count of kernel OOM kills since boot on the destination."""
    dst = cfg["destination"]["ssh"]
    r = ssh(dst, "awk '/MemAvailable/{a=$2} /^Shmem:/{s=$2} END{print a, s}' /proc/meminfo; "
                 "sudo dmesg 2>/dev/null | grep -c -E 'Out of memory|oom-kill:'; true",
            check=False, timeout=60)
    try:
        lines = r.stdout.splitlines()
        avail, shmem = (int(x) for x in lines[0].split())
        return {"mem_avail_mb": avail // 1024, "shmem_mb": shmem // 1024, "oom_kills": int(lines[1])}
    except Exception:
        raise MemoryGuardError(f"destination unreachable or unreadable (ssh rc={r.returncode}): {(r.stderr or r.stdout)[-200:]!r}")


def mem_guard(cfg, args):
    """Refuse to start a run when the destination is already short of memory. Called after the
    cleanup in setup_containers (stale tmpfs checkpoints unmounted), so a low reading is real."""
    st = dest_mem_state(cfg)
    need = args.dest_min_avail_mb
    if st["mem_avail_mb"] < need:
        raise MemoryGuardError(f"destination MemAvailable {st['mem_avail_mb']} MB < {need} MB required "
                               f"(shmem {st['shmem_mb']} MB) -- not starting this run")
    return st


def setup_containers(cfg, args):
    """Recreate both bundles from the pinned image and start the source workload.

    Non-interactive equivalent of source.sh + destination.sh: kill stale
    statepod/runc state, wipe the bundle dirs, `statepod create` on both ends,
    start the source container, wait for Redis, seed data for post-migration
    verification. Returns the canary value written into Redis."""
    src = cfg["source"]["ssh"]
    exp = cfg.get("experiment", {})
    wl = workloads.get(args.workload)
    image = wl.image(cfg)
    port = cfg.get("client", {}).get("probe_port", "6380")
    wl.prepare(sys.modules[__name__], cfg, args)

    for role in ("source", "destination"):
        host = cfg[role]
        t, wd, pod = host["ssh"], host["workdir"], host["statepod"]
        ssh(t, "sudo timedatectl set-ntp true", check=False, timeout=30)
        # CRIU options runc exposes no flags for -> /etc/criu/runc.conf:
        #  skip-in-flight: probe keeps half-open connections to Redis at freeze
        #    time; without it the final dump aborts ("In-flight connection").
        #  tcp-close: dump/restore established client connections as closed —
        #    their peer (the probe via loopback DNAT) does not exist on the
        #    destination, so repairing them fails ("Can't connect inet socket
        #    back"). Clients are expected to reconnect after failover.
        for opt in ("skip-in-flight", "tcp-close"):
            ssh(t, f"sudo mkdir -p /etc/criu && "
                   f"grep -qxF {opt} /etc/criu/runc.conf 2>/dev/null || "
                   f"echo {opt} | sudo tee -a /etc/criu/runc.conf >/dev/null",
                check=False, timeout=30)
        # Stateful bundles  leave "rootfs" as a live
        # overlay mount (lowerdir=lower, upperdir=upper, workdir=work, all
        # root-owned) — rm -rf alone can't remove a mountpoint, and a stale
        # mount left in place would make the *next* create's fresh lower/
        # upper invisible (MountOverlay is idempotent and would just see
        # "rootfs" already mounted and skip remounting onto the new dirs).
        # Unmount before wiping so every rep starts from a clean, unmounted
        # bundle dir. mkdir -p {wd} runs first since umount would otherwise
        # fail the first time a workdir doesn't exist yet.
        # Leftover background clients from a rep that died mid-migration
        # (probe / lost-write / load client) would keep writing into the NEW
        # container (a stale lost-write client broke the next run's seed
        # check, 2026-10-02).
        ssh(t, "pkill -f '[s]tatepod_(probe|lostwrite|load_client|conn)' || true", check=False)
        # A run that died mid-restore (lazy-pages / CRIU restore) leaves a `criu`
        # process and cgroup-yard / mount-namespace mounts under the bundle's
        # checkpoint dir; the next rep's `rm -rf checkpoint/lazy_restore` then fails
        # ("Operation not permitted") and poisons every later run (2026-10-06).
        # Brackets keep pkill from matching this very shell's command line.
        ssh(t, "sudo pkill -9 -f '[c]riu (restore|lazy-pages|dump|page-server)' || true; "
               "sudo pkill -f '[s]tatepod (start|migrate)' || true; "
               "mount | awk '{print $3}' | grep -E '\\.criu\\.(cgyard|mntns)' | sort -r | "
               "xargs -r -n1 sudo umount -l || true",
            check=False, timeout=60)
        # Unmount EVERYTHING under the bundle dir (overlay rootfs, checkpoint tmpfs,
        # restored container's nested mounts), deepest first, falling back to a lazy
        # unmount when busy. A plain `umount checkpoint || true` silently left a
        # 360 MB tmpfs behind after a failed run and the next run OOM-aborted
        # (2026-10-08). Then verify: a leftover mount or a surviving container is a
        # hard error here, not a mysterious OOM later.
        r = ssh(t, f"sudo pkill -f 'statepod (start|migrate)' || true; "
                   f"sudo runc list -q 2>/dev/null | xargs -r -n1 sudo runc delete -f; "
                   f"mkdir -p {wd} && cd {wd} && D=$(pwd -P) && "
                   f"for m in $(mount | awk -v p=\"$D/\" 'index($3,p)==1{{print $3}}' | sort -r); do "
                   f"sudo umount \"$m\" 2>/dev/null || sudo umount -l \"$m\" 2>/dev/null || true; done; "
                   f"sudo rm -rf rootfs checkpoint data lower upper work "
                   f"statepod.json config.json README.md rootfs.tar start.log; "
                   f"if mount | awk -v p=\"$D/\" 'index($3,p)==1' | grep -q .; then echo CLEANUP_FAILED_MOUNT; fi; "
                   f"if [ -n \"$(sudo runc list -q 2>/dev/null)\" ]; then echo CLEANUP_FAILED_CONTAINER; fi; "
                   f"if [ -e {wd}/checkpoint ]; then echo CLEANUP_FAILED_RM; fi",
                check=False, timeout=90)
        if "CLEANUP_FAILED" in (r.stdout or ""):
            raise RuntimeError(f"cleanup on {role} left state behind: {r.stdout.strip()[-200:]}")
        flags = wl.flags(sys.modules[__name__], cfg, args)
        if getattr(args, "deterministic", False):
            flags += " --deterministic"
        print(f"  creating {role} bundle from {image}...")
        ssh(t, f"cd {wd} && sudo {pod} create '{image}' {flags} --host-port {port}",
            timeout=600)
        wl.post_create(sys.modules[__name__], t, wd)

    src_wd, src_pod = cfg["source"]["workdir"], cfg["source"]["statepod"]
    # sync/sync-rsync actually move the container's data dir on migration only
    # if the source was started with --volume-sync (sets cfg.VolumeSync=true,
    # read by SyncStrategy); without it, "restart" degenerates to a fresh
    # empty container at the destination (no data to lose, nothing wrong, but
    # not what these modes are meant to measure).
    volume_sync = args.mode in ("sync", "sync-rsync") and not args.no_volume_sync
    start_flags = "--volume-sync" if volume_sync else ""
    # Resolve ~ in the user's shell BEFORE sudo: under `sudo bash -c` a quoted
    # ~/... would expand to /root/... and the binary would not be found.
    ssh(src, f'cd {src_wd} && POD=$(readlink -f {src_pod}) && '
             f'sudo bash -c "nohup $POD start {start_flags} >> start.log 2>&1 &"',
        timeout=30)

    rx = sys.modules[__name__]
    wl.wait_ready(rx, src, port, src_wd)
    return wl.seed(rx, cfg, args, port)


def ensure_template(cfg, args):
    """Synthesize the destination-side baseline template if it isn't stored yet.

    the destination cold-starts a throwaway instance of the image,
    CRIU-dumps it at readiness, and stores it (plus its page-hash manifest)
    content-addressed by image digest. This is off the critical path and
    reusable across every instance of that image, so we only pay it once per
    image digest per destination host — hence the existence check."""
    dst = cfg["destination"]["ssh"]
    dst_wd = cfg["destination"]["workdir"]
    dst_pod = cfg["destination"]["statepod"]
    exp = cfg.get("experiment", {})
    wl = workloads.get(args.workload)
    image = wl.image(cfg)
    flags = wl.flags(sys.modules[__name__], cfg, args)

    # Take the digest from the destination bundle statepod just created rather
    # than re-resolving it: it is exactly the image_id the source will ask the
    # manifest for, so the lookup can't drift from what the migration uses.
    # `statepod template key` = image digest, plus a spec fingerprint for
    # parameterised workloads (the built-ins keep the bare digest).
    image_id = ssh(dst, f"cd {dst_wd} && POD=$(readlink -f {dst_pod}) && $POD template key",
                   check=False, timeout=30).stdout.strip().splitlines()
    image_id = image_id[-1] if image_id else ""
    if not image_id:
        raise RuntimeError("destination bundle has no template key (no image_id?) — cannot key the template store")
    digest = image_id.replace("sha256:", "")
    # The receiver runs under sudo, so its template store is root's.
    manifest_path = f"/root/.statepod/templates/{digest}/manifest.spm"
    if ssh(dst, f"sudo test -f {manifest_path}", check=False, timeout=30).returncode == 0:
        print(f"  destination template already present for {digest[:12]}")
        return
    print(f"  synthesizing destination template for {image} ({digest[:12]})...")
    ssh(dst, f"cd {dst_wd} && POD=$(readlink -f {dst_pod}) && "
             f"sudo $POD template generate '{image}' {flags}", timeout=900)
    if ssh(dst, f"sudo test -f {manifest_path}", check=False, timeout=30).returncode != 0:
        raise RuntimeError(f"template generate did not produce {manifest_path}")


def verify_state(cfg, args, canary, attempts=30):
    """After migration: check the destination workload from the source VM.

    's "restart" baseline (--mode sync --no-volume-sync) never attempts
    to preserve state by design — the destination gets a fresh empty
    container, so a canary/dbsize mismatch there is the expected outcome, not
    a test failure. "ok" for that variant means the destination is serving
    again, not that data survived.

    Retries with the same budget as the startup readiness probes
    (wait_redis/wait_http, 30 attempts/1s) rather than a single shot or a
    short custom retry. History here: a first version of this function had
    no retry at all, and core-m5-postcopy-node rep 4 in the matrix
    failed with http_status 000 (curl couldn't connect at all) while both
    source and destination logs showed a normal, complete migration --
    looked like M5's lazy-restore fault-latency tail racing a one-shot
    check, so a 5-attempt/1s retry was added. That turned out to be the
    wrong theory AND an insufficient fix: re-running the same cell with the
    5-attempt retry still failed 2/5, with no correlation to drain time
    (the slowest-draining rep passed, one of the fastest failed) -- ruling
    out fault-latency as the cause and pointing at something that just
    needs a longer window to clear, hence matching the already-proven
    30-attempt readiness budget instead of guessing a new number."""
    port = cfg.get("client", {}).get("probe_port", "6380")
    ip = dest_public_ip(cfg)
    return workloads.get(args.workload).verify(
        sys.modules[__name__], cfg, args, canary, ip, port, attempts)


PROBE_REMOTE = "/tmp/statepod_probe.py"


def start_probe(cfg, args, run_dir):
    """Failover availability probe on the source VM (see statepod/docs/testbed.md).

    probe.sh prefers the source's own host-forwarded port (127.0.0.1 works
    because statepod adds an OUTPUT-chain DNAT rule) and fails over to the
    destination's host port when the source stops answering. Downtime =
    gap between last answered source probe and first answered dest probe
    (probe.py JSON lines; experiment/analysis/probe_outage.py).
    """
    client = cfg.get("client", {})
    port = client.get("probe_port", "6380")
    interval = client.get("probe_interval", "0.05")
    conn_timeout = client.get("probe_timeout", "1.0")
    src = cfg["source"]["ssh"]
    dest_ip = dest_public_ip(cfg)
    scp_to(src, Path(__file__).resolve().parent / "probe.py", PROBE_REMOTE)
    cmd = (f"python3 {PROBE_REMOTE} {workloads.get(args.workload).probe_kind} 127.0.0.1:{port} "
           f"{dest_ip}:{port} {interval} {conn_timeout}")
    return ssh_bg_file(src, cmd, "/tmp/statepod_probe.log", "probe.log")


def stop_probe(cfg, probe):
    probe.terminate()
    ssh(cfg["source"]["ssh"], f"pkill -f {PROBE_REMOTE} || true", check=False)


def start_contention(cfg, run_dir):
    """WAN-contention experiment: a sustained background bulk
    transfer (iperf3, source -> destination -- same direction as migration
    traffic) saturating the shared link for the rep's duration, so downtime/
    SLA degradation under contention can be measured against an uncontended
    baseline. A different port from the pre/post link probe's iperf_port
 -- that probe's server is one-shot (`iperf3 -s -1`) and would
    otherwise collide with a persistent flood server on the same port.
    Started right before the migrate trigger and stopped the same place the
    probe/load client are (after receiver.wait), same lifecycle pattern."""
    exp = cfg.get("experiment", {})
    port = exp.get("contend_port", "8090")
    streams = exp.get("contend_streams", "2")
    src, dst = cfg["source"]["ssh"], cfg["destination"]["ssh"]
    dest_ip = dest_public_ip(cfg)
    ssh(dst, f"pkill -f 'iperf3 -s -p {port}' >/dev/null 2>&1; "
             f"nohup iperf3 -s -p {port} >/dev/null 2>&1 & sleep 0.5",
        check=False, timeout=15)
    # -t 0 = run until killed; a migration's length isn't known up front, so
    # stop_contention kills it rather than guessing a fixed duration.
    # stdbuf -oL: iperf3's periodic report is otherwise fully block-buffered
    # once its stdout is a pipe (not a tty, true of every ssh_bg command) --
    # a plain kill loses the whole buffered log with nothing ever flushed,
    # confirmed on the VM pair (empty contention.log with a bare `iperf3 -c`,
    # real interval lines survive the same kill with stdbuf -oL in front).
    cmd = f"stdbuf -oL iperf3 -c {dest_ip} -p {port} -P {streams} -t 0"
    return ssh_bg(src, cmd, run_dir / "contention.log")


def stop_contention(cfg, contention):
    exp = cfg.get("experiment", {})
    port = exp.get("contend_port", "8090")
    # Remote pkill FIRST, local terminate() after: killing the local ssh
    # Popen first closes the channel immediately, and iperf3's own buffered
    # report (even under stdbuf -oL) then has nowhere to flush to -- lost
    # the whole contention.log content that way (confirmed on the VM pair).
    # Sending SIGTERM to the remote process first, while the channel is
    # still open, lets it flush before the connection goes away.
    ssh(cfg["source"]["ssh"], "pkill -f 'iperf3 -c' || true", check=False)
    try:
        contention.wait(timeout=5)
    except subprocess.TimeoutExpired:
        contention.terminate()
    ssh(cfg["destination"]["ssh"], f"pkill -f 'iperf3 -s -p {port}' || true", check=False)


LOAD_CLIENT_REMOTE = "/tmp/statepod_load_client.py"
LOSTWRITE_REMOTE = "/tmp/statepod_lostwrite.py"
CONN_REMOTE = "/tmp/statepod_conn_client.py"


def start_conn_client(cfg, run_dir):
    """connection-continuity client on the source VM: one long-lived connection
    with a write in flight; log -> conn_client.log (analysis/conn_continuity.py)."""
    client = cfg.get("client", {})
    port = client.get("probe_port", "6380")
    src = cfg["source"]["ssh"]
    scp_to(src, Path(__file__).resolve().parent / "conn_client.py", CONN_REMOTE)
    cmd = f"python3 {CONN_REMOTE} --src 127.0.0.1:{port} --dst {dest_public_ip(cfg)}:{port}"
    return ssh_bg_file(src, cmd, "/tmp/statepod_conn_client.log", "conn_client.log")


def stop_conn_client(cfg, proc):
    proc.terminate()
    ssh(cfg["source"]["ssh"], f"pkill -f {CONN_REMOTE} || true", check=False)

UPSTREAM_REMOTE = "/tmp/statepod_upstream_service.py"


def start_upstream(cfg):
    """external service on the DESTINATION host (outside any container); log ->
    upstream.log (analysis/outbound_continuity.py)."""
    dst = cfg["destination"]["ssh"]
    port = cfg["destination"]["upstream_addr"].rpartition(":")[2]
    scp_to(dst, Path(__file__).resolve().parent / "upstream_service.py", UPSTREAM_REMOTE)
    ssh(dst, f"pkill -f {UPSTREAM_REMOTE} || true", check=False)
    return ssh_bg_file(dst, f"python3 {UPSTREAM_REMOTE} --port {port}", "/tmp/statepod_upstream.log", "upstream.log")


def stop_upstream(cfg, proc, wait_reconnect_s=20):
    """Give the application time to re-establish the connection after the restore (the
    interesting number), then stop the service."""
    dst = cfg["destination"]["ssh"]
    deadline = time.time() + wait_reconnect_s
    while time.time() < deadline:
        n = try_out(dst, "grep -c '\"event\": \"conn\"' /tmp/statepod_upstream.log")
        if n.isdigit() and int(n) >= 2:
            time.sleep(3)          # a few heartbeats on the new connection
            break
        time.sleep(1)
    proc.terminate()
    ssh(dst, f"pkill -f {UPSTREAM_REMOTE} || true", check=False)


SAMPLER_REMOTE = "/tmp/statepod_source_sampler.py"
SRC_TIME_REMOTE = "/tmp/statepod_migrate_time.txt"   # `/usr/bin/time -v` of the migrate tree


def start_source_sampler(cfg):
    """Resource sampler on the source VM (source_sampler.py): CPU, memory, tmpfs
    checkpoint size every 0.25 s -> source_resources.log. Cost of the migration to
    the source (reclaim metrics); summarised by analysis/source_cost.py."""
    src = cfg["source"]["ssh"]
    scp_to(src, Path(__file__).resolve().parent / "source_sampler.py", SAMPLER_REMOTE)
    ckpt = f"{cfg['source']['workdir']}/checkpoint"
    cmd = f"python3 {SAMPLER_REMOTE} --ckpt-dir {ckpt}"
    return ssh_bg_file(src, cmd, "/tmp/statepod_source_resources.log", "source_resources.log")


def stop_source_sampler(cfg, proc):
    proc.terminate()
    ssh(cfg["source"]["ssh"], f"pkill -f {SAMPLER_REMOTE} || true", check=False)


def start_load_client(cfg, args, run_dir):
    """Per-request SLA client -- same deployment pattern as
    start_probe, run in parallel with it over the same window. Redis gets
    --keyspace matching the rep's seeded key count (key:1..key:N, same
    convention setup_containers seeds with) so requests exercise real data
    instead of just PING; node has no per-key data, so key/key_class stay
    null in the log and it's GET / traffic only."""
    client = cfg.get("client", {})
    port = client.get("probe_port", "6380")
    rate = client.get("load_rate", "20")
    timeout = client.get("load_timeout", "1.0")
    src = cfg["source"]["ssh"]
    dest_ip = dest_public_ip(cfg)
    scp_to(src, Path(__file__).resolve().parent / "load_client.py", LOAD_CLIENT_REMOTE)
    kind = "redis" if isinstance(workloads.get(args.workload), workloads.Redis) else args.workload
    cmd = (f"python3 {LOAD_CLIENT_REMOTE} --workload {kind} "
           f"--src 127.0.0.1:{port} --dst {dest_ip}:{port} "
           f"--rate {rate} --timeout {timeout}")
    if kind == "redis":
        cmd += f" --keyspace {DATASET_SIZES[args.dataset]}"
    return ssh_bg_file(src, cmd, "/tmp/statepod_load_client.log", "load_client.log")


def start_lostwrite(cfg, run_dir):
    """lost-write client on the source VM; ack log -> lostwrite.log."""
    client = cfg.get("client", {})
    port = client.get("probe_port", "6380")
    src = cfg["source"]["ssh"]
    scp_to(src, Path(__file__).resolve().parent / "lostwrite_client.py", LOSTWRITE_REMOTE)
    cmd = (f"python3 {LOSTWRITE_REMOTE} --src 127.0.0.1:{port} "
           f"--dst {dest_public_ip(cfg)}:{port} --rate {client.get('lostwrite_rate', '100')}")
    return ssh_bg_file(src, cmd, "/tmp/statepod_lostwrite.log", "lostwrite.log")


def stop_lostwrite(cfg, proc):
    proc.terminate()
    ssh(cfg["source"]["ssh"], f"pkill -f {LOSTWRITE_REMOTE} || true", check=False)


def verify_lostwrite(cfg, run_dir, target=None):
    """Read every acknowledged write back from the destination (from the source
    VM, i.e. as a client would) and count the missing/corrupt ones. `target`
    overrides the destination (an aborted migration is checked against the SOURCE,
    which must have resumed with every acknowledged write)."""
    src = cfg["source"]["ssh"]
    port = cfg.get("client", {}).get("probe_port", "6380")
    remote_log = "/tmp/statepod_lostwrite.log"  # written there by the client itself
    out = try_out(src, f"python3 {LOSTWRITE_REMOTE} --verify {remote_log} "
                       f"--target {target or f'{dest_public_ip(cfg)}:{port}'}")
    try:
        return json.loads(out.splitlines()[-1])
    except Exception:
        return {"lost": None, "error": out[-300:]}


def stop_load_client(cfg, load_client):
    load_client.terminate()
    ssh(cfg["source"]["ssh"], f"pkill -f {LOAD_CLIENT_REMOTE} || true", check=False)


# Lines that mean the run actually failed, even if state_verify.ok looks
# fine (e.g. a mid-stream retry that happened to still land the migration).
# `level=error` (not `level=warning`) is deliberate: runc's own logrus wraps
# every internal CRIU dump/restore log line as level=warning regardless of
# its own "Error (...)" content — those are routine CRIU diagnostics (some
# expected by design, e.g. the documented skip-in-flight socket-repair
# failure in setup_containers), not failures. A genuine failure surfaces as
# runc's *own* level=error, a bare "Error:"-prefixed statepod CLI line, a Go
# panic, or the receiver's "Error handling stream type" wrapper.
HARD_ERROR_PATTERNS = [
    re.compile(r"^Error:\s"),
    re.compile(r"level=error", re.I),
    re.compile(r"panic:", re.I),
    re.compile(r"\bFATAL\b"),
    re.compile(r"Error handling stream type"),
    # CRIU's own assertion: seen once, a lazy restore that reported success with a dead service
    re.compile(r"BUG at criu/"),
]
# Anything else mentioning error/panic/fatal/warn is recorded (not hidden)
# but doesn't fail the run — mostly the CRIU internal-log noise above.
NOISE_LINE_PATTERN = re.compile(r"error|panic|fatal|warn", re.I)


def scan_log(path):
    """Classify a log file's error-signal lines: hard failures vs recorded noise."""
    result = {"path": path.name, "exists": path.exists(), "hard_errors": [], "other_lines": 0}
    if not result["exists"]:
        return result
    for line in path.read_text(errors="replace").splitlines():
        if any(p.search(line) for p in HARD_ERROR_PATTERNS):
            result["hard_errors"].append(line.strip())
        elif NOISE_LINE_PATTERN.search(line):
            result["other_lines"] += 1
    return result


def fault_abort_verdict(cfg, args, run_dir, canary, rc, inj, t_end):
    """A fault run whose migration did NOT complete (migrate exited non-zero). The pass
    criterion: the source resumed and serves, every acknowledged write is
    still there (lost-write client, read back from the SOURCE) and the destination is
    not running a second copy. Anything else is `failed_unsafe`."""
    src, wd = cfg["source"]["ssh"], cfg["source"]["workdir"]
    dst, dwd = cfg["destination"]["ssh"], cfg["destination"]["workdir"]
    port = cfg.get("client", {}).get("probe_port", "6380")
    cid = "$(python3 -c \"import json;print(json.load(open('statepod.json'))['container_id'])\")"

    def status(target, wdir):
        return try_out(target, f"cd {wdir} && sudo runc state {cid} 2>/dev/null | "
                               f"python3 -c \"import json,sys;print(json.load(sys.stdin)['status'])\"") or "absent"

    f = {**inj.summary(), "migrate_rc": rc, "outcome": None,
         "time_to_fallback_s": round(t_end - inj.injected_at, 1) if inj.injected_at else None,
         "source_container": status(src, wd), "destination_container": status(dst, dwd)}
    # What clients see decides, not `runc state`: a receiver killed just after the restore left a
    # destination serving writes while its state read as absent, and the run then treated it as
    # not running (2026-11-01).
    if (f["destination_container"] != "running" and isinstance(workloads.get(args.workload), workloads.Redis)
            and try_out(src, f"timeout 5 redis-cli -h {dest_public_ip(cfg)} -p {port} get migration_canary") == canary):
        f["dest_state_said"] = f["destination_container"]
        f["dest_runc_list"] = try_out(dst, "sudo runc list")[:400]
        f["destination_container"] = "running"
    serving = f["source_container"] == "running"
    # A paused source accepts connections (the kernel does) and never answers: reads below
    # would block, which hung these runs for the whole 30-minute run timeout (2026-10-29).
    readable = serving
    if isinstance(workloads.get(args.workload), workloads.Redis):
        got = try_out(src, f"redis-cli -h 127.0.0.1 -p {port} get migration_canary") if readable else None
        f["source_canary_ok"] = got == canary
        serving = serving and got == canary
    v = {"fault": f}
    lost_ok = True
    if args.lost_write and readable:
        lw = verify_lostwrite(cfg, run_dir, target=f"127.0.0.1:{port}")
        v["lost_writes"] = lw
        lost_ok = lw.get("lost") == 0
    split = f["destination_container"] == "running" and serving
    # Lost-ACK commit: the source stopped and exited non-zero (it never saw the destination's
    # ACK) but the destination restored and serves. Not a lost service; verified from the
    # client side (canary + every acknowledged write read back from the DESTINATION).
    dest_ok = False
    if f["destination_container"] == "running" and f["source_container"] != "running":
        dip = dest_public_ip(cfg)
        if isinstance(workloads.get(args.workload), workloads.Redis):
            # retry: right after a partition the destination's port path (relay, DNAT) may need a few
            # seconds; a single read gave a false failed_unsafe for m5 partition@drain (2026-10-08)
            got = None
            for _ in range(8):
                got = try_out(src, f"redis-cli -h {dip} -p {port} get migration_canary")
                if got == canary:
                    break
                time.sleep(2)
            f["dest_canary_ok"] = got == canary
            dest_ok = got == canary
        else:
            dest_ok = True
        if dest_ok and args.lost_write:
            lw = verify_lostwrite(cfg, run_dir)           # default target = the destination
            v["lost_writes"] = lw
            dest_ok = lw.get("lost") == 0
    # Held: migrate could not tell whether the destination restored, so it left the source paused
    # rather than risk two live copies (synth.go, merged restore). The service is down until an
    # operator acts, so this is not a pass; what an operator's `runc resume` would recover is
    # measured here, by doing it.
    held = f["source_container"] == "paused" and f["destination_container"] != "running"
    if held:
        try_out(src, f"cd {wd} && sudo runc resume {cid}")
        time.sleep(2)
        rec = {"source_container": status(src, wd)}
        if isinstance(workloads.get(args.workload), workloads.Redis):
            rec["canary_ok"] = try_out(src, f"redis-cli -h 127.0.0.1 -p {port} get migration_canary") == canary
        if args.lost_write:
            rec["lost_writes"] = verify_lostwrite(cfg, run_dir, target=f"127.0.0.1:{port}")
        f["after_operator_resume"] = rec
    f["outcome"] = ("aborted_source_resumed" if serving and lost_ok and not split else
                    "split_brain" if split else
                    "completed_ack_lost" if dest_ok else
                    "held_paused" if held else "failed_unsafe")
    v["ok"] = f["outcome"] in ("aborted_source_resumed", "completed_ack_lost")
    v["log_scan"] = {"source": scan_log(run_dir / "source_migrate.log"),
                     "destination": scan_log(run_dir / "destination_receiver.log"), "clean": True}
    return v


def run_once(cfg, args, run_dir, rep):
    src, dst = cfg["source"]["ssh"], cfg["destination"]["ssh"]
    src_wd = cfg["source"]["workdir"]
    dst_wd = cfg["destination"]["workdir"]
    src_pod = cfg["source"]["statepod"]
    dst_pod = cfg["destination"]["statepod"]
    port = cfg["source"].get("migrate_listen_port", "8080")
    dest_ip = migrate_dest_addr(cfg)

    metrics_remote = f"{dst_wd}/migration_metrics.json"
    rsync_transport = args.mode in ("precopy-rsync", "sync-rsync")
    rsync_port = cfg.get("experiment", {}).get("rsync_port", "8730")

    # 0. Fresh containers on both ends; source running + seeded.
    canary = setup_containers(cfg, args)
    # after the cleanup in setup_containers: a restored container left over from the previous run would
    # otherwise reconnect to this fresh service before being killed (seen 2026-10-08). The source app
    # retries every 0.2 s, so it connects as soon as the service is up.
    upstream = start_upstream(cfg) if args.outbound else None
    LAST_GUARD.clear()
    LAST_GUARD.update(mem_guard(cfg, args))  # raises MemoryGuardError (exit 75) if the destination is short

    # 0b. synth-precopy needs the destination's synthesized baseline in place
    # before the burst — it is what the manifest and the reassembly resolve
    # against. "burst" now internally dispatches stateful
    # containers to synth-precopy too (burst runs the full M1-M4 +
    # compression stack, not Baseline 2's plain precopy), so it needs the
    # same prerequisite whenever the workload is stateful (redis, here).
    if args.mode in ("synth-precopy", "synth-postcopy") or (args.mode == "burst" and workloads.get(args.workload).stateful):
        ensure_template(cfg, args)

    # 1. Start receiver on destination (backgrounded over SSH).
    # experiment.tmpfs_size: size of the RAM-backed checkpoint dirs (STATEPOD_TMPFS_SIZE,
    # migration/tmpfs.go; default 512m, sized for a ~1 GB destination)
    tsz = cfg.get("experiment", {}).get("tmpfs_size", "")
    tenv = f"env STATEPOD_TMPFS_SIZE={tsz} " if tsz else ""
    recv_cmd = (f"cd {dst_wd} && sudo {tenv}{dst_pod} start --source {src} "
                f"--listen 0.0.0.0:{port} --metrics-out {metrics_remote}")
    # Companion rsync daemon, always: the filesystem pre-copy rounds of every variant
    # (fsprecopy.go) write the upper layer through it, and the 
    # transport A/B (precopy-rsync/sync-rsync) uses its checkpoint/rootfs modules.
    recv_cmd += f" --rsync-port {rsync_port}"
    if args.tmpfs_checkpoint:
        recv_cmd += " --tmpfs-checkpoint"
    receiver = ssh_bg(dst, recv_cmd, run_dir / "destination_receiver.log")
    time.sleep(3)
    if receiver.poll() is not None:
        raise RuntimeError("receiver exited early — see destination_receiver.log")

    # 2. Start availability probe (+ optional SLA load client + optional WAN
    # contention flood, same window).
    probe = start_probe(cfg, args, run_dir)
    load_client = start_load_client(cfg, args, run_dir) if args.load_client else None
    if args.lost_write and not workloads.get(args.workload).supports_lostwrite:
        raise RuntimeError(f"--lost-write is not implemented for workload {args.workload!r}")
    lostwrite = start_lostwrite(cfg, run_dir) if args.lost_write else None
    if args.conn_client and not workloads.get(args.workload).supports_lostwrite:
        raise RuntimeError(f"--conn-client is Redis-only (needs the RESP SET/GET client), not {args.workload!r}")
    conn = start_conn_client(cfg, run_dir) if args.conn_client else None
    contention = start_contention(cfg, run_dir) if args.contend else None
    sampler = start_source_sampler(cfg)
    time.sleep(6)  # a quiet-host baseline before the migration command (source_cost.BASELINE_S)

    # 3. Trigger migration on the source. "burst" exercises the statefulness
    # dispatcher itself (internally picks precopy/sync from the container's
    # `stateful` classification) rather than forcing a mode.
    # `/usr/bin/time -v` wraps the whole migrate tree (statepod + the runc/criu it
    # waits for): exact CPU seconds and largest-process RSS for source_cost.py.
    timev = f"/usr/bin/time -v -o {SRC_TIME_REMOTE} "
    if args.mode == "burst":
        mig_cmd = f"cd {src_wd} && {timev}sudo {src_pod} burst --dest {dest_ip}:{port}"
    else:
        menv = "".join(f"{kv} " for kv in getattr(args, "migrate_env", []) if "=" in kv)
        menv = f"env {menv}" if menv else ""
        mig_cmd = (f"cd {src_wd} && {timev}sudo {menv}{src_pod} migrate "
                   f"--mode {args.mode} --dest {dest_ip}:{port}")
        if rsync_transport:
            mig_cmd += f" --rsync-port {rsync_port}"
        # Only forward when the user set it explicitly (default None): the
        # statepod binary picks its own per-mode default otherwise (0 for
        # synth-precopy, 4 for precopy/criu-incr/precopy-rsync).
        if args.precopy_rounds is not None and args.mode in (
                "precopy", "criu-incr", "precopy-rsync", "synth-precopy"):
            mig_cmd += f" --precopy-rounds {args.precopy_rounds}"
        if args.zstd_level is not None and args.mode in ("synth-precopy", "precopy", "criu-incr", "restart-volume"):
            mig_cmd += f" --zstd-level {args.zstd_level}"
        if getattr(args, "drain_timeout", None) and args.mode in ("synth-postcopy", "criu-lazy"):
            mig_cmd += f" --drain-timeout {args.drain_timeout}"
        if args.tmpfs_checkpoint:
            mig_cmd += " --tmpfs-checkpoint"
        if getattr(args, "no_fs_upper", False) and args.mode == "synth-precopy":
            mig_cmd += " --no-fs-upper"
        if getattr(args, "final_mode", None) and args.mode in ("synth-precopy", "synth-postcopy"):
            mig_cmd += f" --final-mode {args.final_mode}"
        if getattr(args, "merge_restore", False) and args.mode == "synth-precopy":
            mig_cmd += " --merge-restore"
        if getattr(args, "fs_sync", None) and args.mode in ("precopy", "criu-incr"):
            mig_cmd += f" --fs-sync {args.fs_sync}"
    print(f"  [rep {rep}] migrating: {mig_cmd}")
    injector = None
    if getattr(args, "fault", None):
        injector = faults.Injector(sys.modules[__name__], cfg, args, run_dir / "source_migrate.log", args.fault)
        injector.start()
    try:
        with open(run_dir / "source_migrate.log", "w") as f:
            rc = subprocess.run(["ssh", *SSH_OPTS, src, mig_cmd],
                                stdout=f, stderr=subprocess.STDOUT,
                                timeout=int(cfg.get("experiment", {}).get("migrate_timeout", 1800))).returncode
    except BaseException:
        # Timeout or interrupt: never leave the background clients running.
        stop_probe(cfg, probe)
        if load_client is not None:
            stop_load_client(cfg, load_client)
        if lostwrite is not None:
            stop_lostwrite(cfg, lostwrite)
        if conn is not None:
            stop_conn_client(cfg, conn)
        if contention is not None:
            stop_contention(cfg, contention)
        stop_source_sampler(cfg, sampler)
        if injector is not None:
            injector.stop()
        raise
    t_migrate_end = time.time()
    if injector is not None:
        injector.stop()          # undo whatever was injected before anything else talks to the VMs
    # The migrate command has returned: stop sampling (the resource log must not
    # include the probe's 5 s failover tail) and bring back the CPU accounting.
    stop_source_sampler(cfg, sampler)
    try:
        scp_from(src, SRC_TIME_REMOTE, run_dir / "source_migrate_time.txt")
    except Exception as e:
        (run_dir / "source_migrate_time.txt").write_text(f"# could not fetch {SRC_TIME_REMOTE}: {e}\n")

    # 4. Wait for the receiver to finish restore + metrics BEFORE stopping the
    # probe — it must observe the first post-restore "dest OK" ticks or the
    # downtime measurement has no end point. Extra 5 s pads the failover tail.
    recv_timeout = int(cfg.get("experiment", {}).get("migrate_timeout", 1800)) // 4
    if injector is not None and rc != 0:
        # an aborted migration leaves the receiver waiting forever: stop it
        ssh(dst, "sudo pkill -f '[s]tatepod start' || true", check=False)
        recv_timeout = 30
    try:
        receiver.wait(timeout=recv_timeout)
    except subprocess.TimeoutExpired:
        if injector is None:
            raise
        receiver.kill()
    finally:
        time.sleep(5)
        stop_probe(cfg, probe)
        if load_client is not None:
            stop_load_client(cfg, load_client)
        if lostwrite is not None:
            stop_lostwrite(cfg, lostwrite)
        if conn is not None:
            stop_conn_client(cfg, conn)
        if contention is not None:
            stop_contention(cfg, contention)
        fetch_client_logs(run_dir)

    if rc != 0:
        if injector is None:
            raise RuntimeError(f"migrate exited {rc} — see source_migrate.log")
        verdict = fault_abort_verdict(cfg, args, run_dir, canary, rc, injector, t_migrate_end)
        injector.release()
        print(f"  [rep {rep}] fault run: migration aborted -> {verdict['fault']['outcome']} {verdict['fault']}")
        return verdict

    # 5. Harvest metrics from the destination.
    scp_from(dst, metrics_remote, run_dir / "migration_metrics.json")
    print(f"  [rep {rep}] metrics harvested")
    # CRIU's own restore log (timestamps relative to its start): the phase profile
    # of `runc restore` (analysis/criu_restore_profile.py). Newest restore.log under
    # the checkpoint dir -- `final` for every mode that restores from it.
    try:
        # $(echo {dst_wd}) expands a leading ~ in the login shell: inside `sudo sh -c`
        # it would be root's home. The glob is expanded by root's shell (checkpoint
        # dirs created by the receiver are root-owned).
        res = ssh(dst, f"f=$(sudo bash -c \"ls -t $(echo {dst_wd})/checkpoint/*/restore.log 2>/dev/null | head -1\"); "
                       f"[ -n \"$f\" ] && sudo cat \"$f\"", check=False, timeout=60)
        if res.stdout.strip():
            (run_dir / "criu_restore.log").write_text(res.stdout)
    except Exception as e:
        (run_dir / "criu_restore.log").write_text(f"# could not fetch restore.log: {e}\n")

    # 6. Verify migrated state from the source VM's viewpoint.
    verify = verify_state(cfg, args, canary)
    if args.lost_write:
        # Lost-write check: every write the client saw acknowledged
        # must be present at the destination. Writes only ever add keys, so
        # the dbsize equality check in verify_state is relaxed to ">=" (the
        # canary and the seeded dataset are still checked).
        lw = verify_lostwrite(cfg, run_dir)
        verify["lost_writes"] = lw
        verify["lost_write_expected"] = bool(args.runner_expect_loss)
        if lw.get("lost") is None:
            verify["ok"] = False
        elif lw["lost"] > 0 and not args.runner_expect_loss:
            verify["ok"] = False

    # 7. Scan both sides' logs for real failure signal, not just the
    # workload-level check above — a mid-stream CRIU/runc error can be
    # masked by a retry or a later-successful reconnect (research honesty
    # rule: don't call a run clean without looking at what it logged).
    log_scan = {
        "source": scan_log(run_dir / "source_migrate.log"),
        "destination": scan_log(run_dir / "destination_receiver.log"),
    }
    log_scan["clean"] = not (log_scan["source"]["hard_errors"] or log_scan["destination"]["hard_errors"])
    verify["log_scan"] = log_scan
    # Client-observed outage: derived from probe.log by the same
    # code the analysis scripts use, so manifest and analysis cannot disagree.
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "analysis"))
    try:
        import probe_outage
        verify["client_outage"] = probe_outage.outage(probe_outage.load(run_dir / "probe.log"))
    except Exception as e:  # never fail a run over analysis
        verify["client_outage"] = {"outage_s": None, "reason": f"{type(e).__name__}: {e}"}
    # Source-reclaim metrics: eviction time and what the migration cost
    # the source. Derived by the same analysis code the report uses.
    try:
        import reclaim
        import source_cost
        mm = json.loads((run_dir / "migration_metrics.json").read_text())
        verify["source_reclaim"] = reclaim.reclaim(mm)
        verify["source_cost"] = source_cost.summarize(run_dir, mm)
        import source_impact
        verify["source_impact"] = source_impact.impact(run_dir, mm)
        import conn_continuity
        if (run_dir / "conn_client.log").exists():
            verify["conn_continuity"] = conn_continuity.summarize(run_dir / "conn_client.log")
        if upstream is not None:
            # stopped only now: the application's own view (/outbound, read by wl.verify above)
            # must not include the refusals after the service goes away
            stop_upstream(cfg, upstream)
            fetch_client_logs(run_dir)
        if (run_dir / "upstream.log").exists():
            import outbound_continuity
            verify["outbound"] = outbound_continuity.summarize(run_dir / "upstream.log")
            verify["outbound"]["app_view"] = verify.get("outbound_app")
        import criu_restore_profile
        rl = run_dir / "criu_restore.log"
        if rl.exists():
            verify["criu_restore"] = criu_restore_profile.profile(rl.read_text(errors="replace"))
    except Exception as e:
        verify["source_reclaim"] = {"eviction_ms": None, "reason": f"{type(e).__name__}: {e}"}
    verify["ok"] = verify["ok"] and log_scan["clean"]
    if injector is not None:
        # "not_injected": the trigger phase never occurred in this variant (e.g. a drain fault on a
        # mode with no drain) -- reported as not applicable, never as a pass.
        injector.release()
        verify["fault"] = {**injector.summary(), "migrate_rc": rc,
                           "outcome": "completed" if injector.injected_at else "not_injected"}

    print(f"  [rep {rep}] state verify: {'OK' if verify['ok'] else 'FAILED'} {verify}")
    return verify



def build_arg_parser():
    """The runner's CLI (also used by tools that need its exact defaults, e.g. synth_cost.py)."""
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="hosts.yaml")
    ap.add_argument("--name", required=True, help="experiment name for the results dir")
    ap.add_argument("--mode", default=None,
                    help="precopy|sync|burst|... (default from config); "
                         "burst exercises the statefulness dispatcher itself")
    ap.add_argument("--workload", default=None, help="redis|node (default from config)")
    ap.add_argument("--netem", default=None,
                    help='tc netem spec, e.g. "rate 20mbit delay 50ms" (default from config)')
    ap.add_argument("--reps", type=int, default=None)
    ap.add_argument("--precopy-rounds", type=int, default=None,
                    help="Chained --pre-dump rounds before the freeze (0 = single freeze "
                         "dump); the churn-boundary knob. Applies to precopy/criu-incr/"
                         "precopy-rsync/synth-precopy. Default: statepod binary's own "
                         "per-mode default (0 for synth-precopy, 4 for the others) when unset")
    ap.add_argument("--zstd-level", type=int, default=None,
                    help="zstd stage level for the delta stream, synth-precopy only "
                         "(0 disables compression). Nominal 1/3/9 map onto klauspost/compress's "
                         "speed tiers. Default: statepod binary's own default (1) when unset")
    ap.add_argument("--drain-timeout", default=None,
                    help="synth-postcopy only: forwarded to `statepod migrate --drain-timeout` (e.g. 10m). "
                         "The binary's 1m default is shorter than the lazy-set transfer of a JVM (~120 MB) "
                         "on this WAN, so the source aborts a healthy drain. Default: binary's own")
    ap.add_argument("--tmpfs-checkpoint", action="store_true", default=False,
                    help="mount the checkpoint "
                         "dir as tmpfs on both source and destination so CRIU's image I/O "
                         "never touches disk. Default: plain disk (pre-rung-1 baseline)")
    ap.add_argument("--no-fs-upper", dest="no_fs_upper", action="store_true", default=False,
                    help="ablation toggle for M4, "
                         "synth-precopy only: skip the overlay upperdir stream, isolating "
                         "the +M1+M2+compression+M3 cell from +M4")
    ap.add_argument("--deterministic", action="store_true", default=False,
                    help="create bundles with deterministic launch (ASLR off M1); "
                         "forced on for synth-precopy, whose destination templates are "
                         "always synthesized deterministically")
    ap.add_argument("--dataset", choices=sorted(DATASET_SIZES), default=None,
                    help="dataset-size variant, redis workload only (default from "
                         "config, else 'medium' = 5000 keys, the historical seed count). "
                         f"small={DATASET_SIZES['small']}, medium={DATASET_SIZES['medium']}, "
                         f"large={DATASET_SIZES['large']} keys")
    ap.add_argument("--no-volume-sync", dest="no_volume_sync", action="store_true", default=False,
                    help="'restart' baseline: with --mode sync/sync-rsync, skip "
                         "--volume-sync so the destination gets a fresh empty container "
                         "(true restart, no state preserved) instead of a data copy")
    ap.add_argument("--aslr-on", dest="aslr_on", action="store_true", default=False,
                    help="launch-knob check: skip the auto-deterministic-launch "
                         "override normally forced for synth-precopy/synth-postcopy, so "
                         "the source launches with real ASLR while the destination's "
                         "template stays deterministic. Expect the baseline hit rate to "
                         "collapse (this is the measured degradation, not a test failure) "
                         "— see hit-rate experiment (b) vs (c) 2026-07-20")
    ap.add_argument("--load-client", dest="load_client", action="store_true", default=False,
                    help="SLA instrumentation: run load_client.py "
                         "alongside the probe for per-request latency/throughput data "
                         "(load_client.log), covering steady-state through migration "
                         "through the M5 drain when applicable. Off by default -- an "
                         "opt-in on top of the probe, not a replacement for it")
    ap.add_argument("--fault", default=None, metavar="TYPE@PHASE[:PARAM]",
                    help="fault injection (see faults.py): loss|partition|kill-receiver|kill-restore "
                         "at precopy|final|drain. A migration that does not complete passes only if the source "
                         "resumed with every acknowledged write and no second copy runs (Redis + --lost-write)")
    ap.add_argument("--write-rate", dest="write_rate", type=int, default=None,
                    help="writes per second of the lost-write client (default hosts.yaml client.lostwrite_rate, "
                         "else 100): the write-heavy Redis mix of (e.g. 2000)")
    ap.add_argument("--outbound", dest="outbound", action="store_true", default=False,
                    help="external outbound connection (workload pyapp-out): the container holds ONE "
                         "connection to upstream_service.py on the destination host through the migration "
                         "(upstream.log); manifest gets state_verify.outbound (peer address before/after, "
                         "outage, reconnect)")
    ap.add_argument("--conn-client", dest="conn_client", action="store_true", default=False,
                    help="connection continuity (Redis only): a client holds ONE long-lived "
                         "connection with a write in flight through the migration (conn_client.log); "
                         "the manifest gets state_verify.conn_continuity (survived?, error kind, stall, "
                         "fate of the in-flight write)")
    ap.add_argument("--lost-write", dest="lost_write", action="store_true", default=False,
                    help="lost-write test: run lostwrite_client.py (Redis only) through "
                         "the migration, then check every acknowledged write exists at the "
                         "destination. lost > 0 fails the run unless --expect-loss")
    ap.add_argument("--expect-loss", dest="runner_expect_loss", action="store_true", default=False,
                    help="With --lost-write: report lost writes but do not fail the run "
                         "(for the labelled availability-mode ablation and M5 until fixed)")
    ap.add_argument("--final-mode", dest="final_mode", choices=["catchup", "availability"], default=None,
                    help="synth-precopy final phase: catchup (default; pause + final "
                         "dump against the verified checkpoint, no acknowledged write lost) or "
                         "availability (source keeps serving after the dump; writes in "
                         "that window are lost). Passed through to `statepod migrate`")
    ap.add_argument("--merge-restore", dest="merge_restore", action="store_true", default=False,
                    help="(experimental): synth-precopy final delta triggers the restore "
                         "itself (one stream instead of delta + restore-go); passed to `statepod migrate`")
    ap.add_argument("--fs-sync", dest="fs_sync", choices=["flat", "rsync"], default=None,
                    help="filesystem baseline for precopy/criu-incr (without it a workload "
                         "that keeps state in the container filesystem restores against missing files): "
                         "rsync = the filesystem pre-copy every other variant uses ; "
                         "flat = stop-and-copy of the whole writable layer after the final dump (historical). "
                         "Passed through to `statepod migrate --fs-sync`")
    ap.add_argument("--iperf-duration", dest="iperf_duration", type=int, default=None,
                    help="seconds per pre/post iperf3 link probe (default: hosts.yaml "
                         "experiment.iperf_duration, else 5). With a netem rate the probe offers "
                         "exactly that rate (-b), so 3 s is enough")
    ap.add_argument("--meta", action="append", default=[], metavar="KEY=VALUE",
                    help="free-form tag recorded in manifest.json under `meta` (repeatable); "
                         "run_campaign.py records campaign/cell/round/attempt this way")
    ap.add_argument("--wl-param", dest="wl_param", action="append", default=[], metavar="KEY=VALUE",
                    help="workload knob (repeatable), e.g. for --workload synth: resident-mb=128 "
                         "shared-frac=0.5 entropy=random dirty-pps=2000 file-mb=64 files-pps=20")
    ap.add_argument("--migrate-env", dest="migrate_env", action="append", default=[], metavar="KEY=VALUE",
                    help="environment variable for the `statepod migrate` process (A/B switches, e.g. STATEPOD_M5_NO_PRIORITY=1)")
    ap.add_argument("--contend", action="store_true", default=False,
                    help="WAN-contention experiment: run a sustained background "
                         "iperf3 bulk transfer (source->dest) for the rep's duration, "
                         "saturating the shared link, to measure downtime/SLA degradation "
                         "against an uncontended baseline. Off by default")
    ap.add_argument("--ssh-key", default=None,
                    help="path to SSH private key for all hosts (default from config ssh_key)")
    ap.add_argument("--dest-min-avail-mb", dest="dest_min_avail_mb", type=int, default=450,
                    help="refuse to start a run (exit 75) when the destination has less MemAvailable than "
                         "this (MB). On a small destination without swap, a run that overshoots "
                         "OOM-kills statepod and can wedge the VM. Default 450")
    return ap

def main():
    ap = build_arg_parser()
    args = ap.parse_args()

    cfg = load_config(args.config)
    ssh_key = args.ssh_key or cfg.get("ssh_key", "")
    if ssh_key:
        key_path = Path(ssh_key).expanduser()
        if not key_path.is_file():
            sys.exit(f"ERROR: ssh key not found: {key_path}")
        SSH_OPTS.extend(["-i", str(key_path)])
    exp = cfg.get("experiment", {})
    args.mode = args.mode or exp.get("mode", "precopy")
    args.workload = args.workload or exp.get("workload", "redis")
    args.netem = args.netem if args.netem is not None else exp.get("netem", "")
    # experiment.criu_track_shmem: CRIU's anonymous shared-memory dirty tracking
    # (CRIU_TRACK_SHMEM, off by default in CRIU), passed to EVERY migration alike
    # (statepod forwards it past sudo to criu: migration/criuenv.go).
    if str(exp.get("criu_track_shmem", "")).strip() not in ("", "0", "false"):
        if not any(kv.startswith("CRIU_TRACK_SHMEM=") for kv in args.migrate_env):
            args.migrate_env.append("CRIU_TRACK_SHMEM=1")
    if exp.get("tmpfs_size") and not any(kv.startswith("STATEPOD_TMPFS_SIZE=") for kv in args.migrate_env):
        args.migrate_env.append(f"STATEPOD_TMPFS_SIZE={exp['tmpfs_size']}")
    # Templates are always synthesized with deterministic launch, so a
    # synth-precopy/synth-postcopy source must launch the same way or every
    # pointer-bearing page becomes a manifest miss (hit-rate experiments (b)
    # vs (c)) — for synth-postcopy specifically, it also inflates the M5
    # residual/drain set, the address gate needs baseline and
    # source pages to actually line up.
    args.deterministic = args.deterministic or (
        args.mode in ("synth-precopy", "synth-postcopy") and not args.aslr_on)
    args.dataset = args.dataset or exp.get("dataset", "medium")
    if args.dataset == "xlarge" and not isinstance(workloads.get(args.workload), workloads.Redis):
        sys.exit("ERROR: --dataset xlarge (500,000 keys) exists for the Redis workloads only")
    if args.write_rate is not None:
        cfg.setdefault("client", {})["lostwrite_rate"] = str(args.write_rate)
    reps = args.reps or int(exp.get("repetitions", 1))
    results_root = Path(exp.get("results_root", "../results")).expanduser()

    if tunnel_enabled(cfg) and args.mode in ("precopy-rsync", "sync-rsync"):
        # the rsync-daemon transport A/B baseline (addendum)
        # needs port 8730, which this tunnel doesn't carry -- refuse rather
        # than silently running it over the wrong transport (e.g. plain
        # rsync -e ssh), which would measure something else entirely.
        sys.exit(f"ERROR: --mode {args.mode} needs the rsync-daemon transport "
                 f"(port {exp.get('rsync_port', '8730')}), which is not tunneled "
                 "on this source profile (only the migrate port is). "
                 "Use precopy/sync/criu-incr/synth-precopy/burst instead.")

    stamp = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    base_dir = results_root / f"{stamp}_{args.name}"

    manifest = collect_manifest(cfg, args)
    manifest["netem_applied"] = apply_netem(cfg, args.netem)
    start_migrate_tunnel(cfg)
    try:
        for rep in range(1, reps + 1):
            run_dir = base_dir / f"rep{rep}" if reps > 1 else base_dir
            run_dir.mkdir(parents=True, exist_ok=True)
            manifest["repetition"] = rep
            print(f"  [rep {rep}] iperf3 pre-run probe...")
            cap = parse_netem_rate_mbps(args.netem)
            manifest["bandwidth_cap_mbps"] = cap
            probe_s = args.iperf_duration
            manifest["link_probe"] = {"pre": iperf_probe(cfg, "pre", cap, probe_s), "post": None}
            (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
            manifest["dest_mem"] = {"before": dest_mem_state(cfg)}
            try:
                manifest["state_verify"] = run_once(cfg, args, run_dir, rep)
            finally:
                manifest["dest_mem"]["after_cleanup"] = dict(LAST_GUARD)
                try:
                    after = dest_mem_state(cfg)
                    manifest["dest_mem"]["after"] = after
                    if after["oom_kills"] > manifest["dest_mem"]["before"]["oom_kills"]:
                        manifest["oom_on_destination"] = True
                        if isinstance(manifest.get("state_verify"), dict):
                            manifest["state_verify"]["ok"] = False
                        print("  [guard] the destination kernel OOM-killed something during this run", flush=True)
                except MemoryGuardError as e:
                    manifest["dest_mem"]["after"] = {"error": str(e)}
                # Post probe runs even on failure — a degraded link is often
                # the explanation, and the datapoint needs it either way.
                print(f"  [rep {rep}] iperf3 post-run probe...")
                manifest["link_probe"]["post"] = iperf_probe(cfg, "post", cap, probe_s)
                (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    finally:
        clear_netem(cfg)
        stop_migrate_tunnel(cfg)

    print(f"\nDone. Results in {base_dir}")


if __name__ == "__main__":
    try:
        main()
    except MemoryGuardError as e:
        print(f"MEMORY GUARD: {e}", file=sys.stderr)
        sys.exit(75)
    except (subprocess.CalledProcessError, RuntimeError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
