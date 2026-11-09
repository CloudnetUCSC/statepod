#!/usr/bin/env python3
"""multi-container scenario: N (default 3) independent Redis instances
of the same image, migrated to the same destination host, sharing ONE
destination-synthesized baseline template (content-addressed,
per-image-digest template store already makes sharing free -- this script's
job is to prove it empirically, not to build new sharing logic).

Each instance gets its own bundle dir, its own container IP, its own
host-port, and its own *dataset size* (small/medium/large) as a bonus cross-
check that baseline sharing holds regardless of what's layered on top of it.

Migration transfer runs TRULY CONCURRENTLY -- each instance gets
its own receiver listen port (8081..8080+N, inside the 8080-8090 migrate
range opened dest-side for exactly this) and its own host port (6391..,
already inside 6380-6395), so N `statepod migrate` runs launch in parallel
and race on the wire at the same time, not one after another. This was
previously sequential because
the firewall only had a single migrate port (8080) open, already claimed by
the single-container runner -- fixed once the wider port ranges landed
(2026-08-16).
The multi-container motivation is baseline REUSE across instances,
not transport concurrency -- but now that concurrency is cheap to get for
free, it's also a genuine (if informal) confirmation that N simultaneous
transports and N simultaneous CRIU dump/restore pairs don't step on each
other. All N destination containers coexist and serve correct, independent
data in one verification pass after migrations land -- checked over SSH
loopback on the destination VM itself (127.0.0.1), sidestepping the
firewall entirely for verification traffic.

Usage:
    ./run_multicontainer.py --config hosts.yaml
    ./run_multicontainer.py --config hosts.yaml --n 2 --mode synth-precopy
"""

import argparse
import datetime
import json
import subprocess
import sys
import time
from pathlib import Path

from run_experiment import (
    DATASET_SIZES, SSH_OPTS, apply_netem, clear_netem, dest_public_ip, ensure_template, iperf_probe,
    PROBE_REMOTE, load_config, parse_netem_rate_mbps, scan_log, scp_from, scp_to, ssh, ssh_bg, try_out,
    wait_redis,
)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "analysis"))
import probe_outage  # noqa: E402

SYNTH_MODES = ("synth-precopy", "synth-postcopy")
# variant flags passed through to every instance's `statepod migrate` (same names as run_experiment.py)
PASS_FLAGS = ("zstd-level", "precopy-rounds", "drain-timeout", "final-mode")
PASS_BOOLS = ("tmpfs-checkpoint", "merge-restore")

INSTANCE_DATASETS = ["small", "medium", "large"]  # cycled if --n > 3


def prep_host(target):
    """Same CRIU runc.conf options setup_containers applies (skip-in-flight,
    tcp-close) -- host-wide, only needs doing once regardless of N instances."""
    for opt in ("skip-in-flight", "tcp-close"):
        ssh(target, f"sudo mkdir -p /etc/criu && "
                    f"grep -qxF {opt} /etc/criu/runc.conf 2>/dev/null || "
                    f"echo {opt} | sudo tee -a /etc/criu/runc.conf >/dev/null",
            check=False, timeout=30)


def clean_bundle(target, wd):
    """Scoped to this bundle's OWN container only. setup_containers' single-
    instance version globally sweeps `runc list` + `pkill -f statepod`,
    which is safe there (only ever one bundle) but would kill sibling
    instances' already-running containers/processes here -- exactly the bug
    that broke the first multi-instance run (instance 1's container got
    wiped out while instance 2's bundle was being prepped)."""
    ssh(target, f"mkdir -p {wd} && cd {wd} && "
                f"if [ -f statepod.json ]; then "
                f"CID=$(sed -n 's/.*\"container_id\"[[:space:]]*:[[:space:]]*\"\\([a-f0-9]*\\)\".*/\\1/p' statepod.json | head -1); "
                f"[ -n \"$CID\" ] && sudo runc delete -f \"$CID\" 2>/dev/null; fi; "
                f"(mountpoint -q rootfs && sudo umount rootfs || true); "
                f"sudo rm -rf rootfs checkpoint data lower upper work "
                f"statepod.json config.json README.md rootfs.tar start.log",
        check=False, timeout=60)


def build_instances(cfg, n, dataset=None):
    src_base = cfg["source"]["workdir"]
    dst_base = cfg["destination"]["workdir"]
    base_migrate_port = int(cfg["source"].get("migrate_listen_port", "8080"))
    instances = []
    for i in range(1, n + 1):
        instances.append({
            "idx": i,
            "ip": f"10.10.0.{30 + i}",
            "host_port": 6390 + i,
            # own migrate port per instance (true concurrency) --
            # +i so instance 1 doesn't collide with the shared single-
            # container default of base_migrate_port itself.
            "migrate_port": base_migrate_port + i,
            # own rsync daemon per instance: the filesystem pre-copy rounds (fsprecopy.go)
            "rsync_port": int(cfg.get("experiment", {}).get("rsync_port", "8730")) + i,
            # --dataset: every instance the same (comparable N=1/2/4/8); else cycled
            "dataset": dataset or INSTANCE_DATASETS[(i - 1) % len(INSTANCE_DATASETS)],
            "src_wd": f"{src_base}-{i}",
            "dst_wd": f"{dst_base}-{i}",
        })
    return instances


def setup_instance(cfg, image, inst, mode="synth-precopy"):
    src, dst = cfg["source"]["ssh"], cfg["destination"]["ssh"]
    src_pod, dst_pod = cfg["source"]["statepod"], cfg["destination"]["statepod"]
    for target, wd, pod in ((src, inst["src_wd"], src_pod), (dst, inst["dst_wd"], dst_pod)):
        clean_bundle(target, wd)
        print(f"  [{inst['idx']}] creating bundle on {target} at {wd} "
              f"(ip={inst['ip']} host-port={inst['host_port']})...")
        det = " --deterministic" if mode in SYNTH_MODES else ""  # baselines launch with ASLR, as everywhere
        ssh(target, f"cd {wd} && sudo {pod} create '{image}' --redis{det} "
                    f"--ip {inst['ip']} --host-port {inst['host_port']}", timeout=600)
        ssh(target, f"sudo chmod -R a+rwX {wd}/rootfs/data", check=False, timeout=30)

    ssh(src, f'cd {inst["src_wd"]} && POD=$(readlink -f {src_pod}) && '
             f'sudo bash -c "nohup $POD start >> start.log 2>&1 &"', timeout=30)
    if not wait_redis(src, f"-h 127.0.0.1 -p {inst['host_port']}"):
        log = try_out(src, f"tail -20 {inst['src_wd']}/start.log")
        raise RuntimeError(f"[{inst['idx']}] source Redis not answering; start.log:\n{log}")

    canary = f"instance-{inst['idx']}-{datetime.datetime.now().isoformat()}"
    key_count = DATASET_SIZES[inst["dataset"]]
    seed = f"for i=1,{key_count} do redis.call('set','key:'..i,i) end"
    out = ssh(src, f"redis-cli -h 127.0.0.1 -p {inst['host_port']} set migration_canary '{canary}' && "
                   f"redis-cli -h 127.0.0.1 -p {inst['host_port']} eval \"{seed}\" 0 && "
                   f"redis-cli -h 127.0.0.1 -p {inst['host_port']} dbsize", timeout=60)
    dbsize = out.stdout.strip().splitlines()[-1]
    expected = str(key_count + 1)
    if dbsize != expected:
        raise RuntimeError(f"[{inst['idx']}] seeding failed: dbsize={dbsize!r}, expected {expected}")
    inst["canary"] = canary
    inst["expected_dbsize"] = expected
    print(f"  [{inst['idx']}] source ready: {key_count} keys [{inst['dataset']}], canary set")


def start_receiver(cfg, inst, run_dir):
    """Launch just this instance's receiver (own port) and return the Popen
    handle. Called for every instance BEFORE any migrate fires -- receivers
    binding is cheap/fast, so doing all N up front (then one shared grace
    sleep) is simpler than interleaving per-instance sleeps, and doesn't
    affect the concurrency claim (only the migrate calls need to overlap)."""
    dst = cfg["destination"]["ssh"]
    dst_pod = cfg["destination"]["statepod"]
    src = cfg["source"]["ssh"]
    port = inst["migrate_port"]
    inst_dir = run_dir / f"instance-{inst['idx']}"
    inst_dir.mkdir(parents=True, exist_ok=True)
    inst["_inst_dir"] = inst_dir
    inst["_dst_ssh"] = dst
    metrics_remote = f"{inst['dst_wd']}/migration_metrics.json"
    tsz = cfg.get("experiment", {}).get("tmpfs_size", "")
    tenv = f"env STATEPOD_TMPFS_SIZE={tsz} " if tsz else ""   # as run_experiment.py
    recv_cmd = (f"cd {inst['dst_wd']} && sudo {tenv}{dst_pod} start --source {src} "
                f"--listen 0.0.0.0:{port} --metrics-out {metrics_remote} --rsync-port {inst['rsync_port']}")
    inst["_receiver"] = ssh_bg(dst, recv_cmd, inst_dir / "destination_receiver.log")
    return inst["_receiver"]


def start_migrate(cfg, args, inst):
    """Launch this instance's `statepod migrate` as a non-blocking Popen
    (not subprocess.run) -- called for every instance back-to-back so all N
    transfers are actually in flight on the wire at the same time, not one
    started only after the previous one finished."""
    src = cfg["source"]["ssh"]
    src_pod = cfg["source"]["statepod"]
    dest_ip = dest_public_ip(cfg)
    port = inst["migrate_port"]
    if inst["_receiver"].poll() is not None:
        raise RuntimeError(f"[{inst['idx']}] receiver exited early")
    menv = "".join(f"{kv} " for kv in args.migrate_env)
    menv = f"env {menv}" if menv else ""
    mig_cmd = (f"cd {inst['src_wd']} && sudo {menv}{src_pod} migrate --mode {args.mode} --dest {dest_ip}:{port} "
               f"--rsync-port {inst['rsync_port']}")
    for f in PASS_FLAGS:
        v = getattr(args, f.replace("-", "_"))
        if v is not None:
            mig_cmd += f" --{f} {v}"
    for f in PASS_BOOLS:
        if getattr(args, f.replace("-", "_")):
            mig_cmd += f" --{f}"
    print(f"  [{inst['idx']}] migrating (port {port}): {mig_cmd}")
    inst["_migrate_log"] = open(inst["_inst_dir"] / "source_migrate.log", "w")
    inst["_migrate_proc"] = subprocess.Popen(
        ["ssh", *SSH_OPTS, src, mig_cmd],
        stdout=inst["_migrate_log"], stderr=subprocess.STDOUT)


def finish_migration(inst):
    """Block until this instance's migrate + receiver finish and score the
    result. Called for every instance after every start_migrate has already
    fired, so the blocking waits here just join transfers that have been
    running concurrently since start_migrate -- not the point where
    concurrency happens, just where we collect it.

    Records inst["error"] instead of raising: with N transfers in flight at
    once, instance 1 failing must not skip waiting on 2..N and leave their
    processes orphaned -- the for-loop over every instance always runs to
    completion; main() checks for any recorded error only after all N have
    been joined."""
    inst_dir = inst["_inst_dir"]
    inst["error"] = None
    rc = inst["_migrate_proc"].wait(timeout=3600)
    inst["_migrate_log"].close()
    try:
        inst["_receiver"].wait(timeout=120)
    except Exception:
        pass
    if rc != 0:
        inst["error"] = f"[{inst['idx']}] migrate exited {rc} -- see {inst_dir}/source_migrate.log"
        inst["log_scan"] = {"clean": False}
        inst["baseline_line"] = None
        inst["migration_time_ms"] = None
        print(f"  {inst['error']}")
        return

    log_scan = {
        "source": scan_log(inst_dir / "source_migrate.log"),
        "destination": scan_log(inst_dir / "destination_receiver.log"),
    }
    log_scan["clean"] = not (log_scan["source"]["hard_errors"] or log_scan["destination"]["hard_errors"])
    inst["log_scan"] = log_scan
    text = (inst_dir / "destination_receiver.log").read_text(errors="replace")
    inst["baseline_line"] = next((l for l in text.splitlines() if "Loading local baseline from" in l), None)

    inst["migration_time_ms"] = None
    try:
        scp_from(inst["_dst_ssh"], f"{inst['dst_wd']}/migration_metrics.json", inst_dir / "migration_metrics.json")
        inst["migration_time_ms"] = json.loads((inst_dir / "migration_metrics.json").read_text()).get("total_migration_time_ms")
    except Exception as e:
        print(f"  [{inst['idx']}] warning: couldn't fetch migration_metrics.json: {e}")

    print(f"  [{inst['idx']}] migrated, log_scan clean={log_scan['clean']}, "
          f"total_migration_time_ms={inst['migration_time_ms']}, baseline={inst['baseline_line']}")


def start_probes(cfg, instances):
    """One client probe per instance on the source VM, as run_experiment.start_probe: the
    instance's own host port on the source, failing over to the same port on the destination."""
    client = cfg.get("client", {})
    interval = client.get("probe_interval", "0.05")
    conn_timeout = client.get("probe_timeout", "1.0")
    src, dest_ip = cfg["source"]["ssh"], dest_public_ip(cfg)
    ssh(src, f"pkill -f {PROBE_REMOTE} || true", check=False)   # leftovers of an interrupted run
    scp_to(src, Path(__file__).resolve().parent / "probe.py", PROBE_REMOTE)
    for inst in instances:
        p = inst["host_port"]
        remote = f"/tmp/statepod_probe_{inst['idx']}.log"
        inst["_probe_remote"] = remote
        inst["_probe"] = subprocess.Popen(
            ["ssh", *SSH_OPTS, src, f"rm -f {remote}; python3 {PROBE_REMOTE} redis 127.0.0.1:{p} "
                                    f"{dest_ip}:{p} {interval} {conn_timeout} > {remote} 2>&1"],
            stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)


def stop_probes(cfg, instances):
    """Stop every probe, fetch its log into the instance dir, score its client outage
    (probe_outage.py, the same scoring as single-container runs)."""
    src = cfg["source"]["ssh"]
    ssh(src, f"pkill -f {PROBE_REMOTE} || true", check=False)
    for inst in instances:
        if "_probe" not in inst:
            continue
        inst["_probe"].terminate()
        log = inst["_inst_dir"] / "probe.log"
        try:
            scp_from(src, inst["_probe_remote"], log)
            inst["client_outage"] = probe_outage.outage(probe_outage.load(log))
        except Exception as e:
            inst["client_outage"] = {"outage_s": None, "reason": f"{type(e).__name__}: {e}"}


def group_client_outage(instances):
    """Client-observed group numbers. All probes run on the source VM (one clock): the group
    window is the first instance's last source answer -> the last instance's first destination
    answer, i.e. while at least one service of the group was unreachable."""
    outs = [i.get("client_outage") or {} for i in instances]
    ok = [o for o in outs if o.get("outage_s") is not None]
    if not ok or len(ok) < len(outs):
        return {"outage_s": None, "instances_scored": len(ok)}
    return {"outage_s": round(max(o["first_dest_ok"] for o in ok) - min(o["last_source_ok"] for o in ok), 3),
            "max_instance_outage_s": round(max(o["outage_s"] for o in ok), 3),
            "mean_instance_outage_s": round(sum(o["outage_s"] for o in ok) / len(ok), 3),
            "instances_scored": len(ok)}


def group_metrics(instances):
    """Per-instance and group numbers from each instance's migration_metrics.json. All times are
    the SOURCE clock (freeze_start_time and freeze + downtime_source_side_ms = when that instance's
    destination was serving, as the engine measures it), so they are comparable across instances.
    Group downtime = first instance frozen -> last instance serving: the window in which at least
    one service of the group is down."""
    import datetime as dt
    rows = []
    for inst in instances:
        f = inst["_inst_dir"] / "migration_metrics.json"
        if not f.exists():
            continue
        m = json.loads(f.read_text())
        fz = m.get("freeze_start_time") or ""
        if not fz or fz.startswith("0001"):
            continue
        t0 = dt.datetime.fromisoformat(fz.replace("Z", "+00:00")[:26] + "+00:00").timestamp()
        d = m.get("downtime_source_side_ms") or 0
        rows.append({"idx": inst["idx"], "freeze_s": t0, "serve_s": t0 + d / 1000, "downtime_ms": d,
                     "bytes": m.get("total_data_transferred_bytes"), "total_ms": m.get("total_migration_time_ms")})
    if not rows:
        return {}
    first, last = min(r["freeze_s"] for r in rows), max(r["serve_s"] for r in rows)
    return {"instances": rows, "group_downtime_ms": round((last - first) * 1000),
            "max_instance_downtime_ms": max(r["downtime_ms"] for r in rows),
            "mean_instance_downtime_ms": round(sum(r["downtime_ms"] for r in rows) / len(rows)),
            "total_bytes": sum(r["bytes"] or 0 for r in rows)}


def verify_all_coexist(cfg, instances):
    """Query every destination container over SSH loopback, in one pass,
    after all migrations have landed -- proves they coexist and are each
    independently correct, sidestepping the firewall entirely."""
    dst = cfg["destination"]["ssh"]
    for inst in instances:
        p = inst["host_port"]
        canary = try_out(dst, f"redis-cli -h 127.0.0.1 -p {p} get migration_canary")
        dbsize = try_out(dst, f"redis-cli -h 127.0.0.1 -p {p} dbsize")
        inst["verify"] = {
            "canary_sent": inst["canary"], "canary_received": canary,
            "dbsize": dbsize, "expected_dbsize": inst["expected_dbsize"],
            "ok": canary == inst["canary"] and dbsize == inst["expected_dbsize"],
        }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="hosts.yaml")
    ap.add_argument("--n", type=int, default=3)
    ap.add_argument("--mode", default="synth-precopy",
                    help="migration mode -- synth-precopy (M1) is the point: "
                         "demonstrates baseline reuse across instances")
    ap.add_argument("--ssh-key", default=None)
    # campaign interface (run_matrix.cell_cmd with flags["multi"] = N)
    ap.add_argument("--name", default=None, help="results dir suffix (default multicontainer_n<N>)")
    ap.add_argument("--netem", default="", help="emulated WAN for the whole group: the instances share one link")
    ap.add_argument("--dataset", default=None, help="same dataset for every instance (default: cycle small/medium/large)")
    ap.add_argument("--iperf-duration", type=int, default=3)
    ap.add_argument("--meta", action="append", default=[], metavar="KEY=VALUE")
    for f in PASS_FLAGS:
        ap.add_argument(f"--{f}", default=None)
    for f in PASS_BOOLS:
        ap.add_argument(f"--{f}", action="store_true")
    args, ignored = ap.parse_known_args()   # runner-only flags (--reps, --workload, ...) are recorded, not used

    cfg = load_config(args.config)
    ssh_key = args.ssh_key or cfg.get("ssh_key", "")
    if ssh_key:
        key_path = Path(ssh_key).expanduser()
        if not key_path.is_file():
            sys.exit(f"ERROR: ssh key not found: {key_path}")
        SSH_OPTS.extend(["-i", str(key_path)])

    exp = cfg.get("experiment", {})
    args.migrate_env = []
    if str(exp.get("criu_track_shmem", "")).strip() not in ("", "0", "false"):
        args.migrate_env.append("CRIU_TRACK_SHMEM=1")   # same as run_experiment.py: every variant alike
    if exp.get("tmpfs_size"):
        args.migrate_env.append(f"STATEPOD_TMPFS_SIZE={exp['tmpfs_size']}")
    image = exp.get("image", "redis:7.4-alpine")
    results_root = Path(exp.get("results_root", "../results")).expanduser()
    stamp = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    run_dir = results_root / f"{stamp}_{args.name or f'multicontainer_n{args.n}'}"
    run_dir.mkdir(parents=True, exist_ok=True)

    instances = build_instances(cfg, args.n, args.dataset)
    print(f"== prepping hosts ==")
    prep_host(cfg["source"]["ssh"])
    prep_host(cfg["destination"]["ssh"])
    # Each instance now owns its own migrate port, but a stale
    # receiver from a previous crashed run can still be left holding any of
    # them -- sweep all N up front rather than just the single shared port
    # the old sequential design only needed to clear.
    for inst in instances:
        ssh(cfg["destination"]["ssh"], f"sudo fuser -k {inst['migrate_port']}/tcp 2>/dev/null; true",
            check=False, timeout=15)

    print(f"== setting up {args.n} independent source+destination bundles ==")
    for inst in instances:
        setup_instance(cfg, image, inst, args.mode)

    print(f"== ensuring shared destination template (once, for the whole group) ==")
    shared_cfg = json.loads(json.dumps(cfg))  # cheap deep copy
    shared_cfg["destination"]["workdir"] = instances[0]["dst_wd"]
    if args.mode in SYNTH_MODES:
        ns = argparse.Namespace(workload="redis", dataset=args.dataset or "medium", wl_param=[])
        ensure_template(shared_cfg, ns)

    cap = parse_netem_rate_mbps(args.netem)
    netem_applied = apply_netem(cfg, args.netem) if args.netem else None
    link_probe = {"pre": iperf_probe(cfg, "pre", cap, args.iperf_duration), "post": None}

    print(f"== migrating {args.n} instances concurrently (own port each) ==")
    for inst in instances:
        start_receiver(cfg, inst, run_dir)
    start_probes(cfg, instances)
    time.sleep(3)  # one shared grace period for all N receivers to bind (and the probes to start)
    migrate_start = time.time()
    for inst in instances:
        start_migrate(cfg, args, inst)
    for inst in instances:
        finish_migration(inst)
    stop_probes(cfg, instances)
    concurrent_wall_s = round(time.time() - migrate_start, 2)
    link_probe["post"] = iperf_probe(cfg, "post", cap, args.iperf_duration)
    if args.netem:
        clear_netem(cfg)
    print(f"  all {args.n} migrations finished, wall time {concurrent_wall_s}s "
          f"(vs sum of individual migration_metrics.json total_migration_time_ms below)")

    print(f"== verifying all {args.n} destination instances coexist, in one pass ==")
    verify_all_coexist(cfg, instances)

    baselines = {inst["baseline_line"] for inst in instances}
    shared_ok = len(baselines) == 1 and None not in baselines
    all_ok = all(inst["verify"]["ok"] for inst in instances) and all(
        inst["log_scan"]["clean"] for inst in instances)

    individual_times = [i["migration_time_ms"] for i in instances if i["migration_time_ms"] is not None]
    group = group_metrics(instances)
    summary = {
        "timestamp": stamp, "n": args.n, "mode": args.mode, "runner": "multicontainer",
        "netem": args.netem or "unshaped", "netem_applied": netem_applied, "bandwidth_cap_mbps": cap,
        "link_probe": link_probe, "migrate_env": args.migrate_env, "ignored_args": ignored,
        "meta": dict(kv.split("=", 1) for kv in args.meta if "=" in kv),
        "dataset": args.dataset or "cycled",
        "variant_flags": {f: getattr(args, f.replace("-", "_")) for f in PASS_FLAGS + PASS_BOOLS},
        # the campaign's validity gate reads state_verify.ok (every instance verified, logs clean)
        "state_verify": {"ok": None, "instances_ok": None},
        "group": group,
        "client_outage": group_client_outage(instances),
        "shared_baseline_confirmed": shared_ok,
        "baseline_lines": sorted(b for b in baselines if b is not None) + ([None] if None in baselines else []),
        "all_ok": all_ok,
        "concurrency": {
            "wall_time_s": concurrent_wall_s,
            "sum_individual_migration_time_s": round(sum(individual_times) / 1000, 2) if individual_times else None,
            # wall time well under the sum is the actual evidence the N
            # transfers overlapped rather than ran one after another.
        },
        "instances": [{
            "idx": i["idx"], "dataset": i["dataset"], "host_port": i["host_port"],
            "migrate_port": i["migrate_port"], "migration_time_ms": i["migration_time_ms"],
            "verify": i["verify"], "log_scan_clean": i["log_scan"]["clean"], "error": i.get("error"),
            "client_outage": i.get("client_outage"),
        } for i in instances],
    }
    summary["state_verify"] = {"ok": all_ok, "instances_ok": sum(1 for i in instances if i.get("verify", {}).get("ok"))}
    (run_dir / "manifest.json").write_text(json.dumps(summary, indent=2))
    # a run-level migration_metrics.json with the group numbers, where the analysis expects one
    (run_dir / "migration_metrics.json").write_text(json.dumps({
        "mode": args.mode, "multicontainer": True, "n": args.n,
        "downtime_source_side_ms": group.get("group_downtime_ms"),
        "total_migration_time_ms": round(concurrent_wall_s * 1000),
        "total_data_transferred_bytes": group.get("total_bytes"),
        "group": group}, indent=2))

    for inst in instances:
        print(f"  [{inst['idx']}] dataset={inst['dataset']:6s} verify.ok={inst['verify']['ok']} "
              f"log_clean={inst['log_scan']['clean']}")
    print(f"\nShared baseline confirmed (all {args.n} receivers loaded the same "
          f"template path): {shared_ok}")
    print(f"Concurrency: wall time {summary['concurrency']['wall_time_s']}s vs "
          f"sum of individual migration times {summary['concurrency']['sum_individual_migration_time_s']}s")
    print(f"All instances OK: {all_ok}")
    print(f"Results: {run_dir}")
    if not all_ok or not shared_ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
