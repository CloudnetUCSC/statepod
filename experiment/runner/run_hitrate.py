#!/usr/bin/env python3
"""Hit-rate experiments (a)/(b)/(c): page overlap vs the readiness-point dump
.

--mode same (experiment (a), default): one instance per rep; dump at the
readiness point (`d0`, the same lifecycle anchor template synthesis uses),
then after idle intervals, after loading, and after post-load idle — each
later dump pagecmp'd against d0. The *per-instance upper bound*: how much
readiness-point memory survives in the same process over time. ASLR state
is irrelevant (layout fixed within one process).

--mode cross (experiments (b)/(c)): two *independent* instances per rep, run
sequentially. Instance A is dumped at readiness and discarded — it stands in
for the destination's synthesized template. Instance B is dumped at readiness
and after loading; both are pagecmp'd against A's dump. This is the actual
M1 question: how much of a different instance's memory does the baseline
cover? --deterministic launches both instances ASLR-off via the nrand
wrapper — (c) quantifies the pointer-value effect, i.e. the knob's worth.

    <results_root>/<stamp>_<name>/<workload>/rep<N>/
        manifest.json      environment + timeline + dump sizes + comparisons
        cmp_*.json         raw pagecmp outputs

Instances are unreachable from outside the VM (own IP, host_port=-1 — no
DNAT touched); seeding/traffic go straight to the container IP.

Usage:
    ./run_hitrate.py --config hosts.yaml --name hitrate_a
    ./run_hitrate.py --config hosts.yaml --name hitrate_b --mode cross
    ./run_hitrate.py --config hosts.yaml --name hitrate_c --mode cross --deterministic

Stdlib only, same conventions as run_experiment.py.
"""

import argparse
import datetime
import json
import subprocess
import sys
import time
from pathlib import Path

from run_experiment import SSH_OPTS, load_config, scp_to, ssh, try_out, wait_http, wait_redis
import workloads as wlreg

CONTAINER_IP = "10.10.0.30"  # never the serving 10.10.0.10 (template lesson)
IDLE1_S = 10
IDLE2_S = 60
POST_LOAD_IDLE_S = 60
SEED_KEYS = 5000
NODE_REQUESTS = 100


def sh(target, cmd, timeout=120):
    """ssh + raise with stderr attached (CalledProcessError hides it)."""
    out = ssh(target, cmd, check=False, timeout=timeout)
    if out.returncode != 0:
        raise RuntimeError(f"remote command failed ({out.returncode}): {cmd}\n"
                           f"stdout: {out.stdout.strip()}\nstderr: {out.stderr.strip()}")
    return out.stdout


def dump(target, cid, workdir, label, t_ready):
    """runc checkpoint --leave-running into <workdir>/<label>; returns stats."""
    path = f"{workdir}/{label}"
    t0 = time.monotonic()
    sh(target, f"sudo runc checkpoint {cid} --leave-running --image-path {path}")
    size = int(sh(target, f"sudo du -sb {path} | cut -f1").split()[0])
    return {
        "label": label,
        "elapsed_since_ready_s": round(t0 - t_ready, 2),
        "dump_bytes": size,
        "path": path,
    }


def pagecmp(target, pod, dump_a, dump_b):
    return json.loads(sh(target, f"sudo {pod} pagecmp {dump_a} {dump_b}", timeout=300))


def load_redis(target, ip):
    seed = f"for i=1,{SEED_KEYS} do redis.call('set','key:'..i,i) end"
    canary = datetime.datetime.now().isoformat()
    out = sh(target,
             f"redis-cli -h {ip} -p 6379 set migration_canary '{canary}' && "
             f"redis-cli -h {ip} -p 6379 eval \"{seed}\" 0 && "
             f"redis-cli -h {ip} -p 6379 dbsize")
    dbsize = out.strip().splitlines()[-1]
    if dbsize != str(SEED_KEYS + 1):
        raise RuntimeError(f"seeding failed: dbsize={dbsize!r}")
    return {"kind": "redis_seed", "keys": SEED_KEYS + 1}


def load_node(target, ip):
    sh(target, f"for i in $(seq {NODE_REQUESTS}); do "
               f"curl -s -o /dev/null --max-time 3 http://{ip}:3000/; done",
       timeout=NODE_REQUESTS * 3 + 60)
    return {"kind": "node_http_requests", "requests": NODE_REQUESTS}


# workloads beyond redis/node. Each is (image, create flags, loader).
# Readiness is taken from `statepod start`'s own "Workload ready" log line (the
# Spec's readiness check), so no per-workload probe is needed here.
SPEC_REMOTE = "/tmp/hitrate_spec.json"
PG_LOAD = ("pgbench -U postgres -h 127.0.0.1 -i -s 5 postgres && "
           "pgbench -U postgres -h 127.0.0.1 -c 4 -T 5 -N postgres")


def load_exec(target, ip, cid, cmd):
    """Run a state-building command inside the container (runc exec)."""
    sh(target, f"sudo runc exec {cid} sh -c '{cmd}'", timeout=600)
    return {"kind": "exec", "cmd": cmd}


def wl_setup(cfg, args, workload, target):
    """-> (image, create_flags, loader(target, ip, cid))."""
    if workload == "redis":
        return (cfg["experiment"]["image"], "--redis",
                lambda t, ip, cid: load_redis(t, ip))
    if workload == "node":
        return (cfg["experiment"]["node_image"], "--with-node-server",
                lambda t, ip, cid: load_node(t, ip))
    if workload == "postgres":
        spec = Path(__file__).resolve().parent.parent.parent / "workloads" / "postgres.json"
        scp_to(target, spec, SPEC_REMOTE)
        return ("postgres:16", f"--workload {SPEC_REMOTE}",
                lambda t, ip, cid: load_exec(t, ip, cid, PG_LOAD))
    if workload == "synth":
        sw = wlreg.get("synth")
        import tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(sw.spec(args), f)
        scp_to(target, f.name, SPEC_REMOTE)
        # "load" = let the dirtier run for --synth-settle seconds
        return (cfg["experiment"].get("synth_image", "statepod-synth:1"), f"--workload {SPEC_REMOTE}",
                lambda t, ip, cid: (time.sleep(args.synth_settle) or {"kind": "synth_settle", "s": args.synth_settle}))
    raise SystemExit(f"unknown workload {workload!r}")


def wait_started(target, workdir, timeout=180):
    for _ in range(timeout):
        if "Workload ready" in try_out(target, f"cat {workdir}/start.log 2>/dev/null"):
            return True
        time.sleep(1)
    return False


def spawn_instance(target, pod, workdir, image, workload, deterministic, flags=None):
    """create+start a fresh instance at CONTAINER_IP; returns (cid, t_ready)."""
    # Fresh bundle; the workdir never collides with the migration bundles.
    # A leftover statepod.json names the stale container from a crashed run.
    stale = try_out(target, f"sudo cat {workdir}/statepod.json 2>/dev/null")
    if stale.startswith("{"):
        stale_cid = json.loads(stale).get("container_id", "")
        if stale_cid:
            sh(target, f"sudo runc delete -f {stale_cid} 2>/dev/null || true")
    rm_workdirs(target, workdir)
    sh(target, f"mkdir -p {workdir}")
    flags = flags or ("--redis" if workload == "redis" else "--with-node-server")
    if deterministic:
        flags += " --deterministic"
    sh(target, f"cd {workdir} && sudo $(readlink -f {pod}) create '{image}' {flags} "
               f"--ip {CONTAINER_IP} --host-port=-1", timeout=600)
    cid = json.loads(sh(target, f"sudo cat {workdir}/statepod.json"))["container_id"]

    sh(target, f'cd {workdir} && POD=$(readlink -f {pod}) && '
               f'sudo bash -c "nohup $POD start >> start.log 2>&1 &"', timeout=30)
    if workload in ("redis", "node"):
        ready = (wait_redis(target, f"-h {CONTAINER_IP} -p 6379") if workload == "redis"
                 else wait_http(target, CONTAINER_IP, 3000))
    else:
        ready = wait_started(target, workdir)
    if not ready:
        log = try_out(target, f"tail -20 {workdir}/start.log")
        raise RuntimeError(f"{workload} not ready at {CONTAINER_IP}; start.log:\n{log}")
    return cid, time.monotonic()


def rm_workdirs(target, *dirs):
    """Remove instance bundle dirs. Stateful bundles keep a live overlay mount at
    <dir>/rootfs (M4), which `rm -rf` cannot remove: unmount first."""
    for d in dirs:
        sh(target, f"(mountpoint -q {d}/rootfs && sudo umount {d}/rootfs) || true; sudo rm -rf {d}")


def delete_container(target, cid):
    sh(target, f"sudo runc delete -f {cid} || true")


def teardown_instance(target, cid, workdir):
    delete_container(target, cid)
    rm_workdirs(target, workdir)


def run_rep_same(cfg, args, workload, rep, run_dir):
    host = cfg["source"]
    target, pod = host["ssh"], host["statepod"]
    workdir = host["workdir"].rsplit("/", 1)[0] + "/hitrate"
    image, flags, loader = wl_setup(cfg, args, workload, target)

    print(f"  [{workload} rep {rep}] creating bundle from {image}...")
    cid, t_ready = spawn_instance(target, pod, workdir, image, workload, args.deterministic, flags)
    print(f"  [{workload} rep {rep}] ready — dumping timeline...")

    dumps = [dump(target, cid, workdir, "d0", t_ready)]
    time.sleep(IDLE1_S)
    dumps.append(dump(target, cid, workdir, "d_idle1", t_ready))
    time.sleep(IDLE2_S)
    dumps.append(dump(target, cid, workdir, "d_idle2", t_ready))
    load_info = loader(target, CONTAINER_IP, cid)
    dumps.append(dump(target, cid, workdir, "d_load", t_ready))
    time.sleep(POST_LOAD_IDLE_S)
    dumps.append(dump(target, cid, workdir, "d_load_idle", t_ready))

    d0 = dumps[0]["path"]
    comparisons = {}
    for d in dumps[1:]:
        res = pagecmp(target, pod, d["path"], d0)
        comparisons[d["label"] + "_vs_d0"] = res
        (run_dir / f"cmp_{d['label']}.json").write_text(json.dumps(res, indent=2))
        print(f"  [{workload} rep {rep}] {d['label']} vs d0: "
              f"content {res['content_hit_rate']:.3f}, addr {res['addr_hit_rate']:.3f} "
              f"({res['pages_a']} pages)")
    res = pagecmp(target, pod, dumps[4]["path"], dumps[3]["path"])
    comparisons["d_load_idle_vs_d_load"] = res
    (run_dir / "cmp_load_drift.json").write_text(json.dumps(res, indent=2))

    teardown_instance(target, cid, workdir)
    return {"container_id": cid, "load": load_info, "dumps": dumps,
            "comparisons": comparisons}


def run_rep_cross(cfg, args, workload, rep, run_dir):
    host = cfg["source"]
    target, pod = host["ssh"], host["statepod"]
    workdir_root = host["workdir"].rsplit("/", 1)[0] + "/hitrate"
    workdir_a, workdir_b = f"{workdir_root}/a", f"{workdir_root}/b"
    image, flags, loader = wl_setup(cfg, args, workload, target)

    # A and B get separate subdirs — A's dump must survive past its teardown
    # so B's dumps can be pagecmp'd against it.
    print(f"  [{workload} rep {rep}] instance A (template stand-in) from {image}...")
    cid_a, t_ready_a = spawn_instance(target, pod, workdir_a, image, workload, args.deterministic, flags)
    dump_a = dump(target, cid_a, workdir_a, "d0_a", t_ready_a)
    delete_container(target, cid_a)

    print(f"  [{workload} rep {rep}] instance B from {image}...")
    cid_b, t_ready_b = spawn_instance(target, pod, workdir_b, image, workload, args.deterministic, flags)
    dumps = [dump(target, cid_b, workdir_b, "d0_b", t_ready_b)]
    load_info = loader(target, CONTAINER_IP, cid_b)
    dumps.append(dump(target, cid_b, workdir_b, "d_load_b", t_ready_b))
    delete_container(target, cid_b)

    comparisons = {}
    for d in dumps:
        res = pagecmp(target, pod, d["path"], dump_a["path"])
        label = d["label"] + "_vs_a"
        comparisons[label] = res
        (run_dir / f"cmp_{label}.json").write_text(json.dumps(res, indent=2))
        print(f"  [{workload} rep {rep}] {d['label']} vs a: "
              f"content {res['content_hit_rate']:.3f}, addr {res['addr_hit_rate']:.3f} "
              f"({res['pages_a']} pages)")

    rm_workdirs(target, workdir_a, workdir_b)
    return {"container_id_a": cid_a, "container_id_b": cid_b, "load": load_info,
            "dumps": [dump_a] + dumps, "comparisons": comparisons}


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="hosts.yaml")
    ap.add_argument("--name", required=True)
    ap.add_argument("--workloads", default="redis,node",
                    help="comma list of redis, node, postgres, synth")
    ap.add_argument("--wl-param", dest="wl_param", action="append", default=[], metavar="KEY=VALUE",
                    help="synth knobs (resident-mb, shared-frac, entropy, dirty-pps, ...)")
    ap.add_argument("--synth-settle", dest="synth_settle", type=int, default=30,
                    help="synth: seconds of dirtying between the readiness dump and the 'after load' dump")
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--ssh-key", default=None)
    ap.add_argument("--mode", choices=["same", "cross"], default="same",
                     help="same = experiment (a); cross = experiment (b)/(c)")
    ap.add_argument("--deterministic", action="store_true",
                     help="cross mode only: launch both instances ASLR-off (experiment c)")
    args = ap.parse_args()

    cfg = load_config(args.config)
    ssh_key = args.ssh_key or cfg.get("ssh_key", "")
    if ssh_key:
        key_path = Path(ssh_key).expanduser()
        if not key_path.is_file():
            sys.exit(f"ERROR: ssh key not found: {key_path}")
        SSH_OPTS.extend(["-i", str(key_path)])

    src = cfg["source"]["ssh"]
    results_root = Path(cfg["experiment"].get("results_root", "../results")).expanduser()
    stamp = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    base_dir = results_root / f"{stamp}_{args.name}"

    if args.mode == "same":
        experiment_name = "hitrate_a_same_instance"
        aslr = "on (same-instance comparison is layout-invariant)"
        run_rep = run_rep_same
    else:
        experiment_name = ("hitrate_c_cross_aslr_off" if args.deterministic
                            else "hitrate_b_cross_aslr_on")
        aslr = "off (--deterministic, nrand wrapper)" if args.deterministic else "on"
        run_rep = run_rep_cross

    env = {
        "name": args.name,
        "timestamp": datetime.datetime.now().astimezone().isoformat(),
        "experiment": experiment_name,
        "host": {
            "ssh": src,
            "kernel": try_out(src, "uname -r"),
            "criu": try_out(src, "criu --version | head -1"),
            "runc": try_out(src, "runc --version | head -1"),
            "statepod_git": try_out(src, "cd ~/statepod && git rev-parse --short HEAD"),
        },
        "mode": args.mode,
        "aslr": aslr,
        "timeline": {"idle1_s": IDLE1_S, "idle2_s": IDLE2_S,
                     "post_load_idle_s": POST_LOAD_IDLE_S,
                     "seed_keys": SEED_KEYS, "node_requests": NODE_REQUESTS},
        "container_ip": CONTAINER_IP,
    }

    for workload in args.workloads.split(","):
        image = wl_setup(cfg, args, workload, src)[0]
        for rep in range(1, args.reps + 1):
            run_dir = base_dir / workload / f"rep{rep}"
            run_dir.mkdir(parents=True, exist_ok=True)
            manifest = dict(env)
            manifest.update({"workload": workload, "image": image, "repetition": rep,
                             "wl_param": dict(kv.split("=", 1) for kv in args.wl_param if "=" in kv)})
            (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
            manifest.update(run_rep(cfg, args, workload, rep, run_dir))
            (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))

    print(f"\nDone. Results in {base_dir}")


if __name__ == "__main__":
    try:
        main()
    except (subprocess.CalledProcessError, RuntimeError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
