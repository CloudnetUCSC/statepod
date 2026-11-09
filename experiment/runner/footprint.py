#!/usr/bin/env python3
"""Measure how much memory a workload really holds once seeded.

A small destination (~1 GB of RAM, no swap) can be OOM-killed when a run overshoots.
A workload that is too big does not fail cleanly: the kernel OOM-kills statepod and the VM wedges
until a console reboot. This runs each (workload, dataset) on the SOURCE VM only (4 GB), reads the
container's proportional set size (PSS, so PostgreSQL's shared memory is not counted six times) and
writes `results/footprints.json`. run_campaign.py refuses cells whose estimated destination peak
exceeds the budget.

    python footprint.py --config hosts.yaml redis:medium jvm:small jvm:medium pyapp:medium postgres:small

Estimate of the destination peak: the restored workload (PSS) + the checkpoint images held in RAM
(PSS again when --tmpfs-checkpoint, M5's page store or a lazy restore) + a fixed base for the OS,
docker and the receiver. See est_dest_peak_mb().
"""
import argparse
import json
import sys
import time
from pathlib import Path

import run_experiment as rx

RESULTS = Path(__file__).resolve().parent.parent / "results"
FOOTPRINTS = RESULTS / "footprints.json"
DEST_BASE_MB = 300       # OS + dockerd + containerd + receiver on a ~1 GB VM (measured idle: ~240 MB used + cache)
DEST_BUDGET_MB = 850     # leave ~100 MB; the VM wedges, it does not degrade
RAM_COPIES = {"tmpfs": 2, "m5": 2, "criu-lazy": 2, "default": 1}  # restored workload + RAM-resident copy of its state


def est_dest_peak_mb(pss_mb, copies=2):
    """Conservative: the restored workload plus `copies-1` RAM-resident copies of its state."""
    return DEST_BASE_MB + pss_mb * copies


def load():
    try:
        return json.loads(FOOTPRINTS.read_text())
    except Exception:
        return {}


def measure(cfg, args, workload, dataset):
    src, wd = cfg["source"]["ssh"], cfg["source"]["workdir"]
    ns = argparse.Namespace(**{**vars(args), "workload": workload, "dataset": dataset, "mode": "precopy",
                               "fs_sync": None, "wl_param": [], "write_rate": None})
    rx.setup_containers(cfg, ns)   # recreate both bundles, start + seed the source (destination only gets a bundle)
    time.sleep(3)
    cid = "$(python3 -c \"import json;print(json.load(open('statepod.json'))['container_id'])\")"
    script = (f"cd {wd} && for p in $(sudo runc ps -f json {cid} | python3 -c 'import json,sys;print(*json.load(sys.stdin))'); do "
              f"sudo awk '/^Pss:/{{p+=$2}} /^Rss:/{{r+=$2}} END{{print p, r}}' /proc/$p/smaps_rollup 2>/dev/null; done")
    r = rx.ssh(src, script, check=False, timeout=120)
    pss = rss = 0
    for line in r.stdout.splitlines():
        try:
            p, q = line.split()
            pss += int(p)
            rss += int(q)
        except ValueError:
            pass
    return {"pss_mb": round(pss / 1024, 1), "rss_sum_mb": round(rss / 1024, 1), "when": time.strftime("%Y-%m-%dT%H:%M:%S")}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="hosts.yaml")
    ap.add_argument("targets", nargs="+", help="workload:dataset, e.g. jvm:medium")
    a = ap.parse_args()
    cfg = rx.load_config(a.config)
    key = cfg.get("ssh_key", "")
    if key:
        rx.SSH_OPTS.extend(["-i", str(Path(key).expanduser())])
    # the arguments setup_containers reads; defaults match run_experiment's parser
    base = {"deterministic": False, "tmpfs_checkpoint": False, "no_fs_upper": False, "no_volume_sync": False,
            "aslr_on": False, "mode": "precopy", "fault": None, "lost_write": False, "conn_client": False,
            "precopy_rounds": None, "zstd_level": None, "dest_min_avail_mb": 0, "fs_sync": None}
    out = load()
    for t in a.targets:
        w, d = t.split(":")
        ns = argparse.Namespace(**base)
        try:
            res = measure(cfg, ns, w, d)
        except Exception as e:  # keep going: one workload that will not start must not hide the rest
            print(f"{t}: FAILED {e}", file=sys.stderr)
            continue
        out[t] = res
        print(f"{t}: PSS {res['pss_mb']} MB  (sum of RSS {res['rss_sum_mb']} MB)  ->  est. destination peak "
              f"{est_dest_peak_mb(res['pss_mb']):.0f} MB of {DEST_BUDGET_MB}", flush=True)
        FOOTPRINTS.write_text(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
