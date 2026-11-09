#!/usr/bin/env python3
"""Source-host resource sampler (source-reclaim metrics).

Runs on the SOURCE VM next to the migration (started and stopped by
run_experiment.py, copied back as source_resources.log) and writes one JSON line
every --interval seconds:

  {"t": <epoch>, "cpu_busy": <jiffies>, "cpu_total": <jiffies>, "mem_avail_kb": N,
   "mig_rss_kb": <sum RSS of statepod/criu/runc processes>, "ckpt_used_kb": N|null}

`t` is time.time() on the source VM -- the same clock domain as the `src_*` phase
marks in migration_metrics.json -- so analysis/source_cost.py can cut the series at
the migration window without any clock translation. Stdlib only (the VMs have
python3.10).

  source_sampler.py [--interval 0.25] [--ckpt-dir ~/statepod/experiment/source/checkpoint]
"""
import argparse
import json
import os
import sys
import time

PROCS = {"statepod", "criu", "runc"}
PAGE_KB = os.sysconf("SC_PAGE_SIZE") // 1024


def cpu_jiffies():
    with open("/proc/stat") as f:
        v = [int(x) for x in f.readline().split()[1:]]
    total = sum(v[:8])                      # user nice system idle iowait irq softirq steal
    idle = v[3] + v[4]
    return total - idle, total


def mem_avail_kb():
    with open("/proc/meminfo") as f:
        for line in f:
            if line.startswith("MemAvailable:"):
                return int(line.split()[1])
    return None


def migration_rss_kb():
    total = 0
    for d in os.listdir("/proc"):
        if not d.isdigit():
            continue
        try:
            with open(f"/proc/{d}/comm") as f:
                if f.read().strip() not in PROCS:
                    continue
            with open(f"/proc/{d}/statm") as f:
                total += int(f.read().split()[1]) * PAGE_KB
        except OSError:
            continue                          # exited between listdir and open
    return total


def ckpt_used_kb(path):
    """Used space of the checkpoint filesystem (meaningful for the tmpfs variant;
    for a disk checkpoint dir it is the whole filesystem and is reported as null)."""
    try:
        st = os.statvfs(path)
        with open("/proc/mounts") as f:
            tmpfs = any(p[1] == path and p[2] == "tmpfs" for p in (l.split() for l in f))
        return (st.f_blocks - st.f_bfree) * st.f_frsize // 1024 if tmpfs else None
    except OSError:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=float, default=0.25)
    ap.add_argument("--ckpt-dir", default=os.path.expanduser("~/statepod/experiment/source/checkpoint"))
    args = ap.parse_args()
    ckpt = os.path.realpath(args.ckpt_dir)
    try:
        while True:
            t = time.time()
            busy, total = cpu_jiffies()
            print(json.dumps({"t": round(t, 3), "cpu_busy": busy, "cpu_total": total,
                              "mem_avail_kb": mem_avail_kb(), "mig_rss_kb": migration_rss_kb(),
                              "ckpt_used_kb": ckpt_used_kb(ckpt)}), flush=True)
            time.sleep(max(0.0, args.interval - (time.time() - t)))
    except (KeyboardInterrupt, BrokenPipeError):
        return 0


if __name__ == "__main__":
    sys.exit(main())
