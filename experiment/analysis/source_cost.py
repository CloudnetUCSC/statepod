"""What the migration costs the SOURCE host: CPU-seconds and
memory spent between the migration command and the moment the source is released.

Inputs, both produced by run_experiment.py on the source VM:
  source_migrate_time.txt   `/usr/bin/time -v` of the whole `statepod migrate` tree
                            (statepod + every runc/criu it waited for): exact user and
                            system CPU seconds and the largest single process's RSS
  source_resources.log      runner/source_sampler.py time series (0.25 s): host CPU
                            jiffies, MemAvailable, summed RSS of statepod/criu/runc,
                            tmpfs checkpoint usage

and migration_metrics.json for the window [migration start, source release] (the
release instant is reclaim.py's, so eviction time and cost share one definition).

Reported, never imputed:
  tree_cpu_s           user+system CPU of the migration process tree (pre-dump images
                       included). Exact, but excludes the workload's own CPU.
  tree_max_rss_kb      RSS of the largest single process in the tree (getrusage)
  host_cpu_net_s       host-wide busy CPU over the window minus the idle-time baseline
                       (mean busy rate over the 5 s before it) x window: includes the
                       workload's own response to being tracked (soft-dirty faults)
  mig_rss_peak_kb      peak summed RSS of statepod+criu+runc processes (sampled, so a
                       process shorter than the interval can be missed: a lower bound)
  mem_avail_drop_kb    baseline MemAvailable - minimum over the window (what the
                       migration cost the host, incl. tmpfs checkpoints and page cache)
  ckpt_tmpfs_peak_kb   peak tmpfs checkpoint size (null for a disk checkpoint)
"""
import json
import re
from pathlib import Path

import reclaim

CLK_TCK = 100.0
BASELINE_S = 5.0


def parse_time_v(text):
    out = {}
    for key, pat in (("user_s", r"User time \(seconds\):\s*([\d.]+)"),
                     ("sys_s", r"System time \(seconds\):\s*([\d.]+)"),
                     ("max_rss_kb", r"Maximum resident set size \(kbytes\):\s*(\d+)")):
        m = re.search(pat, text)
        out[key] = float(m.group(1)) if m else None
    return out


def load_samples(path):
    rows = []
    try:
        for line in Path(path).read_text().splitlines():
            if line.startswith("{"):
                try:
                    rows.append(json.loads(line))
                except ValueError:
                    pass                       # a line cut by the stop signal
    except OSError:
        pass
    return rows


def summarize(run_dir, metrics):
    run_dir = Path(run_dir)
    out = {"tree_cpu_s": None, "tree_cpu_user_s": None, "tree_cpu_sys_s": None, "tree_max_rss_kb": None,
           "window_s": None, "host_cpu_net_s": None, "mig_rss_peak_kb": None,
           "mem_avail_drop_kb": None, "ckpt_tmpfs_peak_kb": None, "samples": 0}
    tfile = run_dir / "source_migrate_time.txt"
    if tfile.exists():
        t = parse_time_v(tfile.read_text(errors="replace"))
        if t["user_s"] is not None and t["sys_s"] is not None:
            out["tree_cpu_user_s"], out["tree_cpu_sys_s"] = t["user_s"], t["sys_s"]
            out["tree_cpu_s"] = round(t["user_s"] + t["sys_s"], 3)
        out["tree_max_rss_kb"] = int(t["max_rss_kb"]) if t["max_rss_kb"] is not None else None

    start = reclaim._ts(metrics.get("migration_start_time"))
    phases = metrics.get("phases") or {}
    release = reclaim._ts(phases.get(reclaim.RELEASE_PHASE.get(metrics.get("mode"), "")))
    rows = load_samples(run_dir / "source_resources.log")
    out["samples"] = len(rows)
    if start is None or release is None or release <= start or not rows:
        return out
    out["window_s"] = round(release - start, 3)
    win = [r for r in rows if start <= r["t"] <= release]
    base = [r for r in rows if start - BASELINE_S <= r["t"] < start]
    if len(win) < 2:
        return out
    dt = win[-1]["t"] - win[0]["t"]
    busy = (win[-1]["cpu_busy"] - win[0]["cpu_busy"]) / CLK_TCK
    rate = 0.0
    if len(base) >= 2 and base[-1]["t"] > base[0]["t"]:
        rate = (base[-1]["cpu_busy"] - base[0]["cpu_busy"]) / CLK_TCK / (base[-1]["t"] - base[0]["t"])
    out["host_cpu_net_s"] = round(busy - rate * dt, 3)
    out["mig_rss_peak_kb"] = max(r["mig_rss_kb"] for r in win)
    avail = [r["mem_avail_kb"] for r in win if r.get("mem_avail_kb") is not None]
    bavail = [r["mem_avail_kb"] for r in base if r.get("mem_avail_kb") is not None]
    if avail and bavail:
        out["mem_avail_drop_kb"] = int(sum(bavail) / len(bavail) - min(avail))
    ck = [r["ckpt_used_kb"] for r in win if r.get("ckpt_used_kb") is not None]
    out["ckpt_tmpfs_peak_kb"] = max(ck) if ck else None
    return out


if __name__ == "__main__":  # python source_cost.py <run_dir>
    import sys
    d = Path(sys.argv[1])
    print(json.dumps(summarize(d, json.loads((d / "migration_metrics.json").read_text())), indent=2))
