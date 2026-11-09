#!/usr/bin/env python3
"""ablation-matrix driver: runs the trimmed grid used for the evaluation
(docs) as a sequence of `run_experiment.py` invocations, one
per cell, each already doing its own n-rep loop and full re-arm (fresh
`statepod create` + seed) between reps.

Not a re-implementation of run_experiment.py's SSH/experiment logic — this
just enumerates cells and shells out to it, so every cell gets the exact
same manifested results dir / log-scan verification / iperf3 probes as a
manual run does.

Cells (full rationale there):
  - Core: all 9 variants x {redis, node}, dataset=medium, unshaped real-WAN.
  - Dataset spot-check: 4 variants x {small, large}, redis only (medium
    already covered by core) -- the churn-boundary comparison. Includes
    m5-postcopy specifically to get the M5 downtime-vs-state-size curve
    the analysis scripts need (the single-workload demo only has
    one size point).
  - ASLR-on knob-check: 2 variants, redis, medium -- expect the baseline
    hit rate to collapse (that's the measured result, not a failure); see
    hit-rate experiment (b) vs (c) for the already-established magnitude.
  - Network grid: 3 variants x 4 netem points, redis, medium (the unshaped
    real-WAN point is already the core sweep's netem="").
  - Warm-baseline capture is NOT included: `statepod template generate` has
    no warm-capture hook (always dumps at readiness) and research_plan.md's
    own slip-rule lists "warm-capture variant" as the first thing to cut --
    cutting it here rather than building new statepod functionality for it.
  - Multi-container is its own separate scenario/script, not this grid
    (shared-baseline mechanics don't fit the single-container cell shape).
  - "+prefetch" and "+M1" (research_plan.md's ladder) collapse into one
    cell here: in the shipped system, prefetching the destination template
    is a one-time precondition (`ensure_template`), not a togglable stage
    of a single migration invocation, so there's no distinct "+prefetch,
    not yet +M1" migration to run.

Usage:
    ./run_matrix.py --list                     # print the cell table, don't run
    ./run_matrix.py                             # run every cell, n=5, skip already-run
    ./run_matrix.py --only m1                   # substring filter on cell id
    ./run_matrix.py --reps 3 --config hosts.yaml
"""

import argparse
import datetime
import json
import subprocess
import sys
from pathlib import Path

RUNNER_DIR = Path(__file__).resolve().parent

VARIANTS = {
    # id: extra run_experiment.py flags (dict of flag -> value; True = bare flag)
    "restart": {"mode": "sync", "no-volume-sync": True},
    "unopt-precopy": {"mode": "precopy"},
    "criu-incr": {"mode": "criu-incr"},
    "m1": {"mode": "synth-precopy", "precopy-rounds": 0, "zstd-level": 0, "no-fs-upper": True},
    "m2": {"mode": "synth-precopy", "precopy-rounds": 4, "zstd-level": 0, "no-fs-upper": True},
    "compression": {"mode": "synth-precopy", "precopy-rounds": 4, "zstd-level": 1, "no-fs-upper": True},
    "m3": {"mode": "synth-precopy", "precopy-rounds": 4, "zstd-level": 1, "no-fs-upper": True,
           "tmpfs-checkpoint": True},
    "m4-full": {"mode": "synth-precopy", "precopy-rounds": 4, "zstd-level": 1, "tmpfs-checkpoint": True},
    "m5-postcopy": {"mode": "synth-postcopy"},
}

WORKLOADS = ["redis", "node"]

# label -> tc netem spec (rate/delay grid points; the 5th "unshaped" point is
# netem="" and is already what the core sweep runs).
NETEM_POINTS = {
    "net-5m100ms": "rate 5mbit delay 100ms",
    "net-20m50ms": "rate 20mbit delay 50ms",
    "net-50m20ms": "rate 50mbit delay 20ms",
    "net-100m20ms": "rate 100mbit delay 20ms",
}

DATASET_SPOTCHECK_VARIANTS = ["unopt-precopy", "m1", "m4-full", "m5-postcopy"]
DATASET_SPOTCHECK_SIZES = ["small", "large"]
ASLR_KNOBCHECK_VARIANTS = ["m1", "m4-full"]
NETWORK_GRID_VARIANTS = ["unopt-precopy", "m4-full", "m5-postcopy"]


def build_cells():
    cells = []

    def add(cell_id, variant_id, workload, **overrides):
        flags = dict(VARIANTS[variant_id])
        flags.update(overrides)
        cells.append({"id": cell_id, "variant": variant_id, "workload": workload, "flags": flags})

    # Core: every variant x both workloads, dataset=medium, unshaped WAN.
    for vid in VARIANTS:
        for wl in WORKLOADS:
            add(f"core-{vid}-{wl}", vid, wl, dataset="medium")

    # Dataset spot-check (redis only -- dataset size is a no-op for node).
    for vid in DATASET_SPOTCHECK_VARIANTS:
        for size in DATASET_SPOTCHECK_SIZES:
            add(f"dataset-{vid}-{size}", vid, "redis", dataset=size)

    # ASLR-on knob-check (redis, medium).
    for vid in ASLR_KNOBCHECK_VARIANTS:
        add(f"aslr-on-{vid}", vid, "redis", dataset="medium", **{"aslr-on": True})

    # Network sensitivity grid (redis, medium; unshaped point = core sweep).
    for vid in NETWORK_GRID_VARIANTS:
        for label, spec in NETEM_POINTS.items():
            add(f"{label}-{vid}", vid, "redis", dataset="medium", netem=spec)

    return cells


def cell_cmd(cell, cfg_path, reps):
    # flags["multi"] = N: N concurrent instances of the workload in one run (run_multicontainer.py,
    # ); everything else is passed the same way as to run_experiment.py
    multi = cell["flags"].get("multi")
    script = "run_multicontainer.py" if multi else "run_experiment.py"
    cmd = [sys.executable, str(RUNNER_DIR / script),
           "--config", cfg_path, "--name", cell["id"], "--reps", str(reps),
           "--mode", cell["flags"]["mode"], "--workload", cell["workload"]]
    if multi:
        cmd += ["--n", str(multi)]
    if "dataset" in cell["flags"]:
        cmd += ["--dataset", cell["flags"]["dataset"]]
    if "netem" in cell["flags"]:
        cmd += ["--netem", cell["flags"]["netem"]]
    for flag, val in cell["flags"].items():
        if flag in ("mode", "dataset", "netem", "multi"):
            continue
        if val is True:
            cmd.append(f"--{flag}")
        elif isinstance(val, (list, tuple)):
            for v in val:  # repeatable flags (--wl-param, --migrate-env, --meta)
                cmd += [f"--{flag}", str(v)]
        elif val is not False:
            cmd += [f"--{flag}", str(val)]
    return cmd


def already_run(cell, results_root):
    return any(results_root.glob(f"*_{cell['id']}"))


def summarize_cell(cell, results_root):
    """Read back the most recent matching results dir's rep manifests."""
    dirs = sorted(results_root.glob(f"*_{cell['id']}"))
    if not dirs:
        return {"reps_found": 0, "reps_ok": 0}
    latest = dirs[-1]
    manifests = sorted(latest.glob("rep*/manifest.json")) or (
        [latest / "manifest.json"] if (latest / "manifest.json").exists() else [])
    ok = 0
    for m in manifests:
        try:
            data = json.loads(m.read_text())
            if data.get("state_verify", {}).get("ok"):
                ok += 1
        except Exception:
            pass
    return {"dir": str(latest), "reps_found": len(manifests), "reps_ok": ok}


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="hosts.yaml")
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--only", default=None, help="substring filter on cell id")
    ap.add_argument("--list", action="store_true", help="print the cell table and exit")
    ap.add_argument("--force", action="store_true",
                    help="re-run cells even if a results dir for them already exists")
    ap.add_argument("--tag", default=None,
                    help="prefix every cell id with TAG- (e.g. --tag uni). Without this, "
                         "results/ is git-tracked and already carries another "
                         "testbed's dirs under these exact cell ids, so "
                         "the already-run check (and any human/script later globbing by "
                         "cell id) would silently conflate two different testbeds' data. "
                         "Use a tag for any campaign run on a different testbed/config.")
    args = ap.parse_args()

    cells = build_cells()
    if args.tag:
        for c in cells:
            c["id"] = f"{args.tag}-{c['id']}"
    if args.only:
        cells = [c for c in cells if args.only in c["id"]]

    if args.list:
        for c in cells:
            print(f"{c['id']:35s} variant={c['variant']:14s} workload={c['workload']:6s} "
                  f"{ {k: v for k, v in c['flags'].items() if k != 'mode'} }")
        print(f"\n{len(cells)} cells x {args.reps} reps = {len(cells) * args.reps} total reps")
        return

    # Matches hosts.yaml's experiment.results_root default; run_experiment.py
    # resolves the same path independently per invocation.
    results_root = (RUNNER_DIR / "../results").resolve()

    stamp = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    summary_path = results_root / f"_matrix_{stamp}_summary.json"
    summary = {"started": stamp, "reps_per_cell": args.reps, "cells": {}}

    print(f"Matrix: {len(cells)} cells x {args.reps} reps = {len(cells) * args.reps} total reps")
    for i, cell in enumerate(cells, 1):
        if not args.force and already_run(cell, results_root):
            print(f"[{i}/{len(cells)}] {cell['id']}: SKIP (results dir already exists)")
            summary["cells"][cell["id"]] = {"status": "skipped", **summarize_cell(cell, results_root)}
            summary_path.write_text(json.dumps(summary, indent=2))
            continue

        cmd = cell_cmd(cell, args.config, args.reps)
        print(f"[{i}/{len(cells)}] {cell['id']}: {' '.join(cmd[2:])}")
        rc = subprocess.run(cmd, cwd=RUNNER_DIR).returncode
        result = summarize_cell(cell, results_root)
        result["status"] = "ok" if rc == 0 and result["reps_ok"] == result["reps_found"] else "failed"
        result["exit_code"] = rc
        summary["cells"][cell["id"]] = result
        summary_path.write_text(json.dumps(summary, indent=2))
        print(f"    -> {result['status']} ({result['reps_ok']}/{result['reps_found']} reps state_verify.ok)")

    n_ok = sum(1 for c in summary["cells"].values() if c["status"] == "ok")
    n_skip = sum(1 for c in summary["cells"].values() if c["status"] == "skipped")
    n_fail = sum(1 for c in summary["cells"].values() if c["status"] == "failed")
    print(f"\nDone: {n_ok} ok, {n_skip} skipped, {n_fail} failed. Summary: {summary_path}")


if __name__ == "__main__":
    main()
