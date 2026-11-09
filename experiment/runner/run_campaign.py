#!/usr/bin/env python3
"""Interleaved evaluation campaign driver (12g).

Why not run_matrix.py: that runs one cell at a time (all n reps back to back),
so cells are separated by hours on a link whose upload varies 2-23 Mb/s, and
differences between methods mix with differences between times. Here a ROUND
runs every cell once, in a random order drawn from (seed, round), and the
campaign is `--rounds` such rounds: slow drift of the link hits every method
equally and is visible in the per-run bandwidth record.

Rules this driver enforces (fixed before any data):
  * Validity gate. A run counts only if (a) state_verify passed and (b) the
    link probes bracketing it measured enough bandwidth:
      - capped runs (netem `rate X`): iperf3 before AND after >= gate_frac*X
        (default 0.9) -- the probe offers exactly X (-b), 3 s each;
      - uncapped runs: both probes >= --min-mbps if given.
    A run that fails the gate is DISCARDED and the cell re-run (up to
    --max-attempts); discards are never selected by outcome, only by the
    gate, and every attempt is written to the ledger.
  * Everything is recorded: results dirs carry `meta` {campaign, cell, round,
    attempt}; ledger.jsonl has one line per attempt; summary.json gives the
    run-count ledger (attempted / valid / discarded-by-gate / failed) the paper
    must report.
  * Resumable: re-running with the same --campaign skips (cell, round) pairs
    that already have a valid attempt in the ledger.

Cell sources: a preset (`--preset ladder`) or a JSON file (`--cells f.json`,
a list of {"id", "workload", "flags": {...}} -- same shape as run_matrix.py).

    python run_campaign.py --list --preset ladder
    python run_campaign.py --config hosts.yaml --campaign c1 --preset ladder --rounds 10 --seed 7
    python run_campaign.py --campaign c1 --summary          # ledger summary only

Output: experiment/results/_campaign_<name>/{ledger.jsonl,summary.json}.
"""
import argparse
import datetime
import json
import random
import re
import statistics
import subprocess
import sys
import time
from pathlib import Path

import run_matrix as rm
import run_experiment as rx
import footprint as fp

RUNNER_DIR = Path(__file__).resolve().parent
RESULTS = RUNNER_DIR.parent / "results"

# Campaign variants. The ladder below is in the CATCH-UP semantics:
# every synth-precopy variant pauses the source only after a verified
# checkpoint, so none of them lose acknowledged writes; `availability` is the
# labelled ablation and is run with --lost-write --expect-loss so its write
# window is reported next to its (flattering) downtime.
VARIANTS = dict(rm.VARIANTS)
VARIANTS.update({
    "vanilla": {"mode": "precopy"},
    # the same baselines with the same zstd level as the pipeline, so
    # the share of the reduction that is plain compression is visible.
    "vanilla-zstd": {"mode": "precopy", "zstd-level": 1},
    "criu-incr-zstd": {"mode": "criu-incr", "zstd-level": 1},
    # filesystem baseline: the same plain pre-copy plus a stop-and-copy of
    # the writable layer (what M4 is measured against); needed for any workload
    # that keeps state in the container filesystem (synth files, PostgreSQL).
    # the filesystem baseline moves the upper layer exactly like every other
    # variant (rsync pre-copy rounds, fsprecopy.go). The stop-and-copy tar it used before
    # 2026-10-26 stays available as vanilla-zstd-fsflat (historical; PostgreSQL ~73 s downtime).
    "vanilla-zstd-fs": {"mode": "precopy", "zstd-level": 1, "fs-sync": "rsync"},
    "vanilla-zstd-fsflat": {"mode": "precopy", "zstd-level": 1, "fs-sync": "flat"},
    # the volume-sync restart (Kubernetes-StatefulSet style): writable layer
    # pre-synced in the same rsync rounds, graceful stop, cold start from disk at the destination.
    "restart-volume": {"mode": "restart-volume", "zstd-level": 1},
    "m1": {"mode": "synth-precopy", "precopy-rounds": 1, "zstd-level": 0, "no-fs-upper": True},
    "m2": {"mode": "synth-precopy", "precopy-rounds": 4, "zstd-level": 0, "no-fs-upper": True},
    "compression": {"mode": "synth-precopy", "precopy-rounds": 4, "zstd-level": 1, "no-fs-upper": True},
    "m3": {"mode": "synth-precopy", "precopy-rounds": 4, "zstd-level": 1, "no-fs-upper": True,
           "tmpfs-checkpoint": True},
    "m4": {"mode": "synth-precopy", "precopy-rounds": 4, "zstd-level": 1, "tmpfs-checkpoint": True},
    "m4-merge": {"mode": "synth-precopy", "precopy-rounds": 4, "zstd-level": 1, "tmpfs-checkpoint": True,
                 "merge-restore": True},
    "m4-availability": {"mode": "synth-precopy", "precopy-rounds": 4, "zstd-level": 1,
                        "tmpfs-checkpoint": True, "final-mode": "availability",
                        "lost-write": True, "expect-loss": True},
    "m5": {"mode": "synth-postcopy", "drain-timeout": "10m"},
    # the same mechanisms with the source launched WITH ASLR (the
    # destination's template stays deterministic): the measured cost of keeping full ASLR.
    "m4-aslron": {"mode": "synth-precopy", "precopy-rounds": 4, "zstd-level": 1, "tmpfs-checkpoint": True,
                  "aslr-on": True},
    "m5-aslron": {"mode": "synth-postcopy", "drain-timeout": "10m", "aslr-on": True},
    # stock CRIU post-copy (runc checkpoint --lazy-pages + criu lazy-pages
    # page server over the same link). Measures M5's premise instead of citing it.
    "criu-lazy": {"mode": "criu-lazy", "drain-timeout": "30m"},
    # fair restart baseline: state PRESERVED. SyncStrategy with volume
    # sync does a Redis SAVE, kills the source, ships the data directory and
    # starts the destination, which loads the RDB. (`restart` above is the
    # labelled no-persistence variant that deliberately loses the data.)
    "restart-persist": {"mode": "sync"},
})

SWEEP_VARIANTS = ("vanilla-zstd", "m4-merge", "m5")
ENTROPY_VARIANTS = ("vanilla", "vanilla-zstd", "m1", "m4-merge")


def synth_cells(label_fmt, points, variants, base=None):
    """One preset entry per (point, variant): `points` is a list of (knob, value)
    to sweep, `base` the fixed knobs. label_fmt may use {mb}/{pps}/... only via
    the swept value (`{mb}`, `{pps}`) or be a plain string."""
    out = []
    for knob, val in points:
        params = dict(base or {})
        params[knob] = str(val)
        label = label_fmt.format(mb=val, pps=val, v=val)
        for v in variants:
            out.append({"variant": v, "workload": "synth", "label": label,
                        "extra": {"wl-param": [f"{k}={x}" for k, x in sorted(params.items())]}})
    return out


# Variants for workloads that keep state in the container filesystem (PostgreSQL, the JVM/Python
# services' /tmp, synthetic files): every one of them moves the filesystem (M4, or the plain
# baseline's --fs-sync flat), because a variant without it restores against missing files.
FS_VARIANTS = ("vanilla-zstd-fs", "criu-lazy", "m4", "m4-merge", "m5")
# Cells that cannot run on a small (~1 GB, no swap) destination VM (measured OOM).
# They are NOT in the presets and are written to campaign.json so the paper's run-count ledger lists them.
TESTBED_DROPPED = [
    {"cell": "m4-jvm-medium", "reason": "destination OOM: tmpfs checkpoint (~359 MB shmem) + restored JVM in ~1 GB"},
    {"cell": "m4-merge-jvm-medium", "reason": "destination OOM: tmpfs checkpoint (~359 MB shmem) + restored JVM in ~1 GB"},
    {"cell": "criu-lazy-postgres-medium-cap2", "reason": "destination OOM: stock CRIU lazy-pages on Postgres, kernel killed criu "
                                                       "on a ~1 GB destination. "
                                                       "The failed attempt stays in the ledger."},
]
JVM_VARIANTS = tuple(v for v in FS_VARIANTS if v not in ("m4", "m4-merge"))
# Fault matrix: what is injected where. Partitions of 15 s are recoverable, 150 s
# exceeds the transport's 120 s idle deadline and forces the fallback path.
FAULT_SPECS = ("loss@precopy:30", "partition@precopy:15", "partition@final:15", "partition@final:150",
               "kill-receiver@precopy", "kill-receiver@final", "kill-restore@final",
               "partition@drain:30", "kill-receiver@drain")
FAULT_VARIANTS = ("vanilla", "m4-merge", "m5", "criu-lazy")
# Phases a variant does not have: the fault never fires ("not_injected"), so the cell would be a
# false pass. Measured 2026-10-08 (faults_1008): vanilla/m4-merge have no drain phase; m5/criu-lazy
# have no pre-copy rounds. These combinations are "n/a" in the paper's fault matrix, not run.
FAULT_NA = {"vanilla": ("drain",), "m4": ("drain",), "m4-merge": ("drain",), "m5": ("precopy",),
            "criu-lazy": ("precopy",)}
# After the fault-path fixes: the variants those fixes touch, plus plain M4 (the fault
# matrix had only merge-restore), and a stuck lazy-pages daemon for the two post-copy variants.
FAULT_V2_VARIANTS = ("m4", "m4-merge", "m5")


def fault_applicable(variant, spec):
    phase = spec.split("@")[1].split(":")[0]
    return phase not in FAULT_NA.get(variant, ())


def safe(label):
    """A cell-id / results-dir safe form of a free-form label ('@' and ':' are not valid in Windows paths)."""
    import re
    return re.sub(r"[^A-Za-z0-9._-]", "_", label.replace("@", "-at-"))


def wl_cells(workload, variants, label=None, extra=None):
    return [{"variant": v, "workload": workload, **({"label": label} if label else {}),
             "extra": dict(extra or {})} for v in variants]


PRESETS = {
    # The core comparison, redis, medium dataset, real link (lost-write on for
    # every state-preserving variant so each run also checks consistency).
    "ladder": [
        {"variant": v, "workload": "redis", "extra": {"dataset": "medium"}}
        for v in ("vanilla", "vanilla-zstd", "criu-incr", "criu-incr-zstd", "criu-lazy", "m1", "m2", "compression", "m3",
                  "m4", "m4-merge", "m4-availability", "m5")
    ] + [{"variant": "restart-persist", "workload": "redis", "extra": {"dataset": "medium"}},
         {"variant": "restart", "workload": "redis", "extra": {"dataset": "medium"}}],
    # sweeps on the synthetic workload (knobs: workloads.Synth.KNOBS). Each
    # sweep varies ONE axis; the variants are the matched baseline (vanilla with the
    # same zstd), the full catch-up pipeline and M5 (+ M1 without compression where the
    # question is reconciliation vs generic compression).
    "synth-size": synth_cells("size{mb}mb", [("resident-mb", mb) for mb in (16, 64, 256)],
                              SWEEP_VARIANTS, base={"shared-frac": "0.5"}),
    "synth-entropy": [c for ent, sf in (("random", "0.0"), ("zero", "0.0"), ("repeat", "0.0"),
                                         ("random", "0.5"), ("zero", "0.5"), ("repeat", "0.5"),
                                         ("random", "1.0"))
                      for c in synth_cells(f"{ent}-shared{sf}", [("entropy", ent)], ENTROPY_VARIANTS,
                                           base={"shared-frac": sf, "resident-mb": "64"})],
    "synth-dirty": synth_cells("dirty{pps}pps", [("dirty-pps", pps) for pps in (0, 500, 2000, 8000)],
                               SWEEP_VARIANTS, base={"shared-frac": "0.5", "resident-mb": "64"}),
    # the real workloads, each through every filesystem-carrying variant (medium).
    "workloads": ([c for wl in ("postgres", "pyapp")
                   for c in wl_cells(wl, tuple(v for v in FS_VARIANTS if not (wl == "postgres" and v == "criu-lazy")),
                                     extra={"dataset": "medium"})]
                  + wl_cells("jvm", JVM_VARIANTS, extra={"dataset": "medium"})
                  # No "m1" here: it runs with --no-fs-upper, the AOF file lives in the upper layer and
                  # --fs-sync is not available for synth-precopy, so the destination restore always fails.
                  # M1 is isolated on redis / synth (no filesystem state) instead.
                  + wl_cells("redis-aof", ("vanilla-zstd-fs", "criu-lazy", "m4-merge", "m5", "restart-persist"),
                             extra={"dataset": "medium"})
                  + synth_cells("size64mb", [("resident-mb", 64)], FS_VARIANTS, base={"shared-frac": "0.5"})),
    # persistence on, a 500,000-key dataset and a write-heavy mix.
    "redis-extended": (wl_cells("redis-aof", ("vanilla-zstd-fs", "m4-merge", "m5", "criu-lazy", "restart-persist"),
                                label="xlarge-wh2000", extra={"dataset": "xlarge", "write-rate": 2000})
                       + wl_cells("redis-aof", ("vanilla-zstd-fs", "m4-merge", "m5", "criu-lazy"),
                                  label="large-wh500", extra={"dataset": "large", "write-rate": 500})),
    # filesystem sweeps (synthetic): written MB and rewrite rate against M4 and the baseline.
    "synth-fs": synth_cells("files{mb}mb", [("file-mb", mb) for mb in (1, 16, 64, 256)],
                            ("vanilla-zstd-fs", "m4-merge", "m5", "criu-lazy"),
                            base={"resident-mb": "16", "shared-frac": "0.5"}),
    "synth-fs-rewrite": synth_cells("files64mb-rewrite{pps}", [("files-pps", pps) for pps in (0, 5, 20)],
                                    ("vanilla-zstd-fs", "m4-merge", "m5", "criu-lazy"),
                                    base={"resident-mb": "16", "file-mb": "64", "shared-frac": "0.5"}),
    # fault matrix: one cell per (variant, fault); lost-write client on. The run is VALID once
    # its outcome is recorded: an unsafe outcome is a result, not a failure to re-run.
    "faults": [{"variant": v, "workload": "redis", "label": "fault-" + safe(f),
                "extra": {"dataset": "medium", "fault": f, "lost-write": True}}
               for v in FAULT_VARIANTS for f in FAULT_SPECS if fault_applicable(v, f)],
    "faults-v2": [{"variant": v, "workload": "redis", "label": "fault-" + safe(f),
                   "extra": {"dataset": "medium", "fault": f, "lost-write": True}}
                  for v in FAULT_V2_VARIANTS for f in FAULT_SPECS if fault_applicable(v, f)]
                 + [{"variant": v, "workload": "redis", "label": "fault-" + safe("hang-daemon@drain"),
                     "extra": {"dataset": "medium", "fault": "hang-daemon@drain", "lost-write": True}}
                    for v in ("m5", "criu-lazy")],
    # one long-lived client connection through each migration.
    "continuity": [{"variant": v, "workload": "redis", "label": "conn",
                    "extra": {"dataset": "medium", "conn-client": True}} for v in FAULT_VARIANTS],
    # realism: an external outbound connection (pyapp-out + upstream service on the destination host).
    "outbound": [{"variant": v, "workload": "pyapp-out", "extra": {"dataset": "small", "outbound": True}}
                 for v in ("vanilla", "m4-merge", "m5")],
    # CPU heterogeneity: the VAES path chosen on the richer source CPU (expected: dies on the
    # destination, a documented hazard) against the generic path (expected: migrates). One variant.
    "cpu": [{"variant": "vanilla", "workload": "synth-cpu", "label": f"cpu-{p}",
             "extra": {"wl-param": [f"cpu-path={p}"]}} for p in ("auto", "generic")],
    # A small smoke preset to check the plumbing end to end.
    "smoke": [
        {"variant": "m4", "workload": "redis", "extra": {"dataset": "small"}},
        {"variant": "m5", "workload": "redis", "extra": {"dataset": "small"}},
    ],
}

# --- Lab testbed presets . The presets above are the real-WAN
# campaign's and stay as they were. No small-destination limits here: JVM gets M4, PostgreSQL gets stock
# lazy-pages. restart-volume only where the workload keeps its state on disk (for in-memory
# state a restart loses it by definition). Profiles come from the stage (--profiles).
LAB_CORE = ("vanilla-zstd-fs", "criu-lazy", "m4", "m5")
LAB_DISK_STATE = ("redis-aof", "postgres")
PRESETS.update({
    # RQ2 on real applications + RQ3 ASLR cost: every workload x the consistent variants
    "lab-workloads": (
        [c for wl in ("redis-aof", "postgres", "pyapp", "jvm")
         for c in wl_cells(wl, LAB_CORE + ("m4-aslron", "m5-aslron")
                           + (("restart-volume",) if wl in LAB_DISK_STATE else ()), extra={"dataset": "medium"})]
        + synth_cells("size64mb", [("resident-mb", 64)], LAB_CORE, base={"shared-frac": "0.5"})),
    # network sensitivity: one in-memory store and the best-case runtime, across profiles
    "lab-net": (wl_cells("redis", ("vanilla-zstd", "criu-incr-zstd", "criu-lazy", "m4", "m5", "restart-persist"),
                         extra={"dataset": "medium"})
                + wl_cells("pyapp", ("vanilla-zstd-fs", "m4", "m5"), extra={"dataset": "medium"})),
    # state size, TelePod-style, up to GB scale (run at a fast profile: 1 GB at 2 Mb/s is over an hour)
    "lab-size": (synth_cells("size{mb}mb", [("resident-mb", mb) for mb in (16, 64, 256, 1024)],
                             ("vanilla-zstd", "m4", "m5", "criu-lazy"), base={"shared-frac": "0.5"})
                 + [c for ds in ("small", "medium", "large", "xlarge")
                    for c in wl_cells("redis", ("vanilla-zstd", "m4", "m5"), label=ds, extra={"dataset": ds})]),
    # concurrent migration of N replicas sharing one link and one synthesized baseline (§1.7)
    "lab-multi": [{"variant": v, "workload": "redis", "label": f"multi{n}", "extra": {"dataset": "medium", "multi": n}}
                  for n in (1, 2, 4, 8) for v in ("vanilla-zstd", "m4", "m5")],
})
# Emulated-WAN profiles for the lab testbed . `delay` = added RTT
# (run_experiment.host_tap_plan). P1 matches the real Internet path the emulation is
# validated against (2 Mb/s cap, median RTT 201 ms).
PROFILES = {
    "P1": "rate 2mbit delay 200ms",            # constrained uplink to a distant cloud region (validated)
    "P2": "rate 10mbit delay 100ms",           # better uplink, inter-continental RTT
    "P3": "rate 50mbit delay 50ms",            # enterprise uplink, regional cloud
    "P4": "rate 200mbit delay 20ms",           # datacentre interconnect / same-region
    "P2L": "rate 10mbit delay 100ms loss 0.1%",  # P2 with random loss
}
LOSTWRITE_VARIANTS_EXEMPT = {"restart", "restart-persist"}  # no live state moves: writes after the SAVE are lost by design (restart-persist reports it via its own check)


def build_cells(args):
    if args.cells:
        cells = json.loads(Path(args.cells).read_text())
    else:
        cells = []
        for spec in PRESETS[args.preset]:
            flags = dict(VARIANTS[spec["variant"]])
            flags.update(spec.get("extra", {}))
            if (spec["workload"] in ("redis", "redis-aof") and args.lost_write
                    and spec["variant"] not in LOSTWRITE_VARIANTS_EXEMPT):
                flags.setdefault("lost-write", True)
            if "label" in spec:
                cid = f"{spec['variant']}-{spec['workload']}-{spec['label']}"
            else:
                suffix = "".join(f"-{k}{v}" for k, v in sorted(spec.get("extra", {}).items())
                                 if k != "dataset")
                cid = f"{spec['variant']}-{spec['workload']}-{flags.get('dataset', 'medium')}{suffix}"
            if args.netem:
                flags["netem"] = args.netem
                cid += "-capped"
            cells.append({"id": cid, "workload": spec["workload"], "flags": flags})
    if args.profiles:
        out = []
        for name in args.profiles.split(","):
            name = name.strip()
            if name not in PROFILES:
                raise SystemExit(f"unknown profile {name!r} (known: {', '.join(PROFILES)})")
            for c in cells:
                flags = dict(c["flags"])
                flags["netem"] = PROFILES[name]
                out.append({**c, "id": f"{c['id']}-{name}", "flags": flags})
        cells = out
    if args.only:   # after the profile suffix, so ids match --list (e.g. m4-redis-multi2-P2)
        cells = [c for c in cells if any(o in c["id"] for o in args.only.split(","))]
    if args.caps:
        capped = []
        for cap in args.caps.split(","):
            cap = cap.strip()
            for c in cells:
                if cap in ("none", "uncapped"):
                    capped.append({**c, "id": c["id"] + "-uncapped", "flags": dict(c["flags"])})
                    continue
                mbps = float(cap)  # validates the token
                flags = dict(c["flags"])
                flags["netem"] = f"rate {mbps:g}mbit"
                capped.append({**c, "id": f"{c['id']}-cap{mbps:g}", "flags": flags})
        cells = capped
    return cells


def cell_command(cell, args, round_no, attempt):
    name = f"{args.campaign}__{cell['id']}__r{round_no:02d}a{attempt}"
    cmd = rm.cell_cmd({"id": name, "workload": cell["workload"], "flags": cell["flags"]},
                      args.config, 1)
    cmd += ["--iperf-duration", str(args.iperf_duration), "--dest-min-avail-mb", str(args.dest_min_avail_mb),
            "--meta", f"campaign={args.campaign}", "--meta", f"cell={cell['id']}",
            "--meta", f"round={round_no}", "--meta", f"attempt={attempt}"]
    return name, cmd


def cell_footprint_mb(cell):
    """Measured PSS (MB) of the cell's workload at its dataset (footprint.py), or None if never measured."""
    w, flags = cell["workload"], cell["flags"]
    w = {"pyapp-out": "pyapp", "synth-cpu": "synth"}.get(w, w)   # same program, one extra thread / knob
    if w == "synth":  # resident-mb is the knob; the program itself adds a few MB
        for p in flags.get("wl-param") or []:
            if p.startswith("resident-mb="):
                return float(p.split("=")[1]) + 5
        return 64.0 + 5
    ds = flags.get("dataset", "medium")
    table = fp.load()
    for key in (f"{w}:{ds}", f"redis:{ds}" if w == "redis-aof" else None):
        if key and key in table:
            return table[key]["pss_mb"]
    return None


def over_budget(cell, args):
    """(True, why) if this cell would not fit the destination's memory budget. Unmeasured = refused."""
    if args.no_footprint_check:
        return False, ""
    pss = cell_footprint_mb(cell)
    if pss is None:
        return True, "no measured footprint (run footprint.py for this workload:dataset)"
    est = fp.est_dest_peak_mb(pss)
    if est > args.dest_budget_mb:
        return True, f"estimated destination peak {est:.0f} MB > budget {args.dest_budget_mb} MB (workload {pss:.0f} MB)"
    return False, ""


def destination_ok(args):
    """Circuit-breaker probe: the destination answers ssh and reports its memory. Returns (ok, detail).
    A small destination VM without swap can wedge when a run overshoots; a campaign that keeps
    retrying against it burns hours."""
    try:
        cfg = rx.load_config(args.config)
        key = cfg.get("ssh_key", "")
        if key and not any(o == str(Path(key).expanduser()) for o in rx.SSH_OPTS):
            rx.SSH_OPTS.extend(["-i", str(Path(key).expanduser())])
        return True, rx.dest_mem_state(cfg)
    except Exception as e:  # unreachable, banner timeout, unreadable
        return False, {"error": str(e)[:200]}


def find_run_dir(name):
    dirs = sorted(RESULTS.glob(f"*_{name}"))
    return dirs[-1] if dirs else None


def probe_summary(text):
    """What clients saw, from statepod_probe.py's JSON lines: per endpoint the
    answered/unanswered counts and the last/first successful probe."""
    eps = {}
    for line in text.splitlines():
        try:
            r = json.loads(line)
        except ValueError:
            continue
        e = eps.setdefault(r.get("ep"), {"ok": 0, "failed": 0, "first_ok_t": None, "last_ok_t": None})
        if r.get("ok"):
            e["ok"] += 1
            e["first_ok_t"] = e["first_ok_t"] or r.get("t0")
            e["last_ok_t"] = r.get("t0")
        else:
            e["failed"] += 1
    return eps


def salvage_timeout(cfg, name):
    """A run killed by the per-run timeout never reaches run_experiment's harvest
    and cleanup. Keep its evidence (what the clients saw, the stuck state on both
    hosts) in its results dir, then clear what it left running so the next run
    starts clean: probes/samplers/migrate on the source, a stopped lazy-pages
    daemon and the containers on both. Returns ledger fields."""
    src, dst = cfg["source"]["ssh"], cfg["destination"]["ssh"]
    run_dir = find_run_dir(name)
    if run_dir is None:
        run_dir = RESULTS / f"{datetime.datetime.now():%Y-%m-%d_%H%M%S}_{name}"
        run_dir.mkdir(parents=True)
    out = {"run_dir": run_dir.name}
    for remote, local in (("/tmp/statepod_probe.log", "probe.log"), ("/tmp/statepod_lostwrite.log", "lostwrite.log")):
        try:
            rx.scp_from(src, remote, run_dir / local)
        except Exception as e:
            out[f"salvage_{local}"] = f"not copied: {str(e)[:120]}"
    if (run_dir / "probe.log").exists():
        out["probe_at_timeout"] = probe_summary((run_dir / "probe.log").read_text(errors="replace"))
    state = []
    for label, target in (("source", src), ("destination", dst)):
        state.append(f"== {label}: runc list")
        state.append(rx.try_out(target, "sudo runc list"))
        state.append(f"== {label}: criu/statepod processes (STAT T = stopped)")
        state.append(rx.try_out(target, "ps -eo pid,stat,etime,wchan:24,cmd | grep -E '[c]riu|[s]tatepod' | cut -c1-200"))
    (run_dir / "timeout_state.txt").write_text("\n".join(state) + "\n")
    # Bracketed patterns: they do not match the ssh command line that carries them.
    rx.try_out(src, "sudo pkill -f '[s]tatepod_probe.py|[s]tatepod_lostwrite.py|[s]tatepod_source_sampler.py|"
                    "[s]tatepod migrate'; for c in $(sudo runc list -q); do sudo runc delete -f $c; done")
    rx.try_out(dst, "sudo pkill -CONT -f '[c]riu lazy-pages'; sudo pkill -KILL -f '[c]riu lazy-pages'; "
                    "for c in $(sudo runc list -q); do sudo runc delete -f $c; done")
    return out


def evaluate(run_dir, args):
    """Apply the validity gate to one finished run. Returns (status, detail)."""
    try:
        m = json.loads((run_dir / "manifest.json").read_text())
    except Exception as e:
        return "failed", {"reason": f"manifest unreadable: {e}"}
    sv = m.get("state_verify") or {}
    if m.get("oom_on_destination"):
        return "failed", {"reason": "oom_on_destination", "dest_mem": m.get("dest_mem")}
    fault = sv.get("fault")
    if fault and fault.get("outcome"):
        # the outcome (completed / aborted_source_resumed / failed_unsafe / split_brain) IS the
        # measurement; re-running an unsafe outcome would be selecting by result. No bandwidth gate either:
        # the fault, not the link, is the variable.
        return "valid", {"fault_outcome": fault["outcome"], "fault": fault.get("spec")}
    if not sv.get("ok"):
        return "failed", {"reason": "state_verify not ok", "error": m.get("error")}
    lp = m.get("link_probe") or {}
    pre, post = (lp.get("pre") or {}), (lp.get("post") or {})
    got = [pre.get("recv_mbps"), post.get("recv_mbps")]
    detail = {"cap_mbps": m.get("bandwidth_cap_mbps"), "probe_recv_mbps": got}
    cap = m.get("bandwidth_cap_mbps")
    if cap:
        need = args.gate_frac * cap
        if None in got:
            return ("invalid_unmeasured" if not args.allow_unmeasured else "valid"), detail
        ok = all(g >= need for g in got)
        detail["required_mbps"] = round(need, 3)
        return ("valid" if ok else "invalid_bandwidth"), detail
    if args.min_mbps:
        if None in got:
            return ("invalid_unmeasured" if not args.allow_unmeasured else "valid"), detail
        detail["required_mbps"] = args.min_mbps
        return ("valid" if all(g >= args.min_mbps for g in got) else "invalid_bandwidth"), detail
    return "valid", detail


def load_ledger(path):
    rows = []
    if path.exists():
        for line in path.read_text().splitlines():
            if line.strip():
                rows.append(json.loads(line))
    return rows


def summarize(rows):
    per = {}
    for r in rows:
        d = per.setdefault(r["cell"], {"attempted": 0, "valid": 0, "invalid_bandwidth": 0,
                                       "invalid_unmeasured": 0, "failed": 0, "rounds_done": 0})
        if r["status"] == "skipped_budget":  # never run: not an attempt in the run-count ledger
            d["skipped_budget"] = d.get("skipped_budget", 0) + 1
            continue
        d["attempted"] += 1
        d[r["status"]] = d.get(r["status"], 0) + 1
    for c, d in per.items():
        d["rounds_done"] = len({r["round"] for r in rows if r["cell"] == c and r["status"] == "valid"})
    tot = {k: sum(d[k] for d in per.values()) for k in
           ("attempted", "valid", "invalid_bandwidth", "invalid_unmeasured", "failed")}
    tot["skipped_budget"] = sum(d.get("skipped_budget", 0) for d in per.values())
    return {"total": tot, "per_cell": per}


def cap_of(cell):
    """The cell's netem rate cap in Mb/s, or None."""
    m = re.search(r"rate\s+([0-9.]+)\s*mbit", cell["flags"].get("netem") or "")
    return float(m.group(1)) if m else None


def expected_duration_s(rows, cell_id):
    """Median duration of this cell's finished attempts, else the p90 over all, else 600 s."""
    mine = [r["duration_s"] for r in rows if r["cell"] == cell_id and r.get("duration_s")]
    if mine:
        return statistics.median(mine)
    allv = sorted(r["duration_s"] for r in rows if r.get("duration_s"))
    return allv[int(0.9 * (len(allv) - 1))] if allv else 600.0


def release_link(cfg):
    """Remove any netem cap and migrate tunnel a killed run left behind (best effort)."""
    for fn in (rx.clear_netem, rx.stop_migrate_tunnel):
        try:
            fn(cfg)
        except Exception as e:
            print(f"    (release_link: {fn.__name__} failed: {e})", flush=True)


def link_ready(args, cap):
    """Wait until the link delivers the cap now. Returns (ok, last_mbps)."""
    cfg = rx.load_config(args.config)
    key = cfg.get("ssh_key", "")
    if key and not any(o == str(Path(key).expanduser()) for o in rx.SSH_OPTS):
        rx.SSH_OPTS.extend(["-i", str(Path(key).expanduser())])
    # An UNCAPPED probe, no netem active yet: iperf3 `-b <cap>` pacing under-reads badly at low rates on a
    # ~200 ms path (cap 2 offered -> 0.55-0.83 Mb/s received, measured 2026-10-08), while inside a run the
    # netem-shaped probe reads 1.93 for a 2 Mb/s cap. So ask whether the link has HEADROOM over the cap.
    need = args.link_headroom * cap
    # The pre-check must see the raw link. A run killed by the per-run timeout never reaches
    # run_experiment's `finally: clear_netem`, and its leftover cap reads as "link too poor"
    # (2026-10-20: 3.5 h lost to a stale `rate 2Mbit`). Clear it before measuring.
    release_link(cfg)
    waited, step, last = 0.0, 30.0, None
    while True:
        try:
            pr = rx.iperf_probe(cfg, "pre-check", cap_mbps=None, duration=3)
            last = pr.get("recv_mbps") if isinstance(pr, dict) else None
        except Exception:
            last = None
        if last is not None and last >= need:
            return True, last
        if waited >= args.link_wait_min * 60:
            return False, last
        print(f"    link pre-check {last} Mb/s < {need:.2f}: waiting {step:.0f} s", flush=True)
        time.sleep(step)
        waited += step
        step = min(step * 2, 300.0)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="hosts.yaml")
    ap.add_argument("--campaign", default=datetime.datetime.now().strftime("c%Y%m%d"))
    ap.add_argument("--preset", default="ladder", choices=sorted(PRESETS))
    ap.add_argument("--cells", help="JSON file with an explicit cell list (overrides --preset)")
    ap.add_argument("--only", help="substring filter on cell id (comma-separated = any of)")
    ap.add_argument("--caps", help='comma-separated bandwidth caps in Mb/s, e.g. "30,19,9.5,5" ("none" = '
                                   'uncapped): every cell is replicated per cap with netem "rate Xmbit" '
                                   '(cell ids get -cap<X>); the gate then requires 0.9 x X from iperf3')
    ap.add_argument("--rounds", type=int, default=10)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--max-attempts", type=int, default=3)
    ap.add_argument("--max-consecutive-failures", type=int, default=2,
                    help="stop retrying a cell-round after this many `failed` attempts in a row (state_verify, "
                         "timeout, OOM); gate discards (invalid_bandwidth) still use --max-attempts. A "
                         "deterministic failure is not worth three tries (2026-10-06: 6 h wasted)")
    ap.add_argument("--dest-budget-mb", type=int, default=fp.DEST_BUDGET_MB,
                    help="skip cells whose estimated destination peak (footprint.py measurements) exceeds this")
    ap.add_argument("--no-footprint-check", action="store_true", default=False,
                    help="run cells without a measured footprint / over budget (only on a bigger destination)")
    ap.add_argument("--dest-min-avail-mb", type=int, default=450,
                    help="passed to every run: refuse to start when the destination has less MemAvailable (MB)")
    ap.add_argument("--gate-frac", type=float, default=0.9)
    ap.add_argument("--min-mbps", type=float, default=None,
                    help="uncapped runs: both bracketing iperf3 probes must reach this")
    ap.add_argument("--profiles", default=None,
                    help="comma-separated emulated-WAN profiles (lab testbed), each cell replicated per profile: "
                         + ", ".join(f"{k}={v!r}" for k, v in PROFILES.items()).replace("%", "%%"))
    ap.add_argument("--netem", default=None, help='cap every cell, e.g. "rate 2mbit delay 20ms" (below the link floor!)')
    ap.add_argument("--iperf-duration", type=int, default=3)
    ap.add_argument("--allow-unmeasured", action="store_true",
                    help="treat runs whose probe failed/was unavailable as valid (tunneled profiles)")
    ap.add_argument("--no-lost-write", dest="lost_write", action="store_false", default=True,
                    help="do not add the lost-write client to redis cells")
    ap.add_argument("--timeout", type=int, default=1800, help="seconds per run before it counts as failed")
    ap.add_argument("--deadline", default=None,
                    help="ISO local time (YYYY-MM-DDTHH:MM): do not START an attempt that is expected to end after "
                         "it (expected = median ledger duration of that cell, else the p90 of all, else 600 s). "
                         "Exit code 10 = window closed, resume later with the same --campaign")
    ap.add_argument("--link-wait-min", type=float, default=0.0,
                    help="before each capped attempt, run an uncapped 3 s probe and wait (30 s, 60 s, 120 s, "
                         "...) until it reads >= --link-headroom x cap; give up after this many minutes "
                         "(exit code 11 = link too poor now). 0 = off. A discarded run costs a whole run; "
                         "the check costs seconds")
    ap.add_argument("--link-headroom", type=float, default=1.5,
                    help="--link-wait-min waits until an uncapped probe reads >= this x the cap")
    ap.add_argument("--network-label", default="", help="recorded in every ledger row (home / uni / ...)")
    ap.add_argument("--egress-ip", default="", help="recorded in every ledger row")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--summary", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    # experiment.dest_budget_mb: the destination memory budget of the profile;
    # the CLI flag still wins when given explicitly
    _pcfg = rx.load_config(args.config)
    if "--dest-budget-mb" not in sys.argv and _pcfg.get("experiment", {}).get("dest_budget_mb"):
        args.dest_budget_mb = int(_pcfg["experiment"]["dest_budget_mb"])

    cells = build_cells(args)
    out_dir = RESULTS / f"_campaign_{args.campaign}"
    ledger_path = out_dir / "ledger.jsonl"

    if args.list:
        for c in cells:
            print(f"{c['id']:44s} {c['workload']:6s} {json.dumps(c['flags'])}")
        print(f"\n{len(cells)} cells x {args.rounds} rounds = {len(cells) * args.rounds} valid runs needed")
        return 0
    if args.summary:
        print(json.dumps(summarize(load_ledger(ledger_path)), indent=2))
        return 0

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "campaign.json").write_text(json.dumps(
        {"campaign": args.campaign, "seed": args.seed, "rounds": args.rounds, "cells": cells,
         "gate_frac": args.gate_frac, "min_mbps": args.min_mbps, "netem": args.netem,
         "dropped_on_testbed": TESTBED_DROPPED if args.preset == "workloads" else [],
         "max_attempts": args.max_attempts, "started": datetime.datetime.now().isoformat()}, indent=2))
    rows = load_ledger(ledger_path)

    for rnd in range(1, args.rounds + 1):
        order = list(cells)
        random.Random(f"{args.seed}-{rnd}").shuffle(order)
        print(f"\n=== round {rnd}/{args.rounds}: " + ", ".join(c['id'] for c in order))
        for cell in order:
            if any(r["cell"] == cell["id"] and r["round"] == rnd and r["status"] == "valid" for r in rows):
                continue
            skip, why = over_budget(cell, args)
            if skip:
                print(f"  [{cell['id']} r{rnd}] SKIPPED: {why}", flush=True)
                if not args.dry_run and not any(r["cell"] == cell["id"] and r["round"] == rnd
                                                and r["status"] == "skipped_budget" for r in rows):
                    row = {"cell": cell["id"], "round": rnd, "attempt": 0, "status": "skipped_budget",
                           "started": datetime.datetime.now().isoformat(timespec="seconds"), "reason": why}
                    rows.append(row)
                    with open(ledger_path, "a") as f:
                        f.write(json.dumps(row) + "\n")
                continue
            consec_failed = 0
            for attempt in range(1, args.max_attempts + 1):
                if not args.dry_run:
                    ok, info = destination_ok(args)
                    if not ok:
                        print(f"\n!!! ABORT: destination not reachable ({info}). Reboot it, then re-run the same "
                              f"command with the same --campaign to resume.", flush=True)
                        return 3
                if not args.dry_run and args.deadline:
                    end = datetime.datetime.now() + datetime.timedelta(seconds=expected_duration_s(rows, cell["id"]))
                    if end > datetime.datetime.fromisoformat(args.deadline):
                        print(f"\n--- window closes at {args.deadline}; next attempt ({cell['id']} r{rnd}) is expected "
                              f"to end {end:%H:%M}. Resume with the same --campaign.", flush=True)
                        (out_dir / "summary.json").write_text(json.dumps(summarize(rows), indent=2))
                        return 10
                cap = cap_of(cell)
                if not args.dry_run and args.link_wait_min and cap:
                    ok_link, got = link_ready(args, cap)
                    if not ok_link:
                        print(f"\n--- link too poor for the {cap:g} Mb/s cap ({got} Mb/s) after "
                              f"{args.link_wait_min:g} min; pausing (exit 11). Resume later.", flush=True)
                        (out_dir / "summary.json").write_text(json.dumps(summarize(rows), indent=2))
                        return 11
                name, cmd = cell_command(cell, args, rnd, attempt)
                print(f"  [{cell['id']} r{rnd} a{attempt}] {' '.join(cmd[2:8])} ...")
                if args.dry_run:
                    print("   ", " ".join(cmd))
                    break
                t0 = datetime.datetime.now()
                status, detail = "failed", {}
                rc = None
                try:
                    # kept per attempt: a run that dies before the migration leaves no other trace
                    (out_dir / "runner_logs").mkdir(exist_ok=True)
                    with open(out_dir / "runner_logs" / f"{name}.log", "w") as runner_log:
                        rc = subprocess.run(cmd, cwd=RUNNER_DIR, timeout=args.timeout,
                                            stdout=runner_log, stderr=subprocess.STDOUT).returncode
                    run_dir = find_run_dir(name)
                    if run_dir is None:
                        detail = {"reason": f"no results dir (rc={rc})"}
                    else:
                        status, detail = evaluate(run_dir, args)
                        detail["run_dir"] = run_dir.name
                except subprocess.TimeoutExpired:
                    detail = {"reason": f"timeout after {args.timeout}s"}
                    try:
                        detail.update(salvage_timeout(rx.load_config(args.config), name))
                    except Exception as e:  # salvage must never stop the campaign
                        detail["salvage_error"] = str(e)[:200]
                finally:
                    # always leave the link uncapped, whatever killed the run
                    release_link(rx.load_config(args.config))
                if rc == 75:
                    detail = {"reason": "memory_guard: destination short of memory, run not started"}
                row = {"cell": cell["id"], "round": rnd, "attempt": attempt, "status": status,
                       "started": t0.isoformat(timespec="seconds"),
                       "duration_s": round((datetime.datetime.now() - t0).total_seconds()),
                       "network": args.network_label, "egress_ip": args.egress_ip, **detail}
                rows.append(row)
                with open(ledger_path, "a") as f:
                    f.write(json.dumps(row) + "\n")
                print(f"    -> {status} {json.dumps(detail)[:200]}")
                if status == "valid":
                    break
                if rc == 75 or detail.get("reason") == "oom_on_destination":
                    print("\n!!! ABORT: destination memory problem (guard refused or kernel OOM). Not retrying; "
                          "check `sudo dmesg | grep -i oom` on the destination.", flush=True)
                    (out_dir / "summary.json").write_text(json.dumps(summarize(rows), indent=2))
                    return 4
                consec_failed = consec_failed + 1 if status == "failed" else 0
                if consec_failed >= args.max_consecutive_failures:
                    print(f"    (stopping this cell-round after {consec_failed} failures in a row)", flush=True)
                    break
        (out_dir / "summary.json").write_text(json.dumps(summarize(rows), indent=2))

    print(json.dumps(summarize(rows)["total"], indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
