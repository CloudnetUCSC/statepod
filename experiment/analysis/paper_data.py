"""Everything the paper's Methodology/Evaluation show, generated from the final dataset.

Reads results/_final/runs.csv (final_dataset.py) and the fault/ladder ledgers; writes, into
docs/paper/generated/ (the docs repo, next to this repo):

  fig_*.csv   data series that paper.tex plots with pgfplots (\\addplot table)
  tab_*.tex   tables that paper.tex \\input's
  facts.md    every number the prose quotes, with the cell it comes from

Nothing in the Evaluation section is typed by hand: a number in the text must appear in
facts.md or in a generated table. Error bars are one sample SD; ratios are ratio of means
with a 95 % bootstrap CI (stats.compare, both groups resampled), as in final_report.py.

    uv run python paper_data.py [--out ../../../docs/paper/generated]
"""
import argparse
import csv
import statistics
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import final_dataset as fd  # noqa: E402
import final_report as fr  # noqa: E402
import stats  # noqa: E402

# Workload display names and their reference baseline cell prefix.
WL = {  # scenario -> label
    "redis-medium-P2": "Redis", "redis-aof-medium-P2": "Redis-AOF", "postgres-medium-P2": "PostgreSQL",
    "jvm-medium-P2": "JVM", "pyapp-medium-P2": "Python", "synth-size64mb-P2": "Synthetic",
}
PROFILES = [("P1", 2), ("P2", 10), ("P3", 50), ("P4", 200)]  # profile, cap (Mb/s)

CELLS, FACTS = {}, []


def load():
    for r in fr.load():
        CELLS.setdefault(r["cell"], []).append(r)


def col(cell, k):
    return fr.col(CELLS.get(cell, []), k)


def mean(cell, k):
    xs = col(cell, k)
    return statistics.mean(xs) if xs else None


def sd(cell, k):
    xs = col(cell, k)
    return stats.sample_sd(xs) if xs else None


def ref_of(scen):
    for c in (f"vanilla-zstd-{scen}", f"vanilla-zstd-fs-{scen}"):
        if c in CELLS:
            return c
    return None


def ratio(cell, ref, k):
    a, b = col(cell, k), col(ref, k)
    if len(a) < 2 or len(b) < 2:
        return None
    c = stats.compare(a, b)
    return c["ratio_of_means"], c["ci_ratio"][0], c["ci_ratio"][1]


def fact(key, value, src, digits=2):
    if value is None:
        s = "n/a"
    elif isinstance(value, str):
        s = value
    elif isinstance(value, tuple):
        s = f"{value[0]:.{digits}f} [{value[1]:.{digits}f}, {value[2]:.{digits}f}]"
    else:
        s = f"{value:.{digits}f}"
    FACTS.append((key, s, src))
    return value


def f2(x, d=2):
    return "--" if x is None else f"{x:.{d}f}"


def write_csv(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow(["nan" if v is None else (f"{v:.4g}" if isinstance(v, float) else v) for v in r])


# ---------------------------------------------------------------- run ledger

def ledger_counts():
    """Attempts behind the dataset: every ledger row of a merged campaign whose cell is in the dataset."""
    in_ds = set(CELLS)
    att = valid = gate = failed = before = 0
    after = []
    for camp in fd.OLD + fd.NEW:
        for r in fd.ledger(camp):
            if r["cell"] not in in_ds or not fd.allowed(r["cell"], camp):
                continue
            att += 1
            st = r.get("status")
            valid += st == "valid"
            gate += st == "invalid_bandwidth"
            if st == "failed":
                failed += 1
                # no receiver log = the migration never started (workload start-up, seeding, synthesis)
                if r.get("run_dir") and (fd.RESULTS / r["run_dir"] / "destination_receiver.log").exists():
                    after.append(f"{r['cell']} ({r.get('reason')})")
                elif str(r.get("reason", "")).startswith("timeout"):
                    after.append(f"{r['cell']} ({r.get('reason')})")
                else:
                    before += 1
    n = sum(len(v) for v in CELLS.values())
    fact("dataset.valid_runs", f"{n}", "runs.csv")
    fact("dataset.cells", f"{len(CELLS)}", "runs.csv")
    fact("ledger.attempts", f"{att}", "ledgers of merged campaigns, dataset cells")
    fact("ledger.valid", f"{valid}", "")
    fact("ledger.rejected_bandwidth_gate", f"{gate}", "")
    fact("ledger.failed", f"{failed}", "")
    fact("ledger.failed_before_migration", f"{before}", "no destination_receiver.log")
    fact("ledger.failed_during_or_after", "; ".join(after), "ledger reason")


# ---------------------------------------------------------------- RQ1

def hitrate(out):
    rows = []
    for scen, label in WL.items():
        if scen == "synth-size64mb-P2":
            continue
        off = mean(f"m4-{scen}", "hit_rate")
        on = mean(f"m4-aslron-{scen}", "hit_rate")
        rows.append([label, off * 100 if off is not None else None, on * 100 if on is not None else None])
        fact(f"hit.{label}.aslr_off", off, f"m4-{scen}", 3)
        fact(f"hit.{label}.aslr_on", on, f"m4-aslron-{scen}", 3)
        fact(f"hit.{label}.aslr_off_pct", off * 100 if off is not None else None, f"m4-{scen}", 1)
        fact(f"hit.{label}.aslr_on_pct", on * 100 if on is not None else None, f"m4-aslron-{scen}", 1)
    write_csv(out / "fig_hitrate.csv", ["wl", "off", "on"], rows)
    for ds in ("small", "medium", "large", "xlarge"):
        c = f"m4-redis-{ds}-P3"
        fact(f"hit.redis.{ds}.P3", mean(c, "hit_rate"), c, 3)
        fact(f"hit.redis.{ds}.P3_pct", mean(c, "hit_rate") * 100, c, 1)
        fact(f"data.redis.{ds}.P3.m4", mean(c, "bytes_mb"), c, 3)
        fact(f"data.redis.{ds}.P3.vzstd", mean(f"vanilla-zstd-redis-{ds}-P3", "bytes_mb"), f"vanilla-zstd-redis-{ds}-P3", 3)
        fact(f"ratio.data.redis.{ds}.P3.m4", ratio(c, f"vanilla-zstd-redis-{ds}-P3", "bytes_mb"), c, 3)
    # ASLR-on cost in bytes
    for scen, label in WL.items():
        r = ratio(f"m4-aslron-{scen}", f"m4-{scen}", "bytes_mb")
        if r:
            fact(f"ratio.data.{label}.aslron_vs_m4", r, f"m4-aslron-{scen} / m4-{scen}")


def entropy_table(out):
    lines = [r"\begin{tabular}{llrrrr}", r"\toprule",
             r"Content of the & Shared & Vanilla & Vanilla & M1 & M1--M4 \\",
             r"other pages & fraction & & +zstd & (no zstd) & \\", r"\midrule"]
    names = {"random": "random", "repeat": "repeating", "zero": "zero"}
    for ent in ("random", "repeat", "zero"):
        for sf in ("0.0", "0.5", "1.0"):
            scen = f"synth-{ent}-shared{sf}-P2"
            if f"vanilla-zstd-{scen}" not in CELLS:
                continue
            v = [mean(f"{p}-{scen}", "bytes_mb") for p in ("vanilla", "vanilla-zstd", "m1", "m4-merge")]
            h = mean(f"m1-{scen}", "hit_rate")
            lines.append(f"{names[ent] if sf == '0.0' else ''} & {sf} & " + " & ".join(f2(x, 1) for x in v) + r" \\")
            fact(f"entropy.{ent}.{sf}.hit", h, f"m1-{scen}", 3)
            for p, x in zip(("vanilla", "vzstd", "m1", "m4"), v):
                fact(f"entropy.{ent}.{sf}.{p}_mb", x, scen, 3)
    lines += [r"\bottomrule", r"\end{tabular}"]
    (out / "tab_entropy.tex").write_text("\n".join(lines) + "\n", encoding="utf-8")


# ---------------------------------------------------------------- RQ2: Redis ladder at P1

LADDER = [  # variant, label, group
    ("vanilla", "Vanilla pre-copy", "b"), ("criu-incr", "CRIU incremental", "b"),
    ("vanilla-zstd", "Vanilla + zstd", "b"), ("criu-incr-zstd", "CRIU incr.\\ + zstd", "b"),
    ("restart-persist", "Restart from RDB", "b"),
    ("m1", "+M1 (1 round, no zstd)", "s"), ("m2", "+M2 (4 rounds)", "s"), ("compression", "+zstd", "s"),
    ("m3", "+M3 (tmpfs)", "s"), ("m4", "+M4 = \\spod{} pre-copy", "s"),
    ("m4-merge", "\\quad with merged cut-over$^\\dagger$", "s"),
    ("criu-lazy", "CRIU lazy-pages", "p"), ("m5", "\\spod{} post-copy (M5)", "p"),
]


def ladder_table(out):
    scen = "redis-medium-P1"
    ref = f"vanilla-zstd-{scen}"
    hdr = [r"\begin{tabular}{lrrrrr}", r"\toprule",
           r"Method & Data & Down- & Client & Evic- & Source \\",
           r" & (MB) & time (s) & outage (s) & tion (s) & CPU (s) \\", r"\midrule"]
    lines = list(hdr)
    groups = {"b": r"\multicolumn{6}{l}{\emph{Baselines}} \\", "s": r"\multicolumn{6}{l}{\emph{Pre-copy with the synthesized baseline, mechanisms added in order}} \\",
              "p": r"\multicolumn{6}{l}{\emph{Post-copy}} \\"}
    last = None
    for v, label, g in LADDER:
        c = f"{v}-{scen}"
        if c not in CELLS:
            continue
        if g != last:
            if last is not None:
                lines.append(r"\midrule")
            lines.append(groups[g])
            last = g
        vals = [mean(c, k) for k in ("bytes_mb", "downtime_s", "outage_s", "evict_s", "src_cpu_s")]
        sds = [sd(c, k) for k in ("bytes_mb", "downtime_s", "outage_s", "evict_s", "src_cpu_s")]
        cells = [f2(vals[0], 3)] + [f2(x) for x in vals[1:]]
        lines.append(f"{label} & " + " & ".join(cells) + r" \\")
        for k, x, s in zip(("data", "downtime", "outage", "evict", "cpu"), vals, sds):
            fact(f"ladder.{v}.{k}", x, c, 3 if k == "data" else 2)
            fact(f"ladder.{v}.{k}.sd", s, c, 3 if k == "data" else 2)
        fact(f"ladder.{v}.n", f"{len(CELLS[c])}", c)
        if v != "vanilla-zstd":
            fact(f"ladder.{v}.ratio.downtime", ratio(c, ref, "downtime_s"), c)
            fact(f"ladder.{v}.ratio.data", ratio(c, ref, "bytes_mb"), c, 3)
            fact(f"ladder.{v}.ratio.evict", ratio(c, ref, "evict_s"), c)
    lines += [r"\bottomrule", r"\end{tabular}"]
    (out / "tab_ladder.tex").write_text("\n".join(lines) + "\n", encoding="utf-8")
    a = CELLS.get(f"m4-availability-{scen}", [])
    fact("ladder.availability.downtime", mean(f"m4-availability-{scen}", "downtime_s"), "m4-availability")
    fact("ladder.availability.lost", f"{int(sum(fr.col(a, 'lost')))} / {int(sum(fr.col(a, 'acked')))}", "m4-availability")
    lost = {v: int(sum(col(f"{v}-{scen}", "lost"))) for v, _, _ in LADDER if f"{v}-{scen}" in CELLS}
    acked = sum(int(sum(col(f"{v}-{scen}", "acked"))) for v, _, _ in LADDER if f"{v}-{scen}" in CELLS)
    fact("ladder.lost_writes_all_consistent", f"{sum(lost.values())} of {acked} acked", "every ladder cell except availability")


# ---------------------------------------------------------------- RQ2: workloads at P2 (ratios)

WL_VARIANTS = [("m4", "pre"), ("m4-aslron", "preaslr"), ("m5", "post"), ("criu-lazy", "lazy"),
               ("restart-volume", "restart")]


def workload_ratios(out):
    """One CSV per variant: x position (workload index + offset), ratio and asymmetric CI error,
    for data, downtime and eviction against vanilla+zstd(-fs) in the same scenario."""
    offs = {"pre": -0.24, "preaslr": -0.12, "post": 0.0, "lazy": 0.12, "restart": 0.24}
    for v, tag in WL_VARIANTS:
        rows = []
        for i, (scen, label) in enumerate(WL.items()):
            c, ref = f"{v}-{scen}", ref_of(scen)
            if c not in CELLS or not ref:
                continue
            row = [i + offs[tag], label]
            for k in ("bytes_mb", "downtime_s", "evict_s"):
                r = ratio(c, ref, k)
                row += [r[0], r[0] - r[1], r[2] - r[0]] if r else [None] * 3
                fact(f"wl.{label}.{v}.ratio.{k}", r, f"{c} / {ref}", 3 if k == "bytes_mb" else 2)
            rows.append(row)
        write_csv(out / f"fig_wl_{tag}.csv", ["x", "wl", "data", "data_m", "data_p", "down", "down_m", "down_p",
                                             "evict", "evict_m", "evict_p"], rows)
    # absolute values for the prose
    for scen, label in WL.items():
        for v in ("vanilla-zstd", "vanilla-zstd-fs", "m4", "m4-aslron", "m5", "criu-lazy", "restart-volume",
                  "restart-persist"):
            c = f"{v}-{scen}"
            if c in CELLS:
                for k in ("bytes_mb", "downtime_s", "outage_s", "evict_s", "src_cpu_s", "total_s"):
                    fact(f"wl.{label}.{v}.{k}", mean(c, k), c, 3 if k == "bytes_mb" else 2)


# ---------------------------------------------------------------- RQ2: network profiles

NET_SERIES = {  # (workload scenario stem, variant) -> tag
    ("redis-medium", "vanilla-zstd"): "redis_vzstd", ("redis-medium", "criu-incr-zstd"): "redis_incr",
    ("redis-medium", "m4"): "redis_pre", ("redis-medium", "m5"): "redis_post",
    ("redis-medium", "criu-lazy"): "redis_lazy", ("redis-medium", "restart-persist"): "redis_restart",
    ("pyapp-medium", "vanilla-zstd-fs"): "py_vzstd", ("pyapp-medium", "m4"): "py_pre",
    ("pyapp-medium", "m5"): "py_post",
}


def network(out):
    for (stem, v), tag in NET_SERIES.items():
        rows = []
        for p, cap in PROFILES:
            c = f"{v}-{stem}-{p}"
            if c not in CELLS:
                continue
            rows.append([cap, mean(c, "bw_mbps"), mean(c, "downtime_s"), sd(c, "downtime_s"),
                         mean(c, "evict_s"), sd(c, "evict_s"), mean(c, "outage_s"), sd(c, "outage_s"),
                         mean(c, "bytes_mb")])
            for k in ("downtime_s", "evict_s", "outage_s", "bytes_mb", "bw_mbps"):
                fact(f"net.{stem}.{v}.{p}.{k}", mean(c, k), c, 3 if k == "bytes_mb" else 2)
            ref = f"vanilla-zstd-{stem}-{p}" if f"vanilla-zstd-{stem}-{p}" in CELLS else f"vanilla-zstd-fs-{stem}-{p}"
            if v not in ("vanilla-zstd", "vanilla-zstd-fs"):
                fact(f"net.{stem}.{v}.{p}.ratio.evict", ratio(c, ref, "evict_s"), c)
                fact(f"net.{stem}.{v}.{p}.ratio.downtime", ratio(c, ref, "downtime_s"), c)
        write_csv(out / f"fig_net_{tag}.csv", ["cap", "bw", "down", "down_sd", "evict", "evict_sd",
                                               "outage", "outage_sd", "data"], rows)
    for stem in ("redis-medium", "pyapp-medium"):
        for v in ("m4", "m5", "vanilla-zstd", "vanilla-zstd-fs", "criu-lazy"):
            c = f"{v}-{stem}-P2L"
            if c in CELLS:
                fact(f"net.{stem}.{v}.P2L.downtime_s", mean(c, "downtime_s"), c)
                fact(f"net.{stem}.{v}.P2L.ratio.downtime_vs_P2", ratio(c, f"{v}-{stem}-P2", "downtime_s"), c)


# ---------------------------------------------------------------- RQ2: state size and dirty rate

def size_and_dirty(out):
    sizes = [16, 64, 256, 1024]
    for v, tag in (("vanilla-zstd", "vzstd"), ("m4", "pre"), ("m5", "post"), ("criu-lazy", "lazy")):
        rows = []
        for mb in sizes:
            c = f"{v}-synth-size{mb}mb-P3"
            if c not in CELLS:
                continue
            rows.append([mb, mean(c, "downtime_s"), sd(c, "downtime_s"), mean(c, "evict_s"), sd(c, "evict_s"),
                         mean(c, "outage_s"), sd(c, "outage_s"), mean(c, "bytes_mb")])
            for k in ("downtime_s", "evict_s", "outage_s", "bytes_mb", "total_s"):
                fact(f"size.{mb}.{v}.{k}", mean(c, k), c, 3 if k == "bytes_mb" else 2)
            if v != "vanilla-zstd":
                for k in ("downtime_s", "evict_s"):
                    fact(f"size.{mb}.{v}.ratio.{k}", ratio(c, f"vanilla-zstd-synth-size{mb}mb-P3", k), c)
        write_csv(out / f"fig_size_{tag}.csv", ["mb", "down", "down_sd", "evict", "evict_sd", "outage",
                                                "outage_sd", "data"], rows)
    for v, tag in (("vanilla-zstd", "vzstd"), ("m4-merge", "pre"), ("m5", "post")):
        rows = []
        for pps in (0, 500, 2000, 8000):
            c = f"{v}-synth-dirty{pps}pps-P2"
            if c not in CELLS:
                continue
            rows.append([pps, mean(c, "downtime_s"), sd(c, "downtime_s"), mean(c, "evict_s"), sd(c, "evict_s"),
                         mean(c, "outage_s"), sd(c, "outage_s"), mean(c, "bytes_mb")])
            for k in ("downtime_s", "evict_s", "outage_s", "bytes_mb", "total_s"):
                fact(f"dirty.{pps}.{v}.{k}", mean(c, k), c, 3 if k == "bytes_mb" else 2)
            if v != "vanilla-zstd":
                for k in ("downtime_s", "evict_s"):
                    fact(f"dirty.{pps}.{v}.ratio.{k}", ratio(c, f"vanilla-zstd-synth-dirty{pps}pps-P2", k), c)
        write_csv(out / f"fig_dirty_{tag}.csv", ["pps", "down", "down_sd", "evict", "evict_sd", "outage",
                                                 "outage_sd", "data"], rows)


# ---------------------------------------------------------------- post-copy table

POST_ROWS = [("redis-medium-P1", "Redis, P1"), ("redis-medium-P2", "Redis, P2"),
             ("redis-aof-medium-P2", "Redis-AOF"), ("postgres-medium-P2", "PostgreSQL"),
             ("jvm-medium-P2", "JVM"), ("pyapp-medium-P2", "Python"), ("synth-size64mb-P2", "Synthetic 64\\,MB")]


def postcopy_table(out):
    lines = [r"\begin{tabular}{lrrrrrr}", r"\toprule",
             r" & \multicolumn{2}{c}{Downtime (s)} & \multicolumn{2}{c}{Client outage (s)} & \multicolumn{2}{c}{Eviction (s)} \\",
             r"\cmidrule(lr){2-3}\cmidrule(lr){4-5}\cmidrule(lr){6-7}",
             r"Workload & lazy & M5 & lazy & M5 & lazy & M5 \\", r"\midrule"]
    for scen, label in POST_ROWS:
        a, b = f"criu-lazy-{scen}", f"m5-{scen}"
        if a not in CELLS or b not in CELLS:
            continue
        vals = []
        for k in ("downtime_s", "outage_s", "evict_s"):
            vals += [mean(a, k), mean(b, k)]
            fact(f"post.{label}.lazy.{k}", mean(a, k), a)
            fact(f"post.{label}.m5.{k}", mean(b, k), b)
            fact(f"post.{label}.ratio.{k}", ratio(b, a, k), f"{b} / {a}")
        fact(f"post.{label}.lazy.bytes", mean(a, "bytes_mb"), a, 3)
        fact(f"post.{label}.m5.bytes", mean(b, "bytes_mb"), b, 3)
        fmt = [f2(x, 1) if x is not None and x >= 10 else f2(x) for x in vals]
        lines.append(f"{label} & " + " & ".join(fmt) + r" \\")
    lines += [r"\bottomrule", r"\end{tabular}"]
    (out / "tab_postcopy.tex").write_text("\n".join(lines) + "\n", encoding="utf-8")


# ---------------------------------------------------------------- fault table

FAULTS = [("loss-at-precopy_30", "0.1\\,\\% loss, pre-copy (30\\,s)"),
          ("partition-at-precopy_15", "Partition 15\\,s, pre-copy"),
          ("partition-at-final_15", "Partition 15\\,s, final round"),
          ("partition-at-final_150", "Partition 150\\,s, final round"),
          ("kill-receiver-at-precopy", "Kill receiver, pre-copy"),
          ("kill-receiver-at-final", "Kill receiver, final round"),
          ("kill-restore-at-final", "Kill restore, final round"),
          ("partition-at-drain_30", "Partition 30\\,s, post-copy drain"),
          ("kill-receiver-at-drain", "Kill receiver, post-copy drain"),
          ("hang-daemon-at-drain", "Stall lazy-pages daemon, drain")]
FAULT_VARIANTS = [("vanilla", "Van."), ("criu-lazy", "Lazy"), ("m4", "M4"), ("m4-merge", "M4m"), ("m5", "M5")]
CODE = [("completed", "C"), ("completed_ack_lost", "A"), ("aborted_source_resumed", "R"),
        ("held_paused", "H"), ("timeout", "T"), ("failed_unsafe", "U")]


def fault_table(out):
    camps = ["lab_faults", "lab_rerun_faults", "lab_rerun_drift_faults-lazy"]
    by = {}
    for camp in camps:
        for r in fd.ledger(camp):
            if camp in fd.OLD and fd.variant_of(r["cell"]) in fd.FIXED:
                continue
            oc = r.get("fault_outcome") or ("timeout" if str(r.get("reason", "")).startswith("timeout") else None)
            by.setdefault(r["cell"], []).append(oc)
    lines = [r"\begin{tabular}{l" + "c" * len(FAULT_VARIANTS) + "}", r"\toprule",
             "Fault & " + " & ".join(lab for _, lab in FAULT_VARIANTS) + r" \\", r"\midrule"]
    unsafe = {v: [0, 0] for v, _ in FAULT_VARIANTS}
    for f, flabel in FAULTS:
        row = []
        for v, _ in FAULT_VARIANTS:
            ocs = by.get(f"{v}-redis-fault-{f}-P2")
            if not ocs:
                row.append("--")
                continue
            parts = []
            for key, code in CODE:
                n = sum(1 for o in ocs if o == key)
                if n:
                    parts.append(f"{n}{code}")
            other = len(ocs) - sum(1 for o in ocs if o in dict(CODE))
            if other:
                parts.append(f"{other}?")
            cell = " ".join(parts)
            nu = sum(1 for o in ocs if o == "failed_unsafe")
            unsafe[v][0] += nu
            unsafe[v][1] += len(ocs)
            row.append(r"\textbf{" + cell + "}" if nu else cell)
        lines.append(f"{flabel} & " + " & ".join(row) + r" \\")
    lines += [r"\bottomrule", r"\end{tabular}"]
    (out / "tab_faults.tex").write_text("\n".join(lines) + "\n", encoding="utf-8")
    for v, (nu, n) in unsafe.items():
        fact(f"faults.{v}.unsafe", f"{nu} of {n} attempts", "fault ledgers")
    lost = sum(int(r["lost"] or 0) for rs in CELLS.values() for r in rs if "-fault-" in r["cell"] and r["lost"])
    fact("faults.lost_writes_valid_runs", f"{lost}", "runs.csv fault cells")
    for v in ("m5",):
        t = col("m5-redis-fault-hang-daemon-at-drain-P2", "time_to_fallback_s")
        fact("faults.m5.hang_daemon.time_to_fallback", statistics.mean(t) if t else None, "m5 hang-daemon")


# ---------------------------------------------------------------- other measurements for the prose

def misc():
    P1 = "redis-medium-P1"
    fact("m3.vs_compression.downtime", ratio(f"m3-{P1}", f"compression-{P1}", "downtime_s"), "m3 / compression")
    fact("m3.vs_compression.evict", ratio(f"m3-{P1}", f"compression-{P1}", "evict_s"), "m3 / compression")
    fact("m4merge.vs_m4.P1.downtime", ratio(f"m4-merge-{P1}", f"m4-{P1}", "downtime_s"), "m4-merge / m4")
    # multi-container
    for n in (1, 2, 4, 8):
        for v in ("vanilla-zstd", "m4", "m5"):
            c = f"{v}-redis-multi{n}-P2"
            fact(f"multi.{n}.{v}.downtime", mean(c, "downtime_s"), c)
            fact(f"multi.{n}.{v}.total", mean(c, "total_s"), c)
            fact(f"multi.{n}.{v}.bytes", mean(c, "bytes_mb"), c, 3)
    # filesystem sweeps
    for lab in ("files1mb", "files16mb", "files64mb", "files256mb", "files64mb-rewrite0",
                "files64mb-rewrite5", "files64mb-rewrite20"):
        scen = f"synth-{lab}-P2"
        for v in ("vanilla-zstd-fs", "m4-merge", "m5", "criu-lazy"):
            c = f"{v}-{scen}"
            fact(f"fs.{lab}.{v}.downtime", mean(c, "downtime_s"), c)
            fact(f"fs.{lab}.{v}.bytes", mean(c, "bytes_mb"), c, 3)
            fact(f"fs.{lab}.{v}.evict", mean(c, "evict_s"), c)
            if v != "vanilla-zstd-fs":
                fact(f"fs.{lab}.{v}.ratio.downtime", ratio(c, f"vanilla-zstd-fs-{scen}", "downtime_s"), c)
    # connection continuity / outbound
    import conn_continuity as cc
    surv = n = 0
    for c, rs in CELLS.items():
        if c.endswith("-redis-conn-P2"):
            for r in rs:
                log = next((fd.RESULTS / r["run_dir"]).glob("**/conn_client.log"), None)
                if log:
                    n += 1
                    surv += bool(cc.summarize(log).get("survived"))
    fact("conn.survived", f"{surv} of {n} runs", "conn_continuity.py on every *-redis-conn-P2 run")
    for v in ("vanilla", "m4-merge", "m5", "criu-lazy"):
        c = f"{v}-redis-conn-P2"
        for k in ("downtime_s", "outage_s"):
            fact(f"conn.{v}.{k}", mean(c, k), c)
        c = f"{v}-pyapp-out-small-outboundTrue-P2"
        if c in CELLS:
            fact(f"outbound.{v}.downtime", mean(c, "downtime_s"), c)
            fact(f"outbound.{v}.outage", mean(c, "outage_s"), c)
    # restart variants
    for scen in ("redis-aof-medium-P2", "postgres-medium-P2"):
        c = f"restart-volume-{scen}"
        fact(f"restartvol.{scen}.downtime", mean(c, "downtime_s"), c)
        fact(f"restartvol.{scen}.outage", mean(c, "outage_s"), c)
        fact(f"restartvol.{scen}.ratio_vs_m4", ratio(c, f"m4-{scen}", "downtime_s"), c)
    # bandwidth gate: measured link per profile (all cells)
    # synthesis cost: only Redis was measured (synth_cost.py, one cold synthesis on the lab destination)
    import json
    for p in sorted(fd.RESULTS.glob("*_synth_cost/synth_cost.jsonl")):
        for line in p.read_text().splitlines():
            r = json.loads(line)
            for k in ("wall_s", "cpu_s", "max_rss_mb", "template_mb", "manifest_kb"):
                fact(f"synthcost.{r['workload']}.rep{r['rep']}.{k}", r.get(k), p.parent.name, 1)
    for p, cap in PROFILES + [("P2L", 10)]:
        bws = [r["bw_mbps"] for rs in CELLS.values() for r in rs
               if r["scenario"].endswith(f"-{p}") and r["bw_mbps"] is not None]
        fact(f"link.{p}.measured", f"{min(bws):.2f}-{max(bws):.2f} Mb/s (cap {cap})", "bw_mbps, all runs")


PHASES = ("src_manifest_fetched", "src_fs_precopy_start", "src_fs_precopy_end", "src_pause",
          "src_final_dump_end", "dst_restore_end", "src_kill")


def phases():
    """Where eviction time goes (Redis, P1): seconds from the migration command to each phase,
    mean over the cell's runs. Vanilla releases the source at its final dump, before the
    destination has restored; the synthesized-baseline variants release it after."""
    import json
    from datetime import datetime

    def ts(s):
        s = s.rstrip("Z")
        if "." in s:
            a, b = s.split(".")
            s = f"{a}.{b[:6]}"
        return datetime.fromisoformat(s)

    for v in ("vanilla-zstd", "compression", "m3", "m4", "m4-merge", "m5", "criu-lazy"):
        c = f"{v}-redis-medium-P1"
        acc = {}
        for r in CELLS.get(c, []):
            d = fd.RESULTS / r["run_dir"]
            mm = json.loads(next(d.glob("**/migration_metrics.json")).read_text())
            ph, t0 = mm.get("phases") or {}, ts(mm["migration_start_time"])
            for k in PHASES:
                x = ph.get(k)
                if x and not x.startswith("0001"):
                    acc.setdefault(k, []).append((ts(x) - t0).total_seconds())
        for k, xs in acc.items():
            fact(f"phase.{v}.{k}", statistics.mean(xs), c)
        if "src_fs_precopy_end" in acc:
            fs = [b - a for a, b in zip(acc["src_fs_precopy_start"], acc["src_fs_precopy_end"])]
            fact(f"phase.{v}.fs_round_s", statistics.mean(fs), c)


def motivation(out):
    """Where vanilla pre-copy's time goes at P1: the first (full) round against the later ones.
    Round sizes are CRIU's page-file sizes before compression; transfer times as measured."""
    import json
    rows = [("vanilla-redis-medium-P1", "Redis, vanilla"), ("vanilla-zstd-redis-medium-P1", "Redis, vanilla + zstd"),
            ("vanilla-zstd-fs-pyapp-medium-P1", "Python, vanilla + zstd")]
    lines = [r"\begin{tabular}{lrrrr}", r"\toprule",
             r" & Round 1 & Rounds 2--4 & Round 1 & Eviction \\",
             r"Workload, method & (MB) & (MB) & transfer (s) & (s) \\", r"\midrule"]
    for c, label in rows:
        r1, rest, t1 = [], [], []
        for r in CELLS.get(c, []):
            mm = json.loads(next((fd.RESULTS / r["run_dir"]).glob("**/migration_metrics.json")).read_text())
            it = sorted(mm.get("iterations") or [], key=lambda x: x["iteration"])
            if not it:
                continue
            r1.append(it[0]["size_bytes"] / 1e6)
            rest.append(sum(x["size_bytes"] for x in it[1:]) / 1e6)
            t1.append(it[0]["transfer_time_ms"] / 1000)
        v = [statistics.mean(r1), statistics.mean(rest), statistics.mean(t1), mean(c, "evict_s")]
        lines.append(f"{label} & {v[0]:.2f} & {v[1]:.2f} & {v[2]:.1f} & {v[3]:.1f} \\\\")
        tag = c.replace("-medium-P1", "")
        for k, x in zip(("round1_mb", "rounds2_4_mb", "round1_transfer_s", "evict_s"), v):
            fact(f"motiv.{tag}.{k}", x, c)
        fact(f"motiv.{tag}.round1_share_of_evict", v[2] / v[3] * 100, c, 1)
        fact(f"motiv.{tag}.n", f"{len(r1)}", c)
    lines += [r"\bottomrule", r"\end{tabular}"]
    (out / "tab_motivation.tex").write_text("\n".join(lines) + "\n", encoding="utf-8")
    fact("motiv.restart_nopersist.downtime", mean("restart-redis-medium-P1", "downtime_s"), "restart-redis-medium-P1")


def emulation():
    """Lab P1 ladder vs the real-WAN ladder (public-cloud VM, 2 Mb/s cap), cell by cell."""
    import contextlib
    import io
    import emulation_validation as ev
    lab = ev.run_dirs_from_ledger(fd.RESULTS / "_campaign_lab_ladder" / "ledger.jsonl")
    real = ev.run_dirs_from_ledger(fd.RESULTS / "_campaign_final_ladder" / "ledger.jsonl")
    with contextlib.redirect_stdout(io.StringIO()):
        rep = {c: ev.compare(c, lab[c], real[c]) for c in sorted(set(lab) & set(real))}
        rho = ev.replication(rep, "vanilla-zstd-redis-medium")
    fact("emulation.cells", f"{len(rep)}", "lab_ladder vs final_ladder")
    fact("emulation.runs", f"{sum(len(lab[c]) + len(real[c]) for c in rep)}", "lab_ladder vs final_ladder")
    for k, v in rho.items():
        fact(f"emulation.rho.{k}", v["rho"], "Spearman, per-cell means", 3)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(HERE.parents[2] / "docs" / "paper" / "generated"))
    a = ap.parse_args()
    out = Path(a.out)
    if not out.parent.exists():
        sys.exit(f"{out.parent} not found: pass --out <docs repo>/paper/generated")
    out.mkdir(exist_ok=True)
    load()
    ledger_counts()
    hitrate(out)
    entropy_table(out)
    ladder_table(out)
    workload_ratios(out)
    network(out)
    size_and_dirty(out)
    postcopy_table(out)
    fault_table(out)
    misc()
    phases()
    emulation()
    motivation(out)
    with open(out / "facts.md", "w", encoding="utf-8") as f:
        f.write("# Paper facts (generated by statepod/experiment/analysis/paper_data.py; do not edit)\n\n")
        f.write("Means over valid runs; ratios = ratio of means [95 % bootstrap CI].\n\n")
        f.write("| key | value | source |\n|---|---|---|\n")
        for k, v, s in FACTS:
            f.write(f"| {k} | {v} | {s} |\n")
    print(f"{len(FACTS)} facts, tables and figure data -> {out}")


if __name__ == "__main__":
    main()
