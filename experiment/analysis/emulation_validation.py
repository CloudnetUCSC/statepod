"""Emulated WAN vs the real WAN, run for run .

Compares lab runs (emulated profile) with real-WAN runs of the SAME cell on what a migration
does with the link: engine downtime, client outage, total time, data moved, per-round transfer
rate, the validity probe. Each side: mean +- sample SD [95 % bootstrap CI]; the lab/real ratio
of the means. Used twice: calibrating the emulator queue on the vanilla baseline, then the
full ladder (every cell) once the queue is fixed.

    python emulation_validation.py --lab "../results/*_calib_vanilla_lim100*" \
        --real-ledger ../results/_campaign_final_ladder/ledger.jsonl --real-cell vanilla-redis-medium-cap2
    python emulation_validation.py --lab-ledger <lab ladder ledger> --real-ledger <real ladder ledger>   # all cells
"""
import argparse
import glob
import json
import re
import statistics
from pathlib import Path

from stats import bootstrap_ci, sample_sd

RESULTS = Path(__file__).resolve().parent.parent / "results"
ROUND_RE = re.compile(r"Transfer (pre\d+|final): size=(\d+) bytes, time=([0-9.]+)(ms|s|µs|us), bandwidth=([0-9.]+) Mbps")


def base_cell(cell):
    """Cell id without its network suffix, so the real ladder (-cap2) and the lab (-P1) pair up."""
    return re.sub(r"-(cap[0-9.]+|P[0-9]+L?|capped|uncapped)$", "", cell)


def run_dirs_from_ledger(ledger, cell=None):
    out = {}
    for line in Path(ledger).read_text().splitlines():
        if not line.strip():
            continue
        r = json.loads(line)
        if r.get("status") == "valid" and r.get("run_dir") and (cell is None or r["cell"] == cell):
            out.setdefault(base_cell(r["cell"]), []).append(RESULTS / r["run_dir"])
    return out


def expand(pattern):
    """A glob of result dirs; a dir holding rep*/ subdirs (--reps N) counts as its reps."""
    dirs = []
    for d in sorted(glob.glob(pattern)):
        reps = sorted(Path(d).glob("rep*"))
        dirs += reps if reps else [Path(d)]
    return dirs


def metrics_of(d):
    d = Path(d)
    try:
        m = json.loads((d / "migration_metrics.json").read_text())
        man = json.loads((d / "manifest.json").read_text())
    except (OSError, ValueError):
        return None
    sv = man.get("state_verify", {})
    if not sv.get("ok"):
        return None
    lp = man.get("link_probe") or {}
    probes = [p.get("recv_mbps") for p in (lp.get("pre") or {}, lp.get("post") or {}) if isinstance(p, dict)]
    rounds = {}
    log = d / "source_migrate.log"
    if log.exists():
        for name, _size, _t, _u, bw in ROUND_RE.findall(log.read_text(errors="replace")):
            rounds[name] = float(bw)
    return {
        "downtime_s": m.get("downtime_source_side_ms", 0) / 1000,
        "outage_s": (sv.get("client_outage") or {}).get("outage_s"),
        "total_s": m.get("total_migration_time_ms", 0) / 1000,
        "data_mb": m.get("total_data_transferred_bytes", 0) / 1e6,
        "probe_mbps": statistics.mean([p for p in probes if p is not None]) if any(p is not None for p in probes) else None,
        **{f"bw_{k}": v for k, v in rounds.items()},
    }


def summarize(rows, key):
    xs = [r[key] for r in rows if r and r.get(key) is not None]
    if not xs:
        return None
    lo, hi = bootstrap_ci(xs)
    return {"n": len(xs), "mean": statistics.mean(xs), "sd": sample_sd(xs), "ci": (lo, hi)}


def fmt(s):
    if not s:
        return "-"
    return f"{s['mean']:.3f} ±{s['sd']:.3f} [{s['ci'][0]:.3f}, {s['ci'][1]:.3f}] (n={s['n']})"


def compare(name, lab_dirs, real_dirs):
    lab = [metrics_of(d) for d in lab_dirs]
    real = [metrics_of(d) for d in real_dirs]
    keys = sorted({k for r in lab + real if r for k in r}, key=lambda k: (not k.startswith("bw_"), k))
    keys = ["downtime_s", "outage_s", "total_s", "data_mb", "probe_mbps"] + [k for k in keys if k.startswith("bw_")]
    print(f"\n## {name}  (lab valid {sum(1 for r in lab if r)}/{len(lab)}, real valid {sum(1 for r in real if r)}/{len(real)})\n")
    print("| metric | lab | real | lab/real |")
    print("|---|---|---|---|")
    out = {}
    for k in keys:
        a, b = summarize(lab, k), summarize(real, k)
        ratio = a["mean"] / b["mean"] if a and b and b["mean"] else None
        out[k] = {"lab": a, "real": b, "ratio": ratio}
        print(f"| {k} | {fmt(a)} | {fmt(b)} | {ratio:.3f} |" if ratio else f"| {k} | {fmt(a)} | {fmt(b)} | - |")
    return out


def spearman(xs, ys):
    """Spearman rank correlation (average ranks for ties), no scipy."""
    def ranks(v):
        order = sorted(range(len(v)), key=lambda i: v[i])
        r = [0.0] * len(v)
        i = 0
        while i < len(order):
            j = i
            while j + 1 < len(order) and v[order[j + 1]] == v[order[i]]:
                j += 1
            for k in range(i, j + 1):
                r[order[k]] = (i + j) / 2 + 1
            i = j + 1
        return r
    rx_, ry = ranks(xs), ranks(ys)
    mx, my = statistics.mean(rx_), statistics.mean(ry)
    num = sum((a - mx) * (b - my) for a, b in zip(rx_, ry))
    den = (sum((a - mx) ** 2 for a in rx_) * sum((b - my) ** 2 for b in ry)) ** 0.5
    return num / den if den else None


def replication(report, ref):
    """Does the lab reproduce the real WAN's RELATIVE results? Per metric: each cell's mean as a
    ratio to the reference cell's mean on the same testbed, and the Spearman rank correlation of
    the cell means between testbeds (absolute values differ with the hardware; rankings and
    ratios are what the paper's claims rest on)."""
    cells = [c for c in report if not c.startswith("_")]
    out = {}
    print(f"\n## Replication (reference cell: {ref})\n")
    for k in ("downtime_s", "outage_s", "data_mb", "total_s"):
        rows = [(c, report[c][k]["lab"]["mean"], report[c][k]["real"]["mean"]) for c in cells
                if k in report[c] and report[c][k]["lab"] and report[c][k]["real"]]
        if len(rows) < 3:
            continue
        rho = spearman([r[1] for r in rows], [r[2] for r in rows])
        refrow = next((r for r in rows if r[0] == ref), None)
        print(f"**{k}**: Spearman rho lab vs real over {len(rows)} cells = {rho:.3f}")
        if refrow:
            print("| cell | lab / ref | real / ref |\n|---|---|---|")
            for c, l, r in sorted(rows, key=lambda x: x[2]):
                print(f"| {c} | {l / refrow[1]:.2f} | {r / refrow[2]:.2f} |")
        print()
        out[k] = {"rho": rho, "n": len(rows)}
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lab", action="append", default=[], help="glob of lab result dirs (repeatable)")
    ap.add_argument("--lab-ledger")
    ap.add_argument("--real-ledger", required=True)
    ap.add_argument("--real-cell")
    ap.add_argument("--json-out")
    ap.add_argument("--ref", default="vanilla-zstd-redis-medium", help="reference cell for the ratios (base id)")
    a = ap.parse_args()
    real = run_dirs_from_ledger(a.real_ledger, a.real_cell)
    report = {}
    if a.lab:
        for pat in a.lab:
            report[pat] = compare(f"{pat} vs real {a.real_cell}", expand(pat), real.get(base_cell(a.real_cell), []))
    if a.lab_ledger:
        lab = run_dirs_from_ledger(a.lab_ledger)
        for cell in sorted(set(lab) & set(real)):
            report[cell] = compare(cell, lab[cell], real[cell])
        report["_replication"] = replication(report, a.ref)
    if a.json_out:
        Path(a.json_out).write_text(json.dumps(report, indent=1, default=str))


if __name__ == "__main__":
    main()
