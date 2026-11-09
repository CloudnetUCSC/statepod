"""The paper's dataset: one row per valid run, merged from the lab campaign and
the rerun, with the supersede rules in ONE place (below).
Every table/figure for the paper reads this, never a single
campaign ledger.

Rules
  * Variants whose code path the rerun changed (every synth-precopy catch-up
    variant: m1 m2 m3 compression m4 m4-merge m4-aslron; every synth-postcopy
    variant: m5 m5-aslron) come ONLY from the rerun campaigns (lab_rerun_*).
    The four Redis-AOF cells of those variants come only from lab_rerun_aof
    (template-key fix), compression only from lab_rerun_drift_compression.
  * Everything else (baselines, restart, criu-lazy) comes from the lab
    campaign (lab_<stage>, lab_topup*).
  * m4-availability is kept but flagged `ablation` (one row with
    its lost writes, never in the main figures).
  * Fault cells of fixed variants come from lab_rerun_faults (faults-v2).
  * lab_rerun_drift_drift (baselines re-measured next to M4) is NOT merged;
    drift_check.py compares it with the October rows.

    uv run python final_dataset.py            # writes results/_final/runs.csv + prints counts
"""
import csv
import json
import statistics
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
RESULTS = HERE.parent / "results"
sys.path.insert(0, str(HERE.parent / "runner"))
import run_campaign  # noqa: E402

OLD = ["lab_ladder", "lab_workloads", "lab_net", "lab_multi", "lab_size", "lab_faults",
       "lab_continuity", "lab_outbound", "lab_synth-dirty", "lab_synth-entropy", "lab_synth-fs",
       "lab_synth-fs-rewrite", "lab_topup_faults", "lab_topup_p2l", "lab_topup2_faults"]
NEW_STAGES = ["ladder", "workloads", "faults", "faults-lazy", "net", "multi", "size", "continuity",
              "outbound", "synth-dirty", "synth-entropy", "synth-fs", "synth-fs-rewrite"]
NEW = [f"lab_rerun_{s}" for s in NEW_STAGES] + ["lab_rerun_aof_aof", "lab_rerun_drift_compression"]

FIXED = {"m1", "m2", "m3", "compression", "m4", "m4-merge", "m4-aslron", "m5", "m5-aslron"}
ABLATION = {"m4-availability"}
AOF_CELLS = {f"{v}-redis-aof-medium-P2" for v in ("m4", "m4-aslron", "m5", "m5-aslron")}
VARIANTS = sorted(run_campaign.VARIANTS, key=len, reverse=True)


def variant_of(cell):
    return next((v for v in VARIANTS if cell.startswith(v + "-")), None)


def allowed(cell, campaign):
    """Is a valid row of `cell` from `campaign` part of the paper's dataset?"""
    v = variant_of(cell)
    if cell in AOF_CELLS:
        return campaign == "lab_rerun_aof_aof"
    if v == "compression":
        return campaign == "lab_rerun_drift_compression"
    if v in FIXED:
        return campaign in NEW
    return campaign in OLD


def ledger(campaign):
    p = RESULTS / f"_campaign_{campaign}" / "ledger.jsonl"
    return [json.loads(l) for l in p.read_text().splitlines() if l.strip()] if p.exists() else []


def record(run_dir):
    man = json.loads((run_dir / "manifest.json").read_text())
    mfs = sorted(run_dir.glob("**/migration_metrics.json"))
    mm = json.loads(mfs[0].read_text()) if mfs else {}
    sv = man.get("state_verify") or {}
    lp = man.get("link_probe") or {}
    probes = [(lp.get(k) or {}).get("recv_mbps") for k in ("pre", "post")]
    probes = [p for p in probes if p]
    lw = sv.get("lost_writes") or {}
    rec = sv.get("source_reclaim") or {}
    cost = sv.get("source_cost") or {}
    fault = sv.get("fault") or {}
    pipe = mm.get("pipeline") or {}
    ms = lambda x: x / 1000.0 if isinstance(x, (int, float)) else None  # noqa: E731
    return {
        "downtime_s": ms(mm.get("downtime_source_side_ms")),
        "total_s": ms(mm.get("total_migration_time_ms")),
        "outage_s": (sv.get("client_outage") or {}).get("outage_s"),
        "bytes_mb": mm["total_data_transferred_bytes"] / 1e6 if mm.get("total_data_transferred_bytes") is not None else None,
        "evict_s": ms(rec.get("eviction_ms")),
        "src_cpu_s": cost.get("tree_cpu_s"),
        "src_mem_mb": cost["mem_avail_drop_kb"] / 1024.0 if cost.get("mem_avail_drop_kb") is not None else None,
        "bw_mbps": statistics.mean(probes) if probes else None,
        "hit_rate": pipe.get("hit_rate"),
        "lost": lw.get("lost"),
        "acked": lw.get("acked"),
        "fault_outcome": fault.get("outcome"),
        "time_to_fallback_s": fault.get("time_to_fallback_s"),
        "aslr_on": man.get("aslr_on", False),
        "workload": man.get("workload"),
    }


def build():
    rows, dropped = [], {}
    for campaign in OLD + NEW:
        for r in ledger(campaign):
            if r["status"] != "valid" or not r.get("run_dir"):
                continue
            cell = r["cell"]
            if not allowed(cell, campaign):
                dropped[campaign] = dropped.get(campaign, 0) + 1
                continue
            d = RESULTS / r["run_dir"]
            if not (d / "manifest.json").exists():
                sys.exit(f"ledger names {r['run_dir']} but it is not in results/ (pull it first)")
            v = variant_of(cell)
            row = {"cell": cell, "variant": v, "scenario": cell[len(v) + 1:] if v else cell,
                   "campaign": campaign, "round": r["round"], "run_dir": r["run_dir"],
                   "role": "ablation" if v in ABLATION else "main"}
            row.update(record(d))
            rows.append(row)
    return rows, dropped


def main():
    rows, dropped = build()
    out = RESULTS / "_final"
    out.mkdir(exist_ok=True)
    with open(out / "runs.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    by_v = {}
    for r in rows:
        by_v[r["variant"]] = by_v.get(r["variant"], 0) + 1
    print(f"{len(rows)} valid runs, {len({r['cell'] for r in rows})} cells -> {out / 'runs.csv'}")
    for v, n in sorted(by_v.items(), key=lambda x: str(x[0])):
        print(f"  {str(v):18s} {n}")
    print("superseded (valid rows not merged):", dict(sorted(dropped.items())))
    missing = [c for c in sorted({r['cell'] for r in rows}) if variant_of(c) is None]
    if missing:
        print("cells without a known variant:", missing)


if __name__ == "__main__":
    main()
