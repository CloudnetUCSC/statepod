"""Report over the paper's merged dataset (final_dataset.py -> results/_final/runs.csv).

Writes results/_final/report.md:
  1. provenance: valid runs per variant and per source campaign;
  2. per cell: n, downtime / client outage / eviction (mean +/- SD [95% bootstrap CI]),
     data on the wire, lost writes, measured link rate;
  3. each cell against vanilla+zstd in the same scenario. The rerun rows were run
     after the baselines, so rounds no longer pair: ratios are UNPAIRED (both groups
     resampled independently, stats.compare); drift_check.py shows the baselines did
     not move in between;
  4. fault matrix: outcomes of every fault run, including attempts that never became
     valid (stock criu-lazy under a stuck daemon times out), with what clients saw.

    uv run python final_report.py
"""
import csv
import json
import statistics
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import stats  # noqa: E402
import final_dataset as fd  # noqa: E402

OUT = fd.RESULTS / "_final"
NUM = ("downtime_s", "total_s", "outage_s", "bytes_mb", "evict_s", "src_cpu_s", "src_mem_mb",
       "bw_mbps", "hit_rate", "lost", "acked", "time_to_fallback_s")


def load():
    rows = list(csv.DictReader(open(OUT / "runs.csv", encoding="utf-8")))
    for r in rows:
        for k in NUM:
            r[k] = float(r[k]) if r.get(k) not in (None, "") else None
    return rows


def col(rs, k):
    return [r[k] for r in rs if r[k] is not None]


def fmt(xs, digits=2):
    if not xs:
        return "n/a"
    s = stats.summarize(xs)
    lo, hi = s["ci_mean"]
    return f"{s['mean']:.{digits}f} ±{s['sd']:.{digits}f} [{lo:.{digits}f}, {hi:.{digits}f}]"


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    rows = load()
    cells = {}
    for r in rows:
        cells.setdefault(r["cell"], []).append(r)
    out = []
    p = out.append

    p("# Final dataset report\n")
    p(f"{len(rows)} valid runs in {len(cells)} cells. Source of truth: `results/_final/runs.csv` "
      "(final_dataset.py; supersede rules in its docstring).\n")
    p("## Provenance\n")
    p("| variant | runs | from campaigns |")
    p("|---|---|---|")
    byv = {}
    for r in rows:
        byv.setdefault(r["variant"], {}).setdefault(r["campaign"], 0)
        byv[r["variant"]][r["campaign"]] += 1
    for v in sorted(byv):
        camps = ", ".join(f"{c} ({n})" for c, n in sorted(byv[v].items()))
        p(f"| {v} | {sum(byv[v].values())} | {camps} |")
    p("")

    p("## Per cell (mean ±SD [95% bootstrap CI of the mean])\n")
    p("| cell | role | n | downtime (s) | client outage (s) | eviction (s) | data (MB) | lost / acked | link (Mb/s) |")
    p("|---|---|---|---|---|---|---|---|---|")
    for c in sorted(cells):
        rs = cells[c]
        lost, acked = sum(col(rs, "lost")), sum(col(rs, "acked"))
        bw = col(rs, "bw_mbps")
        p(f"| {c} | {rs[0]['role']} | {len(rs)} | {fmt(col(rs, 'downtime_s'))} | {fmt(col(rs, 'outage_s'))} | "
          f"{fmt(col(rs, 'evict_s'))} | {fmt(col(rs, 'bytes_mb'), 3)} | "
          f"{f'{int(lost)} / {int(acked)}' if acked else '-'} | {f'{min(bw):.2f}-{max(bw):.2f}' if bw else 'n/a'} |")
    p("")

    p("## Against vanilla+zstd in the same scenario (unpaired)\n")
    p("| cell | reference | n / n_ref | downtime ratio [95% CI] | Cliff's δ | data ratio [95% CI] | outage ratio |")
    p("|---|---|---|---|---|---|---|")
    for c in sorted(cells):
        rs = cells[c]
        v, scen = rs[0]["variant"], rs[0]["scenario"]
        if v in ("vanilla-zstd",) or "-fault-" in c:
            continue
        ref = cells.get(f"vanilla-zstd-{scen}") or cells.get(f"vanilla-zstd-fs-{scen}")
        if not ref:
            continue
        refname = ref[0]["cell"]
        d = stats.compare(col(rs, "downtime_s"), col(ref, "downtime_s"))
        b = stats.compare(col(rs, "bytes_mb"), col(ref, "bytes_mb"))
        o, orf = col(rs, "outage_s"), col(ref, "outage_s")
        ot = f"{statistics.mean(o) / statistics.mean(orf):.2f}" if o and orf and statistics.mean(orf) else "n/a"
        p(f"| {c} | {refname} | {len(rs)} / {len(ref)} | {d['ratio_of_means']:.2f} "
          f"[{d['ci_ratio'][0]:.2f}, {d['ci_ratio'][1]:.2f}] | {d['cliffs_delta']:+.2f} | "
          f"{b['ratio_of_means']:.3f} [{b['ci_ratio'][0]:.3f}, {b['ci_ratio'][1]:.3f}] | {ot} |")
    p("\nBelow 1 = shorter downtime / fewer bytes than vanilla+zstd. Downtime is the engine's "
      "source-clock definition; client outage is the independent probe.\n")

    p("## Fault matrix (every attempt, valid or not)\n")
    p("| cell | campaign | attempts | completed | completed, ACK lost | aborted, source resumed | "
      "held paused | timed out | unsafe | lost writes | clients at timeout (dest answered / probes) |")
    p("|---|---|---|---|---|---|---|---|---|---|---|")
    # baselines' fault rows from the October campaign (their code did not change), the
    # fixed variants' from the rerun
    fault_camps = ["lab_faults", "lab_rerun_faults", "lab_rerun_drift_faults-lazy"]
    if not (fd.RESULTS / "_campaign_lab_rerun_drift_faults-lazy").exists():
        fault_camps.append("lab_rerun_faults-lazy")
    for camp in fault_camps:
        by = {}
        for r in fd.ledger(camp):
            if camp in fd.OLD and fd.variant_of(r["cell"]) in fd.FIXED:
                continue
            by.setdefault(r["cell"], []).append(r)
        for c in sorted(by):
            rs = by[c]
            oc = [r.get("fault_outcome") for r in rs]
            tout = [r for r in rs if str(r.get("reason", "")).startswith("timeout")]
            lost = 0
            for r in rs:
                if r.get("status") == "valid" and r.get("run_dir"):
                    lost += fd.record(fd.RESULTS / r["run_dir"]).get("lost") or 0
            seen = []
            for r in tout:
                pa = (r.get("probe_at_timeout") or {}).get("dest")
                if pa:
                    seen.append(f"{pa['ok']}/{pa['ok'] + pa['failed']}")
            p(f"| {c} | {camp} | {len(rs)} | {oc.count('completed')} | {oc.count('completed_ack_lost')} | "
              f"{oc.count('aborted_source_resumed')} | {oc.count('held_paused')} | {len(tout)} | "
              f"{oc.count('failed_unsafe') + oc.count('split_brain')} | {lost} | {', '.join(seen) or '-'} |")
    p("")

    text = "\n".join(out)
    (OUT / "report.md").write_text(text, encoding="utf-8")
    print(text[:3000])
    print(f"... -> {OUT / 'report.md'}")


if __name__ == "__main__":
    main()
