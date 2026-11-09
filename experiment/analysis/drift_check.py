"""Does merging October baselines with the November rows hold up?

lab_rerun_drift_drift re-ran the P1 ladder baselines interleaved with M4 on the
fixed engine. Two checks, written to results/_final/drift_check.md:

  1. baseline drift: each baseline's November runs vs its October runs (lab_ladder),
     ratio of means with a bootstrap CI. A CI that contains 1 within a few percent
     = the testbed did not move.
  2. the comparison the paper makes: M4 vs vanilla+zstd PAIRED by round inside the
     drift campaign (same link conditions) against the UNPAIRED merged estimate
     (M4 from lab_rerun_ladder vs vanilla+zstd from lab_ladder). Agreement = the
     merged dataset gives the same answer as a same-session measurement.

    uv run python drift_check.py
"""
import statistics
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import stats  # noqa: E402
import final_dataset as fd  # noqa: E402

BASELINES = ["vanilla-redis-medium-P1", "vanilla-zstd-redis-medium-P1",
             "criu-incr-zstd-redis-medium-P1", "criu-lazy-redis-medium-P1"]
METRICS = (("downtime_s", "downtime"), ("outage_s", "client outage"), ("bytes_mb", "data"))


def runs(campaign, cell):
    out = []
    for r in fd.ledger(campaign):
        if r["cell"] == cell and r["status"] == "valid" and r.get("run_dir"):
            rec = fd.record(fd.RESULTS / r["run_dir"])
            rec["round"] = r["round"]
            out.append(rec)
    return out


def vals(rs, k):
    return [r[k] for r in rs if r.get(k) is not None]


def ci(c):
    return f"{c['ratio_of_means']:.3f} [{c['ci_ratio'][0]:.3f}, {c['ci_ratio'][1]:.3f}]"


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    out = []
    p = out.append
    p("# Drift check: October baselines vs November re-measurement\n")
    p("## 1. Baselines, November / October (ratio of means [95% bootstrap CI])\n")
    p("| cell | n Nov / Oct | " + " | ".join(name for _, name in METRICS) + " |")
    p("|---|---|" + "---|" * len(METRICS))
    for c in BASELINES:
        new, old = runs("lab_rerun_drift_drift", c), runs("lab_ladder", c)
        cols = [ci(stats.compare(vals(new, k), vals(old, k))) if vals(new, k) and vals(old, k) else "n/a"
                for k, _ in METRICS]
        p(f"| {c} | {len(new)} / {len(old)} | " + " | ".join(cols) + " |")
    p("")

    p("## 2. M4 / vanilla+zstd at P1: same-session paired vs merged unpaired\n")
    m4d, vzd = runs("lab_rerun_drift_drift", "m4-redis-medium-P1"), runs("lab_rerun_drift_drift", "vanilla-zstd-redis-medium-P1")
    m4m, vzm = runs("lab_rerun_ladder", "m4-redis-medium-P1"), runs("lab_ladder", "vanilla-zstd-redis-medium-P1")
    p("| metric | paired, same rounds (n) | merged, unpaired (n / n) |")
    p("|---|---|---|")
    for k, name in METRICS:
        vz_by = {r["round"]: r for r in vzd}
        pr = [a[k] / vz_by[a["round"]][k] for a in m4d
              if a["round"] in vz_by and a.get(k) and vz_by[a["round"]].get(k)]
        if len(pr) >= 2:
            lo, hi = stats.bootstrap_ci(pr, statistics.mean)
            paired = f"{statistics.mean(pr):.3f} [{lo:.3f}, {hi:.3f}] ({len(pr)})"
        else:
            paired = "n/a"
        um = stats.compare(vals(m4m, k), vals(vzm, k)) if vals(m4m, k) and vals(vzm, k) else None
        p(f"| {name} | {paired} | {ci(um) if um else 'n/a'} ({len(m4m)} / {len(vzm)}) |")
    p("\nThe two estimates should agree within their CIs; the paper uses the merged one.\n")

    text = "\n".join(out)
    (fd.RESULTS / "_final").mkdir(exist_ok=True)
    (fd.RESULTS / "_final" / "drift_check.md").write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
