"""How much the migration disturbs the SOURCE's service BEFORE the pause (
"source throughput drop"), from the client probe every run already records.

probe.py pings the source every 50 ms (open loop) and logs start/end/ok per attempt.
Because the probe rate is fixed, a throughput drop shows up as latency and failures:
  baseline   [start - 10 s, start)      before the migration command
  migrating  [start, pause - 1.1 s)     pre-copy rounds / pre-staging, source still serving
                                        (the last probe timeout is cut: an attempt that
                                        started just before the pause fails because of it)
and the report is p50/p99 latency and the failure count in each, plus the ratios.

(The pause itself and everything after is downtime, measured elsewhere; this is the
cost paid while the source is still supposed to be serving.)

Only the `source` endpoint is used. A probe that failed (timeout) is counted, never
dropped; latency percentiles use answered probes only.
"""
import json
from pathlib import Path

import reclaim

BASELINE_S = 10.0
EDGE_S = 1.1   # probe timeout (1.0 s) + slack: an attempt started this close to the pause may fail only because the pause began


def _pct(vals, q):
    if not vals:
        return None
    v = sorted(vals)
    return round(v[min(len(v) - 1, int(q * len(v)))], 3)


def _window(rows, a, b):
    sel = [r for r in rows if a <= r["t0"] < b]
    lat = [(r["t1"] - r["t0"]) * 1000.0 for r in sel if r.get("ok") and r.get("t1")]
    return {"n": len(sel), "failed": sum(1 for r in sel if not r.get("ok")),
            "p50_ms": _pct(lat, 0.50), "p99_ms": _pct(lat, 0.99), "max_ms": round(max(lat), 3) if lat else None}


def impact(run_dir, metrics):
    run_dir = Path(run_dir)
    out = {"baseline": None, "migrating": None, "p50_ratio": None, "p99_ratio": None}
    start = reclaim._ts(metrics.get("migration_start_time"))
    pause = reclaim._ts(metrics.get("freeze_start_time"))
    if start is None or pause is None or pause <= start:
        return out
    rows = []
    try:
        for line in (run_dir / "probe.log").read_text().splitlines():
            if line.startswith("{"):
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                if r.get("ep") == "source" and "t0" in r:
                    rows.append(r)
    except OSError:
        return out
    out["baseline"] = _window(rows, start - BASELINE_S, start)
    out["migrating"] = _window(rows, start, pause - EDGE_S)
    b, m = out["baseline"], out["migrating"]
    if b["p50_ms"] and m["p50_ms"]:
        out["p50_ratio"] = round(m["p50_ms"] / b["p50_ms"], 2)
    if b["p99_ms"] and m["p99_ms"]:
        out["p99_ratio"] = round(m["p99_ms"] / b["p99_ms"], 2)
    return out


if __name__ == "__main__":  # python source_impact.py <run_dir>
    import sys
    d = Path(sys.argv[1])
    print(json.dumps(impact(d, json.loads((d / "migration_metrics.json").read_text())), indent=2))
