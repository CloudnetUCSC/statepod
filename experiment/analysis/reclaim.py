"""Eviction time and source-reclaim metrics, derived from the
per-phase timeline in migration_metrics.json -- the same code is used by
run_experiment.py (manifest `source_reclaim`) and the analysis scripts, so the two
cannot disagree.

Definitions (literature: Deshpande et al., Scatter-Gather; Fernando et al., Quick Eviction):

  eviction_ms        migration command issued -> the source holds no state of the
                     container (its process is dead and, for M5, the destination no
                     longer needs pages from it). THE headline reclaim metric.
  release_after_pause_ms
                     the same instant measured from the moment the source stopped
                     serving (pause / final dump start): how long the source is kept
                     after clients lost it.
  drain_end_ms       M5 only: command -> destination's pending set hit zero.

The release instant per mode (all on the SOURCE clock, `src_*` phases):

  precopy / criu-incr / precopy-rsync   src_final_dump_end   `runc checkpoint
                                        --leave-running=false` kills the process when
                                        the dump finishes (the pre-dump images stay on
                                        disk, but no process, memory or CPU is held)
  sync / sync-rsync                     src_kill             explicit kill after freeze
  synth-precopy                         src_kill             ACK-gated: killed once the
                                        destination confirmed a verified image
  criu-lazy (stock CRIU lazy-pages)     src_kill             the dump process exits when the
                                        last page has been served: same hostage cost as M5
  synth-postcopy (M5)                   src_kill             killed only after the drain
                                        completes -- the source is held hostage for the
                                        whole drain, the cost M5 pays for its downtime

Metrics files from before the `phases` timeline (July runs) fall back to the legacy
fields and report `eviction_precise: false`.
"""
from datetime import datetime

RELEASE_PHASE = {
    "precopy": "src_final_dump_end",
    "criu-incr": "src_final_dump_end",
    "precopy-rsync": "src_final_dump_end",
    "sync": "src_kill",
    "sync-rsync": "src_kill",
    "synth-precopy": "src_kill",
    "synth-postcopy": "src_kill",
    "criu-lazy": "src_kill",       # the lazy dump process exits once every page was served
}


def _ts(s):
    """RFC3339Nano (Go) -> epoch seconds; None for the zero time or garbage."""
    if not s or str(s).startswith("0001-01-01"):
        return None
    s = str(s)
    # datetime.fromisoformat (py<3.11) takes at most 6 fractional digits and no 'Z'
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    if "." in s:
        head, rest = s.split(".", 1)
        frac = ""
        i = 0
        while i < len(rest) and rest[i].isdigit():
            frac += rest[i]
            i += 1
        s = f"{head}.{frac[:6].ljust(6, '0')}{rest[i:]}"
    try:
        return datetime.fromisoformat(s).timestamp()
    except ValueError:
        return None


def reclaim(m):
    """m: parsed migration_metrics.json. Returns a dict of eviction metrics
    (None where the data to compute them is missing -- never a guess)."""
    mode = m.get("mode")
    phases = m.get("phases") or {}
    start = _ts(m.get("migration_start_time"))
    out = {"mode": mode, "eviction_ms": None, "release_after_pause_ms": None,
           "drain_end_ms": None, "eviction_precise": False}
    release = _ts(phases.get(RELEASE_PHASE.get(mode, ""))) if phases else None
    if release is not None:
        out["eviction_precise"] = True
    elif start is not None:  # pre-timeline metrics: legacy fields
        legacy = {"synth-precopy": "source_kill_time", "synth-postcopy": "restore_end_time", "criu-lazy": "restore_end_time"}
        release = _ts(m.get(legacy.get(mode, "freeze_start_time")))
    pause = _ts(m.get("freeze_start_time"))
    if release is not None and start is not None:
        out["eviction_ms"] = round((release - start) * 1000.0, 1)
    if release is not None and pause is not None:
        out["release_after_pause_ms"] = round((release - pause) * 1000.0, 1)
    drain = _ts(phases.get("src_drain_end")) if phases else None
    if drain is not None and start is not None:
        out["drain_end_ms"] = round((drain - start) * 1000.0, 1)
    return out


if __name__ == "__main__":  # python reclaim.py <migration_metrics.json>...
    import json
    import sys
    for p in sys.argv[1:]:
        print(p, json.dumps(reclaim(json.load(open(p)))))
