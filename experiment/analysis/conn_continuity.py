"""Client-connection continuity through a migration, from conn_client.log.

Questions answered, per run:
  survived          did the single long-lived connection stay up the whole time?
                    (False whenever any error event occurred)
  error_kinds       how the connection ended: reset / eof / timeout / other, and
                    `detect_s`, how long the client hung before noticing (the last
                    answered write's end -> the error event). A frozen source never
                    answers, so the failure surfaces as a TIMEOUT after the client's
                    request timeout; a restored destination refuses/reset the old
                    4-tuple at once.
  stall_s           last answered write on the OLD connection -> first answered write
                    on the NEW one: what the application experiences as an outage.
  inflight          for each write that was in flight when the connection broke:
                    applied at the destination (true), absent (false) or unknown
                    (null) -- the ambiguity a client must handle (an idempotent
                    retry is needed whenever it is `applied: true` but unacknowledged).

Why the connection cannot survive here: clients reach the service through a host
DNAT port, the destination is a different host, and CRIU restores established TCP
sockets as closed (`tcp-close`, /etc/criu/runc.conf): TCP continuity across hosts
would need IP takeover plus repaired sockets, which this testbed does not do.
"""
import json
from pathlib import Path


def load(path):
    rows = []
    try:
        for line in Path(path).read_text().splitlines():
            if line.startswith("{"):
                try:
                    rows.append(json.loads(line))
                except ValueError:
                    pass
    except OSError:
        pass
    return rows


def summarize(path):
    rows = load(path)
    out = {"events": len(rows), "survived": None, "connections": 0, "writes_acked": 0,
           "errors": [], "stall_s": None, "reconnect_after_s": None, "inflight": []}
    if not rows:
        return out
    oks = [r for r in rows if r["event"] == "ok"]
    errs = [r for r in rows if r["event"] == "error"]
    out["connections"] = sum(1 for r in rows if r["event"] == "connected")
    out["writes_acked"] = len(oks)
    out["survived"] = not errs
    for e in errs:
        prev = [o for o in oks if o["t1"] <= e["t"]]
        detect = round(e["t"] - prev[-1]["t1"], 3) if prev else None
        out["errors"].append({"kind": e["kind"], "ep": e["ep"], "seq": e["seq"], "detect_s": detect})
    if errs:
        first_err = errs[0]
        before = [o for o in oks if o["t1"] <= first_err["t"]]
        rec = [r for r in rows if r["event"] == "reconnected" and r["t"] >= first_err["t"]]
        if rec:
            out["reconnect_after_s"] = rec[0]["after_s"]
            after = [o for o in oks if o["t0"] >= rec[0]["t"]]
            if before and after:
                out["stall_s"] = round(after[0]["t1"] - before[-1]["t1"], 3)
    out["inflight"] = [{"seq": r["seq"], "applied": r["applied"], "ep": r["ep"]}
                       for r in rows if r["event"] == "inflight"]
    return out


if __name__ == "__main__":
    import sys
    print(json.dumps(summarize(sys.argv[1]), indent=2))
