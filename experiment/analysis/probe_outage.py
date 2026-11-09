"""Client-observed outage from a probe.py log.

Outage = first answered dest probe (t1) minus last answered source probe (t1):
the gap in which a client following the move had no endpoint answering. Both
timestamps come from the source VM's clock (probe.py runs there), so no
cross-host skew is involved. Resolution is the probe interval (default 50 ms)
plus RTT asymmetry (the dest answer arrives ~1 RTT after the server replied;
a real client sees the same delay).

If the source still answers after the first dest answer (overlap, e.g. a
leave-running source that is killed late), outage_s is 0 and overlap_s reports
how long both answered.

CLI: python probe_outage.py <probe.log> [...]
"""
import json
import sys
from pathlib import Path


def load(path):
    rows = []
    for line in Path(path).read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def outage(rows):
    src_ok = [r["t1"] for r in rows if r["ep"] == "source" and r["ok"]]
    dst_ok = [r["t1"] for r in rows if r["ep"] == "dest" and r["ok"]]
    if not src_ok or not dst_ok:
        return {"outage_s": None, "reason": "no source OK" if not src_ok else "no dest OK"}
    last_src, first_dst = max(src_ok), min(dst_ok)
    gap = first_dst - last_src
    return {
        "outage_s": max(0.0, gap),
        "overlap_s": max(0.0, -gap),
        "last_source_ok": last_src,
        "first_dest_ok": first_dst,
        "attempts": len(rows),
    }


if __name__ == "__main__":
    for p in sys.argv[1:]:
        print(p, json.dumps(outage(load(p))))
