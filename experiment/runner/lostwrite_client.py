#!/usr/bin/env python3
"""Lost-write test client. Redis only.

Closed-loop writer: SET lw:<seq> <seq> over a persistent connection, with the
same sticky source->dest failover as probe.py / load_client.py. A write counts
as ACKNOWLEDGED only if the server replied +OK; each acknowledgement is logged
(JSON line) with the endpoint that gave it. A write that timed out or errored
is logged as unacked and is never retried under the same seq, so it can never
mask a loss.

After the migration, --verify reads every acknowledged key back from the
destination and reports acknowledged writes that are missing or hold the wrong
value. Any such write is a LOST WRITE: the client was told it was durable and
the migrated service does not have it -- the failure the commit rule closes. Writes
acknowledged before the first migration phase must also survive, so the
counts are reported per acknowledging endpoint.

  write:  lostwrite_client.py --src 127.0.0.1:6380 --dst <dest>:6380 [--log acks.jsonl] [--rate 200] [--timeout 1.0]
  verify: lostwrite_client.py --verify acks.jsonl --target <dest>:6380

Stdlib only; runs on the source VM (clock domain: same as probe.py).
"""
import argparse
import json
import socket
import sys
import time


def parse_target(s):
    host, _, port = s.rpartition(":")
    return host, int(port)


class Conn:
    def __init__(self, host, port, timeout):
        self.host, self.port, self.timeout = host, port, timeout
        self.sock = self.f = None

    def _open(self):
        self.sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        self.sock.settimeout(self.timeout)
        self.f = self.sock.makefile("rb")

    def close(self):
        for o in (self.f, self.sock):
            try:
                o and o.close()
            except OSError:
                pass
        self.sock = self.f = None

    def set(self, key, val):
        if self.sock is None:
            self._open()
        cmd = f"*3\r\n$3\r\nSET\r\n${len(key)}\r\n{key}\r\n${len(val)}\r\n{val}\r\n".encode()
        self.sock.sendall(cmd)
        line = self.f.readline()
        if not line:
            raise ConnectionError("connection closed")
        return line.startswith(b"+OK")


_state = {"op": "start", "seq": 0}


def _heartbeat(out):
    """Every 0.5 s: what is the main loop doing? Distinguishes a frozen process
    (no heartbeats) from a loop blocked inside a call (heartbeats, op unchanged)."""
    while True:
        time.sleep(0.5)
        try:
            out.write(json.dumps({"hb": round(time.time(), 3), "op": _state["op"], "seq": _state["seq"]}) + "\n")
            out.flush()
        except Exception:
            return


def write_loop(args):
    conns = {"source": Conn(*parse_target(args.src), args.timeout),
             "dest": Conn(*parse_target(args.dst), args.timeout)}
    active, interval, seq = "source", 1.0 / args.rate, 0
    out = open(args.log, "w", buffering=1) if args.log else sys.stdout
    import threading
    threading.Thread(target=_heartbeat, args=(out,), daemon=True).start()
    try:
        while True:
            t0 = time.time()
            seq += 1
            key, val = f"lw:{seq}", str(seq)
            acked, ep = False, active
            _state["seq"] = seq
            try:
                _state["op"] = f"set@{active}"
                acked = conns[active].set(key, val)
            except (OSError, socket.timeout, ConnectionError):
                conns[active].close()
                other = "dest" if active == "source" else "source"
                try:  # fail over; the same seq is NOT retried on the old endpoint
                    _state["op"] = f"failover-set@{other}"
                    acked = conns[other].set(key, val)
                    ep = other
                    if acked:
                        active = other
                except (OSError, socket.timeout, ConnectionError):
                    conns[other].close()
            out.write(json.dumps({"seq": seq, "t0": round(t0, 4), "t1": round(time.time(), 4),
                                  "ep": ep, "acked": bool(acked)}) + "\n")
            rest = interval - (time.time() - t0)
            if rest > 0:
                time.sleep(rest)
    except KeyboardInterrupt:
        pass
    finally:
        if out is not sys.stdout:
            out.close()
        for c in conns.values():
            c.close()


def verify(args):
    acked = []
    with open(args.verify, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if line.startswith("{"):
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if r.get("acked"):
                    acked.append(r)
    host, port = parse_target(args.target)
    s = socket.create_connection((host, port), timeout=10)
    s.settimeout(30)
    f = s.makefile("rb")
    found = {}
    B = 500
    for i in range(0, len(acked), B):
        batch = acked[i:i + B]
        s.sendall(b"".join(f"*2\r\n$3\r\nGET\r\n${len(k)}\r\n{k}\r\n".encode()
                           for k in (f"lw:{r['seq']}" for r in batch)))
        for r in batch:
            line = f.readline()
            if not line:
                raise ConnectionError("verify: connection closed")
            if line.startswith(b"$-1"):
                found[r["seq"]] = None
            elif line.startswith(b"$"):
                n = int(line[1:].strip())
                body = f.read(n + 2)[:n]
                found[r["seq"]] = body.decode(errors="replace")
            else:
                found[r["seq"]] = "ERR:" + line.decode(errors="replace").strip()
    missing = [r for r in acked if found[r["seq"]] is None]
    corrupt = [r for r in acked if found[r["seq"]] not in (None, str(r["seq"]))]
    by_ep = {}
    for r in acked:
        d = by_ep.setdefault(r["ep"], {"acked": 0, "lost": 0})
        d["acked"] += 1
    for r in missing + corrupt:
        by_ep[r["ep"]]["lost"] += 1
    unacked = 0
    with open(args.verify, encoding="utf-8", errors="replace") as f2:
        for line in f2:
            line = line.strip()
            if line.startswith("{") and '"seq"' in line and '"acked": false' in line:
                unacked += 1
    res = {
        "acked": len(acked), "unacked": unacked,
        "lost": len(missing) + len(corrupt), "missing": len(missing), "corrupt": len(corrupt),
        "by_endpoint": by_ep,
        "first_lost_seq": min((r["seq"] for r in missing + corrupt), default=None),
        "lost_window_s": (round(max(r["t1"] for r in missing + corrupt)
                                - min(r["t0"] for r in missing + corrupt), 3)
                          if missing or corrupt else 0.0),
    }
    print(json.dumps(res))
    return res


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--src")
    p.add_argument("--dst")
    p.add_argument("--log")
    p.add_argument("--rate", type=float, default=200.0)
    p.add_argument("--timeout", type=float, default=1.0)
    p.add_argument("--verify", help="ack log to verify (read keys back from --target)")
    p.add_argument("--target")
    a = p.parse_args()
    if a.verify:
        verify(a)
    else:
        if not (a.src and a.dst):
            p.error("--src and --dst are required for the write loop")
        write_loop(a)


if __name__ == "__main__":
    main()
