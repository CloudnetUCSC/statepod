#!/usr/bin/env python3
"""Connection-continuity client. Redis only.

One long-lived TCP connection issues a write (SET ck:<seq>) every --interval seconds
and waits for +OK. The point is not throughput but what the CONNECTION does through a
migration: does it survive, what error ends it (reset / EOF / timeout), how long until
the client can talk to the service again, and what happened to the write that was in
flight when the connection broke (applied at the destination or not: the ambiguity
every real client has to handle).

On an error the client closes the socket and reconnects, alternating the source and
the destination endpoints (the same sticky failover as probe.py / lostwrite_client.py);
after the first successful reconnect it asks the new endpoint whether the in-flight
write exists (GET ck:<seq>). Every event is one JSON line, `t` = time.time() on the
source VM (same clock domain as the other client logs and the phase marks):

  {"event": "connected",  "ep": "source"|"dest", "t": ...}
  {"event": "ok",         "seq": N, "ep": ..., "t0": ..., "t1": ...}        (every request)
  {"event": "error",      "seq": N, "kind": "reset"|"eof"|"timeout"|"other", "t": ..., "ep": ...}
  {"event": "reconnected","ep": ..., "t": ..., "after_s": seconds since the error}
  {"event": "inflight",   "seq": N, "applied": true|false|null, "ep": ...}

analysis/conn_continuity.py turns the log into the manifest summary.

  conn_client.py --src 127.0.0.1:6380 --dst <dest>:6380 [--interval 0.05] [--timeout 1.0]
"""
import argparse
import json
import socket
import sys
import time


def parse(s):
    h, _, p = s.rpartition(":")
    return h, int(p)


def emit(**kw):
    print(json.dumps(kw), flush=True)


def resp_cmd(*parts):
    out = [f"*{len(parts)}\r\n"]
    for p in parts:
        p = str(p)
        out.append(f"${len(p)}\r\n{p}\r\n")
    return "".join(out).encode()


def read_reply(f):
    line = f.readline()
    if not line:
        raise EOFError("closed")
    kind, rest = line[:1], line[1:].rstrip(b"\r\n")
    if kind == b"$":
        n = int(rest)
        if n < 0:
            return None
        data = f.read(n + 2)
        return data[:-2].decode()
    if kind == b"-":
        raise RuntimeError(rest.decode())
    return rest.decode()


def classify(e):
    if isinstance(e, (ConnectionResetError, BrokenPipeError)):
        return "reset"
    if isinstance(e, EOFError):
        return "eof"
    if isinstance(e, (socket.timeout, TimeoutError)):
        return "timeout"
    return "other"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True)
    ap.add_argument("--dst", required=True)
    ap.add_argument("--interval", type=float, default=0.05)
    ap.add_argument("--timeout", type=float, default=1.0)
    args = ap.parse_args()
    eps = [("source", parse(args.src)), ("dest", parse(args.dst))]
    cur = 0
    sock = f = None
    seq = 0
    pending = None          # (seq, t_error) of a write whose outcome is unknown

    def connect(idx):
        s = socket.create_connection(eps[idx][1], timeout=min(args.timeout, 0.5))
        s.settimeout(args.timeout)
        return s, s.makefile("rb")

    try:
        while True:
            t_loop = time.time()
            if sock is None:
                try:
                    sock, f = connect(cur)
                    emit(event="connected", ep=eps[cur][0], t=round(time.time(), 4))
                    if pending:
                        # Only a reachable, SERVING endpoint resolves the in-flight write:
                        # a frozen source accepts the connection but never answers.
                        pseq, perr = pending
                        sock.sendall(resp_cmd("GET", f"ck:{pseq}"))
                        applied = read_reply(f) is not None
                        emit(event="reconnected", ep=eps[cur][0], t=round(time.time(), 4),
                             after_s=round(time.time() - perr, 3))
                        emit(event="inflight", seq=pseq, applied=applied, ep=eps[cur][0])
                        pending = None
                except Exception:
                    for o in (f, sock):
                        try:
                            o and o.close()
                        except OSError:
                            pass
                    sock = f = None
                    cur = 1 - cur            # try the other endpoint next time
                    time.sleep(0.1)
                    continue
            seq += 1
            t0 = time.time()
            try:
                sock.sendall(resp_cmd("SET", f"ck:{seq}", seq))
                rep = read_reply(f)
                if rep != "OK":
                    raise RuntimeError(f"unexpected reply {rep!r}")
                emit(event="ok", seq=seq, ep=eps[cur][0], t0=round(t0, 4), t1=round(time.time(), 4))
            except Exception as e:
                emit(event="error", seq=seq, kind=classify(e), t=round(time.time(), 4), ep=eps[cur][0])
                pending = (seq, time.time())
                for o in (f, sock):
                    try:
                        o and o.close()
                    except OSError:
                        pass
                sock = f = None
                cur = 1 - cur
                continue
            time.sleep(max(0.0, args.interval - (time.time() - t_loop)))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
