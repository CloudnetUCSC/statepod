#!/usr/bin/env python3
"""High-resolution client-side availability probe (replaces probe.sh).

Runs on the source VM, driven by run_experiment.py. Probes BOTH the source and
destination endpoints independently and concurrently: every `interval` seconds
each endpoint gets a fresh attempt launched on its own thread, so a slow or
timed-out attempt on one endpoint (the destination is ~190 ms RTT away, and a
RESP PING needs connect + request = 2 RTT) never delays the other endpoint or
the next tick. probe.sh needed timeout >= ~0.6 s for that reason, which gave
~2.2 s outage resolution; here the resolution is the tick interval.

Output (stdout), one JSON object per finished attempt, flushed per line:
  {"ep": "source"|"dest", "t0": <epoch s, attempt start>,
   "t1": <epoch s, attempt end>, "ok": true|false}
Lines are written in completion order (so not necessarily sorted by t0).
Analysis: experiment/analysis/probe_outage.py.

Usage: probe.py <resp|http|line|pg|tcp (or legacy redis|node)> <src_host:port> <dst_host:port> [interval=0.05] [timeout=1.0]
Stdlib only.
"""
import http.client
import json
import socket
import sys
import threading
import time

_lock = threading.Lock()
IDLE_DEST_INTERVAL = 0.5   # destination poll interval while the source still answers
# Destination poll interval once the source has failed. NOT as fast as the source
# probe: a user-mode NAT in front of a VM can lose the reply of a waiting
# connection while the guest opens >= ~10 new connections/s, which can hang a migration. 0.25 s bounds that at 4/s and costs at
# most 0.25 s of resolution on the first answered destination probe.
FAST_DEST_INTERVAL = 0.25


def _emit(ep, t0, t1, ok):
    line = json.dumps({"ep": ep, "t0": round(t0, 4), "t1": round(t1, 4), "ok": ok})
    with _lock:
        sys.stdout.write(line + "\n")
        sys.stdout.flush()


def _ping_redis(host, port, timeout):
    with socket.create_connection((host, port), timeout=timeout) as s:
        s.settimeout(timeout)
        s.sendall(b"PING\r\n")
        return s.recv(16).startswith(b"+PONG")


def _ping_line(host, port, timeout):
    """Application-level liveness for line-protocol workloads (synthetic): a
    plain TCP connect succeeds even while the process is frozen (the kernel
    accepts into the backlog), so require a real PONG."""
    with socket.create_connection((host, port), timeout=timeout) as s:
        s.settimeout(timeout)
        s.sendall(b"PING\n")
        return s.recv(16).startswith(b"PONG")


def _ping_pg(host, port, timeout):
    """Application-level liveness for PostgreSQL: an SSLRequest (length 8, code
    80877103) is answered with one byte ('S' or 'N') by the postmaster itself,
    before any authentication. A plain TCP connect also succeeds while the
    server is frozen (kernel backlog), which made every PostgreSQL outage read
    0 s (2026-10-26)."""
    with socket.create_connection((host, port), timeout=timeout) as s:
        s.settimeout(timeout)
        s.sendall(b"\x00\x00\x00\x08\x04\xd2\x16\x2f")
        return s.recv(1) in (b"S", b"N")


def _tcp_connect(host, port, timeout):
    with socket.create_connection((host, port), timeout=timeout):
        return True


def _get_http(host, port, timeout):
    c = http.client.HTTPConnection(host, port, timeout=timeout)
    try:
        c.request("GET", "/")
        c.getresponse().read()
        return True
    finally:
        c.close()


# Set once any source attempt fails: from then on the destination is probed at
# the full rate. Before that it is probed slowly (IDLE_DEST_INTERVAL) -- the
# destination cannot be serving yet, and hammering it with 20 refused
# connections/s through a NAT can starve the migration transfer itself. Outage resolution is unaffected: the source
# fails (pause/kill) before the destination starts answering.
_source_failed = threading.Event()


def _attempt(check, ep, host, port, timeout):
    t0 = time.time()
    try:
        ok = bool(check(host, port, timeout))
    except Exception:
        ok = False
    if ep == "source" and not ok:
        _source_failed.set()
    _emit(ep, t0, time.time(), ok)


def main():
    workload, src, dst = sys.argv[1:4]
    interval = float(sys.argv[4]) if len(sys.argv) > 4 else 0.05
    timeout = float(sys.argv[5]) if len(sys.argv) > 5 else 1.0
    check = {"resp": _ping_redis, "redis": _ping_redis, "line": _ping_line, "pg": _ping_pg,
             "tcp": _tcp_connect}.get(workload, _get_http)
    eps = []
    for name, addr in (("source", src), ("dest", dst)):
        host, port = addr.rsplit(":", 1)
        eps.append((name, host, int(port)))
    nxt = time.time()
    last_dest = 0.0
    while True:
        for name, host, port in eps:
            if name == "dest":
                gap = FAST_DEST_INTERVAL if _source_failed.is_set() else IDLE_DEST_INTERVAL
                if time.time() - last_dest < gap:
                    continue
            if name == "dest":
                last_dest = time.time()
            threading.Thread(target=_attempt, args=(check, name, host, port, timeout),
                             daemon=True).start()
        nxt += interval
        time.sleep(max(0.0, nxt - time.time()))


if __name__ == "__main__":
    main()
