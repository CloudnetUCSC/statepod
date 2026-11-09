#!/usr/bin/env python3
"""Per-request SLA load client.

Generalizes probe.sh's sticky source->dest failover from tick-level OK/DOWN
into a real per-request client: issues actual GET/PING traffic at a target
rate, times each request, and logs one JSON line per request. Runs on the
source VM, driven by run_experiment.py, same deployment pattern as probe.sh
(scp + background ssh, killed with SIGTERM when the rep ends).

Why a custom client instead of wrk/redis-benchmark: both report
aggregate stats only, with no per-request key identity -- cold/warm key
attribution for M5 needs to know which key each request hit.
Throughput isn't the metric this client optimizes for; p50/p95/p99 latency
and phase-boundary attribution are, so a closed-loop single-connection
client with deterministic request order is the right shape.

Output (one JSON object per stdout/--log line):
  {"ts": <epoch.float>, "endpoint": "source"|"dest", "status": "ok"|"timeout"|"error",
   "latency_ms": <float|null>, "key": <str|null>, "key_class": <str|null>, "error": <str|null>}

`ts` is time.time() (same clock domain as migration_metrics.json's ISO
timestamps and probe.sh's `date +%s.%N`), so downstream analysis joins this
log against both without any clock translation.

Usage:
  load_client.py --workload redis --src 127.0.0.1:6380 --dst <dest_ip>:6380 \
      [--rate 20] [--duration 60] [--timeout 1.0] [--log FILE] \
      [--keyspace 5000 | --keys k1,k2,... [--key-classes warm,cold,...]]

  load_client.py --workload node --src 127.0.0.1:3000 --dst <dest_ip>:3000 \
      [--rate 20] [--duration 60]

Redis mode with no --keys/--keyspace falls back to PING (availability +
latency only, no key identity) -- same coverage as probe.sh but timestamped
per request instead of per tick.
"""

import argparse
import http.client
import itertools
import json
import socket
import sys
import time


def parse_target(s):
    host, _, port = s.rpartition(":")
    return host, int(port)


# --- Redis: minimal RESP client (GET/PING only, not a general RESP parser) ---

def resp_read_reply(f):
    """Read one RESP reply from a buffered socket file object. Returns (ok, value_or_err)."""
    line = f.readline()
    if not line:
        raise ConnectionError("connection closed")
    kind, rest = line[:1], line[1:].rstrip(b"\r\n")
    if kind == b"+":  # simple string, e.g. +PONG
        return True, rest.decode(errors="replace")
    if kind == b"-":  # error
        return False, rest.decode(errors="replace")
    if kind == b":":  # integer
        return True, rest.decode()
    if kind == b"$":  # bulk string
        n = int(rest)
        if n == -1:
            return True, None
        data = f.read(n + 2)[:n]  # payload + trailing CRLF
        return True, data
    raise ConnectionError(f"unexpected RESP type {kind!r}")


class RedisConn:
    def __init__(self, host, port, timeout):
        self.host, self.port, self.timeout = host, port, timeout
        self.sock = None
        self.f = None

    def connect(self):
        self.close()
        self.sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        self.sock.settimeout(self.timeout)
        self.f = self.sock.makefile("rb")

    def close(self):
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
        self.sock = None
        self.f = None

    def request(self, *args):
        if self.sock is None:
            self.connect()
        parts = [f"*{len(args)}\r\n".encode()]
        for a in args:
            b = a.encode() if isinstance(a, str) else a
            parts.append(f"${len(b)}\r\n".encode() + b + b"\r\n")
        self.sock.sendall(b"".join(parts))
        return resp_read_reply(self.f)


class NodeConn:
    def __init__(self, host, port, timeout):
        self.host, self.port, self.timeout = host, port, timeout
        self.conn = None

    def connect(self):
        self.close()
        self.conn = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)

    def close(self):
        if self.conn is not None:
            try:
                self.conn.close()
            except OSError:
                pass
        self.conn = None

    def request(self):
        if self.conn is None:
            self.connect()
        self.conn.request("GET", "/")
        resp = self.conn.getresponse()
        resp.read()  # drain so the connection is reusable
        if resp.status >= 400:
            return False, f"HTTP {resp.status}"
        return True, resp.status


def make_key_source(args):
    """Yields (key, key_class) forever, or (None, None) forever if no keys configured."""
    if args.keys:
        keys = args.keys.split(",")
        classes = args.key_classes.split(",") if args.key_classes else [None] * len(keys)
        if len(classes) != len(keys):
            raise SystemExit("--key-classes must have the same number of entries as --keys")
        return itertools.cycle(zip(keys, classes))
    if args.keyspace:
        # matches run_experiment.py's seeding convention: key:1 .. key:N
        keys = [f"key:{i}" for i in range(1, args.keyspace + 1)]
        return itertools.cycle((k, None) for k in keys)
    return itertools.repeat((None, None))


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--workload", choices=["redis", "node"], required=True)
    p.add_argument("--src", required=True, help="host:port, tried first each request")
    p.add_argument("--dst", required=True, help="host:port, tried on --src failure (sticky failover)")
    p.add_argument("--rate", type=float, default=20.0, help="requests/sec, closed-loop (default 20)")
    p.add_argument("--duration", type=float, default=None, help="seconds to run; omit to run until killed")
    p.add_argument("--timeout", type=float, default=1.0, help="per-request timeout in seconds (default 1.0)")
    p.add_argument("--log", default=None, help="output path (JSONL); default stdout")
    p.add_argument("--keyspace", type=int, default=0, help="redis: cycle key:1..key:N (default: PING only)")
    p.add_argument("--keys", default=None, help="redis: explicit comma-separated key list, overrides --keyspace")
    p.add_argument("--key-classes", default=None,
                    help="comma-separated labels parallel to --keys (e.g. warm,cold,...); opaque, for post-hoc M5 join")
    args = p.parse_args()

    src_host, src_port = parse_target(args.src)
    dst_host, dst_port = parse_target(args.dst)
    out = open(args.log, "w", buffering=1) if args.log else sys.stdout

    Conn = RedisConn if args.workload == "redis" else NodeConn
    conns = {
        "source": Conn(src_host, src_port, args.timeout),
        "dest": Conn(dst_host, dst_port, args.timeout),
    }
    active = "source"
    key_source = make_key_source(args)
    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    start = time.time()

    def do_request(endpoint, key):
        c = conns[endpoint]
        if args.workload == "redis":
            if key is not None:
                return c.request("GET", key)
            return c.request("PING")
        return c.request()

    try:
        while args.duration is None or (time.time() - start) < args.duration:
            tick_start = time.time()
            key, key_class = next(key_source)
            other = "dest" if active == "source" else "source"
            rec = {"ts": tick_start, "endpoint": active, "key": key, "key_class": key_class,
                   "status": "error", "latency_ms": None, "error": None}
            try:
                t0 = time.time()
                ok, val = do_request(active, key)
                rec["latency_ms"] = (time.time() - t0) * 1000.0
                rec["status"] = "ok" if ok else "error"
                if not ok:
                    rec["error"] = str(val)
            except (OSError, socket.timeout, ConnectionError) as e:
                conns[active].close()
                rec["status"] = "timeout" if isinstance(e, socket.timeout) else "error"
                rec["error"] = str(e)
                # sticky failover: try the other endpoint immediately, same tick,
                # mirroring probe.sh -- a real client reconnects, it doesn't wait
                # a full interval to notice the endpoint moved.
                try:
                    t0 = time.time()
                    ok, val = do_request(other, key)
                    rec["endpoint"] = other
                    rec["latency_ms"] = (time.time() - t0) * 1000.0
                    rec["status"] = "ok" if ok else "error"
                    rec["error"] = None if ok else str(val)
                    if ok:
                        active = other
                except (OSError, socket.timeout, ConnectionError) as e2:
                    conns[other].close()
                    rec["error"] = f"{rec['error']} | failover: {e2}"
            out.write(json.dumps(rec) + "\n")
            out.flush()  # unbuffered per-line: survives an abrupt kill (ssh session
                          # teardown, SIGTERM) same as probe.sh's tee-based logging

            elapsed = time.time() - tick_start
            if interval > elapsed:
                time.sleep(interval - elapsed)
    except KeyboardInterrupt:
        pass
    finally:
        for c in conns.values():
            c.close()
        if out is not sys.stdout:
            out.close()


if __name__ == "__main__":
    main()
