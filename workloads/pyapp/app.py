"""Python workload for the migration evaluation: the same HTTP protocol and
deterministic in-memory cache as workloads/jvm (so the runner's JVM state check works
unchanged), but on CPython + glibc malloc/pymalloc: a third runtime and allocator.

  --entries N   cache entries created at start (default 20000)
  --value-kb K  size of each value in KiB (default 4)
  --port P      HTTP port (default 8000)
  --upstream H:P  hold ONE long-lived TCP connection to an external service: send
                  "hb <n>\n" every 0.2 s, expect the echo; on any error reconnect (0.2 s retry).
                  Default off, so the existing pyapp cells are unchanged.

  GET /health, /      -> "ok"
  GET /count          -> number of entries
  GET /checksum       -> "<entries> <crc32 of every key and value> <writes>" (full recompute;
                          "MISMATCH@i" if a value differs from its generating function)
  GET /put?i=N        -> rewrites entry N with the next generation, returns it
  GET /outbound       -> JSON of the upstream connection's own view (connects, errors, last events)
Listening starts AFTER the cache is built (readiness = /health answers).
"""
import argparse
import json
import socket
import sys
import threading
import time
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

M = (1 << 64) - 1
cache, gens = {}, {}
writes = 0
lock = threading.Lock()
value_bytes = 4096


def value(i, gen):
    s = ((i * 0x9E3779B97F4A7C15) ^ (gen << 32) ^ 0xD1B54A32D192ED03) & M
    out = bytearray()
    while len(out) < value_bytes:
        s ^= (s << 13) & M
        s ^= s >> 7
        s ^= (s << 17) & M
        out += s.to_bytes(8, "little")
    return bytes(out[:value_bytes])


def checksum():
    crc = 0
    n = len(cache)
    for i in range(n):
        v = cache[i]
        crc = zlib.crc32(i.to_bytes(4, "little"), crc)
        crc = zlib.crc32(v, crc)
        if v != value(i, gens[i]):
            return f"{i + 1} MISMATCH@{i} {writes}"
    return f"{n} {crc} {writes}"


outbound = {"connects": 0, "errors": 0, "events": []}


def upstream_loop(host, port):
    """The application's side of the external connection: what a real service client does."""
    n = 0
    while True:
        try:
            c = socket.create_connection((host, port), timeout=2)
            c.settimeout(2)
            outbound["connects"] += 1
            outbound["events"].append(["connected", time.time()])
            f = c.makefile("rwb")
            while True:
                n += 1
                f.write(f"hb {n}\n".encode())
                f.flush()
                line = f.readline()
                if not line:
                    raise EOFError("peer closed")
                time.sleep(0.2)
        except Exception as e:                       # reset, EOF, timeout, unreachable: all just reconnect
            outbound["errors"] += 1
            outbound["events"].append(["error:" + type(e).__name__, time.time()])
            del outbound["events"][:-60]
            time.sleep(0.2)


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def reply(self, body):
        b = body.encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        global writes
        u = urlparse(self.path)
        if u.path in ("/", "/health"):
            return self.reply("ok")
        if u.path == "/count":
            return self.reply(str(len(cache)))
        if u.path == "/checksum":
            return self.reply(checksum())
        if u.path == "/outbound":
            return self.reply(json.dumps(outbound))
        if u.path == "/put":
            i = int(parse_qs(u.query)["i"][0]) % len(cache)
            with lock:
                g = gens[i] + 1
                cache[i] = value(i, g)
                gens[i] = g
                writes += 1
            return self.reply(str(g))
        self.send_error(404)


def main():
    global value_bytes
    ap = argparse.ArgumentParser()
    ap.add_argument("--entries", type=int, default=20000)
    ap.add_argument("--value-kb", type=int, default=4)
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--upstream", default="")
    a = ap.parse_args()
    value_bytes = a.value_kb * 1024
    for i in range(a.entries):
        cache[i] = value(i, 0)
        gens[i] = 0
    srv = ThreadingHTTPServer(("0.0.0.0", a.port), H)
    if a.upstream:
        h, _, pt = a.upstream.rpartition(":")
        threading.Thread(target=upstream_loop, args=(h, int(pt)), daemon=True).start()
    print(f"python workload ready: {a.entries} entries x {a.value_kb} KiB on :{a.port}", file=sys.stderr, flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
