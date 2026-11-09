#!/usr/bin/env python3
"""External service for the outbound-connection test. Runs on the DESTINATION
host, outside any migrating container: a line-echo TCP server that logs, per connection,
who connected (peer address as THIS service sees it), when, how many heartbeats arrived
and how the connection ended. One JSON line per event; `t` = time.time() on this host
(NTP-synced like the other hosts).

  {"event": "conn",   "peer": "ip:port", "t": ...}
  {"event": "closed", "peer": "ip:port", "t": ..., "hb": N, "last_hb_t": ..., "reason": "eof|reset|timeout"}

  upstream_service.py --port 8730
"""
import argparse
import json
import socket
import threading
import time


def emit(**kw):
    print(json.dumps(kw), flush=True)


def handle(conn, peer):
    peer = f"{peer[0]}:{peer[1]}"
    emit(event="conn", peer=peer, t=time.time())
    conn.settimeout(5)
    n, last, reason = 0, None, "eof"
    try:
        f = conn.makefile("rwb")
        while True:
            line = f.readline()
            if not line:
                break
            n += 1
            last = time.time()
            f.write(line)
            f.flush()
    except socket.timeout:
        reason = "timeout"
    except OSError:
        reason = "reset"
    finally:
        emit(event="closed", peer=peer, t=time.time(), hb=n, last_hb_t=last, reason=reason)
        conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8730)
    a = ap.parse_args()
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", a.port))
    srv.listen(16)
    emit(event="listening", port=a.port, t=time.time())
    while True:
        c, p = srv.accept()
        threading.Thread(target=handle, args=(c, p), daemon=True).start()


if __name__ == "__main__":
    main()
