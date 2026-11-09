#!/usr/bin/env python3
"""Link-floor benchmark: measure the source->destination
path BEFORE choosing netem caps or a campaign window.

Every `--interval-min` minutes for `--hours` it records
  * upload goodput  (iperf3 source -> destination, the migration direction;
                     `recv_mbps` is what the destination actually received),
  * TCP connect RTT (source -> destination:probe_port, median of 10),
and writes one JSON line per sample plus a summary with the numbers the
campaign rules need:
  * floor  = 5th-percentile upload goodput over the window,
  * suggested caps = fractions of the floor, so a cap is never above what the
    link can deliver (netem can only lower bandwidth, so a cap
    above the achievable rate is silently not applied -- the July failure).

    python link_benchmark.py --config hosts.yaml --hours 2 --interval-min 5
    python link_benchmark.py --config hosts.yaml --hours 0.05 --interval-min 1   # smoke test

Output: experiment/results/_linkbench_<stamp>/{samples.jsonl,summary.json}.
Uses the same iperf3 mechanism as run_experiment.py's pre/post probe
(single-use server on the destination), 5 s per sample by default.
"""
import argparse
import datetime
import json
import statistics
import sys
import time
from pathlib import Path

import run_experiment as rx


def rtt_ms(cfg, n=10):
    """Median application-level round trip (1-byte echo) source -> destination
    over the same path the migration uses (the host relay, when configured).

    A bare TCP connect cannot be used: through a NAT or port relay the
    connect can complete on the local hop, long before the relay dials the
    WAN, so it would measure ~0 ms. Instead a one-shot echo server is started on
    the destination on the iperf port (free between probes) and the source
    times n one-byte ping-pongs on one connection; the first (which includes
    the relay's upstream connect) is dropped. Failures give None, never a number."""
    import base64
    exp = cfg.get("experiment", {})
    port = exp.get("iperf_port", "5201")
    client_port = exp.get("iperf_client_port", port)
    server = f"""
import socket
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("0.0.0.0", {port}))
s.listen(1)
s.settimeout(25)
c, _ = s.accept()
c.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
while True:
    d = c.recv(1)
    if not d:
        break
    c.sendall(d)
"""
    client = f"""
import socket, time, statistics
r = []
try:
    s = socket.create_connection(("{rx.dest_public_ip(cfg)}", {client_port}), timeout=5)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    s.settimeout(5)
    for i in range({n} + 1):
        t = time.time()
        s.sendall(b"x")
        s.recv(1)
        if i:
            r.append((time.time() - t) * 1000)
    s.close()
except Exception:
    pass
print(round(statistics.median(r), 1) if r else "nan")
"""
    sb64 = base64.b64encode(server.encode()).decode()
    cb64 = base64.b64encode(client.encode()).decode()
    rx.ssh(cfg["destination"]["ssh"],
           f"pkill -x iperf3 >/dev/null 2>&1; "
           f"nohup python3 -c \"$(echo {sb64} | base64 -d)\" >/dev/null 2>&1 & sleep 0.7",
           check=False, timeout=15)
    out = rx.try_out(cfg["source"]["ssh"], f"echo {cb64} | base64 -d | python3")
    try:
        v = float(out.splitlines()[-1])
        return None if v != v else v
    except (ValueError, IndexError):
        return None


def percentile(xs, q):
    xs = sorted(xs)
    if not xs:
        return None
    k = (len(xs) - 1) * q
    lo, hi = int(k), min(int(k) + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="hosts.yaml")
    ap.add_argument("--hours", type=float, default=2.0)
    ap.add_argument("--interval-min", type=float, default=5.0)
    ap.add_argument("--ssh-key", default=None)
    args = ap.parse_args()

    cfg = rx.load_config(args.config)
    key = args.ssh_key or cfg.get("ssh_key", "")
    if key:
        rx.SSH_OPTS.extend(["-i", str(Path(key).expanduser())])

    stamp = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    out_dir = Path(__file__).resolve().parent.parent / "results" / f"_linkbench_{stamp}"
    out_dir.mkdir(parents=True, exist_ok=True)
    samples_path = out_dir / "samples.jsonl"
    print(f"link benchmark -> {out_dir}")

    end = time.time() + args.hours * 3600
    samples = []
    with open(samples_path, "w", buffering=1) as f:
        while True:
            t0 = time.time()
            probe = rx.iperf_probe(cfg, "linkbench")
            probe["rtt_ms_median"] = rtt_ms(cfg)
            samples.append(probe)
            f.write(json.dumps(probe) + "\n")
            print(f"  {probe['timestamp'][11:19]}  send {probe.get('send_mbps')}  "
                  f"recv {probe.get('recv_mbps')} Mbit/s  rtt {probe['rtt_ms_median']} ms"
                  + (f"  ERR {probe['error']}" if 'error' in probe else ""))
            if time.time() + args.interval_min * 60 > end:
                break
            time.sleep(max(0.0, args.interval_min * 60 - (time.time() - t0)))

    recv = [s["recv_mbps"] for s in samples if s.get("recv_mbps") is not None]
    rtt = [s["rtt_ms_median"] for s in samples if s.get("rtt_ms_median") is not None]
    floor = percentile(recv, 0.05)
    summary = {
        "samples": len(samples),
        "failed": len(samples) - len(recv),
        "upload_recv_mbps": {
            "min": min(recv, default=None), "p5": floor,
            "median": statistics.median(recv) if recv else None,
            "p95": percentile(recv, 0.95), "max": max(recv, default=None),
        },
        "rtt_ms": {"median": statistics.median(rtt) if rtt else None,
                   "max": max(rtt, default=None)},
        # Caps strictly below the floor, so every capped run can meet the
        # >=90% validity gate. Round down to 0.1 Mbit/s.
        "suggested_caps_mbps": ([round(floor * f - 0.05, 1) for f in (0.8, 0.5, 0.25)]
                                if floor else []),
        "note": "p5 of destination-received goodput; caps are fractions of it",
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    sys.exit(main())
