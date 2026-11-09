"""Calibrate the lab emulator's bottleneck queue against the REAL path .

The one free parameter of the emulated WAN is the bottleneck queue (netem limit). It is chosen so
the lab reproduces what the real Internet path delivered to the VANILLA baseline in the
real-WAN ladder (never tuned on the system under test): the runner's validity probe (paced at the
cap, as run_experiment.iperf_probe does) and bulk transfers of the pre-dump sizes.

    python3 netem_calibrate.py --config hosts.lab.server.yaml --rate 2 --rtt 200 --limits 100,200,400,800 --reps 3

Writes results/<stamp>_netem_calibrate/calibration.jsonl. Run on the emulator host.
"""
import argparse
import datetime
import json
import statistics
import time
from pathlib import Path

import run_experiment as rx


def apply(cfg, rate, rtt, limit):
    n = cfg["netem"]
    half = rtt / 2
    for dev, spec, msrc, mdst in ((n["fwd_dev"], f"delay {half:g}ms rate {rate:g}mbit limit {limit}", n["src_ip"], n["dst_ip"]),
                                  (n["rev_dev"], f"delay {half:g}ms limit 10000", n["dst_ip"], n["src_ip"])):
        rx.on_host("local", f"tc qdisc del dev {dev} root 2>/dev/null; "
                            f"tc qdisc add dev {dev} root handle 1: prio bands 2 priomap {' '.join(['1'] * 16)} && "
                            f"tc qdisc add dev {dev} parent 1:1 handle 10: netem {spec} && "
                            f"tc filter add dev {dev} parent 1: protocol ip prio 1 u32 "
                            f"match ip src {msrc}/32 match ip dst {mdst}/32 flowid 1:1")


def iperf(cfg, args):
    src, dst = cfg["source"]["ssh"], cfg["destination"]["ssh"]
    dip, port = cfg["destination"]["public_ip"], cfg.get("experiment", {}).get("iperf_port", "5201")
    rx.ssh(dst, f"pkill -x iperf3; iperf3 -s -1 -D -p {port}", check=False)
    time.sleep(0.5)
    r = rx.ssh(src, f"iperf3 -c {dip} -p {port} -J {args}", check=False, timeout=120)
    j = json.loads(r.stdout)
    return j["end"]["sum_received"]["bits_per_second"] / 1e6, j["end"]["sum_sent"].get("retransmits", 0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="hosts.lab.server.yaml")
    ap.add_argument("--rate", type=float, default=2.0)
    ap.add_argument("--rtt", type=float, default=200.0)
    ap.add_argument("--limits", default="100,200,400,800")
    ap.add_argument("--reps", type=int, default=3)
    # bulk sizes = the vanilla real-WAN ladder's pre-dump rounds (pre0 ~3.06 MB, later rounds ~0.6 MB)
    ap.add_argument("--bulk", default="3058694,600000")
    a = ap.parse_args()
    cfg = rx.load_config(a.config)
    out = Path(cfg.get("experiment", {}).get("results_root", "../results")) / \
        f"{datetime.datetime.now():%Y-%m-%d_%H%M%S}_netem_calibrate"
    out.mkdir(parents=True, exist_ok=True)
    rows = []
    try:
        for limit in [int(x) for x in a.limits.split(",")]:
            apply(cfg, a.rate, a.rtt, limit)
            res = {"limit": limit, "rate_mbps": a.rate, "rtt_ms": a.rtt}
            probe = [iperf(cfg, f"-b {a.rate:g}M -t 3 -O 2") for _ in range(a.reps)]
            res["probe_mbps"] = [round(p, 3) for p, _ in probe]
            for b in a.bulk.split(","):
                bulk = [iperf(cfg, f"-n {b}") for _ in range(a.reps)]
                res[f"bulk_{b}_mbps"] = [round(p, 3) for p, _ in bulk]
                res[f"bulk_{b}_retrans"] = [r for _, r in bulk]
            res["probe_median"] = statistics.median(res["probe_mbps"])
            rows.append(res)
            print(json.dumps(res), flush=True)
    finally:
        rx.clear_netem(cfg)
    (out / "calibration.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
    print(f"wrote {out / 'calibration.jsonl'}")


if __name__ == "__main__":
    main()
