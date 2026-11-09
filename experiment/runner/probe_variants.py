"""Which link probe reads the emulated 2 Mb/s x 200 ms path stably? (lab, 2026-10-26)

The runner's validity probe (iperf3 paced at the cap, -t 5 -O 2) read the emulated path bimodally
(0.48-0.7 or ~1.9 Mb/s), which made the validity gate discard good lab runs. Each variant runs
`--reps` times under the same profile; prints min/median/max of the received rate.

    python3 probe_variants.py --config hosts.lab.server.yaml --profile "rate 2mbit delay 200ms" --reps 5
"""
import argparse
import json
import statistics
import time

import run_experiment as rx

VARIANTS = {
    "a_paced_t5_O2 (current)": "-t 5 -O 2 -b {cap}M",
    "b_bulk_t5_O2": "-t 5 -O 2",
    "c_paced_t8_O4": "-t 8 -O 4 -b {cap}M",
    "d_fqrate_t5_O2": "-t 5 -O 2 --fq-rate {cap}M",
}


def run(cfg, args):
    src, dst = cfg["source"]["ssh"], cfg["destination"]["ssh"]
    dip = cfg["destination"]["public_ip"]
    rx.ssh(dst, "pkill -x iperf3; iperf3 -s -1 -D -p 5201", check=False)
    time.sleep(0.5)
    r = rx.ssh(src, f"iperf3 -c {dip} -p 5201 -J {args}", check=False, timeout=90)
    try:
        return round(json.loads(r.stdout)["end"]["sum_received"]["bits_per_second"] / 1e6, 3)
    except (ValueError, KeyError):
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="hosts.lab.server.yaml")
    ap.add_argument("--profile", default="rate 2mbit delay 200ms")
    ap.add_argument("--reps", type=int, default=5)
    a = ap.parse_args()
    cfg = rx.load_config(a.config)
    cap = rx.parse_netem_rate_mbps(a.profile)
    rx.clear_netem(cfg)
    rx.apply_netem(cfg, a.profile)
    try:
        for name, tmpl in VARIANTS.items():
            xs = [run(cfg, tmpl.format(cap=f"{cap:g}")) for _ in range(a.reps)]
            ok = [x for x in xs if x is not None]
            print(json.dumps({"variant": name, "reads": xs,
                              "min": min(ok) if ok else None, "median": statistics.median(ok) if ok else None,
                              "max": max(ok) if ok else None}), flush=True)
    finally:
        rx.clear_netem(cfg)


if __name__ == "__main__":
    main()
