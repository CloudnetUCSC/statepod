"""Verify emulated-WAN profiles on the lab testbed before any campaign .

For each profile: apply it exactly as run_experiment.py does, then measure from the source VM
  - RTT to the destination VM (ping, 20 samples)          -> should be base RTT + profile delay
  - goodput source -> destination (iperf3, -O 2, 8 s)    -> should be ~ the rate cap
  - retransmits                                           -> a lossy emulator shows up here
  - control-plane latency (ssh round trip to the dst VM)  -> must NOT include the profile delay
and clear it. Writes one JSON line per profile to results/<stamp>_netem_check/profiles.jsonl.

    python netem_check.py --config hosts.lab.yaml "rate 2mbit delay 200ms" "rate 200mbit delay 20ms"
"""
import argparse
import datetime
import json
import re
import time
from pathlib import Path

import run_experiment as rx


def measure(cfg, secs):
    src, dst = cfg["source"]["ssh"], cfg["destination"]["ssh"]
    dip = cfg["destination"]["public_ip"]
    port = cfg.get("experiment", {}).get("iperf_port", "5201")
    out = {}
    p = rx.ssh(src, f"ping -c 20 -i 0.2 {dip} | tail -1", check=False).stdout
    m = re.search(r"= ([0-9.]+)/([0-9.]+)/([0-9.]+)/([0-9.]+)", p)
    out["rtt_ms"] = dict(zip(("min", "avg", "max", "mdev"), map(float, m.groups()))) if m else p.strip()
    rx.ssh(dst, f"pkill -x iperf3; iperf3 -s -1 -D -p {port}", check=False)
    time.sleep(1)
    r = rx.ssh(src, f"iperf3 -c {dip} -p {port} -t {secs} -O 2 -J", check=False, timeout=secs + 60)
    try:
        j = json.loads(r.stdout)
        out["goodput_mbps"] = round(j["end"]["sum_received"]["bits_per_second"] / 1e6, 3)
        out["retransmits"] = j["end"]["sum_sent"].get("retransmits")
    except (ValueError, KeyError):
        out["iperf_error"] = (r.stdout or r.stderr)[-300:]
    t = time.time()
    rx.ssh(dst, "true", check=False)
    out["ssh_roundtrip_s"] = round(time.time() - t, 3)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="hosts.lab.yaml")
    ap.add_argument("--secs", type=int, default=8)
    ap.add_argument("profiles", nargs="+")
    a = ap.parse_args()
    cfg = rx.load_config(a.config)
    stamp = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    outdir = Path(cfg.get("experiment", {}).get("results_root", "../results")) / f"{stamp}_netem_check"
    outdir.mkdir(parents=True, exist_ok=True)
    rows = [{"profile": "unshaped", **measure(cfg, a.secs)}]
    print(json.dumps(rows[0]))
    for prof in a.profiles:
        rx.clear_netem(cfg)
        plan = rx.apply_netem(cfg, prof)
        try:
            row = {"profile": prof, "cap_mbps": rx.parse_netem_rate_mbps(prof), "applied": plan, **measure(cfg, a.secs)}
        finally:
            rx.clear_netem(cfg)
        rows.append(row)
        print(json.dumps({k: v for k, v in row.items() if k != "applied"}))
    (outdir / "profiles.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
    print(f"wrote {outdir / 'profiles.jsonl'}")


if __name__ == "__main__":
    main()
