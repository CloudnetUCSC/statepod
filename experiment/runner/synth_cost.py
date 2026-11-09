"""Cost of synthesizing the destination baseline .

The examiner's question: is the destination-side baseline a waste of resources? This measures
what one synthesis costs, per workload, on the destination VM: wall time (cold start -> CRIU dump
at readiness -> page hashes), CPU seconds and peak RSS of the whole `template generate` tree
(/usr/bin/time -v), the template's size on disk and its manifest's size (the only bytes that
cross the WAN before a migration). With the bytes a migration saves, this gives the break-even
number of migrations and whether synthesizing ON DEMAND (at the migration trigger, overlapped
with the first pre-dump round) would hide entirely behind that round's WAN transfer.

Each repetition removes the stored template first, so every number is a cold synthesis. Never
run while a campaign uses the testbed (it rebuilds the templates the campaign relies on).

    python3 synth_cost.py --config hosts.lab.server.yaml --workloads redis,postgres,pyapp,jvm,synth --reps 3
"""
import argparse
import datetime
import json
import re
from pathlib import Path

import run_experiment as rx
import workloads


def parse_time_v(text):
    def grab(label):
        m = re.search(rf"{re.escape(label)}:\s*([0-9.:]+)", text)
        return m.group(1) if m else None
    wall = grab("Elapsed (wall clock) time (h:mm:ss or m:ss)")
    secs = None
    if wall:
        parts = [float(p) for p in wall.split(":")]
        secs = sum(p * 60 ** i for i, p in enumerate(reversed(parts)))
    u, s = grab("User time (seconds)"), grab("System time (seconds)")
    rss = grab("Maximum resident set size (kbytes)")
    return {"wall_s": secs, "cpu_s": (float(u) + float(s)) if u and s else None,
            "max_rss_mb": round(int(rss) / 1024, 1) if rss else None}


def one(cfg, args, wl_name):
    args.workload = wl_name
    wl = workloads.get(wl_name)
    dst, wd, pod = cfg["destination"]["ssh"], cfg["destination"]["workdir"], cfg["destination"]["statepod"]
    rx.setup_containers(cfg, args)  # fresh bundles from the pinned image: the template key comes from them
    key = rx.ssh(dst, f"cd {wd} && POD=$(readlink -f {pod}) && $POD template key", check=False, timeout=30)
    digest = key.stdout.strip().splitlines()[-1].replace("sha256:", "")
    store = f"/root/.statepod/templates/{digest}"
    rx.ssh(dst, f"sudo rm -rf {store}", check=False)
    image, flags = wl.image(cfg), wl.flags(rx, cfg, args)
    r = rx.ssh(dst, f"cd {wd} && POD=$(readlink -f {pod}) && sudo /usr/bin/time -v -o /tmp/synth_time.txt "
                    f"$POD template generate '{image}' {flags} >/tmp/synth_out.txt 2>&1; echo rc=$?; "
                    f"cat /tmp/synth_time.txt", check=False, timeout=1200)
    row = {"workload": wl_name, "dataset": args.dataset, "image": image, **parse_time_v(r.stdout),
           "rc": (re.search(r"rc=(\d+)", r.stdout) or [None, None])[1]}
    sizes = rx.ssh(dst, f"sudo du -sb {store} | cut -f1; sudo stat -c %s {store}/manifest.spm", check=False).stdout.split()
    if len(sizes) == 2:
        row["template_mb"] = round(int(sizes[0]) / 1e6, 2)
        row["manifest_kb"] = round(int(sizes[1]) / 1e3, 1)
    return row


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="hosts.lab.server.yaml")
    ap.add_argument("--workloads", default="redis,postgres,pyapp,jvm,synth")
    ap.add_argument("--dataset", default="medium")
    ap.add_argument("--reps", type=int, default=3)
    a = ap.parse_args()
    cfg = rx.load_config(a.config)
    # the runner's own argument defaults, so workload flags resolve exactly as in a run
    args = rx.build_arg_parser().parse_args(["--config", a.config, "--name", "synth_cost", "--dataset", a.dataset])
    out = Path(cfg.get("experiment", {}).get("results_root", "../results")) / \
        f"{datetime.datetime.now():%Y-%m-%d_%H%M%S}_synth_cost"
    out.mkdir(parents=True, exist_ok=True)
    rows = []
    for wl in a.workloads.split(","):
        for rep in range(1, a.reps + 1):
            row = {"rep": rep, **one(cfg, args, wl)}
            rows.append(row)
            print(json.dumps(row), flush=True)
    (out / "synth_cost.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
    print(f"wrote {out / 'synth_cost.jsonl'}")


if __name__ == "__main__":
    main()
