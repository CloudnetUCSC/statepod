# Experiment runner

Runs migration experiments end-to-end over SSH and writes each run to a timestamped directory under `experiment/results/`. Python 3, standard library only.

## Setup

1. Copy `hosts.example.yaml` to `hosts.yaml` and fill in the source and destination hosts.
2. Set up passwordless SSH to both hosts, and passwordless sudo on both for `runc`, `criu` and `tc`. One key for all hosts can be set with `ssh_key:` in the config or `--ssh-key`.
3. Synchronize the clocks of both hosts (NTP). Downtime is computed across hosts.
4. Build statepod on both hosts (`make build`) and pull the same image digests on both.

## One migration

```bash
./run_experiment.py --config hosts.yaml --name test --mode synth-precopy \
    --netem "rate 10mbit delay 100ms"
```

The run directory contains:
- `manifest.json`: kernel, CRIU/runc versions, netem, clock skew, and the bandwidth check before and after the run;
- `migration_metrics.json`: the engine's phase timeline, downtime and bytes;
- `probe.log`: the client probe, from which the client-observed outage is computed;
- the logs of both sides.

## Campaigns

`run_campaign.py` runs a preset of cells (workload × method × network profile), interleaving the cells round by round. It keeps a ledger, so an interrupted campaign resumes where it stopped. A run is valid only if the iperf3 bandwidth check before and after it reaches 90 % of the emulated cap. The `VARIANTS` table defines each method, and `campaign_plan.json` lists the stages of the paper's campaign.

## Client probe

`probe.py` runs on the source host and sends a request every 50 ms to the workload's forwarded port. It switches to the destination once the source stops answering. The client-observed outage is the gap between the last answer from the source and the first answer from the destination.

## Fault injection and lost writes

`faults.py` injects loss, partitions and process kills at defined points of a migration. `lostwrite_client.py` logs every acknowledged write. After the migration, the runner counts the acknowledged writes that are missing on the destination.
