# statepod

statepod is a live migration system for stateful containers over wide-area networks, built for cloud bursting from a private data centre to a public cloud. It runs on unmodified [runc](https://github.com/opencontainers/runc) and [CRIU](https://criu.org).

The destination already holds the container image. Before a migration it starts a temporary instance of that image, checkpoints it once the workload is ready, and keeps the result as a local **synthesized baseline**. The source then sends only what the baseline cannot supply:

- **Pre-copy** (`--mode synth-precopy`). The destination sends a content-hash manifest of its baseline (16 bytes per 4 KiB page). The source replaces each matching page in CRIU's pre-dump rounds with a 16-byte reference and sends the other pages in full, compressed with zstd. The writable filesystem layer moves in rsync rounds.
- **Post-copy** (`--mode synth-postcopy`). The destination restores at once with CRIU lazy-pages. Page faults on pages the baseline holds are served locally; the rest come from the source.
- **Commit rule.** The source is stopped only after the destination reports that the container runs. Otherwise the source resumes, so a failed migration never leaves the service without a running copy.

This repository contains the prototype, the workloads, the experiment runner and the dataset behind the paper's evaluation.

## Layout

| Path | Contents |
|---|---|
| `cmd/statepod`, `internal/` | The engine (Go): container lifecycle, CRIU/runc driving, baseline synthesis and manifest matching, page pipeline, transport, receiver, page server for post-copy |
| `tools/nrand` | Launch wrapper that disables ASLR for the workload process (`personality(ADDR_NO_RANDOMIZE)`) |
| `workloads/` | Workload specs and images: PostgreSQL, JVM and Python HTTP services, and the synthetic memory workload |
| `experiment/runner/` | Python runner: drives a migration over SSH, emulates the WAN with `tc netem`, runs the client probe, the lost-write client and fault injection, and runs whole campaigns (`run_campaign.py`, `campaign_plan.json`) |
| `experiment/analysis/` | Dataset assembly, statistics and the script that produces every figure and table of the paper |
| `experiment/results/_final/` | The final dataset: `runs.csv` (one row per valid run), `report.md`, and `paper/` (the generated figure data, tables and the list of every number quoted in the paper) |

## Requirements

Two Linux hosts (source and destination) with:

- Linux 5.x or later, root (passwordless sudo for the runner)
- runc 1.3.4 and CRIU 4.2. Use the same versions on both hosts.
- Go 1.24 and gcc (to build), Docker (only to pull and export images), iproute2, rsync
- The same container image on both hosts, pinned by digest. Bundles from different image builds make CRIU restore fail.
- Synchronized clocks (NTP); downtime is computed across hosts.

The runner needs Python 3 (standard library only), plus `iperf3` on both hosts.

## Build

```bash
make build          # bin/statepod and bin/nrand
go test ./internal/...
```

## Migrating a container

On both hosts, create the container (the destination needs the same image and configuration):

```bash
sudo ./bin/statepod create --redis --stateful --deterministic
```

On the destination, synthesize the baseline once per image, then start the receiver:

```bash
sudo ./bin/statepod template generate --redis
sudo ./bin/statepod start --source <source-ip> --listen 0.0.0.0:8080 --tmpfs-checkpoint   # receiver mode
```

On the source, start the container and migrate it:

```bash
sudo ./bin/statepod start
sudo ./bin/statepod migrate --dest <destination-ip> --mode synth-precopy \
    --precopy-rounds 4 --zstd-level 1 --tmpfs-checkpoint
```

`--workload <spec.json>` takes a workload spec from `workloads/` instead of `--redis`. `statepod migrate --help` lists all modes, including the baselines used in the evaluation (`precopy`, `criu-incr`, `criu-lazy`, `sync`).

## Methods in the paper

The exact flags of every method are the presets in `experiment/runner/run_campaign.py` (`VARIANTS`):

| Paper | Preset | Mode |
|---|---|---|
| Vanilla pre-copy + zstd | `vanilla-zstd` | `precopy --zstd-level 1` (4 pre-dump rounds) |
| CRIU lazy-pages | `criu-lazy` | `criu-lazy` |
| statepod pre-copy | `m4` | `synth-precopy --precopy-rounds 4 --zstd-level 1 --tmpfs-checkpoint` |
| statepod pre-copy, merged cut-over | `m4-merge` | as above plus `--merge-restore` |
| statepod post-copy | `m5` | `synth-postcopy` |

## Reproducing the evaluation

1. Copy `experiment/runner/hosts.example.yaml` to `hosts.yaml` and fill in the two hosts.
2. Run one migration: `python3 experiment/runner/run_experiment.py --config hosts.yaml --name test --mode synth-precopy`.
3. Run the campaign. Each stage of `experiment/runner/campaign_plan.json` is one `run_campaign.py` call with that stage's preset, profiles, rounds and timeout, for example:
   ```bash
   python3 experiment/runner/run_campaign.py --config hosts.yaml --campaign ladder        --preset ladder --profiles P1 --rounds 10 --timeout 1800
   ```
   Cells are interleaved round by round, and a run is discarded when the iperf3 bandwidth check before or after it falls below 90 % of the emulated cap. `--list` shows the cells of a preset.

`experiment/results/_final/runs.csv` holds the 1,346 valid runs of the paper. `experiment/results/_final/paper/` holds the data of every figure and table, as generated by `experiment/analysis/paper_data.py`; `facts.md` lists every number quoted in the text together with the cell it comes from. `paper_data.py` additionally reads the raw per-run logs, which are not included because of their size; they are available on request.

## Security note

Matching needs a deterministic memory layout, so migratable containers are launched with ASLR disabled inside the container (`--deterministic`); host ASLR is unchanged. statepod's own transport neither authenticates nor encrypts. Run migrations inside a tunnel such as WireGuard, as in our evaluation.

## License

Apache License 2.0, see `LICENSE`.
