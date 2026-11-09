#!/bin/bash
# Build the synthetic-workload image ONCE on the source VM and load the SAME
# image (identical image ID) onto the destination: CRIU restore requires the
# source and destination rootfs to come from the same image build, so building
# separately on each host (different IDs) would break restore.
#
#   bash workloads/synth/build_image.sh <src-ssh-alias> <dst-ssh-alias> [tag]
# Prints the image ID. Re-run is idempotent (same source -> same ID is not
# guaranteed by docker build, so re-running REPLACES the image on both hosts;
# destination templates keyed by the old ID are then unused).
set -euo pipefail
SRC=${1:?src alias}; DST=${2:?dst alias}; TAG=${3:-statepod-synth:1}
HERE=$(cd "$(dirname "$0")" && pwd)
ssh "$SRC" 'rm -rf ~/synth-build && mkdir -p ~/synth-build'
scp -q "$HERE/synth_workload.c" "$HERE/Dockerfile" "$SRC":~/synth-build/
ssh "$SRC" "cd ~/synth-build && gcc -O2 -static -o synth_workload synth_workload.c && sudo docker build -q -t $TAG . >/dev/null && sudo docker image inspect --format '{{.Id}}' $TAG"
ssh "$SRC" "sudo docker save $TAG" | ssh "$DST" "sudo docker load -q"
echo "src: $(ssh "$SRC" "sudo docker image inspect --format '{{.Id}}' $TAG")"
echo "dst: $(ssh "$DST" "sudo docker image inspect --format '{{.Id}}' $TAG")"
