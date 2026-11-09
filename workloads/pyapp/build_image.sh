#!/bin/bash
# bash workloads/pyapp/build_image.sh <src-ssh-alias> <dst-ssh-alias> [tag]
# Builds the image on the source VM and loads the SAME image on the destination.
set -euo pipefail
SRC=${1:?src alias}; DST=${2:?dst alias}; TAG=${3:-statepod-pyapp:1}
HERE=$(cd "$(dirname "$0")" && pwd)
ssh "$SRC" 'rm -rf ~/pyapp-build && mkdir -p ~/pyapp-build'
scp -q "$HERE/app.py" "$HERE/Dockerfile" "$SRC":~/pyapp-build/
ssh "$SRC" "cd ~/pyapp-build && sudo docker build -q -t $TAG . >/dev/null && sudo docker image inspect --format '{{.Id}}' $TAG"
ssh "$SRC" "sudo docker save $TAG" | ssh "$DST" "sudo docker load -q"
echo "src: $(ssh "$SRC" "sudo docker image inspect --format '{{.Id}}' $TAG")"
echo "dst: $(ssh "$DST" "sudo docker image inspect --format '{{.Id}}' $TAG")"
