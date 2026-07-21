#!/usr/bin/env bash
# Launch a local Agora cluster (1 coordinator + 3 workers) as background
# processes without Docker. Logs stream to ./.run/*.log and PIDs are tracked so
# the script can tear the cluster down cleanly.
#
#   scripts/run_local.sh up      # build + start cluster
#   scripts/run_local.sh down    # stop everything
#   scripts/run_local.sh status  # curl the coordinator /status
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="${ROOT_DIR}/.run"
BIN_DIR="${ROOT_DIR}/bin"

COORD_HTTP=":8080"
COORD_GRPC=":9000"

mkdir -p "${RUN_DIR}" "${BIN_DIR}"

build() {
  echo "==> building binaries"
  (cd "${ROOT_DIR}" && go build -o "${BIN_DIR}/agora-coordinator" ./cmd/coordinator)
  (cd "${ROOT_DIR}" && go build -o "${BIN_DIR}/agora-worker" ./cmd/worker)
}

start_proc() {
  local name="$1"; shift
  "$@" >"${RUN_DIR}/${name}.log" 2>&1 &
  echo $! >"${RUN_DIR}/${name}.pid"
  echo "    started ${name} (pid $(cat "${RUN_DIR}/${name}.pid"))"
}

up() {
  build
  echo "==> starting coordinator"
  AGORA_NODE_ID=coordinator AGORA_HTTP="${COORD_HTTP}" AGORA_GRPC="${COORD_GRPC}" \
    AGORA_FANOUT_MS=200 AGORA_HEARTBEAT_TTL_MS=3000 \
    start_proc coordinator "${BIN_DIR}/agora-coordinator"
  sleep 1

  echo "==> starting workers"
  local i
  for i in 0 1 2; do
    AGORA_NODE_ID="worker-${i}" \
      AGORA_LISTEN=":910${i}" AGORA_ADVERTISE="127.0.0.1:910${i}" \
      AGORA_SHARD="${ROOT_DIR}/data/shard_${i}.json" \
      AGORA_COORDINATOR="127.0.0.1:9000" AGORA_HEARTBEAT_MS=1000 \
      start_proc "worker-${i}" "${BIN_DIR}/agora-worker"
  done

  echo "==> cluster up. logs in ${RUN_DIR}"
  echo "    try: curl \"http://localhost:8080/search?q=climate+treaty&k=5\""
}

down() {
  echo "==> stopping cluster"
  local f pid
  for f in "${RUN_DIR}"/*.pid; do
    [ -e "$f" ] || continue
    pid="$(cat "$f")"
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      echo "    stopped $(basename "$f" .pid) (pid ${pid})"
    fi
    rm -f "$f"
  done
}

status() {
  curl -s "http://localhost:8080/status" || true
  echo
}

case "${1:-up}" in
  up) up ;;
  down) down ;;
  status) status ;;
  *) echo "usage: $0 {up|down|status}" >&2; exit 1 ;;
esac
