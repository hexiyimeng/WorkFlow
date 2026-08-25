#!/usr/bin/env bash

# Run this on the approved Slurm submit/service node.  It listens temporarily
# on the configured Scheduler address, asks Slurm for one compute node, and
# verifies that the compute process can connect back to that exact TCP port.
set -euo pipefail
umask 077

SOURCE_PATH="${BASH_SOURCE[0]}"
case "$SOURCE_PATH" in
  /*) ;;
  *) SOURCE_PATH="$PWD/$SOURCE_PATH" ;;
esac
SCRIPT_DIR="$(cd -- "${SOURCE_PATH%/*}" && pwd -P)"
DEFAULT_WORKFLOW_ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd -P)"
WORKFLOW_ROOT="${WORKFLOW_ROOT:-$DEFAULT_WORKFLOW_ROOT}"
WORKFLOW_RUNTIME_DIR="${WORKFLOW_RUNTIME_DIR:-${HOME:?HOME is required}/workflow-runtime}"
PYTHON="$WORKFLOW_ROOT/backend/.venv/bin/python"
PROBE="$SCRIPT_DIR/scheduler_connectivity_probe.py"
HOST="${WorkFlow_DASK_SCHEDULER_HOST:-}"
PORT="${WorkFlow_DASK_SCHEDULER_PORT:-8786}"
PARTITION="${WorkFlow_SLURM_PARTITION:-}"
SINFO_COMMAND="${WorkFlow_SLURM_SINFO:-sinfo}"
EXCLUDED_PARTITIONS=",${WorkFlow_SLURM_EXCLUDED_PARTITIONS:-control},"
PROBE_TIME="${WORKFLOW_CONNECTIVITY_PROBE_TIME:-00:10:00}"
SERVER_TIMEOUT="${WORKFLOW_CONNECTIVITY_PROBE_TIMEOUT_SECONDS:-1800}"

if [[ -z "$HOST" ]]; then
  echo "WorkFlow_DASK_SCHEDULER_HOST is required." >&2
  exit 2
fi
if [[ ! -x "$PYTHON" || ! -f "$PROBE" ]]; then
  echo "Install WorkFlow before running the connectivity probe." >&2
  exit 1
fi
if ! command -v srun >/dev/null 2>&1; then
  echo "srun is unavailable; run this probe on an approved Slurm submit host." >&2
  exit 1
fi
if [[ -z "$PARTITION" ]]; then
  if ! command -v "$SINFO_COMMAND" >/dev/null 2>&1; then
    echo "sinfo is unavailable; cannot discover a probe partition." >&2
    exit 1
  fi
  default_partition=""
  first_partition=""
  while IFS= read -r raw_partition; do
    raw_partition="${raw_partition//[[:space:]]/}"
    [[ -n "$raw_partition" ]] || continue
    candidate="${raw_partition%\*}"
    case "$EXCLUDED_PARTITIONS" in
      *",$candidate,"*) continue ;;
    esac
    [[ -n "$first_partition" ]] || first_partition="$candidate"
    if [[ "$raw_partition" == *\* ]]; then
      default_partition="$candidate"
    fi
  done < <("$SINFO_COMMAND" --noheader --format=%P)
  PARTITION="${default_partition:-$first_partition}"
  if [[ -z "$PARTITION" ]]; then
    echo "sinfo reported no eligible partition for the connectivity probe." >&2
    exit 1
  fi
fi

mkdir -p "$WORKFLOW_RUNTIME_DIR/test-runs"
RUN_DIRECTORY="$(mktemp -d "$WORKFLOW_RUNTIME_DIR/test-runs/scheduler-connectivity-XXXXXXXX")"
REQUEST="$RUN_DIRECTORY/request.json"
SERVER_LOG="$RUN_DIRECTORY/server.log"
"$PYTHON" "$PROBE" create "$REQUEST" --host "$HOST" --port "$PORT"

SERVER_PID=""
cleanup() {
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

"$PYTHON" "$PROBE" server "$REQUEST" --timeout "$SERVER_TIMEOUT" \
  >"$SERVER_LOG" 2>&1 &
SERVER_PID="$!"
for _ in {1..100}; do
  if [[ -f "$RUN_DIRECTORY/ready.json" ]]; then
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    wait "$SERVER_PID" || true
    echo "The service-node listener failed to start. See $SERVER_LOG" >&2
    exit 1
  fi
  sleep 0.1
done
if [[ ! -f "$RUN_DIRECTORY/ready.json" ]]; then
  echo "The service-node listener did not become ready. See $SERVER_LOG" >&2
  exit 1
fi

# This is a diagnostic allocation, not a production workflow.  No Worker,
# Scheduler or Driver is started on the compute node.
srun \
  --partition="$PARTITION" \
  --nodes=1 \
  --ntasks=1 \
  --cpus-per-task=1 \
  --mem=1G \
  --time="$PROBE_TIME" \
  --export=NONE \
  "$PYTHON" "$PROBE" client "$REQUEST"
wait "$SERVER_PID"
SERVER_PID=""

if [[ ! -f "$RUN_DIRECTORY/result.json" ]]; then
  echo "Probe client returned without a durable result. See $SERVER_LOG" >&2
  exit 1
fi
echo "Compute-to-service Scheduler TCP connectivity passed."
echo "partition=$PARTITION"
echo "result=$RUN_DIRECTORY/result.json"
"$PYTHON" -m json.tool "$RUN_DIRECTORY/result.json"
