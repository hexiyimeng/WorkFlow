#!/usr/bin/env bash

#SBATCH --job-name=workflow-server
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=128G
#SBATCH --gres=gpu:1
#SBATCH --time=1-00:00:00
#SBATCH --signal=B:TERM@90

set -euo pipefail

WORKFLOW_ROOT="${WORKFLOW_ROOT:-$HOME/apps/WorkFlow}"
WORKFLOW_RUNTIME_DIR="${WORKFLOW_RUNTIME_DIR:-$HOME/workflow-runtime}"
PYTHON="$WORKFLOW_ROOT/backend/.venv/bin/python"

if [[ ! -x "$PYTHON" ]]; then
  echo "Missing WorkFlow Python environment: $PYTHON" >&2
  exit 1
fi

choose_free_port() {
  "$PYTHON" - <<'PY'
import socket

with socket.socket() as sock:
    sock.bind(("", 0))
    print(sock.getsockname()[1])
PY
}

WEB_PORT="${WORKFLOW_WEB_PORT:-$(choose_free_port)}"
DASHBOARD_PORT="${WORKFLOW_DASHBOARD_PORT:-$(choose_free_port)}"
if [[ "$WEB_PORT" == "$DASHBOARD_PORT" ]]; then
  DASHBOARD_PORT="$(choose_free_port)"
fi

NODE_NAME="$(hostname -f)"
JOB_ID="${SLURM_JOB_ID:-manual}"
TMP_BASE="${SLURM_TMPDIR:-/tmp/$USER/workflow-$JOB_ID}"
mkdir -p \
  "$TMP_BASE/dask" \
  "$WORKFLOW_RUNTIME_DIR/logs" \
  "$WORKFLOW_RUNTIME_DIR/state" \
  "$WORKFLOW_RUNTIME_DIR/models" \
  "$WORKFLOW_RUNTIME_DIR/output" \
  "$WORKFLOW_RUNTIME_DIR/recovery"

export PYTHONPATH="$WORKFLOW_ROOT/backend"
export TMPDIR="$TMP_BASE"
export WorkFlow_CUDA_MODE="auto"
export WorkFlow_MODELS_DIR="$WORKFLOW_RUNTIME_DIR/models"
export CELLPOSE_LOCAL_MODELS_PATH="$WORKFLOW_RUNTIME_DIR/models/cellpose"
export WorkFlow_DASK_LOCAL_DIR="$TMP_BASE/dask"
export WorkFlow_DASHBOARD_ADDRESS=":$DASHBOARD_PORT"
export WorkFlow_DASHBOARD_HOST="http://localhost:$DASHBOARD_PORT"
export WorkFlow_DASK_CLUSTER_START_TIMEOUT_SECONDS="${WorkFlow_DASK_CLUSTER_START_TIMEOUT_SECONDS:-300}"
export WorkFlow_DASK_CLUSTER_CLOSE_TIMEOUT_SECONDS="${WorkFlow_DASK_CLUSTER_CLOSE_TIMEOUT_SECONDS:-120}"
export MALLOC_TRIM_THRESHOLD_=0
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
export MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"
export CUDA_DEVICE_ORDER="PCI_BUS_ID"

COMMIT="$(git -C "$WORKFLOW_ROOT" rev-parse HEAD)"
STATE_FILE="$WORKFLOW_RUNTIME_DIR/state/server-$JOB_ID.env"
CURRENT_FILE="$WORKFLOW_RUNTIME_DIR/state/current-server.env"
STATE_TMP="$STATE_FILE.tmp"
umask 077
printf '%s\n' \
  "WORKFLOW_JOB_ID=$JOB_ID" \
  "WORKFLOW_NODE=$NODE_NAME" \
  "WORKFLOW_WEB_PORT=$WEB_PORT" \
  "WORKFLOW_DASHBOARD_PORT=$DASHBOARD_PORT" \
  "WORKFLOW_COMMIT=$COMMIT" \
  "WORKFLOW_ROOT=$WORKFLOW_ROOT" \
  > "$STATE_TMP"
mv "$STATE_TMP" "$STATE_FILE"
cp "$STATE_FILE" "$CURRENT_FILE"

echo "__WORKFLOW_SERVER_READY_METADATA__"
cat "$STATE_FILE"
echo "Tunnel from the client through mn02:"
echo "ssh -N -L $WEB_PORT:$NODE_NAME:$WEB_PORT -L $DASHBOARD_PORT:$NODE_NAME:$DASHBOARD_PORT songzh@10.200.201.2"
echo "Then open http://localhost:$WEB_PORT"

cd "$WORKFLOW_ROOT/backend"
exec "$PYTHON" -m uvicorn main:app \
  --host 0.0.0.0 \
  --port "$WEB_PORT" \
  --workers 1
