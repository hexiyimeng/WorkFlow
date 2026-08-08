#!/usr/bin/env bash

#SBATCH --job-name=workflow-integration
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=128G
#SBATCH --gres=gpu:1
#SBATCH --time=01:00:00
#SBATCH --signal=B:TERM@90

set -euo pipefail

WORKFLOW_ROOT="${WORKFLOW_ROOT:-$HOME/apps/WorkFlow}"
WORKFLOW_RUNTIME_DIR="${WORKFLOW_RUNTIME_DIR:-$HOME/workflow-runtime}"
PYTHON="$WORKFLOW_ROOT/backend/.venv/bin/python"
ARCHIVE="${WORKFLOW_DEMO_ARCHIVE:-$WORKFLOW_RUNTIME_DIR/data/demo_images.zip}"
JOB_ID="${SLURM_JOB_ID:-manual}"
TMP_BASE="${SLURM_TMPDIR:-/tmp/$USER/workflow-integration-$JOB_ID}"

mkdir -p "$TMP_BASE/dask" "$WORKFLOW_RUNTIME_DIR/test-runs"
export PYTHONPATH="$WORKFLOW_ROOT/backend"
export TMPDIR="$TMP_BASE"
export WorkFlow_CUDA_MODE=auto
export WorkFlow_MODELS_DIR="$WORKFLOW_RUNTIME_DIR/models"
export CELLPOSE_LOCAL_MODELS_PATH="$WORKFLOW_RUNTIME_DIR/models/cellpose"
export WorkFlow_DASK_LOCAL_DIR="$TMP_BASE/dask"
export WorkFlow_DASHBOARD_ADDRESS=":0"
export WorkFlow_DASK_CLUSTER_START_TIMEOUT_SECONDS=300
export WorkFlow_DASK_CLUSTER_CLOSE_TIMEOUT_SECONDS=120
export MALLOC_TRIM_THRESHOLD_=0
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export CUDA_DEVICE_ORDER=PCI_BUS_ID

if [[ ! -f "$ARCHIVE" ]]; then
  echo "Missing actual-data archive: $ARCHIVE" >&2
  exit 1
fi

echo "__WORKFLOW_INTEGRATION_BEGIN__"
echo "job=$JOB_ID host=$(hostname -f) cuda_visible_devices=${CUDA_VISIBLE_DEVICES-}"
"$PYTHON" "$WORKFLOW_ROOT/deploy/hpc/integration_smoke.py" \
  --archive "$ARCHIVE" \
  --runtime-dir "$WORKFLOW_RUNTIME_DIR"
echo "__WORKFLOW_INTEGRATION_END__"
