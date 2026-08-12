#!/usr/bin/env bash

set -euo pipefail

SOURCE_PATH="${BASH_SOURCE[0]}"
case "$SOURCE_PATH" in
  /*) ;;
  *) SOURCE_PATH="$PWD/$SOURCE_PATH" ;;
esac
SCRIPT_DIR="$(cd -- "${SOURCE_PATH%/*}" && pwd -P)"
DEFAULT_WORKFLOW_ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd -P)"

WORKFLOW_ROOT="${WORKFLOW_ROOT:-$DEFAULT_WORKFLOW_ROOT}"
if [[ -z "${WORKFLOW_RUNTIME_DIR:-}" ]]; then
  if [[ -z "${HOME:-}" ]]; then
    echo "Set WORKFLOW_RUNTIME_DIR when HOME is unavailable." >&2
    exit 2
  fi
  WORKFLOW_RUNTIME_DIR="$HOME/workflow-runtime"
fi

PYTHON="$WORKFLOW_ROOT/backend/.venv/bin/python"
EXECUTION_SCRIPT="${WorkFlow_SLURM_EXECUTION_SCRIPT:-$WORKFLOW_ROOT/deploy/hpc/slurm/workflow_execution.sbatch}"
WEB_PORT="${WORKFLOW_WEB_PORT:-8000}"

case "$WORKFLOW_ROOT" in
  /*) ;;
  *) echo "WORKFLOW_ROOT must be an absolute path." >&2; exit 2 ;;
esac
case "$WORKFLOW_RUNTIME_DIR" in
  /*) ;;
  *) echo "WORKFLOW_RUNTIME_DIR must be an absolute path." >&2; exit 2 ;;
esac
case "$EXECUTION_SCRIPT" in
  /*) ;;
  *) echo "WorkFlow_SLURM_EXECUTION_SCRIPT must be an absolute path." >&2; exit 2 ;;
esac
if [[ ! "$WEB_PORT" =~ ^[0-9]+$ ]] || (( WEB_PORT < 1 || WEB_PORT > 65535 )); then
  echo "WORKFLOW_WEB_PORT must be an integer between 1 and 65535." >&2
  exit 2
fi
if [[ ! -x "$PYTHON" ]]; then
  echo "Missing WorkFlow Python environment: $PYTHON" >&2
  echo "Run deploy/hpc/install.sh first." >&2
  exit 1
fi
if [[ ! -f "$EXECUTION_SCRIPT" ]]; then
  echo "Missing Slurm execution script: $EXECUTION_SCRIPT" >&2
  exit 1
fi
SBATCH_COMMAND="${WorkFlow_SLURM_SBATCH:-sbatch}"
SQUEUE_COMMAND="${WorkFlow_SLURM_SQUEUE:-squeue}"
SACCT_COMMAND="${WorkFlow_SLURM_SACCT:-sacct}"
SCANCEL_COMMAND="${WorkFlow_SLURM_SCANCEL:-scancel}"
for command_name in \
  "$SBATCH_COMMAND" \
  "$SQUEUE_COMMAND" \
  "$SACCT_COMMAND" \
  "$SCANCEL_COMMAND"; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "Required Slurm command is unavailable: $command_name" >&2
    echo "Start the control plane on an approved Slurm submit host." >&2
    exit 1
  fi
done
SBATCH_COMMAND="$(command -v "$SBATCH_COMMAND")"
SQUEUE_COMMAND="$(command -v "$SQUEUE_COMMAND")"
SACCT_COMMAND="$(command -v "$SACCT_COMMAND")"
SCANCEL_COMMAND="$(command -v "$SCANCEL_COMMAND")"

mkdir -p \
  "$WORKFLOW_RUNTIME_DIR/logs" \
  "$WORKFLOW_RUNTIME_DIR/state" \
  "$WORKFLOW_RUNTIME_DIR/requests" \
  "$WORKFLOW_RUNTIME_DIR/jobs" \
  "$WORKFLOW_RUNTIME_DIR/models" \
  "$WORKFLOW_RUNTIME_DIR/output" \
  "$WORKFLOW_RUNTIME_DIR/recovery"

# The login/service process is a control plane only. Execution requests are
# submitted to Slurm; this process must neither detect CUDA nor create a local
# Dask Scheduler/Worker pool.
export PYTHONPATH="$WORKFLOW_ROOT/backend"
export WorkFlow_EXECUTION_BACKEND="slurm"
export WorkFlow_SLURM_EXECUTION_SCRIPT="$EXECUTION_SCRIPT"
export WorkFlow_SLURM_RUNTIME_DIR="$WORKFLOW_RUNTIME_DIR"
export WorkFlow_SLURM_SBATCH="$SBATCH_COMMAND"
export WorkFlow_SLURM_SQUEUE="$SQUEUE_COMMAND"
export WorkFlow_SLURM_SACCT="$SACCT_COMMAND"
export WorkFlow_SLURM_SCANCEL="$SCANCEL_COMMAND"
export WorkFlow_MODELS_DIR="$WORKFLOW_RUNTIME_DIR/models"
export CELLPOSE_LOCAL_MODELS_PATH="$WORKFLOW_RUNTIME_DIR/models/cellpose"
export WorkFlow_CUDA_MODE="disabled"
export CUDA_VISIBLE_DEVICES=""

printf '%s\n' \
  "Starting the WorkFlow control plane" \
  "root=$WORKFLOW_ROOT" \
  "runtime=$WORKFLOW_RUNTIME_DIR" \
  "listen=http://127.0.0.1:$WEB_PORT" \
  "execution_backend=slurm" \
  "execution_script=$EXECUTION_SCRIPT" \
  "slurm_commands=$SBATCH_COMMAND,$SQUEUE_COMMAND,$SACCT_COMMAND,$SCANCEL_COMMAND"

cd "$WORKFLOW_ROOT/backend"
exec "$PYTHON" -m uvicorn main:app \
  --host 127.0.0.1 \
  --port "$WEB_PORT" \
  --workers 1
