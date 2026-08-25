#!/bin/bash

# This is only a Slurm allocation holder and Worker launcher.  The WorkFlow
# Driver and Dask Scheduler remain in the service process; this script must
# never invoke the workflow executor or the compute runner.
#SBATCH --job-name=workflow-workers
#SBATCH --signal=B:TERM@90

set -euo pipefail
umask 077

if (( $# != 1 )); then
  echo "Usage: workflow_workers.sbatch ABSOLUTE_WORKER_REQUEST_PATH" >&2
  exit 2
fi

REQUEST_PATH="$1"
case "$REQUEST_PATH" in
  /*) ;;
  *) echo "Worker request path must be absolute: $REQUEST_PATH" >&2; exit 2 ;;
esac
if [[ ! -f "$REQUEST_PATH" || -L "$REQUEST_PATH" ]]; then
  echo "Worker request must be a regular non-symlink file: $REQUEST_PATH" >&2
  exit 1
fi

# sbatch executes a spool copy, so the dynamically supplied --chdir points to
# the shared WorkFlow checkout.  No CPU, memory, GPU or node count is embedded
# in this file; the service must submit the graph-derived allocation.
SUBMIT_DIR="$PWD"
if [[ -x "$SUBMIT_DIR/backend/.venv/bin/python" ]]; then
  WORKFLOW_ROOT="$SUBMIT_DIR"
elif [[ "$SUBMIT_DIR" == */backend && -x "$SUBMIT_DIR/.venv/bin/python" ]]; then
  WORKFLOW_ROOT="${SUBMIT_DIR%/backend}"
else
  echo "Cannot locate backend/.venv/bin/python from: $SUBMIT_DIR" >&2
  exit 1
fi
PYTHON="$WORKFLOW_ROOT/backend/.venv/bin/python"

export PATH="$WORKFLOW_ROOT/backend/.venv/bin:/usr/local/bin:/usr/bin:/bin"
export PYTHONPATH="$WORKFLOW_ROOT/backend"
export PYTHONUNBUFFERED=1
export CUDA_DEVICE_ORDER="PCI_BUS_ID"
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export NUMEXPR_NUM_THREADS=1
export MALLOC_TRIM_THRESHOLD_=0

read -r PLANNED_NODES GPUS_PER_NODE < <(
  "$PYTHON" -m services.slurm_worker_launcher --print-layout "$REQUEST_PATH"
)
if [[ ! "$PLANNED_NODES" =~ ^[1-9][0-9]*$ ]]; then
  echo "Worker request produced an invalid node count: $PLANNED_NODES" >&2
  exit 2
fi
if [[ ! "$GPUS_PER_NODE" =~ ^[0-9]+$ ]]; then
  echo "Worker request produced an invalid GPU count: $GPUS_PER_NODE" >&2
  exit 2
fi
if [[ "${SLURM_NNODES:?SLURM_NNODES is required}" != "$PLANNED_NODES" ]]; then
  echo "Allocation node count does not match Worker request: allocated=$SLURM_NNODES planned=$PLANNED_NODES" >&2
  exit 2
fi

SRUN_ARGS=(
  --nodes="$PLANNED_NODES"
  --ntasks="$PLANNED_NODES"
  --ntasks-per-node=1
  --distribution=block
  --kill-on-bad-exit=1
  --export=ALL
)
# Slurm 19.05 uses --gres=gpu:N as a per-node request.  Explicitly repeat the
# homogeneous allocation on the job step so every launcher receives the correct
# CUDA_VISIBLE_DEVICES mask on old controllers.  An uneven logical layout may
# leave reserved GPUs unused on the final node; the launcher never exposes them to
# a Worker process.
if (( GPUS_PER_NODE > 0 )); then
  SRUN_ARGS+=(--gres="gpu:$GPUS_PER_NODE")
fi

cd "$WORKFLOW_ROOT/backend"
exec srun "${SRUN_ARGS[@]}" \
  "$PYTHON" -m services.slurm_worker_launcher "$REQUEST_PATH"
