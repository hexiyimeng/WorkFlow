#!/bin/bash

#SBATCH --job-name=workflow-execution
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --signal=B:TERM@90

set -euo pipefail
umask 077

if (( $# != 6 )); then
  echo "Usage: workflow_execution.sbatch ABSOLUTE_REQUEST_PATH ABSOLUTE_RUNTIME_DIR CPU_WORKER_MEMORY_GB GPU_WORKER_MEMORY_GB ABSOLUTE_SQUEUE_PATH ABSOLUTE_SACCT_PATH" >&2
  exit 2
fi

REQUEST_PATH="$1"
RUNTIME_DIR="${2%/}"
CPU_WORKER_MEMORY_GB="$3"
GPU_WORKER_MEMORY_GB="$4"
SQUEUE_PATH="$5"
SACCT_PATH="$6"
[[ -n "$RUNTIME_DIR" ]] || RUNTIME_DIR="/"

case "$REQUEST_PATH" in
  /*) ;;
  *) echo "Execution request path must be absolute: $REQUEST_PATH" >&2; exit 2 ;;
esac
case "$RUNTIME_DIR" in
  /*) ;;
  *) echo "Runtime directory must be absolute: $RUNTIME_DIR" >&2; exit 2 ;;
esac
if [[ "$RUNTIME_DIR" == "/" ]]; then
  echo "Runtime directory must not be the filesystem root." >&2
  exit 2
fi
if [[ ! -f "$REQUEST_PATH" ]]; then
  echo "Execution request does not exist: $REQUEST_PATH" >&2
  exit 1
fi
if [[ ! -d "$RUNTIME_DIR" ]]; then
  echo "Runtime directory does not exist: $RUNTIME_DIR" >&2
  exit 1
fi
if [[ ! "$CPU_WORKER_MEMORY_GB" =~ ^[1-9][0-9]*$ ]]; then
  echo "CPU Worker memory must be a positive integer GB value." >&2
  exit 2
fi
if [[ ! "$GPU_WORKER_MEMORY_GB" =~ ^[1-9][0-9]*$ ]]; then
  echo "GPU Worker memory must be a positive integer GB value." >&2
  exit 2
fi
for scheduler_command in "$SQUEUE_PATH" "$SACCT_PATH"; do
  case "$scheduler_command" in
    /*) ;;
    *) echo "Slurm status command path must be absolute: $scheduler_command" >&2; exit 2 ;;
  esac
  if [[ ! -f "$scheduler_command" || ! -x "$scheduler_command" ]]; then
    echo "Slurm status command is not an executable file: $scheduler_command" >&2
    exit 1
  fi
done

# sbatch uses a private spool copy of this script, so BASH_SOURCE cannot locate
# the repository. The submitter sets --chdir to the repository root. Also
# accept submission from backend/ for a useful manual/debug path.
SUBMIT_DIR="$PWD"
WORKFLOW_ROOT=""
if [[ -x "$SUBMIT_DIR/backend/.venv/bin/python" ]]; then
  WORKFLOW_ROOT="$SUBMIT_DIR"
elif [[ "$SUBMIT_DIR" == */backend && -x "$SUBMIT_DIR/.venv/bin/python" ]]; then
  WORKFLOW_ROOT="${SUBMIT_DIR%/backend}"
fi
if [[ -z "$WORKFLOW_ROOT" ]]; then
  echo "Cannot locate backend/.venv/bin/python from Slurm working directory: $SUBMIT_DIR" >&2
  echo "The submitter must pass --chdir=<absolute WorkFlow repository root>." >&2
  exit 1
fi

PYTHON="$WORKFLOW_ROOT/backend/.venv/bin/python"
JOB_ID="${SLURM_JOB_ID:?SLURM_JOB_ID is required}"
if [[ -n "${SLURM_TMPDIR:-}" ]]; then
  case "$SLURM_TMPDIR" in
    /*) SCRATCH_ROOT="$SLURM_TMPDIR/workflow-$JOB_ID" ;;
    *) echo "Ignoring non-absolute SLURM_TMPDIR: $SLURM_TMPDIR" >&2
       SCRATCH_ROOT="$RUNTIME_DIR/jobs/$JOB_ID/scratch" ;;
  esac
else
  SCRATCH_ROOT="$RUNTIME_DIR/jobs/$JOB_ID/scratch"
fi

if [[ -x /usr/bin/mkdir ]]; then
  MKDIR=/usr/bin/mkdir
elif [[ -x /bin/mkdir ]]; then
  MKDIR=/bin/mkdir
else
  echo "A system mkdir executable is required." >&2
  exit 1
fi
"$MKDIR" -p \
  "$SCRATCH_ROOT/dask" \
  "$SCRATCH_ROOT/tmp" \
  "$SCRATCH_ROOT/cache" \
  "$RUNTIME_DIR/jobs/$JOB_ID" \
  "$RUNTIME_DIR/models/cellpose"

# This job must work with sbatch --export=NONE. Critical paths are absolute,
# HOME is an explicit runtime location, and CUDA_VISIBLE_DEVICES is deliberately
# left to Slurm's GRES plugin.
export PATH="$WORKFLOW_ROOT/backend/.venv/bin:/usr/local/bin:/usr/bin:/bin"
export HOME="$RUNTIME_DIR"
export PYTHONPATH="$WORKFLOW_ROOT/backend"
export PYTHONUNBUFFERED=1
export WorkFlow_EXECUTION_BACKEND="local"
export WorkFlow_SLURM_SQUEUE="$SQUEUE_PATH"
export WorkFlow_SLURM_SACCT="$SACCT_PATH"
export TMPDIR="$SCRATCH_ROOT/tmp"
export TMP="$SCRATCH_ROOT/tmp"
export TEMP="$SCRATCH_ROOT/tmp"
export XDG_CACHE_HOME="$SCRATCH_ROOT/cache"
export WorkFlow_MODELS_DIR="$RUNTIME_DIR/models"
export CELLPOSE_LOCAL_MODELS_PATH="$RUNTIME_DIR/models/cellpose"
export WorkFlow_DASK_LOCAL_DIR="$SCRATCH_ROOT/dask"
export WorkFlow_JOB_SCRATCH_ROOT="$SCRATCH_ROOT"
export WorkFlow_DASHBOARD_ADDRESS=":0"
export WorkFlow_DASK_CLUSTER_START_TIMEOUT_SECONDS=300
export WorkFlow_DASK_CLUSTER_CLOSE_TIMEOUT_SECONDS=120
export WorkFlow_CPU_WORKER_MEMORY_LIMIT_GB="$CPU_WORKER_MEMORY_GB"
export WorkFlow_GPU_WORKER_MEMORY_LIMIT_GB="$GPU_WORKER_MEMORY_GB"
export WorkFlow_CUDA_MODE="auto"
export WorkFlow_TRUST_SLURM_CUDA_MASK="1"
export CUDA_DEVICE_ORDER="PCI_BUS_ID"
export MALLOC_TRIM_THRESHOLD_=0
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export NUMEXPR_NUM_THREADS=1

cd "$WORKFLOW_ROOT/backend"
exec "$PYTHON" -m services.slurm_execution_runner \
  "$REQUEST_PATH" \
  "$RUNTIME_DIR"
