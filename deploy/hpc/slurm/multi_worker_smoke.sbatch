#!/usr/bin/env bash

#SBATCH --job-name=workflow-multi-worker-smoke
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --signal=B:TERM@90

set -euo pipefail

if [[ "$#" -ne 7 ]]; then
  echo "Usage: multi_worker_smoke.sbatch WORKFLOW_ROOT RUNTIME_DIR CPU_WORKERS GPU_WORKERS CPU_MEMORY_GIB GPU_MEMORY_GIB RESULT_FILE" >&2
  exit 2
fi

WORKFLOW_ROOT="$1"
WORKFLOW_RUNTIME_DIR="$2"
CPU_WORKERS="$3"
GPU_WORKERS="$4"
CPU_MEMORY_GIB="$5"
GPU_MEMORY_GIB="$6"
JOB_ID="${SLURM_JOB_ID:-missing}"
RESULT_FILE="${7//%j/$JOB_ID}"
PYTHON="$WORKFLOW_ROOT/backend/.venv/bin/python"
JOB_USER="${USER:-${SLURM_JOB_USER:-$(id -un)}}"
TMP_BASE="${SLURM_TMPDIR:-/tmp/$JOB_USER/workflow-multi-worker-$JOB_ID}"

for path in "$WORKFLOW_ROOT" "$WORKFLOW_RUNTIME_DIR" "$RESULT_FILE"; do
  if [[ "$path" != /* ]]; then
    echo "Smoke-test paths must be absolute: $path" >&2
    exit 2
  fi
done
for value in "$CPU_WORKERS" "$GPU_WORKERS"; do
  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    echo "Worker counts must be non-negative integers: $value" >&2
    exit 2
  fi
done
for value in "$CPU_MEMORY_GIB" "$GPU_MEMORY_GIB"; do
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "Worker memory limits must be positive integer GiB values: $value" >&2
    exit 2
  fi
done
if (( CPU_WORKERS + GPU_WORKERS < 2 )); then
  echo "A multi-Worker smoke test requires at least two total Workers." >&2
  exit 2
fi
if [[ ! -x "$PYTHON" ]]; then
  echo "Missing deployment Python interpreter: $PYTHON" >&2
  exit 1
fi

mkdir -p "$TMP_BASE/dask" "$(dirname "$RESULT_FILE")"
export PYTHONPATH="$WORKFLOW_ROOT/backend"
export TMPDIR="$TMP_BASE"
export WorkFlow_DASK_LOCAL_DIR="$TMP_BASE/dask"
export WorkFlow_DASHBOARD_ADDRESS=":0"
export WorkFlow_DASK_CLUSTER_START_TIMEOUT_SECONDS="${WORKFLOW_SMOKE_CLUSTER_START_TIMEOUT_SECONDS:-600}"
export WorkFlow_DASK_WORKER_BATCH_START_TIMEOUT_SECONDS="${WORKFLOW_SMOKE_WORKER_BATCH_TIMEOUT_SECONDS:-180}"
export WorkFlow_DASK_CLUSTER_CLOSE_TIMEOUT_SECONDS="${WORKFLOW_SMOKE_CLUSTER_CLOSE_TIMEOUT_SECONDS:-120}"
export WorkFlow_CPU_WORKER_MEMORY_LIMIT_GB="$CPU_MEMORY_GIB"
export WorkFlow_GPU_WORKER_MEMORY_LIMIT_GB="$GPU_MEMORY_GIB"
export MALLOC_TRIM_THRESHOLD_=0
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export CUDA_DEVICE_ORDER=PCI_BUS_ID

if (( GPU_WORKERS > 0 )); then
  export WorkFlow_CUDA_MODE=auto
  export WorkFlow_TRUST_SLURM_CUDA_MASK=1
  if [[ -z "${CUDA_VISIBLE_DEVICES:-}" || "${CUDA_VISIBLE_DEVICES:-}" == "-1" ]]; then
    echo "Slurm did not expose the requested GPU allocation through CUDA_VISIBLE_DEVICES." >&2
    exit 1
  fi
else
  export WorkFlow_CUDA_MODE=disabled
  export CUDA_VISIBLE_DEVICES=""
fi

echo "__WORKFLOW_MULTI_WORKER_BEGIN__"
echo "job=$JOB_ID host=$(hostname -f) cpu_workers=$CPU_WORKERS gpu_workers=$GPU_WORKERS cuda_visible_devices=${CUDA_VISIBLE_DEVICES-}"
"$PYTHON" "$WORKFLOW_ROOT/deploy/hpc/multi_worker_smoke.py" \
  --cpu-workers "$CPU_WORKERS" \
  --gpu-workers "$GPU_WORKERS" \
  --result-file "$RESULT_FILE"
echo "__WORKFLOW_MULTI_WORKER_END__"
