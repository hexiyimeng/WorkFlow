#!/usr/bin/env bash

#SBATCH --job-name=workflow-gpu-smoke
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --gres=gpu:1
#SBATCH --time=00:10:00

set -euo pipefail

WORKFLOW_ROOT="${WORKFLOW_ROOT:-$HOME/apps/WorkFlow}"
PYTHON="$WORKFLOW_ROOT/backend/.venv/bin/python"
export PYTHONPATH="$WORKFLOW_ROOT/backend"
export CUDA_DEVICE_ORDER=PCI_BUS_ID
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1

echo "__WORKFLOW_GPU_SMOKE_BEGIN__"
echo "job=$SLURM_JOB_ID host=$(hostname -f) cuda_visible_devices=${CUDA_VISIBLE_DEVICES-}"
"$PYTHON" "$WORKFLOW_ROOT/deploy/hpc/gpu_smoke.py"
"$PYTHON" - <<'PY'
from core.config import _get_system_memory_gb

print(f"effectiveMemoryGiB={_get_system_memory_gb():.3f}")
PY
echo "__WORKFLOW_GPU_SMOKE_END__"
