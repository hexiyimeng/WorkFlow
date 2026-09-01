#!/usr/bin/env bash

set -euo pipefail

if [[ "$#" -ne 7 ]]; then
  echo "Usage: convert_h5_block_to_zarr.sbatch PROJECT_ROOT BLOCK_ID IMAGE_H5 REFERENCE_H5 IMAGE_ZARR REFERENCE_ZARR PYTHON" >&2
  exit 2
fi

PROJECT_ROOT="$1"
BLOCK_ID="$2"
IMAGE_H5="$3"
REFERENCE_H5="$4"
IMAGE_ZARR="$5"
REFERENCE_ZARR="$6"
PYTHON="$7"

export PATH="${PYTHON%/*}:/usr/local/bin:/usr/bin:/bin"
export PYTHONPATH="$PROJECT_ROOT/backend"
export PYTHONUNBUFFERED=1
export OMP_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export BLOSC_NTHREADS="${SLURM_CPUS_PER_TASK:-1}"

cd "$PROJECT_ROOT/backend"
exec "$PYTHON" -m tools.convert_h5_block_to_zarr \
  --block-id "$BLOCK_ID" \
  --image-h5 "$IMAGE_H5" \
  --reference-h5 "$REFERENCE_H5" \
  --image-zarr "$IMAGE_ZARR" \
  --reference-zarr "$REFERENCE_ZARR"
