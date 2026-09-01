#!/usr/bin/env bash

set -euo pipefail

if [[ "$#" -ne 2 ]]; then
  echo "Usage: submit_h5_block_conversion.sh BLOCK_ID PARTITION" >&2
  exit 2
fi

BLOCK_ID="$1"
PARTITION="$2"
if [[ ! "$BLOCK_ID" =~ ^[1-9][0-9]*$ ]]; then
  echo "BLOCK_ID must be a positive integer." >&2
  exit 2
fi
if [[ ! "$PARTITION" =~ ^[A-Za-z0-9_.-]+$ ]]; then
  echo "PARTITION contains unsupported characters." >&2
  exit 2
fi

SOURCE_NAME="${WORKFLOW_H5_DATA_NAME:-20210131_ZSS_USTC_THY1-YFP_1779_1}"
SOURCE_ROOT="${WORKFLOW_H5_SOURCE_ROOT:-/share/home/yangcy/multiview_reconstruction/$SOURCE_NAME/slice_image}"
REFERENCE_ROOT="${WORKFLOW_H5_REFERENCE_ROOT:-/share/home/yangcy/cell_segmentation/$SOURCE_NAME}"
DESTINATION_ROOT="${WORKFLOW_H5_DESTINATION_ROOT:-${HOME:?HOME is required}/workflow-runtime/data}"
CPUS="${WORKFLOW_H5_CONVERSION_CPUS:-4}"
MEMORY="${WORKFLOW_H5_CONVERSION_MEMORY:-32G}"
TIME_LIMIT="${WORKFLOW_H5_CONVERSION_TIME:-2-00:00:00}"

SOURCE_PATH="$SOURCE_ROOT/$BLOCK_ID.h5"
REFERENCE_PATH="$REFERENCE_ROOT/${BLOCK_ID}_seg_.h5"
PADDED_BLOCK_ID="$(printf '%03d' "$BLOCK_ID")"
IMAGE_OUTPUT="$DESTINATION_ROOT/${SOURCE_NAME}_block${PADDED_BLOCK_ID}.zarr"
REFERENCE_OUTPUT="$DESTINATION_ROOT/${SOURCE_NAME}_block${PADDED_BLOCK_ID}_reference.zarr"

SCRIPT_PATH="${BASH_SOURCE[0]}"
case "$SCRIPT_PATH" in
  /*) ;;
  *) SCRIPT_PATH="$PWD/$SCRIPT_PATH" ;;
esac
SCRIPT_DIR="$(cd -- "${SCRIPT_PATH%/*}" && pwd -P)"
PROJECT_ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd -P)"
JOB_SCRIPT="$PROJECT_ROOT/deploy/hpc/slurm/convert_h5_block_to_zarr.sbatch"
PYTHON="$PROJECT_ROOT/backend/.venv/bin/python"
LOG_DIRECTORY="${HOME}/workflow-runtime/logs"

for path in "$SOURCE_PATH" "$REFERENCE_PATH"; do
  if [[ -L "$path" || ! -f "$path" || ! -r "$path" ]]; then
    echo "Required readable regular source file is unavailable: $path" >&2
    exit 1
  fi
done
if [[ ! -d "$DESTINATION_ROOT" || -L "$DESTINATION_ROOT" ]]; then
  echo "Destination must be an existing non-symlink directory: $DESTINATION_ROOT" >&2
  exit 1
fi
if [[ ! -w "$DESTINATION_ROOT" ]]; then
  echo "Destination is not writable: $DESTINATION_ROOT" >&2
  exit 1
fi
if [[ ! -x "$PYTHON" ]]; then
  echo "WorkFlow Python is unavailable: $PYTHON" >&2
  exit 1
fi
if ! "$PYTHON" -c 'import h5py, numcodecs, zarr' >/dev/null; then
  echo "Conversion dependencies are unavailable; run deploy/hpc/install.sh first." >&2
  exit 1
fi
if [[ ! -f "$JOB_SCRIPT" || -L "$JOB_SCRIPT" ]]; then
  echo "Conversion Slurm entrypoint is unavailable: $JOB_SCRIPT" >&2
  exit 1
fi
for value in "$CPUS"; do
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "WORKFLOW_H5_CONVERSION_CPUS must be a positive integer." >&2
    exit 2
  fi
done

mkdir -p "$LOG_DIRECTORY"
JOB_ID="$(sbatch \
  --parsable \
  --partition="$PARTITION" \
  --nodes=1 \
  --ntasks=1 \
  --cpus-per-task="$CPUS" \
  --mem="$MEMORY" \
  --time="$TIME_LIMIT" \
  --job-name="wf-h5-zarr-$BLOCK_ID" \
  --output="$LOG_DIRECTORY/h5-zarr-$BLOCK_ID-%j.out" \
  --error="$LOG_DIRECTORY/h5-zarr-$BLOCK_ID-%j.err" \
  "$JOB_SCRIPT" \
  "$PROJECT_ROOT" \
  "$BLOCK_ID" \
  "$SOURCE_PATH" \
  "$REFERENCE_PATH" \
  "$IMAGE_OUTPUT" \
  "$REFERENCE_OUTPUT" \
  "$PYTHON")"
JOB_ID="${JOB_ID%%;*}"

printf '%s\n' \
  "Submitted restartable HDF5-to-Zarr conversion." \
  "jobId=$JOB_ID" \
  "partition=$PARTITION" \
  "image=$IMAGE_OUTPUT" \
  "reference=$REFERENCE_OUTPUT" \
  "stdout=$LOG_DIRECTORY/h5-zarr-$BLOCK_ID-$JOB_ID.out" \
  "stderr=$LOG_DIRECTORY/h5-zarr-$BLOCK_ID-$JOB_ID.err"
