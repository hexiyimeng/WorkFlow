#!/usr/bin/env bash

set -euo pipefail

if [[ "$#" -gt 2 ]]; then
  echo "Usage: submit_multi_worker_smoke.sh [CPU_WORKERS [GPU_WORKERS]]" >&2
  exit 2
fi

CPU_WORKERS="${1:-${WORKFLOW_SMOKE_CPU_WORKERS:-2}}"
GPU_WORKERS="${2:-${WORKFLOW_SMOKE_GPU_WORKERS:-0}}"
WORKFLOW_ROOT="${WORKFLOW_ROOT:-$HOME/apps/WorkFlow}"
WORKFLOW_RUNTIME_DIR="${WORKFLOW_RUNTIME_DIR:-$HOME/workflow-runtime}"
CPU_MEMORY_GIB="${WORKFLOW_SMOKE_CPU_MEMORY_GIB:-2}"
GPU_MEMORY_GIB="${WORKFLOW_SMOKE_GPU_MEMORY_GIB:-16}"
CPUS_PER_WORKER="${WORKFLOW_SMOKE_CPUS_PER_WORKER:-1}"
BASE_MEMORY_GIB="${WORKFLOW_SMOKE_BASE_MEMORY_GIB:-2}"
TIME_LIMIT="${WORKFLOW_SMOKE_TIME_LIMIT:-00:30:00}"
PARTITION="${WORKFLOW_SMOKE_PARTITION:-${WorkFlow_SLURM_PARTITION:-}}"

for value in "$CPU_WORKERS" "$GPU_WORKERS"; do
  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    echo "Worker counts must be non-negative integers: $value" >&2
    exit 2
  fi
done
for value in \
  "$CPU_MEMORY_GIB" \
  "$GPU_MEMORY_GIB" \
  "$CPUS_PER_WORKER" \
  "$BASE_MEMORY_GIB"; do
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "Smoke-test resource values must be positive integers: $value" >&2
    exit 2
  fi
done
if (( CPU_WORKERS + GPU_WORKERS < 2 )); then
  echo "A multi-Worker smoke test requires at least two total Workers." >&2
  exit 2
fi
if [[ "$WORKFLOW_ROOT" != /* || "$WORKFLOW_RUNTIME_DIR" != /* ]]; then
  echo "WORKFLOW_ROOT and WORKFLOW_RUNTIME_DIR must be absolute shared paths." >&2
  exit 2
fi

SCRIPT="$WORKFLOW_ROOT/deploy/hpc/slurm/multi_worker_smoke.sbatch"
if [[ ! -f "$SCRIPT" ]]; then
  echo "Missing smoke-test batch script: $SCRIPT" >&2
  exit 1
fi

TOTAL_WORKERS=$((CPU_WORKERS + GPU_WORKERS))
ALLOCATED_CPUS=$((TOTAL_WORKERS * CPUS_PER_WORKER))
ALLOCATED_MEMORY_GIB=$((
  BASE_MEMORY_GIB
  + CPU_WORKERS * CPU_MEMORY_GIB
  + GPU_WORKERS * GPU_MEMORY_GIB
))
mkdir -p "$WORKFLOW_RUNTIME_DIR/logs" "$WORKFLOW_RUNTIME_DIR/test-runs"
RESULT_FILE="$WORKFLOW_RUNTIME_DIR/test-runs/multi-worker-smoke-%j.json"
OUTPUT_FILE="$WORKFLOW_RUNTIME_DIR/logs/multi-worker-smoke-%j.log"

SBATCH_ARGS=(
  --parsable
  --export=NONE
  --chdir="$WORKFLOW_ROOT"
  --cpus-per-task="$ALLOCATED_CPUS"
  --mem="${ALLOCATED_MEMORY_GIB}G"
  --time="$TIME_LIMIT"
  --output="$OUTPUT_FILE"
)
if [[ -n "$PARTITION" ]]; then
  SBATCH_ARGS+=(--partition="$PARTITION")
fi
if (( GPU_WORKERS > 0 )); then
  SBATCH_ARGS+=(--gres="gpu:$GPU_WORKERS")
fi

JOB_ID="$(
  sbatch "${SBATCH_ARGS[@]}" \
    "$SCRIPT" \
    "$WORKFLOW_ROOT" \
    "$WORKFLOW_RUNTIME_DIR" \
    "$CPU_WORKERS" \
    "$GPU_WORKERS" \
    "$CPU_MEMORY_GIB" \
    "$GPU_MEMORY_GIB" \
    "$RESULT_FILE"
)"
JOB_ID="${JOB_ID%%;*}"
if [[ ! "$JOB_ID" =~ ^[0-9]+$ ]]; then
  echo "sbatch returned an invalid job identifier: $JOB_ID" >&2
  exit 1
fi

printf 'Submitted WorkFlow multi-Worker smoke test\njobId=%s\ncpuWorkers=%s\ngpuWorkers=%s\nlog=%s\nresult=%s\n' \
  "$JOB_ID" \
  "$CPU_WORKERS" \
  "$GPU_WORKERS" \
  "${OUTPUT_FILE//%j/$JOB_ID}" \
  "${RESULT_FILE//%j/$JOB_ID}"
