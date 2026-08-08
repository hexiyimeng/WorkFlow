#!/usr/bin/env bash

set -euo pipefail

WORKFLOW_ROOT="${WORKFLOW_ROOT:-$HOME/apps/WorkFlow}"
WORKFLOW_RUNTIME_DIR="${WORKFLOW_RUNTIME_DIR:-$HOME/workflow-runtime}"
WORKFLOW_PARTITION="${WORKFLOW_PARTITION:-compute}"
WORKFLOW_GPUS="${WORKFLOW_GPUS:-1}"
WORKFLOW_CPUS="${WORKFLOW_CPUS:-8}"
WORKFLOW_MEMORY="${WORKFLOW_MEMORY:-128G}"
WORKFLOW_TIME="${WORKFLOW_TIME:-1-00:00:00}"
SBATCH_SCRIPT="$WORKFLOW_ROOT/deploy/hpc/slurm/workflow_server.sbatch"

if [[ ! "$WORKFLOW_GPUS" =~ ^[0-9]+$ ]]; then
  echo "WORKFLOW_GPUS must be a non-negative integer." >&2
  exit 2
fi
if [[ ! "$WORKFLOW_CPUS" =~ ^[1-9][0-9]*$ ]]; then
  echo "WORKFLOW_CPUS must be a positive integer." >&2
  exit 2
fi
if [[ ! -f "$SBATCH_SCRIPT" ]]; then
  echo "Missing Slurm script: $SBATCH_SCRIPT" >&2
  exit 1
fi
if [[ ! -x "$WORKFLOW_ROOT/backend/.venv/bin/python" ]]; then
  echo "Backend environment is missing. Run deploy/hpc/install.sh first." >&2
  exit 1
fi

mkdir -p "$WORKFLOW_RUNTIME_DIR/logs" "$WORKFLOW_RUNTIME_DIR/state"

args=(
  --parsable
  --partition="$WORKFLOW_PARTITION"
  --cpus-per-task="$WORKFLOW_CPUS"
  --mem="$WORKFLOW_MEMORY"
  --time="$WORKFLOW_TIME"
  --chdir="$WORKFLOW_ROOT"
  --output="$WORKFLOW_RUNTIME_DIR/logs/workflow-server-%j.log"
  --export="ALL,WORKFLOW_ROOT=$WORKFLOW_ROOT,WORKFLOW_RUNTIME_DIR=$WORKFLOW_RUNTIME_DIR"
)
if (( WORKFLOW_GPUS > 0 )); then
  args+=(--gres="gpu:$WORKFLOW_GPUS")
fi

job_id="$(sbatch "${args[@]}" "$SBATCH_SCRIPT")"
printf 'Submitted WorkFlow server job %s\n' "$job_id"
printf 'Monitor: squeue -j %s\n' "$job_id"
printf 'Log: tail -f %s/logs/workflow-server-%s.log\n' \
  "$WORKFLOW_RUNTIME_DIR" "$job_id"
