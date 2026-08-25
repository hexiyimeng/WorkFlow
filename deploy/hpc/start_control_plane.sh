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
EXECUTION_SCRIPT="${WorkFlow_SLURM_EXECUTION_SCRIPT:-$WORKFLOW_ROOT/deploy/hpc/slurm/workflow_workers.sbatch}"
WEB_PORT="${WORKFLOW_WEB_PORT:-8000}"
SCHEDULER_HOST="${WorkFlow_DASK_SCHEDULER_HOST:-}"
SCHEDULER_PORT="${WorkFlow_DASK_SCHEDULER_PORT:-8786}"
WORKER_PORT_RANGE="${WorkFlow_DASK_WORKER_PORT_RANGE:-20000:20999}"
NANNY_PORT_RANGE="${WorkFlow_DASK_NANNY_PORT_RANGE:-21000:21999}"
SLURM_MAX_NODES="${WorkFlow_SLURM_MAX_NODES:-8}"
SLURM_CPUS_PER_NODE="${WorkFlow_SLURM_CPUS_PER_NODE:-64}"
SLURM_GPUS_PER_NODE="${WorkFlow_SLURM_GPUS_PER_NODE:-8}"
SLURM_MEMORY_GIB_PER_NODE="${WorkFlow_SLURM_MEMORY_GIB_PER_NODE:-512}"

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
if [[ -z "$SCHEDULER_HOST" ]]; then
  echo "WorkFlow_DASK_SCHEDULER_HOST is required." >&2
  echo "Set it to the service-node IPv4 address or DNS name that compute nodes can reach." >&2
  exit 2
fi
case "${SCHEDULER_HOST,,}" in
  localhost|localhost.localdomain|0.0.0.0|::|::1|\[::\]|\*|127.*)
    echo "WorkFlow_DASK_SCHEDULER_HOST must be a routable service-node address, not loopback or wildcard: $SCHEDULER_HOST" >&2
    exit 2
    ;;
esac
if [[ ! "$SCHEDULER_HOST" =~ ^[A-Za-z0-9][A-Za-z0-9.-]*$ ]]; then
  echo "WorkFlow_DASK_SCHEDULER_HOST must be an IPv4 address or DNS name without a URL scheme." >&2
  exit 2
fi
if [[ ! "$SCHEDULER_PORT" =~ ^[0-9]+$ ]] || (( SCHEDULER_PORT < 1 || SCHEDULER_PORT > 65535 )); then
  echo "WorkFlow_DASK_SCHEDULER_PORT must be an integer between 1 and 65535." >&2
  exit 2
fi
resource_settings=(
  WorkFlow_SLURM_MAX_NODES "$SLURM_MAX_NODES"
  WorkFlow_SLURM_CPUS_PER_NODE "$SLURM_CPUS_PER_NODE"
  WorkFlow_SLURM_MEMORY_GIB_PER_NODE "$SLURM_MEMORY_GIB_PER_NODE"
)
for (( index=0; index<${#resource_settings[@]}; index+=2 )); do
  resource_setting="${resource_settings[index]}"
  value="${resource_settings[index + 1]}"
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "$resource_setting must be a positive integer." >&2
    exit 2
  fi
done
if [[ ! "$SLURM_GPUS_PER_NODE" =~ ^[0-9]+$ ]]; then
  echo "WorkFlow_SLURM_GPUS_PER_NODE must be a non-negative integer." >&2
  exit 2
fi

parse_port_range() {
  local variable_name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^([0-9]+):([0-9]+)$ ]]; then
    echo "$variable_name must use MIN:MAX syntax." >&2
    exit 2
  fi
  local minimum="${BASH_REMATCH[1]}"
  local maximum="${BASH_REMATCH[2]}"
  if (( minimum < 1024 || maximum > 65535 || minimum > maximum )); then
    echo "$variable_name must be a valid inclusive unprivileged TCP port range." >&2
    exit 2
  fi
  printf '%s %s\n' "$minimum" "$maximum"
}

read -r WORKER_PORT_MIN WORKER_PORT_MAX < <(
  parse_port_range WorkFlow_DASK_WORKER_PORT_RANGE "$WORKER_PORT_RANGE"
)
read -r NANNY_PORT_MIN NANNY_PORT_MAX < <(
  parse_port_range WorkFlow_DASK_NANNY_PORT_RANGE "$NANNY_PORT_RANGE"
)
if (( WORKER_PORT_MIN <= NANNY_PORT_MAX && NANNY_PORT_MIN <= WORKER_PORT_MAX )); then
  echo "WorkFlow_DASK_WORKER_PORT_RANGE and WorkFlow_DASK_NANNY_PORT_RANGE must not overlap." >&2
  exit 2
fi
if (( WEB_PORT == SCHEDULER_PORT )); then
  echo "WORKFLOW_WEB_PORT and WorkFlow_DASK_SCHEDULER_PORT must be different." >&2
  exit 2
fi
for reserved_port in "$WEB_PORT" "$SCHEDULER_PORT"; do
  if (( (reserved_port >= WORKER_PORT_MIN && reserved_port <= WORKER_PORT_MAX) ||
        (reserved_port >= NANNY_PORT_MIN && reserved_port <= NANNY_PORT_MAX) )); then
    echo "Web/Scheduler ports must not fall inside a Worker or Nanny port range." >&2
    exit 2
  fi
done

TLS_CA="${WorkFlow_DASK_TLS_CA:-}"
TLS_CERT="${WorkFlow_DASK_TLS_CERT:-}"
TLS_KEY="${WorkFlow_DASK_TLS_KEY:-}"
ALLOW_INSECURE_CLUSTER="${WorkFlow_DASK_ALLOW_INSECURE_CLUSTER:-0}"
if [[ "$ALLOW_INSECURE_CLUSTER" != 0 && "$ALLOW_INSECURE_CLUSTER" != 1 ]]; then
  echo "WorkFlow_DASK_ALLOW_INSECURE_CLUSTER must be 0 or 1." >&2
  exit 2
fi
if [[ -n "$TLS_CA$TLS_CERT$TLS_KEY" ]]; then
  if [[ -z "$TLS_CA" || -z "$TLS_CERT" || -z "$TLS_KEY" ]]; then
    echo "WorkFlow_DASK_TLS_CA, WorkFlow_DASK_TLS_CERT and WorkFlow_DASK_TLS_KEY must be set together." >&2
    exit 2
  fi
  for tls_path in "$TLS_CA" "$TLS_CERT" "$TLS_KEY"; do
    case "$tls_path" in
      /*) ;;
      *) echo "Dask TLS paths must be absolute: $tls_path" >&2; exit 2 ;;
    esac
    if [[ ! -f "$tls_path" || -L "$tls_path" || ! -r "$tls_path" ]]; then
      echo "Dask TLS path must be a readable regular non-symlink file: $tls_path" >&2
      exit 2
    fi
  done
elif [[ "$ALLOW_INSECURE_CLUSTER" != 1 ]]; then
  echo "Dask mTLS is not configured." >&2
  echo "Set all three WorkFlow_DASK_TLS_* files, or explicitly set WorkFlow_DASK_ALLOW_INSECURE_CLUSTER=1 only on a trusted ACL-restricted cluster network." >&2
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
case "$EXECUTION_SCRIPT" in
  */workflow_execution.sbatch)
    echo "workflow_execution.sbatch is the removed compute-node Driver entrypoint." >&2
    echo "Use deploy/hpc/slurm/workflow_workers.sbatch so only Workers run on compute nodes." >&2
    exit 2
    ;;
esac
SBATCH_COMMAND="${WorkFlow_SLURM_SBATCH:-sbatch}"
SQUEUE_COMMAND="${WorkFlow_SLURM_SQUEUE:-squeue}"
SINFO_COMMAND="${WorkFlow_SLURM_SINFO:-sinfo}"
SCONTROL_COMMAND="${WorkFlow_SLURM_SCONTROL:-scontrol}"
SCANCEL_COMMAND="${WorkFlow_SLURM_SCANCEL:-scancel}"
for command_name in \
  "$SBATCH_COMMAND" \
  "$SQUEUE_COMMAND" \
  "$SINFO_COMMAND" \
  "$SCONTROL_COMMAND" \
  "$SCANCEL_COMMAND"; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "Required Slurm command is unavailable: $command_name" >&2
    echo "Start the control plane on an approved Slurm submit host." >&2
    exit 1
  fi
done
SBATCH_COMMAND="$(command -v "$SBATCH_COMMAND")"
SQUEUE_COMMAND="$(command -v "$SQUEUE_COMMAND")"
SINFO_COMMAND="$(command -v "$SINFO_COMMAND")"
SCONTROL_COMMAND="$(command -v "$SCONTROL_COMMAND")"
SCANCEL_COMMAND="$(command -v "$SCANCEL_COMMAND")"
if [[ -v WorkFlow_SLURM_SACCT && -z "$WorkFlow_SLURM_SACCT" ]]; then
  SACCT_COMMAND=""
elif [[ -n "${WorkFlow_SLURM_SACCT:-}" ]]; then
  if ! command -v "$WorkFlow_SLURM_SACCT" >/dev/null 2>&1; then
    echo "Configured optional Slurm accounting command is unavailable: $WorkFlow_SLURM_SACCT" >&2
    exit 1
  fi
  SACCT_COMMAND="$(command -v "$WorkFlow_SLURM_SACCT")"
elif command -v sacct >/dev/null 2>&1; then
  SACCT_COMMAND="$(command -v sacct)"
else
  SACCT_COMMAND=""
fi

mkdir -p \
  "$WORKFLOW_RUNTIME_DIR/logs" \
  "$WORKFLOW_RUNTIME_DIR/state" \
  "$WORKFLOW_RUNTIME_DIR/requests" \
  "$WORKFLOW_RUNTIME_DIR/jobs" \
  "$WORKFLOW_RUNTIME_DIR/models" \
  "$WORKFLOW_RUNTIME_DIR/output" \
  "$WORKFLOW_RUNTIME_DIR/recovery"

# Uvicorn and the workflow Driver stay on this approved login/service node.
# A Dask Scheduler is created here only while one workflow is executing.  The
# graph-derived Nanny/Worker processes are submitted to compute nodes through
# Slurm and never run in this service process.
export PYTHONPATH="$WORKFLOW_ROOT/backend"
export WorkFlow_EXECUTION_BACKEND="slurm"
export WorkFlow_SLURM_EXECUTION_SCRIPT="$EXECUTION_SCRIPT"
export WorkFlow_SLURM_RUNTIME_DIR="$WORKFLOW_RUNTIME_DIR"
export WorkFlow_SLURM_SBATCH="$SBATCH_COMMAND"
export WorkFlow_SLURM_SQUEUE="$SQUEUE_COMMAND"
export WorkFlow_SLURM_SINFO="$SINFO_COMMAND"
export WorkFlow_SLURM_SACCT="$SACCT_COMMAND"
export WorkFlow_SLURM_SCONTROL="$SCONTROL_COMMAND"
export WorkFlow_SLURM_SCANCEL="$SCANCEL_COMMAND"
export WorkFlow_DASK_SCHEDULER_HOST="$SCHEDULER_HOST"
export WorkFlow_DASK_SCHEDULER_PORT="$SCHEDULER_PORT"
export WorkFlow_DASK_WORKER_PORT_RANGE="$WORKER_PORT_RANGE"
export WorkFlow_DASK_NANNY_PORT_RANGE="$NANNY_PORT_RANGE"
export WorkFlow_DASK_ALLOW_INSECURE_CLUSTER="$ALLOW_INSECURE_CLUSTER"
export WorkFlow_DASK_TLS_CA="$TLS_CA"
export WorkFlow_DASK_TLS_CERT="$TLS_CERT"
export WorkFlow_DASK_TLS_KEY="$TLS_KEY"
export WorkFlow_SLURM_MAX_NODES="$SLURM_MAX_NODES"
export WorkFlow_SLURM_CPUS_PER_NODE="$SLURM_CPUS_PER_NODE"
export WorkFlow_SLURM_GPUS_PER_NODE="$SLURM_GPUS_PER_NODE"
export WorkFlow_SLURM_MEMORY_GIB_PER_NODE="$SLURM_MEMORY_GIB_PER_NODE"
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
  "driver=service-node-process" \
  "scheduler=on-demand:$SCHEDULER_HOST:$SCHEDULER_PORT" \
  "cluster_manager=dask_jobqueue.SLURMCluster" \
  "workers=slurm-nodes-discovered-by-sinfo-and-scontrol" \
  "worker_ports=$WORKER_PORT_RANGE" \
  "nanny_ports=$NANNY_PORT_RANGE" \
  "slurm_node_envelope=max_nodes:$SLURM_MAX_NODES,cpus:$SLURM_CPUS_PER_NODE,gpus:$SLURM_GPUS_PER_NODE,memory_gib:$SLURM_MEMORY_GIB_PER_NODE" \
  "dask_tls_files=$([[ -n "$TLS_CA" ]] && printf configured || printf absent-explicitly-allowed)" \
  "slurm_partitions=${WorkFlow_SLURM_PARTITION:-${WorkFlow_SLURM_ALLOWED_PARTITIONS:-auto}},excluded:${WorkFlow_SLURM_EXCLUDED_PARTITIONS:-control}" \
  "slurm_commands=sbatch:$SBATCH_COMMAND,squeue:$SQUEUE_COMMAND,sacct:${SACCT_COMMAND:-unavailable},sinfo:$SINFO_COMMAND,scontrol:$SCONTROL_COMMAND,scancel:$SCANCEL_COMMAND"

cd "$WORKFLOW_ROOT/backend"
exec "$PYTHON" -m uvicorn main:app \
  --host 127.0.0.1 \
  --port "$WEB_PORT" \
  --workers 1
