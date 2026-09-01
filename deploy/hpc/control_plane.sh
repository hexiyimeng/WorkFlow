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
WORKFLOW_RUNTIME_DIR="${WORKFLOW_RUNTIME_DIR:-${HOME:?HOME is required}/workflow-runtime}"
SESSION_NAME="${WORKFLOW_CONTROL_PLANE_SESSION:-workflow-control-plane}"
START_SCRIPT="$WORKFLOW_ROOT/deploy/hpc/start_control_plane.sh"
LOG_PATH="$WORKFLOW_RUNTIME_DIR/logs/control-plane.log"
ACTION="${1:-status}"
CONFIG_PATH="${WORKFLOW_CONTROL_PLANE_CONFIG_FILE:-$WORKFLOW_RUNTIME_DIR/config/control-plane.env}"
case "$CONFIG_PATH" in
  /*) ;;
  *) echo "WORKFLOW_CONTROL_PLANE_CONFIG_FILE must be an absolute path." >&2; exit 2 ;;
esac
CONTROL_PLANE_ENVIRONMENT=(
  WorkFlow_ALLOWED_ORIGINS
  WorkFlow_DASK_ALLOW_INSECURE_CLUSTER
  WorkFlow_DASK_CLUSTER_CLOSE_TIMEOUT_SECONDS
  WorkFlow_DASK_CLUSTER_START_TIMEOUT_SECONDS
  WorkFlow_DASK_DASHBOARD_ADDRESS
  WorkFlow_DASK_INTERFACE
  WorkFlow_DASK_NANNY_PORT_RANGE
  WorkFlow_DASK_SCHEDULER_HOST
  WorkFlow_DASK_SCHEDULER_PORT
  WorkFlow_DASK_TLS_CA
  WorkFlow_DASK_TLS_CERT
  WorkFlow_DASK_TLS_KEY
  WorkFlow_DASK_WORKER_CONNECT_TIMEOUT_SECONDS
  WorkFlow_DASK_WORKER_REGISTRATION_TIMEOUT_SECONDS
  WorkFlow_DASK_WORKER_PORT_RANGE
  WorkFlow_DASHBOARD_HOST
  WorkFlow_SLURM_ALLOWED_PARTITIONS
  WorkFlow_SLURM_CANCEL_GRACE_SECONDS
  WorkFlow_SLURM_CPUS_PER_NODE
  WorkFlow_SLURM_EXCLUDED_NODES
  WorkFlow_SLURM_EXCLUDED_PARTITIONS
  WorkFlow_SLURM_GPUS_PER_NODE
  WorkFlow_SLURM_MAX_CPUS
  WorkFlow_SLURM_MAX_GPUS
  WorkFlow_SLURM_MAX_MEMORY_GIB
  WorkFlow_SLURM_MAX_NODES
  WorkFlow_SLURM_MEMORY_GIB_PER_NODE
  WorkFlow_SLURM_PARTITION
  WorkFlow_SLURM_POLL_SECONDS
  WorkFlow_SLURM_QUEUE_START_TIMEOUT_SECONDS
  WorkFlow_SLURM_RESULT_GRACE_SECONDS
  WorkFlow_SLURM_SACCT
  WorkFlow_SLURM_SBATCH
  WorkFlow_SLURM_SCANCEL
  WorkFlow_SLURM_SCONTROL
  WorkFlow_SLURM_SINFO
  WorkFlow_SLURM_SQUEUE
  WorkFlow_SLURM_TIME_LIMIT
  WORKFLOW_WEB_PORT
)

is_managed_environment_name() {
  local requested="$1"
  local managed
  for managed in "${CONTROL_PLANE_ENVIRONMENT[@]}"; do
    if [[ "$requested" == "$managed" ]]; then
      return 0
    fi
  done
  return 1
}

load_control_plane_config() {
  if [[ -L "$CONFIG_PATH" || ! -f "$CONFIG_PATH" || ! -r "$CONFIG_PATH" ]]; then
    echo "Control-plane config must be a readable regular non-symlink file: $CONFIG_PATH" >&2
    exit 2
  fi
  local line
  local line_number=0
  local name
  local value
  while IFS= read -r line || [[ -n "$line" ]]; do
    line_number=$((line_number + 1))
    line="${line%$'\r'}"
    if [[ -z "$line" || "$line" == \#* ]]; then
      continue
    fi
    if [[ "$line" != *=* ]]; then
      echo "$CONFIG_PATH:$line_number must use NAME=VALUE syntax." >&2
      exit 2
    fi
    name="${line%%=*}"
    value="${line#*=}"
    if ! is_managed_environment_name "$name"; then
      echo "$CONFIG_PATH:$line_number contains unsupported setting $name." >&2
      exit 2
    fi
    printf -v "$name" '%s' "$value"
  done < "$CONFIG_PATH"
}

write_control_plane_config() {
  if [[ -e "$CONFIG_PATH" || -L "$CONFIG_PATH" ]]; then
    echo "Refusing to overwrite existing control-plane config: $CONFIG_PATH" >&2
    echo "Edit that file directly, or remove it intentionally and run configure again." >&2
    exit 1
  fi
  local scheduler_host="${WorkFlow_DASK_SCHEDULER_HOST:-$(hostname -f)}"
  local allow_insecure="${WorkFlow_DASK_ALLOW_INSECURE_CLUSTER:-0}"
  local tls_ca="${WorkFlow_DASK_TLS_CA:-}"
  local tls_cert="${WorkFlow_DASK_TLS_CERT:-}"
  local tls_key="${WorkFlow_DASK_TLS_KEY:-}"
  if [[ -z "$scheduler_host" ]]; then
    echo "Cannot discover a Scheduler host; set WorkFlow_DASK_SCHEDULER_HOST." >&2
    exit 2
  fi
  if [[ "$allow_insecure" != 0 && "$allow_insecure" != 1 ]]; then
    echo "WorkFlow_DASK_ALLOW_INSECURE_CLUSTER must be 0 or 1." >&2
    exit 2
  fi
  if [[ -z "$tls_ca$tls_cert$tls_key" && "$allow_insecure" != 1 ]]; then
    echo "Explicitly set WorkFlow_DASK_ALLOW_INSECURE_CLUSTER=1 for a trusted test network," >&2
    echo "or provide all three WorkFlow_DASK_TLS_* paths before configuring." >&2
    exit 2
  fi
  if [[ -n "$tls_ca$tls_cert$tls_key" ]] \
    && [[ -z "$tls_ca" || -z "$tls_cert" || -z "$tls_key" ]]; then
    echo "WorkFlow_DASK_TLS_CA, WorkFlow_DASK_TLS_CERT and WorkFlow_DASK_TLS_KEY must be set together." >&2
    exit 2
  fi
  for value in \
    "$scheduler_host" "$allow_insecure" \
    "$tls_ca" "$tls_cert" "$tls_key" \
    "${WorkFlow_DASK_SCHEDULER_PORT:-8786}" \
    "${WorkFlow_DASK_DASHBOARD_ADDRESS:-127.0.0.1:8787}" \
    "${WorkFlow_DASHBOARD_HOST:-127.0.0.1:18787}" \
    "${WorkFlow_SLURM_EXCLUDED_PARTITIONS:-mn,control}" \
    "${WorkFlow_SLURM_MAX_CPUS:-256}"; do
    if [[ "$value" == *$'\n'* || "$value" == *$'\r'* ]]; then
      echo "Control-plane configuration values must not contain newlines." >&2
      exit 2
    fi
  done

  mkdir -p "${CONFIG_PATH%/*}"
  umask 077
  {
    printf '%s\n' \
      '# WorkFlow control-plane site configuration. Values are literal; do not add export or shell quotes.' \
      "WorkFlow_DASK_SCHEDULER_HOST=$scheduler_host" \
      "WorkFlow_DASK_SCHEDULER_PORT=${WorkFlow_DASK_SCHEDULER_PORT:-8786}" \
      "WorkFlow_DASK_DASHBOARD_ADDRESS=${WorkFlow_DASK_DASHBOARD_ADDRESS:-127.0.0.1:8787}" \
      "WorkFlow_DASHBOARD_HOST=${WorkFlow_DASHBOARD_HOST:-127.0.0.1:18787}" \
      "WorkFlow_DASK_ALLOW_INSECURE_CLUSTER=$allow_insecure" \
      "WorkFlow_SLURM_EXCLUDED_PARTITIONS=${WorkFlow_SLURM_EXCLUDED_PARTITIONS:-mn,control}" \
      "WorkFlow_SLURM_MAX_CPUS=${WorkFlow_SLURM_MAX_CPUS:-256}"
    if [[ -v WorkFlow_SLURM_SACCT ]]; then
      printf 'WorkFlow_SLURM_SACCT=%s\n' "$WorkFlow_SLURM_SACCT"
    fi
    if [[ -n "$tls_ca$tls_cert$tls_key" ]]; then
      printf '%s\n' \
        "WorkFlow_DASK_TLS_CA=$tls_ca" \
        "WorkFlow_DASK_TLS_CERT=$tls_cert" \
        "WorkFlow_DASK_TLS_KEY=$tls_key"
    fi
  } > "$CONFIG_PATH"
  chmod 600 "$CONFIG_PATH"
  echo "WorkFlow control-plane config created."
  echo "config=$CONFIG_PATH"
  echo "scheduler=$scheduler_host"
  echo "worker_network=route-selected-ipv4"
  echo "max_cpus=${WorkFlow_SLURM_MAX_CPUS:-256}"
}

# Once configured, the file is authoritative. Clear every managed value from
# the caller/tmux environment before loading it, so stale partition overrides
# cannot silently return and operators do not need repeated `unset` commands.
if [[ "$ACTION" != configure && ( -e "$CONFIG_PATH" || -L "$CONFIG_PATH" ) ]]; then
  for variable_name in "${CONTROL_PLANE_ENVIRONMENT[@]}"; do
    unset "$variable_name"
  done
  load_control_plane_config
fi

# Older configs required one interface name for every compute node.  Keep the
# name in the parser allowlist so existing files migrate without an edit, but
# never export it: Worker jobs now bind to the per-node route-selected IPv4.
if [[ -n "${WorkFlow_DASK_INTERFACE:-}" ]]; then
  echo "Ignoring deprecated WorkFlow_DASK_INTERFACE; Workers use route-selected IPv4 addresses." >&2
  unset WorkFlow_DASK_INTERFACE
fi

WEB_PORT="${WORKFLOW_WEB_PORT:-8000}"
READY_URL="http://127.0.0.1:$WEB_PORT/plugin_status"

if [[ ! "$SESSION_NAME" =~ ^[A-Za-z0-9_-]+$ ]]; then
  echo "WORKFLOW_CONTROL_PLANE_SESSION contains unsupported characters." >&2
  exit 2
fi
if [[ ! -x "$START_SCRIPT" ]]; then
  echo "Missing executable control-plane entrypoint: $START_SCRIPT" >&2
  exit 1
fi
if [[ "$ACTION" != configure ]] && ! command -v tmux >/dev/null 2>&1; then
  echo "tmux is required for the user-managed control plane." >&2
  echo "Ask the administrator for a system service if login-node daemons are not allowed." >&2
  exit 1
fi

session_exists() {
  tmux has-session -t "$SESSION_NAME" 2>/dev/null
}

start_control_plane() {
  if session_exists; then
    echo "WorkFlow control plane is already running (tmux=$SESSION_NAME)."
    return 0
  fi
  mkdir -p "$WORKFLOW_RUNTIME_DIR/logs"
  # A long-lived tmux server keeps its own environment.  Remove every managed
  # setting first so that unsetting a value in the operator's current shell
  # really restores the default instead of resurrecting a stale tmux value.
  # Other site environment (PATH, HOME, Slurm/Munge configuration, locale)
  # remains intact.
  launch=(
    env
    -u WORKFLOW_ROOT
    -u WORKFLOW_RUNTIME_DIR
  )
  for variable_name in "${CONTROL_PLANE_ENVIRONMENT[@]}"; do
    launch+=(-u "$variable_name")
  done
  launch+=(
    "WORKFLOW_ROOT=$WORKFLOW_ROOT"
    "WORKFLOW_RUNTIME_DIR=$WORKFLOW_RUNTIME_DIR"
  )
  for variable_name in "${CONTROL_PLANE_ENVIRONMENT[@]}"; do
    if [[ -v "$variable_name" ]]; then
      launch+=("$variable_name=${!variable_name}")
    fi
  done
  launch+=("$START_SCRIPT")
  printf -v launch_command '%q ' "${launch[@]}"
  printf -v launch_command 'exec %s>>%q 2>&1' "$launch_command" "$LOG_PATH"
  tmux new-session -d -s "$SESSION_NAME" "$launch_command"
  ready=false
  for _ in {1..120}; do
    if ! session_exists; then
      echo "WorkFlow control plane exited during startup. See $LOG_PATH" >&2
      exit 1
    fi
    if curl --silent --fail --max-time 2 --output /dev/null "$READY_URL"; then
      ready=true
      break
    fi
    sleep 0.5
  done
  if [[ "$ready" != true ]]; then
    echo "WorkFlow control plane did not become HTTP-ready within 60 seconds." >&2
    echo "See $LOG_PATH" >&2
    exit 1
  fi
  echo "WorkFlow control plane started (tmux=$SESSION_NAME)."
  if [[ -f "$CONFIG_PATH" ]]; then
    echo "config=$CONFIG_PATH"
  fi
  echo "log=$LOG_PATH"
  echo "remote=http://127.0.0.1:$WEB_PORT"
}

stop_control_plane() {
  if ! session_exists; then
    echo "WorkFlow control plane is not running."
    return 0
  fi
  tmux send-keys -t "$SESSION_NAME" C-c
  for _ in {1..20}; do
    if ! session_exists; then
      echo "WorkFlow control plane stopped."
      return 0
    fi
    sleep 0.5
  done
  tmux kill-session -t "$SESSION_NAME"
  echo "WorkFlow control plane required a forced tmux-session stop." >&2
}

case "$ACTION" in
  configure)
    write_control_plane_config
    ;;
  start)
    start_control_plane
    ;;
  stop)
    stop_control_plane
    ;;
  restart)
    stop_control_plane
    start_control_plane
    ;;
  status)
    if session_exists; then
      echo "running (tmux=$SESSION_NAME, log=$LOG_PATH)"
    else
      echo "stopped (tmux=$SESSION_NAME)"
      exit 3
    fi
    ;;
  logs)
    if [[ ! -f "$LOG_PATH" ]]; then
      echo "Control-plane log does not exist yet: $LOG_PATH" >&2
      exit 1
    fi
    exec tail -n 200 -f "$LOG_PATH"
    ;;
  *)
    echo "Usage: control_plane.sh {configure|start|stop|restart|status|logs}" >&2
    exit 2
    ;;
esac
