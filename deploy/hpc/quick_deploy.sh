#!/usr/bin/env bash

# Install or update WorkFlow, preserve site configuration, and restart the service.
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
CONFIG_PATH="${WORKFLOW_CONTROL_PLANE_CONFIG_FILE:-$WORKFLOW_RUNTIME_DIR/config/control-plane.env}"

WORKFLOW_ROOT="$WORKFLOW_ROOT" \
WORKFLOW_RUNTIME_DIR="$WORKFLOW_RUNTIME_DIR" \
  bash "$WORKFLOW_ROOT/deploy/hpc/install.sh"

if [[ ! -e "$CONFIG_PATH" && ! -L "$CONFIG_PATH" ]]; then
  # The first deployment must explicitly choose TLS or an isolated test network.
  # Do not silently disable Dask encryption in a reusable deployment script.
  WORKFLOW_ROOT="$WORKFLOW_ROOT" \
  WORKFLOW_RUNTIME_DIR="$WORKFLOW_RUNTIME_DIR" \
  WORKFLOW_CONTROL_PLANE_CONFIG_FILE="$CONFIG_PATH" \
    bash "$WORKFLOW_ROOT/deploy/hpc/control_plane.sh" configure
fi

WORKFLOW_ROOT="$WORKFLOW_ROOT" \
WORKFLOW_RUNTIME_DIR="$WORKFLOW_RUNTIME_DIR" \
WORKFLOW_CONTROL_PLANE_CONFIG_FILE="$CONFIG_PATH" \
  bash "$WORKFLOW_ROOT/deploy/hpc/control_plane.sh" restart

echo "WorkFlow quick deployment complete."
echo "config=$CONFIG_PATH"
