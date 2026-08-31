#!/usr/bin/env bash

set -euo pipefail

WORKFLOW_ROOT="${WORKFLOW_ROOT:-$HOME/apps/WorkFlow}"
WORKFLOW_RUNTIME_DIR="${WORKFLOW_RUNTIME_DIR:-$HOME/workflow-runtime}"
WORKFLOW_REPOSITORY="${WORKFLOW_REPOSITORY:-https://github.com/hexiyimeng/WorkFlow.git}"
WORKFLOW_BRANCH="${WORKFLOW_BRANCH:-master}"
UV_VERSION="${UV_VERSION:-0.11.32}"
UV_BIN="${UV_BIN:-$HOME/.workflow-deploy/bin/uv}"
UV_CACHE_DIR="${UV_CACHE_DIR:-$HOME/.cache/workflow-uv}"
MODEL_SHA256="e1440429eb384f95afe32bcba6510f90d518eaedc917ede549bed6804004abe2"
MODEL_PATH="$WORKFLOW_RUNTIME_DIR/models/cellpose/cpsam"
MODEL_URLS=(
  "https://huggingface.co/mouseland/cellpose-sam/resolve/main/cpsam"
  "https://hf-mirror.com/mouseland/cellpose-sam/resolve/main/cpsam"
)

if [[ ! "$WORKFLOW_BRANCH" =~ ^[A-Za-z0-9][A-Za-z0-9._/-]*$ ]] \
  || [[ "$WORKFLOW_BRANCH" == *..* ]] \
  || [[ "$WORKFLOW_BRANCH" == */ ]]; then
  echo "WORKFLOW_BRANCH is not a valid deployable Git branch: $WORKFLOW_BRANCH" >&2
  exit 2
fi

mkdir -p \
  "$(dirname "$UV_BIN")" \
  "$(dirname "$WORKFLOW_ROOT")" \
  "$UV_CACHE_DIR" \
  "$(dirname "$MODEL_PATH")" \
  "$WORKFLOW_RUNTIME_DIR/logs" \
  "$WORKFLOW_RUNTIME_DIR/config" \
  "$WORKFLOW_RUNTIME_DIR/state" \
  "$WORKFLOW_RUNTIME_DIR/data" \
  "$WORKFLOW_RUNTIME_DIR/output" \
  "$WORKFLOW_RUNTIME_DIR/recovery"

if [[ ! -x "$UV_BIN" ]]; then
  curl -LsSf "https://astral.sh/uv/$UV_VERSION/install.sh" \
    | env UV_UNMANAGED_INSTALL="$(dirname "$UV_BIN")" sh
fi

if [[ ! -d "$WORKFLOW_ROOT/.git" ]]; then
  if [[ -e "$WORKFLOW_ROOT" ]]; then
    echo "Refusing to replace non-Git path: $WORKFLOW_ROOT" >&2
    exit 1
  fi
  git -c http.version=HTTP/1.1 clone \
    --branch "$WORKFLOW_BRANCH" \
    --single-branch \
    "$WORKFLOW_REPOSITORY" \
    "$WORKFLOW_ROOT"
else
  if [[ -n "$(git -C "$WORKFLOW_ROOT" status --porcelain)" ]]; then
    echo "Refusing to update a dirty deployment: $WORKFLOW_ROOT" >&2
    exit 1
  fi
  # An explicit refspec is required here. `git fetch origin branch` updates
  # only FETCH_HEAD when an existing single-branch checkout has never seen the
  # requested branch, so the following checkout cannot discover it.
  # Also extend the remote's configured branch set. Without this, Git has the
  # remote-tracking ref on disk but `checkout --track` still rejects it as
  # "not a branch" because a prior --single-branch clone tracks only master.
  branch_refspec="+refs/heads/$WORKFLOW_BRANCH:refs/remotes/origin/$WORKFLOW_BRANCH"
  if ! git -C "$WORKFLOW_ROOT" config --get-all remote.origin.fetch \
    | grep -Fqx "$branch_refspec"; then
    git -C "$WORKFLOW_ROOT" remote set-branches --add origin "$WORKFLOW_BRANCH"
  fi
  git -C "$WORKFLOW_ROOT" fetch origin \
    "$branch_refspec"
  if git -C "$WORKFLOW_ROOT" show-ref \
    --verify --quiet "refs/heads/$WORKFLOW_BRANCH"; then
    git -C "$WORKFLOW_ROOT" checkout "$WORKFLOW_BRANCH"
  else
    git -C "$WORKFLOW_ROOT" checkout \
      --branch "$WORKFLOW_BRANCH" \
      --track "origin/$WORKFLOW_BRANCH"
  fi
  git -C "$WORKFLOW_ROOT" merge --ff-only "origin/$WORKFLOW_BRANCH"
fi

export UV_CACHE_DIR
"$UV_BIN" python install 3.12
(
  cd "$WORKFLOW_ROOT/backend"
  "$UV_BIN" sync --frozen --no-install-project --python 3.12
  .venv/bin/python -m compileall -q .
  .venv/bin/python -c \
    "import dask, dask_jobqueue, distributed, fastapi, zarr; print('backend imports: OK')"
  .venv/bin/python \
    "$WORKFLOW_ROOT/deploy/hpc/validate_frontend_dist.py" \
    "$WORKFLOW_ROOT/backend/dist"
)

if [[ -f "$MODEL_PATH" ]]; then
  echo "$MODEL_SHA256  $MODEL_PATH" | sha256sum -c -
else
  model_downloaded=false
  for model_url in "${MODEL_URLS[@]}"; do
    if curl -fL \
      --retry 4 \
      --retry-delay 3 \
      --retry-connrefused \
      --connect-timeout 20 \
      --continue-at - \
      -o "$MODEL_PATH.partial" \
      "$model_url"; then
      model_downloaded=true
      break
    fi
  done
  if [[ "$model_downloaded" != true ]]; then
    echo "All Cellpose model endpoints failed." >&2
    exit 1
  fi
  echo "$MODEL_SHA256  $MODEL_PATH.partial" | sha256sum -c -
  mv "$MODEL_PATH.partial" "$MODEL_PATH"
fi

printf 'WorkFlow installation complete\nroot=%s\nruntime=%s\ncommit=%s\n' \
  "$WORKFLOW_ROOT" \
  "$WORKFLOW_RUNTIME_DIR" \
  "$(git -C "$WORKFLOW_ROOT" rev-parse HEAD)"
