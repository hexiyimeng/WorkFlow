#!/usr/bin/env bash

set -euo pipefail

WORKFLOW_RUNTIME_DIR="${WORKFLOW_RUNTIME_DIR:-$HOME/workflow-runtime}"
ARCHIVE="$WORKFLOW_RUNTIME_DIR/data/demo_images.zip"
ARCHIVE_URL="https://www.cellpose.org/static/images/demo_images.zip"
ARCHIVE_SHA256="afc6bbfeda55aa6eb7a335934b508e652af19363c92226800f7debadef44a522"

mkdir -p "$(dirname "$ARCHIVE")"
if [[ ! -f "$ARCHIVE" ]]; then
  curl -fL --retry 8 --retry-delay 3 --continue-at - \
    -o "$ARCHIVE.partial" \
    "$ARCHIVE_URL"
  echo "$ARCHIVE_SHA256  $ARCHIVE.partial" | sha256sum -c -
  mv "$ARCHIVE.partial" "$ARCHIVE"
else
  echo "$ARCHIVE_SHA256  $ARCHIVE" | sha256sum -c -
fi
printf 'Actual-data archive ready: %s\n' "$ARCHIVE"
