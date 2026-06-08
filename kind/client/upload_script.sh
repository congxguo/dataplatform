#!/usr/bin/env bash

set -e

# --- Configurable variables ---
NAMESPACE="client"
POD_NAME="python-toolbox"
DEST_PATH="/opt"   # where to put the file inside the pod
# ------------------------------

FILE_NAME="$1"

if [ -z "$FILE_NAME" ]; then
  echo "Usage: $0 <filename>"
  exit 1
fi

# Find the file in current directory
LOCAL_FILE=$(find . -maxdepth 1 -type f -name "$FILE_NAME")

if [ -z "$LOCAL_FILE" ]; then
  echo "Error: File '$FILE_NAME' not found in current directory."
  exit 1
fi

echo "Found file: $LOCAL_FILE"

# Check pod exists
if ! kubectl get pod "$POD_NAME" -n "$NAMESPACE" >/dev/null 2>&1; then
  echo "Error: Pod '$POD_NAME' not found in namespace '$NAMESPACE'"
  exit 1
fi

echo "Uploading to pod: $POD_NAME (namespace: $NAMESPACE)"
kubectl cp "$LOCAL_FILE" "$NAMESPACE/$POD_NAME:$DEST_PATH/"

echo "Upload complete!"

