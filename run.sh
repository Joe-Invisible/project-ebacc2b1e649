#!/bin/sh
# Entry point. Runs with no arguments; exit 0 means "done, check my output".
set -eu
: "${DATA_DIR:=/data}" "${OUTPUT_DIR:=/output}" "${SCRATCH_DIR:=/scratch}"
export DATA_DIR OUTPUT_DIR SCRATCH_DIR
# Read-only root filesystem: everything that wants a temp or home dir gets /scratch.
export TMPDIR="$SCRATCH_DIR" HOME="$SCRATCH_DIR"
# Variant: keep the arenas on 4 KB pages.
export SHOPPING_HUGEPAGES=0
status=0
/app/pipeline/native_pipeline || status=$?
if [ "$status" -eq 10 ]; then
    exec python3 /app/pipeline/launch.py
fi
exit "$status"
