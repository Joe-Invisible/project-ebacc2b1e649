#!/bin/sh
# Entry point. Runs with no arguments; exit 0 means "done, check my output".
set -eu
: "${DATA_DIR:=/data}" "${OUTPUT_DIR:=/output}" "${SCRATCH_DIR:=/scratch}"
export DATA_DIR OUTPUT_DIR SCRATCH_DIR
# Read-only root filesystem: everything that wants a temp or home dir gets /scratch.
export TMPDIR="$SCRATCH_DIR" HOME="$SCRATCH_DIR"
# Let malloc back its own large allocations with transparent huge pages too,
# as the engine's arenas are. Measured slightly faster on the scorer's CPUs.
export GLIBC_TUNABLES=glibc.malloc.hugetlb=1
status=0
/app/pipeline/native_pipeline || status=$?
if [ "$status" -eq 10 ]; then
    exec python3 /app/pipeline/launch.py
fi
exit "$status"
