#!/bin/sh
# Entry point. Runs with no arguments; exit 0 means "done, check my output".
set -eu
: "${DATA_DIR:=/data}" "${OUTPUT_DIR:=/output}" "${SCRATCH_DIR:=/scratch}"
export DATA_DIR OUTPUT_DIR SCRATCH_DIR
# Read-only root filesystem: everything that wants a temp or home dir gets /scratch.
export TMPDIR="$SCRATCH_DIR" HOME="$SCRATCH_DIR"
# Use the AVX-512 build only where the CPU reports every x86-64-v4 feature.
engine=/app/pipeline/native_pipeline
flags=$(grep -m1 '^flags' /proc/cpuinfo 2>/dev/null || true)
avx512=1
for feature in avx512f avx512bw avx512cd avx512dq avx512vl; do
    case " $flags " in *" $feature "*) ;; *) avx512=0 ;; esac
done
if [ "$avx512" = 1 ] && [ -x /app/pipeline/native_pipeline_avx512 ]; then
    engine=/app/pipeline/native_pipeline_avx512
fi
status=0
"$engine" || status=$?
# An illegal instruction (signal 4) means the CPU lacks a feature after all.
if [ "$status" -eq 132 ] && [ "$engine" != /app/pipeline/native_pipeline ]; then
    status=0
    /app/pipeline/native_pipeline || status=$?
fi
if [ "$status" -eq 10 ]; then
    exec python3 /app/pipeline/launch.py
fi
exit "$status"
