#!/bin/sh
# Compare five complete pipeline runs under the same container limits.
set -eu
if [ "$#" -ne 3 ]; then
    echo "usage: $0 DATA_DIR OUTPUT_ROOT SHOPPING_WINDOW_EXTENSION" >&2
    exit 2
fi
repo=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
data=$(realpath "$1")
mkdir -p "$2"
output=$(realpath "$2")
extension=$(realpath "$3")
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
chmod 777 "$scratch"
cpus=${CPUS:-2}
mem=${MEM:-6g}
stable_image=${STABLE_IMAGE:-shopping-sessions:latest}
new_image=${DUCKDB20_IMAGE:-shopping-sessions:duckdb20}

run() {
    label=$1
    image=$2
    shift 2
    mkdir -p "$output/$label"
    chmod 777 "$output/$label"
    /usr/bin/time -f "$label %e s" docker run --rm --network none --read-only \
        --tmpfs /tmp:rw,size=64m,mode=1777 --cap-drop ALL --security-opt no-new-privileges \
        --pids-limit 4096 --cpus "$cpus" --memory "$mem" --memory-swap "$mem" --user 1000:1000 \
        -v "$data":/data:ro -v "$output/$label":/output -v "$scratch":/scratch \
        -v "$repo/pipeline/pipeline.py":/app/pipeline/pipeline.py:ro \
        -v "$repo/experiments/run_variant.py":/app/experiments/run_variant.py:ro \
        -v "$extension":/experiments/shopping_window.duckdb_extension:ro \
        -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch \
        -e NCPU="$cpus" -e MEM_GB="${mem%g}" --entrypoint python3 "$image" /app/experiments/run_variant.py "$@"
}

run 156-baseline "$stable_image" baseline
run 156-sql "$stable_image" sql
run 200-baseline "$new_image" baseline
run 200-sql "$new_image" sql
run 200-extension "$new_image" extension --extension /experiments/shopping_window.duckdb_extension

for variant in 156-sql 200-baseline 200-sql 200-extension; do
    echo "Comparing $variant with 156-baseline"
    python3 "$repo/scripts/compare.py" "$output/$variant" "$output/156-baseline"
done
