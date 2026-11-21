#!/bin/sh
# Build against the same DuckDB release as the benchmark image.
set -eu
if [ "$#" -ne 2 ]; then
    echo "usage: $0 DUCKDB_V2_SOURCE OUTPUT_DIR" >&2
    exit 2
fi
source_dir=$(realpath "$1")
mkdir -p "$2"
output=$(realpath "$2")
experiment=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
extension=$output/shopping_window.duckdb_extension
printf '%s' linux_amd64 >"$output/platform"
"${CXX:-g++}" -std=c++17 -O3 -fPIC -shared -DNDEBUG \
    -I"$source_dir/src/include" "$experiment/shopping_window.cpp" -o "$extension"
cmake -DEXTENSION="$extension" -DPLATFORM_FILE="$output/platform" \
    -DVERSION_FIELD=v2.0.0 -DABI_TYPE=CPP -DNULL_FILE="$source_dir/scripts/null.txt" \
    -P "$source_dir/scripts/append_metadata.cmake"
