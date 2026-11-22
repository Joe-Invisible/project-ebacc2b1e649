#!/bin/sh
# Regenerate the GCC profile that the Docker build uses for profile-guided
# optimization. Run it after changing the native pipeline:
#
#   scripts/train_pgo.sh DATASET [DATASET...]
#
# Training runs in the same base image and compiler as the Dockerfile's build
# stage, with the same source paths, so the profile matches the build exactly.
set -eu
if [ "$#" -eq 0 ]; then
    echo "usage: $0 DATASET [DATASET...]" >&2
    exit 2
fi
here=$(cd "$(dirname "$0")/.." && pwd)
image=python:3.12-bookworm
mounts=""
runs=""
index=0
for dataset in "$@"; do
    dataset=$(cd "$dataset" && pwd)
    mounts="$mounts -v $dataset:/train/$index:ro"
    runs="$runs SHOPPING_PGO_TRAINING=1 DATA_DIR=/train/$index OUTPUT_DIR=/tmp/out-$index NCPU=8 /tmp/train;"
    index=$((index + 1))
done
# Train into a temporary folder; the committed profile is only replaced
# once training succeeds.
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
# shellcheck disable=SC2086
docker run --rm --platform linux/amd64 --user "$(id -u):$(id -g)" \
    -v "$here/pipeline:/pipeline:ro" -v "$work:/pgo" $mounts \
    "$image" sh -euc "
        mkdir -p /tmp/src && cp /pipeline/*.cpp /pipeline/*.hpp /tmp/src/
        cd /tmp/src
        g++ -std=c++20 -O3 -DNDEBUG -march=x86-64-v3 -pthread \
            -fprofile-generate -fprofile-update=atomic \
            -c native.cpp -o native.o
        g++ -pthread -fprofile-generate native.o -Wl,-l:liblz4.so.1 -o /tmp/train
        $runs
        cp /tmp/src/*.gcda /pgo/
    "
ls "$work"/*.gcda >/dev/null
rm -rf "$here/pipeline/pgo"
mkdir -p "$here/pipeline/pgo"
cp "$work"/*.gcda "$here/pipeline/pgo/"
ls -l "$here/pipeline/pgo"
