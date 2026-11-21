IMAGE ?= shopping-sessions:latest
CPUS  ?= 8
MEM   ?= 24g
# The scorer's platform. PLATFORM=linux/arm64 builds and runs natively on an Apple-silicon Mac, for fast local loops.
PLATFORM ?= linux/amd64
DEV   ?= dev
PGO_DATA ?= $(DEV)
PGO_EXTRA_DATA ?=
# In the monorepo the fixtures live two levels up; the published starter has its own copy.
FIXTURES ?= $(if $(wildcard fixtures),fixtures,../../fixtures)
export IMAGE CPUS MEM PLATFORM

.PHONY: build native native-engine native-profiled fixtures dev run-dev check-dev

## Build the image for the scorer's platform, linux/amd64 (emulated on Apple silicon)
build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

## Build the CSV normalizer for direct local runs
native: pipeline/normalize

pipeline/normalize: pipeline/normalize.cpp pipeline/timestamp.hpp
	$(CXX) $(CXXFLAGS) -std=c++20 -O3 -DNDEBUG $< -o $@

## Build the native submission locally
NATIVE_ARCH ?= $(if $(filter x86_64,$(shell uname -m)),-march=x86-64-v3)
NATIVE_FLAGS = $(CXXFLAGS) -std=c++20 -O3 -DNDEBUG $(NATIVE_ARCH) -pthread
native-engine: pipeline/native_pipeline

pipeline/native_pipeline: pipeline/native.cpp pipeline/native_input.hpp pipeline/native_output.hpp pipeline/timestamp.hpp
	$(CXX) $(NATIVE_FLAGS) $< -Wl,-l:liblz4.so.1 -o $@

## Train and install a native GCC build using a separate practice dataset
native-profiled:
	set -eu; \
	work=$$(mktemp -d); \
	staged=; \
	trap 'rm -rf "$$work"; if [ -n "$$staged" ]; then rm -f "$$staged"; fi' EXIT; \
	$(CXX) $(NATIVE_FLAGS) -fprofile-generate="$$work/profile" \
	    -fprofile-update=atomic pipeline/native.cpp -Wl,-l:liblz4.so.1 -o "$$work/native_pipeline"; \
	for dataset in "$(PGO_DATA)" "$(PGO_EXTRA_DATA)"; do \
	    if [ -n "$$dataset" ]; then \
	        SHOPPING_PGO_TRAINING=1 DATA_DIR="$$dataset" OUTPUT_DIR="$$work/output" NCPU="$(CPUS)" "$$work/native_pipeline"; \
	    fi; \
	done; \
	$(CXX) $(NATIVE_FLAGS) -fprofile-use="$$work/profile" -fprofile-correction \
	    -Werror=missing-profile pipeline/native.cpp -Wl,-l:liblz4.so.1 -o "$$work/native_pipeline"; \
	staged=$$(mktemp pipeline/.native-pipeline.XXXXXX); \
	install -m 755 "$$work/native_pipeline" "$$staged"; \
	mv -f "$$staged" pipeline/native_pipeline

## Run the image on all 36 edge-case fixtures and diff against the expected tables
fixtures: build
	python3 scripts/run_fixtures.py $(FIXTURES) -- scripts/docker_run.sh

## Make the 1M-event dev set with the published generator (needs `generate` on PATH)
dev:
	generate --seed 1 --events 1000000 --clicks 250000 --output $(DEV)

## Run on the dev set with scorer flags and time it
run-dev: build
	rm -rf out && time scripts/docker_run.sh $(DEV) out

## Compare your dev output with the published dev answer (dev-answer.zip, unzipped): make check-dev ANSWER=dev-answer
ANSWER ?= dev-answer
check-dev:
	python3 scripts/compare.py out $(ANSWER)
