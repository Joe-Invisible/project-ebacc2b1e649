IMAGE ?= shopping-sessions:latest
CPUS  ?= 8
MEM   ?= 24g
# The scorer's platform. PLATFORM=linux/arm64 builds and runs natively on an Apple-silicon Mac, for fast local loops.
PLATFORM ?= linux/amd64
DEV   ?= dev
# In the monorepo the fixtures live two levels up; the published starter has its own copy.
FIXTURES ?= $(if $(wildcard fixtures),fixtures,../../fixtures)
export IMAGE CPUS MEM PLATFORM

.PHONY: build native native-engine fixtures dev run-dev check-dev

## Build the image for the scorer's platform, linux/amd64 (emulated on Apple silicon)
build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

## Build the CSV normalizer for direct local runs
native: pipeline/normalize

pipeline/normalize: pipeline/normalize.cpp pipeline/timestamp.hpp
	$(CXX) $(CXXFLAGS) -std=c++20 -O3 -DNDEBUG $< -o $@

## Build the native submission locally
NATIVE_ARCH ?= $(if $(filter x86_64,$(shell uname -m)),-march=x86-64-v3)
native-engine: pipeline/native_pipeline

pipeline/native_pipeline: pipeline/native.cpp pipeline/native_input.hpp pipeline/native_output.hpp pipeline/timestamp.hpp
	$(CXX) $(CXXFLAGS) -std=c++20 -O3 -DNDEBUG $(NATIVE_ARCH) -pthread $< -Wl,-l:liblz4.so.1 -o $@

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
