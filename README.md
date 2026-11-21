# Shopping Sessions

The pipeline reads the challenge CSVs, resolves linked IDs, and writes the
`user_daily` and `campaign_daily` Parquet tables. See [rules.md](rules.md) for
the input dialect and output definitions.

The container runs a C++20 pipeline. It reads CSV parts in parallel, builds
the linked-ID graph concurrently, and deduplicates events by exact ID before
validation can drop the earliest copy. Compact per-person timelines drive
sessionization and seven-day click attribution. Each worker writes its own
LZ4-compressed Parquet output through the native writer.

The source includes the original DuckDB SQL pipeline for reference and for
inputs that exceed the native record layout's timestamp or category limits.
[Earlier SQL and DuckDB window benchmark notes](experiments/README.md)

## Native pipeline timings

Each dataset contains 20 million events and five million clicks. These are
medians of three local runs with eight Docker CPUs and 6 GB RAM, measured
from container start to exit.

| Dataset | Previous C++ image | Current image | Current runs |
| --- | ---: | ---: | --- |
| Seed 7 | 2.811 s | 1.956 s | 2.395, 1.956, 1.872 s |
| Seed 37 | 2.583 s | 1.867 s | 1.991, 1.867, 1.867 s |

Both output tables matched the reference exactly on both datasets. The
native writer produced about 23–24 MiB of LZ4-compressed Parquet per dataset.
Set `SHOPPING_PROFILE=1` to print stage timings and peak process RSS to stderr.

## Run and test locally

You need Docker with buildx, and Python 3.10+ with `polars` for the test scripts. Install it with `pip install polars`.

Build the image and run all 36 fixtures:

```sh
make build        # docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
make fixtures     # runs the image on all 36 fixtures with scorer flags, diffs each against expected/
```

Build, run, and check the 1M-event dev set. `make dev` needs `generate` on your PATH: build it from the track page's generator.zip (Rust 1.85 or newer; its README has the two commands).

```sh
make dev                                # generate --seed 1 --events 1000000 --clicks 250000 --output dev
make run-dev                            # runs your image on dev/ with scorer flags and times it
make check-dev                          # exact diff against dev-answer/, the track page's dev-answer.zip unzipped here
```

If you extracted the published answer beside this directory, run
`make check-dev ANSWER=../dev-answer`.

If your laptop cannot spare 8 cores and 24 GB, run with lower limits using `make run-dev CPUS=4 MEM=8g`. Your local times will not match the scorer anyway; only the board's times count.

- **x86 Linux and Windows machines.** Build and run the amd64 image natively.
- **Apple-silicon Macs.** Emulating amd64 is slow. For fast local loops, build a native image with `make build run-dev PLATFORM=linux/arm64`. The Dockerfile pins no platform, so it builds either way. Check the amd64 image before you submit.

To build and check the native pipeline without Docker, use a C++20 compiler,
the Linux LZ4 runtime (`liblz4.so.1`), and the checker dependencies.

```sh
make native-engine
pip install -r requirements.txt polars
export SCRATCH_DIR=${TMPDIR:-/tmp}
python3 scripts/run_fixtures.py fixtures -- python3 pipeline/launch.py
```

The reference SQL pipeline can also be tested directly. `make native` builds
its optional C++20 normalizer.

```sh
pip install -r requirements.txt polars
make native
export SCRATCH_DIR=${TMPDIR:-/tmp}
python3 scripts/run_fixtures.py fixtures -- python3 pipeline/pipeline.py
```

## Submitting and scoring

Submit your public GitHub repository on the Buildathon track page. Enter a commit SHA to score a specific revision, or leave it empty to score the newest commit on the default branch.

The image's `CMD` runs with no arguments. Exit code 0 means "done, check my output". Build time is never timed; only the run is timed. The track page lists the current practice limits and how the final is scored.

## Container limits

Scorers are Google Cloud n2 Intel Xeon machines, Cascade Lake or newer, with AVX-512 available. Your container gets:

| Resource | Setting |
| --- | --- |
| `/data` | The dataset, read-only |
| `/output` | Empty, writable. Write `/output/user_daily/` and `/output/campaign_daily/` as Parquet |
| `/scratch` | Empty, writable, fast local disk for spills and temp files |
| CPU / RAM | 8 vCPUs, hyperthreads of an Intel Xeon, 24 GB, no swap |
| Network | None |
| Env vars | `DATA_DIR=/data`, `OUTPUT_DIR=/output`, `SCRATCH_DIR=/scratch`, `HOME=/scratch`, `NCPU=8`, `MEM_GB=24` |
| Filesystem | Root is read-only apart from `/tmp`, 64 MB in memory. The process runs as uid 1000, not root |

## Common pitfalls

- **DuckDB's CSV reader differs from `rules.md`.** It reads a quoted empty field `""` as an empty string, not null. Its malformed-line handling, including `ignore_errors`, `strict_mode`, and `null_padding`, does not match `rules.md`. Test against the fixtures before relying on it.
- **Memory.** `pipeline.py` sets `memory_limit` to 80% of `MEM_GB` and `temp_directory` under `/scratch`, so big joins and sorts spill to disk instead of getting the container killed.
- **No extension downloads.** The container has no network at run time. Parquet and CSV are built into the wheel; anything else must be installed at build time.
- **`\d` is not `[0-9]`.** In several regex engines, including Rust's used by Polars, `\d` matches any Unicode digit. The rules mean ASCII digits only.
- **Extra files in output folders.** `_SUCCESS` and other non-Parquet files in the output folders are ignored.

## Self-checks

- Summed over all rows, `sessions` and `revenue_cents` are equal in both tables.
- `day` must be a Parquet `DATE`, not a string or timestamp.
- Row order and the number of Parquet files do not matter. Exact column names do, and there must be no extra columns.

## Files in this starter

- `pipeline/launch.py` reads the manifest and starts the native pipeline.
- `pipeline/native.cpp` handles deduplication, linked identities, sessions, and attribution.
- `pipeline/native_input.hpp` maps and parses the CSV input and encodes IDs.
- `pipeline/native_output.hpp` writes typed Parquet directly from C++.
- `pipeline/pipeline.py` is the reference SQL pipeline.
- `Dockerfile` builds a `linux/amd64` image for the Intel Xeon scorers.
- `fixtures/` holds the 36 edge cases, each with an `input/` folder and the `expected/` tables.
- `scripts/` runs your image the way the scorer does and diffs the output.
