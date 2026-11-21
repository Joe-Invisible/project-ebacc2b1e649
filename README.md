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

Each dataset contains 20 million events and five million clicks. Times are
medians of three interleaved local runs per build. Docker runs use
eight CPUs and 6 GB RAM, measured from container start to exit. Native
times use eight workers and include Parquet writes.

| Dataset | Previous Docker | Current Docker | Previous native | Current native |
| --- | ---: | ---: | ---: | ---: |
| Seed 7 | 1.413 s | 1.385 s | 1.20 s | 1.15 s |
| Seed 37 | 1.421 s | 1.382 s | 1.16 s | 1.11 s |

On warm inputs, a read-only probe that touched one word per cache line
took 0.08–0.11 s.
The CSV scanner with field extraction took 0.12–0.15 s before decoding
and validating timestamps, routing records, and computing the results.

Deduplication scans 16-byte event versions. A reversible 64-bit mixer
preserves exact IDs and lets deduplication reuse the partitioning hash.
The pipeline groups event payloads and clicks together by raw user, then
resolves the canonical identity once per raw user. Each session worker
writes its own user Parquet part as it finishes daily summaries. A wide
event layout or ingest timestamp falls back to the reference SQL pipeline.

For people with at least 16 events, the session workers collect 30-minute
bins across their linked IDs. Events within a bin are connected by the
gap rule. Comparing the last event of a bin with the first of the next
occupied bin gives the same session boundaries as sorting every event.
The workers retain event counts and purchases, and assign each session
to the UTC day of its first event. Smaller timelines are sorted directly.
[The Dataflow Model](https://www.vldb.org/pvldb/vol8/p1792-Akidau.pdf)

The CSV reader scans newline, comma, and quote bytes together with AVX2
for short rows and aligns chunk boundaries in the reader threads. Event
readers sample the input density to reserve bucket capacity. The
implementation uses the structural-byte approach also used by simdjson
and the same allocation principle described in Polaris's ingestion trace.
[simdjson paper](https://arxiv.org/abs/1902.08318) ·
[Polaris ingestion trace](https://github.com/enkyuan/polaris/blob/9f20a22bef0446b2000c2e26b6829e63cbe486fe/PERFORMANCE.md#trace-guided-ingestion-changes)

All 37 fixtures passed and both output tables matched the reference exactly
on the public 1M dataset and both 20M datasets. The native writer produced
about 23–24 MiB of LZ4-compressed Parquet per 20M dataset.
Set `SHOPPING_PROFILE=1` to print stage timings and peak process RSS to stderr.

### Memory-access experiments

Packing event payloads into 12 bytes with purchase details in a side array
saved about 70–90 MiB of peak RSS in pinned trials. The layout added time to
both total medians. [Materialization Strategies in a Column-Oriented DBMS](https://www.cs.umd.edu/~abadi/papers/abadiicde2007.pdf)

Grouped prefetching of 16 click identity lookups reduced the median
parse-and-link stage by 19 ms on seed 7 and 37 ms on seed 37. In five
interleaved runs per build with the same compiler, total medians moved
from 1.367 to 1.368 s and from 1.368 to 1.348 s, respectively. Both
datasets matched the reference.
[Asynchronous Memory Access Chaining](https://www.vldb.org/pvldb/vol9/p252-kocberber.pdf)

Reducing event buckets from 256 to 128 made deduplication slower on both
seeds. Reducing person buckets from 512 to 128 lowered some parsing
times and made grouping and sessionization slower.
[Polaris partition experiments](https://github.com/enkyuan/polaris/blob/9f20a22bef0446b2000c2e26b6829e63cbe486fe/OPTIMIZATION_RESEARCH.md#prototype-results)

Transparent huge pages lowered the fault count in some event-buffer trials.
The slowest trial took 2.50 s wall time and 9.14 s of system CPU time as
the host's compaction counter rose. Keeping CSV offsets for ingest dates
cut some decoding work and raised deduplication time to 0.13–0.15 s when
the duplicate comparisons returned to the mapped text.
[Linux transparent huge-page documentation](https://kernel.org/doc/html/latest/admin-guide/mm/transhuge.html)

A preliminary scan of event IDs added about 0.4 s on both 20M inputs.
Moving ingest order into payloads reduced version records from 16 to 12
bytes, with mixed total timings. Aggregating during raw-user routing
reduced roughly 19.6 million events to 6.3 million cells. On the repeat,
the added grouping time exceeded the savings in sessionization.

### Compiler experiment

GCC profile-guided optimization was trained on the public 1M and a separate
5M dataset. The table gives medians of three warmed, interleaved native
eight-worker runs per build. Both 20M evaluation datasets matched exactly.

| Dataset | GCC -O3 | GCC -O3 with PGO |
| --- | ---: | ---: |
| Seed 7 | 1.15 s | 1.07 s |
| Seed 37 | 1.11 s | 1.05 s |

Use GCC and existing training data to build the profiled local executable.
The target installs it after training and compilation succeed.

```sh
make native-profiled PGO_DATA=dev PGO_EXTRA_DATA=/path/to/training-5m
```

`PGO_EXTRA_DATA` is optional. Run `make -B native-engine` to rebuild the
regular executable. The Docker build uses `-O3 -march=x86-64-v3` on amd64.

## Run and test locally

You need Docker with buildx, and Python 3.10+ with `polars` for the test scripts. Install it with `pip install polars`.

Build the image and run all 37 fixtures, including the leap-day session case.

```sh
make build        # docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
make fixtures     # runs the image with scorer flags and diffs each case against expected/
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
- `fixtures/` holds the edge cases, each with an `input/` folder and the `expected/` tables.
- `scripts/` runs your image the way the scorer does and diffs the output.
