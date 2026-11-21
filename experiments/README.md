# Sessionization benchmarks

The SQL variant detects boundaries with `lag` and propagates the latest
boundary with `last_value(... IGNORE NULLS)`. Both SQL variants use a `RANGE`
frame so every event at an equal timestamp sees the same session start.

The C++ variant registers one ordered window function with DuckDB 2.0.
DuckDB supplies the sorted timestamp column and partition boundaries.
The extension records each session boundary in a linear scan, then emits
the start for each output chunk. It is intended for the pipeline's ascending,
non-null timestamps partitioned by person.

## Reproduce on Linux amd64

Run from the DuckDB starter directory with Docker, a C++ compiler, CMake,
and the checker dependencies installed.

```sh
make build
docker build -f experiments/Dockerfile.duckdb20 -t shopping-sessions:duckdb20 .
git clone --depth 1 --branch v2.0.0 https://github.com/duckdb/duckdb.git /tmp/duckdb-v2.0.0
experiments/build_window.sh /tmp/duckdb-v2.0.0 /tmp/shopping-window
experiments/benchmark.sh /path/to/data /tmp/shopping-benchmark \
    /tmp/shopping-window/shopping_window.duckdb_extension
```

The benchmark runs the full pipeline with two CPUs and 6 GB by default.
It compares each variant's Parquet tables exactly against the DuckDB 1.5.6
baseline. The experimental runner loads a locally built unsigned extension
only for the extension variant.

## Local results

The generated run used seed 7, 20 million events, and 5 million clicks.
Times are end-to-end wall-clock measurements from one run per variant with
two Docker CPUs and 6 GB RAM.

| DuckDB | Sessionization | Time |
| --- | --- | ---: |
| 1.5.6 | Original SQL | 52.40 s |
| 1.5.6 | SQL rewrite | 53.55 s |
| 2.0.0 | Original SQL | 52.42 s |
| 2.0.0 | SQL rewrite | 51.86 s |
| 2.0.0 | C++ window extension | 45.36 s |

All variants produced exactly the same two Parquet tables. The C++ window
and the default pipeline each passed all 36 Docker fixtures. The default
pipeline also matched the published one-million-event answers.
