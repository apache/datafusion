<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# Staged sort

Full `ORDER BY` can sort cheap prefix keys first and evaluate later, expensive
keys only for tied rows. The sort automatically groups keys into stages; this
suite does not set the staged-sort switch or prescribe stage sizes. The same SQL
can run on `main`, which may not know that switch.

| Query | Workload                                                                                          |
| ----- | ------------------------------------------------------------------------------------------------- |
| 01    | Unique `id` prefix; `lower` and `md5` can be skipped.                                             |
| 02    | One sparse tie group (~10% of rows); only tied rows reach `md5`.                                 |
| 03    | One prefix group; all rows reach `md5` (fallback).                                                |
| 04    | Sparse ties reach `lower(payload)`, which resolves them and skips `md5`.                          |
| 05    | Nullable prefix, descending with NULLS FIRST; NULLs tie.                                          |
| 06    | Cheap keys only; control with no expensive stage.                                                 |
| 07    | TopK (`LIMIT 100`); eager fetch-path control.                                                     |
| 08    | Same keys as 02, but multiple sorted runs and merge.                                              |

Each query builds a deterministic in-memory table during `load`, outside the
timed region. Defaults: `STAGED_SORT_ROWS=200000` (minimum 100),
`STAGED_SORT_PAYLOAD_WIDTH=32` filler characters appended to the decimal ID.
IDs arrive in descending order rather than already sorted. Fixture assertions
check row count, unique IDs, 100 prefix values, the all-tied key, NULL/non-NULL
rows and minimum payload width. They also verify more than one zero sparse key
and a majority of nonzero keys equal to unique IDs: exactly one tied group, with
all other rows resolved. The zero group contains roughly 10% of the rows. A final unique ID resolves
any remaining ties. `SELECT *` keeps computed sort keys in `SortExec` rather
than projecting them first; untimed `expect_plan` checks this placement.

Each query sets `target_partitions=1`. Queries 01–07 set the in-place sort
threshold to 1 GiB, intended to give one run at defaults without a memory limit;
08 sets it to zero to exercise multiple runs. Very large row/width overrides or
memory limits can change this behavior. Check plans and execution metrics with
`--debug` or `EXPLAIN ANALYZE`; debug output affects the first run's timing.
The timed region includes scanning, sorting, merging where applicable, and
collecting the full output, not table creation or fixture assertions.

From `benchmarks/`:

```bash
./bench.sh data staged_sort  # no external data
./bench.sh run staged_sort
cargo run --release --bin benchmark_runner -- staged_sort --rows 200000 --payload-width 32 --dry-run

# Same branch, explicit A/B; use matching knobs and machine conditions.
DATAFUSION_EXECUTION_ENABLE_STAGED_SORT=false cargo run --release --bin benchmark_runner -- staged_sort
DATAFUSION_EXECUTION_ENABLE_STAGED_SORT=true cargo run --release --bin benchmark_runner -- staged_sort
```

For a `main` baseline, copy the suite files into a main worktree and leave
`DATAFUSION_EXECUTION_ENABLE_STAGED_SORT` unset; do not pass an unknown setting
to main. Use identical fixture options and settings on both branches.

## Correctness and plan checks

Persist full ordered results from the baseline and validate the candidate against
the same files. Results can be large; keep them outside the repository. The
runner's persist/validate modes execute queries, so use small fixtures for a
smoke check and do not treat those runs as performance measurements:

```bash
mkdir -p /tmp/staged-sort-results
export STAGED_SORT_RESULTS_DIR=/tmp/staged-sort-results
# Baseline branch (or staged sort disabled), from benchmarks/:
DATAFUSION_EXECUTION_ENABLE_STAGED_SORT=false cargo run --release --bin benchmark_runner -- staged_sort --rows 1000 --result-mode persist
# Candidate branch, same fixture and results directory:
DATAFUSION_EXECUTION_ENABLE_STAGED_SORT=true cargo run --release --bin benchmark_runner -- staged_sort --rows 1000 --result-mode validate
```

For a main baseline omit the environment variable on its persist command.
`--result-mode validate` compares rows in order. Persist again into a fresh
location whenever fixture options change. The plan assertion checks the
computed key on the `SortExec` line, not just the presence of a sort. Inspect
`--debug`/`EXPLAIN` after planner changes: in particular verify that both
expensive expressions in 01 and 04 remain in the sort, that 07 is TopK, and
that 08 exercises the intended run/merge path. Plan shape alone does not prove
which stages execute; compare results and metrics as well.
