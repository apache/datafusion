<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Set-operation benchmarks

Runs through the shared SQL Criterion harness, including SQL planning,
optimization, and execution. Input generation and cardinality checks run
outside the timed region. Queries consume the full multiset through
`count(*)`, avoiding large result retention in the driver.

| Queries   | Subgroup   | Inputs                                                | Expected output rows                                  |
| --------- | ---------- | ----------------------------------------------------- | ----------------------------------------------------- |
| Q01 / Q02 | distinct   | N unique rows per side, overlapping by N - floor(N/2) | INTERSECT ALL: N - floor(N/2); EXCEPT ALL: floor(N/2) |
| Q03 / Q04 | duplicates | N left rows, floor(N/2) right rows, keys modulo 1,000 | INTERSECT ALL: floor(N/2); EXCEPT ALL: N - floor(N/2) |
| Q05 / Q06 | skew       | Same sizes as duplicates, one key                     | Same as duplicates                                    |
| Q07 / Q08 | control    | Same unique inputs as Q01 / Q02, DISTINCT operators   | Same as Q01 / Q02                                     |

Both the integer key and its deterministic string payload participate in each
operation. The skew cases stress a large per-key expansion; EXCEPT ALL uses
unequal counts so expansion is not skipped. `--rows` / `SET_ROWS` must be a
positive integer.

From the repository root:

```sh
# Quick correctness smoke run, not a performance measurement.
cargo run --release --bin benchmark_runner -- set_operations --rows 1001 --iterations 1 --partitions 4

# Criterion measurements, fixed data size and parallelism.
cargo run --release --bin benchmark_runner -- set_operations --criterion --rows 1000000 --partitions 4

# Large single-key expansion; memory-pool peaks are not total process RSS.
cargo run --release --bin benchmark_runner -- set_operations --criterion --subgroup skew --rows 10000000 --partitions 4 --memory-limit 2G

# Separate basic run for machine-readable memory-pool peaks.
cargo run --release --bin benchmark_runner -- set_operations --subgroup skew --rows 10000000 --partitions 4 --memory-limit 2G --iterations 1 --output /tmp/set-operations-skew.json
```

The suite also supports `cd benchmarks && ./bench.sh run set_operations`
through the existing Criterion SQL target; no separate data generation step
is needed.

Compare the same suite, build profile, row count, and partition count across
branches. For the grouped-count optimization, use corrected-window commit
`84ef8f7` as the baseline, not the older semi/anti lowering with incorrect
ALL multiplicities. The cardinality checks deliberately reject that older
implementation. Copy the suite into a baseline checkout if it predates these
benchmark files.
