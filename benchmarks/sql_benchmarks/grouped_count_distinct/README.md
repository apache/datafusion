<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# Grouped COUNT(DISTINCT)

Compare the default rewrite with the native accumulator opt-in at one target
partition. The suite reads DataFusion settings from the environment; it does not
enable the opt-in itself.

```bash
DATAFUSION_OPTIMIZER_SINGLE_DISTINCT_PREFER_NATIVE=false cargo run --release --bin benchmark_runner -- grouped_count_distinct --iterations 10 --partitions 1 --mem-pool-type greedy --memory-limit 1G
DATAFUSION_OPTIMIZER_SINGLE_DISTINCT_PREFER_NATIVE=true cargo run --release --bin benchmark_runner -- grouped_count_distinct --iterations 10 --partitions 1 --mem-pool-type greedy --memory-limit 1G
```

Run from `benchmarks/`. Repeat at 4 and 12 target partitions to check the
distributed rewrite, which remains selected with either flag value. The same
environment variable works with `./bench.sh run grouped_count_distinct`.

Q01-Q09 vary argument and grouping-key types, group count, distinct cardinality,
and skew. Q10 covers the unfavorable native shape: at the default four million
rows it has one million groups, each with four copies of the same value. Each
native input row probes both the group table and the per-group distinct set;
the rewrite's outer aggregate only sees the deduplicated pairs.

Every query checks its result against an explicit distinct-pairs-then-count
query before timing. Keep the memory pool and limit the same on both sides.
The combined correctness query needs more memory than the measured aggregate.
