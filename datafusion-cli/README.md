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

<!-- Note this file is included in the crates.io page as well https://crates.io/crates/datafusion-cli -->

# DataFusion Command-line Interface

[DataFusion](https://datafusion.apache.org/) is an extensible query execution framework, written in Rust, that uses Apache Arrow as its in-memory format.

DataFusion CLI (`datafusion-cli`) is a small command line utility that runs SQL queries using the DataFusion engine.

# Frequently Asked Questions

## Where can I find more information?

See the [`datafusion-cli` documentation](https://datafusion.apache.org/user-guide/cli/index.html) for further information.

## Memory Profiling

> **Tip:** Memory profiling requires the tracked pool. Start the CLI with `--top-memory-consumers N` (N≥1), or profiling will report no metrics. By default, CLI starts with --top-memory-consumers 5.

Enable memory tracking for the next query and display the report afterwards:

```text
> \memory_profiling enable
Memory profiling enabled
> SELECT v % 100 AS group_key, COUNT(*) AS cnt, SUM(v) AS sum_v FROM generate_series(1,100000) AS t(v) GROUP BY group_key ORDER BY group_key;

+-----------+------+----------+
| group_key | cnt  | sum_v    |
+-----------+------+----------+
| 0         | 1000 | 50050000 |
| 1         | 1000 | 49951000 |
| 2         | 1000 | 49952000 |
...

\memory_profiling show
Peak memory usage: 10.0 MB
Cumulative allocations: 101.6 MB
Memory usage by operator:
Aggregation: 762.2 KB
Other: 887.1 KB
Sorting: 100.0 MB

\memory_profiling disable   # optional
```
