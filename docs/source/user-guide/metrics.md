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

# Metrics

DataFusion operators expose runtime metrics so you can understand where time is spent and how much data flows through the pipeline. See more in [EXPLAIN ANALYZE](sql/explain.md#explain-analyze).

## Common Metrics

### BaselineMetrics

`BaselineMetrics` are available in most physical operators to capture common measurements.

| Metric          | Description                                                                                                                                                                                        |
| --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| elapsed_compute | CPU time the operator actively spends processing work.                                                                                                                                             |
| output_rows     | Total number of rows the operator produces.                                                                                                                                                        |
| output_bytes    | Memory usage of all output batches. Note: This value may be overestimated. If multiple output `RecordBatch` instances share underlying memory buffers, their sizes will be counted multiple times. |
| output_batches  | Total number of output batches the operator produces.                                                                                                                                              |

## Operator-specific Metrics

### FilterExec

| Metric      | Description                                                       |
| ----------- | ----------------------------------------------------------------- |
| selectivity | Selectivity of the filter, calculated as output_rows / input_rows |

### HashJoinExec

`HashJoinExec` also exposes the common `BaselineMetrics`. Its
`elapsed_compute` metric is the sum of the build-side collection time and the
subsequent join processing time.

| Metric                  | Description                                                                                               |
| ----------------------- | --------------------------------------------------------------------------------------------------------- |
| build_time              | Total time spent collecting and building the build side of the join.                                      |
| build_input_batches     | Number of input batches consumed from the build side.                                                     |
| build_input_rows        | Number of input rows consumed from the build side.                                                        |
| build_mem_used          | Peak tracked memory used by the build side, in bytes.                                                     |
| join_time               | Total time spent processing the join after build-side collection.                                         |
| input_batches           | Number of input batches consumed from the probe side.                                                     |
| input_rows              | Number of input rows consumed from the probe side.                                                        |
| probe_hit_rate          | Fraction of probe-side rows with a build-side join-key match before applying any join filter.             |
| avg_fanout              | Average number of build-side join-key matches per matched probe-side row before applying any join filter. |
| array_map_created_count | Number of times `HashJoinExec` created an `ArrayMap` for perfect hash join lookup execution.              |

### AggregateExec

`AggregateExec` records per-aggregate timers in addition to its common and
aggregate operator metrics. The timers are named
`agg_expr_{index}_{phase}_time`, where `index` is the zero-based position of
an aggregate expression in the operator and `phase` is one of the following:

| Phase              | Description                                                                                   |
| ------------------ | --------------------------------------------------------------------------------------------- |
| `arguments`        | Evaluating the aggregate's argument expressions and filters into input arrays.                |
| `update`           | Updating an accumulator from raw input values.                                                |
| `merge`            | Merging partial accumulator states.                                                           |
| `state`            | Obtaining an accumulator's intermediate state for partial output or aggregate spilling.       |
| `convert_to_state` | Converting raw aggregate inputs directly to partial state without normal accumulator updates. |
| `evaluate`         | Evaluating an accumulator to its final result.                                                |

For example, an operator for `SELECT SUM(a), SUM(b) FROM t` reports
`agg_expr_0_arguments_time` and `agg_expr_0_update_time` for `SUM(a)`, and
`agg_expr_1_arguments_time` and `agg_expr_1_update_time` for `SUM(b)`. Each
metric also has an `aggregate` label containing the aggregate expression (for
example, `SUM(a)`), so otherwise identical functions over different columns
remain distinct when metrics are combined across partitions.

The phases present depend on the aggregate mode and implementation. For
non-grouped aggregation, partial mode records `update` and `state`, partial
reduce mode records `merge` and `state`, final mode records `merge` and
`evaluate`, and single mode records `update` and `evaluate`. Hash aggregation
uses `update`, `state`, and `convert_to_state` in partial mode; `merge` and
`state` in partial-reduce mode; `merge`, `state`, and `evaluate` in final mode;
and `update`, `state`, `merge`, and `evaluate` in single mode. Its `state`
timers measure intermediate-state emission, including during spilling. The grouped TopK
aggregate path records only the per-aggregate `arguments` timer, because it
maintains values directly rather than using accumulators.

These detailed timers are additive observability metrics. They are `Dev`
metrics, so they appear in `EXPLAIN ANALYZE` when its analyze level includes
`Dev` (the default), but are omitted at the `Summary` level. Use `EXPLAIN ANALYZE VERBOSE` to show the per-partition values; the normal display combines
partitions. A query such as the following keeps the per-expression metrics
readable while showing which aggregate each indexed timer represents:

```sql
EXPLAIN ANALYZE
SELECT k, SUM(a), SUM(b), COUNT(c)
FROM t
GROUP BY k;
```

## TODO

Add metrics for the remaining operators
