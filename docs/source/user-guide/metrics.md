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

`AggregateExec` exposes the common `BaselineMetrics`, the operator-level
metrics below, and a timer per aggregate expression and execution phase.

| Metric                     | Description                                                                                                                           |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| time_calculating_group_ids | Time spent preparing group keys; see the note below for path-specific coverage.                                                       |
| aggregate_arguments_time   | Total time spent evaluating the inputs to the aggregate functions. `agg_expr_{index}_arguments_time` breaks this down per expression. |
| aggregation_time           | Time spent feeding the evaluated inputs into the accumulators.                                                                        |
| emitting_time              | Time spent producing output batches, including finalizing the grouping expressions and the accumulators.                              |
| skipped_aggregation_rows   | Number of input rows passed through without aggregating them, when partial aggregation is skipped.                                    |
| reduction_factor           | Rows emitted per row consumed by a partial aggregation, displayed as `66.67% (2/3)`.                                                  |
| spill_count                | Number of spill files written when the aggregation exceeds its memory budget.                                                         |
| spilled_bytes              | Total number of bytes written to spill files.                                                                                         |
| spilled_rows               | Total number of rows written to spill files.                                                                                          |
| peak_mem_used              | Peak tracked memory held by the grouped aggregation, in bytes; recorded by the fallback grouped hash path only.                       |

These operator-level metrics are recorded by the grouped aggregation paths
only: an `AggregateExec` without a `GROUP BY` reports just `BaselineMetrics`
and the per-aggregate timers. `reduction_factor` and `skipped_aggregation_rows`
are recorded in partial mode only. `skipped_aggregation_rows` is recorded only
when partial-aggregation skipping is enabled for a single, non-grouping-sets
`GROUP BY`; a `datafusion.execution.skip_partial_aggregation_probe_ratio_threshold`
of `>= 1.0` disables the feature.

`time_calculating_group_ids` and `aggregation_time` do not cover the same work
in every grouped implementation, so their values are comparable only within one
implementation. Three of the paths leave part of the group-key work untimed, so
on those paths the individual timers do not add up to `elapsed_compute`.

| Implementation                                                                                                        | `time_calculating_group_ids` covers                                                                          | `aggregation_time` covers                                                                                                                         |
| --------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| Hash aggregation over unordered input                                                                                 | Evaluating the grouping expressions                                                                          | Interning group values, then the accumulator calls                                                                                                |
| Hash aggregation over ordered input (`ordering_mode=Sorted` or `ordering_mode=PartiallySorted(...)` on the plan line) | Evaluating the grouping expressions                                                                          | The accumulator calls only; interning group values is not covered by any metric                                                                   |
| Legacy grouped hash path (grouping sets and other cases not yet migrated)                                             | Interning group values                                                                                       | The accumulator calls only; skipped partial aggregation records `convert_to_state` instead; value is inflated with multiple aggregate expressions |
| Grouped TopK (`GROUP BY` with a `LIMIT`)                                                                              | Priority-map batch setup and insertion, including null handling; grouping-expression evaluation is not timed | Not recorded, because the path keeps values in a priority map instead of calling accumulators                                                     |

The per-aggregate timers are named `agg_expr_{index}_{phase}_time`, where
`index` is the zero-based position of an aggregate expression in the operator
and `phase` is one of the following:

| Phase              | Description                                                                                   |
| ------------------ | --------------------------------------------------------------------------------------------- |
| `arguments`        | Evaluating the aggregate's argument expressions into input arrays.                            |
| `update`           | Updating an accumulator from raw input values.                                                |
| `merge`            | Merging partial accumulator states.                                                           |
| `state`            | Obtaining an accumulator's intermediate state for partial output or aggregate spilling.       |
| `convert_to_state` | Converting raw aggregate inputs directly to partial state without normal accumulator updates. |
| `evaluate`         | Evaluating an accumulator to its final result.                                                |

For example, when a partial stage is planned, the partial `AggregateExec` for
`SELECT SUM(a), SUM(b) FROM t` reports `agg_expr_0_arguments_time` and
`agg_expr_0_update_time` for `SUM(a)`,
and `agg_expr_1_arguments_time` and `agg_expr_1_update_time` for `SUM(b)`. The
index is positional and refers to the same position in the `aggr=[...]` list
printed on the operator's plan line, which is how an indexed timer is mapped
back to an aggregate expression. Because the index is part of the metric name,
otherwise identical functions over different columns stay distinct when
per-partition metrics are combined.

Each per-aggregate timer additionally carries an `aggregate` label holding the
rendered aggregate expression (for example, `sum(t.a)`). Combining metrics
across partitions drops labels, so this label is only shown in the "Plan with
Full Metrics" section of `EXPLAIN ANALYZE VERBOSE`, which reports metrics per
partition.

`arguments` is recorded in every mode. The accumulator phases that are present
depend on the aggregate mode and implementation. For non-grouped aggregation,
partial mode records `update` and `state`, partial reduce mode records `merge`
and `state`, final mode records `merge` and `evaluate`, and single mode records
`update` and `evaluate`. Hash aggregation uses `update`, `state`, and
`convert_to_state` in partial mode; `merge` and `state` in partial-reduce mode;
`merge`, `state`, and `evaluate` in final mode; and `update`, `state`, `merge`,
and `evaluate` in single mode. Its `state` timers measure intermediate-state
emission, including during spilling. The grouped TopK aggregate path records
only the per-aggregate `arguments` timer, because it maintains values directly
rather than using accumulators.

Where an aggregate has a `FILTER` clause, non-grouped aggregation and the
hash-table grouped paths evaluate that filter inside the aggregate's
`arguments` timer. The legacy grouped hash path evaluates filters outside the
per-aggregate timers, so its `arguments` timers cover argument evaluation only.

Except for the `Summary` metric `reduction_factor`, these operator-level and
per-aggregate metrics are `Dev` metrics. They appear in `EXPLAIN ANALYZE` when
`datafusion.explain.analyze_level` includes `Dev` (the default), but are omitted
at the `Summary` level. The normal display combines partitions; use `EXPLAIN ANALYZE VERBOSE` to additionally show the per-partition values together with
each per-aggregate timer's `aggregate` label. For a query
such as the following, the per-expression metrics stay readable, and the
operator's `aggr=[...]` list names the aggregate behind each timer index:

```sql
EXPLAIN ANALYZE
SELECT k, SUM(a), SUM(b), COUNT(c)
FROM t
GROUP BY k;
```

## TODO

Add metrics for the remaining operators
