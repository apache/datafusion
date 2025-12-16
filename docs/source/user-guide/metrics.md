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

### BuildProbeJoinMetrics

| Metric              | Description                                                          |
| ------------------- | -------------------------------------------------------------------- |
| build_time          | Total time for collecting build-side of join.                        |
| build_input_batches | Number of batches consumed by build-side.                            |
| build_input_rows    | Number of rows consumed by build-side.                               |
| build_mem_used      | Memory used by build-side in bytes.                                  |
| join_time           | Total time for joining probe-side batches to the build-side batches. |
| input_batches       | Number of batches consumed by probe-side of this operator.           |
| input_rows          | Number of rows consumed by probe-side this operator.                 |
| probe_hit_rate      | Fraction of probe rows that found more than one match.               |
| avg_fanout          | Average number of build matches per matched probe row.               |

## Operator-specific Metrics

### FilterExec

| Metric      | Description                                                        |
| ----------- | ------------------------------------------------------------------ |
| selectivity | Selectivity of the filter, calculated as output_rows / input_rows. |

### NestedLoopJoinExec

| Metric      | Description                                                      |
| ----------- | ---------------------------------------------------------------- |
| selectivity | Selectivity of the join: output_rows / (left_rows * right_rows). |

## TODO

Add metrics for the remaining operators
