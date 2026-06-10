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
| max_output_batch_size | Size (in bytes) of the largest single batch the operator produces. Useful for spotting partition skew or an unusually large batch that caused an error or spill. Measured the same way as `output_bytes`, so it carries the same caveat: it may be overestimated when arrays share underlying buffers (counted multiple times). |
| output_batches  | Total number of output batches the operator produces.                                                                                                                                              |

### SpillMetrics

`SpillMetrics` are exposed by operators that may spill batches to disk (e.g. sorts, joins, and grouped aggregates) when memory is constrained.

| Metric                | Description                                                                                                                                                  |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| spill_count           | Number of spill files written during execution.                                                                                                              |
| spilled_bytes         | Total number of bytes written to disk across all spill files.                                                                                                |
| spilled_rows          | Total number of rows written to disk across all spills.                                                                                                       |
| max_spilled_batch_size | In-memory size (in bytes) of the largest single batch spilled, measured via `get_sliced_size` on the GC-compacted batch that is written (the same value used for spill memory accounting). This is the in-memory size, not the bytes written to disk (see `spilled_bytes`). Useful for spotting an unusually large batch that triggered a spill. |

### SplitMetrics

`SplitMetrics` are exposed by operators that split oversized input batches into smaller ones (e.g. when enforcing the configured batch size).

| Metric                | Description                                                                                                                                                  |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| batches_split         | Number of times an input batch was split.                                                                                                                    |
| max_sliced_batch_size | Estimated size (in bytes) of the largest slice produced when splitting a batch. Measured via `get_sliced_size` like `max_spilled_batch_size`, but on a slice that is *not* GC-compacted first, so buffers shared across columns may be counted multiple times — hence an estimate. |

## Operator-specific Metrics

### FilterExec

| Metric      | Description                                                       |
| ----------- | ----------------------------------------------------------------- |
| selectivity | Selectivity of the filter, calculated as output_rows / input_rows |

## TODO

Add metrics for the remaining operators
