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

### RepartitionExec

| Metric            | Level    | Description                                                                                                    |
| ----------------- | -------- | -------------------------------------------------------------------------------------------------------------- |
| fetch_time        | dev      | Time spent polling input partitions for batches.                                                               |
| repartition_time  | dev      | End-to-end time spent repartitioning input batches, excluding input fetch and output channel send time.        |
| send_time         | dev      | Time spent preparing and sending partitioned batches to each output partition channel.                         |
| hash_compute_time | internal | Hash repartition only: time spent evaluating partitioning expressions and computing row hashes.                |
| route_time        | internal | Hash repartition only: time spent assigning hashed row indices to output partitions.                           |
| batch_build_time  | internal | Hash repartition only: time spent materializing output batches from routed row indices.                        |
| channel_wait_time | internal | Time spent waiting for output channel capacity while sending partitioned batches.                              |
| spill_write_time  | internal | Time spent writing partitioned batches to spill storage when memory reservation for an output channel is full. |

## TODO

Add metrics for the remaining operators
