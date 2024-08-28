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

# EXPLAIN

The `EXPLAIN` command shows the logical and physical execution plan for the specified SQL statement.

See the [Reading Explain Plans](../explain-usage.md) page for more information on how to interpret these plans.

<pre>
EXPLAIN [ANALYZE] [VERBOSE] statement
</pre>

## EXPLAIN

Shows the execution plan of a statement.
If you need more detailed output, use `EXPLAIN VERBOSE`.

```
EXPLAIN SELECT SUM(x) FROM table GROUP BY b;
+---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------+
| plan_type     | plan                                                                                                                                                           |
+---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------+
| logical_plan  | Projection: #SUM(table.x)                                                                                                                                        |
|               |   Aggregate: groupBy=[[#table.b]], aggr=[[SUM(#table.x)]]                                                                                                          |
|               |     TableScan: table projection=[x, b]                                                                                                                           |
| physical_plan | ProjectionExec: expr=[SUM(table.x)@1 as SUM(table.x)]                                                                                                              |
|               |   AggregateExec: mode=FinalPartitioned, gby=[b@0 as b], aggr=[SUM(table.x)]                                                                                      |
|               |     CoalesceBatchesExec: target_batch_size=4096                                                                                                                |
|               |       RepartitionExec: partitioning=Hash([Column { name: "b", index: 0 }], 16)                                                                                 |
|               |         AggregateExec: mode=Partial, gby=[b@1 as b], aggr=[SUM(table.x)]                                                                                         |
|               |           RepartitionExec: partitioning=RoundRobinBatch(16)                                                                                                    |
|               |             CsvExec: file_groups={1 group: [[/tmp/table.csv]]}, projection=[x, b], has_header=false                                            |
|               |                                                                                                                                                                |
+---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

## EXPLAIN ANALYZE

Shows the execution plan and metrics of a statement.
If you need more information output, use `EXPLAIN ANALYZE VERBOSE`.

```
EXPLAIN ANALYZE SELECT SUM(x) FROM table GROUP BY b;
+-------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------+
| plan_type         | plan                                                                                                                                                      |
+-------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------+
| Plan with Metrics | CoalescePartitionsExec, metrics=[]                                                                                                                        |
|                   |   ProjectionExec: expr=[SUM(table.x)@1 as SUM(x)], metrics=[]                                                                                             |
|                   |     HashAggregateExec: mode=FinalPartitioned, gby=[b@0 as b], aggr=[SUM(x)], metrics=[outputRows=2]                                                       |
|                   |       CoalesceBatchesExec: target_batch_size=4096, metrics=[]                                                                                             |
|                   |         RepartitionExec: partitioning=Hash([Column { name: "b", index: 0 }], 16), metrics=[sendTime=839560, fetchTime=122528525, repartitionTime=5327877] |
|                   |           HashAggregateExec: mode=Partial, gby=[b@1 as b], aggr=[SUM(x)], metrics=[outputRows=2]                                                          |
|                   |             RepartitionExec: partitioning=RoundRobinBatch(16), metrics=[fetchTime=5660489, repartitionTime=0, sendTime=8012]                              |
|                   |               CsvExec: file_groups={1 group: [[/tmp/table.csv]]}, has_header=false, metrics=[]                                                        |
+-------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------+
```
