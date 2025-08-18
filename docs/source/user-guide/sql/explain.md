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

## Syntax

<pre>
EXPLAIN [ANALYZE] [VERBOSE] [FORMAT format] statement
</pre>

## `EXPLAIN`

Shows the execution plan of a statement.
If you need more detailed output, use `EXPLAIN VERBOSE`.
Note that `EXPLAIN VERBOSE` only supports the `indent` format.

The optional `[FORMAT format]` clause controls how the plan is displayed as
explained below. If this clause is not specified, the plan is displayed using
the format from the [configuration value] `datafusion.explain.format`.

[configuration value]: ../configs.md

### `indent` format (default)

The `indent` format shows both the logical and physical plan, with one line for
each operator in the plan. Child plans are indented to show the hierarchy.

See [Reading Explain Plans](../explain-usage.md) for more information on how to interpret these plans.

```sql
> CREATE TABLE t(x int, b int) AS VALUES (1, 2), (2, 3);
0 row(s) fetched.
Elapsed 0.004 seconds.

> EXPLAIN SELECT SUM(x) FROM t GROUP BY b;
+---------------+-------------------------------------------------------------------------------+
| plan_type     | plan                                                                          |
+---------------+-------------------------------------------------------------------------------+
| logical_plan  | Projection: sum(t.x)                                                          |
|               |   Aggregate: groupBy=[[t.b]], aggr=[[sum(CAST(t.x AS Int64))]]                |
|               |     TableScan: t projection=[x, b]                                            |
| physical_plan | ProjectionExec: expr=[sum(t.x)@1 as sum(t.x)]                                 |
|               |   AggregateExec: mode=FinalPartitioned, gby=[b@0 as b], aggr=[sum(t.x)]       |
|               |     CoalesceBatchesExec: target_batch_size=8192                               |
|               |       RepartitionExec: partitioning=Hash([b@0], 16), input_partitions=16      |
|               |         RepartitionExec: partitioning=RoundRobinBatch(16), input_partitions=1 |
|               |           AggregateExec: mode=Partial, gby=[b@1 as b], aggr=[sum(t.x)]        |
|               |             DataSourceExec: partitions=1, partition_sizes=[1]                 |
|               |                                                                               |
+---------------+-------------------------------------------------------------------------------+
2 row(s) fetched.
Elapsed 0.004 seconds.
```

### `tree` format

The `tree` format is modeled after [DuckDB plans] and is designed to be easier
to see the high level structure of the plan

[duckdb plans]: https://duckdb.org/docs/stable/guides/meta/explain.html

```sql
> EXPLAIN FORMAT TREE SELECT SUM(x) FROM t GROUP BY b;
+---------------+-------------------------------+
| plan_type     | plan                          |
+---------------+-------------------------------+
| physical_plan | ┌───────────────────────────┐ |
|               | │       ProjectionExec      │ |
|               | │    --------------------   │ |
|               | │    sum(t.x): sum(t.x)@1   │ |
|               | └─────────────┬─────────────┘ |
|               | ┌─────────────┴─────────────┐ |
|               | │       AggregateExec       │ |
|               | │    --------------------   │ |
|               | │       aggr: sum(t.x)      │ |
|               | │     group_by: b@0 as b    │ |
|               | │                           │ |
|               | │           mode:           │ |
|               | │      FinalPartitioned     │ |
|               | └─────────────┬─────────────┘ |
|               | ┌─────────────┴─────────────┐ |
|               | │    CoalesceBatchesExec    │ |
|               | └─────────────┬─────────────┘ |
|               | ┌─────────────┴─────────────┐ |
|               | │      RepartitionExec      │ |
|               | │    --------------------   │ |
|               | │  output_partition_count:  │ |
|               | │             16            │ |
|               | │                           │ |
|               | │    partitioning_scheme:   │ |
|               | │      Hash([b@0], 16)      │ |
|               | └─────────────┬─────────────┘ |
|               | ┌─────────────┴─────────────┐ |
|               | │      RepartitionExec      │ |
|               | │    --------------------   │ |
|               | │  output_partition_count:  │ |
|               | │             1             │ |
|               | │                           │ |
|               | │    partitioning_scheme:   │ |
|               | │    RoundRobinBatch(16)    │ |
|               | └─────────────┬─────────────┘ |
|               | ┌─────────────┴─────────────┐ |
|               | │       AggregateExec       │ |
|               | │    --------------------   │ |
|               | │       aggr: sum(t.x)      │ |
|               | │     group_by: b@1 as b    │ |
|               | │       mode: Partial       │ |
|               | └─────────────┬─────────────┘ |
|               | ┌─────────────┴─────────────┐ |
|               | │       DataSourceExec      │ |
|               | │    --------------------   │ |
|               | │         bytes: 224        │ |
|               | │       format: memory      │ |
|               | │          rows: 1          │ |
|               | └───────────────────────────┘ |
|               |                               |
+---------------+-------------------------------+
1 row(s) fetched.
Elapsed 0.016 seconds.
```

### `pgjson` format

The `pgjson` format is modeled after [Postgres JSON] format.

You can use this format to visualize the plan in existing plan visualization
tools, such as [dalibo]

[postgres json]: https://www.postgresql.org/docs/current/sql-explain.html
[dalibo]: https://explain.dalibo.com/

```sql
> EXPLAIN FORMAT PGJSON SELECT SUM(x) FROM t GROUP BY b;
+--------------+----------------------------------------------------+
| plan_type    | plan                                               |
+--------------+----------------------------------------------------+
| logical_plan | [                                                  |
|              |   {                                                |
|              |     "Plan": {                                      |
|              |       "Expressions": [                             |
|              |         "sum(t.x)"                                 |
|              |       ],                                           |
|              |       "Node Type": "Projection",                   |
|              |       "Output": [                                  |
|              |         "sum(t.x)"                                 |
|              |       ],                                           |
|              |       "Plans": [                                   |
|              |         {                                          |
|              |           "Aggregates": "sum(CAST(t.x AS Int64))", |
|              |           "Group By": "t.b",                       |
|              |           "Node Type": "Aggregate",                |
|              |           "Output": [                              |
|              |             "b",                                   |
|              |             "sum(t.x)"                             |
|              |           ],                                       |
|              |           "Plans": [                               |
|              |             {                                      |
|              |               "Node Type": "TableScan",            |
|              |               "Output": [                          |
|              |                 "x",                               |
|              |                 "b"                                |
|              |               ],                                   |
|              |               "Plans": [],                         |
|              |               "Relation Name": "t"                 |
|              |             }                                      |
|              |           ]                                        |
|              |         }                                          |
|              |       ]                                            |
|              |     }                                              |
|              |   }                                                |
|              | ]                                                  |
+--------------+----------------------------------------------------+
1 row(s) fetched.
Elapsed 0.008 seconds.
```

### `graphviz` format

The `graphviz` format uses the [DOT language] that can be used with [Graphviz] to
generate a visual representation of the plan.

[dot language]: https://graphviz.org/doc/info/lang.html
[graphviz]: https://graphviz.org/

```sql
> EXPLAIN FORMAT GRAPHVIZ SELECT SUM(x) FROM t GROUP BY b;
+--------------+------------------------------------------------------------------------------------------------------------------------------+
| plan_type    | plan                                                                                                                         |
+--------------+------------------------------------------------------------------------------------------------------------------------------+
| logical_plan |                                                                                                                              |
|              | // Begin DataFusion GraphViz Plan,                                                                                           |
|              | // display it online here: https://dreampuf.github.io/GraphvizOnline                                                         |
|              |                                                                                                                              |
|              | digraph {                                                                                                                    |
|              |   subgraph cluster_1                                                                                                         |
|              |   {                                                                                                                          |
|              |     graph[label="LogicalPlan"]                                                                                               |
|              |     2[shape=box label="Projection: sum(t.x)"]                                                                                |
|              |     3[shape=box label="Aggregate: groupBy=[[t.b]], aggr=[[sum(CAST(t.x AS Int64))]]"]                                        |
|              |     2 -> 3 [arrowhead=none, arrowtail=normal, dir=back]                                                                      |
|              |     4[shape=box label="TableScan: t projection=[x, b]"]                                                                      |
|              |     3 -> 4 [arrowhead=none, arrowtail=normal, dir=back]                                                                      |
|              |   }                                                                                                                          |
|              |   subgraph cluster_5                                                                                                         |
|              |   {                                                                                                                          |
|              |     graph[label="Detailed LogicalPlan"]                                                                                      |
|              |     6[shape=box label="Projection: sum(t.x)\nSchema: [sum(t.x):Int64;N]"]                                                    |
|              |     7[shape=box label="Aggregate: groupBy=[[t.b]], aggr=[[sum(CAST(t.x AS Int64))]]\nSchema: [b:Int32;N, sum(t.x):Int64;N]"] |
|              |     6 -> 7 [arrowhead=none, arrowtail=normal, dir=back]                                                                      |
|              |     8[shape=box label="TableScan: t projection=[x, b]\nSchema: [x:Int32;N, b:Int32;N]"]                                      |
|              |     7 -> 8 [arrowhead=none, arrowtail=normal, dir=back]                                                                      |
|              |   }                                                                                                                          |
|              | }                                                                                                                            |
|              | // End DataFusion GraphViz Plan                                                                                              |
|              |                                                                                                                              |
+--------------+------------------------------------------------------------------------------------------------------------------------------+
1 row(s) fetched.
Elapsed 0.010 seconds.
```

## `EXPLAIN ANALYZE`

Shows the execution plan and metrics of a statement. If you need more
information output, use `EXPLAIN ANALYZE VERBOSE`. Note that `EXPLAIN ANALYZE`
only supports the `indent` format.

```sql
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
|                   |               DataSourceExec: file_groups={1 group: [[/tmp/table.csv]]}, has_header=false, metrics=[]                                                        |
+-------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------+
```
