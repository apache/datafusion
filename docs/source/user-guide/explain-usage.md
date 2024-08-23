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

# Reading Explain Plans

# Introduction

A query in Datafusion is executed based on a `query plan`. To see the plan without running the query, add
the keyword `EXPLAIN`

```sql
EXPLAIN SELECT "hits.parquet"."WatchID" AS wid, "hits.parquet"."ClientIP" AS ip FROM 'hits.parquet' ORDER BY wid ASC, ip DESC LIMIT 5;
```

The output will look like

```sql
--------------------------------------------------------------------------------------------------------------+
| plan_type     | plan                                                                                        |
+---------------+---------------------------------------------------------------------------------------------+
| logical_plan  | Limit: skip=0, fetch=5                                                                      |
|               |   Sort: wid ASC NULLS LAST, fetch=5                                                         |
|               |     Projection: hits.parquet.WatchID AS wid, hits.parquet.ClientIP AS ip                    |
|               |       TableScan: hits.parquet projection=[WatchID, ClientIP]                                |
| physical_plan | GlobalLimitExec: skip=0, fetch=5                                                            |
|               |   SortPreservingMergeExec: [wid@0 ASC NULLS LAST, ip@1 DESC], fetch=5                       |
|               |.   UnionExec                                                                                | 
|               |       SortExec: expr=[wid@0 ASC NULLS LAST,ip@1 DESC], preserve_partitioning=[true]         |                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|               |         ProjectionExec: expr=[WatchID@0 as wid, ClientIP@2 as ip, EventDate@1 as EventDate] |
|               |           ParquetExec: file_groups={...}, projection=[WatchID, ClientIP]                    |
|               |       SortExec: expr=[wid@0 ASC NULLS LAST,ip@1 DESC], preserve_partitioning=[true]         |                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|               |         ProjectionExec: expr=[WatchID@0 as wid, ClientIP@2 as ip, EventDate@1 as EventDate] |
|               |           ParquetExec: file_groups={...}, projection=[WatchID, ClientIP]                    |
|               |                                                                                             |
+---------------+---------------------------------------------------------------------------------------------+
```

Figure 1: A simplified output of an explain

There are two major plans: logical plan and physical plan

* **Logical Plan:** is a plan generated for a specific SQL dialect without the knowledge of the underlying data
  organization and the cluster configuration.
* **Physical Plan:** is a plan generated from its corresponding logical plan plus the consideration of the hardware
  configuration (e.g number of CPUs) and the underlying data organization (e.g number of files, if the files overlap or
  not, etc). This physical plan is very specific to your hardware configuration and your data. If you load the same
  data to different hardware with different configurations, the same query may generate different query plans.
  Similarly, the same query on the same hardware at different time can have different plans depending on your data at
  that time.

Understanding a query plan can help to explain why your query is slow. For example, when the plan shows your query reads
many files, it signals you to either add more filter in the query to read less data or to modify your cluster
configuration/design to make fewer but larger files. This document focuses on how to read a query plan. How to make a
query run faster depends on the reason it is slow and beyond the scope of this document.

# How to read a query plan

## Query plan is a tree

A query plan is an upside down tree, and we always read from bottom up. The physical plan in Figure 1 in tree format
will
look like

```
                   ┌─────────────────────────┐                  
                   │     GlobalLimitExec     │                  
                   └─────────────────────────┘                  
                                ▲                               
                                │      
                   ┌─────────────────────────┐                  
                   │ SortPreservingMergeExec │                  
                   └─────────────────────────┘                  
                                ▲                               
                                │                               
                   ┌─────────────────────────┐                  
                   │        UnionExec        │                  
                   └─────────────────────────┘                  
                                ▲                               
             ┌──────────────────┴─────────────────┐             
             │                                    │             
┌─────────────────────────┐          ┌─────────────────────────┐
│        SortExec         │          │        SortExec         │
└─────────────────────────┘          └─────────────────────────┘
             ▲                                    ▲             
             │                                    │             
             │                                    │             
┌─────────────────────────┐          ┌─────────────────────────┐
│       ParquetExec       │          │       ParquetExec       │
└─────────────────────────┘          └─────────────────────────┘
```

Figure 2: The tree structure of physical plan in Figure 1

Each node in the tree/plan ends with `Exec` and is sometimes also called an `operator` or `ExecutionPlan` where data is
processed, transformed and sent up.

First, data in parquet files are read in parallel through the two `ParquetExec`, which each outputs a stream of data to
its corresponding `SortExec`. The `SortExc` is responsible for sorting the data in `WatchID` ascendingly and `ClientIP`
descendingly. The sorted outputs from the two `SortExec` are then unioned by the `UnionExec` which is then (sort) merged
by the `SortPreservingMergeExec` to return the sorted data. Finally, it is limited by 5 with `GlobalLimitExec`.

## How to understand a large query plan

A large query plan may look intimidating but if you follow these steps, you can quickly understand what the plan does.

1. As always, read from bottom up, one operator at a time.
2. Understand the job of this operator which mostly can be found
   from [DataFusion Physical Plan documentation](https://docs.rs/datafusion/latest/datafusion/physical_plan/index.html).
3. What the input data of the operator looks like and how large/small it may be.
4. How much data that operator may send out and what it would look like.

If you can answer those questions, you will be able to estimate how much work that plan has to do. However, the
`explain` just shows you the plan without executing it. If you want to know exactly how long a plan and each of its
operators take, you can run `explain analyze` to get the explain with runtime added.

##### More information for debugging

If the plan has to read too many files, not all of them will be shown in the `explain`. To see them, use
`explain verbose`. Like `explain`, `explain verbose` does not run the query and thus you won't get runtime. What you get
is all information that is cut off from the explain and all intermediate physical plans DataFusion
generates before returning the final physical plan. This is very helpful for debugging to see when an operator is added
to or removed from a plan.

## Example of typical plan of leading edge data

Let us delve into an example that covers typical operators on leading edge data.

##### Query and query plan

```sql
EXPLAIN SELECT "hits.parquet"."OS" AS os, COUNT(1) FROM 'hits.parquet' WHERE to_timestamp("hits.parquet"."EventTime") >= to_timestamp(200) AND to_timestamp("hits.parquet"."EventTime") < to_timestamp(700) AND "hits.parquet"."RegionID" = 839 GROUP BY os ORDER BY os ASC;
```

```sql
+---------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| plan_type     | plan                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
+---------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| logical_plan  | Sort: os ASC NULLS LAST                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
|               |   Projection: hits.parquet.OS AS os, count(Int64(1))                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
|               |     Aggregate: groupBy=[[hits.parquet.OS]], aggr=[[count(Int64(1))]]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
|               |       Projection: hits.parquet.OS                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|               |         Filter: __common_expr_4 >= TimestampNanosecond(200000000000, None) AND __common_expr_4 < TimestampNanosecond(700000000000, None) AND hits.parquet.RegionID = Int32(839)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|               |           Projection: to_timestamp(hits.parquet.EventTime) AS __common_expr_4, hits.parquet.RegionID, hits.parquet.OS                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
|               |             TableScan: hits.parquet projection=[EventTime, RegionID, OS], partial_filters=[to_timestamp(hits.parquet.EventTime) >= TimestampNanosecond(200000000000, None), to_timestamp(hits.parquet.EventTime) < TimestampNanosecond(700000000000, None), hits.parquet.RegionID = Int32(839)]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| physical_plan | SortPreservingMergeExec: [os@0 ASC NULLS LAST]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
|               |   SortExec: expr=[os@0 ASC NULLS LAST], preserve_partitioning=[true]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
|               |     ProjectionExec: expr=[OS@0 as os, count(Int64(1))@1 as count(Int64(1))]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
|               |       AggregateExec: mode=FinalPartitioned, gby=[OS@0 as OS], aggr=[count(Int64(1))]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
|               |         CoalesceBatchesExec: target_batch_size=8192                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
|               |           RepartitionExec: partitioning=Hash([OS@0], 10), input_partitions=10                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
|               |             AggregateExec: mode=Partial, gby=[OS@0 as OS], aggr=[count(Int64(1))]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|               |               ProjectionExec: expr=[OS@2 as OS]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|               |                 CoalesceBatchesExec: target_batch_size=8192                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
|               |                   FilterExec: __common_expr_4@0 >= 200000000000 AND __common_expr_4@0 < 700000000000 AND RegionID@1 = 839                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
|               |                     ProjectionExec: expr=[to_timestamp(EventTime@0) as __common_expr_4, RegionID@1 as RegionID, OS@2 as OS]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
|               |                       ParquetExec: file_groups={10 groups: [[/data/hits.parquet:0..1477997645], [/data/hits.parquet:1477997645..2955995290], [/data/hits.parquet:2955995290..4433992935], [/data/hits.parquet:4433992935..5911990580], [/data/hits.parquet:5911990580..7389988225], ...]}, projection=[EventTime, RegionID, OS], predicate=to_timestamp(EventTime@4) >= 200000000000 AND to_timestamp(EventTime@4) < 700000000000 AND RegionID@8 = 839, pruning_predicate=CASE WHEN RegionID_null_count@2 = RegionID_row_count@3 THEN false ELSE RegionID_min@0 <= 839 AND 839 <= RegionID_max@1 END, required_guarantees=[RegionID in (839)] |
|               |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
+---------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

Figure 3: A typical query plan of leading edge (most recent) data

Let us begin reading bottom up. The bottom or leaf nodes are always an execution plan that reads a file format 
such as `ParquetExec`, `CsvExec`, `ArrowExec`, or `AvroExec`. There is a `ParquetExec` leaf in this plan. Let's go over it.

### Bottom leaf operator: `ParquetExec`

**ParqetExec**

```sql
|               |                       ParquetExec: file_groups={10 groups: [[/data/hits.parquet:0..1477997645], [/data/hits.parquet:1477997645..2955995290], [/data/hits.parquet:2955995290..4433992935], [/data/hits.parquet:4433992935..5911990580], [/data/hits.parquet:5911990580..7389988225], ...]}, projection=[EventTime, RegionID, OS], predicate=to_timestamp(EventTime@4) >= 200000000000 AND to_timestamp(EventTime@4) < 700000000000 AND RegionID@8 = 839, pruning_predicate=CASE WHEN RegionID_null_count@2 = RegionID_row_count@3 THEN false ELSE RegionID_min@0 <= 839 AND 839 <= RegionID_max@1 END, required_guarantees=[RegionID in (839)] |
```

Figure 4: `ParquetExec` leaf node

* This `ParquetExec` includes 10 groups. Each group can contain one or many files but, in this example, there is
  one file for many groups. Files in each group are read sequentially but groups will be executed in parallel. So for
  this
  example, 10 groups will be read in parallel.
* `/data/hits.parquet`: this is the path of the file.
* `projection=[EventTime, RegionID, OS]`: there are many columns in this table but only these 4 columns are read.
* `predicate=to_timestamp(EventTime@4) >= 200000000000 AND to_timestamp(EventTime@4) < 700000000000 AND RegionID@8 = 839`: filter in the query used for data pruning,
* `pruning_predicate=CASE WHEN RegionID_null_count@2 = RegionID_row_count@3 THEN false ELSE RegionID_min@0 <= 839 AND 839 <= RegionID_max@1 END`: the actual pruning predicate transformed from the predicate above. This is used to filter files outside that predicate.
* `required_guarantees=[RegionID in (839)]`: TODO, need definition.

### Other operators in physical plan

Now let us look at the rest of the plan

```sql
| physical_plan | SortPreservingMergeExec: [os@0 ASC NULLS LAST]                                                                               |
|               |   SortExec: expr=[os@0 ASC NULLS LAST], preserve_partitioning=[true]                                                         |
|               |     ProjectionExec: expr=[OS@0 as os, count(Int64(1))@1 as count(Int64(1))]                                                  |
|               |       AggregateExec: mode=FinalPartitioned, gby=[OS@0 as OS], aggr=[count(Int64(1))]                                         |
|               |         CoalesceBatchesExec: target_batch_size=8192                                                                          |
|               |           RepartitionExec: partitioning=Hash([OS@0], 10), input_partitions=10                                                |
|               |             AggregateExec: mode=Partial, gby=[OS@0 as OS], aggr=[count(Int64(1))]                                            |
|               |               ProjectionExec: expr=[OS@2 as OS]                                                                              |
|               |                 CoalesceBatchesExec: target_batch_size=8192                                                                  |
|               |                   FilterExec: __common_expr_4@0 >= 200000000000 AND __common_expr_4@0 < 700000000000 AND RegionID@1 = 839    |
|               |                     ProjectionExec: expr=[to_timestamp(EventTime@0) as __common_expr_4, RegionID@1 as RegionID, OS@2 as OS]  |      
```

Figure 9: The rest of the plan structure

#### A note on exchange-based parallelism

It may appear that every execution node has one input and one output but that is not the case. Given `figure 10` below we can see that
there are two nodes with `AggregateExec` that are seemingly running an aggregate on the same data. 

```sql
|               |       AggregateExec: mode=FinalPartitioned, gby=[OS@0 as OS], aggr=[count(Int64(1))]  |                                      
|               |         CoalesceBatchesExec: target_batch_size=8192                                   |
|               |           RepartitionExec: partitioning=Hash([OS@0], 10), input_partitions=10         |
|               |             AggregateExec: mode=Partial, gby=[OS@0 as OS], aggr=[count(Int64(1))]     |  
```
Figure 10: Multi-staged parallel aggregation

**First `AggregateExec`**

The first `AggregateExect` with `mode=Partial` is a partial aggregate that is applied in parallel across all input partitions. 
The partitions in this case are the file group fragments from `ParquetExec`.

**Second `AggregateExec`**

The second `AggregateExec` with `mode=FinalPartitioned` is the final aggregate working on the pre-partitioned data.


#### Explaining the plan

* `ProjectionExec: expr=[to_timestamp(EventTime@0) as __common_expr_4, RegionID@1 as RegionID, OS@2 as OS]`: this will filter the column
data to only send data of column's `EventTime` aliased as `__common_expr_4` converted to a timestamp, `RegionID` and `OS` 
* `FilterExec: __common_expr_4@0 >= 200000000000 AND __common_expr_4@0 < 700000000000 AND RegionID@1 = 839`: places a filter on the data
that meets the conditions. The pruning that happens before is not guaranteed and thus we need to use this filter to complete the filtering.
* `CoalesceBatchesExec: target_batch_size=8192`: groups smaller data in to larger groups
* `ProjectionExec: expr=[OS@2 as OS]`: TODO: explain why there are multiple projection steps
* `AggregateExec: mode=Partial, gby=[OS@0 as OS], aggr=[count(Int64(1))]`: partial aggregate working in parallel across partitions of data.
This group of data is specified as `"hits.parquet".OS AS OS, COUNT(1)`.
* `RepartitionExec: partitioning=Hash([OS@0], 10), input_partitions=10`: this takes 10 input streams and partitions them based on the `Hash`
partition variant using `OS` as the hash. See: https://docs.rs/datafusion/latest/datafusion/physical_plan/enum.Partitioning.html for other variants and more information
* `CoalesceBatchesExec: target_batch_size=8192`: TODO: explain why there are multiple coalesce batches steps
* `AggregateExec: mode=FinalPartitioned, gby=[OS@0 as OS], aggr=[count(Int64(1))]`: we do a final aggregate on the data
* `ProjectionExec: expr=[OS@0 as os, count(Int64(1))@1 as count(Int64(1))]`: filter column data and only send out `os` and `count(1)`
* `SortExec: expr=[os@0 ASC NULLS LAST], preserve_partitioning=[true]`: sorts the 10 streams of data each on the `os` column ascendingly
* `SortPreservingMergeExec: [os@0 ASC NULLS LAST]`: sort and merge the 10 streams of data for final results

### Logical plan operators

```sql
| logical_plan  | Sort: os ASC NULLS LAST                                                                                                                                                                                                                                                                         |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
|               |   Projection: hits.parquet.OS AS os, count(Int64(1))                                                                                                                                                                                                                                            |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
|               |     Aggregate: groupBy=[[hits.parquet.OS]], aggr=[[count(Int64(1))]]                                                                                                                                                                                                                            |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
|               |       Projection: hits.parquet.OS                                                                                                                                                                                                                                                               |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
|               |         Filter: __common_expr_4 >= TimestampNanosecond(200000000000, None) AND __common_expr_4 < TimestampNanosecond(700000000000, None) AND hits.parquet.RegionID = Int32(839)                                                                                                                 |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
|               |           Projection: to_timestamp(hits.parquet.EventTime) AS __common_expr_4, hits.parquet.RegionID, hits.parquet.OS                                                                                                                                                                           |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
|               |             TableScan: hits.parquet projection=[EventTime, RegionID, OS], partial_filters=[to_timestamp(hits.parquet.EventTime) >= TimestampNanosecond(200000000000, None), to_timestamp(hits.parquet.EventTime) < TimestampNanosecond(700000000000, None), hits.parquet.RegionID = Int32(839)] | 
```
Figure 11: Logical plan portion of query plan

