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

If the plan has to read too many files, not all of them will be shown in the explain. To see them, use
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

Let us begin reading bottom up. The bottom or leaf nodes are always either `ParquetExec` or `RecordBatchExec`.
There is a `ParquetExec` leaf in this plan. Let's go over it.

### Bottom leaf operator: `ParquetExec`

**First ParqetExec**

```sql
|               |                       ParquetExec: file_groups={10 groups: [[/data/hits.parquet:0..1477997645], [/data/hits.parquet:1477997645..2955995290], [/data/hits.parquet:2955995290..4433992935], [/data/hits.parquet:4433992935..5911990580], [/data/hits.parquet:5911990580..7389988225], ...]}, projection=[EventTime, RegionID, OS], predicate=to_timestamp(EventTime@4) >= 200000000000 AND to_timestamp(EventTime@4) < 700000000000 AND RegionID@8 = 839, pruning_predicate=CASE WHEN RegionID_null_count@2 = RegionID_row_count@3 THEN false ELSE RegionID_min@0 <= 839 AND 839 <= RegionID_max@1 END, required_guarantees=[RegionID in (839)] |
```

Figure 4: `ParquetExec` leaf node

* This `ParquetExec` includes 10 groups. Each group can contain one or many files but, in this example, there
  is
  one file for many groups. Files in each group are read sequentially but groups will be executed in parallel. So for
  this
  example, 10 groups will be read in parallel.
* `/data/hits.parquet`: this is the path of the file.
* `projection=[EventTime, RegionID, OS]`: there are many columns in this table but only these 4 columns are read.
*

`predicate=to_timestamp(EventTime@4) >= 200000000000 AND to_timestamp(EventTime@4) < 700000000000 AND RegionID@8 = 839`:
filter in the query used for data pruning,

*

`pruning_predicate=CASE WHEN RegionID_null_count@2 = RegionID_row_count@3 THEN false ELSE RegionID_min@0 <= 839 AND 839 <= RegionID_max@1 END`:
the actual
pruning predicate transformed from the predicate above. This is used to filter files outside that predicate.

* `required_guarantees=[RegionID in (839)]`: TODO, need definition.

### Data-scanning structures

So the question is why we split parquet files into different ParquetExec while they are in the same partition? There are
many reasons but two major ones are:

1. Split the non-overlaps from the overlaps so we only need to apply the expensive deduplication operation on the
   overlaps. This is the case of this example.
2. Split the non-overlaps to increase parallel execution

##### When we know there are ovelaps?

```sql
|               |                   DeduplicateExec: [state@2 ASC,city@1 ASC,time@3 ASC]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
|               |                     SortPreservingMergeExec: [state@2 ASC,city@1 ASC,time@3 ASC,__chunk_order@0 ASC]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
|               |                       UnionExec                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
|               |                         SortExec: expr=[state@2 ASC,city@1 ASC,time@3 ASC,__chunk_order@0 ASC]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|               |                           CoalesceBatchesExec: target_batch_size=8192                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|               |                             FilterExec: time@3 >= 200 AND time@3 < 700 AND state@2 = MA                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
|               |                               RecordBatchesExec: chunks=1, projection=[__chunk_order, city, state, time]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
|               |                         CoalesceBatchesExec: target_batch_size=8192                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
|               |                           FilterExec: time@3 >= 200 AND time@3 < 700 AND state@2 = MA                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|               |                             ParquetExec: file_groups={2 groups: [[1/1/b862a7e9b329ee6a418cde191198eaeb1512753f19b87a81def2ae6c3d0ed237/2cbb3992-4607-494d-82e4-66c480123189.parquet], [1/1/b862a7e9b329ee6a418cde191198eaeb1512753f19b87a81def2ae6c3d0ed237/9255eb7f-2b51-427b-9c9b-926199c85bdf.parquet]]}, projection=[__chunk_order, city, state, time], output_ordering=[state@2 ASC, city@1 ASC, time@3 ASC, __chunk_order@0 ASC], predicate=time@5 >= 200 AND time@5 < 700 AND state@4 = MA, pruning_predicate=time_max@0 >= 200 AND time_min@1 < 700 AND state_min@2 <= MA AND MA <= state_max@3 |
```

Figure 7: `DeduplicationExec` is a signal of overlapped data

This structure tells us that since there are `DeduplicationExec`, data underneath it overlaps. More specifically, data
in 2 files overlaps or/and overlap with the data from the Ingesters.

* `FilterExec: time@3 >= 200 AND time@3 < 700 AND state@2 = MA`: This is the place we filter out everything that meets
  the conditions `time@3 >= 200 AND time@3 < 700 AND state@2 = MA`. The pruning before just prune data when possible, it
  does not guarantee all of them are pruned. We need this filter to do the fully filtering job.
* `CoalesceBatchesExec: target_batch_size=8192` is just a way to group smaller data to larger groups if possible. Refer
  to DF documentation for how it works
* `SortExec: expr=[state@2 ASC,city@1 ASC,time@3 ASC,__chunk_order@0 ASC]` : this sorts data on
  `state ASC, city ASC, time ASC, __chunk_order ASC`. Note that this sort operator is only applied on data from
  ingesters because data from files is already sorted on that order.
* `UnionExec` is simply a place to pull many streams together. It does not merge anything.
* `SortPreservingMergeExec: [state@2 ASC,city@1 ASC,time@3 ASC,__chunk_order@0 ASC]` : this merges the already sorted
  data. When you see this, you know data below must be sorted. The output data is in one sorted stream.
* `DeduplicateExec: [state@2 ASC,city@1 ASC,time@3 ASC]` : this deduplicated sorted data coming from strictly one input
  stream. That is why you often see under `DeduplicateExec` is `SortPreservingMergeExec` but it is not a must. As long
  as the input to `DeduplicateExec` is one sorted stream of data, it will work correctly.

##### When we know there are no overlaps?

```sql
|                 ProjectionExec: expr=[city@0 as city]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|                   CoalesceBatchesExec: target_batch_size=8192                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
|                     FilterExec: time@2 >= 200 AND time@2 < 700 AND state@1 = MA                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
|                       ParquetExec: file_groups={2 groups: [[1/1/b862a7e9b329ee6a418cde191198eaeb1512753f19b87a81def2ae6c3d0ed237/243db601-f3f1-401b-afda-82160d8cc1a8.parquet], [1/1/b862a7e9b329ee6a418cde191198eaeb1512753f19b87a81def2ae6c3d0ed237/f5fb7c7d-16ac-49ba-a811-69578d05843f.parquet]]}, projection=[city, state, time], output_ordering=[state@1 ASC, city@0 ASC, time@2 ASC], predicate=time@5 >= 200 AND time@5 < 700 AND state@4 = MA, pruning_predicate=time_max@0 >= 200 AND time_min@1 < 700 AND state_min@2 <= MA AND MA <= state_max@3                                           |
```

Figure 8: No `DeduplicateExec` means files not overlap

Deduplicate is not in this structure and above it means the files here do not overlap

* `ProjectionExec: expr=[city@0 as city]` : this will filter column data and only send out data of column `city`

### Other operators

Now let us look at the rest of the plan

```sql
| physical_plan | SortPreservingMergeExec: [city@0 ASC NULLS LAST]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|               |   SortExec: expr=[city@0 ASC NULLS LAST]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
|               |     AggregateExec: mode=FinalPartitioned, gby=[city@0 as city], aggr=[COUNT(Int64(1))]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
|               |       CoalesceBatchesExec: target_batch_size=8192                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
|               |         RepartitionExec: partitioning=Hash([city@0], 4), input_partitions=4                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
|               |           AggregateExec: mode=Partial, gby=[city@0 as city], aggr=[COUNT(Int64(1))]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
|               |             RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|               |               UnionExec 
```

Figure 9: The rest of the plan structure

* `UnionExec`: union data streams. Note that the number of output streams is the same is the numner of input streams.
  The operator above it is responsible to do actual merge or split streams further. This UnionExec is just here as an
  intermediate steps of the merge/split.
* `RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3`: This split 3 input streams into 4 output
  streams in round-robin fashion. The reason to split is to increase the parallel execution.
* `AggregateExec: mode=Partial, gby=[city@0 as city], aggr=[COUNT(Int64(1))]`:  This group data as specified in
  `city, count(1)`. Becasue there are 4 input streams, each stream is aggregated separately and hence the output also 4
  streams which means the output data is not fully aggregated and the `mode=Partial` signals us that.
* `RepartitionExec: partitioning=Hash([city@0], 4), input_partitions=4` : this repartitions data on hash(`city`) into 4
  streams which means the same city will go into the same stream
* `AggregateExec: mode=FinalPartitioned, gby=[city@0 as city], aggr=[COUNT(Int64(1))]`: Since rows of same city are in
  the same stream, we only need to do the final aggregation.
* `SortExec: expr=[city@0 ASC NULLS LAST]` : Sort 4 streams of data each on `city` per the query request
* `SortPreservingMergeExec: [city@0 ASC NULLS LAST]`: (Sort) merge 4 sorted streams to return the final results

# For your questions and references

If you see the plan reads a lof of files and does deduplication on all of them you may want ask: "do all of them overlap
or not?" The asnwer is either yes or no depending on the situation. There are other reasons that we deduplicate
non-overlap files due to memory limitation, but they will be topics for future documentation.

Here is a subset of our internal content (thanks @NGA-TRAN who originally wrote this)

# Introduction

A SQL/InfluxQL query in InfluxDB 3.0 is executed based on a `query plan`. To see the plan without running the query, add
the keyword `EXPLAIN`

```sql
EXPLAIN SELECT city, min_temp, time FROM h2o ORDER BY city ASC, time DESC;
```

The output will look like

```sql
+---------------+--------------------------------------------------------------------------+
| plan_type     | plan                                                                     |
+---------------+--------------------------------------------------------------------------+
| logical_plan  | Sort: h2o.city ASC NULLS LAST, h2o.time DESC NULLS FIRST                 |
|               |   TableScan: h2o projection=[city, min_temp, time]                       |
| physical_plan | SortPreservingMergeExec: [city@0 ASC NULLS LAST,time@2 DESC]             |
|               |   UnionExec                                                              |
|               |     SortExec: expr=[city@0 ASC NULLS LAST,time@2 DESC]                   |
|               |       ParquetExec: file_groups={...}, projection=[city, min_temp, time]  |
|               |     SortExec: expr=[city@0 ASC NULLS LAST,time@2 DESC]                   |
|               |       ParquetExec: file_groups={...}, projection=[city, min_temp, time]  |
|               |                                                                          |
+---------------+--------------------------------------------------------------------------+
```

Figure 1: A simplified output of an explain

There are two major plans: logical plan and physical plan

* **Logical Plan:** is a plan generated for a specific SQL (or InfluxQL) without the knowledge of the underlying data
  organization and the cluster configuration. Because InfluxDB 3.0 is built on top
  of [DataFusion](https://github.com/apache/arrow-datafusion), this logical plan is the same as if you use DataFusion
  with any data format or storage.
* **Physical Plan:** is a plan generated from its corresponding logical plan plus the consideration of the cluster
  configurations (e.g number of CPUs) and the underlying data organization (e.g number of files, if the files overlap or
  not, etc). This physical plan is very specfic to your Influx cluster configuration and your data. If you load the same
  data to different clusters with different cofiguarions, the same query may be generate different query plans.
  Similarly, the same query on the same cluster at different time can have different plans depending on your data at
  that time.

Understanding a query plan can help to explain why your query is slow. For example, when the plan shows your query reads
many files, it signals you to either add more filter in the query to read less data or to modify your cluster
configuration/design to make fewer but larger files. This document focuses on how to read a query plan. How to make a
query run faster depends on the reason it is slow and beyond the scope of this document.

# How to read a query plan

## Query plan is a tree

A query plan is an upside down tree and we always read from bottom up. The physical plan in Figure 1 in tree format will
look like

```
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
its corresponding `SortExec`. The `SortExc` is responsible for sorting the data in `city` ascendingly and `time`
descendingly. The sorted outputs from the two `SortExec` are then unioned by the `UnionExec` which is then (sort) merged
by the `SortPreservingMergeExec` to return the sorted data.

## How to understand a large query plan

A large query plan may look intimidating but if you follow these steps, you can quickly understand what the plan does.

1. As always, read from bottom up, one operator at a time.
2. Understand the job of this operator which mostly can be found
   from [DataFusion Physical Plan documentation](https://docs.rs/datafusion/latest/datafusion/physical_plan/index.html).
3. What the input data of the operator looks like and how large/small it may be.
4. How much data that operator may send out and what it would look like.

If you can answer those questions, you will be able to estimate how much work that plan has to do. However, the
`explain` just shows you the plan without executing it. If you want to know exactly how long a plan and each of its
operators take, you need other tools.

##### Tools that show the exact runtime for each operator

(INTERNAL STUFF FROM IOX)

2. Run `explain analyze` to get the explain with runtime added

##### More information for debugging

If the plan has to read too many files, not all of them will be shown in the explain. To see them, use
`explain verbose`. Like `explain`, `explain verbose` does not run the query and thus you won't get runtime. What you get
is all information that is cut off from the explain and all intermidiate physical plans IOx querier and DataFusion
generate before returning the final physical plan. This is very helpful for debugging to see when an operator is added
to or removed from a plan.

## Example of typical plan of leading edge data

Let us delve into an example that covers typical operators as well as IOx specific ones on leadng edge data.

##### Data Organization

(EXPLAIN clickbench file here)

##### Query and query plan

TODO change this example

```sql
EXPLAIN SELECT city, count(1) FROM   h2o WHERE  time >= to_timestamp(200) AND time < to_timestamp(700) AND state = 'MA' GROUP BY city ORDER BY city ASC;
+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| plan_type     | plan                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| logical_plan  | Sort: h2o.city ASC NULLS LAST                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
|               |   Aggregate: groupBy=[[h2o.city]], aggr=[[COUNT(Int64(1))]]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
|               |     TableScan: h2o projection=[city], full_filters=[h2o.time >= TimestampNanosecond(200, None), h2o.time < TimestampNanosecond(700, None), h2o.state = Dictionary(Int32, Utf8("MA"))]                                                                                                                                                                                                                                                                                                                                                                                                                   |
| physical_plan | SortPreservingMergeExec: [city@0 ASC NULLS LAST]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|               |   SortExec: expr=[city@0 ASC NULLS LAST]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
|               |     AggregateExec: mode=FinalPartitioned, gby=[city@0 as city], aggr=[COUNT(Int64(1))]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
|               |       CoalesceBatchesExec: target_batch_size=8192                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
|               |         RepartitionExec: partitioning=Hash([city@0], 4), input_partitions=4                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
|               |           AggregateExec: mode=Partial, gby=[city@0 as city], aggr=[COUNT(Int64(1))]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
|               |             RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|               |               UnionExec                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
|               |                 ProjectionExec: expr=[city@0 as city]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|               |                   CoalesceBatchesExec: target_batch_size=8192                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
|               |                     FilterExec: time@2 >= 200 AND time@2 < 700 AND state@1 = MA                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
|               |                       ParquetExec: file_groups={2 groups: [[1/1/b862a7e9b329ee6a418cde191198eaeb1512753f19b87a81def2ae6c3d0ed237/243db601-f3f1-401b-afda-82160d8cc1a8.parquet], [1/1/b862a7e9b329ee6a418cde191198eaeb1512753f19b87a81def2ae6c3d0ed237/f5fb7c7d-16ac-49ba-a811-69578d05843f.parquet]]}, projection=[city, state, time], output_ordering=[state@1 ASC, city@0 ASC, time@2 ASC], predicate=time@5 >= 200 AND time@5 < 700 AND state@4 = MA, pruning_predicate=time_max@0 >= 200 AND time_min@1 < 700 AND state_min@2 <= MA AND MA <= state_max@3                                           |
|               |                 ProjectionExec: expr=[city@1 as city]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|               |                   DeduplicateExec: [state@2 ASC,city@1 ASC,time@3 ASC]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
|               |                     SortPreservingMergeExec: [state@2 ASC,city@1 ASC,time@3 ASC,__chunk_order@0 ASC]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
|               |                       UnionExec                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
|               |                         SortExec: expr=[state@2 ASC,city@1 ASC,time@3 ASC,__chunk_order@0 ASC]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|               |                           CoalesceBatchesExec: target_batch_size=8192                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|               |                             FilterExec: time@3 >= 200 AND time@3 < 700 AND state@2 = MA                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
|               |                               RecordBatchesExec: chunks=1, projection=[__chunk_order, city, state, time]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
|               |                         CoalesceBatchesExec: target_batch_size=8192                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
|               |                           FilterExec: time@3 >= 200 AND time@3 < 700 AND state@2 = MA                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|               |                             ParquetExec: file_groups={2 groups: [[1/1/b862a7e9b329ee6a418cde191198eaeb1512753f19b87a81def2ae6c3d0ed237/2cbb3992-4607-494d-82e4-66c480123189.parquet], [1/1/b862a7e9b329ee6a418cde191198eaeb1512753f19b87a81def2ae6c3d0ed237/9255eb7f-2b51-427b-9c9b-926199c85bdf.parquet]]}, projection=[__chunk_order, city, state, time], output_ordering=[state@2 ASC, city@1 ASC, time@3 ASC, __chunk_order@0 ASC], predicate=time@5 >= 200 AND time@5 < 700 AND state@4 = MA, pruning_predicate=time_max@0 >= 200 AND time_min@1 < 700 AND state_min@2 <= MA AND MA <= state_max@3 |
|               |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

Figure 3: A typical query plan of leading edge (most recent) data

Let us begin reading bottom up. The bottom or leaf nodes are always either `ParquetExec` or `RecordBatchExec`. There are
3 of them in this plan and let us go over one by one.

### Three bottom leaf operators: two `ParquetExec` & one `RecordBatchesExec`

**First ParqetExec**

```sql
|               |                             ParquetExec: file_groups={2 groups: [[1/1/b862a7e9b329ee6a418cde191198eaeb1512753f19b87a81def2ae6c3d0ed237/2cbb3992-4607-494d-82e4-66c480123189.parquet], [1/1/b862a7e9b329ee6a418cde191198eaeb1512753f19b87a81def2ae6c3d0ed237/9255eb7f-2b51-427b-9c9b-926199c85bdf.parquet]]}, projection=[__chunk_order, city, state, time], output_ordering=[state@2 ASC, city@1 ASC, time@3 ASC, __chunk_order@0 ASC], predicate=time@5 >= 200 AND time@5 < 700 AND state@4 = MA, pruning_predicate=time_max@0 >= 200 AND time_min@1 < 700 AND state_min@2 <= MA AND MA <= state_max@3 |
```

Figure 4: First `ParquetExec`

* This `ParquetExec` includes 2 groups of files. Each group can contain one or many files but, in this example, there is
  one file in each group. Files in each group are read sequencially but groups will be executed in parallel. So for this
  example, 2 files will be read in parallel.
* `1/1/b862a7e9b329ee6a418cde191198eaeb1512753f19b87a81def2ae6c3d0ed237/2cbb3992-4607-494d-82e4-66c480123189.parquet`
  this is the path of the file in S3. It is in the structure
  `namespace_id/table_id/partition_hash_id/uuid_of_the_file.parquet`. They tell you a lot:

      * Which namespace (aka database) and table of this query
    * Which partition this file belongs so you know how many partitions this query reads
    * Which file it is for you to devle into its information in the catalog (e.g. size, number of rows, ...) or download
      for local debug as needed. Here are intructions to download files
      from [Serverless](https://github.com/influxdata/docs.influxdata.io/blob/main/content/operations/specifications/iox_runbooks/querier/querier-diagnose-repro-an-issue.md?rgh-link-date=2024-08-20T19%3A38%3A43Z#iii-download-files-and-reproduce-the-problem-locally)
      and [Dedicated](https://github.com/influxdata/docs.influxdata.io/blob/main/content/operations/specifications/iox_runbooks/querier/querier-cst-access.md?rgh-link-date=2024-08-20T19%3A38%3A43Z#down-load-files-and-rebuild-catalog-to-reproduce-the-issue-locally)
* `projection=[__chunk_order, city, state, time]` : there are many columns in this table but only these 4 columns are
  read. `__chunk_order` is an artifical column the IOx code generates to keep the chunks/files in order for
  deduplication.
* `output_ordering=[state@2 ASC, city@1 ASC, time@3 ASC, __chunk_order@0 ASC]` : the output of this `ParquetExec` will
  be sorted on `state ASC, city ASC, time ASC, __chunk_order ASC`. The reason they are in that order becasue the file is
  already sorted like that.
* `predicate=time@5 >= 200 AND time@5 < 700 AND state@4 = MA` : filter in the query that will be used for data pruning
* `pruning_predicate=time_max@0 >= 200 AND time_min@1 < 700 AND state_min@2 <= MA AND MA <= state_max@3` : the actual
  pruning predicate transformed from the predicate above. This is used to filter files outside that predicate. At this
  time (Dec 2023), we only filter files based on `time`. Note that this predicate is for pruning **files of the chosen
  parttions**. Before this physical plan is generated, there is a `partition pruning` step where partitions are pruned
  using the predicates on parititoning columns (a near future document about
  `Partitionning data for better query performance` will explain this in detail).

**RecordbatchesExec**

```sql
|               |                               RecordBatchesExec: chunks=1, projection=[__chunk_order, city, state, time]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
```

Figure 5: `RecordBacthesExec`

Data from ingester can be in many chunks, in this example there is only one. Like `ParquetExec`, only data of 4 columns
are sent to the output. The action of **filtering columns** is called `projection pushdown` and, thus, it is named
`projection` here.

**Second ParquetExec**

```sql
|               |                       ParquetExec: file_groups={2 groups: [[1/1/b862a7e9b329ee6a418cde191198eaeb1512753f19b87a81def2ae6c3d0ed237/243db601-f3f1-401b-afda-82160d8cc1a8.parquet], [1/1/b862a7e9b329ee6a418cde191198eaeb1512753f19b87a81def2ae6c3d0ed237/f5fb7c7d-16ac-49ba-a811-69578d05843f.parquet]]}, projection=[city, state, time], output_ordering=[state@1 ASC, city@0 ASC, time@2 ASC], predicate=time@5 >= 200 AND time@5 < 700 AND state@4 = MA, pruning_predicate=time_max@0 >= 200 AND time_min@1 < 700 AND state_min@2 <= MA AND MA <= state_max@3                                           |
```

Figure 6: Second `ParquetExec`

The readings are similar to the one above. Note that these files and the ones in the first `ParquetExec` belong to the
same (InfluxDB) partition (e.g. the same day)

### Data-scanning structures

So the question is why we split parquet files into different ParquetExec while they are in the same partition? There are
many reasons but two major ones are:

1. Split the non-overlaps from the overlaps so we only need to apply the expensive deduplication operation on the
   overlaps. This is the case of this example.
2. Split the non-overlaps to increase parallel execution

##### When we know there are ovelaps?

```sql
|               |                   DeduplicateExec: [state@2 ASC,city@1 ASC,time@3 ASC]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
|               |                     SortPreservingMergeExec: [state@2 ASC,city@1 ASC,time@3 ASC,__chunk_order@0 ASC]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
|               |                       UnionExec                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
|               |                         SortExec: expr=[state@2 ASC,city@1 ASC,time@3 ASC,__chunk_order@0 ASC]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|               |                           CoalesceBatchesExec: target_batch_size=8192                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|               |                             FilterExec: time@3 >= 200 AND time@3 < 700 AND state@2 = MA                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
|               |                               RecordBatchesExec: chunks=1, projection=[__chunk_order, city, state, time]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
|               |                         CoalesceBatchesExec: target_batch_size=8192                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
|               |                           FilterExec: time@3 >= 200 AND time@3 < 700 AND state@2 = MA                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|               |                             ParquetExec: file_groups={2 groups: [[1/1/b862a7e9b329ee6a418cde191198eaeb1512753f19b87a81def2ae6c3d0ed237/2cbb3992-4607-494d-82e4-66c480123189.parquet], [1/1/b862a7e9b329ee6a418cde191198eaeb1512753f19b87a81def2ae6c3d0ed237/9255eb7f-2b51-427b-9c9b-926199c85bdf.parquet]]}, projection=[__chunk_order, city, state, time], output_ordering=[state@2 ASC, city@1 ASC, time@3 ASC, __chunk_order@0 ASC], predicate=time@5 >= 200 AND time@5 < 700 AND state@4 = MA, pruning_predicate=time_max@0 >= 200 AND time_min@1 < 700 AND state_min@2 <= MA AND MA <= state_max@3 |
```

Figure 7: `DeduplicationExec` is a signal of overlapped data

This structure tells us that since there are `DeduplicationExec`, data underneath it overlaps. More specifically, data
in 2 files overlaps or/and overlap with the data from the Ingesters.

* `FilterExec: time@3 >= 200 AND time@3 < 700 AND state@2 = MA`: This is the place we filter out everything that meets
  the conditions `time@3 >= 200 AND time@3 < 700 AND state@2 = MA`. The pruning before just prune data when possible, it
  does not guarantee all of them are pruned. We need this filter to do the fully filtering job.
* `CoalesceBatchesExec: target_batch_size=8192` is just a way to group smaller data to larger groups if possible. Refer
  to DF documentation for how it works
* `SortExec: expr=[state@2 ASC,city@1 ASC,time@3 ASC,__chunk_order@0 ASC]` : this sorts data on
  `state ASC, city ASC, time ASC, __chunk_order ASC`. Note that this sort operator is only applied on data from
  ingesters because data from files is already sorted on that order.
* `UnionExec` is simply a place to pull many streams together. It does not merge anything.
* `SortPreservingMergeExec: [state@2 ASC,city@1 ASC,time@3 ASC,__chunk_order@0 ASC]` : this merges the already sorted
  data. When you see this, you know data below must be sorted. The output data is in one sorted stream.
* `DeduplicateExec: [state@2 ASC,city@1 ASC,time@3 ASC]` : this deduplicated sorted data coming from strictly one input
  stream. That is why you often see under `DeduplicateExec` is `SortPreservingMergeExec` but it is not a must. As long
  as the input to `DeduplicateExec` is one sorted stream of data, it will work correctly.
*

##### When we know there are no overlaps?

```sql
|               |                 ProjectionExec: expr=[city@0 as city]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|               |                   CoalesceBatchesExec: target_batch_size=8192                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
|               |                     FilterExec: time@2 >= 200 AND time@2 < 700 AND state@1 = MA                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
|               |                       ParquetExec: file_groups={2 groups: [[1/1/b862a7e9b329ee6a418cde191198eaeb1512753f19b87a81def2ae6c3d0ed237/243db601-f3f1-401b-afda-82160d8cc1a8.parquet], [1/1/b862a7e9b329ee6a418cde191198eaeb1512753f19b87a81def2ae6c3d0ed237/f5fb7c7d-16ac-49ba-a811-69578d05843f.parquet]]}, projection=[city, state, time], output_ordering=[state@1 ASC, city@0 ASC, time@2 ASC], predicate=time@5 >= 200 AND time@5 < 700 AND state@4 = MA, pruning_predicate=time_max@0 >= 200 AND time_min@1 < 700 AND state_min@2 <= MA AND MA <= state_max@3                                           |
|
```

>
Figure 8: No `DeduplicateExec` means files not overlap
>
Deduplicate is not in this structure and above it means the files here do not overlap
>

* `ProjectionExec: expr=[city@0 as city]` : this will filter column data and only send out data of column `city`

>

### Other operators

Now let us look at the rest of the plan
>

```sql
| physical_plan | SortPreservingMergeExec: [city@0 ASC NULLS LAST]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|               |   SortExec: expr=[city@0 ASC NULLS LAST]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
|               |     AggregateExec: mode=FinalPartitioned, gby=[city@0 as city], aggr=[COUNT(Int64(1))]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
|               |       CoalesceBatchesExec: target_batch_size=8192                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
|               |         RepartitionExec: partitioning=Hash([city@0], 4), input_partitions=4                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
|               |           AggregateExec: mode=Partial, gby=[city@0 as city], aggr=[COUNT(Int64(1))]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
|               |             RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|               |               UnionExec 
```

>
Figure 9: The rest of the plan structure
>

* `UnionExec`: union data streams. Note that the number of output streams is the same is the numner of input streams.
  The operator above it is responsible to do actual merge or split streams further. This UnionExec is just here as an
  intermediate steps of the merge/split.
* `RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3`: This split 3 input streams into 4 output
  streams in round-robin fashion. The reason to split is to increase the parallel execution.
* `AggregateExec: mode=Partial, gby=[city@0 as city], aggr=[COUNT(Int64(1))]`:  This group data as specified in
  `city, count(1)`. Becasue there are 4 input streams, each stream is aggregated separately and hence the output also 4
  streams which means the output data is not fully aggregated and the `mode=Partial` signals us that.
* `RepartitionExec: partitioning=Hash([city@0], 4), input_partitions=4` : this repartitions data on hash(`city`) into 4
  streams which means the same city will go into the same stream
* `AggregateExec: mode=FinalPartitioned, gby=[city@0 as city], aggr=[COUNT(Int64(1))]`: Since rows of same city are in
  the same stream, we only need to do the final aggregation.
* `SortExec: expr=[city@0 ASC NULLS LAST]` : Sort 4 streams of data each on `city` per the query request
* `SortPreservingMergeExec: [city@0 ASC NULLS LAST]`: (Sort) merge 4 sorted streams to return the final results

>

# For your questions and references

If you see the plan reads a lof of files and does deduplication on all of them you may want ask: "do all of them overlap
or not?" The asnwer is either yes or no depending on the situation. There are other reasons that we deduplicate
non-overlap files due to memory limitation but they will be topics for future documentation.
