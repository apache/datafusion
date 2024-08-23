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

## Introduction

This section describes  of how to read a DataFusion query plan. While fully
comprehending all details of these plans requires significant expertise in the
DataFusion engine, this guide will help you get started with the basics.

Datafusion executes queries using a `query plan`. To see the plan without
running the query, add the keyword `EXPLAIN` to your SQL query or call the
[DataFrame::explain] method

[DataFrame::explain]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.explain

## Example: Select and filter

In this section, we run example queries against the `hits.parquet` file. See
[below](#data-in-this-example)) for information on how to get this file.

Let's see how DataFusion runs a query that selects the top 5 watch lists for the
site `http://domcheloveplanet.ru/`:

```sql
EXPLAIN SELECT "WatchID" AS wid, "hits.parquet"."ClientIP" AS ip
FROM 'hits.parquet'
WHERE starts_with("URL", 'http://domcheloveplanet.ru/')
ORDER BY wid ASC, ip DESC
LIMIT 5;
```

The output will look like

```
+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| plan_type     | plan                                                                                                                                                                                |
+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| logical_plan  | Sort: wid ASC NULLS LAST, ip DESC NULLS FIRST, fetch=5                                                                                                                              |
|               |   Projection: hits.parquet.WatchID AS wid, hits.parquet.ClientIP AS ip                                                                                                              |
|               |     Filter: starts_with(hits.parquet.URL, Utf8("http://domcheloveplanet.ru/"))                                                                                                      |
|               |       TableScan: hits.parquet projection=[WatchID, ClientIP, URL], partial_filters=[starts_with(hits.parquet.URL, Utf8("http://domcheloveplanet.ru/"))]                             |
| physical_plan | SortPreservingMergeExec: [wid@0 ASC NULLS LAST,ip@1 DESC], fetch=5                                                                                                                  |
|               |   SortExec: TopK(fetch=5), expr=[wid@0 ASC NULLS LAST,ip@1 DESC], preserve_partitioning=[true]                                                                                      |
|               |     ProjectionExec: expr=[WatchID@0 as wid, ClientIP@1 as ip]                                                                                                                       |
|               |       CoalesceBatchesExec: target_batch_size=8192                                                                                                                                   |
|               |         FilterExec: starts_with(URL@2, http://domcheloveplanet.ru/)                                                                                                                 |
|               |           ParquetExec: file_groups={16 groups: [[hits.parquet:0..923748528], ...]}, projection=[WatchID, ClientIP, URL], predicate=starts_with(URL@13, http://domcheloveplanet.ru/) |
+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
2 row(s) fetched.
Elapsed 0.060 seconds.
```

There are two sections: logical plan and physical plan

- **Logical Plan:** is a plan generated for a specific SQL query, DataFrame, or other language without the  
  knowledge of the underlying data organization.
- **Physical Plan:** is a plan generated from a logical plan along with consideration of the hardware
  configuration (e.g number of CPUs) and the underlying data organization (e.g number of files).
  This physical plan is specific to your hardware configuration and your data. If you load the same
  data to different hardware with different configurations, the same query may generate different query plans.

Understanding a query plan can help to you understand its performance. For example, when the plan shows your query reads
many files, it signals you to either add more filter in the query to read less data or to modify your file
design to make fewer but larger files. This document focuses on how to read a query plan. How to make a
query run faster depends on the reason it is slow and beyond the scope of this document.

## Query plans are trees

A query plan is an upside down tree, and we always read from bottom up. The
physical plan in Figure 1 in tree format will look like

```
                         ▲
                         │
                         │
┌─────────────────────────────────────────────────┐
│             SortPreservingMergeExec             │
│        [wid@0 ASC NULLS LAST,ip@1 DESC]         │
│                     fetch=5                     │
└─────────────────────────────────────────────────┘
                         ▲
                         │
┌─────────────────────────────────────────────────┐
│             SortExec TopK(fetch=5),             │
│     expr=[wid@0 ASC NULLS LAST,ip@1 DESC],      │
│           preserve_partitioning=[true]          │
└─────────────────────────────────────────────────┘
                         ▲
                         │
┌─────────────────────────────────────────────────┐
│                 ProjectionExec                  │
│    expr=[WatchID@0 as wid, ClientIP@1 as ip]    │
└─────────────────────────────────────────────────┘
                         ▲
                         │
┌─────────────────────────────────────────────────┐
│               CoalesceBatchesExec               │
└─────────────────────────────────────────────────┘
                         ▲
                         │
┌─────────────────────────────────────────────────┐
│                   FilterExec                    │
│ starts_with(URL@2, http://domcheloveplanet.ru/) │
└─────────────────────────────────────────────────┘
                         ▲
                         │
┌────────────────────────────────────────────────┐
│                  ParquetExec                   │
│          hits.parquet (filter = ...)           │
└────────────────────────────────────────────────┘
```

Each node in the tree/plan ends with `Exec` and is sometimes also called an `operator` or `ExecutionPlan` where data is
processed, transformed and sent up.

1. First, data in parquet the `hits.parquet` file us read in parallel using 16 cores in 16 "partitions" (more on this later) from `ParquetExec`, which applies a first pass at filtering during the scan.
2. Next, the output is filtered using `FilterExec` to ensure only rows where `starts_with(URL, 'http://domcheloveplanet.ru/')` evaluates to true are passed on
3. The `CoalesceBatchesExec` then ensures that the data is grouped into larger batches for processing
4. The `ProjectionExec` then projects the data to rename the `WatchID` and `ClientIP` columns to `wid` and `ip` respectively.
5. The `SortExec` then sorts the data by `wid ASC, ip DESC`. The `Topk(fetch=5)` indicates that a special implementation is used that only tracks and emits the top 5 values in each partition.
6. Finally the `SortPreservingMergeExec` merges the sorted data from all partitions and returns the top 5 rows overall.

## Understanding large query plans

A large query plan may look intimidating, but you can quickly understand what it does by following these steps

1. As always, read from bottom up, one operator at a time.
2. Understand the job of this operator by reading
   the [Physical Plan documentation](https://docs.rs/datafusion/latest/datafusion/physical_plan/index.html).
3. Understand the input data of the operator and how large/small it may be.
4. Understand how much data that operator produces and what it would look like.

If you can answer those questions, you will be able to estimate how much work
that plan has to do and thus how long it will take. However, the `EXPLAIN` just
shows you the plan without executing it.

If you want to know more about how much work each operator in query plan does,
you can use the `EXPLAIN ANALYZE` to get the explain with runtime added (see
next section)

##  More Debugging Information: `EXPLAIN VERBOSE` 

If the plan has to read too many files, not all of them will be shown in the
`EXPLAIN`. To see them, use `EXPLAIN VEBOSE`. Like `EXPLAIN`, `EXPLAIN VERBOSE`
does not run the query. Instead it shows the full explain plan, with information
that is omitted from the default explain, as well as all intermediate physical
plans DataFusion generates before returning. This mode can be very helpful for
debugging to see why and when DataFusion added and removed operators from a plan.

## Execution Counters: `EXPLAIN ANALYZE` 

During execution, DataFusion operators collect detailed metrics. You can access
them programmatically via [`ExecutionPlan::metrics`] as well as with the
`EXPLAIN ANALYZE` command. For example here is the same query query as
above but with `EXPLAIN ANALYZE` (note the output is edited for clarity)

[`ExecutionPlan::metrics`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html#method.metrics

```sql
> EXPLAIN ANALYZE SELECT "WatchID" AS wid, "hits.parquet"."ClientIP" AS ip
FROM 'hits.parquet'
WHERE starts_with("URL", 'http://domcheloveplanet.ru/')
ORDER BY wid ASC, ip DESC
LIMIT 5;
+-------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| plan_type         | plan                                                                                                                                                                                                                                                                                                                                                           |
+-------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Plan with Metrics | SortPreservingMergeExec: [wid@0 ASC NULLS LAST,ip@1 DESC], fetch=5, metrics=[output_rows=5, elapsed_compute=2.375µs]                                                                                                                                                                                                                                           |
|                   |   SortExec: TopK(fetch=5), expr=[wid@0 ASC NULLS LAST,ip@1 DESC], preserve_partitioning=[true], metrics=[output_rows=75, elapsed_compute=7.243038ms, row_replacements=482]                                                                                                                                                                                     |
|                   |     ProjectionExec: expr=[WatchID@0 as wid, ClientIP@1 as ip], metrics=[output_rows=811821, elapsed_compute=66.25µs]                                                                                                                                                                                                                                           |
|                   |         FilterExec: starts_with(URL@2, http://domcheloveplanet.ru/), metrics=[output_rows=811821, elapsed_compute=1.36923816s]                                                                                                                                                                                                                                 |
|                   |           ParquetExec: file_groups={16 groups: [[hits.parquet:0..923748528], ...]}, projection=[WatchID, ClientIP, URL], predicate=starts_with(URL@13, http://domcheloveplanet.ru/), metrics=[output_rows=99997497, elapsed_compute=16ns, ... bytes_scanned=3703192723, ...  time_elapsed_opening=308.203002ms, time_elapsed_scanning_total=8.350342183s, ...] |
+-------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row(s) fetched.
Elapsed 0.720 seconds.
```

In this case, DataFusion actually ran the query, but discarded any results, and
instead returned an annotated plan with a new field, `metrics=[...]`

Most operators have the common metrics `output_rows` and `elapsed_compute` and
some have operator specific metrics such as `ParquetExec` which has
`bytes_scanned=3703192723`. Note that times and counters are reported across all
cores, so if you have 16 cores, the time reported is the sum of the time taken
by all 16 cores.

Again, reading from bottom up:
- `ParquetExec`
  - `output_rows=99997497`: A total 99.9M rows were produced 
  - `bytes_scanned=3703192723`: Of the 14GB file, 3.7GB were actually read (due to projection pushdown) 
  - `time_elapsed_opening=308.203002ms`: It took 300ms to open the file and prepare to read it
  - `time_elapsed_scanning_total=8.350342183s`: It took 8.3 seconds of CPU time (across 16 cores) to actually decode the parquet data
- `FilterExec`
  - `output_rows=811821`: Of the 99.9M rows at its input, only 811K rows passed the filter and were produced at the output 
  - `elapsed_compute=1.36923816s`: In total, 1.36s of CPU time (across 16 cores) was spend evaluating the filter
- `CoalesceBatchesExec`
  - `output_rows=811821`, `elapsed_compute=12.873379ms`: Produced 811K rows in 13ms
- `ProjectionExec`
  - `output_rows=811821, elapsed_compute=66.25µs`: Produced 811K rows in 66µs (microseconds). This projection is almost instantaneous as it does not manipulate any data
- `SortExec`
  - `output_rows=75`: Produced 75 rows in total. Each of 16 cores could produce up to 5 rows, but in this case not all cores did.
  - `elapsed_compute=7.243038ms`: 7ms was used to determine the top 5 rows
  - `row_replacements=482`: Internally, the TopK operator updated its top list 482 times 
- `SortPreservingMergeExec`
  - `output_rows=5`, `elapsed_compute=2.375µs`: Produced the final 5 rows in 2.375µs (microseconds)

## Example of an Aggregate Query

Let us delve into an example click bench query that aggregates data from the `hits.parquet` file. 

For example, this query from ClickBench finds the top 10 users by the number of hits:

```sql
SELECT "UserID", COUNT(*) FROM 'hits.parquet' GROUP BY "UserID" ORDER BY COUNT(*) DESC LIMIT 10;
```

##### Query and query plan

TODO: devanbenz can you update this section, trying to follow the model of the simpler one above?
In this case only do the `EXPLAIN` not the `EXPLAIN ANALYZE`




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

- This `ParquetExec` includes 10 groups. Each group can contain one or many files but, in this example, there is
  one file for many groups. Files in each group are read sequentially but groups will be executed in parallel. So for
  this
  example, 10 groups will be read in parallel.
- `/data/hits.parquet`: this is the path of the file.
- `projection=[EventTime, RegionID, OS]`: there are many columns in this table but only these 4 columns are read.
- `predicate=to_timestamp(EventTime@4) >= 200000000000 AND to_timestamp(EventTime@4) < 700000000000 AND RegionID@8 = 839`: filter in the query used for data pruning,
- `pruning_predicate=CASE WHEN RegionID_null_count@2 = RegionID_row_count@3 THEN false ELSE RegionID_min@0 <= 839 AND 839 <= RegionID_max@1 END`: the actual pruning predicate transformed from the predicate above. This is used to filter files outside that predicate.
- `required_guarantees=[RegionID in (839)]`: TODO, need definition.

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

- `ProjectionExec: expr=[to_timestamp(EventTime@0) as __common_expr_4, RegionID@1 as RegionID, OS@2 as OS]`: this will filter the column
  data to only send data of column's `EventTime` aliased as `__common_expr_4` converted to a timestamp, `RegionID` and `OS`
- `FilterExec: __common_expr_4@0 >= 200000000000 AND __common_expr_4@0 < 700000000000 AND RegionID@1 = 839`: places a filter on the data
  that meets the conditions. The pruning that happens before is not guaranteed and thus we need to use this filter to complete the filtering.
- `CoalesceBatchesExec: target_batch_size=8192`: groups smaller data in to larger groups
- `ProjectionExec: expr=[OS@2 as OS]`: TODO: explain why there are multiple projection steps
- `AggregateExec: mode=Partial, gby=[OS@0 as OS], aggr=[count(Int64(1))]`: partial aggregate working in parallel across partitions of data.
  This group of data is specified as `"hits.parquet".OS AS OS, COUNT(1)`.
- `RepartitionExec: partitioning=Hash([OS@0], 10), input_partitions=10`: this takes 10 input streams and partitions them based on the `Hash`
  partition variant using `OS` as the hash. See: https://docs.rs/datafusion/latest/datafusion/physical_plan/enum.Partitioning.html for other variants and more information
- `CoalesceBatchesExec: target_batch_size=8192`: TODO: explain why there are multiple coalesce batches steps
- `AggregateExec: mode=FinalPartitioned, gby=[OS@0 as OS], aggr=[count(Int64(1))]`: we do a final aggregate on the data
- `ProjectionExec: expr=[OS@0 as os, count(Int64(1))@1 as count(Int64(1))]`: filter column data and only send out `os` and `count(1)`
- `SortExec: expr=[os@0 ASC NULLS LAST], preserve_partitioning=[true]`: sorts the 10 streams of data each on the `os` column ascendingly
- `SortPreservingMergeExec: [os@0 ASC NULLS LAST]`: sort and merge the 10 streams of data for final results

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

- `TableScan: hits.parquet projection=[EventTime, RegionID, OS], partial_filters=[to_timestamp(hits.parquet.EventTime) >= TimestampNanosecond(200000000000, None), to_timestamp(hits.parquet.EventTime) < TimestampNanosecond(700000000000, None), hits.parquet.RegionID = Int32(839)]`:
  produces a row of tables from `hits.parquet`, projects the following fields `EventTime`, `RegionID`, and `OS` and does further partial filtering.
- `Projection: to_timestamp(hits.parquet.EventTime) AS __common_expr_4, hits.parquet.RegionID, hits.parquet.OS`: projects a subset of the data
- `Filter: __common_expr_4 >= TimestampNanosecond(200000000000, None) AND __common_expr_4 < TimestampNanosecond(700000000000, None) AND hits.parquet.RegionID = Int32(839)`:
  filters the data
- `Projection: hits.parquet.OS`: projects field to be displayed
- `Aggregate: groupBy=[[hits.parquet.OS]], aggr=[[count(Int64(1))]]`: aggregates the values `OS, count(1)`
- `Projection: hits.parquet.OS AS os, count(Int64(1))`: projects `hits.parquet.OS` as `os`
- `Sort: os ASC NULLS LAST`: final sort of the data where projected `os` field is sorted ascendingly

### Data in this Example

The examples in this section use data from [ClickBench], a benchmark for data
analytics. The examples are in terms of the 14GB [`hits.parquet`] file and can be
downloaded from the website or using the following commands:

```shell
cd benchmarks
./bench.sh data clickbench_1
***************************
DataFusion Benchmark Runner and Data Generator
COMMAND: data
BENCHMARK: clickbench_1
DATA_DIR: /Users/andrewlamb/Software/datafusion2/benchmarks/data
CARGO_COMMAND: cargo run --release
PREFER_HASH_JOIN: true
***************************
Checking hits.parquet...... found 14779976446 bytes ... Done
```

Then you can run `datafusion-cli` to get plans:

```shell
cd datafusion/benchmarks/data
datafusion-cli

DataFusion CLI v41.0.0
> select count(*) from 'hits.parquet';
+----------+
| count(*) |
+----------+
| 99997497 |
+----------+
1 row(s) fetched.
Elapsed 0.062 seconds.
>
```

[clickbench]: https://benchmark.clickhouse.com/
[`hits.parquet`]: https://datasets.clickhouse.com/hits_compatible/hits.parquet
