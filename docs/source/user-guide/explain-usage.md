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

This section describes of how to read a DataFusion query plan. While fully
comprehending all details of these plans requires significant expertise in the
DataFusion engine, this guide will help you get started with the basics.

Datafusion executes queries using a `query plan`. To see the plan without
running the query, add the keyword `EXPLAIN` to your SQL query or call the
[DataFrame::explain] method

[dataframe::explain]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.explain

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

```text
+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| plan_type     | plan                                                                                                                                                                                                      |
+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| logical_plan  | Sort: wid ASC NULLS LAST, ip DESC NULLS FIRST, fetch=5                                                                                                                                                    |
|               |   Projection: hits.parquet.WatchID AS wid, hits.parquet.ClientIP AS ip                                                                                                                                    |
|               |     Filter: starts_with(hits.parquet.URL, Utf8("http://domcheloveplanet.ru/"))                                                                                                                            |
|               |       TableScan: hits.parquet projection=[WatchID, ClientIP, URL], partial_filters=[starts_with(hits.parquet.URL, Utf8("http://domcheloveplanet.ru/"))]                                                   |
| physical_plan | SortPreservingMergeExec: [wid@0 ASC NULLS LAST,ip@1 DESC], fetch=5                                                                                                                                        |
|               |   SortExec: TopK(fetch=5), expr=[wid@0 ASC NULLS LAST,ip@1 DESC], preserve_partitioning=[true]                                                                                                            |
|               |     ProjectionExec: expr=[WatchID@0 as wid, ClientIP@1 as ip]                                                                                                                                             |
|               |       CoalesceBatchesExec: target_batch_size=8192                                                                                                                                                         |
|               |         FilterExec: starts_with(URL@2, http://domcheloveplanet.ru/)                                                                                                                                       |
|               |           DataSourceExec: file_groups={16 groups: [[hits.parquet:0..923748528], ...]}, projection=[WatchID, ClientIP, URL], predicate=starts_with(URL@13, http://domcheloveplanet.ru/), file_type=parquet |
+---------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
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

```text
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
│                  DataSourceExec                │
│          hits.parquet (filter = ...)           │
└────────────────────────────────────────────────┘
```

Each node in the tree/plan ends with `Exec` and is sometimes also called an `operator` or `ExecutionPlan` where data is
processed, transformed and sent up.

1. First, data in parquet the `hits.parquet` file us read in parallel using 16 cores in 16 "partitions" (more on this later) from `DataSourceExec`, which applies a first pass at filtering during the scan.
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

## More Debugging Information: `EXPLAIN VERBOSE`

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

[`executionplan::metrics`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html#method.metrics

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
|                   |           DataSourceExec: file_groups={16 groups: [[hits.parquet:0..923748528], ...]}, projection=[WatchID, ClientIP, URL], predicate=starts_with(URL@13, http://domcheloveplanet.ru/), metrics=[output_rows=99997497, elapsed_compute=16ns, ... bytes_scanned=3703192723, ...  time_elapsed_opening=308.203002ms, time_elapsed_scanning_total=8.350342183s, ...] |
+-------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row(s) fetched.
Elapsed 0.720 seconds.
```

In this case, DataFusion actually ran the query, but discarded any results, and
instead returned an annotated plan with a new field, `metrics=[...]`

Most operators have the common metrics `output_rows` and `elapsed_compute` and
some have operator specific metrics such as `DataSourceExec` with `ParquetSource` which has
`bytes_scanned=3703192723`. Note that times and counters are reported across all
cores, so if you have 16 cores, the time reported is the sum of the time taken
by all 16 cores.

Again, reading from bottom up:

- `DataSourceExec`
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

When predicate pushdown is enabled, `DataSourceExec` with `ParquetSource` gains the following metrics:

- `page_index_rows_matched`: number of rows in pages that were tested by a page index filter, and passed
- `page_index_rows_pruned`: number of rows in pages that were tested by a page index filter, and did not pass
- `row_groups_matched_bloom_filter`: number of rows in row groups that were tested by a Bloom Filter, and passed
- `row_groups_pruned_bloom_filter`: number of rows in row groups that were tested by a Bloom Filter, and did not pass
- `row_groups_matched_statistics`: number of rows in row groups that were tested by row group statistics (min and max value), and passed
- `row_groups_pruned_statistics`: number of rows in row groups that were tested by row group statistics (min and max value), and did not pass
- `pushdown_rows_matched`: rows that were tested by any of the above filtered, and passed all of them (this should be minimum of `page_index_rows_matched`, `row_groups_pruned_bloom_filter`, and `row_groups_pruned_statistics`)
- `pushdown_rows_pruned`: rows that were tested by any of the above filtered, and did not pass one of them (this should be sum of `page_index_rows_matched`, `row_groups_pruned_bloom_filter`, and `row_groups_pruned_statistics`)
- `predicate_evaluation_errors`: number of times evaluating the filter expression failed (expected to be zero in normal operation)
- `num_predicate_creation_errors`: number of errors creating predicates (expected to be zero in normal operation)
- `bloom_filter_eval_time`: time spent parsing and evaluating Bloom Filters
- `statistics_eval_time`: time spent parsing and evaluating row group-level statistics
- `row_pushdown_eval_time`: time spent evaluating row-level filters
- `page_index_eval_time`: time required to evaluate the page index filters

## Partitions and Execution

DataFusion determines the optimal number of cores to use as part of query
planning. Roughly speaking, each "partition" in the plan is run independently using
a separate core. Data crosses between cores only within certain operators such as
`RepartitionExec`, `CoalescePartitions` and `SortPreservingMergeExec`

You can read more about this in the [Partitioning Docs].

[partitoning docs]: https://docs.rs/datafusion/latest/datafusion/physical_expr/enum.Partitioning.html

## Example of an Aggregate Query

Let us delve into an example query that aggregates data from the `hits.parquet`
file. For example, this query from ClickBench finds the top 10 users by their
number of hits:

```sql
SELECT "UserID", COUNT(*)
FROM 'hits.parquet'
GROUP BY "UserID"
ORDER BY COUNT(*) DESC
LIMIT 10;
```

We can again see the query plan by using `EXPLAIN`:

```sql
> EXPLAIN SELECT "UserID", COUNT(*) FROM 'hits.parquet' GROUP BY "UserID" ORDER BY COUNT(*) DESC LIMIT 10;
+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| plan_type     | plan                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| logical_plan  | Limit: skip=0, fetch=10                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
|               |   Sort: count(*) DESC NULLS FIRST, fetch=10                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
|               |     Aggregate: groupBy=[[hits.parquet.UserID]], aggr=[[count(Int64(1)) AS count(*)]]                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
|               |       TableScan: hits.parquet projection=[UserID]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| physical_plan | GlobalLimitExec: skip=0, fetch=10                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
|               |   SortPreservingMergeExec: [count(*)@1 DESC], fetch=10                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
|               |     SortExec: TopK(fetch=10), expr=[count(*)@1 DESC], preserve_partitioning=[true]                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
|               |       AggregateExec: mode=FinalPartitioned, gby=[UserID@0 as UserID], aggr=[count(*)]                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
|               |         CoalesceBatchesExec: target_batch_size=8192                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
|               |           RepartitionExec: partitioning=Hash([UserID@0], 10), input_partitions=10                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
|               |             AggregateExec: mode=Partial, gby=[UserID@0 as UserID], aggr=[count(*)]                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
|               |               DataSourceExec: file_groups={10 groups: [[hits.parquet:0..1477997645], [hits.parquet:1477997645..2955995290], [hits.parquet:2955995290..4433992935], [hits.parquet:4433992935..5911990580], [hits.parquet:5911990580..7389988225], ...]}, projection=[UserID], file_type=parquet                                                                                                                                                                                                                                                    |
|               |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

For this query, let's again read the plan from the bottom to the top:

**Logical plan operators**

- `TableScan`
  - `hits.parquet`: Scans data from the file `hits.parquet`.
  - `projection=[UserID]`: Reads only the `UserID` column
- `Aggregate`
  - `groupBy=[[hits.parquet.UserID]]`: Groups by `UserID` column.
  - `aggr=[[count(Int64(1)) AS count(*)]]`: Applies the `COUNT` aggregate on each distinct group.
- `Sort`
  - `count(*) DESC NULLS FIRST`: Sorts the data in descending count order.
  - `fetch=10`: Returns only the first 10 rows.
- `Limit`
  - `skip=0`: Does not skip any data for the results.
  - `fetch=10`: Limits the results to 10 values.

**Physical plan operators**

- `DataSourceExec`
  - `file_groups={10 groups: [...]}`: Reads 10 groups in parallel from `hits.parquet`file. (The example above was run on a machine with 10 cores.)
  - `projection=[UserID]`: Pushes down projection of the `UserID` column. The parquet format is columnar and the DataFusion reader only decodes the columns required.
- `AggregateExec`
  - `mode=Partial` Runs a [partial aggregation] in parallel across each of the 10 partitions from the `DataSourceExec` immediately after reading.
  - `gby=[UserID@0 as UserID]`: Represents `GROUP BY` in the [physical plan] and groups together the same values of `UserID`.
  - `aggr=[count(*)]`: Applies the `COUNT` aggregate on all rows for each group.
- `RepartitionExec`
  - `partitioning=Hash([UserID@0], 10)`: Divides the input into into 10 (new) output partitions based on the value of `hash(UserID)`. You can read more about this in the [partitioning] documentation.
  - `input_partitions=10`: Number of input partitions.
- `CoalesceBatchesExec`
  - `target_batch_size=8192`: Combines smaller batches in to larger batches. In this case approximately 8192 rows in each batch.
- `AggregateExec`
  - `mode=FinalPartitioned`: Performs the final aggregation on each group. See the [documentation on multi phase grouping] for more information.
  - `gby=[UserID@0 as UserID]`: Groups by `UserID`.
  - `aggr=[count(*)]`: Applies the `COUNT` aggregate on all rows for each group.
- `SortExec`
  - `TopK(fetch=10)`: Use a special "TopK" sort that keeps only the largest 10 values in memory at a time. You can read more about this in the [TopK] documentation.
  - `expr=[count(*)@1 DESC]`: Sorts all rows in descending order. Note this represents the `ORDER BY` in the physical plan.
  - `preserve_partitioning=[true]`: The sort is done in parallel on each partition. In this case the top 10 values are found for each of the 10 partitions, in parallel.
- `SortPreservingMergeExec`
  - `[count(*)@1 DESC]`: This operator merges the 10 distinct streams into a single stream using this expression.
  - `fetch=10`: Returns only the first 10 rows
- `GlobalLimitExec`
  - `skip=0`: Does not skip any rows
  - `fetch=10`: Returns only the first 10 rows, denoted by `LIMIT 10` in the query.

[partial aggregation]: https://docs.rs/datafusion/latest/datafusion/physical_plan/aggregates/enum.AggregateMode.html#variant.Partial
[physical plan]: https://docs.rs/datafusion/latest/datafusion/physical_plan/aggregates/struct.PhysicalGroupBy.html
[partitioning]: https://docs.rs/datafusion/latest/datafusion/physical_plan/repartition/struct.RepartitionExec.html
[topk]: https://docs.rs/datafusion/latest/datafusion/physical_plan/struct.TopK.html
[documentation on multi phase grouping]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.Accumulator.html#tymethod.state

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
