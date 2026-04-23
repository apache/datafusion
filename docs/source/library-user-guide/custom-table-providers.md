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

# Custom Table Provider

One of DataFusion's greatest strengths is its extensibility. If your data lives
in a custom format, behind an API, or in a system that DataFusion does not
natively support, you can teach DataFusion to read it by implementing a
**custom table provider**. This post walks through the three layers you need to
understand to design a table provider and where planning and execution work should happen.

For details on how table constraints such as primary keys or unique
constraints are handled, see [Table Constraint Enforcement](table-constraints.md).

The majority of this content was originally posted in the blog
[Writing Custom Table Providers in Apache DataFusion](https://datafusion.apache.org/blog/2026/03/31/writing-table-providers/).

## The Three Layers

When DataFusion executes a query against a table, three abstractions collaborate
to produce results:

1. **[TableProvider]** -- Describes the table (schema, capabilities) and
   produces an execution plan when queried. This is part of the **Logical Plan**.
2. **[ExecutionPlan]** -- Describes _how_ to compute the result: partitioning,
   ordering, and child plan relationships. This is part of the **Physical Plan**.
3. **[SendableRecordBatchStream]** -- The async stream that _actually does the
   work_, yielding `RecordBatch`es one at a time.

Think of these as a funnel: `TableProvider::scan()` is called once during
planning to create an `ExecutionPlan`, then `ExecutionPlan::execute()` is called
once per partition to create a stream, and those streams are where rows are
actually produced during execution.

[tableprovider]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html
[executionplan]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html
[sendablerecordbatchstream]: https://docs.rs/datafusion/latest/datafusion/execution/type.SendableRecordBatchStream.html
[memtable]: https://docs.rs/datafusion/latest/datafusion/datasource/memory/struct.MemTable.html
[streamtable]: https://docs.rs/datafusion/latest/datafusion/datasource/stream/struct.StreamTable.html
[listingtable]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingTable.html
[viewtable]: https://docs.rs/datafusion/latest/datafusion/datasource/view/struct.ViewTable.html
[planproperties]: https://docs.rs/datafusion/latest/datafusion/physical_plan/struct.PlanProperties.html
[streamingtableexec]: https://docs.rs/datafusion/latest/datafusion/physical_plan/streaming/struct.StreamingTableExec.html
[datasourceexec]: https://docs.rs/datafusion/latest/datafusion/datasource/source/struct.DataSourceExec.html

## Background: Logical and Physical Planning

Before diving into the three layers, it helps to understand how DataFusion
processes a query. There are several phases between a SQL string (or DataFrame
call) and streaming results:

```text
SQL / DataFrame API
  → Logical Plan          (abstract: what to compute)
  → Logical Optimization  (rewrite rules that preserve semantics)
  → Physical Plan         (concrete: how to compute it)
  → Physical Optimization (hardware- and data-aware rewrites)
  → Execution             (streaming RecordBatches)
```

### Logical Planning

A **logical plan** describes _what_ the query computes without specifying _how_.
It is a tree of relational operators -- `TableScan`, `Filter`, `Projection`,
`Aggregate`, `Join`, `Sort`, `Limit`, and so on. The logical optimizer rewrites
this tree to reduce work while preserving the query's meaning. Some logical
optimizations include:

- **Predicate pushdown** -- moves filters as close to the data source as
  possible, so fewer rows flow through the rest of the plan.
- **Projection pruning** -- eliminates columns that are never referenced
  downstream, reducing memory and I/O.
- **Expression simplification** -- rewrites expressions like `1 = 1` or
  `x AND true` into simpler forms.
- **Subquery decorrelation** -- converts correlated `IN` / `EXISTS` subqueries
  into more efficient semi-joins.
- **Limit pushdown** -- pushes `LIMIT` earlier in the plan so operators
  produce less data.

### Physical Planning

The **physical planner** converts the optimized logical plan into an
`ExecutionPlan` tree -- the concrete plan that will actually run. This is where
decisions like "use a hash join vs. a sort-merge join" or "how many partitions
to scan" are made. The physical optimizer then refines this tree further with rewrites such as:

- **Distribution enforcement** -- inserts `RepartitionExec` nodes so that data
  is partitioned correctly for joins and aggregations.
- **Sort enforcement** -- inserts `SortExec` nodes where ordering is required,
  and removes them where the data is already sorted.
- **Join selection** -- picks the most efficient join strategy based on
  statistics and table sizes.
- **Aggregate optimization** -- combines partial and final aggregation stages,
  and can use exact statistics to skip scanning entirely.

### Why This Matters for Table Providers

Your `TableProvider` sits at the boundary between logical and physical planning.
During logical optimization, DataFusion determines which filters and projections
_could_ be pushed down to the source. When `scan()` is called during physical
planning, those hints are passed to you. By implementing capabilities like
`supports_filters_pushdown`, you influence what the optimizer can do -- and the
metadata you declare in your `ExecutionPlan` (partitioning, ordering) directly
affects which physical optimizations apply.

## Choosing the Right Starting Point

Not every custom data source requires implementing all three layers from
scratch. DataFusion provides building blocks that let you plug in at whatever
level makes sense:

| If your data is...                                 | Start with                                                                | You implement                  |
| -------------------------------------------------- | ------------------------------------------------------------------------- | ------------------------------ |
| Already in `RecordBatch`es in memory               | [MemTable]                                                                | Nothing -- just construct it   |
| An async stream of batches                         | [StreamTable]                                                             | A stream factory               |
| A logical transformation of other tables           | [ViewTable] wrapping a logical plan                                       | The logical plan               |
| A variant of an existing file format               | [ListingTable] with a custom [FileFormat] wrapping an existing one        | A thin `FileFormat` wrapper    |
| Files in a custom format on disk or object storage | [ListingTable] with a custom [FileFormat], [FileSource], and [FileOpener] | The format, source, and opener |
| A custom source needing full control               | `TableProvider` + `ExecutionPlan` + stream                                | All three layers               |

[fileformat]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/trait.FileFormat.html
[filesource]: https://docs.rs/datafusion-datasource/latest/datafusion_datasource/file/trait.FileSource.html
[fileopener]: https://docs.rs/datafusion-datasource/latest/datafusion_datasource/file_stream/trait.FileOpener.html

If your data is file-based, `ListingTable` handles file discovery, partition
column inference, and plan construction -- you only need to implement
`FileFormat`, `FileSource`, and `FileOpener` to describe how to read your
files. See the [custom_file_format example] for a minimal wrapping approach,
or [ParquetSource] and [ParquetOpener] for a full custom implementation to
use as a reference.

[custom_file_format example]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/custom_data_source/custom_file_format.rs
[parquetsource]: https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/struct.ParquetSource.html
[parquetopener]: https://github.com/apache/datafusion/blob/main/datafusion/datasource-parquet/src/opener.rs

The rest of this post focuses on the full `TableProvider` + `ExecutionPlan` +
stream path, which gives you complete control and applies to any data source.

## Layer 1: TableProvider

A [TableProvider] represents a queryable data source. For a minimal read-only
table, you need three methods:

```rust,ignore
impl TableProvider for MyTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Build and return an ExecutionPlan -- don't do any execution work here -- keep lightweight!
        Ok(Arc::new(MyExecPlan::new(
            Arc::clone(&self.schema),
            projection,
            limit,
        )))
    }
}
```

The `scan` method is the heart of `TableProvider`. It receives three pushdown
hints from the optimizer, each reducing the amount of data your source needs
to produce:

- **`projection`** -- Which columns are needed. This reduces the **width** of
  the output. If your source supports it, read only these columns rather than
  the full schema.
- **`filters`** -- Predicates the engine would like you to apply during the
  scan. This reduces the **number of rows** by skipping data that does not
  match. Implement `supports_filters_pushdown` to advertise which filters you
  can handle.
- **`limit`** -- A row count cap. This also reduces the **number of rows** --
  if you can stop reading early once you have produced enough rows, this avoids
  unnecessary work.

You can also use the [scan_with_args()](https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html#method.scan_with_args)
variant that provides additional pushdown information for other advanced use cases.

### Keep `scan()` Lightweight

This is a critical point: **`scan()` runs during planning, not execution.** It
should return quickly. Best practice is to avoid performing I/O, network
calls, or heavy computation here. The `scan` method's job is to _describe_ how
the data will be produced, not to produce it. All the real work belongs in the
stream (Layer 3).

A common pitfall is to fetch data or open connections in `scan()`. This blocks
the planning thread and can cause timeouts or deadlocks, especially if the query
involves multiple tables or subqueries that all need to be planned before
execution begins.

### Existing Implementations to Learn From

DataFusion ships several `TableProvider` implementations that are excellent
references:

- **[MemTable]** -- Holds data in memory as `Vec<RecordBatch>`. The simplest
  possible provider; great for tests and small datasets.
- **[StreamTable]** -- Wraps a user-provided stream factory. Useful when your
  data arrives as a continuous stream (e.g., from Kafka or a socket).
- **[ListingTable]** -- The file-based data source behind DataFusion's
  built-in Parquet, CSV, and JSON support. Demonstrates sophisticated filter
  and projection pushdown, file pruning, and schema inference.
- **[ViewTable]** -- Wraps a logical plan, representing a SQL view. Useful
  if your provider is best expressed as a transformation of other tables.

## Layer 2: ExecutionPlan

An [ExecutionPlan] is a node in the physical query plan tree. Your table
provider's `scan()` method returns one. The required methods are:

```rust,ignore
impl ExecutionPlan for MyExecPlan {
    fn name(&self) -> &str { "MyExecPlan" }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]  // Leaf node -- no children
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert!(children.is_empty());
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // This is where you build and return your stream
        // ...
    }
}
```

The key properties to set correctly in [PlanProperties] are **output
partitioning** and **output ordering**.

**Output partitioning** tells the engine how many partitions your data has,
which determines parallelism. If your source naturally partitions data (e.g.,
by file or by shard), expose that here.

**Output ordering** declares whether your data is naturally sorted. This
enables the optimizer to avoid inserting a `SortExec` when a query requires
ordered data. Getting this right can be a significant performance win.

### Partitioning Strategies

Since `execute()` is called once per partition, partitioning directly controls
the parallelism of your table scan. Each partition produces an independent
stream that DataFusion schedules as a **task** on the tokio runtime. It is
important to distinguish tasks from threads: tasks are lightweight units of
async work that are multiplexed onto a thread pool. You can have many more
tasks (partitions) than physical threads -- the runtime will interleave them
efficiently as they await I/O or yield.

**Start simple: match your data's natural layout.** If you have 4 files, expose
4 partitions. If your source has 8 shards, expose 8 partitions. DataFusion will
insert a `RepartitionExec` above your scan when downstream operators need a
different distribution. You can also implement the
[`repartitioned`](https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html#method.repartitioned)
method on your `ExecutionPlan` to let DataFusion request a different partition
count directly from your source, avoiding the extra operator entirely.

Consider how your data source naturally divides its data:

- **By file or object:** If you are reading from S3, each file can be a
  partition. DataFusion will read them in parallel.
- **By shard or region:** If your source is a sharded database, each shard
  maps naturally to a partition.
- **By key range:** If your data is keyed (e.g., by timestamp or customer ID),
  you can split it into ranges.

**Advanced: aligning with `target_partitions`.** Once you have something
working, you can tune further. Having _too many_ partitions is not free: each
partition adds scheduling overhead, and downstream operators may need to
repartition the data anyway. The session configuration exposes a
**target partition count** that reflects how many partitions the optimizer
expects to work with:

```rust,ignore
async fn scan(
    &self,
    state: &dyn Session,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let target_partitions = state.config().target_partitions();
    // Optionally coalesce or split partitions to match target_partitions.
    // ...
}
```

If your source produces data in exactly `target_partitions` partitions, the
optimizer is less likely to insert a `RepartitionExec` above your scan.
For small datasets, `target_partitions` may be set to 1, which avoids any
repartitioning overhead entirely.

**Advanced: declaring hash partitioning.** If your source stores data
pre-partitioned by a specific key (e.g., `customer_id`), you can declare this
in your output partitioning. For a query like:

```sql
SELECT customer_id, SUM(amount)
FROM my_table
GROUP BY customer_id;
```

If you declare your output partitioning as `Hash([customer_id], N)`, the
optimizer recognizes that the data is already distributed correctly for the
aggregation and eliminates the `RepartitionExec` that would otherwise appear
in the plan. You can verify this with `EXPLAIN` (more on this below).

Conversely, if you report `UnknownPartitioning`, DataFusion must assume the
worst case and will always insert repartitioning operators as needed.

### Keep `execute()` Lightweight Too

Like `scan()`, the `execute()` method should construct and return a stream
without doing heavy work. The actual data production happens when the stream
is polled. Do not block on async operations here -- build the stream and let
the runtime drive it.

### Existing Implementations to Learn From

- **[StreamingTableExec]** -- Executes a streaming table scan. It takes a
  stream factory (a closure that produces streams) and handles partitioning.
  Good reference for wrapping external streams.
- **[DataSourceExec]** -- The execution plan behind DataFusion's built-in file
  scanning (Parquet, CSV, JSON). It demonstrates sophisticated partitioning,
  filter pushdown, and projection pushdown.

## Layer 3: SendableRecordBatchStream

[SendableRecordBatchStream] is where the real work happens. It is defined as:

```rust,ignore
type SendableRecordBatchStream =
    Pin<Box<dyn RecordBatchStream<Item = Result<RecordBatch>> + Send>>;
```

This is an async stream of `RecordBatch`es that can be sent across threads. When
the DataFusion runtime polls this stream, your code runs: reading files, calling
APIs, transforming data, etc.

### Using RecordBatchStreamAdapter

The easiest way to create a `SendableRecordBatchStream` is with
[RecordBatchStreamAdapter]. It bridges any `futures::Stream<Item = Result<RecordBatch>>` into the `SendableRecordBatchStream` type:

```rust,ignore
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

fn execute(
    &self,
    partition: usize,
    context: Arc<TaskContext>,
) -> Result<SendableRecordBatchStream> {
    let schema = self.schema();
    let config = self.config.clone();

    let stream = futures::stream::once(async move {
        // ALL the heavy work happens here, inside the stream:
        // - Open connections
        // - Read data from external sources
        // - Transform and batch the results
        let batches = fetch_data_from_source(&config).await?;
        Ok(batches)
    })
    .flat_map(|result| match result {
        Ok(batch) => futures::stream::iter(vec![Ok(batch)]),
        Err(e) => futures::stream::iter(vec![Err(e)]),
    });

    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}
```

[recordbatchstreamadapter]: https://docs.rs/datafusion/latest/datafusion/physical_plan/stream/struct.RecordBatchStreamAdapter.html

### Blocking Work: Use a Separate Thread Pool

If your stream performs **blocking** work -- such as blocking I/O, or CPU work
that runs for hundreds of milliseconds without yielding -- you must avoid
blocking the tokio async runtime. Short CPU work (e.g., parsing a batch in a
few milliseconds) is fine to do inline as long as your code yields back to the
runtime frequently. But for long-running synchronous work that cannot yield,
offload to a dedicated thread pool and send results back through a channel:

```rust,ignore
fn execute(
    &self,
    partition: usize,
    context: Arc<TaskContext>,
) -> Result<SendableRecordBatchStream> {
    let schema = self.schema();
    let config = self.config.clone();

    let (tx, rx) = tokio::sync::mpsc::channel(2);

    // Spawn blocking work on a dedicated thread pool
    tokio::task::spawn_blocking(move || {
        let batches = generate_data(&config);
        for batch in batches {
            if tx.blocking_send(Ok(batch)).is_err() {
                break; // Receiver dropped, query was cancelled
            }
        }
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}
```

This pattern keeps the async runtime responsive while long-running synchronous
work runs on its own threads. For a working example that shows how to configure
separate thread pools for I/O and CPU work, see the
[thread_pools example](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/query_planning/thread_pools.rs)
in the DataFusion repository.

## Where Should the Work Happen?

This table summarizes what belongs at each layer:

| Layer                         | Runs During                    | Should Do                              | Should NOT Do                         |
| ----------------------------- | ------------------------------ | -------------------------------------- | ------------------------------------- |
| `TableProvider::scan()`       | Planning                       | Build an `ExecutionPlan` with metadata | I/O, network calls, heavy computation |
| `ExecutionPlan::execute()`    | Execution (once per partition) | Construct a stream, set up channels    | Block on async work, read data        |
| `RecordBatchStream` (polling) | Execution                      | All I/O, computation, data production  | --                                    |

The guiding principle: **push work as late as possible.** Planning should be
fast so the optimizer can do its job. Execution setup should be fast so all
partitions can start promptly. The stream is where you spend time producing
data.

### Why This Matters

When `scan()` does heavy work, several problems arise:

1. **Planning becomes slow.** If a query touches 10 tables and each `scan()`
   takes 500ms, planning alone takes 5 seconds before any data flows.
2. **Execution is single-threaded.** `scan()` runs on a single thread during
   planning, so any work done there cannot benefit from the parallel execution
   that DataFusion provides across partitions.
3. **The optimizer cannot help.** The optimizer runs between planning and
   execution. If you have already fetched data during planning, optimizations
   like predicate pushdown or partition pruning cannot reduce the work.
4. **Resource management breaks down.** DataFusion manages concurrency and
   memory during execution. Work done during planning bypasses these controls.

## Filter Pushdown: Doing Less Work

One of the most impactful optimizations you can add to a custom table provider
is **filter pushdown** -- letting the source skip data that the query does not
need, rather than reading everything and filtering it afterward.

### How Filter Pushdown Works

When DataFusion plans a query with a `WHERE` clause, it passes the filter
predicates to your `scan()` method as the `filters` parameter. By default,
DataFusion assumes your provider cannot handle any filters and inserts a
`FilterExec` node above your scan to apply them. But if your source _can_
evaluate some predicates during scanning -- for example, by skipping files,
partitions, or row groups that cannot match -- you can eliminate a huge amount
of unnecessary I/O.

To opt in, implement `supports_filters_pushdown`:

```rust
# use std::any::Any;
# use std::sync::Arc;
# use arrow::datatypes::SchemaRef;
# use datafusion::catalog::{TableProvider, Session};
# use datafusion::common::Result;
# use datafusion::datasource::TableType;
# use datafusion::logical_expr::{Expr, BinaryExpr, Operator, TableProviderFilterPushDown};
# use datafusion::physical_plan::ExecutionPlan;
#
# fn is_partition_column(_expr: &Expr) -> bool { false }
#
# #[derive(Debug)]
# struct MyFilterTable;
#
# #[async_trait::async_trait]
# impl TableProvider for MyFilterTable {
#     fn schema(&self) -> SchemaRef { todo!() }
#     fn table_type(&self) -> TableType { TableType::Base }
#     async fn scan(&self, _: &dyn Session, _: Option<&Vec<usize>>, _: &[Expr], _: Option<usize>) -> Result<Arc<dyn ExecutionPlan>> { todo!() }
#
fn supports_filters_pushdown(
    &self,
    filters: &[&Expr],
) -> Result<Vec<TableProviderFilterPushDown>> {
    Ok(filters.iter().map(|f| {
        match f {
            // We can fully evaluate equality filters on
            // the partition column at the source
            Expr::BinaryExpr(BinaryExpr {
                left, op: Operator::Eq, right
            }) if is_partition_column(left) || is_partition_column(right) => {
                TableProviderFilterPushDown::Exact
            }
            // All other filters: let DataFusion handle them
            _ => TableProviderFilterPushDown::Unsupported,
        }
    }).collect())
}
# }
```

The three possible responses for each filter are:

- **`Exact`** -- Your source guarantees that no output rows will have a false
  value for this predicate. Because the filter is fully evaluated at the source,
  DataFusion will **not** add a `FilterExec` for it.
- **`Inexact`** -- Your source has the ability to reduce the data produced, but
  the output may still include rows that do not satisfy the predicate. For
  example, you might skip entire files based on metadata statistics but not
  filter individual rows within a file. DataFusion will still add a `FilterExec`
  above your scan to remove any remaining rows that slipped through.
- **`Unsupported`** -- Your source ignores this filter entirely. DataFusion
  handles it.

### Why Filter Pushdown Matters

Consider a table with 1 billion rows partitioned by `region`, and a query:

```sql
SELECT * FROM events WHERE region = 'us-east-1' AND event_type = 'click';
```

**Without filter pushdown:** Your table provider reads all 1 billion rows
across all regions. DataFusion then applies both filters, discarding the vast
majority of the data.

**With filter pushdown on `region`:** Your `scan()` method sees the
`region = 'us-east-1'` filter and constructs an execution plan that only reads
the `us-east-1` partition. If that partition holds 100 million rows, you have
just eliminated 90% of the I/O. DataFusion still applies the `event_type`
filter via `FilterExec` if you reported it as `Unsupported`.

### Only Push Down Filters When the Data Source Can Do Better

DataFusion already pushes filters as close to the data source as possible, typically placing them directly above the scan. `FilterExec` is also highly optimized, with vectorized evaluation and type-specialized kernels for fast predicate evaluation.

Because of this, you should only implement filter pushdown when your data source
can do strictly better -- for example, by avoiding I/O entirely through
skipping files or partitions based on metadata. If your data source cannot
eliminate I/O in this way, it is usually better to let DataFusion handle the
filter, as its in-memory execution is already highly efficient.

### Using EXPLAIN to Debug Your Table Provider

The `EXPLAIN` statement is your best tool for understanding what DataFusion is
actually doing with your table provider. It shows the physical plan that
DataFusion will execute, including any operators it inserted:

```sql
EXPLAIN SELECT * FROM events WHERE region = 'us-east-1' AND event_type = 'click';
```

If you are using DataFrames, call `.explain(false, false)` for the logical plan
or `.explain(false, true)` for the physical plan. You can also print the plans
in verbose mode with `.explain(true, true)`.

**Before filter pushdown**, the plan might look like:

```text
FilterExec: region@0 = us-east-1 AND event_type@1 = click
  MyExecPlan: partitions=50
```

Here DataFusion is reading all 50 partitions and filtering everything
afterward. The `FilterExec` above your scan is doing all the predicate work.

**After implementing pushdown for `region`** (reported as `Exact`):

```text
FilterExec: event_type@1 = click
  MyExecPlan: partitions=5, filter=[region = us-east-1]
```

Now your exec reads only the 5 partitions for `us-east-1`, and the remaining
`FilterExec` only handles the `event_type` predicate. The `region` filter has
been fully absorbed by your scan.

**After implementing pushdown for both filters** (both `Exact`):

```text
MyExecPlan: partitions=5, filter=[region = us-east-1 AND event_type = click]
```

No `FilterExec` at all -- your source handles everything.

Similarly, `EXPLAIN` will reveal whether DataFusion is inserting unnecessary
`SortExec` or `RepartitionExec` nodes that you could eliminate by declaring
better output properties. Whenever your queries seem slower than expected,
`EXPLAIN` is the first place to look.

### A Complete Filter Pushdown Example

To make filter pushdown concrete, here is an illustrative example. Imagine a
table provider that reads from a set of date-partitioned directories on disk
(e.g., `data/2026-03-01/`, `data/2026-03-02/`, ...). Each directory contains
one or more Parquet files for that date. By pushing down a filter on the `date`
column, the provider can skip entire directories -- avoiding the I/O of listing
and reading files that cannot possibly match the query.

```rust
# use std::any::Any;
# use std::collections::HashMap;
# use std::fmt;
# use std::sync::Arc;
# use arrow::datatypes::SchemaRef;
# use datafusion::catalog::{TableProvider, Session};
# use datafusion::common::Result;
# use datafusion::common::tree_node::TreeNodeRecursion;
# use datafusion::datasource::TableType;
# use datafusion::execution::SendableRecordBatchStream;
# use datafusion::execution::context::TaskContext;
# use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
# use datafusion::physical_expr::EquivalenceProperties;
# use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr, PlanProperties};
# use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
#
/// A table provider backed by date-partitioned directories.
/// Each date directory contains data files; by filtering on the
/// `date` column we can skip entire directories of I/O.
# #[derive(Debug)]
struct DatePartitionedTable {
    schema: SchemaRef,
    /// Maps date strings ("2026-03-01") to directory paths
    partitions: HashMap<String, String>,
}

#[async_trait::async_trait]
impl TableProvider for DatePartitionedTable {
    fn schema(&self) -> SchemaRef { Arc::clone(&self.schema) }
    fn table_type(&self) -> TableType { TableType::Base }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters.iter().map(|f| {
            if Self::is_date_equality_filter(f) {
                // We can fully evaluate this: we will only read
                // directories matching the date, so no rows with
                // a different date will appear in the output.
                TableProviderFilterPushDown::Exact
            } else {
                TableProviderFilterPushDown::Unsupported
            }
        }).collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Determine which date partitions to read by inspecting
        // the pushed-down filters. This is the key optimization:
        // we decide *during planning* which directories to scan,
        // so that execution never touches irrelevant data.
        let dates_to_read: Vec<String> = self
            .extract_date_values(filters)
            .unwrap_or_else(||
                self.partitions.keys().cloned().collect()
            );

        let dirs: Vec<String> = dates_to_read
            .iter()
            .filter_map(|d| self.partitions.get(d).cloned())
            .collect();
        let num_dirs = dirs.len();

        Ok(Arc::new(DatePartitionedExec {
            schema: Arc::clone(&self.schema),
            directories: dirs,
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(
                    Arc::clone(&self.schema),
                ),
                // One partition per date directory -- these
                // will be read in parallel.
                Partitioning::UnknownPartitioning(num_dirs),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        }))
    }
}

impl DatePartitionedTable {
    /// Check if a filter is an equality comparison on the `date` column.
    fn is_date_equality_filter(expr: &Expr) -> bool {
        // In practice, match on BinaryExpr { left, op: Eq, right }
        // and check if either side references the "date" column.
        // Simplified here for clarity.
        todo!("match on date equality expressions")
    }

    /// Extract date literal values from pushed-down equality filters.
    fn extract_date_values(&self, filters: &[Expr]) -> Option<Vec<String>> {
        // Parse filters like `date = '2026-03-01'` and return
        // the literal date strings. Returns None if no date
        // filters are present (meaning: read all partitions).
        todo!("extract date literals from filter expressions")
    }
}
#
# #[derive(Debug)]
# struct DatePartitionedExec {
#     schema: SchemaRef,
#     directories: Vec<String>,
#     properties: Arc<PlanProperties>,
# }
#
# impl DisplayAs for DatePartitionedExec {
#     fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
#         write!(f, "DatePartitionedExec")
#     }
# }
#
# impl ExecutionPlan for DatePartitionedExec {
#     fn name(&self) -> &str { "DatePartitionedExec" }
#     fn properties(&self) -> &Arc<PlanProperties> { &self.properties }
#     fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }
#     fn with_new_children(self: Arc<Self>, _: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> { Ok(self) }
#     fn execute(&self, _: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> { todo!() }
#     fn apply_expressions(&self, _f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>) -> Result<TreeNodeRecursion> { Ok(TreeNodeRecursion::Continue) }
# }
```

The key insight is that the filter pushdown decision (`supports_filters_pushdown`)
and the partition pruning (`scan()`) work together: the first tells DataFusion
that a `FilterExec` is unnecessary for the `date` predicate, and the second
ensures that only the relevant directories are scanned. The actual file reading
happens later, in the stream produced by `execute()`.

## Putting It All Together

Here is a minimal but complete example of a custom table provider that generates
data lazily during streaming:

```rust
use std::any::Any;
# use std::fmt;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::common::Result;
# use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::TableType;
use datafusion::catalog::Session;
use datafusion::execution::SendableRecordBatchStream;
# use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
#     DisplayAs, DisplayFormatType,
    ExecutionPlan, Partitioning,
#     PhysicalExpr,
    PlanProperties,
};
use futures::stream;

/// A table provider that generates sequential numbers on demand.
# #[derive(Debug)]
struct CountingTable {
    schema: SchemaRef,
    num_partitions: usize,
    rows_per_partition: usize,
}

impl CountingTable {
    fn new(num_partitions: usize, rows_per_partition: usize) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("partition", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        Self { schema, num_partitions, rows_per_partition }
    }
}

#[async_trait::async_trait]
impl TableProvider for CountingTable {
    fn schema(&self) -> SchemaRef { Arc::clone(&self.schema) }
    fn table_type(&self) -> TableType { TableType::Base }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Light work only: build the plan with metadata
        Ok(Arc::new(CountingExec {
            schema: Arc::clone(&self.schema),
            num_partitions: self.num_partitions,
            rows_per_partition: limit
                .unwrap_or(self.rows_per_partition)
                .min(self.rows_per_partition),
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&self.schema)),
                Partitioning::UnknownPartitioning(self.num_partitions),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        }))
    }
}

# #[derive(Debug)]
struct CountingExec {
    schema: SchemaRef,
    num_partitions: usize,
    rows_per_partition: usize,
    properties: Arc<PlanProperties>,
}

# impl DisplayAs for CountingExec {
#     fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
#         write!(f, "CountingExec: partitions={}", self.num_partitions)
#     }
# }
#
impl ExecutionPlan for CountingExec {
    fn name(&self) -> &str { "CountingExec" }
    fn properties(&self) -> &Arc<PlanProperties> { &self.properties }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = Arc::clone(&self.schema);
        let rows = self.rows_per_partition;

        // The heavy work (data generation) happens inside the stream,
        // not here in execute().
        let batch_stream = stream::once(async move {
            let partitions = Int64Array::from(
                vec![partition as i64; rows],
            );
            let values = Int64Array::from(
                (0..rows as i64).collect::<Vec<_>>(),
            );
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(partitions), Arc::new(values)],
            )?;
            Ok(batch)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            batch_stream,
        )))
    }

#     fn apply_expressions(
#         &self,
#         _f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
#     ) -> Result<TreeNodeRecursion> {
#         Ok(TreeNodeRecursion::Continue)
#     }
}
```

## Using Your Table Provider

Once you have implemented a `TableProvider`, register it with a `SessionContext`
to make it queryable:

```rust,ignore
use datafusion::execution::context::SessionContext;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    let provider = CountingTable::new(4, 1000);
    ctx.register_table("counting", Arc::new(provider))?;

    let df = ctx.sql("SELECT * FROM counting LIMIT 10").await?;
    df.show().await?;

    Ok(())
}
```

## Further Reading

- [`TableProvider` API docs](https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html)
- [`ExecutionPlan` API docs](https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html)
- [`SendableRecordBatchStream` API docs](https://docs.rs/datafusion/latest/datafusion/execution/type.SendableRecordBatchStream.html)
- [DataFusion examples directory](https://github.com/apache/datafusion/tree/main/datafusion-examples/examples) --
  contains working examples including custom table providers
