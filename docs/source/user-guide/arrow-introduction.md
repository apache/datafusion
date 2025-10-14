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

# A Gentle Introduction to Arrow & RecordBatches (for DataFusion users)

```{contents}
:local:
:depth: 2
```

This guide helps DataFusion users understand Arrow and its RecordBatch format. While you may never need to work with Arrow directly, this knowledge becomes valuable when using DataFusion's extension points or debugging performance issues.

**Why Arrow is central to DataFusion**: Arrow provides the unified type system that makes DataFusion possible. When you query a CSV file, join it with a Parquet file, and aggregate results from JSON—it all works seamlessly because every data source is converted to Arrow's common representation. This unified type system, combined with Arrow's columnar format, enables DataFusion to execute efficient vectorized operations across any combination of data sources while benefiting from zero-copy data sharing between query operators.

## Why Columnar? The Arrow Advantage

Apache Arrow is an open **specification** that defines how analytical data should be organized in memory. Think of it as a blueprint that different systems agree to follow, not a database or programming language.

### Row-oriented vs Columnar Layout

Traditional databases often store data row-by-row:

```
Row 1: [id: 1, name: "Alice", age: 30]
Row 2: [id: 2, name: "Bob",   age: 25]
Row 3: [id: 3, name: "Carol", age: 35]
```

Arrow organizes the same data by column:

```
Column "id":   [1, 2, 3]
Column "name": ["Alice", "Bob", "Carol"]
Column "age":  [30, 25, 35]
```

Visual comparison:

```
Traditional Row Storage:          Arrow Columnar Storage:
┌──────────────────┐              ┌─────────┬─────────┬──────────┐
│ id │ name │ age  │              │   id    │  name   │   age    │
├────┼──────┼──────┤              ├─────────┼─────────┼──────────┤
│ 1  │  A   │  30  │              │ [1,2,3] │ [A,B,C] │[30,25,35]│
│ 2  │  B   │  25  │              └─────────┴─────────┴──────────┘
│ 3  │  C   │  35  │                   ↑          ↑         ↑
└──────────────────┘              Int32Array StringArray Int32Array
(read entire rows)                (process entire columns at once)
```

### Why This Matters

- **Vectorized Execution**: Process entire columns at once using SIMD instructions
- **Better Compression**: Similar values stored together compress more efficiently
- **Cache Efficiency**: Scanning specific columns doesn't load unnecessary data
- **Zero-Copy Data Sharing**: Systems can share Arrow data without conversion overhead

DataFusion, DuckDB, Polars, and Pandas all speak Arrow natively—they can exchange data without expensive serialization/deserialization steps.

## What is a RecordBatch? (And Why Batch?)

A **[`RecordBatch`]** represents a horizontal slice of a table—a collection of equal-length columnar arrays sharing the same schema.

### Why Not Process Entire Tables?

- **Memory Constraints**: A billion-row table might not fit in RAM
- **Pipeline Processing**: Start producing results before reading all data
- **Parallel Execution**: Different threads can process different batches

### Why Not Process Single Rows?

- **Lost Vectorization**: Can't use SIMD instructions on single values
- **Poor Cache Utilization**: Jumping between rows defeats CPU cache optimization
- **High Overhead**: Managing individual rows has significant bookkeeping costs

### RecordBatches: The Sweet Spot

RecordBatches typically contain thousands of rows—enough to benefit from vectorization but small enough to fit in memory. DataFusion streams these batches through operators, achieving both efficiency and scalability.

**Key Properties**:

- Arrays are immutable (create new batches to modify data)
- NULL values tracked via efficient validity bitmaps
- Variable-length data (strings, lists) use offset arrays for efficient access

## From files to Arrow

When you call [`read_csv`], [`read_parquet`], [`read_json`] or [`read_avro`], DataFusion decodes those formats into Arrow arrays and streams them to operators as RecordBatches.

The example below shows how to read data from different file formats. Each `read_*` method returns a [`DataFrame`] that represents a query plan. When you call [`.collect()`], DataFusion executes the plan and returns results as a `Vec<RecordBatch>`—the actual columnar data in Arrow format.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Pick ONE of these per run (each returns a new DataFrame):
    let df = ctx.read_csv("data.csv", CsvReadOptions::new()).await?;
    // let df = ctx.read_parquet("data.parquet", ParquetReadOptions::default()).await?;
    // let df = ctx.read_json("data.ndjson", NdJsonReadOptions::default()).await?; // requires "json" feature
    // let df = ctx.read_avro("data.avro", AvroReadOptions::default()).await?;     // requires "avro" feature

    let batches = df
        .select(vec![col("id")])?
        .filter(col("id").gt(lit(10)))?
        .collect()
        .await?; // Vec<RecordBatch>

    Ok(())
}
```

## Streaming Through the Engine

DataFusion processes queries as pull-based pipelines where operators request batches from their inputs. This streaming approach enables early result production, bounds memory usage (spilling to disk only when necessary), and naturally supports parallel execution across multiple CPU cores.

```
A user's query: SELECT name FROM 'data.parquet' WHERE id > 10

The DataFusion Pipeline:
┌─────────────┐    ┌──────────────┐    ┌────────────────┐    ┌──────────────────┐    ┌──────────┐
│ Parquet     │───▶│ Scan         │───▶│ Filter         │───▶│ Projection       │───▶│ Results  │
│ File        │    │ Operator     │    │ Operator       │    │ Operator         │    │          │
└─────────────┘    └──────────────┘    └────────────────┘    └──────────────────┘    └──────────┘
                   (reads data)        (id > 10)             (keeps "name" col)
                   RecordBatch ───▶    RecordBatch ────▶     RecordBatch ────▶        RecordBatch
```

In this pipeline, [`RecordBatch`]es are the "packages" of columnar data that flow between the different stages of query execution. Each operator processes batches incrementally, enabling the system to produce results before reading the entire input.

## Minimal: build a RecordBatch in Rust

Sometimes you need to create Arrow data programmatically rather than reading from files. This example shows the core building blocks: creating typed arrays (like [`Int32Array`] for numbers), defining a [`Schema`] that describes your columns, and assembling them into a [`RecordBatch`].

You'll notice [`Arc`] ([Atomically Reference Counted](https://doc.rust-lang.org/std/sync/struct.Arc.html)) is used frequently—this is how Arrow enables efficient, zero-copy data sharing. Instead of copying data, different parts of the query engine can safely share read-only references to the same underlying memory. [`ArrayRef`] is simply a type alias for `Arc<dyn Array>`, representing a reference to any Arrow array type.

Notice how nullable columns can contain `None` values, tracked efficiently by Arrow's internal validity bitmap.

```rust
use std::sync::Arc;
use arrow_array::{ArrayRef, Int32Array, StringArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};

fn make_batch() -> arrow_schema::Result<RecordBatch> {
    let ids = Int32Array::from(vec![1, 2, 3]);
    let names = StringArray::from(vec![Some("alice"), None, Some("carol")]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let cols: Vec<ArrayRef> = vec![Arc::new(ids), Arc::new(names)];
    RecordBatch::try_new(schema, cols)
}
```

## Query an in-memory batch with DataFusion

Once you have a [`RecordBatch`], you can query it with DataFusion using a [`MemTable`]. This is useful for testing, processing data from external systems, or combining in-memory data with other sources. The example below creates a batch, wraps it in a [`MemTable`], registers it as a named table, and queries it using SQL—demonstrating how Arrow serves as the bridge between your data and DataFusion's query engine.

```rust
use std::sync::Arc;
use arrow_array::{Int32Array, StringArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // build a batch
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])) as _,
            Arc::new(StringArray::from(vec![Some("foo"), Some("bar"), None])) as _,
        ],
    )?;

    // expose it as a table
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("people", Arc::new(table))?;

    // query it
    let df = ctx.sql("SELECT id, upper(name) AS name FROM people WHERE id >= 2").await?;
    df.show().await?;
    Ok(())
}
```

## Common Pitfalls

When working with Arrow and RecordBatches, watch out for these common issues:

- **Schema consistency**: All batches in a stream must share the exact same [`Schema`]. For example, you can't have one batch where a column is [`Int32`] and the next where it's [`Int64`], even if the values would fit
- **Immutability**: Arrays are immutable—to "modify" data, you must build new arrays or new RecordBatches. For instance, to change a value in an array, you'd create a new array with the updated value
- **Buffer management**: Variable-length types (UTF-8, binary, lists) use offsets + values arrays internally. Avoid manual buffer slicing unless you understand Arrow's internal invariants—use Arrow's built-in compute functions instead
- **Type mismatches**: Mixed input types across files may require explicit casts. For example, a string column `"123"` from a CSV file won't automatically join with an integer column `123` from a Parquet file—you'll need to cast one to match the other
- **Batch size assumptions**: Don't assume a particular batch size; always iterate until the stream ends. One file might produce 8192-row batches while another produces 1024-row batches

## When Arrow knowledge is needed (Extension Points)

For many use cases, you don't need to know about Arrow. DataFusion handles the conversion from formats like CSV and Parquet for you. However, Arrow becomes important when you use DataFusion's **[extension points]** to add your own custom functionality.

These APIs are where you can plug your own code into the engine, and they often operate directly on Arrow [`RecordBatch`] streams.

- **[`TableProvider`] (Custom Data Sources)**: This is the most common extension point. You can teach DataFusion how to read from any source—a custom file format, a network API, a different database—by implementing the [`TableProvider`] trait. Your implementation will be responsible for creating [`RecordBatch`]es to stream data into the engine. See the [Custom Table Providers guide] for detailed examples.

- **[User-Defined Functions (UDFs)]**: If you need to perform a custom transformation on your data that isn't built into DataFusion, you can write a UDF. Your function will receive data as Arrow arrays (inside a [`RecordBatch`]) and must produce an Arrow array as its output.

- **[Custom Optimizer Rules and Operators]**: For advanced use cases, you can even add your own rules to the query optimizer or implement entirely new physical operators (like a special type of join). These also operate on the Arrow-based query plans.

In short, knowing Arrow is key to unlocking the full power of DataFusion's modular and extensible architecture.

## Next Steps: Working with DataFrames

Now that you understand Arrow's RecordBatch format, you're ready to work with DataFusion's high-level APIs. The [DataFrame API](dataframe.md) provides a familiar, ergonomic interface for building queries without needing to think about Arrow internals most of the time.

The DataFrame API handles all the Arrow details under the hood - reading files into RecordBatches, applying transformations, and producing results. You only need to drop down to the Arrow level when implementing custom data sources, UDFs, or other extension points.

**Recommended reading order:**

1. [DataFrame API](dataframe.md) - High-level query building interface
2. [Library User Guide: DataFrame API](../library-user-guide/using-the-dataframe-api.md) - Detailed examples and patterns
3. [Custom Table Providers](../library-user-guide/custom-table-providers.md) - When you need Arrow knowledge

## Further reading

- [Arrow introduction](https://arrow.apache.org/docs/format/Intro.html)
- [Arrow columnar format (overview)](https://arrow.apache.org/docs/format/Columnar.html)
- [Arrow IPC format (files and streams)](https://arrow.apache.org/docs/format/IPC.html)
- [arrow_array::RecordBatch (docs.rs)](https://docs.rs/arrow-array/latest/arrow_array/struct.RecordBatch.html)
- [Apache Arrow DataFusion: A Fast, Embeddable, Modular Analytic Query Engine (Paper)](https://dl.acm.org/doi/10.1145/3626246.3653368)

- DataFusion + Arrow integration (docs.rs):
  - [datafusion::common::arrow](https://docs.rs/datafusion/latest/datafusion/common/arrow/index.html)
  - [datafusion::common::arrow::array](https://docs.rs/datafusion/latest/datafusion/common/arrow/array/index.html)
  - [datafusion::common::arrow::compute](https://docs.rs/datafusion/latest/datafusion/common/arrow/compute/index.html)
  - [SessionContext::read_csv](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_csv)
  - [read_parquet](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_parquet)
  - [read_json](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_json)
  - [DataFrame::collect](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect)
  - [SendableRecordBatchStream](https://docs.rs/datafusion/latest/datafusion/physical_plan/type.SendableRecordBatchStream.html)
  - [TableProvider](https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html)
  - [MemTable](https://docs.rs/datafusion/latest/datafusion/datasource/struct.MemTable.html)
- Deep dive (memory layout internals): [ArrayData on docs.rs](https://docs.rs/datafusion/latest/datafusion/common/arrow/array/struct.ArrayData.html)
- Parquet format and pushdown: [Parquet format](https://parquet.apache.org/docs/file-format/), [Row group filtering / predicate pushdown](https://arrow.apache.org/docs/cpp/parquet.html#row-group-filtering)
- For DataFusion contributors: [DataFusion Invariants](../contributor-guide/specification/invariants.md) - How DataFusion maintains type safety and consistency with Arrow's dynamic type system

[`arc`]: https://doc.rust-lang.org/std/sync/struct.Arc.html
[`arrayref`]: https://docs.rs/arrow-array/latest/arrow_array/array/type.ArrayRef.html
[`field`]: https://docs.rs/arrow-schema/latest/arrow_schema/struct.Field.html
[`schema`]: https://docs.rs/arrow-schema/latest/arrow_schema/struct.Schema.html
[`datatype`]: https://docs.rs/arrow-schema/latest/arrow_schema/enum.DataType.html
[`int32array`]: https://docs.rs/arrow-array/latest/arrow_array/array/struct.Int32Array.html
[`stringarray`]: https://docs.rs/arrow-array/latest/arrow_array/array/struct.StringArray.html
[`int32`]: https://docs.rs/arrow-schema/latest/arrow_schema/enum.DataType.html#variant.Int32
[`int64`]: https://docs.rs/arrow-schema/latest/arrow_schema/enum.DataType.html#variant.Int64
[ extension points]: ../library-user-guide/extensions.md
[`tableprovider`]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html
[custom table providers guide]: ../library-user-guide/custom-table-providers.md
[user-defined functions (udfs)]: ../library-user-guide/functions/adding-udfs.md
[custom optimizer rules and operators]: ../library-user-guide/extending-operators.md
[`.register_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_table
[`.sql()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql
[`.show()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show
[`memtable`]: https://docs.rs/datafusion/latest/datafusion/datasource/struct.MemTable.html
[`sessioncontext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[`csvreadoptions`]: https://docs.rs/datafusion/latest/datafusion/execution/options/struct.CsvReadOptions.html
[`parquetreadoptions`]: https://docs.rs/datafusion/latest/datafusion/execution/options/struct.ParquetReadOptions.html
[`recordbatch`]: https://docs.rs/arrow-array/latest/arrow_array/struct.RecordBatch.html
[`read_csv`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_csv
[`read_parquet`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_parquet
[`read_json`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_json
[`read_avro`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_avro
[`dataframe`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`.collect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect
