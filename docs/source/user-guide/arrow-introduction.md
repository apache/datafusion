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

# Introduction to `Arrow` & RecordBatches

```{contents}
:local:
:depth: 2
```

This guide helps DataFusion users understand [Arrow] and its RecordBatch format. While you may never need to work with Arrow directly, this knowledge becomes valuable when using DataFusion's extension points or debugging performance issues.

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

- **Unified Type System**: All data sources (CSV, Parquet, JSON) convert to the same Arrow types, enabling seamless cross-format queries
- **Vectorized Execution**: Process entire columns at once using SIMD instructions
- **Cache Efficiency**: Scanning specific columns doesn't load unnecessary data into CPU cache
- **Zero-Copy Data Sharing**: Systems can share Arrow data without conversion overhead

Arrow has become the universal standard for in-memory analytics precisely because of its **columnar format**—systems that natively store or process data in Arrow (DataFusion, Polars, InfluxDB 3.0), and runtimes that convert to Arrow for interchange (DuckDB, Spark, pandas), all organize data by column rather than by row. This cross-language, cross-platform adoption of the columnar model enables seamless data flow between systems with minimal conversion overhead.

Within this columnar design, Arrow's standard unit for packaging data is the **RecordBatch**—the key to making columnar format practical for real-world query engines.

## What is a RecordBatch? (And Why Batch?)

A **[`RecordBatch`]** cleverly combines the benefits of columnar storage with the practical need to process data in chunks. It represents a horizontal slice of a table, but critically, each column _within_ that slice remains a contiguous array.

Think of it as having two perspectives:

- **Columnar inside**: Each column (`id`, `name`, `age`) is a contiguous array optimized for vectorized operations
- **Row-oriented outside**: The batch represents a chunk of rows (e.g., rows 1-1000), making it a manageable unit for streaming

RecordBatches are **immutable snapshots**—once created, they cannot be modified. Any transformation produces a _new_ RecordBatch, enabling safe parallel processing without locks or coordination overhead.

This design allows DataFusion to process streams of row-based chunks while gaining maximum performance from the columnar layout. Let's see how this works in practice.

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
    // let df = ctx.read_json("data.ndjson", NdJsonReadOptions::default()).await?; // requires "json" feature; expects newline-delimited JSON
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

Note: You'll see [`Arc`] used frequently in the code—DataFusion's async architecture requires wrapping Arrow arrays in `Arc` (atomically reference-counted pointers) to safely share data across tasks. [`ArrayRef`] is simply a type alias for `Arc<dyn Array>`.

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

**Arrow Documentation:**

- [Arrow Format Introduction](https://arrow.apache.org/docs/format/Intro.html) - Official Arrow specification
- [Arrow Columnar Format](https://arrow.apache.org/docs/format/Columnar.html) - In-depth look at the memory layout

**DataFusion API References:**

- [RecordBatch](https://docs.rs/arrow-array/latest/arrow_array/struct.RecordBatch.html) - Core Arrow data structure
- [DataFrame](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html) - High-level query interface
- [TableProvider](https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html) - Custom data source trait
- [MemTable](https://docs.rs/datafusion/latest/datafusion/datasource/struct.MemTable.html) - In-memory table implementation

**Academic Paper:**

- [Apache Arrow DataFusion: A Fast, Embeddable, Modular Analytic Query Engine](https://dl.acm.org/doi/10.1145/3626246.3653368) - Published at SIGMOD 2024

[arrow]: https://arrow.apache.org/docs/index.html
[`Arc`]: https://doc.rust-lang.org/std/sync/struct.Arc.html
[`ArrayRef`]: https://docs.rs/arrow-array/latest/arrow_array/array/type.ArrayRef.html
[`Field`]: https://docs.rs/arrow-schema/latest/arrow_schema/struct.Field.html
[`Schema`]: https://docs.rs/arrow-schema/latest/arrow_schema/struct.Schema.html
[`DataType`]: https://docs.rs/arrow-schema/latest/arrow_schema/enum.DataType.html
[`Int32Array`]: https://docs.rs/arrow-array/latest/arrow_array/array/struct.Int32Array.html
[`StringArray`]: https://docs.rs/arrow-array/latest/arrow_array/array/struct.StringArray.html
[`Int32`]: https://docs.rs/arrow-schema/latest/arrow_schema/enum.DataType.html#variant.Int32
[`Int64`]: https://docs.rs/arrow-schema/latest/arrow_schema/enum.DataType.html#variant.Int64
[extension points]: ../library-user-guide/extensions.md
[`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html
[Custom Table Providers guide]: ../library-user-guide/custom-table-providers.md
[User-Defined Functions (UDFs)]: ../library-user-guide/functions/adding-udfs.md
[Custom Optimizer Rules and Operators]: ../library-user-guide/extending-operators.md
[`.register_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_table
[`.sql()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql
[`.show()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show
[`MemTable`]: https://docs.rs/datafusion/latest/datafusion/datasource/struct.MemTable.html
[`SessionContext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[`CsvReadOptions`]: https://docs.rs/datafusion/latest/datafusion/execution/options/struct.CsvReadOptions.html
[`ParquetReadOptions`]: https://docs.rs/datafusion/latest/datafusion/execution/options/struct.ParquetReadOptions.html
[`RecordBatch`]: https://docs.rs/arrow-array/latest/arrow_array/struct.RecordBatch.html
[`read_csv`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_csv
[`read_parquet`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_parquet
[`read_json`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_json
[`read_avro`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_avro
[`DataFrame`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`.collect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect
