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

# Gentle Arrow Introduction

```{contents}
:local:
:depth: 2
```

## Overview

DataFusion uses [Apache Arrow] as its native in-memory format, so anyone using DataFusion will likely interact with Arrow at some point. This guide introduces the key Arrow concepts you need to know to effectively use DataFusion.

Apache Arrow defines a standardized columnar representation for in-memory data. This enables different systems and languages (e.g., Rust and Python) to share data with zero-copy interchange, avoiding serialization overhead. In addition to zero copy interchange, Arrow also standardizes best practice columnar data representation enabling high performance analytical processing through vectorized execution.

## Columnar Layout

Quick visual: row-major (left) vs Arrow's columnar layout (right). For a deeper primer, see the [arrow2 guide].

```text
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

## `RecordBatch`

Arrow's standard unit for packaging data is the **[`RecordBatch`]**.

A **[`RecordBatch`]** represents a horizontal slice of a table—a collection of equal-length columnar arrays that conform to a defined schema. Each column within the slice is a contiguous Arrow array, and all columns have the same number of rows (length). This chunked, immutable unit enables efficient streaming and parallel execution.

Think of it as having two perspectives:

- **Columnar inside**: Each column (`id`, `name`, `age`) is a contiguous array optimized for vectorized operations
- **Row-chunked externally**: The batch represents a chunk of rows (e.g., rows 1-1000), making it a manageable unit for streaming

RecordBatches are **immutable snapshots**—once created, they cannot be modified. Any transformation produces a _new_ RecordBatch, enabling safe parallel processing without locks or coordination overhead.

This design allows DataFusion to process streams of row-based chunks while gaining maximum performance from the columnar layout.

## Streaming Through the Engine

DataFusion processes queries as pull-based pipelines where operators request batches from their inputs. This streaming approach enables early result production, bounds memory usage (spilling to disk only when necessary), and naturally supports parallel execution across multiple CPU cores.

For example, given the following query:

```sql
SELECT name FROM 'data.parquet' WHERE id > 10
```

The DataFusion Pipeline looks like this:

```text

┌─────────────┐    ┌──────────────┐    ┌────────────────┐    ┌──────────────────┐    ┌──────────┐
│ Parquet     │───▶│ Scan         │───▶│ Filter         │───▶│ Projection       │───▶│ Results  │
│ File        │    │ Operator     │    │ Operator       │    │ Operator         │    │          │
└─────────────┘    └──────────────┘    └────────────────┘    └──────────────────┘    └──────────┘
                   (reads data)        (id > 10)             (keeps "name" col)
                   RecordBatch ───▶    RecordBatch ────▶     RecordBatch ────▶        RecordBatch
```

In this pipeline, [`RecordBatch`]es are the "packages" of columnar data that flow between the different stages of query execution. Each operator processes batches incrementally, enabling the system to produce results before reading the entire input.

## Creating `ArrayRef` and `RecordBatch`es

Sometimes you need to create Arrow data programmatically rather than reading from files.

The first thing needed is creating an Arrow Array, for each column. [arrow-rs] provides array builders and `From` impls to create arrays from Rust vectors.

```rust
use arrow::array::{StringArray, Int32Array};
// Create an Int32Array from a vector of i32 values
let ids = Int32Array::from(vec![1, 2, 3]);
// There are similar constructors for other array types, e.g., StringArray, Float64Array, etc.
let names = StringArray::from(vec![Some("alice"), None, Some("carol")]);
```

Every element in an Arrow array can be "null" (aka missing). Often, arrays are
created from `Option<T>` values to indicate nullability (e.g., `Some("alice")`
vs `None` above).

Note: You'll see [`Arc`] used frequently in the code—Arrow arrays are wrapped in
[`Arc`] (atomically reference-counted pointers) to enable cheap, thread-safe
sharing across operators and tasks. [`ArrayRef`] is simply a type alias for
`Arc<dyn Array>`. To create an `ArrayRef`, wrap your array in `Arc::new(...)` as shown below.

```rust
use std::sync::Arc;
# use arrow::array::{ArrayRef, Int32Array, StringArray};
// To get an ArrayRef, wrap the Int32Array in an Arc.
// (note you will often have to explicitly type annotate to ArrayRef)
let arr: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));

// you can also store Strings and other types in ArrayRefs
let arr: ArrayRef = Arc::new(
  StringArray::from(vec![Some("alice"), None, Some("carol")])
);
```

To create a [`RecordBatch`], you need to define its [`Schema`] (the column names and types) and provide the corresponding columns as [`ArrayRef`]s as shown below:

```rust
# use std::sync::Arc;
# use arrow_schema::ArrowError;
# use arrow::array::{ArrayRef, Int32Array, StringArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};

// Create the columns as Arrow arrays
let ids = Int32Array::from(vec![1, 2, 3]);
let names = StringArray::from(vec![Some("alice"), None, Some("carol")]);
// Create the schema
let schema = Arc::new(Schema::new(vec![
    Field::new("id", DataType::Int32, false), // false means non-nullable
    Field::new("name", DataType::Utf8, true), // true means nullable
]));
// Assemble the columns
let cols: Vec<ArrayRef> = vec![
      Arc::new(ids),
      Arc::new(names)
];
// Finally, create the RecordBatch
RecordBatch::try_new(schema, cols).expect("Failed to create RecordBatch");
```

## Working with `ArrayRef` and `RecordBatch`

Most DataFusion APIs are in terms of [`ArrayRef`] and [`RecordBatch`]. To work with the
underlying data, you typically downcast the [`ArrayRef`] to its concrete type
(e.g., [`Int32Array`]).

To do so either use the `as_any().downcast_ref::<T>()` method or the
`as_::<T>()` helper method from the [AsArray] trait.

[asarray]: https://docs.rs/arrow-array/latest/arrow_array/cast/trait.AsArray.html

```rust
# use std::sync::Arc;
# use arrow::datatypes::{DataType, Int32Type};
# use arrow::array::{AsArray, ArrayRef, Int32Array, RecordBatch};
# let arr: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
// First check the data type of the array
match arr.data_type() {
   &DataType::Int32 => {
         // Downcast to Int32Array
         let int_array = arr.as_primitive::<Int32Type>();
         // Now you can access Int32Array methods
         for i in 0..int_array.len() {
              println!("Value at index {}: {}", i, int_array.value(i));
         }
   }
    _ => {
        println ! ("Array is not of type Int32");
    }
}
```

The following two downcasting methods are equivalent:

```rust
# use std::sync::Arc;
# use arrow::datatypes::{DataType, Int32Type};
# use arrow::array::{AsArray, ArrayRef, Int32Array, RecordBatch};
# let arr: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
// Downcast to Int32Array using as_any
let int_array1 = arr.as_any().downcast_ref::<Int32Array>().unwrap();
// This is the same as using the as_::<T>() helper
let int_array2 = arr.as_primitive::<Int32Type>();
assert_eq!(int_array1, int_array2);
```

## Common Pitfalls

When working with Arrow and RecordBatches, watch out for these common issues:

- **Schema consistency**: All batches in a stream must share the exact same [`Schema`]. For example, you can't have one batch where a column is [`Int32`] and the next where it's [`Int64`], even if the values would fit
- **Immutability**: Arrays are immutable—to "modify" data, you must build new arrays or new RecordBatches. For instance, to change a value in an array, you'd create a new array with the updated value
- **Row by Row Processing**: Avoid iterating over Arrays element by element when possible, and use Arrow's built-in [compute kernels] instead
- **Type mismatches**: Mixed input types across files may require explicit casts. For example, a string column `"123"` from a CSV file won't automatically join with an integer column `123` from a Parquet file—you'll need to cast one to match the other. Use Arrow's [`cast`] kernel where appropriate
- **Batch size assumptions**: Don't assume a particular batch size; always iterate until the stream ends. One file might produce 8192-row batches while another produces 1024-row batches

[compute kernels]: https://docs.rs/arrow/latest/arrow/compute/index.html

## Further reading

**Arrow Documentation:**

- [Arrow Format Introduction](https://arrow.apache.org/docs/format/Intro.html) - Understand the Arrow specification and why it enables zero-copy data sharing
- [Arrow Columnar Format](https://arrow.apache.org/docs/format/Columnar.html) - Deep dive into memory layout for performance optimization
- [Arrow Rust Documentation](https://docs.rs/arrow/latest/arrow/) - Complete API reference for the Rust implementation

**Key API References:**

- [RecordBatch](https://docs.rs/arrow-array/latest/arrow_array/struct.RecordBatch.html) - The fundamental data structure for columnar data (a table slice)
- [ArrayRef](https://docs.rs/arrow-array/latest/arrow_array/array/type.ArrayRef.html) - Represents a reference-counted Arrow array (single column)
- [DataType](https://docs.rs/arrow-schema/latest/arrow_schema/enum.DataType.html) - Enum of all supported Arrow data types (e.g., Int32, Utf8)
- [Schema](https://docs.rs/arrow-schema/latest/arrow_schema/struct.Schema.html) - Describes the structure of a RecordBatch (column names and types)

[apache arrow]: https://arrow.apache.org/docs/index.html
[`arc`]: https://doc.rust-lang.org/std/sync/struct.Arc.html
[`arrayref`]: https://docs.rs/arrow-array/latest/arrow_array/array/type.ArrayRef.html
[`cast`]: https://docs.rs/arrow/latest/arrow/compute/fn.cast.html
[`field`]: https://docs.rs/arrow-schema/latest/arrow_schema/struct.Field.html
[`schema`]: https://docs.rs/arrow-schema/latest/arrow_schema/struct.Schema.html
[`datatype`]: https://docs.rs/arrow-schema/latest/arrow_schema/enum.DataType.html
[`int32array`]: https://docs.rs/arrow/latest/arrow/array/type.Int32Array.html
[`stringarray`]: https://docs.rs/arrow/latest/arrow/array/type.StringArray.html
[`int32`]: https://docs.rs/arrow-schema/latest/arrow_schema/enum.DataType.html#variant.Int32
[`int64`]: https://docs.rs/arrow-schema/latest/arrow_schema/enum.DataType.html#variant.Int64
[extension points]: ../library-user-guide/extensions.md
[`tableprovider`]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html
[custom table providers guide]: ../library-user-guide/custom-table-providers.md
[user-defined functions (udfs)]: ../library-user-guide/functions/adding-udfs.md
[custom optimizer rules and physical operators]: ../library-user-guide/extending-operators.md
[`executionplan`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html
[`.register_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_table
[`.sql()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql
[`.show()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show
[`memtable`]: https://docs.rs/datafusion/latest/datafusion/datasource/struct.MemTable.html
[`sessioncontext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[`csvreadoptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html
[`parquetreadoptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html
[`recordbatch`]: https://docs.rs/arrow-array/latest/arrow_array/struct.RecordBatch.html
[`read_csv`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_csv
[`read_parquet`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_parquet
[`read_json`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_json
[`read_avro`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_avro
[`dataframe`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`.collect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect
[arrow2 guide]: https://jorgecarleitao.github.io/arrow2/main/guide/arrow.html#what-is-apache-arrow
[configuration settings]: configs.md
[`datafusion.execution.batch_size`]: configs.md#setting-configuration-options
