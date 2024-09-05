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

# Using the DataFrame API

The [Users Guide] introduces the [`DataFrame`] API and this section describes
that API in more depth.

## What is a DataFrame?

As described in the [Users Guide], DataFusion [`DataFrame`]s are modeled after
the [Pandas DataFrame] interface, and are implemented as thin wrapper over a
[`LogicalPlan`] that adds functionality for building and executing those plans.

The simplest possible dataframe is one that scans a table and that table can be
in a file or in memory.

## How to generate a DataFrame

You can construct [`DataFrame`]s programmatically using the API, similarly to
other DataFrame APIs. For example, you can read an in memory `RecordBatch` into
a `DataFrame`:

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::array::{ArrayRef, Int32Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    // Register an in-memory table containing the following data
    // id | bank_account
    // ---|-------------
    // 1  | 9000
    // 2  | 8000
    // 3  | 7000
    let data = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
        ("bank_account", Arc::new(Int32Array::from(vec![9000, 8000, 7000]))),
    ])?;
    // Create a DataFrame that scans the user table, and finds
    // all users with a bank account at least 8000
    // and sorts the results by bank account in descending order
    let dataframe = ctx
        .read_batch(data)?
        .filter(col("bank_account").gt_eq(lit(8000)))? // bank_account >= 8000
        .sort(vec![col("bank_account").sort(false, true)])?; // ORDER BY bank_account DESC

    Ok(())
}
```

You can _also_ generate a `DataFrame` from a SQL query and use the DataFrame's APIs
to manipulate the output of the query.

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::assert_batches_eq;
use datafusion::arrow::array::{ArrayRef, Int32Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    // Register the same in-memory table as the previous example
    let data = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
        ("bank_account", Arc::new(Int32Array::from(vec![9000, 8000, 7000]))),
    ])?;
    ctx.register_batch("users", data)?;
    // Create a DataFrame using SQL
    let dataframe = ctx.sql("SELECT * FROM users;")
        .await?
        // Note we can filter the output of the query using the DataFrame API
        .filter(col("bank_account").gt_eq(lit(8000)))?; // bank_account >= 8000

    let results = &dataframe.collect().await?;

    // use the `assert_batches_eq` macro to show the output
    assert_batches_eq!(
        vec![
            "+----+--------------+",
            "| id | bank_account |",
            "+----+--------------+",
            "| 1  | 9000         |",
            "| 2  | 8000         |",
            "+----+--------------+",
        ],
        &results
    );
    Ok(())
}
```

## Collect / Streaming Exec

DataFusion [`DataFrame`]s are "lazy", meaning they do no processing until
they are executed, which allows for additional optimizations.

You can run a `DataFrame` in one of three ways:

1.  `collect`: executes the query and buffers all the output into a `Vec<RecordBatch>`
2.  `execute_stream`: begins executions and returns a `SendableRecordBatchStream` which incrementally computes output on each call to `next()`
3.  `cache`: executes the query and buffers the output into a new in memory `DataFrame.`

To collect all outputs into a memory buffer, use the `collect` method:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    // read the contents of a CSV file into a DataFrame
    let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    // execute the query and collect the results as a Vec<RecordBatch>
    let batches = df.collect().await?;
    for record_batch in batches {
        println!("{record_batch:?}");
    }
    Ok(())
}
```

Use `execute_stream` to incrementally generate output one `RecordBatch` at a time:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use futures::stream::StreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    // read example.csv file into a DataFrame
    let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    // begin execution (returns quickly, does not compute results)
    let mut stream = df.execute_stream().await?;
    // results are returned incrementally as they are computed
    while let Some(record_batch) = stream.next().await {
        println!("{record_batch:?}");
    }
    Ok(())
}
```

# Write DataFrame to Files

You can also write the contents of a `DataFrame` to a file. When writing a file,
DataFusion executes the `DataFrame` and streams the results to the output.
DataFusion comes with support for writing `csv`, `json` `arrow` `avro`, and
`parquet` files, and supports writing custom file formats via API (see
[`custom_file_format.rs`] for an example)

For example, to read a CSV file and write it to a parquet file, use the
[`DataFrame::write_parquet`] method

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::dataframe::DataFrameWriteOptions;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    // read example.csv file into a DataFrame
    let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    // stream the contents of the DataFrame to the `example.parquet` file
    let target_path = tempfile::tempdir()?.path().join("example.parquet");
    df.write_parquet(
        target_path.to_str().unwrap(),
        DataFrameWriteOptions::new(),
        None, // writer_options
    ).await;
    Ok(())
}
```

[`custom_file_format.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/custom_file_format.rs

The output file will look like (Example Output):

```sql
> select * from '../datafusion/core/example.parquet';
+---+---+---+
| a | b | c |
+---+---+---+
| 1 | 2 | 3 |
+---+---+---+
```

## Relationship between `LogicalPlan`s and `DataFrame`s

The `DataFrame` struct is defined like this:

```rust
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::LogicalPlan;
pub struct DataFrame {
    // state required to execute a LogicalPlan
    session_state: Box<SessionState>,
    // LogicalPlan that describes the computation to perform
    plan: LogicalPlan,
}
```

As shown above, `DataFrame` is a thin wrapper of `LogicalPlan`, so you can
easily go back and forth between them.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::logical_expr::LogicalPlanBuilder;

#[tokio::main]
async fn main() -> Result<()>{
    let ctx = SessionContext::new();
    // read example.csv file into a DataFrame
    let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    // You can easily get the LogicalPlan from the DataFrame
    let (_state, plan) = df.into_parts();
    // Just combine LogicalPlan with SessionContext and you get a DataFrame
    // get LogicalPlan in dataframe
    let new_df = DataFrame::new(ctx.state(), plan);
    Ok(())
}
```

In fact, using the [`DataFrame`]s methods you can create the same
[`LogicalPlan`]s as when using [`LogicalPlanBuilder`]:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::logical_expr::LogicalPlanBuilder;

#[tokio::main]
async fn main() -> Result<()>{
    let ctx = SessionContext::new();
    // read example.csv file into a DataFrame
    let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    // Create a new DataFrame sorted by  `id`, `bank_account`
    let new_df = df.select(vec![col("a"), col("b")])?
        .sort_by(vec![col("a")])?;
    // Build the same plan using the LogicalPlanBuilder
    // Similar to `SELECT a, b FROM example.csv ORDER BY a`
    let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    let (_state, plan) = df.into_parts(); // get the DataFrame's LogicalPlan
    let plan = LogicalPlanBuilder::from(plan)
        .project(vec![col("a"), col("b")])?
        .sort_by(vec![col("a")])?
        .build()?;
    // prove they are the same
    assert_eq!(new_df.logical_plan(), &plan);
    Ok(())
}
```

[users guide]: ../user-guide/dataframe.md
[pandas dataframe]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
[`dataframe`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`logicalplan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[`logicalplanbuilder`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.LogicalPlanBuilder.html
[`dataframe::write_parquet`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_parquet
