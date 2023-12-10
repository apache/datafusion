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

## What is a DataFrame

`DataFrame` in `DataFrame` is modeled after the Pandas DataFrame interface, and is a thin wrapper over LogicalPlan that adds functionality for building and executing those plans.

```rust
pub struct DataFrame {
    session_state: SessionState,
    plan: LogicalPlan,
}
```

You can build up `DataFrame`s using its methods, similarly to building `LogicalPlan`s using `LogicalPlanBuilder`:

```rust
let df = ctx.table("users").await?;

// Create a new DataFrame sorted by  `id`, `bank_account`
let new_df = df.select(vec![col("id"), col("bank_account")])?
    .sort(vec![col("id")])?;

// Build the same plan using the LogicalPlanBuilder
let plan = LogicalPlanBuilder::from(&df.to_logical_plan())
    .project(vec![col("id"), col("bank_account")])?
    .sort(vec![col("id")])?
    .build()?;
```

You can use `collect` or `execute_stream` to execute the query.

## How to generate a DataFrame

You can directly use the `DataFrame` API or generate a `DataFrame` from a SQL query.

For example, to use `sql` to construct `DataFrame`:

```rust
let ctx = SessionContext::new();
// Register the in-memory table containing the data
ctx.register_table("users", Arc::new(create_memtable()?))?;
let dataframe = ctx.sql("SELECT * FROM users;").await?;
```

To construct `DataFrame` using the API:

```rust
let ctx = SessionContext::new();
// Register the in-memory table containing the data
ctx.register_table("users", Arc::new(create_memtable()?))?;
let dataframe = ctx
  .table("users")
  .filter(col("a").lt_eq(col("b")))?
  .sort(vec![col("a").sort(true, true), col("b").sort(false, false)])?;
```

## Collect / Streaming Exec

DataFusion `DataFrame`s are "lazy", meaning they do not do any processing until they are executed, which allows for additional optimizations.

When you have a `DataFrame`, you can run it in one of three ways:

1.  `collect` which executes the query and buffers all the output into a `Vec<RecordBatch>`
2.  `streaming_exec`, which begins executions and returns a `SendableRecordBatchStream` which incrementally computes output on each call to `next()`
3.  `cache` which executes the query and buffers the output into a new in memory DataFrame.

You can just collect all outputs once like:

```rust
let ctx = SessionContext::new();
let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
let batches = df.collect().await?;
```

You can also use stream output to incrementally generate output one `RecordBatch` at a time

```rust
let ctx = SessionContext::new();
let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
let mut stream = df.execute_stream().await?;
while let Some(rb) = stream.next().await {
    println!("{rb:?}");
}
```

# Write DataFrame to Files

You can also serialize `DataFrame` to a file. For now, `Datafusion` supports write `DataFrame` to `csv`, `json` and `parquet`.

When writing a file, DataFusion will execute the DataFrame and stream the results to a file.

For example, to write a csv_file

```rust
let ctx = SessionContext::new();
// Register the in-memory table containing the data
ctx.register_table("users", Arc::new(mem_table))?;
let dataframe = ctx.sql("SELECT * FROM users;").await?;

dataframe
    .write_csv("user_dataframe.csv", DataFrameWriteOptions::default(), None)
    .await;
```

and the file will look like (Example Output):

```
id,bank_account
1,9000
```

## Transform between LogicalPlan and DataFrame

As shown above, `DataFrame` is just a very thin wrapper of `LogicalPlan`, so you can easily go back and forth between them.

```rust
// Just combine LogicalPlan with SessionContext and you get a DataFrame
let ctx = SessionContext::new();
// Register the in-memory table containing the data
ctx.register_table("users", Arc::new(mem_table))?;
let dataframe = ctx.sql("SELECT * FROM users;").await?;

// get LogicalPlan in dataframe
let plan = dataframe.logical_plan().clone();

// construct a DataFrame with LogicalPlan
let new_df = DataFrame::new(ctx.state(), plan);
```
