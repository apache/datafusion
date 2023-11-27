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

`DataFrame` is a basic concept in `datafusion` and is only a thin wrapper over LogicalPlan.

```rust
pub struct DataFrame {
    session_state: SessionState,
    plan: LogicalPlan,
}
```

For both `DataFrame` and `LogicalPlan`, you can build the query manually, such as:

```rust
let df = ctx.table("users").await?;

let new_df = df.select(vec![col("id"), col("bank_account")])?
    .sort(vec![col("id")])?;

let plan = LogicalPlanBuilder::from(&df.to_logical_plan())
    .project(vec![col("id"), col("bank_account")])?
    .sort(vec![col("id")])?
    .build()?;
```

But The main difference between a DataFrame and a LogicalPlan is that the DataFrame contains functionality for executing queries rather than just building plans.

You can use `collect` or `execute_stream` to execute the query.

## How to generate a DataFrame

You can manually call the `DataFrame` API or automatically generate a `DataFrame` through the SQL query planner just like:

use `sql` to construct `DataFrame`:

```rust
let ctx = SessionContext::new();
// Register the in-memory table containing the data
ctx.register_table("users", Arc::new(create_memtable()?))?;
let dataframe = ctx.sql("SELECT * FROM users;").await?;
```

construct `DataFrame` manually

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

When you have a `DataFrame`, you may want to access the results of the internal `LogicalPlan`. You can do this by using `collect` to retrieve all outputs at once, or `streaming_exec` to obtain a `SendableRecordBatchStream`.

You can just collect all outputs once like:

```rust
let ctx = SessionContext::new();
let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
let batches = df.collect().await?;
```

You can also use stream output to iterate the `RecordBatch`

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

Before writing to a file, it will call collect to calculate all the results of the DataFrame and then write to file.

For example, if you write it to a csv_file

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

As it is showed above, `DataFrame` is just a very thin wrapper of `LogicalPlan`, so you can easily go back and forth between them.

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
