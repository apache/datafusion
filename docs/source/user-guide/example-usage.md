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

# Example Usage

In this example some simple processing is performed on the [`example.csv`](https://github.com/apache/datafusion/blob/main/datafusion/core/tests/data/example.csv) file.

Even [`more code examples`](https://github.com/apache/datafusion/tree/main/datafusion-examples) attached to the project.

## Add published DataFusion dependency

Find latest available Datafusion version on [DataFusion's
crates.io] page. Add the dependency to your `Cargo.toml` file:

```toml
datafusion = "latest_version"
tokio = { version = "1.0", features = ["rt-multi-thread"] }
```

## Run a SQL query against data stored in a CSV

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
  // register the table
  let ctx = SessionContext::new();
  ctx.register_csv("example", "tests/data/example.csv", CsvReadOptions::new()).await?;

  // create a plan to run a SQL query
  let df = ctx.sql("SELECT a, MIN(b) FROM example WHERE a <= b GROUP BY a LIMIT 100").await?;

  // execute and print results
  df.show().await?;
  Ok(())
}
```

See [the SQL API](../library-user-guide/using-the-sql-api.md) section of the
library guide for more information on the SQL API.

## Use the DataFrame API to process data stored in a CSV

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::min;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
  // create the dataframe
  let ctx = SessionContext::new();
  let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;

  let df = df.filter(col("a").lt_eq(col("b")))?
           .aggregate(vec![col("a")], vec![min(col("b"))])?
           .limit(0, Some(100))?;

  // execute and print results
  df.show().await?;
  Ok(())
}
```

## Output from both examples

```text
+---+--------+
| a | MIN(b) |
+---+--------+
| 1 | 2      |
+---+--------+
```

## Arrow Versions

Many of DataFusion's public APIs use types from the
[`arrow`] and [`parquet`] crates, so if you use
`arrow` in your project, the `arrow` version must match that used by
DataFusion. You can check the required version on [DataFusion's
crates.io] page.

The easiest way to ensure the versions match is to use the `arrow`
exported by DataFusion, for example:

```rust
use datafusion::arrow::datatypes::Schema;
```

For example, [DataFusion `25.0.0` dependencies] require `arrow`
`39.0.0`. If instead you used `arrow` `40.0.0` in your project you may
see errors such as:

```text
mismatched types [E0308] expected `Schema`, found `arrow_schema::Schema` Note: `arrow_schema::Schema` and `Schema` have similar names, but are actually distinct types Note: `arrow_schema::Schema` is defined in crate `arrow_schema` Note: `Schema` is defined in crate `arrow_schema` Note: perhaps two different versions of crate `arrow_schema` are being used? Note: associated function defined here
```

Or calling `downcast_ref` on an `ArrayRef` may return `None`
unexpectedly.

[`arrow`]: https://docs.rs/arrow/latest/arrow/
[`parquet`]: https://docs.rs/parquet/latest/parquet/
[datafusion's crates.io]: https://crates.io/crates/datafusion
[datafusion `26.0.0` dependencies]: https://crates.io/crates/datafusion/26.0.0/dependencies

## Identifiers and Capitalization

Please be aware that all identifiers are effectively made lower-case in SQL, so if your csv file has capital letters (ex: `Name`) you must put your column name in double quotes or the examples won't work.

To illustrate this behavior, consider the [`capitalized_example.csv`](../../../datafusion/core/tests/data/capitalized_example.csv) file:

## Run a SQL query against data stored in a CSV:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
  // register the table
  let ctx = SessionContext::new();
  ctx.register_csv("example", "tests/data/capitalized_example.csv", CsvReadOptions::new()).await?;

  // create a plan to run a SQL query
  let df = ctx.sql("SELECT \"A\", MIN(b) FROM example WHERE \"A\" <= c GROUP BY \"A\" LIMIT 100").await?;

  // execute and print results
  df.show().await?;
  Ok(())
}
```

## Use the DataFrame API to process data stored in a CSV:

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::min;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
  // create the dataframe
  let ctx = SessionContext::new();
  let df = ctx.read_csv("tests/data/capitalized_example.csv", CsvReadOptions::new()).await?;

  let df = df
      // col will parse the input string, hence requiring double quotes to maintain the capitalization
      .filter(col("\"A\"").lt_eq(col("c")))?
      // alternatively use ident to pass in an unqualified column name directly without parsing
      .aggregate(vec![ident("A")], vec![min(col("b"))])?
      .limit(0, Some(100))?;

  // execute and print results
  df.show().await?;
  Ok(())
}
```

## Output from both examples

```text
+---+--------+
| A | MIN(b) |
+---+--------+
| 2 | 1      |
| 1 | 2      |
+---+--------+
```
