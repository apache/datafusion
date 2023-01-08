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

In this example some simple processing is performed on the [`example.csv`](../../../datafusion/core/tests/data/example.csv) file.

## Update `Cargo.toml`

Add the following to your `Cargo.toml` file:

```toml
datafusion = "11.0"
tokio = "1.0"
```

## Run a SQL query against data stored in a CSV:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
  // register the table
  let ctx = SessionContext::new();
  ctx.register_csv("example", "tests/data/example.csv", CsvReadOptions::new()).await?;

  // create a plan to run a SQL query
  let df = ctx.sql("SELECT a, MIN(b) FROM example GROUP BY a LIMIT 100").await?;

  // execute and print results
  df.show().await?;
  Ok(())
}
```

## Use the DataFrame API to process data stored in a CSV:

```rust
use datafusion::prelude::*;

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

# Identifiers and Capitalization

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
  let df = ctx.sql("SELECT \"A\", MIN(b) FROM example GROUP BY \"A\" LIMIT 100").await?;

  // execute and print results
  df.show().await?;
  Ok(())
}
```

## Use the DataFrame API to process data stored in a CSV:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
  // create the dataframe
  let ctx = SessionContext::new();
  let df = ctx.read_csv("tests/data/capitalized_example.csv", CsvReadOptions::new()).await?;

  let df = df.filter(col("A").lt_eq(col("c")))?
           .aggregate(vec![col("A")], vec![min(col("b"))])?
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
