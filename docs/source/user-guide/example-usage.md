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

Run a SQL query against data stored in a CSV:

```rust
use datafusion::prelude::*;
use arrow::util::pretty::print_batches;
use arrow::record_batch::RecordBatch;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
  // register the table
  let mut ctx = ExecutionContext::new();
  ctx.register_csv("example", "tests/example.csv", CsvReadOptions::new())?;

  // create a plan to run a SQL query
  let df = ctx.sql("SELECT a, MIN(b) FROM example GROUP BY a LIMIT 100")?;

  // execute and print results
  df.show().await?;
  Ok(())
}
```

Use the DataFrame API to process data stored in a CSV:

```rust
use datafusion::prelude::*;
use arrow::util::pretty::print_batches;
use arrow::record_batch::RecordBatch;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
  // create the dataframe
  let mut ctx = ExecutionContext::new();
  let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new())?;

  let df = df.filter(col("a").lt_eq(col("b")))?
           .aggregate(vec![col("a")], vec![min(col("b"))])?;

  // execute and print results
  df.show_limit(100).await?;
  Ok(())
}
```

Both of these examples will produce

```text
+---+--------+
| a | MIN(b) |
+---+--------+
| 1 | 2      |
+---+--------+
```
