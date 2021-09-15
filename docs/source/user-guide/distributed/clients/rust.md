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

# Ballista Rust Client

Ballista usage is very similar to DataFusion. Tha main difference is that the starting point is a `BallistaContext`
instead of the DataFusion `ExecutionContext`. Ballista uses the same DataFrame API as DataFusion.

The following code sample demonstrates how to create a `BallistaContext` to connect to a Ballista scheduler process.

```rust
let config = BallistaConfig::builder()
    .set("ballista.shuffle.partitions", "4")
    .build()?;

// connect to Ballista scheduler
let ctx = BallistaContext::remote("localhost", 50050, &config);
```

Here is a full example using the DataFrame API.

```rust
#[tokio::main]
async fn main() -> Result<()> {
    let config = BallistaConfig::builder()
        .set("ballista.shuffle.partitions", "4")
        .build()?;

    // connect to Ballista scheduler
    let ctx = BallistaContext::remote("localhost", 50050, &config);

    let testdata = datafusion::arrow::util::test_util::parquet_test_data();

    let filename = &format!("{}/alltypes_plain.parquet", testdata);

    // define the query using the DataFrame trait
    let df = ctx
        .read_parquet(filename)?
        .select_columns(&["id", "bool_col", "timestamp_col"])?
        .filter(col("id").gt(lit(1)))?;

    // print the results
    df.show().await?;

    Ok(())
}
```

Here is a full example demonstrating SQL usage.

```rust
#[tokio::main]
async fn main() -> Result<()> {
    let config = BallistaConfig::builder()
        .set("ballista.shuffle.partitions", "4")
        .build()?;

    // connect to Ballista scheduler
    let ctx = BallistaContext::remote("localhost", 50050, &config);

    let testdata = datafusion::arrow::util::test_util::arrow_test_data();

    // register csv file with the execution context
    ctx.register_csv(
        "aggregate_test_100",
        &format!("{}/csv/aggregate_test_100.csv", testdata),
        CsvReadOptions::new(),
    )?;

    // execute the query
    let df = ctx.sql(
        "SELECT c1, MIN(c12), MAX(c12) \
        FROM aggregate_test_100 \
        WHERE c11 > 0.1 AND c11 < 0.9 \
        GROUP BY c1",
    )?;

    // print the results
    df.show().await?;

    Ok(())
}
```
