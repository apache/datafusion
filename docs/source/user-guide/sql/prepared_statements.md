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

# Prepared Statements

The `PREPARE` statement allows for the creation and storage of a SQL statement with placeholder arguments.

The prepared statements can then be executed repeatedly in an efficient manner.

**SQL Example**

Create a prepared statement `greater_than` that selects all records where column "a" is greater than the parameter:

```sql
PREPARE greater_than(INT) AS SELECT * FROM example WHERE a > $1;
```

The prepared statement can then be executed with parameters as needed:

```sql
EXECUTE greater_than(20);
```

**Rust Example**

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
  // Register the table
  let ctx = SessionContext::new();
  ctx.register_csv("example", "tests/data/example.csv", CsvReadOptions::new()).await?;

  // Create the prepared statement `greater_than`
  let prepare_sql = "PREPARE greater_than(INT) AS SELECT * FROM example WHERE a > $1";
  ctx.sql(prepare_sql).await?;

  // Execute the prepared statement `greater_than`
  let execute_sql = "EXECUTE greater_than(20)";
  let df = ctx.sql(execute_sql).await?;

  // Execute and print results
  df.show().await?;
  Ok(())
}
```

## Inferred Types

If the parameter type is not specified, it can be inferred at execution time:

**SQL Example**

Create the prepared statement `greater_than`

```sql
PREPARE greater_than AS SELECT * FROM example WHERE a > $1;
```

Execute the prepared statement `greater_than`

```sql
EXECUTE greater_than(20);
```

**Rust Example**

```rust
# use datafusion::prelude::*;
# #[tokio::main]
# async fn main() -> datafusion::error::Result<()> {
#    let ctx = SessionContext::new();
#    ctx.register_csv("example", "tests/data/example.csv", CsvReadOptions::new()).await?;
#
    // Create the prepared statement `greater_than`
    let prepare_sql = "PREPARE greater_than AS SELECT * FROM example WHERE a > $1";
    ctx.sql(prepare_sql).await?;

    // Execute the prepared statement `greater_than`
    let execute_sql = "EXECUTE greater_than(20)";
    let df = ctx.sql(execute_sql).await?;
#
#    Ok(())
# }
```

## Positional Arguments

In the case of multiple parameters, prepared statements can use positional arguments:

**SQL Example**

Create the prepared statement `greater_than`

```sql
PREPARE greater_than(INT, DOUBLE) AS SELECT * FROM example WHERE a > $1 AND b > $2;
```

Execute the prepared statement `greater_than`

```sql
EXECUTE greater_than(20, 23.3);
```

**Rust Example**

```rust
# use datafusion::prelude::*;
# #[tokio::main]
# async fn main() -> datafusion::error::Result<()> {
#    let ctx = SessionContext::new();
#    ctx.register_csv("example", "tests/data/example.csv", CsvReadOptions::new()).await?;
  // Create the prepared statement `greater_than`
  let prepare_sql = "PREPARE greater_than(INT, DOUBLE) AS SELECT * FROM example WHERE a > $1 AND b > $2";
  ctx.sql(prepare_sql).await?;

  // Execute the prepared statement `greater_than`
  let execute_sql = "EXECUTE greater_than(20, 23.3)";
  let df = ctx.sql(execute_sql).await?;
#    Ok(())
# }
```
