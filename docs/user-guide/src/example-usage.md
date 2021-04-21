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
  let results: Vec<RecordBatch> = df.collect().await?;
  print_batches(&results)?;
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
           .aggregate(vec![col("a")], vec![min(col("b"))])?
           .limit(100)?;

  // execute and print results
  let results: Vec<RecordBatch> = df.collect().await?;
  print_batches(&results)?;
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
