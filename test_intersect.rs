use datafusion::error::Result;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Register a test table
    ctx.sql("CREATE TABLE test (col_int32 INT, col_utf8 VARCHAR)")
        .await?
        .show()
        .await?;

    // Try the intersect query
    let sql = "SELECT col_int32, col_utf8 FROM test \
               INTERSECT SELECT col_int32, col_utf8 FROM test";

    let df = ctx.sql(sql).await?;
    let plan = df.logical_plan();

    println!("Plan:\n{}", plan.display_indent());

    Ok(())
}
