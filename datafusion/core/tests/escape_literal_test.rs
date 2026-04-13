// escape_literal_test.rs
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::test]
async fn escape_literal_debug() -> Result<()> {
    let ctx = SessionContext::new();

    let df = ctx.sql("SELECT length('\\thello')").await?;
    println!("{:?}", df.logical_plan());

    let results = df.collect().await?;
    println!("{:?}", results);

    Ok(())
}