use datafusion::execution::context::ExecutionContext;

#[tokio::test]
async fn main() -> datafusion::error::Result<()> {
    // register the table
    let mut ctx = ExecutionContext::new();
    ctx.register_parquet("example", "tests/blogs.parquet")
        .await?;
    let df = ctx
        .sql("SELECT reply FROM example GROUP BY reply LIMIT 100")
        .await?;
    let record_batches = df.collect().await?;
    println!("sdasd {:?}", record_batches);
    Ok(())
}
