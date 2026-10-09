use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::test]
async fn test_issue_25978() -> Result<()> {
    let ctx = SessionContext::new();
    
    // CREATE TABLE a (k VARCHAR NOT NULL, x VARCHAR) AS VALUES ('k1', 'y'), ('k2', NULL);
    // CREATE TABLE b (k VARCHAR NOT NULL) AS VALUES ('k3');
    ctx.sql("CREATE TABLE a (k VARCHAR NOT NULL, x VARCHAR) AS VALUES ('k1', 'y'), ('k2', NULL)").await?.collect().await?;
    ctx.sql("CREATE TABLE b (k VARCHAR NOT NULL) AS VALUES ('k3')").await?.collect().await?;
    
    // SELECT k, bool_or(f) AS f FROM (
    //     SELECT k, coalesce(x = 'y', FALSE) AS f FROM a
    //     UNION ALL
    //     SELECT k, TRUE AS f FROM b
    // ) u GROUP BY k;
    let df = ctx.sql("SELECT k, bool_or(f) AS f FROM (
        SELECT k, coalesce(x = 'y', FALSE) AS f FROM a
        UNION ALL
        SELECT k, TRUE AS f FROM b
    ) u GROUP BY k").await?;
    
    df.collect().await?;
    
    Ok(())
}
