
#[cfg(test)]
mod tests {

    use datafusion_substrait::consumer::from_substrait_plan;
    use datafusion_substrait::serializer;

    use datafusion::error::Result;
    use datafusion::prelude::*;

    use std::fs;

    #[tokio::test]
    async fn serialize_simple_select() -> Result<()> {
        let mut ctx = create_context().await?;
        let path = "tests/simple_select.bin";
        let sql = "SELECT a, b FROM data";
        // Test reference
        let df_ref = ctx.sql(sql).await?;
        let plan_ref = df_ref.to_logical_plan()?;
        // Test
        // Write substrait plan to file
        serializer::serialize(sql, &ctx, &path).await?;
        // Read substrait plan from file
        let proto = serializer::deserialize(path).await?;
        // Check plan equality
        let df = from_substrait_plan(&mut ctx, &proto).await?;
        let plan = df.to_logical_plan()?;
        let plan_str_ref = format!("{:?}", plan_ref);
        let plan_str = format!("{:?}", plan);
        assert_eq!(plan_str_ref, plan_str);
        // Delete test binary file
        fs::remove_file(path)?;

        Ok(())
    }

    async fn create_context() -> Result<SessionContext> {
        let ctx = SessionContext::new();
        ctx.register_csv("data", "tests/testdata/data.csv", CsvReadOptions::new())
            .await?;
        ctx.register_csv("data2", "tests/testdata/data.csv", CsvReadOptions::new())
            .await?;
        Ok(ctx)
    }
}