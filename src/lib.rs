pub mod consumer;
pub mod producer;

#[cfg(test)]
mod tests {

    use crate::{consumer::from_substrait_rel, producer::to_substrait_rel};
    use datafusion::error::Result;
    use datafusion::prelude::*;

    #[tokio::test]
    async fn simple_select() -> Result<()> {
        roundtrip("SELECT a, b FROM data").await
    }

    #[tokio::test]
    async fn wildcard_select() -> Result<()> {
        roundtrip("SELECT * FROM data").await
    }

    #[tokio::test]
    async fn select_with_filter() -> Result<()> {
        roundtrip("SELECT * FROM data WHERE a > 1").await
    }

    #[tokio::test]
    async fn select_with_filter_date() -> Result<()> {
        roundtrip("SELECT * FROM data WHERE c > CAST('2020-01-01' AS DATE)").await
    }

    #[tokio::test]
    async fn select_with_filter_bool_expr() -> Result<()> {
        roundtrip("SELECT * FROM data WHERE d AND a > 1").await
    }

    #[tokio::test]
    async fn roundtrip_inner_join() -> Result<()> {
        roundtrip("SELECT data.a FROM data JOIN data2 ON data.a = data2.a").await
    }

    #[tokio::test]
    async fn inner_join() -> Result<()> {
        assert_expected_plan(
            "SELECT data.a FROM data JOIN data2 ON data.a = data2.a",
            "Projection: #data.a\
            \n  Inner Join: #data.a = #data2.a\
            \n    TableScan: data projection=[a]\
            \n    TableScan: data2 projection=[a]",
        )
        .await
    }

    async fn assert_expected_plan(sql: &str, expected_plan_str: &str) -> Result<()> {
        let mut ctx = create_context().await?;
        let df = ctx.sql(sql).await?;
        let plan = df.to_logical_plan()?;
        let proto = to_substrait_rel(&plan)?;
        let df = from_substrait_rel(&mut ctx, &proto).await?;
        let plan2 = df.to_logical_plan()?;
        let plan2str = format!("{:?}", plan2);
        assert_eq!(expected_plan_str, &plan2str);
        Ok(())
    }
    async fn roundtrip(sql: &str) -> Result<()> {
        let mut ctx = create_context().await?;
        let df = ctx.sql(sql).await?;
        let plan = df.to_logical_plan()?;
        let proto = to_substrait_rel(&plan)?;

        // pretty print the protobuf struct
        //println!("{:#?}", proto);

        let df = from_substrait_rel(&mut ctx, &proto).await?;
        let plan2 = df.to_logical_plan()?;
        //println!("Roundtrip Plan:\n{:?}", plan2);

        let plan1str = format!("{:?}", plan);
        let plan2str = format!("{:?}", plan2);
        assert_eq!(plan1str, plan2str);
        Ok(())
    }

    async fn create_context() -> Result<SessionContext> {
        let ctx = SessionContext::new();
        ctx.register_csv("data", "testdata/data.csv", CsvReadOptions::new())
            .await?;
        ctx.register_csv("data2", "testdata/data.csv", CsvReadOptions::new())
            .await?;
        Ok(ctx)
    }
}
