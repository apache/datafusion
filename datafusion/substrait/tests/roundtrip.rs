use datafusion_substrait::consumer;
use datafusion_substrait::producer;


#[cfg(test)]
mod tests {

    use crate::{consumer::from_substrait_plan, producer::to_substrait_plan};
    use datafusion::error::Result;
    use datafusion::prelude::*;
    use substrait::protobuf::extensions::simple_extension_declaration::MappingType;

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
    async fn select_with_reused_functions() -> Result<()> {
        let sql = "SELECT * FROM data WHERE a > 1 AND a < 10 AND b > 0";
        roundtrip(sql).await?;
        let (mut function_names, mut function_anchors) = function_extension_info(sql).await?;
        function_names.sort();
        function_anchors.sort();

        assert_eq!(function_names, ["and", "gt", "lt"]);
        assert_eq!(function_anchors, [0, 1, 2]);

        Ok(())
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
    async fn select_with_limit() -> Result<()> {
        roundtrip_fill_na("SELECT * FROM data LIMIT 100").await
    }

    #[tokio::test]
    async fn select_with_limit_offset() -> Result<()> {
        roundtrip("SELECT * FROM data LIMIT 200 OFFSET 10").await
    }

    #[tokio::test]
    async fn simple_aggregate() -> Result<()> {
        roundtrip("SELECT a, sum(b) FROM data GROUP BY a").await
    }

    #[tokio::test]
    async fn aggregate_distinct_with_having() -> Result<()> {
        roundtrip("SELECT a, count(distinct b) FROM data GROUP BY a, c HAVING count(b) > 100").await
    }

    #[tokio::test]
    async fn aggregate_multiple_keys() -> Result<()> {
        roundtrip("SELECT a, c, avg(b) FROM data GROUP BY a, c").await
    }

    #[tokio::test]
    async fn simple_distinct() -> Result<()> {
        test_alias(
            "SELECT * FROM (SELECT distinct a FROM data)", // `SELECT *` is used to add `projection` at the root
            "SELECT a FROM data GROUP BY a",
        ).await
    }

    #[tokio::test]
    async fn select_distinct_two_fields() -> Result<()> {
        test_alias(
            "SELECT * FROM (SELECT distinct a, b FROM data)", // `SELECT *` is used to add `projection` at the root
            "SELECT a, b FROM data GROUP BY a, b",
        ).await
    }

    #[tokio::test]
    async fn simple_alias() -> Result<()> {
        test_alias(
            "SELECT d1.a, d1.b FROM data d1",
            "SELECT a, b FROM data",
        ).await
    }

    #[tokio::test]
    async fn two_table_alias() -> Result<()> {
        test_alias(
            "SELECT d1.a FROM data d1 JOIN data2 d2 ON d1.a = d2.a",
            "SELECT data.a FROM data JOIN data2 ON data.a = data2.a",
        )
        .await
    }

    #[tokio::test]
    async fn between_integers() -> Result<()> {
        test_alias(
            "SELECT * FROM data WHERE a BETWEEN 2 AND 6",
            "SELECT * FROM data WHERE a >= 2 AND a <= 6"
        )
        .await
    }

    #[tokio::test]
    async fn not_between_integers() -> Result<()> {
        test_alias(
            "SELECT * FROM data WHERE a NOT BETWEEN 2 AND 6",
            "SELECT * FROM data WHERE a < 2 OR a > 6"
        )
        .await
    }

    #[tokio::test]
    async fn case_without_base_expression() -> Result<()> {
        roundtrip("SELECT (CASE WHEN a >= 0 THEN 'positive' ELSE 'negative' END) FROM data").await
    }

    #[tokio::test]
    async fn case_with_base_expression() -> Result<()> {
        roundtrip("SELECT (CASE a
                            WHEN 0 THEN 'zero'
                            WHEN 1 THEN 'one'
                            ELSE 'other'
                           END) FROM data").await
    }

    #[tokio::test]
    async fn roundtrip_inner_join() -> Result<()> {
        roundtrip("SELECT data.a FROM data JOIN data2 ON data.a = data2.a").await
    }

    #[tokio::test]
    async fn inner_join() -> Result<()> {
        assert_expected_plan(
            "SELECT data.a FROM data JOIN data2 ON data.a = data2.a",
            "Projection: data.a\
            \n  Inner Join: data.a = data2.a\
            \n    TableScan: data projection=[a]\
            \n    TableScan: data2 projection=[a]",
        )
        .await
    }

    async fn assert_expected_plan(sql: &str, expected_plan_str: &str) -> Result<()> {
        let mut ctx = create_context().await?;
        let df = ctx.sql(sql).await?;
        let plan = df.to_logical_plan()?;
        let proto = to_substrait_plan(&plan)?;
        let df = from_substrait_plan(&mut ctx, &proto).await?;
        let plan2 = df.to_logical_plan()?;
        let plan2str = format!("{:?}", plan2);
        assert_eq!(expected_plan_str, &plan2str);
        Ok(())
    }

    async fn roundtrip_fill_na(sql: &str) -> Result<()> {
        let mut ctx = create_context().await?;
        let df = ctx.sql(sql).await?;
        let plan1 = df.to_logical_plan()?;
        let proto = to_substrait_plan(&plan1)?;

        let df = from_substrait_plan(&mut ctx, &proto).await?;
        let plan2 = df.to_logical_plan()?;

        // Format plan string and replace all None's with 0
        let plan1str = format!("{:?}", plan1).replace("None", "0");
        let plan2str = format!("{:?}", plan2).replace("None", "0");

        assert_eq!(plan1str, plan2str);
        Ok(())
    }

    async fn test_alias(sql_with_alias: &str, sql_no_alias: &str) -> Result<()> {
        // Since we ignore the SubqueryAlias in the producer, the result should be
        // the same as producing a Substrait plan from the same query without aliases
        // sql_with_alias -> substrait -> logical plan = sql_no_alias -> substrait -> logical plan
        let mut ctx = create_context().await?;

        let df_a = ctx.sql(sql_with_alias).await?;
        let proto_a = to_substrait_plan(&df_a.to_logical_plan()?)?;
        let plan_with_alias = from_substrait_plan(&mut ctx, &proto_a).await?.to_logical_plan()?;

        let df = ctx.sql(sql_no_alias).await?;
        let proto = to_substrait_plan(&df.to_logical_plan()?)?;
        let plan = from_substrait_plan(&mut ctx, &proto).await?.to_logical_plan()?;

        println!("{:#?}", plan_with_alias);
        println!("{:#?}", plan);

        let plan1str = format!("{:?}", plan_with_alias);
        let plan2str = format!("{:?}", plan);
        assert_eq!(plan1str, plan2str);
        Ok(())
    }

    async fn roundtrip(sql: &str) -> Result<()> {
        let mut ctx = create_context().await?;
        let df = ctx.sql(sql).await?;
        let plan = df.to_logical_plan()?;
        let proto = to_substrait_plan(&plan)?;

        let df = from_substrait_plan(&mut ctx, &proto).await?;
        let plan2 = df.to_logical_plan()?;

        println!("{:#?}", plan);
        println!("{:#?}", plan2);

        let plan1str = format!("{:?}", plan);
        let plan2str = format!("{:?}", plan2);
        assert_eq!(plan1str, plan2str);
        Ok(())
    }

    async fn function_extension_info(sql: &str) -> Result<(Vec<String>, Vec<u32>)>  {
        let ctx = create_context().await?;
        let df = ctx.sql(sql).await?;
        let plan = df.to_logical_plan()?;
        let proto = to_substrait_plan(&plan)?;

        let mut function_names: Vec<String> = vec![];
        let mut function_anchors: Vec<u32> = vec![];
        for e in &proto.extensions {
            let (function_anchor, function_name) = match e.mapping_type.as_ref().unwrap() {
                MappingType::ExtensionFunction(ext_f) => (ext_f.function_anchor, &ext_f.name),
                _ => unreachable!("Producer does not generate a non-function extension")
            };
            function_names.push(function_name.to_string());
            function_anchors.push(function_anchor);
        }
        
        Ok((function_names, function_anchors))
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
