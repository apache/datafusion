#[cfg(test)]
mod tests {
    use datafusion::common::Result;
    use datafusion::prelude::{CsvReadOptions, SessionContext};
    use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
    use std::fs::File;
    use std::io::BufReader;
    use substrait::proto::Plan;

    #[tokio::test]
    async fn function_compound_signature() -> Result<()> {
        // DataFusion currently produces Substrait that refers to functions only by their name.
        // However, the Substrait spec requires that functions be identified by their compound signature.
        // This test confirms that DataFusion is able to consume plans following the spec, even though
        // we don't yet produce such plans.
        // Once we start producing plans with compound signatures, this test can be replaced by the roundtrip tests.

        let ctx = create_context().await?;

        // File generated with substrait-java's Isthmus:
        // ./isthmus-cli/build/graal/isthmus "select not d from data" -c "create table data (d boolean)"
        let path = "tests/testdata/select_not_bool.substrait.json";
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        let plan = from_substrait_plan(&ctx, &proto).await?;

        assert_eq!(
            format!("{:?}", plan),
            "Projection: NOT DATA.a\
            \n  TableScan: DATA projection=[a, b, c, d, e, f]"
        );
        Ok(())
    }

    async fn create_context() -> datafusion::common::Result<SessionContext> {
        let ctx = SessionContext::new();
        ctx.register_csv("DATA", "tests/testdata/data.csv", CsvReadOptions::new())
            .await?;
        Ok(ctx)
    }
}
