#[cfg(test)]
mod tmp2 {
    use arrow::util::pretty::print_batches;
    use datafusion::common::Result;
    use datafusion::physical_plan::{execute_stream, ExecutionPlan};
    use datafusion::prelude::SessionContext;
    use datafusion_execution::config::SessionConfig;
    use futures::StreamExt;
    use std::sync::Arc;

    fn print_plan(plan: &Arc<dyn ExecutionPlan>) -> () {
        let formatted = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string();
        let actual: Vec<&str> = formatted.trim().lines().collect();
        println!("{:#?}", actual);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_unbounded_hash_selection2() -> Result<()> {
        let config = SessionConfig::new()
            .with_target_partitions(1)
            .with_repartition_joins(false)
            .with_batch_size(10);
        let ctx = SessionContext::with_config(config);
        let abs = "tests/tpch-csv/";
        ctx.sql(&format!(
            "CREATE EXTERNAL TABLE nation (
                    n_nationkey BIGINT,
                    n_name VARCHAR,
                    n_regionkey BIGINT,
                    n_comment VARCHAR,
            ) STORED AS CSV
            WITH ORDER (n_nationkey ASC)
            LOCATION '{abs}nation.csv';"
        ))
        .await?;

        let sql = "SELECT n_regionkey FROM nation WHERE n_nationkey > 10;";

        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        print_plan(&physical_plan);
        let mut stream = execute_stream(physical_plan, ctx.task_ctx())?;
        while let Some(batch) = stream.next().await {
            print_batches(&[batch?])?;
        }
        Ok(())
    }
}
