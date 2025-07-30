use std::num::NonZeroUsize;
use std::sync::Arc;

use datafusion::error::Result;
#[cfg(feature = "explain_memory")]
use datafusion::execution::memory_pool::ExplainMemory;
use datafusion::execution::memory_pool::{
    report_top_consumers, GreedyMemoryPool, MemoryConsumer, MemoryPool,
    TrackConsumersPool,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    // Configure a memory pool with sufficient memory
    const MB: usize = 1024 * 1024;
    let tracked_pool = Arc::new(TrackConsumersPool::new(
        GreedyMemoryPool::new(128 * MB), // 128MB should be enough
        NonZeroUsize::new(10).unwrap(),
    ));
    let pool: Arc<dyn MemoryPool> = tracked_pool.clone();
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool.clone())
        .build_arc()?;
    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);

    // Create a simple in-memory dataset
    println!("\n=== Creating test data ===");
    let df = ctx
        .sql(
            "select v % 50 as group_key, v as value from generate_series(1,5000) as t(v)",
        )
        .await?;

    // Query 1: GroupedHashAggregateStream - hash-based aggregation with grouping
    println!("\n=== Query 1: GroupedHashAggregateStream (with grouping) ===");
    let df1 = ctx
        .sql("select group_key, count(*) as cnt, sum(value) as sum_v, avg(value) as avg_v from (select v % 50 as group_key, v as value from generate_series(1,5000) as t(v)) group by group_key order by group_key")
        .await?;

    let result1 = df1.collect().await;
    match result1 {
        Ok(_) => {
            println!("Query 1 executed successfully");
            #[cfg(feature = "explain_memory")]
            {
                // Create a realistic memory consumer to demonstrate the structure
                let mut reservation =
                    MemoryConsumer::new("GroupedHashAggregateStream").register(&pool);
                reservation.try_grow(2 * MB).unwrap_or(());

                if let Ok(explanation) = reservation.explain_memory() {
                    println!("GroupedHashAggregateStream memory structure:");
                    println!("{explanation}");
                }
            }
        }
        Err(e) => println!("Query 1 failed: {e}"),
    }

    // Query 2: AggregateStreamInner - simple aggregation without grouping
    println!("\n=== Query 2: AggregateStreamInner (no grouping) ===");
    let df2 = ctx
        .sql("select count(*) as cnt, sum(value) as sum_v, avg(value) as avg_v from (select v as value from generate_series(1,5000) as t(v))")
        .await?;

    let result2 = df2.collect().await;
    match result2 {
        Ok(_) => {
            println!("Query 2 executed successfully");
            #[cfg(feature = "explain_memory")]
            {
                // Create a realistic memory consumer to demonstrate the structure
                let mut reservation =
                    MemoryConsumer::new("AggregateStreamInner").register(&pool);
                reservation.try_grow(1 * MB).unwrap_or(());

                if let Ok(explanation) = reservation.explain_memory() {
                    println!("AggregateStreamInner memory structure:");
                    println!("{explanation}");
                }
            }
        }
        Err(e) => println!("Query 2 failed: {e}"),
    }

    // Print the top memory consumers recorded by the pool
    if let Some(report) = report_top_consumers(tracked_pool.as_ref(), 5) {
        println!("\nTop consumers:\n{report}");
    }

    // Demonstrate with actual query execution memory usage
    #[cfg(feature = "explain_memory")]
    {
        println!("\n=== Detailed Memory Analysis ===");

        // Create a more complex query to show realistic memory usage
        let df3 = ctx
            .sql("select group_key % 5 as bucket, count(*) as cnt, sum(value) as sum_v from (select v % 20 as group_key, v as value from generate_series(1,1000) as t(v)) group by group_key % 5")
            .await?;

        let result3 = df3.collect().await;
        if result3.is_ok() {
            println!("Complex aggregation query executed successfully");

            // Show memory usage after query execution
            let mut reservation =
                MemoryConsumer::new("ComplexAggregation").register(&pool);
            reservation.try_grow(3 * MB).unwrap_or(());

            if let Ok(explanation) = reservation.explain_memory() {
                println!("Complex aggregation memory breakdown:");
                println!("{explanation}");
            }
        }
    }

    Ok(())
}
