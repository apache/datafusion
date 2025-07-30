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
    // Configure a memory pool limited to 16 MiB and track consumers
    const MB: usize = 1024 * 1024;
    let tracked_pool = Arc::new(TrackConsumersPool::new(
        GreedyMemoryPool::new(15 * MB),
        NonZeroUsize::new(5).unwrap(),
    ));
    let pool: Arc<dyn MemoryPool> = tracked_pool.clone();
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool.clone())
        .build_arc()?;
    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);

    // Manually allocate memory and print how much was reserved
    let mut reservation = MemoryConsumer::new("manual").register(&pool);
    reservation.try_grow(15 * MB)?;
    #[cfg(feature = "explain_memory")]
    println!("{}", reservation.explain_memory()?);

    let df = ctx
        .sql("select v % 1000 as group_key, count(*) as cnt, sum(v) as sum_v from generate_series(1,500000) as t(v) group by v % 1000 order by group_key")
        .await?;

    if let Err(e) = df.collect().await {
        println!("Query failed: {e}");
    }

    // Print the top memory consumers recorded by the pool
    if let Some(report) = report_top_consumers(tracked_pool.as_ref(), 5) {
        println!("\nTop consumers:\n{report}");
    }
    Ok(())
}
