use datafusion::prelude::*;
use std::time::Instant;

#[tokio::test]
async fn test_memory_profiling_enabled_vs_disabled() {
    // Define a more complex query generating 100k rows, aggregating and sorting
    let sql = "SELECT v % 100 AS group_key, COUNT(*) AS cnt, SUM(v) AS sum_v \n  FROM generate_series(1,100000) AS t(v) \n GROUP BY group_key \n ORDER BY group_key";
    let ctx = SessionContext::new();
    // Baseline run without memory profiling
    let start = Instant::now();
    ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let disabled_duration = start.elapsed();

    // Test with memory profiling enabled
    let mut config = SessionConfig::new();
    config
        .options_mut()
        .set("datafusion.execution.memory_profiling", "on_demand")
        .unwrap();
    let ctx_enabled = SessionContext::new_with_config(config);

    // Run the same complex query with profiling enabled
    let start = Instant::now();
    ctx_enabled.sql(sql).await.unwrap().collect().await.unwrap();
    let enabled_duration = start.elapsed();

    // Assert that enabled duration remains within 110% of the disabled (baseline) duration
    let max_allowed = disabled_duration.mul_f64(1.10);
    // Compute percentage overhead of enabled vs disabled
    let ratio = enabled_duration.as_secs_f64() / disabled_duration.as_secs_f64() * 100.0;
    assert!(
        enabled_duration <= max_allowed,
        "enabled duration {:?} exceeds 110% of disabled duration {:?} ({:.1}%)",
        enabled_duration,
        disabled_duration,
        ratio
    );
}
