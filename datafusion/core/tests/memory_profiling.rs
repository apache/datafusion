use datafusion::prelude::*;
use std::time::{Duration, Instant};

#[tokio::test]
async fn test_memory_profiling_enabled_vs_disabled() {
    let ctx = SessionContext::new();

    // Test with memory profiling disabled (baseline)
    let start = Instant::now();
    ctx.sql("SELECT 1").await.unwrap().collect().await.unwrap();
    let disabled_duration = start.elapsed();

    // Test with memory profiling enabled
    let ctx_enabled = SessionContext::new();
    // Enable memory profiling through configuration
    ctx_enabled
        .conf()
        .set("datafusion.memory_profiling.enabled", "true")
        .unwrap();

    let start = Instant::now();
    ctx_enabled
        .sql("SELECT 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let enabled_duration = start.elapsed();

    // Verify the difference is minimal (less than 100 microseconds)
    let overhead = enabled_duration - disabled_duration;
    assert!(overhead < Duration::from_micros(100));
}
