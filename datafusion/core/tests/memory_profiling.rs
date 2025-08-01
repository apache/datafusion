use datafusion::prelude::*;
use std::time::Instant;

#[tokio::test]
async fn test_memory_profiling_enabled_vs_disabled() {
    let ctx = SessionContext::new();

    // Test with memory profiling disabled (baseline)
    let start = Instant::now();
    ctx.sql("SELECT 1").await.unwrap().collect().await.unwrap();
    let disabled_duration = start.elapsed();

    // Test with memory profiling enabled
    let mut config = SessionConfig::new();
    config
        .options_mut()
        .set("datafusion.execution.memory_profiling", "on_demand")
        .unwrap();
    let ctx_enabled = SessionContext::new_with_config(config);

    let start = Instant::now();
    ctx_enabled
        .sql("SELECT 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
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
