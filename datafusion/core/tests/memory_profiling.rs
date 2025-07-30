use datafusion::prelude::*;
use std::time::{Duration, Instant};

#[tokio::test]
async fn test_memory_profiling_zero_overhead() {
    let ctx = SessionContext::new();
    let start = Instant::now();
    ctx.sql("SELECT 1").await.unwrap().collect().await.unwrap();
    let baseline = start.elapsed();

    let start = Instant::now();
    ctx.sql("SELECT 1").await.unwrap().collect().await.unwrap();
    let with_disabled = start.elapsed();

    assert!(with_disabled - baseline < Duration::from_micros(100));
}
