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

#[tokio::test]
async fn test_memory_profiling_report_content() {
    // Use the same complex query
    let sql = "SELECT v % 100 AS group_key, COUNT(*) AS cnt, SUM(v) AS sum_v \n  FROM generate_series(1,100000) AS t(v) \n GROUP BY group_key \n ORDER BY group_key";
    // Create context and enable memory profiling for next query
    let ctx = SessionContext::new();
    let _prof_handle = ctx.enable_memory_profiling();
    // Run the query
    ctx.sql(sql).await.unwrap().collect().await.unwrap();
    // Retrieve memory report
    let report = ctx.get_last_query_memory_report();
    // Verify that profiling captured some metrics
    assert!(!report.is_empty(), "expected non-empty memory report");
    // Compare the set of operator names to expected
    let mut actual_keys: Vec<String> = report.keys().cloned().collect();
    actual_keys.sort();
    let mut expected_keys = vec![
        // ExternalSorterMerge
        "ExternalSorterMerge[0]".to_string(),
        "ExternalSorterMerge[1]".to_string(),
        "ExternalSorterMerge[2]".to_string(),
        "ExternalSorterMerge[3]".to_string(),
        "ExternalSorterMerge[4]".to_string(),
        "ExternalSorterMerge[5]".to_string(),
        "ExternalSorterMerge[6]".to_string(),
        "ExternalSorterMerge[7]".to_string(),
        "ExternalSorterMerge[8]".to_string(),
        "ExternalSorterMerge[9]".to_string(),
        // ExternalSorter
        "ExternalSorter[0]".to_string(),
        "ExternalSorter[1]".to_string(),
        "ExternalSorter[2]".to_string(),
        "ExternalSorter[3]".to_string(),
        "ExternalSorter[4]".to_string(),
        "ExternalSorter[5]".to_string(),
        "ExternalSorter[6]".to_string(),
        "ExternalSorter[7]".to_string(),
        "ExternalSorter[8]".to_string(),
        "ExternalSorter[9]".to_string(),
        // GroupedHashAggregateStream
        "GroupedHashAggregateStream[0] (count(1), sum(t.v))".to_string(),
        "GroupedHashAggregateStream[1] (count(1), sum(t.v))".to_string(),
        "GroupedHashAggregateStream[2] (count(1), sum(t.v))".to_string(),
        "GroupedHashAggregateStream[3] (count(1), sum(t.v))".to_string(),
        "GroupedHashAggregateStream[4] (count(1), sum(t.v))".to_string(),
        "GroupedHashAggregateStream[5] (count(1), sum(t.v))".to_string(),
        "GroupedHashAggregateStream[6] (count(1), sum(t.v))".to_string(),
        "GroupedHashAggregateStream[7] (count(1), sum(t.v))".to_string(),
        "GroupedHashAggregateStream[8] (count(1), sum(t.v))".to_string(),
        "GroupedHashAggregateStream[9] (count(1), sum(t.v))".to_string(),
        // RepartitionExec
        "RepartitionExec[0]".to_string(),
        "RepartitionExec[1]".to_string(),
        "RepartitionExec[2]".to_string(),
        "RepartitionExec[3]".to_string(),
        "RepartitionExec[4]".to_string(),
        "RepartitionExec[5]".to_string(),
        "RepartitionExec[6]".to_string(),
        "RepartitionExec[7]".to_string(),
        "RepartitionExec[8]".to_string(),
        "RepartitionExec[9]".to_string(),
        // SortPreservingMergeExec
        "SortPreservingMergeExec[0]".to_string(),
        // Final output
        "query_output".to_string(),
    ];
    expected_keys.sort();
    assert_eq!(
        actual_keys, expected_keys,
        "memory report operator names do not match"
    );
}
