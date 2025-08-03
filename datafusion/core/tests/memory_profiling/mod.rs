// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! # Memory Profiling Tests
use datafusion::prelude::*;
use datafusion_common::instant::Instant;

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
        "enabled duration {enabled_duration:?} exceeds 110% of disabled duration {disabled_duration:?} ({ratio:.1}%)"
    );
}

#[tokio::test]
async fn test_memory_profiling_report_content() {
    // Use a complex query which contains multiple operators - ExternalSorterMerge, GroupedHashAggregateStream, RepartitionExec, SortPreservingMergeExec
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
    // For each key operator prefix, ensure there's at least one non-zero entry
    let expected_prefixes = vec![
        "ExternalSorterMerge",
        "ExternalSorter",
        "GroupedHashAggregateStream",
        "RepartitionExec",
        "SortPreservingMergeExec",
        "query_output",
    ];
    for prefix in expected_prefixes {
        let found = report
            .iter()
            .any(|(name, &bytes)| name.starts_with(prefix) && bytes > 0);
        assert!(
            found,
            "no non-zero memory entry found for operator {}. report keys: {:?}",
            prefix,
            report.keys().collect::<Vec<_>>()
        );
    }
}

#[tokio::test]
async fn test_memory_profiling_report_empty_when_not_enabled() {
    // Use the same complex query
    let sql = "SELECT v % 100 AS group_key, COUNT(*) AS cnt, SUM(v) AS sum_v \n  FROM generate_series(1,100000) AS t(v) \n GROUP BY group_key \n ORDER BY group_key";
    // Create context without enabling memory profiling
    let ctx = SessionContext::new();
    // Run the query
    ctx.sql(sql).await.unwrap().collect().await.unwrap();
    // Retrieve memory report
    let report = ctx.get_last_query_memory_report();
    // Expect no metrics when profiling not enabled
    assert!(
        report.is_empty(),
        "expected empty memory report when profiling not enabled"
    );
}
