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

use datafusion_common_runtime::SpawnedTask;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System};
use tokio::time::{interval, Duration};

use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::human_readable_size;
use datafusion_execution::{memory_pool::FairSpillPool, runtime_env::RuntimeEnvBuilder};

/// Measures the maximum RSS (in bytes) during the execution of an async task. RSS
/// will be sampled every 7ms.
///
/// # Arguments
///
/// * `f` - A closure that returns the async task to be measured.
///
/// # Returns
///
/// A tuple containing the result of the async task and the maximum RSS observed.
async fn measure_max_rss<F, Fut, T>(f: F) -> (T, usize)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    // Initialize system information
    let mut system = System::new_all();
    system.refresh_all();

    // Get the current process ID
    let pid = sysinfo::get_current_pid().expect("Failed to get current PID");

    // Shared atomic variable to store max RSS
    let max_rss = Arc::new(AtomicUsize::new(0));

    // Clone for the monitoring task
    let max_rss_clone = Arc::clone(&max_rss);

    // Spawn a monitoring task
    let monitor_handle = SpawnedTask::spawn(async move {
        let mut sys = System::new_all();
        let mut interval = interval(Duration::from_millis(7));

        loop {
            interval.tick().await;
            sys.refresh_processes_specifics(
                ProcessesToUpdate::Some(&[pid]),
                true,
                ProcessRefreshKind::nothing().with_memory(),
            );
            if let Some(process) = sys.process(pid) {
                let rss_bytes = process.memory();
                max_rss_clone
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                        if rss_bytes as usize > current {
                            Some(rss_bytes as usize)
                        } else {
                            None
                        }
                    })
                    .ok();
            } else {
                // Process no longer exists
                break;
            }
        }
    });

    // Execute the async task
    let result = f().await;

    // Give some time for the monitor to catch the final memory state
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Terminate the monitoring task
    drop(monitor_handle);

    // Retrieve the maximum RSS
    let peak_rss = max_rss.load(Ordering::Relaxed);

    (result, peak_rss)
}

/// Query runner that validates the memory usage of the query.
///
/// Note this function is supposed to run in a separate process for accurate memory
/// estimation. If environment variable `DATAFUSION_TEST_MEM_LIMIT_VALIDATION` is
/// not set, this function will return immediately, so test cases calls this function
/// should first set the environment variable, then create a new process to run.
/// See `sort_mem_validation.rs` for more details.
///
/// # Arguments
///
/// * `expected_mem_bytes` - The maximum expected memory usage for the query.
/// * `mem_limit_bytes` - The memory limit of the query in bytes. `None` means no
///   memory limit is presented.
/// * `query` - The SQL query to execute
/// * `baseline_query` - The SQL query to execute for estimating constant overhead.
///   This query should use 10% of the data of the main query.
///
/// # Example
///
///     utils::validate_query_with_memory_limits(
///         40_000_000 * 2,
///         Some(40_000_000),
///         "SELECT * FROM generate_series(1, 100000000) AS t(i) ORDER BY i",
///         "SELECT * FROM generate_series(1, 10000000) AS t(i) ORDER BY i"
///     );
///
/// The above function call means:
/// Set the memory limit to 40MB, and the profiled memory usage of {query - baseline_query}
/// should be less than 40MB * 2.
pub async fn validate_query_with_memory_limits(
    expected_mem_bytes: i64,
    mem_limit_bytes: Option<i64>,
    query: &str,
    baseline_query: &str,
) {
    if std::env::var("DATAFUSION_TEST_MEM_LIMIT_VALIDATION").is_err() {
        println!("Skipping test because DATAFUSION_TEST_MEM_LIMIT_VALIDATION is not set");

        return;
    }

    println!("Current process ID: {}", std::process::id());

    let runtime_builder = RuntimeEnvBuilder::new();

    let runtime = match mem_limit_bytes {
        Some(mem_limit_bytes) => runtime_builder
            .with_memory_pool(Arc::new(FairSpillPool::new(mem_limit_bytes as usize)))
            .build_arc()
            .unwrap(),
        None => runtime_builder.build_arc().unwrap(),
    };

    let session_config = SessionConfig::new().with_target_partitions(4); // Make sure the configuration is the same if test is running on different machines

    let ctx = SessionContext::new_with_config_rt(session_config, runtime);

    let df = ctx.sql(query).await.unwrap();
    // Run a query with 10% data to estimate the constant overhead
    let df_small = ctx.sql(baseline_query).await.unwrap();

    let (_, baseline_max_rss) =
        measure_max_rss(|| async { df_small.collect().await.unwrap() }).await;

    let (_, max_rss) = measure_max_rss(|| async { df.collect().await.unwrap() }).await;

    println!(
        "Memory before: {}, Memory after: {}",
        human_readable_size(baseline_max_rss),
        human_readable_size(max_rss)
    );

    let actual_mem_usage = max_rss as f64 - baseline_max_rss as f64;

    println!(
        "Query: {}, Memory usage: {}, Memory limit: {}",
        query,
        human_readable_size(actual_mem_usage as usize),
        human_readable_size(expected_mem_bytes as usize)
    );

    assert!(
        actual_mem_usage < expected_mem_bytes as f64,
        "Memory usage exceeded the theoretical limit. Actual: {}, Expected limit: {}",
        human_readable_size(actual_mem_usage as usize),
        human_readable_size(expected_mem_bytes as usize)
    );
}
