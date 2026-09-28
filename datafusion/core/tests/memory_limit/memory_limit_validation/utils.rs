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
use std::process::Command;
use std::str;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System};
use tokio::time::{Duration, interval};

use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
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
    Fut: Future<Output = T>,
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

/// Helper function that executes a test in a separate process with the required
/// environment variable set. Re-invokes the current test binary directly,
/// avoiding cargo overhead and recompilation.
pub fn spawn_test_process(module: &str, test: &str) {
    let test_path = format!("memory_limit::memory_limit_validation::{module}::{test}");
    let exe = std::env::current_exe().expect("Failed to get test binary path");
    let output = Command::new(exe)
        .arg(&test_path)
        .arg("--exact")
        .arg("--nocapture")
        .env("DATAFUSION_TEST_MEM_LIMIT_VALIDATION", "1")
        .output()
        .expect("Failed to execute test command");

    let stdout = str::from_utf8(&output.stdout).unwrap_or("");
    let stderr = str::from_utf8(&output.stderr).unwrap_or("");
    assert!(
        output.status.success(),
        "Test '{test}' failed with status: {}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status,
    );
}

fn operator_spill_count(plan: &dyn ExecutionPlan, operator_name: &str) -> usize {
    let own = if plan.name() == operator_name {
        plan.metrics().and_then(|m| m.spill_count()).unwrap_or(0)
    } else {
        0
    };
    own + plan
        .children()
        .into_iter()
        .map(|child| operator_spill_count(child.as_ref(), operator_name))
        .sum::<usize>()
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
    let session_config = SessionConfig::new().with_target_partitions(4); // Make sure the configuration is the same if test is running on different machines
    validate_query_with_memory_limits_and_config(
        expected_mem_bytes,
        mem_limit_bytes,
        query,
        baseline_query,
        session_config,
        None,
        None,
    )
    .await;
}

/// Validate memory usage with a custom session configuration and optional
/// operator and spill assertions.
pub async fn validate_query_with_memory_limits_and_config(
    expected_mem_bytes: i64,
    mem_limit_bytes: Option<i64>,
    query: &str,
    baseline_query: &str,
    session_config: SessionConfig,
    expected_operator_name: Option<&str>,
    expected_operator_spill: Option<bool>,
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

    let ctx = SessionContext::new_with_config_rt(session_config, runtime);

    let df = ctx.sql(query).await.unwrap();
    let physical_plan = df.create_physical_plan().await.unwrap();

    if let Some(expected) = expected_operator_name {
        let plan_display = displayable(physical_plan.as_ref()).indent(true).to_string();
        assert!(
            plan_display.contains(expected),
            "expected physical plan to contain `{expected}`, but got:\n{plan_display}",
        );
    }

    // Run a query with 10% data to estimate the constant overhead
    let baseline_plan = ctx
        .sql(baseline_query)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let baseline_task_ctx = ctx.task_ctx();
    let (_, baseline_max_rss) = measure_max_rss(|| async move {
        collect(baseline_plan, baseline_task_ctx).await.unwrap()
    })
    .await;

    let execution_plan = Arc::clone(&physical_plan);
    let execution_task_ctx = ctx.task_ctx();
    let (_, max_rss) = measure_max_rss(|| async move {
        collect(execution_plan, execution_task_ctx).await.unwrap()
    })
    .await;

    if let (Some(operator), Some(expect_spill)) =
        (expected_operator_name, expected_operator_spill)
    {
        let spill_count = operator_spill_count(physical_plan.as_ref(), operator);
        assert_eq!(
            spill_count > 0,
            expect_spill,
            "unexpected spill_count={spill_count} for {operator}",
        );
    }

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
