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
use futures::StreamExt;
use std::process::Command;
use std::str;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System};
use tokio::time::{Duration, interval};

use datafusion::execution::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, displayable, execute_stream};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::human_readable_size;
use datafusion_execution::memory_pool::{
    FairSpillPool, MemoryDriftTracker, MemoryPool, PeakRecordingPool, UnboundedMemoryPool,
};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;

use test_utils::counting_allocator::allocated_bytes;

/// Measures the maximum RSS (in bytes) during the execution of an async task. RSS
/// will be sampled every 7ms, and so will `drift`, in addition to the samples it
/// takes on every reservation change.
///
/// # Arguments
///
/// * `drift` - The tracker of the memory pool the task uses.
/// * `f` - A closure that returns the async task to be measured.
///
/// # Returns
///
/// A tuple containing the result of the async task and the maximum RSS observed.
async fn measure_max_rss<F, Fut, T>(drift: &Arc<MemoryDriftTracker>, f: F) -> (T, usize)
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
    let drift = Arc::clone(drift);

    // Spawn a monitoring task
    let monitor_handle = SpawnedTask::spawn(async move {
        let mut sys = System::new_all();
        let mut interval = interval(Duration::from_millis(7));

        loop {
            interval.tick().await;
            drift.sample("sampler");
            sys.refresh_processes_specifics(
                ProcessesToUpdate::Some(&[pid]),
                true,
                ProcessRefreshKind::nothing().with_memory(),
            );
            if let Some(process) = sys.process(pid) {
                let rss_bytes = process.memory();
                max_rss_clone.fetch_max(rss_bytes as usize, Ordering::Relaxed);
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
    let test_path = format!("{module}::{test}");
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
    // A path that matches no test passes without running anything.
    assert!(
        stdout.contains("running 1 test"),
        "Test path '{test_path}' did not select exactly one test\nstdout:\n{stdout}",
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

/// Validates `sql` with no memory limit, over [`rows_with_strings`] of 10M rows
/// and 1M rows for the baseline. `operator` must be in the plan.
///
/// Callers run under a multi-threaded runtime: some operators only buffer
/// unreserved batches when their inputs run ahead on other threads.
pub async fn validate_query_over_rows_with_strings(
    expected_mem_bytes: i64,
    session_config: SessionConfig,
    operator: &str,
    sql: impl Fn(&str) -> String,
) {
    validate_query_with_memory_limits_and_config(
        expected_mem_bytes,
        None,
        &sql(&rows_with_strings(10_000_000)),
        &sql(&rows_with_strings(1_000_000)),
        session_config,
        Some(operator),
        None,
    )
    .await;
}

/// A table of `rows` rows: `v` is an Int64 from 1 to `rows`, in order, and `s`
/// is a string of about 25 bytes.
fn rows_with_strings(rows: usize) -> String {
    format!(
        "(SELECT v, concat('payload-', v, '-xxxxxxxx') AS s \
         FROM generate_series(1, {rows}) AS g(v))"
    )
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

    let (ctx, drift) = session(mem_limit_bytes, session_config.clone());
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
    let (baseline_ctx, baseline_drift) = session(mem_limit_bytes, session_config);
    let baseline_plan = baseline_ctx
        .sql(baseline_query)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let baseline_task_ctx = baseline_ctx.task_ctx();
    let (_, baseline_max_rss) = measure_max_rss(&baseline_drift, || {
        run_to_completion(baseline_plan, baseline_task_ctx)
    })
    .await;

    let execution_plan = Arc::clone(&physical_plan);
    let execution_task_ctx = ctx.task_ctx();
    let (_, max_rss) = measure_max_rss(&drift, || {
        run_to_completion(execution_plan, execution_task_ctx)
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

    let baseline_peak = baseline_drift
        .peak_drift()
        .expect("the sampler samples drift");
    let peak = drift.peak_drift().expect("the sampler samples drift");
    let unreserved_growth = peak.drift - baseline_peak.drift;
    println!(
        "Unreserved memory at its peak: before: {baseline_peak}, after: {peak}, growth: {}",
        signed_size(unreserved_growth),
    );

    assert!(
        actual_mem_usage < expected_mem_bytes as f64,
        "Memory usage exceeded the theoretical limit. Actual: {}, Expected limit: {}",
        human_readable_size(actual_mem_usage as usize),
        human_readable_size(expected_mem_bytes as usize)
    );
    assert!(
        unreserved_growth < MAX_UNRESERVED_GROWTH,
        "Unreserved memory grew with the input by {}, more than {}. Memory proportional \
         to the input must be reserved, see the `MemoryPool` docs",
        signed_size(unreserved_growth),
        human_readable_size(MAX_UNRESERVED_GROWTH as usize),
    );
}

/// Largest growth of unreserved memory (allocated minus reserved, at its peak)
/// allowed from the baseline query to the main query.
///
/// Batches in flight between operators need not be reserved, but they are the
/// same size whatever the input size, so they cancel out. Memory that grows
/// with the input must be reserved. The allocated byte count is off by at most
/// 256 KB per thread, far below this.
const MAX_UNRESERVED_GROWTH: isize = 16 * 1024 * 1024;

/// A session whose memory pool reports every reservation change to the
/// returned tracker, which compares the bytes reserved with the bytes allocated.
fn session(
    mem_limit_bytes: Option<i64>,
    session_config: SessionConfig,
) -> (SessionContext, Arc<MemoryDriftTracker>) {
    let pool: Arc<dyn MemoryPool> = match mem_limit_bytes {
        Some(mem_limit_bytes) => Arc::new(FairSpillPool::new(mem_limit_bytes as usize)),
        None => Arc::new(UnboundedMemoryPool::default()),
    };
    let drift = Arc::new(MemoryDriftTracker::new(Arc::new(allocated_bytes)));
    let pool =
        PeakRecordingPool::new(pool).with_drift_tracker(Arc::clone(&drift), "query");
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(pool))
        .build_arc()
        .unwrap();
    (
        SessionContext::new_with_config_rt(session_config, runtime),
        drift,
    )
}

/// Runs `plan` and drops each output batch as it arrives, so the output does not
/// count as memory the query holds.
async fn run_to_completion(plan: Arc<dyn ExecutionPlan>, task_ctx: Arc<TaskContext>) {
    let mut stream = execute_stream(plan, task_ctx).unwrap();
    while let Some(batch) = stream.next().await {
        batch.unwrap();
    }
}

fn signed_size(bytes: isize) -> String {
    let size = human_readable_size(bytes.unsigned_abs());
    if bytes < 0 { format!("-{size}") } else { size }
}
