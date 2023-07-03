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

//! This module contains tests for limiting memory at runtime in DataFusion

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::streaming::PartitionStream;
use futures::StreamExt;
use std::sync::Arc;

use datafusion::datasource::streaming::StreamingTable;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionState;
use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::physical_optimizer::pipeline_fixer::PipelineFixer;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion_common::assert_contains;

use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_execution::TaskContext;
use test_utils::AccessLogGenerator;

#[cfg(test)]
#[ctor::ctor]
fn init() {
    // Enable RUST_LOG logging configuration for test
    let _ = env_logger::try_init();
}

#[tokio::test]
async fn oom_sort() {
    run_limit_test(
        "select * from t order by host DESC",
        vec![
            "Resources exhausted: Memory Exhausted while Sorting (DiskManager is disabled)",
        ],
        200_000,
    )
    .await
}

#[tokio::test]
async fn group_by_none() {
    run_limit_test(
        "select median(image) from t",
        vec![
            "Resources exhausted: Failed to allocate additional",
            "AggregateStream",
        ],
        20_000,
    )
    .await
}

#[tokio::test]
async fn group_by_row_hash() {
    run_limit_test(
        "select count(*) from t GROUP BY response_bytes",
        vec![
            "Resources exhausted: Failed to allocate additional",
            "GroupedHashAggregateStream",
        ],
        2_000,
    )
    .await
}

#[tokio::test]
async fn group_by_hash() {
    run_limit_test(
        // group by dict column
        "select count(*) from t GROUP BY service, host, pod, container",
        vec![
            "Resources exhausted: Failed to allocate additional",
            "GroupedHashAggregateStream",
        ],
        1_000,
    )
    .await
}

#[tokio::test]
async fn join_by_key_multiple_partitions() {
    let config = SessionConfig::new().with_target_partitions(2);
    run_limit_test_with_config(
        "select t1.* from t t1 JOIN t t2 ON t1.service = t2.service",
        vec![
            "Resources exhausted: Failed to allocate additional",
            "HashJoinInput[0]",
        ],
        1_000,
        config,
    )
    .await
}

#[tokio::test]
async fn join_by_key_single_partition() {
    let config = SessionConfig::new().with_target_partitions(1);
    run_limit_test_with_config(
        "select t1.* from t t1 JOIN t t2 ON t1.service = t2.service",
        vec![
            "Resources exhausted: Failed to allocate additional",
            "HashJoinInput",
        ],
        1_000,
        config,
    )
    .await
}

#[tokio::test]
async fn join_by_expression() {
    run_limit_test(
        "select t1.* from t t1 JOIN t t2 ON t1.service != t2.service",
        vec![
            "Resources exhausted: Failed to allocate additional",
            "NestedLoopJoinLoad[0]",
        ],
        1_000,
    )
    .await
}

#[tokio::test]
async fn cross_join() {
    run_limit_test(
        "select t1.* from t t1 CROSS JOIN t t2",
        vec![
            "Resources exhausted: Failed to allocate additional",
            "CrossJoinExec",
        ],
        1_000,
    )
    .await
}

#[tokio::test]
async fn merge_join() {
    // Planner chooses MergeJoin only if number of partitions > 1
    let config = SessionConfig::new()
        .with_target_partitions(2)
        .set_bool("datafusion.optimizer.prefer_hash_join", false);

    run_limit_test_with_config(
        "select t1.* from t t1 JOIN t t2 ON t1.pod = t2.pod AND t1.time = t2.time",
        vec![
            "Resources exhausted: Failed to allocate additional",
            "SMJStream",
        ],
        1_000,
        config,
    )
    .await
}

#[tokio::test]
async fn test_limit_symmetric_hash_join() {
    let config = SessionConfig::new();

    run_streaming_test_with_config(
        "select t1.* from t t1 JOIN t t2 ON t1.pod = t2.pod AND t1.time = t2.time",
        vec![
            "Resources exhausted: Failed to allocate additional",
            "SymmetricHashJoinStream",
        ],
        1_000,
        config,
    )
    .await
}

/// 50 byte memory limit
const MEMORY_FRACTION: f64 = 0.95;

/// runs the specified query against 1000 rows with specified
/// memory limit and no disk manager enabled with default SessionConfig.
async fn run_limit_test(
    query: &str,
    expected_error_contains: Vec<&str>,
    memory_limit: usize,
) {
    let config = SessionConfig::new();
    run_limit_test_with_config(query, expected_error_contains, memory_limit, config).await
}

/// runs the specified query against 1000 rows with a 50
/// byte memory limit and no disk manager enabled
/// with specified SessionConfig instance
async fn run_limit_test_with_config(
    query: &str,
    expected_error_contains: Vec<&str>,
    memory_limit: usize,
    config: SessionConfig,
) {
    let batches: Vec<_> = AccessLogGenerator::new()
        .with_row_limit(1000)
        .with_max_batch_size(50)
        .collect();

    let table = MemTable::try_new(batches[0].schema(), vec![batches]).unwrap();

    let rt_config = RuntimeConfig::new()
        // do not allow spilling
        .with_disk_manager(DiskManagerConfig::Disabled)
        .with_memory_limit(memory_limit, MEMORY_FRACTION);

    let runtime = RuntimeEnv::new(rt_config).unwrap();

    // Disabling physical optimizer rules to avoid sorts / repartitions
    // (since RepartitionExec / SortExec also has a memory budget which we'll likely hit first)
    let state = SessionState::with_config_rt(config, Arc::new(runtime))
        .with_physical_optimizer_rules(vec![]);

    let ctx = SessionContext::with_state(state);
    ctx.register_table("t", Arc::new(table))
        .expect("registering table");

    let df = ctx.sql(query).await.expect("Planning query");

    match df.collect().await {
        Ok(_batches) => {
            panic!("Unexpected success when running, expected memory limit failure")
        }
        Err(e) => {
            for error_substring in expected_error_contains {
                assert_contains!(e.to_string(), error_substring);
            }
        }
    }
}

struct DummyStreamPartition {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl PartitionStream for DummyStreamPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        // We create an iterator from the record batches and map them into Ok values,
        // converting the iterator into a futures::stream::Stream
        Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            futures::stream::iter(self.batches.clone()).map(Ok),
        ))
    }
}

async fn run_streaming_test_with_config(
    query: &str,
    expected_error_contains: Vec<&str>,
    memory_limit: usize,
    config: SessionConfig,
) {
    // Generate a set of access logs with a row limit of 1000 and a max batch size of 50
    let batches: Vec<_> = AccessLogGenerator::new()
        .with_row_limit(1000)
        .with_max_batch_size(50)
        .collect();

    // Create a new streaming table with the generated schema and batches
    let table = StreamingTable::try_new(
        batches[0].schema(),
        vec![Arc::new(DummyStreamPartition {
            schema: batches[0].schema(),
            batches: batches.clone(),
        })],
    )
    .unwrap()
    .with_infinite_table(true);

    // Configure the runtime environment with custom settings
    let rt_config = RuntimeConfig::new()
        // Disable disk manager to disallow spilling
        .with_disk_manager(DiskManagerConfig::Disabled)
        // Set memory limit to 50 bytes
        .with_memory_limit(memory_limit, MEMORY_FRACTION);

    // Create a new runtime environment with the configured settings
    let runtime = RuntimeEnv::new(rt_config).unwrap();

    // Create a new session state with the given configuration and runtime environment
    // Disable all physical optimizer rules except the PipelineFixer rule to avoid sorts or
    // repartition, as they also have memory budgets that may be hit first
    let state = SessionState::with_config_rt(config, Arc::new(runtime))
        .with_physical_optimizer_rules(vec![Arc::new(PipelineFixer::new())]);

    // Create a new session context with the session state
    let ctx = SessionContext::with_state(state);
    // Register the streaming table with the session context
    ctx.register_table("t", Arc::new(table))
        .expect("registering table");

    // Execute the SQL query and get a DataFrame
    let df = ctx.sql(query).await.expect("Planning query");

    // Collect the results of the DataFrame execution
    match df.collect().await {
        // If the execution succeeds, panic as we expect memory limit failure
        Ok(_batches) => {
            panic!("Unexpected success when running, expected memory limit failure")
        }
        // If the execution fails, verify if the error contains the expected substrings
        Err(e) => {
            for error_substring in expected_error_contains {
                assert_contains!(e.to_string(), error_substring);
            }
        }
    }
}
