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
#![allow(clippy::items_after_test_module)]

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::streaming::PartitionStream;
use futures::StreamExt;
use rstest::rstest;
use std::sync::Arc;

use datafusion::datasource::streaming::StreamingTable;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::execution::context::SessionState;
use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::physical_optimizer::join_selection::JoinSelection;
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

#[rstest]
#[case::cant_grow_reservation(vec!["Resources exhausted: Failed to allocate additional", "ExternalSorter"], 100_000)]
#[case::cant_spill_to_disk(vec!["Resources exhausted: Memory Exhausted while Sorting (DiskManager is disabled)"], 200_000)]
#[case::no_oom(vec![], 600_000)]
#[tokio::test]
async fn sort(#[case] expected_errors: Vec<&str>, #[case] memory_limit: usize) {
    TestCase::new(
        "select * from t order by host DESC",
        expected_errors,
        memory_limit,
    )
    .run()
    .await
}

// We expect to see lower memory thresholds in general when applying a `LIMIT` clause due to eager sorting
#[rstest]
#[case::cant_grow_reservation(vec!["Resources exhausted: Failed to allocate additional", "ExternalSorter"], 20_000)]
#[case::cant_spill_to_disk(vec!["Memory Exhausted while Sorting (DiskManager is disabled)"], 40_000)]
//#[case::no_oom(vec![], 80_000)]
#[tokio::test]
async fn sort_with_limit(
    #[case] expected_errors: Vec<&str>,
    #[case] memory_limit: usize,
) {
    TestCase::new(
        "select * from t order by host DESC limit 10",
        expected_errors,
        memory_limit,
    )
    .run()
    .await
}

#[tokio::test]
async fn group_by_none() {
    TestCase::new(
        "select median(image) from t",
        vec![
            "Resources exhausted: Failed to allocate additional",
            "AggregateStream",
        ],
        20_000,
    )
    .run()
    .await
}

#[tokio::test]
async fn group_by_row_hash() {
    TestCase::new(
        "select count(*) from t GROUP BY response_bytes",
        vec![
            "Resources exhausted: Failed to allocate additional",
            "GroupedHashAggregateStream",
        ],
        2_000,
    )
    .run()
    .await
}

#[tokio::test]
async fn group_by_hash() {
    TestCase::new(
        // group by dict column
        "select count(*) from t GROUP BY service, host, pod, container",
        vec![
            "Resources exhausted: Failed to allocate additional",
            "GroupedHashAggregateStream",
        ],
        1_000,
    )
    .run()
    .await
}

#[tokio::test]
async fn join_by_key_multiple_partitions() {
    let config = SessionConfig::new().with_target_partitions(2);
    TestCase::new(
        "select t1.* from t t1 JOIN t t2 ON t1.service = t2.service",
        vec![
            "Resources exhausted: Failed to allocate additional",
            "HashJoinInput[0]",
        ],
        1_000,
    )
    .with_config(config)
    .run()
    .await
}

#[tokio::test]
async fn join_by_key_single_partition() {
    let config = SessionConfig::new().with_target_partitions(1);
    TestCase::new(
        "select t1.* from t t1 JOIN t t2 ON t1.service = t2.service",
        vec![
            "Resources exhausted: Failed to allocate additional",
            "HashJoinInput",
        ],
        1_000,
    )
    .with_config(config)
    .run()
    .await
}

#[tokio::test]
async fn join_by_expression() {
    TestCase::new(
        "select t1.* from t t1 JOIN t t2 ON t1.service != t2.service",
        vec![
            "Resources exhausted: Failed to allocate additional",
            "NestedLoopJoinLoad[0]",
        ],
        1_000,
    )
    .run()
    .await
}

#[tokio::test]
async fn cross_join() {
    TestCase::new(
        "select t1.* from t t1 CROSS JOIN t t2",
        vec![
            "Resources exhausted: Failed to allocate additional",
            "CrossJoinExec",
        ],
        1_000,
    )
    .run()
    .await
}

#[tokio::test]
async fn merge_join() {
    // Planner chooses MergeJoin only if number of partitions > 1
    let config = SessionConfig::new()
        .with_target_partitions(2)
        .set_bool("datafusion.optimizer.prefer_hash_join", false);

    TestCase::new(
        "select t1.* from t t1 JOIN t t2 ON t1.pod = t2.pod AND t1.time = t2.time",
        vec![
            "Resources exhausted: Failed to allocate additional",
            "SMJStream",
        ],
        1_000,
    )
    .with_config(config)
    .run()
    .await
}

#[tokio::test]
async fn symmetric_hash_join() {
    TestCase::new(
        "select t1.* from t t1 JOIN t t2 ON t1.pod = t2.pod AND t1.time = t2.time",
        vec![
            "Resources exhausted: Failed to allocate additional",
            "SymmetricHashJoinStream",
        ],
        1_000,
    )
    .with_scenario(Scenario::AccessLogStreaming)
    .run()
    .await
}

/// Run the query with the specified memory limit,
/// and verifies the expected errors are returned
#[derive(Clone, Debug)]
struct TestCase {
    query: String,
    expected_errors: Vec<String>,
    memory_limit: usize,
    config: SessionConfig,
    scenario: Scenario,
}

impl TestCase {
    fn new<'a>(
        query: impl Into<String>,
        expected_errors: impl IntoIterator<Item = &'a str>,
        memory_limit: usize,
    ) -> Self {
        let expected_errors: Vec<String> =
            expected_errors.into_iter().map(|s| s.to_string()).collect();

        Self {
            query: query.into(),
            expected_errors,
            memory_limit,
            config: SessionConfig::new(),
            scenario: Scenario::AccessLog,
        }
    }

    /// Specify the configuration to use
    pub fn with_config(mut self, config: SessionConfig) -> Self {
        self.config = config;
        self
    }

    /// Specify the scenario to run
    pub fn with_scenario(mut self, scenario: Scenario) -> Self {
        self.scenario = scenario;
        self
    }

    /// Run the test, panic'ing on error
    async fn run(self) {
        let Self {
            query,
            expected_errors,
            memory_limit,
            config,
            scenario,
        } = self;

        let table = scenario.table();

        let rt_config = RuntimeConfig::new()
            // do not allow spilling
            .with_disk_manager(DiskManagerConfig::Disabled)
            .with_memory_limit(memory_limit, MEMORY_FRACTION);

        let runtime = RuntimeEnv::new(rt_config).unwrap();

        // Configure execution
        let state = SessionState::with_config_rt(config, Arc::new(runtime))
            .with_physical_optimizer_rules(scenario.rules());

        let ctx = SessionContext::with_state(state);
        ctx.register_table("t", table).expect("registering table");

        let df = ctx.sql(&query).await.expect("Planning query");

        match df.collect().await {
            Ok(_batches) => {
                if !expected_errors.is_empty() {
                    panic!(
                        "Unexpected success when running, expected memory limit failure"
                    )
                }
            }
            Err(e) => {
                if expected_errors.is_empty() {
                    panic!(
                        "Unexpected failure when running, expected sufficient memory {e}"
                    )
                }

                for error_substring in expected_errors {
                    assert_contains!(e.to_string(), error_substring);
                }
            }
        }
    }
}

/// 50 byte memory limit
const MEMORY_FRACTION: f64 = 0.95;

/// Different data scenarios
#[derive(Clone, Debug)]
enum Scenario {
    /// 1000 rows of access log data with batches of 50 rows
    AccessLog,

    /// 1000 rows of access log data with batches of 50 rows in a
    /// [`StreamingTable`]
    AccessLogStreaming,
}

impl Scenario {
    /// return a TableProvider with data for the test
    fn table(&self) -> Arc<dyn TableProvider> {
        match self {
            Self::AccessLog => {
                let batches = access_log_batches();
                let table =
                    MemTable::try_new(batches[0].schema(), vec![batches]).unwrap();
                Arc::new(table)
            }
            Self::AccessLogStreaming => {
                let batches = access_log_batches();

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
                Arc::new(table)
            }
        }
    }

    /// return the optimizer rules to use
    fn rules(&self) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
        match self {
            Self::AccessLog => {
                // Disabling physical optimizer rules to avoid sorts /
                // repartitions (since RepartitionExec / SortExec also
                // has a memory budget which we'll likely hit first)
                vec![]
            }
            Self::AccessLogStreaming => {
                // Disable all physical optimizer rules except the
                // JoinSelection rule to avoid sorts or repartition,
                // as they also have memory budgets that may be hit
                // first
                vec![Arc::new(JoinSelection::new())]
            }
        }
    }
}

fn access_log_batches() -> Vec<RecordBatch> {
    AccessLogGenerator::new()
        .with_row_limit(1000)
        .with_max_batch_size(50)
        .collect()
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
