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

//! Fuzz tests that run every valid `AggregateExec` chain over the same data
//! and assert identical results.
//!
//! One test per operator [`Chain`], each running the chain over the cross
//! product of the other axes:
//! - The group [`Keys`] (single column with specific types, multiple columns, or fallback implementation)
//! - The [`Aggregates`] can be no aggregates or multiple aggregates that both need to track uniqueness and not (so more memory will be used),
//! - The source whether it is ordered by all keys, subset, or none [`Order`].
//! - The group [`Cardinality`] too test different memory and spill behavior
//! - The [`Memory`] budget - whether it is limited or not

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    BooleanArray, Int64Array, Int64Builder, ListBuilder, RecordBatch, StringArray,
    StringViewArray, StructArray, UInt32Array,
};
use arrow::buffer::NullBuffer;
use arrow::compute::{SortColumn, lexsort_to_indices, take_record_batch};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, SortOptions};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::prelude::SessionConfig;
use datafusion_common::test_util::batches_to_sort_string;
use datafusion_common::utils::get_available_parallelism;
use datafusion_common_runtime::JoinSet;
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{FairSpillPool, TrackConsumersPool};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_functions_aggregate::average::avg_udaf;
use datafusion_functions_aggregate::count::count_udaf;
use datafusion_functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion_functions_aggregate::sum::sum_udaf;
use datafusion_physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion_physical_expr::expressions::{cast, col};
use datafusion_physical_expr::{
    LexOrdering, Partitioning, PhysicalExpr, PhysicalSortExpr,
};
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, LimitOptions, PhysicalGroupBy,
};
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::{ExecutionPlan, InputOrderMode, collect, displayable};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

use AggregateMode::*;
use Operator::*;

mod assertions;
mod case_space;
mod context;
mod data;
mod plan;

use assertions::*;
use case_space::*;
use context::*;
use data::*;
use plan::*;

// Every test below is one physical plan shape, an `AggregateExec` chain, run
// over the same table for every combination of the axes in its `ChainTest`:
// which `GROUP BY` (one per `GroupValues` implementation), which aggregates,
// how the source is ordered and how many groups it has, how much memory the
// pool allows, and whether the skip-partial probe may fire. Every run must
// return the same rows as the plain single-stage aggregate `SINGLE`, and
// `assertions.rs` checks along the way that the plan was built as intended,
// that nothing runs out of memory or hangs, and that spilling and the
// skip-partial probe happen exactly where they may.
//
// `chain` lists the operators bottom-up, source first; the doc comment shows
// the same plan as DataFusion prints it. To run one case, narrow the lists
// in place. The axes and their values are documented in `case_space.rs`, the
// table in `data.rs`.

/// The reference chain every other chain is compared against.
const SINGLE: Chain = chain("single", &[Aggregate(Single)], 1);

/// `Single` on one partition. Also the reference every other chain is compared
/// against.
///
/// ```text
/// AggregateExec: mode=Single
///   DataSourceExec: partitions=1
/// ```
#[tokio::test(flavor = "multi_thread")]
async fn single() {
    ChainTest {
        chain: SINGLE,
        group_by: &Keys::ALL,
        aggregates: &Aggregates::HASH,
        orders: &Order::ALL,
        cardinalities: &Cardinality::ALL,
        memory: &Memory::ALL,
        skip_partial_config: &[true, false],
    }
    .assert_matches_single_aggregate()
    .await;
}

/// Each partition aggregates its own keys in one pass.
///
/// ```text
/// AggregateExec: mode=SinglePartitioned
///   RepartitionExec: partitioning=Hash(keys)
///     DataSourceExec: partitions=PARTITIONS
/// ```
#[tokio::test(flavor = "multi_thread")]
async fn single_partitioned() {
    ChainTest {
        chain: chain(
            "single_partitioned",
            &[HashRepartition, Aggregate(SinglePartitioned)],
            PARTITIONS,
        ),
        group_by: &Keys::GROUPED,
        aggregates: &Aggregates::HASH,
        orders: &Order::ALL,
        cardinalities: &Cardinality::ALL,
        memory: &Memory::ALL,
        skip_partial_config: &[true, false],
    }
    .assert_matches_single_aggregate()
    .await;
}

/// The shuffle keeps the source ordering, so the single stage still sees sorted
/// input.
///
/// ```text
/// AggregateExec: mode=SinglePartitioned
///   RepartitionExec: partitioning=Hash(keys), preserve_order=true
///     DataSourceExec: partitions=PARTITIONS
/// ```
#[tokio::test(flavor = "multi_thread")]
async fn single_partitioned_order_preserving() {
    ChainTest {
        chain: chain(
            "single_partitioned_order_preserving",
            &[OrderPreservingHashRepartition, Aggregate(SinglePartitioned)],
            PARTITIONS,
        ),
        group_by: &Keys::GROUPED,
        aggregates: &Aggregates::HASH,
        orders: &Order::SORTED,
        cardinalities: &Cardinality::ALL,
        memory: &Memory::ALL,
        skip_partial_config: &[true, false],
    }
    .assert_matches_single_aggregate()
    .await;
}

/// The planner's default two-stage plan.
///
/// ```text
/// AggregateExec: mode=FinalPartitioned
///   RepartitionExec: partitioning=Hash(keys)
///     AggregateExec: mode=Partial
///       DataSourceExec: partitions=PARTITIONS
/// ```
#[tokio::test(flavor = "multi_thread")]
async fn partial_repartition_final() {
    ChainTest {
        chain: chain(
            "partial_repartition_final",
            &[
                Aggregate(Partial),
                HashRepartition,
                Aggregate(FinalPartitioned),
            ],
            PARTITIONS,
        ),
        group_by: &Keys::GROUPED,
        aggregates: &Aggregates::HASH,
        orders: &Order::ALL,
        cardinalities: &Cardinality::ALL,
        memory: &Memory::ALL,
        skip_partial_config: &[true, false],
    }
    .assert_matches_single_aggregate()
    .await;
}

/// Two stages merged into one output partition.
///
/// ```text
/// AggregateExec: mode=Final
///   CoalescePartitionsExec
///     AggregateExec: mode=Partial
///       DataSourceExec: partitions=PARTITIONS
/// ```
#[tokio::test(flavor = "multi_thread")]
async fn partial_coalesce_final() {
    ChainTest {
        chain: chain(
            "partial_coalesce_final",
            &[Aggregate(Partial), CoalescePartitions, Aggregate(Final)],
            PARTITIONS,
        ),
        group_by: &Keys::ALL,
        aggregates: &Aggregates::HASH,
        orders: &Order::ALL,
        cardinalities: &Cardinality::ALL,
        memory: &Memory::ALL,
        skip_partial_config: &[true, false],
    }
    .assert_matches_single_aggregate()
    .await;
}

/// Two stages whose shuffle keeps the source ordering, so the final stage sees
/// sorted input.
///
/// ```text
/// AggregateExec: mode=FinalPartitioned
///   RepartitionExec: partitioning=Hash(keys), preserve_order=true
///     AggregateExec: mode=Partial
///       DataSourceExec: partitions=PARTITIONS
/// ```
#[tokio::test(flavor = "multi_thread")]
async fn partial_order_preserving_repartition_final() {
    ChainTest {
        chain: chain(
            "partial_order_preserving_repartition_final",
            &[
                Aggregate(Partial),
                OrderPreservingHashRepartition,
                Aggregate(FinalPartitioned),
            ],
            PARTITIONS,
        ),
        group_by: &Keys::GROUPED,
        aggregates: &Aggregates::HASH,
        orders: &Order::SORTED,
        cardinalities: &Cardinality::ALL,
        memory: &Memory::ALL,
        skip_partial_config: &[true, false],
    }
    .assert_matches_single_aggregate()
    .await;
}

/// Two stages merged by a sort-preserving merge, so the final stage sees sorted
/// input.
///
/// ```text
/// AggregateExec: mode=Final
///   SortPreservingMergeExec: [keys]
///     AggregateExec: mode=Partial
///       DataSourceExec: partitions=PARTITIONS
/// ```
#[tokio::test(flavor = "multi_thread")]
async fn partial_sort_preserving_merge_final() {
    ChainTest {
        chain: chain(
            "partial_sort_preserving_merge_final",
            &[Aggregate(Partial), SortPreservingMerge, Aggregate(Final)],
            PARTITIONS,
        ),
        group_by: &Keys::GROUPED,
        aggregates: &Aggregates::HASH,
        orders: &Order::SORTED,
        cardinalities: &Cardinality::ALL,
        memory: &Memory::ALL,
        skip_partial_config: &[true, false],
    }
    .assert_matches_single_aggregate()
    .await;
}

/// Two stages back to back on one partition, no shuffle between.
///
/// ```text
/// AggregateExec: mode=Final
///   AggregateExec: mode=Partial
///     DataSourceExec: partitions=1
/// ```
#[tokio::test(flavor = "multi_thread")]
async fn partial_final_single_partition() {
    ChainTest {
        chain: chain(
            "partial_final_single_partition",
            &[Aggregate(Partial), Aggregate(Final)],
            1,
        ),
        group_by: &Keys::ALL,
        aggregates: &Aggregates::HASH,
        orders: &Order::ALL,
        cardinalities: &Cardinality::ALL,
        memory: &Memory::ALL,
        skip_partial_config: &[true, false],
    }
    .assert_matches_single_aggregate()
    .await;
}

/// Three stages with a `PartialReduce` between two shuffles.
///
/// ```text
/// AggregateExec: mode=FinalPartitioned
///   RepartitionExec: partitioning=Hash(keys)
///     AggregateExec: mode=PartialReduce
///       RepartitionExec: partitioning=Hash(keys)
///         AggregateExec: mode=Partial
///           DataSourceExec: partitions=PARTITIONS
/// ```
#[tokio::test(flavor = "multi_thread")]
async fn partial_repartition_reduce_repartition_final() {
    ChainTest {
        chain: chain(
            "partial_repartition_reduce_repartition_final",
            &[
                Aggregate(Partial),
                HashRepartition,
                Aggregate(PartialReduce),
                HashRepartition,
                Aggregate(FinalPartitioned),
            ],
            PARTITIONS,
        ),
        group_by: &Keys::GROUPED,
        aggregates: &Aggregates::HASH,
        orders: &Order::ALL,
        cardinalities: &Cardinality::ALL,
        memory: &Memory::ALL,
        skip_partial_config: &[true, false],
    }
    .assert_matches_single_aggregate()
    .await;
}

/// Three stages: a shuffled `PartialReduce` merged into one final partition.
///
/// ```text
/// AggregateExec: mode=Final
///   CoalescePartitionsExec
///     AggregateExec: mode=PartialReduce
///       RepartitionExec: partitioning=Hash(keys)
///         AggregateExec: mode=Partial
///           DataSourceExec: partitions=PARTITIONS
/// ```
#[tokio::test(flavor = "multi_thread")]
async fn partial_repartition_reduce_coalesce_final() {
    ChainTest {
        chain: chain(
            "partial_repartition_reduce_coalesce_final",
            &[
                Aggregate(Partial),
                HashRepartition,
                Aggregate(PartialReduce),
                CoalescePartitions,
                Aggregate(Final),
            ],
            PARTITIONS,
        ),
        group_by: &Keys::GROUPED,
        aggregates: &Aggregates::HASH,
        orders: &Order::ALL,
        cardinalities: &Cardinality::ALL,
        memory: &Memory::ALL,
        skip_partial_config: &[true, false],
    }
    .assert_matches_single_aggregate()
    .await;
}

/// Three stages where `PartialReduce` and `Final` each run on one coalesced
/// partition.
///
/// ```text
/// AggregateExec: mode=Final
///   CoalescePartitionsExec
///     AggregateExec: mode=PartialReduce
///       CoalescePartitionsExec
///         AggregateExec: mode=Partial
///           DataSourceExec: partitions=PARTITIONS
/// ```
#[tokio::test(flavor = "multi_thread")]
async fn partial_coalesce_reduce_coalesce_final() {
    ChainTest {
        chain: chain(
            "partial_coalesce_reduce_coalesce_final",
            &[
                Aggregate(Partial),
                CoalescePartitions,
                Aggregate(PartialReduce),
                CoalescePartitions,
                Aggregate(Final),
            ],
            PARTITIONS,
        ),
        group_by: &Keys::ALL,
        aggregates: &Aggregates::HASH,
        orders: &Order::ALL,
        cardinalities: &Cardinality::ALL,
        memory: &Memory::ALL,
        skip_partial_config: &[true, false],
    }
    .assert_matches_single_aggregate()
    .await;
}

/// `PartialReduce` directly on top of `Partial`, before the shuffle.
///
/// ```text
/// AggregateExec: mode=FinalPartitioned
///   RepartitionExec: partitioning=Hash(keys)
///     AggregateExec: mode=PartialReduce
///       AggregateExec: mode=Partial
///         DataSourceExec: partitions=PARTITIONS
/// ```
#[tokio::test(flavor = "multi_thread")]
async fn partial_local_reduce_repartition_final() {
    ChainTest {
        chain: chain(
            "partial_local_reduce_repartition_final",
            &[
                Aggregate(Partial),
                Aggregate(PartialReduce),
                HashRepartition,
                Aggregate(FinalPartitioned),
            ],
            PARTITIONS,
        ),
        group_by: &Keys::GROUPED,
        aggregates: &Aggregates::HASH,
        orders: &Order::ALL,
        cardinalities: &Cardinality::ALL,
        memory: &Memory::ALL,
        skip_partial_config: &[true, false],
    }
    .assert_matches_single_aggregate()
    .await;
}

/// Three stages joined by order-preserving shuffles. Ordered `PartialReduce`
/// has no dedicated stream and lands on the fallback.
///
/// ```text
/// AggregateExec: mode=FinalPartitioned
///   RepartitionExec: partitioning=Hash(keys), preserve_order=true
///     AggregateExec: mode=PartialReduce
///       RepartitionExec: partitioning=Hash(keys), preserve_order=true
///         AggregateExec: mode=Partial
///           DataSourceExec: partitions=PARTITIONS
/// ```
#[tokio::test(flavor = "multi_thread")]
async fn partial_reduce_final_order_preserving() {
    ChainTest {
        chain: chain(
            "partial_reduce_final_order_preserving",
            &[
                Aggregate(Partial),
                OrderPreservingHashRepartition,
                Aggregate(PartialReduce),
                OrderPreservingHashRepartition,
                Aggregate(FinalPartitioned),
            ],
            PARTITIONS,
        ),
        group_by: &Keys::GROUPED,
        aggregates: &Aggregates::HASH,
        orders: &Order::SORTED,
        cardinalities: &Cardinality::ALL,
        memory: &Memory::ALL,
        skip_partial_config: &[true, false],
    }
    .assert_matches_single_aggregate()
    .await;
}

/// `GroupedTopKAggregateStream` alone. The limit is above the group count, so
/// every group survives.
///
/// ```text
/// AggregateExec: mode=Single, lim=[TOP_K_LIMIT]
///   DataSourceExec: partitions=1
/// ```
#[tokio::test(flavor = "multi_thread")]
async fn top_k_single() {
    ChainTest {
        chain: chain("top_k_single", &[TopK(Single)], 1),
        group_by: &Keys::TOP_K,
        aggregates: &Aggregates::TOP_K,
        orders: &Order::ALL,
        cardinalities: &Cardinality::ALL,
        memory: &Memory::ALL,
        skip_partial_config: &[true, false],
    }
    .assert_matches_single_aggregate()
    .await;
}

/// Planner shape for `GROUP BY ... ORDER BY max(v) LIMIT n`: the limit lands on
/// the final stage.
///
/// ```text
/// AggregateExec: mode=FinalPartitioned, lim=[TOP_K_LIMIT]
///   RepartitionExec: partitioning=Hash(keys)
///     AggregateExec: mode=Partial
///       DataSourceExec: partitions=PARTITIONS
/// ```
#[tokio::test(flavor = "multi_thread")]
async fn top_k_partial_repartition_final() {
    ChainTest {
        chain: chain(
            "top_k_partial_repartition_final",
            &[Aggregate(Partial), HashRepartition, TopK(FinalPartitioned)],
            PARTITIONS,
        ),
        group_by: &Keys::TOP_K,
        aggregates: &Aggregates::TOP_K,
        orders: &Order::ALL,
        cardinalities: &Cardinality::ALL,
        memory: &Memory::ALL,
        skip_partial_config: &[true, false],
    }
    .assert_matches_single_aggregate()
    .await;
}

/// TopK final stage on one coalesced partition.
///
/// ```text
/// AggregateExec: mode=Final, lim=[TOP_K_LIMIT]
///   CoalescePartitionsExec
///     AggregateExec: mode=Partial
///       DataSourceExec: partitions=PARTITIONS
/// ```
#[tokio::test(flavor = "multi_thread")]
async fn top_k_partial_coalesce_final() {
    ChainTest {
        chain: chain(
            "top_k_partial_coalesce_final",
            &[Aggregate(Partial), CoalescePartitions, TopK(Final)],
            PARTITIONS,
        ),
        group_by: &Keys::TOP_K,
        aggregates: &Aggregates::TOP_K,
        orders: &Order::ALL,
        cardinalities: &Cardinality::ALL,
        memory: &Memory::ALL,
        skip_partial_config: &[true, false],
    }
    .assert_matches_single_aggregate()
    .await;
}

/// TopK on both stages.
///
/// ```text
/// AggregateExec: mode=FinalPartitioned, lim=[TOP_K_LIMIT]
///   RepartitionExec: partitioning=Hash(keys)
///     AggregateExec: mode=Partial, lim=[TOP_K_LIMIT]
///       DataSourceExec: partitions=PARTITIONS
/// ```
#[tokio::test(flavor = "multi_thread")]
async fn top_k_both_stages() {
    ChainTest {
        chain: chain(
            "top_k_both_stages",
            &[TopK(Partial), HashRepartition, TopK(FinalPartitioned)],
            PARTITIONS,
        ),
        group_by: &Keys::TOP_K,
        aggregates: &Aggregates::TOP_K,
        orders: &Order::ALL,
        cardinalities: &Cardinality::ALL,
        memory: &Memory::ALL,
        skip_partial_config: &[true, false],
    }
    .assert_matches_single_aggregate()
    .await;
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

/// Sorted output plus the stages that spilled, empty if none did.
struct Outcome {
    output: String,
    spilled: Vec<String>,
}

/// Arranged source partitions by `(keys, order, partition count)`, the only
/// case dimensions the arrangement depends on. Arranging costs about a third
/// of a case, so it is shared across chains, memory budgets and skip-partial
/// settings.
type Inputs = HashMap<(Keys, Order, usize), Arc<Vec<Vec<RecordBatch>>>>;

fn input_key(case: &Case) -> (Keys, Order, usize) {
    (
        case.shape.query.keys,
        case.params.order,
        case.shape.chain.source_partitions,
    )
}

fn arrange_all<'a>(rows: &RecordBatch, cases: impl Iterator<Item = &'a Case>) -> Inputs {
    let mut inputs = Inputs::new();
    for case in cases {
        inputs.entry(input_key(case)).or_insert_with(|| {
            Arc::new(arrange(
                rows,
                case.shape.query.keys,
                case.params.order,
                case.shape.chain.source_partitions,
            ))
        });
    }
    inputs
}

/// Runs one case, checks plan shape and metrics, and returns its outcome.
///
/// Running out of memory is never accepted: every stream either spills, emits
/// early, or is bounded, so an error there is a bug in a stream's memory
/// handling or in how the stages share the pool.
async fn run_case(case: Case, inputs: Arc<Inputs>) -> Outcome {
    log::debug!("start {case:?}");
    let outcome = run_case_inner(&case, &inputs[&input_key(&case)]).await;
    log::debug!("done  {case:?}");
    outcome
}

async fn run_case_inner(case: &Case, partitions: &[Vec<RecordBatch>]) -> Outcome {
    let plan = build_plan(
        &case.shape,
        source(partitions, case.shape.query.keys, case.params.order),
    );
    check_plan_shape(case, &plan);

    // A hang is a failure too: name the case instead of stalling the run.
    let collected = tokio::time::timeout(
        Duration::from_secs(CASE_TIMEOUT_SECS),
        collect(Arc::clone(&plan), task_context(case)),
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "{case:?} did not finish within {CASE_TIMEOUT_SECS}s\n{}",
            displayable(plan.as_ref()).indent(true)
        )
    });
    let batches = match collected {
        Ok(batches) => batches,
        Err(error) => panic!(
            "{case:?} failed: {error}\n{}",
            displayable(plan.as_ref()).indent(true)
        ),
    };
    let spilled = check_metrics(case, &plan);
    Outcome {
        output: batches_to_sort_string(&batches),
        spilled,
    }
}

/// The case whose output is the reference for `query`: the `single` chain,
/// one partition, unordered input, unlimited memory.
fn reference_case(query: Query, cardinality: Cardinality) -> Case {
    Case {
        shape: Shape {
            chain: SINGLE,
            query,
        },
        params: CaseParams {
            order: Order::Unordered,
            cardinality,
            memory: Memory::Unlimited,
            skip_partial_enabled: true,
        },
    }
}

impl ChainTest {
    /// Runs every case and asserts each returns the rows of the `SINGLE`
    /// chain for its query, see the preamble for the full list of checks. A
    /// failure does not stop the run, so one run reports every failing case.
    async fn assert_matches_single_aggregate(self) {
        const SEED: u64 = 42;
        let chain = self.chain;
        let mut total_spilled = 0;
        let mut failures: Vec<String> = vec![];
        // Every in-flight case holds several copies of the dataset and its own
        // partitioned streams, so bound the concurrency by the cores at hand
        // instead of spawning the whole matrix.
        let max_concurrent_cases = get_available_parallelism();

        for cardinality in Cardinality::ALL {
            let rows = generate_rows(cardinality, SEED);
            let cases: Vec<Case> = self
                .cases()
                .into_iter()
                .filter(|case| case.params.cardinality == cardinality)
                .collect();
            let mut reference_cases: Vec<Case> = vec![];
            for case in &cases {
                let query = case.shape.query;
                if !reference_cases.iter().any(|case| case.shape.query == query) {
                    reference_cases.push(reference_case(query, cardinality));
                }
            }
            let inputs =
                Arc::new(arrange_all(&rows, cases.iter().chain(&reference_cases)));

            let mut expected_by_query: Vec<(Query, String)> = Vec::new();
            for case in reference_cases {
                let query = case.shape.query;
                let outcome = run_case(case, Arc::clone(&inputs)).await;
                expected_by_query.push((query, outcome.output));
            }

            let mut join_set = JoinSet::new();
            let (mut spilled, mut finished) = (vec![], vec![]);
            for case in cases {
                let inputs = Arc::clone(&inputs);
                let expected = expected_by_query
                    .iter()
                    .find(|(query, _)| *query == case.shape.query)
                    .map(|(_, expected)| expected.clone())
                    .unwrap();
                while join_set.len() >= max_concurrent_cases {
                    collect_finished(
                        &mut join_set,
                        &mut spilled,
                        &mut finished,
                        &mut failures,
                    )
                    .await;
                }
                join_set.spawn(async move {
                    let outcome = run_case(case.clone(), inputs).await;
                    assert_eq!(outcome.output, expected, "{case:?}");
                    (case, outcome.spilled)
                });
            }
            while !join_set.is_empty() {
                collect_finished(
                    &mut join_set,
                    &mut spilled,
                    &mut finished,
                    &mut failures,
                )
                .await;
            }
            print_cases(cardinality, "spilled", &spilled);
            print_cases(cardinality, "finished without spilling", &finished);
            total_spilled += spilled.len();
        }
        if self.expects_spill() {
            assert!(
                total_spilled > 0,
                "{}: no case exercised the spill path",
                chain.name
            );
        }
        assert!(
            failures.is_empty(),
            "{}: {} cases failed:\n\n{}",
            chain.name,
            failures.len(),
            failures.join("\n\n")
        );
    }
}

/// A case takes about two seconds alone in a debug build, but CI runs the
/// whole fuzz binary on a four-core runner, and has taken over a minute per
/// case there. Generous, so only a real hang fires it.
const CASE_TIMEOUT_SECS: u64 = 600;

/// Waits for one case and files it under spilled, finished or failed.
async fn collect_finished(
    join_set: &mut JoinSet<(Case, Vec<String>)>,
    spilled: &mut Vec<(Case, Vec<String>)>,
    finished: &mut Vec<(Case, Vec<String>)>,
    failures: &mut Vec<String>,
) {
    let Some(result) = join_set.join_next().await else {
        return;
    };
    match result {
        Ok((case, stages)) if stages.is_empty() => finished.push((case, stages)),
        Ok((case, stages)) => spilled.push((case, stages)),
        Err(error) => failures.push(error.to_string()),
    }
}

/// One line per case; `spilled_stages` names the aggregate operators that
/// spilled and flags when more than one did.
fn print_cases(cardinality: Cardinality, outcome: &str, cases: &[(Case, Vec<String>)]) {
    let mut lines: Vec<String> = cases
        .iter()
        .map(|(case, spilled_stages)| {
            let spilled = match spilled_stages.len() {
                0 => String::new(),
                1 => format!("  spilled: {}", spilled_stages[0]),
                _ => format!(
                    "  spilled: {} (multiple stages)",
                    spilled_stages.join(" + ")
                ),
            };
            let skip_partial = if case.shape.has_skip_partial_candidate(case.params.order)
            {
                format!(" skip_partial={:<5}", case.params.skip_partial_enabled)
            } else {
                " ".repeat(19)
            };
            format!(
                "  {:<45} {:<17} memory={:<9}{skip_partial}{spilled}",
                case.shape.name(),
                format!("{:?}", case.params.order),
                format!("{:?}", case.params.memory),
            )
        })
        .collect();
    lines.sort();
    // Enable with `RUST_LOG=debug`
    log::debug!("{cardinality:?}: {} cases {outcome}", lines.len());
    for line in lines {
        log::debug!("{line}");
    }
}
