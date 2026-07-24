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

//! Splits a physical plan into a [`StageGraph`] at shuffle boundaries.
//!
//! Mirrors the shape of Ballista's
//! `DefaultDistributedPlanner::plan_query_stages_internal`
//! (`ballista/scheduler/src/planner.rs`), but simplified for this in-process
//! model: no broadcast-join promotion, no subquery special-casing, no AQE.

use std::sync::Arc;

use datafusion::error::Result;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{
    ExecutionPlan, Partitioning, with_new_children_if_necessary,
};

use crate::exchange::{ExchangeSinkExec, ExchangeSourceExec, InMemoryExchange};
use crate::stage::{QueryStage, StageGraph, StageId};

/// Splits `plan` into a [`StageGraph`] at shuffle boundaries (hash
/// repartitioning, coalescing to a single partition, and sort-preserving
/// merges), wiring each boundary through the shared streaming `exchange`.
///
/// Stage ids are allocated in the order stages are produced during the
/// recursion, so producer stages always have a lower id than the consumer
/// stages that read from them. The final stage (the rewritten top-level
/// plan, representing the "collect final result" step) is pushed last and
/// is not wrapped in an `ExchangeSinkExec`.
///
/// The `exchange` is cloned into every `ExchangeSinkExec`/`ExchangeSourceExec`
/// built here; the executor registers the boundary channels on that same
/// instance before spawning tasks, so producers and consumers share one wire.
pub fn create_stages(
    plan: Arc<dyn ExecutionPlan>,
    exchange: &Arc<InMemoryExchange>,
) -> Result<StageGraph> {
    let mut next_id: StageId = 0;
    let (final_plan, mut stages) =
        split_at_shuffle_boundaries(plan, exchange, &mut next_id)?;

    let final_stage_id = next_id;
    let input_stage_ids = collect_source_stage_ids(&final_plan);
    let output_partition_count = final_plan
        .properties()
        .output_partitioning()
        .partition_count();
    stages.push(QueryStage {
        id: final_stage_id,
        plan: final_plan,
        output_partition_count,
        input_stage_ids,
    });

    Ok(StageGraph {
        stages,
        final_stage_id,
    })
}

/// Recursively rewrites `plan`, replacing every shuffle boundary with an
/// `ExchangeSourceExec` leaf fed by a new producer `QueryStage`. Returns the
/// rewritten plan along with every producer stage created while recursing
/// through it (in the order they were created).
fn split_at_shuffle_boundaries(
    plan: Arc<dyn ExecutionPlan>,
    exchange: &Arc<InMemoryExchange>,
    next_id: &mut StageId,
) -> Result<(Arc<dyn ExecutionPlan>, Vec<QueryStage>)> {
    if plan.children().is_empty() {
        return Ok((plan, Vec::new()));
    }

    let mut stages = Vec::new();
    let mut children = Vec::new();
    for child in plan.children() {
        let (new_child, mut child_stages) =
            split_at_shuffle_boundaries(Arc::clone(child), exchange, next_id)?;
        children.push(new_child);
        stages.append(&mut child_stages);
    }

    // CoalescePartitionsExec and SortPreservingMergeExec both collapse their
    // (possibly many) input partitions down to a single output partition;
    // they get identical shuffle-boundary treatment here.
    if plan.downcast_ref::<CoalescePartitionsExec>().is_some()
        || plan.downcast_ref::<SortPreservingMergeExec>().is_some()
    {
        let child = children[0].clone();
        let source = shuffle_boundary(
            child,
            Partitioning::RoundRobinBatch(1),
            1,
            exchange,
            next_id,
            &mut stages,
        )?;
        return Ok((source, stages));
    }

    if let Some(repart) = plan.downcast_ref::<RepartitionExec>() {
        return match repart.partitioning().clone() {
            Partitioning::Hash(exprs, p) => {
                let child = children[0].clone();
                let source = shuffle_boundary(
                    child,
                    Partitioning::Hash(exprs, p),
                    p,
                    exchange,
                    next_id,
                    &mut stages,
                )?;
                Ok((source, stages))
            }
            // v1 simplification: non-hash repartitions (e.g. round-robin)
            // exist purely to change intra-stage parallelism, not to cross a
            // shuffle boundary. This simplified scheduler has no notion of
            // "reshape partition count without a shuffle", so we simply drop
            // the node and keep the child's own partitioning/task count.
            _ => Ok((children[0].clone(), stages)),
        };
    }

    let new_plan = with_new_children_if_necessary(plan, children)?;
    Ok((new_plan, stages))
}

/// Wraps `child` in a producer `QueryStage` rooted at an `ExchangeSinkExec`
/// (pushed into `stages`), and returns an `ExchangeSourceExec` that stands in
/// for `child` at the shuffle boundary.
fn shuffle_boundary(
    child: Arc<dyn ExecutionPlan>,
    sink_partitioning: Partitioning,
    output_partition_count: usize,
    exchange: &Arc<InMemoryExchange>,
    next_id: &mut StageId,
    stages: &mut Vec<QueryStage>,
) -> Result<Arc<dyn ExecutionPlan>> {
    // Number of producer tasks = the child's own partition count, computed
    // before it gets wrapped in the sink (the sink's output partitioning
    // controls how many *buckets* each task fans out to, not how many tasks
    // the sink itself has -- see `ExchangeSinkExec`).
    let num_producer_tasks = child.properties().output_partitioning().partition_count();
    let child_schema = child.schema();
    let input_stage_ids = collect_source_stage_ids(&child);

    let stage_id = *next_id;
    *next_id += 1;

    let sink: Arc<dyn ExecutionPlan> = Arc::new(ExchangeSinkExec::try_new(
        stage_id,
        child,
        sink_partitioning,
        exchange.clone(),
    )?);

    stages.push(QueryStage {
        id: stage_id,
        plan: sink,
        output_partition_count,
        input_stage_ids,
    });

    let source: Arc<dyn ExecutionPlan> = Arc::new(ExchangeSourceExec::try_new(
        stage_id,
        child_schema,
        num_producer_tasks,
        output_partition_count,
        exchange.clone(),
    )?);

    Ok(source)
}

/// Walks `plan`'s tree looking for `ExchangeSourceExec` leaves and collects
/// their producer stage ids -- these are the upstream stages `plan` depends on.
fn collect_source_stage_ids(plan: &Arc<dyn ExecutionPlan>) -> Vec<StageId> {
    let mut ids = Vec::new();
    walk_collect_source_stage_ids(plan, &mut ids);
    ids
}

fn walk_collect_source_stage_ids(plan: &Arc<dyn ExecutionPlan>, ids: &mut Vec<StageId>) {
    if let Some(source) = plan.downcast_ref::<ExchangeSourceExec>() {
        ids.push(source.from_stage_id());
    }
    for child in plan.children() {
        walk_collect_source_stage_ids(child, ids);
    }
}

#[cfg(test)]
mod tests {
    //! Tests for `create_stages`, which splits a physical plan into a
    //! `StageGraph` at shuffle boundaries.
    //!
    //! Expected stage shape below was derived by printing
    //! `displayable(plan.as_ref()).indent(false)` for each query against this
    //! DataFusion checkout before writing assertions -- see the plans quoted
    //! in each test's doc comment.

    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::{SessionConfig, SessionContext};

    use crate::exchange::{ExchangeSinkExec, ExchangeSourceExec, InMemoryExchange};
    use crate::scheduler::create_stages;
    use crate::stage::StageGraph;

    /// Registers table `t(a INT)` backed by a 2-partition `MemTable`, against
    /// a `SessionContext` configured with `target_partitions = 4`.
    async fn context_with_partitioned_table() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch0 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![2, 3, 4]))],
        )
        .unwrap();

        let config = SessionConfig::new().with_target_partitions(4);
        let ctx = SessionContext::new_with_config(config);
        let table = MemTable::try_new(schema, vec![vec![batch0], vec![batch1]]).unwrap();
        ctx.register_table("t", Arc::new(table)).unwrap();
        ctx
    }

    fn contains_exchange_sink(plan: &Arc<dyn ExecutionPlan>) -> bool {
        plan.downcast_ref::<ExchangeSinkExec>().is_some()
            || plan.children().iter().any(|c| contains_exchange_sink(c))
    }

    fn contains_exchange_source(plan: &Arc<dyn ExecutionPlan>) -> bool {
        plan.downcast_ref::<ExchangeSourceExec>().is_some()
            || plan.children().iter().any(|c| contains_exchange_source(c))
    }

    /// Registers two 2-partition `MemTable`s, `l(k INT, v INT)` and
    /// `r(k INT, v INT)`, against a `SessionContext` configured with
    /// `target_partitions = 4` and the hash-join collect-left thresholds
    /// dropped to 0 so a join on `k` plans as a `Partitioned` hash join
    /// (`RepartitionExec: partitioning=Hash(...)` on both sides) rather than
    /// `CollectLeft`, which these tiny in-memory tables would otherwise
    /// qualify for.
    async fn context_with_two_partitioned_tables() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ]));

        let lbatch0 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();
        let lbatch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![40, 50, 60])),
            ],
        )
        .unwrap();
        let rbatch0 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![100, 200, 300])),
            ],
        )
        .unwrap();
        let rbatch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![400, 500, 600])),
            ],
        )
        .unwrap();

        let mut config = SessionConfig::new().with_target_partitions(4);
        config
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold = 0;
        config
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold_rows = 0;
        let ctx = SessionContext::new_with_config(config);

        let ltable =
            MemTable::try_new(schema.clone(), vec![vec![lbatch0], vec![lbatch1]])
                .unwrap();
        let rtable =
            MemTable::try_new(schema, vec![vec![rbatch0], vec![rbatch1]]).unwrap();
        ctx.register_table("l", Arc::new(ltable)).unwrap();
        ctx.register_table("r", Arc::new(rtable)).unwrap();
        ctx
    }

    /// Asserts the topological-order invariant `StageGraph` promises: every
    /// producer stage id referenced via `input_stage_ids` is strictly lower
    /// than the id of the stage that reads it, and `final_stage_id` is both
    /// the highest id in the graph and a stage no other stage depends on.
    fn assert_topological_order(graph: &StageGraph) {
        for stage in &graph.stages {
            for &producer_id in &stage.input_stage_ids {
                assert!(
                    producer_id < stage.id,
                    "stage {} depends on stage {} which does not have a lower id",
                    stage.id,
                    producer_id
                );
            }
        }

        let max_id = graph.stages.iter().map(|s| s.id).max().unwrap();
        assert_eq!(
            graph.final_stage_id, max_id,
            "final_stage_id should be the highest stage id in the graph"
        );
        assert!(
            !graph
                .stages
                .iter()
                .any(|s| s.input_stage_ids.contains(&graph.final_stage_id)),
            "no stage should read from the final stage"
        );
    }

    /// `SELECT a, count(*) FROM t GROUP BY a` with `target_partitions = 4`
    /// over a 2-partition input produces (verified via
    /// `displayable(..).indent(false)` against this checkout):
    ///
    /// ```text
    /// ProjectionExec: expr=[a@0 as a, count(Int64(1))@1 as count(*)]
    ///   AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[count(Int64(1))]
    ///     RepartitionExec: partitioning=Hash([a@0], 4), input_partitions=2
    ///       AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[count(Int64(1))]
    ///         DataSourceExec: partitions=2, partition_sizes=[1, 1]
    /// ```
    ///
    /// The single `RepartitionExec: partitioning=Hash(...)` is the only
    /// shuffle boundary, so this should split into exactly 2 stages: a
    /// producer stage rooted at `ExchangeSinkExec` (wrapping the
    /// partial-aggregate subtree), and a final stage (the
    /// projection/final-aggregate subtree) reading the producer's output via
    /// a `ExchangeSourceExec` leaf.
    #[tokio::test]
    async fn group_by_splits_into_producer_and_final_stage_at_hash_repartition() {
        let ctx = context_with_partitioned_table().await;
        let df = ctx
            .sql("SELECT a, count(*) FROM t GROUP BY a")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();

        let exchange = InMemoryExchange::new();
        let graph = create_stages(plan, &exchange).unwrap();

        assert_eq!(graph.stages.len(), 2, "expected exactly 2 stages");
        assert_eq!(
            graph.final_stage_id,
            graph.stages.last().unwrap().id,
            "final stage should be the last one pushed"
        );

        // Producer stage (id 0): the partial-aggregate subtree wrapped in a
        // ExchangeSinkExec, hash-partitioned into 4 buckets, with no upstream
        // shuffle dependency of its own.
        let producer = &graph.stages[0];
        assert_eq!(producer.id, 0);
        assert!(
            producer.plan.downcast_ref::<ExchangeSinkExec>().is_some(),
            "producer stage's plan root should be a ExchangeSinkExec, got: {}",
            producer.plan.name()
        );
        assert_eq!(producer.output_partition_count, 4);
        assert!(
            producer.input_stage_ids.is_empty(),
            "producer stage should have no upstream shuffle dependencies"
        );

        // Final stage: the projection/final-aggregate subtree, containing a
        // ExchangeSourceExec leaf that reads the producer stage's output, and
        // NOT itself wrapped in a ExchangeSinkExec.
        let final_stage = &graph.stages[1];
        assert_eq!(final_stage.id, graph.final_stage_id);
        assert!(
            final_stage
                .plan
                .downcast_ref::<ExchangeSinkExec>()
                .is_none(),
            "final stage must not be wrapped in a ExchangeSinkExec"
        );
        assert!(
            contains_exchange_source(&final_stage.plan),
            "final stage should contain a ExchangeSourceExec leaf"
        );
        assert_eq!(
            final_stage.input_stage_ids,
            vec![producer.id],
            "final stage should depend on the producer stage"
        );
        assert_eq!(final_stage.output_partition_count, 4);
    }

    /// `SELECT a FROM t WHERE a > 2` has no repartitioning at all (verified
    /// via `displayable(..).indent(false)`):
    ///
    /// ```text
    /// FilterExec: a@0 > 2
    ///   DataSourceExec: partitions=2, partition_sizes=[1, 1]
    /// ```
    ///
    /// so it should produce exactly 1 stage containing neither a
    /// `ExchangeSinkExec` nor a `ExchangeSourceExec` anywhere in its plan.
    #[tokio::test]
    async fn filter_query_with_no_repartition_stays_a_single_stage() {
        let ctx = context_with_partitioned_table().await;
        let df = ctx.sql("SELECT a FROM t WHERE a > 2").await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();

        let exchange = InMemoryExchange::new();
        let graph = create_stages(plan, &exchange).unwrap();

        assert_eq!(graph.stages.len(), 1, "expected exactly 1 stage");
        let only_stage = &graph.stages[0];
        assert_eq!(only_stage.id, graph.final_stage_id);
        assert!(only_stage.input_stage_ids.is_empty());
        assert!(
            !contains_exchange_sink(&only_stage.plan),
            "no-shuffle plan must not contain a ExchangeSinkExec"
        );
        assert!(
            !contains_exchange_source(&only_stage.plan),
            "no-shuffle plan must not contain a ExchangeSourceExec"
        );
    }

    /// `SELECT a, count(*) AS c FROM t GROUP BY a ORDER BY c` with
    /// `target_partitions = 4` over a 2-partition input produces (verified
    /// via `displayable(..).indent(false)` against this checkout):
    ///
    /// ```text
    /// SortPreservingMergeExec: [c@1 ASC NULLS LAST]
    ///   SortExec: expr=[c@1 ASC NULLS LAST], preserve_partitioning=[true]
    ///     ProjectionExec: expr=[a@0 as a, count(Int64(1))@1 as c]
    ///       AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[count(Int64(1))]
    ///         RepartitionExec: partitioning=Hash([a@0], 4), input_partitions=2
    ///           AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[count(Int64(1))]
    ///             DataSourceExec: partitions=2, partition_sizes=[1, 1]
    /// ```
    ///
    /// There are TWO shuffle boundaries here: the `RepartitionExec: Hash(...)`
    /// feeding the final aggregate, and the top-level
    /// `SortPreservingMergeExec` collapsing the sorted partitions down to 1.
    /// That means the split walks a chain of dependent producer stages
    /// (stage 1's subtree embeds stage 0's `ExchangeSourceExec`) rather than
    /// just a single boundary.
    ///
    /// This should split into exactly 3 stages:
    /// - stage 0: producer wrapping the partial-aggregate subtree (hash
    ///   repartition boundary), no upstream dependency.
    /// - stage 1: producer wrapping the final-aggregate/projection/sort
    ///   subtree (sort-preserving-merge boundary), depending on stage 0.
    /// - stage 2 (final): a bare `ExchangeSourceExec` reading stage 1's
    ///   output.
    #[tokio::test]
    async fn multi_boundary_chain_has_ascending_topological_order() {
        let ctx = context_with_partitioned_table().await;
        let df = ctx
            .sql("SELECT a, count(*) AS c FROM t GROUP BY a ORDER BY c")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();

        let exchange = InMemoryExchange::new();
        let graph = create_stages(plan, &exchange).unwrap();

        assert_eq!(
            graph.stages.len(),
            3,
            "expected 2 producer stages plus a final stage"
        );

        assert_topological_order(&graph);

        let stage0 = &graph.stages[0];
        assert_eq!(stage0.id, 0);
        assert!(
            stage0.plan.downcast_ref::<ExchangeSinkExec>().is_some(),
            "stage 0 should be rooted at a ExchangeSinkExec"
        );
        assert!(
            stage0.input_stage_ids.is_empty(),
            "stage 0 (partial-aggregate producer) has no upstream shuffle dependency"
        );

        let stage1 = &graph.stages[1];
        assert_eq!(stage1.id, 1);
        assert!(
            stage1.plan.downcast_ref::<ExchangeSinkExec>().is_some(),
            "stage 1 should be rooted at a ExchangeSinkExec"
        );
        assert_eq!(
            stage1.input_stage_ids,
            vec![stage0.id],
            "stage 1 (sort-preserving-merge producer) depends on stage 0"
        );
        assert!(
            contains_exchange_source(&stage1.plan),
            "stage 1's subtree should embed a ExchangeSourceExec reading stage 0"
        );

        let final_stage = &graph.stages[2];
        assert_eq!(final_stage.id, graph.final_stage_id);
        assert!(
            final_stage
                .plan
                .downcast_ref::<ExchangeSinkExec>()
                .is_none(),
            "final stage must not be wrapped in a ExchangeSinkExec"
        );
        assert!(
            final_stage
                .plan
                .downcast_ref::<ExchangeSourceExec>()
                .is_some(),
            "final stage's plan should itself be the ExchangeSourceExec reading stage 1"
        );
        assert_eq!(
            final_stage.input_stage_ids,
            vec![stage1.id],
            "final stage depends on stage 1"
        );
    }

    /// `SELECT l.k, l.v, r.v FROM l JOIN r ON l.k = r.k` with
    /// `target_partitions = 4` and the hash-join collect-left thresholds
    /// forced to 0 (see `context_with_two_partitioned_tables`) produces
    /// (verified via `displayable(..).indent(false)` against this checkout):
    ///
    /// ```text
    /// HashJoinExec: mode=Partitioned, join_type=Inner, on=[(k@0, k@0)], projection=[k@0, v@1, v@3]
    ///   RepartitionExec: partitioning=Hash([k@0], 4), input_partitions=2
    ///     DataSourceExec: partitions=2, partition_sizes=[1, 1]
    ///   RepartitionExec: partitioning=Hash([k@0], 4), input_partitions=2
    ///     DataSourceExec: partitions=2, partition_sizes=[1, 1]
    /// ```
    ///
    /// Both join inputs are independently hash-repartitioned, so this should
    /// split into exactly 3 stages: two independent producer stages (one per
    /// join side, neither depending on the other) and a final stage rooted
    /// at the (rewritten) `HashJoinExec` itself, whose plan contains two
    /// `ExchangeSourceExec` leaves -- one per producer.
    #[tokio::test]
    async fn join_with_two_shuffled_sides_has_two_input_stage_ids() {
        let ctx = context_with_two_partitioned_tables().await;
        let df = ctx
            .sql("SELECT l.k, l.v, r.v FROM l JOIN r ON l.k = r.k")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();

        let exchange = InMemoryExchange::new();
        let graph = create_stages(plan, &exchange).unwrap();

        assert_eq!(
            graph.stages.len(),
            3,
            "expected 2 independent producer stages plus a final (join) stage"
        );

        assert_topological_order(&graph);

        let final_stage = &graph.stages[2];
        assert_eq!(final_stage.id, graph.final_stage_id);
        assert!(
            final_stage
                .plan
                .downcast_ref::<ExchangeSinkExec>()
                .is_none(),
            "final (join) stage must not be wrapped in a ExchangeSinkExec"
        );
        assert_eq!(
            final_stage.input_stage_ids.len(),
            2,
            "join stage should depend on exactly 2 upstream producer stages"
        );

        let stage_ids: std::collections::HashSet<_> =
            graph.stages.iter().map(|s| s.id).collect();
        for &producer_id in &final_stage.input_stage_ids {
            assert!(
                stage_ids.contains(&producer_id),
                "referenced producer stage {producer_id} must exist in the graph"
            );
            assert!(
                producer_id < final_stage.id,
                "producer stage {producer_id} must have a lower id than the join stage"
            );
            let producer = graph
                .stages
                .iter()
                .find(|s| s.id == producer_id)
                .expect("producer stage must exist");
            assert!(
                producer.plan.downcast_ref::<ExchangeSinkExec>().is_some(),
                "producer stage {producer_id} should be rooted at a ExchangeSinkExec"
            );
            assert!(
                producer.input_stage_ids.is_empty(),
                "producer stage {producer_id} (leaf hash-repartition) has no upstream dependency"
            );
        }
    }
}
