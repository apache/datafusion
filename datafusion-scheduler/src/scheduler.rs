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
