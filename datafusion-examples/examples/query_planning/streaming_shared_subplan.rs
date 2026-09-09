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

//! See `main.rs` for how to run this example.
//!
//! # Streaming one shared subplan through a fan-out exchange
//!
//! Reusing a [`LogicalPlan`] subplan in several branches does not make their
//! physical executions share work. This example starts with one expensive join
//! subplan, derives two filtered aggregates from it, and combines them with
//! `UNION ALL`.
//!
//! The final physical plan uses two custom [`ExecutionPlan`] nodes. They are
//! part of this example, not built-in DataFusion operators:
//!
//! - `StreamingFanoutExec` executes the expensive input once, owns the bounded
//!   buffering, and exposes one output lane per consumer and input partition.
//! - Each `StreamingFanoutReaderExec` selects one consumer's lanes. The first
//!   reader keeps the fan-out visible in the physical plan; later readers
//!   reference the same fan-out through a shared [`Arc`].
//!
//! ## Extension points used
//!
//! The example also defines the planning and runtime glue:
//!
//! - `StreamingShareNode` implements [`UserDefinedLogicalNodeCore`] and marks a
//!   logical subplan with a stable sharing ID.
//! - `StreamingShareQueryPlanner` implements [`QueryPlanner`] and installs
//!   `StreamingShareExtensionPlanner`, an [`ExtensionPlanner`] that converts the
//!   logical marker into a temporary `StreamingShareMarkerExec`.
//! - `RewriteStreamingShares` implements [`PhysicalOptimizerRule`]. It counts
//!   consumers and replaces the temporary markers with one fan-out and one
//!   reader per consumer.
//! - `StreamingFanoutState` and `FanoutPartition` are example-only runtime
//!   helpers. They use [`RecordBatchReceiverStreamBuilder`] for bounded streams
//!   and [`SpawnedTask`] to run each input partition once.
//!
//! The resulting plan, drawn parent above child to match the printed plan:
//!
//! ```text
//! UnionExec
//! +-- east aggregate
//! |   +-- StreamingFanoutReaderExec (consumer 0)
//! |       +-- StreamingFanoutExec
//! |           +-- expensive join
//! +-- west aggregate
//!     +-- StreamingFanoutReaderExec (consumer 1)  ....> StreamingFanoutExec
//! ```
//!
//! The dotted connection is deliberately not a physical child edge. Consumer
//! 1's reader holds an [`Arc`] to the same fan-out but reports no children, so
//! the physical plan remains a tree and later tree walks see the shared input
//! exactly once. The queues hold at most [`CHANNEL_CAPACITY`] batches per
//! consumer and input partition. The shared output is not collected into a
//! `MemTable` or registered as a table.
//!
//! ## Where this example does not work
//!
//! - The plan is single-use because each consumer takes its queue on first
//!   execution. Plan the query again to run it again; separate `collect` calls
//!   cannot share one execution.
//! - All consumers must be polled concurrently. For example, sharing this
//!   bounded stream across both sides of a hash join can deadlock while one
//!   side is drained before the other is polled.
//! - Every planned consumer must execute. Otherwise its queue remains open and
//!   the producer blocks when that queue fills.
//! - The example assumes every copy of a marked subplan is planned identically
//!   and does not support nested shares.
//! - Dropping only the consumer streams does not immediately abort the producer
//!   while the plan remains alive.
//! - Buffering is bounded by batch count, not bytes, and is not registered with
//!   the [`MemoryPool`]. This is an illustrative extension, not a production
//!   operator.
//!
//! The supported alternative today is [`DataFrame::cache`], which materializes
//! the shared result before its consumers run. Streaming the result instead is
//! discussed in <https://github.com/apache/datafusion/issues/8777>.
//!
//! [`DataFrame::cache`]: datafusion::dataframe::DataFrame::cache
//! [`MemoryPool`]: datafusion::execution::memory_pool::MemoryPool
//!

use std::collections::HashMap;
use std::fmt::{self, Formatter};
use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use arrow::array::{RecordBatch, record_batch};
use arrow::util::pretty::print_batches;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::config::ConfigOptions;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::stats::Statistics;
use datafusion::common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion::common::{
    DFSchemaRef, DataFusionError, Result, SharedResult, assert_batches_sorted_eq,
    exec_err, not_impl_err, plan_err,
};
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::{
    SendableRecordBatchStream, SessionStateBuilder, TaskContext,
};
use datafusion::functions_aggregate::expr_fn::sum;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{
    Extension, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNode,
    UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::{
    PhysicalOptimizerRule, ensure_coop::EnsureCooperative,
    sanity_checker::SanityCheckPlan,
};
use datafusion::physical_plan::execution_plan::{
    CardinalityEffect, EvaluationType, SchedulingType,
};
use datafusion::physical_plan::stream::{
    RecordBatchReceiverStreamBuilder, RecordBatchStreamAdapter,
};
use datafusion::physical_plan::{
    ChildStats, ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
    ExecutionPlanProperties, PlanProperties, ReplaceChildrenOptions, StatisticsArgs,
    collect, displayable,
};
use datafusion::physical_planner::{
    DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner,
};
use datafusion::prelude::*;
use futures::future::{BoxFuture, Shared};
use futures::{FutureExt, StreamExt};
use tokio::sync::mpsc::Sender;
use tokio::time::timeout;

const CHANNEL_CAPACITY: usize = 1;
const COLLECT_TIMEOUT: Duration = Duration::from_secs(30);
static NEXT_STREAMING_SHARE_ID: AtomicUsize = AtomicUsize::new(0);

/// Streams one shared subplan to two consumers under `UNION ALL`.
pub async fn streaming_shared_subplan() -> Result<()> {
    let metrics = Arc::new(FanoutMetrics::default());
    // One target partition makes the expected source execution count explicit.
    // A small batch size ensures more batches than one queue can hold.
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(2);
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .with_query_planner(Arc::new(StreamingShareQueryPlanner))
        // This runs after DataFusion's built-in physical optimizer rules, so
        // the one visible producer subtree is already optimized.
        .with_physical_optimizer_rule(Arc::new(RewriteStreamingShares {
            metrics: Arc::clone(&metrics),
        }))
        // The custom rewrite is appended after the default rules, so run the
        // final scheduling and invariant checks again on its output.
        .with_physical_optimizer_rule(Arc::new(EnsureCooperative::new()))
        .with_physical_optimizer_rule(Arc::new(SanityCheckPlan::new()))
        .build();
    let ctx = SessionContext::new_with_state(state);

    // Multiple input batches make it clear that the fan-out forwards a stream
    // of batches; it does not first collect the complete join result.
    let orders = ctx
        .read_batches([
            record_batch!(("customer_id", Int32, [1, 1]), ("amount", Int64, [10, 20]))?,
            record_batch!(("customer_id", Int32, [2, 3]), ("amount", Int64, [5, 7]))?,
            record_batch!(("customer_id", Int32, [3, 4]), ("amount", Int64, [8, 100]))?,
        ])?
        .alias("orders")?;
    let customers = ctx
        .read_batch(record_batch!(
            ("customer_id", Int32, [1, 2, 3, 4]),
            ("region", Utf8, ["east", "west", "east", "west"])
        )?)?
        .alias("customers")?;

    // This stands in for any expensive subplan whose output several downstream
    // branches need.
    let expensive_join = orders
        .join(
            customers,
            JoinType::Inner,
            &["customer_id"],
            &["customer_id"],
            None,
        )?
        .select_columns(&["region", "amount"])?;
    let (session_state, expensive_join) = expensive_join.into_parts();

    // Repeating the logical subplan does not share physical execution.
    let unshared_query = union_query(expensive_join.clone())?;
    let unshared_plan = session_state.create_physical_plan(&unshared_query).await?;
    let unshared_text = displayable(unshared_plan.as_ref()).indent(true).to_string();
    println!("\nWithout sharing\nPhysical plan:\n{unshared_text}");
    assert_eq!(unshared_text.matches("HashJoinExec").count(), 2);

    // Mark the expensive logical subplan once, then derive independent
    // consumers from it.
    let shared = mark_shared_subplan(expensive_join);
    let query = union_query(shared)?;
    let shared_plan = session_state.create_physical_plan(&query).await?;
    let shared_text = displayable(shared_plan.as_ref()).indent(true).to_string();
    println!("\nWith streaming sharing\nPhysical plan:\n{shared_text}");

    assert_eq!(shared_text.matches("HashJoinExec").count(), 1);
    assert_eq!(shared_text.matches("StreamingFanoutExec").count(), 1);
    assert_eq!(shared_text.matches("StreamingFanoutReaderExec").count(), 2);

    let Ok(results) =
        timeout(COLLECT_TIMEOUT, collect(shared_plan, ctx.task_ctx())).await
    else {
        return exec_err!(
            "the shared plan did not finish within {COLLECT_TIMEOUT:?}: \
             a consumer may not have been polled"
        );
    };
    let results = results?;
    print_batches(&results)?;
    assert_batches_sorted_eq!(
        [
            "+--------+--------------+",
            "| region | total_amount |",
            "+--------+--------------+",
            "| east   | 45           |",
            "| west   | 105          |",
            "+--------+--------------+",
        ],
        &results
    );

    let source_executions = metrics.source_partition_executions.load(Ordering::SeqCst);
    assert_eq!(
        source_executions, 1,
        "the shared source partition must execute once"
    );
    let broadcast = metrics.batches_broadcast.load(Ordering::SeqCst);
    assert!(
        broadcast > CHANNEL_CAPACITY,
        "the whole shared stream ({broadcast} batches) fits in one consumer queue, \
         so this run does not demonstrate streaming"
    );

    println!("Source executions: {source_executions}; batches broadcast: {broadcast}");
    Ok(())
}

fn union_query(input: LogicalPlan) -> Result<LogicalPlan> {
    let east = regional_total(input.clone(), "east")?;
    let west = regional_total(input, "west")?;
    LogicalPlanBuilder::from(east).union(west)?.build()
}

fn regional_total(input: LogicalPlan, region: &'static str) -> Result<LogicalPlan> {
    LogicalPlanBuilder::from(input)
        .filter(col("region").eq(lit(region)))?
        .aggregate(
            Vec::<Expr>::new(),
            vec![sum(col("amount")).alias("total_amount")],
        )?
        .project(vec![lit(region).alias("region"), col("total_amount")])?
        .build()
}

// ---------------------------------------------------------------------------
// Logical extension: mark a subplan for reuse
// ---------------------------------------------------------------------------

/// Wraps a logical subplan in an extension node with a stable ID.
///
/// Clones of the returned plan retain that ID, allowing the physical rewrite
/// to recognize consumers of the same shared stream.
fn mark_shared_subplan(input: LogicalPlan) -> LogicalPlan {
    let id = NEXT_STREAMING_SHARE_ID.fetch_add(1, Ordering::Relaxed);
    LogicalPlan::Extension(Extension {
        node: Arc::new(StreamingShareNode { id, input }),
    })
}

#[derive(Debug, Eq, PartialEq, PartialOrd, Hash)]
struct StreamingShareNode {
    id: usize,
    input: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for StreamingShareNode {
    fn name(&self) -> &str {
        "StreamingShare"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "StreamingShare: id={}", self.id)
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if inputs.len() != 1 {
            return plan_err!("StreamingShareNode requires exactly one input");
        }
        Ok(Self {
            id: self.id,
            input: inputs.swap_remove(0),
        })
    }
}

// ---------------------------------------------------------------------------
// Extension planner: preserve the marker through physical optimization
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct StreamingShareQueryPlanner;

#[async_trait]
impl QueryPlanner for StreamingShareQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            StreamingShareExtensionPlanner,
        )])
        .create_physical_plan(logical_plan, session_state)
        .await
    }
}

struct StreamingShareExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for StreamingShareExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<StreamingShareNode>() else {
            return Ok(None);
        };
        if physical_inputs.len() != 1 {
            return plan_err!("StreamingShareNode requires one physical input");
        }
        Ok(Some(Arc::new(StreamingShareMarkerExec::new(
            node.id,
            Arc::clone(&physical_inputs[0]),
        ))))
    }
}

/// A pass-through node that keeps the sharing ID in the physical plan.
#[derive(Debug)]
struct StreamingShareMarkerExec {
    id: usize,
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl StreamingShareMarkerExec {
    fn new(id: usize, input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            id,
            properties: Arc::clone(input.properties()),
            input,
        }
    }
}

impl DisplayAs for StreamingShareMarkerExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "StreamingShareMarkerExec: id={}", self.id)
    }
}

impl ExecutionPlan for StreamingShareMarkerExec {
    fn name(&self) -> &str {
        "StreamingShareMarkerExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return plan_err!("StreamingShareMarkerExec requires one child");
        }
        Ok(Arc::new(Self::new(self.id, children.swap_remove(0))))
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }

    // The marker is transparent to optimizer statistics and cardinality.
    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        Ok(Arc::clone(&input_stats[0]))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }
}

// ---------------------------------------------------------------------------
// Physical rewrite: one exchange plus one reader per consumer
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct RewriteStreamingShares {
    metrics: Arc<FanoutMetrics>,
}

impl PhysicalOptimizerRule for RewriteStreamingShares {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut consumer_counts = HashMap::<usize, usize>::new();
        plan.apply(|plan| {
            if let Some(marker) = plan.downcast_ref::<StreamingShareMarkerExec>() {
                *consumer_counts.entry(marker.id).or_default() += 1;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;

        let mut fanouts: HashMap<usize, Arc<StreamingFanoutExec>> = HashMap::new();
        let mut next_consumers = HashMap::<usize, usize>::new();

        plan.transform_up(|plan| {
            let Some(marker) = plan.downcast_ref::<StreamingShareMarkerExec>() else {
                return Ok(Transformed::no(plan));
            };
            let id = marker.id;
            let input = Arc::clone(&marker.input);
            let Some(&consumer_count) = consumer_counts.get(&id) else {
                return plan_err!("Streaming share {id} has no registered consumers");
            };
            let next_consumer = next_consumers.entry(id).or_default();
            let consumer = *next_consumer;
            *next_consumer += 1;

            let (fanout, visible_child) = if let Some(fanout) = fanouts.get(&id) {
                (Arc::clone(fanout), false)
            } else {
                let fanout = Arc::new(StreamingFanoutExec::try_new(
                    id,
                    input,
                    consumer_count,
                    Arc::clone(&self.metrics),
                )?);
                fanouts.insert(id, Arc::clone(&fanout));
                (fanout, true)
            };
            let replacement: Arc<dyn ExecutionPlan> = Arc::new(
                StreamingFanoutReaderExec::try_new(id, consumer, fanout, visible_child)?,
            );
            Ok(Transformed::yes(replacement))
        })
        .data()
    }

    fn name(&self) -> &str {
        "rewrite_streaming_shares"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Owns the shared producer and one bounded queue per consumer and input
/// partition.
///
/// Readers select queues through [`StreamingFanoutExec::consumer_stream`].
/// Executing this node directly would incorrectly model the duplicated lanes as
/// disjoint output partitions, so [`ExecutionPlan::execute`] rejects that use.
#[derive(Debug)]
struct StreamingFanoutExec {
    id: usize,
    input: Arc<dyn ExecutionPlan>,
    state: Arc<StreamingFanoutState>,
    consumer_count: usize,
    input_partition_count: usize,
    properties: Arc<PlanProperties>,
}

impl StreamingFanoutExec {
    fn try_new(
        id: usize,
        input: Arc<dyn ExecutionPlan>,
        consumer_count: usize,
        metrics: Arc<FanoutMetrics>,
    ) -> Result<Self> {
        let state = Arc::new(StreamingFanoutState::new(
            Arc::clone(&input),
            consumer_count,
            metrics,
        ));
        Self::with_state(id, input, consumer_count, state)
    }

    fn with_state(
        id: usize,
        input: Arc<dyn ExecutionPlan>,
        consumer_count: usize,
        state: Arc<StreamingFanoutState>,
    ) -> Result<Self> {
        let input_partition_count = input.output_partitioning().partition_count();
        if input_partition_count == 0 || consumer_count == 0 {
            return plan_err!(
                "Streaming fan-out requires at least one input partition and one \
                 consumer, got {input_partition_count} and {consumer_count}"
            );
        }
        let properties = Arc::new(
            input
                .properties()
                .as_ref()
                .clone()
                .with_evaluation_type(EvaluationType::Eager)
                .with_scheduling_type(SchedulingType::Cooperative),
        );
        Ok(Self {
            id,
            input,
            state,
            consumer_count,
            input_partition_count,
            properties,
        })
    }
}

impl DisplayAs for StreamingFanoutExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "StreamingFanoutExec: id={}, consumers={}, input_partitions={}, capacity={}",
            self.id, self.consumer_count, self.input_partition_count, CHANNEL_CAPACITY
        )
    }
}

impl ExecutionPlan for StreamingFanoutExec {
    fn name(&self) -> &str {
        "StreamingFanoutExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return plan_err!("StreamingFanoutExec requires one child");
        }
        let input = children.swap_remove(0);
        if input.output_partitioning().partition_count() != self.input_partition_count {
            return plan_err!(
                "StreamingFanoutExec cannot change its input partition count"
            );
        }
        self.state.replace_input(Arc::clone(&input));
        Ok(Arc::new(Self::with_state(
            self.id,
            input,
            self.consumer_count,
            Arc::clone(&self.state),
        )?))
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        exec_err!(
            "StreamingFanoutExec {} must be executed through a \
             StreamingFanoutReaderExec",
            self.id
        )
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!(
            "StreamingFanoutExec {} cannot be re-executed; plan the shared \
             subplan again",
            self.id
        )
    }
}

impl StreamingFanoutExec {
    fn consumer_stream(
        &self,
        consumer: usize,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if consumer >= self.consumer_count {
            return exec_err!(
                "Streaming fan-out {} has {} consumers, asked for {consumer}",
                self.id,
                self.consumer_count
            );
        }
        if partition >= self.input_partition_count {
            return exec_err!(
                "Streaming fan-out {} has {} input partitions, asked for {partition}",
                self.id,
                self.input_partition_count
            );
        }
        self.state.stream(consumer, partition, context)
    }
}

/// Reads one consumer's view of the shared subplan.
#[derive(Debug)]
struct StreamingFanoutReaderExec {
    id: usize,
    consumer: usize,
    fanout: Arc<dyn ExecutionPlan>,
    // Only one reader exposes the shared fan-out as a child, keeping the plan a tree.
    visible_child: bool,
    properties: Arc<PlanProperties>,
}

impl StreamingFanoutReaderExec {
    fn try_new(
        id: usize,
        consumer: usize,
        fanout: Arc<dyn ExecutionPlan>,
        visible_child: bool,
    ) -> Result<Self> {
        let Some(exchange) = fanout.downcast_ref::<StreamingFanoutExec>() else {
            return plan_err!(
                "StreamingFanoutReaderExec requires a StreamingFanoutExec, got {}",
                fanout.name()
            );
        };
        let properties = Arc::new(
            exchange
                .input
                .properties()
                .as_ref()
                .clone()
                .with_evaluation_type(EvaluationType::Eager)
                .with_scheduling_type(SchedulingType::Cooperative),
        );
        Ok(Self {
            id,
            consumer,
            fanout,
            visible_child,
            properties,
        })
    }
}

impl DisplayAs for StreamingFanoutReaderExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "StreamingFanoutReaderExec: id={}, consumer={}",
            self.id, self.consumer
        )
    }
}

impl ExecutionPlan for StreamingFanoutReaderExec {
    fn name(&self) -> &str {
        "StreamingFanoutReaderExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        if self.visible_child {
            vec![true]
        } else {
            vec![]
        }
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        if self.visible_child {
            vec![&self.fanout]
        } else {
            vec![]
        }
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if self.visible_child {
            if children.len() != 1 {
                return plan_err!(
                    "The first StreamingFanoutReaderExec requires one child"
                );
            }
            return Ok(Arc::new(Self::try_new(
                self.id,
                self.consumer,
                children.swap_remove(0),
                true,
            )?));
        }
        if !children.is_empty() {
            return plan_err!(
                "Additional StreamingFanoutReaderExec nodes cannot have children"
            );
        }
        Ok(self)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let Some(fanout) = self.fanout.downcast_ref::<StreamingFanoutExec>() else {
            return exec_err!(
                "StreamingFanoutReaderExec {} lost its StreamingFanoutExec",
                self.id
            );
        };
        fanout.consumer_stream(self.consumer, partition, context)
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!(
            "StreamingFanoutReaderExec {} cannot be re-executed; plan the shared \
             subplan again",
            self.id
        )
    }
}

// ---------------------------------------------------------------------------
// Runtime fan-out: one bounded queue per consumer and input partition
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct FanoutMetrics {
    source_partition_executions: AtomicUsize,
    batches_broadcast: AtomicUsize,
}

#[derive(Debug)]
struct StreamingFanoutState {
    input: RwLock<Arc<dyn ExecutionPlan>>,
    partitions: Vec<Arc<FanoutPartition>>,
    metrics: Arc<FanoutMetrics>,
}

impl StreamingFanoutState {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        consumer_count: usize,
        metrics: Arc<FanoutMetrics>,
    ) -> Self {
        let partition_count = input.output_partitioning().partition_count();
        let schema = input.schema();
        let partitions = (0..partition_count)
            .map(|_| Arc::new(FanoutPartition::new(consumer_count, &schema)))
            .collect();
        Self {
            input: RwLock::new(input),
            partitions,
            metrics,
        }
    }

    fn replace_input(&self, input: Arc<dyn ExecutionPlan>) {
        *self.input.write().unwrap() = input;
    }

    fn stream(
        self: &Arc<Self>,
        consumer: usize,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = Arc::clone(&self.input.read().unwrap());
        let schema = input.schema();
        let Some(partition_state) = self.partitions.get(partition) else {
            return exec_err!("Streaming fan-out partition {partition} not found");
        };
        let receiver = partition_state.take_receiver(consumer)?;
        let producer = partition_state.start(
            &input,
            partition,
            context,
            Arc::clone(&self.metrics),
        )?;

        // Retaining `self` in the stream keeps the producer task alive until
        // this query execution finishes.
        let state = Arc::clone(self);
        let batches = futures::stream::unfold(
            (receiver, state),
            |(mut receiver, state)| async move {
                let item = receiver.stream.next().await?;
                Some((item, (receiver, state)))
            },
        );
        // Join the producer after the queue closes so task failures do not look
        // like a successful, truncated stream.
        let producer_failure = futures::stream::once(producer)
            .filter_map(|result| async move { result.err() })
            .map(|error| Err(DataFusionError::Shared(error)));
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            batches.chain(producer_failure),
        )))
    }
}

type ProducerHandle = Shared<BoxFuture<'static, SharedResult<()>>>;

struct FanoutPartition {
    senders: Mutex<Option<Vec<Sender<Result<RecordBatch>>>>>,
    receivers: Vec<Mutex<Option<FanoutReceiver>>>,
    producer: Mutex<Option<ProducerHandle>>,
}

impl fmt::Debug for FanoutPartition {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("FanoutPartition").finish_non_exhaustive()
    }
}

impl FanoutPartition {
    fn new(consumer_count: usize, schema: &arrow::datatypes::SchemaRef) -> Self {
        let mut senders = Vec::with_capacity(consumer_count);
        let mut receivers = Vec::with_capacity(consumer_count);
        for _ in 0..consumer_count {
            let builder = RecordBatchReceiverStreamBuilder::new(
                Arc::clone(schema),
                CHANNEL_CAPACITY,
            );
            senders.push(builder.tx());
            receivers.push(Mutex::new(Some(FanoutReceiver {
                stream: builder.build(),
            })));
        }
        Self {
            senders: Mutex::new(Some(senders)),
            receivers,
            producer: Mutex::new(None),
        }
    }

    fn take_receiver(&self, consumer: usize) -> Result<FanoutReceiver> {
        let Some(receiver) = self.receivers.get(consumer) else {
            return exec_err!("Streaming fan-out consumer {consumer} not found");
        };
        let Some(receiver) = receiver.lock().unwrap().take() else {
            return exec_err!(
                "Streaming fan-out consumer {consumer} was executed more than once"
            );
        };
        Ok(receiver)
    }

    fn start(
        &self,
        input: &Arc<dyn ExecutionPlan>,
        partition: usize,
        context: Arc<TaskContext>,
        metrics: Arc<FanoutMetrics>,
    ) -> Result<ProducerHandle> {
        let mut senders_slot = self.senders.lock().unwrap();
        if let Some(producer) = self.producer.lock().unwrap().clone() {
            return Ok(producer);
        }
        if senders_slot.is_none() {
            return exec_err!(
                "Streaming fan-out partition {partition} has no queues to write to"
            );
        }
        let input = input.execute(partition, context)?;
        let senders = senders_slot.take().expect("checked above");
        metrics
            .source_partition_executions
            .fetch_add(1, Ordering::SeqCst);
        let task = SpawnedTask::spawn(run_producer(input, senders, metrics));
        let producer: BoxFuture<'static, SharedResult<()>> = Box::pin(async move {
            match task.join().await {
                Ok(result) => result.map_err(Arc::new),
                Err(error) => {
                    Err(Arc::new(DataFusionError::ExecutionJoin(Box::new(error))))
                }
            }
        });
        let producer = producer.shared();
        *self.producer.lock().unwrap() = Some(producer.clone());
        Ok(producer)
    }
}

struct FanoutReceiver {
    stream: SendableRecordBatchStream,
}

impl fmt::Debug for FanoutReceiver {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("FanoutReceiver").finish_non_exhaustive()
    }
}

async fn run_producer(
    mut input: SendableRecordBatchStream,
    mut senders: Vec<Sender<Result<RecordBatch>>>,
    metrics: Arc<FanoutMetrics>,
) -> Result<()> {
    while let Some(item) = input.next().await {
        let is_error = item.is_err();
        if !is_error {
            metrics.batches_broadcast.fetch_add(1, Ordering::SeqCst);
        }
        let item = item.map_err(Arc::new);
        broadcast(&mut senders, &item).await;
        if is_error || senders.is_empty() {
            break;
        }
    }
    Ok(())
}

async fn broadcast(
    senders: &mut Vec<Sender<Result<RecordBatch>>>,
    item: &SharedResult<RecordBatch>,
) {
    let mut index = 0;
    while index < senders.len() {
        let sender = senders[index].clone();
        let Ok(permit) = sender.reserve_owned().await else {
            senders.swap_remove(index);
            continue;
        };
        permit.send(match item {
            Ok(batch) => Ok(batch.clone()),
            Err(error) => Err(DataFusionError::Shared(Arc::clone(error))),
        });
        index += 1;
    }
}
