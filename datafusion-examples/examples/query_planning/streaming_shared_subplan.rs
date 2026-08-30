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
//! # Streaming one shared subplan to multiple consumers
//!
//! Reusing a [`LogicalPlan`] subplan in several branches does not make their
//! physical executions share work. This example starts with one expensive join
//! subplan, derives two filtered aggregates from it, and combines them with
//! `UNION ALL`.
//!
//! It defines two custom [`ExecutionPlan`] nodes using DataFusion's extension
//! APIs. They are part of this example, not built-in DataFusion operators:
//!
//! - `StreamingFanoutExec` owns the expensive input and executes it once. Shared
//!   state sends each [`RecordBatch`] to one bounded receiver stream per
//!   consumer, built with [`RecordBatchReceiverStreamBuilder`].
//! - `StreamingFanoutReaderExec` is a leaf node that reads an additional
//!   consumer's queue from the same shared state.
//!
//! ```text
//! expensive join (executed once)
//!              |
//!     StreamingFanoutExec
//!          +---> east aggregate ------------------------+
//!          |                                             |
//!          +---> StreamingFanoutReaderExec               +---> UNION ALL
//!                         +---> west aggregate -----------+
//! ```
//!
//! The reader is connected to the fan-out through shared Rust state, so it has
//! no physical child. The queues hold at most one batch per consumer and
//! partition. The physical rewrite counts consumers first and creates all
//! queues before execution. The shared output is not collected into a
//! `MemTable` or registered as a table.
//!
//! ## Where this example does not work
//!
//! - Separate `collect` calls create separate physical plans and do not share
//!   this execution-scoped state.
//! - All consumers must be polled concurrently. For example, sharing this
//!   bounded stream across both sides of a hash join can deadlock while one
//!   side is drained before the other is polled.
//!

use std::collections::HashMap;
use std::fmt::{self, Formatter};
use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use arrow::array::{RecordBatch, record_batch};
use arrow::util::pretty::print_batches;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::config::ConfigOptions;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion::common::{
    DFSchemaRef, DataFusionError, Result, SharedResult, assert_batches_sorted_eq,
    exec_err, plan_err,
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
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::stream::{
    RecordBatchReceiverStreamBuilder, RecordBatchStreamAdapter,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    collect, displayable,
};
use datafusion::physical_planner::{
    DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner,
};
use datafusion::prelude::*;
use futures::StreamExt;
use tokio::sync::mpsc::Sender;

const CHANNEL_CAPACITY: usize = 1;
static NEXT_STREAMING_SHARE_ID: AtomicUsize = AtomicUsize::new(0);

/// Streams one shared subplan to two consumers under `UNION ALL`.
pub async fn streaming_shared_subplan() -> Result<()> {
    let metrics = Arc::new(FanoutMetrics::default());
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
    assert_eq!(shared_text.matches("StreamingFanoutReaderExec").count(), 1);

    let results = collect(shared_plan, ctx.task_ctx()).await?;
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

    assert_eq!(
        metrics.source_partition_executions.load(Ordering::SeqCst),
        1,
        "the shared source partition must execute once"
    );
    assert!(
        metrics.batches_broadcast.load(Ordering::SeqCst) > 1,
        "the example must broadcast multiple batches"
    );
    assert!(
        metrics.max_buffered_batches.load(Ordering::SeqCst) <= CHANNEL_CAPACITY,
        "a consumer queue exceeded its configured bound"
    );

    println!(
        "Source executions: {}; batches broadcast: {}; max queued per consumer: {}",
        metrics.source_partition_executions.load(Ordering::SeqCst),
        metrics.batches_broadcast.load(Ordering::SeqCst),
        metrics.max_buffered_batches.load(Ordering::SeqCst),
    );
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

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return plan_err!("StreamingShareMarkerExec requires one child");
        }
        Ok(Arc::new(Self::new(self.id, children.swap_remove(0))))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}

// ---------------------------------------------------------------------------
// Physical rewrite: one producer plus reader leaves
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

        let mut shares: HashMap<usize, Arc<StreamingFanoutState>> = HashMap::new();
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

            let replacement: Arc<dyn ExecutionPlan> = if let Some(state) = shares.get(&id)
            {
                Arc::new(StreamingFanoutReaderExec::new(
                    id,
                    consumer,
                    Arc::clone(state),
                ))
            } else {
                let state = Arc::new(StreamingFanoutState::new(
                    Arc::clone(&input),
                    consumer_count,
                    Arc::clone(&self.metrics),
                ));
                shares.insert(id, Arc::clone(&state));
                Arc::new(StreamingFanoutExec::new(id, consumer, input, state))
            };
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

/// Example-defined node that owns the input and starts the streaming fan-out.
#[derive(Debug)]
struct StreamingFanoutExec {
    id: usize,
    consumer: usize,
    input: Arc<dyn ExecutionPlan>,
    state: Arc<StreamingFanoutState>,
    properties: Arc<PlanProperties>,
}

impl StreamingFanoutExec {
    fn new(
        id: usize,
        consumer: usize,
        input: Arc<dyn ExecutionPlan>,
        state: Arc<StreamingFanoutState>,
    ) -> Self {
        Self {
            id,
            consumer,
            properties: Arc::clone(input.properties()),
            input,
            state,
        }
    }
}

impl DisplayAs for StreamingFanoutExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "StreamingFanoutExec: id={}, consumer={}, capacity={}",
            self.id, self.consumer, CHANNEL_CAPACITY
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

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return plan_err!("StreamingFanoutExec requires one child");
        }
        let input = children.swap_remove(0);
        self.state.replace_input(Arc::clone(&input));
        Ok(Arc::new(Self::new(
            self.id,
            self.consumer,
            input,
            Arc::clone(&self.state),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.state.stream(self.consumer, partition, context)
    }
}

/// Example-defined leaf that reads the fan-out through shared Rust state.
#[derive(Debug)]
struct StreamingFanoutReaderExec {
    id: usize,
    consumer: usize,
    state: Arc<StreamingFanoutState>,
    properties: Arc<PlanProperties>,
}

impl StreamingFanoutReaderExec {
    fn new(id: usize, consumer: usize, state: Arc<StreamingFanoutState>) -> Self {
        let properties = Arc::clone(state.input.read().unwrap().properties());
        Self {
            id,
            consumer,
            state,
            properties,
        }
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

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return plan_err!("StreamingFanoutReaderExec cannot have children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.state.stream(self.consumer, partition, context)
    }
}

// ---------------------------------------------------------------------------
// Runtime fan-out: one bounded queue per consumer and input partition
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct FanoutMetrics {
    source_partition_executions: AtomicUsize,
    batches_broadcast: AtomicUsize,
    max_buffered_batches: AtomicUsize,
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
        partition_state.start(&input, partition, context, Arc::clone(&self.metrics))?;

        // Retaining `self` in the stream keeps the producer task alive until
        // this query execution finishes.
        let state = Arc::clone(self);
        let stream = futures::stream::unfold(
            (receiver, state),
            |(mut receiver, state)| async move {
                let item = receiver.stream.next().await?;
                Some((item, (receiver, state)))
            },
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[derive(Debug)]
struct FanoutPartition {
    senders: Mutex<Option<Vec<Sender<Result<RecordBatch>>>>>,
    receivers: Vec<Mutex<Option<FanoutReceiver>>>,
    task: Mutex<Option<SpawnedTask<()>>>,
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
            task: Mutex::new(None),
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
    ) -> Result<()> {
        let Some(senders) = self.senders.lock().unwrap().take() else {
            return Ok(());
        };
        let input = input.execute(partition, context)?;
        metrics
            .source_partition_executions
            .fetch_add(1, Ordering::SeqCst);
        let task = SpawnedTask::spawn(run_producer(input, senders, metrics));
        *self.task.lock().unwrap() = Some(task);
        Ok(())
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
) {
    while let Some(item) = input.next().await {
        let is_error = item.is_err();
        if !is_error {
            metrics.batches_broadcast.fetch_add(1, Ordering::SeqCst);
        }
        let item = item.map_err(Arc::new);
        broadcast(&mut senders, &item, &metrics.max_buffered_batches).await;
        if is_error || senders.is_empty() {
            break;
        }
    }
}

async fn broadcast(
    senders: &mut Vec<Sender<Result<RecordBatch>>>,
    item: &SharedResult<RecordBatch>,
    max_buffered_batches: &AtomicUsize,
) {
    let mut index = 0;
    while index < senders.len() {
        let sender = senders[index].clone();
        let Ok(permit) = sender.reserve_owned().await else {
            senders.swap_remove(index);
            continue;
        };

        let buffered = senders[index].max_capacity() - senders[index].capacity();
        max_buffered_batches.fetch_max(buffered, Ordering::SeqCst);
        permit.send(match item {
            Ok(batch) => Ok(batch.clone()),
            Err(error) => Err(DataFusionError::Shared(Arc::clone(error))),
        });
        index += 1;
    }
}
