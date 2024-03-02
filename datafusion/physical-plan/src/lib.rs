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

//! Traits for physical query plan, supporting parallel execution for partitioned relations.

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use crate::coalesce_partitions::CoalescePartitionsExec;
use crate::display::DisplayableExecutionPlan;
use crate::metrics::MetricsSet;
use crate::repartition::RepartitionExec;
use crate::sorts::sort_preserving_merge::SortPreservingMergeExec;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::Transformed;
use datafusion_common::utils::DataPtr;
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{
    EquivalenceProperties, LexOrdering, PhysicalSortExpr, PhysicalSortRequirement,
};

use futures::stream::TryStreamExt;
use tokio::task::JoinSet;

mod ordering;
mod topk;
mod visitor;

pub mod aggregates;
pub mod analyze;
pub mod coalesce_batches;
pub mod coalesce_partitions;
pub mod common;
pub mod display;
pub mod empty;
pub mod explain;
pub mod filter;
pub mod insert;
pub mod joins;
pub mod limit;
pub mod memory;
pub mod metrics;
pub mod placeholder_row;
pub mod projection;
pub mod recursive_query;
pub mod repartition;
pub mod sorts;
pub mod stream;
pub mod streaming;
pub mod tree_node;
pub mod udaf;
pub mod union;
pub mod unnest;
pub mod values;
pub mod windows;
pub mod work_table;

pub use crate::display::{DefaultDisplay, DisplayAs, DisplayFormatType, VerboseDisplay};
pub use crate::metrics::Metric;
pub use crate::ordering::InputOrderMode;
pub use crate::topk::TopK;
pub use crate::visitor::{accept, visit_execution_plan, ExecutionPlanVisitor};

pub use datafusion_common::hash_utils;
pub use datafusion_common::utils::project_schema;
pub use datafusion_common::{internal_err, ColumnStatistics, Statistics};
pub use datafusion_expr::{Accumulator, ColumnarValue};
pub use datafusion_physical_expr::window::WindowExpr;
pub use datafusion_physical_expr::{
    expressions, functions, udf, AggregateExpr, Distribution, Partitioning, PhysicalExpr,
};

// Backwards compatibility
pub use crate::stream::EmptyRecordBatchStream;
pub use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};

/// Represent nodes in the DataFusion Physical Plan.
///
/// Calling [`execute`] produces an `async` [`SendableRecordBatchStream`] of
/// [`RecordBatch`] that incrementally computes a partition of the
/// `ExecutionPlan`'s output from its input. See [`Partitioning`] for more
/// details on partitioning.
///
/// Methods such as [`Self::schema`] and [`Self::properties`] communicate
/// properties of the output to the DataFusion optimizer, and methods such as
/// [`required_input_distribution`] and [`required_input_ordering`] express
/// requirements of the `ExecutionPlan` from its input.
///
/// [`ExecutionPlan`] can be displayed in a simplified form using the
/// return value from [`displayable`] in addition to the (normally
/// quite verbose) `Debug` output.
///
/// [`execute`]: ExecutionPlan::execute
/// [`required_input_distribution`]: ExecutionPlan::required_input_distribution
/// [`required_input_ordering`]: ExecutionPlan::required_input_ordering
pub trait ExecutionPlan: Debug + DisplayAs + Send + Sync {
    /// Returns the execution plan as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.properties().schema().clone()
    }

    /// Return properties of the output of the `ExecutionPlan`, such as output
    /// ordering(s), partitioning information etc.
    ///
    /// This information is available via methods on [`ExecutionPlanProperties`]
    /// trait, which is implemented for all `ExecutionPlan`s.
    fn properties(&self) -> &PlanProperties;

    /// Specifies the data distribution requirements for all the
    /// children for this `ExecutionPlan`, By default it's [[Distribution::UnspecifiedDistribution]] for each child,
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution; self.children().len()]
    }

    /// Specifies the ordering required for all of the children of this
    /// `ExecutionPlan`.
    ///
    /// For each child, it's the local ordering requirement within
    /// each partition rather than the global ordering
    ///
    /// NOTE that checking `!is_empty()` does **not** check for a
    /// required input ordering. Instead, the correct check is that at
    /// least one entry must be `Some`
    fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
        vec![None; self.children().len()]
    }

    /// Returns `false` if this `ExecutionPlan`'s implementation may reorder
    /// rows within or between partitions.
    ///
    /// For example, Projection, Filter, and Limit maintain the order
    /// of inputs -- they may transform values (Projection) or not
    /// produce the same number of rows that went in (Filter and
    /// Limit), but the rows that are produced go in the same way.
    ///
    /// DataFusion uses this metadata to apply certain optimizations
    /// such as automatically repartitioning correctly.
    ///
    /// The default implementation returns `false`
    ///
    /// WARNING: if you override this default, you *MUST* ensure that
    /// the `ExecutionPlan`'s maintains the ordering invariant or else
    /// DataFusion may produce incorrect results.
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false; self.children().len()]
    }

    /// Specifies whether the `ExecutionPlan` benefits from increased
    /// parallelization at its input for each child.
    ///
    /// If returns `true`, the `ExecutionPlan` would benefit from partitioning
    /// its corresponding child (and thus from more parallelism). For
    /// `ExecutionPlan` that do very little work the overhead of extra
    /// parallelism may outweigh any benefits
    ///
    /// The default implementation returns `true` unless this `ExecutionPlan`
    /// has signalled it requires a single child input partition.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // By default try to maximize parallelism with more CPUs if
        // possible
        self.required_input_distribution()
            .into_iter()
            .map(|dist| !matches!(dist, Distribution::SinglePartition))
            .collect()
    }

    /// Get a list of children `ExecutionPlan`s that act as inputs to this plan.
    /// The returned list will be empty for leaf nodes such as scans, will contain
    /// a single value for unary nodes, or two values for binary nodes (such as
    /// joins).
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>>;

    /// Returns a new `ExecutionPlan` where all existing children were replaced
    /// by the `children`, in order
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// If supported, attempt to increase the partitioning of this `ExecutionPlan` to
    /// produce `target_partitions` partitions.
    ///
    /// If the `ExecutionPlan` does not support changing its partitioning,
    /// returns `Ok(None)` (the default).
    ///
    /// It is the `ExecutionPlan` can increase its partitioning, but not to the
    /// `target_partitions`, it may return an ExecutionPlan with fewer
    /// partitions. This might happen, for example, if each new partition would
    /// be too small to be efficiently processed individually.
    ///
    /// The DataFusion optimizer attempts to use as many threads as possible by
    /// repartitioning its inputs to match the target number of threads
    /// available (`target_partitions`). Some data sources, such as the built in
    /// CSV and Parquet readers, implement this method as they are able to read
    /// from their input files in parallel, regardless of how the source data is
    /// split amongst files.
    fn repartitioned(
        &self,
        _target_partitions: usize,
        _config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    /// Begin execution of `partition`, returning a [`Stream`] of
    /// [`RecordBatch`]es.
    ///
    /// # Notes
    ///
    /// The `execute` method itself is not `async` but it returns an `async`
    /// [`futures::stream::Stream`]. This `Stream` should incrementally compute
    /// the output, `RecordBatch` by `RecordBatch` (in a streaming fashion).
    /// Most `ExecutionPlan`s should not do any work before the first
    /// `RecordBatch` is requested from the stream.
    ///
    /// [`RecordBatchStreamAdapter`] can be used to convert an `async`
    /// [`Stream`] into a [`SendableRecordBatchStream`].
    ///
    /// Using `async` `Streams` allows for network I/O during execution and
    /// takes advantage of Rust's built in support for `async` continuations and
    /// crate ecosystem.
    ///
    /// [`Stream`]: futures::stream::Stream
    /// [`StreamExt`]: futures::stream::StreamExt
    /// [`TryStreamExt`]: futures::stream::TryStreamExt
    /// [`RecordBatchStreamAdapter`]: crate::stream::RecordBatchStreamAdapter
    ///
    /// # Cancellation / Aborting Execution
    ///
    /// The [`Stream`] that is returned must ensure that any allocated resources
    /// are freed when the stream itself is dropped. This is particularly
    /// important for [`spawn`]ed tasks or threads. Unless care is taken to
    /// "abort" such tasks, they may continue to consume resources even after
    /// the plan is dropped, generating intermediate results that are never
    /// used.
    /// Thus, [`spawn`] is disallowed, and instead use [`SpawnedTask`].
    ///
    /// For more details see [`SpawnedTask`], [`JoinSet`] and [`RecordBatchReceiverStreamBuilder`]
    /// for structures to help ensure all background tasks are cancelled.
    ///
    /// [`spawn`]: tokio::task::spawn
    /// [`JoinSet`]: tokio::task::JoinSet
    /// [`SpawnedTask`]: datafusion_common_runtime::SpawnedTask
    /// [`RecordBatchReceiverStreamBuilder`]: crate::stream::RecordBatchReceiverStreamBuilder
    ///
    /// # Implementation Examples
    ///
    /// While `async` `Stream`s have a non trivial learning curve, the
    /// [`futures`] crate provides [`StreamExt`] and [`TryStreamExt`]
    /// which help simplify many common operations.
    ///
    /// Here are some common patterns:
    ///
    /// ## Return Precomputed `RecordBatch`
    ///
    /// We can return a precomputed `RecordBatch` as a `Stream`:
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_array::RecordBatch;
    /// # use arrow_schema::SchemaRef;
    /// # use datafusion_common::Result;
    /// # use datafusion_execution::{SendableRecordBatchStream, TaskContext};
    /// # use datafusion_physical_plan::memory::MemoryStream;
    /// # use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
    /// struct MyPlan {
    ///     batch: RecordBatch,
    /// }
    ///
    /// impl MyPlan {
    ///     fn execute(
    ///         &self,
    ///         partition: usize,
    ///         context: Arc<TaskContext>
    ///     ) -> Result<SendableRecordBatchStream> {
    ///         // use functions from futures crate convert the batch into a stream
    ///         let fut = futures::future::ready(Ok(self.batch.clone()));
    ///         let stream = futures::stream::once(fut);
    ///         Ok(Box::pin(RecordBatchStreamAdapter::new(self.batch.schema(), stream)))
    ///     }
    /// }
    /// ```
    ///
    /// ## Lazily (async) Compute `RecordBatch`
    ///
    /// We can also lazily compute a `RecordBatch` when the returned `Stream` is polled
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_array::RecordBatch;
    /// # use arrow_schema::SchemaRef;
    /// # use datafusion_common::Result;
    /// # use datafusion_execution::{SendableRecordBatchStream, TaskContext};
    /// # use datafusion_physical_plan::memory::MemoryStream;
    /// # use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
    /// struct MyPlan {
    ///     schema: SchemaRef,
    /// }
    ///
    /// /// Returns a single batch when the returned stream is polled
    /// async fn get_batch() -> Result<RecordBatch> {
    ///     todo!()
    /// }
    ///
    /// impl MyPlan {
    ///     fn execute(
    ///         &self,
    ///         partition: usize,
    ///         context: Arc<TaskContext>
    ///     ) -> Result<SendableRecordBatchStream> {
    ///         let fut = get_batch();
    ///         let stream = futures::stream::once(fut);
    ///         Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream)))
    ///     }
    /// }
    /// ```
    ///
    /// ## Lazily (async) create a Stream
    ///
    /// If you need to to create the return `Stream` using an `async` function,
    /// you can do so by flattening the result:
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_array::RecordBatch;
    /// # use arrow_schema::SchemaRef;
    /// # use futures::TryStreamExt;
    /// # use datafusion_common::Result;
    /// # use datafusion_execution::{SendableRecordBatchStream, TaskContext};
    /// # use datafusion_physical_plan::memory::MemoryStream;
    /// # use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
    /// struct MyPlan {
    ///     schema: SchemaRef,
    /// }
    ///
    /// /// async function that returns a stream
    /// async fn get_batch_stream() -> Result<SendableRecordBatchStream> {
    ///     todo!()
    /// }
    ///
    /// impl MyPlan {
    ///     fn execute(
    ///         &self,
    ///         partition: usize,
    ///         context: Arc<TaskContext>
    ///     ) -> Result<SendableRecordBatchStream> {
    ///         // A future that yields a stream
    ///         let fut = get_batch_stream();
    ///         // Use TryStreamExt::try_flatten to flatten the stream of streams
    ///         let stream = futures::stream::once(fut).try_flatten();
    ///         Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream)))
    ///     }
    /// }
    /// ```
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream>;

    /// Return a snapshot of the set of [`Metric`]s for this
    /// [`ExecutionPlan`]. If no `Metric`s are available, return None.
    ///
    /// While the values of the metrics in the returned
    /// [`MetricsSet`]s may change as execution progresses, the
    /// specific metrics will not.
    ///
    /// Once `self.execute()` has returned (technically the future is
    /// resolved) for all available partitions, the set of metrics
    /// should be complete. If this function is called prior to
    /// `execute()` new metrics may appear in subsequent calls.
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    /// Returns statistics for this `ExecutionPlan` node. If statistics are not
    /// available, should return [`Statistics::new_unknown`] (the default), not
    /// an error.
    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }
}

/// Extension trait provides an easy API to fetch various properties of
/// [`ExecutionPlan`] objects based on [`ExecutionPlan::properties`].
pub trait ExecutionPlanProperties {
    /// Specifies how the output of this `ExecutionPlan` is split into
    /// partitions.
    fn output_partitioning(&self) -> &Partitioning;

    /// Specifies whether this plan generates an infinite stream of records.
    /// If the plan does not support pipelining, but its input(s) are
    /// infinite, returns [`ExecutionMode::PipelineBreaking`] to indicate this.
    fn execution_mode(&self) -> ExecutionMode;

    /// If the output of this `ExecutionPlan` within each partition is sorted,
    /// returns `Some(keys)` describing the ordering. A `None` return value
    /// indicates no assumptions should be made on the output ordering.
    ///
    /// For example, `SortExec` (obviously) produces sorted output as does
    /// `SortPreservingMergeStream`. Less obviously, `Projection` produces sorted
    /// output if its input is sorted as it does not reorder the input rows.
    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]>;

    /// Get the [`EquivalenceProperties`] within the plan.
    ///
    /// Equivalence properties tell DataFusion what columns are known to be
    /// equal, during various optimization passes. By default, this returns "no
    /// known equivalences" which is always correct, but may cause DataFusion to
    /// unnecessarily resort data.
    ///
    /// If this ExecutionPlan makes no changes to the schema of the rows flowing
    /// through it or how columns within each row relate to each other, it
    /// should return the equivalence properties of its input. For
    /// example, since `FilterExec` may remove rows from its input, but does not
    /// otherwise modify them, it preserves its input equivalence properties.
    /// However, since `ProjectionExec` may calculate derived expressions, it
    /// needs special handling.
    ///
    /// See also [`ExecutionPlan::maintains_input_order`] and [`Self::output_ordering`]
    /// for related concepts.
    fn equivalence_properties(&self) -> &EquivalenceProperties;
}

impl ExecutionPlanProperties for Arc<dyn ExecutionPlan> {
    fn output_partitioning(&self) -> &Partitioning {
        self.properties().output_partitioning()
    }

    fn execution_mode(&self) -> ExecutionMode {
        self.properties().execution_mode()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.properties().output_ordering()
    }

    fn equivalence_properties(&self) -> &EquivalenceProperties {
        self.properties().equivalence_properties()
    }
}

impl ExecutionPlanProperties for &dyn ExecutionPlan {
    fn output_partitioning(&self) -> &Partitioning {
        self.properties().output_partitioning()
    }

    fn execution_mode(&self) -> ExecutionMode {
        self.properties().execution_mode()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.properties().output_ordering()
    }

    fn equivalence_properties(&self) -> &EquivalenceProperties {
        self.properties().equivalence_properties()
    }
}

/// Describes the execution mode of an operator's resulting stream with respect
/// to its size and behavior. There are three possible execution modes: `Bounded`,
/// `Unbounded` and `PipelineBreaking`.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum ExecutionMode {
    /// Represents the mode where generated stream is bounded, e.g. finite.
    Bounded,
    /// Represents the mode where generated stream is unbounded, e.g. infinite.
    /// Even though the operator generates an unbounded stream of results, it
    /// works with bounded memory and execution can still continue successfully.
    ///
    /// The stream that results from calling `execute` on an `ExecutionPlan` that is `Unbounded`
    /// will never be done (return `None`), except in case of error.
    Unbounded,
    /// Represents the mode where some of the operator's input stream(s) are
    /// unbounded; however, the operator cannot generate streaming results from
    /// these streaming inputs. In this case, the execution mode will be pipeline
    /// breaking, e.g. the operator requires unbounded memory to generate results.
    PipelineBreaking,
}

impl ExecutionMode {
    /// Check whether the execution mode is unbounded or not.
    pub fn is_unbounded(&self) -> bool {
        matches!(self, ExecutionMode::Unbounded)
    }

    /// Check whether the execution is pipeline friendly. If so, operator can
    /// execute safely.
    pub fn pipeline_friendly(&self) -> bool {
        matches!(self, ExecutionMode::Bounded | ExecutionMode::Unbounded)
    }
}

/// Conservatively "combines" execution modes of a given collection of operators.
fn execution_mode_from_children<'a>(
    children: impl IntoIterator<Item = &'a Arc<dyn ExecutionPlan>>,
) -> ExecutionMode {
    let mut result = ExecutionMode::Bounded;
    for mode in children.into_iter().map(|child| child.execution_mode()) {
        match (mode, result) {
            (ExecutionMode::PipelineBreaking, _)
            | (_, ExecutionMode::PipelineBreaking) => {
                // If any of the modes is `PipelineBreaking`, so is the result:
                return ExecutionMode::PipelineBreaking;
            }
            (ExecutionMode::Unbounded, _) | (_, ExecutionMode::Unbounded) => {
                // Unbounded mode eats up bounded mode:
                result = ExecutionMode::Unbounded;
            }
            (ExecutionMode::Bounded, ExecutionMode::Bounded) => {
                // When both modes are bounded, so is the result:
                result = ExecutionMode::Bounded;
            }
        }
    }
    result
}

/// Stores certain, often expensive to compute, plan properties used in query
/// optimization.
///
/// These properties are stored a single structure to permit this information to
/// be computed once and then those cached results used multiple times without
/// recomputation (aka a cache)
#[derive(Debug, Clone)]
pub struct PlanProperties {
    /// See [ExecutionPlanProperties::equivalence_properties]
    pub eq_properties: EquivalenceProperties,
    /// See [ExecutionPlanProperties::output_partitioning]
    pub partitioning: Partitioning,
    /// See [ExecutionPlanProperties::execution_mode]
    pub execution_mode: ExecutionMode,
    /// See [ExecutionPlanProperties::output_ordering]
    output_ordering: Option<LexOrdering>,
}

impl PlanProperties {
    /// Construct a new `PlanPropertiesCache` from the
    pub fn new(
        eq_properties: EquivalenceProperties,
        partitioning: Partitioning,
        execution_mode: ExecutionMode,
    ) -> Self {
        // Output ordering can be derived from `eq_properties`.
        let output_ordering = eq_properties.oeq_class().output_ordering();
        Self {
            eq_properties,
            partitioning,
            execution_mode,
            output_ordering,
        }
    }

    /// Overwrite output partitioning with its new value.
    pub fn with_partitioning(mut self, partitioning: Partitioning) -> Self {
        self.partitioning = partitioning;
        self
    }

    /// Overwrite the execution Mode with its new value.
    pub fn with_execution_mode(mut self, execution_mode: ExecutionMode) -> Self {
        self.execution_mode = execution_mode;
        self
    }

    /// Overwrite equivalence properties with its new value.
    pub fn with_eq_properties(mut self, eq_properties: EquivalenceProperties) -> Self {
        // Changing equivalence properties also changes output ordering, so
        // make sure to overwrite it:
        self.output_ordering = eq_properties.oeq_class().output_ordering();
        self.eq_properties = eq_properties;
        self
    }

    pub fn equivalence_properties(&self) -> &EquivalenceProperties {
        &self.eq_properties
    }

    pub fn output_partitioning(&self) -> &Partitioning {
        &self.partitioning
    }

    pub fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.output_ordering.as_deref()
    }

    pub fn execution_mode(&self) -> ExecutionMode {
        self.execution_mode
    }

    /// Get schema of the node.
    fn schema(&self) -> &SchemaRef {
        self.eq_properties.schema()
    }
}

/// Indicate whether a data exchange is needed for the input of `plan`, which will be very helpful
/// especially for the distributed engine to judge whether need to deal with shuffling.
/// Currently there are 3 kinds of execution plan which needs data exchange
///     1. RepartitionExec for changing the partition number between two `ExecutionPlan`s
///     2. CoalescePartitionsExec for collapsing all of the partitions into one without ordering guarantee
///     3. SortPreservingMergeExec for collapsing all of the sorted partitions into one with ordering guarantee
pub fn need_data_exchange(plan: Arc<dyn ExecutionPlan>) -> bool {
    if let Some(repartition) = plan.as_any().downcast_ref::<RepartitionExec>() {
        !matches!(
            repartition.properties().output_partitioning(),
            Partitioning::RoundRobinBatch(_)
        )
    } else if let Some(coalesce) = plan.as_any().downcast_ref::<CoalescePartitionsExec>()
    {
        coalesce.input().output_partitioning().partition_count() > 1
    } else if let Some(sort_preserving_merge) =
        plan.as_any().downcast_ref::<SortPreservingMergeExec>()
    {
        sort_preserving_merge
            .input()
            .output_partitioning()
            .partition_count()
            > 1
    } else {
        false
    }
}

/// Returns a copy of this plan if we change any child according to the pointer comparison.
/// The size of `children` must be equal to the size of `ExecutionPlan::children()`.
pub fn with_new_children_if_necessary(
    plan: Arc<dyn ExecutionPlan>,
    children: Vec<Arc<dyn ExecutionPlan>>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let old_children = plan.children();
    if children.len() != old_children.len() {
        internal_err!("Wrong number of children")
    } else if children.is_empty()
        || children
            .iter()
            .zip(old_children.iter())
            .any(|(c1, c2)| !Arc::data_ptr_eq(c1, c2))
    {
        Ok(Transformed::Yes(plan.with_new_children(children)?))
    } else {
        Ok(Transformed::No(plan))
    }
}

/// Return a [wrapper](DisplayableExecutionPlan) around an
/// [`ExecutionPlan`] which can be displayed in various easier to
/// understand ways.
pub fn displayable(plan: &dyn ExecutionPlan) -> DisplayableExecutionPlan<'_> {
    DisplayableExecutionPlan::new(plan)
}

/// Execute the [ExecutionPlan] and collect the results in memory
pub async fn collect(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<Vec<RecordBatch>> {
    let stream = execute_stream(plan, context)?;
    common::collect(stream).await
}

/// Execute the [ExecutionPlan] and return a single stream of `RecordBatch`es.
///
/// See [collect] to buffer the `RecordBatch`es in memory.
///
/// # Aborting Execution
///
/// Dropping the stream will abort the execution of the query, and free up
/// any allocated resources
pub fn execute_stream(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<SendableRecordBatchStream> {
    match plan.output_partitioning().partition_count() {
        0 => Ok(Box::pin(EmptyRecordBatchStream::new(plan.schema()))),
        1 => plan.execute(0, context),
        _ => {
            // merge into a single partition
            let plan = CoalescePartitionsExec::new(plan.clone());
            // CoalescePartitionsExec must produce a single partition
            assert_eq!(1, plan.properties().output_partitioning().partition_count());
            plan.execute(0, context)
        }
    }
}

/// Execute the [ExecutionPlan] and collect the results in memory
pub async fn collect_partitioned(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<Vec<Vec<RecordBatch>>> {
    let streams = execute_stream_partitioned(plan, context)?;

    let mut join_set = JoinSet::new();
    // Execute the plan and collect the results into batches.
    streams.into_iter().enumerate().for_each(|(idx, stream)| {
        join_set.spawn(async move {
            let result: Result<Vec<RecordBatch>> = stream.try_collect().await;
            (idx, result)
        });
    });

    let mut batches = vec![];
    // Note that currently this doesn't identify the thread that panicked
    //
    // TODO: Replace with [join_next_with_id](https://docs.rs/tokio/latest/tokio/task/struct.JoinSet.html#method.join_next_with_id
    // once it is stable
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok((idx, res)) => batches.push((idx, res?)),
            Err(e) => {
                if e.is_panic() {
                    std::panic::resume_unwind(e.into_panic());
                } else {
                    unreachable!();
                }
            }
        }
    }

    batches.sort_by_key(|(idx, _)| *idx);
    let batches = batches.into_iter().map(|(_, batch)| batch).collect();

    Ok(batches)
}

/// Execute the [ExecutionPlan] and return a vec with one stream per output
/// partition
///
/// # Aborting Execution
///
/// Dropping the stream will abort the execution of the query, and free up
/// any allocated resources
pub fn execute_stream_partitioned(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<Vec<SendableRecordBatchStream>> {
    let num_partitions = plan.output_partitioning().partition_count();
    let mut streams = Vec::with_capacity(num_partitions);
    for i in 0..num_partitions {
        streams.push(plan.execute(i, context.clone())?);
    }
    Ok(streams)
}

/// Utility function yielding a string representation of the given [`ExecutionPlan`].
pub fn get_plan_string(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    let formatted = displayable(plan.as_ref()).indent(true).to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    actual.iter().map(|elem| elem.to_string()).collect()
}

#[cfg(test)]
#[allow(clippy::single_component_path_imports)]
use rstest_reuse;

pub mod test;
