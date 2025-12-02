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

pub use crate::display::{DefaultDisplay, DisplayAs, DisplayFormatType, VerboseDisplay};
use crate::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase,
    FilterPushdownPropagation,
};
pub use crate::metrics::Metric;
pub use crate::ordering::InputOrderMode;
pub use crate::stream::EmptyRecordBatchStream;

pub use datafusion_common::hash_utils;
pub use datafusion_common::utils::project_schema;
pub use datafusion_common::{internal_err, ColumnStatistics, Statistics};
pub use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
pub use datafusion_expr::{Accumulator, ColumnarValue};
pub use datafusion_physical_expr::window::WindowExpr;
pub use datafusion_physical_expr::{
    expressions, Distribution, Partitioning, PhysicalExpr,
};

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use crate::coalesce_partitions::CoalescePartitionsExec;
use crate::display::DisplayableExecutionPlan;
use crate::metrics::MetricsSet;
use crate::projection::ProjectionExec;
use crate::stream::RecordBatchStreamAdapter;

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::SchemaRef;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{
    assert_eq_or_internal_err, assert_or_internal_err, exec_err, Constraints,
    DataFusionError, Result,
};
use datafusion_common_runtime::JoinSet;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, OrderingRequirements};

use futures::stream::{StreamExt, TryStreamExt};

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
///
/// # Examples
///
/// See [`datafusion-examples`] for examples, including
/// [`memory_pool_execution_plan.rs`] which shows how to implement a custom
/// `ExecutionPlan` with memory tracking and spilling support.
///
/// [`datafusion-examples`]: https://github.com/apache/datafusion/tree/main/datafusion-examples
/// [`memory_pool_execution_plan.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/execution_monitoring/memory_pool_execution_plan.rs
pub trait ExecutionPlan: Debug + DisplayAs + Send + Sync {
    /// Short name for the ExecutionPlan, such as 'DataSourceExec'.
    ///
    /// Implementation note: this method can just proxy to
    /// [`static_name`](ExecutionPlan::static_name) if no special action is
    /// needed. It doesn't provide a default implementation like that because
    /// this method doesn't require the `Sized` constrain to allow a wilder
    /// range of use cases.
    fn name(&self) -> &str;

    /// Short name for the ExecutionPlan, such as 'DataSourceExec'.
    /// Like [`name`](ExecutionPlan::name) but can be called without an instance.
    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        let full_name = std::any::type_name::<Self>();
        let maybe_start_idx = full_name.rfind(':');
        match maybe_start_idx {
            Some(start_idx) => &full_name[start_idx + 1..],
            None => "UNKNOWN",
        }
    }

    /// Returns the execution plan as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        Arc::clone(self.properties().schema())
    }

    /// Return properties of the output of the `ExecutionPlan`, such as output
    /// ordering(s), partitioning information etc.
    ///
    /// This information is available via methods on [`ExecutionPlanProperties`]
    /// trait, which is implemented for all `ExecutionPlan`s.
    fn properties(&self) -> &PlanProperties;

    /// Returns an error if this individual node does not conform to its invariants.
    /// These invariants are typically only checked in debug mode.
    ///
    /// A default set of invariants is provided in the [check_default_invariants] function.
    /// The default implementation of `check_invariants` calls this function.
    /// Extension nodes can provide their own invariants.
    fn check_invariants(&self, check: InvariantLevel) -> Result<()> {
        check_default_invariants(self, check)
    }

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
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
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
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>>;

    /// Returns a new `ExecutionPlan` where all existing children were replaced
    /// by the `children`, in order
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Reset any internal state within this [`ExecutionPlan`].
    ///
    /// This method is called when an [`ExecutionPlan`] needs to be re-executed,
    /// such as in recursive queries. Unlike [`ExecutionPlan::with_new_children`], this method
    /// ensures that any stateful components (e.g., [`DynamicFilterPhysicalExpr`])
    /// are reset to their initial state.
    ///
    /// The default implementation simply calls [`ExecutionPlan::with_new_children`] with the existing children,
    /// effectively creating a new instance of the [`ExecutionPlan`] with the same children but without
    /// necessarily resetting any internal state. Implementations that require resetting of some
    /// internal state should override this method to provide the necessary logic.
    ///
    /// This method should *not* reset state recursively for children, as it is expected that
    /// it will be called from within a walk of the execution plan tree so that it will be called on each child later
    /// or was already called on each child.
    ///
    /// Note to implementers: unlike [`ExecutionPlan::with_new_children`] this method does not accept new children as an argument,
    /// thus it is expected that any cached plan properties will remain valid after the reset.
    ///
    /// [`DynamicFilterPhysicalExpr`]: datafusion_physical_expr::expressions::DynamicFilterPhysicalExpr
    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        let children = self.children().into_iter().cloned().collect();
        self.with_new_children(children)
    }

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
    /// # Error handling
    ///
    /// Any error that occurs during execution is sent as an `Err` in the output
    /// stream.
    ///
    /// `ExecutionPlan` implementations in DataFusion cancel additional work
    /// immediately once an error occurs. The rationale is that if the overall
    /// query will return an error,  any additional work such as continued
    /// polling of inputs will be wasted as it will be thrown away.
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
    /// To enable timely cancellation, the [`Stream`] that is returned must not
    /// block the CPU indefinitely and must yield back to the tokio runtime regularly.
    /// In a typical [`ExecutionPlan`], this automatically happens unless there are
    /// special circumstances; e.g. when the computational complexity of processing a
    /// batch is superlinear. See this [general guideline][async-guideline] for more context
    /// on this point, which explains why one should avoid spending a long time without
    /// reaching an `await`/yield point in asynchronous runtimes.
    /// This can be achieved by using the utilities from the [`coop`](crate::coop) module, by
    /// manually returning [`Poll::Pending`] and setting up wakers appropriately, or by calling
    /// [`tokio::task::yield_now()`] when appropriate.
    /// In special cases that warrant manual yielding, determination for "regularly" may be
    /// made using the [Tokio task budget](https://docs.rs/tokio/latest/tokio/task/coop/index.html),
    /// a timer (being careful with the overhead-heavy system call needed to take the time), or by
    /// counting rows or batches.
    ///
    /// The [cancellation benchmark] tracks some cases of how quickly queries can
    /// be cancelled.
    ///
    /// For more details see [`SpawnedTask`], [`JoinSet`] and [`RecordBatchReceiverStreamBuilder`]
    /// for structures to help ensure all background tasks are cancelled.
    ///
    /// [`spawn`]: tokio::task::spawn
    /// [cancellation benchmark]: https://github.com/apache/datafusion/blob/main/benchmarks/README.md#cancellation
    /// [`JoinSet`]: datafusion_common_runtime::JoinSet
    /// [`SpawnedTask`]: datafusion_common_runtime::SpawnedTask
    /// [`RecordBatchReceiverStreamBuilder`]: crate::stream::RecordBatchReceiverStreamBuilder
    /// [`Poll::Pending`]: std::task::Poll::Pending
    /// [async-guideline]: https://ryhl.io/blog/async-what-is-blocking/
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
    /// # use arrow::array::RecordBatch;
    /// # use arrow::datatypes::SchemaRef;
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
    ///         context: Arc<TaskContext>,
    ///     ) -> Result<SendableRecordBatchStream> {
    ///         // use functions from futures crate convert the batch into a stream
    ///         let fut = futures::future::ready(Ok(self.batch.clone()));
    ///         let stream = futures::stream::once(fut);
    ///         Ok(Box::pin(RecordBatchStreamAdapter::new(
    ///             self.batch.schema(),
    ///             stream,
    ///         )))
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
    /// # use arrow::array::RecordBatch;
    /// # use arrow::datatypes::SchemaRef;
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
    ///         context: Arc<TaskContext>,
    ///     ) -> Result<SendableRecordBatchStream> {
    ///         let fut = get_batch();
    ///         let stream = futures::stream::once(fut);
    ///         Ok(Box::pin(RecordBatchStreamAdapter::new(
    ///             self.schema.clone(),
    ///             stream,
    ///         )))
    ///     }
    /// }
    /// ```
    ///
    /// ## Lazily (async) create a Stream
    ///
    /// If you need to create the return `Stream` using an `async` function,
    /// you can do so by flattening the result:
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow::array::RecordBatch;
    /// # use arrow::datatypes::SchemaRef;
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
    ///         context: Arc<TaskContext>,
    ///     ) -> Result<SendableRecordBatchStream> {
    ///         // A future that yields a stream
    ///         let fut = get_batch_stream();
    ///         // Use TryStreamExt::try_flatten to flatten the stream of streams
    ///         let stream = futures::stream::once(fut).try_flatten();
    ///         Ok(Box::pin(RecordBatchStreamAdapter::new(
    ///             self.schema.clone(),
    ///             stream,
    ///         )))
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
    ///
    /// For TableScan executors, which supports filter pushdown, special attention
    /// needs to be paid to whether the stats returned by this method are exact or not
    #[deprecated(since = "48.0.0", note = "Use `partition_statistics` method instead")]
    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }

    /// Returns statistics for a specific partition of this `ExecutionPlan` node.
    /// If statistics are not available, should return [`Statistics::new_unknown`]
    /// (the default), not an error.
    /// If `partition` is `None`, it returns statistics for the entire plan.
    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        if let Some(idx) = partition {
            // Validate partition index
            let partition_count = self.properties().partitioning.partition_count();
            assert_or_internal_err!(
                idx < partition_count,
                "Invalid partition index: {}, the partition count is {}",
                idx,
                partition_count
            );
        }
        Ok(Statistics::new_unknown(&self.schema()))
    }

    /// Returns `true` if a limit can be safely pushed down through this
    /// `ExecutionPlan` node.
    ///
    /// If this method returns `true`, and the query plan contains a limit at
    /// the output of this node, DataFusion will push the limit to the input
    /// of this node.
    fn supports_limit_pushdown(&self) -> bool {
        false
    }

    /// Returns a fetching variant of this `ExecutionPlan` node, if it supports
    /// fetch limits. Returns `None` otherwise.
    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    /// Gets the fetch count for the operator, `None` means there is no fetch.
    fn fetch(&self) -> Option<usize> {
        None
    }

    /// Gets the effect on cardinality, if known
    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Unknown
    }

    /// Attempts to push down the given projection into the input of this `ExecutionPlan`.
    ///
    /// If the operator supports this optimization, the resulting plan will be:
    /// `self_new <- projection <- source`, starting from `projection <- self <- source`.
    /// Otherwise, it returns the current `ExecutionPlan` as-is.
    ///
    /// Returns `Ok(Some(...))` if pushdown is applied, `Ok(None)` if it is not supported
    /// or not possible, or `Err` on failure.
    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    /// Collect filters that this node can push down to its children.
    /// Filters that are being pushed down from parents are passed in,
    /// and the node may generate additional filters to push down.
    /// For example, given the plan FilterExec -> HashJoinExec -> DataSourceExec,
    /// what will happen is that we recurse down the plan calling `ExecutionPlan::gather_filters_for_pushdown`:
    /// 1. `FilterExec::gather_filters_for_pushdown` is called with no parent
    ///    filters so it only returns that `FilterExec` wants to push down its own predicate.
    /// 2. `HashJoinExec::gather_filters_for_pushdown` is called with the filter from
    ///    `FilterExec`, which it only allows to push down to one side of the join (unless it's on the join key)
    ///    but it also adds its own filters (e.g. pushing down a bloom filter of the hash table to the scan side of the join).
    /// 3. `DataSourceExec::gather_filters_for_pushdown` is called with both filters from `HashJoinExec`
    ///    and `FilterExec`, however `DataSourceExec::gather_filters_for_pushdown` doesn't actually do anything
    ///    since it has no children and no additional filters to push down.
    ///    It's only once [`ExecutionPlan::handle_child_pushdown_result`] is called on `DataSourceExec` as we recurse
    ///    up the plan that `DataSourceExec` can actually bind the filters.
    ///
    /// The default implementation bars all parent filters from being pushed down and adds no new filters.
    /// This is the safest option, making filter pushdown opt-in on a per-node pasis.
    ///
    /// There are two different phases in filter pushdown, which some operators may handle the same and some differently.
    /// Depending on the phase the operator may or may not be allowed to modify the plan.
    /// See [`FilterPushdownPhase`] for more details.
    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        Ok(FilterDescription::all_unsupported(
            &parent_filters,
            &self.children(),
        ))
    }

    /// Handle the result of a child pushdown.
    /// This method is called as we recurse back up the plan tree after pushing
    /// filters down to child nodes via [`ExecutionPlan::gather_filters_for_pushdown`].
    /// It allows the current node to process the results of filter pushdown from
    /// its children, deciding whether to absorb filters, modify the plan, or pass
    /// filters back up to its parent.
    ///
    /// **Purpose and Context:**
    /// Filter pushdown is a critical optimization in DataFusion that aims to
    /// reduce the amount of data processed by applying filters as early as
    /// possible in the query plan. This method is part of the second phase of
    /// filter pushdown, where results are propagated back up the tree after
    /// being pushed down. Each node can inspect the pushdown results from its
    /// children and decide how to handle any unapplied filters, potentially
    /// optimizing the plan structure or filter application.
    ///
    /// **Behavior in Different Nodes:**
    /// - For a `DataSourceExec`, this often means absorbing the filters to apply
    ///   them during the scan phase (late materialization), reducing the data
    ///   read from the source.
    /// - A `FilterExec` may absorb any filters its children could not handle,
    ///   combining them with its own predicate. If no filters remain (i.e., the
    ///   predicate becomes trivially true), it may remove itself from the plan
    ///   altogether. It typically marks parent filters as supported, indicating
    ///   they have been handled.
    /// - A `HashJoinExec` might ignore the pushdown result if filters need to
    ///   be applied during the join operation. It passes the parent filters back
    ///   up wrapped in [`FilterPushdownPropagation::if_any`], discarding
    ///   any self-filters from children.
    ///
    /// **Example Walkthrough:**
    /// Consider a query plan: `FilterExec (f1) -> HashJoinExec -> DataSourceExec`.
    /// 1. **Downward Phase (`gather_filters_for_pushdown`):** Starting at
    ///    `FilterExec`, the filter `f1` is gathered and pushed down to
    ///    `HashJoinExec`. `HashJoinExec` may allow `f1` to pass to one side of
    ///    the join or add its own filters (e.g., a min-max filter from the build side),
    ///    then pushes filters to `DataSourceExec`. `DataSourceExec`, being a leaf node,
    ///    has no children to push to, so it prepares to handle filters in the
    ///    upward phase.
    /// 2. **Upward Phase (`handle_child_pushdown_result`):** Starting at
    ///    `DataSourceExec`, it absorbs applicable filters from `HashJoinExec`
    ///    for late materialization during scanning, marking them as supported.
    ///    `HashJoinExec` receives the result, decides whether to apply any
    ///    remaining filters during the join, and passes unhandled filters back
    ///    up to `FilterExec`. `FilterExec` absorbs any unhandled filters,
    ///    updates its predicate if necessary, or removes itself if the predicate
    ///    becomes trivial (e.g., `lit(true)`), and marks filters as supported
    ///    for its parent.
    ///
    /// The default implementation is a no-op that passes the result of pushdown
    /// from the children to its parent transparently, ensuring no filters are
    /// lost if a node does not override this behavior.
    ///
    /// **Notes for Implementation:**
    /// When returning filters via [`FilterPushdownPropagation`], the order of
    /// filters need not match the order they were passed in via
    /// `child_pushdown_result`. However, preserving the order is recommended for
    /// debugging and ease of reasoning about the resulting plans.
    ///
    /// **Helper Methods for Customization:**
    /// There are various helper methods to simplify implementing this method:
    /// - [`FilterPushdownPropagation::if_any`]: Marks all parent filters as
    ///   supported as long as at least one child supports them.
    /// - [`FilterPushdownPropagation::if_all`]: Marks all parent filters as
    ///   supported as long as all children support them.
    /// - [`FilterPushdownPropagation::with_parent_pushdown_result`]: Allows adding filters
    ///   to the propagation result, indicating which filters are supported by
    ///   the current node.
    /// - [`FilterPushdownPropagation::with_updated_node`]: Allows updating the
    ///   current node in the propagation result, used if the node
    ///   has modified its plan based on the pushdown results.
    ///
    /// **Filter Pushdown Phases:**
    /// There are two different phases in filter pushdown (`Pre` and others),
    /// which some operators may handle differently. Depending on the phase, the
    /// operator may or may not be allowed to modify the plan. See
    /// [`FilterPushdownPhase`] for more details on phase-specific behavior.
    ///
    /// [`PushedDownPredicate::supported`]: crate::filter_pushdown::PushedDownPredicate::supported
    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(FilterPushdownPropagation::if_all(child_pushdown_result))
    }

    /// Injects arbitrary run-time state into this execution plan, returning a new plan
    /// instance that incorporates that state *if* it is relevant to the concrete
    /// node implementation.
    ///
    /// This is a generic entry point: the `state` can be any type wrapped in
    /// `Arc<dyn Any + Send + Sync>`.  A node that cares about the state should
    /// down-cast it to the concrete type it expects and, if successful, return a
    /// modified copy of itself that captures the provided value.  If the state is
    /// not applicable, the default behaviour is to return `None` so that parent
    /// nodes can continue propagating the attempt further down the plan tree.
    ///
    /// For example, [`WorkTableExec`](crate::work_table::WorkTableExec)
    /// down-casts the supplied state to an `Arc<WorkTable>`
    /// in order to wire up the working table used during recursive-CTE execution.
    /// Similar patterns can be followed by custom nodes that need late-bound
    /// dependencies or shared state.
    fn with_new_state(
        &self,
        _state: Arc<dyn Any + Send + Sync>,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }
}

/// [`ExecutionPlan`] Invariant Level
///
/// What set of assertions ([Invariant]s)  holds for a particular `ExecutionPlan`
///
/// [Invariant]: https://en.wikipedia.org/wiki/Invariant_(mathematics)#Invariants_in_computer_science
#[derive(Clone, Copy)]
pub enum InvariantLevel {
    /// Invariants that are always true for the [`ExecutionPlan`] node
    /// such as the number of expected children.
    Always,
    /// Invariants that must hold true for the [`ExecutionPlan`] node
    /// to be "executable", such as ordering and/or distribution requirements
    /// being fulfilled.
    Executable,
}

/// Extension trait provides an easy API to fetch various properties of
/// [`ExecutionPlan`] objects based on [`ExecutionPlan::properties`].
pub trait ExecutionPlanProperties {
    /// Specifies how the output of this `ExecutionPlan` is split into
    /// partitions.
    fn output_partitioning(&self) -> &Partitioning;

    /// If the output of this `ExecutionPlan` within each partition is sorted,
    /// returns `Some(keys)` describing the ordering. A `None` return value
    /// indicates no assumptions should be made on the output ordering.
    ///
    /// For example, `SortExec` (obviously) produces sorted output as does
    /// `SortPreservingMergeStream`. Less obviously, `Projection` produces sorted
    /// output if its input is sorted as it does not reorder the input rows.
    fn output_ordering(&self) -> Option<&LexOrdering>;

    /// Boundedness information of the stream corresponding to this `ExecutionPlan`.
    /// For more details, see [`Boundedness`].
    fn boundedness(&self) -> Boundedness;

    /// Indicates how the stream of this `ExecutionPlan` emits its results.
    /// For more details, see [`EmissionType`].
    fn pipeline_behavior(&self) -> EmissionType;

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
    /// example, since [`FilterExec`] may remove rows from its input, but does not
    /// otherwise modify them, it preserves its input equivalence properties.
    /// However, since `ProjectionExec` may calculate derived expressions, it
    /// needs special handling.
    ///
    /// See also [`ExecutionPlan::maintains_input_order`] and [`Self::output_ordering`]
    /// for related concepts.
    ///
    /// [`FilterExec`]: crate::filter::FilterExec
    fn equivalence_properties(&self) -> &EquivalenceProperties;
}

impl ExecutionPlanProperties for Arc<dyn ExecutionPlan> {
    fn output_partitioning(&self) -> &Partitioning {
        self.properties().output_partitioning()
    }

    fn output_ordering(&self) -> Option<&LexOrdering> {
        self.properties().output_ordering()
    }

    fn boundedness(&self) -> Boundedness {
        self.properties().boundedness
    }

    fn pipeline_behavior(&self) -> EmissionType {
        self.properties().emission_type
    }

    fn equivalence_properties(&self) -> &EquivalenceProperties {
        self.properties().equivalence_properties()
    }
}

impl ExecutionPlanProperties for &dyn ExecutionPlan {
    fn output_partitioning(&self) -> &Partitioning {
        self.properties().output_partitioning()
    }

    fn output_ordering(&self) -> Option<&LexOrdering> {
        self.properties().output_ordering()
    }

    fn boundedness(&self) -> Boundedness {
        self.properties().boundedness
    }

    fn pipeline_behavior(&self) -> EmissionType {
        self.properties().emission_type
    }

    fn equivalence_properties(&self) -> &EquivalenceProperties {
        self.properties().equivalence_properties()
    }
}

/// Represents whether a stream of data **generated** by an operator is bounded (finite)
/// or unbounded (infinite).
///
/// This is used to determine whether an execution plan will eventually complete
/// processing all its data (bounded) or could potentially run forever (unbounded).
///
/// For unbounded streams, it also tracks whether the operator requires finite memory
/// to process the stream or if memory usage could grow unbounded.
///
/// Boundedness of the output stream is based on the boundedness of the input stream and the nature of
/// the operator. For example, limit or topk with fetch operator can convert an unbounded stream to a bounded stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Boundedness {
    /// The data stream is bounded (finite) and will eventually complete
    Bounded,
    /// The data stream is unbounded (infinite) and could run forever
    Unbounded {
        /// Whether this operator requires infinite memory to process the unbounded stream.
        /// If false, the operator can process an infinite stream with bounded memory.
        /// If true, memory usage may grow unbounded while processing the stream.
        ///
        /// For example, `Median` requires infinite memory to compute the median of an unbounded stream.
        /// `Min/Max` requires infinite memory if the stream is unordered, but can be computed with bounded memory if the stream is ordered.
        requires_infinite_memory: bool,
    },
}

impl Boundedness {
    pub fn is_unbounded(&self) -> bool {
        matches!(self, Boundedness::Unbounded { .. })
    }
}

/// Represents how an operator emits its output records.
///
/// This is used to determine whether an operator emits records incrementally as they arrive,
/// only emits a final result at the end, or can do both. Note that it generates the output -- record batch with `batch_size` rows
/// but it may still buffer data internally until it has enough data to emit a record batch or the source is exhausted.
///
/// For example, in the following plan:
/// ```text
///   SortExec [EmissionType::Final]
///     |_ on: [col1 ASC]
///     FilterExec [EmissionType::Incremental]
///       |_ pred: col2 > 100
///       DataSourceExec [EmissionType::Incremental]
///         |_ file: "data.csv"
/// ```
/// - DataSourceExec emits records incrementally as it reads from the file
/// - FilterExec processes and emits filtered records incrementally as they arrive
/// - SortExec must wait for all input records before it can emit the sorted result,
///   since it needs to see all values to determine their final order
///
/// Left joins can emit both incrementally and finally:
/// - Incrementally emit matches as they are found
/// - Finally emit non-matches after all input is processed
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmissionType {
    /// Records are emitted incrementally as they arrive and are processed
    Incremental,
    /// Records are only emitted once all input has been processed
    Final,
    /// Records can be emitted both incrementally and as a final result
    Both,
}

/// Represents whether an operator's `Stream` has been implemented to actively cooperate with the
/// Tokio scheduler or not. Please refer to the [`coop`](crate::coop) module for more details.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulingType {
    /// The stream generated by [`execute`](ExecutionPlan::execute) does not actively participate in
    /// cooperative scheduling. This means the implementation of the `Stream` returned by
    /// [`ExecutionPlan::execute`] does not contain explicit task budget consumption such as
    /// [`tokio::task::coop::consume_budget`].
    ///
    /// `NonCooperative` is the default value and is acceptable for most operators. Please refer to
    /// the [`coop`](crate::coop) module for details on when it may be useful to use
    /// `Cooperative` instead.
    NonCooperative,
    /// The stream generated by [`execute`](ExecutionPlan::execute) actively participates in
    /// cooperative scheduling by consuming task budget when it was able to produce a
    /// [`RecordBatch`].
    Cooperative,
}

/// Represents how an operator's `Stream` implementation generates `RecordBatch`es.
///
/// Most operators in DataFusion generate `RecordBatch`es when asked to do so by a call to
/// `Stream::poll_next`. This is known as demand-driven or lazy evaluation.
///
/// Some operators like `Repartition` need to drive `RecordBatch` generation themselves though. This
/// is known as data-driven or eager evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvaluationType {
    /// The stream generated by [`execute`](ExecutionPlan::execute) only generates `RecordBatch`
    /// instances when it is demanded by invoking `Stream::poll_next`.
    /// Filter, projection, and join are examples of such lazy operators.
    ///
    /// Lazy operators are also known as demand-driven operators.
    Lazy,
    /// The stream generated by [`execute`](ExecutionPlan::execute) eagerly generates `RecordBatch`
    /// in one or more spawned Tokio tasks. Eager evaluation is only started the first time
    /// `Stream::poll_next` is called.
    /// Examples of eager operators are repartition, coalesce partitions, and sort preserving merge.
    ///
    /// Eager operators are also known as a data-driven operators.
    Eager,
}

/// Utility to determine an operator's boundedness based on its children's boundedness.
///
/// Assumes boundedness can be inferred from child operators:
/// - Unbounded (requires_infinite_memory: true) takes precedence.
/// - Unbounded (requires_infinite_memory: false) is considered next.
/// - Otherwise, the operator is bounded.
///
/// **Note:** This is a general-purpose utility and may not apply to
/// all multi-child operators. Ensure your operator's behavior aligns
/// with these assumptions before using.
pub(crate) fn boundedness_from_children<'a>(
    children: impl IntoIterator<Item = &'a Arc<dyn ExecutionPlan>>,
) -> Boundedness {
    let mut unbounded_with_finite_mem = false;

    for child in children {
        match child.boundedness() {
            Boundedness::Unbounded {
                requires_infinite_memory: true,
            } => {
                return Boundedness::Unbounded {
                    requires_infinite_memory: true,
                }
            }
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            } => {
                unbounded_with_finite_mem = true;
            }
            Boundedness::Bounded => {}
        }
    }

    if unbounded_with_finite_mem {
        Boundedness::Unbounded {
            requires_infinite_memory: false,
        }
    } else {
        Boundedness::Bounded
    }
}

/// Determines the emission type of an operator based on its children's pipeline behavior.
///
/// The precedence of emission types is:
/// - `Final` has the highest precedence.
/// - `Both` is next: if any child emits both incremental and final results, the parent inherits this behavior unless a `Final` is present.
/// - `Incremental` is the default if all children emit incremental results.
///
/// **Note:** This is a general-purpose utility and may not apply to
/// all multi-child operators. Verify your operator's behavior aligns
/// with these assumptions.
pub(crate) fn emission_type_from_children<'a>(
    children: impl IntoIterator<Item = &'a Arc<dyn ExecutionPlan>>,
) -> EmissionType {
    let mut inc_and_final = false;

    for child in children {
        match child.pipeline_behavior() {
            EmissionType::Final => return EmissionType::Final,
            EmissionType::Both => inc_and_final = true,
            EmissionType::Incremental => continue,
        }
    }

    if inc_and_final {
        EmissionType::Both
    } else {
        EmissionType::Incremental
    }
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
    /// See [ExecutionPlanProperties::pipeline_behavior]
    pub emission_type: EmissionType,
    /// See [ExecutionPlanProperties::boundedness]
    pub boundedness: Boundedness,
    pub evaluation_type: EvaluationType,
    pub scheduling_type: SchedulingType,
    /// See [ExecutionPlanProperties::output_ordering]
    output_ordering: Option<LexOrdering>,
}

impl PlanProperties {
    /// Construct a new `PlanPropertiesCache` from the
    pub fn new(
        eq_properties: EquivalenceProperties,
        partitioning: Partitioning,
        emission_type: EmissionType,
        boundedness: Boundedness,
    ) -> Self {
        // Output ordering can be derived from `eq_properties`.
        let output_ordering = eq_properties.output_ordering();
        Self {
            eq_properties,
            partitioning,
            emission_type,
            boundedness,
            evaluation_type: EvaluationType::Lazy,
            scheduling_type: SchedulingType::NonCooperative,
            output_ordering,
        }
    }

    /// Overwrite output partitioning with its new value.
    pub fn with_partitioning(mut self, partitioning: Partitioning) -> Self {
        self.partitioning = partitioning;
        self
    }

    /// Overwrite equivalence properties with its new value.
    pub fn with_eq_properties(mut self, eq_properties: EquivalenceProperties) -> Self {
        // Changing equivalence properties also changes output ordering, so
        // make sure to overwrite it:
        self.output_ordering = eq_properties.output_ordering();
        self.eq_properties = eq_properties;
        self
    }

    /// Overwrite boundedness with its new value.
    pub fn with_boundedness(mut self, boundedness: Boundedness) -> Self {
        self.boundedness = boundedness;
        self
    }

    /// Overwrite emission type with its new value.
    pub fn with_emission_type(mut self, emission_type: EmissionType) -> Self {
        self.emission_type = emission_type;
        self
    }

    /// Set the [`SchedulingType`].
    ///
    /// Defaults to [`SchedulingType::NonCooperative`]
    pub fn with_scheduling_type(mut self, scheduling_type: SchedulingType) -> Self {
        self.scheduling_type = scheduling_type;
        self
    }

    /// Set the [`EvaluationType`].
    ///
    /// Defaults to [`EvaluationType::Lazy`]
    pub fn with_evaluation_type(mut self, drive_type: EvaluationType) -> Self {
        self.evaluation_type = drive_type;
        self
    }

    /// Overwrite constraints with its new value.
    pub fn with_constraints(mut self, constraints: Constraints) -> Self {
        self.eq_properties = self.eq_properties.with_constraints(constraints);
        self
    }

    pub fn equivalence_properties(&self) -> &EquivalenceProperties {
        &self.eq_properties
    }

    pub fn output_partitioning(&self) -> &Partitioning {
        &self.partitioning
    }

    pub fn output_ordering(&self) -> Option<&LexOrdering> {
        self.output_ordering.as_ref()
    }

    /// Get schema of the node.
    pub(crate) fn schema(&self) -> &SchemaRef {
        self.eq_properties.schema()
    }
}

macro_rules! check_len {
    ($target:expr, $func_name:ident, $expected_len:expr) => {
        let actual_len = $target.$func_name().len();
        assert_eq_or_internal_err!(
            actual_len,
            $expected_len,
            "{}::{} returned Vec with incorrect size: {} != {}",
            $target.name(),
            stringify!($func_name),
            actual_len,
            $expected_len
        );
    };
}

/// Checks a set of invariants that apply to all ExecutionPlan implementations.
/// Returns an error if the given node does not conform.
pub fn check_default_invariants<P: ExecutionPlan + ?Sized>(
    plan: &P,
    _check: InvariantLevel,
) -> Result<(), DataFusionError> {
    let children_len = plan.children().len();

    check_len!(plan, maintains_input_order, children_len);
    check_len!(plan, required_input_ordering, children_len);
    check_len!(plan, required_input_distribution, children_len);
    check_len!(plan, benefits_from_input_partitioning, children_len);

    Ok(())
}

/// Indicate whether a data exchange is needed for the input of `plan`, which will be very helpful
/// especially for the distributed engine to judge whether need to deal with shuffling.
/// Currently, there are 3 kinds of execution plan which needs data exchange
///     1. RepartitionExec for changing the partition number between two `ExecutionPlan`s
///     2. CoalescePartitionsExec for collapsing all of the partitions into one without ordering guarantee
///     3. SortPreservingMergeExec for collapsing all of the sorted partitions into one with ordering guarantee
#[expect(clippy::needless_pass_by_value)]
pub fn need_data_exchange(plan: Arc<dyn ExecutionPlan>) -> bool {
    plan.properties().evaluation_type == EvaluationType::Eager
}

/// Returns a copy of this plan if we change any child according to the pointer comparison.
/// The size of `children` must be equal to the size of `ExecutionPlan::children()`.
pub fn with_new_children_if_necessary(
    plan: Arc<dyn ExecutionPlan>,
    children: Vec<Arc<dyn ExecutionPlan>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let old_children = plan.children();
    assert_eq_or_internal_err!(
        children.len(),
        old_children.len(),
        "Wrong number of children"
    );
    if children.is_empty()
        || children
            .iter()
            .zip(old_children.iter())
            .any(|(c1, c2)| !Arc::ptr_eq(c1, c2))
    {
        plan.with_new_children(children)
    } else {
        Ok(plan)
    }
}

/// Return a [`DisplayableExecutionPlan`] wrapper around an
/// [`ExecutionPlan`] which can be displayed in various easier to
/// understand ways.
///
/// See examples on [`DisplayableExecutionPlan`]
pub fn displayable(plan: &dyn ExecutionPlan) -> DisplayableExecutionPlan<'_> {
    DisplayableExecutionPlan::new(plan)
}

/// Execute the [ExecutionPlan] and collect the results in memory
pub async fn collect(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<Vec<RecordBatch>> {
    let stream = execute_stream(plan, context)?;
    crate::common::collect(stream).await
}

/// Execute the [ExecutionPlan] and return a single stream of `RecordBatch`es.
///
/// See [collect] to buffer the `RecordBatch`es in memory.
///
/// # Aborting Execution
///
/// Dropping the stream will abort the execution of the query, and free up
/// any allocated resources
#[expect(
    clippy::needless_pass_by_value,
    reason = "Public API that historically takes owned Arcs"
)]
pub fn execute_stream(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<SendableRecordBatchStream> {
    match plan.output_partitioning().partition_count() {
        0 => Ok(Box::pin(EmptyRecordBatchStream::new(plan.schema()))),
        1 => plan.execute(0, context),
        2.. => {
            // merge into a single partition
            let plan = CoalescePartitionsExec::new(Arc::clone(&plan));
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
#[expect(
    clippy::needless_pass_by_value,
    reason = "Public API that historically takes owned Arcs"
)]
pub fn execute_stream_partitioned(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<Vec<SendableRecordBatchStream>> {
    let num_partitions = plan.output_partitioning().partition_count();
    let mut streams = Vec::with_capacity(num_partitions);
    for i in 0..num_partitions {
        streams.push(plan.execute(i, Arc::clone(&context))?);
    }
    Ok(streams)
}

/// Executes an input stream and ensures that the resulting stream adheres to
/// the `not null` constraints specified in the `sink_schema`.
///
/// # Arguments
///
/// * `input` - An execution plan
/// * `sink_schema` - The schema to be applied to the output stream
/// * `partition` - The partition index to be executed
/// * `context` - The task context
///
/// # Returns
///
/// * `Result<SendableRecordBatchStream>` - A stream of `RecordBatch`es if successful
///
/// This function first executes the given input plan for the specified partition
/// and context. It then checks if there are any columns in the input that might
/// violate the `not null` constraints specified in the `sink_schema`. If there are
/// such columns, it wraps the resulting stream to enforce the `not null` constraints
/// by invoking the [`check_not_null_constraints`] function on each batch of the stream.
#[expect(
    clippy::needless_pass_by_value,
    reason = "Public API that historically takes owned Arcs"
)]
pub fn execute_input_stream(
    input: Arc<dyn ExecutionPlan>,
    sink_schema: SchemaRef,
    partition: usize,
    context: Arc<TaskContext>,
) -> Result<SendableRecordBatchStream> {
    let input_stream = input.execute(partition, context)?;

    debug_assert_eq!(sink_schema.fields().len(), input.schema().fields().len());

    // Find input columns that may violate the not null constraint.
    let risky_columns: Vec<_> = sink_schema
        .fields()
        .iter()
        .zip(input.schema().fields().iter())
        .enumerate()
        .filter_map(|(idx, (sink_field, input_field))| {
            (!sink_field.is_nullable() && input_field.is_nullable()).then_some(idx)
        })
        .collect();

    if risky_columns.is_empty() {
        Ok(input_stream)
    } else {
        // Check not null constraint on the input stream
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            sink_schema,
            input_stream
                .map(move |batch| check_not_null_constraints(batch?, &risky_columns)),
        )))
    }
}

/// Checks a `RecordBatch` for `not null` constraints on specified columns.
///
/// # Arguments
///
/// * `batch` - The `RecordBatch` to be checked
/// * `column_indices` - A vector of column indices that should be checked for
///   `not null` constraints.
///
/// # Returns
///
/// * `Result<RecordBatch>` - The original `RecordBatch` if all constraints are met
///
/// This function iterates over the specified column indices and ensures that none
/// of the columns contain null values. If any column contains null values, an error
/// is returned.
pub fn check_not_null_constraints(
    batch: RecordBatch,
    column_indices: &Vec<usize>,
) -> Result<RecordBatch> {
    for &index in column_indices {
        if batch.num_columns() <= index {
            return exec_err!(
                "Invalid batch column count {} expected > {}",
                batch.num_columns(),
                index
            );
        }

        if batch
            .column(index)
            .logical_nulls()
            .map(|nulls| nulls.null_count())
            .unwrap_or_default()
            > 0
        {
            return exec_err!(
                "Invalid batch column at '{}' has null but schema specifies non-nullable",
                index
            );
        }
    }

    Ok(batch)
}

/// Utility function yielding a string representation of the given [`ExecutionPlan`].
pub fn get_plan_string(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    let formatted = displayable(plan.as_ref()).indent(true).to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    actual.iter().map(|elem| (*elem).to_string()).collect()
}

/// Indicates the effect an execution plan operator will have on the cardinality
/// of its input stream
pub enum CardinalityEffect {
    /// Unknown effect. This is the default
    Unknown,
    /// The operator is guaranteed to produce exactly one row for
    /// each input row
    Equal,
    /// The operator may produce fewer output rows than it receives input rows
    LowerEqual,
    /// The operator may produce more output rows than it receives input rows
    GreaterEqual,
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Arc;

    use super::*;
    use crate::{DisplayAs, DisplayFormatType, ExecutionPlan};

    use arrow::array::{DictionaryArray, Int32Array, NullArray, RunArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::{Result, Statistics};
    use datafusion_execution::{SendableRecordBatchStream, TaskContext};

    #[derive(Debug)]
    pub struct EmptyExec;

    impl EmptyExec {
        pub fn new(_schema: SchemaRef) -> Self {
            Self
        }
    }

    impl DisplayAs for EmptyExec {
        fn fmt_as(
            &self,
            _t: DisplayFormatType,
            _f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            unimplemented!()
        }
    }

    impl ExecutionPlan for EmptyExec {
        fn name(&self) -> &'static str {
            Self::static_name()
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &PlanProperties {
            unimplemented!()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!()
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            unimplemented!()
        }

        fn statistics(&self) -> Result<Statistics> {
            unimplemented!()
        }

        fn partition_statistics(&self, _partition: Option<usize>) -> Result<Statistics> {
            unimplemented!()
        }
    }

    #[derive(Debug)]
    pub struct RenamedEmptyExec;

    impl RenamedEmptyExec {
        pub fn new(_schema: SchemaRef) -> Self {
            Self
        }
    }

    impl DisplayAs for RenamedEmptyExec {
        fn fmt_as(
            &self,
            _t: DisplayFormatType,
            _f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            unimplemented!()
        }
    }

    impl ExecutionPlan for RenamedEmptyExec {
        fn name(&self) -> &'static str {
            Self::static_name()
        }

        fn static_name() -> &'static str
        where
            Self: Sized,
        {
            "MyRenamedEmptyExec"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &PlanProperties {
            unimplemented!()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!()
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            unimplemented!()
        }

        fn statistics(&self) -> Result<Statistics> {
            unimplemented!()
        }

        fn partition_statistics(&self, _partition: Option<usize>) -> Result<Statistics> {
            unimplemented!()
        }
    }

    #[test]
    fn test_execution_plan_name() {
        let schema1 = Arc::new(Schema::empty());
        let default_name_exec = EmptyExec::new(schema1);
        assert_eq!(default_name_exec.name(), "EmptyExec");

        let schema2 = Arc::new(Schema::empty());
        let renamed_exec = RenamedEmptyExec::new(schema2);
        assert_eq!(renamed_exec.name(), "MyRenamedEmptyExec");
        assert_eq!(RenamedEmptyExec::static_name(), "MyRenamedEmptyExec");
    }

    /// A compilation test to ensure that the `ExecutionPlan::name()` method can
    /// be called from a trait object.
    /// Related ticket: https://github.com/apache/datafusion/pull/11047
    #[expect(unused)]
    fn use_execution_plan_as_trait_object(plan: &dyn ExecutionPlan) {
        let _ = plan.name();
    }

    #[test]
    fn test_check_not_null_constraints_accept_non_null() -> Result<()> {
        check_not_null_constraints(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)])),
                vec![Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)]))],
            )?,
            &vec![0],
        )?;
        Ok(())
    }

    #[test]
    fn test_check_not_null_constraints_reject_null() -> Result<()> {
        let result = check_not_null_constraints(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)])),
                vec![Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]))],
            )?,
            &vec![0],
        );
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().strip_backtrace(),
            "Execution error: Invalid batch column at '0' has null but schema specifies non-nullable",
        );
        Ok(())
    }

    #[test]
    fn test_check_not_null_constraints_with_run_end_array() -> Result<()> {
        // some null value inside REE array
        let run_ends = Int32Array::from(vec![1, 2, 3, 4]);
        let values = Int32Array::from(vec![Some(0), None, Some(1), None]);
        let run_end_array = RunArray::try_new(&run_ends, &values)?;
        let result = check_not_null_constraints(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "a",
                    run_end_array.data_type().to_owned(),
                    true,
                )])),
                vec![Arc::new(run_end_array)],
            )?,
            &vec![0],
        );
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().strip_backtrace(),
            "Execution error: Invalid batch column at '0' has null but schema specifies non-nullable",
        );
        Ok(())
    }

    #[test]
    fn test_check_not_null_constraints_with_dictionary_array_with_null() -> Result<()> {
        let values = Arc::new(Int32Array::from(vec![Some(1), None, Some(3), Some(4)]));
        let keys = Int32Array::from(vec![0, 1, 2, 3]);
        let dictionary = DictionaryArray::new(keys, values);
        let result = check_not_null_constraints(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "a",
                    dictionary.data_type().to_owned(),
                    true,
                )])),
                vec![Arc::new(dictionary)],
            )?,
            &vec![0],
        );
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().strip_backtrace(),
            "Execution error: Invalid batch column at '0' has null but schema specifies non-nullable",
        );
        Ok(())
    }

    #[test]
    fn test_check_not_null_constraints_with_dictionary_masking_null() -> Result<()> {
        // some null value marked out by dictionary array
        let values = Arc::new(Int32Array::from(vec![
            Some(1),
            None, // this null value is masked by dictionary keys
            Some(3),
            Some(4),
        ]));
        let keys = Int32Array::from(vec![0, /*1,*/ 2, 3]);
        let dictionary = DictionaryArray::new(keys, values);
        check_not_null_constraints(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "a",
                    dictionary.data_type().to_owned(),
                    true,
                )])),
                vec![Arc::new(dictionary)],
            )?,
            &vec![0],
        )?;
        Ok(())
    }

    #[test]
    fn test_check_not_null_constraints_on_null_type() -> Result<()> {
        // null value of Null type
        let result = check_not_null_constraints(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("a", DataType::Null, true)])),
                vec![Arc::new(NullArray::new(3))],
            )?,
            &vec![0],
        );
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().strip_backtrace(),
            "Execution error: Invalid batch column at '0' has null but schema specifies non-nullable",
        );
        Ok(())
    }
}
