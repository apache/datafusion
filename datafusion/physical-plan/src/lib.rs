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

mod visitor;
pub use self::metrics::Metric;
use self::metrics::MetricsSet;
use self::{
    coalesce_partitions::CoalescePartitionsExec, display::DisplayableExecutionPlan,
};
pub use datafusion_common::{internal_err, ColumnStatistics, Statistics};
use datafusion_common::{plan_err, Result};
use datafusion_physical_expr::PhysicalSortExpr;
pub use visitor::{accept, visit_execution_plan, ExecutionPlanVisitor};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use datafusion_common::utils::DataPtr;
pub use datafusion_expr::Accumulator;
pub use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::equivalence::OrderingEquivalenceProperties;
pub use display::{DefaultDisplay, DisplayAs, DisplayFormatType, VerboseDisplay};
use futures::stream::TryStreamExt;
use std::fmt::Debug;
use tokio::task::JoinSet;

use datafusion_common::tree_node::Transformed;
use datafusion_common::DataFusionError;
use std::any::Any;
use std::sync::Arc;

// backwards compatibility
pub use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
pub use stream::EmptyRecordBatchStream;

/// `ExecutionPlan` represent nodes in the DataFusion Physical Plan.
///
/// Each `ExecutionPlan` is partition-aware and is responsible for
/// creating the actual `async` [`SendableRecordBatchStream`]s
/// of [`RecordBatch`] that incrementally compute the operator's
/// output from its input partition.
///
/// [`ExecutionPlan`] can be displayed in a simplified form using the
/// return value from [`displayable`] in addition to the (normally
/// quite verbose) `Debug` output.
pub trait ExecutionPlan: Debug + DisplayAs + Send + Sync {
    /// Returns the execution plan as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef;

    /// Specifies the output partitioning scheme of this plan
    fn output_partitioning(&self) -> Partitioning;

    /// Specifies whether this plan generates an infinite stream of records.
    /// If the plan does not support pipelining, but its input(s) are
    /// infinite, returns an error to indicate this.
    fn unbounded_output(&self, _children: &[bool]) -> Result<bool> {
        if _children.iter().any(|&x| x) {
            plan_err!("Plan does not support infinite stream from its children")
        } else {
            Ok(false)
        }
    }

    /// If the output of this operator within each partition is sorted,
    /// returns `Some(keys)` with the description of how it was sorted.
    ///
    /// For example, Sort, (obviously) produces sorted output as does
    /// SortPreservingMergeStream. Less obviously `Projection`
    /// produces sorted output if its input was sorted as it does not
    /// reorder the input rows,
    ///
    /// It is safe to return `None` here if your operator does not
    /// have any particular output order here
    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]>;

    /// Specifies the data distribution requirements for all the
    /// children for this operator, By default it's [[Distribution::UnspecifiedDistribution]] for each child,
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution; self.children().len()]
    }

    /// Specifies the ordering requirements for all of the children
    /// For each child, it's the local ordering requirement within
    /// each partition rather than the global ordering
    ///
    /// NOTE that checking `!is_empty()` does **not** check for a
    /// required input ordering. Instead, the correct check is that at
    /// least one entry must be `Some`
    fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
        vec![None; self.children().len()]
    }

    /// Returns `false` if this operator's implementation may reorder
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
    /// the operator's maintains the ordering invariant or else
    /// DataFusion may produce incorrect results.
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false; self.children().len()]
    }

    /// Specifies whether the operator benefits from increased parallelization
    /// at its input for each child. If set to `true`, this indicates that the
    /// operator would benefit from partitioning its corresponding child
    /// (and thus from more parallelism). For operators that do very little work
    /// the overhead of extra parallelism may outweigh any benefits
    ///
    /// The default implementation returns `true` unless this operator
    /// has signalled it requires a single child input partition.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // By default try to maximize parallelism with more CPUs if
        // possible
        self.required_input_distribution()
            .into_iter()
            .map(|dist| !matches!(dist, Distribution::SinglePartition))
            .collect()
    }

    /// Get the EquivalenceProperties within the plan
    fn equivalence_properties(&self) -> EquivalenceProperties {
        EquivalenceProperties::new(self.schema())
    }

    /// Get the OrderingEquivalenceProperties within the plan
    fn ordering_equivalence_properties(&self) -> OrderingEquivalenceProperties {
        OrderingEquivalenceProperties::new(self.schema())
    }

    /// Get a list of child execution plans that provide the input for this plan. The returned list
    /// will be empty for leaf nodes, will contain a single value for unary nodes, or two
    /// values for binary nodes (such as joins).
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>>;

    /// Returns a new plan where all children were replaced by new plans.
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// creates an iterator
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream>;

    /// Return a snapshot of the set of [`Metric`]s for this
    /// [`ExecutionPlan`].
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

    /// Returns the global output statistics for this `ExecutionPlan` node.
    fn statistics(&self) -> Statistics;
}

/// Indicate whether a data exchange is needed for the input of `plan`, which will be very helpful
/// especially for the distributed engine to judge whether need to deal with shuffling.
/// Currently there are 3 kinds of execution plan which needs data exchange
///     1. RepartitionExec for changing the partition number between two operators
///     2. CoalescePartitionsExec for collapsing all of the partitions into one without ordering guarantee
///     3. SortPreservingMergeExec for collapsing all of the sorted partitions into one with ordering guarantee
pub fn need_data_exchange(plan: Arc<dyn ExecutionPlan>) -> bool {
    if let Some(repart) = plan.as_any().downcast_ref::<RepartitionExec>() {
        !matches!(
            repart.output_partitioning(),
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

/// Execute the [ExecutionPlan] and return a single stream of results
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
            assert_eq!(1, plan.output_partitioning().partition_count());
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

/// Execute the [ExecutionPlan] and return a vec with one stream per output partition
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

use datafusion_physical_expr::expressions::Column;
pub use datafusion_physical_expr::window::WindowExpr;
pub use datafusion_physical_expr::{AggregateExpr, PhysicalExpr};
pub use datafusion_physical_expr::{Distribution, Partitioning};
use datafusion_physical_expr::{EquivalenceProperties, PhysicalSortRequirement};

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
pub mod projection;
pub mod repartition;
mod row_converter;
pub mod sorts;
pub mod stream;
pub mod streaming;
pub mod tree_node;
pub mod udaf;
pub mod union;
pub mod unnest;
pub mod values;
pub mod windows;

use crate::repartition::RepartitionExec;
use crate::sorts::sort_preserving_merge::SortPreservingMergeExec;
pub use datafusion_common::utils::project_schema;
use datafusion_execution::TaskContext;
pub use datafusion_physical_expr::{
    expressions, functions, hash_utils, ordering_equivalence_properties_helper, udf,
};

#[cfg(test)]
pub mod test;
