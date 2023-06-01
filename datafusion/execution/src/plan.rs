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

use arrow::datatypes::SchemaRef;
use core::fmt;
use datafusion_common::{Result, Statistics};
use datafusion_expr::Partitioning;
use datafusion_physical_expr::{
    Distribution, EquivalenceProperties, OrderingEquivalenceProperties, PhysicalSortExpr,
    PhysicalSortRequirement,
};
use std::{any::Any, fmt::Debug, sync::Arc};

use crate::{stream::SendableRecordBatchStream, TaskContext};

/// Options for controlling how each [`ExecutionPlan`] should format itself
#[derive(Debug, Clone, Copy)]
pub enum DisplayFormatType {
    /// Default, compact format. Example: `FilterExec: c12 < 10.0`
    Default,
}

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
pub trait ExecutionPlan: Debug + Send + Sync {
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
        Ok(false)
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

    /// Returns `true` if this operator would benefit from
    /// partitioning its input (and thus from more parallelism). For
    /// operators that do very little work the overhead of extra
    /// parallelism may outweigh any benefits
    ///
    /// The default implementation returns `true` unless this operator
    /// has signalled it requires a single child input partition.
    fn benefits_from_input_partitioning(&self) -> bool {
        // By default try to maximize parallelism with more CPUs if
        // possible
        !self
            .required_input_distribution()
            .into_iter()
            .any(|dist| matches!(dist, Distribution::SinglePartition))
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

    /// Format this `ExecutionPlan` to `f` in the specified type.
    ///
    /// Should not include a newline
    ///
    /// Note this function prints a placeholder by default to preserve
    /// backwards compatibility.
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ExecutionPlan(PlaceHolder)")
    }

    /// Returns the global output statistics for this `ExecutionPlan` node.
    fn statistics(&self) -> Statistics;
}
