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

// Some of these functions reference the Postgres documentation
// or implementation to ensure compatibility and are subject to
// the Postgres license.

//! The Union operator combines multiple inputs with the same schema

use std::borrow::Borrow;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, RecordBatchStream, SendableRecordBatchStream, Statistics,
    metrics::{ExecutionPlanMetricsSet, MetricsSet},
};
use crate::execution_plan::{
    CardinalityEffect, InvariantLevel, boundedness_from_children,
    check_default_invariants, emission_type_from_children,
};
use crate::filter::FilterExec;
use crate::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase,
    FilterPushdownPropagation, PushedDown,
};
use crate::metrics::BaselineMetrics;
use crate::projection::{ProjectionExec, ProjectionExpr, make_with_child};
use crate::statistics::{ChildStats, StatisticsArgs};
use crate::stream::ObservedStream;
use crate::{ChildrenPropertiesMode, ReplaceChildrenOptions, validate_child_count};

use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::config::ConfigOptions;
use datafusion_common::stats::NdvFallback;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{
    Result, assert_or_internal_err, exec_err, internal_datafusion_err, plan_err,
};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::expressions::{CastExpr, Column};
use datafusion_physical_expr::{
    EquivalenceProperties, PhysicalExpr, calculate_union, conjunction,
};

use futures::Stream;
use itertools::Itertools;
use log::{debug, trace, warn};
use tokio::macros::support::thread_rng_n;

/// Coerces `input`'s output schema to exactly `schema` via a `ProjectionExec`
/// that re-stamps each column with the union's merged field (same
/// `DataType`, but the union's merged nullability/name/metadata), or returns
/// `input` unchanged if its schema already matches. [`UnionExec::try_new`]
/// and [`InterleaveExec::try_new`] call this on every child, so the coercion
/// is visible in the plan tree (e.g. in `EXPLAIN`) instead of happening
/// invisibly inside the union operator's own `execute()`.
///
/// A column whose `DataType` doesn't already match the union's is a genuine
/// data type mismatch (as opposed to a nullability/name/metadata-only one),
/// and is rejected eagerly here rather than silently cast or deferred to a
/// runtime failure -- this only ever changes a column's declared schema,
/// never its values.
///
/// Casting a column to its own `DataType` (only the `Field`'s nullability,
/// name, or metadata changes) is a zero-copy relabeling: the cast kernel's
/// same-type fast path (`cast_array_by_name`) just clones the `Arc<dyn
/// Array>`, so this carries no runtime overhead over the schema it replaces.
///
/// See <https://github.com/apache/datafusion/issues/15394>.
fn coerce_schema(
    input: Arc<dyn ExecutionPlan>,
    schema: &SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    let input_schema = input.schema();
    if &input_schema == schema {
        return Ok(input);
    }

    let exprs = input_schema
        .fields()
        .iter()
        .zip(schema.fields())
        .enumerate()
        .map(|(i, (input_field, target_field))| {
            if input_field.data_type() != target_field.data_type() {
                return plan_err!(
                    "UnionExec/InterleaveExec requires all inputs to have the same \
                     data type per column; column {i} has type {} in one input, but \
                     the union schema expects {}",
                    input_field.data_type(),
                    target_field.data_type()
                );
            }
            let column: Arc<dyn PhysicalExpr> =
                Arc::new(Column::new(input_field.name(), i));
            let expr = if input_field == target_field {
                column
            } else {
                Arc::new(CastExpr::new_with_target_field(
                    column,
                    Arc::clone(target_field),
                    None,
                )) as Arc<dyn PhysicalExpr>
            };
            Ok(ProjectionExpr {
                expr,
                alias: target_field.name().clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Arc::new(ProjectionExec::try_new(exprs, input)?))
}

/// `UnionExec`: `UNION ALL` execution plan.
///
/// `UnionExec` combines multiple inputs with the same schema by
/// concatenating the partitions.  It does not mix or copy data within
/// or across partitions. Thus if the input partitions are sorted, the
/// output partitions of the union are also sorted.
///
/// For example, given a `UnionExec` of two inputs, with `N`
/// partitions, and `M` partitions, there will be `N+M` output
/// partitions. The first `N` output partitions are from Input 1
/// partitions, and then next `M` output partitions are from Input 2.
///
/// ```text
///                        ▲       ▲           ▲         ▲
///                        │       │           │         │
///      Output            │  ...  │           │         │
///    Partitions          │0      │N-1        │ N       │N+M-1
/// (passes through   ┌────┴───────┴───────────┴─────────┴───┐
///  the N+M input    │              UnionExec               │
///   partitions)     │                                      │
///                   └──────────────────────────────────────┘
///                                      ▲
///                                      │
///                                      │
///       Input           ┌────────┬─────┴────┬──────────┐
///     Partitions        │ ...    │          │     ...  │
///                    0  │        │ N-1      │ 0        │  M-1
///                  ┌────┴────────┴───┐  ┌───┴──────────┴───┐
///                  │                 │  │                  │
///                  │                 │  │                  │
///                  │                 │  │                  │
///                  │                 │  │                  │
///                  │                 │  │                  │
///                  │                 │  │                  │
///                  │Input 1          │  │Input 2           │
///                  └─────────────────┘  └──────────────────┘
/// ```
#[derive(Debug, Clone)]
pub struct UnionExec {
    /// Input execution plan
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: Arc<PlanProperties>,
}

impl UnionExec {
    /// Try to create a new UnionExec.
    ///
    /// # Errors
    /// Returns an error if:
    /// - `inputs` is empty
    ///
    /// # Optimization
    /// If there is only one input, returns that input directly rather than wrapping it in a UnionExec
    pub fn try_new(
        inputs: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match inputs.len() {
            0 => exec_err!("UnionExec requires at least one input"),
            1 => Ok(inputs.into_iter().next().unwrap()),
            _ => {
                let schema = union_schema(&inputs)?;
                // The schema of the inputs and the union schema is consistent when:
                // - They have the same number of fields, and
                // - Their fields have same types at the same indices.
                let inputs = inputs
                    .into_iter()
                    .map(|input| coerce_schema(input, &schema))
                    .collect::<Result<Vec<_>>>()?;
                let cache = Self::compute_properties(&inputs, schema)?;
                Ok(Arc::new(UnionExec {
                    inputs,
                    metrics: ExecutionPlanMetricsSet::new(),
                    cache: Arc::new(cache),
                }))
            }
        }
    }

    /// Get inputs of the execution plan
    pub fn inputs(&self) -> &Vec<Arc<dyn ExecutionPlan>> {
        &self.inputs
    }

    /// Maps a global output partition index to the `(input index, local
    /// partition index)` of the input that owns it, or `None` if out of range.
    fn owning_input(&self, partition: usize) -> Option<(usize, usize)> {
        let mut remaining = partition;
        for (i, input) in self.inputs.iter().enumerate() {
            let count = input.output_partitioning().partition_count();
            if remaining < count {
                return Some((i, remaining));
            }
            remaining -= count;
        }
        None
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        inputs: &[Arc<dyn ExecutionPlan>],
        schema: SchemaRef,
    ) -> Result<PlanProperties> {
        // Calculate equivalence properties:
        let children_eqps = inputs
            .iter()
            .map(|child| child.equivalence_properties().clone())
            .collect::<Vec<_>>();
        let eq_properties = calculate_union(children_eqps, schema)?;

        // Calculate output partitioning; i.e. sum output partitions of the inputs.
        let num_partitions = inputs
            .iter()
            .map(|plan| plan.output_partitioning().partition_count())
            .sum();
        let output_partitioning = Partitioning::UnknownPartitioning(num_partitions);
        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            emission_type_from_children(inputs),
            boundedness_from_children(inputs),
        ))
    }
}

impl DisplayAs for UnionExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "UnionExec")
            }
            DisplayFormatType::TreeRender => Ok(()),
        }
    }
}

impl ExecutionPlan for UnionExec {
    fn name(&self) -> &'static str {
        "UnionExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn check_invariants(&self, check: InvariantLevel) -> Result<()> {
        check_default_invariants(self, check)?;

        (self.inputs().len() >= 2).then_some(()).ok_or_else(|| {
            internal_datafusion_err!("UnionExec should have at least 2 children")
        })
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // If the Union has an output ordering, it maintains at least one
        // child's ordering (i.e. the meet).
        // For instance, assume that the first child is SortExpr('a','b','c'),
        // the second child is SortExpr('a','b') and the third child is
        // SortExpr('a','b'). The output ordering would be SortExpr('a','b'),
        // which is the "meet" of all input orderings. In this example, this
        // function will return vec![false, true, true], indicating that we
        // preserve the orderings for the 2nd and the 3rd children.
        if let Some(output_ordering) = self.properties().output_ordering() {
            self.inputs()
                .iter()
                .map(|child| {
                    if let Some(child_ordering) = child.output_ordering() {
                        output_ordering.len() == child_ordering.len()
                    } else {
                        false
                    }
                })
                .collect()
        } else {
            vec![false; self.inputs().len()]
        }
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false; self.children().len()]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().collect()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        validate_child_count!(self, children);
        match options.children_properties {
            ChildrenPropertiesMode::Keep => Ok(Arc::new(Self {
                inputs: children,
                metrics: ExecutionPlanMetricsSet::new(),
                ..Self::clone(&*self)
            })),
            ChildrenPropertiesMode::Recompute => UnionExec::try_new(children),
        }
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

    fn with_new_children_and_same_properties(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Keep),
        )
    }

    fn execute(
        &self,
        mut partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!(
            "Start UnionExec::execute for partition {} of context session_id {} and task_id {:?}",
            partition,
            context.session_id(),
            context.task_id()
        );
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        // record the tiny amount of work done in this function so
        // elapsed_compute is reported as non zero
        let elapsed_compute = baseline_metrics.elapsed_compute().clone();
        let _timer = elapsed_compute.timer(); // record on drop

        // find partition to execute
        for input in self.inputs.iter() {
            // Calculate whether partition belongs to the current partition
            if partition < input.output_partitioning().partition_count() {
                let stream = input.execute(partition, context)?;
                debug!("Found a Union partition to execute");
                return Ok(Box::pin(ObservedStream::new(
                    stream,
                    baseline_metrics,
                    None,
                )));
            } else {
                partition -= input.output_partitioning().partition_count();
            }
        }

        warn!("Error in Union: Partition {partition} not found");

        exec_err!("Partition {partition} not found in Union")
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        if let Some(partition_idx) = partition {
            // For a specific partition, compute stats only for the input that
            // owns it; the other inputs are not needed and are skipped.
            let targeted = self.owning_input(partition_idx);
            self.inputs
                .iter()
                .enumerate()
                .map(|(i, _)| match targeted {
                    Some((target_i, target_partition)) if i == target_i => {
                        ChildStats::At(Some(target_partition))
                    }
                    _ => ChildStats::Skip,
                })
                .collect()
        } else {
            vec![ChildStats::At(None); self.inputs.len()]
        }
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        if let Some(partition_idx) = args.partition() {
            // For a specific partition, find which input it belongs to
            if let Some((target_i, _)) = self.owning_input(partition_idx) {
                // This partition belongs to this input - return its stats
                return Ok(Arc::clone(&input_stats[target_i]));
            }
            // If we get here, the partition index is out of bounds
            Ok(Arc::new(Statistics::new_unknown(&self.schema())))
        } else {
            let stats_refs = input_stats.iter().map(|s| s.as_ref()).collect::<Vec<_>>();

            Ok(Arc::new(Statistics::try_merge_iter_with_ndv_fallback(
                stats_refs,
                self.schema().as_ref(),
                NdvFallback::Sum,
            )?))
        }
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        // Union combines rows from multiple inputs, so output rows are not tied
        // to any single input and can only be constrained as greater-or-equal.
        CardinalityEffect::GreaterEqual
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    /// Tries to push `projection` down through `union`. If possible, performs the
    /// pushdown and returns a new [`UnionExec`] as the top plan which has projections
    /// as its children. Otherwise, returns `None`.
    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // If the projection doesn't narrow the schema, we shouldn't try to push it down.
        if projection.expr().len() >= projection.input().schema().fields().len() {
            return Ok(None);
        }

        let new_children = self
            .children()
            .into_iter()
            .map(|child| make_with_child(projection, child))
            .collect::<Result<Vec<_>>>()?;

        Ok(Some(UnionExec::try_new(new_children.clone())?))
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }

    fn handle_child_pushdown_result(
        &self,
        phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        // Pre phase: handle heterogeneous pushdown by wrapping individual
        // children with FilterExec and reporting all filters as handled.
        // Post phase: use default behavior to let the filter creator decide how to handle
        // filters that weren't fully pushed down.
        if phase != FilterPushdownPhase::Pre {
            return Ok(FilterPushdownPropagation::if_all(child_pushdown_result));
        }

        // UnionExec needs specialized filter pushdown handling when children have
        // heterogeneous pushdown support. Without this, when some children support
        // pushdown and others don't, the default behavior would leave FilterExec
        // above UnionExec, re-applying filters to outputs of all children—including
        // those that already applied the filters via pushdown. This specialized
        // implementation adds FilterExec only to children that don't support
        // pushdown, avoiding redundant filtering and improving performance.
        //
        // Example: Given Child1 (no pushdown support) and Child2 (has pushdown support)
        //   Default behavior:          This implementation:
        //   FilterExec                 UnionExec
        //     UnionExec                  FilterExec
        //       Child1                     Child1
        //       Child2(filter)           Child2(filter)

        // Collect unsupported filters for each child
        let mut unsupported_filters_per_child = vec![Vec::new(); self.inputs.len()];
        for parent_filter_result in child_pushdown_result.parent_filters.iter() {
            for (child_idx, &child_result) in
                parent_filter_result.child_results.iter().enumerate()
            {
                if matches!(child_result, PushedDown::No) {
                    unsupported_filters_per_child[child_idx]
                        .push(Arc::clone(&parent_filter_result.filter));
                }
            }
        }

        // Wrap children that have unsupported filters with FilterExec
        let mut new_children = self.inputs.clone();
        for (child_idx, unsupported_filters) in
            unsupported_filters_per_child.iter().enumerate()
        {
            if !unsupported_filters.is_empty() {
                let combined_filter = conjunction(unsupported_filters.clone());
                new_children[child_idx] = Arc::new(FilterExec::try_new(
                    combined_filter,
                    Arc::clone(&self.inputs[child_idx]),
                )?);
            }
        }

        // Check if any children were modified
        let children_modified = new_children
            .iter()
            .zip(self.inputs.iter())
            .any(|(new, old)| !Arc::ptr_eq(new, old));

        let all_filters_pushed =
            vec![PushedDown::Yes; child_pushdown_result.parent_filters.len()];
        let propagation = if children_modified {
            let updated_node = UnionExec::try_new(new_children)?;
            FilterPushdownPropagation::with_parent_pushdown_result(all_filters_pushed)
                .with_updated_node(updated_node)
        } else {
            FilterPushdownPropagation::with_parent_pushdown_result(all_filters_pushed)
        };

        // Report all parent filters as supported since we've ensured they're applied
        // on all children (either pushed down or via FilterExec)
        Ok(propagation)
    }
    #[cfg(feature = "proto")]
    fn try_to_proto(
        &self,
        ctx: &crate::proto::ExecutionPlanEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto_models::protobuf::PhysicalPlanNode>> {
        use datafusion_proto_models::protobuf;
        // Destructure exhaustively (no `..`) so that adding a field to
        // `UnionExec` is a compile error here until it is either serialized or
        // explicitly documented as not needing to be.
        let Self {
            inputs,
            // Runtime metrics, not part of the plan shape.
            metrics: _,
            // Derived plan properties, recomputed on decode.
            cache: _,
        } = self;
        let inputs = ctx.encode_children(inputs)?;
        Ok(Some(protobuf::PhysicalPlanNode {
            physical_plan_type: Some(
                protobuf::physical_plan_node::PhysicalPlanType::Union(
                    protobuf::UnionExecNode { inputs },
                ),
            ),
        }))
    }
}

#[cfg(feature = "proto")]
impl UnionExec {
    pub fn try_from_proto(
        node: &datafusion_proto_models::protobuf::PhysicalPlanNode,
        ctx: &crate::proto::ExecutionPlanDecodeCtx<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion_proto_models::protobuf;
        let union = crate::expect_plan_variant!(
            node,
            protobuf::physical_plan_node::PhysicalPlanType::Union,
            "UnionExec",
        );
        // Destructure exhaustively so that a new field on `UnionExecNode` is a
        // compile error here rather than a silently dropped field.
        let protobuf::UnionExecNode { inputs } = union;
        let inputs = inputs
            .iter()
            .map(|input| ctx.decode_child(input))
            .collect::<Result<Vec<_>>>()?;
        UnionExec::try_new(inputs)
    }
}

/// Combines multiple input streams by interleaving them.
///
/// All inputs must share an identical [`Partitioning::Hash`] or [`Partitioning::Range`] so that
/// partition `k` covers the same data across every input. Each output partition is the
/// interleaving of the same-indexed partition from all inputs:
/// `output[k] = input[0][k] + input[1][k] + ... + input[n-1][k]`
///
/// # Data Flow
/// ```text
/// +---------+
/// |         |---+
/// | Input 1 |   |
/// |         |-------------+
/// +---------+   |         |
///               |         |         +---------+
///               +------------------>|         |
///                 +---------------->| Combine |-->
///                 | +-------------->|         |
///                 | |     |         +---------+
/// +---------+     | |     |
/// |         |-----+ |     |
/// | Input 2 |       |     |
/// |         |---------------+
/// +---------+       |     | |
///                   |     | |       +---------+
///                   |     +-------->|         |
///                   |       +------>| Combine |-->
///                   |         +---->|         |
///                   |         |     +---------+
/// +---------+       |         |
/// |         |-------+         |
/// | Input 3 |                 |
/// |         |-----------------+
/// +---------+
/// ```
#[derive(Debug, Clone)]
pub struct InterleaveExec {
    /// Input execution plan
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: Arc<PlanProperties>,
}

impl InterleaveExec {
    /// Create a new InterleaveExec
    pub fn try_new(inputs: Vec<Arc<dyn ExecutionPlan>>) -> Result<Self> {
        assert_or_internal_err!(
            can_interleave(inputs.iter()),
            "Not all InterleaveExec children have a consistent hash or range partitioning"
        );
        let schema = union_schema(&inputs)?;
        let inputs = inputs
            .into_iter()
            .map(|input| coerce_schema(input, &schema))
            .collect::<Result<Vec<_>>>()?;
        let cache = Self::compute_properties(&inputs, schema)?;
        Ok(InterleaveExec {
            inputs,
            metrics: ExecutionPlanMetricsSet::new(),
            cache: Arc::new(cache),
        })
    }

    /// Get inputs of the execution plan
    pub fn inputs(&self) -> &Vec<Arc<dyn ExecutionPlan>> {
        &self.inputs
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        inputs: &[Arc<dyn ExecutionPlan>],
        schema: SchemaRef,
    ) -> Result<PlanProperties> {
        let eq_properties = EquivalenceProperties::new(schema);
        // Get output partitioning:
        let output_partitioning = inputs[0].output_partitioning().clone();
        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            emission_type_from_children(inputs),
            boundedness_from_children(inputs),
        ))
    }
}

impl DisplayAs for InterleaveExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "InterleaveExec")
            }
            DisplayFormatType::TreeRender => Ok(()),
        }
    }
}

impl ExecutionPlan for InterleaveExec {
    fn name(&self) -> &'static str {
        "InterleaveExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().collect()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false; self.inputs().len()]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        validate_child_count!(self, children);
        match options.children_properties {
            ChildrenPropertiesMode::Keep => Ok(Arc::new(Self {
                inputs: children,
                metrics: ExecutionPlanMetricsSet::new(),
                ..Self::clone(&*self)
            })),
            ChildrenPropertiesMode::Recompute => {
                // New children are no longer interleavable, which might be a bug of optimization rewrite.
                assert_or_internal_err!(
                    can_interleave(children.iter()),
                    "Can not create InterleaveExec: new children can not be interleaved"
                );
                Ok(Arc::new(InterleaveExec::try_new(children)?))
            }
        }
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

    fn with_new_children_and_same_properties(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Keep),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!(
            "Start InterleaveExec::execute for partition {} of context session_id {} and task_id {:?}",
            partition,
            context.session_id(),
            context.task_id()
        );
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        // record the tiny amount of work done in this function so
        // elapsed_compute is reported as non zero
        let elapsed_compute = baseline_metrics.elapsed_compute().clone();
        let _timer = elapsed_compute.timer(); // record on drop

        let mut input_stream_vec = vec![];
        for input in self.inputs.iter() {
            if partition < input.output_partitioning().partition_count() {
                let stream = input.execute(partition, Arc::clone(&context))?;
                input_stream_vec.push(stream);
            } else {
                // Do not find a partition to execute
                break;
            }
        }
        if input_stream_vec.len() == self.inputs.len() {
            let stream = Box::pin(CombinedRecordBatchStream::new(
                self.schema(),
                input_stream_vec,
            ));
            return Ok(Box::pin(ObservedStream::new(
                stream,
                baseline_metrics,
                None,
            )));
        }

        warn!("Error in InterleaveExec: Partition {partition} not found");

        exec_err!("Partition {partition} not found in InterleaveExec")
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition); self.inputs.len()]
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        let stats = input_stats
            .iter()
            .map(|s| s.as_ref().clone())
            .collect::<Vec<_>>();

        Ok(Arc::new(Statistics::try_merge_iter_with_ndv_fallback(
            stats.iter(),
            self.schema().as_ref(),
            NdvFallback::Sum,
        )?))
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false; self.children().len()]
    }
    #[cfg(feature = "proto")]
    fn try_to_proto(
        &self,
        ctx: &crate::proto::ExecutionPlanEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto_models::protobuf::PhysicalPlanNode>> {
        use datafusion_proto_models::protobuf;
        // Destructure exhaustively (no `..`) so that adding a field to
        // `InterleaveExec` is a compile error here until it is either
        // serialized or explicitly documented as not needing to be.
        let Self {
            inputs,
            // Runtime metrics, not part of the plan shape.
            metrics: _,
            // Derived plan properties, recomputed on decode.
            cache: _,
        } = self;
        let inputs = ctx.encode_children(inputs)?;
        Ok(Some(protobuf::PhysicalPlanNode {
            physical_plan_type: Some(
                protobuf::physical_plan_node::PhysicalPlanType::Interleave(
                    protobuf::InterleaveExecNode { inputs },
                ),
            ),
        }))
    }
}

#[cfg(feature = "proto")]
impl InterleaveExec {
    pub fn try_from_proto(
        node: &datafusion_proto_models::protobuf::PhysicalPlanNode,
        ctx: &crate::proto::ExecutionPlanDecodeCtx<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion_proto_models::protobuf;
        let interleave = crate::expect_plan_variant!(
            node,
            protobuf::physical_plan_node::PhysicalPlanType::Interleave,
            "InterleaveExec",
        );
        // Destructure exhaustively so that a new field on `InterleaveExecNode`
        // is a compile error here rather than a silently dropped field.
        let protobuf::InterleaveExecNode { inputs } = interleave;
        let inputs = inputs
            .iter()
            .map(|input| ctx.decode_child(input))
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(InterleaveExec::try_new(inputs)?))
    }
}

/// Returns true if all inputs have the same [`Partitioning::Hash`] or [`Partitioning::Range`]
/// spec, making them safe to interleave. Two inputs are interleave-compatible when partition
/// `k` covers the identical key range or hash bucket across every input.
///
/// Note: compatibility is checked sequentially against the first input, so
/// `InputDistributionRequirements::co_partitioned` is not needed here.
///
/// It might be too strict here in the case that the input partition specs are compatible but not exactly the same.
/// For example one input partition has the partition spec Hash('a','b','c') and
/// other has the partition spec Hash('a'), It is safe to derive the out partition with the spec Hash('a','b','c').
pub fn can_interleave<T: Borrow<Arc<dyn ExecutionPlan>>>(
    mut inputs: impl Iterator<Item = T>,
) -> bool {
    let Some(first) = inputs.next() else {
        return false;
    };

    let reference = first.borrow().output_partitioning();
    matches!(reference, Partitioning::Hash(_, _) | Partitioning::Range(_))
        && inputs
            .map(|plan| plan.borrow().output_partitioning().clone())
            .all(|partition| partition == *reference)
}

fn union_schema(inputs: &[Arc<dyn ExecutionPlan>]) -> Result<SchemaRef> {
    if inputs.is_empty() {
        return exec_err!("Cannot create union schema from empty inputs");
    }

    let first_schema = inputs[0].schema();

    // Fast path: when every input already shares the first input's schema, the
    // field-by-field metadata/nullability merge below is redundant work that
    // scales as O(n^2 * fields). This is common in practice: unions built from
    // repartitioned copies of the same plan (e.g. observed in InfluxDB) hand us
    // children that all carry the exact same schema. A pointer-equality check
    // catches the shared-`Arc` case for free, and a content `==` comparison
    // catches distinct-but-equal schemas; both let us return early and hand back
    // the first schema unchanged.
    if inputs[1..].iter().all(|input| {
        let schema = input.schema();
        Arc::ptr_eq(&schema, &first_schema) || schema == first_schema
    }) {
        return Ok(first_schema);
    }

    let first_field_count = first_schema.fields().len();

    // validate that all inputs have the same number of fields
    for (idx, input) in inputs.iter().enumerate().skip(1) {
        let field_count = input.schema().fields().len();
        if field_count != first_field_count {
            return exec_err!(
                "UnionExec/InterleaveExec requires all inputs to have the same number of fields. \
                 Input 0 has {first_field_count} fields, but input {idx} has {field_count} fields"
            );
        }
    }

    let fields = (0..first_field_count)
        .map(|i| {
            // We take the name from the left side of the union to match how names are coerced during logical planning,
            // which also uses the left side names.
            let base_field = first_schema.field(i).clone();

            // Coerce metadata and nullability across all inputs

            inputs
                .iter()
                .enumerate()
                .map(|(input_idx, input)| {
                    let field = input.schema().field(i).clone();
                    let mut metadata = field.metadata().clone();

                    let other_metadatas = inputs
                        .iter()
                        .enumerate()
                        .filter(|(other_idx, _)| *other_idx != input_idx)
                        .flat_map(|(_, other_input)| {
                            other_input.schema().field(i).metadata().clone().into_iter()
                        });

                    metadata.extend(other_metadatas);
                    field.with_metadata(metadata)
                })
                .find_or_first(Field::is_nullable)
                // We can unwrap this because if inputs was empty, this would've already panic'ed when we
                // indexed into inputs[0].
                .unwrap()
                .with_name(base_field.name())
        })
        .collect::<Vec<_>>();

    let all_metadata_merged = inputs
        .iter()
        .flat_map(|i| i.schema().metadata().clone().into_iter())
        .collect();

    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        all_metadata_merged,
    )))
}

/// CombinedRecordBatchStream can be used to combine a Vec of SendableRecordBatchStreams into one
struct CombinedRecordBatchStream {
    /// Schema wrapped by Arc
    schema: SchemaRef,
    /// Stream entries
    entries: Vec<SendableRecordBatchStream>,
}

impl CombinedRecordBatchStream {
    /// Create an CombinedRecordBatchStream
    pub fn new(schema: SchemaRef, entries: Vec<SendableRecordBatchStream>) -> Self {
        Self { schema, entries }
    }
}

impl RecordBatchStream for CombinedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for CombinedRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        use Poll::*;

        let start = thread_rng_n(self.entries.len() as u32) as usize;
        let mut idx = start;

        for _ in 0..self.entries.len() {
            let stream = self.entries.get_mut(idx).unwrap();

            match Pin::new(stream).poll_next(cx) {
                Ready(Some(val)) => return Ready(Some(val)),
                Ready(None) => {
                    // Remove the entry
                    self.entries.swap_remove(idx);

                    // Check if this was the last entry, if so the cursor needs
                    // to wrap
                    if idx == self.entries.len() {
                        idx = 0;
                    } else if idx < start && start <= self.entries.len() {
                        // The stream being swapped into the current index has
                        // already been polled, so skip it.
                        idx = idx.wrapping_add(1) % self.entries.len();
                    }
                }
                Pending => {
                    idx = idx.wrapping_add(1) % self.entries.len();
                }
            }
        }

        // If the map is empty, then the stream is complete.
        if self.entries.is_empty() {
            Ready(None)
        } else {
            Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collect;
    use crate::repartition::RepartitionExec;
    use crate::statistics::{StatisticsArgs, StatisticsContext};
    use crate::test::exec::StatisticsExec;
    use crate::test::{self, TestMemoryExec};

    use arrow::compute::SortOptions;
    use arrow::datatypes::DataType;
    use datafusion_common::SplitPoint;
    use datafusion_common::stats::Precision;
    use datafusion_common::{ColumnStatistics, ScalarValue};
    use datafusion_physical_expr::RangePartitioning;
    use datafusion_physical_expr::equivalence::convert_to_orderings;
    use datafusion_physical_expr::expressions::col;
    use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};

    // Generate a schema which consists of 7 columns (a, b, c, d, e, f, g)
    fn create_test_schema() -> Result<SchemaRef> {
        let a = Field::new("a", DataType::Int32, true);
        let b = Field::new("b", DataType::Int32, true);
        let c = Field::new("c", DataType::Int32, true);
        let d = Field::new("d", DataType::Int32, true);
        let e = Field::new("e", DataType::Int32, true);
        let f = Field::new("f", DataType::Int32, true);
        let g = Field::new("g", DataType::Int32, true);
        let schema = Arc::new(Schema::new(vec![a, b, c, d, e, f, g]));

        Ok(schema)
    }

    fn create_test_schema2() -> Result<SchemaRef> {
        let a = Field::new("a", DataType::Int32, true);
        let b = Field::new("b", DataType::Int32, true);
        let c = Field::new("c", DataType::Int32, true);
        let d = Field::new("d", DataType::Int32, true);
        let e = Field::new("e", DataType::Int32, true);
        let f = Field::new("f", DataType::Int32, true);
        let schema = Arc::new(Schema::new(vec![a, b, c, d, e, f]));

        Ok(schema)
    }

    #[tokio::test]
    async fn test_union_partitions() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());

        // Create inputs with different partitioning
        let csv = test::scan_partitioned(4);
        let csv2 = test::scan_partitioned(5);

        let union_exec: Arc<dyn ExecutionPlan> = UnionExec::try_new(vec![csv, csv2])?;

        // Should have 9 partitions and 9 output batches
        assert_eq!(
            union_exec
                .properties()
                .output_partitioning()
                .partition_count(),
            9
        );

        let result: Vec<RecordBatch> = collect(union_exec, task_ctx).await?;
        assert_eq!(result.len(), 9);

        Ok(())
    }

    #[tokio::test]
    async fn test_interleave_conforms_batch_schema() -> Result<()> {
        // Two inputs agree on the column's type but disagree on nullability;
        // InterleaveExec's declared schema ORs nullability across inputs, so
        // every yielded batch must be re-stamped with that schema. See
        // <https://github.com/apache/datafusion/issues/15394>.
        let task_ctx = Arc::new(TaskContext::default());

        let schema_not_null =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch_not_null = RecordBatch::try_new(
            Arc::clone(&schema_not_null),
            vec![Arc::new(arrow::array::Int32Array::from(vec![1, 2]))],
        )?;

        let schema_nullable =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let batch_nullable = RecordBatch::try_new(
            Arc::clone(&schema_nullable),
            vec![Arc::new(arrow::array::Int32Array::from(vec![3, 4]))],
        )?;

        let hash_expr = vec![col("a", schema_not_null.as_ref())?];
        let left: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
            TestMemoryExec::try_new_exec(&[vec![batch_not_null]], schema_not_null, None)?,
            Partitioning::Hash(hash_expr.clone(), 1),
        )?);
        let right: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
            TestMemoryExec::try_new_exec(&[vec![batch_nullable]], schema_nullable, None)?,
            Partitioning::Hash(hash_expr, 1),
        )?);

        let interleave: Arc<dyn ExecutionPlan> =
            Arc::new(InterleaveExec::try_new(vec![left, right])?);
        let interleave_schema = interleave.schema();
        assert!(interleave_schema.field(0).is_nullable());

        let batches = collect(interleave, task_ctx).await?;
        assert!(!batches.is_empty());
        for batch in &batches {
            assert_eq!(batch.schema(), interleave_schema);
        }

        Ok(())
    }

    fn stats_merge_inputs() -> (SchemaRef, Statistics, Statistics, Statistics) {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::UInt32, true)]));

        let left = Statistics::default()
            .with_num_rows(Precision::Exact(5))
            .with_total_byte_size(Precision::Exact(23))
            .add_column_statistics(
                ColumnStatistics::new_unknown()
                    .with_distinct_count(Precision::Exact(5))
                    .with_min_value(Precision::Exact(ScalarValue::UInt32(Some(1))))
                    .with_max_value(Precision::Exact(ScalarValue::UInt32(Some(21))))
                    .with_sum_value(Precision::Exact(ScalarValue::UInt32(Some(42))))
                    .with_null_count(Precision::Exact(0))
                    .with_byte_size(Precision::Exact(40)),
            );

        let right = Statistics::default()
            .with_num_rows(Precision::Exact(7))
            .with_total_byte_size(Precision::Exact(29))
            .add_column_statistics(
                ColumnStatistics::new_unknown()
                    .with_distinct_count(Precision::Exact(3))
                    .with_min_value(Precision::Exact(ScalarValue::UInt32(Some(22))))
                    .with_max_value(Precision::Exact(ScalarValue::UInt32(Some(34))))
                    .with_sum_value(Precision::Exact(ScalarValue::UInt32(Some(8))))
                    .with_null_count(Precision::Exact(1))
                    .with_byte_size(Precision::Exact(60)),
            );

        let expected = Statistics::default()
            .with_num_rows(Precision::Exact(12))
            .with_total_byte_size(Precision::Exact(52))
            .add_column_statistics(
                ColumnStatistics::new_unknown()
                    .with_distinct_count(Precision::Inexact(8))
                    .with_min_value(Precision::Exact(ScalarValue::UInt32(Some(1))))
                    .with_max_value(Precision::Exact(ScalarValue::UInt32(Some(34))))
                    .with_sum_value(Precision::Exact(ScalarValue::UInt64(Some(50))))
                    .with_null_count(Precision::Exact(1))
                    .with_byte_size(Precision::Exact(100)),
            );

        (schema, left, right, expected)
    }

    fn stats_merge_multicolumn_inputs() -> (SchemaRef, Statistics, Statistics, Statistics)
    {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Float32, true),
        ]));

        let left = Statistics::default()
            .with_num_rows(Precision::Exact(5))
            .with_total_byte_size(Precision::Exact(23))
            .add_column_statistics(
                ColumnStatistics::new_unknown()
                    .with_distinct_count(Precision::Exact(5))
                    .with_min_value(Precision::Exact(ScalarValue::Int64(Some(-4))))
                    .with_max_value(Precision::Exact(ScalarValue::Int64(Some(21))))
                    .with_sum_value(Precision::Exact(ScalarValue::Int64(Some(42))))
                    .with_null_count(Precision::Exact(0)),
            )
            .add_column_statistics(
                ColumnStatistics::new_unknown()
                    .with_distinct_count(Precision::Exact(2))
                    .with_min_value(Precision::Exact(ScalarValue::from("a")))
                    .with_max_value(Precision::Exact(ScalarValue::from("x")))
                    .with_null_count(Precision::Exact(3)),
            )
            .add_column_statistics(
                ColumnStatistics::new_unknown()
                    .with_max_value(Precision::Exact(ScalarValue::Float32(Some(1.1))))
                    .with_min_value(Precision::Exact(ScalarValue::Float32(Some(0.1))))
                    .with_sum_value(Precision::Exact(ScalarValue::Float32(Some(42.0)))),
            );

        let right = Statistics::default()
            .with_num_rows(Precision::Exact(7))
            .with_total_byte_size(Precision::Exact(29))
            .add_column_statistics(
                ColumnStatistics::new_unknown()
                    .with_distinct_count(Precision::Exact(3))
                    .with_min_value(Precision::Exact(ScalarValue::Int64(Some(1))))
                    .with_max_value(Precision::Exact(ScalarValue::Int64(Some(34))))
                    .with_sum_value(Precision::Exact(ScalarValue::Int64(Some(42))))
                    .with_null_count(Precision::Exact(1)),
            )
            .add_column_statistics(
                ColumnStatistics::new_unknown()
                    .with_distinct_count(Precision::Exact(3))
                    .with_min_value(Precision::Exact(ScalarValue::from("b")))
                    .with_max_value(Precision::Exact(ScalarValue::from("z"))),
            )
            .add_column_statistics(ColumnStatistics::new_unknown());

        let expected = Statistics::default()
            .with_num_rows(Precision::Exact(12))
            .with_total_byte_size(Precision::Exact(52))
            .add_column_statistics(
                ColumnStatistics::new_unknown()
                    .with_distinct_count(Precision::Inexact(6))
                    .with_min_value(Precision::Exact(ScalarValue::Int64(Some(-4))))
                    .with_max_value(Precision::Exact(ScalarValue::Int64(Some(34))))
                    .with_sum_value(Precision::Exact(ScalarValue::Int64(Some(84))))
                    .with_null_count(Precision::Exact(1)),
            )
            .add_column_statistics(
                ColumnStatistics::new_unknown()
                    .with_distinct_count(Precision::Inexact(5))
                    .with_min_value(Precision::Exact(ScalarValue::from("a")))
                    .with_max_value(Precision::Exact(ScalarValue::from("z"))),
            )
            .add_column_statistics(ColumnStatistics::new_unknown());

        (schema, left, right, expected)
    }

    #[test]
    fn test_union_partition_statistics_uses_shared_statistics_merge() -> Result<()> {
        let (schema, left, right, expected) = stats_merge_inputs();

        let left: Arc<dyn ExecutionPlan> =
            Arc::new(StatisticsExec::new(left, schema.as_ref().clone()));
        let right: Arc<dyn ExecutionPlan> =
            Arc::new(StatisticsExec::new(right, schema.as_ref().clone()));

        let union = UnionExec::try_new(vec![left, right])?;
        let stats =
            StatisticsContext::new().compute(union.as_ref(), &StatisticsArgs::new())?;

        assert_eq!(stats.as_ref(), &expected);
        Ok(())
    }

    #[test]
    fn test_union_partition_statistics_uses_shared_statistics_merge_multicolumn()
    -> Result<()> {
        let (schema, left, right, expected) = stats_merge_multicolumn_inputs();

        let left: Arc<dyn ExecutionPlan> =
            Arc::new(StatisticsExec::new(left, schema.as_ref().clone()));
        let right: Arc<dyn ExecutionPlan> =
            Arc::new(StatisticsExec::new(right, schema.as_ref().clone()));

        let union = UnionExec::try_new(vec![left, right])?;
        let stats =
            StatisticsContext::new().compute(union.as_ref(), &StatisticsArgs::new())?;

        assert_eq!(stats.as_ref(), &expected);
        Ok(())
    }

    #[test]
    fn test_union_partition_statistics_with_mismatched_nullability() -> Result<()> {
        // Regression test for the `ProjectionExec` wrapper `UnionExec::try_new`
        // inserts above the non-nullable leg here (via `coerce_schema`):
        // exact column statistics (min/max/null/distinct/sum/byte_size) must
        // still make it through the wrapper's same-type `CastExpr`, not get
        // poisoned into `Absent` the way a generic (type-changing) cast's
        // statistics would be.
        let (_, left, right, expected) = stats_merge_inputs();

        // `total_byte_size` differs from the plain-merge fixture (52): the
        // wrapper is a `ProjectionExec`, whose `statistics_from_inputs`
        // recomputes `total_byte_size` from the (unchanged) schema's row
        // width times row count, rather than trusting the wrapped leg's own
        // self-reported total -- still `Exact`, just derived differently.
        // left: 5 rows * 4 bytes (UInt32) = 20 (was 23); right is untouched
        // (already nullable, so `coerce_schema` doesn't wrap it): 20 + 29 = 49.
        let expected = expected.with_total_byte_size(Precision::Exact(49));

        let non_nullable_schema =
            Schema::new(vec![Field::new("a", DataType::UInt32, false)]);
        let nullable_schema = Schema::new(vec![Field::new("a", DataType::UInt32, true)]);

        let left: Arc<dyn ExecutionPlan> =
            Arc::new(StatisticsExec::new(left, non_nullable_schema));
        let right: Arc<dyn ExecutionPlan> =
            Arc::new(StatisticsExec::new(right, nullable_schema));

        let union = UnionExec::try_new(vec![left, right])?;
        let stats =
            StatisticsContext::new().compute(union.as_ref(), &StatisticsArgs::new())?;

        assert_eq!(stats.as_ref(), &expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_coerce_schema_no_op_when_already_matching() -> Result<()> {
        let schema_not_null =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let input: Arc<dyn ExecutionPlan> =
            TestMemoryExec::try_new_exec(&[vec![]], Arc::clone(&schema_not_null), None)?;

        let coerced = coerce_schema(Arc::clone(&input), &schema_not_null)?;
        assert!(Arc::ptr_eq(&coerced, &input));

        Ok(())
    }

    #[tokio::test]
    async fn test_coerce_schema_casts_only_nullability() -> Result<()> {
        // Mismatched nullability: the input gets wrapped in a `ProjectionExec`
        // whose `CastExpr` re-stamps the column with the target's `Field`
        // (same `DataType`, so this is a zero-copy relabeling, not a real cast).
        let schema_not_null =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch_not_null = RecordBatch::try_new(
            Arc::clone(&schema_not_null),
            vec![Arc::new(arrow::array::Int32Array::from(vec![1, 2]))],
        )?;
        let input: Arc<dyn ExecutionPlan> = TestMemoryExec::try_new_exec(
            &[vec![batch_not_null]],
            Arc::clone(&schema_not_null),
            None,
        )?;

        let nullable_schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let coerced = coerce_schema(Arc::clone(&input), &nullable_schema)?;
        assert_eq!(&coerced.schema(), &nullable_schema);
        let plan_str = crate::displayable(coerced.as_ref())
            .indent(true)
            .to_string();
        assert!(
            plan_str.contains("CAST"),
            "expected a CAST in the coerced plan:\n{plan_str}"
        );

        let task_ctx = Arc::new(TaskContext::default());
        let batches = collect(coerced, task_ctx).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].schema(), nullable_schema);

        Ok(())
    }

    #[test]
    fn test_coerce_schema_rejects_genuine_type_mismatch() -> Result<()> {
        let schema_int =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let input: Arc<dyn ExecutionPlan> =
            TestMemoryExec::try_new_exec(&[vec![]], Arc::clone(&schema_int), None)?;

        let schema_utf8 =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));
        let err = coerce_schema(input, &schema_utf8).unwrap_err();
        assert!(err.to_string().contains("same data type per column"));

        Ok(())
    }

    #[test]
    fn test_interleave_partition_statistics_uses_shared_statistics_merge() -> Result<()> {
        let (schema, left, right, expected) = stats_merge_inputs();
        let hash_expr = vec![col("a", schema.as_ref())?];

        let left: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
            Arc::new(StatisticsExec::new(left, schema.as_ref().clone())),
            Partitioning::Hash(hash_expr.clone(), 2),
        )?);
        let right: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
            Arc::new(StatisticsExec::new(right, schema.as_ref().clone())),
            Partitioning::Hash(hash_expr, 2),
        )?);

        let interleave = InterleaveExec::try_new(vec![left, right])?;
        let stats =
            StatisticsContext::new().compute(&interleave, &StatisticsArgs::new())?;

        assert_eq!(stats.as_ref(), &expected);
        Ok(())
    }

    #[test]
    fn test_interleave_partition_statistics_for_partition_uses_shared_statistics_merge()
    -> Result<()> {
        let (schema, left, right, _) = stats_merge_inputs();
        let hash_expr = vec![col("a", schema.as_ref())?];

        let left: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
            Arc::new(StatisticsExec::new(left, schema.as_ref().clone())),
            Partitioning::Hash(hash_expr.clone(), 2),
        )?);
        let right: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
            Arc::new(StatisticsExec::new(right, schema.as_ref().clone())),
            Partitioning::Hash(hash_expr, 2),
        )?);

        let interleave = InterleaveExec::try_new(vec![left, right])?;
        let stats = StatisticsContext::new()
            .compute(&interleave, &StatisticsArgs::new().with_partition(Some(0)))?;

        let expected = Statistics::default()
            .with_num_rows(Precision::Inexact(5))
            .with_total_byte_size(Precision::Inexact(25))
            .add_column_statistics(ColumnStatistics::new_unknown());

        assert_eq!(stats.as_ref(), &expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_union_equivalence_properties() -> Result<()> {
        let schema = create_test_schema()?;
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let col_c = &col("c", &schema)?;
        let col_d = &col("d", &schema)?;
        let col_e = &col("e", &schema)?;
        let col_f = &col("f", &schema)?;
        let options = SortOptions::default();
        let test_cases = [
            //-----------TEST CASE 1----------//
            (
                // First child orderings
                vec![
                    // [a ASC, b ASC, f ASC]
                    vec![(col_a, options), (col_b, options), (col_f, options)],
                ],
                // Second child orderings
                vec![
                    // [a ASC, b ASC, c ASC]
                    vec![(col_a, options), (col_b, options), (col_c, options)],
                    // [a ASC, b ASC, f ASC]
                    vec![(col_a, options), (col_b, options), (col_f, options)],
                ],
                // Union output orderings
                vec![
                    // [a ASC, b ASC, f ASC]
                    vec![(col_a, options), (col_b, options), (col_f, options)],
                ],
            ),
            //-----------TEST CASE 2----------//
            (
                // First child orderings
                vec![
                    // [a ASC, b ASC, f ASC]
                    vec![(col_a, options), (col_b, options), (col_f, options)],
                    // d ASC
                    vec![(col_d, options)],
                ],
                // Second child orderings
                vec![
                    // [a ASC, b ASC, c ASC]
                    vec![(col_a, options), (col_b, options), (col_c, options)],
                    // [e ASC]
                    vec![(col_e, options)],
                ],
                // Union output orderings
                vec![
                    // [a ASC, b ASC]
                    vec![(col_a, options), (col_b, options)],
                ],
            ),
        ];

        for (
            test_idx,
            (first_child_orderings, second_child_orderings, union_orderings),
        ) in test_cases.iter().enumerate()
        {
            let first_orderings = convert_to_orderings(first_child_orderings);
            let second_orderings = convert_to_orderings(second_child_orderings);
            let union_expected_orderings = convert_to_orderings(union_orderings);
            let child1_exec = TestMemoryExec::try_new(&[], Arc::clone(&schema), None)?
                .try_with_sort_information(first_orderings)?;
            let child1 = Arc::new(child1_exec);
            let child1 = Arc::new(TestMemoryExec::update_cache(&child1));
            let child2_exec = TestMemoryExec::try_new(&[], Arc::clone(&schema), None)?
                .try_with_sort_information(second_orderings)?;
            let child2 = Arc::new(child2_exec);
            let child2 = Arc::new(TestMemoryExec::update_cache(&child2));

            let mut union_expected_eq = EquivalenceProperties::new(Arc::clone(&schema));
            union_expected_eq.add_orderings(union_expected_orderings);

            let union: Arc<dyn ExecutionPlan> = UnionExec::try_new(vec![child1, child2])?;
            let union_eq_properties = union.properties().equivalence_properties();
            let err_msg = format!(
                "Error in test id: {:?}, test case: {:?}",
                test_idx, test_cases[test_idx]
            );
            assert_eq_properties_same(union_eq_properties, &union_expected_eq, err_msg);
        }
        Ok(())
    }

    fn assert_eq_properties_same(
        lhs: &EquivalenceProperties,
        rhs: &EquivalenceProperties,
        err_msg: String,
    ) {
        // Check whether orderings are same.
        let lhs_orderings = lhs.oeq_class();
        let rhs_orderings = rhs.oeq_class();
        assert_eq!(lhs_orderings.len(), rhs_orderings.len(), "{err_msg}");
        for rhs_ordering in rhs_orderings.iter() {
            assert!(lhs_orderings.contains(rhs_ordering), "{}", err_msg);
        }
    }

    #[test]
    fn test_union_empty_inputs() {
        // Test that UnionExec::try_new fails with empty inputs
        let result = UnionExec::try_new(vec![]);
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("UnionExec requires at least one input")
        );
    }

    #[test]
    fn test_union_schema_empty_inputs() {
        // Test that union_schema fails with empty inputs
        let result = union_schema(&[]);
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Cannot create union schema from empty inputs")
        );
    }

    #[test]
    fn test_union_single_input() -> Result<()> {
        // Test that UnionExec::try_new returns the single input directly
        let schema = create_test_schema()?;
        let memory_exec: Arc<dyn ExecutionPlan> =
            Arc::new(TestMemoryExec::try_new(&[], Arc::clone(&schema), None)?);
        let memory_exec_clone = Arc::clone(&memory_exec);
        let result = UnionExec::try_new(vec![memory_exec])?;

        // Check that the result is the same as the input (no UnionExec wrapper)
        assert_eq!(result.schema(), schema);
        // Verify it's the same execution plan
        assert!(Arc::ptr_eq(&result, &memory_exec_clone));

        Ok(())
    }

    #[test]
    fn test_union_schema_multiple_inputs() -> Result<()> {
        // Test that existing functionality with multiple inputs still works
        let schema = create_test_schema()?;
        let memory_exec1 =
            Arc::new(TestMemoryExec::try_new(&[], Arc::clone(&schema), None)?);
        let memory_exec2 =
            Arc::new(TestMemoryExec::try_new(&[], Arc::clone(&schema), None)?);

        let union_plan = UnionExec::try_new(vec![memory_exec1, memory_exec2])?;

        // Downcast to verify it's a UnionExec
        let union = union_plan
            .downcast_ref::<UnionExec>()
            .expect("Expected UnionExec");

        // Check that schema is correct
        assert_eq!(union.schema(), schema);
        // Check that we have 2 inputs
        assert_eq!(union.inputs().len(), 2);

        Ok(())
    }

    #[test]
    fn test_union_schema_fast_path_content_equal() -> Result<()> {
        // Inputs whose schemas are pointer-distinct but structurally equal must
        // take the content-equality (`==`) fast path and still produce a schema
        // equal to the shared one, matching the slow-path merge exactly.
        let schema = create_test_schema()?;
        let distinct: SchemaRef = Arc::new((*schema).clone());
        // Guard the branch under test: these must NOT be the same allocation, so
        // the fast path is reached via `==` rather than `Arc::ptr_eq`.
        assert!(!Arc::ptr_eq(&schema, &distinct));

        let memory_exec1 =
            Arc::new(TestMemoryExec::try_new(&[], Arc::clone(&schema), None)?);
        let memory_exec2 =
            Arc::new(TestMemoryExec::try_new(&[], Arc::clone(&distinct), None)?);
        let memory_exec3 =
            Arc::new(TestMemoryExec::try_new(&[], Arc::clone(&distinct), None)?);

        // Capture the first child's schema before it is moved into the union.
        let first_input_schema = memory_exec1.schema();
        let union_plan =
            UnionExec::try_new(vec![memory_exec1, memory_exec2, memory_exec3])?;

        // The fast path returns the first child's schema Arc unchanged. Assert
        // pointer equality (not just `==`): a slow-path merge would build a new,
        // merely-equal Schema, so only ptr-eq proves the merge was skipped.
        assert!(Arc::ptr_eq(&union_plan.schema(), &first_input_schema));
        Ok(())
    }

    #[test]
    fn test_union_schema_mismatch() {
        // Test that UnionExec properly rejects inputs with different field counts
        let schema = create_test_schema().unwrap();
        let schema2 = create_test_schema2().unwrap();
        let memory_exec1 =
            Arc::new(TestMemoryExec::try_new(&[], Arc::clone(&schema), None).unwrap());
        let memory_exec2 =
            Arc::new(TestMemoryExec::try_new(&[], Arc::clone(&schema2), None).unwrap());

        let result = UnionExec::try_new(vec![memory_exec1, memory_exec2]);
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains(
                "UnionExec/InterleaveExec requires all inputs to have the same number of fields"
            )
        );
    }

    fn make_hash_exec(
        schema: &SchemaRef,
        hash_cols: Vec<&str>,
        buckets: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exprs = hash_cols
            .iter()
            .map(|c| col(c, schema))
            .collect::<Result<Vec<_>>>()?;
        let base = Arc::new(TestMemoryExec::try_new(&[], Arc::clone(schema), None)?);
        Ok(Arc::new(RepartitionExec::try_new(
            base,
            Partitioning::Hash(exprs, buckets),
        )?))
    }

    fn make_range_exec(
        schema: &SchemaRef,
        split_values: Vec<i32>,
        sort_options: SortOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let sort_expr =
            PhysicalSortExpr::new(col(schema.field(0).name(), schema)?, sort_options);
        let ordering = LexOrdering::new(vec![sort_expr]).unwrap();
        let split_points = split_values
            .into_iter()
            .map(|v| SplitPoint::new(vec![ScalarValue::Int32(Some(v))]))
            .collect();
        let base = Arc::new(TestMemoryExec::try_new(&[], Arc::clone(schema), None)?);
        Ok(Arc::new(RepartitionExec::try_new(
            base,
            Partitioning::Range(RangePartitioning::try_new(ordering, split_points)?),
        )?))
    }

    #[test]
    fn test_can_interleave_matrix() -> Result<()> {
        let name_column = "name";
        let age_column = "age";
        let schema = Arc::new(Schema::new(vec![
            Field::new(name_column, DataType::Int32, true),
            Field::new(age_column, DataType::Int32, true),
        ]));

        let ascending = SortOptions {
            descending: false,
            nulls_first: false,
        };
        struct Case {
            inputs: Vec<Arc<dyn ExecutionPlan>>,
            expected: bool,
            label: &'static str,
        }

        let cases = vec![
            // compatible
            Case {
                label: "matching hash on single column",
                expected: true,
                inputs: vec![
                    make_hash_exec(&schema, vec![name_column], 3)?,
                    make_hash_exec(&schema, vec![name_column], 3)?,
                ],
            },
            Case {
                label: "matching hash on multiple columns",
                expected: true,
                inputs: vec![
                    make_hash_exec(&schema, vec![name_column, age_column], 3)?,
                    make_hash_exec(&schema, vec![name_column, age_column], 3)?,
                ],
            },
            Case {
                label: "matching range same splits and order",
                expected: true,
                inputs: vec![
                    make_range_exec(&schema, vec![10, 20], ascending)?,
                    make_range_exec(&schema, vec![10, 20], ascending)?,
                ],
            },
            // incompatible
            Case {
                label: "subset range partition",
                expected: false,
                inputs: vec![
                    make_range_exec(&schema, vec![10, 20], ascending)?,
                    make_range_exec(&schema, vec![10, 15], ascending)?,
                ],
            },
            Case {
                label: "range different split points",
                expected: false,
                inputs: vec![
                    make_range_exec(&schema, vec![10, 20], ascending)?,
                    make_range_exec(&schema, vec![10, 30], ascending)?,
                ],
            },
            Case {
                label: "mixed range and hash",
                expected: false,
                inputs: vec![
                    make_range_exec(&schema, vec![10, 20], ascending)?,
                    make_hash_exec(&schema, vec![name_column], 3)?,
                ],
            },
        ];

        for case in cases {
            assert_eq!(
                can_interleave(case.inputs.iter()),
                case.expected,
                "{}",
                case.label
            );
        }
        Ok(())
    }

    #[test]
    fn test_union_cardinality_effect() -> Result<()> {
        let schema = create_test_schema()?;
        let input1: Arc<dyn ExecutionPlan> =
            Arc::new(TestMemoryExec::try_new(&[], Arc::clone(&schema), None)?);
        let input2: Arc<dyn ExecutionPlan> =
            Arc::new(TestMemoryExec::try_new(&[], Arc::clone(&schema), None)?);

        let union = UnionExec::try_new(vec![input1, input2])?;
        let union = union
            .downcast_ref::<UnionExec>()
            .expect("expected UnionExec for multiple inputs");

        assert!(matches!(
            union.cardinality_effect(),
            CardinalityEffect::GreaterEqual
        ));
        Ok(())
    }
}
