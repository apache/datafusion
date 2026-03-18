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

//! GroupJoin execution plan.
//!
//! Implements a fused hash-join + aggregation operator based on
//! "Accelerating Queries with Group-By and Join by Groupjoin"
//! (Moerkotte & Neumann, VLDB 2011).
//!
//! The operator builds a hash table on the right (build) side, then
//! for each left (probe) row, looks up matching right rows and feeds
//! them directly into group-by accumulators—avoiding materialization of
//! the potentially large intermediate join result.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, RecordBatch, UInt32Array, UInt64Array};
use arrow::compute;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use datafusion_common::hash_utils::{RandomState, create_hashes};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::utils::memory::get_record_batch_memory_size;
use datafusion_common::{NullEquality, Result};
use datafusion_expr::JoinType;
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_expr::{EmitTo, GroupsAccumulator};
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
use datafusion_physical_expr_common::utils::evaluate_expressions_to_arrays;
use futures::StreamExt;
use futures::future::poll_fn;

use crate::aggregates::group_values::{GroupValues, new_group_values};
use crate::aggregates::order::GroupOrdering;
use crate::aggregates::row_hash::create_group_accumulator;
use crate::aggregates::{AggregateMode, AggregateOutputMode};
use crate::execution_plan::{CardinalityEffect, EmissionType, PlanProperties};
use crate::joins::hash_join::stream::lookup_join_hashmap;
use crate::joins::join_hash_map::{JoinHashMapU32, JoinHashMapU64};
use crate::joins::utils::{JoinHashMapType, OnceAsync, OnceFut, update_hash};
use crate::joins::{JoinOn, Map, MapOffset};
use crate::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use crate::stream::RecordBatchStreamAdapter;
use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PhysicalExpr,
    SendableRecordBatchStream,
};

/// Indicates which input side a group-by column or aggregate argument comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupBySide {
    Left,
    Right,
}

/// Built right-side data: hash map, concatenated batch, join-key values, and group-by arrays.
/// Computed once and shared (via `Arc`) across all left-side partitions.
struct RightData {
    map: Arc<Map>,
    batch: RecordBatch,
    /// Evaluated join key columns (for hash lookups)
    values: Vec<ArrayRef>,
    /// Evaluated right-side group-by columns (for group key construction)
    group_by_arrays: Vec<ArrayRef>,
    /// Pre-evaluated right-side (build-side) aggregate argument arrays.
    /// One Vec<ArrayRef> per build-side aggregate, computed once and reused.
    build_agg_arrays: Vec<Vec<ArrayRef>>,
}

impl fmt::Debug for RightData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RightData")
            .field("num_rows", &self.batch.num_rows())
            .finish()
    }
}

/// GroupJoinExec fuses a hash join with a subsequent aggregation.
///
/// Instead of materializing the full join result and then grouping,
/// it builds a hash table on the right (build) side and, for each
/// left (probe) row, directly aggregates matching right-side values.
///
/// Group-by columns may reference either the left (probe) or right (build) input.
/// Aggregate argument expressions are evaluated on the side specified by `aggr_arg_sides`.
///
/// This is beneficial when the join has high fan-out (many right rows
/// per left row) but the final result is much smaller after aggregation.
#[derive(Debug)]
pub struct GroupJoinExec {
    /// Aggregation mode (Partial, Single, etc.)
    mode: AggregateMode,
    /// Left input (probe side)
    left: Arc<dyn ExecutionPlan>,
    /// Right input (build side)
    right: Arc<dyn ExecutionPlan>,
    /// Equi-join keys: (left_expr, right_expr)
    on: JoinOn,
    /// Group-by expressions referencing the LEFT (probe) input schema
    left_group_by: Vec<(PhysicalExprRef, String)>,
    /// Group-by expressions referencing the RIGHT (build) input schema
    right_group_by: Vec<(PhysicalExprRef, String)>,
    /// Interleave order for output group-by columns: Left(left_group_by[i]) or Right(right_group_by[j])
    group_by_order: Vec<GroupBySide>,
    /// Aggregate function expressions
    aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
    /// For each aggr_expr, which input side provides the argument values
    aggr_arg_sides: Vec<GroupBySide>,
    /// Join type (Inner or Left)
    join_type: JoinType,
    /// Whether to build per-partition (Partitioned mode) or shared (CollectLeft mode)
    partitioned: bool,
    /// Output schema (matches the original AggregateExec's output)
    schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cached plan properties
    cache: Arc<PlanProperties>,
    /// Shared right-side hash table — built once, reused by all left partitions (CollectLeft mode only)
    right_fut: Arc<OnceAsync<Arc<RightData>>>,
}

impl GroupJoinExec {
    /// Create a GroupJoinExec where all group-by columns come from the left (probe) side
    /// and all aggregate arguments come from the right (build) side.
    pub fn try_new(
        mode: AggregateMode,
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        group_by_exprs: Vec<(PhysicalExprRef, String)>,
        aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
        schema: SchemaRef,
    ) -> Result<Self> {
        let n = group_by_exprs.len();
        let a = aggr_expr.len();
        Self::try_new_extended(
            mode,
            left,
            right,
            on,
            group_by_exprs,
            vec![],
            vec![GroupBySide::Left; n],
            aggr_expr,
            vec![GroupBySide::Right; a],
            JoinType::Inner,
            false,
            schema,
        )
    }

    /// Create a GroupJoinExec with full control over which side each group-by column
    /// and aggregate argument comes from.
    ///
    /// - `left_group_by`: expressions referencing the left (probe) input schema
    /// - `right_group_by`: expressions referencing the right (build) input schema
    /// - `group_by_order`: for each output group-by column, `Left` or `Right`
    ///   (consumed left-to-right within each side's list)
    /// - `aggr_arg_sides`: for each aggregate expression, which input side provides args
    #[expect(clippy::too_many_arguments)]
    pub fn try_new_extended(
        mode: AggregateMode,
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        left_group_by: Vec<(PhysicalExprRef, String)>,
        right_group_by: Vec<(PhysicalExprRef, String)>,
        group_by_order: Vec<GroupBySide>,
        aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
        aggr_arg_sides: Vec<GroupBySide>,
        join_type: JoinType,
        partitioned: bool,
        schema: SchemaRef,
    ) -> Result<Self> {
        let cache = Arc::new(Self::compute_properties(
            left.properties(),
            Arc::clone(&schema),
        ));
        Ok(Self {
            mode,
            left,
            right,
            on,
            left_group_by,
            right_group_by,
            group_by_order,
            aggr_expr,
            aggr_arg_sides,
            join_type,
            partitioned,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
            right_fut: Arc::new(OnceAsync::default()),
        })
    }

    /// Reconstruct the full group-by list in output order, as (expr, alias, side) triples.
    fn ordered_group_by(&self) -> Vec<(&PhysicalExprRef, &str, GroupBySide)> {
        let mut left_i = 0;
        let mut right_i = 0;
        self.group_by_order
            .iter()
            .map(|&side| match side {
                GroupBySide::Left => {
                    let (e, a) = &self.left_group_by[left_i];
                    left_i += 1;
                    (e, a.as_str(), GroupBySide::Left)
                }
                GroupBySide::Right => {
                    let (e, a) = &self.right_group_by[right_i];
                    right_i += 1;
                    (e, a.as_str(), GroupBySide::Right)
                }
            })
            .collect()
    }

    fn compute_properties(
        left_props: &PlanProperties,
        schema: SchemaRef,
    ) -> PlanProperties {
        use datafusion_physical_expr::equivalence::EquivalenceProperties;
        let eq_properties = EquivalenceProperties::new(schema);
        PlanProperties::new(
            eq_properties,
            left_props.output_partitioning().clone(),
            EmissionType::Final,
            left_props.boundedness,
        )
    }
}

impl DisplayAs for GroupJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let on = self
                    .on
                    .iter()
                    .map(|(l, r)| format!("({l}, {r})"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let group_by: Vec<_> = self
                    .ordered_group_by()
                    .iter()
                    .map(|(e, alias, _)| format!("{e} as {alias}"))
                    .collect();
                let aggrs: Vec<_> = self
                    .aggr_expr
                    .iter()
                    .map(|e| e.name().to_string())
                    .collect();
                write!(
                    f,
                    "GroupJoinExec: mode={:?}, join_type={:?}, on=[{}], group_by=[{}], aggr=[{}]",
                    self.mode,
                    self.join_type,
                    on,
                    group_by.join(", "),
                    aggrs.join(", "),
                )
            }
            DisplayFormatType::TreeRender => {
                let on = self
                    .on
                    .iter()
                    .map(|(l, r)| format!("({l} = {r})"))
                    .collect::<Vec<_>>()
                    .join(", ");
                writeln!(f, "on={on}")?;
                let group_by: Vec<_> = self
                    .ordered_group_by()
                    .iter()
                    .map(|(e, alias, _)| format!("{e} as {alias}"))
                    .collect();
                writeln!(f, "group_by=[{}]", group_by.join(", "))?;
                let aggrs: Vec<_> = self
                    .aggr_expr
                    .iter()
                    .map(|e| e.name().to_string())
                    .collect();
                writeln!(f, "aggr=[{}]", aggrs.join(", "))?;
                Ok(())
            }
        }
    }
}

impl ExecutionPlan for GroupJoinExec {
    fn name(&self) -> &'static str {
        "GroupJoinExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false, false]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        let mut tnr = TreeNodeRecursion::Continue;
        for (left, right) in &self.on {
            tnr = tnr.visit_sibling(|| f(left.as_ref()))?;
            tnr = tnr.visit_sibling(|| f(right.as_ref()))?;
        }
        for (expr, _) in &self.left_group_by {
            tnr = tnr.visit_sibling(|| f(expr.as_ref()))?;
        }
        for (expr, _) in &self.right_group_by {
            tnr = tnr.visit_sibling(|| f(expr.as_ref()))?;
        }
        for agg in &self.aggr_expr {
            for arg in agg.expressions() {
                tnr = tnr.visit_sibling(|| f(arg.as_ref()))?;
            }
        }
        Ok(tnr)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Reset right_fut so the new plan re-collects the right side from scratch
        Ok(Arc::new(GroupJoinExec::try_new_extended(
            self.mode,
            Arc::clone(&children[0]),
            Arc::clone(&children[1]),
            self.on.clone(),
            self.left_group_by.clone(),
            self.right_group_by.clone(),
            self.group_by_order.clone(),
            self.aggr_expr.clone(),
            self.aggr_arg_sides.clone(),
            self.join_type,
            self.partitioned,
            Arc::clone(&self.schema),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let on_left: Vec<PhysicalExprRef> =
            self.on.iter().map(|on| Arc::clone(&on.0)).collect();

        let left_stream = self.left.execute(partition, Arc::clone(&context))?;

        // Build right-side hash table.
        let right_fut: OnceFut<Arc<RightData>> = {
            let right = Arc::clone(&self.right);
            let on_right: Vec<PhysicalExprRef> =
                self.on.iter().map(|on| Arc::clone(&on.1)).collect();
            let right_group_by_exprs: Vec<PhysicalExprRef> = self
                .right_group_by
                .iter()
                .map(|(e, _)| Arc::clone(e))
                .collect();
            // Collect build-side aggregate argument expressions to pre-evaluate once
            let build_agg_exprs: Vec<Vec<Arc<dyn PhysicalExpr>>> = self
                .aggr_expr
                .iter()
                .zip(self.aggr_arg_sides.iter())
                .filter(|&(_, side)| *side == GroupBySide::Right)
                .map(|(agg, _)| agg.expressions())
                .collect();
            let random_state = RandomState::with_seed(0);
            let ctx = Arc::clone(&context);
            if self.partitioned {
                // Partitioned mode: each partition builds from its own right partition only.
                let part = partition;
                OnceFut::new(collect_right_input(
                    right,
                    Some(part),
                    on_right,
                    right_group_by_exprs,
                    build_agg_exprs,
                    random_state,
                    ctx,
                ))
            } else {
                // CollectLeft mode: build once, shared across all left partitions.
                self.right_fut.try_once(|| {
                    Ok(collect_right_input(
                        right,
                        None, // all partitions
                        on_right,
                        right_group_by_exprs,
                        build_agg_exprs,
                        random_state,
                        ctx,
                    ))
                })?
            }
        };

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        let accumulators: Vec<Box<dyn GroupsAccumulator>> = self
            .aggr_expr
            .iter()
            .map(create_group_accumulator)
            .collect::<Result<_>>()?;

        // For each aggregate, collect its argument expressions paired with side info
        let agg_args: Vec<(Vec<Arc<dyn PhysicalExpr>>, GroupBySide)> = self
            .aggr_expr
            .iter()
            .zip(self.aggr_arg_sides.iter())
            .map(|(agg, &side)| (agg.expressions(), side))
            .collect();

        let left_group_by_exprs: Vec<PhysicalExprRef> = self
            .left_group_by
            .iter()
            .map(|(e, _)| Arc::clone(e))
            .collect();
        let group_by_order = self.group_by_order.clone();

        let mode = self.mode;
        let schema = Arc::clone(&self.schema);
        let batch_size = context.session_config().batch_size();

        // Build group schema from the first N fields of the output schema
        // (AggregateExec always places group-by fields first)
        let num_group_cols = self.left_group_by.len() + self.right_group_by.len();
        let group_schema = Arc::new(arrow::datatypes::Schema::new(
            self.schema.fields()[..num_group_cols].to_vec(),
        ));
        let group_values = new_group_values(group_schema, &GroupOrdering::None)?;

        let reservation =
            MemoryConsumer::new("GroupJoinExec").register(context.memory_pool());

        let random_state = RandomState::with_seed(0);

        let is_left_join = self.join_type == JoinType::Left;

        let stream = GroupJoinStream {
            schema: Arc::clone(&schema),
            on_left,
            right_fut,
            left_stream: Some(left_stream),
            mode,
            is_left_join,
            left_group_by_exprs,
            group_by_order,
            agg_args,
            accumulators,
            group_values,
            current_group_indices: Vec::new(),
            batch_size,
            baseline_metrics,
            state: GroupJoinStreamState::BuildRight,
            right_data: None,
            reservation,
            hashes_buffer: Vec::new(),
            random_state,
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::unfold(stream, |mut stream| async move {
                match stream.next_batch().await {
                    Ok(Some(batch)) => Some((Ok(batch), stream)),
                    Ok(None) => None,
                    Err(e) => Some((Err(e), stream)),
                }
            }),
        )))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::LowerEqual
    }
}

enum GroupJoinStreamState {
    BuildRight,
    ProbeAndAggregate,
    Emit,
    Done,
}

struct GroupJoinStream {
    schema: SchemaRef,
    on_left: Vec<PhysicalExprRef>,

    /// Future that resolves to the shared right-side hash table (built once)
    right_fut: OnceFut<Arc<RightData>>,
    left_stream: Option<SendableRecordBatchStream>,

    mode: AggregateMode,
    /// Whether this is a LEFT OUTER JOIN (unmatched probe rows produce NULL build-side values)
    is_left_join: bool,
    /// Group-by expressions evaluated against the LEFT (probe) input
    left_group_by_exprs: Vec<PhysicalExprRef>,
    /// Interleave order for group-by output columns
    group_by_order: Vec<GroupBySide>,
    /// Per-aggregate: (arg expressions, which input side to evaluate them on)
    agg_args: Vec<(Vec<Arc<dyn PhysicalExpr>>, GroupBySide)>,
    accumulators: Vec<Box<dyn GroupsAccumulator>>,
    group_values: Box<dyn GroupValues>,
    current_group_indices: Vec<usize>,

    batch_size: usize,
    #[expect(dead_code)]
    baseline_metrics: BaselineMetrics,
    state: GroupJoinStreamState,
    /// Resolved right-side data (set after BuildRight state completes)
    right_data: Option<Arc<RightData>>,
    reservation: MemoryReservation,
    hashes_buffer: Vec<u64>,
    random_state: RandomState,
}

impl GroupJoinStream {
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            match &self.state {
                GroupJoinStreamState::BuildRight => {
                    self.build_right_side().await?;
                    self.state = GroupJoinStreamState::ProbeAndAggregate;
                }
                GroupJoinStreamState::ProbeAndAggregate => {
                    let left_stream = self.left_stream.as_mut().unwrap();
                    match left_stream.next().await {
                        Some(Ok(batch)) => {
                            self.process_left_batch(&batch)?;
                        }
                        Some(Err(e)) => return Err(e),
                        None => {
                            self.state = GroupJoinStreamState::Emit;
                        }
                    }
                }
                GroupJoinStreamState::Emit => {
                    let result = self.emit_results()?;
                    self.state = GroupJoinStreamState::Done;
                    if result.num_rows() > 0 {
                        return Ok(Some(result));
                    }
                    return Ok(None);
                }
                GroupJoinStreamState::Done => return Ok(None),
            }
        }
    }

    async fn build_right_side(&mut self) -> Result<()> {
        // Await the shared right-side future (built once across all left partitions).
        // OnceFut uses a poll-based API, so we bridge it via poll_fn.
        let right_data: Arc<RightData> =
            poll_fn(|cx| self.right_fut.get(cx).map(|r| r.map(Arc::clone)))
                .await?;
        let batch_mem = get_record_batch_memory_size(&right_data.batch);
        self.reservation.try_grow(batch_mem)?;
        self.right_data = Some(right_data);
        Ok(())
    }

    fn process_left_batch(&mut self, left_batch: &RecordBatch) -> Result<()> {
        let right_data = self.right_data.as_ref().ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(
                "Right data not built".to_string(),
            )
        })?;

        // Pre-evaluate left-side group-by columns against the current left batch
        let left_gby_arrays: Vec<ArrayRef> = self
            .left_group_by_exprs
            .iter()
            .map(|expr| {
                expr.evaluate(left_batch)
                    .and_then(|v| v.into_array(left_batch.num_rows()))
            })
            .collect::<Result<_>>()?;

        // For LEFT JOIN with empty build side: all rows are unmatched
        if right_data.map.is_empty() {
            if self.is_left_join {
                self.emit_unmatched_left_rows(
                    left_batch,
                    &left_gby_arrays,
                    &BooleanArray::from(vec![false; left_batch.num_rows()]),
                )?;
            }
            return Ok(());
        }

        let left_key_values = evaluate_expressions_to_arrays(&self.on_left, left_batch)?;

        self.hashes_buffer.clear();
        self.hashes_buffer.resize(left_batch.num_rows(), 0);
        create_hashes(
            &left_key_values,
            &self.random_state,
            &mut self.hashes_buffer,
        )?;

        let mut probe_indices_buf: Vec<u32> = Vec::new();
        let mut build_indices_buf: Vec<u64> = Vec::new();
        let mut offset: Option<MapOffset> = Some((0, None));

        // Use pre-computed build-side aggregate arrays from RightData
        let right_agg_arrays = &right_data.build_agg_arrays;

        // For LEFT JOIN, track which probe rows had at least one match
        let mut matched_probe = vec![false; left_batch.num_rows()];

        while let Some(current_offset) = offset {
            probe_indices_buf.clear();
            build_indices_buf.clear();

            let (build_indices, probe_indices, next_offset) =
                match right_data.map.as_ref() {
                    Map::HashMap(map) => lookup_join_hashmap(
                        map.as_ref(),
                        &right_data.values,
                        &left_key_values,
                        NullEquality::NullEqualsNothing,
                        &self.hashes_buffer,
                        self.batch_size,
                        current_offset,
                        &mut probe_indices_buf,
                        &mut build_indices_buf,
                    )?,
                    Map::ArrayMap(array_map) => {
                        let next = array_map.get_matched_indices_with_limit_offset(
                            &left_key_values,
                            self.batch_size,
                            current_offset,
                            &mut probe_indices_buf,
                            &mut build_indices_buf,
                        )?;
                        (
                            UInt64Array::from(std::mem::take(&mut build_indices_buf)),
                            UInt32Array::from(std::mem::take(&mut probe_indices_buf)),
                            next,
                        )
                    }
                };

            if build_indices.is_empty() {
                offset = next_offset;
                if offset.is_none() {
                    break;
                }
                continue;
            }

            // Mark matched probe rows
            for &idx in probe_indices.values() {
                matched_probe[idx as usize] = true;
            }

            // Build the ordered group-by array list by interleaving left and right
            let left_taken: Vec<ArrayRef> = left_gby_arrays
                .iter()
                .map(|arr| compute::take(arr.as_ref(), &probe_indices, None))
                .collect::<std::result::Result<_, _>>()?;
            let right_taken: Vec<ArrayRef> = right_data
                .group_by_arrays
                .iter()
                .map(|arr| compute::take(arr.as_ref(), &build_indices, None))
                .collect::<std::result::Result<_, _>>()?;

            let mut left_i = 0;
            let mut right_i = 0;
            let matched_group_values: Vec<ArrayRef> = self
                .group_by_order
                .iter()
                .map(|side| match side {
                    GroupBySide::Left => {
                        let v = Arc::clone(&left_taken[left_i]);
                        left_i += 1;
                        v
                    }
                    GroupBySide::Right => {
                        let v = Arc::clone(&right_taken[right_i]);
                        right_i += 1;
                        v
                    }
                })
                .collect();

            self.group_values
                .intern(&matched_group_values, &mut self.current_group_indices)?;
            let total_num_groups = self.group_values.len();

            // Feed matched values to accumulators (left args taken at probe_indices, right at build_indices)
            let mut right_agg_idx = 0;
            for (acc, (arg_exprs, side)) in self
                .accumulators
                .iter_mut()
                .zip(self.agg_args.iter())
            {
                let values: Vec<ArrayRef> = match side {
                    GroupBySide::Left => arg_exprs
                        .iter()
                        .map(|expr| {
                            expr.evaluate(left_batch)
                                .and_then(|v| v.into_array(left_batch.num_rows()))
                                .and_then(|arr| {
                                    compute::take(arr.as_ref(), &probe_indices, None)
                                        .map_err(Into::into)
                                })
                        })
                        .collect::<Result<_>>()?,
                    GroupBySide::Right => {
                        let arrays = &right_agg_arrays[right_agg_idx];
                        right_agg_idx += 1;
                        arrays
                            .iter()
                            .map(|arr| {
                                compute::take(arr.as_ref(), &build_indices, None)
                                    .map_err(Into::into)
                            })
                            .collect::<Result<_>>()?
                    }
                };
                acc.update_batch(
                    &values,
                    &self.current_group_indices,
                    None,
                    total_num_groups,
                )?;
            }

            offset = next_offset;
        }

        // For LEFT JOIN: emit group entries for unmatched probe rows.
        // Accumulators are NOT updated for these rows, so they keep default values
        // (0 for COUNT, NULL for SUM, etc.) which is correct LEFT JOIN semantics.
        if self.is_left_join {
            let matched_bitmap = BooleanArray::from(matched_probe);
            self.emit_unmatched_left_rows(left_batch, &left_gby_arrays, &matched_bitmap)?;
        }

        Ok(())
    }

    /// For LEFT JOIN: intern group keys for probe rows that had no matches.
    /// Accumulators are not updated, so they retain default values (correct for LEFT JOIN).
    fn emit_unmatched_left_rows(
        &mut self,
        _left_batch: &RecordBatch,
        left_gby_arrays: &[ArrayRef],
        matched_bitmap: &BooleanArray,
    ) -> Result<()> {
        let unmatched: Vec<u32> = matched_bitmap
            .iter()
            .enumerate()
            .filter(|(_, matched)| !matched.unwrap_or(false))
            .map(|(i, _)| i as u32)
            .collect();

        if unmatched.is_empty() {
            return Ok(());
        }

        let unmatched_indices = UInt32Array::from(unmatched);

        // Build group keys: probe-side taken at unmatched indices, build-side filled with NULLs
        let unmatched_left_gby: Vec<ArrayRef> = left_gby_arrays
            .iter()
            .map(|arr| compute::take(arr.as_ref(), &unmatched_indices, None))
            .collect::<std::result::Result<_, _>>()?;

        let right_data = self.right_data.as_ref().unwrap();
        let unmatched_right_gby: Vec<ArrayRef> = right_data
            .group_by_arrays
            .iter()
            .map(|arr| {
                arrow::array::new_null_array(arr.data_type(), unmatched_indices.len())
            })
            .collect();

        let mut left_i = 0;
        let mut right_i = 0;
        let group_values: Vec<ArrayRef> = self
            .group_by_order
            .iter()
            .map(|side| match side {
                GroupBySide::Left => {
                    let v = Arc::clone(&unmatched_left_gby[left_i]);
                    left_i += 1;
                    v
                }
                GroupBySide::Right => {
                    let v = Arc::clone(&unmatched_right_gby[right_i]);
                    right_i += 1;
                    v
                }
            })
            .collect();

        self.group_values
            .intern(&group_values, &mut self.current_group_indices)?;

        Ok(())
    }

    fn emit_results(&mut self) -> Result<RecordBatch> {
        if self.group_values.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }
        let group_columns = self.group_values.emit(EmitTo::All)?;

        let agg_columns: Vec<ArrayRef> = match self.mode.output_mode() {
            AggregateOutputMode::Final => self
                .accumulators
                .iter_mut()
                .map(|acc| acc.evaluate(EmitTo::All))
                .collect::<Result<Vec<_>>>()?,
            AggregateOutputMode::Partial => self
                .accumulators
                .iter_mut()
                .flat_map(|acc| match acc.state(EmitTo::All) {
                    Ok(states) => states.into_iter().map(Ok).collect::<Vec<_>>(),
                    Err(e) => vec![Err(e)],
                })
                .collect::<Result<Vec<_>>>()?,
        };

        let mut columns = group_columns;
        columns.extend(agg_columns);

        if columns.is_empty() || columns[0].is_empty() {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }

        Ok(RecordBatch::try_new(Arc::clone(&self.schema), columns)?)
    }
}

/// Collects all right-side (build) partitions into a single `RightData`.
///
/// Mirrors the pattern used by `HashJoinExec(CollectLeft)`:
/// - Executes all right partitions and concatenates their batches.
/// - Builds a hash map keyed on the equi-join columns.
/// - Evaluates right-side group-by expressions once on the combined batch.
///
/// The resulting `Arc<RightData>` is shared across all left (probe) partitions
/// via `OnceAsync`, so the right side is collected exactly once.
async fn collect_right_input(
    right: Arc<dyn ExecutionPlan>,
    single_partition: Option<usize>,
    on_right: Vec<PhysicalExprRef>,
    right_group_by_exprs: Vec<PhysicalExprRef>,
    build_agg_exprs: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    random_state: RandomState,
    context: Arc<TaskContext>,
) -> Result<Arc<RightData>> {
    let schema = right.schema();

    // Execute right partitions and collect batches.
    // single_partition=Some(p) collects only partition p (Partitioned mode).
    // single_partition=None collects all partitions (CollectLeft mode).
    let mut all_batches: Vec<RecordBatch> = Vec::new();
    let partitions: Vec<usize> = match single_partition {
        Some(p) => vec![p],
        None => (0..right.output_partitioning().partition_count()).collect(),
    };
    for p in partitions {
        let mut stream = right.execute(p, Arc::clone(&context))?;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if batch.num_rows() > 0 {
                all_batches.push(batch);
            }
        }
    }

    if all_batches.is_empty() {
        // Empty right side: return an empty RightData with an empty map.
        let empty_batch = RecordBatch::new_empty(schema);
        let hashmap = Box::new(JoinHashMapU32::with_capacity(0));
        return Ok(Arc::new(RightData {
            map: Arc::new(Map::HashMap(hashmap)),
            batch: empty_batch,
            values: vec![],
            group_by_arrays: vec![],
            build_agg_arrays: vec![],
        }));
    }

    let num_rows: usize = all_batches.iter().map(|b| b.num_rows()).sum();

    // Build the hash map (process batches in reverse order, as HashJoinExec does).
    let mut hashmap: Box<dyn JoinHashMapType> = if num_rows > u32::MAX as usize {
        Box::new(JoinHashMapU64::with_capacity(num_rows))
    } else {
        Box::new(JoinHashMapU32::with_capacity(num_rows))
    };
    let mut hashes_buffer: Vec<u64> = Vec::new();
    let mut offset = 0usize;
    for batch in all_batches.iter().rev() {
        hashes_buffer.clear();
        hashes_buffer.resize(batch.num_rows(), 0);
        update_hash(
            &on_right,
            batch,
            &mut *hashmap,
            offset,
            &random_state,
            &mut hashes_buffer,
            0,
            true,
        )?;
        offset += batch.num_rows();
    }

    // Concatenate all right batches into one (for index-based lookups).
    let batch = concat_batches(&schema, all_batches.iter().rev())?;

    // Evaluate join key columns on the concatenated batch.
    let values = evaluate_expressions_to_arrays(&on_right, &batch)?;

    // Evaluate right-side group-by columns on the concatenated batch.
    let group_by_arrays = evaluate_expressions_to_arrays(&right_group_by_exprs, &batch)?;

    // Pre-evaluate build-side aggregate argument arrays once (shared across all probe batches).
    let build_agg_arrays: Vec<Vec<ArrayRef>> = build_agg_exprs
        .iter()
        .map(|exprs| {
            exprs
                .iter()
                .map(|expr| {
                    expr.evaluate(&batch)
                        .and_then(|v| v.into_array(batch.num_rows()))
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<_>>()?;

    Ok(Arc::new(RightData {
        map: Arc::new(Map::HashMap(hashmap)),
        batch,
        values,
        group_by_arrays,
        build_agg_arrays,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::Column;

    use crate::test::TestMemoryExec;

    fn build_task_context() -> Arc<TaskContext> {
        let runtime = RuntimeEnvBuilder::new().build_arc().unwrap();
        let config = SessionConfig::new().with_batch_size(4096);
        Arc::new(TaskContext::new(
            None,
            "test_session".to_string(),
            config,
            Default::default(),
            Default::default(),
            Default::default(),
            runtime,
        ))
    }

    #[tokio::test]
    async fn test_group_join_basic() -> Result<()> {
        // Left table: orders (order_id, customer_id)
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int32, false),
            Field::new("customer_id", DataType::Int32, false),
        ]));
        let left_batch = RecordBatch::try_new(
            Arc::clone(&left_schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(Int32Array::from(vec![10, 10, 20, 20])),
            ],
        )?;

        // Right table: line_items (order_id, amount)
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int32, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        let right_batch = RecordBatch::try_new(
            Arc::clone(&right_schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 2, 3, 3, 3, 4])),
                Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500, 600, 700])),
            ],
        )?;

        let left_exec = Arc::new(TestMemoryExec::try_new(
            &[vec![left_batch]],
            Arc::clone(&left_schema),
            None,
        )?);
        let right_exec = Arc::new(TestMemoryExec::try_new(
            &[vec![right_batch]],
            Arc::clone(&right_schema),
            None,
        )?);

        // GROUP BY customer_id, SUM(amount)
        // Join on order_id
        let on: JoinOn = vec![(
            Arc::new(Column::new("order_id", 0)),
            Arc::new(Column::new("order_id", 0)),
        )];

        let group_by_exprs = vec![(
            Arc::new(Column::new("customer_id", 1)) as PhysicalExprRef,
            "customer_id".to_string(),
        )];

        // SUM(amount) - amount is column 1 in the right schema
        let sum_udf = datafusion_functions_aggregate::sum::sum_udaf();
        let sum_expr = Arc::new(
            AggregateExprBuilder::new(sum_udf, vec![Arc::new(Column::new("amount", 1))])
                .schema(Arc::clone(&right_schema))
                .alias("sum_amount")
                .build()?,
        );

        let output_schema = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Int32, false),
            Field::new("sum_amount", DataType::Int64, true),
        ]));

        let group_join = GroupJoinExec::try_new(
            AggregateMode::Single,
            left_exec,
            right_exec,
            on,
            group_by_exprs,
            vec![sum_expr],
            output_schema,
        )?;

        let context = build_task_context();
        let mut stream = group_join.execute(0, context)?;

        let mut results: Vec<RecordBatch> = Vec::new();
        while let Some(batch) = stream.next().await {
            results.push(batch?);
        }

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(result.num_rows(), 2);

        // Collect results into a map for order-independent comparison
        let customer_ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let sums = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let mut result_map = std::collections::HashMap::new();
        for i in 0..result.num_rows() {
            result_map.insert(customer_ids.value(i), sums.value(i));
        }

        // Customer 10 has orders 1, 2:
        //   order 1: amounts 100 + 200 = 300
        //   order 2: amount 300
        //   total = 600
        assert_eq!(result_map[&10], 600);

        // Customer 20 has orders 3, 4:
        //   order 3: amounts 400 + 500 + 600 = 1500
        //   order 4: amount 700
        //   total = 2200
        assert_eq!(result_map[&20], 2200);

        Ok(())
    }

    #[tokio::test]
    async fn test_group_join_empty_right() -> Result<()> {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]));
        let left_batch = RecordBatch::try_new(
            Arc::clone(&left_schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![10, 20])),
            ],
        )?;

        let right_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        // Empty right side
        let right_batch = RecordBatch::new_empty(Arc::clone(&right_schema));

        let left_exec = Arc::new(TestMemoryExec::try_new(
            &[vec![left_batch]],
            Arc::clone(&left_schema),
            None,
        )?);
        let right_exec = Arc::new(TestMemoryExec::try_new(
            &[vec![right_batch]],
            Arc::clone(&right_schema),
            None,
        )?);

        let on: JoinOn = vec![(
            Arc::new(Column::new("key", 0)),
            Arc::new(Column::new("key", 0)),
        )];

        let group_by_exprs = vec![(
            Arc::new(Column::new("val", 1)) as PhysicalExprRef,
            "val".to_string(),
        )];

        let sum_udf = datafusion_functions_aggregate::sum::sum_udaf();
        let sum_expr = Arc::new(
            AggregateExprBuilder::new(sum_udf, vec![Arc::new(Column::new("amount", 1))])
                .schema(Arc::clone(&right_schema))
                .alias("sum_amount")
                .build()?,
        );

        let output_schema = Arc::new(Schema::new(vec![
            Field::new("val", DataType::Int32, false),
            Field::new("sum_amount", DataType::Int64, true),
        ]));

        let group_join = GroupJoinExec::try_new(
            AggregateMode::Single,
            left_exec,
            right_exec,
            on,
            group_by_exprs,
            vec![sum_expr],
            output_schema,
        )?;

        let context = build_task_context();
        let mut stream = group_join.execute(0, context)?;

        let mut results: Vec<RecordBatch> = Vec::new();
        while let Some(batch) = stream.next().await {
            results.push(batch?);
        }

        // INNER join with empty right → no results
        assert!(results.is_empty());

        Ok(())
    }
}
