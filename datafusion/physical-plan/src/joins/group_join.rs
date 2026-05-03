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

//! [`GroupJoinExec`] fuses a hash join and a subsequent group-by aggregation
//! into a single operator when they share the same key.
//!
//! Based on: Moerkotte & Neumann, "Accelerating Queries with Group-By and Join
//! by Groupjoin", PVLDB 4(11), 2011. Strategy 2 (Memoizing GroupJoin) from
//! Fent et al., VLDB Journal 2023.
//!
//! Instead of building two hash tables (one for the join, one for aggregation),
//! GroupJoin builds a single hash table on the build side with aggregate
//! accumulators embedded in each entry. During the probe phase, matching rows
//! update the accumulators in-place, avoiding materialization of the full
//! intermediate join result.

use std::fmt::{self, Debug};
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{ArrayRef, BooleanArray, BooleanBufferBuilder};
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{JoinType, Result, internal_err};
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion_expr::{EmitTo, GroupsAccumulator};
use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
use datafusion_physical_expr::{
    EquivalenceProperties, GroupsAccumulatorAdapter, PhysicalExpr, PhysicalExprRef,
};
use log::debug;

use crate::aggregates::group_values::{GroupValues, new_group_values};
use crate::aggregates::order::GroupOrdering;
use crate::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use crate::{DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties};

use futures::{Stream, StreamExt, ready};

/// A fused join + group-by operator that combines a hash join and subsequent
/// aggregation into a single operator when they share the same key.
///
/// # Preconditions
///
/// These are enforced by the `GroupJoinOptimizer` physical optimizer rule:
/// - The aggregate GROUP BY keys exactly match the join equi-keys
/// - The join type is Inner or Left
/// - All aggregate functions support [`GroupsAccumulator`]
/// - The aggregate has at least one aggregate expression (not just DISTINCT)
///
/// # Algorithm
///
/// 1. **Build phase**: Consume the left (build) side, interning each row's
///    group key into a [`GroupValues`] hash table.
/// 2. **Probe phase**: For each right (probe) batch, look up group indices
///    via the same hash table and update [`GroupsAccumulator`]s in-place.
///    Probe rows that don't match any build-side group are filtered out
///    (for Inner join) or ignored (for Left join — the build-side group
///    retains its initial accumulator value).
/// 3. **Emit phase**: Scan the hash table and produce one output row per
///    build-side group: the group key columns plus the evaluated aggregates.
///
#[derive(Debug)]
pub struct GroupJoinExec {
    /// Build side (left) — each unique key becomes one output group
    left: Arc<dyn ExecutionPlan>,
    /// Probe side (right) — rows update aggregate accumulators
    right: Arc<dyn ExecutionPlan>,
    /// Equi-join key expressions: (left_expr, right_expr)
    on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
    /// Join type (Inner or Left)
    join_type: JoinType,
    /// GROUP BY expressions with output aliases (evaluated against build side)
    group_by_exprs: Vec<(PhysicalExprRef, String)>,
    /// Aggregate function expressions (e.g., COUNT, SUM)
    aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
    /// Input schema used to build aggregate expressions
    aggr_input_schema: SchemaRef,
    /// Output schema: group-by columns + aggregate outputs
    schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cached plan properties
    cache: Arc<PlanProperties>,
}

impl GroupJoinExec {
    /// Create a new `GroupJoinExec`.
    ///
    /// Returns an error if the join type is not supported.
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
        join_type: JoinType,
        group_by_exprs: Vec<(PhysicalExprRef, String)>,
        aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
    ) -> Result<Self> {
        let aggr_input_schema = right.schema();
        Self::try_new_with_aggr_input_schema(
            left,
            right,
            on,
            join_type,
            group_by_exprs,
            aggr_expr,
            aggr_input_schema,
        )
    }

    /// Create a new `GroupJoinExec` with the schema used to build aggregate
    /// expressions.
    pub fn try_new_with_aggr_input_schema(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
        join_type: JoinType,
        group_by_exprs: Vec<(PhysicalExprRef, String)>,
        aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
        aggr_input_schema: SchemaRef,
    ) -> Result<Self> {
        if !matches!(join_type, JoinType::Inner | JoinType::Left) {
            return internal_err!(
                "GroupJoinExec only supports Inner/Left joins, got {:?}",
                join_type
            );
        }

        let left_schema = left.schema();
        let mut fields: Vec<Arc<Field>> = Vec::new();

        for (expr, alias) in &group_by_exprs {
            let dt = expr.data_type(&left_schema)?;
            let nullable = expr.nullable(&left_schema)?;
            fields.push(Arc::new(Field::new(alias, dt, nullable)));
        }

        for agg in &aggr_expr {
            fields.push(Arc::clone(&agg.field()));
        }

        let schema = Arc::new(Schema::new(fields));

        let props = left.properties();
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            props.partitioning.clone(),
            props.emission_type,
            props.boundedness,
        ));

        Ok(Self {
            left,
            right,
            on,
            join_type,
            group_by_exprs,
            aggr_expr,
            aggr_input_schema,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        })
    }

    /// Build side input.
    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        &self.left
    }

    /// Probe side input.
    pub fn right(&self) -> &Arc<dyn ExecutionPlan> {
        &self.right
    }

    /// Equi-join key expressions.
    pub fn on(&self) -> &[(PhysicalExprRef, PhysicalExprRef)] {
        &self.on
    }

    /// Join type.
    pub fn join_type(&self) -> &JoinType {
        &self.join_type
    }

    /// GROUP BY expressions with output aliases.
    pub fn group_by_exprs(&self) -> &[(PhysicalExprRef, String)] {
        &self.group_by_exprs
    }

    /// Aggregate expressions.
    pub fn aggr_expr(&self) -> &[Arc<AggregateFunctionExpr>] {
        &self.aggr_expr
    }

    /// Input schema used to build aggregate expressions.
    pub fn aggr_input_schema(&self) -> &SchemaRef {
        &self.aggr_input_schema
    }
}

impl DisplayAs for GroupJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let on: Vec<String> =
                    self.on.iter().map(|(l, r)| format!("({l}, {r})")).collect();
                let aggrs: Vec<String> = self
                    .aggr_expr
                    .iter()
                    .map(|a| a.name().to_string())
                    .collect();
                write!(
                    f,
                    "GroupJoinExec: join_type={:?}, on=[{}], aggr=[{}]",
                    self.join_type,
                    on.join(", "),
                    aggrs.join(", "),
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "GroupJoinExec")
            }
        }
    }
}

impl ExecutionPlan for GroupJoinExec {
    fn name(&self) -> &'static str {
        "GroupJoinExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(GroupJoinExec::try_new_with_aggr_input_schema(
            Arc::clone(&children[0]),
            Arc::clone(&children[1]),
            self.on.clone(),
            self.join_type,
            self.group_by_exprs.clone(),
            self.aggr_expr.clone(),
            Arc::clone(&self.aggr_input_schema),
        )?))
    }
    fn required_input_distribution(&self) -> Vec<Distribution> {
        let left_exprs: Vec<PhysicalExprRef> =
            self.on.iter().map(|(l, _)| Arc::clone(l)).collect();
        let right_exprs: Vec<PhysicalExprRef> =
            self.on.iter().map(|(_, r)| Arc::clone(r)).collect();
        vec![
            Distribution::HashPartitioned(left_exprs),
            Distribution::HashPartitioned(right_exprs),
        ]
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        for (left_expr, right_expr) in &self.on {
            let r = f(left_expr.as_ref())?;
            if r == TreeNodeRecursion::Stop {
                return Ok(r);
            }
            let r = f(right_expr.as_ref())?;
            if r == TreeNodeRecursion::Stop {
                return Ok(r);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let left_stream = self.left.execute(partition, Arc::clone(&context))?;
        let right_stream = self.right.execute(partition, Arc::clone(&context))?;
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        let left_schema = self.left.schema();
        let key_fields: Vec<Arc<Field>> = self
            .group_by_exprs
            .iter()
            .map(|(expr, alias)| {
                let dt = expr.data_type(&left_schema).unwrap();
                let nullable = expr.nullable(&left_schema).unwrap();
                Arc::new(Field::new(alias, dt, nullable))
            })
            .collect();
        let key_schema = Arc::new(Schema::new(key_fields));

        let accumulators: Vec<Box<dyn GroupsAccumulator>> = self
            .aggr_expr
            .iter()
            .map(|agg_expr| -> Result<Box<dyn GroupsAccumulator>> {
                if agg_expr.groups_accumulator_supported() {
                    agg_expr.create_groups_accumulator()
                } else {
                    debug!(
                        "GroupJoinExec: using GroupsAccumulatorAdapter for {}",
                        agg_expr.name()
                    );
                    let captured = Arc::clone(agg_expr);
                    let factory = move || captured.create_accumulator();
                    Ok(Box::new(GroupsAccumulatorAdapter::new(factory)))
                }
            })
            .collect::<Result<_>>()?;

        let group_values = new_group_values(key_schema, &GroupOrdering::None)?;

        let group_by_exprs: Vec<PhysicalExprRef> = self
            .group_by_exprs
            .iter()
            .map(|(expr, _)| Arc::clone(expr))
            .collect();

        Ok(Box::pin(GroupJoinStream {
            left_stream: Some(left_stream),
            right_stream,
            right_on: self.on.iter().map(|(_, r)| Arc::clone(r)).collect(),
            group_by_exprs,
            aggr_expr: self.aggr_expr.clone(),
            accumulators,
            group_values,
            schema: Arc::clone(&self.schema),
            baseline_metrics,
            state: GroupJoinState::CollectBuildSide,
            group_indices: Vec::new(),
            num_build_groups: 0,
            join_type: self.join_type,
            visited: BooleanBufferBuilder::new(0),
        }))
    }
}

// ── Stream implementation ──────────────────────────────────────────────

enum GroupJoinState {
    CollectBuildSide,
    Probe,
    Emit,
    Done,
}

struct GroupJoinStream {
    left_stream: Option<SendableRecordBatchStream>,
    right_stream: SendableRecordBatchStream,
    right_on: Vec<PhysicalExprRef>,
    group_by_exprs: Vec<PhysicalExprRef>,
    aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
    accumulators: Vec<Box<dyn GroupsAccumulator>>,
    group_values: Box<dyn GroupValues>,
    schema: SchemaRef,
    baseline_metrics: BaselineMetrics,
    state: GroupJoinState,
    group_indices: Vec<usize>,
    num_build_groups: usize,
    join_type: JoinType,
    /// Tracks which build-side groups received at least one probe match.
    /// For Inner joins, only visited groups are emitted.
    /// For Left joins, all build-side groups are emitted (visited or not).
    visited: BooleanBufferBuilder,
}

impl Debug for GroupJoinStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupJoinStream").finish_non_exhaustive()
    }
}

impl GroupJoinStream {
    fn collect_build_side(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let stream = self.left_stream.as_mut().unwrap();
        loop {
            match ready!(stream.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let key_arrays: Vec<ArrayRef> = self
                        .group_by_exprs
                        .iter()
                        .map(|expr| {
                            expr.evaluate(&batch)
                                .and_then(|v| v.into_array(batch.num_rows()))
                        })
                        .collect::<Result<_>>()?;

                    self.group_indices.clear();
                    self.group_values
                        .intern(&key_arrays, &mut self.group_indices)?;
                }
                Some(Err(e)) => return Poll::Ready(Err(e)),
                None => {
                    self.left_stream = None;
                    self.num_build_groups = self.group_values.len();
                    // Initialize visited bitmap for Inner join filtering
                    self.visited = BooleanBufferBuilder::new(self.num_build_groups);
                    self.visited.append_n(self.num_build_groups, false);
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }

    fn process_probe_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let key_arrays: Vec<ArrayRef> = self
            .right_on
            .iter()
            .map(|expr| {
                expr.evaluate(batch)
                    .and_then(|v| v.into_array(batch.num_rows()))
            })
            .collect::<Result<_>>()?;

        self.group_indices.clear();
        self.group_values
            .intern(&key_arrays, &mut self.group_indices)?;
        let total_num_groups = self.group_values.len();

        // Filter: only update accumulators for rows matching build-side groups.
        // Probe rows creating new groups (not in build side) are excluded.
        let filter: Option<BooleanArray> = if total_num_groups > self.num_build_groups {
            let mask: Vec<bool> = self
                .group_indices
                .iter()
                .map(|&idx| idx < self.num_build_groups)
                .collect();
            Some(BooleanArray::from(mask))
        } else {
            None
        };

        // Mark build-side groups that received at least one probe match
        for &idx in &self.group_indices {
            if idx < self.num_build_groups {
                self.visited.set_bit(idx, true);
            }
        }

        for (acc_idx, agg) in self.aggr_expr.iter().enumerate() {
            let values: Vec<ArrayRef> = agg
                .expressions()
                .iter()
                .map(|expr| {
                    expr.evaluate(batch)
                        .and_then(|v| v.into_array(batch.num_rows()))
                })
                .collect::<Result<_>>()?;

            self.accumulators[acc_idx].update_batch(
                &values,
                &self.group_indices,
                filter.as_ref(),
                total_num_groups,
            )?;
        }

        Ok(())
    }

    fn emit_results(&mut self) -> Result<RecordBatch> {
        if self.num_build_groups == 0 {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }

        let emit_to = if self.group_values.len() > self.num_build_groups {
            EmitTo::First(self.num_build_groups)
        } else {
            EmitTo::All
        };

        let mut columns: Vec<ArrayRef> = self.group_values.emit(emit_to)?;

        for acc in &mut self.accumulators {
            columns.push(acc.evaluate(emit_to)?);
        }

        let batch = RecordBatch::try_new(Arc::clone(&self.schema), columns)?;

        // For Inner joins, filter to only groups that had at least one probe match
        if self.join_type == JoinType::Inner {
            let visited_mask = BooleanArray::new(self.visited.finish(), None);
            let filtered = arrow::compute::filter_record_batch(&batch, &visited_mask)?;
            Ok(filtered)
        } else {
            // Left joins emit all build-side groups (unmatched get default values)
            Ok(batch)
        }
    }
}

impl Stream for GroupJoinStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match self.state {
                GroupJoinState::CollectBuildSide => {
                    match ready!(self.collect_build_side(cx)) {
                        Ok(()) => self.state = GroupJoinState::Probe,
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    }
                }
                GroupJoinState::Probe => {
                    match ready!(self.right_stream.poll_next_unpin(cx)) {
                        Some(Ok(batch)) => {
                            if let Err(e) = self.process_probe_batch(&batch) {
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                        Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                        None => {
                            self.state = GroupJoinState::Emit;
                        }
                    }
                }
                GroupJoinState::Emit => {
                    self.state = GroupJoinState::Done;
                    let result = self.emit_results();
                    if let Ok(ref batch) = result {
                        self.baseline_metrics.record_output(batch.num_rows());
                    }
                    return Poll::Ready(Some(result));
                }
                GroupJoinState::Done => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl RecordBatchStream for GroupJoinStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
