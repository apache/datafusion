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

//! Aggregates functionalities

use crate::aggregates::{
    no_grouping::AggregateStream, row_hash::GroupedHashAggregateStream,
};
use crate::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::{
    DisplayFormatType, Distribution, ExecutionPlan, Partitioning,
    SendableRecordBatchStream, Statistics,
};

use arrow::array::ArrayRef;
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::{not_impl_err, plan_err, DataFusionError, Result};
use datafusion_execution::TaskContext;
use datafusion_expr::Accumulator;
use datafusion_physical_expr::{
    expressions::Column, physical_exprs_contains, project_out_expr, reverse_order_bys,
    AggregateExpr, LexOrdering, LexOrderingReq, PhysicalExpr, PhysicalSortExpr,
    PhysicalSortRequirement, SchemaProperties,
};

use itertools::{izip, Itertools};
use std::any::Any;
use std::sync::Arc;

mod group_values;
mod no_grouping;
mod order;
mod row_hash;
mod topk;
mod topk_stream;

use crate::aggregates::topk_stream::GroupedTopKAggregateStream;
use crate::common::calculate_projection_mapping;
use crate::windows::{
    get_ordered_partition_by_indices, get_window_mode, PartitionSearchMode,
};
pub use datafusion_expr::AggregateFunction;
use datafusion_physical_expr::aggregate::is_order_sensitive;
use datafusion_physical_expr::equivalence::collapse_lex_req;
pub use datafusion_physical_expr::expressions::create_aggregate_expr;
use datafusion_physical_expr::expressions::{Max, Min};

use super::DisplayAs;

/// Hash aggregate modes
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AggregateMode {
    /// Partial aggregate that can be applied in parallel across input partitions
    Partial,
    /// Final aggregate that produces a single partition of output
    Final,
    /// Final aggregate that works on pre-partitioned data.
    ///
    /// This requires the invariant that all rows with a particular
    /// grouping key are in the same partitions, such as is the case
    /// with Hash repartitioning on the group keys. If a group key is
    /// duplicated, duplicate groups would be produced
    FinalPartitioned,
    /// Applies the entire logical aggregation operation in a single operator,
    /// as opposed to Partial / Final modes which apply the logical aggregation using
    /// two operators.
    /// This mode requires tha the input is a single partition (like Final)
    Single,
    /// Applies the entire logical aggregation operation in a single operator,
    /// as opposed to Partial / Final modes which apply the logical aggregation using
    /// two operators.
    /// This mode requires tha the input is partitioned by group key (like FinalPartitioned)
    SinglePartitioned,
}

impl AggregateMode {
    /// Checks whether this aggregation step describes a "first stage" calculation.
    /// In other words, its input is not another aggregation result and the
    /// `merge_batch` method will not be called for these modes.
    pub fn is_first_stage(&self) -> bool {
        match self {
            AggregateMode::Partial
            | AggregateMode::Single
            | AggregateMode::SinglePartitioned => true,
            AggregateMode::Final | AggregateMode::FinalPartitioned => false,
        }
    }
}

/// Group By expression modes
///
/// `PartiallyOrdered` and `FullyOrdered` are used to reason about
/// when certain group by keys will never again be seen (and thus can
/// be emitted by the grouping operator).
///
/// Specifically, each distinct combination of the relevant columns
/// are contiguous in the input, and once a new combination is seen
/// previous combinations are guaranteed never to appear again
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupByOrderMode {
    /// The input is known to be ordered by a preset (prefix but
    /// possibly reordered) of the expressions in the `GROUP BY` clause.
    ///
    /// For example, if the input is ordered by `a, b, c` and we group
    /// by `b, a, d`, `PartiallyOrdered` means a subset of group `b,
    /// a, d` defines a preset for the existing ordering, in this case
    /// `a, b`.
    PartiallyOrdered,
    /// The input is known to be ordered by *all* the expressions in the
    /// `GROUP BY` clause.
    ///
    /// For example, if the input is ordered by `a, b, c, d` and we group by b, a,
    /// `Ordered` means that all of the of group by expressions appear
    ///  as a preset for the existing ordering, in this case `a, b`.
    FullyOrdered,
}

/// Represents `GROUP BY` clause in the plan (including the more general GROUPING SET)
/// In the case of a simple `GROUP BY a, b` clause, this will contain the expression [a, b]
/// and a single group [false, false].
/// In the case of `GROUP BY GROUPING SET/CUBE/ROLLUP` the planner will expand the expression
/// into multiple groups, using null expressions to align each group.
/// For example, with a group by clause `GROUP BY GROUPING SET ((a,b),(a),(b))` the planner should
/// create a `PhysicalGroupBy` like
/// ```text
/// PhysicalGroupBy {
///     expr: [(col(a), a), (col(b), b)],
///     null_expr: [(NULL, a), (NULL, b)],
///     groups: [
///         [false, false], // (a,b)
///         [false, true],  // (a) <=> (a, NULL)
///         [true, false]   // (b) <=> (NULL, b)
///     ]
/// }
/// ```
#[derive(Clone, Debug, Default)]
pub struct PhysicalGroupBy {
    /// Distinct (Physical Expr, Alias) in the grouping set
    expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
    /// Corresponding NULL expressions for expr
    null_expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
    /// Null mask for each group in this grouping set. Each group is
    /// composed of either one of the group expressions in expr or a null
    /// expression in null_expr. If `groups[i][j]` is true, then the the
    /// j-th expression in the i-th group is NULL, otherwise it is `expr[j]`.
    groups: Vec<Vec<bool>>,
}

impl PhysicalGroupBy {
    /// Create a new `PhysicalGroupBy`
    pub fn new(
        expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
        null_expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
        groups: Vec<Vec<bool>>,
    ) -> Self {
        Self {
            expr,
            null_expr,
            groups,
        }
    }

    /// Create a GROUPING SET with only a single group. This is the "standard"
    /// case when building a plan from an expression such as `GROUP BY a,b,c`
    pub fn new_single(expr: Vec<(Arc<dyn PhysicalExpr>, String)>) -> Self {
        let num_exprs = expr.len();
        Self {
            expr,
            null_expr: vec![],
            groups: vec![vec![false; num_exprs]],
        }
    }

    /// Returns true if this GROUP BY contains NULL expressions
    pub fn contains_null(&self) -> bool {
        self.groups.iter().flatten().any(|is_null| *is_null)
    }

    /// Returns the group expressions
    pub fn expr(&self) -> &[(Arc<dyn PhysicalExpr>, String)] {
        &self.expr
    }

    /// Returns the null expressions
    pub fn null_expr(&self) -> &[(Arc<dyn PhysicalExpr>, String)] {
        &self.null_expr
    }

    /// Returns the group null masks
    pub fn groups(&self) -> &[Vec<bool>] {
        &self.groups
    }

    /// Returns true if this `PhysicalGroupBy` has no group expressions
    pub fn is_empty(&self) -> bool {
        self.expr.is_empty()
    }

    /// Check whether grouping set is single group
    pub fn is_single(&self) -> bool {
        self.null_expr.is_empty()
    }

    /// Calculate group by expressions according to input schema.
    pub fn input_exprs(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.expr
            .iter()
            .map(|(expr, _alias)| expr.clone())
            .collect::<Vec<_>>()
    }

    /// This function returns grouping expressions as they occur in the output schema.
    fn output_exprs(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        // Update column indices. Since the group by columns come first in the output schema, their
        // indices are simply 0..self.group_expr(len).
        self.expr
            .iter()
            .enumerate()
            .map(|(index, (_, name))| Arc::new(Column::new(name, index)) as _)
            .collect()
    }
}

impl PartialEq for PhysicalGroupBy {
    fn eq(&self, other: &PhysicalGroupBy) -> bool {
        self.expr.len() == other.expr.len()
            && self
                .expr
                .iter()
                .zip(other.expr.iter())
                .all(|((expr1, name1), (expr2, name2))| expr1.eq(expr2) && name1 == name2)
            && self.null_expr.len() == other.null_expr.len()
            && self
                .null_expr
                .iter()
                .zip(other.null_expr.iter())
                .all(|((expr1, name1), (expr2, name2))| expr1.eq(expr2) && name1 == name2)
            && self.groups == other.groups
    }
}

enum StreamType {
    AggregateStream(AggregateStream),
    GroupedHash(GroupedHashAggregateStream),
    GroupedPriorityQueue(GroupedTopKAggregateStream),
}

impl From<StreamType> for SendableRecordBatchStream {
    fn from(stream: StreamType) -> Self {
        match stream {
            StreamType::AggregateStream(stream) => Box::pin(stream),
            StreamType::GroupedHash(stream) => Box::pin(stream),
            StreamType::GroupedPriorityQueue(stream) => Box::pin(stream),
        }
    }
}

/// Hash aggregate execution plan
#[derive(Debug)]
pub struct AggregateExec {
    /// Aggregation mode (full, partial)
    mode: AggregateMode,
    /// Group by expressions
    group_by: PhysicalGroupBy,
    /// Aggregate expressions
    aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    /// FILTER (WHERE clause) expression for each aggregate expression
    filter_expr: Vec<Option<Arc<dyn PhysicalExpr>>>,
    /// (ORDER BY clause) expression for each aggregate expression
    order_by_expr: Vec<Option<LexOrdering>>,
    /// Set if the output of this aggregation is truncated by a upstream sort/limit clause
    limit: Option<usize>,
    /// Input plan, could be a partial aggregate or the input to the aggregate
    pub input: Arc<dyn ExecutionPlan>,
    /// Schema after the aggregate is applied
    schema: SchemaRef,
    /// Input schema before any aggregation is applied. For partial aggregate this will be the
    /// same as input.schema() but for the final aggregate it will be the same as the input
    /// to the partial aggregate, i.e., partial and final aggregates have same `input_schema`.
    /// We need the input schema of partial aggregate to be able to deserialize aggregate
    /// expressions from protobuf for final aggregate.
    pub input_schema: SchemaRef,
    /// The source_to_target_mapping used to normalize out expressions like Partitioning and PhysicalSortExpr
    /// The key is the expression from the input schema and the value is the expression from the output schema.
    source_to_target_mapping: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
    /// Execution Metrics
    metrics: ExecutionPlanMetricsSet,
    required_input_ordering: Option<LexOrderingReq>,
    partition_search_mode: PartitionSearchMode,
    output_ordering: Option<LexOrdering>,
}

/// This function returns the ordering requirement of the first non-reversible
/// order-sensitive aggregate function such as ARRAY_AGG. This requirement serves
/// as the initial requirement while calculating the finest requirement among all
/// aggregate functions. If this function returns `None`, it means there is no
/// hard ordering requirement for the aggregate functions (in terms of direction).
/// Then, we can generate two alternative requirements with opposite directions.
fn get_init_req(
    aggr_expr: &[Arc<dyn AggregateExpr>],
    order_by_expr: &[Option<LexOrdering>],
) -> Option<LexOrdering> {
    for (aggr_expr, fn_reqs) in aggr_expr.iter().zip(order_by_expr.iter()) {
        // If the aggregation function is a non-reversible order-sensitive function
        // and there is a hard requirement, choose first such requirement:
        if is_order_sensitive(aggr_expr)
            && aggr_expr.reverse_expr().is_none()
            && fn_reqs.is_some()
        {
            return fn_reqs.clone();
        }
    }
    None
}

/// This function gets the finest ordering requirement among all the aggregation
/// functions. If requirements are conflicting, (i.e. we can not compute the
/// aggregations in a single [`AggregateExec`]), the function returns an error.
fn get_finest_requirement(
    aggr_expr: &mut [Arc<dyn AggregateExpr>],
    order_by_expr: &mut [Option<LexOrdering>],
    schema_properties: &SchemaProperties,
) -> Result<Option<LexOrdering>> {
    // Check at the beginning if all the requirements are satisfied by existing ordering
    // If so return None, to indicate all of the requirements are already satisfied.
    let mut all_satisfied = true;
    for (aggr_expr, fn_req) in aggr_expr.iter_mut().zip(order_by_expr.iter_mut()) {
        if schema_properties.ordering_satisfy(fn_req.as_deref()) {
            continue;
        }
        if let Some(reverse) = aggr_expr.reverse_expr() {
            let reverse_req = fn_req.as_ref().map(|item| reverse_order_bys(item));
            if schema_properties.ordering_satisfy(reverse_req.as_deref()) {
                // We need to update `aggr_expr` with its reverse, since only its
                // reverse requirement is compatible with existing requirements:
                *aggr_expr = reverse;
                *fn_req = reverse_req;
                continue;
            }
        }
        // requirement is not satisfied
        all_satisfied = false;
    }
    if all_satisfied {
        // All of the requirements are already satisfied.
        return Ok(None);
    }
    let mut finest_req = get_init_req(aggr_expr, order_by_expr);
    for (aggr_expr, fn_req) in aggr_expr.iter_mut().zip(order_by_expr.iter_mut()) {
        let fn_req = if let Some(fn_req) = fn_req {
            fn_req
        } else {
            continue;
        };

        if let Some(finest_req) = &mut finest_req {
            if let Some(finer) = schema_properties.get_finer_ordering(finest_req, fn_req)
            {
                *finest_req = finer;
                continue;
            }
            // If an aggregate function is reversible, analyze whether its reverse
            // direction is compatible with existing requirements:
            if let Some(reverse) = aggr_expr.reverse_expr() {
                let fn_req_reverse = reverse_order_bys(fn_req);
                if let Some(finer) =
                    schema_properties.get_finer_ordering(finest_req, &fn_req_reverse)
                {
                    // We need to update `aggr_expr` with its reverse, since only its
                    // reverse requirement is compatible with existing requirements:
                    *aggr_expr = reverse;
                    *finest_req = finer;
                    *fn_req = fn_req_reverse;
                    continue;
                }
            }
            // If neither of the requirements satisfy the other, this means
            // requirements are conflicting. Currently, we do not support
            // conflicting requirements.
            return not_impl_err!(
                "Conflicting ordering requirements in aggregate functions is not supported"
            );
        } else {
            finest_req = Some(fn_req.clone());
        }
    }
    Ok(finest_req)
}

/// Calculates search_mode for the aggregation
fn get_aggregate_search_mode(
    group_by: &PhysicalGroupBy,
    input: &Arc<dyn ExecutionPlan>,
    aggr_expr: &mut [Arc<dyn AggregateExpr>],
    order_by_expr: &mut [Option<LexOrdering>],
    ordering_req: &mut Vec<PhysicalSortExpr>,
) -> Result<PartitionSearchMode> {
    let groupby_exprs = group_by
        .expr
        .iter()
        .map(|(item, _)| item.clone())
        .collect::<Vec<_>>();
    let mut partition_search_mode = PartitionSearchMode::Linear;
    if !group_by.is_single() || groupby_exprs.is_empty() {
        return Ok(partition_search_mode);
    }

    if let Some((should_reverse, mode)) =
        get_window_mode(&groupby_exprs, ordering_req, input)?
    {
        let all_reversible = aggr_expr
            .iter()
            .all(|expr| !is_order_sensitive(expr) || expr.reverse_expr().is_some());
        if should_reverse && all_reversible {
            izip!(aggr_expr.iter_mut(), order_by_expr.iter_mut()).for_each(
                |(aggr, order_by)| {
                    if let Some(reverse) = aggr.reverse_expr() {
                        *aggr = reverse;
                    } else {
                        unreachable!();
                    }
                    *order_by = order_by.as_ref().map(|ob| reverse_order_bys(ob));
                },
            );
            *ordering_req = reverse_order_bys(ordering_req);
        }
        partition_search_mode = mode;
    }
    Ok(partition_search_mode)
}

/// Check whether group by expression contains all of the expression inside `requirement`
// As an example Group By (c,b,a) contains all of the expressions in the `requirement`: (a ASC, b DESC)
fn group_by_contains_all_requirements(
    group_by: &PhysicalGroupBy,
    requirement: &LexOrdering,
) -> bool {
    let physical_exprs = group_by.input_exprs();
    // When we have multiple groups (grouping set)
    // since group by may be calculated on the subset of the group_by.expr()
    // it is not guaranteed to have all of the requirements among group by expressions.
    // Hence do the analysis: whether group by contains all requirements in the single group case.
    group_by.is_single()
        && requirement
            .iter()
            .all(|req| physical_exprs_contains(&physical_exprs, &req.expr))
}

impl AggregateExec {
    /// Create a new hash aggregate execution plan
    pub fn try_new(
        mode: AggregateMode,
        group_by: PhysicalGroupBy,
        mut aggr_expr: Vec<Arc<dyn AggregateExpr>>,
        filter_expr: Vec<Option<Arc<dyn PhysicalExpr>>>,
        // Ordering requirement of each aggregate expression
        mut order_by_expr: Vec<Option<LexOrdering>>,
        input: Arc<dyn ExecutionPlan>,
        input_schema: SchemaRef,
    ) -> Result<Self> {
        let schema = create_schema(
            &input.schema(),
            &group_by.expr,
            &aggr_expr,
            group_by.contains_null(),
            mode,
        )?;

        let schema = Arc::new(schema);
        // Reset ordering requirement to `None` if aggregator is not order-sensitive
        order_by_expr = aggr_expr
            .iter()
            .zip(order_by_expr)
            .map(|(aggr_expr, fn_reqs)| {
                // If
                // - aggregation function is order-sensitive and
                // - aggregation is performing a "first stage" calculation, and
                // - at least one of the aggregate function requirement is not inside group by expression
                // keep the ordering requirement as is; otherwise ignore the ordering requirement.
                // In non-first stage modes, we accumulate data (using `merge_batch`)
                // from different partitions (i.e. merge partial results). During
                // this merge, we consider the ordering of each partial result.
                // Hence, we do not need to use the ordering requirement in such
                // modes as long as partial results are generated with the
                // correct ordering.
                fn_reqs.filter(|req| {
                    is_order_sensitive(aggr_expr)
                        && mode.is_first_stage()
                        && !group_by_contains_all_requirements(&group_by, req)
                })
            })
            .collect::<Vec<_>>();
        let requirement = get_finest_requirement(
            &mut aggr_expr,
            &mut order_by_expr,
            &input.schema_properties(),
        )?;
        let mut ordering_req = requirement.unwrap_or(vec![]);
        let partition_search_mode = get_aggregate_search_mode(
            &group_by,
            &input,
            &mut aggr_expr,
            &mut order_by_expr,
            &mut ordering_req,
        )?;

        // get group by exprs
        let groupby_exprs = group_by.input_exprs();
        // If existing ordering satisfies a prefix of groupby expression, prefix requirement
        // with this section. In this case, group by will work more efficient
        let indices = get_ordered_partition_by_indices(&groupby_exprs, &input);
        let mut new_requirement = indices
            .into_iter()
            .map(|idx| PhysicalSortRequirement {
                expr: groupby_exprs[idx].clone(),
                options: None,
            })
            .collect::<Vec<_>>();
        // Postfix ordering requirement of the aggregation to the requirement.
        let req = PhysicalSortRequirement::from_sort_exprs(&ordering_req);
        new_requirement.extend(req);
        new_requirement = collapse_lex_req(new_requirement);

        // construct a map from the input expression to the output expression of the Aggregation group by
        let source_to_target_mapping =
            calculate_projection_mapping(&group_by.expr, &input.schema())?;

        let required_input_ordering = if new_requirement.is_empty() {
            None
        } else {
            Some(new_requirement)
        };

        let aggregate_oeq = input
            .schema_properties()
            .project(&source_to_target_mapping, schema.clone());
        let output_ordering = aggregate_oeq.oeq_group().output_ordering();

        Ok(AggregateExec {
            mode,
            group_by,
            aggr_expr,
            filter_expr,
            order_by_expr,
            input,
            schema,
            input_schema,
            source_to_target_mapping,
            metrics: ExecutionPlanMetricsSet::new(),
            required_input_ordering,
            limit: None,
            partition_search_mode,
            output_ordering,
        })
    }

    /// Aggregation mode (full, partial)
    pub fn mode(&self) -> &AggregateMode {
        &self.mode
    }

    /// Set the `limit` of this AggExec
    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }
    /// Grouping expressions
    pub fn group_expr(&self) -> &PhysicalGroupBy {
        &self.group_by
    }

    /// Grouping expressions as they occur in the output schema
    pub fn output_group_expr(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.group_by.output_exprs()
    }

    /// Aggregate expressions
    pub fn aggr_expr(&self) -> &[Arc<dyn AggregateExpr>] {
        &self.aggr_expr
    }

    /// FILTER (WHERE clause) expression for each aggregate expression
    pub fn filter_expr(&self) -> &[Option<Arc<dyn PhysicalExpr>>] {
        &self.filter_expr
    }

    /// ORDER BY clause expression for each aggregate expression
    pub fn order_by_expr(&self) -> &[Option<LexOrdering>] {
        &self.order_by_expr
    }

    /// Input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Get the input schema before any aggregates are applied
    pub fn input_schema(&self) -> SchemaRef {
        self.input_schema.clone()
    }

    fn execute_typed(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<StreamType> {
        // no group by at all
        if self.group_by.expr.is_empty() {
            return Ok(StreamType::AggregateStream(AggregateStream::new(
                self, context, partition,
            )?));
        }

        // grouping by an expression that has a sort/limit upstream
        if let Some(limit) = self.limit {
            return Ok(StreamType::GroupedPriorityQueue(
                GroupedTopKAggregateStream::new(self, context, partition, limit)?,
            ));
        }

        // grouping by something else and we need to just materialize all results
        Ok(StreamType::GroupedHash(GroupedHashAggregateStream::new(
            self, context, partition,
        )?))
    }

    /// Finds the DataType and SortDirection for this Aggregate, if there is one
    pub fn get_minmax_desc(&self) -> Option<(Field, bool)> {
        let agg_expr = self.aggr_expr.iter().exactly_one().ok()?;
        if let Some(max) = agg_expr.as_any().downcast_ref::<Max>() {
            Some((max.field().ok()?, true))
        } else if let Some(min) = agg_expr.as_any().downcast_ref::<Min>() {
            Some((min.field().ok()?, false))
        } else {
            None
        }
    }

    pub fn group_by(&self) -> &PhysicalGroupBy {
        &self.group_by
    }
}

impl DisplayAs for AggregateExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "AggregateExec: mode={:?}", self.mode)?;
                let g: Vec<String> = if self.group_by.is_single() {
                    self.group_by
                        .expr
                        .iter()
                        .map(|(e, alias)| {
                            let e = e.to_string();
                            if &e != alias {
                                format!("{e} as {alias}")
                            } else {
                                e
                            }
                        })
                        .collect()
                } else {
                    self.group_by
                        .groups
                        .iter()
                        .map(|group| {
                            let terms = group
                                .iter()
                                .enumerate()
                                .map(|(idx, is_null)| {
                                    if *is_null {
                                        let (e, alias) = &self.group_by.null_expr[idx];
                                        let e = e.to_string();
                                        if &e != alias {
                                            format!("{e} as {alias}")
                                        } else {
                                            e
                                        }
                                    } else {
                                        let (e, alias) = &self.group_by.expr[idx];
                                        let e = e.to_string();
                                        if &e != alias {
                                            format!("{e} as {alias}")
                                        } else {
                                            e
                                        }
                                    }
                                })
                                .collect::<Vec<String>>()
                                .join(", ");
                            format!("({terms})")
                        })
                        .collect()
                };

                write!(f, ", gby=[{}]", g.join(", "))?;

                let a: Vec<String> = self
                    .aggr_expr
                    .iter()
                    .map(|agg| agg.name().to_string())
                    .collect();
                write!(f, ", aggr=[{}]", a.join(", "))?;
                if let Some(limit) = self.limit {
                    write!(f, ", lim=[{limit}]")?;
                }

                if self.partition_search_mode != PartitionSearchMode::Linear {
                    write!(f, ", ordering_mode={:?}", self.partition_search_mode)?;
                }
            }
        }
        Ok(())
    }
}

impl ExecutionPlan for AggregateExec {
    /// Return a reference to Any that can be used for down-casting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        match &self.mode {
            AggregateMode::Partial | AggregateMode::Single => {
                // Partial and Single Aggregation will not change the output partitioning but need to respect the Alias
                let input_partition = self.input.output_partitioning();
                match input_partition {
                    Partitioning::Hash(exprs, part) => {
                        let normalized_exprs = exprs
                            .into_iter()
                            .map(|expr| {
                                project_out_expr(expr, &self.source_to_target_mapping)
                            })
                            .collect::<Vec<_>>();
                        Partitioning::Hash(normalized_exprs, part)
                    }
                    _ => input_partition,
                }
            }
            // Final Aggregation's output partitioning is the same as its real input
            _ => self.input.output_partitioning(),
        }
    }

    /// Specifies whether this plan generates an infinite stream of records.
    /// If the plan does not support pipelining, but its input(s) are
    /// infinite, returns an error to indicate this.
    fn unbounded_output(&self, children: &[bool]) -> Result<bool> {
        if children[0] {
            if self.partition_search_mode == PartitionSearchMode::Linear {
                // Cannot run without breaking pipeline.
                plan_err!(
                    "Aggregate Error: `GROUP BY` clauses with columns without ordering and GROUPING SETS are not supported for unbounded inputs."
                )
            } else {
                Ok(true)
            }
        } else {
            Ok(false)
        }
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.output_ordering.as_deref()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        match &self.mode {
            AggregateMode::Partial => {
                vec![Distribution::UnspecifiedDistribution]
            }
            AggregateMode::FinalPartitioned | AggregateMode::SinglePartitioned => {
                vec![Distribution::HashPartitioned(self.output_group_expr())]
            }
            AggregateMode::Final | AggregateMode::Single => {
                vec![Distribution::SinglePartition]
            }
        }
    }

    fn required_input_ordering(&self) -> Vec<Option<LexOrderingReq>> {
        vec![self.required_input_ordering.clone()]
    }

    fn schema_properties(&self) -> SchemaProperties {
        self.input
            .schema_properties()
            .project(&self.source_to_target_mapping, self.schema())
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut me = AggregateExec::try_new(
            self.mode,
            self.group_by.clone(),
            self.aggr_expr.clone(),
            self.filter_expr.clone(),
            self.order_by_expr.clone(),
            children[0].clone(),
            self.input_schema.clone(),
        )?;
        me.limit = self.limit;
        Ok(Arc::new(me))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.execute_typed(partition, context)
            .map(|stream| stream.into())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        // TODO stats: group expressions:
        // - once expressions will be able to compute their own stats, use it here
        // - case where we group by on a column for which with have the `distinct` stat
        // TODO stats: aggr expression:
        // - aggregations somtimes also preserve invariants such as min, max...
        match self.mode {
            AggregateMode::Final | AggregateMode::FinalPartitioned
                if self.group_by.expr.is_empty() =>
            {
                Statistics {
                    num_rows: Some(1),
                    is_exact: true,
                    ..Default::default()
                }
            }
            _ => Statistics {
                // the output row count is surely not larger than its input row count
                num_rows: self.input.statistics().num_rows,
                is_exact: false,
                ..Default::default()
            },
        }
    }
}

fn create_schema(
    input_schema: &Schema,
    group_expr: &[(Arc<dyn PhysicalExpr>, String)],
    aggr_expr: &[Arc<dyn AggregateExpr>],
    contains_null_expr: bool,
    mode: AggregateMode,
) -> Result<Schema> {
    let mut fields = Vec::with_capacity(group_expr.len() + aggr_expr.len());
    for (expr, name) in group_expr {
        fields.push(Field::new(
            name,
            expr.data_type(input_schema)?,
            // In cases where we have multiple grouping sets, we will use NULL expressions in
            // order to align the grouping sets. So the field must be nullable even if the underlying
            // schema field is not.
            contains_null_expr || expr.nullable(input_schema)?,
        ))
    }

    match mode {
        AggregateMode::Partial => {
            // in partial mode, the fields of the accumulator's state
            for expr in aggr_expr {
                fields.extend(expr.state_fields()?.iter().cloned())
            }
        }
        AggregateMode::Final
        | AggregateMode::FinalPartitioned
        | AggregateMode::Single
        | AggregateMode::SinglePartitioned => {
            // in final mode, the field with the final result of the accumulator
            for expr in aggr_expr {
                fields.push(expr.field()?)
            }
        }
    }

    Ok(Schema::new(fields))
}

fn group_schema(schema: &Schema, group_count: usize) -> SchemaRef {
    let group_fields = schema.fields()[0..group_count].to_vec();
    Arc::new(Schema::new(group_fields))
}

/// returns physical expressions for arguments to evaluate against a batch
/// The expressions are different depending on `mode`:
/// * Partial: AggregateExpr::expressions
/// * Final: columns of `AggregateExpr::state_fields()`
fn aggregate_expressions(
    aggr_expr: &[Arc<dyn AggregateExpr>],
    mode: &AggregateMode,
    col_idx_base: usize,
) -> Result<Vec<Vec<Arc<dyn PhysicalExpr>>>> {
    match mode {
        AggregateMode::Partial
        | AggregateMode::Single
        | AggregateMode::SinglePartitioned => Ok(aggr_expr
            .iter()
            .map(|agg| {
                let mut result = agg.expressions().clone();
                // In partial mode, append ordering requirements to expressions' results.
                // Ordering requirements are used by subsequent executors to satisfy the required
                // ordering for `AggregateMode::FinalPartitioned`/`AggregateMode::Final` modes.
                if matches!(mode, AggregateMode::Partial) {
                    if let Some(ordering_req) = agg.order_bys() {
                        let ordering_exprs = ordering_req
                            .iter()
                            .map(|item| item.expr.clone())
                            .collect::<Vec<_>>();
                        result.extend(ordering_exprs);
                    }
                }
                result
            })
            .collect()),
        // in this mode, we build the merge expressions of the aggregation
        AggregateMode::Final | AggregateMode::FinalPartitioned => {
            let mut col_idx_base = col_idx_base;
            Ok(aggr_expr
                .iter()
                .map(|agg| {
                    let exprs = merge_expressions(col_idx_base, agg)?;
                    col_idx_base += exprs.len();
                    Ok(exprs)
                })
                .collect::<Result<Vec<_>>>()?)
        }
    }
}

/// uses `state_fields` to build a vec of physical column expressions required to merge the
/// AggregateExpr' accumulator's state.
///
/// `index_base` is the starting physical column index for the next expanded state field.
fn merge_expressions(
    index_base: usize,
    expr: &Arc<dyn AggregateExpr>,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    Ok(expr
        .state_fields()?
        .iter()
        .enumerate()
        .map(|(idx, f)| {
            Arc::new(Column::new(f.name(), index_base + idx)) as Arc<dyn PhysicalExpr>
        })
        .collect::<Vec<_>>())
}

pub(crate) type AccumulatorItem = Box<dyn Accumulator>;

fn create_accumulators(
    aggr_expr: &[Arc<dyn AggregateExpr>],
) -> Result<Vec<AccumulatorItem>> {
    aggr_expr
        .iter()
        .map(|expr| expr.create_accumulator())
        .collect::<Result<Vec<_>>>()
}

/// returns a vector of ArrayRefs, where each entry corresponds to either the
/// final value (mode = Final, FinalPartitioned and Single) or states (mode = Partial)
fn finalize_aggregation(
    accumulators: &[AccumulatorItem],
    mode: &AggregateMode,
) -> Result<Vec<ArrayRef>> {
    match mode {
        AggregateMode::Partial => {
            // build the vector of states
            let a = accumulators
                .iter()
                .map(|accumulator| accumulator.state())
                .map(|value| {
                    value.map(|e| {
                        e.iter().map(|v| v.to_array()).collect::<Vec<ArrayRef>>()
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(a.iter().flatten().cloned().collect::<Vec<_>>())
        }
        AggregateMode::Final
        | AggregateMode::FinalPartitioned
        | AggregateMode::Single
        | AggregateMode::SinglePartitioned => {
            // merge the state to the final value
            accumulators
                .iter()
                .map(|accumulator| accumulator.evaluate().map(|v| v.to_array()))
                .collect::<Result<Vec<ArrayRef>>>()
        }
    }
}

/// Evaluates expressions against a record batch.
fn evaluate(
    expr: &[Arc<dyn PhysicalExpr>],
    batch: &RecordBatch,
) -> Result<Vec<ArrayRef>> {
    expr.iter()
        .map(|expr| expr.evaluate(batch))
        .map(|r| r.map(|v| v.into_array(batch.num_rows())))
        .collect::<Result<Vec<_>>>()
}

/// Evaluates expressions against a record batch.
pub(crate) fn evaluate_many(
    expr: &[Vec<Arc<dyn PhysicalExpr>>],
    batch: &RecordBatch,
) -> Result<Vec<Vec<ArrayRef>>> {
    expr.iter()
        .map(|expr| evaluate(expr, batch))
        .collect::<Result<Vec<_>>>()
}

fn evaluate_optional(
    expr: &[Option<Arc<dyn PhysicalExpr>>],
    batch: &RecordBatch,
) -> Result<Vec<Option<ArrayRef>>> {
    expr.iter()
        .map(|expr| {
            expr.as_ref()
                .map(|expr| expr.evaluate(batch))
                .transpose()
                .map(|r| r.map(|v| v.into_array(batch.num_rows())))
        })
        .collect::<Result<Vec<_>>>()
}

/// Evaluate a group by expression against a `RecordBatch`
///
/// Arguments:
/// `group_by`: the expression to evaluate
/// `batch`: the `RecordBatch` to evaluate against
///
/// Returns: A Vec of Vecs of Array of results
/// The outer Vect appears to be for grouping sets
/// The inner Vect contains the results per expression
/// The inner-inner Array contains the results per row
pub(crate) fn evaluate_group_by(
    group_by: &PhysicalGroupBy,
    batch: &RecordBatch,
) -> Result<Vec<Vec<ArrayRef>>> {
    let exprs: Vec<ArrayRef> = group_by
        .expr
        .iter()
        .map(|(expr, _)| {
            let value = expr.evaluate(batch)?;
            Ok(value.into_array(batch.num_rows()))
        })
        .collect::<Result<Vec<_>>>()?;

    let null_exprs: Vec<ArrayRef> = group_by
        .null_expr
        .iter()
        .map(|(expr, _)| {
            let value = expr.evaluate(batch)?;
            Ok(value.into_array(batch.num_rows()))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(group_by
        .groups
        .iter()
        .map(|group| {
            group
                .iter()
                .enumerate()
                .map(|(idx, is_null)| {
                    if *is_null {
                        null_exprs[idx].clone()
                    } else {
                        exprs[idx].clone()
                    }
                })
                .collect()
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregates::{
        get_finest_requirement, AggregateExec, AggregateMode, PhysicalGroupBy,
    };
    use crate::coalesce_batches::CoalesceBatchesExec;
    use crate::coalesce_partitions::CoalescePartitionsExec;
    use crate::common;
    use crate::expressions::{col, Avg};
    use crate::memory::MemoryExec;
    use crate::test::assert_is_pending;
    use crate::test::exec::{assert_strong_count_converges_to_zero, BlockingExec};
    use crate::{
        DisplayAs, ExecutionPlan, Partitioning, RecordBatchStream,
        SendableRecordBatchStream, Statistics,
    };

    use arrow::array::{Float64Array, UInt32Array};
    use arrow::compute::{concat_batches, SortOptions};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use datafusion_common::{
        assert_batches_eq, assert_batches_sorted_eq, internal_err, DataFusionError,
        Result, ScalarValue,
    };
    use datafusion_execution::runtime_env::{RuntimeConfig, RuntimeEnv};
    use datafusion_physical_expr::expressions::{
        lit, ApproxDistinct, Count, FirstValue, LastValue, Median,
    };
    use datafusion_physical_expr::{
        AggregateExpr, PhysicalExpr, PhysicalSortExpr, SchemaProperties,
    };

    use std::any::Any;
    use std::sync::Arc;
    use std::task::{Context, Poll};

    use datafusion_execution::config::SessionConfig;
    use futures::{FutureExt, Stream};

    // Generate a schema which consists of 5 columns (a, b, c, d, e)
    fn create_test_schema() -> Result<SchemaRef> {
        let a = Field::new("a", DataType::Int32, true);
        let b = Field::new("b", DataType::Int32, true);
        let c = Field::new("c", DataType::Int32, true);
        let d = Field::new("d", DataType::Int32, true);
        let e = Field::new("e", DataType::Int32, true);
        let schema = Arc::new(Schema::new(vec![a, b, c, d, e]));

        Ok(schema)
    }

    /// some mock data to aggregates
    fn some_data() -> (Arc<Schema>, Vec<RecordBatch>) {
        // define a schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, false),
            Field::new("b", DataType::Float64, false),
        ]));

        // define data.
        (
            schema.clone(),
            vec![
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(UInt32Array::from(vec![2, 3, 4, 4])),
                        Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
                    ],
                )
                .unwrap(),
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(UInt32Array::from(vec![2, 3, 3, 4])),
                        Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
                    ],
                )
                .unwrap(),
            ],
        )
    }

    /// Generates some mock data for aggregate tests.
    fn some_data_v2() -> (Arc<Schema>, Vec<RecordBatch>) {
        // Define a schema:
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, false),
            Field::new("b", DataType::Float64, false),
        ]));

        // Generate data so that first and last value results are at 2nd and
        // 3rd partitions.  With this construction, we guarantee we don't receive
        // the expected result by accident, but merging actually works properly;
        // i.e. it doesn't depend on the data insertion order.
        (
            schema.clone(),
            vec![
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(UInt32Array::from(vec![2, 3, 4, 4])),
                        Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
                    ],
                )
                .unwrap(),
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(UInt32Array::from(vec![2, 3, 3, 4])),
                        Arc::new(Float64Array::from(vec![0.0, 1.0, 2.0, 3.0])),
                    ],
                )
                .unwrap(),
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(UInt32Array::from(vec![2, 3, 3, 4])),
                        Arc::new(Float64Array::from(vec![3.0, 4.0, 5.0, 6.0])),
                    ],
                )
                .unwrap(),
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(UInt32Array::from(vec![2, 3, 3, 4])),
                        Arc::new(Float64Array::from(vec![2.0, 3.0, 4.0, 5.0])),
                    ],
                )
                .unwrap(),
            ],
        )
    }

    fn new_spill_ctx(batch_size: usize, max_memory: usize) -> Arc<TaskContext> {
        let session_config = SessionConfig::new().with_batch_size(batch_size);
        let runtime = Arc::new(
            RuntimeEnv::new(RuntimeConfig::default().with_memory_limit(max_memory, 1.0))
                .unwrap(),
        );
        let task_ctx = TaskContext::default()
            .with_session_config(session_config)
            .with_runtime(runtime);
        Arc::new(task_ctx)
    }

    async fn check_grouping_sets(
        input: Arc<dyn ExecutionPlan>,
        spill: bool,
    ) -> Result<()> {
        let input_schema = input.schema();

        let grouping_set = PhysicalGroupBy {
            expr: vec![
                (col("a", &input_schema)?, "a".to_string()),
                (col("b", &input_schema)?, "b".to_string()),
            ],
            null_expr: vec![
                (lit(ScalarValue::UInt32(None)), "a".to_string()),
                (lit(ScalarValue::Float64(None)), "b".to_string()),
            ],
            groups: vec![
                vec![false, true],  // (a, NULL)
                vec![true, false],  // (NULL, b)
                vec![false, false], // (a,b)
            ],
        };

        let aggregates: Vec<Arc<dyn AggregateExpr>> = vec![Arc::new(Count::new(
            lit(1i8),
            "COUNT(1)".to_string(),
            DataType::Int64,
        ))];

        let task_ctx = if spill {
            new_spill_ctx(4, 1000)
        } else {
            Arc::new(TaskContext::default())
        };

        let partial_aggregate = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            grouping_set.clone(),
            aggregates.clone(),
            vec![None],
            vec![None],
            input,
            input_schema.clone(),
        )?);

        let result =
            common::collect(partial_aggregate.execute(0, task_ctx.clone())?).await?;

        let expected = if spill {
            vec![
                "+---+-----+-----------------+",
                "| a | b   | COUNT(1)[count] |",
                "+---+-----+-----------------+",
                "|   | 1.0 | 1               |",
                "|   | 1.0 | 1               |",
                "|   | 2.0 | 1               |",
                "|   | 2.0 | 1               |",
                "|   | 3.0 | 1               |",
                "|   | 3.0 | 1               |",
                "|   | 4.0 | 1               |",
                "|   | 4.0 | 1               |",
                "| 2 |     | 1               |",
                "| 2 |     | 1               |",
                "| 2 | 1.0 | 1               |",
                "| 2 | 1.0 | 1               |",
                "| 3 |     | 1               |",
                "| 3 |     | 2               |",
                "| 3 | 2.0 | 2               |",
                "| 3 | 3.0 | 1               |",
                "| 4 |     | 1               |",
                "| 4 |     | 2               |",
                "| 4 | 3.0 | 1               |",
                "| 4 | 4.0 | 2               |",
                "+---+-----+-----------------+",
            ]
        } else {
            vec![
                "+---+-----+-----------------+",
                "| a | b   | COUNT(1)[count] |",
                "+---+-----+-----------------+",
                "|   | 1.0 | 2               |",
                "|   | 2.0 | 2               |",
                "|   | 3.0 | 2               |",
                "|   | 4.0 | 2               |",
                "| 2 |     | 2               |",
                "| 2 | 1.0 | 2               |",
                "| 3 |     | 3               |",
                "| 3 | 2.0 | 2               |",
                "| 3 | 3.0 | 1               |",
                "| 4 |     | 3               |",
                "| 4 | 3.0 | 1               |",
                "| 4 | 4.0 | 2               |",
                "+---+-----+-----------------+",
            ]
        };
        assert_batches_sorted_eq!(expected, &result);

        let groups = partial_aggregate.group_expr().expr().to_vec();

        let merge = Arc::new(CoalescePartitionsExec::new(partial_aggregate));

        let final_group: Vec<(Arc<dyn PhysicalExpr>, String)> = groups
            .iter()
            .map(|(_expr, name)| Ok((col(name, &input_schema)?, name.clone())))
            .collect::<Result<_>>()?;

        let final_grouping_set = PhysicalGroupBy::new_single(final_group);

        let task_ctx = if spill {
            new_spill_ctx(4, 3160)
        } else {
            task_ctx
        };

        let merged_aggregate = Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            final_grouping_set,
            aggregates,
            vec![None],
            vec![None],
            merge,
            input_schema,
        )?);

        let result =
            common::collect(merged_aggregate.execute(0, task_ctx.clone())?).await?;
        let batch = concat_batches(&result[0].schema(), &result)?;
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.num_rows(), 12);

        let expected = vec![
            "+---+-----+----------+",
            "| a | b   | COUNT(1) |",
            "+---+-----+----------+",
            "|   | 1.0 | 2        |",
            "|   | 2.0 | 2        |",
            "|   | 3.0 | 2        |",
            "|   | 4.0 | 2        |",
            "| 2 |     | 2        |",
            "| 2 | 1.0 | 2        |",
            "| 3 |     | 3        |",
            "| 3 | 2.0 | 2        |",
            "| 3 | 3.0 | 1        |",
            "| 4 |     | 3        |",
            "| 4 | 3.0 | 1        |",
            "| 4 | 4.0 | 2        |",
            "+---+-----+----------+",
        ];

        assert_batches_sorted_eq!(&expected, &result);

        let metrics = merged_aggregate.metrics().unwrap();
        let output_rows = metrics.output_rows().unwrap();
        assert_eq!(12, output_rows);

        Ok(())
    }

    /// build the aggregates on the data from some_data() and check the results
    async fn check_aggregates(input: Arc<dyn ExecutionPlan>, spill: bool) -> Result<()> {
        let input_schema = input.schema();

        let grouping_set = PhysicalGroupBy {
            expr: vec![(col("a", &input_schema)?, "a".to_string())],
            null_expr: vec![],
            groups: vec![vec![false]],
        };

        let aggregates: Vec<Arc<dyn AggregateExpr>> = vec![Arc::new(Avg::new(
            col("b", &input_schema)?,
            "AVG(b)".to_string(),
            DataType::Float64,
        ))];

        let task_ctx = if spill {
            new_spill_ctx(2, 1500)
        } else {
            Arc::new(TaskContext::default())
        };

        let partial_aggregate = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            grouping_set.clone(),
            aggregates.clone(),
            vec![None],
            vec![None],
            input,
            input_schema.clone(),
        )?);

        let result =
            common::collect(partial_aggregate.execute(0, task_ctx.clone())?).await?;

        let expected = if spill {
            vec![
                "+---+---------------+-------------+",
                "| a | AVG(b)[count] | AVG(b)[sum] |",
                "+---+---------------+-------------+",
                "| 2 | 1             | 1.0         |",
                "| 2 | 1             | 1.0         |",
                "| 3 | 1             | 2.0         |",
                "| 3 | 2             | 5.0         |",
                "| 4 | 3             | 11.0        |",
                "+---+---------------+-------------+",
            ]
        } else {
            vec![
                "+---+---------------+-------------+",
                "| a | AVG(b)[count] | AVG(b)[sum] |",
                "+---+---------------+-------------+",
                "| 2 | 2             | 2.0         |",
                "| 3 | 3             | 7.0         |",
                "| 4 | 3             | 11.0        |",
                "+---+---------------+-------------+",
            ]
        };
        assert_batches_sorted_eq!(expected, &result);

        let merge = Arc::new(CoalescePartitionsExec::new(partial_aggregate));

        let final_group: Vec<(Arc<dyn PhysicalExpr>, String)> = grouping_set
            .expr
            .iter()
            .map(|(_expr, name)| Ok((col(name, &input_schema)?, name.clone())))
            .collect::<Result<_>>()?;

        let final_grouping_set = PhysicalGroupBy::new_single(final_group);

        let merged_aggregate = Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            final_grouping_set,
            aggregates,
            vec![None],
            vec![None],
            merge,
            input_schema,
        )?);

        let result =
            common::collect(merged_aggregate.execute(0, task_ctx.clone())?).await?;
        let batch = concat_batches(&result[0].schema(), &result)?;
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 3);

        let expected = vec![
            "+---+--------------------+",
            "| a | AVG(b)             |",
            "+---+--------------------+",
            "| 2 | 1.0                |",
            "| 3 | 2.3333333333333335 |", // 3, (2 + 3 + 2) / 3
            "| 4 | 3.6666666666666665 |", // 4, (3 + 4 + 4) / 3
            "+---+--------------------+",
        ];

        assert_batches_sorted_eq!(&expected, &result);

        let metrics = merged_aggregate.metrics().unwrap();
        let output_rows = metrics.output_rows().unwrap();
        if spill {
            // When spilling, the output rows metrics become partial output size + final output size
            // This is because final aggregation starts while partial aggregation is still emitting
            assert_eq!(8, output_rows);
        } else {
            assert_eq!(3, output_rows);
        }

        Ok(())
    }

    /// Define a test source that can yield back to runtime before returning its first item ///

    #[derive(Debug)]
    struct TestYieldingExec {
        /// True if this exec should yield back to runtime the first time it is polled
        pub yield_first: bool,
    }

    impl DisplayAs for TestYieldingExec {
        fn fmt_as(
            &self,
            t: DisplayFormatType,
            f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            match t {
                DisplayFormatType::Default | DisplayFormatType::Verbose => {
                    write!(f, "TestYieldingExec")
                }
            }
        }
    }

    impl ExecutionPlan for TestYieldingExec {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn schema(&self) -> SchemaRef {
            some_data().0
        }

        fn output_partitioning(&self) -> Partitioning {
            Partitioning::UnknownPartitioning(1)
        }

        fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
            None
        }

        fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            internal_err!("Children cannot be replaced in {self:?}")
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            let stream = if self.yield_first {
                TestYieldingStream::New
            } else {
                TestYieldingStream::Yielded
            };

            Ok(Box::pin(stream))
        }

        fn statistics(&self) -> Statistics {
            let (_, batches) = some_data();
            common::compute_record_batch_statistics(&[batches], &self.schema(), None)
        }
    }

    /// A stream using the demo data. If inited as new, it will first yield to runtime before returning records
    enum TestYieldingStream {
        New,
        Yielded,
        ReturnedBatch1,
        ReturnedBatch2,
    }

    impl Stream for TestYieldingStream {
        type Item = Result<RecordBatch>;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            match &*self {
                TestYieldingStream::New => {
                    *(self.as_mut()) = TestYieldingStream::Yielded;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                TestYieldingStream::Yielded => {
                    *(self.as_mut()) = TestYieldingStream::ReturnedBatch1;
                    Poll::Ready(Some(Ok(some_data().1[0].clone())))
                }
                TestYieldingStream::ReturnedBatch1 => {
                    *(self.as_mut()) = TestYieldingStream::ReturnedBatch2;
                    Poll::Ready(Some(Ok(some_data().1[1].clone())))
                }
                TestYieldingStream::ReturnedBatch2 => Poll::Ready(None),
            }
        }
    }

    impl RecordBatchStream for TestYieldingStream {
        fn schema(&self) -> SchemaRef {
            some_data().0
        }
    }

    //--- Tests ---//

    #[tokio::test]
    async fn aggregate_source_not_yielding() -> Result<()> {
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(TestYieldingExec { yield_first: false });

        check_aggregates(input, false).await
    }

    #[tokio::test]
    async fn aggregate_grouping_sets_source_not_yielding() -> Result<()> {
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(TestYieldingExec { yield_first: false });

        check_grouping_sets(input, false).await
    }

    #[tokio::test]
    async fn aggregate_source_with_yielding() -> Result<()> {
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(TestYieldingExec { yield_first: true });

        check_aggregates(input, false).await
    }

    #[tokio::test]
    async fn aggregate_grouping_sets_with_yielding() -> Result<()> {
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(TestYieldingExec { yield_first: true });

        check_grouping_sets(input, false).await
    }

    #[tokio::test]
    async fn aggregate_source_not_yielding_with_spill() -> Result<()> {
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(TestYieldingExec { yield_first: false });

        check_aggregates(input, true).await
    }

    #[tokio::test]
    async fn aggregate_grouping_sets_source_not_yielding_with_spill() -> Result<()> {
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(TestYieldingExec { yield_first: false });

        check_grouping_sets(input, true).await
    }

    #[tokio::test]
    #[ignore]
    async fn aggregate_source_with_yielding_with_spill() -> Result<()> {
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(TestYieldingExec { yield_first: true });

        check_aggregates(input, true).await
    }

    #[tokio::test]
    async fn aggregate_grouping_sets_with_yielding_with_spill() -> Result<()> {
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(TestYieldingExec { yield_first: true });

        check_grouping_sets(input, true).await
    }

    #[tokio::test]
    async fn test_oom() -> Result<()> {
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(TestYieldingExec { yield_first: true });
        let input_schema = input.schema();

        let runtime = Arc::new(
            RuntimeEnv::new(RuntimeConfig::default().with_memory_limit(1, 1.0)).unwrap(),
        );
        let task_ctx = TaskContext::default().with_runtime(runtime);
        let task_ctx = Arc::new(task_ctx);

        let groups_none = PhysicalGroupBy::default();
        let groups_some = PhysicalGroupBy {
            expr: vec![(col("a", &input_schema)?, "a".to_string())],
            null_expr: vec![],
            groups: vec![vec![false]],
        };

        // something that allocates within the aggregator
        let aggregates_v0: Vec<Arc<dyn AggregateExpr>> = vec![Arc::new(Median::new(
            col("a", &input_schema)?,
            "MEDIAN(a)".to_string(),
            DataType::UInt32,
        ))];

        // use slow-path in `hash.rs`
        let aggregates_v1: Vec<Arc<dyn AggregateExpr>> =
            vec![Arc::new(ApproxDistinct::new(
                col("a", &input_schema)?,
                "APPROX_DISTINCT(a)".to_string(),
                DataType::UInt32,
            ))];

        // use fast-path in `row_hash.rs`.
        let aggregates_v2: Vec<Arc<dyn AggregateExpr>> = vec![Arc::new(Avg::new(
            col("b", &input_schema)?,
            "AVG(b)".to_string(),
            DataType::Float64,
        ))];

        for (version, groups, aggregates) in [
            (0, groups_none, aggregates_v0),
            (1, groups_some.clone(), aggregates_v1),
            (2, groups_some, aggregates_v2),
        ] {
            let partial_aggregate = Arc::new(AggregateExec::try_new(
                AggregateMode::Partial,
                groups,
                aggregates,
                vec![None; 3],
                vec![None; 3],
                input.clone(),
                input_schema.clone(),
            )?);

            let stream = partial_aggregate.execute_typed(0, task_ctx.clone())?;

            // ensure that we really got the version we wanted
            match version {
                0 => {
                    assert!(matches!(stream, StreamType::AggregateStream(_)));
                }
                1 => {
                    assert!(matches!(stream, StreamType::GroupedHash(_)));
                }
                2 => {
                    assert!(matches!(stream, StreamType::GroupedHash(_)));
                }
                _ => panic!("Unknown version: {version}"),
            }

            let stream: SendableRecordBatchStream = stream.into();
            let err = common::collect(stream).await.unwrap_err();

            // error root cause traversal is a bit complicated, see #4172.
            let err = err.find_root();
            assert!(
                matches!(err, DataFusionError::ResourcesExhausted(_)),
                "Wrong error type: {err}",
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_cancel_without_groups() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));

        let groups = PhysicalGroupBy::default();

        let aggregates: Vec<Arc<dyn AggregateExpr>> = vec![Arc::new(Avg::new(
            col("a", &schema)?,
            "AVG(a)".to_string(),
            DataType::Float64,
        ))];

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 1));
        let refs = blocking_exec.refs();
        let aggregate_exec = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            groups.clone(),
            aggregates.clone(),
            vec![None],
            vec![None],
            blocking_exec,
            schema,
        )?);

        let fut = crate::collect(aggregate_exec, task_ctx);
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_cancel_with_groups() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Float32, true),
            Field::new("b", DataType::Float32, true),
        ]));

        let groups =
            PhysicalGroupBy::new_single(vec![(col("a", &schema)?, "a".to_string())]);

        let aggregates: Vec<Arc<dyn AggregateExpr>> = vec![Arc::new(Avg::new(
            col("b", &schema)?,
            "AVG(b)".to_string(),
            DataType::Float64,
        ))];

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 1));
        let refs = blocking_exec.refs();
        let aggregate_exec = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            groups,
            aggregates.clone(),
            vec![None],
            vec![None],
            blocking_exec,
            schema,
        )?);

        let fut = crate::collect(aggregate_exec, task_ctx);
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        Ok(())
    }

    #[tokio::test]
    async fn run_first_last_multi_partitions() -> Result<()> {
        for use_coalesce_batches in [false, true] {
            for is_first_acc in [false, true] {
                for spill in [false, true] {
                    first_last_multi_partitions(use_coalesce_batches, is_first_acc, spill)
                        .await?
                }
            }
        }
        Ok(())
    }

    // This function either constructs the physical plan below,
    //
    // "AggregateExec: mode=Final, gby=[a@0 as a], aggr=[FIRST_VALUE(b)]",
    // "  CoalesceBatchesExec: target_batch_size=1024",
    // "    CoalescePartitionsExec",
    // "      AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[FIRST_VALUE(b)], ordering_mode=None",
    // "        MemoryExec: partitions=4, partition_sizes=[1, 1, 1, 1]",
    //
    // or
    //
    // "AggregateExec: mode=Final, gby=[a@0 as a], aggr=[FIRST_VALUE(b)]",
    // "  CoalescePartitionsExec",
    // "    AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[FIRST_VALUE(b)], ordering_mode=None",
    // "      MemoryExec: partitions=4, partition_sizes=[1, 1, 1, 1]",
    //
    // and checks whether the function `merge_batch` works correctly for
    // FIRST_VALUE and LAST_VALUE functions.
    async fn first_last_multi_partitions(
        use_coalesce_batches: bool,
        is_first_acc: bool,
        spill: bool,
    ) -> Result<()> {
        let task_ctx = if spill {
            new_spill_ctx(2, 2812)
        } else {
            Arc::new(TaskContext::default())
        };

        let (schema, data) = some_data_v2();
        let partition1 = data[0].clone();
        let partition2 = data[1].clone();
        let partition3 = data[2].clone();
        let partition4 = data[3].clone();

        let groups =
            PhysicalGroupBy::new_single(vec![(col("a", &schema)?, "a".to_string())]);

        let ordering_req = vec![PhysicalSortExpr {
            expr: col("b", &schema)?,
            options: SortOptions::default(),
        }];
        let aggregates: Vec<Arc<dyn AggregateExpr>> = if is_first_acc {
            vec![Arc::new(FirstValue::new(
                col("b", &schema)?,
                "FIRST_VALUE(b)".to_string(),
                DataType::Float64,
                ordering_req.clone(),
                vec![DataType::Float64],
            ))]
        } else {
            vec![Arc::new(LastValue::new(
                col("b", &schema)?,
                "LAST_VALUE(b)".to_string(),
                DataType::Float64,
                ordering_req.clone(),
                vec![DataType::Float64],
            ))]
        };

        let memory_exec = Arc::new(MemoryExec::try_new(
            &[
                vec![partition1],
                vec![partition2],
                vec![partition3],
                vec![partition4],
            ],
            schema.clone(),
            None,
        )?);
        let aggregate_exec = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            groups.clone(),
            aggregates.clone(),
            vec![None],
            vec![Some(ordering_req.clone())],
            memory_exec,
            schema.clone(),
        )?);
        let coalesce = if use_coalesce_batches {
            let coalesce = Arc::new(CoalescePartitionsExec::new(aggregate_exec));
            Arc::new(CoalesceBatchesExec::new(coalesce, 1024)) as Arc<dyn ExecutionPlan>
        } else {
            Arc::new(CoalescePartitionsExec::new(aggregate_exec))
                as Arc<dyn ExecutionPlan>
        };
        let aggregate_final = Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            groups,
            aggregates.clone(),
            vec![None],
            vec![Some(ordering_req)],
            coalesce,
            schema,
        )?) as Arc<dyn ExecutionPlan>;

        let result = crate::collect(aggregate_final, task_ctx).await?;
        if is_first_acc {
            let expected = [
                "+---+----------------+",
                "| a | FIRST_VALUE(b) |",
                "+---+----------------+",
                "| 2 | 0.0            |",
                "| 3 | 1.0            |",
                "| 4 | 3.0            |",
                "+---+----------------+",
            ];
            assert_batches_eq!(expected, &result);
        } else {
            let expected = [
                "+---+---------------+",
                "| a | LAST_VALUE(b) |",
                "+---+---------------+",
                "| 2 | 3.0           |",
                "| 3 | 5.0           |",
                "| 4 | 6.0           |",
                "+---+---------------+",
            ];
            assert_batches_eq!(expected, &result);
        };
        Ok(())
    }

    #[tokio::test]
    async fn test_get_finest_requirements() -> Result<()> {
        let test_schema = create_test_schema()?;
        // Assume column a and b are aliases
        // Assume also that a ASC and c DESC describe the same global ordering for the table. (Since they are ordering equivalent).
        let options1 = SortOptions {
            descending: false,
            nulls_first: false,
        };
        // This is the reverse requirement of options1
        let options2 = SortOptions {
            descending: true,
            nulls_first: true,
        };
        let col_a = &col("a", &test_schema)?;
        let col_b = &col("b", &test_schema)?;
        let col_c = &col("c", &test_schema)?;
        let mut schema_properties = SchemaProperties::new(test_schema);
        // Columns a and b are equal.
        schema_properties.add_equal_conditions((col_a, col_b));
        // Aggregate requirements are
        // [None], [a ASC], [a ASC, b ASC, c ASC], [a ASC, b ASC] respectively
        let mut order_by_exprs = vec![
            None,
            Some(vec![PhysicalSortExpr {
                expr: col_a.clone(),
                options: options1,
            }]),
            Some(vec![
                PhysicalSortExpr {
                    expr: col_a.clone(),
                    options: options1,
                },
                PhysicalSortExpr {
                    expr: col_b.clone(),
                    options: options1,
                },
                PhysicalSortExpr {
                    expr: col_c.clone(),
                    options: options1,
                },
            ]),
            Some(vec![
                PhysicalSortExpr {
                    expr: col_a.clone(),
                    options: options1,
                },
                PhysicalSortExpr {
                    expr: col_b.clone(),
                    options: options1,
                },
            ]),
            // Since aggregate expression is reversible (FirstValue), we should be able to resolve below
            // contradictory requirement by reversing it.
            Some(vec![PhysicalSortExpr {
                expr: col_b.clone(),
                options: options2,
            }]),
        ];
        let common_requirement = Some(vec![
            PhysicalSortExpr {
                expr: col_a.clone(),
                options: options1,
            },
            PhysicalSortExpr {
                expr: col_c.clone(),
                options: options1,
            },
        ]);
        let aggr_expr = Arc::new(FirstValue::new(
            col_a.clone(),
            "first1",
            DataType::Int32,
            vec![],
            vec![],
        )) as _;
        let mut aggr_exprs = vec![aggr_expr; order_by_exprs.len()];
        let res = get_finest_requirement(
            &mut aggr_exprs,
            &mut order_by_exprs,
            &schema_properties,
        )?;
        assert_eq!(res, common_requirement);
        Ok(())
    }
}
