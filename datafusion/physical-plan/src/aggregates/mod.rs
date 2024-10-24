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

use std::any::Any;
use std::sync::Arc;

use super::{DisplayAs, ExecutionMode, ExecutionPlanProperties, PlanProperties};
use crate::aggregates::{
    no_grouping::AggregateStream, row_hash::GroupedHashAggregateStream,
    topk_stream::GroupedTopKAggregateStream,
};
use crate::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::projection::get_field_metadata;
use crate::windows::get_ordered_partition_by_indices;
use crate::{
    DisplayFormatType, Distribution, ExecutionPlan, InputOrderMode,
    SendableRecordBatchStream, Statistics,
};

use arrow::array::ArrayRef;
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_array::{UInt16Array, UInt32Array, UInt64Array, UInt8Array};
use datafusion_common::stats::Precision;
use datafusion_common::{internal_err, not_impl_err, Result};
use datafusion_execution::TaskContext;
use datafusion_expr::{Accumulator, Aggregate};
use datafusion_physical_expr::{
    equivalence::{collapse_lex_req, ProjectionMapping},
    expressions::Column,
    physical_exprs_contains, EquivalenceProperties, LexOrdering, LexRequirement,
    PhysicalExpr, PhysicalSortRequirement,
};

use crate::execution_plan::CardinalityEffect;
use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
use itertools::Itertools;

pub mod group_values;
mod no_grouping;
pub mod order;
mod row_hash;
mod topk;
mod topk_stream;

/// Hash aggregate modes
///
/// See [`Accumulator::state`] for background information on multi-phase
/// aggregation and how these modes are used.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AggregateMode {
    /// Partial aggregate that can be applied in parallel across input
    /// partitions.
    ///
    /// This is the first phase of a multi-phase aggregation.
    Partial,
    /// Final aggregate that produces a single partition of output by combining
    /// the output of multiple partial aggregates.
    ///
    /// This is the second phase of a multi-phase aggregation.
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
    ///
    /// This mode requires that the input is a single partition (like Final)
    Single,
    /// Applies the entire logical aggregation operation in a single operator,
    /// as opposed to Partial / Final modes which apply the logical aggregation using
    /// two operators.
    ///
    /// This mode requires that the input is partitioned by group key (like
    /// FinalPartitioned)
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

/// Represents `GROUP BY` clause in the plan (including the more general GROUPING SET)
/// In the case of a simple `GROUP BY a, b` clause, this will contain the expression [a, b]
/// and a single group [false, false].
/// In the case of `GROUP BY GROUPING SETS/CUBE/ROLLUP` the planner will expand the expression
/// into multiple groups, using null expressions to align each group.
/// For example, with a group by clause `GROUP BY GROUPING SETS ((a,b),(a),(b))` the planner should
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
    /// expression in null_expr. If `groups[i][j]` is true, then the
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

    /// Calculate GROUP BY expressions nullable
    pub fn exprs_nullable(&self) -> Vec<bool> {
        let mut exprs_nullable = vec![false; self.expr.len()];
        for group in self.groups.iter() {
            group.iter().enumerate().for_each(|(index, is_null)| {
                if *is_null {
                    exprs_nullable[index] = true;
                }
            })
        }
        exprs_nullable
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

    /// Calculate GROUP BY expressions according to input schema.
    pub fn input_exprs(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.expr
            .iter()
            .map(|(expr, _alias)| Arc::clone(expr))
            .collect()
    }

    /// The number of expressions in the output schema.
    fn num_output_exprs(&self) -> usize {
        let mut num_exprs = self.expr.len();
        if !self.is_single() {
            num_exprs += 1
        }
        num_exprs
    }

    /// Return grouping expressions as they occur in the output schema.
    pub fn output_exprs(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        let num_output_exprs = self.num_output_exprs();
        let mut output_exprs = Vec::with_capacity(num_output_exprs);
        output_exprs.extend(
            self.expr
                .iter()
                .enumerate()
                .take(num_output_exprs)
                .map(|(index, (_, name))| Arc::new(Column::new(name, index)) as _),
        );
        if !self.is_single() {
            output_exprs.push(Arc::new(Column::new(
                Aggregate::INTERNAL_GROUPING_ID,
                self.expr.len(),
            )) as _);
        }
        output_exprs
    }

    /// Returns the number expression as grouping keys.
    fn num_group_exprs(&self) -> usize {
        if self.is_single() {
            self.expr.len()
        } else {
            self.expr.len() + 1
        }
    }

    /// Returns the fields that are used as the grouping keys.
    fn group_fields(&self, input_schema: &Schema) -> Result<Vec<Field>> {
        let mut fields = Vec::with_capacity(self.num_group_exprs());
        for ((expr, name), group_expr_nullable) in
            self.expr.iter().zip(self.exprs_nullable().into_iter())
        {
            fields.push(
                Field::new(
                    name,
                    expr.data_type(input_schema)?,
                    group_expr_nullable || expr.nullable(input_schema)?,
                )
                .with_metadata(
                    get_field_metadata(expr, input_schema).unwrap_or_default(),
                ),
            );
        }
        if !self.is_single() {
            fields.push(Field::new(
                Aggregate::INTERNAL_GROUPING_ID,
                Aggregate::grouping_id_type(self.expr.len()),
                false,
            ));
        }
        Ok(fields)
    }

    /// Returns the output fields of the group by.
    ///
    /// This might be different from the `group_fields` that might contain internal expressions that
    /// should not be part of the output schema.
    fn output_fields(&self, input_schema: &Schema) -> Result<Vec<Field>> {
        let mut fields = self.group_fields(input_schema)?;
        fields.truncate(self.num_output_exprs());
        Ok(fields)
    }

    /// Returns the `PhysicalGroupBy` for a final aggregation if `self` is used for a partial
    /// aggregation.
    pub fn as_final(&self) -> PhysicalGroupBy {
        let expr: Vec<_> =
            self.output_exprs()
                .into_iter()
                .zip(
                    self.expr.iter().map(|t| t.1.clone()).chain(std::iter::once(
                        Aggregate::INTERNAL_GROUPING_ID.to_owned(),
                    )),
                )
                .collect();
        let num_exprs = expr.len();
        Self {
            expr,
            null_expr: vec![],
            groups: vec![vec![false; num_exprs]],
        }
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
    aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
    /// FILTER (WHERE clause) expression for each aggregate expression
    filter_expr: Vec<Option<Arc<dyn PhysicalExpr>>>,
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
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    required_input_ordering: Option<LexRequirement>,
    /// Describes how the input is ordered relative to the group by columns
    input_order_mode: InputOrderMode,
    cache: PlanProperties,
}

impl AggregateExec {
    /// Function used in `OptimizeAggregateOrder` optimizer rule,
    /// where we need parts of the new value, others cloned from the old one
    /// Rewrites aggregate exec with new aggregate expressions.
    pub fn with_new_aggr_exprs(
        &self,
        aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
    ) -> Self {
        Self {
            aggr_expr,
            // clone the rest of the fields
            required_input_ordering: self.required_input_ordering.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
            input_order_mode: self.input_order_mode.clone(),
            cache: self.cache.clone(),
            mode: self.mode,
            group_by: self.group_by.clone(),
            filter_expr: self.filter_expr.clone(),
            limit: self.limit,
            input: Arc::clone(&self.input),
            schema: Arc::clone(&self.schema),
            input_schema: Arc::clone(&self.input_schema),
        }
    }

    pub fn cache(&self) -> &PlanProperties {
        &self.cache
    }

    /// Create a new hash aggregate execution plan
    pub fn try_new(
        mode: AggregateMode,
        group_by: PhysicalGroupBy,
        aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
        filter_expr: Vec<Option<Arc<dyn PhysicalExpr>>>,
        input: Arc<dyn ExecutionPlan>,
        input_schema: SchemaRef,
    ) -> Result<Self> {
        let schema = create_schema(&input.schema(), &group_by, &aggr_expr, mode)?;

        let schema = Arc::new(schema);
        AggregateExec::try_new_with_schema(
            mode,
            group_by,
            aggr_expr,
            filter_expr,
            input,
            input_schema,
            schema,
        )
    }

    /// Create a new hash aggregate execution plan with the given schema.
    /// This constructor isn't part of the public API, it is used internally
    /// by DataFusion to enforce schema consistency during when re-creating
    /// `AggregateExec`s inside optimization rules. Schema field names of an
    /// `AggregateExec` depends on the names of aggregate expressions. Since
    /// a rule may re-write aggregate expressions (e.g. reverse them) during
    /// initialization, field names may change inadvertently if one re-creates
    /// the schema in such cases.
    #[allow(clippy::too_many_arguments)]
    fn try_new_with_schema(
        mode: AggregateMode,
        group_by: PhysicalGroupBy,
        mut aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
        filter_expr: Vec<Option<Arc<dyn PhysicalExpr>>>,
        input: Arc<dyn ExecutionPlan>,
        input_schema: SchemaRef,
        schema: SchemaRef,
    ) -> Result<Self> {
        // Make sure arguments are consistent in size
        if aggr_expr.len() != filter_expr.len() {
            return internal_err!("Inconsistent aggregate expr: {:?} and filter expr: {:?} for AggregateExec, their size should match", aggr_expr, filter_expr);
        }

        let input_eq_properties = input.equivalence_properties();
        // Get GROUP BY expressions:
        let groupby_exprs = group_by.input_exprs();
        // If existing ordering satisfies a prefix of the GROUP BY expressions,
        // prefix requirements with this section. In this case, aggregation will
        // work more efficiently.
        let indices = get_ordered_partition_by_indices(&groupby_exprs, &input);
        let mut new_requirement = LexRequirement::new(
            indices
                .iter()
                .map(|&idx| PhysicalSortRequirement {
                    expr: Arc::clone(&groupby_exprs[idx]),
                    options: None,
                })
                .collect::<Vec<_>>(),
        );

        let req = get_finer_aggregate_exprs_requirement(
            &mut aggr_expr,
            &group_by,
            input_eq_properties,
            &mode,
        )?;
        new_requirement.inner.extend(req);
        new_requirement = collapse_lex_req(new_requirement);

        // If our aggregation has grouping sets then our base grouping exprs will
        // be expanded based on the flags in `group_by.groups` where for each
        // group we swap the grouping expr for `null` if the flag is `true`
        // That means that each index in `indices` is valid if and only if
        // it is not null in every group
        let indices: Vec<usize> = indices
            .into_iter()
            .filter(|idx| group_by.groups.iter().all(|group| !group[*idx]))
            .collect();

        let input_order_mode = if indices.len() == groupby_exprs.len()
            && !indices.is_empty()
            && group_by.groups.len() == 1
        {
            InputOrderMode::Sorted
        } else if !indices.is_empty() {
            InputOrderMode::PartiallySorted(indices)
        } else {
            InputOrderMode::Linear
        };

        // construct a map from the input expression to the output expression of the Aggregation group by
        let projection_mapping =
            ProjectionMapping::try_new(&group_by.expr, &input.schema())?;

        let required_input_ordering =
            (!new_requirement.is_empty()).then_some(new_requirement);

        let cache = Self::compute_properties(
            &input,
            Arc::clone(&schema),
            &projection_mapping,
            &mode,
            &input_order_mode,
        );

        Ok(AggregateExec {
            mode,
            group_by,
            aggr_expr,
            filter_expr,
            input,
            schema,
            input_schema,
            metrics: ExecutionPlanMetricsSet::new(),
            required_input_ordering,
            limit: None,
            input_order_mode,
            cache,
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
    pub fn aggr_expr(&self) -> &[Arc<AggregateFunctionExpr>] {
        &self.aggr_expr
    }

    /// FILTER (WHERE clause) expression for each aggregate expression
    pub fn filter_expr(&self) -> &[Option<Arc<dyn PhysicalExpr>>] {
        &self.filter_expr
    }

    /// Input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Get the input schema before any aggregates are applied
    pub fn input_schema(&self) -> SchemaRef {
        Arc::clone(&self.input_schema)
    }

    /// number of rows soft limit of the AggregateExec
    pub fn limit(&self) -> Option<usize> {
        self.limit
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
            if !self.is_unordered_unfiltered_group_by_distinct() {
                return Ok(StreamType::GroupedPriorityQueue(
                    GroupedTopKAggregateStream::new(self, context, partition, limit)?,
                ));
            }
        }

        // grouping by something else and we need to just materialize all results
        Ok(StreamType::GroupedHash(GroupedHashAggregateStream::new(
            self, context, partition,
        )?))
    }

    /// Finds the DataType and SortDirection for this Aggregate, if there is one
    pub fn get_minmax_desc(&self) -> Option<(Field, bool)> {
        let agg_expr = self.aggr_expr.iter().exactly_one().ok()?;
        agg_expr.get_minmax_desc()
    }

    /// true, if this Aggregate has a group-by with no required or explicit ordering,
    /// no filtering and no aggregate expressions
    /// This method qualifies the use of the LimitedDistinctAggregation rewrite rule
    /// on an AggregateExec.
    pub fn is_unordered_unfiltered_group_by_distinct(&self) -> bool {
        // ensure there is a group by
        if self.group_expr().is_empty() {
            return false;
        }
        // ensure there are no aggregate expressions
        if !self.aggr_expr().is_empty() {
            return false;
        }
        // ensure there are no filters on aggregate expressions; the above check
        // may preclude this case
        if self.filter_expr().iter().any(|e| e.is_some()) {
            return false;
        }
        // ensure there are no order by expressions
        if self.aggr_expr().iter().any(|e| e.order_bys().is_some()) {
            return false;
        }
        // ensure there is no output ordering; can this rule be relaxed?
        if self.properties().output_ordering().is_some() {
            return false;
        }
        // ensure no ordering is required on the input
        if self.required_input_ordering()[0].is_some() {
            return false;
        }
        true
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    pub fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        projection_mapping: &ProjectionMapping,
        mode: &AggregateMode,
        input_order_mode: &InputOrderMode,
    ) -> PlanProperties {
        // Construct equivalence properties:
        let eq_properties = input
            .equivalence_properties()
            .project(projection_mapping, schema);

        // Get output partitioning:
        let input_partitioning = input.output_partitioning().clone();
        let output_partitioning = if mode.is_first_stage() {
            // First stage aggregation will not change the output partitioning,
            // but needs to respect aliases (e.g. mapping in the GROUP BY
            // expression).
            let input_eq_properties = input.equivalence_properties();
            input_partitioning.project(projection_mapping, input_eq_properties)
        } else {
            input_partitioning.clone()
        };

        // Determine execution mode:
        let mut exec_mode = input.execution_mode();
        if exec_mode == ExecutionMode::Unbounded
            && *input_order_mode == InputOrderMode::Linear
        {
            // Cannot run without breaking the pipeline
            exec_mode = ExecutionMode::PipelineBreaking;
        }

        PlanProperties::new(eq_properties, output_partitioning, exec_mode)
    }

    pub fn input_order_mode(&self) -> &InputOrderMode {
        &self.input_order_mode
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

                if self.input_order_mode != InputOrderMode::Linear {
                    write!(f, ", ordering_mode={:?}", self.input_order_mode)?;
                }
            }
        }
        Ok(())
    }
}

impl ExecutionPlan for AggregateExec {
    fn name(&self) -> &'static str {
        "AggregateExec"
    }

    /// Return a reference to Any that can be used for down-casting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        match &self.mode {
            AggregateMode::Partial => {
                vec![Distribution::UnspecifiedDistribution]
            }
            AggregateMode::FinalPartitioned | AggregateMode::SinglePartitioned => {
                vec![Distribution::HashPartitioned(self.group_by.input_exprs())]
            }
            AggregateMode::Final | AggregateMode::Single => {
                vec![Distribution::SinglePartition]
            }
        }
    }

    fn required_input_ordering(&self) -> Vec<Option<LexRequirement>> {
        vec![self.required_input_ordering.clone()]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut me = AggregateExec::try_new_with_schema(
            self.mode,
            self.group_by.clone(),
            self.aggr_expr.clone(),
            self.filter_expr.clone(),
            Arc::clone(&children[0]),
            Arc::clone(&self.input_schema),
            Arc::clone(&self.schema),
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

    fn statistics(&self) -> Result<Statistics> {
        // TODO stats: group expressions:
        // - once expressions will be able to compute their own stats, use it here
        // - case where we group by on a column for which with have the `distinct` stat
        // TODO stats: aggr expression:
        // - aggregations sometimes also preserve invariants such as min, max...
        let column_statistics = Statistics::unknown_column(&self.schema());
        match self.mode {
            AggregateMode::Final | AggregateMode::FinalPartitioned
                if self.group_by.expr.is_empty() =>
            {
                Ok(Statistics {
                    num_rows: Precision::Exact(1),
                    column_statistics,
                    total_byte_size: Precision::Absent,
                })
            }
            _ => {
                // When the input row count is 0 or 1, we can adopt that statistic keeping its reliability.
                // When it is larger than 1, we degrade the precision since it may decrease after aggregation.
                let num_rows = if let Some(value) =
                    self.input().statistics()?.num_rows.get_value()
                {
                    if *value > 1 {
                        self.input().statistics()?.num_rows.to_inexact()
                    } else if *value == 0 {
                        // Aggregation on an empty table creates a null row.
                        self.input()
                            .statistics()?
                            .num_rows
                            .add(&Precision::Exact(1))
                    } else {
                        // num_rows = 1 case
                        self.input().statistics()?.num_rows
                    }
                } else {
                    Precision::Absent
                };
                Ok(Statistics {
                    num_rows,
                    column_statistics,
                    total_byte_size: Precision::Absent,
                })
            }
        }
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::LowerEqual
    }
}

fn create_schema(
    input_schema: &Schema,
    group_by: &PhysicalGroupBy,
    aggr_expr: &[Arc<AggregateFunctionExpr>],
    mode: AggregateMode,
) -> Result<Schema> {
    let mut fields = Vec::with_capacity(group_by.num_output_exprs() + aggr_expr.len());
    fields.extend(group_by.output_fields(input_schema)?);

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
                fields.push(expr.field())
            }
        }
    }

    Ok(Schema::new_with_metadata(
        fields,
        input_schema.metadata().clone(),
    ))
}

fn group_schema(input_schema: &Schema, group_by: &PhysicalGroupBy) -> Result<SchemaRef> {
    Ok(Arc::new(Schema::new(group_by.group_fields(input_schema)?)))
}

/// Determines the lexical ordering requirement for an aggregate expression.
///
/// # Parameters
///
/// - `aggr_expr`: A reference to an `AggregateFunctionExpr` representing the
///   aggregate expression.
/// - `group_by`: A reference to a `PhysicalGroupBy` instance representing the
///   physical GROUP BY expression.
/// - `agg_mode`: A reference to an `AggregateMode` instance representing the
///   mode of aggregation.
///
/// # Returns
///
/// A `LexOrdering` instance indicating the lexical ordering requirement for
/// the aggregate expression.
fn get_aggregate_expr_req(
    aggr_expr: &AggregateFunctionExpr,
    group_by: &PhysicalGroupBy,
    agg_mode: &AggregateMode,
) -> LexOrdering {
    // If the aggregation function is ordering requirement is not absolutely
    // necessary, or the aggregation is performing a "second stage" calculation,
    // then ignore the ordering requirement.
    if !aggr_expr.order_sensitivity().hard_requires() || !agg_mode.is_first_stage() {
        return vec![];
    }

    let mut req = aggr_expr.order_bys().unwrap_or_default().to_vec();

    // In non-first stage modes, we accumulate data (using `merge_batch`) from
    // different partitions (i.e. merge partial results). During this merge, we
    // consider the ordering of each partial result. Hence, we do not need to
    // use the ordering requirement in such modes as long as partial results are
    // generated with the correct ordering.
    if group_by.is_single() {
        // Remove all orderings that occur in the group by. These requirements
        // will definitely be satisfied -- Each group by expression will have
        // distinct values per group, hence all requirements are satisfied.
        let physical_exprs = group_by.input_exprs();
        req.retain(|sort_expr| {
            !physical_exprs_contains(&physical_exprs, &sort_expr.expr)
        });
    }
    req
}

/// Computes the finer ordering for between given existing ordering requirement
/// of aggregate expression.
///
/// # Parameters
///
/// * `existing_req` - The existing lexical ordering that needs refinement.
/// * `aggr_expr` - A reference to an aggregate expression trait object.
/// * `group_by` - Information about the physical grouping (e.g group by expression).
/// * `eq_properties` - Equivalence properties relevant to the computation.
/// * `agg_mode` - The mode of aggregation (e.g., Partial, Final, etc.).
///
/// # Returns
///
/// An `Option<LexOrdering>` representing the computed finer lexical ordering,
/// or `None` if there is no finer ordering; e.g. the existing requirement and
/// the aggregator requirement is incompatible.
fn finer_ordering(
    existing_req: &LexOrdering,
    aggr_expr: &AggregateFunctionExpr,
    group_by: &PhysicalGroupBy,
    eq_properties: &EquivalenceProperties,
    agg_mode: &AggregateMode,
) -> Option<LexOrdering> {
    let aggr_req = get_aggregate_expr_req(aggr_expr, group_by, agg_mode);
    eq_properties.get_finer_ordering(existing_req, &aggr_req)
}

/// Concatenates the given slices.
pub fn concat_slices<T: Clone>(lhs: &[T], rhs: &[T]) -> Vec<T> {
    [lhs, rhs].concat()
}

/// Get the common requirement that satisfies all the aggregate expressions.
///
/// # Parameters
///
/// - `aggr_exprs`: A slice of `AggregateFunctionExpr` containing all the
///   aggregate expressions.
/// - `group_by`: A reference to a `PhysicalGroupBy` instance representing the
///   physical GROUP BY expression.
/// - `eq_properties`: A reference to an `EquivalenceProperties` instance
///   representing equivalence properties for ordering.
/// - `agg_mode`: A reference to an `AggregateMode` instance representing the
///   mode of aggregation.
///
/// # Returns
///
/// A `LexRequirement` instance, which is the requirement that satisfies all the
/// aggregate requirements. Returns an error in case of conflicting requirements.
pub fn get_finer_aggregate_exprs_requirement(
    aggr_exprs: &mut [Arc<AggregateFunctionExpr>],
    group_by: &PhysicalGroupBy,
    eq_properties: &EquivalenceProperties,
    agg_mode: &AggregateMode,
) -> Result<LexRequirement> {
    let mut requirement = vec![];
    for aggr_expr in aggr_exprs.iter_mut() {
        if let Some(finer_ordering) =
            finer_ordering(&requirement, aggr_expr, group_by, eq_properties, agg_mode)
        {
            if eq_properties.ordering_satisfy(&finer_ordering) {
                // Requirement is satisfied by existing ordering
                requirement = finer_ordering;
                continue;
            }
        }
        if let Some(reverse_aggr_expr) = aggr_expr.reverse_expr() {
            if let Some(finer_ordering) = finer_ordering(
                &requirement,
                &reverse_aggr_expr,
                group_by,
                eq_properties,
                agg_mode,
            ) {
                if eq_properties.ordering_satisfy(&finer_ordering) {
                    // Reverse requirement is satisfied by exiting ordering.
                    // Hence reverse the aggregator
                    requirement = finer_ordering;
                    *aggr_expr = Arc::new(reverse_aggr_expr);
                    continue;
                }
            }
        }
        if let Some(finer_ordering) =
            finer_ordering(&requirement, aggr_expr, group_by, eq_properties, agg_mode)
        {
            // There is a requirement that both satisfies existing requirement and current
            // aggregate requirement. Use updated requirement
            requirement = finer_ordering;
            continue;
        }
        if let Some(reverse_aggr_expr) = aggr_expr.reverse_expr() {
            if let Some(finer_ordering) = finer_ordering(
                &requirement,
                &reverse_aggr_expr,
                group_by,
                eq_properties,
                agg_mode,
            ) {
                // There is a requirement that both satisfies existing requirement and reverse
                // aggregate requirement. Use updated requirement
                requirement = finer_ordering;
                *aggr_expr = Arc::new(reverse_aggr_expr);
                continue;
            }
        }

        // Neither the existing requirement and current aggregate requirement satisfy the other, this means
        // requirements are conflicting. Currently, we do not support
        // conflicting requirements.
        return not_impl_err!(
            "Conflicting ordering requirements in aggregate functions is not supported"
        );
    }

    Ok(PhysicalSortRequirement::from_sort_exprs(&requirement))
}

/// Returns physical expressions for arguments to evaluate against a batch.
///
/// The expressions are different depending on `mode`:
/// * Partial: AggregateFunctionExpr::expressions
/// * Final: columns of `AggregateFunctionExpr::state_fields()`
pub fn aggregate_expressions(
    aggr_expr: &[Arc<AggregateFunctionExpr>],
    mode: &AggregateMode,
    col_idx_base: usize,
) -> Result<Vec<Vec<Arc<dyn PhysicalExpr>>>> {
    match mode {
        AggregateMode::Partial
        | AggregateMode::Single
        | AggregateMode::SinglePartitioned => Ok(aggr_expr
            .iter()
            .map(|agg| {
                let mut result = agg.expressions();
                // Append ordering requirements to expressions' results. This
                // way order sensitive aggregators can satisfy requirement
                // themselves.
                if let Some(ordering_req) = agg.order_bys() {
                    result.extend(ordering_req.iter().map(|item| Arc::clone(&item.expr)));
                }
                result
            })
            .collect()),
        // In this mode, we build the merge expressions of the aggregation.
        AggregateMode::Final | AggregateMode::FinalPartitioned => {
            let mut col_idx_base = col_idx_base;
            aggr_expr
                .iter()
                .map(|agg| {
                    let exprs = merge_expressions(col_idx_base, agg)?;
                    col_idx_base += exprs.len();
                    Ok(exprs)
                })
                .collect()
        }
    }
}

/// uses `state_fields` to build a vec of physical column expressions required to merge the
/// AggregateFunctionExpr' accumulator's state.
///
/// `index_base` is the starting physical column index for the next expanded state field.
fn merge_expressions(
    index_base: usize,
    expr: &AggregateFunctionExpr,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    expr.state_fields().map(|fields| {
        fields
            .iter()
            .enumerate()
            .map(|(idx, f)| Arc::new(Column::new(f.name(), index_base + idx)) as _)
            .collect()
    })
}

pub type AccumulatorItem = Box<dyn Accumulator>;

pub fn create_accumulators(
    aggr_expr: &[Arc<AggregateFunctionExpr>],
) -> Result<Vec<AccumulatorItem>> {
    aggr_expr
        .iter()
        .map(|expr| expr.create_accumulator())
        .collect()
}

/// returns a vector of ArrayRefs, where each entry corresponds to either the
/// final value (mode = Final, FinalPartitioned and Single) or states (mode = Partial)
pub fn finalize_aggregation(
    accumulators: &mut [AccumulatorItem],
    mode: &AggregateMode,
) -> Result<Vec<ArrayRef>> {
    match mode {
        AggregateMode::Partial => {
            // Build the vector of states
            accumulators
                .iter_mut()
                .map(|accumulator| {
                    accumulator.state().and_then(|e| {
                        e.iter()
                            .map(|v| v.to_array())
                            .collect::<Result<Vec<ArrayRef>>>()
                    })
                })
                .flatten_ok()
                .collect()
        }
        AggregateMode::Final
        | AggregateMode::FinalPartitioned
        | AggregateMode::Single
        | AggregateMode::SinglePartitioned => {
            // Merge the state to the final value
            accumulators
                .iter_mut()
                .map(|accumulator| accumulator.evaluate().and_then(|v| v.to_array()))
                .collect()
        }
    }
}

/// Evaluates expressions against a record batch.
fn evaluate(
    expr: &[Arc<dyn PhysicalExpr>],
    batch: &RecordBatch,
) -> Result<Vec<ArrayRef>> {
    expr.iter()
        .map(|expr| {
            expr.evaluate(batch)
                .and_then(|v| v.into_array(batch.num_rows()))
        })
        .collect()
}

/// Evaluates expressions against a record batch.
pub(crate) fn evaluate_many(
    expr: &[Vec<Arc<dyn PhysicalExpr>>],
    batch: &RecordBatch,
) -> Result<Vec<Vec<ArrayRef>>> {
    expr.iter().map(|expr| evaluate(expr, batch)).collect()
}

fn evaluate_optional(
    expr: &[Option<Arc<dyn PhysicalExpr>>],
    batch: &RecordBatch,
) -> Result<Vec<Option<ArrayRef>>> {
    expr.iter()
        .map(|expr| {
            expr.as_ref()
                .map(|expr| {
                    expr.evaluate(batch)
                        .and_then(|v| v.into_array(batch.num_rows()))
                })
                .transpose()
        })
        .collect()
}

fn group_id_array(group: &[bool], batch: &RecordBatch) -> Result<ArrayRef> {
    if group.len() > 64 {
        return not_impl_err!(
            "Grouping sets with more than 64 columns are not supported"
        );
    }
    let group_id = group.iter().fold(0u64, |acc, &is_null| {
        (acc << 1) | if is_null { 1 } else { 0 }
    });
    let num_rows = batch.num_rows();
    if group.len() <= 8 {
        Ok(Arc::new(UInt8Array::from(vec![group_id as u8; num_rows])))
    } else if group.len() <= 16 {
        Ok(Arc::new(UInt16Array::from(vec![group_id as u16; num_rows])))
    } else if group.len() <= 32 {
        Ok(Arc::new(UInt32Array::from(vec![group_id as u32; num_rows])))
    } else {
        Ok(Arc::new(UInt64Array::from(vec![group_id; num_rows])))
    }
}

/// Evaluate a group by expression against a `RecordBatch`
///
/// Arguments:
/// - `group_by`: the expression to evaluate
/// - `batch`: the `RecordBatch` to evaluate against
///
/// Returns: A Vec of Vecs of Array of results
/// The outer Vec appears to be for grouping sets
/// The inner Vec contains the results per expression
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
            value.into_array(batch.num_rows())
        })
        .collect::<Result<Vec<_>>>()?;

    let null_exprs: Vec<ArrayRef> = group_by
        .null_expr
        .iter()
        .map(|(expr, _)| {
            let value = expr.evaluate(batch)?;
            value.into_array(batch.num_rows())
        })
        .collect::<Result<Vec<_>>>()?;

    group_by
        .groups
        .iter()
        .map(|group| {
            let mut group_values = Vec::with_capacity(group_by.num_group_exprs());
            group_values.extend(group.iter().enumerate().map(|(idx, is_null)| {
                if *is_null {
                    Arc::clone(&null_exprs[idx])
                } else {
                    Arc::clone(&exprs[idx])
                }
            }));
            if !group_by.is_single() {
                group_values.push(group_id_array(group, batch)?);
            }
            Ok(group_values)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::task::{Context, Poll};

    use super::*;
    use crate::coalesce_batches::CoalesceBatchesExec;
    use crate::coalesce_partitions::CoalescePartitionsExec;
    use crate::common;
    use crate::expressions::col;
    use crate::memory::MemoryExec;
    use crate::test::assert_is_pending;
    use crate::test::exec::{assert_strong_count_converges_to_zero, BlockingExec};
    use crate::RecordBatchStream;

    use arrow::array::{Float64Array, UInt32Array};
    use arrow::compute::{concat_batches, SortOptions};
    use arrow::datatypes::{DataType, Int32Type};
    use arrow_array::{
        DictionaryArray, Float32Array, Int32Array, StructArray, UInt64Array,
    };
    use datafusion_common::{
        assert_batches_eq, assert_batches_sorted_eq, internal_err, DataFusionError,
        ScalarValue,
    };
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::memory_pool::FairSpillPool;
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_functions_aggregate::array_agg::array_agg_udaf;
    use datafusion_functions_aggregate::average::avg_udaf;
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_aggregate::first_last::{first_value_udaf, last_value_udaf};
    use datafusion_functions_aggregate::median::median_udaf;
    use datafusion_functions_aggregate::sum::sum_udaf;
    use datafusion_physical_expr::expressions::lit;
    use datafusion_physical_expr::PhysicalSortExpr;

    use crate::common::collect;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::Literal;
    use datafusion_physical_expr::Partitioning;
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
            Arc::clone(&schema),
            vec![
                RecordBatch::try_new(
                    Arc::clone(&schema),
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
            Arc::clone(&schema),
            vec![
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(UInt32Array::from(vec![2, 3, 4, 4])),
                        Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
                    ],
                )
                .unwrap(),
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(UInt32Array::from(vec![2, 3, 3, 4])),
                        Arc::new(Float64Array::from(vec![0.0, 1.0, 2.0, 3.0])),
                    ],
                )
                .unwrap(),
                RecordBatch::try_new(
                    Arc::clone(&schema),
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
        let runtime = RuntimeEnvBuilder::default()
            .with_memory_pool(Arc::new(FairSpillPool::new(max_memory)))
            .build_arc()
            .unwrap();
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

        let grouping_set = PhysicalGroupBy::new(
            vec![
                (col("a", &input_schema)?, "a".to_string()),
                (col("b", &input_schema)?, "b".to_string()),
            ],
            vec![
                (lit(ScalarValue::UInt32(None)), "a".to_string()),
                (lit(ScalarValue::Float64(None)), "b".to_string()),
            ],
            vec![
                vec![false, true],  // (a, NULL)
                vec![true, false],  // (NULL, b)
                vec![false, false], // (a,b)
            ],
        );

        let aggregates = vec![Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![lit(1i8)])
                .schema(Arc::clone(&input_schema))
                .alias("COUNT(1)")
                .build()?,
        )];

        let task_ctx = if spill {
            // adjust the max memory size to have the partial aggregate result for spill mode.
            new_spill_ctx(4, 500)
        } else {
            Arc::new(TaskContext::default())
        };

        let partial_aggregate = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            grouping_set.clone(),
            aggregates.clone(),
            vec![None],
            input,
            Arc::clone(&input_schema),
        )?);

        let result =
            collect(partial_aggregate.execute(0, Arc::clone(&task_ctx))?).await?;

        let expected = if spill {
            // In spill mode, we test with the limited memory, if the mem usage exceeds,
            // we trigger the early emit rule, which turns out the partial aggregate result.
            vec![
                "+---+-----+---------------+-----------------+",
                "| a | b   | __grouping_id | COUNT(1)[count] |",
                "+---+-----+---------------+-----------------+",
                "|   | 1.0 | 2             | 1               |",
                "|   | 1.0 | 2             | 1               |",
                "|   | 2.0 | 2             | 1               |",
                "|   | 2.0 | 2             | 1               |",
                "|   | 3.0 | 2             | 1               |",
                "|   | 3.0 | 2             | 1               |",
                "|   | 4.0 | 2             | 1               |",
                "|   | 4.0 | 2             | 1               |",
                "| 2 |     | 1             | 1               |",
                "| 2 |     | 1             | 1               |",
                "| 2 | 1.0 | 0             | 1               |",
                "| 2 | 1.0 | 0             | 1               |",
                "| 3 |     | 1             | 1               |",
                "| 3 |     | 1             | 2               |",
                "| 3 | 2.0 | 0             | 2               |",
                "| 3 | 3.0 | 0             | 1               |",
                "| 4 |     | 1             | 1               |",
                "| 4 |     | 1             | 2               |",
                "| 4 | 3.0 | 0             | 1               |",
                "| 4 | 4.0 | 0             | 2               |",
                "+---+-----+---------------+-----------------+",
            ]
        } else {
            vec![
                "+---+-----+---------------+-----------------+",
                "| a | b   | __grouping_id | COUNT(1)[count] |",
                "+---+-----+---------------+-----------------+",
                "|   | 1.0 | 2             | 2               |",
                "|   | 2.0 | 2             | 2               |",
                "|   | 3.0 | 2             | 2               |",
                "|   | 4.0 | 2             | 2               |",
                "| 2 |     | 1             | 2               |",
                "| 2 | 1.0 | 0             | 2               |",
                "| 3 |     | 1             | 3               |",
                "| 3 | 2.0 | 0             | 2               |",
                "| 3 | 3.0 | 0             | 1               |",
                "| 4 |     | 1             | 3               |",
                "| 4 | 3.0 | 0             | 1               |",
                "| 4 | 4.0 | 0             | 2               |",
                "+---+-----+---------------+-----------------+",
            ]
        };
        assert_batches_sorted_eq!(expected, &result);

        let merge = Arc::new(CoalescePartitionsExec::new(partial_aggregate));

        let final_grouping_set = grouping_set.as_final();

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
            merge,
            input_schema,
        )?);

        let result = collect(merged_aggregate.execute(0, Arc::clone(&task_ctx))?).await?;
        let batch = concat_batches(&result[0].schema(), &result)?;
        assert_eq!(batch.num_columns(), 4);
        assert_eq!(batch.num_rows(), 12);

        let expected = vec![
            "+---+-----+---------------+----------+",
            "| a | b   | __grouping_id | COUNT(1) |",
            "+---+-----+---------------+----------+",
            "|   | 1.0 | 2             | 2        |",
            "|   | 2.0 | 2             | 2        |",
            "|   | 3.0 | 2             | 2        |",
            "|   | 4.0 | 2             | 2        |",
            "| 2 |     | 1             | 2        |",
            "| 2 | 1.0 | 0             | 2        |",
            "| 3 |     | 1             | 3        |",
            "| 3 | 2.0 | 0             | 2        |",
            "| 3 | 3.0 | 0             | 1        |",
            "| 4 |     | 1             | 3        |",
            "| 4 | 3.0 | 0             | 1        |",
            "| 4 | 4.0 | 0             | 2        |",
            "+---+-----+---------------+----------+",
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

        let grouping_set = PhysicalGroupBy::new(
            vec![(col("a", &input_schema)?, "a".to_string())],
            vec![],
            vec![vec![false]],
        );

        let aggregates: Vec<Arc<AggregateFunctionExpr>> = vec![Arc::new(
            AggregateExprBuilder::new(avg_udaf(), vec![col("b", &input_schema)?])
                .schema(Arc::clone(&input_schema))
                .alias("AVG(b)")
                .build()?,
        )];

        let task_ctx = if spill {
            // set to an appropriate value to trigger spill
            new_spill_ctx(2, 1600)
        } else {
            Arc::new(TaskContext::default())
        };

        let partial_aggregate = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            grouping_set.clone(),
            aggregates.clone(),
            vec![None],
            input,
            Arc::clone(&input_schema),
        )?);

        let result =
            collect(partial_aggregate.execute(0, Arc::clone(&task_ctx))?).await?;

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

        let final_grouping_set = grouping_set.as_final();

        let merged_aggregate = Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            final_grouping_set,
            aggregates,
            vec![None],
            merge,
            input_schema,
        )?);

        let task_ctx = if spill {
            // enlarge memory limit to let the final aggregation finish
            new_spill_ctx(2, 2600)
        } else {
            Arc::clone(&task_ctx)
        };
        let result = collect(merged_aggregate.execute(0, task_ctx)?).await?;
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
        let spill_count = metrics.spill_count().unwrap();
        let spilled_bytes = metrics.spilled_bytes().unwrap();
        let spilled_rows = metrics.spilled_rows().unwrap();

        if spill {
            // When spilling, the output rows metrics become partial output size + final output size
            // This is because final aggregation starts while partial aggregation is still emitting
            assert_eq!(8, output_rows);

            assert!(spill_count > 0);
            assert!(spilled_bytes > 0);
            assert!(spilled_rows > 0);
        } else {
            assert_eq!(3, output_rows);

            assert_eq!(0, spill_count);
            assert_eq!(0, spilled_bytes);
            assert_eq!(0, spilled_rows);
        }

        Ok(())
    }

    /// Define a test source that can yield back to runtime before returning its first item ///

    #[derive(Debug)]
    struct TestYieldingExec {
        /// True if this exec should yield back to runtime the first time it is polled
        pub yield_first: bool,
        cache: PlanProperties,
    }

    impl TestYieldingExec {
        fn new(yield_first: bool) -> Self {
            let schema = some_data().0;
            let cache = Self::compute_properties(schema);
            Self { yield_first, cache }
        }

        /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
        fn compute_properties(schema: SchemaRef) -> PlanProperties {
            let eq_properties = EquivalenceProperties::new(schema);
            PlanProperties::new(
                eq_properties,
                // Output Partitioning
                Partitioning::UnknownPartitioning(1),
                // Execution Mode
                ExecutionMode::Bounded,
            )
        }
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
        fn name(&self) -> &'static str {
            "TestYieldingExec"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &PlanProperties {
            &self.cache
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
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

        fn statistics(&self) -> Result<Statistics> {
            let (_, batches) = some_data();
            Ok(common::compute_record_batch_statistics(
                &[batches],
                &self.schema(),
                None,
            ))
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
        let input: Arc<dyn ExecutionPlan> = Arc::new(TestYieldingExec::new(false));

        check_aggregates(input, false).await
    }

    #[tokio::test]
    async fn aggregate_grouping_sets_source_not_yielding() -> Result<()> {
        let input: Arc<dyn ExecutionPlan> = Arc::new(TestYieldingExec::new(false));

        check_grouping_sets(input, false).await
    }

    #[tokio::test]
    async fn aggregate_source_with_yielding() -> Result<()> {
        let input: Arc<dyn ExecutionPlan> = Arc::new(TestYieldingExec::new(true));

        check_aggregates(input, false).await
    }

    #[tokio::test]
    async fn aggregate_grouping_sets_with_yielding() -> Result<()> {
        let input: Arc<dyn ExecutionPlan> = Arc::new(TestYieldingExec::new(true));

        check_grouping_sets(input, false).await
    }

    #[tokio::test]
    async fn aggregate_source_not_yielding_with_spill() -> Result<()> {
        let input: Arc<dyn ExecutionPlan> = Arc::new(TestYieldingExec::new(false));

        check_aggregates(input, true).await
    }

    #[tokio::test]
    async fn aggregate_grouping_sets_source_not_yielding_with_spill() -> Result<()> {
        let input: Arc<dyn ExecutionPlan> = Arc::new(TestYieldingExec::new(false));

        check_grouping_sets(input, true).await
    }

    #[tokio::test]
    async fn aggregate_source_with_yielding_with_spill() -> Result<()> {
        let input: Arc<dyn ExecutionPlan> = Arc::new(TestYieldingExec::new(true));

        check_aggregates(input, true).await
    }

    #[tokio::test]
    async fn aggregate_grouping_sets_with_yielding_with_spill() -> Result<()> {
        let input: Arc<dyn ExecutionPlan> = Arc::new(TestYieldingExec::new(true));

        check_grouping_sets(input, true).await
    }

    // Median(a)
    fn test_median_agg_expr(schema: SchemaRef) -> Result<AggregateFunctionExpr> {
        AggregateExprBuilder::new(median_udaf(), vec![col("a", &schema)?])
            .schema(schema)
            .alias("MEDIAN(a)")
            .build()
    }

    #[tokio::test]
    async fn test_oom() -> Result<()> {
        let input: Arc<dyn ExecutionPlan> = Arc::new(TestYieldingExec::new(true));
        let input_schema = input.schema();

        let runtime = RuntimeEnvBuilder::default()
            .with_memory_limit(1, 1.0)
            .build_arc()?;
        let task_ctx = TaskContext::default().with_runtime(runtime);
        let task_ctx = Arc::new(task_ctx);

        let groups_none = PhysicalGroupBy::default();
        let groups_some = PhysicalGroupBy::new(
            vec![(col("a", &input_schema)?, "a".to_string())],
            vec![],
            vec![vec![false]],
        );

        // something that allocates within the aggregator
        let aggregates_v0: Vec<Arc<AggregateFunctionExpr>> =
            vec![Arc::new(test_median_agg_expr(Arc::clone(&input_schema))?)];

        // use fast-path in `row_hash.rs`.
        let aggregates_v2: Vec<Arc<AggregateFunctionExpr>> = vec![Arc::new(
            AggregateExprBuilder::new(avg_udaf(), vec![col("b", &input_schema)?])
                .schema(Arc::clone(&input_schema))
                .alias("AVG(b)")
                .build()?,
        )];

        for (version, groups, aggregates) in [
            (0, groups_none, aggregates_v0),
            (2, groups_some, aggregates_v2),
        ] {
            let n_aggr = aggregates.len();
            let partial_aggregate = Arc::new(AggregateExec::try_new(
                AggregateMode::Partial,
                groups,
                aggregates,
                vec![None; n_aggr],
                Arc::clone(&input),
                Arc::clone(&input_schema),
            )?);

            let stream = partial_aggregate.execute_typed(0, Arc::clone(&task_ctx))?;

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
            let err = collect(stream).await.unwrap_err();

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
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float64, true)]));

        let groups = PhysicalGroupBy::default();

        let aggregates: Vec<Arc<AggregateFunctionExpr>> = vec![Arc::new(
            AggregateExprBuilder::new(avg_udaf(), vec![col("a", &schema)?])
                .schema(Arc::clone(&schema))
                .alias("AVG(a)")
                .build()?,
        )];

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 1));
        let refs = blocking_exec.refs();
        let aggregate_exec = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            groups.clone(),
            aggregates.clone(),
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
            Field::new("a", DataType::Float64, true),
            Field::new("b", DataType::Float64, true),
        ]));

        let groups =
            PhysicalGroupBy::new_single(vec![(col("a", &schema)?, "a".to_string())]);

        let aggregates: Vec<Arc<AggregateFunctionExpr>> = vec![Arc::new(
            AggregateExprBuilder::new(avg_udaf(), vec![col("b", &schema)?])
                .schema(Arc::clone(&schema))
                .alias("AVG(b)")
                .build()?,
        )];

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 1));
        let refs = blocking_exec.refs();
        let aggregate_exec = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            groups,
            aggregates.clone(),
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
                    first_last_multi_partitions(
                        use_coalesce_batches,
                        is_first_acc,
                        spill,
                        4200,
                    )
                    .await?
                }
            }
        }
        Ok(())
    }

    // FIRST_VALUE(b ORDER BY b <SortOptions>)
    fn test_first_value_agg_expr(
        schema: &Schema,
        sort_options: SortOptions,
    ) -> Result<Arc<AggregateFunctionExpr>> {
        let ordering_req = [PhysicalSortExpr {
            expr: col("b", schema)?,
            options: sort_options,
        }];
        let args = [col("b", schema)?];

        AggregateExprBuilder::new(first_value_udaf(), args.to_vec())
            .order_by(ordering_req.to_vec())
            .schema(Arc::new(schema.clone()))
            .alias(String::from("first_value(b) ORDER BY [b ASC NULLS LAST]"))
            .build()
            .map(Arc::new)
    }

    // LAST_VALUE(b ORDER BY b <SortOptions>)
    fn test_last_value_agg_expr(
        schema: &Schema,
        sort_options: SortOptions,
    ) -> Result<Arc<AggregateFunctionExpr>> {
        let ordering_req = [PhysicalSortExpr {
            expr: col("b", schema)?,
            options: sort_options,
        }];
        let args = [col("b", schema)?];
        AggregateExprBuilder::new(last_value_udaf(), args.to_vec())
            .order_by(ordering_req.to_vec())
            .schema(Arc::new(schema.clone()))
            .alias(String::from("last_value(b) ORDER BY [b ASC NULLS LAST]"))
            .build()
            .map(Arc::new)
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
        max_memory: usize,
    ) -> Result<()> {
        let task_ctx = if spill {
            new_spill_ctx(2, max_memory)
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

        let sort_options = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let aggregates: Vec<Arc<AggregateFunctionExpr>> = if is_first_acc {
            vec![test_first_value_agg_expr(&schema, sort_options)?]
        } else {
            vec![test_last_value_agg_expr(&schema, sort_options)?]
        };

        let memory_exec = Arc::new(MemoryExec::try_new(
            &[
                vec![partition1],
                vec![partition2],
                vec![partition3],
                vec![partition4],
            ],
            Arc::clone(&schema),
            None,
        )?);
        let aggregate_exec = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            groups.clone(),
            aggregates.clone(),
            vec![None],
            memory_exec,
            Arc::clone(&schema),
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
            coalesce,
            schema,
        )?) as Arc<dyn ExecutionPlan>;

        let result = crate::collect(aggregate_final, task_ctx).await?;
        if is_first_acc {
            let expected = [
                "+---+--------------------------------------------+",
                "| a | first_value(b) ORDER BY [b ASC NULLS LAST] |",
                "+---+--------------------------------------------+",
                "| 2 | 0.0                                        |",
                "| 3 | 1.0                                        |",
                "| 4 | 3.0                                        |",
                "+---+--------------------------------------------+",
            ];
            assert_batches_eq!(expected, &result);
        } else {
            let expected = [
                "+---+-------------------------------------------+",
                "| a | last_value(b) ORDER BY [b ASC NULLS LAST] |",
                "+---+-------------------------------------------+",
                "| 2 | 3.0                                       |",
                "| 3 | 5.0                                       |",
                "| 4 | 6.0                                       |",
                "+---+-------------------------------------------+",
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
        let col_a = &col("a", &test_schema)?;
        let col_b = &col("b", &test_schema)?;
        let col_c = &col("c", &test_schema)?;
        let mut eq_properties = EquivalenceProperties::new(Arc::clone(&test_schema));
        // Columns a and b are equal.
        eq_properties.add_equal_conditions(col_a, col_b)?;
        // Aggregate requirements are
        // [None], [a ASC], [a ASC, b ASC, c ASC], [a ASC, b ASC] respectively
        let order_by_exprs = vec![
            None,
            Some(vec![PhysicalSortExpr {
                expr: Arc::clone(col_a),
                options: options1,
            }]),
            Some(vec![
                PhysicalSortExpr {
                    expr: Arc::clone(col_a),
                    options: options1,
                },
                PhysicalSortExpr {
                    expr: Arc::clone(col_b),
                    options: options1,
                },
                PhysicalSortExpr {
                    expr: Arc::clone(col_c),
                    options: options1,
                },
            ]),
            Some(vec![
                PhysicalSortExpr {
                    expr: Arc::clone(col_a),
                    options: options1,
                },
                PhysicalSortExpr {
                    expr: Arc::clone(col_b),
                    options: options1,
                },
            ]),
        ];

        let common_requirement = vec![
            PhysicalSortExpr {
                expr: Arc::clone(col_a),
                options: options1,
            },
            PhysicalSortExpr {
                expr: Arc::clone(col_c),
                options: options1,
            },
        ];
        let mut aggr_exprs = order_by_exprs
            .into_iter()
            .map(|order_by_expr| {
                let ordering_req = order_by_expr.unwrap_or_default();
                AggregateExprBuilder::new(array_agg_udaf(), vec![Arc::clone(col_a)])
                    .alias("a")
                    .order_by(ordering_req.to_vec())
                    .schema(Arc::clone(&test_schema))
                    .build()
                    .map(Arc::new)
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let group_by = PhysicalGroupBy::new_single(vec![]);
        let res = get_finer_aggregate_exprs_requirement(
            &mut aggr_exprs,
            &group_by,
            &eq_properties,
            &AggregateMode::Partial,
        )?;
        let res = PhysicalSortRequirement::to_sort_exprs(res);
        assert_eq!(res, common_requirement);
        Ok(())
    }

    #[test]
    fn test_agg_exec_same_schema() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Float32, true),
            Field::new("b", DataType::Float32, true),
        ]));

        let col_a = col("a", &schema)?;
        let option_desc = SortOptions {
            descending: true,
            nulls_first: true,
        };
        let groups = PhysicalGroupBy::new_single(vec![(col_a, "a".to_string())]);

        let aggregates: Vec<Arc<AggregateFunctionExpr>> = vec![
            test_first_value_agg_expr(&schema, option_desc)?,
            test_last_value_agg_expr(&schema, option_desc)?,
        ];
        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 1));
        let aggregate_exec = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            groups,
            aggregates,
            vec![None, None],
            Arc::clone(&blocking_exec) as Arc<dyn ExecutionPlan>,
            schema,
        )?);
        let new_agg =
            Arc::clone(&aggregate_exec).with_new_children(vec![blocking_exec])?;
        assert_eq!(new_agg.schema(), aggregate_exec.schema());
        Ok(())
    }

    #[tokio::test]
    async fn test_agg_exec_group_by_const() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Float32, true),
            Field::new("b", DataType::Float32, true),
            Field::new("const", DataType::Int32, false),
        ]));

        let col_a = col("a", &schema)?;
        let col_b = col("b", &schema)?;
        let const_expr = Arc::new(Literal::new(ScalarValue::Int32(Some(1))));

        let groups = PhysicalGroupBy::new(
            vec![
                (col_a, "a".to_string()),
                (col_b, "b".to_string()),
                (const_expr, "const".to_string()),
            ],
            vec![
                (
                    Arc::new(Literal::new(ScalarValue::Float32(None))),
                    "a".to_string(),
                ),
                (
                    Arc::new(Literal::new(ScalarValue::Float32(None))),
                    "b".to_string(),
                ),
                (
                    Arc::new(Literal::new(ScalarValue::Int32(None))),
                    "const".to_string(),
                ),
            ],
            vec![
                vec![false, true, true],
                vec![true, false, true],
                vec![true, true, false],
            ],
        );

        let aggregates: Vec<Arc<AggregateFunctionExpr>> =
            vec![AggregateExprBuilder::new(count_udaf(), vec![lit(1)])
                .schema(Arc::clone(&schema))
                .alias("1")
                .build()
                .map(Arc::new)?];

        let input_batches = (0..4)
            .map(|_| {
                let a = Arc::new(Float32Array::from(vec![0.; 8192]));
                let b = Arc::new(Float32Array::from(vec![0.; 8192]));
                let c = Arc::new(Int32Array::from(vec![1; 8192]));

                RecordBatch::try_new(Arc::clone(&schema), vec![a, b, c]).unwrap()
            })
            .collect();

        let input = Arc::new(MemoryExec::try_new(
            &[input_batches],
            Arc::clone(&schema),
            None,
        )?);

        let aggregate_exec = Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            groups,
            aggregates.clone(),
            vec![None],
            input,
            schema,
        )?);

        let output =
            collect(aggregate_exec.execute(0, Arc::new(TaskContext::default()))?).await?;

        let expected = [
            "+-----+-----+-------+---------------+-------+",
            "| a   | b   | const | __grouping_id | 1     |",
            "+-----+-----+-------+---------------+-------+",
            "|     |     | 1     | 6             | 32768 |",
            "|     | 0.0 |       | 5             | 32768 |",
            "| 0.0 |     |       | 3             | 32768 |",
            "+-----+-----+-------+---------------+-------+",
        ];
        assert_batches_sorted_eq!(expected, &output);

        Ok(())
    }

    #[tokio::test]
    async fn test_agg_exec_struct_of_dicts() -> Result<()> {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(
                    "labels".to_string(),
                    DataType::Struct(
                        vec![
                            Field::new_dict(
                                "a".to_string(),
                                DataType::Dictionary(
                                    Box::new(DataType::Int32),
                                    Box::new(DataType::Utf8),
                                ),
                                true,
                                0,
                                false,
                            ),
                            Field::new_dict(
                                "b".to_string(),
                                DataType::Dictionary(
                                    Box::new(DataType::Int32),
                                    Box::new(DataType::Utf8),
                                ),
                                true,
                                0,
                                false,
                            ),
                        ]
                        .into(),
                    ),
                    false,
                ),
                Field::new("value", DataType::UInt64, false),
            ])),
            vec![
                Arc::new(StructArray::from(vec![
                    (
                        Arc::new(Field::new_dict(
                            "a".to_string(),
                            DataType::Dictionary(
                                Box::new(DataType::Int32),
                                Box::new(DataType::Utf8),
                            ),
                            true,
                            0,
                            false,
                        )),
                        Arc::new(
                            vec![Some("a"), None, Some("a")]
                                .into_iter()
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new_dict(
                            "b".to_string(),
                            DataType::Dictionary(
                                Box::new(DataType::Int32),
                                Box::new(DataType::Utf8),
                            ),
                            true,
                            0,
                            false,
                        )),
                        Arc::new(
                            vec![Some("b"), Some("c"), Some("b")]
                                .into_iter()
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef,
                    ),
                ])),
                Arc::new(UInt64Array::from(vec![1, 1, 1])),
            ],
        )
        .expect("Failed to create RecordBatch");

        let group_by = PhysicalGroupBy::new_single(vec![(
            col("labels", &batch.schema())?,
            "labels".to_string(),
        )]);

        let aggr_expr = vec![AggregateExprBuilder::new(
            sum_udaf(),
            vec![col("value", &batch.schema())?],
        )
        .schema(Arc::clone(&batch.schema()))
        .alias(String::from("SUM(value)"))
        .build()
        .map(Arc::new)?];

        let input = Arc::new(MemoryExec::try_new(
            &[vec![batch.clone()]],
            Arc::<Schema>::clone(&batch.schema()),
            None,
        )?);
        let aggregate_exec = Arc::new(AggregateExec::try_new(
            AggregateMode::FinalPartitioned,
            group_by,
            aggr_expr,
            vec![None],
            Arc::clone(&input) as Arc<dyn ExecutionPlan>,
            batch.schema(),
        )?);

        let session_config = SessionConfig::default();
        let ctx = TaskContext::default().with_session_config(session_config);
        let output = collect(aggregate_exec.execute(0, Arc::new(ctx))?).await?;

        let expected = [
            "+--------------+------------+",
            "| labels       | SUM(value) |",
            "+--------------+------------+",
            "| {a: a, b: b} | 2          |",
            "| {a: , b: c}  | 1          |",
            "+--------------+------------+",
        ];
        assert_batches_eq!(expected, &output);

        Ok(())
    }

    #[tokio::test]
    async fn test_skip_aggregation_after_first_batch() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, true),
            Field::new("val", DataType::Int32, true),
        ]));

        let group_by =
            PhysicalGroupBy::new_single(vec![(col("key", &schema)?, "key".to_string())]);

        let aggr_expr =
            vec![
                AggregateExprBuilder::new(count_udaf(), vec![col("val", &schema)?])
                    .schema(Arc::clone(&schema))
                    .alias(String::from("COUNT(val)"))
                    .build()
                    .map(Arc::new)?,
            ];

        let input_data = vec![
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(Int32Array::from(vec![0, 0, 0])),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from(vec![2, 3, 4])),
                    Arc::new(Int32Array::from(vec![0, 0, 0])),
                ],
            )
            .unwrap(),
        ];

        let input = Arc::new(MemoryExec::try_new(
            &[input_data],
            Arc::clone(&schema),
            None,
        )?);
        let aggregate_exec = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            group_by,
            aggr_expr,
            vec![None],
            Arc::clone(&input) as Arc<dyn ExecutionPlan>,
            schema,
        )?);

        let mut session_config = SessionConfig::default();
        session_config = session_config.set(
            "datafusion.execution.skip_partial_aggregation_probe_rows_threshold",
            &ScalarValue::Int64(Some(2)),
        );
        session_config = session_config.set(
            "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
            &ScalarValue::Float64(Some(0.1)),
        );

        let ctx = TaskContext::default().with_session_config(session_config);
        let output = collect(aggregate_exec.execute(0, Arc::new(ctx))?).await?;

        let expected = [
            "+-----+-------------------+",
            "| key | COUNT(val)[count] |",
            "+-----+-------------------+",
            "| 1   | 1                 |",
            "| 2   | 1                 |",
            "| 3   | 1                 |",
            "| 2   | 1                 |",
            "| 3   | 1                 |",
            "| 4   | 1                 |",
            "+-----+-------------------+",
        ];
        assert_batches_eq!(expected, &output);

        Ok(())
    }

    #[tokio::test]
    async fn test_skip_aggregation_after_threshold() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, true),
            Field::new("val", DataType::Int32, true),
        ]));

        let group_by =
            PhysicalGroupBy::new_single(vec![(col("key", &schema)?, "key".to_string())]);

        let aggr_expr =
            vec![
                AggregateExprBuilder::new(count_udaf(), vec![col("val", &schema)?])
                    .schema(Arc::clone(&schema))
                    .alias(String::from("COUNT(val)"))
                    .build()
                    .map(Arc::new)?,
            ];

        let input_data = vec![
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(Int32Array::from(vec![0, 0, 0])),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from(vec![2, 3, 4])),
                    Arc::new(Int32Array::from(vec![0, 0, 0])),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from(vec![2, 3, 4])),
                    Arc::new(Int32Array::from(vec![0, 0, 0])),
                ],
            )
            .unwrap(),
        ];

        let input = Arc::new(MemoryExec::try_new(
            &[input_data],
            Arc::clone(&schema),
            None,
        )?);
        let aggregate_exec = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            group_by,
            aggr_expr,
            vec![None],
            Arc::clone(&input) as Arc<dyn ExecutionPlan>,
            schema,
        )?);

        let mut session_config = SessionConfig::default();
        session_config = session_config.set(
            "datafusion.execution.skip_partial_aggregation_probe_rows_threshold",
            &ScalarValue::Int64(Some(5)),
        );
        session_config = session_config.set(
            "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
            &ScalarValue::Float64(Some(0.1)),
        );

        let ctx = TaskContext::default().with_session_config(session_config);
        let output = collect(aggregate_exec.execute(0, Arc::new(ctx))?).await?;

        let expected = [
            "+-----+-------------------+",
            "| key | COUNT(val)[count] |",
            "+-----+-------------------+",
            "| 1   | 1                 |",
            "| 2   | 2                 |",
            "| 3   | 2                 |",
            "| 4   | 1                 |",
            "| 2   | 1                 |",
            "| 3   | 1                 |",
            "| 4   | 1                 |",
            "+-----+-------------------+",
        ];
        assert_batches_eq!(expected, &output);

        Ok(())
    }

    #[test]
    fn group_exprs_nullable() -> Result<()> {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Float32, false),
            Field::new("b", DataType::Float32, false),
        ]));

        let aggr_expr =
            vec![
                AggregateExprBuilder::new(count_udaf(), vec![col("a", &input_schema)?])
                    .schema(Arc::clone(&input_schema))
                    .alias("COUNT(a)")
                    .build()
                    .map(Arc::new)?,
            ];

        let grouping_set = PhysicalGroupBy::new(
            vec![
                (col("a", &input_schema)?, "a".to_string()),
                (col("b", &input_schema)?, "b".to_string()),
            ],
            vec![
                (lit(ScalarValue::Float32(None)), "a".to_string()),
                (lit(ScalarValue::Float32(None)), "b".to_string()),
            ],
            vec![
                vec![false, true],  // (a, NULL)
                vec![false, false], // (a,b)
            ],
        );
        let aggr_schema = create_schema(
            &input_schema,
            &grouping_set,
            &aggr_expr,
            AggregateMode::Final,
        )?;
        let expected_schema = Schema::new(vec![
            Field::new("a", DataType::Float32, false),
            Field::new("b", DataType::Float32, true),
            Field::new("__grouping_id", DataType::UInt8, false),
            Field::new("COUNT(a)", DataType::Int64, false),
        ]);
        assert_eq!(aggr_schema, expected_schema);
        Ok(())
    }
}
