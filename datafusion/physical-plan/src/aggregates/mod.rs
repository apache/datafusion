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

use super::{DisplayAs, ExecutionPlanProperties, PlanProperties};
use crate::aggregates::{
    no_grouping::AggregateStream, row_hash::GroupedHashAggregateStream,
    topk_stream::GroupedTopKAggregateStream,
};
use crate::execution_plan::{CardinalityEffect, EmissionType};
use crate::filter_pushdown::{
    ChildFilterDescription, ChildPushdownResult, FilterDescription, FilterPushdownPhase,
    FilterPushdownPropagation, PushedDownPredicate,
};
use crate::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::windows::get_ordered_partition_by_indices;
use crate::{
    DisplayFormatType, Distribution, ExecutionPlan, InputOrderMode,
    SendableRecordBatchStream, Statistics,
};
use datafusion_common::config::ConfigOptions;
use datafusion_physical_expr::utils::collect_columns;
use parking_lot::Mutex;
use std::collections::HashSet;

use arrow::array::{ArrayRef, UInt16Array, UInt32Array, UInt64Array, UInt8Array};
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_schema::FieldRef;
use datafusion_common::stats::Precision;
use datafusion_common::{
    assert_eq_or_internal_err, not_impl_err, Constraint, Constraints, Result, ScalarValue,
};
use datafusion_execution::TaskContext;
use datafusion_expr::{Accumulator, Aggregate};
use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
use datafusion_physical_expr::equivalence::ProjectionMapping;
use datafusion_physical_expr::expressions::{lit, Column, DynamicFilterPhysicalExpr};
use datafusion_physical_expr::{
    physical_exprs_contains, ConstExpr, EquivalenceProperties,
};
use datafusion_physical_expr_common::physical_expr::{fmt_sql, PhysicalExpr};
use datafusion_physical_expr_common::sort_expr::{
    LexOrdering, LexRequirement, OrderingRequirements, PhysicalSortRequirement,
};

use datafusion_expr::utils::AggregateOrderSensitivity;
use datafusion_physical_expr_common::utils::evaluate_expressions_to_arrays;
use itertools::Itertools;

pub mod group_values;
mod no_grouping;
pub mod order;
mod row_hash;
mod topk;
mod topk_stream;

/// Hard-coded seed for aggregations to ensure hash values differ from `RepartitionExec`, avoiding collisions.
const AGGREGATION_HASH_SEED: ahash::RandomState =
    ahash::RandomState::with_seeds('A' as u64, 'G' as u64, 'G' as u64, 'R' as u64);

/// Aggregation modes
///
/// See [`Accumulator::state`] for background information on multi-phase
/// aggregation and how these modes are used.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AggregateMode {
    /// One of multiple layers of aggregation, any input partitioning
    ///
    /// Partial aggregate that can be applied in parallel across input
    /// partitions.
    ///
    /// This is the first phase of a multi-phase aggregation.
    Partial,
    /// *Final* of multiple layers of aggregation, in exactly one partition
    ///
    /// Final aggregate that produces a single partition of output by combining
    /// the output of multiple partial aggregates.
    ///
    /// This is the second phase of a multi-phase aggregation.
    ///
    /// This mode requires that the input is a single partition
    ///
    /// Note: Adjacent `Partial` and `Final` mode aggregation is equivalent to a `Single`
    /// mode aggregation node. The `Final` mode is required since this is used in an
    /// intermediate step. The [`CombinePartialFinalAggregate`] physical optimizer rule
    /// will replace this combination with `Single` mode for more efficient execution.
    ///
    /// [`CombinePartialFinalAggregate`]: https://docs.rs/datafusion/latest/datafusion/physical_optimizer/combine_partial_final_agg/struct.CombinePartialFinalAggregate.html
    Final,
    /// *Final* of multiple layers of aggregation, input is *Partitioned*
    ///
    /// Final aggregate that works on pre-partitioned data.
    ///
    /// This mode requires that all rows with a particular grouping key are in
    /// the same partitions, such as is the case with Hash repartitioning on the
    /// group keys. If a group key is duplicated, duplicate groups would be
    /// produced
    FinalPartitioned,
    /// *Single* layer of Aggregation, input is exactly one partition
    ///
    /// Applies the entire logical aggregation operation in a single operator,
    /// as opposed to Partial / Final modes which apply the logical aggregation using
    /// two operators.
    ///
    /// This mode requires that the input is a single partition (like Final)
    Single,
    /// *Single* layer of Aggregation, input is *Partitioned*
    ///
    /// Applies the entire logical aggregation operation in a single operator,
    /// as opposed to Partial / Final modes which apply the logical aggregation
    /// using two operators.
    ///
    /// This mode requires that the input has more than one partition, and is
    /// partitioned by group key (like FinalPartitioned).
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
    pub fn num_group_exprs(&self) -> usize {
        if self.is_single() {
            self.expr.len()
        } else {
            self.expr.len() + 1
        }
    }

    pub fn group_schema(&self, schema: &Schema) -> Result<SchemaRef> {
        Ok(Arc::new(Schema::new(self.group_fields(schema)?)))
    }

    /// Returns the fields that are used as the grouping keys.
    fn group_fields(&self, input_schema: &Schema) -> Result<Vec<FieldRef>> {
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
                .with_metadata(expr.return_field(input_schema)?.metadata().clone())
                .into(),
            );
        }
        if !self.is_single() {
            fields.push(
                Field::new(
                    Aggregate::INTERNAL_GROUPING_ID,
                    Aggregate::grouping_id_type(self.expr.len()),
                    false,
                )
                .into(),
            );
        }
        Ok(fields)
    }

    /// Returns the output fields of the group by.
    ///
    /// This might be different from the `group_fields` that might contain internal expressions that
    /// should not be part of the output schema.
    fn output_fields(&self, input_schema: &Schema) -> Result<Vec<FieldRef>> {
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
        let groups = if self.expr.is_empty() {
            // No GROUP BY expressions - should have no groups
            vec![]
        } else {
            // Has GROUP BY expressions - create a single group
            vec![vec![false; num_exprs]]
        };
        Self {
            expr,
            null_expr: vec![],
            groups,
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

#[expect(clippy::large_enum_variant)]
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

/// # Aggregate Dynamic Filter Pushdown Overview
///
/// For queries like
///   -- `example_table(type TEXT, val INT)`
///   SELECT min(val)
///   FROM example_table
///   WHERE type='A';
///
/// And `example_table`'s physical representation is a partitioned parquet file with
/// column statistics
/// - part-0.parquet: val {min=0, max=100}
/// - part-1.parquet: val {min=100, max=200}
/// - ...
/// - part-100.parquet: val {min=10000, max=10100}
///
/// After scanning the 1st file, we know we only have to read files if their minimal
/// value on `val` column is less than 0, the minimal `val` value in the 1st file.
///
/// We can skip scanning the remaining file by implementing dynamic filter, the
/// intuition is we keep a shared data structure for current min in both `AggregateExec`
/// and `DataSourceExec`, and let it update during execution, so the scanner can
/// know during execution if it's possible to skip scanning certain files. See
/// physical optimizer rule `FilterPushdown` for details.
///
/// # Implementation
///
/// ## Enable Condition
/// - No grouping (no `GROUP BY` clause in the sql, only a single global group to aggregate)
/// - The aggregate expression must be `min`/`max`, and evaluate directly on columns.
///   Note multiple aggregate expressions that satisfy this requirement are allowed,
///   and a dynamic filter will be constructed combining all applicable expr's
///   states. See more in the following example with dynamic filter on multiple columns.
///
/// ## Filter Construction
/// The filter is kept in the `DataSourceExec`, and it will gets update during execution,
/// the reader will interpret it as "the upstream only needs rows that such filter
/// predicate is evaluated to true", and certain scanner implementation like `parquet`
/// can evalaute column statistics on those dynamic filters, to decide if they can
/// prune a whole range.
///
/// ### Examples
/// - Expr: `min(a)`, Dynamic Filter: `a < a_cur_min`
/// - Expr: `min(a), max(a), min(b)`, Dynamic Filter: `(a < a_cur_min) OR (a > a_cur_max) OR (b < b_cur_min)`
#[derive(Debug, Clone)]
struct AggrDynFilter {
    /// The physical expr for the dynamic filter shared between the `AggregateExec`
    /// and the parquet scanner.
    filter: Arc<DynamicFilterPhysicalExpr>,
    /// The current bounds for the dynamic filter, updates during the execution to
    /// tighten the bound for more effective pruning.
    ///
    /// Each vector element is for the accumulators that support dynamic filter.
    /// e.g. This `AggregateExec` has accumulator:
    /// min(a), avg(a), max(b)
    /// And this field stores [PerAccumulatorDynFilter(min(a)), PerAccumulatorDynFilter(min(b))]
    supported_accumulators_info: Vec<PerAccumulatorDynFilter>,
}

// ---- Aggregate Dynamic Filter Utility Structs ----

/// Aggregate expressions that support the dynamic filter pushdown in aggregation.
/// See comments in [`AggrDynFilter`] for conditions.
#[derive(Debug, Clone)]
struct PerAccumulatorDynFilter {
    aggr_type: DynamicFilterAggregateType,
    /// During planning and optimization, the parent structure is kept in `AggregateExec`,
    /// this index is into `aggr_expr` vec inside `AggregateExec`.
    /// During execution, the parent struct is moved into `AggregateStream` (stream
    /// for no grouping aggregate execution), and this index is into    `aggregate_expressions`
    /// vec inside `AggregateStreamInner`
    aggr_index: usize,
    // The current bound. Shared among all streams.
    shared_bound: Arc<Mutex<ScalarValue>>,
}

/// Aggregate types that are supported for dynamic filter in `AggregateExec`
#[derive(Debug, Clone)]
enum DynamicFilterAggregateType {
    Min,
    Max,
}

/// Hash aggregate execution plan
#[derive(Debug, Clone)]
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
    required_input_ordering: Option<OrderingRequirements>,
    /// Describes how the input is ordered relative to the group by columns
    input_order_mode: InputOrderMode,
    cache: PlanProperties,
    /// During initialization, if the plan supports dynamic filtering (see [`AggrDynFilter`]),
    /// it is set to `Some(..)` regardless of whether it can be pushed down to a child node.
    ///
    /// During filter pushdown optimization, if a child node can accept this filter,
    /// it remains `Some(..)` to enable dynamic filtering during aggregate execution;
    /// otherwise, it is cleared to `None`.
    dynamic_filter: Option<Arc<AggrDynFilter>>,
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
            dynamic_filter: self.dynamic_filter.clone(),
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
        assert_eq_or_internal_err!(
            aggr_expr.len(),
            filter_expr.len(),
            "Inconsistent aggregate expr: {:?} and filter expr: {:?} for AggregateExec, their size should match",
            aggr_expr,
            filter_expr
        );

        let input_eq_properties = input.equivalence_properties();
        // Get GROUP BY expressions:
        let groupby_exprs = group_by.input_exprs();
        // If existing ordering satisfies a prefix of the GROUP BY expressions,
        // prefix requirements with this section. In this case, aggregation will
        // work more efficiently.
        let indices = get_ordered_partition_by_indices(&groupby_exprs, &input)?;
        let mut new_requirements = indices
            .iter()
            .map(|&idx| {
                PhysicalSortRequirement::new(Arc::clone(&groupby_exprs[idx]), None)
            })
            .collect::<Vec<_>>();

        let req = get_finer_aggregate_exprs_requirement(
            &mut aggr_expr,
            &group_by,
            input_eq_properties,
            &mode,
        )?;
        new_requirements.extend(req);

        let required_input_ordering =
            LexRequirement::new(new_requirements).map(OrderingRequirements::new_soft);

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
        let group_expr_mapping =
            ProjectionMapping::try_new(group_by.expr.clone(), &input.schema())?;

        let cache = Self::compute_properties(
            &input,
            Arc::clone(&schema),
            &group_expr_mapping,
            &mode,
            &input_order_mode,
            aggr_expr.as_slice(),
        )?;

        let mut exec = AggregateExec {
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
            dynamic_filter: None,
        };

        exec.init_dynamic_filter();

        Ok(exec)
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
        context: &Arc<TaskContext>,
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
    pub fn get_minmax_desc(&self) -> Option<(FieldRef, bool)> {
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
        if !self.aggr_expr().iter().all(|e| e.order_bys().is_empty()) {
            return false;
        }
        // ensure there is no output ordering; can this rule be relaxed?
        if self.properties().output_ordering().is_some() {
            return false;
        }
        // ensure no ordering is required on the input
        if let Some(requirement) = self.required_input_ordering().swap_remove(0) {
            return matches!(requirement, OrderingRequirements::Hard(_));
        }
        true
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    pub fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        group_expr_mapping: &ProjectionMapping,
        mode: &AggregateMode,
        input_order_mode: &InputOrderMode,
        aggr_exprs: &[Arc<AggregateFunctionExpr>],
    ) -> Result<PlanProperties> {
        // Construct equivalence properties:
        let mut eq_properties = input
            .equivalence_properties()
            .project(group_expr_mapping, schema);

        // If the group by is empty, then we ensure that the operator will produce
        // only one row, and mark the generated result as a constant value.
        if group_expr_mapping.is_empty() {
            let new_constants = aggr_exprs.iter().enumerate().map(|(idx, func)| {
                let column = Arc::new(Column::new(func.name(), idx));
                ConstExpr::from(column as Arc<dyn PhysicalExpr>)
            });
            eq_properties.add_constants(new_constants)?;
        }

        // Group by expression will be a distinct value after the aggregation.
        // Add it into the constraint set.
        let mut constraints = eq_properties.constraints().to_vec();
        let new_constraint = Constraint::Unique(
            group_expr_mapping
                .iter()
                .flat_map(|(_, target_cols)| {
                    target_cols.iter().flat_map(|(expr, _)| {
                        expr.as_any().downcast_ref::<Column>().map(|c| c.index())
                    })
                })
                .collect(),
        );
        constraints.push(new_constraint);
        eq_properties =
            eq_properties.with_constraints(Constraints::new_unverified(constraints));

        // Get output partitioning:
        let input_partitioning = input.output_partitioning().clone();
        let output_partitioning = if mode.is_first_stage() {
            // First stage aggregation will not change the output partitioning,
            // but needs to respect aliases (e.g. mapping in the GROUP BY
            // expression).
            let input_eq_properties = input.equivalence_properties();
            input_partitioning.project(group_expr_mapping, input_eq_properties)
        } else {
            input_partitioning.clone()
        };

        // TODO: Emission type and boundedness information can be enhanced here
        let emission_type = if *input_order_mode == InputOrderMode::Linear {
            EmissionType::Final
        } else {
            input.pipeline_behavior()
        };

        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            emission_type,
            input.boundedness(),
        ))
    }

    pub fn input_order_mode(&self) -> &InputOrderMode {
        &self.input_order_mode
    }

    fn statistics_inner(&self, child_statistics: &Statistics) -> Result<Statistics> {
        // TODO stats: group expressions:
        // - once expressions will be able to compute their own stats, use it here
        // - case where we group by on a column for which with have the `distinct` stat
        // TODO stats: aggr expression:
        // - aggregations sometimes also preserve invariants such as min, max...

        let column_statistics = {
            // self.schema: [<group by exprs>, <aggregate exprs>]
            let mut column_statistics = Statistics::unknown_column(&self.schema());

            for (idx, (expr, _)) in self.group_by.expr.iter().enumerate() {
                if let Some(col) = expr.as_any().downcast_ref::<Column>() {
                    column_statistics[idx].max_value = child_statistics.column_statistics
                        [col.index()]
                    .max_value
                    .clone();

                    column_statistics[idx].min_value = child_statistics.column_statistics
                        [col.index()]
                    .min_value
                    .clone();
                }
            }

            column_statistics
        };
        match self.mode {
            AggregateMode::Final | AggregateMode::FinalPartitioned
                if self.group_by.expr.is_empty() =>
            {
                let total_byte_size =
                    Self::calculate_scaled_byte_size(child_statistics, 1);

                Ok(Statistics {
                    num_rows: Precision::Exact(1),
                    column_statistics,
                    total_byte_size,
                })
            }
            _ => {
                // When the input row count is 1, we can adopt that statistic keeping its reliability.
                // When it is larger than 1, we degrade the precision since it may decrease after aggregation.
                let num_rows = if let Some(value) = child_statistics.num_rows.get_value()
                {
                    if *value > 1 {
                        child_statistics.num_rows.to_inexact()
                    } else if *value == 0 {
                        child_statistics.num_rows
                    } else {
                        // num_rows = 1 case
                        let grouping_set_num = self.group_by.groups.len();
                        child_statistics.num_rows.map(|x| x * grouping_set_num)
                    }
                } else {
                    Precision::Absent
                };

                let total_byte_size = num_rows
                    .get_value()
                    .and_then(|&output_rows| {
                        Self::calculate_scaled_byte_size(child_statistics, output_rows)
                            .get_value()
                            .map(|&bytes| Precision::Inexact(bytes))
                    })
                    .unwrap_or(Precision::Absent);

                Ok(Statistics {
                    num_rows,
                    column_statistics,
                    total_byte_size,
                })
            }
        }
    }

    /// Check if dynamic filter is possible for the current plan node.
    /// - If yes, init one inside `AggregateExec`'s `dynamic_filter` field.
    /// - If not supported, `self.dynamic_filter` should be kept `None`
    fn init_dynamic_filter(&mut self) {
        if (!self.group_by.is_empty()) || (!matches!(self.mode, AggregateMode::Partial)) {
            debug_assert!(
                self.dynamic_filter.is_none(),
                "The current operator node does not support dynamic filter"
            );
            return;
        }

        // Already initialized.
        if self.dynamic_filter.is_some() {
            return;
        }

        // Collect supported accumulators
        // It is assumed the order of aggregate expressions are not changed from `AggregateExec`
        // to `AggregateStream`
        let mut aggr_dyn_filters = Vec::new();
        // All column references in the dynamic filter, used when initializing the dynamic
        // filter, and it's used to decide if this dynamic filter is able to get push
        // through certain node during optimization.
        let mut all_cols: Vec<Arc<dyn PhysicalExpr>> = Vec::new();
        for (i, aggr_expr) in self.aggr_expr.iter().enumerate() {
            // 1. Only `min` or `max` aggregate function
            let fun_name = aggr_expr.fun().name();
            // HACK: Should check the function type more precisely
            // Issue: <https://github.com/apache/datafusion/issues/18643>
            let aggr_type = if fun_name.eq_ignore_ascii_case("min") {
                DynamicFilterAggregateType::Min
            } else if fun_name.eq_ignore_ascii_case("max") {
                DynamicFilterAggregateType::Max
            } else {
                continue;
            };

            // 2. arg should be only 1 column reference
            if let [arg] = aggr_expr.expressions().as_slice() {
                if arg.as_any().is::<Column>() {
                    all_cols.push(Arc::clone(arg));
                    aggr_dyn_filters.push(PerAccumulatorDynFilter {
                        aggr_type,
                        aggr_index: i,
                        shared_bound: Arc::new(Mutex::new(ScalarValue::Null)),
                    });
                }
            }
        }

        if !aggr_dyn_filters.is_empty() {
            self.dynamic_filter = Some(Arc::new(AggrDynFilter {
                filter: Arc::new(DynamicFilterPhysicalExpr::new(all_cols, lit(true))),
                supported_accumulators_info: aggr_dyn_filters,
            }))
        }
    }

    /// Calculate scaled byte size based on row count ratio.
    /// Returns `Precision::Absent` if input statistics are insufficient.
    /// Returns `Precision::Inexact` with the scaled value otherwise.
    ///
    /// This is a simple heuristic that assumes uniform row sizes.
    #[inline]
    fn calculate_scaled_byte_size(
        input_stats: &Statistics,
        target_row_count: usize,
    ) -> Precision<usize> {
        match (
            input_stats.num_rows.get_value(),
            input_stats.total_byte_size.get_value(),
        ) {
            (Some(&input_rows), Some(&input_bytes)) if input_rows > 0 => {
                let bytes_per_row = input_bytes as f64 / input_rows as f64;
                let scaled_bytes =
                    (bytes_per_row * target_row_count as f64).ceil() as usize;
                Precision::Inexact(scaled_bytes)
            }
            _ => Precision::Absent,
        }
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
                let format_expr_with_alias =
                    |(e, alias): &(Arc<dyn PhysicalExpr>, String)| -> String {
                        let e = e.to_string();
                        if &e != alias {
                            format!("{e} as {alias}")
                        } else {
                            e
                        }
                    };

                write!(f, "AggregateExec: mode={:?}", self.mode)?;
                let g: Vec<String> = if self.group_by.is_single() {
                    self.group_by
                        .expr
                        .iter()
                        .map(format_expr_with_alias)
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
                                        format_expr_with_alias(
                                            &self.group_by.null_expr[idx],
                                        )
                                    } else {
                                        format_expr_with_alias(&self.group_by.expr[idx])
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
            DisplayFormatType::TreeRender => {
                let format_expr_with_alias =
                    |(e, alias): &(Arc<dyn PhysicalExpr>, String)| -> String {
                        let expr_sql = fmt_sql(e.as_ref()).to_string();
                        if &expr_sql != alias {
                            format!("{expr_sql} as {alias}")
                        } else {
                            expr_sql
                        }
                    };

                let g: Vec<String> = if self.group_by.is_single() {
                    self.group_by
                        .expr
                        .iter()
                        .map(format_expr_with_alias)
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
                                        format_expr_with_alias(
                                            &self.group_by.null_expr[idx],
                                        )
                                    } else {
                                        format_expr_with_alias(&self.group_by.expr[idx])
                                    }
                                })
                                .collect::<Vec<String>>()
                                .join(", ");
                            format!("({terms})")
                        })
                        .collect()
                };
                let a: Vec<String> = self
                    .aggr_expr
                    .iter()
                    .map(|agg| agg.human_display().to_string())
                    .collect();
                writeln!(f, "mode={:?}", self.mode)?;
                if !g.is_empty() {
                    writeln!(f, "group_by={}", g.join(", "))?;
                }
                if !a.is_empty() {
                    writeln!(f, "aggr={}", a.join(", "))?;
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

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![self.required_input_ordering.clone()]
    }

    /// The output ordering of [`AggregateExec`] is determined by its `group_by`
    /// columns. Although this method is not explicitly used by any optimizer
    /// rules yet, overriding the default implementation ensures that it
    /// accurately reflects the actual behavior.
    ///
    /// If the [`InputOrderMode`] is `Linear`, the `group_by` columns don't have
    /// an ordering, which means the results do not either. However, in the
    /// `Ordered` and `PartiallyOrdered` cases, the `group_by` columns do have
    /// an ordering, which is preserved in the output.
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![self.input_order_mode != InputOrderMode::Linear]
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
        me.dynamic_filter = self.dynamic_filter.clone();

        Ok(Arc::new(me))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.execute_typed(partition, &context)
            .map(|stream| stream.into())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.partition_statistics(None)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        let child_statistics = self.input().partition_statistics(partition)?;
        self.statistics_inner(&child_statistics)
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::LowerEqual
    }

    /// Push down parent filters when possible (see implementation comment for details),
    /// and also pushdown self dynamic filters (see `AggrDynFilter` for details)
    fn gather_filters_for_pushdown(
        &self,
        phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        // It's safe to push down filters through aggregates when filters only reference
        // grouping columns, because such filters determine which groups to compute, not
        // *how* to compute them. Each group's aggregate values (SUM, COUNT, etc.) are
        // calculated from the same input rows regardless of whether we filter before or
        // after grouping - filtering before just eliminates entire groups early.
        // This optimization is NOT safe for filters on aggregated columns (like filtering on
        // the result of SUM or COUNT), as those require computing all groups first.

        let grouping_columns: HashSet<_> = self
            .group_by
            .expr()
            .iter()
            .flat_map(|(expr, _)| collect_columns(expr))
            .collect();

        // Analyze each filter separately to determine if it can be pushed down
        let mut safe_filters = Vec::new();
        let mut unsafe_filters = Vec::new();

        for filter in parent_filters {
            let filter_columns: HashSet<_> =
                collect_columns(&filter).into_iter().collect();

            // Check if this filter references non-grouping columns
            let references_non_grouping = !grouping_columns.is_empty()
                && !filter_columns.is_subset(&grouping_columns);

            if references_non_grouping {
                unsafe_filters.push(filter);
                continue;
            }

            // For GROUPING SETS, verify this filter's columns appear in all grouping sets
            if self.group_by.groups().len() > 1 {
                let filter_column_indices: Vec<usize> = filter_columns
                    .iter()
                    .filter_map(|filter_col| {
                        self.group_by.expr().iter().position(|(expr, _)| {
                            collect_columns(expr).contains(filter_col)
                        })
                    })
                    .collect();

                // Check if any of this filter's columns are missing from any grouping set
                let has_missing_column = self.group_by.groups().iter().any(|null_mask| {
                    filter_column_indices
                        .iter()
                        .any(|&idx| null_mask.get(idx) == Some(&true))
                });

                if has_missing_column {
                    unsafe_filters.push(filter);
                    continue;
                }
            }

            // This filter is safe to push down
            safe_filters.push(filter);
        }

        // Build child filter description with both safe and unsafe filters
        let child = self.children()[0];
        let mut child_desc = ChildFilterDescription::from_child(&safe_filters, child)?;

        // Add unsafe filters as unsupported
        child_desc.parent_filters.extend(
            unsafe_filters
                .into_iter()
                .map(PushedDownPredicate::unsupported),
        );

        // Include self dynamic filter when it's possible
        if matches!(phase, FilterPushdownPhase::Post)
            && config.optimizer.enable_aggregate_dynamic_filter_pushdown
        {
            if let Some(self_dyn_filter) = &self.dynamic_filter {
                let dyn_filter = Arc::clone(&self_dyn_filter.filter);
                child_desc = child_desc.with_self_filter(dyn_filter);
            }
        }

        Ok(FilterDescription::new().with_child(child_desc))
    }

    /// If child accepts self's dynamic filter, keep `self.dynamic_filter` with Some,
    /// otherwise clear it to None.
    fn handle_child_pushdown_result(
        &self,
        phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        let mut result = FilterPushdownPropagation::if_any(child_pushdown_result.clone());

        // If this node tried to pushdown some dynamic filter before, now we check
        // if the child accept the filter
        if matches!(phase, FilterPushdownPhase::Post) && self.dynamic_filter.is_some() {
            // let child_accepts_dyn_filter = child_pushdown_result
            //     .self_filters
            //     .first()
            //     .map(|filters| {
            //         assert_eq_or_internal_err!(
            //             filters.len(),
            //             1,
            //             "Aggregate only pushdown one self dynamic filter"
            //         );
            //         let filter = filters.get(0).unwrap(); // Asserted above
            //         Ok(matches!(filter.discriminant, PushedDown::Yes))
            //     })
            //     .unwrap_or_else(|| internal_err!("The length of self filters equals to the number of child of this ExecutionPlan, so it must be 1"))?;

            // HACK: The above snippet should be used, however, now the child reply
            // `PushDown::No` can indicate they're not able to push down row-level
            // filter, but still keep the filter for statistics pruning.
            // So here, we try to use ref count to determine if the dynamic filter
            // has actually be pushed down.
            // Issue: <https://github.com/apache/datafusion/issues/18856>
            let dyn_filter = self.dynamic_filter.as_ref().unwrap();
            let child_accepts_dyn_filter = Arc::strong_count(dyn_filter) > 1;

            if !child_accepts_dyn_filter {
                // Child can't consume the self dynamic filter, so disable it by setting
                // to `None`
                let mut new_node = self.clone();
                new_node.dynamic_filter = None;

                result = result
                    .with_updated_node(Arc::new(new_node) as Arc<dyn ExecutionPlan>);
            }
        }

        Ok(result)
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
                fields.extend(expr.state_fields()?.iter().cloned());
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
/// - `include_soft_requirement`: When `false`, only hard requirements are
///   considered, as indicated by [`AggregateFunctionExpr::order_sensitivity`]
///   returning [`AggregateOrderSensitivity::HardRequirement`].
///   Otherwise, also soft requirements ([`AggregateOrderSensitivity::SoftRequirement`])
///   are considered.
///
/// # Returns
///
/// A `LexOrdering` instance indicating the lexical ordering requirement for
/// the aggregate expression.
fn get_aggregate_expr_req(
    aggr_expr: &AggregateFunctionExpr,
    group_by: &PhysicalGroupBy,
    agg_mode: &AggregateMode,
    include_soft_requirement: bool,
) -> Option<LexOrdering> {
    // If the aggregation is performing a "second stage" calculation,
    // then ignore the ordering requirement. Ordering requirement applies
    // only to the aggregation input data.
    if !agg_mode.is_first_stage() {
        return None;
    }

    match aggr_expr.order_sensitivity() {
        AggregateOrderSensitivity::Insensitive => return None,
        AggregateOrderSensitivity::HardRequirement => {}
        AggregateOrderSensitivity::SoftRequirement => {
            if !include_soft_requirement {
                return None;
            }
        }
        AggregateOrderSensitivity::Beneficial => return None,
    }

    let mut sort_exprs = aggr_expr.order_bys().to_vec();
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
        sort_exprs.retain(|sort_expr| {
            !physical_exprs_contains(&physical_exprs, &sort_expr.expr)
        });
    }
    LexOrdering::new(sort_exprs)
}

/// Concatenates the given slices.
pub fn concat_slices<T: Clone>(lhs: &[T], rhs: &[T]) -> Vec<T> {
    [lhs, rhs].concat()
}

// Determines if the candidate ordering is finer than the current ordering.
// Returns `None` if they are incomparable, `Some(true)` if there is no current
// ordering or candidate ordering is finer, and `Some(false)` otherwise.
fn determine_finer(
    current: &Option<LexOrdering>,
    candidate: &LexOrdering,
) -> Option<bool> {
    if let Some(ordering) = current {
        candidate.partial_cmp(ordering).map(|cmp| cmp.is_gt())
    } else {
        Some(true)
    }
}

/// Gets the common requirement that satisfies all the aggregate expressions.
/// When possible, chooses the requirement that is already satisfied by the
/// equivalence properties.
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
/// A `Result<Vec<PhysicalSortRequirement>>` instance, which is the requirement
/// that satisfies all the aggregate requirements. Returns an error in case of
/// conflicting requirements.
pub fn get_finer_aggregate_exprs_requirement(
    aggr_exprs: &mut [Arc<AggregateFunctionExpr>],
    group_by: &PhysicalGroupBy,
    eq_properties: &EquivalenceProperties,
    agg_mode: &AggregateMode,
) -> Result<Vec<PhysicalSortRequirement>> {
    let mut requirement = None;

    // First try and find a match for all hard and soft requirements.
    // If a match can't be found, try a second time just matching hard
    // requirements.
    for include_soft_requirement in [false, true] {
        for aggr_expr in aggr_exprs.iter_mut() {
            let Some(aggr_req) = get_aggregate_expr_req(
                aggr_expr,
                group_by,
                agg_mode,
                include_soft_requirement,
            )
            .and_then(|o| eq_properties.normalize_sort_exprs(o)) else {
                // There is no aggregate ordering requirement, or it is trivially
                // satisfied -- we can skip this expression.
                continue;
            };
            // If the common requirement is finer than the current expression's,
            // we can skip this expression. If the latter is finer than the former,
            // adopt it if it is satisfied by the equivalence properties. Otherwise,
            // defer the analysis to the reverse expression.
            let forward_finer = determine_finer(&requirement, &aggr_req);
            if let Some(finer) = forward_finer {
                if !finer {
                    continue;
                } else if eq_properties.ordering_satisfy(aggr_req.clone())? {
                    requirement = Some(aggr_req);
                    continue;
                }
            }
            if let Some(reverse_aggr_expr) = aggr_expr.reverse_expr() {
                let Some(rev_aggr_req) = get_aggregate_expr_req(
                    &reverse_aggr_expr,
                    group_by,
                    agg_mode,
                    include_soft_requirement,
                )
                .and_then(|o| eq_properties.normalize_sort_exprs(o)) else {
                    // The reverse requirement is trivially satisfied -- just reverse
                    // the expression and continue with the next one:
                    *aggr_expr = Arc::new(reverse_aggr_expr);
                    continue;
                };
                // If the common requirement is finer than the reverse expression's,
                // just reverse it and continue the loop with the next aggregate
                // expression. If the latter is finer than the former, adopt it if
                // it is satisfied by the equivalence properties. Otherwise, adopt
                // the forward expression.
                if let Some(finer) = determine_finer(&requirement, &rev_aggr_req) {
                    if !finer {
                        *aggr_expr = Arc::new(reverse_aggr_expr);
                    } else if eq_properties.ordering_satisfy(rev_aggr_req.clone())? {
                        *aggr_expr = Arc::new(reverse_aggr_expr);
                        requirement = Some(rev_aggr_req);
                    } else {
                        requirement = Some(aggr_req);
                    }
                } else if forward_finer.is_some() {
                    requirement = Some(aggr_req);
                } else {
                    // Neither the existing requirement nor the current aggregate
                    // requirement satisfy the other (forward or reverse), this
                    // means they are conflicting. This is a problem only for hard
                    // requirements. Unsatisfied soft requirements can be ignored.
                    if !include_soft_requirement {
                        return not_impl_err!(
                            "Conflicting ordering requirements in aggregate functions is not supported"
                        );
                    }
                }
            }
        }
    }

    Ok(requirement.map_or_else(Vec::new, |o| o.into_iter().map(Into::into).collect()))
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
                result.extend(agg.order_bys().iter().map(|item| Arc::clone(&item.expr)));
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

/// Evaluates groups of expressions against a record batch.
pub fn evaluate_many(
    expr: &[Vec<Arc<dyn PhysicalExpr>>],
    batch: &RecordBatch,
) -> Result<Vec<Vec<ArrayRef>>> {
    expr.iter()
        .map(|expr| evaluate_expressions_to_arrays(expr, batch))
        .collect()
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
pub fn evaluate_group_by(
    group_by: &PhysicalGroupBy,
    batch: &RecordBatch,
) -> Result<Vec<Vec<ArrayRef>>> {
    let exprs = evaluate_expressions_to_arrays(
        group_by.expr.iter().map(|(expr, _)| expr),
        batch,
    )?;
    let null_exprs = evaluate_expressions_to_arrays(
        group_by.null_expr.iter().map(|(expr, _)| expr),
        batch,
    )?;

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
    use crate::common::collect;
    use crate::execution_plan::Boundedness;
    use crate::expressions::col;
    use crate::metrics::MetricValue;
    use crate::test::assert_is_pending;
    use crate::test::exec::{assert_strong_count_converges_to_zero, BlockingExec};
    use crate::test::TestMemoryExec;
    use crate::RecordBatchStream;

    use arrow::array::{
        DictionaryArray, Float32Array, Float64Array, Int32Array, StructArray,
        UInt32Array, UInt64Array,
    };
    use arrow::compute::{concat_batches, SortOptions};
    use arrow::datatypes::{DataType, Int32Type};
    use datafusion_common::test_util::{batches_to_sort_string, batches_to_string};
    use datafusion_common::{internal_err, DataFusionError, ScalarValue};
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::memory_pool::FairSpillPool;
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_functions_aggregate::array_agg::array_agg_udaf;
    use datafusion_functions_aggregate::average::avg_udaf;
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_aggregate::first_last::{first_value_udaf, last_value_udaf};
    use datafusion_functions_aggregate::median::median_udaf;
    use datafusion_functions_aggregate::sum::sum_udaf;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::lit;
    use datafusion_physical_expr::expressions::Literal;
    use datafusion_physical_expr::Partitioning;
    use datafusion_physical_expr::PhysicalSortExpr;

    use futures::{FutureExt, Stream};
    use insta::{allow_duplicates, assert_snapshot};

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
        let runtime = RuntimeEnvBuilder::new()
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

        if spill {
            // In spill mode, we test with the limited memory, if the mem usage exceeds,
            // we trigger the early emit rule, which turns out the partial aggregate result.
            allow_duplicates! {
            assert_snapshot!(batches_to_sort_string(&result),
            @r"
+---+-----+---------------+-----------------+
| a | b   | __grouping_id | COUNT(1)[count] |
+---+-----+---------------+-----------------+
|   | 1.0 | 2             | 1               |
|   | 1.0 | 2             | 1               |
|   | 2.0 | 2             | 1               |
|   | 2.0 | 2             | 1               |
|   | 3.0 | 2             | 1               |
|   | 3.0 | 2             | 1               |
|   | 4.0 | 2             | 1               |
|   | 4.0 | 2             | 1               |
| 2 |     | 1             | 1               |
| 2 |     | 1             | 1               |
| 2 | 1.0 | 0             | 1               |
| 2 | 1.0 | 0             | 1               |
| 3 |     | 1             | 1               |
| 3 |     | 1             | 2               |
| 3 | 2.0 | 0             | 2               |
| 3 | 3.0 | 0             | 1               |
| 4 |     | 1             | 1               |
| 4 |     | 1             | 2               |
| 4 | 3.0 | 0             | 1               |
| 4 | 4.0 | 0             | 2               |
+---+-----+---------------+-----------------+
            "
            );
            }
        } else {
            allow_duplicates! {
            assert_snapshot!(batches_to_sort_string(&result),
            @r"
+---+-----+---------------+-----------------+
| a | b   | __grouping_id | COUNT(1)[count] |
+---+-----+---------------+-----------------+
|   | 1.0 | 2             | 2               |
|   | 2.0 | 2             | 2               |
|   | 3.0 | 2             | 2               |
|   | 4.0 | 2             | 2               |
| 2 |     | 1             | 2               |
| 2 | 1.0 | 0             | 2               |
| 3 |     | 1             | 3               |
| 3 | 2.0 | 0             | 2               |
| 3 | 3.0 | 0             | 1               |
| 4 |     | 1             | 3               |
| 4 | 3.0 | 0             | 1               |
| 4 | 4.0 | 0             | 2               |
+---+-----+---------------+-----------------+
            "
            );
            }
        };

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

        allow_duplicates! {
        assert_snapshot!(
            batches_to_sort_string(&result),
            @r"
            +---+-----+---------------+----------+
            | a | b   | __grouping_id | COUNT(1) |
            +---+-----+---------------+----------+
            |   | 1.0 | 2             | 2        |
            |   | 2.0 | 2             | 2        |
            |   | 3.0 | 2             | 2        |
            |   | 4.0 | 2             | 2        |
            | 2 |     | 1             | 2        |
            | 2 | 1.0 | 0             | 2        |
            | 3 |     | 1             | 3        |
            | 3 | 2.0 | 0             | 2        |
            | 3 | 3.0 | 0             | 1        |
            | 4 |     | 1             | 3        |
            | 4 | 3.0 | 0             | 1        |
            | 4 | 4.0 | 0             | 2        |
            +---+-----+---------------+----------+
            "
        );
        }

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

        if spill {
            allow_duplicates! {
            assert_snapshot!(batches_to_sort_string(&result), @r"
                +---+---------------+-------------+
                | a | AVG(b)[count] | AVG(b)[sum] |
                +---+---------------+-------------+
                | 2 | 1             | 1.0         |
                | 2 | 1             | 1.0         |
                | 3 | 1             | 2.0         |
                | 3 | 2             | 5.0         |
                | 4 | 3             | 11.0        |
                +---+---------------+-------------+
            ");
            }
        } else {
            allow_duplicates! {
            assert_snapshot!(batches_to_sort_string(&result), @r"
                +---+---------------+-------------+
                | a | AVG(b)[count] | AVG(b)[sum] |
                +---+---------------+-------------+
                | 2 | 2             | 2.0         |
                | 3 | 3             | 7.0         |
                | 4 | 3             | 11.0        |
                +---+---------------+-------------+
            ");
            }
        };

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

        // Verify statistics are preserved proportionally through aggregation
        let final_stats = merged_aggregate.partition_statistics(None)?;
        assert!(final_stats.total_byte_size.get_value().is_some());

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

        allow_duplicates! {
        assert_snapshot!(batches_to_sort_string(&result), @r"
            +---+--------------------+
            | a | AVG(b)             |
            +---+--------------------+
            | 2 | 1.0                |
            | 3 | 2.3333333333333335 |
            | 4 | 3.6666666666666665 |
            +---+--------------------+
            ");
            // For row 2: 3, (2 + 3 + 2) / 3
            // For row 3: 4, (3 + 4 + 4) / 3
        }

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
            PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
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
                DisplayFormatType::TreeRender => {
                    // TODO: collect info
                    write!(f, "")
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
            self.partition_statistics(None)
        }

        fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
            if partition.is_some() {
                return Ok(Statistics::new_unknown(self.schema().as_ref()));
            }
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

        let runtime = RuntimeEnvBuilder::new()
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

            let stream = partial_aggregate.execute_typed(0, &task_ctx)?;

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
        let order_bys = vec![PhysicalSortExpr {
            expr: col("b", schema)?,
            options: sort_options,
        }];
        let args = [col("b", schema)?];

        AggregateExprBuilder::new(first_value_udaf(), args.to_vec())
            .order_by(order_bys)
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
        let order_bys = vec![PhysicalSortExpr {
            expr: col("b", schema)?,
            options: sort_options,
        }];
        let args = [col("b", schema)?];
        AggregateExprBuilder::new(last_value_udaf(), args.to_vec())
            .order_by(order_bys)
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
    // "        DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]",
    //
    // or
    //
    // "AggregateExec: mode=Final, gby=[a@0 as a], aggr=[FIRST_VALUE(b)]",
    // "  CoalescePartitionsExec",
    // "    AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[FIRST_VALUE(b)], ordering_mode=None",
    // "      DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]",
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

        let memory_exec = TestMemoryExec::try_new_exec(
            &[
                vec![partition1],
                vec![partition2],
                vec![partition3],
                vec![partition4],
            ],
            Arc::clone(&schema),
            None,
        )?;
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
            allow_duplicates! {
            assert_snapshot!(batches_to_string(&result), @r"
                +---+--------------------------------------------+
                | a | first_value(b) ORDER BY [b ASC NULLS LAST] |
                +---+--------------------------------------------+
                | 2 | 0.0                                        |
                | 3 | 1.0                                        |
                | 4 | 3.0                                        |
                +---+--------------------------------------------+
                ");
            }
        } else {
            allow_duplicates! {
            assert_snapshot!(batches_to_string(&result), @r"
                +---+-------------------------------------------+
                | a | last_value(b) ORDER BY [b ASC NULLS LAST] |
                +---+-------------------------------------------+
                | 2 | 3.0                                       |
                | 3 | 5.0                                       |
                | 4 | 6.0                                       |
                +---+-------------------------------------------+
                ");
            }
        };
        Ok(())
    }

    #[tokio::test]
    async fn test_get_finest_requirements() -> Result<()> {
        let test_schema = create_test_schema()?;

        let options = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let col_a = &col("a", &test_schema)?;
        let col_b = &col("b", &test_schema)?;
        let col_c = &col("c", &test_schema)?;
        let mut eq_properties = EquivalenceProperties::new(Arc::clone(&test_schema));
        // Columns a and b are equal.
        eq_properties.add_equal_conditions(Arc::clone(col_a), Arc::clone(col_b))?;
        // Aggregate requirements are
        // [None], [a ASC], [a ASC, b ASC, c ASC], [a ASC, b ASC] respectively
        let order_by_exprs = vec![
            vec![],
            vec![PhysicalSortExpr {
                expr: Arc::clone(col_a),
                options,
            }],
            vec![
                PhysicalSortExpr {
                    expr: Arc::clone(col_a),
                    options,
                },
                PhysicalSortExpr {
                    expr: Arc::clone(col_b),
                    options,
                },
                PhysicalSortExpr {
                    expr: Arc::clone(col_c),
                    options,
                },
            ],
            vec![
                PhysicalSortExpr {
                    expr: Arc::clone(col_a),
                    options,
                },
                PhysicalSortExpr {
                    expr: Arc::clone(col_b),
                    options,
                },
            ],
        ];

        let common_requirement = vec![
            PhysicalSortRequirement::new(Arc::clone(col_a), Some(options)),
            PhysicalSortRequirement::new(Arc::clone(col_c), Some(options)),
        ];
        let mut aggr_exprs = order_by_exprs
            .into_iter()
            .map(|order_by_expr| {
                AggregateExprBuilder::new(array_agg_udaf(), vec![Arc::clone(col_a)])
                    .alias("a")
                    .order_by(order_by_expr)
                    .schema(Arc::clone(&test_schema))
                    .build()
                    .map(Arc::new)
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let group_by = PhysicalGroupBy::new_single(vec![]);
        let result = get_finer_aggregate_exprs_requirement(
            &mut aggr_exprs,
            &group_by,
            &eq_properties,
            &AggregateMode::Partial,
        )?;
        assert_eq!(result, common_requirement);
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

        let input =
            TestMemoryExec::try_new_exec(&[input_batches], Arc::clone(&schema), None)?;

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

        allow_duplicates! {
        assert_snapshot!(batches_to_sort_string(&output), @r"
            +-----+-----+-------+---------------+-------+
            | a   | b   | const | __grouping_id | 1     |
            +-----+-----+-------+---------------+-------+
            |     |     | 1     | 6             | 32768 |
            |     | 0.0 |       | 5             | 32768 |
            | 0.0 |     |       | 3             | 32768 |
            +-----+-----+-------+---------------+-------+
        ");
        }

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
                            Field::new(
                                "a".to_string(),
                                DataType::Dictionary(
                                    Box::new(DataType::Int32),
                                    Box::new(DataType::Utf8),
                                ),
                                true,
                            ),
                            Field::new(
                                "b".to_string(),
                                DataType::Dictionary(
                                    Box::new(DataType::Int32),
                                    Box::new(DataType::Utf8),
                                ),
                                true,
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
                        Arc::new(Field::new(
                            "a".to_string(),
                            DataType::Dictionary(
                                Box::new(DataType::Int32),
                                Box::new(DataType::Utf8),
                            ),
                            true,
                        )),
                        Arc::new(
                            vec![Some("a"), None, Some("a")]
                                .into_iter()
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new(
                            "b".to_string(),
                            DataType::Dictionary(
                                Box::new(DataType::Int32),
                                Box::new(DataType::Utf8),
                            ),
                            true,
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

        let input = TestMemoryExec::try_new_exec(
            &[vec![batch.clone()]],
            Arc::<Schema>::clone(&batch.schema()),
            None,
        )?;
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

        allow_duplicates! {
        assert_snapshot!(batches_to_string(&output), @r"
            +--------------+------------+
            | labels       | SUM(value) |
            +--------------+------------+
            | {a: a, b: b} | 2          |
            | {a: , b: c}  | 1          |
            +--------------+------------+
            ");
        }

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

        let input =
            TestMemoryExec::try_new_exec(&[input_data], Arc::clone(&schema), None)?;
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

        allow_duplicates! {
            assert_snapshot!(batches_to_string(&output), @r"
            +-----+-------------------+
            | key | COUNT(val)[count] |
            +-----+-------------------+
            | 1   | 1                 |
            | 2   | 1                 |
            | 3   | 1                 |
            | 2   | 1                 |
            | 3   | 1                 |
            | 4   | 1                 |
            +-----+-------------------+
            ");
        }

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

        let input =
            TestMemoryExec::try_new_exec(&[input_data], Arc::clone(&schema), None)?;
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

        allow_duplicates! {
            assert_snapshot!(batches_to_string(&output), @r"
            +-----+-------------------+
            | key | COUNT(val)[count] |
            +-----+-------------------+
            | 1   | 1                 |
            | 2   | 2                 |
            | 3   | 2                 |
            | 4   | 1                 |
            | 2   | 1                 |
            | 3   | 1                 |
            | 4   | 1                 |
            +-----+-------------------+
            ");
        }

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

    // test for https://github.com/apache/datafusion/issues/13949
    async fn run_test_with_spill_pool_if_necessary(
        pool_size: usize,
        expect_spill: bool,
    ) -> Result<()> {
        fn create_record_batch(
            schema: &Arc<Schema>,
            data: (Vec<u32>, Vec<f64>),
        ) -> Result<RecordBatch> {
            Ok(RecordBatch::try_new(
                Arc::clone(schema),
                vec![
                    Arc::new(UInt32Array::from(data.0)),
                    Arc::new(Float64Array::from(data.1)),
                ],
            )?)
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, false),
            Field::new("b", DataType::Float64, false),
        ]));

        let batches = vec![
            create_record_batch(&schema, (vec![2, 3, 4, 4], vec![1.0, 2.0, 3.0, 4.0]))?,
            create_record_batch(&schema, (vec![2, 3, 4, 4], vec![1.0, 2.0, 3.0, 4.0]))?,
        ];
        let plan: Arc<dyn ExecutionPlan> =
            TestMemoryExec::try_new_exec(&[batches], Arc::clone(&schema), None)?;

        let grouping_set = PhysicalGroupBy::new(
            vec![(col("a", &schema)?, "a".to_string())],
            vec![],
            vec![vec![false]],
        );

        // Test with MIN for simple intermediate state (min) and AVG for multiple intermediate states (partial sum, partial count).
        let aggregates: Vec<Arc<AggregateFunctionExpr>> = vec![
            Arc::new(
                AggregateExprBuilder::new(
                    datafusion_functions_aggregate::min_max::min_udaf(),
                    vec![col("b", &schema)?],
                )
                .schema(Arc::clone(&schema))
                .alias("MIN(b)")
                .build()?,
            ),
            Arc::new(
                AggregateExprBuilder::new(avg_udaf(), vec![col("b", &schema)?])
                    .schema(Arc::clone(&schema))
                    .alias("AVG(b)")
                    .build()?,
            ),
        ];

        let single_aggregate = Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            grouping_set,
            aggregates,
            vec![None, None],
            plan,
            Arc::clone(&schema),
        )?);

        let batch_size = 2;
        let memory_pool = Arc::new(FairSpillPool::new(pool_size));
        let task_ctx = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::new().with_batch_size(batch_size))
                .with_runtime(Arc::new(
                    RuntimeEnvBuilder::new()
                        .with_memory_pool(memory_pool)
                        .build()?,
                )),
        );

        let result = collect(single_aggregate.execute(0, Arc::clone(&task_ctx))?).await?;

        assert_spill_count_metric(expect_spill, single_aggregate);

        allow_duplicates! {
            assert_snapshot!(batches_to_string(&result), @r"
                +---+--------+--------+
                | a | MIN(b) | AVG(b) |
                +---+--------+--------+
                | 2 | 1.0    | 1.0    |
                | 3 | 2.0    | 2.0    |
                | 4 | 3.0    | 3.5    |
                +---+--------+--------+
            ");
        }

        Ok(())
    }

    fn assert_spill_count_metric(
        expect_spill: bool,
        single_aggregate: Arc<AggregateExec>,
    ) {
        if let Some(metrics_set) = single_aggregate.metrics() {
            let mut spill_count = 0;

            // Inspect metrics for SpillCount
            for metric in metrics_set.iter() {
                if let MetricValue::SpillCount(count) = metric.value() {
                    spill_count = count.value();
                    break;
                }
            }

            if expect_spill && spill_count == 0 {
                panic!(
                    "Expected spill but SpillCount metric not found or SpillCount was 0."
                );
            } else if !expect_spill && spill_count > 0 {
                panic!("Expected no spill but found SpillCount metric with value greater than 0.");
            }
        } else {
            panic!("No metrics returned from the operator; cannot verify spilling.");
        }
    }

    #[tokio::test]
    async fn test_aggregate_with_spill_if_necessary() -> Result<()> {
        // test with spill
        run_test_with_spill_pool_if_necessary(2_000, true).await?;
        // test without spill
        run_test_with_spill_pool_if_necessary(20_000, false).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_aggregate_statistics_edge_cases() -> Result<()> {
        use crate::test::exec::StatisticsExec;
        use datafusion_common::ColumnStatistics;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Float64, false),
        ]));

        // Test 1: Absent statistics remain absent
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Exact(100),
                total_byte_size: Precision::Absent,
                column_statistics: vec![
                    ColumnStatistics::new_unknown(),
                    ColumnStatistics::new_unknown(),
                ],
            },
            (*schema).clone(),
        )) as Arc<dyn ExecutionPlan>;

        let agg = Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            PhysicalGroupBy::default(),
            vec![Arc::new(
                AggregateExprBuilder::new(count_udaf(), vec![col("a", &schema)?])
                    .schema(Arc::clone(&schema))
                    .alias("COUNT(a)")
                    .build()?,
            )],
            vec![None],
            input,
            Arc::clone(&schema),
        )?);

        let stats = agg.partition_statistics(None)?;
        assert_eq!(stats.total_byte_size, Precision::Absent);

        // Test 2: Zero rows returns Absent (can't estimate output size from zero input)
        let input_zero = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Exact(0),
                total_byte_size: Precision::Exact(0),
                column_statistics: vec![
                    ColumnStatistics::new_unknown(),
                    ColumnStatistics::new_unknown(),
                ],
            },
            (*schema).clone(),
        )) as Arc<dyn ExecutionPlan>;

        let agg_zero = Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            PhysicalGroupBy::default(),
            vec![Arc::new(
                AggregateExprBuilder::new(count_udaf(), vec![col("a", &schema)?])
                    .schema(Arc::clone(&schema))
                    .alias("COUNT(a)")
                    .build()?,
            )],
            vec![None],
            input_zero,
            Arc::clone(&schema),
        )?);

        let stats_zero = agg_zero.partition_statistics(None)?;
        assert_eq!(stats_zero.total_byte_size, Precision::Absent);

        Ok(())
    }
}
