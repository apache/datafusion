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

use std::borrow::Cow;
use std::sync::Arc;

use super::{DisplayAs, ExecutionPlanProperties, PlanProperties};
use crate::aggregates::{
    hash_aggregate::{FinalHashAggregateStream, PartialHashAggregateStream},
    no_grouping::AggregateStream,
    row_hash::GroupedHashAggregateStream,
    topk_stream::GroupedTopKAggregateStream,
};
use crate::execution_plan::{CardinalityEffect, EmissionType};
use crate::filter_pushdown::{
    ChildFilterDescription, ChildPushdownResult, FilterDescription, FilterPushdownPhase,
    FilterPushdownPropagation, PushedDownPredicate,
};
use crate::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::statistics::StatisticsArgs;
use crate::{
    DisplayFormatType, Distribution, ExecutionPlan, InputOrderMode,
    SendableRecordBatchStream, Statistics, check_if_same_properties,
};
use datafusion_common::config::ConfigOptions;
use datafusion_physical_expr::utils::collect_columns;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};

use arrow::array::{ArrayRef, UInt8Array, UInt16Array, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_schema::FieldRef;
use datafusion_common::stats::Precision;
use datafusion_common::{
    Constraint, Constraints, Result, ScalarValue, assert_eq_or_internal_err,
    internal_err, not_impl_err,
};
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::MemoryLimit;
use datafusion_expr::{Accumulator, Aggregate};
use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
use datafusion_physical_expr::equivalence::ProjectionMapping;
use datafusion_physical_expr::expressions::{Column, DynamicFilterPhysicalExpr, lit};
use datafusion_physical_expr::{
    ConstExpr, EquivalenceProperties, physical_exprs_contains,
};
use datafusion_physical_expr_common::physical_expr::{PhysicalExpr, fmt_sql};
use datafusion_physical_expr_common::sort_expr::{
    LexOrdering, LexRequirement, OrderingRequirements, PhysicalSortRequirement,
};

use datafusion_expr::utils::AggregateOrderSensitivity;
use datafusion_physical_expr_common::utils::evaluate_expressions_to_arrays;
use itertools::Itertools;
use topk::hash_table::is_supported_hash_key_type;
use topk::heap::is_supported_heap_type;

mod aggregate_hash_table;
pub mod group_values;
mod hash_aggregate;
mod no_grouping;
pub mod order;
mod row_hash;
mod skip_partial;
mod topk;
mod topk_stream;

/// Returns true if TopK aggregation data structures support the provided key and value types.
///
/// This function checks whether both the key type (used for grouping) and value type
/// (used in min/max aggregation) can be handled by the TopK aggregation heap and hash table.
/// Supported types include Arrow primitives (integers, floats, decimals, intervals) and
/// UTF-8 strings (`Utf8`, `LargeUtf8`, `Utf8View`).
/// ```text
pub fn topk_types_supported(key_type: &DataType, value_type: &DataType) -> bool {
    is_supported_hash_key_type(key_type) && is_supported_heap_type(value_type)
}

/// Hard-coded seed for aggregations to ensure hash values differ from `RepartitionExec`, avoiding collisions.
const AGGREGATION_HASH_SEED: datafusion_common::hash_utils::RandomState =
    // This seed is chosen to be a large 64-bit number
    datafusion_common::hash_utils::RandomState::with_seed(15395726432021054657);

/// Whether an aggregate stage consumes raw input data or intermediate
/// accumulator state from a previous aggregation stage.
///
/// See the [table on `AggregateMode`](AggregateMode#variants-and-their-inputoutput-modes)
/// for how this relates to aggregate modes.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum AggregateInputMode {
    /// The stage consumes raw, unaggregated input data and calls
    /// [`Accumulator::update_batch`].
    Raw,
    /// The stage consumes intermediate accumulator state from a previous
    /// aggregation stage and calls [`Accumulator::merge_batch`].
    Partial,
}

/// Whether an aggregate stage produces intermediate accumulator state
/// or final output values.
///
/// See the [table on `AggregateMode`](AggregateMode#variants-and-their-inputoutput-modes)
/// for how this relates to aggregate modes.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum AggregateOutputMode {
    /// The stage produces intermediate accumulator state, serialized via
    /// [`Accumulator::state`].
    Partial,
    /// The stage produces final output values via
    /// [`Accumulator::evaluate`].
    Final,
}

/// Aggregation modes
///
/// See [`Accumulator::state`] for background information on multi-phase
/// aggregation and how these modes are used.
///
/// # Variants and their input/output modes
///
/// Each variant can be characterized by its [`AggregateInputMode`] and
/// [`AggregateOutputMode`]:
///
/// ```text
///                       | Input: Raw data           | Input: Partial state
/// Output: Final values  | Single, SinglePartitioned | Final, FinalPartitioned
/// Output: Partial state | Partial                   | PartialReduce
/// ```
///
/// Use [`AggregateMode::input_mode`] and [`AggregateMode::output_mode`]
/// to query these properties.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
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
    /// Combine multiple partial aggregations to produce a new partial
    /// aggregation.
    ///
    /// Input is intermediate accumulator state (like Final), but output is
    /// also intermediate accumulator state (like Partial). This enables
    /// tree-reduce aggregation strategies where partial results from
    /// multiple workers are combined in multiple stages before a final
    /// evaluation.
    ///
    /// ```text
    ///               Final
    ///            /        \
    ///     PartialReduce   PartialReduce
    ///     /         \      /         \
    ///  Partial   Partial  Partial   Partial
    /// ```
    ///
    /// # Motivation
    ///
    /// This reduces shuffling traffic in a distributed setting. See
    /// <https://github.com/datafusion-contrib/datafusion-distributed/issues/360>
    /// for details.
    PartialReduce,
}

impl AggregateMode {
    /// Returns the [`AggregateInputMode`] for this mode: whether this
    /// stage consumes raw input data or intermediate accumulator state.
    ///
    /// See the [table above](AggregateMode#variants-and-their-inputoutput-modes)
    /// for details.
    pub fn input_mode(&self) -> AggregateInputMode {
        match self {
            AggregateMode::Partial
            | AggregateMode::Single
            | AggregateMode::SinglePartitioned => AggregateInputMode::Raw,
            AggregateMode::Final
            | AggregateMode::FinalPartitioned
            | AggregateMode::PartialReduce => AggregateInputMode::Partial,
        }
    }

    /// Returns the [`AggregateOutputMode`] for this mode: whether this
    /// stage produces intermediate accumulator state or final output values.
    ///
    /// See the [table above](AggregateMode#variants-and-their-inputoutput-modes)
    /// for details.
    pub fn output_mode(&self) -> AggregateOutputMode {
        match self {
            AggregateMode::Final
            | AggregateMode::FinalPartitioned
            | AggregateMode::Single
            | AggregateMode::SinglePartitioned => AggregateOutputMode::Final,
            AggregateMode::Partial | AggregateMode::PartialReduce => {
                AggregateOutputMode::Partial
            }
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
    /// True when GROUPING SETS/CUBE/ROLLUP are used so `__grouping_id` should
    /// be included in the output schema.
    has_grouping_set: bool,
}

impl PhysicalGroupBy {
    /// Create a new `PhysicalGroupBy`
    pub fn new(
        expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
        null_expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
        groups: Vec<Vec<bool>>,
        has_grouping_set: bool,
    ) -> Self {
        Self {
            expr,
            null_expr,
            groups,
            has_grouping_set,
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
            has_grouping_set: false,
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

    /// Returns true if this has no grouping at all (including no GROUPING SETS)
    pub fn is_true_no_grouping(&self) -> bool {
        self.is_empty() && !self.has_grouping_set
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

    /// Returns true if this grouping uses GROUPING SETS, CUBE or ROLLUP.
    pub fn has_grouping_set(&self) -> bool {
        self.has_grouping_set
    }

    /// Returns true if this `PhysicalGroupBy` has no group expressions
    pub fn is_empty(&self) -> bool {
        self.expr.is_empty()
    }

    /// Returns true if this is a "simple" GROUP BY (not using GROUPING SETS/CUBE/ROLLUP).
    /// This determines whether the `__grouping_id` column is included in the output schema.
    pub fn is_single(&self) -> bool {
        !self.has_grouping_set
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
        if self.has_grouping_set {
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
        if self.has_grouping_set {
            output_exprs.push(Arc::new(Column::new(
                Aggregate::INTERNAL_GROUPING_ID,
                self.expr.len(),
            )) as _);
        }
        output_exprs
    }

    /// Returns the number expression as grouping keys.
    pub fn num_group_exprs(&self) -> usize {
        self.expr.len() + usize::from(self.has_grouping_set)
    }

    /// Returns the Arrow data type of the `__grouping_id` column.
    ///
    /// The type is chosen to be wide enough to hold both the semantic bitmask
    /// (in the low `n` bits, where `n` is the number of grouping expressions)
    /// and the duplicate ordinal (in the high bits).
    fn grouping_id_data_type(&self) -> DataType {
        Aggregate::grouping_id_type(self.expr.len(), max_duplicate_ordinal(&self.groups))
    }

    pub fn group_schema(&self, schema: &Schema) -> Result<SchemaRef> {
        Ok(Arc::new(Schema::new(self.group_fields(schema)?)))
    }

    /// Returns the fields that are used as the grouping keys.
    fn group_fields(&self, input_schema: &Schema) -> Result<Vec<FieldRef>> {
        let mut fields = Vec::with_capacity(self.num_group_exprs());
        for ((expr, name), group_expr_nullable) in
            self.expr.iter().zip(self.exprs_nullable())
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
        if self.has_grouping_set {
            fields.push(
                Field::new(
                    Aggregate::INTERNAL_GROUPING_ID,
                    self.grouping_id_data_type(),
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
        let groups = if self.expr.is_empty() && !self.has_grouping_set {
            // No GROUP BY expressions - should have no groups
            vec![]
        } else {
            vec![vec![false; num_exprs]]
        };
        Self {
            expr,
            null_expr: vec![],
            groups,
            has_grouping_set: false,
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
            && self.has_grouping_set == other.has_grouping_set
    }
}

/// Streams used by [`AggregateExec`].
///
/// # Stream Variant Schema Notation
/// For example, `SELECT g, AVG(x) FROM t GROUP BY g` uses these schemas:
///
/// ```text
/// initial input:              [g, x]
/// partial state:              [g, AVG(x) state columns, e.g. sum/count]
/// final result:               [g, AVG(x)]
/// ```
#[expect(clippy::large_enum_variant)]
enum StreamType {
    /// Single group (no group by) aggregate stream.
    /// Input output scheme: initial input -> final result
    AggregateStream(AggregateStream),
    /// Partial stage of the hash aggregation
    /// Input output scheme: initial input -> partial state
    PartialHash(PartialHashAggregateStream),
    /// Final stage of the hash aggregation
    /// Input output scheme: partial state -> final result
    FinalHash(FinalHashAggregateStream),
    /// Hash aggregation reused for multiple stages
    ///
    /// Note this is being incrementally migrated to dedicated streams like
    /// [`StreamType::PartialHash`] and [`StreamType::FinalHash`]
    ///
    /// See issue for details: <https://github.com/apache/datafusion/issues/22710>
    GroupedHash(GroupedHashAggregateStream),
    /// Grouped TopK aggregate stream.
    /// Input output scheme: initial input -> final result
    ///
    /// Used for grouped aggregation with LIMIT / ordering, where the stream keeps
    /// only the top groups required by the query.
    GroupedPriorityQueue(GroupedTopKAggregateStream),
}

impl From<StreamType> for SendableRecordBatchStream {
    fn from(stream: StreamType) -> Self {
        match stream {
            StreamType::AggregateStream(stream) => Box::pin(stream),
            StreamType::PartialHash(stream) => Box::pin(stream),
            StreamType::FinalHash(stream) => Box::pin(stream),
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
/// can evaluate column statistics on those dynamic filters, to decide if they can
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

/// Configuration for limit-based optimizations in aggregation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LimitOptions {
    /// The maximum number of rows to return
    pub limit: usize,
    /// Optional ordering direction (true = descending, false = ascending)
    /// This is used for TopK aggregation to maintain a priority queue with the correct ordering
    pub descending: Option<bool>,
}

impl LimitOptions {
    /// Create a new LimitOptions with a limit and no specific ordering
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            descending: None,
        }
    }

    /// Create a new LimitOptions with a limit and ordering direction
    pub fn new_with_order(limit: usize, descending: bool) -> Self {
        Self {
            limit,
            descending: Some(descending),
        }
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn descending(&self) -> Option<bool> {
        self.descending
    }
}

/// Hash aggregate execution plan
#[derive(Debug, Clone)]
pub struct AggregateExec {
    /// Aggregation mode (full, partial)
    mode: AggregateMode,
    /// Group by expressions
    /// [`Arc`] used for a cheap clone, which improves physical plan optimization performance.
    group_by: Arc<PhysicalGroupBy>,
    /// Aggregate expressions
    /// The same reason to [`Arc`] it as for [`Self::group_by`].
    aggr_expr: Arc<[Arc<AggregateFunctionExpr>]>,
    /// FILTER (WHERE clause) expression for each aggregate expression
    /// The same reason to [`Arc`] it as for [`Self::group_by`].
    filter_expr: Arc<[Option<Arc<dyn PhysicalExpr>>]>,
    /// Configuration for limit-based optimizations
    limit_options: Option<LimitOptions>,
    /// Input plan, could be a partial aggregate or the input to the aggregate
    pub input: Arc<dyn ExecutionPlan>,
    /// Schema after the aggregate is applied. Contains the group by columns followed by the
    /// aggregate outputs.
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
    cache: Arc<PlanProperties>,
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
        aggr_expr: impl Into<Arc<[Arc<AggregateFunctionExpr>]>>,
    ) -> Self {
        Self {
            aggr_expr: aggr_expr.into(),
            // clone the rest of the fields
            required_input_ordering: self.required_input_ordering.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
            input_order_mode: self.input_order_mode.clone(),
            cache: Arc::clone(&self.cache),
            mode: self.mode,
            group_by: Arc::clone(&self.group_by),
            filter_expr: Arc::clone(&self.filter_expr),
            limit_options: self.limit_options,
            input: Arc::clone(&self.input),
            schema: Arc::clone(&self.schema),
            input_schema: Arc::clone(&self.input_schema),
            dynamic_filter: self.dynamic_filter.clone(),
        }
    }

    /// Clone this exec, overriding only the limit hint.
    pub fn with_new_limit_options(&self, limit_options: Option<LimitOptions>) -> Self {
        Self {
            limit_options,
            // clone the rest of the fields
            required_input_ordering: self.required_input_ordering.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
            input_order_mode: self.input_order_mode.clone(),
            cache: Arc::clone(&self.cache),
            mode: self.mode,
            group_by: Arc::clone(&self.group_by),
            aggr_expr: Arc::clone(&self.aggr_expr),
            filter_expr: Arc::clone(&self.filter_expr),
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
        group_by: impl Into<Arc<PhysicalGroupBy>>,
        aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
        filter_expr: Vec<Option<Arc<dyn PhysicalExpr>>>,
        input: Arc<dyn ExecutionPlan>,
        input_schema: SchemaRef,
    ) -> Result<Self> {
        let group_by = group_by.into();
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
        group_by: impl Into<Arc<PhysicalGroupBy>>,
        mut aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
        filter_expr: impl Into<Arc<[Option<Arc<dyn PhysicalExpr>>]>>,
        input: Arc<dyn ExecutionPlan>,
        input_schema: SchemaRef,
        schema: SchemaRef,
    ) -> Result<Self> {
        let group_by = group_by.into();
        let filter_expr = filter_expr.into();

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
        // Copy the `PhysicalSortExpr`s to retain the sort options.
        let (new_sort_exprs, indices) =
            input_eq_properties.find_longest_permutation(&groupby_exprs)?;

        let mut new_requirements = new_sort_exprs
            .into_iter()
            .map(PhysicalSortRequirement::from)
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
            group_by.is_true_no_grouping(),
            &mode,
            &input_order_mode,
            aggr_expr.as_ref(),
        )?;

        let mut exec = AggregateExec {
            mode,
            group_by,
            aggr_expr: aggr_expr.into(),
            filter_expr,
            input,
            schema,
            input_schema,
            metrics: ExecutionPlanMetricsSet::new(),
            required_input_ordering,
            limit_options: None,
            input_order_mode,
            cache: Arc::new(cache),
            dynamic_filter: None,
        };

        exec.init_dynamic_filter();

        Ok(exec)
    }

    /// Aggregation mode (full, partial)
    pub fn mode(&self) -> &AggregateMode {
        &self.mode
    }

    /// Set the limit options for this AggExec
    pub fn with_limit_options(mut self, limit_options: Option<LimitOptions>) -> Self {
        self.limit_options = limit_options;
        self
    }

    /// Get the limit options (if set)
    pub fn limit_options(&self) -> Option<LimitOptions> {
        self.limit_options
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

    /// Returns the dynamic filter expression for this aggregate, if set.
    pub fn dynamic_filter_expr(&self) -> Option<&Arc<DynamicFilterPhysicalExpr>> {
        self.dynamic_filter.as_ref().map(|df| &df.filter)
    }

    /// Replace the dynamic filter expression. This method errors if the aggregate does not
    /// support dynamic filtering or if the filter expression is incompatible with this
    /// [`AggregateExec`].
    pub fn with_dynamic_filter_expr(
        mut self,
        filter: Arc<DynamicFilterPhysicalExpr>,
    ) -> Result<Self> {
        // If there is no dynamic filter state initialized via `try_new`, then
        // we can safely assume that the aggregate does not support dynamic filtering.
        let Some(dyn_filter) = self.dynamic_filter.as_ref() else {
            return internal_err!("Aggregate does not support dynamic filtering");
        };

        // Validate that the filter is compatible with the aggregation columns.
        let cols = self.cols_for_dynamic_filter(&dyn_filter.supported_accumulators_info);
        if cols.len() != filter.children().len() {
            return internal_err!(
                "Dynamic filter expression is incompatible with aggregate due to mismatched number of columns"
            );
        }
        for (col, child) in cols.iter().zip(filter.children()) {
            if !col.eq(child) {
                return internal_err!(
                    "Dynamic filter expression is incompatible with aggregate due to mismatched column references {col} != {child}"
                );
            }
        }

        // Overwrite our filter
        self.dynamic_filter = Some(Arc::new(AggrDynFilter {
            filter,
            supported_accumulators_info: dyn_filter.supported_accumulators_info.clone(),
        }));
        Ok(self)
    }

    /// Input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Get the input schema before any aggregates are applied
    pub fn input_schema(&self) -> SchemaRef {
        Arc::clone(&self.input_schema)
    }

    fn execute_typed(
        &self,
        partition: usize,
        context: &Arc<TaskContext>,
    ) -> Result<StreamType> {
        if self.group_by.is_true_no_grouping() {
            return Ok(StreamType::AggregateStream(AggregateStream::new(
                self, context, partition,
            )?));
        }

        // grouping by an expression that has a sort/limit upstream
        if let Some(config) = self.limit_options
            && !self.is_unordered_unfiltered_group_by_distinct()
        {
            return Ok(StreamType::GroupedPriorityQueue(
                GroupedTopKAggregateStream::new(self, context, partition, config.limit)?,
            ));
        }

        // `GroupedHashAggregateStream` is being incrementally refactored. See the
        // tracking issue for details.
        //
        // New features and improvements should go directly into the new implementation.
        // Please coordinate through the tracking issue.
        //
        // Issue: <https://github.com/apache/datafusion/issues/22710>
        if context
            .session_config()
            .options()
            .execution
            .enable_migration_aggregate
        {
            if self.should_use_partial_hash_stream(context) {
                return Ok(StreamType::PartialHash(PartialHashAggregateStream::new(
                    self, context, partition,
                )?));
            }

            if self.should_use_final_hash_stream(context) {
                return Ok(StreamType::FinalHash(FinalHashAggregateStream::new(
                    self, context, partition,
                )?));
            }
        }

        // Execution paths that have not been migrated use the fallback implementation
        Ok(StreamType::GroupedHash(GroupedHashAggregateStream::new(
            self, context, partition,
        )?))
    }

    fn should_use_partial_hash_stream(&self, context: &TaskContext) -> bool {
        // TODO: implement memory-limited path and remove this limitation
        if matches!(context.memory_pool().memory_limit(), MemoryLimit::Finite(_)) {
            return false;
        }

        self.mode == AggregateMode::Partial
            && self.input_order_mode == InputOrderMode::Linear
            && !self.group_by.is_true_no_grouping()
            && self.group_by.is_single()
            && self.limit_options_supported_by_hash_stream()
    }

    fn should_use_final_hash_stream(&self, context: &TaskContext) -> bool {
        // TODO: implement memory-limited path and remove this limitation
        if matches!(context.memory_pool().memory_limit(), MemoryLimit::Finite(_)) {
            return false;
        }

        matches!(
            self.mode,
            AggregateMode::Final | AggregateMode::FinalPartitioned
        ) && self.limit_options_supported_by_hash_stream()
            && self.input_order_mode == InputOrderMode::Linear
            && !self.group_by.is_true_no_grouping()
            && self.group_by.is_single()
    }

    /// See comments in `PartialHashAggregateStream` limit optimization section
    fn limit_options_supported_by_hash_stream(&self) -> bool {
        self.limit_options.is_none() || self.is_unordered_unfiltered_group_by_distinct()
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
        if self
            .limit_options()
            .and_then(|config| config.descending)
            .is_some()
        {
            return false;
        }
        // ensure there is a group by
        if self.group_expr().is_empty() && !self.group_expr().has_grouping_set() {
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
        is_true_no_grouping: bool,
        mode: &AggregateMode,
        input_order_mode: &InputOrderMode,
        aggr_exprs: &[Arc<AggregateFunctionExpr>],
    ) -> Result<PlanProperties> {
        // Construct equivalence properties:
        let mut eq_properties = input
            .equivalence_properties()
            .project(group_expr_mapping, schema);

        // True no-group aggregates produce only one row in each output
        // partition, so aggregate outputs are constants within the partition.
        // Grouping sets with empty grouping expressions are not covered here:
        // their output schema can include grouping-set columns before the
        // aggregate columns, so this aggregate-column mapping does not apply.
        if is_true_no_grouping {
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
                        expr.downcast_ref::<Column>().map(|c| c.index())
                    })
                })
                .collect(),
        );
        constraints.push(new_constraint);
        eq_properties =
            eq_properties.with_constraints(Constraints::new_unverified(constraints));

        // Get output partitioning:
        let input_partitioning = input.output_partitioning().clone();
        let output_partitioning = match mode.input_mode() {
            AggregateInputMode::Raw => {
                // First stage aggregation will not change the output partitioning,
                // but needs to respect aliases (e.g. mapping in the GROUP BY
                // expression).
                let input_eq_properties = input.equivalence_properties();
                input_partitioning.project(group_expr_mapping, input_eq_properties)
            }
            AggregateInputMode::Partial => input_partitioning.clone(),
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

    /// Estimates output statistics for this aggregate node.
    ///
    /// For aggregations without group-by expressions, row count follows the
    /// number of logical aggregate rows and the aggregate output mode. True
    /// no-group aggregates have one logical row; empty grouping sets have one
    /// logical row per grouping-set occurrence.
    ///
    /// For grouped aggregations with known input row count > 1, the output row
    /// count is estimated as:
    ///
    /// ```text
    /// ndv        = sum over each grouping set of product(max(NDV_i + nulls_i, 1))
    /// output_rows = input_rows                       // baseline
    /// output_rows = min(output_rows, ndv)             // if NDV available
    /// output_rows = min(output_rows, limit)           // if TopK active
    /// ```
    ///
    /// **Example 1 — single group key:**
    /// `GROUP BY city` where input_rows = 10,000, NDV(city) = 200
    /// → output_rows = min(10_000, 200) = 200
    ///
    /// **Example 2 — two group keys with TopK:**
    /// `GROUP BY city, category` where input_rows = 10,000, NDV(city) = 200,
    /// NDV(category) = 5, limit = 100
    /// → ndv = 200 × 5 = 1,000
    /// → output_rows = min(10_000, 1_000) = 1,000
    /// → output_rows = min(1_000, 100) = 100
    ///
    /// When `input_rows` is absent but NDV is available, falls back to:
    ///
    /// ```text
    /// output_rows = min(ndv, limit)   // if both available
    /// output_rows = ndv               // if only NDV available
    /// output_rows = limit             // if only limit available
    /// ```
    ///
    /// NDV estimation details (see [`Self::compute_group_ndv`]):
    /// - For each grouping set, only active (non-NULL) columns contribute
    /// - Per-column contribution is `max(NDV + null_adj, 1)` where `null_adj`
    ///   is 1 when nulls are present, 0 otherwise (a null group is a distinct
    ///   output row; `.max(1)` prevents a zero NDV from zeroing the product)
    /// - Per-set products are summed across all grouping sets
    /// - Requires NDV stats for ALL active group-by columns; if any lacks stats,
    ///   falls back to `input_rows` (or `Absent` if that is also unknown)
    fn statistics_inner(
        &self,
        child_statistics: &Statistics,
        partition: Option<usize>,
    ) -> Result<Statistics> {
        // TODO stats: group expressions:
        // - once expressions will be able to compute their own stats, use it here
        // - case where we group by on a column for which with have the `distinct` stat
        // TODO stats: aggr expression:
        // - aggregations sometimes also preserve invariants such as min, max...

        let column_statistics = {
            // self.schema: [<group by exprs>, <aggregate exprs>]
            let mut column_statistics = Statistics::unknown_column(&self.schema());

            for (idx, (expr, _)) in self.group_by.expr.iter().enumerate() {
                if let Some(col) = expr.downcast_ref::<Column>() {
                    let child_col_stats =
                        &child_statistics.column_statistics[col.index()];
                    column_statistics[idx].max_value = child_col_stats.max_value.clone();
                    column_statistics[idx].min_value = child_col_stats.min_value.clone();
                    column_statistics[idx].distinct_count =
                        child_col_stats.distinct_count;
                }
            }

            column_statistics
        };
        match self.exact_output_rows_without_group_exprs(partition) {
            Some(output_rows) => {
                let total_byte_size =
                    Self::calculate_scaled_byte_size(child_statistics, output_rows);

                Ok(Statistics {
                    num_rows: Precision::Exact(output_rows),
                    column_statistics,
                    total_byte_size,
                })
            }
            None => {
                let num_rows = self.estimate_num_rows(child_statistics);

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

    /// Exact physical output row count for aggregates without group-by
    /// expressions.
    ///
    /// `partition` follows [`ExecutionPlan::partition_statistics`]: `Some(_)`
    /// requests one output partition, while `None` requests the entire plan.
    /// Partial-state output contains the logical rows in each output partition;
    /// final-value output contains the global logical rows once.
    /// This mirrors execution, where partial aggregation without group-by
    /// expressions emits its logical rows from every output partition, including
    /// empty input partitions.
    ///
    /// Returns `None` when grouping expressions are present and grouped
    /// cardinality estimation should be used instead.
    fn exact_output_rows_without_group_exprs(
        &self,
        partition: Option<usize>,
    ) -> Option<usize> {
        let logical_rows = self.logical_rows_without_group_exprs()?;

        Some(match (self.mode.output_mode(), partition) {
            (AggregateOutputMode::Final, _) => logical_rows,
            (AggregateOutputMode::Partial, Some(_)) => logical_rows,
            (AggregateOutputMode::Partial, None) => {
                logical_rows * self.cache.output_partitioning().partition_count()
            }
        })
    }

    /// Exact number of logical aggregate rows for aggregates without group-by
    /// expressions.
    ///
    /// A true no-group aggregate has one logical aggregate row. Empty grouping
    /// sets have one logical aggregate row per grouping-set occurrence, even
    /// when there are duplicate empty grouping sets. Returns `None` when there
    /// are grouping expressions.
    fn logical_rows_without_group_exprs(&self) -> Option<usize> {
        if self.group_by.is_true_no_grouping() {
            Some(1)
        } else if self.group_by.expr.is_empty() {
            Some(self.group_by.groups.len())
        } else {
            None
        }
    }

    /// Estimates the output row count for grouped aggregations, combining NDV,
    /// input row count, and TopK limit into a single [`Precision<usize>`].
    fn estimate_num_rows(&self, child_statistics: &Statistics) -> Precision<usize> {
        let ndv = if !self.group_by.expr.is_empty() {
            self.compute_group_ndv(child_statistics)
        } else {
            None
        };
        let limit = self.limit_options.as_ref().map(|lo| lo.limit);

        if let Some(&value) = child_statistics.num_rows.get_value() {
            if value > 1 {
                let mut num_rows = child_statistics.num_rows.to_inexact();
                if let Some(ndv) = ndv {
                    num_rows = num_rows.map(|n| n.min(ndv));
                }
                if let Some(limit) = limit {
                    num_rows = num_rows.map(|n| n.min(limit));
                }
                num_rows
            } else if value == 0 {
                child_statistics.num_rows
            } else {
                let grouping_set_num = self.group_by.groups.len();
                let mut num_rows =
                    child_statistics.num_rows.map(|x| x * grouping_set_num);
                if let Some(limit) = limit {
                    num_rows = num_rows.map(|n| n.min(limit));
                }
                num_rows
            }
        } else {
            match (ndv, limit) {
                (Some(n), Some(l)) => Precision::Inexact(n.min(l)),
                (Some(n), None) => Precision::Inexact(n),
                (None, Some(l)) => Precision::Inexact(l),
                (None, None) => Precision::Absent,
            }
        }
    }

    /// Computes the estimated number of distinct groups across all grouping sets.
    /// For each grouping set, computes `product(NDV_i + null_adj_i)` for active columns,
    /// then sums across all sets. Returns `None` if any active column is not a direct
    /// column reference or lacks `distinct_count` stats. Non-column expressions
    /// (e.g. `abs(a)`) are not yet supported because expression-level statistics
    /// propagation is still in progress (see <https://github.com/apache/datafusion/pull/21122>).
    /// When `null_count` is absent or unknown, null_adjustment defaults to 0.
    ///
    /// **Single key:** `GROUP BY a` where NDV(a) = 100, null_count(a) = 5
    /// → product = max(100 + 1, 1) = 101, total = 101
    ///
    /// **Two keys:** `GROUP BY a, b` where NDV(a) = 100, NDV(b) = 50, no nulls
    /// → product = 100 × 50 = 5,000, total = 5,000
    ///
    /// **Grouping sets:** `GROUPING SETS ((a), (b), (a, b))` with NDV(a) = 100, NDV(b) = 50
    /// → set(a) = 100, set(b) = 50, set(a, b) = 100 × 50 = 5,000
    /// → total = 100 + 50 + 5,000 = 5,150
    fn compute_group_ndv(&self, child_statistics: &Statistics) -> Option<usize> {
        let mut total: usize = 0;
        for group_mask in &self.group_by.groups {
            let mut set_product: usize = 1;
            for (j, (expr, _)) in self.group_by.expr.iter().enumerate() {
                if group_mask[j] {
                    continue;
                }
                let col = expr.downcast_ref::<Column>()?;
                let col_stats = &child_statistics.column_statistics[col.index()];
                let ndv = *col_stats.distinct_count.get_value()?;
                let null_adjustment = match col_stats.null_count.get_value() {
                    Some(&n) if n > 0 => 1usize,
                    _ => 0,
                };
                set_product = set_product
                    .saturating_mul(ndv.saturating_add(null_adjustment).max(1));
            }
            total = total.saturating_add(set_product);
        }
        Some(total)
    }

    /// Check if dynamic filter is possible for the current plan node.
    /// - If yes, init one inside `AggregateExec`'s `dynamic_filter` field.
    /// - If not supported, `self.dynamic_filter` should be kept `None`
    fn init_dynamic_filter(&mut self) {
        if (!self.group_by.is_empty()) || (self.mode != AggregateMode::Partial) {
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
                return;
            };

            // 2. arg should be only 1 column reference
            if let [arg] = aggr_expr.expressions().as_slice()
                && arg.is::<Column>()
            {
                all_cols.push(Arc::clone(arg));
                aggr_dyn_filters.push(PerAccumulatorDynFilter {
                    aggr_type,
                    aggr_index: i,
                    shared_bound: Arc::new(Mutex::new(ScalarValue::Null)),
                });
            }
        }

        if !aggr_dyn_filters.is_empty() {
            self.dynamic_filter = Some(Arc::new(AggrDynFilter {
                filter: Arc::new(DynamicFilterPhysicalExpr::new(all_cols, lit(true))),
                supported_accumulators_info: aggr_dyn_filters,
            }))
        }
    }

    // Collect column references for the dynamic filter expression from the supported accumulators.
    fn cols_for_dynamic_filter(
        &self,
        supported_accumulators_info: &[PerAccumulatorDynFilter],
    ) -> Vec<Arc<dyn PhysicalExpr>> {
        let all_cols: Vec<Arc<dyn PhysicalExpr>> = supported_accumulators_info
            .iter()
            .filter_map(|info| {
                // This should always be true due to how the supported accumulators
                // are constructed. See `init_dynamic_filter` for more details.
                if let [arg] = &self.aggr_expr[info.aggr_index].expressions().as_slice()
                    && arg.is::<Column>()
                {
                    return Some(Arc::clone(arg));
                }
                None
            })
            .collect();
        debug_assert!(all_cols.len() == supported_accumulators_info.len());
        all_cols
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

    fn with_new_children_and_same_properties(
        &self,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Self {
        Self {
            input: children.swap_remove(0),
            metrics: ExecutionPlanMetricsSet::new(),
            ..Self::clone(self)
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
                    .map(|agg| format_aggregate_exec_expr(agg).to_string())
                    .collect();
                write!(f, ", aggr=[{}]", a.join(", "))?;
                if let Some(config) = self.limit_options {
                    write!(f, ", lim=[{}]", config.limit)?;
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
                    .map(|agg| format_tree_aggregate_expr(agg).to_string())
                    .collect();
                writeln!(f, "mode={:?}", self.mode)?;
                if !g.is_empty() {
                    writeln!(f, "group_by={}", g.join(", "))?;
                }
                if !a.is_empty() {
                    writeln!(f, "aggr={}", a.join(", "))?;
                }
                if let Some(config) = self.limit_options {
                    writeln!(f, "limit={}", config.limit)?;
                }
            }
        }
        Ok(())
    }
}

fn format_aggregate_exec_expr(agg: &AggregateFunctionExpr) -> Cow<'_, str> {
    match agg.human_display_alias() {
        Some(_) => format_human_display(agg.human_display(), agg.human_display_alias())
            .unwrap_or_else(|| Cow::Borrowed(agg.name())),
        None => Cow::Borrowed(agg.name()),
    }
}

fn format_tree_aggregate_expr(agg: &AggregateFunctionExpr) -> Cow<'_, str> {
    format_human_display(agg.human_display(), agg.human_display_alias())
        .unwrap_or_else(|| Cow::Borrowed(agg.name()))
}

fn format_human_display<'a>(
    human_display: Option<&'a str>,
    alias: Option<&'a str>,
) -> Option<Cow<'a, str>> {
    human_display.map(|human_display| match alias {
        Some(alias) => Cow::Owned(format!("{human_display} as {alias}")),
        None => Cow::Borrowed(human_display),
    })
}

impl ExecutionPlan for AggregateExec {
    fn name(&self) -> &'static str {
        "AggregateExec"
    }

    /// Return a reference to Any that can be used for down-casting
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        match &self.mode {
            AggregateMode::Partial | AggregateMode::PartialReduce => {
                vec![Distribution::UnspecifiedDistribution]
            }
            AggregateMode::FinalPartitioned | AggregateMode::SinglePartitioned => {
                vec![Distribution::KeyPartitioned(self.group_by.input_exprs())]
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
        check_if_same_properties!(self, children);

        let mut me = AggregateExec::try_new_with_schema(
            self.mode,
            Arc::clone(&self.group_by),
            self.aggr_expr.to_vec(),
            Arc::clone(&self.filter_expr),
            Arc::clone(&children[0]),
            Arc::clone(&self.input_schema),
            Arc::clone(&self.schema),
        )?;
        me.limit_options = self.limit_options;
        me.dynamic_filter.clone_from(&self.dynamic_filter);

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

    fn statistics_with_args(&self, args: &StatisticsArgs) -> Result<Arc<Statistics>> {
        let child_statistics =
            args.compute_child_statistics(&self.input, args.partition())?;
        Ok(Arc::new(
            self.statistics_inner(&child_statistics, args.partition())?,
        ))
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

        // Build grouping columns using output indices because parent filters reference the
        // AggregateExec's output schema where grouping columns in the output schema. The
        // grouping expressions reference input columns which may not match the output schema.
        //
        // It is safe to assume that the output_schema contains group by columns in the same order
        // as the group by expression. See [`create_schema`] and [`AggregateExec`].
        let output_schema = self.schema();
        let grouping_columns: HashSet<_> = (0..self.group_by.expr().len())
            .map(|i| Column::new(output_schema.field(i).name(), i))
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
                        grouping_columns.get(filter_col).map(|col| col.index())
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
        if phase == FilterPushdownPhase::Post
            && config.optimizer.enable_aggregate_dynamic_filter_pushdown
            && let Some(self_dyn_filter) = &self.dynamic_filter
        {
            let dyn_filter = Arc::clone(&self_dyn_filter.filter);
            child_desc = child_desc.with_self_filter(dyn_filter);
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
        if phase == FilterPushdownPhase::Post
            && let Some(dyn_filter) = &self.dynamic_filter
        {
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

/// Creates the output schema for an [`AggregateExec`] containing the group by columns followed
/// by the aggregate columns.
fn create_schema(
    input_schema: &Schema,
    group_by: &PhysicalGroupBy,
    aggr_expr: &[Arc<AggregateFunctionExpr>],
    mode: AggregateMode,
) -> Result<Schema> {
    let mut fields = Vec::with_capacity(group_by.num_output_exprs() + aggr_expr.len());
    fields.extend(group_by.output_fields(input_schema)?);

    match mode.output_mode() {
        AggregateOutputMode::Final => {
            // in final mode, the field with the final result of the accumulator
            for expr in aggr_expr {
                fields.push(expr.field())
            }
        }
        AggregateOutputMode::Partial => {
            // in partial mode, the fields of the accumulator's state
            for expr in aggr_expr {
                fields.extend(expr.state_fields()?.iter().cloned());
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
    if agg_mode.input_mode() == AggregateInputMode::Partial {
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
    match mode.input_mode() {
        AggregateInputMode::Raw => Ok(aggr_expr
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
        AggregateInputMode::Partial => {
            // In merge mode, we build the merge expressions of the aggregation.
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
    match mode.output_mode() {
        AggregateOutputMode::Final => {
            // Merge the state to the final value
            accumulators
                .iter_mut()
                .map(|accumulator| accumulator.evaluate().and_then(|v| v.to_array()))
                .collect()
        }
        AggregateOutputMode::Partial => {
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

/// Builds the internal `__grouping_id` array for a single grouping set.
///
/// The returned array packs two values into a single integer:
///
/// - Low `n` bits (positions 0 .. n-1): the semantic bitmask.  A `1` bit
///   at position `i` means that the `i`-th grouping column (counting from the
///   least significant bit, i.e. the *last* column in the `group` slice) is
///   `NULL` for this grouping set.
/// - High bits (positions n and above): the duplicate `ordinal`, which
///   distinguishes multiple occurrences of the same grouping-set pattern.  The
///   ordinal is `0` for the first occurrence, `1` for the second, and so on.
///
/// The integer type is chosen to be the smallest `UInt8 / UInt16 / UInt32 /
/// UInt64` that can represent both parts.  It matches the type returned by
/// [`Aggregate::grouping_id_type`].
pub(crate) fn group_id_array(
    group: &[bool],
    ordinal: usize,
    max_ordinal: usize,
    num_rows: usize,
) -> Result<ArrayRef> {
    let n = group.len();
    if n > 64 {
        return not_impl_err!(
            "Grouping sets with more than 64 columns are not supported"
        );
    }
    let ordinal_bits = usize::BITS as usize - max_ordinal.leading_zeros() as usize;
    let total_bits = n + ordinal_bits;
    if total_bits > 64 {
        return not_impl_err!(
            "Grouping sets with {n} columns and a maximum duplicate ordinal of \
             {max_ordinal} require {total_bits} bits, which exceeds 64"
        );
    }
    let semantic_id = group.iter().fold(0u64, |acc, &is_null| {
        (acc << 1) | if is_null { 1 } else { 0 }
    });
    let full_id = semantic_id | ((ordinal as u64) << n);
    if total_bits <= 8 {
        Ok(Arc::new(UInt8Array::from(vec![full_id as u8; num_rows])))
    } else if total_bits <= 16 {
        Ok(Arc::new(UInt16Array::from(vec![full_id as u16; num_rows])))
    } else if total_bits <= 32 {
        Ok(Arc::new(UInt32Array::from(vec![full_id as u32; num_rows])))
    } else {
        Ok(Arc::new(UInt64Array::from(vec![full_id; num_rows])))
    }
}

/// Returns the highest duplicate ordinal across all grouping sets.
///
/// At the call-site, the ordinal is the 0-based index assigned to each
/// occurrence of a repeated grouping-set pattern: the first occurrence gets
/// ordinal 0, the second gets 1, and so on.  If the same `Vec<bool>` appears
/// three times the ordinals are 0, 1, 2 and this function returns 2.
/// Returns 0 when no grouping set is duplicated.
pub(crate) fn max_duplicate_ordinal(groups: &[Vec<bool>]) -> usize {
    let mut counts: HashMap<&[bool], usize> = HashMap::new();
    for group in groups {
        *counts.entry(group).or_insert(0) += 1;
    }
    counts.into_values().max().unwrap_or(0).saturating_sub(1)
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
///
/// For example, for `GROUP BY GROUPING SETS ((a, b), (a))` with input:
///
/// ```text
/// a  b
/// 1  1
/// 1  2
/// 2  1
/// ```
///
/// The output is:
///
/// ```text
/// [
///   [
///     a:           [1, 1, 2]
///     b:           [1, 2, 1]
///     grouping_id: [0, 0, 0]
///   ],
///   [
///     a:           [1, 1, 2]
///     b:           [NULL, NULL, NULL]
///     grouping_id: [1, 1, 1]
///   ]
/// ]
/// ```
pub fn evaluate_group_by(
    group_by: &PhysicalGroupBy,
    batch: &RecordBatch,
) -> Result<Vec<Vec<ArrayRef>>> {
    let max_ordinal = max_duplicate_ordinal(&group_by.groups);
    let mut ordinal_per_pattern: HashMap<&[bool], usize> = HashMap::new();
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
            let ordinal = ordinal_per_pattern.entry(group).or_insert(0);
            let current_ordinal = *ordinal;
            *ordinal += 1;

            let mut group_values = Vec::with_capacity(group_by.num_group_exprs());
            group_values.extend(group.iter().enumerate().map(|(idx, is_null)| {
                if *is_null {
                    Arc::clone(&null_exprs[idx])
                } else {
                    Arc::clone(&exprs[idx])
                }
            }));
            if !group_by.is_single() {
                group_values.push(group_id_array(
                    group,
                    current_ordinal,
                    max_ordinal,
                    batch.num_rows(),
                )?);
            }
            Ok(group_values)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::task::{Context, Poll};

    use super::*;
    use crate::RecordBatchStream;
    use crate::coalesce_partitions::CoalescePartitionsExec;
    use crate::common;
    use crate::common::collect;
    use crate::empty::EmptyExec;
    use crate::execution_plan::Boundedness;
    use crate::expressions::col;
    use crate::metrics::MetricValue;
    use crate::statistics::StatisticsArgs;
    use crate::test::TestMemoryExec;
    use crate::test::assert_is_pending;
    use crate::test::exec::{
        BlockingExec, StatisticsExec, assert_strong_count_converges_to_zero,
    };

    use arrow::array::{
        BooleanArray, DictionaryArray, Float32Array, Float64Array, Int32Array,
        Int64Array, StructArray, UInt32Array, UInt64Array,
    };
    use arrow::compute::{SortOptions, concat_batches};
    use arrow::datatypes::Int32Type;
    use datafusion_common::test_util::{batches_to_sort_string, batches_to_string};
    use datafusion_common::{DataFusionError, internal_err};
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::memory_pool::FairSpillPool;
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
    use datafusion_expr::{
        Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator,
        Signature, Volatility,
    };
    use datafusion_functions_aggregate::approx_percentile_cont::approx_percentile_cont_udaf;
    use datafusion_functions_aggregate::array_agg::array_agg_udaf;
    use datafusion_functions_aggregate::average::avg_udaf;
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_aggregate::first_last::{first_value_udaf, last_value_udaf};
    use datafusion_functions_aggregate::median::median_udaf;
    use datafusion_functions_aggregate::min_max::min_udaf;
    use datafusion_functions_aggregate::sum::sum_udaf;
    use datafusion_physical_expr::Partitioning;
    use datafusion_physical_expr::PhysicalSortExpr;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::Literal;

    use crate::projection::ProjectionExec;
    use datafusion_physical_expr::projection::ProjectionExpr;
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
            true,
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
            false,
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
        let final_stats = merged_aggregate.statistics_with_args(&StatisticsArgs::new())?;
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
        cache: Arc<PlanProperties>,
    }

    impl TestYieldingExec {
        fn new(yield_first: bool) -> Self {
            let schema = some_data().0;
            let cache = Self::compute_properties(schema);
            Self {
                yield_first,
                cache: Arc::new(cache),
            }
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

        fn properties(&self) -> &Arc<PlanProperties> {
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

        fn statistics_with_args(&self, args: &StatisticsArgs) -> Result<Arc<Statistics>> {
            if args.partition().is_some() {
                return Ok(Arc::new(Statistics::new_unknown(self.schema().as_ref())));
            }
            let (_, batches) = some_data();
            Ok(Arc::new(common::compute_record_batch_statistics(
                &[batches],
                &self.schema(),
                None,
            )))
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
            false,
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
                AggregateMode::Single,
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
    async fn partial_grouped_aggregate_uses_raw_partial_stream() -> Result<()> {
        let (schema, batches) = some_data();
        let input = TestMemoryExec::try_new_exec(&[batches], Arc::clone(&schema), None)?;
        let group_by =
            PhysicalGroupBy::new_single(vec![(col("a", &schema)?, "a".to_string())]);
        let udaf = Arc::new(AggregateUDF::from(InputTypeAssertingUdaf::new(
            vec![DataType::Float64],
            vec![DataType::Int32],
            DataType::Int64,
        )));
        let aggregates: Vec<Arc<AggregateFunctionExpr>> = vec![Arc::new(
            AggregateExprBuilder::new(udaf, vec![col("b", &schema)?])
                .schema(Arc::clone(&schema))
                .alias("input_type_asserting(b)")
                .build()?,
        )];

        let partial_aggregate = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            group_by.clone(),
            aggregates.clone(),
            vec![None],
            input,
            Arc::clone(&schema),
        )?);
        let task_ctx = Arc::new(
            TaskContext::default().with_session_config(
                SessionConfig::new()
                    .with_batch_size(2)
                    .set_bool("datafusion.execution.enable_migration_aggregate", true),
            ),
        );

        let partial_stream = partial_aggregate.execute_typed(0, &task_ctx)?;
        assert!(matches!(partial_stream, StreamType::PartialHash(_)));

        let fallback_task_ctx = Arc::new(
            TaskContext::default().with_session_config(
                SessionConfig::new()
                    .with_batch_size(2)
                    .set_bool("datafusion.execution.enable_migration_aggregate", false),
            ),
        );
        let stream = partial_aggregate.execute_typed(0, &fallback_task_ctx)?;
        assert!(matches!(stream, StreamType::GroupedHash(_)));

        let stream: SendableRecordBatchStream = partial_stream.into();
        let batches = collect(stream).await?;
        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            vec![2, 1]
        );
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);

        let merge = Arc::new(CoalescePartitionsExec::new(partial_aggregate));
        let final_aggregate = AggregateExec::try_new(
            AggregateMode::Final,
            group_by.as_final(),
            aggregates,
            vec![None],
            merge,
            Arc::clone(&schema),
        )?;

        let final_stream = final_aggregate.execute_typed(0, &task_ctx)?;
        assert!(matches!(final_stream, StreamType::FinalHash(_)));

        let stream = final_aggregate.execute_typed(0, &fallback_task_ctx)?;
        assert!(matches!(stream, StreamType::GroupedHash(_)));

        let stream: SendableRecordBatchStream = final_stream.into();
        let batches = collect(stream).await?;
        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            vec![2, 1]
        );
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);

        Ok(())
    }

    #[tokio::test]
    async fn partial_grouped_aggregate_materializes_before_slicing() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let input_batches = vec![RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )?];
        let input =
            TestMemoryExec::try_new_exec(&[input_batches], Arc::clone(&schema), None)?;
        let group_by =
            PhysicalGroupBy::new_single(vec![(col("key", &schema)?, "key".to_string())]);
        let udaf = Arc::new(AggregateUDF::from(NoFirstEmitUdaf::new()));
        let aggregates: Vec<Arc<AggregateFunctionExpr>> = vec![Arc::new(
            AggregateExprBuilder::new(udaf, vec![col("value", &schema)?])
                .schema(Arc::clone(&schema))
                .alias("no_first_emit(value)")
                .build()?,
        )];
        let aggregate = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            group_by,
            aggregates,
            vec![None],
            input,
            Arc::clone(&schema),
        )?);
        let task_ctx = Arc::new(
            TaskContext::default().with_session_config(
                SessionConfig::new()
                    .with_batch_size(2)
                    .set_bool("datafusion.execution.enable_migration_aggregate", true)
                    .set(
                        "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
                        &ScalarValue::Float64(Some(2.0)),
                    ),
            ),
        );

        let stream = aggregate.execute_typed(0, &task_ctx)?;
        assert!(matches!(stream, StreamType::PartialHash(_)));

        let stream: SendableRecordBatchStream = stream.into();
        let batches = collect(stream).await?;
        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            vec![2, 1]
        );
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        assert_snapshot!(batches_to_sort_string(&batches), @r"
        +-----+-----------------------------+
        | key | no_first_emit(value)[count] |
        +-----+-----------------------------+
        | 1   | 1                           |
        | 2   | 1                           |
        | 3   | 1                           |
        +-----+-----------------------------+
        ");

        Ok(())
    }

    #[tokio::test]
    async fn limited_distinct_aggregate_uses_migrated_hash_streams() -> Result<()> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::UInt32, false)]));
        let input_batches = vec![
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(UInt32Array::from(vec![1, 2, 1]))],
            )?,
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(UInt32Array::from(vec![3, 4]))],
            )?,
        ];
        let group_by =
            PhysicalGroupBy::new_single(vec![(col("a", &schema)?, "a".to_string())]);
        let task_ctx = Arc::new(
            TaskContext::default().with_session_config(
                SessionConfig::new()
                    .set_bool("datafusion.execution.enable_migration_aggregate", true),
            ),
        );

        let partial_input = TestMemoryExec::try_new_exec(
            std::slice::from_ref(&input_batches),
            Arc::clone(&schema),
            None,
        )?;
        let partial_aggregate = Arc::new(
            AggregateExec::try_new(
                AggregateMode::Partial,
                group_by.clone(),
                vec![],
                vec![],
                partial_input,
                Arc::clone(&schema),
            )?
            .with_limit_options(Some(LimitOptions::new(2))),
        );

        let partial_stream = partial_aggregate.execute_typed(0, &task_ctx)?;
        assert!(matches!(partial_stream, StreamType::PartialHash(_)));
        let stream: SendableRecordBatchStream = partial_stream.into();
        let partial_output = collect(stream).await?;
        assert_eq!(
            partial_output
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            2
        );
        assert_snapshot!(batches_to_sort_string(&partial_output), @r"
+---+
| a |
+---+
| 1 |
| 2 |
+---+
");

        let final_input =
            TestMemoryExec::try_new_exec(&[input_batches], Arc::clone(&schema), None)?;
        let final_aggregate = Arc::new(
            AggregateExec::try_new(
                AggregateMode::Final,
                group_by.as_final(),
                vec![],
                vec![],
                final_input,
                Arc::clone(&schema),
            )?
            .with_limit_options(Some(LimitOptions::new(2))),
        );

        let final_stream = final_aggregate.execute_typed(0, &task_ctx)?;
        assert!(matches!(final_stream, StreamType::FinalHash(_)));
        let stream: SendableRecordBatchStream = final_stream.into();
        let final_output = collect(stream).await?;
        assert_eq!(
            final_output
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            2
        );
        assert_snapshot!(batches_to_sort_string(&final_output), @r"
+---+
| a |
+---+
| 1 |
| 2 |
+---+
");

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
        for is_first_acc in [false, true] {
            for spill in [false, true] {
                first_last_multi_partitions(is_first_acc, spill, 4200).await?
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

    fn first_value_agg_expr(
        schema: &SchemaRef,
        column: &str,
        alias: &str,
        human_display: Option<&str>,
        human_display_alias: Option<&str>,
    ) -> Result<AggregateFunctionExpr> {
        let mut builder =
            AggregateExprBuilder::new(first_value_udaf(), vec![col(column, schema)?])
                .order_by(vec![PhysicalSortExpr {
                    expr: col(column, schema)?,
                    options: SortOptions::new(false, false),
                }])
                .schema(Arc::clone(schema))
                .alias(alias);

        if let Some(human_display) = human_display {
            builder = builder.human_display(human_display);
        }
        if let Some(human_display_alias) = human_display_alias {
            builder = builder.human_display_alias(human_display_alias);
        }

        builder.build()
    }

    #[test]
    fn test_reverse_expr_preserves_aliased_human_display() -> Result<()> {
        let schema = create_test_schema()?;
        let agg = first_value_agg_expr(
            &schema,
            "b",
            "agg",
            Some("first_value(b) ORDER BY [b ASC NULLS LAST]"),
            Some("agg"),
        )?;

        let reversed = agg.reverse_expr().expect("expected reverse expr");

        assert_eq!(reversed.name(), "agg");
        assert_eq!(reversed.human_display_alias(), Some("agg"));
        assert_eq!(
            format_tree_aggregate_expr(&reversed),
            "last_value(b) ORDER BY [b DESC NULLS FIRST] as agg"
        );
        assert_eq!(
            reversed.human_display(),
            Some("last_value(b) ORDER BY [b DESC NULLS FIRST]")
        );

        Ok(())
    }

    #[test]
    fn test_reverse_expr_does_not_rewrite_column_names_in_human_display() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "first_value_col",
            DataType::Int32,
            true,
        )]));
        let agg = first_value_agg_expr(
            &schema,
            "first_value_col",
            "agg",
            Some(
                "first_value(first_value_col) ORDER BY [first_value_col ASC NULLS LAST]",
            ),
            Some("agg"),
        )?;

        let reversed = agg.reverse_expr().expect("expected reverse expr");

        assert_eq!(reversed.name(), "agg");
        assert_eq!(
            reversed.human_display(),
            Some(
                "last_value(first_value_col) ORDER BY [first_value_col DESC NULLS FIRST]"
            )
        );
        assert_eq!(
            format_tree_aggregate_expr(&reversed),
            "last_value(first_value_col) ORDER BY [first_value_col DESC NULLS FIRST] as agg"
        );

        Ok(())
    }

    #[test]
    fn test_empty_human_display_is_treated_as_absent() -> Result<()> {
        let schema = create_test_schema()?;
        let agg = first_value_agg_expr(&schema, "b", "agg", Some(""), None)?;

        assert_eq!(agg.human_display(), None);
        assert_eq!(format_tree_aggregate_expr(&agg), "agg");

        Ok(())
    }

    #[test]
    fn test_human_display_alias_must_match_name() -> Result<()> {
        let schema = create_test_schema()?;
        let error = first_value_agg_expr(
            &schema,
            "b",
            "agg",
            Some("first_value(b) ORDER BY [b ASC NULLS LAST]"),
            Some("other_alias"),
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("aggregate human_display_alias must match")
        );

        Ok(())
    }

    #[test]
    fn test_reverse_expr_preserves_non_aliased_display_path() -> Result<()> {
        let schema = create_test_schema()?;
        let agg = first_value_agg_expr(
            &schema,
            "b",
            "first_value(b) ORDER BY [b ASC NULLS LAST]",
            None,
            None,
        )?;

        let reversed = agg.reverse_expr().expect("expected reverse expr");

        assert_eq!(
            reversed.name(),
            "last_value(b) ORDER BY [b DESC NULLS FIRST]"
        );
        assert_eq!(reversed.human_display(), None);

        Ok(())
    }

    // This function constructs the physical plan below,
    //
    // "AggregateExec: mode=Final, gby=[a@0 as a], aggr=[FIRST_VALUE(b)]",
    // "  CoalescePartitionsExec",
    // "    AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[FIRST_VALUE(b)], ordering_mode=None",
    // "      DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]",
    //
    // and checks whether the function `merge_batch` works correctly for
    // FIRST_VALUE and LAST_VALUE functions.
    async fn first_last_multi_partitions(
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
        let coalesce = Arc::new(CoalescePartitionsExec::new(aggregate_exec))
            as Arc<dyn ExecutionPlan>;
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
            true,
        );

        let aggregates: Vec<Arc<AggregateFunctionExpr>> = vec![
            AggregateExprBuilder::new(count_udaf(), vec![lit(1)])
                .schema(Arc::clone(&schema))
                .alias("1")
                .build()
                .map(Arc::new)?,
        ];

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

        let aggr_expr = vec![
            AggregateExprBuilder::new(sum_udaf(), vec![col("value", &batch.schema())?])
                .schema(Arc::clone(&batch.schema()))
                .alias(String::from("SUM(value)"))
                .build()
                .map(Arc::new)?,
        ];

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

    // Migrated to PartialHashAggregateStream coverage below;
    // kept here for the legacy GroupedHashAggregateStream implementation.
    #[tokio::test]
    async fn test_skip_aggregation_after_first_batch() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, true),
            Field::new("val", DataType::Int32, true),
        ]));

        let group_by =
            PhysicalGroupBy::new_single(vec![(col("key", &schema)?, "key".to_string())]);

        let aggr_expr = vec![
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

        let ctx = Arc::new(TaskContext::default().with_session_config(session_config));
        let stream: SendableRecordBatchStream = Box::pin(
            GroupedHashAggregateStream::new(aggregate_exec.as_ref(), &ctx, 0)?,
        );
        let output = collect(stream).await?;

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

    // Migrated to PartialHashAggregateStream coverage below;
    // kept here for the legacy GroupedHashAggregateStream implementation.
    #[tokio::test]
    async fn test_skip_aggregation_after_threshold() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, true),
            Field::new("val", DataType::Int32, true),
        ]));

        let group_by =
            PhysicalGroupBy::new_single(vec![(col("key", &schema)?, "key".to_string())]);

        let aggr_expr = vec![
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

        let ctx = Arc::new(TaskContext::default().with_session_config(session_config));
        let stream: SendableRecordBatchStream = Box::pin(
            GroupedHashAggregateStream::new(aggregate_exec.as_ref(), &ctx, 0)?,
        );
        let output = collect(stream).await?;

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

    #[tokio::test]
    async fn test_partial_hash_stream_skip_aggregation_after_first_batch() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, true),
            Field::new("val", DataType::Int32, true),
        ]));

        let group_by =
            PhysicalGroupBy::new_single(vec![(col("key", &schema)?, "key".to_string())]);

        let aggr_expr = vec![
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

        let session_config = SessionConfig::default()
            .set_bool("datafusion.execution.enable_migration_aggregate", true)
            .set(
                "datafusion.execution.skip_partial_aggregation_probe_rows_threshold",
                &ScalarValue::Int64(Some(2)),
            )
            .set(
                "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
                &ScalarValue::Float64(Some(0.1)),
            );

        let ctx = Arc::new(TaskContext::default().with_session_config(session_config));
        let output = collect(aggregate_exec.execute(0, Arc::clone(&ctx))?).await?;

        allow_duplicates! {
            assert_snapshot!(batches_to_sort_string(&output), @r"
            +-----+-------------------+
            | key | COUNT(val)[count] |
            +-----+-------------------+
            | 1   | 1                 |
            | 2   | 1                 |
            | 2   | 1                 |
            | 3   | 1                 |
            | 3   | 1                 |
            | 4   | 1                 |
            +-----+-------------------+
            ");
        }

        let metrics = aggregate_exec.metrics().unwrap();
        let skipped_rows = metrics
            .sum_by_name("skipped_aggregation_rows")
            .map(|m| m.as_usize())
            .unwrap_or(0);
        assert_eq!(skipped_rows, 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_partial_hash_stream_skip_aggregation_after_threshold() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, true),
            Field::new("val", DataType::Int32, true),
        ]));

        let group_by =
            PhysicalGroupBy::new_single(vec![(col("key", &schema)?, "key".to_string())]);

        let aggr_expr = vec![
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

        let session_config = SessionConfig::default()
            .set_bool("datafusion.execution.enable_migration_aggregate", true)
            .set(
                "datafusion.execution.skip_partial_aggregation_probe_rows_threshold",
                &ScalarValue::Int64(Some(5)),
            )
            .set(
                "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
                &ScalarValue::Float64(Some(0.1)),
            );

        let ctx = Arc::new(TaskContext::default().with_session_config(session_config));
        let output = collect(aggregate_exec.execute(0, Arc::clone(&ctx))?).await?;

        allow_duplicates! {
        assert_snapshot!(batches_to_sort_string(&output), @r"
        +-----+-------------------+
        | key | COUNT(val)[count] |
        +-----+-------------------+
        | 1   | 1                 |
        | 2   | 1                 |
        | 2   | 2                 |
        | 3   | 1                 |
        | 3   | 2                 |
        | 4   | 1                 |
        | 4   | 1                 |
        +-----+-------------------+
        ");
        }

        let metrics = aggregate_exec.metrics().unwrap();
        let skipped_rows = metrics
            .sum_by_name("skipped_aggregation_rows")
            .map(|m| m.as_usize())
            .unwrap_or(0);
        assert_eq!(skipped_rows, 3);

        Ok(())
    }

    /// When `skip_partial_aggregation_probe_ratio_threshold` is set to 1.0,
    /// the feature must be effectively disabled: even with 100% cardinality
    /// (every row is a unique group), no rows should be skipped.
    #[tokio::test]
    async fn test_skip_aggregation_disabled_at_threshold_one() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, true),
            Field::new("val", DataType::Int32, true),
        ]));

        let group_by =
            PhysicalGroupBy::new_single(vec![(col("key", &schema)?, "key".to_string())]);

        let aggr_expr = vec![
            AggregateExprBuilder::new(count_udaf(), vec![col("val", &schema)?])
                .schema(Arc::clone(&schema))
                .alias(String::from("COUNT(val)"))
                .build()
                .map(Arc::new)?,
        ];

        // Two batches are required: batch 1 triggers the probe threshold so the
        // skip decision is evaluated; batch 2 is what would be skipped on main
        // (where >= caused threshold=1.0 to still skip at 100% cardinality).
        // All rows have unique keys => ratio = 1.0 (100% cardinality).
        let input_data = vec![
            // Batch 1: fires the probe check (ratio = 5/5 = 1.0)
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                    Arc::new(Int32Array::from(vec![0, 0, 0, 0, 0])),
                ],
            )
            .unwrap(),
            // Batch 2: would be skipped if threshold=1.0 did not disable the feature
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from(vec![6, 7, 8, 9, 10])),
                    Arc::new(Int32Array::from(vec![0, 0, 0, 0, 0])),
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

        let session_config = SessionConfig::default()
            .set(
                "datafusion.execution.skip_partial_aggregation_probe_rows_threshold",
                &ScalarValue::Int64(Some(1)),
            )
            .set(
                "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
                &ScalarValue::Float64(Some(1.0)),
            );

        let ctx = TaskContext::default().with_session_config(session_config);
        collect(aggregate_exec.execute(0, Arc::new(ctx))?).await?;

        let metrics = aggregate_exec.metrics().unwrap();
        let skipped_rows = metrics
            .sum_by_name("skipped_aggregation_rows")
            .map(|m| m.as_usize())
            .unwrap_or(0);

        assert_eq!(
            skipped_rows, 0,
            "threshold=1.0 should disable skip aggregation, but {skipped_rows} rows were skipped"
        );

        Ok(())
    }

    #[test]
    fn group_exprs_nullable() -> Result<()> {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Float32, false),
            Field::new("b", DataType::Float32, false),
        ]));

        let aggr_expr = vec![
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
            true,
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
            false,
        );

        // Test with MIN for simple intermediate state (min) and AVG for multiple intermediate states (partial sum, partial count).
        let aggregates: Vec<Arc<AggregateFunctionExpr>> = vec![
            Arc::new(
                AggregateExprBuilder::new(min_udaf(), vec![col("b", &schema)?])
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
                panic!(
                    "Expected no spill but found SpillCount metric with value greater than 0."
                );
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
    async fn test_grouped_aggregation_respects_memory_limit() -> Result<()> {
        // test with spill
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
        let proj = ProjectionExec::try_new(
            vec![
                ProjectionExpr::new(lit("0"), "l".to_string()),
                ProjectionExpr::new_from_expression(col("a", &schema)?, &schema)?,
                ProjectionExpr::new_from_expression(col("b", &schema)?, &schema)?,
            ],
            plan,
        )?;
        let plan: Arc<dyn ExecutionPlan> = Arc::new(proj);
        let schema = plan.schema();

        let grouping_set = PhysicalGroupBy::new(
            vec![
                (col("l", &schema)?, "l".to_string()),
                (col("a", &schema)?, "a".to_string()),
            ],
            vec![],
            vec![vec![false, false]],
            false,
        );

        // Test with MIN for simple intermediate state (min) and AVG for multiple intermediate states (partial sum, partial count).
        let aggregates: Vec<Arc<AggregateFunctionExpr>> = vec![
            Arc::new(
                AggregateExprBuilder::new(min_udaf(), vec![col("b", &schema)?])
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
        let memory_pool = Arc::new(FairSpillPool::new(2000));
        let task_ctx = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::new().with_batch_size(batch_size))
                .with_runtime(Arc::new(
                    RuntimeEnvBuilder::new()
                        .with_memory_pool(memory_pool)
                        .build()?,
                )),
        );

        let result = collect(single_aggregate.execute(0, Arc::clone(&task_ctx))?).await;
        match result {
            Ok(result) => {
                assert_spill_count_metric(true, single_aggregate);

                allow_duplicates! {
                    assert_snapshot!(batches_to_string(&result), @r"
                +---+---+--------+--------+
                | l | a | MIN(b) | AVG(b) |
                +---+---+--------+--------+
                | 0 | 2 | 1.0    | 1.0    |
                | 0 | 3 | 2.0    | 2.0    |
                | 0 | 4 | 3.0    | 3.5    |
                +---+---+--------+--------+
            ");
                }
            }
            Err(e) => assert!(matches!(e, DataFusionError::ResourcesExhausted(_))),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_aggregate_statistics_edge_cases() -> Result<()> {
        use datafusion_common::ColumnStatistics;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Float64, false),
        ]));

        let absent_byte_stats = Statistics {
            num_rows: Precision::Exact(100),
            total_byte_size: Precision::Absent,
            column_statistics: vec![
                ColumnStatistics::new_unknown(),
                ColumnStatistics::new_unknown(),
            ],
        };
        let agg = build_test_aggregate(
            &schema,
            absent_byte_stats,
            PhysicalGroupBy::default(),
            None,
        )?;
        let stats = agg.statistics_with_args(&StatisticsArgs::new())?;
        assert_eq!(stats.total_byte_size, Precision::Absent);

        let zero_row_stats = Statistics {
            num_rows: Precision::Exact(0),
            total_byte_size: Precision::Exact(0),
            column_statistics: vec![
                ColumnStatistics::new_unknown(),
                ColumnStatistics::new_unknown(),
            ],
        };
        let agg_zero = build_test_aggregate(
            &schema,
            zero_row_stats,
            PhysicalGroupBy::default(),
            None,
        )?;
        let stats_zero = agg_zero.statistics_with_args(&StatisticsArgs::new())?;
        assert_eq!(stats_zero.total_byte_size, Precision::Absent);

        let single_input =
            Arc::new(EmptyExec::new(Arc::clone(&schema))) as Arc<dyn ExecutionPlan>;
        let single_agg_zero = AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::default(),
            vec![count_a_aggregate(&schema)?],
            vec![None],
            single_input,
            Arc::clone(&schema),
        )?;
        assert_eq!(
            single_agg_zero
                .properties()
                .output_partitioning()
                .partition_count(),
            1
        );
        let single_stats_zero =
            single_agg_zero.statistics_with_args(&StatisticsArgs::new())?;
        assert_eq!(single_stats_zero.num_rows, Precision::Exact(1));

        Ok(())
    }

    fn build_test_aggregate(
        schema: &SchemaRef,
        stats: Statistics,
        group_by: PhysicalGroupBy,
        limit: Option<LimitOptions>,
    ) -> Result<AggregateExec> {
        build_test_aggregate_with_mode(
            schema,
            stats,
            group_by,
            limit,
            AggregateMode::Final,
        )
    }

    fn count_a_aggregate(schema: &SchemaRef) -> Result<Arc<AggregateFunctionExpr>> {
        Ok(Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![col("a", schema)?])
                .schema(Arc::clone(schema))
                .alias("COUNT(a)")
                .build()?,
        ))
    }

    fn build_test_aggregate_with_mode(
        schema: &SchemaRef,
        stats: Statistics,
        group_by: PhysicalGroupBy,
        limit: Option<LimitOptions>,
        mode: AggregateMode,
    ) -> Result<AggregateExec> {
        let input = Arc::new(StatisticsExec::new(stats, (**schema).clone()))
            as Arc<dyn ExecutionPlan>;

        let mut agg = AggregateExec::try_new(
            mode,
            group_by,
            vec![count_a_aggregate(schema)?],
            vec![None],
            input,
            Arc::clone(schema),
        )?;

        if let Some(limit) = limit {
            agg = agg.with_limit_options(Some(limit));
        }

        Ok(agg)
    }

    fn simple_group_by(schema: &SchemaRef, cols: &[&str]) -> PhysicalGroupBy {
        if cols.is_empty() {
            PhysicalGroupBy::default()
        } else {
            PhysicalGroupBy::new_single(
                cols.iter()
                    .map(|name| {
                        (
                            col(name, schema).unwrap() as Arc<dyn PhysicalExpr>,
                            name.to_string(),
                        )
                    })
                    .collect(),
            )
        }
    }

    #[test]
    fn test_aggregate_cardinality_estimation() -> Result<()> {
        use datafusion_common::ColumnStatistics;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]));

        struct TestCase {
            name: &'static str,
            input_rows: Precision<usize>,
            col_a_stats: ColumnStatistics,
            col_b_stats: ColumnStatistics,
            group_by_cols: Vec<&'static str>,
            limit_options: Option<LimitOptions>,
            expected_num_rows: Precision<usize>,
        }

        let cases = vec![
            // --- NDV-based estimation ---
            TestCase {
                name: "single group-by col with NDV tightens estimate",
                input_rows: Precision::Exact(1_000_000),
                col_a_stats: ColumnStatistics {
                    distinct_count: Precision::Exact(500),
                    ..ColumnStatistics::new_unknown()
                },
                col_b_stats: ColumnStatistics::new_unknown(),
                group_by_cols: vec!["a"],
                limit_options: None,
                expected_num_rows: Precision::Inexact(500),
            },
            TestCase {
                name: "multi-col group-by multiplies NDVs",
                input_rows: Precision::Exact(1_000_000),
                col_a_stats: ColumnStatistics {
                    distinct_count: Precision::Exact(100),
                    ..ColumnStatistics::new_unknown()
                },
                col_b_stats: ColumnStatistics {
                    distinct_count: Precision::Exact(50),
                    ..ColumnStatistics::new_unknown()
                },
                group_by_cols: vec!["a", "b"],
                limit_options: None,
                expected_num_rows: Precision::Inexact(5_000),
            },
            TestCase {
                name: "NDV product capped by input rows",
                input_rows: Precision::Exact(200),
                col_a_stats: ColumnStatistics {
                    distinct_count: Precision::Exact(100),
                    ..ColumnStatistics::new_unknown()
                },
                col_b_stats: ColumnStatistics {
                    distinct_count: Precision::Exact(50),
                    ..ColumnStatistics::new_unknown()
                },
                group_by_cols: vec!["a", "b"],
                limit_options: None,
                expected_num_rows: Precision::Inexact(200),
            },
            TestCase {
                name: "null adjustment adds +1 per column",
                input_rows: Precision::Exact(1_000_000),
                col_a_stats: ColumnStatistics {
                    distinct_count: Precision::Exact(99),
                    null_count: Precision::Exact(10),
                    ..ColumnStatistics::new_unknown()
                },
                col_b_stats: ColumnStatistics::new_unknown(),
                group_by_cols: vec!["a"],
                limit_options: None,
                // 99 + 1 (null adjustment) = 100
                expected_num_rows: Precision::Inexact(100),
            },
            TestCase {
                name: "null adjustment on multiple columns",
                input_rows: Precision::Exact(1_000_000),
                col_a_stats: ColumnStatistics {
                    distinct_count: Precision::Exact(99),
                    null_count: Precision::Exact(5),
                    ..ColumnStatistics::new_unknown()
                },
                col_b_stats: ColumnStatistics {
                    distinct_count: Precision::Exact(49),
                    null_count: Precision::Exact(3),
                    ..ColumnStatistics::new_unknown()
                },
                group_by_cols: vec!["a", "b"],
                limit_options: None,
                // (99+1) * (49+1) = 100 * 50 = 5000
                expected_num_rows: Precision::Inexact(5_000),
            },
            TestCase {
                name: "zero null_count means no adjustment",
                input_rows: Precision::Exact(1_000_000),
                col_a_stats: ColumnStatistics {
                    distinct_count: Precision::Exact(100),
                    null_count: Precision::Exact(0),
                    ..ColumnStatistics::new_unknown()
                },
                col_b_stats: ColumnStatistics::new_unknown(),
                group_by_cols: vec!["a"],
                limit_options: None,
                expected_num_rows: Precision::Inexact(100),
            },
            // --- Bail-out: partial NDV stats (Spark-style) ---
            TestCase {
                name: "bail out when one group-by col lacks NDV",
                input_rows: Precision::Exact(1_000_000),
                col_a_stats: ColumnStatistics {
                    distinct_count: Precision::Exact(100),
                    ..ColumnStatistics::new_unknown()
                },
                col_b_stats: ColumnStatistics::new_unknown(),
                group_by_cols: vec!["a", "b"],
                limit_options: None,
                expected_num_rows: Precision::Inexact(1_000_000),
            },
            TestCase {
                name: "bail out when all group-by cols lack NDV",
                input_rows: Precision::Exact(1_000_000),
                col_a_stats: ColumnStatistics::new_unknown(),
                col_b_stats: ColumnStatistics::new_unknown(),
                group_by_cols: vec!["a"],
                limit_options: None,
                expected_num_rows: Precision::Inexact(1_000_000),
            },
            // --- TopK limit capping ---
            TestCase {
                name: "TopK limit caps output rows",
                input_rows: Precision::Exact(1_000_000),
                col_a_stats: ColumnStatistics::new_unknown(),
                col_b_stats: ColumnStatistics::new_unknown(),
                group_by_cols: vec!["a"],
                limit_options: Some(LimitOptions::new(10)),
                expected_num_rows: Precision::Inexact(10),
            },
            TestCase {
                name: "NDV + TopK limit: min(NDV, limit) when NDV < limit",
                input_rows: Precision::Exact(1_000_000),
                col_a_stats: ColumnStatistics {
                    distinct_count: Precision::Exact(5),
                    ..ColumnStatistics::new_unknown()
                },
                col_b_stats: ColumnStatistics::new_unknown(),
                group_by_cols: vec!["a"],
                limit_options: Some(LimitOptions::new(10)),
                expected_num_rows: Precision::Inexact(5),
            },
            TestCase {
                name: "NDV + TopK limit: min(NDV, limit) when limit < NDV",
                input_rows: Precision::Exact(1_000_000),
                col_a_stats: ColumnStatistics {
                    distinct_count: Precision::Exact(500),
                    ..ColumnStatistics::new_unknown()
                },
                col_b_stats: ColumnStatistics::new_unknown(),
                group_by_cols: vec!["a"],
                limit_options: Some(LimitOptions::new(10)),
                expected_num_rows: Precision::Inexact(10),
            },
            // --- Absent input rows ---
            TestCase {
                name: "absent input rows without limit stays absent",
                input_rows: Precision::Absent,
                col_a_stats: ColumnStatistics::new_unknown(),
                col_b_stats: ColumnStatistics::new_unknown(),
                group_by_cols: vec!["a"],
                limit_options: None,
                expected_num_rows: Precision::Absent,
            },
            TestCase {
                name: "absent input rows with TopK limit gives inexact(limit)",
                input_rows: Precision::Absent,
                col_a_stats: ColumnStatistics::new_unknown(),
                col_b_stats: ColumnStatistics::new_unknown(),
                group_by_cols: vec!["a"],
                limit_options: Some(LimitOptions::new(10)),
                expected_num_rows: Precision::Inexact(10),
            },
            // --- No group-by (global aggregation) ---
            TestCase {
                name: "no group-by cols (Final mode) returns Exact(1)",
                input_rows: Precision::Exact(1_000_000),
                col_a_stats: ColumnStatistics::new_unknown(),
                col_b_stats: ColumnStatistics::new_unknown(),
                group_by_cols: vec![],
                limit_options: None,
                expected_num_rows: Precision::Exact(1),
            },
            // --- One input row ---
            TestCase {
                name: "one input row returns Exact(1)",
                input_rows: Precision::Exact(1),
                col_a_stats: ColumnStatistics {
                    distinct_count: Precision::Exact(1),
                    ..ColumnStatistics::new_unknown()
                },
                col_b_stats: ColumnStatistics::new_unknown(),
                group_by_cols: vec!["a"],
                limit_options: None,
                expected_num_rows: Precision::Exact(1),
            },
            // --- Zero input rows ---
            TestCase {
                name: "zero input rows returns Exact(0)",
                input_rows: Precision::Exact(0),
                col_a_stats: ColumnStatistics::new_unknown(),
                col_b_stats: ColumnStatistics::new_unknown(),
                group_by_cols: vec!["a"],
                limit_options: None,
                expected_num_rows: Precision::Exact(0),
            },
            // --- Inexact NDV stats ---
            TestCase {
                name: "inexact NDV still used for estimation",
                input_rows: Precision::Exact(1_000_000),
                col_a_stats: ColumnStatistics {
                    distinct_count: Precision::Inexact(200),
                    ..ColumnStatistics::new_unknown()
                },
                col_b_stats: ColumnStatistics::new_unknown(),
                group_by_cols: vec!["a"],
                limit_options: None,
                expected_num_rows: Precision::Inexact(200),
            },
            TestCase {
                name: "inexact NDV combined with limit",
                input_rows: Precision::Exact(1_000_000),
                col_a_stats: ColumnStatistics {
                    distinct_count: Precision::Inexact(200),
                    ..ColumnStatistics::new_unknown()
                },
                col_b_stats: ColumnStatistics::new_unknown(),
                group_by_cols: vec!["a"],
                limit_options: Some(LimitOptions::new(10)),
                expected_num_rows: Precision::Inexact(10),
            },
            // --- NDV zero column (all-null) ---
            TestCase {
                name: "all-null column contributes 1 to the product, not 0",
                input_rows: Precision::Exact(1_000),
                col_a_stats: ColumnStatistics {
                    distinct_count: Precision::Exact(0),
                    null_count: Precision::Exact(1_000),
                    ..ColumnStatistics::new_unknown()
                },
                col_b_stats: ColumnStatistics {
                    distinct_count: Precision::Exact(50),
                    ..ColumnStatistics::new_unknown()
                },
                group_by_cols: vec!["a", "b"],
                limit_options: None,
                // NDV(a)=0 with nulls => max(0+1, 1)=1, NDV(b)=50 => 1*50=50
                expected_num_rows: Precision::Inexact(50),
            },
            // --- Absent num_rows with NDV ---
            TestCase {
                name: "absent num_rows falls back to NDV estimate",
                input_rows: Precision::Absent,
                col_a_stats: ColumnStatistics {
                    distinct_count: Precision::Exact(100),
                    ..ColumnStatistics::new_unknown()
                },
                col_b_stats: ColumnStatistics::new_unknown(),
                group_by_cols: vec!["a"],
                limit_options: None,
                expected_num_rows: Precision::Inexact(100),
            },
            TestCase {
                name: "absent num_rows with NDV and limit returns min(ndv, limit)",
                input_rows: Precision::Absent,
                col_a_stats: ColumnStatistics {
                    distinct_count: Precision::Exact(100),
                    ..ColumnStatistics::new_unknown()
                },
                col_b_stats: ColumnStatistics::new_unknown(),
                group_by_cols: vec!["a"],
                limit_options: Some(LimitOptions::new(10)),
                expected_num_rows: Precision::Inexact(10),
            },
        ];

        for case in cases {
            let input_stats = Statistics {
                num_rows: case.input_rows,
                total_byte_size: Precision::Inexact(1_000_000),
                column_statistics: vec![
                    case.col_a_stats.clone(),
                    case.col_b_stats.clone(),
                ],
            };

            let group_by = simple_group_by(&schema, &case.group_by_cols);
            let agg =
                build_test_aggregate(&schema, input_stats, group_by, case.limit_options)?;

            let stats = agg.statistics_with_args(&StatisticsArgs::new())?;
            assert_eq!(
                stats.num_rows, case.expected_num_rows,
                "FAILED: '{}' — expected {:?}, got {:?}",
                case.name, case.expected_num_rows, stats.num_rows
            );
        }

        Ok(())
    }

    #[test]
    fn test_aggregate_stats_distinct_count_propagation() -> Result<()> {
        use datafusion_common::ColumnStatistics;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]));

        let input_stats = Statistics {
            num_rows: Precision::Exact(1000),
            total_byte_size: Precision::Inexact(10000),
            column_statistics: vec![
                ColumnStatistics {
                    distinct_count: Precision::Exact(100),
                    null_count: Precision::Exact(5),
                    ..ColumnStatistics::new_unknown()
                },
                ColumnStatistics::new_unknown(),
            ],
        };
        let agg = build_test_aggregate(
            &schema,
            input_stats,
            simple_group_by(&schema, &["a"]),
            None,
        )?;

        let stats = agg.statistics_with_args(&StatisticsArgs::new())?;
        assert_eq!(
            stats.column_statistics[0].distinct_count,
            Precision::Exact(100),
            "distinct_count should be propagated from child for group-by columns"
        );

        Ok(())
    }

    #[test]
    fn test_aggregate_stats_grouping_sets() -> Result<()> {
        use datafusion_common::ColumnStatistics;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]));

        let input_stats = Statistics {
            num_rows: Precision::Exact(1_000_000),
            total_byte_size: Precision::Inexact(1_000_000),
            column_statistics: vec![
                ColumnStatistics {
                    distinct_count: Precision::Exact(100),
                    ..ColumnStatistics::new_unknown()
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(50),
                    ..ColumnStatistics::new_unknown()
                },
            ],
        };

        // CUBE-like grouping set: (a, NULL), (NULL, b), (a, b) — 3 groups
        let grouping_set = PhysicalGroupBy::new(
            vec![
                (col("a", &schema)? as Arc<dyn PhysicalExpr>, "a".to_string()),
                (col("b", &schema)? as Arc<dyn PhysicalExpr>, "b".to_string()),
            ],
            vec![
                (lit(ScalarValue::Int32(None)), "a".to_string()),
                (lit(ScalarValue::Int32(None)), "b".to_string()),
            ],
            vec![
                vec![false, true],  // (a, NULL)
                vec![true, false],  // (NULL, b)
                vec![false, false], // (a, b)
            ],
            true,
        );

        let agg = build_test_aggregate(&schema, input_stats, grouping_set, None)?;

        let stats = agg.statistics_with_args(&StatisticsArgs::new())?;
        // Per-set NDV: (a,NULL)=100, (NULL,b)=50, (a,b)=100*50=5000
        // Total = 100 + 50 + 5000 = 5150
        assert_eq!(
            stats.num_rows,
            Precision::Inexact(5_150),
            "grouping sets should sum per-set NDV products"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_aggregate_stats_duplicate_empty_grouping_sets() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));

        let duplicate_empty_grouping_sets =
            PhysicalGroupBy::new(vec![], vec![], vec![vec![], vec![]], true);

        let single_input =
            Arc::new(EmptyExec::new(Arc::clone(&schema))) as Arc<dyn ExecutionPlan>;
        let single_agg = AggregateExec::try_new(
            AggregateMode::Single,
            duplicate_empty_grouping_sets.clone(),
            vec![count_a_aggregate(&schema)?],
            vec![None],
            single_input,
            Arc::clone(&schema),
        )?;
        assert_eq!(
            single_agg
                .statistics_with_args(&StatisticsArgs::new())?
                .num_rows,
            Precision::Exact(2)
        );

        let partial_input =
            Arc::new(EmptyExec::new(Arc::clone(&schema)).with_partitions(2))
                as Arc<dyn ExecutionPlan>;
        let partial_agg = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            duplicate_empty_grouping_sets,
            vec![count_a_aggregate(&schema)?],
            vec![None],
            partial_input,
            Arc::clone(&schema),
        )?);

        assert_eq!(
            partial_agg
                .properties()
                .output_partitioning()
                .partition_count(),
            2
        );
        let task_ctx = Arc::new(TaskContext::default());
        for partition in 0..2 {
            assert_eq!(
                partial_agg
                    .statistics_with_args(
                        &StatisticsArgs::new().with_partition(Some(partition))
                    )?
                    .num_rows,
                Precision::Exact(2)
            );
            let result =
                collect(partial_agg.execute(partition, Arc::clone(&task_ctx))?).await?;
            assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        }

        assert_eq!(
            partial_agg
                .statistics_with_args(&StatisticsArgs::new())?
                .num_rows,
            Precision::Exact(4)
        );

        Ok(())
    }

    #[test]
    fn test_aggregate_stats_non_column_expr_bails_out() -> Result<()> {
        use datafusion_common::ColumnStatistics;
        use datafusion_expr::Operator;
        use datafusion_physical_expr::expressions::BinaryExpr;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]));

        let input_stats = Statistics {
            num_rows: Precision::Exact(1_000_000),
            total_byte_size: Precision::Inexact(1_000_000),
            column_statistics: vec![
                ColumnStatistics {
                    distinct_count: Precision::Exact(100),
                    ..ColumnStatistics::new_unknown()
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(50),
                    ..ColumnStatistics::new_unknown()
                },
            ],
        };

        // GROUP BY (a + b) — not a direct column reference
        let expr_a_plus_b: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            col("a", &schema)?,
            Operator::Plus,
            col("b", &schema)?,
        ));

        let group_by =
            PhysicalGroupBy::new_single(vec![(expr_a_plus_b, "a+b".to_string())]);
        let agg = build_test_aggregate(&schema, input_stats, group_by, None)?;

        let stats = agg.statistics_with_args(&StatisticsArgs::new())?;
        assert_eq!(
            stats.num_rows,
            Precision::Inexact(1_000_000),
            "non-column group-by expression should bail out to input_rows"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_order_is_retained_when_spilling() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Int64, false),
        ]));

        let batches = vec![vec![
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![2])),
                    Arc::new(Int64Array::from(vec![2])),
                    Arc::new(Int64Array::from(vec![1])),
                ],
            )?,
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(Int64Array::from(vec![1])),
                ],
            )?,
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![0])),
                    Arc::new(Int64Array::from(vec![0])),
                    Arc::new(Int64Array::from(vec![1])),
                ],
            )?,
        ]];
        let scan = TestMemoryExec::try_new(&batches, Arc::clone(&schema), None)?;
        let scan = scan.try_with_sort_information(vec![
            LexOrdering::new([PhysicalSortExpr::new(
                col("b", schema.as_ref())?,
                SortOptions::default().desc(),
            )])
            .unwrap(),
        ])?;

        let aggr = Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new(
                vec![
                    (col("b", schema.as_ref())?, "b".to_string()),
                    (col("c", schema.as_ref())?, "c".to_string()),
                ],
                vec![],
                vec![vec![false, false]],
                false,
            ),
            vec![Arc::new(
                AggregateExprBuilder::new(sum_udaf(), vec![col("c", schema.as_ref())?])
                    .schema(Arc::clone(&schema))
                    .alias("SUM(c)")
                    .build()?,
            )],
            vec![None],
            Arc::new(scan) as Arc<dyn ExecutionPlan>,
            Arc::clone(&schema),
        )?);

        let task_ctx = new_spill_ctx(1, 600);
        let result = collect(aggr.execute(0, Arc::clone(&task_ctx))?).await?;
        assert_spill_count_metric(true, aggr);

        allow_duplicates! {
            assert_snapshot!(batches_to_string(&result), @r"
            +---+---+--------+
            | b | c | SUM(c) |
            +---+---+--------+
            | 2 | 1 | 1      |
            | 1 | 1 | 1      |
            | 0 | 1 | 1      |
            +---+---+--------+
        ");
        }
        Ok(())
    }

    /// Tests that when the memory pool is too small to accommodate the sort
    /// reservation during spill, the error is properly propagated as
    /// ResourcesExhausted rather than silently exceeding memory limits.
    #[tokio::test]
    async fn test_sort_reservation_fails_during_spill() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, false),
            Field::new("a", DataType::Float64, false),
            Field::new("b", DataType::Float64, false),
            Field::new("c", DataType::Float64, false),
            Field::new("d", DataType::Float64, false),
            Field::new("e", DataType::Float64, false),
        ]));

        let batches = vec![vec![
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(Float64Array::from(vec![10.0])),
                    Arc::new(Float64Array::from(vec![20.0])),
                    Arc::new(Float64Array::from(vec![30.0])),
                    Arc::new(Float64Array::from(vec![40.0])),
                    Arc::new(Float64Array::from(vec![50.0])),
                ],
            )?,
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![2])),
                    Arc::new(Float64Array::from(vec![11.0])),
                    Arc::new(Float64Array::from(vec![21.0])),
                    Arc::new(Float64Array::from(vec![31.0])),
                    Arc::new(Float64Array::from(vec![41.0])),
                    Arc::new(Float64Array::from(vec![51.0])),
                ],
            )?,
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![3])),
                    Arc::new(Float64Array::from(vec![12.0])),
                    Arc::new(Float64Array::from(vec![22.0])),
                    Arc::new(Float64Array::from(vec![32.0])),
                    Arc::new(Float64Array::from(vec![42.0])),
                    Arc::new(Float64Array::from(vec![52.0])),
                ],
            )?,
        ]];

        let scan = TestMemoryExec::try_new(&batches, Arc::clone(&schema), None)?;

        let aggr = Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new(
                vec![(col("g", schema.as_ref())?, "g".to_string())],
                vec![],
                vec![vec![false]],
                false,
            ),
            vec![
                Arc::new(
                    AggregateExprBuilder::new(
                        avg_udaf(),
                        vec![col("a", schema.as_ref())?],
                    )
                    .schema(Arc::clone(&schema))
                    .alias("AVG(a)")
                    .build()?,
                ),
                Arc::new(
                    AggregateExprBuilder::new(
                        avg_udaf(),
                        vec![col("b", schema.as_ref())?],
                    )
                    .schema(Arc::clone(&schema))
                    .alias("AVG(b)")
                    .build()?,
                ),
                Arc::new(
                    AggregateExprBuilder::new(
                        avg_udaf(),
                        vec![col("c", schema.as_ref())?],
                    )
                    .schema(Arc::clone(&schema))
                    .alias("AVG(c)")
                    .build()?,
                ),
                Arc::new(
                    AggregateExprBuilder::new(
                        avg_udaf(),
                        vec![col("d", schema.as_ref())?],
                    )
                    .schema(Arc::clone(&schema))
                    .alias("AVG(d)")
                    .build()?,
                ),
                Arc::new(
                    AggregateExprBuilder::new(
                        avg_udaf(),
                        vec![col("e", schema.as_ref())?],
                    )
                    .schema(Arc::clone(&schema))
                    .alias("AVG(e)")
                    .build()?,
                ),
            ],
            vec![None, None, None, None, None],
            Arc::new(scan) as Arc<dyn ExecutionPlan>,
            Arc::clone(&schema),
        )?);

        // Pool must be large enough for accumulation to start but too small for
        // sort_memory after clearing.
        let task_ctx = new_spill_ctx(1, 500);
        let result = collect(aggr.execute(0, Arc::clone(&task_ctx))?).await;

        match &result {
            Ok(_) => panic!("Expected ResourcesExhausted error but query succeeded"),
            Err(e) => {
                let root = e.find_root();
                assert!(
                    matches!(root, DataFusionError::ResourcesExhausted(_)),
                    "Expected ResourcesExhausted, got: {root}",
                );
                let msg = root.to_string();
                assert!(
                    msg.contains("Failed to reserve memory for sort during spill"),
                    "Expected sort reservation error, got: {msg}",
                );
            }
        }

        Ok(())
    }

    /// Tests that PartialReduce mode:
    /// 1. Accepts state as input (like Final)
    /// 2. Produces state as output (like Partial)
    /// 3. Can be followed by a Final stage to get the correct result
    ///
    /// This simulates a tree-reduce pattern:
    ///   Partial -> PartialReduce -> Final
    async fn evaluate_partial_reduce(
        groups: PhysicalGroupBy,
        aggregates: Vec<Arc<AggregateFunctionExpr>>,
        partition_1_and_2_batches: [Vec<RecordBatch>; 2],
    ) -> Result<Vec<RecordBatch>> {
        let schema = partition_1_and_2_batches
            .iter()
            .flatten()
            .next()
            .expect("Must have at least 1 batch")
            .schema();

        let [partition_1, partition_2] = partition_1_and_2_batches;

        // Step 1: Partial aggregation on partition 1
        let input1 =
            TestMemoryExec::try_new_exec(&[partition_1], Arc::clone(&schema), None)?;
        let partial1 = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            groups.clone(),
            aggregates.clone(),
            vec![None; aggregates.len()],
            input1,
            Arc::clone(&schema),
        )?);

        // Step 2: Partial aggregation on partition 2
        let input2 =
            TestMemoryExec::try_new_exec(&[partition_2], Arc::clone(&schema), None)?;
        let partial2 = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            groups.clone(),
            aggregates.clone(),
            vec![None; aggregates.len()],
            input2,
            Arc::clone(&schema),
        )?);

        // Collect partial results
        let task_ctx = Arc::new(TaskContext::default());
        let partial_result1 =
            crate::collect(Arc::clone(&partial1) as _, Arc::clone(&task_ctx)).await?;
        let partial_result2 =
            crate::collect(Arc::clone(&partial2) as _, Arc::clone(&task_ctx)).await?;

        // The partial results have state schema (group cols + accumulator state)
        let partial_schema = partial1.schema();

        // Step 3: PartialReduce — combine partial results, still producing state
        let combined_input = TestMemoryExec::try_new_exec(
            &[partial_result1, partial_result2],
            Arc::clone(&partial_schema),
            None,
        )?;
        // Coalesce into a single partition for the PartialReduce
        let coalesced = Arc::new(CoalescePartitionsExec::new(combined_input));

        let partial_reduce = Arc::new(AggregateExec::try_new(
            AggregateMode::PartialReduce,
            groups.clone(),
            aggregates.clone(),
            vec![None; aggregates.len()],
            coalesced,
            Arc::clone(&partial_schema),
        )?);

        // Verify PartialReduce output schema matches Partial output schema
        // (both produce state, not final values)
        assert_eq!(partial_reduce.schema(), partial_schema);

        // Collect PartialReduce results
        let reduce_result =
            crate::collect(Arc::clone(&partial_reduce) as _, Arc::clone(&task_ctx))
                .await?;

        // Step 4: Final aggregation on the PartialReduce output
        let final_input = TestMemoryExec::try_new_exec(
            &[reduce_result],
            Arc::clone(&partial_schema),
            None,
        )?;
        let final_agg = Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            groups.clone(),
            aggregates.clone(),
            vec![None; aggregates.len()],
            final_input,
            Arc::clone(&partial_schema),
        )?);

        let result = crate::collect(final_agg, Arc::clone(&task_ctx)).await?;

        Ok(result)
    }

    /// Builds the shared `Partial -> PartialReduce -> Final` fixture used by
    /// the `test_partial_reduce_*` tests below and runs the pipeline against
    /// the aggregate produced by `build_aggregates`.
    ///
    /// Each test only needs to supply the UDAF/alias under test, so the test
    /// body stays focused on which aggregate shape is being exercised.
    async fn run_partial_reduce_pipeline<F>(
        build_aggregates: F,
    ) -> Result<Vec<RecordBatch>>
    where
        F: FnOnce(&Arc<Schema>) -> Result<Vec<Arc<AggregateFunctionExpr>>>,
    {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, false),
            Field::new("b", DataType::Float64, false),
        ]));

        // Two partitions of input data so the Partial stage produces multiple
        // partial states that PartialReduce must combine.
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(UInt32Array::from(vec![1, 2, 3])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0])),
            ],
        )?;
        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(UInt32Array::from(vec![1, 2, 3])),
                Arc::new(Float64Array::from(vec![40.0, 50.0, 60.0])),
            ],
        )?;

        let groups =
            PhysicalGroupBy::new_single(vec![(col("a", &schema)?, "a".to_string())]);
        let aggregates = build_aggregates(&schema)?;

        evaluate_partial_reduce(groups, aggregates, [vec![batch1], vec![batch2]]).await
    }

    // -------------------------------------------------------------------
    // PartialReduce regression coverage.
    //
    // Each shape (single state field / single input arg, multi-state /
    // single-input, more-state-than-input) is covered twice:
    //   * once against a real UDAF, to round-trip an actual aggregate end
    //     to end through `Partial -> PartialReduce -> Final`; and
    //   * once against [`InputTypeAssertingUdaf`], whose input / state /
    //     output types are deliberately pairwise-disjoint within each test
    //     so a regression that swapped state-field types for input-field
    //     types (or vice versa) fails the assertion instead of slipping
    //     through on a coincidental type match.
    //
    // The stub variants do the heavy lifting on the contract; the real
    // ones make sure no real aggregate is broken by it.
    // -------------------------------------------------------------------

    /// Real-UDAF round-trip: aggregate with a single state field and a
    /// single input argument (`SUM(b)` — state and input are both `Float64`).
    #[tokio::test]
    async fn test_partial_reduce_with_single_state_field_and_single_input_arg()
    -> Result<()> {
        let result = run_partial_reduce_pipeline(|schema| {
            Ok(vec![Arc::new(
                AggregateExprBuilder::new(sum_udaf(), vec![col("b", schema)?])
                    .schema(Arc::clone(schema))
                    .alias("SUM(b)")
                    .build()?,
            )])
        })
        .await?;

        // Expected: group 1 -> 10+40=50, group 2 -> 20+50=70, group 3 -> 30+60=90
        assert_snapshot!(batches_to_sort_string(&result), @r"
        +---+--------+
        | a | SUM(b) |
        +---+--------+
        | 1 | 50.0   |
        | 2 | 70.0   |
        | 3 | 90.0   |
        +---+--------+
        ");

        Ok(())
    }

    /// Real-UDAF round-trip: aggregate with multiple state fields and a
    /// single input argument (`AVG(b)` — state is `[sum: Float64, count:
    /// UInt64]`).
    #[tokio::test]
    async fn test_partial_reduce_with_multiple_state_fields_and_single_input_arg()
    -> Result<()> {
        let result = run_partial_reduce_pipeline(|schema| {
            Ok(vec![Arc::new(
                AggregateExprBuilder::new(avg_udaf(), vec![col("b", schema)?])
                    .schema(Arc::clone(schema))
                    .alias("AVG(b)")
                    .build()?,
            )])
        })
        .await?;

        assert_snapshot!(batches_to_sort_string(&result), @r"
        +---+--------+
        | a | AVG(b) |
        +---+--------+
        | 1 | 25.0   |
        | 2 | 35.0   |
        | 3 | 45.0   |
        +---+--------+
        ");

        Ok(())
    }

    /// Real-UDAF round-trip: aggregate whose state has more fields than the
    /// input has arguments (`approx_percentile_cont` carries a t-digest).
    #[tokio::test]
    async fn test_partial_reduce_with_more_state_fields_than_input_args() -> Result<()> {
        let result = run_partial_reduce_pipeline(|schema| {
            Ok(vec![Arc::new(
                AggregateExprBuilder::new(
                    approx_percentile_cont_udaf(),
                    vec![col("b", schema)?, lit(0.75f32)],
                )
                .schema(Arc::clone(schema))
                .alias("approx_percentile_cont(b, 0.75)")
                .build()?,
            )])
        })
        .await?;

        assert_snapshot!(batches_to_sort_string(&result), @r"
        +---+---------------------------------+
        | a | approx_percentile_cont(b, 0.75) |
        +---+---------------------------------+
        | 1 | 40.0                            |
        | 2 | 50.0                            |
        | 3 | 60.0                            |
        +---+---------------------------------+
        ");

        Ok(())
    }

    /// Stub variant of
    /// [`test_partial_reduce_with_single_state_field_and_single_input_arg`]
    /// with disjoint input / state / output types.
    ///
    /// - input: `Float64`
    /// - state: `Int32`
    /// - output: `Int64`
    ///
    /// Any mode that accidentally forwarded state-field types in place of
    /// input-field types would fail the assertion in
    /// [`InputTypeAssertingUdaf`] instead of being masked by a coincidental
    /// type match.
    #[tokio::test]
    async fn test_partial_reduce_with_single_state_field_and_single_input_arg_using_unique_types()
    -> Result<()> {
        let result = run_partial_reduce_pipeline(|schema| {
            let udaf = Arc::new(AggregateUDF::from(InputTypeAssertingUdaf::new(
                vec![DataType::Float64],
                vec![DataType::Int32],
                DataType::Int64,
            )));
            Ok(vec![Arc::new(
                AggregateExprBuilder::new(udaf, vec![col("b", schema)?])
                    .schema(Arc::clone(schema))
                    .alias("input_type_asserting(b)")
                    .build()?,
            )])
        })
        .await?;

        // Pipeline completing without error is the real assertion. The
        // snapshot guards against silent regressions in the row shape.
        assert_snapshot!(batches_to_sort_string(&result), @r"
        +---+-------------------------+
        | a | input_type_asserting(b) |
        +---+-------------------------+
        | 1 | 0                       |
        | 2 | 0                       |
        | 3 | 0                       |
        +---+-------------------------+
        ");

        Ok(())
    }

    /// Stub variant of
    /// [`test_partial_reduce_with_multiple_state_fields_and_single_input_arg`]
    /// with disjoint input / state / output types.
    ///
    /// - input: `Float64`
    /// - state: `[Int32, Utf8]`
    /// - output: `Int64`
    #[tokio::test]
    async fn test_partial_reduce_with_multiple_state_fields_and_single_input_arg_using_unique_types()
    -> Result<()> {
        let result = run_partial_reduce_pipeline(|schema| {
            let udaf = Arc::new(AggregateUDF::from(InputTypeAssertingUdaf::new(
                vec![DataType::Float64],
                vec![DataType::Int32, DataType::Utf8],
                DataType::Int64,
            )));
            Ok(vec![Arc::new(
                AggregateExprBuilder::new(udaf, vec![col("b", schema)?])
                    .schema(Arc::clone(schema))
                    .alias("input_type_asserting(b)")
                    .build()?,
            )])
        })
        .await?;

        assert_snapshot!(batches_to_sort_string(&result), @r"
        +---+-------------------------+
        | a | input_type_asserting(b) |
        +---+-------------------------+
        | 1 | 0                       |
        | 2 | 0                       |
        | 3 | 0                       |
        +---+-------------------------+
        ");

        Ok(())
    }

    /// Stub variant of
    /// [`test_partial_reduce_with_more_state_fields_than_input_args`] with
    /// disjoint input / state / output types — and with multiple input
    /// arguments to exercise the multi-arg path explicitly.
    ///
    /// - input: `[Float64, Date32]`
    /// - state: `[Int32, Utf8, Boolean]`
    /// - output: `Int64`
    #[tokio::test]
    async fn test_partial_reduce_with_more_state_fields_than_input_args_using_unique_types()
    -> Result<()> {
        let result = run_partial_reduce_pipeline(|schema| {
            let udaf = Arc::new(AggregateUDF::from(InputTypeAssertingUdaf::new(
                vec![DataType::Float64, DataType::Date32],
                vec![DataType::Int32, DataType::Utf8, DataType::Boolean],
                DataType::Int64,
            )));
            Ok(vec![Arc::new(
                AggregateExprBuilder::new(
                    udaf,
                    vec![col("b", schema)?, lit(ScalarValue::Date32(Some(1)))],
                )
                .schema(Arc::clone(schema))
                .alias("input_type_asserting(b, lit)")
                .build()?,
            )])
        })
        .await?;

        assert_snapshot!(batches_to_sort_string(&result), @r"
        +---+------------------------------+
        | a | input_type_asserting(b, lit) |
        +---+------------------------------+
        | 1 | 0                            |
        | 2 | 0                            |
        | 3 | 0                            |
        +---+------------------------------+
        ");

        Ok(())
    }

    /// Stub test: many input args, few state fields (5 inputs / 2 state).
    ///
    /// All eight types involved are pairwise-disjoint:
    ///   - input:  `[Float64, Date32, UInt16, Boolean, Int32]`
    ///   - state:  `[Utf8, Int64]`
    ///   - output: `Float32`
    #[tokio::test]
    async fn test_partial_reduce_with_5_input_args_and_2_state_fields_using_unique_types()
    -> Result<()> {
        let result = run_partial_reduce_pipeline(|schema| {
            let udaf = Arc::new(AggregateUDF::from(InputTypeAssertingUdaf::new(
                vec![
                    DataType::Float64,
                    DataType::Date32,
                    DataType::UInt16,
                    DataType::Boolean,
                    DataType::Int32,
                ],
                vec![DataType::Utf8, DataType::Int64],
                DataType::Float32,
            )));
            Ok(vec![Arc::new(
                AggregateExprBuilder::new(
                    udaf,
                    vec![
                        col("b", schema)?,
                        lit(ScalarValue::Date32(Some(1))),
                        lit(ScalarValue::UInt16(Some(1))),
                        lit(ScalarValue::Boolean(Some(false))),
                        lit(ScalarValue::Int32(Some(1))),
                    ],
                )
                .schema(Arc::clone(schema))
                .alias("input_type_asserting(b, l1, l2, l3, l4)")
                .build()?,
            )])
        })
        .await?;

        assert_snapshot!(batches_to_sort_string(&result), @r"
        +---+-----------------------------------------+
        | a | input_type_asserting(b, l1, l2, l3, l4) |
        +---+-----------------------------------------+
        | 1 | 0.0                                     |
        | 2 | 0.0                                     |
        | 3 | 0.0                                     |
        +---+-----------------------------------------+
        ");

        Ok(())
    }

    /// Stub test: few input args, many state fields (2 inputs / 5 state).
    ///
    /// All eight types involved are pairwise-disjoint:
    ///   - input:  `[Float64, Date32]`
    ///   - state:  `[Boolean, Int32, Utf8, Int64, UInt16]`
    ///   - output: `Float32`
    #[tokio::test]
    async fn test_partial_reduce_with_2_input_args_and_5_state_fields_using_unique_types()
    -> Result<()> {
        let result = run_partial_reduce_pipeline(|schema| {
            let udaf = Arc::new(AggregateUDF::from(InputTypeAssertingUdaf::new(
                vec![DataType::Float64, DataType::Date32],
                vec![
                    DataType::Boolean,
                    DataType::Int32,
                    DataType::Utf8,
                    DataType::Int64,
                    DataType::UInt16,
                ],
                DataType::Float32,
            )));
            Ok(vec![Arc::new(
                AggregateExprBuilder::new(
                    udaf,
                    vec![col("b", schema)?, lit(ScalarValue::Date32(Some(1)))],
                )
                .schema(Arc::clone(schema))
                .alias("input_type_asserting(b, lit)")
                .build()?,
            )])
        })
        .await?;

        assert_snapshot!(batches_to_sort_string(&result), @r"
        +---+------------------------------+
        | a | input_type_asserting(b, lit) |
        +---+------------------------------+
        | 1 | 0.0                          |
        | 2 | 0.0                          |
        | 3 | 0.0                          |
        +---+------------------------------+
        ");

        Ok(())
    }

    /// Test-only aggregate whose `return_type`, `state_fields`, and
    /// `accumulator` hooks all assert that they receive the originally-
    /// declared input types; the companion accumulator further asserts
    /// `update_batch` sees inputs and `merge_batch` sees state.
    ///
    /// Each test instantiates it with input / state / output types that
    /// are pairwise-disjoint, so a regression that forwarded the wrong
    /// types fails on type mismatch rather than passing by accident.
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct InputTypeAssertingUdaf {
        signature: Signature,
        input_types: Vec<DataType>,
        state_types: Vec<DataType>,
        output_type: DataType,
    }

    fn assert_data_types(
        what: &str,
        expected: &[DataType],
        actual: &[DataType],
    ) -> Result<()> {
        if actual != expected {
            return internal_err!(
                "InputTypeAssertingUdaf: {} expected types {:?} but got {:?} — a regression is leaking the wrong types into the accumulator contract",
                what,
                expected,
                actual
            );
        }
        Ok(())
    }

    /// Produce a zeroed [`ScalarValue`] for `dt`. Only the data types the
    /// tests above plug into [`InputTypeAssertingUdaf`] are listed; adding
    /// a new type to a test requires extending this match.
    fn zero_scalar_for(dt: &DataType) -> Result<ScalarValue> {
        match dt {
            DataType::Boolean => Ok(ScalarValue::Boolean(Some(false))),
            DataType::Int32 => Ok(ScalarValue::Int32(Some(0))),
            DataType::Int64 => Ok(ScalarValue::Int64(Some(0))),
            DataType::UInt16 => Ok(ScalarValue::UInt16(Some(0))),
            DataType::Float32 => Ok(ScalarValue::Float32(Some(0.0))),
            DataType::Utf8 => Ok(ScalarValue::Utf8(Some(String::new()))),
            other => internal_err!(
                "InputTypeAssertingUdaf: no zero ScalarValue registered for {other:?} \
                 — extend `zero_scalar_for` when adding a new state/output type"
            ),
        }
    }

    impl InputTypeAssertingUdaf {
        fn new(
            input_types: Vec<DataType>,
            state_types: Vec<DataType>,
            output_type: DataType,
        ) -> Self {
            // Within-test type-disjointness is enforced by construction so
            // a future test author can't quietly reintroduce overlap.
            assert!(
                all_pairwise_distinct(&input_types, &state_types, &output_type),
                "InputTypeAssertingUdaf::new: input ({input_types:?}), state \
                 ({state_types:?}), and output ({output_type:?}) types must be \
                 pairwise-disjoint to avoid accidental passes",
            );
            Self {
                signature: Signature::exact(input_types.clone(), Volatility::Immutable),
                input_types,
                state_types,
                output_type,
            }
        }
    }

    /// True iff every type in `inputs ∪ states ∪ {output}` is unique.
    fn all_pairwise_distinct(
        inputs: &[DataType],
        states: &[DataType],
        output: &DataType,
    ) -> bool {
        let mut seen = HashSet::new();
        for dt in inputs
            .iter()
            .chain(states.iter())
            .chain(std::iter::once(output))
        {
            if !seen.insert(dt) {
                return false;
            }
        }
        true
    }

    impl AggregateUDFImpl for InputTypeAssertingUdaf {
        fn name(&self) -> &str {
            "input_type_asserting"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
            assert_data_types("return_type(arg_types)", &self.input_types, arg_types)?;
            Ok(self.output_type.clone())
        }

        fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
            let actual: Vec<DataType> = args
                .input_fields
                .iter()
                .map(|f| f.data_type().clone())
                .collect();
            assert_data_types(
                "state_fields(args.input_fields)",
                &self.input_types,
                &actual,
            )?;
            Ok(self
                .state_types
                .iter()
                .enumerate()
                .map(|(i, dt)| {
                    Field::new(format!("{}[s{i}]", args.name), dt.clone(), true).into()
                })
                .collect())
        }

        fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
            let actual: Vec<DataType> = acc_args
                .expr_fields
                .iter()
                .map(|f| f.data_type().clone())
                .collect();
            assert_data_types(
                "accumulator(acc_args.expr_fields)",
                &self.input_types,
                &actual,
            )?;
            Ok(Box::new(InputTypeAssertingAccumulator {
                input_types: self.input_types.clone(),
                state_types: self.state_types.clone(),
                output_type: self.output_type.clone(),
            }))
        }
    }

    /// Companion accumulator for [`InputTypeAssertingUdaf`].
    ///
    /// - `update_batch` must always receive arrays of the original input
    ///   types.
    /// - `merge_batch` must always receive arrays of the declared state
    ///   types.
    ///
    /// Anything else means a non-input mode is calling the wrong path.
    #[derive(Debug)]
    struct InputTypeAssertingAccumulator {
        input_types: Vec<DataType>,
        state_types: Vec<DataType>,
        output_type: DataType,
    }

    impl Accumulator for InputTypeAssertingAccumulator {
        fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
            let actual: Vec<DataType> =
                values.iter().map(|a| a.data_type().clone()).collect();
            assert_data_types("update_batch(values)", &self.input_types, &actual)
        }

        fn evaluate(&mut self) -> Result<ScalarValue> {
            zero_scalar_for(&self.output_type)
        }

        fn size(&self) -> usize {
            size_of_val(self)
        }

        fn state(&mut self) -> Result<Vec<ScalarValue>> {
            self.state_types.iter().map(zero_scalar_for).collect()
        }

        fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
            let actual: Vec<DataType> =
                states.iter().map(|a| a.data_type().clone()).collect();
            assert_data_types("merge_batch(states)", &self.state_types, &actual)
        }
    }

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct NoFirstEmitUdaf {
        signature: Signature,
    }

    impl NoFirstEmitUdaf {
        fn new() -> Self {
            Self {
                signature: Signature::exact(vec![DataType::Int32], Volatility::Immutable),
            }
        }
    }

    impl AggregateUDFImpl for NoFirstEmitUdaf {
        fn name(&self) -> &str {
            "no_first_emit"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Int64)
        }

        fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
            Ok(vec![Arc::new(Field::new(
                format!("{}[count]", args.name),
                DataType::Int64,
                false,
            ))])
        }

        fn accumulator(
            &self,
            _acc_args: AccumulatorArgs,
        ) -> Result<Box<dyn Accumulator>> {
            Ok(Box::new(NoFirstEmitAccumulator))
        }

        fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
            true
        }

        fn create_groups_accumulator(
            &self,
            _args: AccumulatorArgs,
        ) -> Result<Box<dyn GroupsAccumulator>> {
            Ok(Box::new(NoFirstEmitGroupsAccumulator { counts: vec![] }))
        }
    }

    #[derive(Debug)]
    struct NoFirstEmitAccumulator;

    impl Accumulator for NoFirstEmitAccumulator {
        fn update_batch(&mut self, _values: &[ArrayRef]) -> Result<()> {
            Ok(())
        }

        fn evaluate(&mut self) -> Result<ScalarValue> {
            Ok(ScalarValue::Int64(Some(0)))
        }

        fn size(&self) -> usize {
            size_of_val(self)
        }

        fn state(&mut self) -> Result<Vec<ScalarValue>> {
            Ok(vec![ScalarValue::Int64(Some(0))])
        }

        fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
            Ok(())
        }
    }

    #[derive(Debug)]
    struct NoFirstEmitGroupsAccumulator {
        counts: Vec<i64>,
    }

    impl NoFirstEmitGroupsAccumulator {
        fn emit_counts(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
            match emit_to {
                EmitTo::All => {
                    let counts = std::mem::take(&mut self.counts);
                    Ok(Arc::new(Int64Array::from(counts)))
                }
                EmitTo::First(_) => internal_err!(
                    "partial grouped aggregate output must materialize with EmitTo::All before slicing"
                ),
            }
        }
    }

    impl GroupsAccumulator for NoFirstEmitGroupsAccumulator {
        fn update_batch(
            &mut self,
            _values: &[ArrayRef],
            group_indices: &[usize],
            _opt_filter: Option<&BooleanArray>,
            total_num_groups: usize,
        ) -> Result<()> {
            self.counts.resize(total_num_groups, 0);
            for group_index in group_indices {
                self.counts[*group_index] += 1;
            }
            Ok(())
        }

        fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
            self.emit_counts(emit_to)
        }

        fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
            Ok(vec![self.emit_counts(emit_to)?])
        }

        fn merge_batch(
            &mut self,
            _values: &[ArrayRef],
            _group_indices: &[usize],
            _total_num_groups: usize,
        ) -> Result<()> {
            Ok(())
        }

        fn size(&self) -> usize {
            size_of_val(self) + self.counts.capacity() * size_of::<i64>()
        }
    }

    /// Test that [`AggregateExec::with_dynamic_filter_expr`] overrides the existing dynamic filter
    #[test]
    fn test_with_dynamic_filter() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let child = Arc::new(EmptyExec::new(Arc::clone(&schema)));

        // Partial min aggregate supports dynamic filtering
        let agg = AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(vec![]),
            vec![Arc::new(
                AggregateExprBuilder::new(min_udaf(), vec![col("a", &schema)?])
                    .schema(Arc::clone(&schema))
                    .alias("min_a")
                    .build()?,
            )],
            vec![None],
            child,
            Arc::clone(&schema),
        )?;

        // Assertion 1: A filter with the same children can override the existing
        // dynamic filter.
        let new_df = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![col("a", &schema)?],
            lit(false),
        ));
        let agg = agg.with_dynamic_filter_expr(Arc::clone(&new_df))?;

        // The aggregate's filter should now resolve to the new inner expression.
        let swapped = agg
            .dynamic_filter_expr()
            .expect("should still have dynamic filter")
            .current()?;
        assert_eq!(format!("{swapped}"), format!("{}", lit(false)));

        // Assertion 2: A filter that has been through `PhysicalExpr::with_new_children`
        // should still be accepted when the new children are equivalent to the originals.
        let new_df_as_pexpr: Arc<dyn PhysicalExpr> =
            Arc::<DynamicFilterPhysicalExpr>::clone(&new_df);
        let remapped_pexpr =
            new_df_as_pexpr.with_new_children(vec![col("a", &schema)?])?;
        let Ok(remapped_df) = (remapped_pexpr as Arc<dyn std::any::Any + Send + Sync>)
            .downcast::<DynamicFilterPhysicalExpr>()
        else {
            panic!("should be DynamicFilterPhysicalExpr after with_new_children");
        };
        // Hard to assert this because the filter is identical. No error means
        // the filter was accepted. That's a good enough assertion for now.
        let _agg = agg.with_dynamic_filter_expr(remapped_df)?;
        Ok(())
    }

    /// Test that [`AggregateExec::with_dynamic_filter_expr`] errors when the aggregate does not support dynamic filtering
    #[test]
    fn test_with_dynamic_filter_error_unsupported() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let child = Arc::new(EmptyExec::new(Arc::clone(&schema)));

        // Final mode with a group-by does not support dynamic filters.
        let agg = AggregateExec::try_new(
            AggregateMode::Final,
            PhysicalGroupBy::new_single(vec![(col("a", &schema)?, "a".to_string())]),
            vec![Arc::new(
                AggregateExprBuilder::new(sum_udaf(), vec![col("b", &schema)?])
                    .schema(Arc::clone(&schema))
                    .alias("sum_b")
                    .build()?,
            )],
            vec![None],
            child,
            Arc::clone(&schema),
        )?;
        assert!(agg.dynamic_filter_expr().is_none());

        let df = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![col("a", &schema)?],
            lit(true),
        ));
        assert!(agg.with_dynamic_filter_expr(df).is_err());
        Ok(())
    }

    /// Test that [`AggregateExec::with_dynamic_filter_expr`] errors when the column is not in the schema
    #[test]
    fn test_with_dynamic_filter_error_column_mismatch() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let child = Arc::new(EmptyExec::new(Arc::clone(&schema)));

        let agg = AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(vec![]),
            vec![Arc::new(
                AggregateExprBuilder::new(min_udaf(), vec![col("a", &schema)?])
                    .schema(Arc::clone(&schema))
                    .alias("min_a")
                    .build()?,
            )],
            vec![None],
            child,
            Arc::clone(&schema),
        )?;

        let df = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("bad", 99)) as _],
            lit(true),
        ));
        assert!(agg.with_dynamic_filter_expr(df).is_err());
        Ok(())
    }
}
