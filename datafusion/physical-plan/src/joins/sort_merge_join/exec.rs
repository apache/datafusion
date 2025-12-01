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

//! Defines the Sort-Merge join execution plan.
//! A Sort-Merge join plan consumes two sorted children plans and produces
//! joined output by given join type and other options.

use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::execution_plan::{boundedness_from_children, EmissionType};
use crate::expressions::PhysicalSortExpr;
use crate::joins::sort_merge_join::metrics::SortMergeJoinMetrics;
use crate::joins::sort_merge_join::stream::SortMergeJoinStream;
use crate::joins::utils::{
    build_join_schema, check_join_is_valid, estimate_join_statistics,
    reorder_output_after_swap, symmetric_join_output_partitioning, JoinFilter, JoinOn,
    JoinOnRef,
};
use crate::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::projection::{
    join_allows_pushdown, join_table_borders, new_join_children,
    physical_to_column_exprs, update_join_on, ProjectionExec,
};
use crate::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, SendableRecordBatchStream, Statistics,
};

use arrow::compute::SortOptions;
use arrow::datatypes::SchemaRef;
use datafusion_common::{
    assert_eq_or_internal_err, internal_err, plan_err, JoinSide, JoinType, NullEquality,
    Result,
};
use datafusion_execution::memory_pool::MemoryConsumer;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::equivalence::join_equivalence_properties;
use datafusion_physical_expr_common::physical_expr::{fmt_sql, PhysicalExprRef};
use datafusion_physical_expr_common::sort_expr::{LexOrdering, OrderingRequirements};

/// Join execution plan that executes equi-join predicates on multiple partitions using Sort-Merge
/// join algorithm and applies an optional filter post join. Can be used to join arbitrarily large
/// inputs where one or both of the inputs don't fit in the available memory.
///
/// # Join Expressions
///
/// Equi-join predicate (e.g. `<col1> = <col2>`) expressions are represented by [`Self::on`].
///
/// Non-equality predicates, which can not be pushed down to join inputs (e.g.
/// `<col1> != <col2>`) are known as "filter expressions" and are evaluated
/// after the equijoin predicates. They are represented by [`Self::filter`]. These are optional
/// expressions.
///
/// # Sorting
///
/// Assumes that both the left and right input to the join are pre-sorted. It is not the
/// responsibility of this execution plan to sort the inputs.
///
/// # "Streamed" vs "Buffered"
///
/// The number of record batches of streamed input currently present in the memory will depend
/// on the output batch size of the execution plan. There is no spilling support for streamed input.
/// The comparisons are performed from values of join keys in streamed input with the values of
/// join keys in buffered input. One row in streamed record batch could be matched with multiple rows in
/// buffered input batches. The streamed input is managed through the states in `StreamedState`
/// and streamed input batches are represented by `StreamedBatch`.
///
/// Buffered input is buffered for all record batches having the same value of join key.
/// If the memory limit increases beyond the specified value and spilling is enabled,
/// buffered batches could be spilled to disk. If spilling is disabled, the execution
/// will fail under the same conditions. Multiple record batches of buffered could currently reside
/// in memory/disk during the execution. The number of buffered batches residing in
/// memory/disk depends on the number of rows of buffered input having the same value
/// of join key as that of streamed input rows currently present in memory. Due to pre-sorted inputs,
/// the algorithm understands when it is not needed anymore, and releases the buffered batches
/// from memory/disk. The buffered input is managed through the states in `BufferedState`
/// and buffered input batches are represented by `BufferedBatch`.
///
/// Depending on the type of join, left or right input may be selected as streamed or buffered
/// respectively. For example, in a left-outer join, the left execution plan will be selected as
/// streamed input while in a right-outer join, the right execution plan will be selected as the
/// streamed input.
///
/// Reference for the algorithm:
/// <https://en.wikipedia.org/wiki/Sort-merge_join>.
///
/// Helpful short video demonstration:
/// <https://www.youtube.com/watch?v=jiWCPJtDE2c>.
#[derive(Debug, Clone)]
pub struct SortMergeJoinExec {
    /// Left sorted joining execution plan
    pub left: Arc<dyn ExecutionPlan>,
    /// Right sorting joining execution plan
    pub right: Arc<dyn ExecutionPlan>,
    /// Set of common columns used to join on
    pub on: JoinOn,
    /// Filters which are applied while finding matching rows
    pub filter: Option<JoinFilter>,
    /// How the join is performed
    pub join_type: JoinType,
    /// The schema once the join is applied
    schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// The left SortExpr
    left_sort_exprs: LexOrdering,
    /// The right SortExpr
    right_sort_exprs: LexOrdering,
    /// Sort options of join columns used in sorting left and right execution plans
    pub sort_options: Vec<SortOptions>,
    /// Defines the null equality for the join.
    pub null_equality: NullEquality,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: PlanProperties,
}

impl SortMergeJoinExec {
    /// Tries to create a new [SortMergeJoinExec].
    /// The inputs are sorted using `sort_options` are applied to the columns in the `on`
    /// # Error
    /// This function errors when it is not possible to join the left and right sides on keys `on`.
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        filter: Option<JoinFilter>,
        join_type: JoinType,
        sort_options: Vec<SortOptions>,
        null_equality: NullEquality,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();

        check_join_is_valid(&left_schema, &right_schema, &on)?;
        if sort_options.len() != on.len() {
            return plan_err!(
                "Expected number of sort options: {}, actual: {}",
                on.len(),
                sort_options.len()
            );
        }

        let (left_sort_exprs, right_sort_exprs): (Vec<_>, Vec<_>) = on
            .iter()
            .zip(sort_options.iter())
            .map(|((l, r), sort_op)| {
                let left = PhysicalSortExpr {
                    expr: Arc::clone(l),
                    options: *sort_op,
                };
                let right = PhysicalSortExpr {
                    expr: Arc::clone(r),
                    options: *sort_op,
                };
                (left, right)
            })
            .unzip();
        let Some(left_sort_exprs) = LexOrdering::new(left_sort_exprs) else {
            return plan_err!(
                "SortMergeJoinExec requires valid sort expressions for its left side"
            );
        };
        let Some(right_sort_exprs) = LexOrdering::new(right_sort_exprs) else {
            return plan_err!(
                "SortMergeJoinExec requires valid sort expressions for its right side"
            );
        };

        let schema =
            Arc::new(build_join_schema(&left_schema, &right_schema, &join_type).0);
        let cache =
            Self::compute_properties(&left, &right, Arc::clone(&schema), join_type, &on)?;
        Ok(Self {
            left,
            right,
            on,
            filter,
            join_type,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            left_sort_exprs,
            right_sort_exprs,
            sort_options,
            null_equality,
            cache,
        })
    }

    /// Get probe side (e.g streaming side) information for this sort merge join.
    /// In current implementation, probe side is determined according to join type.
    pub fn probe_side(join_type: &JoinType) -> JoinSide {
        // When output schema contains only the right side, probe side is right.
        // Otherwise probe side is the left side.
        match join_type {
            // TODO: sort merge support for right mark (tracked here: https://github.com/apache/datafusion/issues/16226)
            JoinType::Right
            | JoinType::RightSemi
            | JoinType::RightAnti
            | JoinType::RightMark => JoinSide::Right,
            JoinType::Inner
            | JoinType::Left
            | JoinType::Full
            | JoinType::LeftAnti
            | JoinType::LeftSemi
            | JoinType::LeftMark => JoinSide::Left,
        }
    }

    /// Calculate order preservation flags for this sort merge join.
    fn maintains_input_order(join_type: JoinType) -> Vec<bool> {
        match join_type {
            JoinType::Inner => vec![true, false],
            JoinType::Left
            | JoinType::LeftSemi
            | JoinType::LeftAnti
            | JoinType::LeftMark => vec![true, false],
            JoinType::Right
            | JoinType::RightSemi
            | JoinType::RightAnti
            | JoinType::RightMark => {
                vec![false, true]
            }
            _ => vec![false, false],
        }
    }

    /// Set of common columns used to join on
    pub fn on(&self) -> &[(PhysicalExprRef, PhysicalExprRef)] {
        &self.on
    }

    /// Ref to right execution plan
    pub fn right(&self) -> &Arc<dyn ExecutionPlan> {
        &self.right
    }

    /// Join type
    pub fn join_type(&self) -> JoinType {
        self.join_type
    }

    /// Ref to left execution plan
    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        &self.left
    }

    /// Ref to join filter
    pub fn filter(&self) -> &Option<JoinFilter> {
        &self.filter
    }

    /// Ref to sort options
    pub fn sort_options(&self) -> &[SortOptions] {
        &self.sort_options
    }

    /// Null equality
    pub fn null_equality(&self) -> NullEquality {
        self.null_equality
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        left: &Arc<dyn ExecutionPlan>,
        right: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        join_type: JoinType,
        join_on: JoinOnRef,
    ) -> Result<PlanProperties> {
        // Calculate equivalence properties:
        let eq_properties = join_equivalence_properties(
            left.equivalence_properties().clone(),
            right.equivalence_properties().clone(),
            &join_type,
            schema,
            &Self::maintains_input_order(join_type),
            Some(Self::probe_side(&join_type)),
            join_on,
        )?;

        let output_partitioning =
            symmetric_join_output_partitioning(left, right, &join_type)?;

        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            EmissionType::Incremental,
            boundedness_from_children([left, right]),
        ))
    }

    /// # Notes:
    ///
    /// This function should be called BEFORE inserting any repartitioning
    /// operators on the join's children. Check [`super::super::HashJoinExec::swap_inputs`]
    /// for more details.
    pub fn swap_inputs(&self) -> Result<Arc<dyn ExecutionPlan>> {
        let left = self.left();
        let right = self.right();
        let new_join = SortMergeJoinExec::try_new(
            Arc::clone(right),
            Arc::clone(left),
            self.on()
                .iter()
                .map(|(l, r)| (Arc::clone(r), Arc::clone(l)))
                .collect::<Vec<_>>(),
            self.filter().as_ref().map(JoinFilter::swap),
            self.join_type().swap(),
            self.sort_options.clone(),
            self.null_equality,
        )?;

        // TODO: OR this condition with having a built-in projection (like
        //       ordinary hash join) when we support it.
        if matches!(
            self.join_type(),
            JoinType::LeftSemi
                | JoinType::RightSemi
                | JoinType::LeftAnti
                | JoinType::RightAnti
        ) {
            Ok(Arc::new(new_join))
        } else {
            reorder_output_after_swap(Arc::new(new_join), &left.schema(), &right.schema())
        }
    }
}

impl DisplayAs for SortMergeJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let on = self
                    .on
                    .iter()
                    .map(|(c1, c2)| format!("({c1}, {c2})"))
                    .collect::<Vec<String>>()
                    .join(", ");
                let display_null_equality =
                    if matches!(self.null_equality(), NullEquality::NullEqualsNull) {
                        ", NullsEqual: true"
                    } else {
                        ""
                    };
                write!(
                    f,
                    "SortMergeJoin: join_type={:?}, on=[{}]{}{}",
                    self.join_type,
                    on,
                    self.filter.as_ref().map_or_else(
                        || "".to_string(),
                        |f| format!(", filter={}", f.expression())
                    ),
                    display_null_equality,
                )
            }
            DisplayFormatType::TreeRender => {
                let on = self
                    .on
                    .iter()
                    .map(|(c1, c2)| {
                        format!("({} = {})", fmt_sql(c1.as_ref()), fmt_sql(c2.as_ref()))
                    })
                    .collect::<Vec<String>>()
                    .join(", ");

                if self.join_type() != JoinType::Inner {
                    writeln!(f, "join_type={:?}", self.join_type)?;
                }
                writeln!(f, "on={on}")?;

                if matches!(self.null_equality(), NullEquality::NullEqualsNull) {
                    writeln!(f, "NullsEqual: true")?;
                }

                Ok(())
            }
        }
    }
}

impl ExecutionPlan for SortMergeJoinExec {
    fn name(&self) -> &'static str {
        "SortMergeJoinExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        let (left_expr, right_expr) = self
            .on
            .iter()
            .map(|(l, r)| (Arc::clone(l), Arc::clone(r)))
            .unzip();
        vec![
            Distribution::HashPartitioned(left_expr),
            Distribution::HashPartitioned(right_expr),
        ]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![
            Some(OrderingRequirements::from(self.left_sort_exprs.clone())),
            Some(OrderingRequirements::from(self.right_sort_exprs.clone())),
        ]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        Self::maintains_input_order(self.join_type)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match &children[..] {
            [left, right] => Ok(Arc::new(SortMergeJoinExec::try_new(
                Arc::clone(left),
                Arc::clone(right),
                self.on.clone(),
                self.filter.clone(),
                self.join_type,
                self.sort_options.clone(),
                self.null_equality,
            )?)),
            _ => internal_err!("SortMergeJoin wrong number of children"),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let left_partitions = self.left.output_partitioning().partition_count();
        let right_partitions = self.right.output_partitioning().partition_count();
        assert_eq_or_internal_err!(
            left_partitions,
            right_partitions,
            "Invalid SortMergeJoinExec, partition count mismatch {left_partitions}!={right_partitions},\
                 consider using RepartitionExec"
        );
        let (on_left, on_right) = self.on.iter().cloned().unzip();
        let (streamed, buffered, on_streamed, on_buffered) =
            if SortMergeJoinExec::probe_side(&self.join_type) == JoinSide::Left {
                (
                    Arc::clone(&self.left),
                    Arc::clone(&self.right),
                    on_left,
                    on_right,
                )
            } else {
                (
                    Arc::clone(&self.right),
                    Arc::clone(&self.left),
                    on_right,
                    on_left,
                )
            };

        // execute children plans
        let streamed = streamed.execute(partition, Arc::clone(&context))?;
        let buffered = buffered.execute(partition, Arc::clone(&context))?;

        // create output buffer
        let batch_size = context.session_config().batch_size();

        // create memory reservation
        let reservation = MemoryConsumer::new(format!("SMJStream[{partition}]"))
            .register(context.memory_pool());

        // create join stream
        Ok(Box::pin(SortMergeJoinStream::try_new(
            context.session_config().spill_compression(),
            Arc::clone(&self.schema),
            self.sort_options.clone(),
            self.null_equality,
            streamed,
            buffered,
            on_streamed,
            on_buffered,
            self.filter.clone(),
            self.join_type,
            batch_size,
            SortMergeJoinMetrics::new(partition, &self.metrics),
            reservation,
            context.runtime_env(),
        )?))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.partition_statistics(None)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        if partition.is_some() {
            return Ok(Statistics::new_unknown(&self.schema()));
        }
        // TODO stats: it is not possible in general to know the output size of joins
        // There are some special cases though, for example:
        // - `A LEFT JOIN B ON A.col=B.col` with `COUNT_DISTINCT(B.col)=COUNT(B.col)`
        estimate_join_statistics(
            self.left.partition_statistics(None)?,
            self.right.partition_statistics(None)?,
            &self.on,
            &self.join_type,
            &self.schema,
        )
    }

    /// Tries to swap the projection with its input [`SortMergeJoinExec`]. If it can be done,
    /// it returns the new swapped version having the [`SortMergeJoinExec`] as the top plan.
    /// Otherwise, it returns None.
    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // Convert projected PhysicalExpr's to columns. If not possible, we cannot proceed.
        let Some(projection_as_columns) = physical_to_column_exprs(projection.expr())
        else {
            return Ok(None);
        };

        let (far_right_left_col_ind, far_left_right_col_ind) = join_table_borders(
            self.left().schema().fields().len(),
            &projection_as_columns,
        );

        if !join_allows_pushdown(
            &projection_as_columns,
            &self.schema(),
            far_right_left_col_ind,
            far_left_right_col_ind,
        ) {
            return Ok(None);
        }

        let Some(new_on) = update_join_on(
            &projection_as_columns[0..=far_right_left_col_ind as _],
            &projection_as_columns[far_left_right_col_ind as _..],
            self.on(),
            self.left().schema().fields().len(),
        ) else {
            return Ok(None);
        };

        let (new_left, new_right) = new_join_children(
            &projection_as_columns,
            far_right_left_col_ind,
            far_left_right_col_ind,
            self.children()[0],
            self.children()[1],
        )?;

        Ok(Some(Arc::new(SortMergeJoinExec::try_new(
            Arc::new(new_left),
            Arc::new(new_right),
            new_on,
            self.filter.clone(),
            self.join_type,
            self.sort_options.clone(),
            self.null_equality,
        )?)))
    }
}
