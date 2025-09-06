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
    internal_err, plan_err, JoinSide, JoinType, NullEquality, Result,
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
                write!(
                    f,
                    "SortMergeJoin: join_type={:?}, on=[{}]{}",
                    self.join_type,
                    on,
                    self.filter.as_ref().map_or("".to_string(), |f| format!(
                        ", filter={}",
                        f.expression()
                    ))
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
                writeln!(f, "on={on}")
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
        if left_partitions != right_partitions {
            return internal_err!(
                "Invalid SortMergeJoinExec, partition count mismatch {left_partitions}!={right_partitions},\
                 consider using RepartitionExec"
            );
        }
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
            self.on.clone(),
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        builder::{BooleanBuilder, UInt64Builder},
        BooleanArray, Date32Array, Date64Array, Int32Array, RecordBatch, UInt64Array,
    };
    use arrow::compute::{concat_batches, filter_record_batch, SortOptions};
    use arrow::datatypes::{DataType, Field, Schema};

    use datafusion_common::JoinType::*;
    use datafusion_common::{
        assert_batches_eq, assert_contains, JoinType, NullEquality, Result,
    };
    use datafusion_common::{
        test_util::{batches_to_sort_string, batches_to_string},
        JoinSide,
    };
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_execution::TaskContext;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::BinaryExpr;
    use insta::{allow_duplicates, assert_snapshot};

    use crate::{
        expressions::Column,
        joins::sort_merge_join::stream::{
            get_corrected_filter_mask, JoinedRecordBatches,
        },
    };

    use crate::joins::utils::{ColumnIndex, JoinFilter, JoinOn};
    use crate::joins::SortMergeJoinExec;
    use crate::test::TestMemoryExec;
    use crate::test::{build_table_i32, build_table_i32_two_cols};
    use crate::{common, ExecutionPlan};

    fn build_table(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_i32(a, b, c);
        let schema = batch.schema();
        TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    fn build_table_from_batches(batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
        let schema = batches.first().unwrap().schema();
        TestMemoryExec::try_new_exec(&[batches], schema, None).unwrap()
    }

    fn build_date_table(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Date32, false),
            Field::new(b.0, DataType::Date32, false),
            Field::new(c.0, DataType::Date32, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Date32Array::from(a.1.clone())),
                Arc::new(Date32Array::from(b.1.clone())),
                Arc::new(Date32Array::from(c.1.clone())),
            ],
        )
        .unwrap();

        let schema = batch.schema();
        TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    fn build_date64_table(
        a: (&str, &Vec<i64>),
        b: (&str, &Vec<i64>),
        c: (&str, &Vec<i64>),
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Date64, false),
            Field::new(b.0, DataType::Date64, false),
            Field::new(c.0, DataType::Date64, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Date64Array::from(a.1.clone())),
                Arc::new(Date64Array::from(b.1.clone())),
                Arc::new(Date64Array::from(c.1.clone())),
            ],
        )
        .unwrap();

        let schema = batch.schema();
        TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    /// returns a table with 3 columns of i32 in memory
    pub fn build_table_i32_nullable(
        a: (&str, &Vec<Option<i32>>),
        b: (&str, &Vec<Option<i32>>),
        c: (&str, &Vec<Option<i32>>),
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new(a.0, DataType::Int32, true),
            Field::new(b.0, DataType::Int32, true),
            Field::new(c.0, DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(a.1.clone())),
                Arc::new(Int32Array::from(b.1.clone())),
                Arc::new(Int32Array::from(c.1.clone())),
            ],
        )
        .unwrap();
        TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    pub fn build_table_two_cols(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_i32_two_cols(a, b);
        let schema = batch.schema();
        TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    fn join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
    ) -> Result<SortMergeJoinExec> {
        let sort_options = vec![SortOptions::default(); on.len()];
        SortMergeJoinExec::try_new(
            left,
            right,
            on,
            None,
            join_type,
            sort_options,
            NullEquality::NullEqualsNothing,
        )
    }

    fn join_with_options(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
        sort_options: Vec<SortOptions>,
        null_equality: NullEquality,
    ) -> Result<SortMergeJoinExec> {
        SortMergeJoinExec::try_new(
            left,
            right,
            on,
            None,
            join_type,
            sort_options,
            null_equality,
        )
    }

    fn join_with_filter(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        filter: JoinFilter,
        join_type: JoinType,
        sort_options: Vec<SortOptions>,
        null_equality: NullEquality,
    ) -> Result<SortMergeJoinExec> {
        SortMergeJoinExec::try_new(
            left,
            right,
            on,
            Some(filter),
            join_type,
            sort_options,
            null_equality,
        )
    }

    async fn join_collect(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        let sort_options = vec![SortOptions::default(); on.len()];
        join_collect_with_options(
            left,
            right,
            on,
            join_type,
            sort_options,
            NullEquality::NullEqualsNothing,
        )
        .await
    }

    async fn join_collect_with_filter(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        filter: JoinFilter,
        join_type: JoinType,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        let sort_options = vec![SortOptions::default(); on.len()];

        let task_ctx = Arc::new(TaskContext::default());
        let join = join_with_filter(
            left,
            right,
            on,
            filter,
            join_type,
            sort_options,
            NullEquality::NullEqualsNothing,
        )?;
        let columns = columns(&join.schema());

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;
        Ok((columns, batches))
    }

    async fn join_collect_with_options(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
        sort_options: Vec<SortOptions>,
        null_equality: NullEquality,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        let task_ctx = Arc::new(TaskContext::default());
        let join =
            join_with_options(left, right, on, join_type, sort_options, null_equality)?;
        let columns = columns(&join.schema());

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;
        Ok((columns, batches))
    }

    async fn join_collect_batch_size_equals_two(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        let task_ctx = TaskContext::default()
            .with_session_config(SessionConfig::new().with_batch_size(2));
        let task_ctx = Arc::new(task_ctx);
        let join = join(left, right, on, join_type)?;
        let columns = columns(&join.schema());

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;
        Ok((columns, batches))
    }

    #[tokio::test]
    async fn join_inner_one() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 5]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (_, batches) = join_collect(left, right, on, Inner).await?;

        // The output order is important as SMJ preserves sortedness
        assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b1 | c2 |
            +----+----+----+----+----+----+
            | 1  | 4  | 7  | 10 | 4  | 70 |
            | 2  | 5  | 8  | 20 | 5  | 80 |
            | 3  | 5  | 9  | 20 | 5  | 80 |
            +----+----+----+----+----+----+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_two() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 2]),
            ("b2", &vec![1, 2, 2]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b2", &vec![1, 2, 2]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![
            (
                Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
            ),
            (
                Arc::new(Column::new_with_schema("b2", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
            ),
        ];

        let (_columns, batches) = join_collect(left, right, on, Inner).await?;

        // The output order is important as SMJ preserves sortedness
        assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b2 | c1 | a1 | b2 | c2 |
            +----+----+----+----+----+----+
            | 1  | 1  | 7  | 1  | 1  | 70 |
            | 2  | 2  | 8  | 2  | 2  | 80 |
            | 2  | 2  | 9  | 2  | 2  | 80 |
            +----+----+----+----+----+----+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_two_two() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 1, 2]),
            ("b2", &vec![1, 1, 2]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a1", &vec![1, 1, 3]),
            ("b2", &vec![1, 1, 2]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![
            (
                Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
            ),
            (
                Arc::new(Column::new_with_schema("b2", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
            ),
        ];

        let (_columns, batches) = join_collect(left, right, on, Inner).await?;

        // The output order is important as SMJ preserves sortedness
        assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b2 | c1 | a1 | b2 | c2 |
            +----+----+----+----+----+----+
            | 1  | 1  | 7  | 1  | 1  | 70 |
            | 1  | 1  | 7  | 1  | 1  | 80 |
            | 1  | 1  | 8  | 1  | 1  | 70 |
            | 1  | 1  | 8  | 1  | 1  | 80 |
            +----+----+----+----+----+----+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_with_nulls() -> Result<()> {
        let left = build_table_i32_nullable(
            ("a1", &vec![Some(1), Some(1), Some(2), Some(2)]),
            ("b2", &vec![None, Some(1), Some(2), Some(2)]), // null in key field
            ("c1", &vec![Some(1), None, Some(8), Some(9)]), // null in non-key field
        );
        let right = build_table_i32_nullable(
            ("a1", &vec![Some(1), Some(1), Some(2), Some(3)]),
            ("b2", &vec![None, Some(1), Some(2), Some(2)]),
            ("c2", &vec![Some(10), Some(70), Some(80), Some(90)]),
        );
        let on = vec![
            (
                Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
            ),
            (
                Arc::new(Column::new_with_schema("b2", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
            ),
        ];

        let (_, batches) = join_collect(left, right, on, Inner).await?;
        // The output order is important as SMJ preserves sortedness
        assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b2 | c1 | a1 | b2 | c2 |
            +----+----+----+----+----+----+
            | 1  | 1  |    | 1  | 1  | 70 |
            | 2  | 2  | 8  | 2  | 2  | 80 |
            | 2  | 2  | 9  | 2  | 2  | 80 |
            +----+----+----+----+----+----+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_with_nulls_with_options() -> Result<()> {
        let left = build_table_i32_nullable(
            ("a1", &vec![Some(2), Some(2), Some(1), Some(1)]),
            ("b2", &vec![Some(2), Some(2), Some(1), None]), // null in key field
            ("c1", &vec![Some(9), Some(8), None, Some(1)]), // null in non-key field
        );
        let right = build_table_i32_nullable(
            ("a1", &vec![Some(3), Some(2), Some(1), Some(1)]),
            ("b2", &vec![Some(2), Some(2), Some(1), None]),
            ("c2", &vec![Some(90), Some(80), Some(70), Some(10)]),
        );
        let on = vec![
            (
                Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
            ),
            (
                Arc::new(Column::new_with_schema("b2", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
            ),
        ];
        let (_, batches) = join_collect_with_options(
            left,
            right,
            on,
            Inner,
            vec![
                SortOptions {
                    descending: true,
                    nulls_first: false,
                };
                2
            ],
            NullEquality::NullEqualsNull,
        )
        .await?;
        // The output order is important as SMJ preserves sortedness
        assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b2 | c1 | a1 | b2 | c2 |
            +----+----+----+----+----+----+
            | 2  | 2  | 9  | 2  | 2  | 80 |
            | 2  | 2  | 8  | 2  | 2  | 80 |
            | 1  | 1  |    | 1  | 1  | 70 |
            | 1  |    | 1  | 1  |    | 10 |
            +----+----+----+----+----+----+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_output_two_batches() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 2]),
            ("b2", &vec![1, 2, 2]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b2", &vec![1, 2, 2]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![
            (
                Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
            ),
            (
                Arc::new(Column::new_with_schema("b2", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
            ),
        ];

        let (_, batches) =
            join_collect_batch_size_equals_two(left, right, on, Inner).await?;
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[1].num_rows(), 1);
        // The output order is important as SMJ preserves sortedness
        assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b2 | c1 | a1 | b2 | c2 |
            +----+----+----+----+----+----+
            | 1  | 1  | 7  | 1  | 1  | 70 |
            | 2  | 2  | 8  | 2  | 2  | 80 |
            | 2  | 2  | 9  | 2  | 2  | 80 |
            +----+----+----+----+----+----+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_left_one() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (_, batches) = join_collect(left, right, on, Left).await?;
        // The output order is important as SMJ preserves sortedness
        assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b1 | c2 |
            +----+----+----+----+----+----+
            | 1  | 4  | 7  | 10 | 4  | 70 |
            | 2  | 5  | 8  | 20 | 5  | 80 |
            | 3  | 7  | 9  |    |    |    |
            +----+----+----+----+----+----+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_one() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]), // 6 does not exist on the left
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (_, batches) = join_collect(left, right, on, Right).await?;
        // The output order is important as SMJ preserves sortedness
        assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b1 | c2 |
            +----+----+----+----+----+----+
            | 1  | 4  | 7  | 10 | 4  | 70 |
            | 2  | 5  | 8  | 20 | 5  | 80 |
            |    |    |    | 30 | 6  | 90 |
            +----+----+----+----+----+----+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_full_one() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b2", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema()).unwrap()) as _,
        )];

        let (_, batches) = join_collect(left, right, on, Full).await?;
        // The output order is important as SMJ preserves sortedness
        assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b2 | c2 |
            +----+----+----+----+----+----+
            |    |    |    | 30 | 6  | 90 |
            | 1  | 4  | 7  | 10 | 4  | 70 |
            | 2  | 5  | 8  | 20 | 5  | 80 |
            | 3  | 7  | 9  |    |    |    |
            +----+----+----+----+----+----+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_left_anti() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 2, 3, 5]),
            ("b1", &vec![4, 5, 5, 7, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 8, 9, 11]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (_, batches) = join_collect(left, right, on, LeftAnti).await?;

        // The output order is important as SMJ preserves sortedness
        assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+
            | a1 | b1 | c1 |
            +----+----+----+
            | 3  | 7  | 9  |
            | 5  | 7  | 11 |
            +----+----+----+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_anti_one_one() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 2]),
            ("b1", &vec![4, 5, 5]),
            ("c1", &vec![7, 8, 8]),
        );
        let right =
            build_table_two_cols(("a2", &vec![10, 20, 30]), ("b1", &vec![4, 5, 6]));
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (_, batches) = join_collect(left, right, on, RightAnti).await?;
        // The output order is important as SMJ preserves sortedness
        assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+
            | a2 | b1 |
            +----+----+
            | 30 | 6  |
            +----+----+
            "#);

        let left2 = build_table(
            ("a1", &vec![1, 2, 2]),
            ("b1", &vec![4, 5, 5]),
            ("c1", &vec![7, 8, 8]),
        );
        let right2 = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left2.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right2.schema())?) as _,
        )];

        let (_, batches2) = join_collect(left2, right2, on, RightAnti).await?;
        // The output order is important as SMJ preserves sortedness
        assert_snapshot!(batches_to_string(&batches2), @r#"
            +----+----+----+
            | a2 | b1 | c2 |
            +----+----+----+
            | 30 | 6  | 90 |
            +----+----+----+
            "#);

        Ok(())
    }

    #[tokio::test]
    async fn join_right_anti_two_two() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 2]),
            ("b1", &vec![4, 5, 5]),
            ("c1", &vec![7, 8, 8]),
        );
        let right =
            build_table_two_cols(("a2", &vec![10, 20, 30]), ("b1", &vec![4, 5, 6]));
        let on = vec![
            (
                Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("a2", &right.schema())?) as _,
            ),
            (
                Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
            ),
        ];

        let (_, batches) = join_collect(left, right, on, RightAnti).await?;
        // The output order is important as SMJ preserves sortedness
        assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+
            | a2 | b1 |
            +----+----+
            | 10 | 4  |
            | 20 | 5  |
            | 30 | 6  |
            +----+----+
            "#);

        let left = build_table(
            ("a1", &vec![1, 2, 2]),
            ("b1", &vec![4, 5, 5]),
            ("c1", &vec![7, 8, 8]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = vec![
            (
                Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("a2", &right.schema())?) as _,
            ),
            (
                Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
            ),
        ];

        let (_, batches) = join_collect(left, right, on, RightAnti).await?;
        let expected = [
            "+----+----+----+",
            "| a2 | b1 | c2 |",
            "+----+----+----+",
            "| 10 | 4  | 70 |",
            "| 20 | 5  | 80 |",
            "| 30 | 6  | 90 |",
            "+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_right_anti_two_with_filter() -> Result<()> {
        let left = build_table(("a1", &vec![1]), ("b1", &vec![10]), ("c1", &vec![30]));
        let right = build_table(("a1", &vec![1]), ("b1", &vec![10]), ("c2", &vec![20]));
        let on = vec![
            (
                Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
            ),
            (
                Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
            ),
        ];
        let filter = JoinFilter::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("c2", 1)),
                Operator::Gt,
                Arc::new(Column::new("c1", 0)),
            )),
            vec![
                ColumnIndex {
                    index: 2,
                    side: JoinSide::Left,
                },
                ColumnIndex {
                    index: 2,
                    side: JoinSide::Right,
                },
            ],
            Arc::new(Schema::new(vec![
                Field::new("c1", DataType::Int32, true),
                Field::new("c2", DataType::Int32, true),
            ])),
        );
        let (_, batches) =
            join_collect_with_filter(left, right, on, filter, RightAnti).await?;
        assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+
            | a1 | b1 | c2 |
            +----+----+----+
            | 1  | 10 | 20 |
            +----+----+----+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_anti_with_nulls() -> Result<()> {
        let left = build_table_i32_nullable(
            ("a1", &vec![Some(0), Some(1), Some(2), Some(2), Some(3)]),
            ("b1", &vec![Some(3), Some(4), Some(5), None, Some(6)]),
            ("c2", &vec![Some(60), None, Some(80), Some(85), Some(90)]),
        );
        let right = build_table_i32_nullable(
            ("a1", &vec![Some(1), Some(2), Some(2), Some(3)]),
            ("b1", &vec![Some(4), Some(5), None, Some(6)]), // null in key field
            ("c2", &vec![Some(7), Some(8), Some(8), None]), // null in non-key field
        );
        let on = vec![
            (
                Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
            ),
            (
                Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
            ),
        ];

        let (_, batches) = join_collect(left, right, on, RightAnti).await?;
        // The output order is important as SMJ preserves sortedness
        assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+
            | a1 | b1 | c2 |
            +----+----+----+
            | 2  |    | 8  |
            +----+----+----+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_anti_with_nulls_with_options() -> Result<()> {
        let left = build_table_i32_nullable(
            ("a1", &vec![Some(1), Some(2), Some(1), Some(0), Some(2)]),
            ("b1", &vec![Some(4), Some(5), Some(5), None, Some(5)]),
            ("c1", &vec![Some(7), Some(8), Some(8), Some(60), None]),
        );
        let right = build_table_i32_nullable(
            ("a1", &vec![Some(3), Some(2), Some(2), Some(1)]),
            ("b1", &vec![None, Some(5), Some(5), Some(4)]), // null in key field
            ("c2", &vec![Some(9), None, Some(8), Some(7)]), // null in non-key field
        );
        let on = vec![
            (
                Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
            ),
            (
                Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
            ),
        ];

        let (_, batches) = join_collect_with_options(
            left,
            right,
            on,
            RightAnti,
            vec![
                SortOptions {
                    descending: true,
                    nulls_first: false,
                };
                2
            ],
            NullEquality::NullEqualsNull,
        )
        .await?;

        // The output order is important as SMJ preserves sortedness
        assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+
            | a1 | b1 | c2 |
            +----+----+----+
            | 3  |    | 9  |
            | 2  | 5  |    |
            | 2  | 5  | 8  |
            +----+----+----+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_anti_output_two_batches() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 2]),
            ("b1", &vec![4, 5, 5]),
            ("c1", &vec![7, 8, 8]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![
            (
                Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("a2", &right.schema())?) as _,
            ),
            (
                Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
            ),
        ];

        let (_, batches) =
            join_collect_batch_size_equals_two(left, right, on, LeftAnti).await?;
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[1].num_rows(), 1);
        assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+
            | a1 | b1 | c1 |
            +----+----+----+
            | 1  | 4  | 7  |
            | 2  | 5  | 8  |
            | 2  | 5  | 8  |
            +----+----+----+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_left_semi() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 2, 3]),
            ("b1", &vec![4, 5, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]), // 5 is double on the right
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (_, batches) = join_collect(left, right, on, LeftSemi).await?;
        // The output order is important as SMJ preserves sortedness
        assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+
            | a1 | b1 | c1 |
            +----+----+----+
            | 1  | 4  | 7  |
            | 2  | 5  | 8  |
            | 2  | 5  | 8  |
            +----+----+----+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_semi_one() -> Result<()> {
        let left = build_table(
            ("a1", &vec![10, 20, 30, 40]),
            ("b1", &vec![4, 5, 5, 6]),
            ("c1", &vec![70, 80, 90, 100]),
        );
        let right = build_table(
            ("a2", &vec![1, 2, 2, 3]),
            ("b1", &vec![4, 5, 5, 7]),
            ("c2", &vec![7, 8, 8, 9]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (_, batches) = join_collect(left, right, on, RightSemi).await?;
        let expected = [
            "+----+----+----+",
            "| a2 | b1 | c2 |",
            "+----+----+----+",
            "| 1  | 4  | 7  |",
            "| 2  | 5  | 8  |",
            "| 2  | 5  | 8  |",
            "+----+----+----+",
        ];
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_semi_two() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 2, 3]),
            ("b1", &vec![4, 5, 5, 6]),
            ("c1", &vec![70, 80, 90, 100]),
        );
        let right = build_table(
            ("a1", &vec![1, 2, 2, 3]),
            ("b1", &vec![4, 5, 5, 7]),
            ("c2", &vec![7, 8, 8, 9]),
        );
        let on = vec![
            (
                Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
            ),
            (
                Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
            ),
        ];

        let (_, batches) = join_collect(left, right, on, RightSemi).await?;
        let expected = [
            "+----+----+----+",
            "| a1 | b1 | c2 |",
            "+----+----+----+",
            "| 1  | 4  | 7  |",
            "| 2  | 5  | 8  |",
            "| 2  | 5  | 8  |",
            "+----+----+----+",
        ];
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_semi_two_with_filter() -> Result<()> {
        let left = build_table(("a1", &vec![1]), ("b1", &vec![10]), ("c1", &vec![30]));
        let right = build_table(("a1", &vec![1]), ("b1", &vec![10]), ("c2", &vec![20]));
        let on = vec![
            (
                Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
            ),
            (
                Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
            ),
        ];
        let filter = JoinFilter::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("c2", 1)),
                Operator::Lt,
                Arc::new(Column::new("c1", 0)),
            )),
            vec![
                ColumnIndex {
                    index: 2,
                    side: JoinSide::Left,
                },
                ColumnIndex {
                    index: 2,
                    side: JoinSide::Right,
                },
            ],
            Arc::new(Schema::new(vec![
                Field::new("c1", DataType::Int32, true),
                Field::new("c2", DataType::Int32, true),
            ])),
        );
        let (_, batches) =
            join_collect_with_filter(left, right, on, filter, RightSemi).await?;
        let expected = [
            "+----+----+----+",
            "| a1 | b1 | c2 |",
            "+----+----+----+",
            "| 1  | 10 | 20 |",
            "+----+----+----+",
        ];
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_semi_with_nulls() -> Result<()> {
        let left = build_table_i32_nullable(
            ("a1", &vec![Some(0), Some(1), Some(2), Some(2), Some(3)]),
            ("b1", &vec![Some(3), Some(4), Some(5), None, Some(6)]),
            ("c2", &vec![Some(60), None, Some(80), Some(85), Some(90)]),
        );
        let right = build_table_i32_nullable(
            ("a1", &vec![Some(1), Some(2), Some(2), Some(3)]),
            ("b1", &vec![Some(4), Some(5), None, Some(6)]), // null in key field
            ("c2", &vec![Some(7), Some(8), Some(8), None]), // null in non-key field
        );
        let on = vec![
            (
                Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
            ),
            (
                Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
            ),
        ];

        let (_, batches) = join_collect(left, right, on, RightSemi).await?;
        let expected = [
            "+----+----+----+",
            "| a1 | b1 | c2 |",
            "+----+----+----+",
            "| 1  | 4  | 7  |",
            "| 2  | 5  | 8  |",
            "| 3  | 6  |    |",
            "+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_semi_with_nulls_with_options() -> Result<()> {
        let left = build_table_i32_nullable(
            ("a1", &vec![Some(3), Some(2), Some(1), Some(0), Some(2)]),
            ("b1", &vec![None, Some(5), Some(4), None, Some(5)]),
            ("c2", &vec![Some(90), Some(80), Some(70), Some(60), None]),
        );
        let right = build_table_i32_nullable(
            ("a1", &vec![Some(3), Some(2), Some(2), Some(1)]),
            ("b1", &vec![None, Some(5), Some(5), Some(4)]), // null in key field
            ("c2", &vec![Some(9), None, Some(8), Some(7)]), // null in non-key field
        );
        let on = vec![
            (
                Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
            ),
            (
                Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
            ),
        ];

        let (_, batches) = join_collect_with_options(
            left,
            right,
            on,
            RightSemi,
            vec![
                SortOptions {
                    descending: true,
                    nulls_first: false,
                };
                2
            ],
            NullEquality::NullEqualsNull,
        )
        .await?;

        let expected = [
            "+----+----+----+",
            "| a1 | b1 | c2 |",
            "+----+----+----+",
            "| 3  |    | 9  |",
            "| 2  | 5  |    |",
            "| 2  | 5  | 8  |",
            "| 1  | 4  | 7  |",
            "+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_semi_output_two_batches() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 2, 3]),
            ("b1", &vec![4, 5, 5, 6]),
            ("c1", &vec![70, 80, 90, 100]),
        );
        let right = build_table(
            ("a1", &vec![1, 2, 2, 3]),
            ("b1", &vec![4, 5, 5, 7]),
            ("c2", &vec![7, 8, 8, 9]),
        );
        let on = vec![
            (
                Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
            ),
            (
                Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
            ),
        ];

        let (_, batches) =
            join_collect_batch_size_equals_two(left, right, on, RightSemi).await?;
        let expected = [
            "+----+----+----+",
            "| a1 | b1 | c2 |",
            "+----+----+----+",
            "| 1  | 4  | 7  |",
            "| 2  | 5  | 8  |",
            "| 2  | 5  | 8  |",
            "+----+----+----+",
        ];
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[1].num_rows(), 1);
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_left_mark() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 2, 3]),
            ("b1", &vec![4, 5, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30, 40]),
            ("b1", &vec![4, 4, 5, 6]), // 5 is double on the right
            ("c2", &vec![60, 70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (_, batches) = join_collect(left, right, on, LeftMark).await?;
        // The output order is important as SMJ preserves sortedness
        assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+-------+
            | a1 | b1 | c1 | mark  |
            +----+----+----+-------+
            | 1  | 4  | 7  | true  |
            | 2  | 5  | 8  | true  |
            | 2  | 5  | 8  | true  |
            | 3  | 7  | 9  | false |
            +----+----+----+-------+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_with_duplicated_column_names() -> Result<()> {
        let left = build_table(
            ("a", &vec![1, 2, 3]),
            ("b", &vec![4, 5, 7]),
            ("c", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a", &vec![10, 20, 30]),
            ("b", &vec![1, 2, 7]),
            ("c", &vec![70, 80, 90]),
        );
        let on = vec![(
            // join on a=b so there are duplicate column names on unjoined columns
            Arc::new(Column::new_with_schema("a", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b", &right.schema())?) as _,
        )];

        let (_, batches) = join_collect(left, right, on, Inner).await?;
        // The output order is important as SMJ preserves sortedness
        assert_snapshot!(batches_to_string(&batches), @r#"
            +---+---+---+----+---+----+
            | a | b | c | a  | b | c  |
            +---+---+---+----+---+----+
            | 1 | 4 | 7 | 10 | 1 | 70 |
            | 2 | 5 | 8 | 20 | 2 | 80 |
            +---+---+---+----+---+----+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_date32() -> Result<()> {
        let left = build_date_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![19107, 19108, 19108]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_date_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![19107, 19108, 19109]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (_, batches) = join_collect(left, right, on, Inner).await?;

        // The output order is important as SMJ preserves sortedness
        assert_snapshot!(batches_to_string(&batches), @r#"
            +------------+------------+------------+------------+------------+------------+
            | a1         | b1         | c1         | a2         | b1         | c2         |
            +------------+------------+------------+------------+------------+------------+
            | 1970-01-02 | 2022-04-25 | 1970-01-08 | 1970-01-11 | 2022-04-25 | 1970-03-12 |
            | 1970-01-03 | 2022-04-26 | 1970-01-09 | 1970-01-21 | 2022-04-26 | 1970-03-22 |
            | 1970-01-04 | 2022-04-26 | 1970-01-10 | 1970-01-21 | 2022-04-26 | 1970-03-22 |
            +------------+------------+------------+------------+------------+------------+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_date64() -> Result<()> {
        let left = build_date64_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1650703441000, 1650903441000, 1650903441000]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_date64_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![1650703441000, 1650503441000, 1650903441000]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (_, batches) = join_collect(left, right, on, Inner).await?;

        // The output order is important as SMJ preserves sortedness
        assert_snapshot!(batches_to_string(&batches), @r#"
            +-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+
            | a1                      | b1                  | c1                      | a2                      | b1                  | c2                      |
            +-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+
            | 1970-01-01T00:00:00.001 | 2022-04-23T08:44:01 | 1970-01-01T00:00:00.007 | 1970-01-01T00:00:00.010 | 2022-04-23T08:44:01 | 1970-01-01T00:00:00.070 |
            | 1970-01-01T00:00:00.002 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.008 | 1970-01-01T00:00:00.030 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.090 |
            | 1970-01-01T00:00:00.003 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.009 | 1970-01-01T00:00:00.030 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.090 |
            +-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_left_sort_order() -> Result<()> {
        let left = build_table(
            ("a1", &vec![0, 1, 2, 3, 4, 5]),
            ("b1", &vec![3, 4, 5, 6, 6, 7]),
            ("c1", &vec![4, 5, 6, 7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![0, 10, 20, 30, 40]),
            ("b2", &vec![2, 4, 6, 6, 8]),
            ("c2", &vec![50, 60, 70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];

        let (_, batches) = join_collect(left, right, on, Left).await?;
        assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b2 | c2 |
            +----+----+----+----+----+----+
            | 0  | 3  | 4  |    |    |    |
            | 1  | 4  | 5  | 10 | 4  | 60 |
            | 2  | 5  | 6  |    |    |    |
            | 3  | 6  | 7  | 20 | 6  | 70 |
            | 3  | 6  | 7  | 30 | 6  | 80 |
            | 4  | 6  | 8  | 20 | 6  | 70 |
            | 4  | 6  | 8  | 30 | 6  | 80 |
            | 5  | 7  | 9  |    |    |    |
            +----+----+----+----+----+----+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_sort_order() -> Result<()> {
        let left = build_table(
            ("a1", &vec![0, 1, 2, 3]),
            ("b1", &vec![3, 4, 5, 7]),
            ("c1", &vec![6, 7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![0, 10, 20, 30]),
            ("b2", &vec![2, 4, 5, 6]),
            ("c2", &vec![60, 70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];

        let (_, batches) = join_collect(left, right, on, Right).await?;
        assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b2 | c2 |
            +----+----+----+----+----+----+
            |    |    |    | 0  | 2  | 60 |
            | 1  | 4  | 7  | 10 | 4  | 70 |
            | 2  | 5  | 8  | 20 | 5  | 80 |
            |    |    |    | 30 | 6  | 90 |
            +----+----+----+----+----+----+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_left_multiple_batches() -> Result<()> {
        let left_batch_1 = build_table_i32(
            ("a1", &vec![0, 1, 2]),
            ("b1", &vec![3, 4, 5]),
            ("c1", &vec![4, 5, 6]),
        );
        let left_batch_2 = build_table_i32(
            ("a1", &vec![3, 4, 5, 6]),
            ("b1", &vec![6, 6, 7, 9]),
            ("c1", &vec![7, 8, 9, 9]),
        );
        let right_batch_1 = build_table_i32(
            ("a2", &vec![0, 10, 20]),
            ("b2", &vec![2, 4, 6]),
            ("c2", &vec![50, 60, 70]),
        );
        let right_batch_2 = build_table_i32(
            ("a2", &vec![30, 40]),
            ("b2", &vec![6, 8]),
            ("c2", &vec![80, 90]),
        );
        let left = build_table_from_batches(vec![left_batch_1, left_batch_2]);
        let right = build_table_from_batches(vec![right_batch_1, right_batch_2]);
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];

        let (_, batches) = join_collect(left, right, on, Left).await?;
        assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b2 | c2 |
            +----+----+----+----+----+----+
            | 0  | 3  | 4  |    |    |    |
            | 1  | 4  | 5  | 10 | 4  | 60 |
            | 2  | 5  | 6  |    |    |    |
            | 3  | 6  | 7  | 20 | 6  | 70 |
            | 3  | 6  | 7  | 30 | 6  | 80 |
            | 4  | 6  | 8  | 20 | 6  | 70 |
            | 4  | 6  | 8  | 30 | 6  | 80 |
            | 5  | 7  | 9  |    |    |    |
            | 6  | 9  | 9  |    |    |    |
            +----+----+----+----+----+----+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_multiple_batches() -> Result<()> {
        let right_batch_1 = build_table_i32(
            ("a2", &vec![0, 1, 2]),
            ("b2", &vec![3, 4, 5]),
            ("c2", &vec![4, 5, 6]),
        );
        let right_batch_2 = build_table_i32(
            ("a2", &vec![3, 4, 5, 6]),
            ("b2", &vec![6, 6, 7, 9]),
            ("c2", &vec![7, 8, 9, 9]),
        );
        let left_batch_1 = build_table_i32(
            ("a1", &vec![0, 10, 20]),
            ("b1", &vec![2, 4, 6]),
            ("c1", &vec![50, 60, 70]),
        );
        let left_batch_2 = build_table_i32(
            ("a1", &vec![30, 40]),
            ("b1", &vec![6, 8]),
            ("c1", &vec![80, 90]),
        );
        let left = build_table_from_batches(vec![left_batch_1, left_batch_2]);
        let right = build_table_from_batches(vec![right_batch_1, right_batch_2]);
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];

        let (_, batches) = join_collect(left, right, on, Right).await?;
        assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b2 | c2 |
            +----+----+----+----+----+----+
            |    |    |    | 0  | 3  | 4  |
            | 10 | 4  | 60 | 1  | 4  | 5  |
            |    |    |    | 2  | 5  | 6  |
            | 20 | 6  | 70 | 3  | 6  | 7  |
            | 30 | 6  | 80 | 3  | 6  | 7  |
            | 20 | 6  | 70 | 4  | 6  | 8  |
            | 30 | 6  | 80 | 4  | 6  | 8  |
            |    |    |    | 5  | 7  | 9  |
            |    |    |    | 6  | 9  | 9  |
            +----+----+----+----+----+----+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_full_multiple_batches() -> Result<()> {
        let left_batch_1 = build_table_i32(
            ("a1", &vec![0, 1, 2]),
            ("b1", &vec![3, 4, 5]),
            ("c1", &vec![4, 5, 6]),
        );
        let left_batch_2 = build_table_i32(
            ("a1", &vec![3, 4, 5, 6]),
            ("b1", &vec![6, 6, 7, 9]),
            ("c1", &vec![7, 8, 9, 9]),
        );
        let right_batch_1 = build_table_i32(
            ("a2", &vec![0, 10, 20]),
            ("b2", &vec![2, 4, 6]),
            ("c2", &vec![50, 60, 70]),
        );
        let right_batch_2 = build_table_i32(
            ("a2", &vec![30, 40]),
            ("b2", &vec![6, 8]),
            ("c2", &vec![80, 90]),
        );
        let left = build_table_from_batches(vec![left_batch_1, left_batch_2]);
        let right = build_table_from_batches(vec![right_batch_1, right_batch_2]);
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];

        let (_, batches) = join_collect(left, right, on, Full).await?;
        assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b2 | c2 |
            +----+----+----+----+----+----+
            |    |    |    | 0  | 2  | 50 |
            |    |    |    | 40 | 8  | 90 |
            | 0  | 3  | 4  |    |    |    |
            | 1  | 4  | 5  | 10 | 4  | 60 |
            | 2  | 5  | 6  |    |    |    |
            | 3  | 6  | 7  | 20 | 6  | 70 |
            | 3  | 6  | 7  | 30 | 6  | 80 |
            | 4  | 6  | 8  | 20 | 6  | 70 |
            | 4  | 6  | 8  | 30 | 6  | 80 |
            | 5  | 7  | 9  |    |    |    |
            | 6  | 9  | 9  |    |    |    |
            +----+----+----+----+----+----+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn overallocation_single_batch_no_spill() -> Result<()> {
        let left = build_table(
            ("a1", &vec![0, 1, 2, 3, 4, 5]),
            ("b1", &vec![1, 2, 3, 4, 5, 6]),
            ("c1", &vec![4, 5, 6, 7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![0, 10, 20, 30, 40]),
            ("b2", &vec![1, 3, 4, 6, 8]),
            ("c2", &vec![50, 60, 70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];
        let sort_options = vec![SortOptions::default(); on.len()];

        let join_types = vec![
            Inner, Left, Right, RightSemi, Full, LeftSemi, LeftAnti, LeftMark,
        ];

        // Disable DiskManager to prevent spilling
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(100, 1.0)
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
            )
            .build_arc()?;
        let session_config = SessionConfig::default().with_batch_size(50);

        for join_type in join_types {
            let task_ctx = TaskContext::default()
                .with_session_config(session_config.clone())
                .with_runtime(Arc::clone(&runtime));
            let task_ctx = Arc::new(task_ctx);

            let join = join_with_options(
                Arc::clone(&left),
                Arc::clone(&right),
                on.clone(),
                join_type,
                sort_options.clone(),
                NullEquality::NullEqualsNothing,
            )?;

            let stream = join.execute(0, task_ctx)?;
            let err = common::collect(stream).await.unwrap_err();

            assert_contains!(err.to_string(), "Failed to allocate additional");
            assert_contains!(err.to_string(), "SMJStream[0]");
            assert_contains!(err.to_string(), "Disk spilling disabled");
            assert!(join.metrics().is_some());
            assert_eq!(join.metrics().unwrap().spill_count(), Some(0));
            assert_eq!(join.metrics().unwrap().spilled_bytes(), Some(0));
            assert_eq!(join.metrics().unwrap().spilled_rows(), Some(0));
        }

        Ok(())
    }

    #[tokio::test]
    async fn overallocation_multi_batch_no_spill() -> Result<()> {
        let left_batch_1 = build_table_i32(
            ("a1", &vec![0, 1]),
            ("b1", &vec![1, 1]),
            ("c1", &vec![4, 5]),
        );
        let left_batch_2 = build_table_i32(
            ("a1", &vec![2, 3]),
            ("b1", &vec![1, 1]),
            ("c1", &vec![6, 7]),
        );
        let left_batch_3 = build_table_i32(
            ("a1", &vec![4, 5]),
            ("b1", &vec![1, 1]),
            ("c1", &vec![8, 9]),
        );
        let right_batch_1 = build_table_i32(
            ("a2", &vec![0, 10]),
            ("b2", &vec![1, 1]),
            ("c2", &vec![50, 60]),
        );
        let right_batch_2 = build_table_i32(
            ("a2", &vec![20, 30]),
            ("b2", &vec![1, 1]),
            ("c2", &vec![70, 80]),
        );
        let right_batch_3 =
            build_table_i32(("a2", &vec![40]), ("b2", &vec![1]), ("c2", &vec![90]));
        let left =
            build_table_from_batches(vec![left_batch_1, left_batch_2, left_batch_3]);
        let right =
            build_table_from_batches(vec![right_batch_1, right_batch_2, right_batch_3]);
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];
        let sort_options = vec![SortOptions::default(); on.len()];

        let join_types = vec![
            Inner, Left, Right, RightSemi, Full, LeftSemi, LeftAnti, LeftMark,
        ];

        // Disable DiskManager to prevent spilling
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(100, 1.0)
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
            )
            .build_arc()?;
        let session_config = SessionConfig::default().with_batch_size(50);

        for join_type in join_types {
            let task_ctx = TaskContext::default()
                .with_session_config(session_config.clone())
                .with_runtime(Arc::clone(&runtime));
            let task_ctx = Arc::new(task_ctx);
            let join = join_with_options(
                Arc::clone(&left),
                Arc::clone(&right),
                on.clone(),
                join_type,
                sort_options.clone(),
                NullEquality::NullEqualsNothing,
            )?;

            let stream = join.execute(0, task_ctx)?;
            let err = common::collect(stream).await.unwrap_err();

            assert_contains!(err.to_string(), "Failed to allocate additional");
            assert_contains!(err.to_string(), "SMJStream[0]");
            assert_contains!(err.to_string(), "Disk spilling disabled");
            assert!(join.metrics().is_some());
            assert_eq!(join.metrics().unwrap().spill_count(), Some(0));
            assert_eq!(join.metrics().unwrap().spilled_bytes(), Some(0));
            assert_eq!(join.metrics().unwrap().spilled_rows(), Some(0));
        }

        Ok(())
    }

    #[tokio::test]
    async fn overallocation_single_batch_spill() -> Result<()> {
        let left = build_table(
            ("a1", &vec![0, 1, 2, 3, 4, 5]),
            ("b1", &vec![1, 2, 3, 4, 5, 6]),
            ("c1", &vec![4, 5, 6, 7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![0, 10, 20, 30, 40]),
            ("b2", &vec![1, 3, 4, 6, 8]),
            ("c2", &vec![50, 60, 70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];
        let sort_options = vec![SortOptions::default(); on.len()];

        let join_types = [
            Inner, Left, Right, RightSemi, Full, LeftSemi, LeftAnti, LeftMark,
        ];

        // Enable DiskManager to allow spilling
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(100, 1.0)
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::OsTmpDirectory),
            )
            .build_arc()?;

        for batch_size in [1, 50] {
            let session_config = SessionConfig::default().with_batch_size(batch_size);

            for join_type in &join_types {
                let task_ctx = TaskContext::default()
                    .with_session_config(session_config.clone())
                    .with_runtime(Arc::clone(&runtime));
                let task_ctx = Arc::new(task_ctx);

                let join = join_with_options(
                    Arc::clone(&left),
                    Arc::clone(&right),
                    on.clone(),
                    *join_type,
                    sort_options.clone(),
                    NullEquality::NullEqualsNothing,
                )?;

                let stream = join.execute(0, task_ctx)?;
                let spilled_join_result = common::collect(stream).await.unwrap();

                assert!(join.metrics().is_some());
                assert!(join.metrics().unwrap().spill_count().unwrap() > 0);
                assert!(join.metrics().unwrap().spilled_bytes().unwrap() > 0);
                assert!(join.metrics().unwrap().spilled_rows().unwrap() > 0);

                // Run the test with no spill configuration as
                let task_ctx_no_spill =
                    TaskContext::default().with_session_config(session_config.clone());
                let task_ctx_no_spill = Arc::new(task_ctx_no_spill);

                let join = join_with_options(
                    Arc::clone(&left),
                    Arc::clone(&right),
                    on.clone(),
                    *join_type,
                    sort_options.clone(),
                    NullEquality::NullEqualsNothing,
                )?;
                let stream = join.execute(0, task_ctx_no_spill)?;
                let no_spilled_join_result = common::collect(stream).await.unwrap();

                assert!(join.metrics().is_some());
                assert_eq!(join.metrics().unwrap().spill_count(), Some(0));
                assert_eq!(join.metrics().unwrap().spilled_bytes(), Some(0));
                assert_eq!(join.metrics().unwrap().spilled_rows(), Some(0));
                // Compare spilled and non spilled data to check spill logic doesn't corrupt the data
                assert_eq!(spilled_join_result, no_spilled_join_result);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn overallocation_multi_batch_spill() -> Result<()> {
        let left_batch_1 = build_table_i32(
            ("a1", &vec![0, 1]),
            ("b1", &vec![1, 1]),
            ("c1", &vec![4, 5]),
        );
        let left_batch_2 = build_table_i32(
            ("a1", &vec![2, 3]),
            ("b1", &vec![1, 1]),
            ("c1", &vec![6, 7]),
        );
        let left_batch_3 = build_table_i32(
            ("a1", &vec![4, 5]),
            ("b1", &vec![1, 1]),
            ("c1", &vec![8, 9]),
        );
        let right_batch_1 = build_table_i32(
            ("a2", &vec![0, 10]),
            ("b2", &vec![1, 1]),
            ("c2", &vec![50, 60]),
        );
        let right_batch_2 = build_table_i32(
            ("a2", &vec![20, 30]),
            ("b2", &vec![1, 1]),
            ("c2", &vec![70, 80]),
        );
        let right_batch_3 =
            build_table_i32(("a2", &vec![40]), ("b2", &vec![1]), ("c2", &vec![90]));
        let left =
            build_table_from_batches(vec![left_batch_1, left_batch_2, left_batch_3]);
        let right =
            build_table_from_batches(vec![right_batch_1, right_batch_2, right_batch_3]);
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];
        let sort_options = vec![SortOptions::default(); on.len()];

        let join_types = [
            Inner, Left, Right, RightSemi, Full, LeftSemi, LeftAnti, LeftMark,
        ];

        // Enable DiskManager to allow spilling
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(500, 1.0)
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::OsTmpDirectory),
            )
            .build_arc()?;

        for batch_size in [1, 50] {
            let session_config = SessionConfig::default().with_batch_size(batch_size);

            for join_type in &join_types {
                let task_ctx = TaskContext::default()
                    .with_session_config(session_config.clone())
                    .with_runtime(Arc::clone(&runtime));
                let task_ctx = Arc::new(task_ctx);
                let join = join_with_options(
                    Arc::clone(&left),
                    Arc::clone(&right),
                    on.clone(),
                    *join_type,
                    sort_options.clone(),
                    NullEquality::NullEqualsNothing,
                )?;

                let stream = join.execute(0, task_ctx)?;
                let spilled_join_result = common::collect(stream).await.unwrap();
                assert!(join.metrics().is_some());
                assert!(join.metrics().unwrap().spill_count().unwrap() > 0);
                assert!(join.metrics().unwrap().spilled_bytes().unwrap() > 0);
                assert!(join.metrics().unwrap().spilled_rows().unwrap() > 0);

                // Run the test with no spill configuration as
                let task_ctx_no_spill =
                    TaskContext::default().with_session_config(session_config.clone());
                let task_ctx_no_spill = Arc::new(task_ctx_no_spill);

                let join = join_with_options(
                    Arc::clone(&left),
                    Arc::clone(&right),
                    on.clone(),
                    *join_type,
                    sort_options.clone(),
                    NullEquality::NullEqualsNothing,
                )?;
                let stream = join.execute(0, task_ctx_no_spill)?;
                let no_spilled_join_result = common::collect(stream).await.unwrap();

                assert!(join.metrics().is_some());
                assert_eq!(join.metrics().unwrap().spill_count(), Some(0));
                assert_eq!(join.metrics().unwrap().spilled_bytes(), Some(0));
                assert_eq!(join.metrics().unwrap().spilled_rows(), Some(0));
                // Compare spilled and non spilled data to check spill logic doesn't corrupt the data
                assert_eq!(spilled_join_result, no_spilled_join_result);
            }
        }

        Ok(())
    }

    fn build_joined_record_batches() -> Result<JoinedRecordBatches> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("x", DataType::Int32, true),
            Field::new("y", DataType::Int32, true),
        ]));

        let mut batches = JoinedRecordBatches {
            batches: vec![],
            filter_mask: BooleanBuilder::new(),
            row_indices: UInt64Builder::new(),
            batch_ids: vec![],
        };

        // Insert already prejoined non-filtered rows
        batches.batches.push(RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 1])),
                Arc::new(Int32Array::from(vec![10, 10])),
                Arc::new(Int32Array::from(vec![1, 1])),
                Arc::new(Int32Array::from(vec![11, 9])),
            ],
        )?);

        batches.batches.push(RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![11])),
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![12])),
            ],
        )?);

        batches.batches.push(RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 1])),
                Arc::new(Int32Array::from(vec![12, 12])),
                Arc::new(Int32Array::from(vec![1, 1])),
                Arc::new(Int32Array::from(vec![11, 13])),
            ],
        )?);

        batches.batches.push(RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![13])),
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![12])),
            ],
        )?);

        batches.batches.push(RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 1])),
                Arc::new(Int32Array::from(vec![14, 14])),
                Arc::new(Int32Array::from(vec![1, 1])),
                Arc::new(Int32Array::from(vec![12, 11])),
            ],
        )?);

        let streamed_indices = vec![0, 0];
        batches.batch_ids.extend(vec![0; streamed_indices.len()]);
        batches
            .row_indices
            .extend(&UInt64Array::from(streamed_indices));

        let streamed_indices = vec![1];
        batches.batch_ids.extend(vec![0; streamed_indices.len()]);
        batches
            .row_indices
            .extend(&UInt64Array::from(streamed_indices));

        let streamed_indices = vec![0, 0];
        batches.batch_ids.extend(vec![1; streamed_indices.len()]);
        batches
            .row_indices
            .extend(&UInt64Array::from(streamed_indices));

        let streamed_indices = vec![0];
        batches.batch_ids.extend(vec![2; streamed_indices.len()]);
        batches
            .row_indices
            .extend(&UInt64Array::from(streamed_indices));

        let streamed_indices = vec![0, 0];
        batches.batch_ids.extend(vec![3; streamed_indices.len()]);
        batches
            .row_indices
            .extend(&UInt64Array::from(streamed_indices));

        batches
            .filter_mask
            .extend(&BooleanArray::from(vec![true, false]));
        batches.filter_mask.extend(&BooleanArray::from(vec![true]));
        batches
            .filter_mask
            .extend(&BooleanArray::from(vec![false, true]));
        batches.filter_mask.extend(&BooleanArray::from(vec![false]));
        batches
            .filter_mask
            .extend(&BooleanArray::from(vec![false, false]));

        Ok(batches)
    }

    #[tokio::test]
    async fn test_left_outer_join_filtered_mask() -> Result<()> {
        let mut joined_batches = build_joined_record_batches()?;
        let schema = joined_batches.batches.first().unwrap().schema();

        let output = concat_batches(&schema, &joined_batches.batches)?;
        let out_mask = joined_batches.filter_mask.finish();
        let out_indices = joined_batches.row_indices.finish();

        assert_eq!(
            get_corrected_filter_mask(
                Left,
                &UInt64Array::from(vec![0]),
                &[0usize],
                &BooleanArray::from(vec![true]),
                output.num_rows()
            )
            .unwrap(),
            BooleanArray::from(vec![
                true, false, false, false, false, false, false, false
            ])
        );

        assert_eq!(
            get_corrected_filter_mask(
                Left,
                &UInt64Array::from(vec![0]),
                &[0usize],
                &BooleanArray::from(vec![false]),
                output.num_rows()
            )
            .unwrap(),
            BooleanArray::from(vec![
                false, false, false, false, false, false, false, false
            ])
        );

        assert_eq!(
            get_corrected_filter_mask(
                Left,
                &UInt64Array::from(vec![0, 0]),
                &[0usize; 2],
                &BooleanArray::from(vec![true, true]),
                output.num_rows()
            )
            .unwrap(),
            BooleanArray::from(vec![
                true, true, false, false, false, false, false, false
            ])
        );

        assert_eq!(
            get_corrected_filter_mask(
                Left,
                &UInt64Array::from(vec![0, 0, 0]),
                &[0usize; 3],
                &BooleanArray::from(vec![true, true, true]),
                output.num_rows()
            )
            .unwrap(),
            BooleanArray::from(vec![true, true, true, false, false, false, false, false])
        );

        assert_eq!(
            get_corrected_filter_mask(
                Left,
                &UInt64Array::from(vec![0, 0, 0]),
                &[0usize; 3],
                &BooleanArray::from(vec![true, false, true]),
                output.num_rows()
            )
            .unwrap(),
            BooleanArray::from(vec![
                Some(true),
                None,
                Some(true),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
                Some(false)
            ])
        );

        assert_eq!(
            get_corrected_filter_mask(
                Left,
                &UInt64Array::from(vec![0, 0, 0]),
                &[0usize; 3],
                &BooleanArray::from(vec![false, false, true]),
                output.num_rows()
            )
            .unwrap(),
            BooleanArray::from(vec![
                None,
                None,
                Some(true),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
                Some(false)
            ])
        );

        assert_eq!(
            get_corrected_filter_mask(
                Left,
                &UInt64Array::from(vec![0, 0, 0]),
                &[0usize; 3],
                &BooleanArray::from(vec![false, true, true]),
                output.num_rows()
            )
            .unwrap(),
            BooleanArray::from(vec![
                None,
                Some(true),
                Some(true),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
                Some(false)
            ])
        );

        assert_eq!(
            get_corrected_filter_mask(
                Left,
                &UInt64Array::from(vec![0, 0, 0]),
                &[0usize; 3],
                &BooleanArray::from(vec![false, false, false]),
                output.num_rows()
            )
            .unwrap(),
            BooleanArray::from(vec![
                None,
                None,
                Some(false),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
                Some(false)
            ])
        );

        let corrected_mask = get_corrected_filter_mask(
            Left,
            &out_indices,
            &joined_batches.batch_ids,
            &out_mask,
            output.num_rows(),
        )
        .unwrap();

        assert_eq!(
            corrected_mask,
            BooleanArray::from(vec![
                Some(true),
                None,
                Some(true),
                None,
                Some(true),
                Some(false),
                None,
                Some(false)
            ])
        );

        let filtered_rb = filter_record_batch(&output, &corrected_mask)?;

        assert_snapshot!(batches_to_string(&[filtered_rb]), @r#"
                +---+----+---+----+
                | a | b  | x | y  |
                +---+----+---+----+
                | 1 | 10 | 1 | 11 |
                | 1 | 11 | 1 | 12 |
                | 1 | 12 | 1 | 13 |
                +---+----+---+----+
            "#);

        // output null rows

        let null_mask = arrow::compute::not(&corrected_mask)?;
        assert_eq!(
            null_mask,
            BooleanArray::from(vec![
                Some(false),
                None,
                Some(false),
                None,
                Some(false),
                Some(true),
                None,
                Some(true)
            ])
        );

        let null_joined_batch = filter_record_batch(&output, &null_mask)?;

        assert_snapshot!(batches_to_string(&[null_joined_batch]), @r#"
                +---+----+---+----+
                | a | b  | x | y  |
                +---+----+---+----+
                | 1 | 13 | 1 | 12 |
                | 1 | 14 | 1 | 11 |
                +---+----+---+----+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn test_semi_join_filtered_mask() -> Result<()> {
        for join_type in [LeftSemi, RightSemi] {
            let mut joined_batches = build_joined_record_batches()?;
            let schema = joined_batches.batches.first().unwrap().schema();

            let output = concat_batches(&schema, &joined_batches.batches)?;
            let out_mask = joined_batches.filter_mask.finish();
            let out_indices = joined_batches.row_indices.finish();

            assert_eq!(
                get_corrected_filter_mask(
                    join_type,
                    &UInt64Array::from(vec![0]),
                    &[0usize],
                    &BooleanArray::from(vec![true]),
                    output.num_rows()
                )
                .unwrap(),
                BooleanArray::from(vec![true])
            );

            assert_eq!(
                get_corrected_filter_mask(
                    join_type,
                    &UInt64Array::from(vec![0]),
                    &[0usize],
                    &BooleanArray::from(vec![false]),
                    output.num_rows()
                )
                .unwrap(),
                BooleanArray::from(vec![None])
            );

            assert_eq!(
                get_corrected_filter_mask(
                    join_type,
                    &UInt64Array::from(vec![0, 0]),
                    &[0usize; 2],
                    &BooleanArray::from(vec![true, true]),
                    output.num_rows()
                )
                .unwrap(),
                BooleanArray::from(vec![Some(true), None])
            );

            assert_eq!(
                get_corrected_filter_mask(
                    join_type,
                    &UInt64Array::from(vec![0, 0, 0]),
                    &[0usize; 3],
                    &BooleanArray::from(vec![true, true, true]),
                    output.num_rows()
                )
                .unwrap(),
                BooleanArray::from(vec![Some(true), None, None])
            );

            assert_eq!(
                get_corrected_filter_mask(
                    join_type,
                    &UInt64Array::from(vec![0, 0, 0]),
                    &[0usize; 3],
                    &BooleanArray::from(vec![true, false, true]),
                    output.num_rows()
                )
                .unwrap(),
                BooleanArray::from(vec![Some(true), None, None])
            );

            assert_eq!(
                get_corrected_filter_mask(
                    join_type,
                    &UInt64Array::from(vec![0, 0, 0]),
                    &[0usize; 3],
                    &BooleanArray::from(vec![false, false, true]),
                    output.num_rows()
                )
                .unwrap(),
                BooleanArray::from(vec![None, None, Some(true),])
            );

            assert_eq!(
                get_corrected_filter_mask(
                    join_type,
                    &UInt64Array::from(vec![0, 0, 0]),
                    &[0usize; 3],
                    &BooleanArray::from(vec![false, true, true]),
                    output.num_rows()
                )
                .unwrap(),
                BooleanArray::from(vec![None, Some(true), None])
            );

            assert_eq!(
                get_corrected_filter_mask(
                    join_type,
                    &UInt64Array::from(vec![0, 0, 0]),
                    &[0usize; 3],
                    &BooleanArray::from(vec![false, false, false]),
                    output.num_rows()
                )
                .unwrap(),
                BooleanArray::from(vec![None, None, None])
            );

            let corrected_mask = get_corrected_filter_mask(
                join_type,
                &out_indices,
                &joined_batches.batch_ids,
                &out_mask,
                output.num_rows(),
            )
            .unwrap();

            assert_eq!(
                corrected_mask,
                BooleanArray::from(vec![
                    Some(true),
                    None,
                    Some(true),
                    None,
                    Some(true),
                    None,
                    None,
                    None
                ])
            );

            let filtered_rb = filter_record_batch(&output, &corrected_mask)?;

            assert_batches_eq!(
                &[
                    "+---+----+---+----+",
                    "| a | b  | x | y  |",
                    "+---+----+---+----+",
                    "| 1 | 10 | 1 | 11 |",
                    "| 1 | 11 | 1 | 12 |",
                    "| 1 | 12 | 1 | 13 |",
                    "+---+----+---+----+",
                ],
                &[filtered_rb]
            );

            // output null rows
            let null_mask = arrow::compute::not(&corrected_mask)?;
            assert_eq!(
                null_mask,
                BooleanArray::from(vec![
                    Some(false),
                    None,
                    Some(false),
                    None,
                    Some(false),
                    None,
                    None,
                    None
                ])
            );

            let null_joined_batch = filter_record_batch(&output, &null_mask)?;

            assert_batches_eq!(
                &[
                    "+---+---+---+---+",
                    "| a | b | x | y |",
                    "+---+---+---+---+",
                    "+---+---+---+---+",
                ],
                &[null_joined_batch]
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_anti_join_filtered_mask() -> Result<()> {
        for join_type in [LeftAnti, RightAnti] {
            let mut joined_batches = build_joined_record_batches()?;
            let schema = joined_batches.batches.first().unwrap().schema();

            let output = concat_batches(&schema, &joined_batches.batches)?;
            let out_mask = joined_batches.filter_mask.finish();
            let out_indices = joined_batches.row_indices.finish();

            assert_eq!(
                get_corrected_filter_mask(
                    join_type,
                    &UInt64Array::from(vec![0]),
                    &[0usize],
                    &BooleanArray::from(vec![true]),
                    1
                )
                .unwrap(),
                BooleanArray::from(vec![None])
            );

            assert_eq!(
                get_corrected_filter_mask(
                    join_type,
                    &UInt64Array::from(vec![0]),
                    &[0usize],
                    &BooleanArray::from(vec![false]),
                    1
                )
                .unwrap(),
                BooleanArray::from(vec![Some(true)])
            );

            assert_eq!(
                get_corrected_filter_mask(
                    join_type,
                    &UInt64Array::from(vec![0, 0]),
                    &[0usize; 2],
                    &BooleanArray::from(vec![true, true]),
                    2
                )
                .unwrap(),
                BooleanArray::from(vec![None, None])
            );

            assert_eq!(
                get_corrected_filter_mask(
                    join_type,
                    &UInt64Array::from(vec![0, 0, 0]),
                    &[0usize; 3],
                    &BooleanArray::from(vec![true, true, true]),
                    3
                )
                .unwrap(),
                BooleanArray::from(vec![None, None, None])
            );

            assert_eq!(
                get_corrected_filter_mask(
                    join_type,
                    &UInt64Array::from(vec![0, 0, 0]),
                    &[0usize; 3],
                    &BooleanArray::from(vec![true, false, true]),
                    3
                )
                .unwrap(),
                BooleanArray::from(vec![None, None, None])
            );

            assert_eq!(
                get_corrected_filter_mask(
                    join_type,
                    &UInt64Array::from(vec![0, 0, 0]),
                    &[0usize; 3],
                    &BooleanArray::from(vec![false, false, true]),
                    3
                )
                .unwrap(),
                BooleanArray::from(vec![None, None, None])
            );

            assert_eq!(
                get_corrected_filter_mask(
                    join_type,
                    &UInt64Array::from(vec![0, 0, 0]),
                    &[0usize; 3],
                    &BooleanArray::from(vec![false, true, true]),
                    3
                )
                .unwrap(),
                BooleanArray::from(vec![None, None, None])
            );

            assert_eq!(
                get_corrected_filter_mask(
                    join_type,
                    &UInt64Array::from(vec![0, 0, 0]),
                    &[0usize; 3],
                    &BooleanArray::from(vec![false, false, false]),
                    3
                )
                .unwrap(),
                BooleanArray::from(vec![None, None, Some(true)])
            );

            let corrected_mask = get_corrected_filter_mask(
                join_type,
                &out_indices,
                &joined_batches.batch_ids,
                &out_mask,
                output.num_rows(),
            )
            .unwrap();

            assert_eq!(
                corrected_mask,
                BooleanArray::from(vec![
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(true),
                    None,
                    Some(true)
                ])
            );

            let filtered_rb = filter_record_batch(&output, &corrected_mask)?;

            allow_duplicates! {
                assert_snapshot!(batches_to_string(&[filtered_rb]), @r#"
                    +---+----+---+----+
                    | a | b  | x | y  |
                    +---+----+---+----+
                    | 1 | 13 | 1 | 12 |
                    | 1 | 14 | 1 | 11 |
                    +---+----+---+----+
            "#);
            }

            // output null rows
            let null_mask = arrow::compute::not(&corrected_mask)?;
            assert_eq!(
                null_mask,
                BooleanArray::from(vec![
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(false),
                    None,
                    Some(false),
                ])
            );

            let null_joined_batch = filter_record_batch(&output, &null_mask)?;

            allow_duplicates! {
                assert_snapshot!(batches_to_string(&[null_joined_batch]), @r#"
                        +---+---+---+---+
                        | a | b | x | y |
                        +---+---+---+---+
                        +---+---+---+---+
                "#);
            }
        }
        Ok(())
    }

    /// Returns the column names on the schema
    fn columns(schema: &Schema) -> Vec<String> {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }
}
