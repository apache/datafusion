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

//! Defines the nested loop join plan, it supports all [`JoinType`].
//! The nested loop join can execute in parallel by partitions and it is
//! determined by the [`JoinType`].

use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use std::task::Poll;

use crate::coalesce_batches::concat_batches;
use crate::joins::utils::{
    append_right_indices, apply_join_filter_to_indices, build_batch_from_indices,
    build_join_schema, check_join_is_valid, estimate_join_statistics, get_anti_indices,
    get_anti_u64_indices, get_final_indices_from_bit_map, get_semi_indices,
    get_semi_u64_indices, partitioned_join_output_partitioning, BuildProbeJoinMetrics,
    ColumnIndex, JoinFilter, OnceAsync, OnceFut,
};
use crate::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning,
    RecordBatchStream, SendableRecordBatchStream,
};

use arrow::array::{
    BooleanBufferBuilder, UInt32Array, UInt32Builder, UInt64Array, UInt64Builder,
};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::util::bit_util;
use datafusion_common::{exec_err, DataFusionError, JoinSide, Result, Statistics};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::TaskContext;
use datafusion_expr::JoinType;
use datafusion_physical_expr::equivalence::join_equivalence_properties;
use datafusion_physical_expr::{EquivalenceProperties, PhysicalSortExpr};

use futures::{ready, Stream, StreamExt, TryStreamExt};

/// Data of the inner table side
type JoinLeftData = (RecordBatch, MemoryReservation);

/// NestedLoopJoinExec executes partitions in parallel.
/// One input will be collected to a single partition, call it inner-table.
/// The other side of the input is treated as outer-table, and the output Partitioning is from it.
/// Giving an output partition number x, the execution will be:
///
/// ```text
/// for outer-table-batch in outer-table-partition-x
///     check-join(outer-table-batch, inner-table-data)
/// ```
///
/// One of the inputs will become inner table, and it is decided by the join type.
/// Following is the relation table:
///
/// | JoinType                       | Distribution (left, right)                 | Inner-table |
/// |--------------------------------|--------------------------------------------|-------------|
/// | Inner/Left/LeftSemi/LeftAnti   | (UnspecifiedDistribution, SinglePartition) | right       |
/// | Right/RightSemi/RightAnti/Full | (SinglePartition, UnspecifiedDistribution) | left        |
/// | Full                           | (SinglePartition, SinglePartition)         | left        |
///
#[derive(Debug)]
pub struct NestedLoopJoinExec {
    /// left side
    pub(crate) left: Arc<dyn ExecutionPlan>,
    /// right side
    pub(crate) right: Arc<dyn ExecutionPlan>,
    /// Filters which are applied while finding matching rows
    pub(crate) filter: Option<JoinFilter>,
    /// How the join is performed
    pub(crate) join_type: JoinType,
    /// The schema once the join is applied
    schema: SchemaRef,
    /// Build-side data
    inner_table: OnceAsync<JoinLeftData>,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl NestedLoopJoinExec {
    /// Try to create a nwe [`NestedLoopJoinExec`]
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        filter: Option<JoinFilter>,
        join_type: &JoinType,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        check_join_is_valid(&left_schema, &right_schema, &[])?;
        let (schema, column_indices) =
            build_join_schema(&left_schema, &right_schema, join_type);
        Ok(NestedLoopJoinExec {
            left,
            right,
            filter,
            join_type: *join_type,
            schema: Arc::new(schema),
            inner_table: Default::default(),
            column_indices,
            metrics: Default::default(),
        })
    }

    /// left side
    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        &self.left
    }

    /// right side
    pub fn right(&self) -> &Arc<dyn ExecutionPlan> {
        &self.right
    }

    /// Filters applied before join output
    pub fn filter(&self) -> Option<&JoinFilter> {
        self.filter.as_ref()
    }

    /// How the join is performed
    pub fn join_type(&self) -> &JoinType {
        &self.join_type
    }
}

impl DisplayAs for NestedLoopJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let display_filter = self.filter.as_ref().map_or_else(
                    || "".to_string(),
                    |f| format!(", filter={}", f.expression()),
                );
                write!(
                    f,
                    "NestedLoopJoinExec: join_type={:?}{}",
                    self.join_type, display_filter
                )
            }
        }
    }
}

impl ExecutionPlan for NestedLoopJoinExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        // the partition of output is determined by the rule of `required_input_distribution`
        if self.join_type == JoinType::Full {
            self.left.output_partitioning()
        } else {
            partitioned_join_output_partitioning(
                self.join_type,
                self.left.output_partitioning(),
                self.right.output_partitioning(),
                self.left.schema().fields.len(),
            )
        }
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        // no specified order for the output
        None
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        distribution_from_join_type(&self.join_type)
    }

    fn equivalence_properties(&self) -> EquivalenceProperties {
        join_equivalence_properties(
            self.left.equivalence_properties(),
            self.right.equivalence_properties(),
            &self.join_type,
            self.schema(),
            &self.maintains_input_order(),
            None,
            // No on columns in nested loop join
            &[],
        )
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(NestedLoopJoinExec::try_new(
            children[0].clone(),
            children[1].clone(),
            self.filter.clone(),
            &self.join_type,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let join_metrics = BuildProbeJoinMetrics::new(partition, &self.metrics);

        // Initialization reservation for load of inner table
        let load_reservation =
            MemoryConsumer::new(format!("NestedLoopJoinLoad[{partition}]"))
                .register(context.memory_pool());

        // Initialization of stream-level reservation
        let reservation =
            MemoryConsumer::new(format!("NestedLoopJoinStream[{partition}]"))
                .register(context.memory_pool());

        let (outer_table, inner_table) = if left_is_build_side(self.join_type) {
            // left must be single partition
            let inner_table = self.inner_table.once(|| {
                load_specified_partition_of_input(
                    0,
                    self.left.clone(),
                    context.clone(),
                    join_metrics.clone(),
                    load_reservation,
                )
            });
            let outer_table = self.right.execute(partition, context)?;
            (outer_table, inner_table)
        } else {
            // right must be single partition
            let inner_table = self.inner_table.once(|| {
                load_specified_partition_of_input(
                    0,
                    self.right.clone(),
                    context.clone(),
                    join_metrics.clone(),
                    load_reservation,
                )
            });
            let outer_table = self.left.execute(partition, context)?;
            (outer_table, inner_table)
        };

        Ok(Box::pin(NestedLoopJoinStream {
            schema: self.schema.clone(),
            filter: self.filter.clone(),
            join_type: self.join_type,
            outer_table,
            inner_table,
            is_exhausted: false,
            visited_left_side: None,
            column_indices: self.column_indices.clone(),
            join_metrics,
            reservation,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        estimate_join_statistics(
            self.left.clone(),
            self.right.clone(),
            vec![],
            &self.join_type,
            &self.schema,
        )
    }
}

// For the nested loop join, different join type need the different distribution for
// left and right node.
fn distribution_from_join_type(join_type: &JoinType) -> Vec<Distribution> {
    match join_type {
        JoinType::Inner | JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti => {
            // need the left data, and the right should be one partition
            vec![
                Distribution::UnspecifiedDistribution,
                Distribution::SinglePartition,
            ]
        }
        JoinType::Right | JoinType::RightSemi | JoinType::RightAnti => {
            // need the right data, and the left should be one partition
            vec![
                Distribution::SinglePartition,
                Distribution::UnspecifiedDistribution,
            ]
        }
        JoinType::Full => {
            // need the left and right data, and the left and right should be one partition
            vec![Distribution::SinglePartition, Distribution::SinglePartition]
        }
    }
}

/// Asynchronously collect the specified partition data of the input
async fn load_specified_partition_of_input(
    partition: usize,
    input: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    join_metrics: BuildProbeJoinMetrics,
    reservation: MemoryReservation,
) -> Result<JoinLeftData> {
    let stream = input.execute(partition, context)?;

    // Load all batches and count the rows
    let (batches, num_rows, _, reservation) = stream
        .try_fold(
            (Vec::new(), 0usize, join_metrics, reservation),
            |mut acc, batch| async {
                let batch_size = batch.get_array_memory_size();
                // Reserve memory for incoming batch
                acc.3.try_grow(batch_size)?;
                // Update metrics
                acc.2.build_mem_used.add(batch_size);
                acc.2.build_input_batches.add(1);
                acc.2.build_input_rows.add(batch.num_rows());
                // Update rowcount
                acc.1 += batch.num_rows();
                // Push batch to output
                acc.0.push(batch);
                Ok(acc)
            },
        )
        .await?;

    let merged_batch = concat_batches(&input.schema(), &batches, num_rows)?;

    Ok((merged_batch, reservation))
}

// BuildLeft means the left relation is the single patrition side.
// For full join, both side are single partition, so it is BuildLeft and BuildRight, treat it as BuildLeft.
pub fn left_is_build_side(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Right | JoinType::RightSemi | JoinType::RightAnti | JoinType::Full
    )
}
/// A stream that issues [RecordBatch]es as they arrive from the right  of the join.
struct NestedLoopJoinStream {
    /// Input schema
    schema: Arc<Schema>,
    /// join filter
    filter: Option<JoinFilter>,
    /// type of the join
    join_type: JoinType,
    /// the outer table data of the nested loop join
    outer_table: SendableRecordBatchStream,
    /// the inner table data of the nested loop join
    inner_table: OnceFut<JoinLeftData>,
    /// There is nothing to process anymore and left side is processed in case of full join
    is_exhausted: bool,
    /// Keeps track of the left side rows whether they are visited
    visited_left_side: Option<BooleanBufferBuilder>,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    // TODO: support null aware equal
    // null_equals_null: bool
    /// Join execution metrics
    join_metrics: BuildProbeJoinMetrics,
    /// Memory reservation for visited_left_side
    reservation: MemoryReservation,
}

fn build_join_indices(
    left_row_index: usize,
    right_batch: &RecordBatch,
    left_batch: &RecordBatch,
    filter: Option<&JoinFilter>,
) -> Result<(UInt64Array, UInt32Array)> {
    // left indices: [left_index, left_index, ...., left_index]
    // right indices: [0, 1, 2, 3, 4,....,right_row_count]

    let right_row_count = right_batch.num_rows();
    let left_indices = UInt64Array::from(vec![left_row_index as u64; right_row_count]);
    let right_indices = UInt32Array::from_iter_values(0..(right_row_count as u32));
    // in the nested loop join, the filter can contain non-equal and equal condition.
    if let Some(filter) = filter {
        apply_join_filter_to_indices(
            left_batch,
            right_batch,
            left_indices,
            right_indices,
            filter,
            JoinSide::Left,
        )
    } else {
        Ok((left_indices, right_indices))
    }
}

impl NestedLoopJoinStream {
    /// For Right/RightSemi/RightAnti/Full joins, left is the single partition side.
    fn poll_next_impl_for_build_left(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        // all left row
        let build_timer = self.join_metrics.build_time.timer();
        let (left_data, _) = match ready!(self.inner_table.get(cx)) {
            Ok(data) => data,
            Err(e) => return Poll::Ready(Some(Err(e))),
        };
        build_timer.done();

        if self.visited_left_side.is_none() && self.join_type == JoinType::Full {
            // TODO: Replace `ceil` wrapper with stable `div_cell` after
            // https://github.com/rust-lang/rust/issues/88581
            let visited_bitmap_size = bit_util::ceil(left_data.num_rows(), 8);
            self.reservation.try_grow(visited_bitmap_size)?;
            self.join_metrics.build_mem_used.add(visited_bitmap_size);
        }

        // add a bitmap for full join.
        let visited_left_side = self.visited_left_side.get_or_insert_with(|| {
            let left_num_rows = left_data.num_rows();
            // only full join need bitmap
            if self.join_type == JoinType::Full {
                let mut buffer = BooleanBufferBuilder::new(left_num_rows);
                buffer.append_n(left_num_rows, false);
                buffer
            } else {
                BooleanBufferBuilder::new(0)
            }
        });

        self.outer_table
            .poll_next_unpin(cx)
            .map(|maybe_batch| match maybe_batch {
                Some(Ok(right_batch)) => {
                    // Setting up timer & updating input metrics
                    self.join_metrics.input_batches.add(1);
                    self.join_metrics.input_rows.add(right_batch.num_rows());
                    let timer = self.join_metrics.join_time.timer();

                    let result = join_left_and_right_batch(
                        left_data,
                        &right_batch,
                        self.join_type,
                        self.filter.as_ref(),
                        &self.column_indices,
                        &self.schema,
                        visited_left_side,
                    );

                    // Recording time & updating output metrics
                    if let Ok(batch) = &result {
                        timer.done();
                        self.join_metrics.output_batches.add(1);
                        self.join_metrics.output_rows.add(batch.num_rows());
                    }

                    Some(result)
                }
                Some(err) => Some(err),
                None => {
                    if self.join_type == JoinType::Full && !self.is_exhausted {
                        // Only setting up timer, input is exhausted
                        let timer = self.join_metrics.join_time.timer();

                        // use the global left bitmap to produce the left indices and right indices
                        let (left_side, right_side) = get_final_indices_from_bit_map(
                            visited_left_side,
                            self.join_type,
                        );
                        let empty_right_batch =
                            RecordBatch::new_empty(self.outer_table.schema());
                        // use the left and right indices to produce the batch result
                        let result = build_batch_from_indices(
                            &self.schema,
                            left_data,
                            &empty_right_batch,
                            &left_side,
                            &right_side,
                            &self.column_indices,
                            JoinSide::Left,
                        );
                        self.is_exhausted = true;

                        // Recording time & updating output metrics
                        if let Ok(batch) = &result {
                            timer.done();
                            self.join_metrics.output_batches.add(1);
                            self.join_metrics.output_rows.add(batch.num_rows());
                        }

                        Some(result)
                    } else {
                        // end of the join loop
                        None
                    }
                }
            })
    }

    /// For Inner/Left/LeftSemi/LeftAnti joins, right is the single partition side.
    fn poll_next_impl_for_build_right(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        // all right row
        let build_timer = self.join_metrics.build_time.timer();
        let (right_data, _) = match ready!(self.inner_table.get(cx)) {
            Ok(data) => data,
            Err(e) => return Poll::Ready(Some(Err(e))),
        };
        build_timer.done();

        // for build right, bitmap is not needed.
        let mut empty_visited_left_side = BooleanBufferBuilder::new(0);
        self.outer_table
            .poll_next_unpin(cx)
            .map(|maybe_batch| match maybe_batch {
                Some(Ok(left_batch)) => {
                    // Setting up timer & updating input metrics
                    self.join_metrics.input_batches.add(1);
                    self.join_metrics.input_rows.add(left_batch.num_rows());
                    let timer = self.join_metrics.join_time.timer();

                    // Actual join execution
                    let result = join_left_and_right_batch(
                        &left_batch,
                        right_data,
                        self.join_type,
                        self.filter.as_ref(),
                        &self.column_indices,
                        &self.schema,
                        &mut empty_visited_left_side,
                    );

                    // Recording time & updating output metrics
                    if let Ok(batch) = &result {
                        timer.done();
                        self.join_metrics.output_batches.add(1);
                        self.join_metrics.output_rows.add(batch.num_rows());
                    }

                    Some(result)
                }
                Some(err) => Some(err),
                None => None,
            })
    }
}

fn join_left_and_right_batch(
    left_batch: &RecordBatch,
    right_batch: &RecordBatch,
    join_type: JoinType,
    filter: Option<&JoinFilter>,
    column_indices: &[ColumnIndex],
    schema: &Schema,
    visited_left_side: &mut BooleanBufferBuilder,
) -> Result<RecordBatch> {
    let indices_result = (0..left_batch.num_rows())
        .map(|left_row_index| {
            build_join_indices(left_row_index, right_batch, left_batch, filter)
        })
        .collect::<Result<Vec<(UInt64Array, UInt32Array)>>>();

    let mut left_indices_builder = UInt64Builder::new();
    let mut right_indices_builder = UInt32Builder::new();
    let left_right_indices = match indices_result {
        Err(err) => {
            exec_err!("Fail to build join indices in NestedLoopJoinExec, error:{err}")
        }
        Ok(indices) => {
            for (left_side, right_side) in indices {
                left_indices_builder
                    .append_values(left_side.values(), &vec![true; left_side.len()]);
                right_indices_builder
                    .append_values(right_side.values(), &vec![true; right_side.len()]);
            }
            Ok((
                left_indices_builder.finish(),
                right_indices_builder.finish(),
            ))
        }
    };
    match left_right_indices {
        Ok((left_side, right_side)) => {
            // set the left bitmap
            // and only full join need the left bitmap
            if join_type == JoinType::Full {
                left_side.iter().flatten().for_each(|x| {
                    visited_left_side.set_bit(x as usize, true);
                });
            }
            // adjust the two side indices base on the join type
            let (left_side, right_side) = adjust_indices_by_join_type(
                left_side,
                right_side,
                left_batch.num_rows(),
                right_batch.num_rows(),
                join_type,
            );

            build_batch_from_indices(
                schema,
                left_batch,
                right_batch,
                &left_side,
                &right_side,
                column_indices,
                JoinSide::Left,
            )
        }
        Err(e) => Err(e),
    }
}

fn adjust_indices_by_join_type(
    left_indices: UInt64Array,
    right_indices: UInt32Array,
    count_left_batch: usize,
    count_right_batch: usize,
    join_type: JoinType,
) -> (UInt64Array, UInt32Array) {
    match join_type {
        JoinType::Inner => (left_indices, right_indices),
        JoinType::Left => {
            // matched
            // unmatched left row will be produced in this batch
            let left_unmatched_indices =
                get_anti_u64_indices(count_left_batch, &left_indices);
            // combine the matched and unmatched left result together
            append_left_indices(left_indices, right_indices, left_unmatched_indices)
        }
        JoinType::LeftSemi => {
            // need to remove the duplicated record in the left side
            let left_indices = get_semi_u64_indices(count_left_batch, &left_indices);
            // the right_indices will not be used later for the `left semi` join
            (left_indices, right_indices)
        }
        JoinType::LeftAnti => {
            // need to remove the duplicated record in the left side
            // get the anti index for the left side
            let left_indices = get_anti_u64_indices(count_left_batch, &left_indices);
            // the right_indices will not be used later for the `left anti` join
            (left_indices, right_indices)
        }
        // right/right-semi/right-anti => right = outer_table, left = inner_table
        JoinType::Right | JoinType::Full => {
            // matched
            // unmatched right row will be produced in this batch
            let right_unmatched_indices =
                get_anti_indices(count_right_batch, &right_indices);
            // combine the matched and unmatched right result together
            append_right_indices(left_indices, right_indices, right_unmatched_indices)
        }
        JoinType::RightSemi => {
            // need to remove the duplicated record in the right side
            let right_indices = get_semi_indices(count_right_batch, &right_indices);
            // the left_indices will not be used later for the `right semi` join
            (left_indices, right_indices)
        }
        JoinType::RightAnti => {
            // need to remove the duplicated record in the right side
            // get the anti index for the right side
            let right_indices = get_anti_indices(count_right_batch, &right_indices);
            // the left_indices will not be used later for the `right anti` join
            (left_indices, right_indices)
        }
    }
}

/// Appends the `left_unmatched_indices` to the `left_indices`,
/// and fills Null to tail of `right_indices` to
/// keep the length of `left_indices` and `right_indices` consistent.
fn append_left_indices(
    left_indices: UInt64Array,
    right_indices: UInt32Array,
    left_unmatched_indices: UInt64Array,
) -> (UInt64Array, UInt32Array) {
    if left_unmatched_indices.is_empty() {
        (left_indices, right_indices)
    } else {
        let unmatched_size = left_unmatched_indices.len();
        // the new left indices: left_indices + null array
        // the new right indices: right_indices + right_unmatched_indices
        let new_left_indices = left_indices
            .iter()
            .chain(left_unmatched_indices.iter())
            .collect::<UInt64Array>();
        let new_right_indices = right_indices
            .iter()
            .chain(std::iter::repeat(None).take(unmatched_size))
            .collect::<UInt32Array>();

        (new_left_indices, new_right_indices)
    }
}

impl Stream for NestedLoopJoinStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if left_is_build_side(self.join_type) {
            self.poll_next_impl_for_build_left(cx)
        } else {
            self.poll_next_impl_for_build_right(cx)
        }
    }
}

impl RecordBatchStream for NestedLoopJoinStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{
        common, expressions::Column, memory::MemoryExec, repartition::RepartitionExec,
        test::build_table_i32,
    };

    use arrow::datatypes::{DataType, Field};
    use datafusion_common::{assert_batches_sorted_eq, assert_contains, ScalarValue};
    use datafusion_execution::runtime_env::{RuntimeConfig, RuntimeEnv};
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{BinaryExpr, Literal};
    use datafusion_physical_expr::PhysicalExpr;

    fn build_table(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_i32(a, b, c);
        let schema = batch.schema();
        Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    fn build_left_table() -> Arc<dyn ExecutionPlan> {
        build_table(
            ("a1", &vec![5, 9, 11]),
            ("b1", &vec![5, 8, 8]),
            ("c1", &vec![50, 90, 110]),
        )
    }

    fn build_right_table() -> Arc<dyn ExecutionPlan> {
        build_table(
            ("a2", &vec![12, 2, 10]),
            ("b2", &vec![10, 2, 10]),
            ("c2", &vec![40, 80, 100]),
        )
    }

    fn prepare_join_filter() -> JoinFilter {
        let column_indices = vec![
            ColumnIndex {
                index: 1,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 1,
                side: JoinSide::Right,
            },
        ];
        let intermediate_schema = Schema::new(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("x", DataType::Int32, true),
        ]);
        // left.b1!=8
        let left_filter = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 0)),
            Operator::NotEq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(8)))),
        )) as Arc<dyn PhysicalExpr>;
        // right.b2!=10
        let right_filter = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 1)),
            Operator::NotEq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
        )) as Arc<dyn PhysicalExpr>;
        // filter = left.b1!=8 and right.b2!=10
        // after filter:
        // left table:
        // ("a1", &vec![5]),
        // ("b1", &vec![5]),
        // ("c1", &vec![50]),
        // right table:
        // ("a2", &vec![12, 2]),
        // ("b2", &vec![10, 2]),
        // ("c2", &vec![40, 80]),
        let filter_expression =
            Arc::new(BinaryExpr::new(left_filter, Operator::And, right_filter))
                as Arc<dyn PhysicalExpr>;

        JoinFilter::new(filter_expression, column_indices, intermediate_schema)
    }

    async fn multi_partitioned_join_collect(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        join_type: &JoinType,
        join_filter: Option<JoinFilter>,
        context: Arc<TaskContext>,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        let partition_count = 4;
        let mut output_partition = 1;
        let distribution = distribution_from_join_type(join_type);
        // left
        let left = if matches!(distribution[0], Distribution::SinglePartition) {
            left
        } else {
            output_partition = partition_count;
            Arc::new(RepartitionExec::try_new(
                left,
                Partitioning::RoundRobinBatch(partition_count),
            )?)
        } as Arc<dyn ExecutionPlan>;

        let right = if matches!(distribution[1], Distribution::SinglePartition) {
            right
        } else {
            output_partition = partition_count;
            Arc::new(RepartitionExec::try_new(
                right,
                Partitioning::RoundRobinBatch(partition_count),
            )?)
        } as Arc<dyn ExecutionPlan>;

        // Use the required distribution for nested loop join to test partition data
        let nested_loop_join =
            NestedLoopJoinExec::try_new(left, right, join_filter, join_type)?;
        let columns = columns(&nested_loop_join.schema());
        let mut batches = vec![];
        for i in 0..output_partition {
            let stream = nested_loop_join.execute(i, context.clone())?;
            let more_batches = common::collect(stream).await?;
            batches.extend(
                more_batches
                    .into_iter()
                    .filter(|b| b.num_rows() > 0)
                    .collect::<Vec<_>>(),
            );
        }
        Ok((columns, batches))
    }

    #[tokio::test]
    async fn join_inner_with_filter() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let left = build_left_table();
        let right = build_right_table();
        let filter = prepare_join_filter();
        let (columns, batches) = multi_partitioned_join_collect(
            left,
            right,
            &JoinType::Inner,
            Some(filter),
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);
        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 5  | 5  | 50 | 2  | 2  | 80 |",
            "+----+----+----+----+----+----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_left_with_filter() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let left = build_left_table();
        let right = build_right_table();

        let filter = prepare_join_filter();
        let (columns, batches) = multi_partitioned_join_collect(
            left,
            right,
            &JoinType::Left,
            Some(filter),
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);
        let expected = [
            "+----+----+-----+----+----+----+",
            "| a1 | b1 | c1  | a2 | b2 | c2 |",
            "+----+----+-----+----+----+----+",
            "| 11 | 8  | 110 |    |    |    |",
            "| 5  | 5  | 50  | 2  | 2  | 80 |",
            "| 9  | 8  | 90  |    |    |    |",
            "+----+----+-----+----+----+----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_right_with_filter() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let left = build_left_table();
        let right = build_right_table();

        let filter = prepare_join_filter();
        let (columns, batches) = multi_partitioned_join_collect(
            left,
            right,
            &JoinType::Right,
            Some(filter),
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);
        let expected = [
            "+----+----+----+----+----+-----+",
            "| a1 | b1 | c1 | a2 | b2 | c2  |",
            "+----+----+----+----+----+-----+",
            "|    |    |    | 10 | 10 | 100 |",
            "|    |    |    | 12 | 10 | 40  |",
            "| 5  | 5  | 50 | 2  | 2  | 80  |",
            "+----+----+----+----+----+-----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_full_with_filter() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let left = build_left_table();
        let right = build_right_table();

        let filter = prepare_join_filter();
        let (columns, batches) = multi_partitioned_join_collect(
            left,
            right,
            &JoinType::Full,
            Some(filter),
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);
        let expected = [
            "+----+----+-----+----+----+-----+",
            "| a1 | b1 | c1  | a2 | b2 | c2  |",
            "+----+----+-----+----+----+-----+",
            "|    |    |     | 10 | 10 | 100 |",
            "|    |    |     | 12 | 10 | 40  |",
            "| 11 | 8  | 110 |    |    |     |",
            "| 5  | 5  | 50  | 2  | 2  | 80  |",
            "| 9  | 8  | 90  |    |    |     |",
            "+----+----+-----+----+----+-----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_left_semi_with_filter() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let left = build_left_table();
        let right = build_right_table();

        let filter = prepare_join_filter();
        let (columns, batches) = multi_partitioned_join_collect(
            left,
            right,
            &JoinType::LeftSemi,
            Some(filter),
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["a1", "b1", "c1"]);
        let expected = [
            "+----+----+----+",
            "| a1 | b1 | c1 |",
            "+----+----+----+",
            "| 5  | 5  | 50 |",
            "+----+----+----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_left_anti_with_filter() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let left = build_left_table();
        let right = build_right_table();

        let filter = prepare_join_filter();
        let (columns, batches) = multi_partitioned_join_collect(
            left,
            right,
            &JoinType::LeftAnti,
            Some(filter),
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["a1", "b1", "c1"]);
        let expected = [
            "+----+----+-----+",
            "| a1 | b1 | c1  |",
            "+----+----+-----+",
            "| 11 | 8  | 110 |",
            "| 9  | 8  | 90  |",
            "+----+----+-----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_right_semi_with_filter() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let left = build_left_table();
        let right = build_right_table();

        let filter = prepare_join_filter();
        let (columns, batches) = multi_partitioned_join_collect(
            left,
            right,
            &JoinType::RightSemi,
            Some(filter),
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["a2", "b2", "c2"]);
        let expected = [
            "+----+----+----+",
            "| a2 | b2 | c2 |",
            "+----+----+----+",
            "| 2  | 2  | 80 |",
            "+----+----+----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_right_anti_with_filter() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let left = build_left_table();
        let right = build_right_table();

        let filter = prepare_join_filter();
        let (columns, batches) = multi_partitioned_join_collect(
            left,
            right,
            &JoinType::RightAnti,
            Some(filter),
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["a2", "b2", "c2"]);
        let expected = [
            "+----+----+-----+",
            "| a2 | b2 | c2  |",
            "+----+----+-----+",
            "| 10 | 10 | 100 |",
            "| 12 | 10 | 40  |",
            "+----+----+-----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_overallocation() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
            ("b1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
            ("c1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
        );
        let right = build_table(
            ("a2", &vec![10, 11]),
            ("b2", &vec![12, 13]),
            ("c2", &vec![14, 15]),
        );
        let filter = prepare_join_filter();

        let join_types = vec![
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightSemi,
            JoinType::RightAnti,
        ];

        for join_type in join_types {
            let runtime_config = RuntimeConfig::new().with_memory_limit(100, 1.0);
            let runtime = Arc::new(RuntimeEnv::new(runtime_config)?);
            let task_ctx = TaskContext::default().with_runtime(runtime);
            let task_ctx = Arc::new(task_ctx);

            let err = multi_partitioned_join_collect(
                left.clone(),
                right.clone(),
                &join_type,
                Some(filter.clone()),
                task_ctx,
            )
            .await
            .unwrap_err();

            assert_contains!(
                err.to_string(),
                "External error: Resources exhausted: Failed to allocate additional"
            );
            assert_contains!(err.to_string(), "NestedLoopJoinLoad[0]");
        }

        Ok(())
    }

    /// Returns the column names on the schema
    fn columns(schema: &Schema) -> Vec<String> {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }
}
