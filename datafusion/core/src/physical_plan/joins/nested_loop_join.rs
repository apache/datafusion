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

use crate::physical_plan::joins::utils::{
    adjust_indices_by_join_type, adjust_right_output_partitioning,
    apply_join_filter_to_indices, build_batch_from_indices, build_join_schema,
    check_join_is_valid, combine_join_equivalence_properties, estimate_join_statistics,
    get_final_indices_from_bit_map, need_produce_result_in_final, ColumnIndex,
    JoinFilter, OnceAsync, OnceFut,
};
use crate::physical_plan::{
    DisplayFormatType, Distribution, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream,
};
use arrow::array::{
    BooleanBufferBuilder, UInt32Array, UInt32Builder, UInt64Array, UInt64Builder,
};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::RecordBatch;
use datafusion_common::Statistics;
use datafusion_expr::JoinType;
use datafusion_physical_expr::{EquivalenceProperties, PhysicalSortExpr};
use futures::{ready, Stream, StreamExt, TryStreamExt};
use log::debug;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use std::task::Poll;
use std::time::Instant;

use crate::error::Result;
use crate::execution::context::TaskContext;
use crate::physical_plan::coalesce_batches::concat_batches;

/// Data of the left side
type JoinLeftData = RecordBatch;

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
    left_fut: OnceAsync<JoinLeftData>,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
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
            left_fut: Default::default(),
            column_indices,
        })
    }

    fn is_single_partition_for_left(&self) -> bool {
        matches!(
            self.required_input_distribution()[0],
            Distribution::SinglePartition
        )
    }

    fn is_single_partition_for_right(&self) -> bool {
        matches!(
            self.required_input_distribution()[1],
            Distribution::SinglePartition
        )
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
        // TODO we can replace it by `partitioned_join_output_partitioning`
        match self.join_type {
            // use the left partition
            JoinType::Inner
            | JoinType::Left
            | JoinType::LeftSemi
            | JoinType::LeftAnti
            | JoinType::Full => self.left.output_partitioning(),
            // use the right partition
            JoinType::Right => {
                // if the partition of right is hash,
                // and the right partition should be adjusted the column index for the right expr
                adjust_right_output_partitioning(
                    self.right.output_partitioning(),
                    self.left.schema().fields.len(),
                )
            }
            // use the right partition
            JoinType::RightSemi | JoinType::RightAnti => self.right.output_partitioning(),
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
        let left_columns_len = self.left.schema().fields.len();
        combine_join_equivalence_properties(
            self.join_type,
            self.left.equivalence_properties(),
            self.right.equivalence_properties(),
            left_columns_len,
            &[], // empty join keys
            self.schema(),
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
        // left side
        let left_fut = if self.is_single_partition_for_left() {
            // if the distribution of left is `SinglePartition`, just need to collect the left one
            self.left_fut.once(|| {
                // just one partition for the left side, and the first partition is all of data for left
                load_left_specified_partition(0, self.left.clone(), context.clone())
            })
        } else {
            // the distribution of left is not single partition, just need the specified partition for left
            OnceFut::new(load_left_specified_partition(
                partition,
                self.left.clone(),
                context.clone(),
            ))
        };
        // right side
        let right_side = if self.is_single_partition_for_right() {
            // the distribution of right is `SinglePartition`
            // if the distribution of right is `SinglePartition`, just need to collect the right one
            self.right.execute(0, context)?
        } else {
            // the distribution of right is not single partition, just need the specified partition for right
            self.right.execute(partition, context)?
        };

        Ok(Box::pin(NestedLoopJoinStream {
            schema: self.schema.clone(),
            filter: self.filter.clone(),
            join_type: self.join_type,
            left_fut,
            right: right_side,
            is_exhausted: false,
            visited_left_side: None,
            column_indices: self.column_indices.clone(),
        }))
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                let display_filter = self.filter.as_ref().map_or_else(
                    || "".to_string(),
                    |f| format!(", filter={:?}", f.expression()),
                );
                write!(
                    f,
                    "NestedLoopJoinExec: join_type={:?}{}",
                    self.join_type, display_filter
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        estimate_join_statistics(
            self.left.clone(),
            self.right.clone(),
            vec![],
            &self.join_type,
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

/// Asynchronously collect the result of the left child for the specified partition
async fn load_left_specified_partition(
    partition: usize,
    left: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<JoinLeftData> {
    let start = Instant::now();
    let stream = left.execute(partition, context)?;

    // Load all batches and count the rows
    let (batches, num_rows) = stream
        .try_fold((Vec::new(), 0usize), |mut acc, batch| async {
            acc.1 += batch.num_rows();
            acc.0.push(batch);
            Ok(acc)
        })
        .await?;

    let merged_batch = concat_batches(&left.schema(), &batches, num_rows)?;

    debug!(
        "Built left-side of nested loop join containing {} rows in {} ms for partition {}",
        num_rows,
        start.elapsed().as_millis(),
        partition
    );

    Ok(merged_batch)
}

/// A stream that issues [RecordBatch]es as they arrive from the right  of the join.
struct NestedLoopJoinStream {
    /// Input schema
    schema: Arc<Schema>,
    /// join filter
    filter: Option<JoinFilter>,
    /// type of the join
    join_type: JoinType,
    /// future for data from left side
    left_fut: OnceFut<JoinLeftData>,
    /// right
    right: SendableRecordBatchStream,
    /// There is nothing to process anymore and left side is processed in case of left/left semi/left anti/full join
    is_exhausted: bool,
    /// Keeps track of the left side rows whether they are visited
    visited_left_side: Option<BooleanBufferBuilder>,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    // TODO: support null aware equal
    // null_equals_null: bool
}

fn build_join_indices(
    left_index: usize,
    batch: &RecordBatch,
    left_data: &JoinLeftData,
    filter: Option<&JoinFilter>,
) -> Result<(UInt64Array, UInt32Array)> {
    let right_row_count = batch.num_rows();
    // left indices: [left_index, left_index, ...., left_index]
    // right indices: [0, 1, 2, 3, 4,....,right_row_count]
    let left_indices = UInt64Array::from(vec![left_index as u64; right_row_count]);
    let right_indices = UInt32Array::from_iter_values(0..(right_row_count as u32));
    // in the nested loop join, the filter can contain non-equal and equal condition.
    if let Some(filter) = filter {
        apply_join_filter_to_indices(
            left_data,
            batch,
            left_indices,
            right_indices,
            filter,
        )
    } else {
        Ok((left_indices, right_indices))
    }
}

impl NestedLoopJoinStream {
    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<ArrowResult<RecordBatch>>> {
        // all left row
        let left_data = match ready!(self.left_fut.get(cx)) {
            Ok(left_data) => left_data,
            Err(e) => return Poll::Ready(Some(Err(e))),
        };

        let visited_left_side = self.visited_left_side.get_or_insert_with(|| {
            let left_num_rows = left_data.num_rows();
            if need_produce_result_in_final(self.join_type) {
                // these join type need the bitmap to identify which row has be matched or unmatched.
                // For the `left semi` join, need to use the bitmap to produce the matched row in the left side
                // For the `left` join, need to use the bitmap to produce the unmatched row in the left side with null
                // For the `left anti` join, need to use the bitmap to produce the unmatched row in the left side
                // For the `full` join, need to use the bitmap to produce the unmatched row in the left side with null
                let mut buffer = BooleanBufferBuilder::new(left_num_rows);
                buffer.append_n(left_num_rows, false);
                buffer
            } else {
                BooleanBufferBuilder::new(0)
            }
        });

        // iter the right batch
        self.right
            .poll_next_unpin(cx)
            .map(|maybe_batch| match maybe_batch {
                Some(Ok(right_batch)) => {
                    // TODO: optimize this logic like the cross join, and just return a small batch for each loop
                    // get the matched left and right indices
                    // each left row will try to match every right row
                    let indices_result = (0..left_data.num_rows())
                        .map(|left_row_index| {
                            build_join_indices(
                                left_row_index,
                                &right_batch,
                                left_data,
                                self.filter.as_ref(),
                            )
                        })
                        .collect::<Result<Vec<(UInt64Array, UInt32Array)>>>();
                    let mut left_indices_builder = UInt64Builder::new();
                    let mut right_indices_builder = UInt32Builder::new();
                    let left_right_indices = match indices_result {
                        Err(_) => {
                            // TODO why the type of result stream is `Result<T, ArrowError>`, and not the `DataFusionError`
                            Err(ArrowError::ComputeError(
                                "Build left right indices error".to_string(),
                            ))
                        }
                        Ok(indices) => {
                            for (left_side, right_side) in indices {
                                left_indices_builder.append_values(
                                    left_side.values(),
                                    &vec![true; left_side.len()],
                                );
                                right_indices_builder.append_values(
                                    right_side.values(),
                                    &vec![true; right_side.len()],
                                );
                            }
                            Ok((
                                left_indices_builder.finish(),
                                right_indices_builder.finish(),
                            ))
                        }
                    };
                    let result = match left_right_indices {
                        Ok((left_side, right_side)) => {
                            // set the left bitmap
                            // and only left, full, left semi, left anti need the left bitmap
                            if need_produce_result_in_final(self.join_type) {
                                left_side.iter().flatten().for_each(|x| {
                                    visited_left_side.set_bit(x as usize, true);
                                });
                            }
                            // adjust the two side indices base on the join type
                            let (left_side, right_side) = adjust_indices_by_join_type(
                                left_side,
                                right_side,
                                right_batch.num_rows(),
                                self.join_type,
                            );

                            let result = build_batch_from_indices(
                                &self.schema,
                                left_data,
                                &right_batch,
                                left_side,
                                right_side,
                                &self.column_indices,
                            );
                            Some(result)
                        }
                        Err(e) => Some(Err(e)),
                    };
                    result
                }
                Some(err) => Some(err),
                None => {
                    if need_produce_result_in_final(self.join_type) && !self.is_exhausted
                    {
                        // use the global left bitmap to produce the left indices and right indices
                        let (left_side, right_side) = get_final_indices_from_bit_map(
                            visited_left_side,
                            self.join_type,
                        );
                        let empty_right_batch =
                            RecordBatch::new_empty(self.right.schema());
                        // use the left and right indices to produce the batch result
                        let result = build_batch_from_indices(
                            &self.schema,
                            left_data,
                            &empty_right_batch,
                            left_side,
                            right_side,
                            &self.column_indices,
                        );
                        self.is_exhausted = true;
                        Some(result)
                    } else {
                        // end of the join loop
                        None
                    }
                }
            })
    }
}

impl Stream for NestedLoopJoinStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

impl RecordBatchStream for NestedLoopJoinStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::physical_expr::expressions::BinaryExpr;
    use crate::{
        assert_batches_sorted_eq,
        physical_plan::{
            common, expressions::Column, memory::MemoryExec, repartition::RepartitionExec,
        },
        test::{build_table_i32, columns},
    };
    use arrow::datatypes::{DataType, Field};
    use datafusion_expr::Operator;

    use super::*;
    use crate::physical_plan::joins::utils::JoinSide;
    use crate::prelude::SessionContext;
    use datafusion_common::ScalarValue;
    use datafusion_physical_expr::expressions::Literal;
    use datafusion_physical_expr::PhysicalExpr;
    use std::sync::Arc;

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
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
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
        let expected = vec![
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
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
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
        let expected = vec![
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
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
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
        let expected = vec![
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
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
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
        let expected = vec![
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
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
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
        let expected = vec![
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
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
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
        let expected = vec![
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
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
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
        let expected = vec![
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
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
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
        let expected = vec![
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
}
