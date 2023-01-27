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
    get_final_indices_from_bit_map, load_join_input, need_produce_result_in_final,
    ColumnIndex, JoinFilter, OnceAsync, OnceFut,
};
use crate::physical_plan::{
    DisplayFormatType, Distribution, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream,
};
use arrow::array::{
    BooleanBufferBuilder, UInt32Array, UInt32Builder, UInt64Array, UInt64Builder,
};
use arrow::buffer::{buffer_bin_or, Buffer};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::RecordBatch;
use datafusion_common::Statistics;
use datafusion_expr::JoinType;
use datafusion_physical_expr::{EquivalenceProperties, PhysicalSortExpr};
use futures::{ready, Stream, StreamExt};
use parking_lot::Mutex;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use std::task::Poll;

use crate::error::Result;
use crate::execution::context::TaskContext;

/// Data of the left side
type JoinLeftData = RecordBatch;

/// Global state for all partations.
#[derive(Debug)]
struct NestedLoopJoinState {
    remain_partation_count: usize,
    visited_left_buffer: Option<Buffer>,
}

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
    // Inner state of nested loop join
    state: Arc<Mutex<NestedLoopJoinState>>,
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
        let right_partation_count = right.output_partitioning().partition_count();
        Ok(NestedLoopJoinExec {
            left,
            right,
            filter,
            join_type: *join_type,
            schema: Arc::new(schema),
            left_fut: Default::default(),
            column_indices,
            state: Arc::new(Mutex::new(NestedLoopJoinState {
                remain_partation_count: right_partation_count,
                visited_left_buffer: None,
            })),
        })
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
        let left_columns_len = self.left.schema().fields.len();
        match self.join_type {
            JoinType::Inner | JoinType::Right => adjust_right_output_partitioning(
                self.right.output_partitioning(),
                left_columns_len,
            ),
            JoinType::RightSemi | JoinType::RightAnti => self.right.output_partitioning(),
            JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti | JoinType::Full => {
                Partitioning::UnknownPartitioning(
                    self.right.output_partitioning().partition_count(),
                )
            }
        }
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        // no specified order for the output
        None
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![
            Distribution::SinglePartition,
            Distribution::UnspecifiedDistribution,
        ]
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
        let stream = self.right.execute(partition, context.clone())?;

        let left_fut = self
            .left_fut
            .once(|| load_join_input(self.left.clone(), context));

        Ok(Box::pin(NestedLoopJoinStream {
            schema: self.schema.clone(),
            filter: self.filter.clone(),
            join_type: self.join_type,
            left_fut,
            right: stream,
            is_exhausted: false,
            visited_left_side: None,
            column_indices: self.column_indices.clone(),
            state: self.state.clone(),
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
    /// state
    state: Arc<Mutex<NestedLoopJoinState>>,
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
                        // set flag
                        self.is_exhausted = true;

                        let mut state = self.state.lock();
                        assert!(state.remain_partation_count > 0);

                        // merge current visited_left_side to global state
                        let num_rows = left_data.num_rows();
                        let current_buffer = visited_left_side.finish();
                        let merged_buffer = if let Some(global_buffer) =
                            state.visited_left_buffer.as_ref()
                        {
                            buffer_bin_or(global_buffer, 0, &current_buffer, 0, num_rows)
                        } else {
                            current_buffer
                        };

                        state.remain_partation_count -= 1;
                        // the states of other partitions have been merged to global, produce the final result.
                        if state.remain_partation_count == 0 {
                            let merged_visited_left_side =
                                match merged_buffer.into_mutable() {
                                    Ok(buffer) => BooleanBufferBuilder::new_from_buffer(
                                        buffer, num_rows,
                                    ),
                                    Err(_) => {
                                        return Some(Err(ArrowError::ComputeError(
                                            "Build global visited_left_side error"
                                                .to_string(),
                                        )))
                                    }
                                };
                            // use the global left bitmap to produce the left indices and right indices
                            let (left_side, right_side) = get_final_indices_from_bit_map(
                                &merged_visited_left_side,
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

                            state.visited_left_buffer = None;
                            Some(result)
                        } else {
                            state.visited_left_buffer = Some(merged_buffer);
                            None
                        }
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
        physical_plan::{common, expressions::Column, memory::MemoryExec},
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

    fn build_multi_part_right_table() -> Arc<dyn ExecutionPlan> {
        let batch1 =
            build_table_i32(("a2", &vec![12]), ("b2", &vec![10]), ("c2", &vec![40]));
        let batch2 =
            build_table_i32(("a2", &vec![2]), ("b2", &vec![2]), ("c2", &vec![80]));
        let batch3 =
            build_table_i32(("a2", &vec![10]), ("b2", &vec![10]), ("c2", &vec![100]));
        let batch4 = build_table_i32(("a2", &vec![]), ("b2", &vec![]), ("c2", &vec![]));
        let schema = batch1.schema();
        Arc::new(
            MemoryExec::try_new(&[vec![batch1, batch2, batch3, batch4]], schema, None)
                .unwrap(),
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
        let output_partation_count = right.output_partitioning().partition_count();
        let nested_loop_join =
            NestedLoopJoinExec::try_new(left, right, join_filter, join_type)?;
        let columns = columns(&nested_loop_join.schema());
        let mut batches = vec![];
        for i in 0..output_partation_count {
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

    // single batch collect
    async fn join_collect(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        join_type: &JoinType,
        join_filter: Option<JoinFilter>,
        context: Arc<TaskContext>,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        // let right = if matches!(distribution[1], Distribution::SinglePartition) {
        //     right
        // } else {
        //     output_partition = partition_count;
        //     Arc::new(RepartitionExec::try_new(
        //         right,
        //         Partitioning::RoundRobinBatch(partition_count),
        //     )?)
        // } as Arc<dyn ExecutionPlan>;

        // // Use the required distribution for nested loop join to test partition data
        // let nested_loop_join =
        //     NestedLoopJoinExec::try_new(left, right, join_filter, join_type)?;
        // let columns = columns(&nested_loop_join.schema());
        // let mut batches = vec![];
        // for i in 0..output_partition {
        //     let stream = nested_loop_join.execute(i, context.clone())?;
        //     let more_batches = common::collect(stream).await?;
        //     batches.extend(
        //         more_batches
        //             .into_iter()
        //             .filter(|b| b.num_rows() > 0)
        //             .collect::<Vec<_>>(),
        //     );
        // }
        // Ok((columns, batches))
        let join = NestedLoopJoinExec::try_new(left, right, join_filter, join_type)?;
        let columns_header = columns(&join.schema());

        let stream = join.execute(0, context.clone())?;
        let batches = common::collect(stream).await?;

        Ok((columns_header, batches))
    }

    #[tokio::test]
    async fn join_inner_with_filter() -> Result<()> {
        let session_ctx = SessionContext::new();
        let left = build_left_table();
        let filter = prepare_join_filter();

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 5  | 5  | 50 | 2  | 2  | 80 |",
            "+----+----+----+----+----+----+",
        ];

        // single partation
        {
            let task_ctx = session_ctx.task_ctx();
            let (columns, batches) = join_collect(
                left.clone(),
                build_right_table(),
                &JoinType::Inner,
                Some(filter.clone()),
                task_ctx,
            )
            .await?;
            assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

            assert_batches_sorted_eq!(expected, &batches);
        }

        // multiple partation
        {
            let task_ctx = session_ctx.task_ctx();
            let (columns, batches) = multi_partitioned_join_collect(
                left,
                build_multi_part_right_table(),
                &JoinType::Inner,
                Some(filter),
                task_ctx,
            )
            .await?;
            assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);
            assert_batches_sorted_eq!(expected, &batches);
        }

        Ok(())
    }

    #[tokio::test]
    async fn join_left_with_filter() -> Result<()> {
        let session_ctx = SessionContext::new();
        let left = build_left_table();
        let filter = prepare_join_filter();

        let expected = vec![
            "+----+----+-----+----+----+----+",
            "| a1 | b1 | c1  | a2 | b2 | c2 |",
            "+----+----+-----+----+----+----+",
            "| 11 | 8  | 110 |    |    |    |",
            "| 5  | 5  | 50  | 2  | 2  | 80 |",
            "| 9  | 8  | 90  |    |    |    |",
            "+----+----+-----+----+----+----+",
        ];

        // single partation
        {
            let right = build_right_table();
            let task_ctx = session_ctx.task_ctx();
            let (columns, batches) = join_collect(
                left.clone(),
                right.clone(),
                &JoinType::Left,
                Some(filter.clone()),
                task_ctx.clone(),
            )
            .await?;
            assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);
            assert_batches_sorted_eq!(expected, &batches);
        }

        // multiple partation
        {
            let right = build_multi_part_right_table();
            let task_ctx = session_ctx.task_ctx();
            let (columns, batches) = multi_partitioned_join_collect(
                left,
                right,
                &JoinType::Left,
                Some(filter),
                task_ctx,
            )
            .await?;
            assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);
            assert_batches_sorted_eq!(expected, &batches);
        }

        Ok(())
    }

    #[tokio::test]
    async fn join_right_with_filter() -> Result<()> {
        let session_ctx = SessionContext::new();
        let left = build_left_table();
        let filter = prepare_join_filter();

        let expected = vec![
            "+----+----+----+----+----+-----+",
            "| a1 | b1 | c1 | a2 | b2 | c2  |",
            "+----+----+----+----+----+-----+",
            "|    |    |    | 10 | 10 | 100 |",
            "|    |    |    | 12 | 10 | 40  |",
            "| 5  | 5  | 50 | 2  | 2  | 80  |",
            "+----+----+----+----+----+-----+",
        ];

        // single partation
        {
            let right = build_right_table();
            let task_ctx = session_ctx.task_ctx();
            let (columns, batches) = join_collect(
                left.clone(),
                right.clone(),
                &JoinType::Right,
                Some(filter.clone()),
                task_ctx.clone(),
            )
            .await?;
            assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);
            assert_batches_sorted_eq!(expected, &batches);
        }

        // multiple partation
        {
            let right = build_multi_part_right_table();
            let task_ctx = session_ctx.task_ctx();
            let (columns, batches) = multi_partitioned_join_collect(
                left,
                right,
                &JoinType::Right,
                Some(filter),
                task_ctx,
            )
            .await?;
            assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);
            assert_batches_sorted_eq!(expected, &batches);
        }

        Ok(())
    }

    #[tokio::test]
    async fn join_full_with_filter() -> Result<()> {
        let session_ctx = SessionContext::new();
        let left = build_left_table();
        let filter = prepare_join_filter();
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

        // single partation
        {
            let right = build_right_table();
            let task_ctx = session_ctx.task_ctx();
            let (columns, batches) = join_collect(
                left.clone(),
                right.clone(),
                &JoinType::Full,
                Some(filter.clone()),
                task_ctx.clone(),
            )
            .await?;
            assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);
            assert_batches_sorted_eq!(expected, &batches);
        }

        // multiple partation
        {
            let right = build_multi_part_right_table();
            let task_ctx = session_ctx.task_ctx();
            let (columns, batches) = multi_partitioned_join_collect(
                left,
                right,
                &JoinType::Full,
                Some(filter),
                task_ctx,
            )
            .await?;
            assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);
            assert_batches_sorted_eq!(expected, &batches);
        }

        Ok(())
    }

    #[tokio::test]
    async fn join_left_semi_with_filter() -> Result<()> {
        let session_ctx = SessionContext::new();
        let left = build_left_table();
        let filter = prepare_join_filter();
        let expected = vec![
            "+----+----+----+",
            "| a1 | b1 | c1 |",
            "+----+----+----+",
            "| 5  | 5  | 50 |",
            "+----+----+----+",
        ];

        // single partation
        {
            let right = build_right_table();
            let task_ctx = session_ctx.task_ctx();
            let (columns, batches) = join_collect(
                left.clone(),
                right.clone(),
                &JoinType::LeftSemi,
                Some(filter.clone()),
                task_ctx.clone(),
            )
            .await?;
            assert_eq!(columns, vec!["a1", "b1", "c1"]);
            assert_batches_sorted_eq!(expected, &batches);
        }

        // multiple partation
        {
            let right = build_multi_part_right_table();
            let task_ctx = session_ctx.task_ctx();
            let (columns, batches) = multi_partitioned_join_collect(
                left,
                right,
                &JoinType::LeftSemi,
                Some(filter),
                task_ctx,
            )
            .await?;
            assert_eq!(columns, vec!["a1", "b1", "c1"]);
            assert_batches_sorted_eq!(expected, &batches);
        }

        Ok(())
    }

    #[tokio::test]
    async fn join_left_anti_with_filter() -> Result<()> {
        let session_ctx = SessionContext::new();
        let left = build_left_table();
        let filter = prepare_join_filter();
        let expected = vec![
            "+----+----+-----+",
            "| a1 | b1 | c1  |",
            "+----+----+-----+",
            "| 11 | 8  | 110 |",
            "| 9  | 8  | 90  |",
            "+----+----+-----+",
        ];

        // single partation
        {
            let right = build_right_table();
            let task_ctx = session_ctx.task_ctx();
            let (columns, batches) = join_collect(
                left.clone(),
                right.clone(),
                &JoinType::LeftAnti,
                Some(filter.clone()),
                task_ctx.clone(),
            )
            .await?;
            assert_eq!(columns, vec!["a1", "b1", "c1"]);
            assert_batches_sorted_eq!(expected, &batches);
        }

        // multiple partation
        {
            let right = build_multi_part_right_table();
            let task_ctx = session_ctx.task_ctx();
            let (columns, batches) = multi_partitioned_join_collect(
                left,
                right,
                &JoinType::LeftAnti,
                Some(filter),
                task_ctx,
            )
            .await?;
            assert_eq!(columns, vec!["a1", "b1", "c1"]);
            assert_batches_sorted_eq!(expected, &batches);
        }

        Ok(())
    }

    #[tokio::test]
    async fn join_right_semi_with_filter() -> Result<()> {
        let session_ctx = SessionContext::new();
        let left = build_left_table();
        let filter = prepare_join_filter();
        let expected = vec![
            "+----+----+----+",
            "| a2 | b2 | c2 |",
            "+----+----+----+",
            "| 2  | 2  | 80 |",
            "+----+----+----+",
        ];

        // single partation
        {
            let right = build_right_table();
            let task_ctx = session_ctx.task_ctx();
            let (columns, batches) = join_collect(
                left.clone(),
                right.clone(),
                &JoinType::RightSemi,
                Some(filter.clone()),
                task_ctx.clone(),
            )
            .await?;
            assert_eq!(columns, vec!["a2", "b2", "c2"]);
            assert_batches_sorted_eq!(expected, &batches);
        }

        // multiple partation
        {
            let right = build_multi_part_right_table();
            let task_ctx = session_ctx.task_ctx();
            let (columns, batches) = multi_partitioned_join_collect(
                left,
                right,
                &JoinType::RightSemi,
                Some(filter),
                task_ctx,
            )
            .await?;
            assert_eq!(columns, vec!["a2", "b2", "c2"]);
            assert_batches_sorted_eq!(expected, &batches);
        }

        Ok(())
    }

    #[tokio::test]
    async fn join_right_anti_with_filter() -> Result<()> {
        let session_ctx = SessionContext::new();
        let left = build_left_table();
        let filter = prepare_join_filter();
        let expected = vec![
            "+----+----+-----+",
            "| a2 | b2 | c2  |",
            "+----+----+-----+",
            "| 10 | 10 | 100 |",
            "| 12 | 10 | 40  |",
            "+----+----+-----+",
        ];

        // single partation
        {
            let right = build_right_table();
            let task_ctx = session_ctx.task_ctx();
            let (columns, batches) = join_collect(
                left.clone(),
                right.clone(),
                &JoinType::RightAnti,
                Some(filter.clone()),
                task_ctx.clone(),
            )
            .await?;
            assert_eq!(columns, vec!["a2", "b2", "c2"]);
            assert_batches_sorted_eq!(expected, &batches);
        }

        // multiple partation
        {
            let right = build_multi_part_right_table();
            let task_ctx = session_ctx.task_ctx();
            let (columns, batches) = multi_partitioned_join_collect(
                left,
                right,
                &JoinType::RightAnti,
                Some(filter),
                task_ctx,
            )
            .await?;
            assert_eq!(columns, vec!["a2", "b2", "c2"]);
            assert_batches_sorted_eq!(expected, &batches);
        }

        Ok(())
    }
}
