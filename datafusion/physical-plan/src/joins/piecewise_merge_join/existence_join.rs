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

//! Implementation for PiecewiseMergeJoin's Existence Join (Left, Right, Full, Inner)

use arrow::array::Array;
use arrow::array::{ArrayRef, RecordBatch};
use arrow::compute::BatchCoalescer;
use arrow_schema::{Schema, SchemaRef, SortOptions};
use datafusion_common::NullEquality;
use datafusion_common::{Result, internal_err};
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_expr::Accumulator;
use datafusion_expr::{JoinType, Operator};
use datafusion_functions_aggregate_common::min_max::{MaxAccumulator, MinAccumulator};
use datafusion_physical_expr::PhysicalExprRef;
use futures::{Stream, StreamExt};
use std::{cmp::Ordering, task::ready};
use std::{sync::Arc, task::Poll};

use crate::handle_state;
use crate::joins::piecewise_merge_join::exec::{BufferedSide, BufferedSideReadyState};
use crate::joins::utils::compare_join_arrays;
use crate::joins::utils::{BuildProbeJoinMetrics, StatefulStreamResult};

pub(super) enum ExistencePWMJStreamState {
    CollectBufferedSide,
    FetchAndProcessStreamBatch,
    ProcessMatched,
    Completed,
}

pub(crate) struct ExistencePWMJStream {
    // Output schema of the `PiecewiseMergeJoin`
    pub schema: Arc<Schema>,

    // Physical expression that is evaluated on the streamed side
    // We do not need on_buffered as this is already evaluated when
    // creating the buffered side which happens before initializing
    // `PiecewiseMergeJoinStream`
    pub on_streamed: PhysicalExprRef,
    // Type of join
    pub join_type: JoinType,
    // Comparison operator
    pub operator: Operator,
    // Streamed batch
    pub streamed: SendableRecordBatchStream,
    // Buffered side data
    buffered_side: BufferedSide,
    // Tracks the state of the `PiecewiseMergeJoin`
    state: ExistencePWMJStreamState,
    // Metrics for build + probe joins
    join_metrics: BuildProbeJoinMetrics,
    // Tracking incremental state for emitting record batches
    batch_process_state: BatchProcessState,
}

impl RecordBatchStream for ExistencePWMJStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

// `ExistencePWMJStreamState` is separated into `CollectBufferedSide`, `FetchAndProcessStreamBatch`,
// `ProcessMatched`, and `Completed`.
//
// Classic Joins
//  1. `CollectBufferedSide` - Load in the buffered side data into memory.
//  2. `FetchAndProcessStreamBatch` - Finds the min/max value in the stream batch and keep a running
//      min/max value across all stream batches.
//  3. `ProcessMatched` - Compare the min/max value against the buffered side data to determine
//      which rows match the join condition and produce output batches.
//  4. `Completed` - All data has been processed.
impl ExistencePWMJStream {
    #[expect(clippy::too_many_arguments)]
    pub fn try_new(
        schema: Arc<Schema>,
        on_streamed: PhysicalExprRef,
        join_type: JoinType,
        operator: Operator,
        streamed: SendableRecordBatchStream,
        buffered_side: BufferedSide,
        state: ExistencePWMJStreamState,
        join_metrics: BuildProbeJoinMetrics,
        batch_size: usize,
    ) -> Self {
        Self {
            schema: Arc::clone(&schema),
            on_streamed,
            join_type,
            operator,
            streamed,
            buffered_side,
            state,
            join_metrics,
            batch_process_state: BatchProcessState::new(schema, batch_size),
        }
    }

    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            return match self.state {
                ExistencePWMJStreamState::CollectBufferedSide => {
                    handle_state!(ready!(self.collect_buffered_side(cx)))
                }
                ExistencePWMJStreamState::FetchAndProcessStreamBatch => {
                    handle_state!(ready!(self.fetch_and_process_stream_batch(cx)))
                }
                ExistencePWMJStreamState::ProcessMatched => {
                    handle_state!(self.process_matched_batch())
                }
                ExistencePWMJStreamState::Completed => Poll::Ready(None),
            };
        }
    }

    // Collects buffered side data
    fn collect_buffered_side(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        let build_timer = self.join_metrics.build_time.timer();
        let buffered_data = ready!(
            self.buffered_side
                .try_as_initial_mut()?
                .buffered_fut
                .get_shared(cx)
        )?;
        build_timer.done();

        // Start fetching stream batches for classic joins
        self.state = ExistencePWMJStreamState::FetchAndProcessStreamBatch;

        self.buffered_side =
            BufferedSide::Ready(BufferedSideReadyState { buffered_data });

        Poll::Ready(Ok(StatefulStreamResult::Continue))
    }

    fn fetch_and_process_stream_batch(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        // Load RecordBatches from the streamed side
        match ready!(self.streamed.poll_next_unpin(cx)) {
            None => {
                if self
                    .buffered_side
                    .try_as_ready_mut()?
                    .buffered_data
                    .remaining_partitions
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
                    == 1
                {
                    self.state = ExistencePWMJStreamState::ProcessMatched;
                } else {
                    self.state = ExistencePWMJStreamState::Completed;
                }
            }
            Some(Ok(batch)) => {
                // Evaluate the streamed physical expression on the stream batch
                let stream_values: ArrayRef = self
                    .on_streamed
                    .evaluate(&batch)?
                    .into_array(batch.num_rows())?;

                self.join_metrics.input_batches.add(1);
                self.join_metrics.input_rows.add(batch.num_rows());

                let buffered_data =
                    Arc::clone(&self.buffered_side.try_as_ready_mut()?.buffered_data);

                let min_max_val =
                    if matches!(self.operator, Operator::Lt | Operator::LtEq) {
                        let mut max_accumulator =
                            MaxAccumulator::try_new(stream_values.data_type())?;
                        max_accumulator.update_batch(&[stream_values])?;
                        max_accumulator.evaluate()?
                    } else {
                        let mut min_accumulator =
                            MinAccumulator::try_new(stream_values.data_type())?;
                        min_accumulator.update_batch(&[stream_values])?;
                        min_accumulator.evaluate()?
                    };

                let mut min_max = buffered_data.min_max_value.lock();
                match &mut *min_max {
                    None => {
                        if !min_max_val.is_null() {
                            *min_max = Some(min_max_val)
                        }
                    }
                    Some(cur) => {
                        if !min_max_val.is_null() {
                            let matches = if matches!(self.operator, Operator::Lt) {
                                min_max_val > *cur
                            } else if matches!(self.operator, Operator::LtEq) {
                                min_max_val >= *cur
                            } else if matches!(self.operator, Operator::Gt) {
                                min_max_val < *cur
                            } else {
                                min_max_val <= *cur
                            };

                            if matches {
                                *cur = min_max_val;
                            }
                        }
                    }
                }
            }
            Some(Err(err)) => return Poll::Ready(Err(err)),
        };

        Poll::Ready(Ok(StatefulStreamResult::Continue))
    }

    fn process_matched_batch(
        &mut self,
    ) -> Result<StatefulStreamResult<Option<RecordBatch>>> {
        // If batches were already returned, keep processing the coalescer until empty
        if self.batch_process_state.finished_processing_batches {
            if let Some(batch) = self
                .batch_process_state
                .output_batches
                .next_completed_batch()
            {
                return Ok(StatefulStreamResult::Ready(Some(batch)));
            } else {
                self.state = ExistencePWMJStreamState::Completed;
                return Ok(StatefulStreamResult::Continue);
            }
        }

        let min_max = self
            .buffered_side
            .try_as_ready_mut()?
            .buffered_data
            .min_max_value
            .as_ref()
            .lock()
            .clone();

        // If no min/max value was found in the streamed side, then
        // for anti joins, return the whole buffered side as output
        if min_max.is_none() {
            let buffered_values =
                self.buffered_side.try_as_ready()?.buffered_data.values();

            if matches!(self.join_type, JoinType::LeftAnti | JoinType::RightAnti) {
                let whole_buffer_batch = self
                    .buffered_side
                    .try_as_ready()?
                    .buffered_data
                    .batch()
                    .slice(0, buffered_values.len());
                self.state = ExistencePWMJStreamState::Completed;
                return Ok(StatefulStreamResult::Ready(Some(RecordBatch::try_new(
                    Arc::clone(&self.schema),
                    whole_buffer_batch.columns().to_vec(),
                )?)));
            } else {
                self.state = ExistencePWMJStreamState::Completed;
                return Ok(StatefulStreamResult::Ready(Some(RecordBatch::new_empty(
                    Arc::clone(&self.schema),
                ))));
            }
        }

        // Convert the min_max value to array for comparison
        let min_max_array = min_max.unwrap().to_array()?;
        let buffered_values = self.buffered_side.try_as_ready()?.buffered_data.values();

        // Start the pointer after the null values
        let mut buffer_idx = buffered_values.null_count();

        while buffer_idx < buffered_values.len() {
            let compare = {
                compare_join_arrays(
                    &[Arc::clone(buffered_values)],
                    buffer_idx,
                    // Always index into the single value which is the min/max value
                    &[Arc::clone(&min_max_array)],
                    0,
                    &[SortOptions::default()],
                    NullEquality::NullEqualsNothing,
                )?
            };

            let matched = match self.operator {
                Operator::Gt => {
                    matches!(compare, Ordering::Greater)
                }
                Operator::GtEq => {
                    matches!(compare, Ordering::Greater | Ordering::Equal)
                }
                Operator::Lt => {
                    matches!(compare, Ordering::Less)
                }
                Operator::LtEq => {
                    matches!(compare, Ordering::Less | Ordering::Equal)
                }
                _ => {
                    return internal_err!(
                        "PiecewiseMergeJoin should not contain operator, {}",
                        self.operator
                    );
                }
            };

            if matched {
                break;
            }

            buffer_idx += 1;
        }

        // Determine the start index and length of the new buffered batch
        // For anti joins, we include all rows before the matched index
        let start_buffer_idx =
            if matches!(self.join_type, JoinType::LeftAnti | JoinType::RightAnti) {
                0
            } else {
                buffer_idx
            };

        let buffer_length =
            if matches!(self.join_type, JoinType::LeftAnti | JoinType::RightAnti) {
                buffer_idx
            } else {
                buffered_values.len() - buffer_idx
            };

        let new_buffered_batch = self
            .buffered_side
            .try_as_ready()?
            .buffered_data
            .batch()
            .slice(start_buffer_idx, buffer_length);

        let buffered_columns = new_buffered_batch.columns().to_vec();
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), buffered_columns)?;

        if buffer_length == 0 {
            self.state = ExistencePWMJStreamState::Completed;
            return Ok(StatefulStreamResult::Ready(Some(batch)));
        }

        self.batch_process_state.output_batches.push_batch(batch)?;

        self.batch_process_state
            .output_batches
            .finish_buffered_batch()?;

        if let Some(batch) = self
            .batch_process_state
            .output_batches
            .next_completed_batch()
        {
            self.batch_process_state.finished_processing_batches = true;
            return Ok(StatefulStreamResult::Ready(Some(batch)));
        }

        self.state = ExistencePWMJStreamState::Completed;
        Ok(StatefulStreamResult::Continue)
    }
}

impl Stream for ExistencePWMJStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

struct BatchProcessState {
    // Coalescer for output batches
    output_batches: Box<BatchCoalescer>,
    // Flag to indicate if we have finished processing batches
    finished_processing_batches: bool,
}

impl BatchProcessState {
    pub fn new(join_schema: Arc<Schema>, batch_size: usize) -> Self {
        Self {
            output_batches: Box::new(BatchCoalescer::new(join_schema, batch_size)),
            finished_processing_batches: false,
        }
    }
}

// Tests for Existence Joins can only properly handle Left Semi/Anti joins because
// Right Semi/Anti are swapped
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ExecutionPlan, common,
        joins::PiecewiseMergeJoinExec,
        test::{TestMemoryExec, build_table_i32},
    };
    use arrow::array::{Date32Array, Int32Array};
    use arrow_schema::{DataType, Field};
    use datafusion_common::test_util::batches_to_string;
    use datafusion_execution::TaskContext;
    use datafusion_expr::JoinType;
    use datafusion_physical_expr::{PhysicalExpr, expressions::Column};
    use insta::assert_snapshot;
    use std::sync::Arc;

    fn columns(schema: &Schema) -> Vec<String> {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }

    fn build_table(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_i32(a, b, c);
        let schema = batch.schema();
        TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap()
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

    fn join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: (Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>),
        operator: Operator,
        join_type: JoinType,
    ) -> Result<PiecewiseMergeJoinExec> {
        PiecewiseMergeJoinExec::try_new(left, right, on, operator, join_type)
    }

    async fn join_collect(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: (PhysicalExprRef, PhysicalExprRef),
        operator: Operator,
        join_type: JoinType,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        join_collect_with_options(left, right, on, operator, join_type).await
    }

    async fn join_collect_with_options(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: (PhysicalExprRef, PhysicalExprRef),
        operator: Operator,
        join_type: JoinType,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        let task_ctx = Arc::new(TaskContext::default());
        let join = join(left, right, on, operator, join_type)?;
        let columns = columns(&join.schema());

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;
        Ok((columns, batches))
    }

    #[tokio::test]
    async fn join_left_mark_less_than() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 3  | 7  |
        // | 2  | 2  | 8  |
        // | 3  | 1  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![3, 2, 1]),
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 2  | 70 |
        // | 20 | 3  | 80 |
        // | 30 | 4  | 90 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![2, 3, 4]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::Lt, JoinType::LeftSemi).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+
        | a1 | b1 | c1 |
        +----+----+----+
        | 1  | 3  | 7  |
        | 2  | 2  | 8  |
        | 3  | 1  | 9  |
        +----+----+----+
        "#);

        Ok(())
    }

    #[tokio::test]
    async fn join_left_semi_greater_than() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 1  | 7  |
        // | 2  | 2  | 8  |
        // | 3  | 3  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1, 2, 3]),
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 2  | 70 |
        // | 20 | 3  | 80 |
        // | 30 | 4  | 90 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![2, 3, 4]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::Gt, JoinType::LeftSemi).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+
        | a1 | b1 | c1 |
        +----+----+----+
        | 3  | 3  | 9  |
        +----+----+----+
        "#);

        Ok(())
    }

    #[tokio::test]
    async fn join_left_anti_greater_than() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 1  | 7  |
        // | 2  | 2  | 8  |
        // | 3  | 3  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1, 2, 3]),
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 2  | 70 |
        // | 20 | 3  | 80 |
        // | 30 | 4  | 90 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![2, 3, 4]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::Gt, JoinType::LeftAnti).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+
        | a1 | b1 | c1 |
        +----+----+----+
        | 1  | 1  | 7  |
        | 2  | 2  | 8  |
        +----+----+----+
        "#);

        Ok(())
    }

    #[tokio::test]
    async fn join_left_semi_less_than_equal() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 3  | 7  |
        // | 2  | 2  | 8  |
        // | 3  | 1  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![3, 2, 1]),
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 2  | 70 |
        // | 20 | 3  | 80 |
        // | 30 | 4  | 90 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![2, 3, 4]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::LtEq, JoinType::LeftSemi).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+
        | a1 | b1 | c1 |
        +----+----+----+
        | 1  | 3  | 7  |
        | 2  | 2  | 8  |
        | 3  | 1  | 9  |
        +----+----+----+
        "#);

        Ok(())
    }

    #[tokio::test]
    async fn join_left_anti_greater_than_equal() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 1  | 7  |
        // | 2  | 2  | 8  |
        // | 3  | 3  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1, 2, 3]),
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 2  | 70 |
        // | 20 | 3  | 80 |
        // | 30 | 4  | 90 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![2, 3, 4]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::GtEq, JoinType::LeftAnti).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
    +----+----+----+
    | a1 | b1 | c1 |
    +----+----+----+
    | 1  | 1  | 7  |
    +----+----+----+
    "#);

        Ok(())
    }

    #[tokio::test]
    async fn join_left_semi_date32_greater_than() -> Result<()> {
        // +------------+------------+------------+
        // | a1         | b1         | c1         |
        // +------------+------------+------------+
        // | 1970-01-11 | 1970-01-11 | 1970-01-01 |
        // | 1970-01-21 | 1970-01-21 | 1970-01-01 |
        // | 1970-01-31 | 1970-01-31 | 1970-01-01 |
        // +------------+------------+------------+
        let left = build_date_table(
            ("a1", &vec![10, 20, 30]),
            ("b1", &vec![10, 20, 30]),
            ("c1", &vec![0, 0, 0]),
        );

        // +------------+------------+------------+
        // | a2         | b1         | c2         |
        // +------------+------------+------------+
        // | 1970-04-11 | 1970-01-16 | 1970-01-01 |
        // | 1970-07-19 | 1970-01-26 | 1970-01-01 |
        // | 1970-10-27 | 1970-02-05 | 1970-01-01 |
        // +------------+------------+------------+
        let right = build_date_table(
            ("a2", &vec![100, 200, 300]),
            ("b1", &vec![15, 25, 35]),
            ("c2", &vec![0, 0, 0]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::Gt, JoinType::LeftSemi).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +------------+------------+------------+
        | a1         | b1         | c1         |
        +------------+------------+------------+
        | 1970-01-21 | 1970-01-21 | 1970-01-01 |
        | 1970-01-31 | 1970-01-31 | 1970-01-01 |
        +------------+------------+------------+
        "#);

        Ok(())
    }

    // TESTING NULL CASES
    fn build_table_i32_nullable(
        a: (&str, &Vec<Option<i32>>),
        b: (&str, &Vec<Option<i32>>),
        c: (&str, &Vec<Option<i32>>),
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Int32, true),
            Field::new(b.0, DataType::Int32, true),
            Field::new(c.0, DataType::Int32, true),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(a.1.clone())) as _,
                Arc::new(Int32Array::from(b.1.clone())) as _,
                Arc::new(Int32Array::from(c.1.clone())) as _,
            ],
        )
        .unwrap();

        let schema = batch.schema();
        TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    #[tokio::test]
    async fn join_left_semi_greater_than_with_nulls() -> Result<()> {
        // +----+------+----+
        // | a1 | b1   | c1 |
        // +----+------+----+
        // | 1  | NULL | 7  |
        // | 2  | 1    | 8  |
        // | 3  | 2    | 9  |
        // | 4  | 3    | 10 |
        // +----+------+----+
        let left = build_table_i32_nullable(
            ("a1", &vec![Some(1), Some(2), Some(3), Some(4)]),
            ("b1", &vec![None, Some(1), Some(2), Some(3)]),
            ("c1", &vec![Some(7), Some(8), Some(9), Some(10)]),
        );

        // +----+------+-----+
        // | a2 | b1   | c2  |
        // +----+------+-----+
        // | 10 | 2    | 70  |
        // | 20 | 3    | 80  |
        // | 30 | NULL | 90  |
        // | 40 | 4    | 100 |
        // +----+------+-----+
        let right = build_table_i32_nullable(
            ("a2", &vec![Some(10), Some(20), Some(30), Some(40)]),
            ("b1", &vec![Some(2), Some(3), None, Some(4)]),
            ("c2", &vec![Some(70), Some(80), Some(90), Some(100)]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::Gt, JoinType::LeftSemi).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+
        | a1 | b1 | c1 |
        +----+----+----+
        | 4  | 3  | 10 |
        +----+----+----+
        "#);

        Ok(())
    }

    #[tokio::test]
    async fn join_left_anti_greater_than_with_nulls() -> Result<()> {
        // +----+------+----+
        // | a1 | b1   | c1 |
        // +----+------+----+
        // | 1  | NULL | 7  |
        // | 2  | 1    | 8  |
        // | 3  | 2    | 9  |
        // | 4  | 3    | 10 |
        // +----+------+----+
        let left = build_table_i32_nullable(
            ("a1", &vec![Some(1), Some(2), Some(3), Some(4)]),
            ("b1", &vec![None, Some(1), Some(2), Some(3)]),
            ("c1", &vec![Some(7), Some(8), Some(9), Some(10)]),
        );

        // +----+------+-----+
        // | a2 | b1   | c2  |
        // +----+------+-----+
        // | 10 | 2    | 70  |
        // | 20 | 3    | 80  |
        // | 30 | NULL | 90  |
        // | 40 | 4    | 100 |
        // +----+------+-----+
        let right = build_table_i32_nullable(
            ("a2", &vec![Some(10), Some(20), Some(30), Some(40)]),
            ("b1", &vec![Some(2), Some(3), None, Some(4)]),
            ("c2", &vec![Some(70), Some(80), Some(90), Some(100)]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::Gt, JoinType::LeftAnti).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+
        | a1 | b1 | c1 |
        +----+----+----+
        | 1  |    | 7  |
        | 2  | 1  | 8  |
        | 3  | 2  | 9  |
        +----+----+----+
        "#);

        Ok(())
    }

    #[tokio::test]
    async fn join_left_semi_less_than_equal_with_nulls() -> Result<()> {
        // +----+------+----+
        // | a1 | b1   | c1 |
        // +----+------+----+
        // | 1  | NULL | 7  |
        // | 2  | 3    | 8  |
        // | 3  | 2    | 9  |
        // | 4  | 1    | 10 |
        // +----+------+----+
        let left = build_table_i32_nullable(
            ("a1", &vec![Some(1), Some(2), Some(3), Some(4)]),
            ("b1", &vec![None, Some(3), Some(2), Some(1)]),
            ("c1", &vec![Some(7), Some(8), Some(9), Some(10)]),
        );

        // +----+------+----+
        // | a2 | b1   | c2 |
        // +----+------+----+
        // | 10 | 2    | 70 |
        // | 20 | 3    | 80 |
        // | 30 | NULL | 90 |
        // +----+------+----+
        let right = build_table_i32_nullable(
            ("a2", &vec![Some(10), Some(20), Some(30)]),
            ("b1", &vec![Some(2), Some(3), None]),
            ("c2", &vec![Some(70), Some(80), Some(90)]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::LtEq, JoinType::LeftSemi).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+
        | a1 | b1 | c1 |
        +----+----+----+
        | 2  | 3  | 8  |
        | 3  | 2  | 9  |
        | 4  | 1  | 10 |
        +----+----+----+
        "#);

        Ok(())
    }

    #[tokio::test]
    async fn join_left_anti_with_all_right_nulls() -> Result<()> {
        // +----+------+----+
        // | a1 | b1   | c1 |
        // +----+------+----+
        // | 1  | 10   | 7  |
        // | 2  | NULL | 8  |
        // +----+------+----+
        let left = build_table_i32_nullable(
            ("a1", &vec![Some(1), Some(2)]),
            ("b1", &vec![Some(10), None]),
            ("c1", &vec![Some(7), Some(8)]),
        );

        // +----+------+----+
        // | a2 | b1   | c2 |
        // +----+------+----+
        // | 10 | NULL | 70 |
        // | 20 | NULL | 80 |
        // +----+------+----+
        let right = build_table_i32_nullable(
            ("a2", &vec![Some(10), Some(20)]),
            ("b1", &vec![None, None]),
            ("c2", &vec![Some(70), Some(80)]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::Gt, JoinType::LeftAnti).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+
        | a1 | b1 | c1 |
        +----+----+----+
        | 1  | 10 | 7  |
        | 2  |    | 8  |
        +----+----+----+
        "#);

        Ok(())
    }

    #[tokio::test]
    async fn join_left_semi_with_all_right_nulls() -> Result<()> {
        // +----+------+----+
        // | a1 | b1   | c1 |
        // +----+------+----+
        // | 1  | 10   | 7  |
        // | 2  | NULL | 8  |
        // +----+------+----+
        let left = build_table_i32_nullable(
            ("a1", &vec![Some(1), Some(2)]),
            ("b1", &vec![Some(10), None]),
            ("c1", &vec![Some(7), Some(8)]),
        );

        // +----+------+----+
        // | a2 | b1   | c2 |
        // +----+------+----+
        // | 10 | NULL | 70 |
        // | 20 | NULL | 80 |
        // +----+------+----+
        let right = build_table_i32_nullable(
            ("a2", &vec![Some(10), Some(20)]),
            ("b1", &vec![None, None]),
            ("c2", &vec![Some(70), Some(80)]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::Gt, JoinType::LeftSemi).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+
        | a1 | b1 | c1 |
        +----+----+----+
        +----+----+----+
        "#);

        Ok(())
    }
}
