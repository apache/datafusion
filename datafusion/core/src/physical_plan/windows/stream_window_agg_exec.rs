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

//! Stream and channel implementations for window function expressions.

use crate::error::Result;
use crate::execution::context::TaskContext;
use crate::physical_plan::expressions::PhysicalSortExpr;
use crate::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet,
};
use crate::physical_plan::{
    common, ColumnStatistics, DisplayFormatType, Distribution, ExecutionPlan,
    Partitioning, RecordBatchStream, SendableRecordBatchStream, Statistics, WindowExpr,
};
use arrow::array::Array;
use arrow::compute::concat;
use arrow::util::pretty::print_batches;
use arrow::{
    array::ArrayRef,
    datatypes::{Schema, SchemaRef},
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};
use datafusion_common::ScalarValue;
use futures::stream::Stream;
use futures::{ready, StreamExt};
use std::any::Any;
use std::cmp::min;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion_expr::utils::{WindowSortKey, WindowSortKeys};
use datafusion_physical_expr::window::{
    AggregateWindowAccumulatorState, WindowAccumulatorResult,
};
use datafusion_physical_expr::PhysicalExpr;

/// Window execution plan
#[derive(Debug)]
pub struct StreamWindowAggExec {
    /// Input plan
    input: Arc<dyn ExecutionPlan>,
    /// Window function expression
    window_expr: Vec<Arc<dyn WindowExpr>>,
    /// Schema after the window is run
    schema: SchemaRef,
    /// Schema before the window
    input_schema: SchemaRef,
    /// Partition Keys
    pub partition_keys: Vec<Arc<dyn PhysicalExpr>>,
    /// Sort Keys
    pub sort_keys: Option<Vec<PhysicalSortExpr>>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// actual sorting of the fields
    window_sort_keys: WindowSortKeys,
}

impl StreamWindowAggExec {
    /// Create a new execution plan for window aggregates
    pub fn try_new(
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: Arc<dyn ExecutionPlan>,
        input_schema: SchemaRef,
        partition_keys: Vec<Arc<dyn PhysicalExpr>>,
        sort_keys: Option<Vec<PhysicalSortExpr>>,
        window_sort_keys: WindowSortKeys,
    ) -> Result<Self> {
        let schema = create_schema(&input_schema, &window_expr)?;
        let schema = Arc::new(schema);
        Ok(Self {
            input,
            window_expr,
            schema,
            input_schema,
            partition_keys,
            sort_keys,
            metrics: ExecutionPlanMetricsSet::new(),
            window_sort_keys,
        })
    }

    /// Window expressions
    pub fn window_expr(&self) -> &[Arc<dyn WindowExpr>] {
        &self.window_expr
    }

    /// Input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Get the input schema before any window functions are applied
    pub fn input_schema(&self) -> SchemaRef {
        self.input_schema.clone()
    }
}

impl ExecutionPlan for StreamWindowAggExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        // because we can have repartitioning using the partition keys
        // this would be either 1 or more than 1 depending on the presense of
        // repartitioning
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn maintains_input_order(&self) -> bool {
        true
    }

    fn relies_on_input_order(&self) -> bool {
        true
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(StreamWindowAggExec::try_new(
            self.window_expr.clone(),
            children[0].clone(),
            self.input_schema.clone(),
            self.partition_keys.clone(),
            self.sort_keys.clone(),
            self.window_sort_keys.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let stream = Box::pin(WindowAggStream::new(
            self.schema.clone(),
            self.window_expr.clone(),
            self.window_sort_keys.clone(),
            input,
            BaselineMetrics::new(&self.metrics, partition),
        ));
        Ok(stream)
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "StreamWindowAggExec: ")?;
                let g: Vec<String> = self
                    .window_expr
                    .iter()
                    .map(|e| format!("{}: {:?}", e.name().to_owned(), e.field()))
                    .collect();
                write!(f, "wdw=[{}]", g.join(", "))?;
            }
        }
        Ok(())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        let input_stat = self.input.statistics();
        let win_cols = self.window_expr.len();
        let input_cols = self.input_schema.fields().len();
        // TODO stats: some windowing function will maintain invariants such as min, max...
        let mut column_statistics = vec![ColumnStatistics::default(); win_cols];
        if let Some(input_col_stats) = input_stat.column_statistics {
            column_statistics.extend(input_col_stats);
        } else {
            column_statistics.extend(vec![ColumnStatistics::default(); input_cols]);
        }
        Statistics {
            is_exact: input_stat.is_exact,
            num_rows: input_stat.num_rows,
            column_statistics: Some(column_statistics),
            // TODO stats: knowing the type of the new columns we can guess the output size
            total_byte_size: None,
        }
    }
}

fn create_schema(
    input_schema: &Schema,
    window_expr: &[Arc<dyn WindowExpr>],
) -> Result<Schema> {
    let mut fields = Vec::with_capacity(input_schema.fields().len() + window_expr.len());
    for expr in window_expr {
        fields.push(expr.field()?);
    }
    fields.extend_from_slice(input_schema.fields());
    Ok(Schema::new(fields))
}

/// Compute the window aggregate columns
fn compute_window_aggregates(
    window_expr: &[Arc<dyn WindowExpr>],
    window_sort_keys: &WindowSortKeys,
    state: &mut [WindowAggState],
    batch: &Option<RecordBatch>,
) -> Result<Vec<Vec<WindowAccumulatorResult>>> {
    window_expr
        .iter()
        .enumerate()
        .map(|(idx, window_expr)| {
            let mut accumulator_state = state[idx].accumulator_state.clone();
            let res = window_expr.evaluate_stream(
                batch,
                &mut accumulator_state,
                window_sort_keys,
            );
            state[idx].accumulator_state = accumulator_state;
            res
        })
        .collect()
}

#[derive(Debug, Clone)]
pub struct WindowAggState {
    accumulator_results: Vec<WindowAccumulatorResult>,
    accumulator_state: HashMap<Vec<ScalarValue>, AggregateWindowAccumulatorState>,
}

/// stream for window aggregation plan
pub struct WindowAggStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    batch: Option<RecordBatch>,
    batch_state: HashMap<Vec<ScalarValue>, RecordBatch>,
    state: Vec<WindowAggState>,
    finished: bool,
    window_expr: Vec<Arc<dyn WindowExpr>>,
    window_sort_keys: WindowSortKeys,
    baseline_metrics: BaselineMetrics,
}

impl WindowAggStream {
    /// Create a new WindowAggStream
    pub fn new(
        schema: SchemaRef,
        window_expr: Vec<Arc<dyn WindowExpr>>,
        window_sort_keys: WindowSortKeys,
        input: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        let mut state = vec![];
        for i in 0..window_expr.len() {
            let cur_expr_state = WindowAggState {
                accumulator_results: vec![],
                accumulator_state: HashMap::new(),
            };
            state.push(cur_expr_state);
        }
        Self {
            schema,
            input,
            batch: None,
            batch_state: HashMap::new(),
            state,
            finished: false,
            window_expr,
            window_sort_keys,
            baseline_metrics,
        }
    }

    fn calc_len_to_show(&self) -> usize {
        // different window aggregators may produce with different rates
        // produce the overall result with the same speed as slowest one
        self.state
            .iter()
            .map(|window_agg_state| {
                let mut min_len = 0;
                let WindowAggState {
                    accumulator_results,
                    ..
                } = window_agg_state;
                for accumulator_result in accumulator_results {
                    let WindowAccumulatorResult {
                        col,
                        n_partition_range,
                        ..
                    } = accumulator_result;
                    let cur_len = if let Some(col) = col { col.len() } else { 0 };
                    min_len += cur_len;
                    if cur_len < *n_partition_range {
                        break;
                    } else {
                        assert_eq!(cur_len, *n_partition_range);
                    }
                }
                min_len
            })
            .into_iter()
            .min()
            .unwrap_or(0)
    }

    fn concat_state_with_new_results(
        state: &mut Vec<WindowAggState>,
        window_agg_results: &Vec<Vec<WindowAccumulatorResult>>,
    ) {
        assert_eq!(window_agg_results.len(), state.len());
        for (
            i,
            WindowAggState {
                accumulator_results,
                ..
            },
        ) in state.iter_mut().enumerate()
        {
            let window_agg_result = &window_agg_results[i];
            let partition_rows: Vec<Vec<ScalarValue>> = window_agg_result
                .iter()
                .map(|WindowAccumulatorResult { partition_id, .. }| partition_id.clone())
                .collect();
            let partition_rows_state: Vec<Vec<ScalarValue>> = accumulator_results
                .iter()
                .map(|WindowAccumulatorResult { partition_id, .. }| partition_id.clone())
                .collect();
            let mut res: Vec<WindowAccumulatorResult> = accumulator_results
                .iter()
                .enumerate()
                .map(
                    |(
                        j,
                        WindowAccumulatorResult {
                            partition_id: partition_row_state,
                            col: col_state,
                            n_partition_range: n_partition_range_state,
                        },
                    )| {
                        // we assume that one of the newly calculated partition row would be equal
                        let (calc_col, n_partition_range) = if let Some(idx) =
                            partition_rows
                                .iter()
                                .position(|elem| elem == partition_row_state)
                        {
                            let WindowAccumulatorResult {
                                partition_id: _part,
                                col,
                                n_partition_range,
                            } = window_agg_result[idx].clone();
                            (col, n_partition_range)
                        } else {
                            (None, 0)
                        };
                        // let state_range_length = range_state.1 - range_state.0;
                        // let cur_range_length = cur_range.1 - cur_range.0;
                        let new_n_partition_range =
                            n_partition_range_state + n_partition_range;
                        let res = match (col_state, calc_col) {
                            (None, None) => None,
                            (None, Some(part)) => Some(part.clone()),
                            (Some(elem), None) => Some(elem.clone()),
                            (Some(elem), Some(part)) => {
                                Some(arrow::compute::concat(&[elem, &part]).unwrap())
                            }
                        };
                        WindowAccumulatorResult {
                            partition_id: partition_row_state.clone(),
                            col: res,
                            n_partition_range: new_n_partition_range,
                        }
                    },
                )
                .collect();
            let mut a: Vec<WindowAccumulatorResult> = window_agg_result
                .iter()
                .filter(|WindowAccumulatorResult { partition_id, .. }| {
                    !partition_rows_state.contains(partition_id)
                })
                .map(|elem| elem.clone())
                .collect();
            res.append(&mut a);
            *accumulator_results = res;
        }
    }

    fn calc_columns_to_show(&self, len_to_show: usize) -> Vec<ArrayRef> {
        let columns_to_show: Vec<ArrayRef> = self
            .state
            .iter()
            .enumerate()
            .map(
                |(
                    i,
                    WindowAggState {
                        accumulator_state,
                        accumulator_results,
                    },
                )| {
                    let mut ret = None;
                    let mut running_length = 0;
                    for accumulator_result in accumulator_results {
                        // let (col_state, range_state, _) = &window_agg_state[j];
                        let WindowAccumulatorResult { col: col_state, .. } =
                            accumulator_result;
                        if running_length < len_to_show {
                            ret = match (ret, col_state) {
                                (None, None) => None,
                                (Some(ret), Some(col_state)) => {
                                    let n_to_use = min(
                                        len_to_show - running_length,
                                        col_state.len(),
                                    );
                                    running_length += n_to_use;
                                    Some(
                                        concat(&[&ret, &col_state.slice(0, n_to_use)])
                                            .unwrap(),
                                    )
                                }
                                (None, Some(col_state)) => {
                                    let n_to_use = min(
                                        len_to_show - running_length,
                                        col_state.len(),
                                    );
                                    running_length += n_to_use;
                                    Some(col_state.slice(0, n_to_use))
                                }
                                (Some(res), None) => Some(res),
                            }
                        }
                        if running_length == len_to_show {
                            // println!("return chunk with length");
                            return ret.unwrap();
                        }
                        assert!(running_length < len_to_show);
                    }
                    panic!("Err it should early return")
                },
            )
            .collect();
        columns_to_show
    }

    fn retract_showed_elements_from_state(
        state: &mut Vec<WindowAggState>,
        len_showed: usize,
    ) {
        for WindowAggState {
            accumulator_results,
            ..
        } in state.iter_mut()
        {
            let mut ret = vec![];
            let mut running_length = 0;
            for accumulator_result in accumulator_results.iter() {
                let WindowAccumulatorResult {
                    partition_id,
                    col,
                    n_partition_range,
                } = accumulator_result;
                if running_length >= len_showed {
                    ret.push(WindowAccumulatorResult {
                        partition_id: partition_id.clone(),
                        col: col.clone(),
                        n_partition_range: *n_partition_range,
                    });
                } else {
                    match col {
                        Some(col) => {
                            if col.len() + running_length >= len_showed {
                                let n_to_keep = col.len() + running_length - len_showed;
                                let slice_start = col.len() - n_to_keep;
                                // keep last n_to_keep values
                                let new_n_partition_range =
                                    n_partition_range + running_length - len_showed;
                                ret.push(WindowAccumulatorResult {
                                    partition_id: partition_id.clone(),
                                    col: Some(col.slice(slice_start, n_to_keep)),
                                    n_partition_range: new_n_partition_range,
                                });
                            } else {
                                // ret.push((None, (0, 0), partition_row));
                            }
                            running_length += col.len();
                        }
                        None => {
                            println!("Entered None in retract");
                            panic!("Err shouldn't have None in state");
                        }
                    }
                }
            }
            *accumulator_results = ret;
        }
    }

    fn compute_aggregates(
        &mut self,
        batch_inc: &Option<RecordBatch>,
    ) -> ArrowResult<RecordBatch> {
        // record compute time on drop
        let _timer = self.baseline_metrics.elapsed_compute().timer();

        if let Some(batch) = &self.batch {

            // calculate window cols
            let window_agg_results = compute_window_aggregates(
                &self.window_expr,
                &self.window_sort_keys.clone(),
                &mut self.state,
                batch_inc,
                // self.batch
            )
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
            Self::concat_state_with_new_results(&mut self.state, &window_agg_results);
            let len_to_show = self.calc_len_to_show();

            let len_batch = batch.num_rows();
            let n_to_keep = len_batch - len_to_show;

            if len_to_show > 0 {
                let batch_to_show = batch
                    .columns()
                    .iter()
                    .map(|elem| elem.slice(0, len_to_show))
                    .collect::<Vec<_>>();

                let mut columns_to_show = self.calc_columns_to_show(len_to_show);
                let batch_to_keep = batch
                    .columns()
                    .iter()
                    .map(|elem| elem.slice(len_to_show, n_to_keep))
                    .collect::<Vec<_>>();
                self.batch = Some(RecordBatch::try_new(batch.schema(), batch_to_keep)?);
                Self::retract_showed_elements_from_state(&mut self.state, len_to_show);

                columns_to_show.extend_from_slice(&batch_to_show);

                RecordBatch::try_new(self.schema.clone(), columns_to_show)
            } else {
                Ok(RecordBatch::new_empty(self.schema.clone()))
            }
        } else {
            Ok(RecordBatch::new_empty(self.schema.clone()))
        }
    }
}

impl Stream for WindowAggStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);
        self.baseline_metrics.record_poll(poll)
    }
}

impl WindowAggStream {
    #[inline]
    fn poll_next_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<ArrowResult<RecordBatch>>> {
        if self.finished {
            return Poll::Ready(None);
        }

        let result = match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                let window_expr = if let Some(window_expr) = self.window_expr.first(){
                    window_expr
                }else{
                    panic!("empty window expr");
                };
                let partition_columns = window_expr.partition_columns(&batch)?;
                let num_rows = batch.num_rows();
                let partition_points =
                    window_expr.evaluate_partition_points(num_rows, &partition_columns)?;
                println!("partition points:{:?}", partition_points);
                for partition_range in partition_points{
                    let partition_row = partition_columns
                        .iter()
                        .map(|arr| {
                            ScalarValue::try_from_array(&arr.values, partition_range.start)
                        })
                        .collect::<Result<Vec<ScalarValue>>>()?;
                    let batch_chunk = batch.slice(partition_range.start, partition_range.end-partition_range.start);
                    if let Some(state) = self.batch_state.get_mut(&partition_row) {
                        let res = common::append_new_batch(state, &batch_chunk, self.input.schema())?;
                        self.batch_state.insert(partition_row.clone(), res);
                    } else {
                        self.batch_state.insert(partition_row.clone(), batch_chunk);
                    };
                }
                println!("batch state: {:?}", self.batch_state);

                self.batch = Some(if let Some(state_batch) = &self.batch {
                    common::append_new_batch(state_batch, &batch, self.input.schema())?
                } else {
                    batch.clone()
                });
                self.compute_aggregates(&Some(batch))
            }
            Some(Err(e)) => Err(e),
            None => {
                self.finished = true;
                self.compute_aggregates(&None)
            }
        };
        Poll::Ready(Some(result))
    }
}

impl RecordBatchStream for WindowAggStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
