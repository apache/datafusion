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
    common, ColumnStatistics, DisplayFormatType, ExecutionPlan, Partitioning,
    RecordBatchStream, SendableRecordBatchStream, Statistics, WindowExpr,
};
use arrow::array::Array;
use arrow::compute::concat;
use arrow::{
    array::ArrayRef,
    datatypes::{Schema, SchemaRef},
    error::Result as ArrowResult,
    record_batch::RecordBatch,
};
use datafusion_common::{DataFusionError, ScalarValue};
use futures::stream::Stream;
use futures::{ready, StreamExt};
use indexmap::IndexMap;
use std::any::Any;
use std::cmp::min;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion_expr::utils::WindowSortKeys;
use datafusion_physical_expr::window::{
    BatchState, PartitionKey, WindowAggState, WindowState,
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
    batch_state: &BatchState,
    is_end: bool,
) -> Result<()> {
    for (idx, cur_window_expr) in window_expr.iter().enumerate() {
        cur_window_expr.evaluate_stream(
            batch_state,
            &mut state[idx],
            window_sort_keys,
            is_end,
        )?;
    }
    Ok(())
}

/// stream for window aggregation plan
pub struct WindowAggStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    batch: Option<RecordBatch>,
    batch_state: BatchState,
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
        for _i in 0..window_expr.len() {
            state.push(IndexMap::new());
        }
        Self {
            schema,
            input,
            batch: None,
            batch_state: IndexMap::new(),
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
                // TODO: Add sorting according to ts for iteration
                for (_, state) in window_agg_state.iter() {
                    let cur_len = if let Some(col) = &state.col {
                        col.len()
                    } else {
                        0
                    };
                    min_len += cur_len;
                    if cur_len < state.num_rows {
                        break;
                    }
                    assert!(cur_len <= state.num_rows);
                }
                min_len
            })
            .into_iter()
            .min()
            .unwrap_or(0)
    }

    fn calc_columns_to_show(&self, len_to_show: usize) -> Result<Vec<ArrayRef>> {
        let mut columns_to_show = vec![];
        for accumulator_state in self.state.iter() {
            let mut ret = None;
            let mut running_length = 0;
            for (_, WindowState { col: col_state, .. }) in accumulator_state {
                if running_length < len_to_show {
                    ret = match (ret, col_state) {
                        (None, None) => None,
                        (Some(ret), Some(col_state)) => {
                            let n_to_use =
                                min(len_to_show - running_length, col_state.len());
                            running_length += n_to_use;
                            Some(concat(&[&ret, &col_state.slice(0, n_to_use)])?)
                        }
                        (None, Some(col_state)) => {
                            let n_to_use =
                                min(len_to_show - running_length, col_state.len());
                            running_length += n_to_use;
                            Some(col_state.slice(0, n_to_use))
                        }
                        (Some(res), None) => Some(res),
                    }
                }
                if running_length == len_to_show {
                    columns_to_show.push(ret.ok_or_else(|| {
                        DataFusionError::Execution("Should contain something".to_string())
                    })?);
                    break;
                }
                assert!(running_length < len_to_show);
            }
        }
        Ok(columns_to_show)
    }

    /// retracts sections in the batch state that are no longer needed during
    /// window frame range calculations
    fn retract_state(state: &mut [WindowAggState], batch_state: &mut BatchState) {
        // Fill the earliest boundary for each partition range in the batch state
        // For instance in window frame one uses [10, 20] and window frame 2 uses [5, 15]
        // We retract only first 5.
        let mut retract_state: HashMap<PartitionKey, usize> = HashMap::new();
        for accumulator_state in state.iter_mut() {
            for (partition_row, value) in accumulator_state {
                if let Some(state) = retract_state.get_mut(partition_row) {
                    if value.last_range.0 < *state {
                        *state = value.last_range.0;
                    }
                } else {
                    retract_state.insert(partition_row.clone(), value.last_range.0);
                }
            }
        }

        for (partition_row, offset) in retract_state.iter() {
            if let Some(record_batch) = batch_state.get(partition_row) {
                let new_record_batch =
                    record_batch.slice(*offset, record_batch.num_rows() - offset);
                batch_state.insert(partition_row.clone(), new_record_batch);
                for accumulator_state in state.iter_mut() {
                    if let Some(state) = accumulator_state.get_mut(partition_row) {
                        state.last_range =
                            (state.last_range.0 - offset, state.last_range.1 - offset);
                        state.cur_range =
                            (state.cur_range.0 - offset, state.cur_range.1 - offset);
                        state.last_idx -= offset;
                        state.n_retracted += offset;
                    } else {
                        panic!("should have this partition");
                    }
                }
            } else {
                panic!("should have this partition");
            }
        }
    }

    fn retract_showed_elements_from_state(
        state: &mut [WindowAggState],
        len_showed: usize,
    ) -> Result<()> {
        for accumulator_state in state.iter_mut() {
            let mut running_length = 0;
            // TODO: make iteration order according to ts
            for (_, WindowState { col, num_rows, .. }) in accumulator_state {
                if running_length < len_showed {
                    match col {
                        Some(col) => {
                            if col.len() + running_length >= len_showed {
                                let n_to_keep = col.len() + running_length - len_showed;
                                let slice_start = col.len() - n_to_keep;
                                // keep last n_to_keep values
                                let new_num_rows =
                                    *num_rows + running_length - len_showed;

                                *num_rows = new_num_rows;
                                *col = col.slice(slice_start, n_to_keep);
                            }
                            running_length += col.len();
                        }
                        None => {
                            return Err(DataFusionError::Execution(
                                "Expects something to retract".to_string(),
                            ))
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn compute_aggregates(&mut self, is_end: bool) -> ArrowResult<RecordBatch> {
        // record compute time on drop
        let _timer = self.baseline_metrics.elapsed_compute().timer();

        if let Some(batch) = &self.batch {
            // calculate window cols
            compute_window_aggregates(
                &self.window_expr,
                &self.window_sort_keys.clone(),
                &mut self.state,
                &self.batch_state,
                is_end,
            )?;

            let len_to_show = self.calc_len_to_show();

            let len_batch = batch.num_rows();
            let n_to_keep = len_batch - len_to_show;
            if len_to_show > 0 {
                let batch_to_show = batch
                    .columns()
                    .iter()
                    .map(|elem| elem.slice(0, len_to_show))
                    .collect::<Vec<_>>();

                let mut columns_to_show = self.calc_columns_to_show(len_to_show)?;
                let batch_to_keep = batch
                    .columns()
                    .iter()
                    .map(|elem| elem.slice(len_to_show, n_to_keep))
                    .collect::<Vec<_>>();
                self.batch = Some(RecordBatch::try_new(batch.schema(), batch_to_keep)?);
                Self::retract_showed_elements_from_state(&mut self.state, len_to_show)?;
                Self::retract_state(&mut self.state, &mut self.batch_state);

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
                // all window expressions have same other than window frame boundaries hence we can use any one of the window expressions
                let window_expr = self.window_expr.first().ok_or_else(|| {
                    DataFusionError::Execution(
                        "window expr cannot be empty to support streaming".to_string(),
                    )
                })?;
                let partition_columns = window_expr.partition_columns(&batch)?;
                let num_rows = batch.num_rows();
                if num_rows > 0 {
                    let partition_points = window_expr
                        .evaluate_partition_points(num_rows, &partition_columns)?;
                    for partition_range in partition_points {
                        let partition_row = partition_columns
                            .iter()
                            .map(|arr| {
                                ScalarValue::try_from_array(
                                    &arr.values,
                                    partition_range.start,
                                )
                            })
                            .collect::<Result<PartitionKey>>()?;
                        let batch_chunk = batch.slice(
                            partition_range.start,
                            partition_range.end - partition_range.start,
                        );
                        if let Some(state) = self.batch_state.get(&partition_row) {
                            let res = common::append_new_batch(
                                state,
                                &batch_chunk,
                                self.input.schema(),
                            )?;
                            self.batch_state.insert(partition_row.clone(), res.clone());
                        } else {
                            self.batch_state.insert(partition_row.clone(), batch_chunk);
                        };
                    }
                }

                self.batch = Some(if let Some(state_batch) = &self.batch {
                    common::append_new_batch(state_batch, &batch, self.input.schema())?
                } else {
                    batch
                });
                self.compute_aggregates(false)
            }
            Some(Err(e)) => Err(e),
            None => {
                self.finished = true;
                self.compute_aggregates(true)
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
