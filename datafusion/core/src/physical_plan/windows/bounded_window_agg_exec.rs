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
    ColumnStatistics, DisplayFormatType, Distribution, ExecutionPlan, Partitioning,
    RecordBatchStream, SendableRecordBatchStream, Statistics, WindowExpr,
};
use arrow::array::Array;
use arrow::compute::{concat, lexicographical_partition_ranges, SortColumn};
use arrow::{
    array::ArrayRef,
    datatypes::{Schema, SchemaRef},
    error::Result as ArrowResult,
    record_batch::RecordBatch,
};
use datafusion_common::{DataFusionError, ScalarValue};
use futures::stream::Stream;
use futures::{ready, StreamExt};
use std::any::Any;
use std::cmp::min;
use std::collections::HashMap;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::physical_plan::common::combine_batches_with_ref;
use datafusion_physical_expr::window::{
    PartitionBatchState, PartitionBatches, PartitionKey, PartitionWindowAggStates,
    WindowAggState, WindowState,
};
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExpr};
use indexmap::IndexMap;
use log::debug;

/// Window execution plan
#[derive(Debug)]
pub struct BoundedWindowAggExec {
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
}

impl BoundedWindowAggExec {
    /// Create a new execution plan for window aggregates
    pub fn try_new(
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: Arc<dyn ExecutionPlan>,
        input_schema: SchemaRef,
        partition_keys: Vec<Arc<dyn PhysicalExpr>>,
        sort_keys: Option<Vec<PhysicalSortExpr>>,
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

    /// Return the output sort order of partition keys: For example
    /// OVER(PARTITION BY a, ORDER BY b) -> would give sorting of the column a
    // We are sure that partition by columns are always at the beginning of sort_keys
    // Hence returned `PhysicalSortExpr` corresponding to `PARTITION BY` columns can be used safely
    // to calculate partition separation points
    pub fn partition_by_sort_keys(&self) -> Result<Vec<PhysicalSortExpr>> {
        let mut result = vec![];
        // All window exprs have the same partition by, so we just use the first one:
        let partition_by = self.window_expr()[0].partition_by();
        let sort_keys = self.sort_keys.as_deref().unwrap_or(&[]);
        for item in partition_by {
            if let Some(a) = sort_keys.iter().find(|&e| e.expr.eq(item)) {
                result.push(a.clone());
            } else {
                return Err(DataFusionError::Execution(
                    "Partition key not found in sort keys".to_string(),
                ));
            }
        }
        Ok(result)
    }
}

impl ExecutionPlan for BoundedWindowAggExec {
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
        // This executor maintains input order, and has required input_ordering filled
        // hence output_ordering would be `required_input_ordering`
        self.required_input_ordering()[0]
    }

    fn required_input_ordering(&self) -> Vec<Option<&[PhysicalSortExpr]>> {
        let sort_keys = self.sort_keys.as_deref();
        vec![sort_keys]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        if self.partition_keys.is_empty() {
            debug!("No partition defined for WindowAggExec!!!");
            vec![Distribution::SinglePartition]
        } else {
            //TODO support PartitionCollections if there is no common partition columns in the window_expr
            vec![Distribution::HashPartitioned(self.partition_keys.clone())]
        }
    }

    fn equivalence_properties(&self) -> EquivalenceProperties {
        self.input.equivalence_properties()
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
        Ok(Arc::new(BoundedWindowAggExec::try_new(
            self.window_expr.clone(),
            children[0].clone(),
            self.input_schema.clone(),
            self.partition_keys.clone(),
            self.sort_keys.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let stream = Box::pin(SortedPartitionByBoundedWindowStream::new(
            self.schema.clone(),
            self.window_expr.clone(),
            input,
            BaselineMetrics::new(&self.metrics, partition),
            self.partition_by_sort_keys()?,
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
                write!(f, "BoundedWindowAggExec: ")?;
                let g: Vec<String> = self
                    .window_expr
                    .iter()
                    .map(|e| {
                        format!(
                            "{}: {:?}, frame: {:?}",
                            e.name().to_owned(),
                            e.field(),
                            e.get_window_frame()
                        )
                    })
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

/// Trait for updating state, calculate results for window functions
/// According to partition by column assumptions Sorted/Unsorted we may have different
/// implementations for these fields
pub trait PartitionByHandler {
    /// Method to construct output columns from window_expression results
    fn calculate_out_columns(&self) -> Result<Option<Vec<ArrayRef>>>;
    /// Given how many rows we emitted as results
    /// prune no longer needed sections from the state
    fn prune_state(&mut self, n_out: usize) -> Result<()>;
    /// method to update record batches for each partition
    /// when new record batches are received
    fn update_partition_batch(&mut self, record_batch: RecordBatch) -> Result<()>;
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

/// stream for window aggregation plan
/// assuming partition by column is sorted (or without PARTITION BY expression)
pub struct SortedPartitionByBoundedWindowStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    /// The record_batch executor receives as input (columns needed during aggregate results calculation)
    input_buffer_record_batch: RecordBatch,
    /// we separate `input_buffer_record_batch` according to different partitions (determined by PARTITION BY columns)
    /// and store the result record_batches per partition base in the `partition_batches`.
    /// This variable is used during result calculation for each window_expression
    /// This enables us to use same batch for different window_expressions (without copying)
    // We may have keep record_batches for each window expression in the `PartitionWindowAggStates`
    // However, this would use more memory (on the order of window_expression number)
    partition_batches: PartitionBatches,
    /// Each executor can run multiple window expressions given
    /// their PARTITION BY and ORDER BY sections are same
    /// We keep state of the each window expression inside `window_agg_states`
    window_agg_states: Vec<PartitionWindowAggStates>,
    finished: bool,
    window_expr: Vec<Arc<dyn WindowExpr>>,
    baseline_metrics: BaselineMetrics,
    partition_by_sort_keys: Vec<PhysicalSortExpr>,
}

impl PartitionByHandler for SortedPartitionByBoundedWindowStream {
    /// This method constructs output columns using the result of each window expression
    fn calculate_out_columns(&self) -> Result<Option<Vec<ArrayRef>>> {
        let n_out = self.calculate_n_out_row();
        if n_out > 0 {
            let mut out_columns = vec![];
            for partition_window_agg_states in self.window_agg_states.iter() {
                out_columns.push(get_aggregate_result_out_column(
                    partition_window_agg_states,
                    n_out,
                )?);
            }

            let batch_to_show = self
                .input_buffer_record_batch
                .columns()
                .iter()
                .map(|elem| elem.slice(0, n_out))
                .collect::<Vec<_>>();
            out_columns.extend_from_slice(&batch_to_show);

            Ok(Some(out_columns))
        } else {
            Ok(None)
        }
    }

    /// Prunes sections in the state that are no longer needed
    /// for calculating result. Determined by window frame boundaries
    // For instance if `n_out` number of rows are calculated, we can remove first `n_out` rows from
    // the `self.input_buffer_record_batch`
    fn prune_state(&mut self, n_out: usize) -> Result<()> {
        // Prunes `self.partition_batches`
        self.prune_partition_batches()?;
        // Prunes `self.input_buffer_record_batch`
        self.prune_input_batch(n_out)?;
        // Prunes `self.window_agg_states`
        self.prune_out_columns(n_out)?;
        Ok(())
    }

    fn update_partition_batch(&mut self, record_batch: RecordBatch) -> Result<()> {
        let partition_columns = self.partition_columns(&record_batch)?;
        let num_rows = record_batch.num_rows();
        if num_rows > 0 {
            let partition_points =
                self.evaluate_partition_points(num_rows, &partition_columns)?;
            for partition_range in partition_points {
                let partition_row = partition_columns
                    .iter()
                    .map(|arr| {
                        ScalarValue::try_from_array(&arr.values, partition_range.start)
                    })
                    .collect::<Result<PartitionKey>>()?;
                let partition_batch = record_batch.slice(
                    partition_range.start,
                    partition_range.end - partition_range.start,
                );
                if let Some(partition_batch_state) =
                    self.partition_batches.get_mut(&partition_row)
                {
                    let combined_partition_batch = combine_batches_with_ref(
                        &[&partition_batch_state.record_batch, &partition_batch],
                        self.input.schema(),
                    )?
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "Should contain at least one entry".to_string(),
                        )
                    })?;
                    partition_batch_state.record_batch = combined_partition_batch;
                } else {
                    let partition_batch_state = PartitionBatchState {
                        record_batch: partition_batch,
                        is_end: false,
                    };
                    self.partition_batches
                        .insert(partition_row, partition_batch_state);
                };
            }
        }
        let n_partitions = self.partition_batches.len();
        for (idx, (_, partition_batch_state)) in
            self.partition_batches.iter_mut().enumerate()
        {
            if idx < n_partitions - 1 {
                partition_batch_state.is_end = true;
            }
        }
        if self.input_buffer_record_batch.num_rows() == 0 {
            self.input_buffer_record_batch = record_batch;
        } else {
            self.input_buffer_record_batch = combine_batches_with_ref(
                &[&self.input_buffer_record_batch, &record_batch],
                self.input.schema(),
            )?
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "Should contain at least one entry".to_string(),
                )
            })?;
        }

        Ok(())
    }
}

impl Stream for SortedPartitionByBoundedWindowStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);
        self.baseline_metrics.record_poll(poll)
    }
}

impl SortedPartitionByBoundedWindowStream {
    /// Create a new WindowAggStream
    pub fn new(
        schema: SchemaRef,
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
        partition_by_sort_keys: Vec<PhysicalSortExpr>,
    ) -> Self {
        let mut state = vec![];
        for _i in 0..window_expr.len() {
            state.push(IndexMap::new());
        }
        let empty_batch = RecordBatch::new_empty(schema.clone());
        Self {
            schema,
            input,
            input_buffer_record_batch: empty_batch,
            partition_batches: IndexMap::new(),
            window_agg_states: state,
            finished: false,
            window_expr,
            baseline_metrics,
            partition_by_sort_keys,
        }
    }

    fn compute_aggregates(&mut self) -> ArrowResult<RecordBatch> {
        // calculate window cols
        for (idx, cur_window_expr) in self.window_expr.iter().enumerate() {
            cur_window_expr.evaluate_stateful(
                &self.partition_batches,
                &mut self.window_agg_states[idx],
            )?;
        }

        let columns_to_show = self.calculate_out_columns()?;
        if let Some(columns_to_show) = columns_to_show {
            let n_generated = columns_to_show[0].len();
            self.prune_state(n_generated)?;
            RecordBatch::try_new(self.schema.clone(), columns_to_show)
        } else {
            Ok(RecordBatch::new_empty(self.schema.clone()))
        }
    }

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
                self.update_partition_batch(batch)?;
                self.compute_aggregates()
            }
            Some(Err(e)) => Err(e),
            None => {
                self.finished = true;
                for (_, partition_batch_state) in self.partition_batches.iter_mut() {
                    partition_batch_state.is_end = true;
                }
                self.compute_aggregates()
            }
        };
        Poll::Ready(Some(result))
    }

    /// Method to calculate how many rows SortedPartitionByBoundedWindowStream can produce as output
    fn calculate_n_out_row(&self) -> usize {
        // different window aggregators may produce results with different rates
        // we produce the overall batch result with the same speed as slowest one
        self.window_agg_states
            .iter()
            .map(|window_agg_state| {
                // below variable stores how many elements are generated (can displayed) for current
                // window expression
                let mut cur_window_expr_out_result_len = 0;
                // We iterate over window_agg_state
                // since it is IndexMap, iteration is over insertion order
                // Hence we preserve sorting when partition columns are sorted
                for (_, WindowState { state, .. }) in window_agg_state.iter() {
                    cur_window_expr_out_result_len += state.out_col.len();
                    // If we do not generate all results for current partition
                    // we do not generate result for next partition
                    // otherwise we will lose input ordering
                    if state.n_row_result_missing > 0 {
                        break;
                    }
                }
                cur_window_expr_out_result_len
            })
            .into_iter()
            .min()
            .unwrap_or(0)
    }

    /// prunes the sections of the record batch (for each partition)
    /// we no longer need to calculate window function result
    fn prune_partition_batches(&mut self) -> Result<()> {
        // Remove partitions which we know that ended (is_end flag is true)
        // Retain method keep the remaining elements in the insertion order
        // Hence after removal we still preserve ordering in between partitions
        self.partition_batches
            .retain(|_, partition_batch_state| !partition_batch_state.is_end);

        // `self.partition_batches` data are used by all window expressions
        // hence when removing from `self.partition_batches` we need to remove from the earliest range boundary
        // among all window expressions. `n_prune_each_partition` fill the earliest range boundary information
        // for each partition. By this way we can delete no longer needed sections from the `self.partition_batches`.
        // For instance if window frame one uses [10, 20] and window frame 2 uses [5, 15]
        // We prune only first 5 elements from corresponding record batch in `self.partition_batches`
        // Calculate how many element to prune for each partition_batch
        let mut n_prune_each_partition: HashMap<PartitionKey, usize> = HashMap::new();
        for window_agg_state in self.window_agg_states.iter_mut() {
            window_agg_state.retain(|_, WindowState { state, .. }| !state.is_end);
            for (partition_row, WindowState { state: value, .. }) in window_agg_state {
                if let Some(state) = n_prune_each_partition.get_mut(partition_row) {
                    if value.window_frame_range.start < *state {
                        *state = value.window_frame_range.start;
                    }
                } else {
                    n_prune_each_partition
                        .insert(partition_row.clone(), value.window_frame_range.start);
                }
            }
        }

        let err = || DataFusionError::Execution("Expects to have partition".to_string());
        // Retracts no longer needed parts during window calculations from partition batch
        for (partition_row, n_prune) in n_prune_each_partition.iter() {
            let partition_batch_state = self
                .partition_batches
                .get_mut(partition_row)
                .ok_or_else(err)?;
            let new_record_batch = partition_batch_state.record_batch.slice(
                *n_prune,
                partition_batch_state.record_batch.num_rows() - n_prune,
            );
            partition_batch_state.record_batch = new_record_batch;

            // Update State indices, since we have pruned some rows from the beginning
            for window_agg_state in self.window_agg_states.iter_mut() {
                let window_state =
                    window_agg_state.get_mut(partition_row).ok_or_else(err)?;
                let mut state = &mut window_state.state;
                state.window_frame_range = Range {
                    start: state.window_frame_range.start - n_prune,
                    end: state.window_frame_range.end - n_prune,
                };
                state.last_calculated_index -= n_prune;
                state.offset_pruned_rows += n_prune;
            }
        }
        Ok(())
    }

    /// Prunes the section of the input batch whose aggregate results
    /// are calculated and emitted as result
    fn prune_input_batch(&mut self, n_out: usize) -> Result<()> {
        let len_batch = self.input_buffer_record_batch.num_rows();
        let n_to_keep = len_batch - n_out;
        let batch_to_keep = self
            .input_buffer_record_batch
            .columns()
            .iter()
            .map(|elem| elem.slice(n_out, n_to_keep))
            .collect::<Vec<_>>();
        self.input_buffer_record_batch =
            RecordBatch::try_new(self.input_buffer_record_batch.schema(), batch_to_keep)?;
        Ok(())
    }

    /// Prunes emitted parts from WindowAggState `out_col` field
    fn prune_out_columns(&mut self, n_out: usize) -> Result<()> {
        // We store generated columns for each window expression in `out_col` field of the `WindowAggState`
        // Given how many rows are emitted to output we remove these sections from state
        for partition_window_agg_states in self.window_agg_states.iter_mut() {
            let mut running_length = 0;
            // Remove total of `n_out` entries from `out_col` field of `WindowAggState`. Iterates in the
            // insertion order. Hence we preserve per partition ordering. Without emitting all results for a partition
            // we do not generate result for another partition
            for (
                _,
                WindowState {
                    state: WindowAggState { out_col, .. },
                    ..
                },
            ) in partition_window_agg_states
            {
                if running_length < n_out {
                    let n_to_del = min(out_col.len(), n_out - running_length);
                    let n_to_keep = out_col.len() - n_to_del;
                    *out_col = out_col.slice(n_to_del, n_to_keep);
                    running_length += n_to_del;
                }
            }
        }
        Ok(())
    }

    /// Get Partition Columns
    pub fn partition_columns(&self, batch: &RecordBatch) -> Result<Vec<SortColumn>> {
        self.partition_by_sort_keys
            .iter()
            .map(|elem| elem.evaluate_to_sort_column(batch))
            .collect::<Result<Vec<_>>>()
    }

    /// evaluate the partition points given the sort columns; if the sort columns are
    /// empty then the result will be a single element vec of the whole column rows.
    fn evaluate_partition_points(
        &self,
        num_rows: usize,
        partition_columns: &[SortColumn],
    ) -> Result<Vec<Range<usize>>> {
        if partition_columns.is_empty() {
            Ok(vec![Range {
                start: 0,
                end: num_rows,
            }])
        } else {
            Ok(lexicographical_partition_ranges(partition_columns)
                .map_err(DataFusionError::ArrowError)?
                .collect::<Vec<_>>())
        }
    }
}

impl RecordBatchStream for SortedPartitionByBoundedWindowStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Calculates the section we can show results for expression
fn get_aggregate_result_out_column(
    partition_window_agg_states: &PartitionWindowAggStates,
    len_to_show: usize,
) -> Result<ArrayRef> {
    let mut ret = None;
    let mut running_length = 0;
    // We assume that iteration order is according to insertion order
    for (
        _,
        WindowState {
            state: WindowAggState { out_col, .. },
            ..
        },
    ) in partition_window_agg_states
    {
        if running_length < len_to_show {
            let n_to_use = min(len_to_show - running_length, out_col.len());
            running_length += n_to_use;
            let slice_to_use = out_col.slice(0, n_to_use);
            ret = match ret {
                Some(ret) => Some(concat(&[&ret, &slice_to_use])?),
                None => Some(slice_to_use),
            }
        } else {
            break;
        }
    }
    if running_length != len_to_show {
        return Err(DataFusionError::Execution(format!(
            "Generated row number should be {}, it is {}",
            len_to_show, running_length
        )));
    }
    ret.ok_or_else(|| DataFusionError::Execution("Should contain something".to_string()))
}
