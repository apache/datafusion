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
//! The executor given here uses bounded memory (does not maintain all
//! the input data seen so far), which makes it appropriate when processing
//! infinite inputs.

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
use arrow::util::pretty::print_batches;
use arrow::{
    array::ArrayRef,
    datatypes::{Schema, SchemaRef},
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

use crate::physical_plan::common::merge_batches;
use crate::physical_plan::windows::PartitionSearchMode;
use datafusion_common::utils::{bisect, get_row_at_idx};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::window::{
    PartitionBatchState, PartitionBatches, PartitionKey, PartitionWindowAggStates,
    WindowAggState, WindowState,
};
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExpr};
use indexmap::IndexMap;
use log::debug;

/// This trait defines the interface for updating the state and calculating
/// results for window functions. Depending on the partitioning scheme, one
/// may have different implementations for the functions within.
pub trait PartitionByHandler {
    /// Constructs output columns from window_expression results.
    fn calculate_out_columns(&mut self) -> Result<Option<Vec<ArrayRef>>>;
    /// Prunes the window state to remove any unnecessary information
    /// given how many rows we emitted so far.
    fn prune_state(&mut self, n_out: usize) -> Result<()>;
    /// Updates record batches for each partition when new batches are
    /// received.
    fn update_partition_batch(&mut self, record_batch: RecordBatch) -> Result<()>;
}

/// stream for window aggregation plan
/// assuming partition by column is sorted (or without PARTITION BY expression)
pub struct LinearPartitionByBoundedWindowStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    /// The record batch executor receives as input (i.e. the columns needed
    /// while calculating aggregation results).
    input_buffer: RecordBatch,
    /// We separate `input_buffer_record_batch` based on partitions (as
    /// determined by PARTITION BY columns) and store them per partition
    /// in `partition_batches`. We use this variable when calculating results
    /// for each window expression. This enables us to use the same batch for
    /// different window expressions without copying.
    // Note that we could keep record batches for each window expression in
    // `PartitionWindowAggStates`. However, this would use more memory (as
    // many times as the number of window expressions).
    partition_buffers: PartitionBatches,
    /// An executor can run multiple window expressions if the PARTITION BY
    /// and ORDER BY sections are same. We keep state of the each window
    /// expression inside `window_agg_states`.
    window_agg_states: Vec<PartitionWindowAggStates>,
    finished: bool,
    window_expr: Vec<Arc<dyn WindowExpr>>,
    partition_by_sort_keys: Vec<PhysicalSortExpr>,
    baseline_metrics: BaselineMetrics,
}

impl PartitionByHandler for LinearPartitionByBoundedWindowStream {
    /// This method constructs output columns using the result of each window expression
    fn calculate_out_columns(&mut self) -> Result<Option<Vec<ArrayRef>>> {
        let partition_by_columns =
            self.evaluate_partition_by_column_values(&self.input_buffer)?;
        let mut res_generated = vec![];
        for window_agg_state in &self.window_agg_states {
            let mut map = IndexMap::new();
            for (key, value) in window_agg_state {
                // println!("key:{:?}", key);
                // println!("value:{:?}", value.state.out_col.len());
                map.insert(key, value.state.out_col.len());
            }
            res_generated.push(map);
        }
        // for (key, elem) in &self.partition_buffers{
        //     println!("key: {:?}, elem:{:?}", key, elem.indices);
        //     println!("key: {:?}, len:{:?}", key, elem.indices.len());
        // }
        // println!("res_generated:{:?}", res_generated);
        let mut counter: IndexMap<Vec<ScalarValue>, Vec<usize>> = IndexMap::new();
        let n_window_col = self.window_agg_states.len();
        let mut rows_gen = vec![vec![]; n_window_col];
        for idx in 0..self.input_buffer.num_rows() {
            let row = get_row_at_idx(&partition_by_columns, idx)?;
            let counts = if let Some(res) = counter.get_mut(&row) {
                res
            } else {
                let mut res: Vec<usize> = vec![0; n_window_col];
                counter.insert(row.clone(), res);
                counter.get_mut(&row).unwrap()
            };
            let mut row_res = vec![];
            for (window_idx, window_agg_state) in
                self.window_agg_states.iter().enumerate()
            {
                // println!("row:{:?}", row);
                let partition = window_agg_state.get(&row).unwrap();
                if counts[window_idx] < partition.state.out_col.len() {
                    let res = ScalarValue::try_from_array(
                        &partition.state.out_col,
                        counts[window_idx],
                    )?;
                    row_res.push(res);
                    counts[window_idx] += 1;
                } else {
                    break;
                }
                // println!("partition.out_col: {:?}", partition.state.out_col);
            }
            if row_res.len() != n_window_col {
                break;
            }
            for (col_idx, elem) in row_res.into_iter().enumerate() {
                rows_gen[col_idx].push(elem)
            }
            // println!("row_res:{:?}", row_res);
            // println!("counts: {:?}", counts);
        }
        println!("counter:{:?}", counter);
        for (key, val) in counter.iter() {
            if let Some(partition_batch_state) = self.partition_buffers.get_mut(key) {
                partition_batch_state.n_out_row = *val.iter().min().unwrap();
                // println!("partition_batch_state.n_out_row: {:?}", partition_batch_state.n_out_row);
            }
        }
        // println!("rows_gen:{:?}", rows_gen);
        if rows_gen.is_empty() {
            Ok(None)
        } else {
            let n_out = rows_gen[0].len();
            self.input_buffer
                .columns()
                .iter()
                .map(|elem| Ok(elem.slice(0, n_out)))
                .chain(
                    rows_gen
                        .into_iter()
                        .map(|col| ScalarValue::iter_to_array(col)),
                )
                .collect::<Result<Vec<_>>>()
                .map(Some)
        }
    }

    /// Prunes sections of the state that are no longer needed when calculating
    /// results (as determined by window frame boundaries and number of results generated).
    // For instance, if first `n` (not necessarily same with `n_out`) elements are no longer needed to
    // calculate window expression result (outside the window frame boundary) we retract first `n` elements
    // from `self.partition_batches` in corresponding partition.
    // For instance, if `n_out` number of rows are calculated, we can remove
    // first `n_out` rows from `self.input_buffer_record_batch`.
    fn prune_state(&mut self, n_out: usize) -> Result<()> {
        // Prune `self.window_agg_states`:
        println!("before pruning out columns");
        self.print_partition_batches()?;
        self.prune_out_columns(n_out)?;
        println!("before pruning partition batches");
        self.print_partition_batches()?;
        // Prune `self.partition_batches`:
        self.prune_partition_batches()?;
        println!("before pruning input batchs");
        // Prune `self.input_buffer_record_batch`:
        self.prune_input_batch(n_out)?;
        self.print_partition_batches()?;
        Ok(())
    }

    fn update_partition_batch(&mut self, record_batch: RecordBatch) -> Result<()> {
        let num_rows = record_batch.num_rows();
        println!("num_rows:{:?}", num_rows);
        if num_rows > 0 {
            let partition_bys =
                self.evaluate_partition_by_column_values(&record_batch)?;
            let partition_points =
                self.evaluate_partition_points(num_rows, &partition_bys)?;
            for partition_range in partition_points {
                let indices =
                    (partition_range.start..partition_range.end).collect::<Vec<_>>();
                let partition_row =
                    get_row_at_idx(&partition_bys, partition_range.start)?;
                let partition_batch = record_batch.slice(
                    partition_range.start,
                    partition_range.end - partition_range.start,
                );
                if let Some(partition_batch_state) =
                    self.partition_buffers.get_mut(&partition_row)
                {
                    partition_batch_state.record_batch = merge_batches(
                        &partition_batch_state.record_batch,
                        &partition_batch,
                        self.input.schema(),
                    )?;
                    partition_batch_state.indices.extend(indices);
                } else {
                    let partition_batch_state = PartitionBatchState {
                        record_batch: partition_batch,
                        is_end: false,
                        indices,
                        n_out_row: 0,
                    };
                    self.partition_buffers
                        .insert(partition_row, partition_batch_state);
                };
            }
        }
        self.input_buffer = if self.input_buffer.num_rows() == 0 {
            record_batch
        } else {
            merge_batches(&self.input_buffer, &record_batch, self.input.schema())?
        };

        Ok(())
    }
}

impl Stream for LinearPartitionByBoundedWindowStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);
        self.baseline_metrics.record_poll(poll)
    }
}

impl LinearPartitionByBoundedWindowStream {
    /// Create a new BoundedWindowAggStream
    pub fn new(
        schema: SchemaRef,
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
        partition_by_sort_keys: Vec<PhysicalSortExpr>,
    ) -> Self {
        let state = window_expr.iter().map(|_| IndexMap::new()).collect();
        let empty_batch = RecordBatch::new_empty(schema.clone());
        Self {
            schema,
            input,
            input_buffer: empty_batch,
            partition_buffers: IndexMap::new(),
            window_agg_states: state,
            finished: false,
            window_expr,
            baseline_metrics,
            partition_by_sort_keys,
        }
    }

    fn print_partition_batches(&self) -> Result<()> {
        println!("n_partition:{:?}", self.partition_buffers.len());
        for (partition_key, partition_batch) in &self.partition_buffers {
            println!(
                "partition_key:{:?}, n_row: {:?}",
                partition_key,
                partition_batch.record_batch.num_rows()
            );
            println!(
                "n_out_row: {:?}, len indices: {:?}",
                partition_batch.n_out_row,
                partition_batch.indices.len()
            );
            // print_batches(&vec![partition_batch.record_batch.clone()])?;
        }
        Ok(())
    }

    fn compute_aggregates(&mut self) -> Result<RecordBatch> {
        // calculate window cols
        for (cur_window_expr, state) in
            self.window_expr.iter().zip(&mut self.window_agg_states)
        {
            cur_window_expr.evaluate_stateful(&self.partition_buffers, state)?;
        }

        let schema = self.schema.clone();
        // for elem in &self.window_agg_states{
        //     for (key, value) in elem.iter(){
        //         println!("{:?}", value);
        //     }
        // }
        // println!("self.partition_buffers.len(): {:?}", self.partition_buffers.len());
        // println!("self.partition_buffers.keys(): {:?}", self.partition_buffers.keys());
        // println!("self.window_agg_states[0].len(): {:?}", self.window_agg_states[0].len());
        // println!("self.window_agg_states[0].keys(): {:?}", self.window_agg_states[0].keys());
        let columns_to_show = self.calculate_out_columns()?;
        if let Some(columns_to_show) = columns_to_show {
            let n_generated = columns_to_show[0].len();
            self.prune_state(n_generated)?;
            Ok(RecordBatch::try_new(schema, columns_to_show)?)
        } else {
            Ok(RecordBatch::new_empty(schema))
        }
    }

    #[inline]
    fn poll_next_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
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
                for (partition_row, partition_batch_state) in
                    self.partition_buffers.iter_mut()
                {
                    println!("partition row is end set: {:?}", partition_row);
                    partition_batch_state.is_end = true;
                }
                self.compute_aggregates()
            }
        };
        Poll::Ready(Some(result))
    }

    /// Calculates how many rows [SortedPartitionByBoundedWindowStream]
    /// can produce as output.
    fn calculate_n_out_row(&self) -> usize {
        // Different window aggregators may produce results with different rates.
        // We produce the overall batch result with the same speed as slowest one.
        self.window_agg_states
            .iter()
            .map(|window_agg_state| {
                // Store how many elements are generated for the current
                // window expression:
                let mut cur_window_expr_out_result_len = 0;
                // We iterate over `window_agg_state`, which is an IndexMap.
                // Iterations follow the insertion order, hence we preserve
                // sorting when partition columns are sorted.
                for (_, WindowState { state, .. }) in window_agg_state.iter() {
                    cur_window_expr_out_result_len += state.out_col.len();
                    // If we do not generate all results for the current
                    // partition, we do not generate results for next
                    // partition --  otherwise we will lose input ordering.
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

    /// Prunes the sections of the record batch (for each partition)
    /// that we no longer need to calculate the window function result.
    fn prune_partition_batches(&mut self) -> Result<()> {
        // Remove partitions which we know already ended (is_end flag is true).
        // Since the retain method preserves insertion order, we still have
        // ordering in between partitions after removal.
        self.partition_buffers
            .retain(|_, partition_batch_state| !partition_batch_state.is_end);

        // The data in `self.partition_batches` is used by all window expressions.
        // Therefore, when removing from `self.partition_batches`, we need to remove
        // from the earliest range boundary among all window expressions. Variable
        // `n_prune_each_partition` fill the earliest range boundary information for
        // each partition. This way, we can delete the no-longer-needed sections from
        // `self.partition_batches`.
        // For instance, if window frame one uses [10, 20] and window frame two uses
        // [5, 15]; we only prune the first 5 elements from the corresponding record
        // batch in `self.partition_batches`.

        // Calculate how many elements to prune for each partition batch
        let mut n_prune_each_partition: HashMap<PartitionKey, usize> = HashMap::new();
        for window_agg_state in self.window_agg_states.iter_mut() {
            window_agg_state.retain(|_, WindowState { state, .. }| !state.is_end);
            for (partition_row, WindowState { state: value, .. }) in window_agg_state {
                let n_prune =
                    min(value.window_frame_range.start, value.last_calculated_index);
                if let Some(state) = n_prune_each_partition.get_mut(partition_row) {
                    if n_prune < *state {
                        *state = n_prune;
                    }
                } else {
                    n_prune_each_partition.insert(partition_row.clone(), n_prune);
                }
            }
        }
        println!("n_prune_each_partition:{:?}", n_prune_each_partition);

        let err = || DataFusionError::Execution("Expects to have partition".to_string());
        // Retract no longer needed parts during window calculations from partition batch:
        for (partition_row, n_prune) in n_prune_each_partition.iter() {
            let partition_batch_state = self
                .partition_buffers
                .get_mut(partition_row)
                .ok_or_else(err)?;
            // TODO: do not ues below value for each window state take minimum beginning
            // let n_prune = partition_batch_state.n_out_row;
            let batch = &partition_batch_state.record_batch;
            partition_batch_state.record_batch =
                batch.slice(*n_prune, batch.num_rows() - n_prune);

            partition_batch_state.indices.drain(0..*n_prune);
            println!(
                "partition_batch_state.n_out_row:{:?}, n_prune{:?}",
                partition_batch_state.n_out_row, n_prune
            );
            // partition_batch_state.n_out_row -= *n_prune;

            // Update state indices since we have pruned some rows from the beginning:
            for window_agg_state in self.window_agg_states.iter_mut() {
                let window_state =
                    window_agg_state.get_mut(partition_row).ok_or_else(err)?;
                let mut state = &mut window_state.state;
                println!(
                    "state.window_frame_range:{:?}, n_prune:{:?}",
                    state.window_frame_range, n_prune
                );
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
    /// are calculated and emitted.
    fn prune_input_batch(&mut self, n_out: usize) -> Result<()> {
        // TODO: ADD pruning for indices field in PartitionBatchState
        let n_to_keep = self.input_buffer.num_rows() - n_out;
        let batch_to_keep = self
            .input_buffer
            .columns()
            .iter()
            .map(|elem| elem.slice(n_out, n_to_keep))
            .collect::<Vec<_>>();
        self.input_buffer =
            RecordBatch::try_new(self.input_buffer.schema(), batch_to_keep)?;
        Ok(())
    }

    /// Prunes emitted parts from WindowAggState `out_col` field.
    fn prune_out_columns(&mut self, n_out: usize) -> Result<()> {
        // TODO: ADD pruning for indices field in PartitionBatchState
        // We store generated columns for each window expression in the `out_col`
        // field of `WindowAggState`. Given how many rows are emitted, we remove
        // these sections from state.
        for partition_window_agg_states in self.window_agg_states.iter_mut() {
            // Remove `n_out` entries from the `out_col` field of `WindowAggState`.
            // Preserve per partition ordering by iterating in the order of insertion.
            // Do not generate a result for a new partition without emitting all results
            // for the current partition.
            for (
                partition_key,
                WindowState {
                    state:
                        WindowAggState {
                            out_col,
                            offset_pruned_rows,
                            last_calculated_index,
                            ..
                        },
                    ..
                },
            ) in partition_window_agg_states
            {
                // TODO: we need to consider offset_pruned_rows in calculations also
                let partition_batch =
                    self.partition_buffers.get_mut(partition_key).unwrap();
                assert_eq!(
                    partition_batch.record_batch.num_rows(),
                    partition_batch.indices.len()
                );
                let n_to_del = partition_batch.n_out_row;
                let n_to_keep = out_col.len() - n_to_del;
                *out_col = out_col.slice(n_to_del, n_to_keep);
            }
        }
        Ok(())
    }

    /// Get Partition Columns
    pub fn partition_columns(&self, batch: &RecordBatch) -> Result<Vec<SortColumn>> {
        self.partition_by_sort_keys
            .iter()
            .map(|e| e.evaluate_to_sort_column(batch))
            .collect::<Result<Vec<_>>>()
    }

    /// evaluate the partition points given the sort columns; if the sort columns are
    /// empty then the result will be a single element vec of the whole column rows.
    fn evaluate_partition_points(
        &self,
        num_rows: usize,
        columns: &[ArrayRef],
    ) -> Result<Vec<Range<usize>>> {
        Ok(if columns.is_empty() {
            vec![Range {
                start: 0,
                end: num_rows,
            }]
        } else {
            // Do linear search
            let mut res = vec![];
            let mut last_row = get_row_at_idx(&columns, 0)?;
            let mut start = 0;
            for idx in 0..num_rows {
                let cur_row = get_row_at_idx(&columns, idx)?;
                if !last_row.eq(&cur_row) {
                    res.push(Range { start, end: idx });
                    start = idx;
                    last_row = cur_row;
                }
            }
            res.push(Range {
                start,
                end: num_rows,
            });
            res
        })
    }

    fn evaluate_partition_by_column_values(
        &self,
        record_batch: &RecordBatch,
    ) -> Result<Vec<ArrayRef>> {
        self.window_expr[0]
            .partition_by()
            .iter()
            .map(|elem| {
                let value_to_sort = elem.evaluate(record_batch)?;
                let array_to_sort = match value_to_sort {
                    ColumnarValue::Array(array) => Ok(array),
                    ColumnarValue::Scalar(scalar) => Err(DataFusionError::Plan(format!(
                        "Sort operation is not applicable to scalar value {scalar}"
                    ))),
                };
                // elem.evaluate(&record_batch).unwrap()
                array_to_sort
            })
            .collect::<Result<Vec<ArrayRef>>>()
    }
}

impl RecordBatchStream for LinearPartitionByBoundedWindowStream {
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
    let mut result = None;
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
            let slice_to_use = out_col.slice(0, n_to_use);
            result = Some(match result {
                Some(arr) => concat(&[&arr, &slice_to_use])?,
                None => slice_to_use,
            });
            running_length += n_to_use;
        } else {
            break;
        }
    }
    if running_length != len_to_show {
        return Err(DataFusionError::Execution(format!(
            "Generated row number should be {len_to_show}, it is {running_length}"
        )));
    }
    result
        .ok_or_else(|| DataFusionError::Execution("Should contain something".to_string()))
}
