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
use arrow::array::{Array, UInt32Builder, UInt64Builder};
use arrow::compute::{concat, lexicographical_partition_ranges, SortColumn};
use arrow::util::pretty::print_batches;
use arrow::{
    array::ArrayRef,
    compute,
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
use datafusion_common::utils::get_row_at_idx;
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::window::{
    PartitionBatchState, PartitionBatches, PartitionKey, PartitionWindowAggStates,
    WindowAggState, WindowState,
};
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExpr};
use indexmap::IndexMap;
use log::debug;

#[derive(Debug, Clone, PartialEq)]
pub enum PartitionSearchMode {
    Linear,
    PartiallySorted(Vec<PhysicalSortExpr>),
    Sorted,
}

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
    /// Partition by mode
    partition_search_mode: PartitionSearchMode,
}

impl BoundedWindowAggExec {
    /// Create a new execution plan for window aggregates
    pub fn try_new(
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: Arc<dyn ExecutionPlan>,
        input_schema: SchemaRef,
        partition_keys: Vec<Arc<dyn PhysicalExpr>>,
        sort_keys: Option<Vec<PhysicalSortExpr>>,
        partition_search_mode: PartitionSearchMode,
    ) -> Result<Self> {
        let schema = create_schema(&input_schema, &window_expr)?;
        let schema = Arc::new(schema);
        println!("input output ordering: {:?}", input.output_ordering());
        println!("partition_search_mode: {:?}", partition_search_mode);
        println!("partition_keys:{:?}", partition_keys);
        println!("partition_bys:{:?}", window_expr[0].partition_by());
        Ok(Self {
            input,
            window_expr,
            schema,
            input_schema,
            partition_keys,
            sort_keys,
            metrics: ExecutionPlanMetricsSet::new(),
            partition_search_mode,
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
            }
        }
        Ok(result)
    }

    /// Return the output sort order of order by keys: For example
    /// OVER(PARTITION BY a, ORDER BY b) -> would give sorting of the column b
    // We are sure that partition by columns are always at the beginning of sort_keys
    // Hence returned `PhysicalSortExpr` corresponding to `PARTITION BY` columns can be used safely
    // to calculate partition separation points
    pub fn order_by_sort_keys(&self) -> Result<Vec<PhysicalSortExpr>> {
        let mut result = vec![];
        // All window exprs have the same partition by, so we just use the first one:
        let order_by = self.window_expr()[0].order_by();
        let sort_keys = self.sort_keys.as_deref().unwrap_or(&[]);
        for item in order_by {
            if let Some(elem) = sort_keys.iter().find(|e| e.expr.eq(&item.expr)) {
                result.push(elem.clone());
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
        // As we can have repartitioning using the partition keys, this can
        // be either one or more than one, depending on the presence of
        // repartitioning.
        self.input.output_partitioning()
    }

    fn unbounded_output(&self, children: &[bool]) -> Result<bool> {
        Ok(children[0])
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input().output_ordering()
    }

    fn required_input_ordering(&self) -> Vec<Option<&[PhysicalSortExpr]>> {
        let sort_keys = self.sort_keys.as_deref();
        vec![sort_keys]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        if self.partition_keys.is_empty() {
            debug!("No partition defined for BoundedWindowAggExec!!!");
            vec![Distribution::SinglePartition]
        } else {
            //TODO support PartitionCollections if there is no common partition columns in the window_expr
            vec![Distribution::HashPartitioned(self.partition_keys.clone())]
        }
    }

    fn equivalence_properties(&self) -> EquivalenceProperties {
        self.input().equivalence_properties()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
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
            self.partition_search_mode.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let stream = Box::pin(BoundedWindowAggStream::new(
            self.schema.clone(),
            self.window_expr.clone(),
            input,
            BaselineMetrics::new(&self.metrics, partition),
            self.partition_by_sort_keys()?,
            self.partition_search_mode.clone(),
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
        let mut column_statistics = Vec::with_capacity(win_cols + input_cols);
        if let Some(input_col_stats) = input_stat.column_statistics {
            column_statistics.extend(input_col_stats);
        } else {
            column_statistics.extend(vec![ColumnStatistics::default(); input_cols]);
        }
        column_statistics.extend(vec![ColumnStatistics::default(); win_cols]);
        Statistics {
            is_exact: input_stat.is_exact,
            num_rows: input_stat.num_rows,
            column_statistics: Some(column_statistics),
            total_byte_size: None,
        }
    }
}

fn create_schema(
    input_schema: &Schema,
    window_expr: &[Arc<dyn WindowExpr>],
) -> Result<Schema> {
    let mut fields = Vec::with_capacity(input_schema.fields().len() + window_expr.len());
    fields.extend_from_slice(input_schema.fields());
    // append results to the schema
    for expr in window_expr {
        fields.push(expr.field()?);
    }
    Ok(Schema::new(fields))
}

/// stream for window aggregation plan
/// assuming partition by column is sorted (or without PARTITION BY expression)
pub struct BoundedWindowAggStream {
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
    search_mode: PartitionSearchMode,
}

impl BoundedWindowAggStream {
    /// This method constructs output columns using the result of each window expression
    fn calculate_out_columns(&mut self) -> Result<Option<Vec<ArrayRef>>> {
        match self.search_mode {
            PartitionSearchMode::Sorted => {
                let n_out = self.calculate_n_out_row();
                if n_out == 0 {
                    Ok(None)
                } else {
                    self.input_buffer
                        .columns()
                        .iter()
                        .map(|elem| Ok(elem.slice(0, n_out)))
                        .chain(
                            self.window_agg_states
                                .iter()
                                .map(|elem| get_aggregate_result_out_column(elem, n_out)),
                        )
                        .collect::<Result<Vec<_>>>()
                        .map(Some)
                }
            }
            PartitionSearchMode::Linear | PartitionSearchMode::PartiallySorted(_) => {
                let partition_by_columns =
                    self.evaluate_partition_by_column_values(&self.input_buffer)?;
                let mut res_generated = vec![];
                for window_agg_state in &self.window_agg_states {
                    let mut map = IndexMap::new();
                    for (key, value) in window_agg_state {
                        map.insert(key, value.state.out_col.len());
                    }
                    res_generated.push(map);
                }
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
                    }
                    if row_res.len() != n_window_col {
                        break;
                    }
                    for (col_idx, elem) in row_res.into_iter().enumerate() {
                        rows_gen[col_idx].push(elem)
                    }
                }
                for (key, val) in counter.iter() {
                    if let Some(partition_batch_state) =
                        self.partition_buffers.get_mut(key)
                    {
                        partition_batch_state.n_out_row = *val.iter().min().unwrap();
                    }
                }
                if rows_gen[0].len() > 0 {
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
                } else {
                    Ok(None)
                }
            }
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
        self.prune_out_columns(n_out)?;
        // Prune `self.partition_batches`:
        self.prune_partition_batches()?;
        // Prune `self.input_buffer_record_batch`:
        self.prune_input_batch(n_out)?;
        Ok(())
    }

    fn update_partition_batch(&mut self, record_batch: RecordBatch) -> Result<()> {
        let num_rows = record_batch.num_rows();
        println!("num rows: {:?}", num_rows);
        if num_rows > 0 {
            let partition_batches = self.evaluate_partition_batches(&record_batch)?;
            for (partition_row, (partition_batch, indices)) in partition_batches {
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
        println!("self.search_mode: {:?}", self.search_mode);
        match &self.search_mode {
            PartitionSearchMode::Sorted => {
                let n_partitions = self.partition_buffers.len();
                for (idx, (_, partition_batch_state)) in
                    self.partition_buffers.iter_mut().enumerate()
                {
                    partition_batch_state.is_end |= idx < n_partitions - 1;
                }
            }
            PartitionSearchMode::PartiallySorted(ordered_partition_bys) => {
                if let Some((last_row, _)) = self.partition_buffers.last() {
                    println!("last_row:{:?}", last_row);
                    println!("partition keys:{:?}", self.partition_by_sort_keys);
                    println!("partition bys:{:?}", self.window_expr[0].partition_by());
                    println!("ordered_partition_bys:{:?}", ordered_partition_bys);
                    // TODO: Get ordered_partition_bys indices to partition by mapping
                    let indices = (0..ordered_partition_bys.len()).collect::<Vec<_>>();
                    println!("indices: {:?}", indices);
                    let last_sorted_cols = indices
                        .iter()
                        .map(|idx| last_row[*idx].clone())
                        .collect::<Vec<_>>();
                    println!(
                        "last_sorted_cols:{:?}", last_sorted_cols
                    );
                    for (partition_row, partition_batch_state) in
                        self.partition_buffers.iter_mut()
                    {
                        println!("partition_row:{:?}", partition_row);
                        let sorted_cols = indices
                            .iter()
                            .map(|idx| partition_row[*idx].clone())
                            .collect::<Vec<_>>();
                        println!(
                            "sorted_cols:{:?}", sorted_cols
                        );
                        if sorted_cols != last_sorted_cols {
                            // It is guaranteed that we will no longer receive value for these partitions
                            println!("marking as end");
                            partition_batch_state.is_end = true;
                        }
                    }
                }
            }
            _ => {}
        };

        self.input_buffer = if self.input_buffer.num_rows() == 0 {
            record_batch
        } else {
            merge_batches(&self.input_buffer, &record_batch, self.input.schema())?
        };

        Ok(())
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
}

impl Stream for BoundedWindowAggStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);
        self.baseline_metrics.record_poll(poll)
    }
}

impl BoundedWindowAggStream {
    /// Create a new BoundedWindowAggStream
    pub fn new(
        schema: SchemaRef,
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
        partition_by_sort_keys: Vec<PhysicalSortExpr>,
        search_mode: PartitionSearchMode,
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
            search_mode,
        }
    }

    fn compute_aggregates(&mut self) -> Result<RecordBatch> {
        // calculate window cols
        for (cur_window_expr, state) in
            self.window_expr.iter().zip(&mut self.window_agg_states)
        {
            cur_window_expr.evaluate_stateful(&self.partition_buffers, state)?;
        }

        let schema = self.schema.clone();
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
                for (_, partition_batch_state) in self.partition_buffers.iter_mut() {
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
            .min()
            .unwrap_or(0)
    }

    /// Prunes the sections of the record batch (for each partition)
    /// that we no longer need to calculate the window function result.
    fn prune_partition_batches(&mut self) -> Result<()> {
        // Implementation is same for Linear and Sorted Versions

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

        let err = || DataFusionError::Execution("Expects to have partition".to_string());
        // Retract no longer needed parts during window calculations from partition batch:
        for (partition_row, n_prune) in n_prune_each_partition.iter() {
            let partition_batch_state = self
                .partition_buffers
                .get_mut(partition_row)
                .ok_or_else(err)?;
            // TODO: do not ues below value for each window state take minimum beginning
            let batch = &partition_batch_state.record_batch;
            partition_batch_state.record_batch =
                batch.slice(*n_prune, batch.num_rows() - n_prune);

            partition_batch_state.indices.drain(0..*n_prune);
            partition_batch_state.n_out_row = 0;

            // Update state indices since we have pruned some rows from the beginning:
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
    /// are calculated and emitted.
    fn prune_input_batch(&mut self, n_out: usize) -> Result<()> {
        // Implementation is same for both Linear and Sorted version
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
        match self.search_mode {
            PartitionSearchMode::Sorted => {
                // We store generated columns for each window expression in the `out_col`
                // field of `WindowAggState`. Given how many rows are emitted, we remove
                // these sections from state.
                for partition_window_agg_states in self.window_agg_states.iter_mut() {
                    let mut running_length = 0;
                    // Remove `n_out` entries from the `out_col` field of `WindowAggState`.
                    // Preserve per partition ordering by iterating in the order of insertion.
                    // Do not generate a result for a new partition without emitting all results
                    // for the current partition.
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
            }
            PartitionSearchMode::Linear | PartitionSearchMode::PartiallySorted(_) => {
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
                            state: WindowAggState { out_col, .. },
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
        partition_columns: &[SortColumn],
    ) -> Result<Vec<Range<usize>>> {
        Ok(if partition_columns.is_empty() {
            vec![Range {
                start: 0,
                end: num_rows,
            }]
        } else {
            lexicographical_partition_ranges(partition_columns)?.collect()
        })
    }

    fn evaluate_partition_batches(
        &self,
        record_batch: &RecordBatch,
    ) -> Result<IndexMap<Vec<ScalarValue>, (RecordBatch, Vec<usize>)>> {
        let mut res: IndexMap<Vec<ScalarValue>, (RecordBatch, Vec<usize>)> =
            IndexMap::new();
        let num_rows = record_batch.num_rows();
        match self.search_mode {
            PartitionSearchMode::Sorted => {
                let partition_columns = self.partition_columns(&record_batch)?;
                let partition_points =
                    self.evaluate_partition_points(num_rows, &partition_columns)?;
                let partition_bys = partition_columns
                    .into_iter()
                    .map(|arr| arr.values)
                    .collect::<Vec<ArrayRef>>();
                for range in partition_points {
                    let partition_row = get_row_at_idx(&partition_bys, range.start)?;
                    let len = range.end - range.start;
                    let slice = record_batch.slice(range.start, len);
                    let indices = (range.start..range.end).collect();
                    res.insert(partition_row, (slice, indices));
                }
            }
            PartitionSearchMode::Linear | PartitionSearchMode::PartiallySorted(_) => {
                let partition_bys =
                    self.evaluate_partition_by_column_values(&record_batch)?;
                // In PartiallySorted implementation we expect indices_map to remember insertion order
                // hence we use IndexMap.
                let mut indices_map: IndexMap<Vec<ScalarValue>, Vec<usize>> =
                    IndexMap::new();
                for idx in 0..num_rows {
                    let partition_row = get_row_at_idx(&partition_bys, idx)?;
                    let indices = indices_map.entry(partition_row).or_default();
                    indices.push(idx);
                }
                for (partition_row, indices) in indices_map {
                    let partition_batch = get_at_indices(&record_batch, &indices)?;
                    res.insert(partition_row, (partition_batch, indices));
                }
            }
        }
        Ok(res)
    }

    /// evaluate the partition points given the sort columns; if the sort columns are
    /// empty then the result will be a single element vec of the whole column rows.
    fn evaluate_partition_points_linear(
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

impl RecordBatchStream for BoundedWindowAggStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn get_at_indices(record_batch: &RecordBatch, indices: &[usize]) -> Result<RecordBatch> {
    let mut batch_indices: UInt64Builder = UInt64Builder::with_capacity(0);
    let casted_indices = indices.iter().map(|elem| *elem as u64).collect::<Vec<_>>();
    batch_indices.append_slice(&casted_indices);
    let batch_indices = batch_indices.finish();
    let new_columns = record_batch
        .columns()
        .iter()
        .map(|array| {
            compute::take(
                array.as_ref(),
                &batch_indices,
                None, // None: no index check
            )
            .unwrap()
        })
        .collect();
    RecordBatch::try_new(record_batch.schema(), new_columns)
        .map_err(DataFusionError::ArrowError)
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
