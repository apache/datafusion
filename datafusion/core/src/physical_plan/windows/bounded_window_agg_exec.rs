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
use ahash::RandomState;
use arrow::array::Array;
use arrow::compute::{
    concat, concat_batches, lexicographical_partition_ranges, SortColumn,
};
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
use std::cmp::{min, Ordering};
use std::collections::HashMap;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::physical_optimizer::sort_enforcement::get_at_indices;
use arrow::array::UInt64Builder;
use arrow::datatypes::DataType;
use arrow::row::{Row, RowConverter, Rows, SortField};
use datafusion_common::utils::get_row_at_idx;
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::hash_utils::create_hashes;
use datafusion_physical_expr::utils::{convert_to_expr, get_indices_of_matching_exprs};
use datafusion_physical_expr::window::{
    PartitionBatchState, PartitionBatches, PartitionWindowAggStates, WindowAggState,
    WindowState,
};
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExpr};
use hashbrown::raw::RawTable;
use indexmap::IndexMap;
use log::debug;

#[derive(Debug, Clone, PartialEq)]
/// Specifies partition column propoerties in terms of input ordering
pub enum PartitionSearchMode {
    /// None of the columns of the partition columns is ordered.
    Linear,
    /// Some columns of the partition columns are ordered but not all
    PartiallySorted,
    /// All Partition columns are ordered
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
    /// Partition by indices that define ordering
    ordered_partition_by_indices: Vec<usize>,
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
        let partition_by_exprs = window_expr[0].partition_by();
        let ordered_partition_by_indices =
            if let Some(input_ordering) = input.output_ordering() {
                let input_ordering_exprs = convert_to_expr(input_ordering);
                let equal_properties = || input.equivalence_properties();
                get_indices_of_matching_exprs(
                    partition_by_exprs,
                    &input_ordering_exprs,
                    equal_properties,
                )
            } else {
                (0..partition_by_exprs.len()).collect()
            };
        Ok(Self {
            input,
            window_expr,
            schema,
            input_schema,
            partition_keys,
            sort_keys,
            metrics: ExecutionPlanMetricsSet::new(),
            partition_search_mode,
            ordered_partition_by_indices,
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
        // All window exprs have the same partition by, so we just use the first one:
        let partition_by = self.window_expr()[0].partition_by();
        let sort_keys = self.sort_keys.as_deref().unwrap_or(&[]);
        let sort_keys_exprs = convert_to_expr(sort_keys);
        let equal_properties = || self.equivalence_properties();
        let indices = get_indices_of_matching_exprs(
            partition_by,
            &sort_keys_exprs,
            equal_properties,
        );
        get_at_indices(sort_keys, &indices)
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
            self.ordered_partition_by_indices.clone(),
        )?);
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
                let mode = &self.partition_search_mode;
                write!(f, "wdw=[{}], mode=[{:?}]", g.join(", "), mode)?;
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
    // Keeps the hash of input buffer calculated from the PARTITION BY columns
    // its length is equal to the RecordBatch length.
    input_buffer_hashes: Vec<u64>,
    /// We separate `input_buffer` based on partitions (as
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
    // Keeps the search mode for partition columns. this determines algorithm
    // for how to group each partition.
    search_mode: PartitionSearchMode,
    // Input ordering and partition by key ordering may not always same
    // this vector stores the mapping between them.
    // For instance, if input is ordered by a,b and window expression is
    // PARTITION BY b,a this stores (1, 0).
    ordered_partition_by_indices: Vec<usize>,
    row_converter: RowConverter,
    random_state: RandomState,
}

impl BoundedWindowAggStream {
    /// This method constructs output columns using the result of each window expression
    fn calculate_out_columns(&mut self) -> Result<Option<Vec<ArrayRef>>> {
        match &self.search_mode {
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
            PartitionSearchMode::Linear | PartitionSearchMode::PartiallySorted => {
                let partition_by_columns =
                    self.evaluate_partition_by_column_values(&self.input_buffer)?;
                let n_window_col = self.window_agg_states.len();
                // `row_map` stores hash id and partition index starting from 0 to n. unique to each partition.
                // partition index corresponds to index of the partition in the vector `partition_indices`
                let mut row_map: RawTable<(u64, usize)> = RawTable::with_capacity(0);
                // `partition_indices` store vector of tuple. First entry is the partition key (unique for each partition)
                // second entry is indices of the rows which partition is constructed in the input record batch
                // Third entry is the how many outputs are generated for the partition.
                let mut partition_indices: Vec<(Vec<ScalarValue>, Vec<usize>, usize)> =
                    vec![];
                let err = || {
                    DataFusionError::Execution("Expects to have partition".to_string())
                };
                for (row_idx, hash) in self.input_buffer_hashes.iter().enumerate() {
                    let entry = row_map.get_mut(*hash, |(_hash, group_idx)| {
                        let row = get_row_at_idx(&partition_by_columns, row_idx).unwrap();
                        row == partition_indices[*group_idx].0
                    });
                    match entry {
                        Some((_hash, group_idx)) => {
                            let (_row, indices, n_out) =
                                &mut partition_indices[*group_idx];
                            if indices.len() >= *n_out {
                                break;
                            }
                            indices.push(row_idx);
                        }
                        None => {
                            row_map.insert(
                                *hash,
                                (*hash, partition_indices.len()),
                                |(hash, _group_index)| *hash,
                            );
                            let row = get_row_at_idx(&partition_by_columns, row_idx)?;
                            let min_out = self
                                .window_agg_states
                                .iter()
                                .map(|window_agg_state| {
                                    window_agg_state
                                        .get(&row)
                                        .map(|partition| partition.state.out_col.len())
                                        .unwrap_or(0)
                                })
                                .min()
                                .unwrap_or(0);
                            if min_out == 0 {
                                break;
                            }
                            partition_indices.push((row, vec![row_idx], min_out));
                        }
                    }
                }
                let mut new_columns = vec![vec![]; n_window_col];
                let mut all_indices = vec![];
                for (row, indices, _) in partition_indices {
                    for (idx, window_agg_state) in
                        self.window_agg_states.iter().enumerate()
                    {
                        let partition = window_agg_state.get(&row).ok_or_else(err)?;
                        let values =
                            partition.state.out_col.slice(0, indices.len()).clone();
                        new_columns[idx].push(values);
                    }
                    let partition_batch_state =
                        self.partition_buffers.get_mut(&row).ok_or_else(err)?;
                    // Store how many rows are generated for each partition
                    partition_batch_state.n_out_row = indices.len();

                    // For each row keep corresponding index in the input record batch
                    all_indices.extend(indices);
                }
                // We couldn't generate any new value return None.
                if all_indices.is_empty() {
                    return Ok(None);
                }

                // Concat results for each column
                let new_columns = new_columns
                    .iter()
                    .map(|elems| {
                        concat(
                            &elems.iter().map(|elem| elem.as_ref()).collect::<Vec<_>>(),
                        )
                        .map_err(DataFusionError::ArrowError)
                    })
                    .collect::<Result<Vec<_>>>()?;
                // We should output column according to row index ordering.
                let sorted_indices = argsort(&all_indices);
                // Construct new column according to row ordering.
                let new_columns = get_arrayref_at_indices(&new_columns, &sorted_indices)?;

                let n_out = new_columns[0].len();
                let res = self
                    .input_buffer
                    .columns()
                    .iter()
                    .map(|elem| elem.slice(0, n_out))
                    .chain(new_columns.into_iter())
                    .collect::<Vec<_>>();
                Ok(Some(res))
            }
        }
    }

    /// Prunes sections of the state that are no longer needed when calculating
    /// results (as determined by window frame boundaries and number of results generated).
    // For instance, if first `n` (not necessarily same with `n_out`) elements are no longer needed to
    // calculate window expression result (outside the window frame boundary) we retract first `n` elements
    // from `self.partition_batches` in corresponding partition.
    // For instance, if `n_out` number of rows are calculated, we can remove
    // first `n_out` rows from `self.input_buffer`.
    fn prune_state(&mut self, n_out: usize) -> Result<()> {
        // Prune `self.window_agg_states`:
        self.prune_out_columns()?;
        // Prune `self.partition_batches`:
        self.prune_partition_batches()?;
        // Prune `self.input_buffer`:
        self.prune_input_batch(n_out)?;
        Ok(())
    }

    fn update_partition_batch(&mut self, record_batch: RecordBatch) -> Result<()> {
        let num_rows = record_batch.num_rows();
        if num_rows > 0 {
            let partition_batches = self.evaluate_partition_batches(&record_batch)?;
            for (partition_row, partition_batch) in partition_batches {
                if let Some(partition_batch_state) =
                    self.partition_buffers.get_mut(&partition_row)
                {
                    partition_batch_state.record_batch = concat_batches(
                        &self.input.schema(),
                        [&partition_batch_state.record_batch, &partition_batch],
                    )?;
                } else {
                    let partition_batch_state = PartitionBatchState {
                        record_batch: partition_batch,
                        is_end: false,
                        n_out_row: 0,
                    };
                    self.partition_buffers
                        .insert(partition_row, partition_batch_state);
                };
            }
        }
        match &self.search_mode {
            PartitionSearchMode::Sorted => {
                let n_partitions = self.partition_buffers.len();
                for (idx, (_, partition_batch_state)) in
                    self.partition_buffers.iter_mut().enumerate()
                {
                    partition_batch_state.is_end |= idx < n_partitions - 1;
                }
            }
            PartitionSearchMode::PartiallySorted => {
                if let Some((last_row, _)) = self.partition_buffers.last() {
                    let last_sorted_cols = self
                        .ordered_partition_by_indices
                        .iter()
                        .map(|idx| last_row[*idx].clone())
                        .collect::<Vec<_>>();
                    for (row, partition_batch_state) in self.partition_buffers.iter_mut()
                    {
                        let sorted_cols = self
                            .ordered_partition_by_indices
                            .iter()
                            .map(|idx| row[*idx].clone())
                            .collect::<Vec<_>>();
                        if sorted_cols != last_sorted_cols {
                            // It is guaranteed that we will no longer receive value for these partitions
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
            concat_batches(&self.input.schema(), [&self.input_buffer, &record_batch])?
        };

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
        ordered_partition_by_indices: Vec<usize>,
    ) -> Result<Self> {
        // TODO: make this function return Result. remove unwraps.
        let state = window_expr.iter().map(|_| IndexMap::new()).collect();
        let empty_batch = RecordBatch::new_empty(schema.clone());
        let partition_by = window_expr[0].partition_by();
        // let res = partition_by[0].data_type(&schema)?;
        let row_converter = if partition_by.is_empty() {
            // If empty create dummy converted with datatype null.
            RowConverter::new(vec![SortField::new(DataType::Null)]).unwrap()
        } else {
            RowConverter::new(
                partition_by
                    .iter()
                    .map(|f| Ok(SortField::new(f.data_type(&schema)?)))
                    .collect::<Result<Vec<_>>>()?,
            )?
        };
        Ok(Self {
            schema,
            input,
            input_buffer: empty_batch,
            input_buffer_hashes: vec![],
            partition_buffers: IndexMap::new(),
            window_agg_states: state,
            finished: false,
            window_expr,
            baseline_metrics,
            partition_by_sort_keys,
            search_mode,
            ordered_partition_by_indices,
            row_converter,
            random_state: Default::default(),
        })
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
    fn calculate_n_out_row(&mut self) -> usize {
        // Different window aggregators may produce results with different rates.
        // We produce the overall batch result with the same speed as slowest one.
        let mut counts = vec![];
        let out_col_counts = self
            .window_agg_states
            .iter()
            .map(|window_agg_state| {
                // Store how many elements are generated for the current
                // window expression:
                let mut cur_window_expr_out_result_len = 0;
                // We iterate over `window_agg_state`, which is an IndexMap.
                // Iterations follow the insertion order, hence we preserve
                // sorting when partition columns are sorted.
                let mut per_partition_out_results = HashMap::new();
                for (row, WindowState { state, .. }) in window_agg_state.iter() {
                    cur_window_expr_out_result_len += state.out_col.len();
                    let count = per_partition_out_results.entry(row.clone()).or_insert(0);
                    if *count < state.out_col.len() {
                        *count = state.out_col.len();
                    }
                    // If we do not generate all results for the current
                    // partition, we do not generate results for next
                    // partition --  otherwise we will lose input ordering.
                    if state.n_row_result_missing > 0 {
                        break;
                    }
                }
                counts.push(per_partition_out_results);
                cur_window_expr_out_result_len
            })
            .collect::<Vec<_>>();
        if let Some(min_idx) = out_col_counts
            .iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(Ordering::Equal))
            .map(|(index, _)| index)
        {
            let err =
                || DataFusionError::Execution("Expects to have partition".to_string());
            let per_partition_out_results = &counts[min_idx];
            for (row, count) in per_partition_out_results.iter() {
                let partition_batch =
                    self.partition_buffers.get_mut(row).ok_or_else(err).unwrap();
                partition_batch.n_out_row = *count;
            }
            out_col_counts[min_idx]
        } else {
            0
        }
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
        let mut n_prune_each_partition = HashMap::new();
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

            let batch = &partition_batch_state.record_batch;
            partition_batch_state.record_batch =
                batch.slice(*n_prune, batch.num_rows() - n_prune);
            // reset n_out_row
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
        let n_to_keep = self.input_buffer.num_rows() - n_out;
        let batch_to_keep = self
            .input_buffer
            .columns()
            .iter()
            .map(|elem| elem.slice(n_out, n_to_keep))
            .collect::<Vec<_>>();

        if matches!(
            self.search_mode,
            PartitionSearchMode::PartiallySorted | PartitionSearchMode::Linear
        ) {
            self.input_buffer_hashes.drain(0..n_out);
        }
        self.input_buffer =
            RecordBatch::try_new(self.input_buffer.schema(), batch_to_keep)?;
        Ok(())
    }

    /// Prunes emitted parts from WindowAggState `out_col` field.
    fn prune_out_columns(&mut self) -> Result<()> {
        let err = || DataFusionError::Execution("Expects to have partition".to_string());
        // We store generated columns for each window expression in the `out_col`
        // field of `WindowAggState`. Given how many rows are emitted, we remove
        // these sections from state.
        for partition_window_agg_states in self.window_agg_states.iter_mut() {
            // Remove `n_out` entries from the `out_col` field of `WindowAggState`.
            // `n_out` is stored in `self.partition_buffers` for each partition.
            // If is_end is set directly remove them. This decreases map hash map size.
            partition_window_agg_states
                .retain(|_, partition_batch_state| !partition_batch_state.state.is_end);
            for (
                partition_key,
                WindowState {
                    state: WindowAggState { out_col, .. },
                    ..
                },
            ) in partition_window_agg_states
            {
                let partition_batch = self
                    .partition_buffers
                    .get_mut(partition_key)
                    .ok_or_else(err)?;
                let n_to_del = partition_batch.n_out_row;
                let n_to_keep = out_col.len() - n_to_del;
                *out_col = out_col.slice(n_to_del, n_to_keep);
            }
        }
        Ok(())
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
        &mut self,
        record_batch: &RecordBatch,
    ) -> Result<IndexMap<Vec<ScalarValue>, RecordBatch>> {
        let mut res = IndexMap::new();
        let num_rows = record_batch.num_rows();
        match &self.search_mode {
            PartitionSearchMode::Sorted => {
                // In Sorted case all partition by columns should have ordering, otherwise we cannot
                // determine boundaries
                assert_eq!(
                    self.partition_by_sort_keys.len(),
                    self.ordered_partition_by_indices.len()
                );
                let partition_columns = self
                    .partition_by_sort_keys
                    .iter()
                    .map(|elem| elem.evaluate_to_sort_column(record_batch))
                    .collect::<Result<Vec<_>>>()?;
                let partition_columns_ordered = get_at_indices(
                    &partition_columns,
                    &self.ordered_partition_by_indices,
                )?;
                let partition_points =
                    self.evaluate_partition_points(num_rows, &partition_columns_ordered)?;
                let partition_bys = partition_columns
                    .into_iter()
                    .map(|arr| arr.values)
                    .collect::<Vec<ArrayRef>>();
                for range in partition_points {
                    let row = get_row_at_idx(&partition_bys, range.start)?;
                    let len = range.end - range.start;
                    let slice = record_batch.slice(range.start, len);
                    res.insert(row, slice);
                }
            }
            PartitionSearchMode::Linear | PartitionSearchMode::PartiallySorted => {
                let partition_bys =
                    self.evaluate_partition_by_column_values(record_batch)?;
                // In Linear or PartiallySorted mode we are sure that partition_by columns are not empty.
                // Convert columns to row format for efficient processing
                let rows = self.row_converter.convert_columns(&partition_bys)?;
                // Calculate indices for each partition
                let per_partition_indices =
                    self.get_per_partition_indices(&partition_bys, record_batch, &rows)?;
                // Construct new record batch from the rows at the calculated indices for each partition.
                for (_, indices) in per_partition_indices.into_iter() {
                    let row = get_row_at_idx(&partition_bys, indices[0])?;
                    let partition_batch =
                        get_record_batch_at_indices(record_batch, &indices)?;
                    res.insert(row, partition_batch);
                }
            }
        }
        Ok(res)
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
                array_to_sort
            })
            .collect::<Result<Vec<ArrayRef>>>()
    }

    fn get_per_partition_indices<'a>(
        &mut self,
        columns: &[ArrayRef],
        batch: &RecordBatch,
        rows: &'a Rows,
    ) -> Result<Vec<(Row<'a>, Vec<usize>)>> {
        let mut batch_hashes = vec![0; batch.num_rows()];
        create_hashes(columns, &self.random_state, &mut batch_hashes)?;
        self.input_buffer_hashes.extend_from_slice(&batch_hashes);
        let mut row_map: RawTable<(u64, usize)> = RawTable::with_capacity(0);
        let mut res: Vec<(Row, Vec<usize>)> = vec![];
        for (row_idx, hash) in batch_hashes.into_iter().enumerate() {
            let entry = row_map.get_mut(hash, |(_hash, group_idx)| {
                rows.row(row_idx) == res[*group_idx].0
            });
            match entry {
                Some((_hash, group_idx)) => res[*group_idx].1.push(row_idx),
                None => {
                    row_map.insert(hash, (hash, res.len()), |(hash, _group_index)| *hash);
                    res.push((rows.row(row_idx), vec![row_idx]));
                }
            }
        }
        Ok(res)
    }
}

impl RecordBatchStream for BoundedWindowAggStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

pub fn argsort<T: Ord>(data: &[T]) -> Vec<usize> {
    let mut indices = (0..data.len()).collect::<Vec<_>>();
    indices.sort_by_key(|&i| &data[i]);
    indices
}

fn get_record_batch_at_indices(
    record_batch: &RecordBatch,
    indices: &[usize],
) -> Result<RecordBatch> {
    let new_columns = get_arrayref_at_indices(record_batch.columns(), indices)?;
    RecordBatch::try_new(record_batch.schema(), new_columns)
        .map_err(DataFusionError::ArrowError)
}

fn get_arrayref_at_indices(
    arrays: &[ArrayRef],
    indices: &[usize],
) -> Result<Vec<ArrayRef>> {
    let mut batch_indices = UInt64Builder::with_capacity(0);
    let casted_indices = indices.iter().map(|elem| *elem as u64).collect::<Vec<_>>();
    batch_indices.append_slice(&casted_indices);
    let batch_indices = batch_indices.finish();
    arrays
        .iter()
        .map(|array| {
            compute::take(
                array.as_ref(),
                &batch_indices,
                None, // None: no index check
            )
            .map_err(DataFusionError::ArrowError)
        })
        .collect::<Result<Vec<_>>>()
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
