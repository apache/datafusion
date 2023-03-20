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
use arrow::array::{Array, UInt32Builder};
use arrow::compute::{
    concat, concat_batches, lexicographical_partition_ranges, SortColumn,
};
use arrow::{
    array::ArrayRef,
    datatypes::{Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion_common::DataFusionError;
use futures::stream::Stream;
use futures::{ready, StreamExt};
use std::any::Any;
use std::cmp::{min, Ordering};
use std::collections::{HashMap, VecDeque};
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::physical_optimizer::sort_enforcement::get_at_indices;
use arrow::compute::sort_to_indices;
use datafusion_common::utils::{
    get_arrayref_at_indices, get_record_batch_at_indices, get_row_at_idx,
};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::hash_utils::create_hashes;
use datafusion_physical_expr::utils::{convert_to_expr, get_indices_of_matching_exprs};
use datafusion_physical_expr::window::{
    PartitionBatchState, PartitionBatches, PartitionKey, PartitionWindowAggStates,
    WindowAggState, WindowState,
};
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExpr};
use hashbrown::raw::RawTable;
use indexmap::IndexMap;
use log::debug;

#[derive(Debug, Clone, PartialEq)]
/// Specifies partition column properties in terms of input ordering
pub enum PartitionSearchMode {
    /// None of the columns among the partition columns is ordered.
    Linear,
    /// Some columns of the partition columns are ordered but not all
    PartiallySorted(Vec<usize>),
    /// All Partition columns are ordered (Also empty case)
    Sorted,
}

impl PartialOrd for PartitionSearchMode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (PartitionSearchMode::Sorted, PartitionSearchMode::Sorted) => {
                Some(Ordering::Equal)
            }
            (PartitionSearchMode::Sorted, _) => Some(Ordering::Greater),
            (PartitionSearchMode::PartiallySorted(_), PartitionSearchMode::Sorted) => {
                Some(Ordering::Less)
            }
            (
                PartitionSearchMode::PartiallySorted(lhs),
                PartitionSearchMode::PartiallySorted(rhs),
            ) => Some(lhs.len().cmp(&rhs.len())),
            (PartitionSearchMode::PartiallySorted(_), PartitionSearchMode::Linear) => {
                Some(Ordering::Greater)
            }
            (PartitionSearchMode::Linear, PartitionSearchMode::Linear) => {
                Some(Ordering::Equal)
            }
            (PartitionSearchMode::Linear, _) => Some(Ordering::Less),
        }
    }
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
    pub partition_search_mode: PartitionSearchMode,
    /// Partition by indices that define ordering
    // For example, if input ordering is ORDER BY a,b
    // and window expression contains PARTITION BY b,a
    // `ordered_partition_by_indices` would be 1, 0.
    // Similarly, if window expression contains PARTITION BY a,b
    // `ordered_partition_by_indices` would be 0, 1.
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
        let ordered_partition_by_indices = match &partition_search_mode {
            PartitionSearchMode::Sorted => {
                let input_ordering = sort_keys.as_deref().unwrap_or(&[]);
                let input_ordering_exprs = convert_to_expr(input_ordering);
                let equal_properties = || input.equivalence_properties();
                let ordered_indices = get_indices_of_matching_exprs(
                    partition_by_exprs,
                    &input_ordering_exprs,
                    equal_properties,
                );
                assert_eq!(ordered_indices.len(), partition_by_exprs.len());
                ordered_indices
            }
            PartitionSearchMode::PartiallySorted(ordered_indices) => {
                ordered_indices.clone()
            }
            PartitionSearchMode::Linear => {
                vec![]
            }
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
        // Partition by sort keys indices are stored in self.ordered_partition_by_indices.
        let sort_keys = self.sort_keys.as_deref().unwrap_or(&[]);
        get_at_indices(sort_keys, &self.ordered_partition_by_indices)
    }

    // Initialize appropriate PartitionSearcher implementation from the state.
    fn get_search_algo(&self) -> Result<Box<dyn PartitionSearcher>> {
        let search_mode = &self.partition_search_mode;
        let partition_by_sort_keys = self.partition_by_sort_keys()?;
        let ordered_partition_by_indices = self.ordered_partition_by_indices.clone();
        Ok(match search_mode {
            PartitionSearchMode::Sorted => Box::new(SortedSearch {
                partition_by_sort_keys,
                ordered_partition_by_indices,
            }),
            PartitionSearchMode::Linear | PartitionSearchMode::PartiallySorted(_) => {
                Box::new(LinearSearch::new(ordered_partition_by_indices))
            }
        })
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
        let search_mode = self.get_search_algo()?;
        let stream = Box::pin(BoundedWindowAggStream::new(
            self.schema.clone(),
            self.window_expr.clone(),
            input,
            BaselineMetrics::new(&self.metrics, partition),
            search_mode,
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

// Trait to calculate different partitions. It has 2 implementations Sorted and Linear search
trait PartitionSearcher: Send {
    // This method constructs output columns using the result of each window expression
    fn calculate_out_columns(
        &mut self,
        input_buffer: &mut RecordBatch,
        window_agg_states: &[PartitionWindowAggStates],
        partition_buffers: &mut PartitionBatches,
        window_expr: &[Arc<dyn WindowExpr>],
    ) -> Result<Option<Vec<ArrayRef>>>;

    // Constructs corresponding batches for each partition for the record_batch
    fn evaluate_partition_batches(
        &mut self,
        record_batch: &RecordBatch,
        window_expr: &[Arc<dyn WindowExpr>],
    ) -> Result<Vec<(PartitionKey, RecordBatch)>>;

    // prunes the state
    fn prune(&mut self, _n_out: usize) -> Result<()> {
        Ok(())
    }

    // Mark the partition as end if we are sure that corresponding partition cannot receive
    // any more values
    fn mark_partition_end(&mut self, partition_buffers: &mut PartitionBatches);

    // Update `input_buffer`, and `partition_buffers` with the new `record_batch`.
    fn update_partition_batch(
        &mut self,
        input_buffer: &mut RecordBatch,
        record_batch: RecordBatch,
        window_expr: &[Arc<dyn WindowExpr>],
        partition_buffers: &mut PartitionBatches,
    ) -> Result<()> {
        let num_rows = record_batch.num_rows();
        if num_rows > 0 {
            let partition_batches =
                self.evaluate_partition_batches(&record_batch, window_expr)?;
            for (partition_row, partition_batch) in partition_batches {
                let partition_batch_state =
                    partition_buffers.entry(partition_row).or_insert_with(|| {
                        let empty_batch =
                            RecordBatch::new_empty(partition_batch.schema());
                        PartitionBatchState {
                            record_batch: empty_batch,
                            is_end: false,
                            n_out_row: 0,
                        }
                    });
                partition_batch_state.record_batch = concat_batches(
                    &partition_batch.schema(),
                    [&partition_batch_state.record_batch, &partition_batch],
                )?;
            }
        }
        self.mark_partition_end(partition_buffers);

        *input_buffer = if input_buffer.num_rows() == 0 {
            record_batch
        } else {
            concat_batches(&input_buffer.schema(), [input_buffer, &record_batch])?
        };

        Ok(())
    }
}

pub struct LinearSearch {
    // Keeps the hash of input buffer calculated from the PARTITION BY columns
    // its length is equal to the `input_buffer` length.
    input_buffer_hashes: VecDeque<u64>,
    // Used during hash value calculation
    random_state: RandomState,
    // Input ordering and partition by key ordering may not always same
    // this vector stores the mapping between them.
    // For instance, if input is ordered by a,b and window expression is
    // PARTITION BY b,a this stores (1, 0).
    ordered_partition_by_indices: Vec<usize>,
    // RawTable that is used to calculate unique partitions for each new RecordBatch.
    // First entry in the tuple is hash value.
    // Second entry is the unique id for each partition (increments from 0 to n)
    row_map_batch: RawTable<(u64, usize)>,
    // RawTable that is used to calculate output columns that can be produced at each cycle.
    // First entry in the tuple is hash value.
    // Second entry is the unique id for each partition (increments from 0 to n)
    // Third entry is the how many new outputs is calculated for corresponding partition.
    row_map_out: RawTable<(u64, usize, usize)>,
}

impl PartitionSearcher for LinearSearch {
    /// This method constructs output columns using the result of each window expression
    // Assume input buffer is         |      Partition Buffers would be (Where each partition and its data is seperated)
    // a, 2                           |      a, 2
    // b, 2                           |      a, 2
    // a, 2                           |      a, 2
    // b, 2                           |
    // a, 2                           |      b, 2
    // b, 2                           |      b, 2
    // b, 2                           |      b, 2
    //                                |      b, 2
    // Also assume we happen to calculate 2 new values for a, and 3 for b (To be calculate missing values we may need to consider future values).
    // Partition buffers effectively will be
    // a, 2, 1
    // a, 2, 2
    // a, 2, (missing)
    //
    // b, 2, 1
    // b, 2, 2
    // b, 2, 3
    // b, 2, (missing)
    // When partition buffers are mapped back to the original record batch. Result becomes
    // a, 2, 1
    // b, 2, 1
    // a, 2, 2
    // b, 2, 2
    // a, 2, (missing)
    // b, 2, 3
    // b, 2, (missing)
    // This function calculates the column result of window expression(s) (First 4 entry of 3rd column in the above section.)
    // 1
    // 1
    // 2
    // 2
    // Above section corresponds to calculated result which can be emitted without breaking input buffer ordering.
    fn calculate_out_columns(
        &mut self,
        input_buffer: &mut RecordBatch,
        window_agg_states: &[PartitionWindowAggStates],
        partition_buffers: &mut PartitionBatches,
        window_expr: &[Arc<dyn WindowExpr>],
    ) -> Result<Option<Vec<ArrayRef>>> {
        let partition_output_indices = self.calc_partition_output_indices(
            input_buffer,
            window_agg_states,
            window_expr,
        )?;

        let n_window_col = window_agg_states.len();
        let err = || DataFusionError::Execution("Expects to have partition".to_string());
        let mut new_columns = vec![vec![]; n_window_col];
        // size of all_indices can be at most input_buffer.num_rows(). This is upper bound.
        let mut all_indices: UInt32Builder =
            UInt32Builder::with_capacity(input_buffer.num_rows());
        for (row, indices) in partition_output_indices {
            for (idx, window_agg_state) in window_agg_states.iter().enumerate() {
                let partition = window_agg_state.get(&row).ok_or_else(err)?;
                let values = partition.state.out_col.slice(0, indices.len()).clone();
                new_columns[idx].push(values);
            }
            let partition_batch_state =
                partition_buffers.get_mut(&row).ok_or_else(err)?;
            // Store how many rows are generated for each partition
            partition_batch_state.n_out_row = indices.len();

            // For each row keep corresponding index in the input record batch
            all_indices.append_slice(&indices);
        }
        let all_indices = all_indices.finish();
        // We couldn't generate any new value return None.
        if all_indices.is_empty() {
            return Ok(None);
        }

        // Concat results for each column.
        // Converts Vec<Vec<ArrayRef>> to Vec<ArrayRef> where inner Vec<ArrayRef>
        // is converted to ArrayRef.
        let new_columns = new_columns
            .iter()
            .map(|elems| {
                concat(&elems.iter().map(|elem| elem.as_ref()).collect::<Vec<_>>())
                    .map_err(DataFusionError::ArrowError)
            })
            .collect::<Result<Vec<_>>>()?;
        // Convert PrimitiveArray to ArrayRef (sort_to_indices works on ArrayRef).
        let all_indices = Arc::new(all_indices) as ArrayRef;

        // We should emit columns, according to row index ordering.
        let sorted_indices = sort_to_indices(&all_indices, None, None)?;
        // Construct new column according to row ordering. This fixes ordering
        get_arrayref_at_indices(&new_columns, &sorted_indices).map(Some)
    }

    fn evaluate_partition_batches(
        &mut self,
        record_batch: &RecordBatch,
        window_expr: &[Arc<dyn WindowExpr>],
    ) -> Result<Vec<(PartitionKey, RecordBatch)>> {
        let mut res = vec![];
        let partition_bys =
            self.evaluate_partition_by_column_values(record_batch, window_expr)?;
        // In Linear or PartiallySorted mode we are sure that partition_by columns are not empty.
        // Calculate indices for each partition
        let per_partition_indices =
            self.get_per_partition_indices(&partition_bys, record_batch)?;
        // Construct new record batch from the rows at the calculated indices for each partition.
        for (row, indices) in per_partition_indices.into_iter() {
            let mut new_indices = UInt32Builder::with_capacity(indices.len());
            new_indices.append_slice(&indices);
            let indices = new_indices.finish();
            let partition_batch = get_record_batch_at_indices(record_batch, &indices)?;
            res.push((row, partition_batch));
        }
        Ok(res)
    }

    fn prune(&mut self, n_out: usize) -> Result<()> {
        // Delete hashes for the rows that are outputted.
        self.input_buffer_hashes.drain(0..n_out);
        Ok(())
    }

    fn mark_partition_end(&mut self, partition_buffers: &mut PartitionBatches) {
        // PartiallySorted case, otherwise we cannot mark end of a partition
        if !self.ordered_partition_by_indices.is_empty() {
            if let Some((last_row, _)) = partition_buffers.last() {
                let last_sorted_cols = self
                    .ordered_partition_by_indices
                    .iter()
                    .map(|idx| last_row[*idx].clone())
                    .collect::<Vec<_>>();
                for (row, partition_batch_state) in partition_buffers.iter_mut() {
                    let sorted_cols = self
                        .ordered_partition_by_indices
                        .iter()
                        .map(|idx| row[*idx].clone())
                        .collect::<Vec<_>>();
                    // All the partition other than last_sorted_cols can be marked as end.
                    // We are sure that we will no longer receive value for these partitions
                    // (Receiving new value would violate ordering invariant)
                    if sorted_cols != last_sorted_cols {
                        // It is guaranteed that we will no longer receive value for these partitions
                        partition_batch_state.is_end = true;
                    }
                }
            }
        }
    }
}

impl LinearSearch {
    // Initialize new LinearSearch partition searcher.
    fn new(ordered_partition_by_indices: Vec<usize>) -> Self {
        LinearSearch {
            input_buffer_hashes: VecDeque::new(),
            random_state: Default::default(),
            ordered_partition_by_indices,
            row_map_batch: RawTable::with_capacity(0),
            row_map_out: RawTable::with_capacity(0),
        }
    }

    fn evaluate_partition_by_column_values(
        &self,
        record_batch: &RecordBatch,
        window_expr: &[Arc<dyn WindowExpr>],
    ) -> Result<Vec<ArrayRef>> {
        window_expr[0]
            .partition_by()
            .iter()
            .map(|elem| {
                let value_to_sort = elem.evaluate(record_batch)?;
                match value_to_sort {
                    ColumnarValue::Array(array) => Ok(array),
                    ColumnarValue::Scalar(scalar) => Err(DataFusionError::Plan(format!(
                        "Sort operation is not applicable to scalar value {scalar}"
                    ))),
                }
            })
            .collect::<Result<Vec<ArrayRef>>>()
    }

    fn get_per_partition_indices(
        &mut self,
        columns: &[ArrayRef],
        batch: &RecordBatch,
    ) -> Result<Vec<(PartitionKey, Vec<u32>)>> {
        let mut batch_hashes = vec![0; batch.num_rows()];
        create_hashes(columns, &self.random_state, &mut batch_hashes)?;
        self.input_buffer_hashes.extend(&batch_hashes);
        // reset row_map for new calculation
        self.row_map_batch.clear();
        // res stores PartitionKey and row indices (indices where these partition occurs in the `batch`) for each partition.
        let mut res: Vec<(PartitionKey, Vec<u32>)> = vec![];
        for (hash, row_idx) in batch_hashes.into_iter().zip(0u32..) {
            let entry = self.row_map_batch.get_mut(hash, |(_hash, group_idx)| {
                // In case of hash collusion. Get the partition where PartitionKey is same.
                // We can safely get first index of the partition indices (During initialization one partition indices has size 1)
                let row = get_row_at_idx(columns, row_idx as usize).unwrap();
                row.eq(&res[*group_idx].0)
                // equal_rows(row_idx, res[*group_idx].1[0], columns, columns, true).unwrap()
            });
            match entry {
                // Existing partition. Update indices for the corresponding partition.
                Some((_hash, group_idx)) => res[*group_idx].1.push(row_idx),
                None => {
                    self.row_map_batch.insert(
                        hash,
                        (hash, res.len()),
                        |(hash, _group_index)| *hash,
                    );
                    let row = get_row_at_idx(columns, row_idx as usize)?;
                    // This is a new partition its only index is row_idx for now.
                    res.push((row, vec![row_idx]));
                }
            }
        }
        Ok(res)
    }

    // Calculates partition key, result indices for each partition
    fn calc_partition_output_indices(
        &mut self,
        input_buffer: &mut RecordBatch,
        window_agg_states: &[PartitionWindowAggStates],
        window_expr: &[Arc<dyn WindowExpr>],
    ) -> Result<Vec<(PartitionKey, Vec<u32>)>> {
        let partition_by_columns =
            self.evaluate_partition_by_column_values(input_buffer, window_expr)?;
        // reset the row_map state.
        self.row_map_out.clear();
        // `partition_indices` store vector of tuple. First entry is the partition key (unique for each partition)
        // second entry is indices of the rows which partition is constructed in the input record batch
        let mut partition_indices: Vec<(PartitionKey, Vec<u32>)> = vec![];
        for (hash, row_idx) in self.input_buffer_hashes.iter().zip(0u32..) {
            let entry = self
                .row_map_out
                .get_mut(*hash, |(_hash, group_idx, _n_out)| {
                    let row =
                        get_row_at_idx(&partition_by_columns, row_idx as usize).unwrap();
                    row == partition_indices[*group_idx].0
                });
            match entry {
                Some((_hash, group_idx, n_out)) => {
                    let (_row, indices) = &mut partition_indices[*group_idx];
                    if indices.len() >= *n_out {
                        break;
                    }
                    indices.push(row_idx);
                }
                None => {
                    let row = get_row_at_idx(&partition_by_columns, row_idx as usize)?;
                    let min_out = window_agg_states
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
                    self.row_map_out.insert(
                        *hash,
                        (*hash, partition_indices.len(), min_out),
                        |(hash, _group_index, _n_out)| *hash,
                    );
                    partition_indices.push((row, vec![row_idx]));
                }
            }
        }
        Ok(partition_indices)
    }
}

pub struct SortedSearch {
    // Stores partition by columns and their ordering information
    partition_by_sort_keys: Vec<PhysicalSortExpr>,
    // Input ordering and partition by key ordering may not always same
    // this vector stores the mapping between them.
    // For instance, if input is ordered by a,b and window expression is
    // PARTITION BY b,a this stores (1, 0).
    ordered_partition_by_indices: Vec<usize>,
}

impl PartitionSearcher for SortedSearch {
    /// This method constructs new output columns using the result of each window expression
    fn calculate_out_columns(
        &mut self,
        _input_buffer: &mut RecordBatch,
        window_agg_states: &[PartitionWindowAggStates],
        partition_buffers: &mut PartitionBatches,
        _window_expr: &[Arc<dyn WindowExpr>],
    ) -> Result<Option<Vec<ArrayRef>>> {
        let n_out = self.calculate_n_out_row(window_agg_states, partition_buffers)?;
        if n_out == 0 {
            Ok(None)
        } else {
            window_agg_states
                .iter()
                .map(|elem| get_aggregate_result_out_column(elem, n_out))
                .collect::<Result<Vec<_>>>()
                .map(Some)
        }
    }

    fn evaluate_partition_batches(
        &mut self,
        record_batch: &RecordBatch,
        _window_expr: &[Arc<dyn WindowExpr>],
    ) -> Result<Vec<(PartitionKey, RecordBatch)>> {
        let mut res = vec![];
        let num_rows = record_batch.num_rows();
        // Calculate result of partition by column expressions
        let partition_columns = self
            .partition_by_sort_keys
            .iter()
            .map(|elem| elem.evaluate_to_sort_column(record_batch))
            .collect::<Result<Vec<_>>>()?;
        // Reorder `partition_columns` such that its ordering matches input ordering.
        let partition_columns_ordered =
            get_at_indices(&partition_columns, &self.ordered_partition_by_indices)?;
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
            res.push((row, slice));
        }
        Ok(res)
    }

    fn mark_partition_end(&mut self, partition_buffers: &mut PartitionBatches) {
        // In Sorted case. We can mark all partitions besides last partition as ended.
        // We are sure that those partitions will never receive any values.
        // (Otherwise ordering invariant is violated.)
        let n_partitions = partition_buffers.len();
        for (idx, (_, partition_batch_state)) in partition_buffers.iter_mut().enumerate()
        {
            partition_batch_state.is_end |= idx < n_partitions - 1;
        }
    }
}

impl SortedSearch {
    /// Calculates how many rows [SortedSearch]
    /// can produce as output.
    fn calculate_n_out_row(
        &mut self,
        window_agg_states: &[PartitionWindowAggStates],
        partition_buffers: &mut PartitionBatches,
    ) -> Result<usize> {
        // Different window aggregators may produce results with different rates.
        // We produce the overall batch result with the same speed as slowest one.
        let mut counts = vec![];
        let out_col_counts = window_agg_states
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
        if let Some(min_idx) = argmin(&out_col_counts) {
            let err =
                || DataFusionError::Execution("Expects to have partition".to_string());
            for (row, count) in counts[min_idx].iter() {
                let partition_batch = partition_buffers.get_mut(row).ok_or_else(err)?;
                partition_batch.n_out_row = *count;
            }
            Ok(out_col_counts[min_idx])
        } else {
            Ok(0)
        }
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
    baseline_metrics: BaselineMetrics,
    // Keeps the search mode for partition columns. this determines algorithm
    // for how to group each partition.
    search_mode: Box<dyn PartitionSearcher>,
}

impl BoundedWindowAggStream {
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
        // Prune internal state of search algorithm.
        self.search_mode.prune(n_out)?;
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
    fn new(
        schema: SchemaRef,
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
        search_mode: Box<dyn PartitionSearcher>,
    ) -> Result<Self> {
        let state = window_expr.iter().map(|_| IndexMap::new()).collect();
        let empty_batch = RecordBatch::new_empty(schema.clone());
        Ok(Self {
            schema,
            input,
            input_buffer: empty_batch,
            partition_buffers: IndexMap::new(),
            window_agg_states: state,
            finished: false,
            window_expr,
            baseline_metrics,
            search_mode,
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
        let window_expr_out = self.search_mode.calculate_out_columns(
            &mut self.input_buffer,
            &self.window_agg_states,
            &mut self.partition_buffers,
            &self.window_expr,
        )?;
        if let Some(window_expr_out) = window_expr_out {
            let n_out = window_expr_out[0].len();
            // right append new columns to corresponding section in the original input buffer.
            let columns_to_show = self
                .input_buffer
                .columns()
                .iter()
                .map(|elem| elem.slice(0, n_out))
                .chain(window_expr_out.into_iter())
                .collect::<Vec<_>>();
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
                self.search_mode.update_partition_batch(
                    &mut self.input_buffer,
                    batch,
                    &self.window_expr,
                    &mut self.partition_buffers,
                )?;
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
        let mut n_prune_each_partition = HashMap::new();
        for window_agg_state in self.window_agg_states.iter_mut() {
            window_agg_state.retain(|_, WindowState { state, .. }| !state.is_end);
            for (partition_row, WindowState { state: value, .. }) in window_agg_state {
                let n_prune =
                    min(value.window_frame_range.start, value.last_calculated_index);
                if let Some(current) = n_prune_each_partition.get_mut(partition_row) {
                    if n_prune < *current {
                        *current = n_prune;
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
                window_agg_state[partition_row].state.prune_state(*n_prune);
            }
        }

        Ok(())
    }

    /// Prunes the section of the input batch whose aggregate results
    /// are calculated and emitted.
    fn prune_input_batch(&mut self, n_out: usize) -> Result<()> {
        // Prune first n_out rows from the input_buffer
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
}

impl RecordBatchStream for BoundedWindowAggStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// get the index of minimum entry. If empty return None.
fn argmin<T: PartialOrd>(data: &[T]) -> Option<usize> {
    let index_of_min: Option<usize> = data
        .iter()
        .enumerate()
        .min_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(Ordering::Equal))
        .map(|(index, _)| index);
    index_of_min
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
