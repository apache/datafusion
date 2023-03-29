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

//! Hash aggregation through row format

use std::cmp::min;
use std::ops::Range;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::vec;

use ahash::RandomState;
use arrow::row::{OwnedRow, RowConverter, Rows, SortField};
use datafusion_physical_expr::hash_utils::create_hashes;
use futures::ready;
use futures::stream::{Stream, StreamExt};

use crate::execution::context::TaskContext;
use crate::execution::memory_pool::proxy::{RawTableAllocExt, VecAllocExt};
use crate::physical_plan::aggregates::{
    evaluate_group_by, evaluate_many, group_schema, AccumulatorItem, AggregateMode,
    PhysicalGroupBy, RowAccumulatorItem,
};
use crate::physical_plan::metrics::{BaselineMetrics, RecordOutput};
use crate::physical_plan::{aggregates, AggregateExpr, PhysicalExpr};
use crate::physical_plan::{RecordBatchStream, SendableRecordBatchStream};

use crate::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use arrow::array::{new_null_array, PrimitiveArray};
use arrow::array::{Array, UInt32Builder};
use arrow::compute::{cast, SortColumn};
use arrow::datatypes::{DataType, Schema, UInt32Type};
use arrow::{array::ArrayRef, compute};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion_common::utils::{evaluate_partition_points, get_row_at_idx};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::Accumulator;
use datafusion_physical_expr::window::PartitionKey;
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_row::accessor::RowAccessor;
use datafusion_row::layout::RowLayout;
use datafusion_row::reader::{read_row, RowReader};
use datafusion_row::{MutableRecordBatch, RowType};
use hashbrown::raw::RawTable;

/// Grouping aggregate with row-format aggregation states inside.
///
/// For each aggregation entry, we use:
/// - [Compact] row represents grouping keys for fast hash computation and comparison directly on raw bytes.
/// - [WordAligned] row to store aggregation state, designed to be CPU-friendly when updates over every field are often.
///
/// The architecture is the following:
///
/// 1. For each input RecordBatch, update aggregation states corresponding to all appeared grouping keys.
/// 2. At the end of the aggregation (e.g. end of batches in a partition), the accumulator converts its state to a RecordBatch of a single row
/// 3. The RecordBatches of all accumulators are merged (`concatenate` in `rust/arrow`) together to a single RecordBatch.
/// 4. The state's RecordBatch is `merge`d to a new state
/// 5. The state is mapped to the final value
///
/// [Compact]: datafusion_row::layout::RowType::Compact
/// [WordAligned]: datafusion_row::layout::RowType::WordAligned
pub(crate) struct GroupedHashAggregateStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    mode: AggregateMode,
    exec_state: ExecutionState,
    normal_aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    row_aggr_state: RowAggregationState,
    /// Aggregate expressions not supporting row accumulation
    normal_aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    /// Aggregate expressions supporting row accumulation
    row_aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpr>>>,

    group_by: PhysicalGroupBy,
    row_accumulators: Vec<RowAccumulatorItem>,

    row_converter: RowConverter,
    row_aggr_schema: SchemaRef,
    row_aggr_layout: Arc<RowLayout>,

    baseline_metrics: BaselineMetrics,
    random_state: RandomState,
    /// size to be used for resulting RecordBatches
    batch_size: usize,
    /// if the result is chunked into batches,
    /// last offset is preserved for continuation.
    row_group_skip_position: usize,
    /// keeps range for each accumulator in the field
    /// first element in the array corresponds to normal accumulators
    /// second element in the array corresponds to row accumulators
    indices: [Vec<Range<usize>>; 2],
    ordered_indices: Vec<usize>,
    ordering: Vec<PhysicalSortExpr>,
    is_end: bool,
    is_fully_sorted: bool,
}

#[derive(Debug)]
/// tracks what phase the aggregation is in
enum ExecutionState {
    ReadingInput,
    ProducingOutput,
    Done,
}

fn aggr_state_schema(aggr_expr: &[Arc<dyn AggregateExpr>]) -> Result<SchemaRef> {
    let fields = aggr_expr
        .iter()
        .flat_map(|expr| expr.state_fields().unwrap().into_iter())
        .collect::<Vec<_>>();
    Ok(Arc::new(Schema::new(fields)))
}

impl GroupedHashAggregateStream {
    /// Create a new GroupedHashAggregateStream
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        mode: AggregateMode,
        schema: SchemaRef,
        group_by: PhysicalGroupBy,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
        input: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
        batch_size: usize,
        context: Arc<TaskContext>,
        partition: usize,
        ordered_indices: Vec<usize>,
        ordering: Vec<PhysicalSortExpr>,
    ) -> Result<Self> {
        let timer = baseline_metrics.elapsed_compute().timer();
        let mut start_idx = group_by.expr.len();
        let mut row_aggr_expr = vec![];
        let mut row_agg_indices = vec![];
        let mut row_aggregate_expressions = vec![];
        let mut normal_aggr_expr = vec![];
        let mut normal_agg_indices = vec![];
        let mut normal_aggregate_expressions = vec![];
        // The expressions to evaluate the batch, one vec of expressions per aggregation.
        // Assuming create_schema() always puts group columns in front of aggregation columns, we set
        // col_idx_base to the group expression count.
        let all_aggregate_expressions =
            aggregates::aggregate_expressions(&aggr_expr, &mode, start_idx)?;
        for (expr, others) in aggr_expr.iter().zip(all_aggregate_expressions.into_iter())
        {
            let n_fields = match mode {
                // In partial aggregation, we keep additional fields in order to successfully
                // merge aggregation results downstream.
                AggregateMode::Partial => expr.state_fields()?.len(),
                _ => 1,
            };
            // Stores range of each expression:
            let aggr_range = Range {
                start: start_idx,
                end: start_idx + n_fields,
            };
            if expr.row_accumulator_supported() {
                row_aggregate_expressions.push(others);
                row_agg_indices.push(aggr_range);
                row_aggr_expr.push(expr.clone());
            } else {
                normal_aggregate_expressions.push(others);
                normal_agg_indices.push(aggr_range);
                normal_aggr_expr.push(expr.clone());
            }
            start_idx += n_fields;
        }

        let row_accumulators = aggregates::create_row_accumulators(&row_aggr_expr)?;

        let row_aggr_schema = aggr_state_schema(&row_aggr_expr)?;

        let group_schema = group_schema(&schema, group_by.expr.len());
        let row_converter = RowConverter::new(
            group_schema
                .fields()
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )?;

        let row_aggr_layout =
            Arc::new(RowLayout::new(&row_aggr_schema, RowType::WordAligned));

        let name = format!("GroupedHashAggregateStream[{partition}]");
        let row_aggr_state = RowAggregationState {
            reservation: MemoryConsumer::new(name).register(context.memory_pool()),
            map: RawTable::with_capacity(0),
            group_states: Vec::with_capacity(0),
        };

        timer.done();

        let exec_state = ExecutionState::ReadingInput;

        let is_fully_sorted = ordered_indices.len() == group_by.expr.len();
        Ok(GroupedHashAggregateStream {
            schema: Arc::clone(&schema),
            mode,
            exec_state,
            input,
            group_by,
            normal_aggr_expr,
            row_accumulators,
            row_converter,
            row_aggr_schema,
            row_aggr_layout,
            baseline_metrics,
            normal_aggregate_expressions,
            row_aggregate_expressions,
            row_aggr_state,
            random_state: Default::default(),
            batch_size,
            row_group_skip_position: 0,
            indices: [normal_agg_indices, row_agg_indices],
            ordered_indices,
            is_end: false,
            ordering,
            is_fully_sorted,
        })
    }
}

impl Stream for GroupedHashAggregateStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();

        loop {
            match self.exec_state {
                ExecutionState::ReadingInput => {
                    match ready!(self.input.poll_next_unpin(cx)) {
                        // new batch to aggregate
                        Some(Ok(batch)) => {
                            let timer = elapsed_compute.timer();
                            let result = self.group_aggregate_batch(batch);
                            timer.done();

                            // allocate memory
                            // This happens AFTER we actually used the memory, but simplifies the whole accounting and we are OK with
                            // overshooting a bit. Also this means we either store the whole record batch or not.
                            let result = result.and_then(|allocated| {
                                self.row_aggr_state.reservation.try_grow(allocated)
                            });

                            if let Err(e) = result {
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                        // inner had error, return to caller
                        Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                        // inner is done, producing output
                        None => {
                            self.set_can_emits()?;
                            self.exec_state = ExecutionState::ProducingOutput;
                        }
                    }
                }

                ExecutionState::ProducingOutput => {
                    let timer = elapsed_compute.timer();
                    let result = self.create_batch_from_map();
                    // println!("ProducingOutput result:{:?}", result);

                    timer.done();

                    match result {
                        // made output
                        Ok(Some(result)) => {
                            let batch = result.record_output(&self.baseline_metrics);
                            // println!("ReadingInput Mode.");
                            self.row_group_skip_position += batch.num_rows();
                            self.exec_state = ExecutionState::ReadingInput;
                            if !self.ordered_indices.is_empty() {
                                self.prune()?;
                            }
                            return Poll::Ready(Some(Ok(batch)));
                        }
                        // end of output
                        Ok(None) => {
                            self.exec_state = ExecutionState::Done;
                        }
                        // error making output
                        Err(error) => return Poll::Ready(Some(Err(error))),
                    }
                }
                ExecutionState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for GroupedHashAggregateStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl GroupedHashAggregateStream {
    fn get_per_partition_indices(
        &self,
        group_rows: &Rows,
        group_values: &[ArrayRef],
    ) -> Result<Vec<(PartitionKey, u64, Vec<u32>)>> {
        let n_rows = group_rows.num_rows();
        // 1.1 Calculate the group keys for the group values
        let mut batch_hashes = vec![0; n_rows];
        create_hashes(group_values, &self.random_state, &mut batch_hashes)?;
        let mut res: Vec<(PartitionKey, u64, Vec<u32>)> = vec![];
        if self.is_fully_sorted {
            let sort_column = self
                .ordered_indices
                .iter()
                .enumerate()
                .map(|(idx, cur_idx)| SortColumn {
                    values: group_values[*cur_idx].clone(),
                    options: Some(self.ordering[idx].options),
                })
                .collect::<Vec<_>>();
            let n_rows = group_rows.num_rows();
            let ranges = evaluate_partition_points(n_rows, &sort_column)?;
            for range in ranges {
                let row = get_row_at_idx(group_values, range.start)?;
                let indices = (range.start as u32..range.end as u32).collect::<Vec<_>>();
                res.push((row, batch_hashes[range.start], indices))
            }
        } else {
            let mut row_map: RawTable<(u64, usize)> = RawTable::with_capacity(n_rows);
            for (hash, row_idx) in batch_hashes.into_iter().zip(0u32..) {
                let entry = row_map.get_mut(hash, |(_hash, group_idx)| {
                    // In case of hash collusion. Get the partition where PartitionKey is same.
                    // We can safely get first index of the partition indices (During initialization one partition indices has size 1)
                    let row = get_row_at_idx(group_values, row_idx as usize).unwrap();
                    row.eq(&res[*group_idx].0)
                    // equal_rows(row_idx, res[*group_idx].1[0], columns, columns, true).unwrap()
                });
                match entry {
                    // Existing partition. Update indices for the corresponding partition.
                    Some((_hash, group_idx)) => res[*group_idx].2.push(row_idx),
                    None => {
                        row_map.insert(
                            hash,
                            (hash, res.len()),
                            |(hash, _group_index)| *hash,
                        );
                        let row = get_row_at_idx(group_values, row_idx as usize)?;
                        // This is a new partition its only index is row_idx for now.
                        res.push((row, hash, vec![row_idx]));
                    }
                }
            }
        }
        Ok(res)
    }

    /// Perform group-by aggregation for the given [`RecordBatch`].
    ///
    /// If successful, this returns the additional number of bytes that were allocated during this process.
    ///
    fn group_aggregate_batch(&mut self, batch: RecordBatch) -> Result<usize> {
        // Evaluate the grouping expressions:
        let group_by_values = evaluate_group_by(&self.group_by, &batch)?;
        // Keep track of memory allocated:
        let mut allocated = 0usize;

        // Evaluate the aggregation expressions.
        // We could evaluate them after the `take`, but since we need to evaluate all
        // of them anyways, it is more performant to do it while they are together.
        let row_aggr_input_values =
            evaluate_many(&self.row_aggregate_expressions, &batch)?;
        let normal_aggr_input_values =
            evaluate_many(&self.normal_aggregate_expressions, &batch)?;

        let row_converter_size_pre = self.row_converter.size();
        for group_values in &group_by_values {
            let group_rows = self.row_converter.convert_columns(group_values)?;

            // 1.1 construct the key from the group values
            // 1.2 construct the mapping key if it does not exist
            // 1.3 add the row' index to `indices`

            // track which entries in `aggr_state` have rows in this batch to aggregate
            let mut groups_with_rows = vec![];

            let n_rows = group_rows.num_rows();
            // 1.1 Calculate the group keys for the group values
            let mut batch_hashes = vec![0; n_rows];
            create_hashes(group_values, &self.random_state, &mut batch_hashes)?;
            let per_partition_indices =
                self.get_per_partition_indices(&group_rows, group_values)?;

            let RowAggregationState {
                map: row_map,
                group_states: row_group_states,
                ..
            } = &mut self.row_aggr_state;

            for (row, hash, indices) in per_partition_indices {
                let entry = row_map.get_mut(hash, |(_hash, group_idx)| {
                    // verify that a group that we are inserting with hash is
                    // actually the same key value as the group in
                    // existing_idx  (aka group_values @ row)
                    let group_state = &row_group_states[*group_idx];
                    group_rows.row(indices[0] as usize)
                        == group_state.group_by_values.row()
                });
                match entry {
                    // Existing entry for this group value
                    Some((_, group_idx)) => {
                        let group_state = &mut row_group_states[*group_idx];

                        // 1.3
                        if group_state.indices.is_empty() {
                            groups_with_rows.push(*group_idx);
                        };
                        for row in indices {
                            group_state.indices.push_accounted(row, &mut allocated);
                            // remember this row
                        }
                    }
                    //  1.2 Need to create new entry
                    None => {
                        let accumulator_set =
                            aggregates::create_accumulators(&self.normal_aggr_expr)?;
                        let ordered_columns = self
                            .ordered_indices
                            .iter()
                            .map(|idx| row[*idx].clone())
                            .collect::<Vec<_>>();
                        // Add new entry to group_states and save newly created index
                        let group_state = RowGroupState {
                            group_by_values: group_rows.row(indices[0] as usize).owned(),
                            ordered_columns,
                            emit_status: GroupStatus::CanEmit(false),
                            hash,
                            aggregation_buffer: vec![
                                0;
                                self.row_aggr_layout
                                    .fixed_part_width()
                            ],
                            accumulator_set,
                            indices, // 1.3
                        };
                        let group_idx = row_group_states.len();

                        // NOTE: do NOT include the `RowGroupState` struct size in here because this is captured by
                        // `group_states` (see allocation down below)
                        allocated += (std::mem::size_of::<u8>()
                            * group_state.group_by_values.as_ref().len())
                            + (std::mem::size_of::<u8>()
                                * group_state.aggregation_buffer.capacity())
                            + (std::mem::size_of::<u32>()
                                * group_state.indices.capacity());

                        // Allocation done by normal accumulators
                        allocated += (std::mem::size_of::<Box<dyn Accumulator>>()
                            * group_state.accumulator_set.capacity())
                            + group_state
                                .accumulator_set
                                .iter()
                                .map(|accu| accu.size())
                                .sum::<usize>();

                        // for hasher function, use precomputed hash value
                        row_map.insert_accounted(
                            (hash, group_idx),
                            |(hash, _group_index)| *hash,
                            &mut allocated,
                        );

                        row_group_states.push_accounted(group_state, &mut allocated);

                        groups_with_rows.push(group_idx);
                    }
                };
            }

            let RowAggregationState {
                group_states: row_group_states,
                ..
            } = &mut self.row_aggr_state;

            // Collect all indices + offsets based on keys in this vec
            let mut batch_indices: UInt32Builder = UInt32Builder::with_capacity(0);
            let mut offsets = vec![0];
            let mut offset_so_far = 0;

            let (row_values, normal_values) = if self.is_fully_sorted {
                for &group_idx in groups_with_rows.iter() {
                    let indices = &row_group_states[group_idx].indices;
                    let start = indices[0];
                    // Contains at least 1 element.
                    let end = indices[indices.len() - 1] + 1;
                    offset_so_far += (end - start) as usize;
                    offsets.push(offset_so_far);
                }
                let row_values = row_aggr_input_values.clone();
                let normal_values = normal_aggr_input_values.clone();
                (row_values, normal_values)
            } else {
                for &group_idx in groups_with_rows.iter() {
                    let indices = &row_group_states[group_idx].indices;
                    batch_indices.append_slice(indices);
                    offset_so_far += indices.len();
                    offsets.push(offset_so_far);
                }
                let batch_indices = batch_indices.finish();
                let row_values = get_at_indices(&row_aggr_input_values, &batch_indices);
                let normal_values =
                    get_at_indices(&normal_aggr_input_values, &batch_indices);
                (row_values, normal_values)
            };

            // 2.1 for each key in this batch
            // 2.2 for each aggregation
            // 2.3 `slice` from each of its arrays the keys' values
            // 2.4 update / merge the accumulator with the values
            // 2.5 clear indices
            groups_with_rows
                .iter()
                .zip(offsets.windows(2))
                .try_for_each(|(group_idx, offsets)| {
                    let group_state = &mut row_group_states[*group_idx];
                    // 2.2
                    self.row_accumulators
                        .iter_mut()
                        .zip(row_values.iter())
                        .map(|(accumulator, aggr_array)| {
                            (
                                accumulator,
                                aggr_array
                                    .iter()
                                    .map(|array| {
                                        // 2.3
                                        array.slice(offsets[0], offsets[1] - offsets[0])
                                    })
                                    .collect::<Vec<ArrayRef>>(),
                            )
                        })
                        .try_for_each(|(accumulator, values)| {
                            let mut state_accessor = RowAccessor::new_from_layout(
                                self.row_aggr_layout.clone(),
                            );
                            state_accessor.point_to(
                                0,
                                group_state.aggregation_buffer.as_mut_slice(),
                            );
                            match self.mode {
                                AggregateMode::Partial => {
                                    accumulator.update_batch(&values, &mut state_accessor)
                                }
                                AggregateMode::FinalPartitioned
                                | AggregateMode::Final => {
                                    // note: the aggregation here is over states, not values, thus the merge
                                    accumulator.merge_batch(&values, &mut state_accessor)
                                }
                            }
                        })
                        // 2.5
                        .and(Ok(()))?;
                    // normal accumulators
                    group_state
                        .accumulator_set
                        .iter_mut()
                        .zip(normal_values.iter())
                        .map(|(accumulator, aggr_array)| {
                            (
                                accumulator,
                                aggr_array
                                    .iter()
                                    .map(|array| {
                                        // 2.3
                                        array.slice(offsets[0], offsets[1] - offsets[0])
                                    })
                                    .collect::<Vec<ArrayRef>>(),
                            )
                        })
                        .try_for_each(|(accumulator, values)| {
                            let size_pre = accumulator.size();
                            let res = match self.mode {
                                AggregateMode::Partial => {
                                    accumulator.update_batch(&values)
                                }
                                AggregateMode::FinalPartitioned
                                | AggregateMode::Final => {
                                    // note: the aggregation here is over states, not values, thus the merge
                                    accumulator.merge_batch(&values)
                                }
                            };
                            let size_post = accumulator.size();
                            allocated += size_post.saturating_sub(size_pre);
                            res
                        })
                        // 2.5
                        .and({
                            group_state.indices.clear();
                            Ok(())
                        })
                })?;
        }
        allocated += self
            .row_converter
            .size()
            .saturating_sub(row_converter_size_pre);

        let mut new_result = false;
        let last_ordered_columns = self
            .row_aggr_state
            .group_states
            .last()
            .map(|elem| elem.ordered_columns.clone());
        if let Some(last_ordered_columns) = last_ordered_columns {
            for cur_group in &mut self.row_aggr_state.group_states {
                if cur_group.ordered_columns != last_ordered_columns {
                    // We will no longer receive value. Set status to GroupStatus::CanEmit(true)
                    // meaning we can generate result for this group.
                    cur_group.emit_status = GroupStatus::CanEmit(true);
                    new_result = true;
                }
            }
        }
        if new_result {
            // println!("ProducingOutput mode");
            self.exec_state = ExecutionState::ProducingOutput;
        }

        Ok(allocated)
    }
}

#[derive(Debug, PartialEq)]
enum GroupStatus {
    // `CanEmit(true)` means data for current group is completed. And its result can emitted.
    // `CanEmit(false)` means data for current group is completed. And its result can emitted.
    CanEmit(bool),
    // Emitted means that result for the groups is outputted. Group can be pruned from state.
    Emitted,
}

/// The state that is built for each output group.
#[derive(Debug)]
pub struct RowGroupState {
    /// The actual group by values, stored sequentially
    group_by_values: OwnedRow,

    ordered_columns: Vec<ScalarValue>,
    emit_status: GroupStatus,
    hash: u64,

    // Accumulator state, stored sequentially
    pub aggregation_buffer: Vec<u8>,

    // Accumulator state, one for each aggregate that doesn't support row accumulation
    pub accumulator_set: Vec<AccumulatorItem>,

    /// scratch space used to collect indices for input rows in a
    /// bach that have values to aggregate. Reset on each batch
    pub indices: Vec<u32>,
}

/// The state of all the groups
pub struct RowAggregationState {
    pub reservation: MemoryReservation,

    /// Logically maps group values to an index in `group_states`
    ///
    /// Uses the raw API of hashbrown to avoid actually storing the
    /// keys in the table
    ///
    /// keys: u64 hashes of the GroupValue
    /// values: (hash, index into `group_states`)
    pub map: RawTable<(u64, usize)>,

    /// State for each group
    pub group_states: Vec<RowGroupState>,
}

impl std::fmt::Debug for RowAggregationState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // hashes are not store inline, so could only get values
        let map_string = "RawTable";
        f.debug_struct("AggregationState")
            .field("map", &map_string)
            .field("group_states", &self.group_states)
            .finish()
    }
}

impl GroupedHashAggregateStream {
    fn prune(&mut self) -> Result<()> {
        let n_partition = self.row_aggr_state.group_states.len();
        self.row_aggr_state
            .group_states
            .retain(|elem| elem.emit_status != GroupStatus::Emitted);
        let n_partition_new = self.row_aggr_state.group_states.len();
        let n_pruned = n_partition - n_partition_new;
        self.row_aggr_state.map.clear();
        for (idx, elem) in self.row_aggr_state.group_states.iter().enumerate() {
            self.row_aggr_state
                .map
                .insert(elem.hash, (elem.hash, idx), |(hash, _)| *hash);
        }
        self.row_group_skip_position -= n_pruned;
        Ok(())
    }

    fn set_can_emits(&mut self) -> Result<()> {
        self.row_aggr_state
            .group_states
            .iter_mut()
            .for_each(|elem| elem.emit_status = GroupStatus::CanEmit(true));
        Ok(())
    }

    /// Create a RecordBatch with all group keys and accumulator' states or values.
    fn create_batch_from_map(&mut self) -> Result<Option<RecordBatch>> {
        let skip_items = self.row_group_skip_position;
        if skip_items > self.row_aggr_state.group_states.len() || self.is_end {
            return Ok(None);
        }
        if skip_items == self.row_aggr_state.group_states.len() {
            self.is_end = true;
        }
        if self.row_aggr_state.group_states.is_empty() {
            let schema = self.schema.clone();
            return Ok(Some(RecordBatch::new_empty(schema)));
        }

        let end_idx = min(
            skip_items + self.batch_size,
            self.row_aggr_state.group_states.len(),
        );
        let group_state_chunk = &self.row_aggr_state.group_states[skip_items..end_idx];
        let group_state_chunk = &group_state_chunk
            .iter()
            .filter(|elem| elem.emit_status == GroupStatus::CanEmit(true))
            .collect::<Vec<_>>();

        if group_state_chunk.is_empty() {
            let schema = self.schema.clone();
            return Ok(Some(RecordBatch::new_empty(schema)));
        }

        // Buffers for each distinct group (i.e. row accumulator memories)
        let mut state_buffers = group_state_chunk
            .iter()
            .map(|gs| gs.aggregation_buffer.clone())
            .collect::<Vec<_>>();

        let output_fields = self.schema.fields();
        // Store row accumulator results (either final output or intermediate state):
        let row_columns = match self.mode {
            AggregateMode::Partial => {
                read_as_batch(&state_buffers, &self.row_aggr_schema, RowType::WordAligned)
            }
            AggregateMode::Final | AggregateMode::FinalPartitioned => {
                let mut results = vec![];
                for (idx, acc) in self.row_accumulators.iter().enumerate() {
                    let mut state_accessor =
                        RowAccessor::new(&self.row_aggr_schema, RowType::WordAligned);
                    let current = state_buffers
                        .iter_mut()
                        .map(|buffer| {
                            state_accessor.point_to(0, buffer);
                            acc.evaluate(&state_accessor)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    // Get corresponding field for row accumulator
                    let field = &output_fields[self.indices[1][idx].start];
                    let result = if current.is_empty() {
                        Ok(arrow::array::new_empty_array(field.data_type()))
                    } else {
                        let item = ScalarValue::iter_to_array(current)?;
                        // cast output if needed (e.g. for types like Dictionary where
                        // the intermediate GroupByScalar type was not the same as the
                        // output
                        cast(&item, field.data_type())
                    }?;
                    results.push(result);
                }
                results
            }
        };

        // Store normal accumulator results (either final output or intermediate state):
        let mut columns = vec![];
        for (idx, &Range { start, end }) in self.indices[0].iter().enumerate() {
            for (field_idx, field) in output_fields[start..end].iter().enumerate() {
                let current = match self.mode {
                    AggregateMode::Partial => ScalarValue::iter_to_array(
                        group_state_chunk.iter().map(|row_group_state| {
                            row_group_state.accumulator_set[idx]
                                .state()
                                .map(|v| v[field_idx].clone())
                                .expect("Unexpected accumulator state in hash aggregate")
                        }),
                    ),
                    AggregateMode::Final | AggregateMode::FinalPartitioned => {
                        ScalarValue::iter_to_array(group_state_chunk.iter().map(
                            |row_group_state| {
                                row_group_state.accumulator_set[idx].evaluate().expect(
                                    "Unexpected accumulator state in hash aggregate",
                                )
                            },
                        ))
                    }
                }?;
                // Cast output if needed (e.g. for types like Dictionary where
                // the intermediate GroupByScalar type was not the same as the
                // output
                let result = cast(&current, field.data_type())?;
                columns.push(result);
            }
        }

        // Stores the group by fields
        let group_buffers = group_state_chunk
            .iter()
            .map(|gs| gs.group_by_values.row())
            .collect::<Vec<_>>();
        let mut output: Vec<ArrayRef> = self.row_converter.convert_rows(group_buffers)?;

        // The size of the place occupied by row and normal accumulators
        let extra: usize = self
            .indices
            .iter()
            .flatten()
            .map(|Range { start, end }| end - start)
            .sum();
        let empty_arr = new_null_array(&DataType::Null, 1);
        output.extend(std::iter::repeat(empty_arr).take(extra));

        // Write results of both accumulator types to the corresponding location in
        // the output schema:
        let results = [columns.into_iter(), row_columns.into_iter()];
        for (outer, mut current) in results.into_iter().enumerate() {
            for &Range { start, end } in self.indices[outer].iter() {
                for item in output.iter_mut().take(end).skip(start) {
                    *item = current.next().expect("Columns cannot be empty");
                }
            }
        }

        // Set status of the emitted groups to GroupStatus::Emitted mode.
        self.row_aggr_state.group_states[skip_items..end_idx]
            .iter_mut()
            .for_each(|elem| {
                if elem.emit_status == GroupStatus::CanEmit(true) {
                    elem.emit_status = GroupStatus::Emitted;
                }
            });

        Ok(Some(RecordBatch::try_new(self.schema.clone(), output)?))
    }
}

fn read_as_batch(rows: &[Vec<u8>], schema: &Schema, row_type: RowType) -> Vec<ArrayRef> {
    let row_num = rows.len();
    let mut output = MutableRecordBatch::new(row_num, Arc::new(schema.clone()));
    let mut row = RowReader::new(schema, row_type);

    for data in rows {
        row.point_to(0, data);
        read_row(&row, &mut output, schema);
    }

    output.output_as_columns()
}

fn get_at_indices(
    input_values: &[Vec<ArrayRef>],
    batch_indices: &PrimitiveArray<UInt32Type>,
) -> Vec<Vec<ArrayRef>> {
    input_values
        .iter()
        .map(|array| {
            array
                .iter()
                .map(|array| {
                    compute::take(
                        array.as_ref(),
                        batch_indices,
                        None, // None: no index check
                    )
                    .unwrap()
                })
                .collect()
        })
        .collect()
}
