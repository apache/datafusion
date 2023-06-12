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
use arrow::row::{RowConverter, SortField};
use datafusion_physical_expr::hash_utils::create_hashes;
use futures::ready;
use futures::stream::{Stream, StreamExt};

use crate::execution::context::TaskContext;
use crate::execution::memory_pool::proxy::{RawTableAllocExt, VecAllocExt};
use crate::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use crate::physical_plan::aggregates::utils::{
    aggr_state_schema, col_to_value, get_at_indices, get_optional_filters, read_as_batch,
    slice_and_maybe_filter, ExecutionState, GroupState,
};
use crate::physical_plan::aggregates::{
    evaluate_group_by, evaluate_many, evaluate_optional, group_schema, AggregateMode,
    PhysicalGroupBy, RowAccumulatorItem,
};
use crate::physical_plan::metrics::{BaselineMetrics, RecordOutput};
use crate::physical_plan::{aggregates, AggregateExpr, PhysicalExpr};
use crate::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use arrow::array::*;
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion_common::cast::as_boolean_array;
use datafusion_common::{downcast_value, DataFusionError, Result, ScalarValue};
use datafusion_expr::Accumulator;
use datafusion_row::accessor::{ArrowArrayReader, RowAccessor};
use datafusion_row::layout::RowLayout;
use hashbrown::raw::RawTable;
use itertools::izip;

use super::AggregateExec;

/// Grouping aggregate with row-format aggregation states inside.
///
/// For each aggregation entry, we use:
/// - [Arrow-row] represents grouping keys for fast hash computation and comparison directly on raw bytes.
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
/// [WordAligned]: datafusion_row::layout
pub(crate) struct GroupedHashAggregateStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    mode: AggregateMode,

    normal_aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    /// Aggregate expressions not supporting row accumulation
    normal_aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    /// Filter expression for each normal aggregate expression
    normal_filter_expressions: Vec<Option<Arc<dyn PhysicalExpr>>>,

    /// Aggregate expressions supporting row accumulation
    row_aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    /// Filter expression for each row aggregate expression
    row_filter_expressions: Vec<Option<Arc<dyn PhysicalExpr>>>,
    row_accumulators: Vec<RowAccumulatorItem>,
    row_converter: RowConverter,
    row_aggr_schema: SchemaRef,
    row_aggr_layout: Arc<RowLayout>,

    group_by: PhysicalGroupBy,

    aggr_state: AggregationState,
    exec_state: ExecutionState,
    baseline_metrics: BaselineMetrics,
    random_state: RandomState,
    /// size to be used for resulting RecordBatches
    batch_size: usize,
    /// threshold for using `ScalarValue`s to update
    /// accumulators during high-cardinality aggregations for each input batch.
    scalar_update_factor: usize,
    /// if the result is chunked into batches,
    /// last offset is preserved for continuation.
    row_group_skip_position: usize,
    /// keeps range for each accumulator in the field
    /// first element in the array corresponds to normal accumulators
    /// second element in the array corresponds to row accumulators
    indices: [Vec<Range<usize>>; 2],
}

impl GroupedHashAggregateStream {
    /// Create a new GroupedHashAggregateStream
    pub fn new(
        agg: &AggregateExec,
        context: Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        let agg_schema = Arc::clone(&agg.schema);
        let agg_group_by = agg.group_by.clone();
        let agg_filter_expr = agg.filter_expr.clone();

        let batch_size = context.session_config().batch_size();
        let scalar_update_factor = context.session_config().agg_scalar_update_factor();
        let input = agg.input.execute(partition, Arc::clone(&context))?;
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);

        let timer = baseline_metrics.elapsed_compute().timer();

        let mut start_idx = agg_group_by.expr.len();
        let mut row_aggr_expr = vec![];
        let mut row_agg_indices = vec![];
        let mut row_aggregate_expressions = vec![];
        let mut row_filter_expressions = vec![];
        let mut normal_aggr_expr = vec![];
        let mut normal_agg_indices = vec![];
        let mut normal_aggregate_expressions = vec![];
        let mut normal_filter_expressions = vec![];
        // The expressions to evaluate the batch, one vec of expressions per aggregation.
        // Assuming create_schema() always puts group columns in front of aggregation columns, we set
        // col_idx_base to the group expression count.
        let all_aggregate_expressions =
            aggregates::aggregate_expressions(&agg.aggr_expr, &agg.mode, start_idx)?;
        let filter_expressions = match agg.mode {
            AggregateMode::Partial | AggregateMode::Single => agg_filter_expr,
            AggregateMode::Final | AggregateMode::FinalPartitioned => {
                vec![None; agg.aggr_expr.len()]
            }
        };
        for ((expr, others), filter) in agg
            .aggr_expr
            .iter()
            .zip(all_aggregate_expressions.into_iter())
            .zip(filter_expressions.into_iter())
        {
            let n_fields = match agg.mode {
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
                row_filter_expressions.push(filter.clone());
                row_agg_indices.push(aggr_range);
                row_aggr_expr.push(expr.clone());
            } else {
                normal_aggregate_expressions.push(others);
                normal_filter_expressions.push(filter.clone());
                normal_agg_indices.push(aggr_range);
                normal_aggr_expr.push(expr.clone());
            }
            start_idx += n_fields;
        }

        let row_accumulators = aggregates::create_row_accumulators(&row_aggr_expr)?;

        let row_aggr_schema = aggr_state_schema(&row_aggr_expr);

        let group_schema = group_schema(&agg_schema, agg_group_by.expr.len());
        let row_converter = RowConverter::new(
            group_schema
                .fields()
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )?;

        let row_aggr_layout = Arc::new(RowLayout::new(&row_aggr_schema));

        let name = format!("GroupedHashAggregateStream[{partition}]");
        let aggr_state = AggregationState {
            reservation: MemoryConsumer::new(name).register(context.memory_pool()),
            map: RawTable::with_capacity(0),
            group_states: Vec::with_capacity(0),
        };

        timer.done();

        let exec_state = ExecutionState::ReadingInput;

        Ok(GroupedHashAggregateStream {
            schema: agg_schema,
            input,
            mode: agg.mode,
            normal_aggr_expr,
            normal_aggregate_expressions,
            normal_filter_expressions,
            row_aggregate_expressions,
            row_filter_expressions,
            row_accumulators,
            row_converter,
            row_aggr_schema,
            row_aggr_layout,
            group_by: agg_group_by,
            aggr_state,
            exec_state,
            baseline_metrics,
            random_state: Default::default(),
            batch_size,
            scalar_update_factor,
            row_group_skip_position: 0,
            indices: [normal_agg_indices, row_agg_indices],
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
                                self.aggr_state.reservation.try_grow(allocated)
                            });

                            if let Err(e) = result {
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                        // inner had error, return to caller
                        Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                        // inner is done, producing output
                        None => {
                            self.exec_state = ExecutionState::ProducingOutput;
                        }
                    }
                }

                ExecutionState::ProducingOutput => {
                    let timer = elapsed_compute.timer();
                    let result = self.create_batch_from_map();

                    timer.done();
                    self.row_group_skip_position += self.batch_size;

                    match result {
                        // made output
                        Ok(Some(result)) => {
                            let batch = result.record_output(&self.baseline_metrics);
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
    // Update the row_aggr_state according to groub_by values (result of group_by_expressions)
    fn update_group_state(
        &mut self,
        group_values: &[ArrayRef],
        allocated: &mut usize,
    ) -> Result<Vec<usize>> {
        let group_rows = self.row_converter.convert_columns(group_values)?;
        let n_rows = group_rows.num_rows();
        // 1.1 construct the key from the group values
        // 1.2 construct the mapping key if it does not exist
        // 1.3 add the row' index to `indices`

        // track which entries in `aggr_state` have rows in this batch to aggregate
        let mut groups_with_rows = vec![];

        // 1.1 Calculate the group keys for the group values
        let mut batch_hashes = vec![0; n_rows];
        create_hashes(group_values, &self.random_state, &mut batch_hashes)?;

        let AggregationState {
            map, group_states, ..
        } = &mut self.aggr_state;

        for (row, hash) in batch_hashes.into_iter().enumerate() {
            let entry = map.get_mut(hash, |(_hash, group_idx)| {
                // verify that a group that we are inserting with hash is
                // actually the same key value as the group in
                // existing_idx  (aka group_values @ row)
                let group_state = &group_states[*group_idx];
                group_rows.row(row) == group_state.group_by_values.row()
            });

            match entry {
                // Existing entry for this group value
                Some((_hash, group_idx)) => {
                    let group_state = &mut group_states[*group_idx];

                    // 1.3
                    if group_state.indices.is_empty() {
                        groups_with_rows.push(*group_idx);
                    };

                    group_state.indices.push_accounted(row as u32, allocated); // remember this row
                }
                //  1.2 Need to create new entry
                None => {
                    let accumulator_set =
                        aggregates::create_accumulators(&self.normal_aggr_expr)?;
                    // Add new entry to group_states and save newly created index
                    let group_state = GroupState {
                        group_by_values: group_rows.row(row).owned(),
                        aggregation_buffer: vec![
                            0;
                            self.row_aggr_layout.fixed_part_width()
                        ],
                        accumulator_set,
                        indices: vec![row as u32], // 1.3
                    };
                    let group_idx = group_states.len();

                    // NOTE: do NOT include the `GroupState` struct size in here because this is captured by
                    // `group_states` (see allocation down below)
                    *allocated += (std::mem::size_of::<u8>()
                        * group_state.group_by_values.as_ref().len())
                        + (std::mem::size_of::<u8>()
                            * group_state.aggregation_buffer.capacity())
                        + (std::mem::size_of::<u32>() * group_state.indices.capacity());

                    // Allocation done by normal accumulators
                    *allocated += (std::mem::size_of::<Box<dyn Accumulator>>()
                        * group_state.accumulator_set.capacity())
                        + group_state
                            .accumulator_set
                            .iter()
                            .map(|accu| accu.size())
                            .sum::<usize>();

                    // for hasher function, use precomputed hash value
                    map.insert_accounted(
                        (hash, group_idx),
                        |(hash, _group_index)| *hash,
                        allocated,
                    );

                    group_states.push_accounted(group_state, allocated);

                    groups_with_rows.push(group_idx);
                }
            };
        }
        Ok(groups_with_rows)
    }

    // Update the accumulator results, according to row_aggr_state.
    #[allow(clippy::too_many_arguments)]
    fn update_accumulators_using_batch(
        &mut self,
        groups_with_rows: &[usize],
        offsets: &[usize],
        row_values: &[Vec<ArrayRef>],
        normal_values: &[Vec<ArrayRef>],
        row_filter_values: &[Option<ArrayRef>],
        normal_filter_values: &[Option<ArrayRef>],
        allocated: &mut usize,
    ) -> Result<()> {
        // 2.1 for each key in this batch
        // 2.2 for each aggregation
        // 2.3 `slice` from each of its arrays the keys' values
        // 2.4 update / merge the accumulator with the values
        // 2.5 clear indices
        groups_with_rows
            .iter()
            .zip(offsets.windows(2))
            .try_for_each(|(group_idx, offsets)| {
                let group_state = &mut self.aggr_state.group_states[*group_idx];
                // 2.2
                // Process row accumulators
                self.row_accumulators
                    .iter_mut()
                    .zip(row_values.iter())
                    .zip(row_filter_values.iter())
                    .try_for_each(|((accumulator, aggr_array), filter_opt)| {
                        let values = slice_and_maybe_filter(
                            aggr_array,
                            filter_opt.as_ref(),
                            offsets,
                        )?;
                        let mut state_accessor =
                            RowAccessor::new_from_layout(self.row_aggr_layout.clone());
                        state_accessor
                            .point_to(0, group_state.aggregation_buffer.as_mut_slice());
                        match self.mode {
                            AggregateMode::Partial | AggregateMode::Single => {
                                accumulator.update_batch(&values, &mut state_accessor)
                            }
                            AggregateMode::FinalPartitioned | AggregateMode::Final => {
                                // note: the aggregation here is over states, not values, thus the merge
                                accumulator.merge_batch(&values, &mut state_accessor)
                            }
                        }
                    })?;
                // normal accumulators
                group_state
                    .accumulator_set
                    .iter_mut()
                    .zip(normal_values.iter())
                    .zip(normal_filter_values.iter())
                    .try_for_each(|((accumulator, aggr_array), filter_opt)| {
                        let values = slice_and_maybe_filter(
                            aggr_array,
                            filter_opt.as_ref(),
                            offsets,
                        )?;
                        let size_pre = accumulator.size();
                        let res = match self.mode {
                            AggregateMode::Partial | AggregateMode::Single => {
                                accumulator.update_batch(&values)
                            }
                            AggregateMode::FinalPartitioned | AggregateMode::Final => {
                                // note: the aggregation here is over states, not values, thus the merge
                                accumulator.merge_batch(&values)
                            }
                        };
                        let size_post = accumulator.size();
                        *allocated += size_post.saturating_sub(size_pre);
                        res
                    })
                    // 2.5
                    .and({
                        group_state.indices.clear();
                        Ok(())
                    })
            })?;
        Ok(())
    }

    // Update the accumulator results, according to row_aggr_state.
    fn update_accumulators_using_scalar(
        &mut self,
        groups_with_rows: &[usize],
        row_values: &[Vec<ArrayRef>],
        row_filter_values: &[Option<ArrayRef>],
    ) -> Result<()> {
        let filter_bool_array = row_filter_values
            .iter()
            .map(|filter_opt| match filter_opt {
                Some(f) => Ok(Some(as_boolean_array(f)?)),
                None => Ok(None),
            })
            .collect::<Result<Vec<_>>>()?;

        let mut single_value_acc_idx = vec![];
        let mut single_row_acc_idx = vec![];
        self.row_accumulators
            .iter()
            .zip(row_values.iter())
            .enumerate()
            .for_each(|(idx, (acc, values))| {
                if let RowAccumulatorItem::COUNT(_) = acc {
                    single_row_acc_idx.push(idx);
                } else if values.len() == 1 {
                    single_value_acc_idx.push(idx);
                } else {
                    single_row_acc_idx.push(idx);
                };
            });

        if single_value_acc_idx.len() == 1 && single_row_acc_idx.len() == 0 {
            let acc_idx1 = single_value_acc_idx[0];
            let array1 = &row_values[acc_idx1][0];
            let array1_dt = array1.data_type();
            for_all_data_types! { impl_one_accumulator_dispatch, array1_dt, array1, self, update_one_accumulator_with_native_value, groups_with_rows, acc_idx1, filter_bool_array}
        } else if single_value_acc_idx.len() == 2 && single_row_acc_idx.len() == 0 {
            let acc_idx1 = single_value_acc_idx[0];
            let acc_idx2 = single_value_acc_idx[1];
            let array1 = &row_values[acc_idx1][0];
            let array2 = &row_values[acc_idx2][0];
            let array1_dt = array1.data_type();
            let array2_dt = array2.data_type();
            for_all_data_types2! { impl_two_accumulators_dispatch, array1_dt, array2_dt, array1, array2, self, update_two_accumulator2_with_native_value, groups_with_rows, acc_idx1, acc_idx2,filter_bool_array}
        } else {
            for group_idx in groups_with_rows {
                let group_state = &mut self.aggr_state.group_states[*group_idx];
                let mut state_accessor =
                    RowAccessor::new_from_layout(self.row_aggr_layout.clone());
                state_accessor.point_to(0, group_state.aggregation_buffer.as_mut_slice());
                for idx in &group_state.indices {
                    for (accumulator, values_array, filter_array) in izip!(
                        self.row_accumulators.iter_mut(),
                        row_values.iter(),
                        filter_bool_array.iter()
                    ) {
                        accumulator.update_single_row(
                            values_array,
                            filter_array,
                            *idx as usize,
                            &mut state_accessor,
                        )?;
                    }
                }
                // clear the group indices in this group
                group_state.indices.clear();
            }
        }
        Ok(())
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
        let row_filter_values = evaluate_optional(&self.row_filter_expressions, &batch)?;
        let normal_filter_values =
            evaluate_optional(&self.normal_filter_expressions, &batch)?;

        let row_converter_size_pre = self.row_converter.size();
        for group_values in &group_by_values {
            let groups_with_rows =
                self.update_group_state(group_values, &mut allocated)?;
            // Decide the accumulators update mode, use scalar value to update the accumulators when all of the conditions are meet:
            // 1) The aggregation mode is Partial or Single
            // 2) There is not normal aggregation expressions
            // 3) The number of affected groups is high (entries in `aggr_state` have rows need to update). Usually the high cardinality case
            if matches!(self.mode, AggregateMode::Partial | AggregateMode::Single)
                && normal_aggr_input_values.is_empty()
                && normal_filter_values.is_empty()
                && groups_with_rows.len() >= batch.num_rows() / self.scalar_update_factor
            {
                self.update_accumulators_using_scalar(
                    &groups_with_rows,
                    &row_aggr_input_values,
                    &row_filter_values,
                )?;
            } else {
                // Collect all indices + offsets based on keys in this vec
                let mut batch_indices: UInt32Builder = UInt32Builder::with_capacity(0);
                let mut offsets = vec![0];
                let mut offset_so_far = 0;
                for &group_idx in groups_with_rows.iter() {
                    let indices = &self.aggr_state.group_states[group_idx].indices;
                    batch_indices.append_slice(indices);
                    offset_so_far += indices.len();
                    offsets.push(offset_so_far);
                }
                let batch_indices = batch_indices.finish();

                let row_values = get_at_indices(&row_aggr_input_values, &batch_indices)?;
                let normal_values =
                    get_at_indices(&normal_aggr_input_values, &batch_indices)?;
                let row_filter_values =
                    get_optional_filters(&row_filter_values, &batch_indices);
                let normal_filter_values =
                    get_optional_filters(&normal_filter_values, &batch_indices);
                self.update_accumulators_using_batch(
                    &groups_with_rows,
                    &offsets,
                    &row_values,
                    &normal_values,
                    &row_filter_values,
                    &normal_filter_values,
                    &mut allocated,
                )?;
            }
        }
        allocated += self
            .row_converter
            .size()
            .saturating_sub(row_converter_size_pre);
        Ok(allocated)
    }
}

/// The state of all the groups
pub(crate) struct AggregationState {
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
    pub group_states: Vec<GroupState>,
}

impl std::fmt::Debug for AggregationState {
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
    /// Create a RecordBatch with all group keys and accumulator' states or values.
    fn create_batch_from_map(&mut self) -> Result<Option<RecordBatch>> {
        let skip_items = self.row_group_skip_position;
        if skip_items > self.aggr_state.group_states.len() {
            return Ok(None);
        }
        if self.aggr_state.group_states.is_empty() {
            let schema = self.schema.clone();
            return Ok(Some(RecordBatch::new_empty(schema)));
        }

        let end_idx = min(
            skip_items + self.batch_size,
            self.aggr_state.group_states.len(),
        );
        let group_state_chunk = &self.aggr_state.group_states[skip_items..end_idx];

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
                read_as_batch(&state_buffers, &self.row_aggr_schema)
            }
            AggregateMode::Final
            | AggregateMode::FinalPartitioned
            | AggregateMode::Single => {
                let mut results = vec![];
                for (idx, acc) in self.row_accumulators.iter().enumerate() {
                    let mut state_accessor = RowAccessor::new(&self.row_aggr_schema);
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
                        group_state_chunk.iter().map(|group_state| {
                            group_state.accumulator_set[idx]
                                .state()
                                .map(|v| v[field_idx].clone())
                                .expect("Unexpected accumulator state in hash aggregate")
                        }),
                    ),
                    AggregateMode::Final
                    | AggregateMode::FinalPartitioned
                    | AggregateMode::Single => ScalarValue::iter_to_array(
                        group_state_chunk.iter().map(|group_state| {
                            group_state.accumulator_set[idx]
                                .evaluate()
                                .expect("Unexpected accumulator state in hash aggregate")
                        }),
                    ),
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
        Ok(Some(RecordBatch::try_new(self.schema.clone(), output)?))
    }

    fn update_one_accumulator_with_native_value<T1>(
        &mut self,
        groups_with_rows: &[usize],
        agg_input_array1: &T1,
        acc_idx1: usize,
        filter_bool_array: &[Option<&BooleanArray>],
    ) -> Result<()>
    where
        T1: ArrowArrayReader,
    {
        let accumulator1 = &self.row_accumulators[acc_idx1];
        let filter_array1 = &filter_bool_array[acc_idx1];
        for group_idx in groups_with_rows {
            let group_state = &mut self.aggr_state.group_states[*group_idx];
            let mut state_accessor =
                RowAccessor::new_from_layout(self.row_aggr_layout.clone());
            state_accessor.point_to(0, group_state.aggregation_buffer.as_mut_slice());
            for idx in &group_state.indices {
                let value = col_to_value(agg_input_array1, filter_array1, *idx as usize);
                accumulator1.update_value::<T1::Item>(value, &mut state_accessor);
            }
            // clear the group indices in this group
            group_state.indices.clear();
        }

        Ok(())
    }

    fn update_two_accumulator2_with_native_value<T1, T2>(
        &mut self,
        groups_with_rows: &[usize],
        agg_input_array1: &T1,
        agg_input_array2: &T2,
        acc_idx1: usize,
        acc_idx2: usize,
        filter_bool_array: &[Option<&BooleanArray>],
    ) -> Result<()>
    where
        T1: ArrowArrayReader,
        T2: ArrowArrayReader,
    {
        let accumulator1 = &self.row_accumulators[acc_idx1];
        let accumulator2 = &self.row_accumulators[acc_idx2];
        let filter_array1 = &filter_bool_array[acc_idx1];
        let filter_array2 = &filter_bool_array[acc_idx2];
        for group_idx in groups_with_rows {
            let group_state = &mut self.aggr_state.group_states[*group_idx];
            let mut state_accessor =
                RowAccessor::new_from_layout(self.row_aggr_layout.clone());
            state_accessor.point_to(0, group_state.aggregation_buffer.as_mut_slice());
            for idx in &group_state.indices {
                let value = col_to_value(agg_input_array1, filter_array1, *idx as usize);
                accumulator1.update_value::<T1::Item>(value, &mut state_accessor);
                let value = col_to_value(agg_input_array2, filter_array2, *idx as usize);
                accumulator2.update_value::<T2::Item>(value, &mut state_accessor);
            }
            // clear the group indices in this group
            group_state.indices.clear();
        }

        Ok(())
    }
}

macro_rules! for_all_data_types {
    ($macro:ident $(, $x:ident)*) => {
        $macro! {
                [$($x),*],
                { Boolean, BooleanArray },
                { Int8, Int8Array },
                { Int16, Int16Array },
                { Int32, Int32Array },
                { Int64, Int64Array },
                { UInt8, UInt8Array },
                { UInt16, UInt16Array },
                { UInt32, UInt32Array },
                { UInt64, UInt64Array },
                { Float32, Float32Array },
                { Float64, Float64Array },
                { Decimal128, Decimal128Array }
        }
    };
}
pub(crate) use for_all_data_types;

macro_rules! for_all_data_types2 {
    ($macro:ident $(, $x:ident)*) => {
       $macro! {
                [$($x),*],
                { Boolean, BooleanArray, Boolean, BooleanArray },
                { Int8, Int8Array, Boolean, BooleanArray },
                { Int16, Int16Array, Boolean, BooleanArray },
                { Int32, Int32Array, Boolean, BooleanArray },
                { Int64, Int64Array, Boolean, BooleanArray },
                { UInt8, UInt8Array, Boolean, BooleanArray },
                { UInt16, UInt16Array, Boolean, BooleanArray },
                { UInt32, UInt32Array, Boolean, BooleanArray },
                { UInt64, UInt64Array, Boolean, BooleanArray },
                { Float32, Float32Array, Boolean, BooleanArray },
                { Float64, Float64Array, Boolean, BooleanArray },
                { Decimal128, Decimal128Array, Boolean, BooleanArray },
                { Boolean, BooleanArray, Int8, Int8Array },
                { Int8, Int8Array, Int8, Int8Array },
                { Int16, Int16Array, Int8, Int8Array },
                { Int32, Int32Array, Int8, Int8Array },
                { Int64, Int64Array, Int8, Int8Array },
                { UInt8, UInt8Array, Int8, Int8Array },
                { UInt16, UInt16Array, Int8, Int8Array },
                { UInt32, UInt32Array, Int8, Int8Array },
                { UInt64, UInt64Array, Int8, Int8Array },
                { Float32, Float32Array, Int8, Int8Array },
                { Float64, Float64Array, Int8, Int8Array },
                { Decimal128, Decimal128Array, Int8, Int8Array },
                { Boolean, BooleanArray, Int16, Int16Array},
                { Int8, Int8Array, Int16, Int16Array },
                { Int16, Int16Array, Int16, Int16Array },
                { Int32, Int32Array, Int16, Int16Array },
                { Int64, Int64Array, Int16, Int16Array },
                { UInt8, UInt8Array, Int16, Int16Array },
                { UInt16, UInt16Array, Int16, Int16Array },
                { UInt32, UInt32Array, Int16, Int16Array },
                { UInt64, UInt64Array, Int16, Int16Array },
                { Float32, Float32Array, Int16, Int16Array },
                { Float64, Float64Array, Int16, Int16Array },
                { Decimal128, Decimal128Array, Int16, Int16Array },
                { Boolean, BooleanArray, Int32, Int32Array },
                { Int8, Int8Array, Int32, Int32Array },
                { Int16, Int16Array, Int32, Int32Array },
                { Int32, Int32Array, Int32, Int32Array },
                { Int64, Int64Array, Int32, Int32Array },
                { UInt8, UInt8Array, Int32, Int32Array },
                { UInt16, UInt16Array, Int32, Int32Array },
                { UInt32, UInt32Array, Int32, Int32Array },
                { UInt64, UInt64Array, Int32, Int32Array },
                { Float32, Float32Array, Int32, Int32Array },
                { Float64, Float64Array, Int32, Int32Array },
                { Decimal128, Decimal128Array, Int32, Int32Array },
                { Boolean, BooleanArray, Int64, Int64Array },
                { Int8, Int8Array, Int64, Int64Array },
                { Int16, Int16Array, Int64, Int64Array },
                { Int32, Int32Array, Int64, Int64Array },
                { Int64, Int64Array, Int64, Int64Array },
                { UInt8, UInt8Array, Int64, Int64Array },
                { UInt16, UInt16Array, Int64, Int64Array },
                { UInt32, UInt32Array, Int64, Int64Array },
                { UInt64, UInt64Array, Int64, Int64Array },
                { Float32, Float32Array, Int64, Int64Array },
                { Float64, Float64Array, Int64, Int64Array },
                { Decimal128, Decimal128Array, Int64, Int64Array },
                { Boolean, BooleanArray, UInt8, UInt8Array },
                { Int8, Int8Array, UInt8, UInt8Array },
                { Int16, Int16Array, UInt8, UInt8Array },
                { Int32, Int32Array, UInt8, UInt8Array },
                { Int64, Int64Array, UInt8, UInt8Array },
                { UInt8, UInt8Array, UInt8, UInt8Array },
                { UInt16, UInt16Array, UInt8, UInt8Array },
                { UInt32, UInt32Array, UInt8, UInt8Array },
                { UInt64, UInt64Array, UInt8, UInt8Array },
                { Float32, Float32Array, UInt8, UInt8Array },
                { Float64, Float64Array, UInt8, UInt8Array },
                { Decimal128, Decimal128Array, UInt8, UInt8Array },
                { Boolean, BooleanArray, UInt16, UInt16Array },
                { Int8, Int8Array, UInt16, UInt16Array },
                { Int16, Int16Array, UInt16, UInt16Array },
                { Int32, Int32Array, UInt16, UInt16Array },
                { Int64, Int64Array, UInt16, UInt16Array },
                { UInt8, UInt8Array, UInt16, UInt16Array },
                { UInt16, UInt16Array, UInt16, UInt16Array },
                { UInt32, UInt32Array, UInt16, UInt16Array },
                { UInt64, UInt64Array, UInt16, UInt16Array },
                { Float32, Float32Array, UInt16, UInt16Array },
                { Float64, Float64Array, UInt16, UInt16Array },
                { Decimal128, Decimal128Array, UInt16, UInt16Array },
                { Boolean, BooleanArray, UInt32, UInt32Array },
                { Int8, Int8Array, UInt32, UInt32Array },
                { Int16, Int16Array, UInt32, UInt32Array },
                { Int32, Int32Array, UInt32, UInt32Array },
                { Int64, Int64Array, UInt32, UInt32Array },
                { UInt8, UInt8Array, UInt32, UInt32Array },
                { UInt16, UInt16Array, UInt32, UInt32Array },
                { UInt32, UInt32Array, UInt32, UInt32Array },
                { UInt64, UInt64Array, UInt32, UInt32Array },
                { Float32, Float32Array, UInt32, UInt32Array },
                { Float64, Float64Array, UInt32, UInt32Array },
                { Decimal128, Decimal128Array, UInt32, UInt32Array },
                { Boolean, BooleanArray, UInt64, UInt64Array },
                { Int8, Int8Array, UInt64, UInt64Array },
                { Int16, Int16Array, UInt64, UInt64Array },
                { Int32, Int32Array, UInt64, UInt64Array },
                { Int64, Int64Array, UInt64, UInt64Array },
                { UInt8, UInt8Array, UInt64, UInt64Array },
                { UInt16, UInt16Array, UInt64, UInt64Array },
                { UInt32, UInt32Array, UInt64, UInt64Array },
                { UInt64, UInt64Array, UInt64, UInt64Array },
                { Float32, Float32Array, UInt64, UInt64Array },
                { Float64, Float64Array, UInt64, UInt64Array },
                { Decimal128, Decimal128Array, UInt64, UInt64Array },
                { Boolean, BooleanArray, Float32, Float32Array },
                { Int8, Int8Array, Float32, Float32Array },
                { Int16, Int16Array, Float32, Float32Array },
                { Int32, Int32Array, Float32, Float32Array },
                { Int64, Int64Array, Float32, Float32Array },
                { UInt8, UInt8Array, Float32, Float32Array },
                { UInt16, UInt16Array, Float32, Float32Array },
                { UInt32, UInt32Array, Float32, Float32Array },
                { UInt64, UInt64Array, Float32, Float32Array },
                { Float32, Float32Array, Float32, Float32Array },
                { Float64, Float64Array, Float32, Float32Array },
                { Decimal128, Decimal128Array, Float32, Float32Array },
                { Boolean, BooleanArray, Float64, Float64Array },
                { Int8, Int8Array, Float64, Float64Array },
                { Int16, Int16Array, Float64, Float64Array },
                { Int32, Int32Array, Float64, Float64Array },
                { Int64, Int64Array, Float64, Float64Array },
                { UInt8, UInt8Array, Float64, Float64Array },
                { UInt16, UInt16Array, Float64, Float64Array },
                { UInt32, UInt32Array, Float64, Float64Array },
                { UInt64, UInt64Array, Float64, Float64Array },
                { Float32, Float32Array, Float64, Float64Array },
                { Float64, Float64Array, Float64, Float64Array },
                { Decimal128, Decimal128Array, Float64, Float64Array },
                { Boolean, BooleanArray, Decimal128, Decimal128Array },
                { Int8, Int8Array, Decimal128, Decimal128Array },
                { Int16, Int16Array, Decimal128, Decimal128Array },
                { Int32, Int32Array, Decimal128, Decimal128Array },
                { Int64, Int64Array, Decimal128, Decimal128Array },
                { UInt8, UInt8Array, Decimal128, Decimal128Array },
                { UInt16, UInt16Array, Decimal128, Decimal128Array },
                { UInt32, UInt32Array, Decimal128, Decimal128Array },
                { UInt64, UInt64Array, Decimal128, Decimal128Array },
                { Float32, Float32Array, Decimal128, Decimal128Array },
                { Float64, Float64Array, Decimal128, Decimal128Array },
                { Decimal128, Decimal128Array, Decimal128, Decimal128Array }
        }
    };
}

pub(crate) use for_all_data_types2;

/// Generate one accumulator dispatch logic
macro_rules! impl_one_accumulator_dispatch {
    (
        [$array1_dt:ident, $array1:ident, $self:ident, $update_func:ident, $groups_with_rows:ident, $acc_idx1:ident, $filter_bool_array:ident], $({ $i1t:ident, $i1:ident}),*
    ) => {
        match ($array1_dt) {
            $(
                ($i1t! { datatype_match_pattern }) => {
                        let typed_array = downcast_value!($array1, $i1);
                        $self.$update_func(
                            $groups_with_rows,
                            &typed_array,
                            $acc_idx1,
                            &$filter_bool_array,
                        )?;
                }
            )*
            (_) => return Err(DataFusionError::Internal(format!(
                        "Unsupported data type in RowAccumulator: {}",
                        $array1_dt
                    )))
        }
    };
}

/// Generate two accumulators dispatch logic
macro_rules! impl_two_accumulators_dispatch {
    (
        [$array1_dt:ident, $array2_dt:ident, $array1:ident, $array2:ident, $self:ident, $update_func:ident, $groups_with_rows:ident, $acc_idx1:ident, $acc_idx2:ident, $filter_bool_array:ident], $({ $i1t:ident, $i1:ident, $i2t:ident, $i2:ident}),*
    ) => {
        match ($array1_dt, $array2_dt) {
            $(
                ($i1t! { datatype_match_pattern }, $i2t! { datatype_match_pattern }) => {
                        let typed_array1 = downcast_value!($array1, $i1);
                        let typed_array2 = downcast_value!($array2, $i2);
                        $self.$update_func(
                            $groups_with_rows,
                            &typed_array1,
                            &typed_array2,
                            $acc_idx1,
                            $acc_idx2,
                            &$filter_bool_array,
                        )?;
                }
            )*
            (_, _) => return Err(DataFusionError::Internal(format!(
                        "Unsupported data type {} or {} in RowAccumulator",
                        $array1_dt, $array2_dt
                    )))
        }
    };
}

pub(crate) use impl_one_accumulator_dispatch;
pub(crate) use impl_two_accumulators_dispatch;

#[macro_export]
macro_rules! datatype_match_pattern {
    ($match_pattern:pat) => {
        $match_pattern
    };
}

pub use datatype_match_pattern;

macro_rules! Boolean {
    ($macro:ident) => {
        $macro! {
            DataType::Boolean
        }
    };
}

pub(crate) use Boolean;

macro_rules! Int8 {
    ($macro:ident) => {
        $macro! {
            DataType::Int8
        }
    };
}

pub(crate) use Int8;

macro_rules! Int16 {
    ($macro:ident) => {
        $macro! {
            DataType::Int16
        }
    };
}

pub(crate) use Int16;

macro_rules! Int32 {
    ($macro:ident) => {
        $macro! {
            DataType::Int32
        }
    };
}

pub(crate) use Int32;

macro_rules! Int64 {
    ($macro:ident) => {
        $macro! {
            DataType::Int64
        }
    };
}

pub(crate) use Int64;

macro_rules! UInt8 {
    ($macro:ident) => {
        $macro! {
            DataType::UInt8
        }
    };
}

pub(crate) use UInt8;

macro_rules! UInt16 {
    ($macro:ident) => {
        $macro! {
            DataType::UInt16
        }
    };
}

pub(crate) use UInt16;

macro_rules! UInt32 {
    ($macro:ident) => {
        $macro! {
            DataType::UInt32
        }
    };
}

pub(crate) use UInt32;

macro_rules! UInt64 {
    ($macro:ident) => {
        $macro! {
            DataType::UInt64
        }
    };
}

pub(crate) use UInt64;

macro_rules! Float32 {
    ($macro:ident) => {
        $macro! {
            DataType::Float32
        }
    };
}

pub(crate) use Float32;

macro_rules! Float64 {
    ($macro:ident) => {
        $macro! {
            DataType::Float64
        }
    };
}

pub(crate) use Float64;

macro_rules! Decimal128 {
    ($macro:ident) => {
        $macro! {
            DataType::Decimal128 { .. }
        }
    };
}

pub(crate) use Decimal128;
