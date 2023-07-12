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

//! This file implements streaming aggregation on ordered GROUP BY expressions.
//! Generated output will itself have an ordering and the executor can run with
//! bounded memory, ensuring composability in streaming cases.

use std::cmp::min;
use std::ops::Range;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::vec;

use ahash::RandomState;
use futures::ready;
use futures::stream::{Stream, StreamExt};
use hashbrown::raw::RawTable;
use itertools::izip;

use crate::physical_plan::aggregates::{
    evaluate_group_by, evaluate_many, evaluate_optional, group_schema, AggregateMode,
    AggregationOrdering, GroupByOrderMode, PhysicalGroupBy, RowAccumulatorItem,
};
use crate::physical_plan::metrics::{BaselineMetrics, RecordOutput};
use crate::physical_plan::{aggregates, AggregateExpr, PhysicalExpr};
use crate::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_execution::memory_pool::proxy::{RawTableAllocExt, VecAllocExt};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::TaskContext;

use crate::physical_plan::aggregates::utils::{
    aggr_state_schema, col_to_scalar, get_at_indices, get_optional_filters,
    read_as_batch, slice_and_maybe_filter, ExecutionState, GroupState,
};
use arrow::array::{new_null_array, ArrayRef, UInt32Builder};
use arrow::compute::{cast, SortColumn};
use arrow::datatypes::DataType;
use arrow::row::{OwnedRow, RowConverter, SortField};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion_common::cast::as_boolean_array;
use datafusion_common::utils::{evaluate_partition_ranges, get_row_at_idx};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::Accumulator;
use datafusion_physical_expr::hash_utils::create_hashes;
use datafusion_row::accessor::RowAccessor;
use datafusion_row::layout::RowLayout;

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
/// [Arrow-row]: OwnedRow
/// [WordAligned]: datafusion_row::layout
pub(crate) struct BoundedAggregateStream {
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
    /// Information on how the input of this group is ordered
    aggregation_ordering: AggregationOrdering,
    /// Has this stream finished producing output
    is_end: bool,
}

impl BoundedAggregateStream {
    /// Create a new BoundedAggregateStream
    pub fn new(
        agg: &AggregateExec,
        context: Arc<TaskContext>,
        partition: usize,
        aggregation_ordering: AggregationOrdering, // Stores algorithm mode and output ordering
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
            AggregateMode::Partial
            | AggregateMode::Single
            | AggregateMode::Partitioned => agg_filter_expr,
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

        let name = format!("BoundedAggregateStream[{partition}]");
        let aggr_state = AggregationState {
            reservation: MemoryConsumer::new(name).register(context.memory_pool()),
            map: RawTable::with_capacity(0),
            ordered_group_states: Vec::with_capacity(0),
        };

        timer.done();

        let exec_state = ExecutionState::ReadingInput;

        Ok(BoundedAggregateStream {
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
            is_end: false,
            aggregation_ordering,
        })
    }
}

impl Stream for BoundedAggregateStream {
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
                        // inner is done, switch to producing output
                        None => {
                            let states = self.aggr_state.ordered_group_states.iter_mut();
                            for state in states {
                                state.status = GroupStatus::CanEmit;
                            }
                            self.exec_state = ExecutionState::ProducingOutput;
                        }
                    }
                }

                ExecutionState::ProducingOutput => {
                    let timer = elapsed_compute.timer();
                    let result = self.create_batch_from_map();

                    timer.done();

                    match result {
                        // made output
                        Ok(Some(result)) => {
                            let batch = result.record_output(&self.baseline_metrics);
                            self.row_group_skip_position += batch.num_rows();
                            // try to read more input
                            self.exec_state = ExecutionState::ReadingInput;
                            self.prune();
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

impl RecordBatchStream for BoundedAggregateStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// This utility object encapsulates the row object, the hash and the group
/// indices for a group. This information is used when executing streaming
/// GROUP BY calculations.
struct GroupOrderInfo {
    /// The group by key
    owned_row: OwnedRow,
    /// the hash value of the group
    hash: u64,
    /// the range of row indices in the input batch that belong to this group
    range: Range<usize>,
}

impl BoundedAggregateStream {
    /// Update the aggr_state hash table according to group_by values
    /// (result of group_by_expressions) when group by expressions are
    /// fully ordered.
    fn update_fully_ordered_group_state(
        &mut self,
        group_values: &[ArrayRef],
        per_group_order_info: Vec<GroupOrderInfo>,
        allocated: &mut usize,
    ) -> Result<Vec<usize>> {
        // 1.1 construct the key from the group values
        // 1.2 construct the mapping key if it does not exist
        // 1.3 add the row' index to `indices`

        // track which entries in `aggr_state` have rows in this batch to aggregate
        let mut groups_with_rows = vec![];

        let AggregationState {
            map: row_map,
            ordered_group_states,
            ..
        } = &mut self.aggr_state;

        for GroupOrderInfo {
            owned_row,
            hash,
            range,
        } in per_group_order_info
        {
            let entry = row_map.get_mut(hash, |(_hash, group_idx)| {
                // verify that a group that we are inserting with hash is
                // actually the same key value as the group in
                // existing_idx  (aka group_values @ row)
                let ordered_group_state = &ordered_group_states[*group_idx];
                let group_state = &ordered_group_state.group_state;
                owned_row.row() == group_state.group_by_values.row()
            });

            match entry {
                // Existing entry for this group value
                Some((_hash, group_idx)) => {
                    let group_state = &mut ordered_group_states[*group_idx].group_state;

                    // 1.3
                    if group_state.indices.is_empty() {
                        groups_with_rows.push(*group_idx);
                    };
                    for row in range.start..range.end {
                        // remember this row
                        group_state.indices.push_accounted(row as u32, allocated);
                    }
                }
                //  1.2 Need to create new entry
                None => {
                    let accumulator_set =
                        aggregates::create_accumulators(&self.normal_aggr_expr)?;
                    // Save the value of the ordering columns as Vec<ScalarValue>
                    let row = get_row_at_idx(group_values, range.start)?;
                    let ordered_columns = self
                        .aggregation_ordering
                        .order_indices
                        .iter()
                        .map(|idx| row[*idx].clone())
                        .collect::<Vec<_>>();

                    // Add new entry to group_states and save newly created index
                    let group_state = GroupState {
                        group_by_values: owned_row,
                        aggregation_buffer: vec![
                            0;
                            self.row_aggr_layout.fixed_part_width()
                        ],
                        accumulator_set,
                        indices: (range.start as u32..range.end as u32)
                            .collect::<Vec<_>>(), // 1.3
                    };
                    let group_idx = ordered_group_states.len();

                    // NOTE: do NOT include the `RowGroupState` struct size in here because this is captured by
                    // `group_states` (see allocation down below)
                    *allocated += std::mem::size_of_val(&group_state.group_by_values)
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
                    row_map.insert_accounted(
                        (hash, group_idx),
                        |(hash, _group_index)| *hash,
                        allocated,
                    );

                    let ordered_group_state = OrderedGroupState {
                        group_state,
                        ordered_columns,
                        status: GroupStatus::GroupInProgress,
                        hash,
                    };
                    ordered_group_states.push_accounted(ordered_group_state, allocated);

                    groups_with_rows.push(group_idx);
                }
            };
        }
        Ok(groups_with_rows)
    }

    // Update the aggr_state according to group_by values (result of group_by_expressions)
    fn update_group_state(
        &mut self,
        group_values: &[ArrayRef],
        allocated: &mut usize,
    ) -> Result<Vec<usize>> {
        // 1.1 construct the key from the group values
        // 1.2 construct the mapping key if it does not exist
        // 1.3 add the row' index to `indices`

        // track which entries in `aggr_state` have rows in this batch to aggregate
        let mut groups_with_rows = vec![];

        let group_rows = self.row_converter.convert_columns(group_values)?;
        let n_rows = group_rows.num_rows();
        // 1.1 Calculate the group keys for the group values
        let mut batch_hashes = vec![0; n_rows];
        create_hashes(group_values, &self.random_state, &mut batch_hashes)?;

        let AggregationState {
            map,
            ordered_group_states: group_states,
            ..
        } = &mut self.aggr_state;

        for (row, hash) in batch_hashes.into_iter().enumerate() {
            let entry = map.get_mut(hash, |(_hash, group_idx)| {
                // verify that a group that we are inserting with hash is
                // actually the same key value as the group in
                // existing_idx  (aka group_values @ row)
                let group_state = &group_states[*group_idx].group_state;
                group_rows.row(row) == group_state.group_by_values.row()
            });

            match entry {
                // Existing entry for this group value
                Some((_hash, group_idx)) => {
                    let group_state = &mut group_states[*group_idx].group_state;

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
                    let row_values = get_row_at_idx(group_values, row)?;
                    let ordered_columns = self
                        .aggregation_ordering
                        .order_indices
                        .iter()
                        .map(|idx| row_values[*idx].clone())
                        .collect::<Vec<_>>();
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
                    *allocated += std::mem::size_of_val(&group_state.group_by_values)
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

                    // Add new entry to group_states and save newly created index
                    let ordered_group_state = OrderedGroupState {
                        group_state,
                        ordered_columns,
                        status: GroupStatus::GroupInProgress,
                        hash,
                    };
                    group_states.push_accounted(ordered_group_state, allocated);

                    groups_with_rows.push(group_idx);
                }
            };
        }
        Ok(groups_with_rows)
    }

    // Update the accumulator results, according to aggr_state.
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
                let group_state =
                    &mut self.aggr_state.ordered_group_states[*group_idx].group_state;
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
                            AggregateMode::Partial
                            | AggregateMode::Single
                            | AggregateMode::Partitioned => {
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
                            AggregateMode::Partial
                            | AggregateMode::Single
                            | AggregateMode::Partitioned => {
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

    // Update the accumulator results, according to aggr_state.
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

        for group_idx in groups_with_rows {
            let group_state =
                &mut self.aggr_state.ordered_group_states[*group_idx].group_state;
            let mut state_accessor =
                RowAccessor::new_from_layout(self.row_aggr_layout.clone());
            state_accessor.point_to(0, group_state.aggregation_buffer.as_mut_slice());
            for idx in &group_state.indices {
                for (accumulator, values_array, filter_array) in izip!(
                    self.row_accumulators.iter_mut(),
                    row_values.iter(),
                    filter_bool_array.iter()
                ) {
                    if values_array.len() == 1 {
                        let scalar_value =
                            col_to_scalar(&values_array[0], filter_array, *idx as usize)?;
                        accumulator.update_scalar(&scalar_value, &mut state_accessor)?;
                    } else {
                        let scalar_values = values_array
                            .iter()
                            .map(|array| {
                                col_to_scalar(array, filter_array, *idx as usize)
                            })
                            .collect::<Result<Vec<_>>>()?;
                        accumulator
                            .update_scalar_values(&scalar_values, &mut state_accessor)?;
                    }
                }
            }
            // clear the group indices in this group
            group_state.indices.clear();
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
            // If the input is fully sorted on its grouping keys
            let groups_with_rows = if let AggregationOrdering {
                mode: GroupByOrderMode::FullyOrdered,
                order_indices,
                ordering,
            } = &self.aggregation_ordering
            {
                let group_rows = self.row_converter.convert_columns(group_values)?;
                let n_rows = group_rows.num_rows();
                // 1.1 Calculate the group keys for the group values
                let mut batch_hashes = vec![0; n_rows];
                create_hashes(group_values, &self.random_state, &mut batch_hashes)?;
                let sort_column = order_indices
                    .iter()
                    .enumerate()
                    .map(|(idx, cur_idx)| SortColumn {
                        values: group_values[*cur_idx].clone(),
                        options: Some(ordering[idx].options),
                    })
                    .collect::<Vec<_>>();
                let n_rows = group_rows.num_rows();
                // determine the boundaries between groups
                let ranges = evaluate_partition_ranges(n_rows, &sort_column)?;
                let per_group_order_info = ranges
                    .into_iter()
                    .map(|range| GroupOrderInfo {
                        owned_row: group_rows.row(range.start).owned(),
                        hash: batch_hashes[range.start],
                        range,
                    })
                    .collect::<Vec<_>>();
                self.update_fully_ordered_group_state(
                    group_values,
                    per_group_order_info,
                    &mut allocated,
                )?
            } else {
                self.update_group_state(group_values, &mut allocated)?
            };

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
                let mut batch_indices = UInt32Builder::with_capacity(0);
                let mut offsets = vec![0];
                let mut offset_so_far = 0;
                for &group_idx in groups_with_rows.iter() {
                    let indices = &self.aggr_state.ordered_group_states[group_idx]
                        .group_state
                        .indices;
                    batch_indices.append_slice(indices);
                    offset_so_far += indices.len();
                    offsets.push(offset_so_far);
                }
                let batch_indices = batch_indices.finish();

                let row_filter_values =
                    get_optional_filters(&row_filter_values, &batch_indices);
                let normal_filter_values =
                    get_optional_filters(&normal_filter_values, &batch_indices);
                if self.aggregation_ordering.mode == GroupByOrderMode::FullyOrdered {
                    self.update_accumulators_using_batch(
                        &groups_with_rows,
                        &offsets,
                        &row_aggr_input_values,
                        &normal_aggr_input_values,
                        &row_filter_values,
                        &normal_filter_values,
                        &mut allocated,
                    )?;
                } else {
                    let row_values =
                        get_at_indices(&row_aggr_input_values, &batch_indices)?;
                    let normal_values =
                        get_at_indices(&normal_aggr_input_values, &batch_indices)?;
                    self.update_accumulators_using_batch(
                        &groups_with_rows,
                        &offsets,
                        &row_values,
                        &normal_values,
                        &row_filter_values,
                        &normal_filter_values,
                        &mut allocated,
                    )?;
                };
            }
        }
        allocated += self
            .row_converter
            .size()
            .saturating_sub(row_converter_size_pre);

        let mut new_result = false;
        let last_ordered_columns = self
            .aggr_state
            .ordered_group_states
            .last()
            .map(|item| item.ordered_columns.clone());

        if let Some(last_ordered_columns) = last_ordered_columns {
            for cur_group in &mut self.aggr_state.ordered_group_states {
                if cur_group.ordered_columns != last_ordered_columns {
                    // We will no longer receive value. Set status to GroupStatus::CanEmit
                    // meaning we can generate result for this group.
                    cur_group.status = GroupStatus::CanEmit;
                    new_result = true;
                }
            }
        }
        if new_result {
            self.exec_state = ExecutionState::ProducingOutput;
        }

        Ok(allocated)
    }
}

/// Tracks the state of the ordered grouping
#[derive(Debug, PartialEq)]
enum GroupStatus {
    /// Data for current group is not complete, and new data may yet
    /// arrive.
    GroupInProgress,
    /// Data for current group is completed, and its result can emitted.
    CanEmit,
    /// Result for the groups has been successfully emitted, and group
    /// state can be pruned.
    Emitted,
}

/// Information about the order of the state that is built for each
/// output group.
#[derive(Debug)]
pub struct OrderedGroupState {
    /// Aggregate values
    group_state: GroupState,
    /// The actual value of the ordered columns for this group
    ordered_columns: Vec<ScalarValue>,
    /// Can we emit this group?
    status: GroupStatus,
    /// Hash value of the group
    hash: u64,
}

/// The state of all the groups
pub struct AggregationState {
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
    pub ordered_group_states: Vec<OrderedGroupState>,
}

impl std::fmt::Debug for AggregationState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // hashes are not store inline, so could only get values
        let map_string = "RawTable";
        f.debug_struct("AggregationState")
            .field("map", &map_string)
            .field("ordered_group_states", &self.ordered_group_states)
            .finish()
    }
}

impl BoundedAggregateStream {
    /// Prune the groups from `[Self::ordered_group_states]` which are in
    /// [`GroupStatus::Emitted`].
    ///
    /// Emitted means that the result of this group has already been
    /// emitted, and we are sure that these groups can not receive new
    /// rows.
    fn prune(&mut self) {
        // clear out emitted groups
        let n_partition = self.aggr_state.ordered_group_states.len();
        self.aggr_state
            .ordered_group_states
            .retain(|elem| elem.status != GroupStatus::Emitted);

        let n_partition_new = self.aggr_state.ordered_group_states.len();
        let n_pruned = n_partition - n_partition_new;

        // update hash table with the new indexes of the remaining groups
        self.aggr_state.map.clear();
        for (idx, item) in self.aggr_state.ordered_group_states.iter().enumerate() {
            self.aggr_state
                .map
                .insert(item.hash, (item.hash, idx), |(hash, _)| *hash);
        }
        self.row_group_skip_position -= n_pruned;
    }

    /// Create a RecordBatch with all group keys and accumulator' states or values.
    fn create_batch_from_map(&mut self) -> Result<Option<RecordBatch>> {
        let skip_items = self.row_group_skip_position;
        if skip_items > self.aggr_state.ordered_group_states.len() || self.is_end {
            return Ok(None);
        }
        self.is_end |= skip_items == self.aggr_state.ordered_group_states.len();
        if self.aggr_state.ordered_group_states.is_empty() {
            let schema = self.schema.clone();
            return Ok(Some(RecordBatch::new_empty(schema)));
        }

        let end_idx = min(
            skip_items + self.batch_size,
            self.aggr_state.ordered_group_states.len(),
        );
        let group_state_chunk =
            &self.aggr_state.ordered_group_states[skip_items..end_idx];

        // Consider only the groups that can be emitted. (The ones we
        // are sure that will not receive new entry.)
        let group_state_chunk = group_state_chunk
            .iter()
            .filter(|item| item.status == GroupStatus::CanEmit)
            .collect::<Vec<_>>();

        if group_state_chunk.is_empty() {
            let schema = self.schema.clone();
            return Ok(Some(RecordBatch::new_empty(schema)));
        }

        // Buffers for each distinct group (i.e. row accumulator memories)
        let mut state_buffers = group_state_chunk
            .iter()
            .map(|gs| gs.group_state.aggregation_buffer.clone())
            .collect::<Vec<_>>();

        let output_fields = self.schema.fields();
        // Store row accumulator results (either final output or intermediate state):
        let row_columns = match self.mode {
            AggregateMode::Partial => {
                read_as_batch(&state_buffers, &self.row_aggr_schema)
            }
            AggregateMode::Final
            | AggregateMode::FinalPartitioned
            | AggregateMode::Single
            | AggregateMode::Partitioned => {
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
                            group_state.group_state.accumulator_set[idx]
                                .state()
                                .map(|v| v[field_idx].clone())
                                .expect("Unexpected accumulator state in hash aggregate")
                        }),
                    ),
                    AggregateMode::Final
                    | AggregateMode::FinalPartitioned
                    | AggregateMode::Single
                    | AggregateMode::Partitioned => ScalarValue::iter_to_array(
                        group_state_chunk.iter().map(|group_state| {
                            group_state.group_state.accumulator_set[idx]
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
            .map(|gs| gs.group_state.group_by_values.row())
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
        for gs in self.aggr_state.ordered_group_states[skip_items..end_idx].iter_mut() {
            if gs.status == GroupStatus::CanEmit {
                gs.status = GroupStatus::Emitted;
            }
        }

        Ok(Some(RecordBatch::try_new(self.schema.clone(), output)?))
    }
}
