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
use std::sync::Arc;
use std::task::{Context, Poll};
use std::vec;

use ahash::RandomState;
use futures::stream::BoxStream;
use futures::stream::{Stream, StreamExt};

use crate::error::Result;
use crate::execution::context::TaskContext;
use crate::execution::memory_pool::proxy::{RawTableAllocExt, VecAllocExt};
use crate::physical_plan::aggregates::{
    evaluate_group_by, evaluate_many, group_schema, AccumulatorItem, AccumulatorItemV2,
    AggregateMode, PhysicalGroupBy,
};
use crate::physical_plan::metrics::{BaselineMetrics, RecordOutput};
use crate::physical_plan::{aggregates, AggregateExpr, PhysicalExpr};
use crate::physical_plan::{RecordBatchStream, SendableRecordBatchStream};

use crate::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use arrow::{
    array::{new_null_array, Array, ArrayRef, UInt32Builder},
    compute,
    compute::cast,
    datatypes::{DataType, Schema, SchemaRef},
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::Accumulator;
use datafusion_physical_expr::hash_utils::create_hashes;
use datafusion_row::accessor::RowAccessor;
use datafusion_row::layout::RowLayout;
use datafusion_row::reader::{read_row, RowReader};
use datafusion_row::{row_supported, MutableRecordBatch, RowType};
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
    stream: BoxStream<'static, ArrowResult<RecordBatch>>,
    schema: SchemaRef,
}

/// Actual implementation of [`GroupedHashAggregateStream`].
///
/// This is wrapped into yet another struct because we need to interact with the async memory management subsystem
/// during poll. To have as little code "weirdness" as possible, we chose to just use [`BoxStream`] together with
/// [`futures::stream::unfold`]. The latter requires a state object, which is [`GroupedHashAggregateStreamInner`].
struct GroupedHashAggregateStreamInner {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    mode: AggregateMode,
    normal_aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    row_aggr_state: RowAggregationState,
    normal_aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    row_aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpr>>>,

    group_by: PhysicalGroupBy,
    row_accumulators: Vec<AccumulatorItemV2>,

    row_aggr_schema: SchemaRef,
    row_aggr_layout: Arc<RowLayout>,

    baseline_metrics: BaselineMetrics,
    random_state: RandomState,
    /// size to be used for resulting RecordBatches
    batch_size: usize,
    /// if the result is chunked into batches,
    /// last offset is preserved for continuation.
    row_group_skip_position: usize,
    indices: [Vec<(usize, usize, usize)>; 2],
}

fn aggr_state_schema(aggr_expr: &[Arc<dyn AggregateExpr>]) -> Result<SchemaRef> {
    let fields = aggr_expr
        .iter()
        .flat_map(|expr| expr.state_fields().unwrap().into_iter())
        .collect::<Vec<_>>();
    Ok(Arc::new(Schema::new(fields)))
}

fn is_supported(expr: &Arc<dyn AggregateExpr>, group_schema: &Schema) -> bool {
    expr.row_accumulator_supported() && row_supported(group_schema, RowType::Compact)
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
    ) -> Result<Self> {
        let timer = baseline_metrics.elapsed_compute().timer();

        let group_schema = group_schema(&schema, group_by.expr.len());
        let mut start_idx = group_by.expr.len();
        let mut row_aggr_expr = vec![];
        let mut row_agg_indices = vec![];
        let mut row_aggregate_expressions = vec![];
        let mut normal_aggr_expr = vec![];
        let mut normal_agg_indices = vec![];
        let mut normal_aggregate_expressions = vec![];
        // The expressions to evaluate the batch, one vec of expressions per aggregation.
        // Assume create_schema() always put group columns in front of aggr columns, we set
        // col_idx_base to group expression count.
        let all_aggregate_expressions =
            aggregates::aggregate_expressions(&aggr_expr, &mode, start_idx)?;
        for (idx, (expr, others)) in aggr_expr
            .iter()
            .zip(all_aggregate_expressions.into_iter())
            .enumerate()
        {
            let n_fields = match mode {
                AggregateMode::Partial => expr.state_fields()?.len(),
                _ => 1,
            };
            if is_supported(expr, &group_schema) {
                row_aggregate_expressions.push(others);
                row_agg_indices.push((idx, start_idx, start_idx + n_fields));
                row_aggr_expr.push(expr.clone());
            } else {
                normal_aggregate_expressions.push(others);
                normal_agg_indices.push((idx, start_idx, start_idx + n_fields));
                normal_aggr_expr.push(expr.clone());
            }
            start_idx += n_fields;
        }

        let row_accumulators = aggregates::create_row_accumulators(&row_aggr_expr)?;

        let row_aggr_schema = aggr_state_schema(&row_aggr_expr)?;

        let row_aggr_layout =
            Arc::new(RowLayout::new(&row_aggr_schema, RowType::WordAligned));

        let name = format!("GroupedHashAggregateStream[{}]", partition);
        let row_aggr_state = RowAggregationState {
            reservation: MemoryConsumer::new(name).register(context.memory_pool()),
            map: RawTable::with_capacity(0),
            group_states: Vec::with_capacity(0),
        };

        timer.done();

        let inner = GroupedHashAggregateStreamInner {
            schema: Arc::clone(&schema),
            mode,
            input,
            group_by,
            normal_aggr_expr,
            row_accumulators,
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
        };

        let stream = futures::stream::unfold(inner, |mut this| async move {
            let elapsed_compute = this.baseline_metrics.elapsed_compute();

            loop {
                let result: ArrowResult<Option<RecordBatch>> =
                    match this.input.next().await {
                        Some(Ok(batch)) => {
                            let timer = elapsed_compute.timer();
                            let result = group_aggregate_batch(
                                &this.mode,
                                &this.random_state,
                                &this.group_by,
                                &this.normal_aggr_expr,
                                &mut this.row_accumulators,
                                this.row_aggr_layout.clone(),
                                batch,
                                &mut this.row_aggr_state,
                                &this.normal_aggregate_expressions,
                                &this.row_aggregate_expressions,
                            );

                            timer.done();

                            // allocate memory
                            // This happens AFTER we actually used the memory, but simplifies the whole accounting and we are OK with
                            // overshooting a bit. Also this means we either store the whole record batch or not.
                            match result.and_then(|allocated| {
                                this.row_aggr_state.reservation.try_grow(allocated)
                            }) {
                                Ok(_) => continue,
                                Err(e) => Err(ArrowError::ExternalError(Box::new(e))),
                            }
                        }
                        Some(Err(e)) => Err(e),
                        None => {
                            let timer = this.baseline_metrics.elapsed_compute().timer();
                            let result = create_batch_from_map(
                                &this.mode,
                                &this.row_aggr_schema,
                                this.batch_size,
                                this.row_group_skip_position,
                                &mut this.row_aggr_state,
                                &mut this.row_accumulators,
                                &this.schema,
                                &this.indices,
                                this.group_by.expr.len(),
                            );

                            timer.done();
                            result
                        }
                    };

                this.row_group_skip_position += this.batch_size;
                match result {
                    Ok(Some(result)) => {
                        return Some((
                            Ok(result.record_output(&this.baseline_metrics)),
                            this,
                        ));
                    }
                    Ok(None) => return None,
                    Err(error) => return Some((Err(error), this)),
                }
            }
        });

        // seems like some consumers call this stream even after it returned `None`, so let's fuse the stream.
        let stream = stream.fuse();
        let stream = Box::pin(stream);

        Ok(Self { schema, stream })
    }
}

impl Stream for GroupedHashAggregateStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        this.stream.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for GroupedHashAggregateStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Perform group-by aggregation for the given [`RecordBatch`].
///
/// If successfull, this returns the additional number of bytes that were allocated during this process.
///
/// TODO: Make this a member function of [`GroupedHashAggregateStream`]
#[allow(clippy::too_many_arguments)]
fn group_aggregate_batch(
    mode: &AggregateMode,
    random_state: &RandomState,
    grouping_set: &PhysicalGroupBy,
    normal_aggr_expr: &[Arc<dyn AggregateExpr>],
    row_accumulators: &mut [AccumulatorItemV2],
    state_layout: Arc<RowLayout>,
    batch: RecordBatch,
    aggr_state: &mut RowAggregationState,
    normal_aggregate_expressions: &[Vec<Arc<dyn PhysicalExpr>>],
    row_aggregate_expressions: &[Vec<Arc<dyn PhysicalExpr>>],
) -> Result<usize> {
    // evaluate the grouping expressions
    let group_by_values = evaluate_group_by(grouping_set, &batch)?;
    let mut row_allocated = 0usize;
    // track memory allocations
    let mut normal_allocated = 0usize;
    let RowAggregationState {
        map: row_map,
        group_states: row_group_states,
        ..
    } = aggr_state;

    // evaluate the aggregation expressions.
    // We could evaluate them after the `take`, but since we need to evaluate all
    // of them anyways, it is more performant to do it while they are together.
    let row_aggr_input_values = evaluate_many(row_aggregate_expressions, &batch)?;
    let normal_aggr_input_values = evaluate_many(normal_aggregate_expressions, &batch)?;

    for group_values in &group_by_values {
        // 1.1 construct the key from the group values
        // 1.2 construct the mapping key if it does not exist
        // 1.3 add the row' index to `indices`

        // track which entries in `aggr_state` have rows in this batch to aggregate
        let mut groups_with_rows = vec![];

        // 1.1 Calculate the group keys for the group values
        let mut batch_hashes = vec![0; batch.num_rows()];
        create_hashes(group_values, random_state, &mut batch_hashes)?;

        for (row, hash) in batch_hashes.into_iter().enumerate() {
            let entry = row_map.get_mut(hash, |(_hash, group_idx)| {
                // verify that a group that we are inserting with hash is
                // actually the same key value as the group in
                // existing_idx  (aka group_values @ row)
                let group_state = &row_group_states[*group_idx];
                let group_by_values = get_at_row(group_values, row).unwrap();
                group_by_values
                    .iter()
                    .zip(group_state.group_by_values.iter())
                    .all(|(lhs, rhs)| lhs.eq(rhs))
            });

            match entry {
                // Existing entry for this group value
                Some((_hash, group_idx)) => {
                    let group_state = &mut row_group_states[*group_idx];

                    // 1.3
                    if group_state.indices.is_empty() {
                        groups_with_rows.push(*group_idx);
                    };

                    group_state
                        .indices
                        .push_accounted(row as u32, &mut row_allocated); // remember this row
                }
                //  1.2 Need to create new entry
                None => {
                    let group_by_values = get_at_row(group_values, row).unwrap();
                    let accumulator_set =
                        aggregates::create_accumulators(normal_aggr_expr)?;
                    // Add new entry to group_states and save newly created index
                    let group_state = RowGroupState {
                        group_by_values: group_by_values.into_boxed_slice(),
                        aggregation_buffer: vec![0; state_layout.fixed_part_width()],
                        accumulator_set,
                        indices: vec![row as u32], // 1.3
                    };
                    let group_idx = row_group_states.len();

                    // NOTE: do NOT include the `RowGroupState` struct size in here because this is captured by
                    // `group_states` (see allocation down below)
                    row_allocated += group_state
                        .group_by_values
                        .iter()
                        .map(|sv| sv.size())
                        .sum::<usize>()
                        + (std::mem::size_of::<u8>()
                            * group_state.aggregation_buffer.capacity())
                        + (std::mem::size_of::<u32>() * group_state.indices.capacity());

                    normal_allocated += (std::mem::size_of::<Box<dyn Accumulator>>()
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
                        &mut row_allocated,
                    );

                    row_group_states.push_accounted(group_state, &mut row_allocated);

                    groups_with_rows.push(group_idx);
                }
            };
        }

        // Collect all indices + offsets based on keys in this vec
        let mut batch_indices: UInt32Builder = UInt32Builder::with_capacity(0);
        let mut offsets = vec![0];
        let mut offset_so_far = 0;
        for group_idx in groups_with_rows.iter() {
            let indices = &row_group_states[*group_idx].indices;
            batch_indices.append_slice(indices);
            offset_so_far += indices.len();
            offsets.push(offset_so_far);
        }
        let batch_indices = batch_indices.finish();

        // `Take` all values based on indices into Arrays
        let row_values: Vec<Vec<Arc<dyn Array>>> = row_aggr_input_values
            .iter()
            .map(|array| {
                array
                    .iter()
                    .map(|array| {
                        compute::take(
                            array.as_ref(),
                            &batch_indices,
                            None, // None: no index check
                        )
                        .unwrap()
                    })
                    .collect()
                // 2.3
            })
            .collect();

        // `Take` all values based on indices into Arrays
        let normal_values: Vec<Vec<Arc<dyn Array>>> = normal_aggr_input_values
            .iter()
            .map(|array| {
                array
                    .iter()
                    .map(|array| {
                        compute::take(
                            array.as_ref(),
                            &batch_indices,
                            None, // None: no index check
                        )
                        .unwrap()
                    })
                    .collect()
                // 2.3
            })
            .collect();
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
                row_accumulators
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
                        let mut state_accessor =
                            RowAccessor::new_from_layout(state_layout.clone());
                        state_accessor
                            .point_to(0, group_state.aggregation_buffer.as_mut_slice());
                        match mode {
                            AggregateMode::Partial => {
                                accumulator.update_batch(&values, &mut state_accessor)
                            }
                            AggregateMode::FinalPartitioned | AggregateMode::Final => {
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
                        let res = match mode {
                            AggregateMode::Partial => accumulator.update_batch(&values),
                            AggregateMode::FinalPartitioned | AggregateMode::Final => {
                                // note: the aggregation here is over states, not values, thus the merge
                                accumulator.merge_batch(&values)
                            }
                        };
                        let size_post = accumulator.size();
                        normal_allocated += size_post.saturating_sub(size_pre);
                        res
                    })
                    // 2.5
                    .and({
                        group_state.indices.clear();
                        Ok(())
                    })?;

                Ok::<(), DataFusionError>(())
            })?;
    }

    Ok(row_allocated + normal_allocated)
}

/// The state that is built for each output group.
#[derive(Debug)]
pub struct RowGroupState {
    /// The actual group by values, stored sequentially
    pub group_by_values: Box<[ScalarValue]>,

    // Accumulator state, stored sequentially
    pub aggregation_buffer: Vec<u8>,

    // Accumulator state, one for each aggregate
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

/// Create a RecordBatch with all group keys and accumulator' states or values.
#[allow(clippy::too_many_arguments)]
fn create_batch_from_map(
    mode: &AggregateMode,
    aggr_schema: &Schema,
    batch_size: usize,
    skip_items: usize,
    row_aggr_state: &mut RowAggregationState,
    row_accumulators: &mut [AccumulatorItemV2],
    output_schema: &Schema,
    indices: &[Vec<(usize, usize, usize)>],
    num_group_expr: usize,
) -> ArrowResult<Option<RecordBatch>> {
    if skip_items > row_aggr_state.group_states.len() {
        return Ok(None);
    }
    if row_aggr_state.group_states.is_empty() {
        let schema = Arc::new(output_schema.to_owned());
        return Ok(Some(RecordBatch::new_empty(schema)));
    }

    let end_idx = min(skip_items + batch_size, row_aggr_state.group_states.len());
    let group_state_chunk = &row_aggr_state.group_states[skip_items..end_idx];

    if group_state_chunk.is_empty() {
        let schema = Arc::new(output_schema.to_owned());
        return Ok(Some(RecordBatch::new_empty(schema)));
    }

    let mut state_accessor = RowAccessor::new(aggr_schema, RowType::WordAligned);
    let mut state_buffers = group_state_chunk
        .iter()
        .map(|gs| gs.aggregation_buffer.clone())
        .collect::<Vec<_>>();

    let output_fields = output_schema.fields();
    let mut row_columns = match mode {
        AggregateMode::Partial => {
            read_as_batch(&state_buffers, aggr_schema, RowType::WordAligned)
        }
        AggregateMode::Final | AggregateMode::FinalPartitioned => {
            let mut results: Vec<Vec<ScalarValue>> = vec![vec![]; row_accumulators.len()];
            for buffer in state_buffers.iter_mut() {
                state_accessor.point_to(0, buffer);
                for (i, acc) in row_accumulators.iter().enumerate() {
                    results[i].push(acc.evaluate(&state_accessor)?);
                }
            }
            // We skip over the first `columns.len()` elements.
            //
            // This shouldn't panic if the `output_schema` has enough fields.
            let remaining_fields = output_fields[num_group_expr..].iter();
            results
                .into_iter()
                .zip(remaining_fields)
                .map(|(scalars, field)| {
                    if scalars.is_empty() {
                        Ok(arrow::array::new_empty_array(field.data_type()))
                    } else {
                        ScalarValue::iter_to_array(scalars)
                    }
                })
                .collect::<Result<Vec<_>>>()?
        }
    };

    // cast output if needed (e.g. for types like Dictionary where
    // the intermediate GroupByScalar type was not the same as the
    // output
    let mut start_idx = 0;
    for (_, begin, end) in indices[1].iter() {
        for field in &output_fields[*begin..*end] {
            row_columns[start_idx] = cast(&row_columns[start_idx], field.data_type())?;
            start_idx += 1;
        }
    }

    // Calculate number/shape of state arrays
    let accs = &row_aggr_state.group_states[0].accumulator_set;
    let acc_data_types = match mode {
        AggregateMode::Partial => accs
            .iter()
            .map(|a| a.state().map(|s| s.len()))
            .collect::<Result<Vec<_>>>()?,
        AggregateMode::Final | AggregateMode::FinalPartitioned => {
            vec![1; accs.len()]
        }
    };

    // next, output aggregates: either intermediate state or final output
    let mut columns = vec![];
    for (x, &state_len) in acc_data_types.iter().enumerate() {
        for y in 0..state_len {
            columns.push(match mode {
                AggregateMode::Partial => ScalarValue::iter_to_array(
                    group_state_chunk.iter().map(|row_group_state| {
                        row_group_state.accumulator_set[x]
                            .state()
                            .map(|x| x[y].clone())
                            .expect("unexpected accumulator state in hash aggregate")
                    }),
                ),
                AggregateMode::Final | AggregateMode::FinalPartitioned => {
                    ScalarValue::iter_to_array(group_state_chunk.iter().map(
                        |row_group_state| {
                            row_group_state.accumulator_set[x]
                                .evaluate()
                                .expect("unexpected accumulator state in hash aggregate")
                        },
                    ))
                }
            }?);
        }
    }
    // cast output if needed (e.g. for types like Dictionary where
    // the intermediate GroupByScalar type was not the same as the
    // output

    let mut start_idx = 0;
    for (_, begin, end) in indices[0].iter() {
        for field in &output_fields[*begin..*end] {
            columns[start_idx] = cast(&columns[start_idx], field.data_type())?;
            start_idx += 1;
        }
    }

    let mut res = (0..num_group_expr)
        .map(|idx| {
            ScalarValue::iter_to_array(
                group_state_chunk
                    .iter()
                    .map(|gs| gs.group_by_values[idx].clone()),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let extra: usize = indices
        .iter()
        .flatten()
        .map(|(_, begin, end)| end - begin)
        .sum();
    let empty_arr = new_null_array(&DataType::Null, 1);
    res.extend(std::iter::repeat(empty_arr).take(extra));

    let results = vec![columns, row_columns];
    for (outer, cur_res) in results.into_iter().enumerate() {
        let mut start_idx = 0;
        for (_, begin, end) in indices[outer].iter() {
            for elem in res.iter_mut().take(*end).skip(*begin) {
                *elem = cur_res[start_idx].clone();
                start_idx += 1;
            }
        }
    }
    Ok(Some(RecordBatch::try_new(
        Arc::new(output_schema.to_owned()),
        res,
    )?))
}

pub fn read_as_batch(
    rows: &[Vec<u8>],
    schema: &Schema,
    row_type: RowType,
) -> Vec<ArrayRef> {
    let row_num = rows.len();
    let mut output = MutableRecordBatch::new(row_num, Arc::new(schema.clone()));
    let mut row = RowReader::new(schema, row_type);

    for data in rows {
        row.point_to(0, data);
        read_row(&row, &mut output, schema);
    }

    output.output_as_columns()
}

fn get_at_row(grouping_set_values: &[ArrayRef], row: usize) -> Result<Vec<ScalarValue>> {
    // Copy group values out of arrays into `ScalarValue`s
    grouping_set_values
        .iter()
        .map(|col| ScalarValue::try_from_array(col, row))
        .collect::<Result<Vec<_>>>()
}
