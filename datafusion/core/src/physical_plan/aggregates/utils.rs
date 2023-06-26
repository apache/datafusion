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

//! This file contains various utility functions that are common to both
//! batch and streaming aggregation code.

use crate::physical_plan::aggregates::row_agg_macros::*;
use crate::physical_plan::aggregates::AccumulatorItem;
use arrow::array::*;
use arrow::compute;
use arrow::compute::filter;
use arrow::datatypes::DataType;
use arrow::row::OwnedRow;
use arrow_array::types::UInt32Type;
use arrow_array::{Array, ArrayRef, BooleanArray, PrimitiveArray};
use arrow_schema::{Schema, SchemaRef};
use datafusion_common::cast::as_boolean_array;
use datafusion_common::utils::get_arrayref_at_indices;
use datafusion_common::{downcast_value, DataFusionError, Result};
use datafusion_physical_expr::aggregate::row_accumulator::RowAccumulatorItem;
use datafusion_physical_expr::AggregateExpr;
use datafusion_row::accessor::{ArrowArrayReader, RowAccessor};
use datafusion_row::layout::RowLayout;
use datafusion_row::reader::{read_row, RowReader};
use datafusion_row::MutableRecordBatch;
use itertools::izip;
use std::sync::Arc;

/// This object encapsulates the state that is built for each output group.
#[derive(Debug)]
pub(crate) struct GroupState {
    /// The actual group by values, stored sequentially
    pub group_by_values: OwnedRow,

    // Accumulator state, stored sequentially
    pub aggregation_buffer: Vec<u8>,

    // Accumulator state, one for each aggregate that doesn't support row accumulation
    pub accumulator_set: Vec<AccumulatorItem>,

    /// Scratch space used to collect indices for input rows in a
    /// batch that have values to aggregate, reset on each batch.
    pub indices: Vec<usize>,
}

#[derive(Debug)]
/// This object tracks the aggregation phase.
pub(crate) enum ExecutionState {
    ReadingInput,
    ProducingOutput,
    Done,
}

/// A helper trait used for GroupState row accumulators update.
pub(crate) trait GroupStateRowAccumulatorsUpdater {
    fn get_row_accumulator(&self, acc_idx: usize) -> *const RowAccumulatorItem;
    fn get_row_accumulators(&self) -> &[RowAccumulatorItem];
    fn get_group_state(&self, group_idx: &usize) -> &GroupState;
    fn get_mut_group_state(&mut self, group_idx: &usize) -> &mut GroupState;
    fn get_mut_group_state_and_row_accumulators(
        &mut self,
        group_idx: &usize,
    ) -> (&mut GroupState, &[RowAccumulatorItem]);

    fn update_row_accumulators(
        &mut self,
        groups_with_rows: &[usize],
        row_values: &[Vec<ArrayRef>],
        row_filter_values: &[Option<ArrayRef>],
        row_layout: Arc<RowLayout>,
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
        self.get_row_accumulators()
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

        if single_value_acc_idx.len() == 1 && single_row_acc_idx.is_empty() {
            let acc_idx1 = single_value_acc_idx[0];
            let array1 = &row_values[acc_idx1][0];
            let array1_dt = array1.data_type();
            dispatch_all_supported_data_types! { impl_one_row_accumulator_dispatch, array1_dt, array1, acc_idx1, self, update_one_accumulator_with_native_value, groups_with_rows, filter_bool_array, row_layout}
        } else if single_value_acc_idx.len() == 2 && single_row_acc_idx.is_empty() {
            let acc_idx1 = single_value_acc_idx[0];
            let acc_idx2 = single_value_acc_idx[1];
            let array1 = &row_values[acc_idx1][0];
            let array2 = &row_values[acc_idx2][0];
            let array1_dt = array1.data_type();
            let array2_dt = array2.data_type();
            dispatch_all_supported_data_types! { dispatch_all_supported_data_types_pairs, impl_two_row_accumulators_dispatch, array1_dt, array2_dt, array1, array2, acc_idx1, acc_idx2, self, update_two_accumulator2_with_native_value, groups_with_rows, filter_bool_array, row_layout}
        } else {
            for group_idx in groups_with_rows {
                let (group_state, accumulators) =
                    self.get_mut_group_state_and_row_accumulators(group_idx);
                let mut state_accessor = RowAccessor::new_from_layout(row_layout.clone());
                state_accessor.point_to(0, group_state.aggregation_buffer.as_mut_slice());
                for (accumulator, values_array, filter_array) in izip!(
                    accumulators.iter(),
                    row_values.iter(),
                    filter_bool_array.iter()
                ) {
                    accumulator.update_row_indices(
                        values_array,
                        filter_array,
                        &group_state.indices,
                        &mut state_accessor,
                    )?;
                }

                // clear the group indices in this group
                group_state.indices.clear();
            }
        }

        Ok(())
    }

    fn update_one_accumulator_with_native_value<T1>(
        &mut self,
        groups_with_rows: &[usize],
        agg_input_array1: &T1,
        acc_idx1: usize,
        filter_bool_array: &[Option<&BooleanArray>],
        row_layout: Arc<RowLayout>,
    ) -> Result<()>
    where
        T1: ArrowArrayReader,
    {
        let acc_ptr1 = self.get_row_accumulator(acc_idx1);
        let filter_array1 = &filter_bool_array[acc_idx1];
        for group_idx in groups_with_rows {
            let group_state = self.get_mut_group_state(group_idx);
            let mut state_accessor = RowAccessor::new_from_layout(row_layout.clone());
            state_accessor.point_to(0, group_state.aggregation_buffer.as_mut_slice());
            for idx in &group_state.indices {
                let value = col_to_value(agg_input_array1, filter_array1, *idx);
                unsafe {
                    (*acc_ptr1).update_value::<T1::Item>(value, &mut state_accessor);
                }
            }
            // clear the group indices in this group
            group_state.indices.clear();
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn update_two_accumulator2_with_native_value<T1, T2>(
        &mut self,
        groups_with_rows: &[usize],
        agg_input_array1: &T1,
        agg_input_array2: &T2,
        acc_idx1: usize,
        acc_idx2: usize,
        filter_bool_array: &[Option<&BooleanArray>],
        row_layout: Arc<RowLayout>,
    ) -> Result<()>
    where
        T1: ArrowArrayReader,
        T2: ArrowArrayReader,
    {
        let acc_ptr1 = self.get_row_accumulator(acc_idx1);
        let acc_ptr2 = self.get_row_accumulator(acc_idx2);
        let filter_array1 = &filter_bool_array[acc_idx1];
        let filter_array2 = &filter_bool_array[acc_idx2];
        for group_idx in groups_with_rows {
            let group_state = self.get_mut_group_state(group_idx);
            let mut state_accessor = RowAccessor::new_from_layout(row_layout.clone());
            state_accessor.point_to(0, group_state.aggregation_buffer.as_mut_slice());
            for idx in &group_state.indices {
                let value1 = col_to_value(agg_input_array1, filter_array1, *idx);
                let value2 = col_to_value(agg_input_array2, filter_array2, *idx);
                unsafe {
                    (*acc_ptr1).update_value::<T1::Item>(value1, &mut state_accessor);
                    (*acc_ptr2).update_value::<T2::Item>(value2, &mut state_accessor);
                }
            }
            // clear the group indices in this group
            group_state.indices.clear();
        }

        Ok(())
    }
}

pub(crate) fn aggr_state_schema(aggr_expr: &[Arc<dyn AggregateExpr>]) -> SchemaRef {
    let fields = aggr_expr
        .iter()
        .flat_map(|expr| expr.state_fields().unwrap().into_iter())
        .collect::<Vec<_>>();
    Arc::new(Schema::new(fields))
}

pub(crate) fn read_as_batch(rows: &[Vec<u8>], schema: &Schema) -> Vec<ArrayRef> {
    let mut output = MutableRecordBatch::new(rows.len(), Arc::new(schema.clone()));
    let mut row = RowReader::new(schema);

    for data in rows {
        row.point_to(0, data);
        read_row(&row, &mut output, schema);
    }

    output.output_as_columns()
}

pub(crate) fn get_at_indices(
    input_values: &[Vec<ArrayRef>],
    batch_indices: &PrimitiveArray<UInt32Type>,
) -> Result<Vec<Vec<ArrayRef>>> {
    input_values
        .iter()
        .map(|array| get_arrayref_at_indices(array, batch_indices))
        .collect()
}

pub(crate) fn get_optional_filters(
    original_values: &[Option<Arc<dyn Array>>],
    batch_indices: &PrimitiveArray<UInt32Type>,
) -> Vec<Option<Arc<dyn Array>>> {
    original_values
        .iter()
        .map(|array| {
            array.as_ref().map(|array| {
                compute::take(
                    array.as_ref(),
                    batch_indices,
                    None, // None: no index check
                )
                .unwrap()
            })
        })
        .collect()
}

pub(crate) fn slice_and_maybe_filter(
    aggr_array: &[ArrayRef],
    filter_opt: Option<&Arc<dyn Array>>,
    offsets: &[usize],
) -> Result<Vec<ArrayRef>> {
    let (offset, length) = (offsets[0], offsets[1] - offsets[0]);
    let sliced_arrays: Vec<ArrayRef> = aggr_array
        .iter()
        .map(|array| array.slice(offset, length))
        .collect();

    if let Some(f) = filter_opt {
        let sliced = f.slice(offset, length);
        let filter_array = as_boolean_array(&sliced)?;

        sliced_arrays
            .iter()
            .map(|array| filter(array, filter_array).map_err(DataFusionError::ArrowError))
            .collect()
    } else {
        Ok(sliced_arrays)
    }
}

pub(crate) fn col_to_value<T1: ArrowArrayReader>(
    array: &T1,
    filter: &Option<&BooleanArray>,
    row_index: usize,
) -> Option<T1::Item> {
    if array.is_null(row_index) {
        return None;
    }
    if let Some(filter) = filter {
        if !filter.value(row_index) {
            return None;
        }
    }
    Some(array.value_at(row_index))
}
