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

use crate::physical_plan::aggregates::AccumulatorItem;
use arrow::compute;
use arrow::compute::filter;
use arrow::row::OwnedRow;
use arrow_array::types::UInt32Type;
use arrow_array::{Array, ArrayRef, BooleanArray, PrimitiveArray};
use arrow_schema::{Schema, SchemaRef};
use datafusion_common::cast::as_boolean_array;
use datafusion_common::utils::get_arrayref_at_indices;
use datafusion_common::{Result, ScalarValue};
use datafusion_physical_expr::AggregateExpr;
use datafusion_row::reader::{read_row, RowReader};
use datafusion_row::MutableRecordBatch;
use std::sync::Arc;

/// The state that is built for each output group.
#[derive(Debug)]
pub(crate) struct GroupState {
    /// The actual group by values, stored sequentially
    pub group_by_values: OwnedRow,

    // Accumulator state, stored sequentially
    pub aggregation_buffer: Vec<u8>,

    // Accumulator state, one for each aggregate that doesn't support row accumulation
    pub accumulator_set: Vec<AccumulatorItem>,

    /// scratch space used to collect indices for input rows in a
    /// bach that have values to aggregate. Reset on each batch
    pub indices: Vec<u32>,
}

#[derive(Debug)]
/// tracks what phase the aggregation is in
pub(crate) enum ExecutionState {
    ReadingInput,
    ProducingOutput,
    Done,
}

pub(crate) fn aggr_state_schema(
    aggr_expr: &[Arc<dyn AggregateExpr>],
) -> Result<SchemaRef> {
    let fields = aggr_expr
        .iter()
        .flat_map(|expr| expr.state_fields().unwrap().into_iter())
        .collect::<Vec<_>>();
    Ok(Arc::new(Schema::new(fields)))
}

pub(crate) fn read_as_batch(rows: &[Vec<u8>], schema: &Schema) -> Vec<ArrayRef> {
    let row_num = rows.len();
    let mut output = MutableRecordBatch::new(row_num, Arc::new(schema.clone()));
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
    let sliced_arrays: Vec<ArrayRef> = aggr_array
        .iter()
        .map(|array| array.slice(offsets[0], offsets[1] - offsets[0]))
        .collect();

    let filtered_arrays = match filter_opt.as_ref() {
        Some(f) => {
            let sliced = f.slice(offsets[0], offsets[1] - offsets[0]);
            let filter_array = as_boolean_array(&sliced)?;

            sliced_arrays
                .iter()
                .map(|array| filter(array, filter_array).unwrap())
                .collect::<Vec<ArrayRef>>()
        }
        None => sliced_arrays,
    };
    Ok(filtered_arrays)
}

/// This method is similar to Scalar::try_from_array except for the Null handling.
/// This method returns [ScalarValue::Null] instead of [ScalarValue::Type(None)]
pub(crate) fn col_to_scalar(
    array: &ArrayRef,
    filter: &Option<&BooleanArray>,
    row_index: usize,
) -> Result<ScalarValue> {
    if array.is_null(row_index) {
        return Ok(ScalarValue::Null);
    }
    if let Some(filter) = filter {
        if !filter.value(row_index) {
            return Ok(ScalarValue::Null);
        }
    }
    ScalarValue::try_from_array(array, row_index)
}
