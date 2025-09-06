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

use arrow::array::{Array, ArrayRef, BooleanArray, UInt32Array};
use arrow::compute::take as arrow_take;
use arrow::datatypes::DataType;
use datafusion_common::{
    arrow_datafusion_err, exec_datafusion_err, DataFusionError, Result, ScalarValue,
};

use super::kinds::MRShiftKind;

pub(super) fn get_classifier_mask(values: &[ArrayRef]) -> Result<&BooleanArray> {
    // The classifier mask is the last argument when present.
    // The caller ensures it is only invoked when the mask exists.
    values
        .last()
        .and_then(|a| a.as_any().downcast_ref::<BooleanArray>())
        .ok_or_else(|| {
            exec_datafusion_err!("classifier mask column must be a BooleanArray")
        })
}

pub(super) fn create_default_value(data_type: &DataType) -> Result<ScalarValue> {
    ScalarValue::try_from(data_type)
}

pub(super) fn resolve_default(
    default: &ScalarValue,
    data_type: &DataType,
) -> Result<ScalarValue> {
    if default.is_null() {
        create_default_value(data_type)
    } else {
        // Ensure provided default matches the input data type to avoid
        // producing mixed ScalarValue variants (e.g., Int64 defaults with Int32 data),
        // which would later cause ScalarValue::iter_to_array type inconsistencies.
        default.cast_to(data_type)
    }
}

#[inline]
pub(super) fn compute_target_index(
    kind: MRShiftKind,
    base: usize,
    offset: usize,
    upper_bound: usize,
) -> Option<usize> {
    match kind {
        MRShiftKind::Prev => base.checked_sub(offset),
        MRShiftKind::Next => {
            let t = base + offset;
            (t < upper_bound).then_some(t)
        }
    }
}

pub(super) fn gather_array_by_indices(
    data_arr: &ArrayRef,
    indices: Vec<Option<usize>>,
) -> Result<ArrayRef> {
    let idx_u32: Vec<Option<u32>> = indices
        .into_iter()
        .map(|opt| opt.map(|v| v as u32))
        .collect();
    let idx_array = UInt32Array::from(idx_u32);
    arrow_take(data_arr.as_ref(), &idx_array, None).map_err(|e| arrow_datafusion_err!(e))
}

pub(super) fn gather_with_default(
    data_arr: &ArrayRef,
    indices: Vec<Option<usize>>,
    default_mask: Vec<bool>,
    default_value: &ScalarValue,
) -> Result<ArrayRef> {
    let gathered = gather_array_by_indices(data_arr, indices)?;
    if default_value.is_null() {
        return Ok(gathered);
    }
    if !default_mask.iter().any(|b| *b) {
        return Ok(gathered);
    }

    let default_arr = default_value.to_array_of_size(gathered.len())?;
    let mask_array = BooleanArray::from(default_mask);
    arrow::compute::kernels::zip::zip(&mask_array, &default_arr, &gathered)
        .map_err(|e| arrow_datafusion_err!(e))
}
