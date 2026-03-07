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

use std::any::Any;

use crate::function::map::utils::{
    map_from_keys_values_offsets_nulls, map_type_from_key_value_types,
};
use arrow::array::{Array, ArrayRef, NullArray, new_null_array};
use arrow::buffer::OffsetBuffer;
use arrow::compute::concat;
use arrow::compute::kernels::cast;
use arrow::datatypes::DataType;
use datafusion_common::{Result, exec_err};
use datafusion_expr::type_coercion::binary::comparison_coercion;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_functions::utils::make_scalar_function;

/// Spark-compatible `map` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#map>
///
/// Creates a map from alternating key-value pairs.
/// Supports 0 or more pairs.
/// Example: map(key1, value1, key2, value2, ...) -> {key1: value1, key2: value2, ...}
/// Example: map() -> {}
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Map {
    signature: Signature,
}

impl Default for Map {
    fn default() -> Self {
        Self::new()
    }
}

impl Map {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Map {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "map"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if !arg_types.len().is_multiple_of(2) {
            return exec_err!(
                "map requires an even number of arguments, got {}",
                arg_types.len()
            );
        }

        if arg_types.is_empty() {
            return Ok(map_type_from_key_value_types(
                &DataType::Null,
                &DataType::Null,
            ));
        }

        let key_type = arg_types
            .iter()
            .step_by(2)
            .cloned()
            .reduce(|common, t| comparison_coercion(&common, &t).unwrap_or(common))
            .unwrap();
        let value_type = arg_types
            .iter()
            .skip(1)
            .step_by(2)
            .cloned()
            .reduce(|common, t| comparison_coercion(&common, &t).unwrap_or(common))
            .unwrap();

        Ok(map_type_from_key_value_types(&key_type, &value_type))
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        make_scalar_function(map_inner, vec![])(&args.args)
    }
}

fn map_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.is_empty() {
        let offsets = OffsetBuffer::new(vec![0].into());
        return map_from_keys_values_offsets_nulls(
            &new_null_array(&DataType::Null, 0),
            &new_null_array(&DataType::Null, 0),
            offsets.as_ref(),
            offsets.as_ref(),
            None,
            None,
        );
    }

    let num_rows = args[0].len();
    let num_pairs = args.len() / 2;

    let all_null = args
        .iter()
        .all(|arg| matches!(arg.data_type(), DataType::Null));

    if all_null {
        return Ok(cast(
            &NullArray::new(num_rows),
            &map_type_from_key_value_types(&DataType::Null, &DataType::Null),
        )?);
    }

    // Collect key arrays and value arrays from alternating arguments
    let key_arrays: Vec<&dyn Array> =
        (0..num_pairs).map(|i| args[i * 2].as_ref()).collect();
    let value_arrays: Vec<&dyn Array> =
        (0..num_pairs).map(|i| args[i * 2 + 1].as_ref()).collect();

    // Concatenate all keys and all values into flat arrays
    let flat_keys: ArrayRef = if key_arrays.is_empty() {
        new_null_array(args[0].data_type(), 0)
    } else {
        concat(&key_arrays)?
    };
    let flat_values: ArrayRef = if value_arrays.is_empty() {
        new_null_array(args[1].data_type(), 0)
    } else {
        concat(&value_arrays)?
    };

    // flat_keys layout: [row0_key0, row1_key0, ..., rowN_key0, row0_key1, row1_key1, ..., rowN_key1, ...]
    // But we need: [row0_key0, row0_key1, ..., row0_keyM, row1_key0, row1_key1, ..., row1_keyM, ...]
    // Rearrange for each row, gather keys from each pair.
    // Source index for (row, pair) is (pair * num_rows + row)
    let total_entries = num_rows * num_pairs;
    let mut reorder_indices = Vec::with_capacity(total_entries);
    for row in 0..num_rows {
        for pair in 0..num_pairs {
            reorder_indices.push((pair * num_rows + row) as u32);
        }
    }

    let indices = arrow::array::UInt32Array::from(reorder_indices);
    let flat_keys = arrow::compute::take(&flat_keys, &indices, None)?;
    let flat_values = arrow::compute::take(&flat_values, &indices, None)?;

    let offsets: Vec<i32> = (0..=num_rows as i32)
        .map(|i| i * num_pairs as i32)
        .collect();
    let offsets_buffer = OffsetBuffer::new(offsets.into());

    map_from_keys_values_offsets_nulls(
        &flat_keys,
        &flat_values,
        offsets_buffer.as_ref(),
        offsets_buffer.as_ref(),
        None,
        None,
    )
}
