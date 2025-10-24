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

use crate::function::functions_nested_utils::make_scalar_function;
use arrow::array::{
    Array, ArrayRef, Capacities, FixedSizeListArray, GenericListArray, MutableArrayData,
    OffsetSizeTrait,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::DataType::{FixedSizeList, LargeList, List, Null};
use arrow::datatypes::{DataType, FieldRef};
use datafusion_common::cast::{
    as_fixed_size_list_array, as_large_list_array, as_list_array,
};
use datafusion_common::{exec_err, utils::take_function_args, Result};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use rand::rng;
use rand::seq::SliceRandom;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkShuffle {
    signature: Signature,
}

impl Default for SparkShuffle {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkShuffle {
    pub fn new() -> Self {
        Self {
            signature: Signature::arrays(1, None, Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for SparkShuffle {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "shuffle"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        make_scalar_function(array_shuffle_inner)(&args.args)
    }
}

/// array_shuffle SQL function
pub fn array_shuffle_inner(arg: &[ArrayRef]) -> Result<ArrayRef> {
    let [input_array] = take_function_args("shuffle", arg)?;
    match &input_array.data_type() {
        List(field) => {
            let array = as_list_array(input_array)?;
            general_array_shuffle::<i32>(array, field)
        }
        LargeList(field) => {
            let array = as_large_list_array(input_array)?;
            general_array_shuffle::<i64>(array, field)
        }
        FixedSizeList(field, _) => {
            let array = as_fixed_size_list_array(input_array)?;
            fixed_size_array_shuffle(array, field)
        }
        Null => Ok(Arc::clone(input_array)),
        array_type => exec_err!("shuffle does not support type '{array_type}'."),
    }
}

fn general_array_shuffle<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    field: &FieldRef,
) -> Result<ArrayRef> {
    let values = array.values();
    let original_data = values.to_data();
    let capacity = Capacities::Array(original_data.len());
    let mut offsets = vec![O::usize_as(0)];
    let mut nulls = vec![];
    let mut mutable =
        MutableArrayData::with_capacities(vec![&original_data], false, capacity);
    let mut rng = rng();

    for (row_index, offset_window) in array.offsets().windows(2).enumerate() {
        // skip the null value
        if array.is_null(row_index) {
            nulls.push(false);
            offsets.push(offsets[row_index] + O::one());
            mutable.extend(0, 0, 1);
            continue;
        }
        nulls.push(true);
        let start = offset_window[0];
        let end = offset_window[1];
        let length = (end - start).to_usize().unwrap();

        // Create indices and shuffle them
        let mut indices: Vec<usize> =
            (start.to_usize().unwrap()..end.to_usize().unwrap()).collect();
        indices.shuffle(&mut rng);

        // Add shuffled elements
        for &index in &indices {
            mutable.extend(0, index, index + 1);
        }

        offsets.push(offsets[row_index] + O::usize_as(length));
    }

    let data = mutable.freeze();
    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::clone(field),
        OffsetBuffer::<O>::new(offsets.into()),
        arrow::array::make_array(data),
        Some(nulls.into()),
    )?))
}

fn fixed_size_array_shuffle(
    array: &FixedSizeListArray,
    field: &FieldRef,
) -> Result<ArrayRef> {
    let values = array.values();
    let original_data = values.to_data();
    let capacity = Capacities::Array(original_data.len());
    let mut nulls = vec![];
    let mut mutable =
        MutableArrayData::with_capacities(vec![&original_data], false, capacity);
    let value_length = array.value_length() as usize;
    let mut rng = rng();

    for row_index in 0..array.len() {
        // skip the null value
        if array.is_null(row_index) {
            nulls.push(false);
            mutable.extend(0, 0, value_length);
            continue;
        }
        nulls.push(true);

        let start = row_index * value_length;
        let end = start + value_length;

        // Create indices and shuffle them
        let mut indices: Vec<usize> = (start..end).collect();
        indices.shuffle(&mut rng);

        // Add shuffled elements
        for &index in &indices {
            mutable.extend(0, index, index + 1);
        }
    }

    let data = mutable.freeze();
    Ok(Arc::new(FixedSizeListArray::try_new(
        Arc::clone(field),
        array.value_length(),
        arrow::array::make_array(data),
        Some(nulls.into()),
    )?))
}
