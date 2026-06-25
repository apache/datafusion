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

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, new_null_array};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::utils::SingleRowListArrayBuilder;
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_functions_nested::make_array::{array_array, coerce_types_inner};

use crate::function::functions_nested_utils::make_scalar_function;

const ARRAY_FIELD_DEFAULT_NAME: &str = "element";

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkArray {
    signature: Signature,
}

impl Default for SparkArray {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArray {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkArray {
    fn name(&self) -> &str {
        "array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let data_types = args
            .arg_fields
            .iter()
            .map(|f| f.data_type())
            .cloned()
            .collect::<Vec<_>>();

        let mut expr_type = DataType::Null;
        for arg_type in &data_types {
            if !arg_type.equals_datatype(&DataType::Null) {
                expr_type = arg_type.clone();
                break;
            }
        }

        let return_type = DataType::List(Arc::new(Field::new(
            ARRAY_FIELD_DEFAULT_NAME,
            expr_type,
            true,
        )));

        Ok(Arc::new(Field::new(
            "this_field_name_is_irrelevant",
            return_type,
            false,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(make_array_inner)(args.as_slice())
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() {
            Ok(vec![])
        } else {
            coerce_types_inner(arg_types, self.name())
        }
    }
}

/// `make_array_inner` is the implementation of the `make_array` function.
/// Constructs an array using the input `data` as `ArrayRef`.
/// Returns a reference-counted `Array` instance result.
pub fn make_array_inner(arrays: &[ArrayRef]) -> Result<ArrayRef> {
    // Zero arguments are the only case that should build a scalar empty list.
    if arrays.is_empty() {
        let array = new_null_array(&DataType::Null, 0);
        Ok(Arc::new(
            SingleRowListArrayBuilder::new(array)
                .with_field_name(Some(ARRAY_FIELD_DEFAULT_NAME.to_string()))
                .build_list_array(),
        ))
    } else {
        // All-null inputs still need to flow through `array_array()` so rows
        // are built per input row instead of collapsing to one value.
        let data_type = arrays
            .iter()
            .find_map(|arg| {
                let arg_type = arg.data_type();
                (!arg_type.is_null()).then_some(arg_type)
            })
            .unwrap_or(&DataType::Null);
        array_array::<i32>(arrays, data_type.clone(), ARRAY_FIELD_DEFAULT_NAME)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ListArray, NullArray};
    use arrow::datatypes::DataType;

    #[test]
    fn spark_array_inner_all_null_arrays_preserves_row_count_and_width() {
        let inputs = vec![
            Arc::new(NullArray::new(3)) as ArrayRef,
            Arc::new(NullArray::new(3)) as ArrayRef,
        ];

        let result = make_array_inner(&inputs).unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(list.len(), 3);
        assert_eq!(list.value_type(), DataType::Null);
        assert_eq!(list.values().len(), 6);

        for row in 0..list.len() {
            assert_eq!(list.value_length(row), 2);
            let values = list.value(row);
            assert_eq!(values.len(), 2);
            assert_eq!(values.logical_null_count(), 2);
        }
    }
}
