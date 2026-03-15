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
    get_element_type, get_list_offsets, get_list_values,
    map_from_keys_values_offsets_nulls, map_type_from_key_value_types,
};
use arrow::array::{Array, ArrayRef, NullArray};
use arrow::compute::kernels::cast;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions::utils::make_scalar_function;
use std::sync::Arc;

/// Spark-compatible `map_from_arrays` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#map_from_arrays>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MapFromArrays {
    signature: Signature,
}

impl Default for MapFromArrays {
    fn default() -> Self {
        Self::new()
    }
}

impl MapFromArrays {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MapFromArrays {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "map_from_arrays"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [keys_field, values_field] = args.arg_fields else {
            return internal_err!("map_from_arrays expects exactly 2 arguments");
        };

        let map_type = map_type_from_key_value_types(
            get_element_type(keys_field.data_type())?,
            get_element_type(values_field.data_type())?,
        );
        // Spark marks map_from_arrays as null intolerant, so the output is
        // nullable if either input is nullable.
        let nullable = keys_field.is_nullable() || values_field.is_nullable();
        Ok(Arc::new(Field::new(self.name(), map_type, nullable)))
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        make_scalar_function(map_from_arrays_inner, vec![])(&args.args)
    }
}

fn map_from_arrays_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [keys, values] = take_function_args("map_from_arrays", args)?;

    if *keys.data_type() == DataType::Null || *values.data_type() == DataType::Null {
        return Ok(cast(
            &NullArray::new(keys.len()),
            &map_type_from_key_value_types(
                get_element_type(keys.data_type())?,
                get_element_type(values.data_type())?,
            ),
        )?);
    }

    map_from_keys_values_offsets_nulls(
        get_list_values(keys)?,
        get_list_values(values)?,
        &get_list_offsets(keys)?,
        &get_list_offsets(values)?,
        keys.nulls(),
        values.nulls(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;
    use datafusion_expr::ReturnFieldArgs;

    #[test]
    fn test_map_from_arrays_nullability_and_type() {
        let func = MapFromArrays::new();

        let keys_field: FieldRef = Arc::new(Field::new(
            "keys",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
            false,
        ));
        let values_field: FieldRef = Arc::new(Field::new(
            "values",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ));

        let out = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&keys_field), Arc::clone(&values_field)],
                scalar_arguments: &[None, None],
            })
            .expect("return_field_from_args should succeed");

        let expected_type =
            map_type_from_key_value_types(&DataType::Int32, &DataType::Utf8);
        assert_eq!(out.data_type(), &expected_type);
        assert!(
            !out.is_nullable(),
            "map_from_arrays should be non-nullable when both inputs are non-nullable"
        );

        let nullable_keys: FieldRef = Arc::new(Field::new(
            "keys",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
            true,
        ));

        let out_nullable = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[nullable_keys, values_field],
                scalar_arguments: &[None, None],
            })
            .expect("return_field_from_args should succeed");

        assert!(
            out_nullable.is_nullable(),
            "map_from_arrays should be nullable when any input is nullable"
        );
    }
}
