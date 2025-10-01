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
use arrow::datatypes::DataType;
use datafusion_common::utils::take_function_args;
use datafusion_common::Result;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_functions::utils::make_scalar_function;

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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [key_type, value_type] = take_function_args("map_from_arrays", arg_types)?;
        Ok(map_type_from_key_value_types(
            get_element_type(key_type)?,
            get_element_type(value_type)?,
        ))
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

    if matches!(keys.data_type(), DataType::Null)
        || matches!(values.data_type(), DataType::Null)
    {
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
