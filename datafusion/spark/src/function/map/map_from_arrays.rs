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
use std::borrow::Cow;

use crate::function::map::utils::{
    map_from_keys_values_offsets_nulls, map_type_from_key_value_types,
};
use arrow::array::{Array, ArrayRef, AsArray, NullArray};
use arrow::compute::kernels::cast;
use arrow::datatypes::DataType;
use datafusion_common::utils::take_function_args;
use datafusion_common::{exec_err, internal_err, Result};
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

fn get_element_type(data_type: &DataType) -> Result<&DataType> {
    match data_type {
        DataType::Null => Ok(data_type),
        DataType::List(element)
        | DataType::LargeList(element)
        | DataType::FixedSizeList(element, _) => Ok(element.data_type()),
        _ => exec_err!(
            "map_from_arrays expects 2 listarrays for keys and values as arguments, got {data_type:?}"
        ),
    }
}

fn get_list_values(array: &ArrayRef) -> Result<&ArrayRef> {
    match array.data_type() {
        DataType::Null => Ok(array),
        DataType::List(_) => Ok(array.as_list::<i32>().values()),
        DataType::LargeList(_) => Ok(array.as_list::<i64>().values()),
        DataType::FixedSizeList(..) => Ok(array.as_fixed_size_list().values()),
        wrong_type => internal_err!(
            "get_list_values expects List/LargeList/FixedSizeList as argument, got {wrong_type:?}"
        ),
    }
}

fn get_list_offsets(array: &ArrayRef) -> Result<Cow<'_, [i32]>> {
    match array.data_type() {
        DataType::List(_) => Ok(Cow::Borrowed(array.as_list::<i32>().offsets().as_ref())),
        DataType::LargeList(_) => Ok(Cow::Owned(
            array.as_list::<i64>()
                .offsets()
                .iter()
                .map(|i| *i as i32)
                .collect::<Vec<_>>(),
        )),
        DataType::FixedSizeList(_, size) => Ok(Cow::Owned(
             (0..=array.len() as i32).map(|i| size * i).collect()
        )),
        wrong_type => internal_err!(
            "map_from_arrays expects List/LargeList/FixedSizeList as first argument, got {wrong_type:?}"
        ),
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
