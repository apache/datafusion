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

//! [`ScalarUDFImpl`] definitions for map_extract functions.

use arrow::array::ArrayRef;

use arrow::datatypes::{DataType, Float64Type, Int64Type, UInt32Type};
use arrow_array::{
    new_null_array, Array, ArrowPrimitiveType, MapArray, PrimitiveArray, StringArray,
};
use datafusion_common::cast::{as_primitive_array, as_string_array};
use datafusion_common::utils::get_map_entry_field;
use datafusion_common::{cast::as_map_array, exec_err, Result};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;

use crate::utils::make_scalar_function;

// Create static instances of ScalarUDFs for each function
make_udf_expr_and_func!(
    MapExtract,
    map_extract,
    map key,
    "Return a list containing the value for a given key or an empty list if the key is not contained in the map.",
    map_extract_udf
);

#[derive(Debug)]
pub(super) struct MapExtract {
    signature: Signature,
    aliases: Vec<String>,
}

impl MapExtract {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![String::from("element_at")],
        }
    }
}

impl ScalarUDFImpl for MapExtract {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "map_extract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return exec_err!("map_extract expects two arguments");
        }
        let map_type = &arg_types[0];
        let map_fields = get_map_entry_field(map_type)?;
        Ok(map_fields[1].data_type().clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(map_extract_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return exec_err!("map_extract expects two arguments");
        }

        let field = get_map_entry_field(&arg_types[0])?;
        Ok(vec![
            arg_types[0].clone(),
            field.first().unwrap().data_type().clone(),
        ])
    }
}

fn map_extract_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("map_extract expects two arguments");
    }

    let map_array = as_map_array(&args[0])?;

    if map_array.key_type() != args[1].data_type() {
        return exec_err!(
            "The key type {} does not match the map key type {}",
            args[1].data_type(),
            map_array.key_type()
        );
    }

    let key_type = map_array.key_type();
    if key_type.is_integer() {
        generic_map_extract_inner::<Int64Type>(
            map_array,
            as_primitive_array::<Int64Type>(map_array.keys())?,
            as_primitive_array::<Int64Type>(&args[1])?,
        )
    } else if key_type.is_floating() {
        generic_map_extract_inner::<Float64Type>(
            map_array,
            as_primitive_array::<Float64Type>(map_array.keys())?,
            as_primitive_array::<Float64Type>(&args[1])?,
        )
    } else if key_type.is_unsigned_integer() {
        generic_map_extract_inner::<UInt32Type>(
            map_array,
            as_primitive_array::<UInt32Type>(map_array.keys())?,
            as_primitive_array::<UInt32Type>(&args[1])?,
        )
    } else if key_type == &DataType::Utf8 {
        string_map_extract_inner(
            map_array,
            as_string_array(map_array.keys())?,
            as_string_array(&args[1])?,
        )
    } else {
        exec_err!("Unsupported key type: {:?}", args[1].data_type())
    }
}

fn generic_map_extract_inner<T: ArrowPrimitiveType>(
    map_array: &MapArray,
    keys_array: &PrimitiveArray<T>,
    query_keys_array: &PrimitiveArray<T>,
) -> Result<ArrayRef> {
    let query_key = query_keys_array.value(0);
    // key cannot be NULL, so we can unwrap
    let index = keys_array.iter().position(|key| key.unwrap() == query_key);

    match index {
        Some(idx) => Ok(map_array.values().slice(idx, 1)),
        None => Ok(new_null_array(map_array.value_type(), 1)),
    }
}

fn string_map_extract_inner(
    map_array: &MapArray,
    keys_array: &StringArray,
    query_keys_array: &StringArray,
) -> Result<ArrayRef> {
    let query_key = query_keys_array.value(0);
    // key cannot be NULL, so we can unwrap
    let index = keys_array.iter().position(|key| key.unwrap() == query_key);

    match index {
        Some(idx) => Ok(map_array.values().slice(idx, 1)),
        None => Ok(new_null_array(map_array.value_type(), 1)),
    }
}
