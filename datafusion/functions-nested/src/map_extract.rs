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

use arrow::array::{ArrayRef, Capacities, MutableArrayData};
use arrow_array::{make_array, ListArray};

use arrow::datatypes::DataType;
use arrow_array::{Array, MapArray};
use arrow_buffer::OffsetBuffer;
use arrow_schema::Field;
use datafusion_common::utils::get_map_entry_field;

use datafusion_common::{cast::as_map_array, exec_err, Result};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;
use std::vec;

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
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            map_fields.last().unwrap().data_type().clone(),
            true,
        ))))
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

fn general_map_extract_inner(
    map_array: &MapArray,
    query_keys_array: &dyn Array,
) -> Result<ArrayRef> {
    let keys = map_array.keys();
    let mut offsets = vec![0_i32];

    let values = map_array.values();
    let original_data = values.to_data();
    let capacity = Capacities::Array(original_data.len());

    let mut mutable =
        MutableArrayData::with_capacities(vec![&original_data], true, capacity);

    for (row_index, offset_window) in map_array.value_offsets().windows(2).enumerate() {
        let start = offset_window[0] as usize;
        let end = offset_window[1] as usize;
        let len = end - start;

        let query_key = query_keys_array.slice(row_index, 1);

        let value_index =
            (0..len).find(|&i| keys.slice(start + i, 1).as_ref() == query_key.as_ref());

        match value_index {
            Some(index) => {
                mutable.extend(0, start + index, start + index + 1);
            }
            None => {
                mutable.extend_nulls(1);
            }
        }
        offsets.push(offsets[row_index] + 1);
    }

    let data = mutable.freeze();

    Ok(Arc::new(ListArray::new(
        Arc::new(Field::new("item", map_array.value_type().clone(), true)),
        OffsetBuffer::<i32>::new(offsets.into()),
        Arc::new(make_array(data)),
        None,
    )))
}

fn map_extract_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("map_extract expects two arguments");
    }

    let map_array = match args[0].data_type() {
        DataType::Map(_, _) => as_map_array(&args[0])?,
        _ => return exec_err!("The first argument in map_extract must be a map"),
    };

    let key_type = map_array.key_type();

    if key_type != args[1].data_type() {
        return exec_err!(
            "The key type {} does not match the map key type {}",
            args[1].data_type(),
            key_type
        );
    }

    general_map_extract_inner(map_array, &args[1])
}
