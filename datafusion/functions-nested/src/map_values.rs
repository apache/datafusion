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

//! [`ScalarUDFImpl`] definitions for map_values function.

use crate::utils::{get_map_entry_field, make_scalar_function};
use arrow_array::{Array, ArrayRef, ListArray};
use arrow_schema::{DataType, Field};
use datafusion_common::{cast::as_map_array, exec_err, Result};
use datafusion_expr::{
    ArrayFunctionSignature, ColumnarValue, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use std::any::Any;
use std::sync::Arc;

make_udf_expr_and_func!(
    MapValuesFunc,
    map_values,
    map,
    "Return a list of all values in the map.",
    map_values_udf
);

#[derive(Debug)]
pub(crate) struct MapValuesFunc {
    signature: Signature,
}

impl MapValuesFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::ArraySignature(ArrayFunctionSignature::MapArray),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for MapValuesFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "map_values"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        if arg_types.len() != 1 {
            return exec_err!("map_values expects single argument");
        }
        let map_type = &arg_types[0];
        let map_fields = get_map_entry_field(map_type)?;
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            map_fields.last().unwrap().data_type().clone(),
            true,
        ))))
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion_common::Result<ColumnarValue> {
        make_scalar_function(map_values_inner)(args)
    }
}

fn map_values_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("map_values expects single argument");
    }

    let map_array = match args[0].data_type() {
        DataType::Map(_, _) => as_map_array(&args[0])?,
        _ => return exec_err!("Argument for map_values should be a map"),
    };

    Ok(Arc::new(ListArray::new(
        Arc::new(Field::new("item", map_array.value_type().clone(), true)),
        map_array.offsets().clone(),
        Arc::clone(map_array.values()),
        None,
    )))
}
