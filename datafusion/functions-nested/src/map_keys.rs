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

//! [`ScalarUDFImpl`] definitions for map_keys function.

use crate::utils::{get_map_entry_field, make_scalar_function};
use arrow_array::{Array, ArrayRef, ListArray};
use arrow_schema::{DataType, Field};
use datafusion_common::{cast::as_map_array, exec_err, Result};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

make_udf_expr_and_func!(
    MapKeysFunc,
    map_keys,
    map,
    "Return a list of all keys in the map.",
    map_keys_udf
);

#[derive(Debug)]
pub(crate) struct MapKeysFunc {
    signature: Signature,
}

impl MapKeysFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MapKeysFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "map_keys"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        if arg_types.len() != 1 {
            return exec_err!("map_keys expects single argument");
        }
        let map_type = &arg_types[0];
        let map_fields = get_map_entry_field(map_type)?;
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            map_fields.first().unwrap().data_type().to_owned(),
            false,
        ))))
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion_common::Result<ColumnarValue> {
        make_scalar_function(map_keys_inner)(args)
    }

    fn coerce_types(
        &self,
        arg_types: &[DataType],
    ) -> datafusion_common::Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return exec_err!("map_keys expects single argument");
        }
        Ok(vec![arg_types[0].to_owned()])
    }
}

fn map_keys_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("map_keys expects single argument");
    }

    let map_array = match args[0].data_type() {
        DataType::Map(_, _) => as_map_array(&args[0])?,
        _ => return exec_err!("Argument for map_extract should be a map"),
    };

    Ok(Arc::new(ListArray::new(
        Arc::new(Field::new("item", map_array.key_type().clone(), false)),
        map_array.offsets().clone(),
        Arc::clone(map_array.keys()),
        None,
    )))
}
