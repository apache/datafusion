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

//! [`ScalarUDFImpl`] definitions for map_entries function.

use crate::utils::{get_map_entry_field, make_scalar_function};
use arrow::array::{Array, ArrayRef, ListArray};
use arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, cast::as_map_array, exec_err};
use datafusion_expr::{
    ArrayFunctionSignature, ColumnarValue, Documentation, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;
use std::sync::Arc;

make_udf_expr_and_func!(
    MapEntriesFunc,
    map_entries,
    map,
    "Return a list of all entries in the map.",
    map_entries_udf
);

#[user_doc(
    doc_section(label = "Map Functions"),
    description = "Returns a list of all entries in the map.",
    syntax_example = "map_entries(map)",
    sql_example = r#"```sql
SELECT map_entries(MAP {'a': 1, 'b': NULL, 'c': 3});
----
[{'key': a, 'value': 1}, {'key': b, 'value': NULL}, {'key': c, 'value': 3}]

SELECT map_entries(map([100, 5], [42, 43]));
----
[{'key': 100, 'value': 42}, {'key': 5, 'value': 43}]
```"#,
    argument(
        name = "map",
        description = "Map expression. Can be a constant, column, or function, and any combination of map operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MapEntriesFunc {
    signature: Signature,
}

impl Default for MapEntriesFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl MapEntriesFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::ArraySignature(ArrayFunctionSignature::MapArray),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for MapEntriesFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "map_entries"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [map_type] = take_function_args(self.name(), arg_types)?;
        let map_fields = get_map_entry_field(map_type)?;
        Ok(DataType::List(Arc::new(Field::new_list_field(
            DataType::Struct(Fields::from(vec![
                Field::new(
                    "key",
                    map_fields.first().unwrap().data_type().clone(),
                    false,
                ),
                Field::new(
                    "value",
                    map_fields.get(1).unwrap().data_type().clone(),
                    true,
                ),
            ])),
            false,
        ))))
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        make_scalar_function(map_entries_inner)(&args.args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn map_entries_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [map_arg] = take_function_args("map_entries", args)?;

    let map_array = match map_arg.data_type() {
        DataType::Map(_, _) => as_map_array(&map_arg)?,
        _ => return exec_err!("Argument for map_entries should be a map"),
    };

    Ok(Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(
            DataType::Struct(Fields::from(vec![
                Field::new("key", map_array.key_type().clone(), false),
                Field::new("value", map_array.value_type().clone(), true),
            ])),
            false,
        )),
        map_array.offsets().clone(),
        Arc::new(map_array.entries().clone()),
        map_array.nulls().cloned(),
    )))
}
