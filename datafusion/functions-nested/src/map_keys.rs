// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`ScalarUDFImpl`] definitions for map_keys function.

use crate::utils::{get_map_entry_field, make_scalar_function};
use arrow::array::{Array, ArrayRef, ListArray};
use arrow::datatypes::{DataType, Field};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, cast::as_map_array, exec_err};
use datafusion_expr::{
    ArrayFunctionSignature, ColumnarValue, Documentation, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion_macros::user_doc;
use std::sync::Arc;

make_udf_expr_and_func!(
    MapKeysFunc,
    map_keys,
    map,
    "Return a list of all keys in the map.",
    map_keys_udf
);

#[user_doc(
    doc_section(label = "Map Functions"),
    description = "Returns a list of all keys in the map.",
    syntax_example = "map_keys(map)",
    sql_example = r#"```sql
SELECT map_keys(MAP {'a': 1, 'b': NULL, 'c': 3});
----
[a, b, c]

SELECT map_keys(map([100, 5], [42, 43]));
----
[100, 5]
```"#,
    argument(
        name = "map",
        description = "Map expression. Can be a constant, column, or function, and any combination of map operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MapKeysFunc {
    signature: Signature,
}

impl Default for MapKeysFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl MapKeysFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::ArraySignature(ArrayFunctionSignature::MapArray),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for MapKeysFunc {
    fn name(&self) -> &str {
        "map_keys"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [map_type] = take_function_args(self.name(), arg_types)?;
        let map_fields = get_map_entry_field(map_type)?;
        // internal array nullability is true to be in sync with DuckDB
        Ok(DataType::List(Arc::new(Field::new_list_field(
            map_fields.first().unwrap().data_type().clone(),
            true,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(map_keys_inner)(&args.args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn map_keys_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [map_arg] = take_function_args("map_keys", args)?;

    let map_array = match map_arg.data_type() {
        DataType::Map(_, _) => as_map_array(&map_arg)?,
        _ => return exec_err!("Argument for map_keys should be a map"),
    };

    Ok(Arc::new(ListArray::new(
        // internal array nullability is true to be in sync with DuckDB
        Arc::new(Field::new_list_field(map_array.key_type().clone(), true)),
        map_array.offsets().clone(),
        Arc::clone(map_array.keys()),
        map_array.nulls().cloned(),
    )))
}
