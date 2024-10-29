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
use datafusion_expr::scalar_doc_sections::DOC_SECTION_MAP;
use datafusion_expr::{
    ArrayFunctionSignature, ColumnarValue, Documentation, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use std::any::Any;
use std::sync::{Arc, OnceLock};

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
            signature: Signature::new(
                TypeSignature::ArraySignature(ArrayFunctionSignature::MapArray),
                Volatility::Immutable,
            ),
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return exec_err!("map_keys expects single argument");
        }
        let map_type = &arg_types[0];
        let map_fields = get_map_entry_field(map_type)?;
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            map_fields.first().unwrap().data_type().clone(),
            false,
        ))))
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(map_keys_inner)(args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_map_keys_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_map_keys_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_MAP)
            .with_description(
                "Returns a list of all keys in the map."
            )
            .with_syntax_example("map_keys(map)")
            .with_sql_example(
                r#"```sql
SELECT map_keys(MAP {'a': 1, 'b': NULL, 'c': 3});
----
[a, b, c]

SELECT map_keys(map([100, 5], [42, 43]));
----
[100, 5]
```"#,
            )
            .with_argument(
                "map",
                "Map expression. Can be a constant, column, or function, and any combination of map operators."
            )
            .build()
            .unwrap()
    })
}

fn map_keys_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("map_keys expects single argument");
    }

    let map_array = match args[0].data_type() {
        DataType::Map(_, _) => as_map_array(&args[0])?,
        _ => return exec_err!("Argument for map_keys should be a map"),
    };

    Ok(Arc::new(ListArray::new(
        Arc::new(Field::new("item", map_array.key_type().clone(), false)),
        map_array.offsets().clone(),
        Arc::clone(map_array.keys()),
        None,
    )))
}
