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
use arrow::array::{Array, ArrayRef, ListArray};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, cast::as_map_array, exec_err, internal_err};
use datafusion_expr::{
    ArrayFunctionSignature, ColumnarValue, Documentation, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;
use std::ops::Deref;
use std::sync::Arc;

make_udf_expr_and_func!(
    MapValuesFunc,
    map_values,
    map,
    "Return a list of all values in the map.",
    map_values_udf
);

#[user_doc(
    doc_section(label = "Map Functions"),
    description = "Returns a list of all values in the map.",
    syntax_example = "map_values(map)",
    sql_example = r#"```sql
SELECT map_values(MAP {'a': 1, 'b': NULL, 'c': 3});
----
[1, , 3]

SELECT map_values(map([100, 5], [42, 43]));
----
[42, 43]
```"#,
    argument(
        name = "map",
        description = "Map expression. Can be a constant, column, or function, and any combination of map operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct MapValuesFunc {
    signature: Signature,
}

impl Default for MapValuesFunc {
    fn default() -> Self {
        Self::new()
    }
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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(
        &self,
        args: datafusion_expr::ReturnFieldArgs,
    ) -> Result<FieldRef> {
        let [map_type] = take_function_args(self.name(), args.arg_fields)?;

        Ok(Field::new(
            self.name(),
            DataType::List(get_map_values_field_as_list_field(map_type.data_type())?),
            // Nullable if the map is nullable
            args.arg_fields.iter().any(|x| x.is_nullable()),
        )
        .into())
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        make_scalar_function(map_values_inner)(&args.args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn map_values_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [map_arg] = take_function_args("map_values", args)?;

    let map_array = match map_arg.data_type() {
        DataType::Map(_, _) => as_map_array(&map_arg)?,
        _ => return exec_err!("Argument for map_values should be a map"),
    };

    Ok(Arc::new(ListArray::new(
        get_map_values_field_as_list_field(map_arg.data_type())?,
        map_array.offsets().clone(),
        Arc::clone(map_array.values()),
        map_array.nulls().cloned(),
    )))
}

fn get_map_values_field_as_list_field(map_type: &DataType) -> Result<FieldRef> {
    let map_fields = get_map_entry_field(map_type)?;

    let values_field = map_fields
        .last()
        .unwrap()
        .deref()
        .clone()
        .with_name(Field::LIST_FIELD_DEFAULT_NAME);

    Ok(Arc::new(values_field))
}

#[cfg(test)]
mod tests {
    use crate::map_values::MapValuesFunc;
    use arrow::datatypes::{DataType, Field, FieldRef};
    use datafusion_common::ScalarValue;
    use datafusion_expr::ScalarUDFImpl;
    use std::sync::Arc;

    #[test]
    fn return_type_field() {
        fn get_map_field(
            is_map_nullable: bool,
            is_keys_nullable: bool,
            is_values_nullable: bool,
        ) -> FieldRef {
            Field::new_map(
                "something",
                "entries",
                Arc::new(Field::new("keys", DataType::Utf8, is_keys_nullable)),
                Arc::new(Field::new(
                    "values",
                    DataType::LargeUtf8,
                    is_values_nullable,
                )),
                false,
                is_map_nullable,
            )
            .into()
        }

        fn get_list_field(
            name: &str,
            is_list_nullable: bool,
            list_item_type: DataType,
            is_list_items_nullable: bool,
        ) -> FieldRef {
            Field::new_list(
                name,
                Arc::new(Field::new_list_field(
                    list_item_type,
                    is_list_items_nullable,
                )),
                is_list_nullable,
            )
            .into()
        }

        fn get_return_field(field: FieldRef) -> FieldRef {
            let func = MapValuesFunc::new();
            let args = datafusion_expr::ReturnFieldArgs {
                arg_fields: &[field],
                scalar_arguments: &[None::<&ScalarValue>],
            };

            func.return_field_from_args(args).unwrap()
        }

        // Test cases:
        //
        // |                      Input Map                         ||                   Expected Output                     |
        // | ------------------------------------------------------ || ----------------------------------------------------- |
        // | map nullable | map keys nullable | map values nullable || expected list nullable | expected list items nullable |
        // | ------------ | ----------------- | ------------------- || ---------------------- | ---------------------------- |
        // | false        | false             | false               || false                  | false                        |
        // | false        | false             | true                || false                  | true                         |
        // | false        | true              | false               || false                  | false                        |
        // | false        | true              | true                || false                  | true                         |
        // | true         | false             | false               || true                   | false                        |
        // | true         | false             | true                || true                   | true                         |
        // | true         | true              | false               || true                   | false                        |
        // | true         | true              | true                || true                   | true                         |
        //
        // ---------------
        // We added the key nullability to show that it does not affect the nullability of the list or the list items.

        assert_eq!(
            get_return_field(get_map_field(false, false, false)),
            get_list_field("map_values", false, DataType::LargeUtf8, false)
        );

        assert_eq!(
            get_return_field(get_map_field(false, false, true)),
            get_list_field("map_values", false, DataType::LargeUtf8, true)
        );

        assert_eq!(
            get_return_field(get_map_field(false, true, false)),
            get_list_field("map_values", false, DataType::LargeUtf8, false)
        );

        assert_eq!(
            get_return_field(get_map_field(false, true, true)),
            get_list_field("map_values", false, DataType::LargeUtf8, true)
        );

        assert_eq!(
            get_return_field(get_map_field(true, false, false)),
            get_list_field("map_values", true, DataType::LargeUtf8, false)
        );

        assert_eq!(
            get_return_field(get_map_field(true, false, true)),
            get_list_field("map_values", true, DataType::LargeUtf8, true)
        );

        assert_eq!(
            get_return_field(get_map_field(true, true, false)),
            get_list_field("map_values", true, DataType::LargeUtf8, false)
        );

        assert_eq!(
            get_return_field(get_map_field(true, true, true)),
            get_list_field("map_values", true, DataType::LargeUtf8, true)
        );
    }
}
