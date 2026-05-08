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

use arrow::array::{
    Array, BooleanArray, MapBuilder, StringArray, StringBuilder, StructArray,
};
use arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::{Result, ScalarValue, utils::take_function_args};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;
use std::sync::Arc;

#[user_doc(
    doc_section(label = "Other Functions"),
    description = "Returns a struct containing the Arrow field information of the expression, including name, data type, nullability, and metadata.",
    syntax_example = "arrow_field(expression)",
    sql_example = r#"```sql
> select arrow_field(1);
+-------------------------------------------------------------+
| arrow_field(Int64(1))                                       |
+-------------------------------------------------------------+
| {name: lit, data_type: Int64, nullable: false, metadata: {}} |
+-------------------------------------------------------------+

> select arrow_field(1)['data_type'];
+-----------------------------------+
| arrow_field(Int64(1))[data_type]  |
+-----------------------------------+
| Int64                             |
+-----------------------------------+
```"#,
    argument(
        name = "expression",
        description = "Expression to evaluate. The expression can be a constant, column, or function, and any combination of operators."
    )
)]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ArrowFieldFunc {
    signature: Signature,
}

impl Default for ArrowFieldFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrowFieldFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }

    fn return_struct_type() -> DataType {
        DataType::Struct(Fields::from(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("nullable", DataType::Boolean, false),
            Field::new(
                "metadata",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            Field::new("keys", DataType::Utf8, false),
                            Field::new("values", DataType::Utf8, true),
                        ])),
                        false,
                    )),
                    false,
                ),
                false,
            ),
        ]))
    }
}

impl ScalarUDFImpl for ArrowFieldFunc {
    fn name(&self) -> &str {
        "arrow_field"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Self::return_struct_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let return_type = args.return_type().clone();
        let [field] = take_function_args(self.name(), args.arg_fields)?;

        // Build the name array
        let name_array =
            Arc::new(StringArray::from(vec![field.name().as_str()])) as Arc<dyn Array>;

        // Build the data_type array
        let data_type_str = field.data_type().to_string();
        let data_type_array =
            Arc::new(StringArray::from(vec![data_type_str.as_str()])) as Arc<dyn Array>;

        // Build the nullable array
        let nullable_array =
            Arc::new(BooleanArray::from(vec![field.is_nullable()])) as Arc<dyn Array>;

        // Build the metadata map array (same pattern as arrow_metadata.rs)
        let metadata = field.metadata();
        let mut map_builder =
            MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());

        let mut entries: Vec<_> = metadata.iter().collect();
        entries.sort_by_key(|(k, _)| *k);

        for (k, v) in entries {
            map_builder.keys().append_value(k);
            map_builder.values().append_value(v);
        }
        map_builder.append(true)?;

        let metadata_array = Arc::new(map_builder.finish()) as Arc<dyn Array>;

        // Build the struct
        let DataType::Struct(fields) = return_type else {
            unreachable!()
        };

        let struct_array = StructArray::new(
            fields,
            vec![name_array, data_type_array, nullable_array, metadata_array],
            None,
        );

        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &struct_array,
            0,
        )?))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
