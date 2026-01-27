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

use arrow::array::{Array, AsArray, DictionaryArray, Int8Array, StringArray};
use arrow::datatypes::DataType;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_doc::Documentation;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;
use std::sync::Arc;

#[user_doc(
    doc_section(label = "Union Functions"),
    description = "Returns the name of the currently selected field in the union",
    syntax_example = "union_tag(union_expression)",
    sql_example = r#"```sql
❯ select union_column, union_tag(union_column) from table_with_union;
+--------------+-------------------------+
| union_column | union_tag(union_column) |
+--------------+-------------------------+
| {a=1}        | a                       |
| {b=3.0}      | b                       |
| {a=4}        | a                       |
| {b=}         | b                       |
| {a=}         | a                       |
+--------------+-------------------------+
```"#,
    standard_argument(name = "union", prefix = "Union")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct UnionTagFunc {
    signature: Signature,
}

impl Default for UnionTagFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl UnionTagFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for UnionTagFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "union_tag"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::Utf8),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [union_] = take_function_args("union_tag", args.args)?;

        match union_ {
            ColumnarValue::Array(array)
                if matches!(array.data_type(), DataType::Union(_, _)) =>
            {
                let union_array = array.as_union();

                let keys = Int8Array::try_new(union_array.type_ids().clone(), None)?;

                let fields = match union_array.data_type() {
                    DataType::Union(fields, _) => fields,
                    _ => unreachable!(),
                };

                // Union fields type IDs only constraints are being unique and in the 0..128 range:
                // They may not start at 0, be sequential, or even contiguous.
                // Therefore, we allocate a values vector with a length equal to the highest type ID plus one,
                // ensuring that each field's name can be placed at the index corresponding to its type ID.
                let values_len = fields
                    .iter()
                    .map(|(type_id, _)| type_id + 1)
                    .max()
                    .unwrap_or_default() as usize;

                let mut values = vec![""; values_len];

                for (type_id, field) in fields.iter() {
                    values[type_id as usize] = field.name().as_str()
                }

                let values = Arc::new(StringArray::from(values));

                // SAFETY: union type_ids are validated to not be smaller than zero.
                // values len is the union biggest type id plus one.
                // keys is built from the union type_ids, which contains only valid type ids
                // therefore, `keys[i] >= values.len() || keys[i] < 0` never occurs
                let dict = unsafe { DictionaryArray::new_unchecked(keys, values) };

                Ok(ColumnarValue::Array(Arc::new(dict)))
            }
            ColumnarValue::Scalar(ScalarValue::Union(value, fields, _)) => match value {
                Some((value_type_id, _)) => fields
                    .iter()
                    .find(|(type_id, _)| value_type_id == *type_id)
                    .map(|(_, field)| {
                        ColumnarValue::Scalar(ScalarValue::Dictionary(
                            Box::new(DataType::Int8),
                            Box::new(field.name().as_str().into()),
                        ))
                    })
                    .ok_or_else(|| {
                        exec_datafusion_err!(
                            "union_tag: union scalar with unknown type_id {value_type_id}"
                        )
                    }),
                None => Ok(ColumnarValue::Scalar(ScalarValue::try_new_null(
                    args.return_field.data_type(),
                )?)),
            },
            v => exec_err!("union_tag only support unions, got {}", v.data_type()),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[cfg(test)]
mod tests {
    use super::UnionTagFunc;
    use arrow::datatypes::{DataType, Field, UnionFields, UnionMode};
    use datafusion_common::ScalarValue;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
    use std::sync::Arc;

    // when it becomes possible to construct union scalars in SQL, this should go to sqllogictests
    #[test]
    fn union_scalar() {
        let fields = [(0, Arc::new(Field::new("a", DataType::UInt32, false)))]
            .into_iter()
            .collect();

        let scalar = ScalarValue::Union(
            Some((0, Box::new(ScalarValue::UInt32(Some(0))))),
            fields,
            UnionMode::Dense,
        );

        let return_type =
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));

        let result = UnionTagFunc::new()
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Scalar(scalar)],
                number_rows: 1,
                return_field: Field::new("res", return_type, true).into(),
                arg_fields: vec![],
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap();

        assert_scalar(
            result,
            ScalarValue::Dictionary(Box::new(DataType::Int8), Box::new("a".into())),
        );
    }

    #[test]
    fn union_scalar_empty() {
        let scalar = ScalarValue::Union(None, UnionFields::empty(), UnionMode::Dense);

        let return_type =
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));

        let result = UnionTagFunc::new()
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Scalar(scalar)],
                number_rows: 1,
                return_field: Field::new("res", return_type, true).into(),
                arg_fields: vec![],
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap();

        assert_scalar(
            result,
            ScalarValue::Dictionary(
                Box::new(DataType::Int8),
                Box::new(ScalarValue::Utf8(None)),
            ),
        );
    }

    fn assert_scalar(value: ColumnarValue, expected: ScalarValue) {
        match value {
            ColumnarValue::Array(array) => panic!("expected scalar got {array:?}"),
            ColumnarValue::Scalar(scalar) => assert_eq!(scalar, expected),
        }
    }
}
