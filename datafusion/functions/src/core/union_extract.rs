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

use arrow::array::Array;
use arrow::datatypes::{DataType, FieldRef, UnionFields};
use datafusion_common::cast::as_union_array;
use datafusion_common::utils::take_function_args;
use datafusion_common::{
    exec_datafusion_err, exec_err, internal_err, Result, ScalarValue,
};
use datafusion_doc::Documentation;
use datafusion_expr::{ColumnarValue, ReturnInfo, ReturnTypeArgs, ScalarFunctionArgs};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Union Functions"),
    description = "Returns the value of the given field in the union when selected, or NULL otherwise.",
    syntax_example = "union_extract(union, field_name)",
    sql_example = r#"```sql
❯ select union_column, union_extract(union_column, 'a'), union_extract(union_column, 'b') from table_with_union;
+--------------+----------------------------------+----------------------------------+
| union_column | union_extract(union_column, 'a') | union_extract(union_column, 'b') |
+--------------+----------------------------------+----------------------------------+
| {a=1}        | 1                                |                                  |
| {b=3.0}      |                                  | 3.0                              |
| {a=4}        | 4                                |                                  |
| {b=}         |                                  |                                  |
| {a=}         |                                  |                                  |
+--------------+----------------------------------+----------------------------------+
```"#,
    standard_argument(name = "union", prefix = "Union"),
    argument(
        name = "field_name",
        description = "String expression to operate on. Must be a constant."
    )
)]
#[derive(Debug)]
pub struct UnionExtractFun {
    signature: Signature,
}

impl Default for UnionExtractFun {
    fn default() -> Self {
        Self::new()
    }
}

impl UnionExtractFun {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for UnionExtractFun {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "union_extract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        // should be using return_type_from_args and not calling the default implementation
        internal_err!("union_extract should return type from args")
    }

    fn return_type_from_args(&self, args: ReturnTypeArgs) -> Result<ReturnInfo> {
        if args.arg_types.len() != 2 {
            return exec_err!(
                "union_extract expects 2 arguments, got {} instead",
                args.arg_types.len()
            );
        }

        let DataType::Union(fields, _) = &args.arg_types[0] else {
            return exec_err!(
                "union_extract first argument must be a union, got {} instead",
                args.arg_types[0]
            );
        };

        let Some(ScalarValue::Utf8(Some(field_name))) = &args.scalar_arguments[1] else {
            return exec_err!(
                "union_extract second argument must be a non-null string literal, got {} instead",
                args.arg_types[1]
            );
        };

        let field = find_field(fields, field_name)?.1;

        Ok(ReturnInfo::new_nullable(field.data_type().clone()))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [array, target_name] = take_function_args("union_extract", args.args)?;

        let target_name = match target_name {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(target_name))) => Ok(target_name),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => exec_err!("union_extract second argument must be a non-null string literal, got a null instead"),
            _ => exec_err!("union_extract second argument must be a non-null string literal, got {} instead", target_name.data_type()),
        }?;

        match array {
            ColumnarValue::Array(array) => {
                let union_array = as_union_array(&array).map_err(|_| {
                    exec_datafusion_err!(
                        "union_extract first argument must be a union, got {} instead",
                        array.data_type()
                    )
                })?;

                Ok(ColumnarValue::Array(
                    arrow::compute::kernels::union_extract::union_extract(
                        union_array,
                        &target_name,
                    )?,
                ))
            }
            ColumnarValue::Scalar(ScalarValue::Union(value, fields, _)) => {
                let (target_type_id, target) = find_field(&fields, &target_name)?;

                let result = match value {
                    Some((type_id, value)) if target_type_id == type_id => *value,
                    _ => ScalarValue::try_new_null(target.data_type())?,
                };

                Ok(ColumnarValue::Scalar(result))
            }
            other => exec_err!(
                "union_extract first argument must be a union, got {} instead",
                other.data_type()
            ),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn find_field<'a>(fields: &'a UnionFields, name: &str) -> Result<(i8, &'a FieldRef)> {
    fields
        .iter()
        .find(|field| field.1.name() == name)
        .ok_or_else(|| exec_datafusion_err!("field {name} not found on union"))
}

#[cfg(test)]
mod tests {

    use arrow::datatypes::{DataType, Field, UnionFields, UnionMode};
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};

    use super::UnionExtractFun;

    // when it becomes possible to construct union scalars in SQL, this should go to sqllogictests
    #[test]
    fn test_scalar_value() -> Result<()> {
        let fun = UnionExtractFun::new();

        let fields = UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, false),
                Field::new("int", DataType::Int32, false),
            ],
        );

        let result = fun.invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Union(
                    None,
                    fields.clone(),
                    UnionMode::Dense,
                )),
                ColumnarValue::Scalar(ScalarValue::new_utf8("str")),
            ],
            number_rows: 1,
            return_type: &DataType::Utf8,
        })?;

        assert_scalar(result, ScalarValue::Utf8(None));

        let result = fun.invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Union(
                    Some((3, Box::new(ScalarValue::Int32(Some(42))))),
                    fields.clone(),
                    UnionMode::Dense,
                )),
                ColumnarValue::Scalar(ScalarValue::new_utf8("str")),
            ],
            number_rows: 1,
            return_type: &DataType::Utf8,
        })?;

        assert_scalar(result, ScalarValue::Utf8(None));

        let result = fun.invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Union(
                    Some((1, Box::new(ScalarValue::new_utf8("42")))),
                    fields.clone(),
                    UnionMode::Dense,
                )),
                ColumnarValue::Scalar(ScalarValue::new_utf8("str")),
            ],
            number_rows: 1,
            return_type: &DataType::Utf8,
        })?;

        assert_scalar(result, ScalarValue::new_utf8("42"));

        Ok(())
    }

    fn assert_scalar(value: ColumnarValue, expected: ScalarValue) {
        match value {
            ColumnarValue::Array(array) => panic!("expected scalar got {array:?}"),
            ColumnarValue::Scalar(scalar) => assert_eq!(scalar, expected),
        }
    }
}
