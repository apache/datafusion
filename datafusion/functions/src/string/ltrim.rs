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

use arrow::array::{ArrayRef, OffsetSizeTrait};
use arrow::datatypes::DataType;
use std::any::Any;
use std::sync::Arc;

use crate::string::common::*;
use crate::utils::make_scalar_function;
use datafusion_common::types::logical_string;
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::function::Hint;
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;

/// Returns the longest string  with leading characters removed. If the characters are not specified, whitespace is removed.
/// ltrim('zzzytest', 'xyz') = 'test'
fn ltrim<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let use_string_view = args[0].data_type() == &DataType::Utf8View;
    let args = if args.len() > 1 {
        let arg1 = arrow::compute::kernels::cast::cast(&args[1], args[0].data_type())?;
        vec![Arc::clone(&args[0]), arg1]
    } else {
        args.to_owned()
    };
    general_trim::<T, TrimLeft>(&args, use_string_view)
}

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Trims the specified trim string from the beginning of a string. If no trim string is provided, all whitespace is removed from the start of the input string.",
    syntax_example = "ltrim(str[, trim_str])",
    sql_example = r#"```sql
> select ltrim('  datafusion  ');
+-------------------------------+
| ltrim(Utf8("  datafusion  ")) |
+-------------------------------+
| datafusion                    |
+-------------------------------+
> select ltrim('___datafusion___', '_');
+-------------------------------------------+
| ltrim(Utf8("___datafusion___"),Utf8("_")) |
+-------------------------------------------+
| datafusion___                             |
+-------------------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    argument(
        name = "trim_str",
        description = r"String expression to trim from the beginning of the input string. Can be a constant, column, or function, and any combination of arithmetic operators. _Default is whitespace characters._"
    ),
    alternative_syntax = "trim(LEADING trim_str FROM str)",
    related_udf(name = "btrim"),
    related_udf(name = "rtrim")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct LtrimFunc {
    signature: Signature,
}

impl Default for LtrimFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl LtrimFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    ]),
                    TypeSignature::Coercible(vec![Coercion::new_exact(
                        TypeSignatureClass::Native(logical_string()),
                    )]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for LtrimFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ltrim"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let return_type = args.return_field.data_type();
        let number_rows = args.number_rows;
        let args = args.args;

        // If any argument is a scalar NULL, the output is NULL for all rows
        if args
            .iter()
            .any(|v| matches!(v, ColumnarValue::Scalar(s) if s.is_null()))
        {
            if args.iter().any(|v| matches!(v, ColumnarValue::Array(_))) {
                return Ok(ColumnarValue::Array(arrow::array::new_null_array(
                    return_type,
                    number_rows,
                )));
            }
            return Ok(ColumnarValue::Scalar(ScalarValue::try_from(return_type)?));
        }

        // Scalar fast path
        if args.iter().all(|v| matches!(v, ColumnarValue::Scalar(_))) {
            let arg0 = &args[0];
            let arg1 = args.get(1);

            let (value, pattern) = match (arg0, arg1) {
                (ColumnarValue::Scalar(s0), None) => (s0, None),
                (ColumnarValue::Scalar(s0), Some(ColumnarValue::Scalar(s1))) => {
                    (s0, Some(s1))
                }
                _ => {
                    return internal_err!(
                        "Unexpected argument combination in ltrim scalar fast path"
                    );
                }
            };

            let trim_chars: Vec<char> = match pattern {
                None => vec![' '],
                Some(ScalarValue::Utf8(Some(p)))
                | Some(ScalarValue::LargeUtf8(Some(p)))
                | Some(ScalarValue::Utf8View(Some(p))) => p.chars().collect(),
                Some(other) => {
                    return internal_err!(
                        "Unexpected data type {:?} for ltrim pattern",
                        other.data_type()
                    );
                }
            };

            let trimmed = match value {
                ScalarValue::Utf8(Some(s)) => ScalarValue::Utf8(Some(
                    s.trim_start_matches(&trim_chars[..]).to_string(),
                )),
                ScalarValue::Utf8View(Some(s)) => ScalarValue::Utf8View(Some(
                    s.trim_start_matches(&trim_chars[..]).to_string(),
                )),
                ScalarValue::LargeUtf8(Some(s)) => ScalarValue::LargeUtf8(Some(
                    s.trim_start_matches(&trim_chars[..]).to_string(),
                )),
                other => {
                    return internal_err!(
                        "Unexpected data type {:?} for function ltrim",
                        other.data_type()
                    );
                }
            };

            return Ok(ColumnarValue::Scalar(trimmed));
        }

        // Array path
        match args[0].data_type() {
            DataType::Utf8 | DataType::Utf8View => make_scalar_function(
                ltrim::<i32>,
                vec![Hint::Pad, Hint::AcceptsSingular],
            )(&args),
            DataType::LargeUtf8 => make_scalar_function(
                ltrim::<i64>,
                vec![Hint::Pad, Hint::AcceptsSingular],
            )(&args),
            other => exec_err!(
                "Unsupported data type {other:?} for function ltrim, expected Utf8, LargeUtf8 or Utf8View."
            ),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
