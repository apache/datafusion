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

use arrow::array::{ArrayRef, AsArray, Int32Array, StringArrayType};
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use datafusion_common::types::logical_string;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::{ColumnarValue, Documentation, TypeSignatureClass};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_expr_common::signature::Coercion;
use datafusion_macros::user_doc;
use std::any::Any;
use std::sync::Arc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Returns the first Unicode scalar value of a string.",
    syntax_example = "ascii(str)",
    sql_example = r#"```sql
> select ascii('abc');
+--------------------+
| ascii(Utf8("abc")) |
+--------------------+
| 97                 |
+--------------------+
> select ascii('🚀');
+-------------------+
| ascii(Utf8("🚀")) |
+-------------------+
| 128640            |
+-------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    related_udf(name = "chr")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct AsciiFunc {
    signature: Signature,
}

impl Default for AsciiFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl AsciiFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![Coercion::new_exact(TypeSignatureClass::Native(
                    logical_string(),
                ))],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for AsciiFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ascii"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = take_function_args(self.name(), args.args)?;

        match arg {
            ColumnarValue::Scalar(scalar) => {
                if scalar.is_null() {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Int32(None)));
                }

                match scalar {
                    ScalarValue::Utf8(Some(s))
                    | ScalarValue::LargeUtf8(Some(s))
                    | ScalarValue::Utf8View(Some(s)) => {
                        let result = s.chars().next().map_or(0, |c| c as i32);
                        Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(result))))
                    }
                    _ => {
                        internal_err!(
                            "Unexpected data type {:?} for function ascii",
                            scalar.data_type()
                        )
                    }
                }
            }
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(ascii(&[array])?)),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn calculate_ascii<'a, V>(array: &V) -> Result<ArrayRef, ArrowError>
where
    V: StringArrayType<'a, Item = &'a str>,
{
    let values: Vec<_> = (0..array.len())
        .map(|i| {
            if array.is_null(i) {
                0
            } else {
                let s = array.value(i);
                s.chars().next().map_or(0, |c| c as i32)
            }
        })
        .collect();

    let array = Int32Array::new(values.into(), array.nulls().cloned());

    Ok(Arc::new(array))
}

/// Returns the numeric code of the first character of the argument.
pub fn ascii(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Utf8 => {
            let string_array = args[0].as_string::<i32>();
            Ok(calculate_ascii(&string_array)?)
        }
        DataType::LargeUtf8 => {
            let string_array = args[0].as_string::<i64>();
            Ok(calculate_ascii(&string_array)?)
        }
        DataType::Utf8View => {
            let string_array = args[0].as_string_view();
            Ok(calculate_ascii(&string_array)?)
        }
        _ => internal_err!("Unsupported data type"),
    }
}

#[cfg(test)]
mod tests {
    use crate::string::ascii::AsciiFunc;
    use crate::utils::test::test_function;
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::DataType::Int32;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    macro_rules! test_ascii {
        ($INPUT:expr, $EXPECTED:expr) => {
            test_function!(
                AsciiFunc::new(),
                vec![ColumnarValue::Scalar(ScalarValue::Utf8($INPUT))],
                $EXPECTED,
                i32,
                Int32,
                Int32Array
            );

            test_function!(
                AsciiFunc::new(),
                vec![ColumnarValue::Scalar(ScalarValue::LargeUtf8($INPUT))],
                $EXPECTED,
                i32,
                Int32,
                Int32Array
            );

            test_function!(
                AsciiFunc::new(),
                vec![ColumnarValue::Scalar(ScalarValue::Utf8View($INPUT))],
                $EXPECTED,
                i32,
                Int32,
                Int32Array
            );
        };
    }

    #[test]
    fn test_functions() -> Result<()> {
        test_ascii!(Some(String::from("x")), Ok(Some(120)));
        test_ascii!(Some(String::from("a")), Ok(Some(97)));
        test_ascii!(Some(String::from("")), Ok(Some(0)));
        test_ascii!(Some(String::from("🚀")), Ok(Some(128640)));
        test_ascii!(Some(String::from("\n")), Ok(Some(10)));
        test_ascii!(Some(String::from("\t")), Ok(Some(9)));
        test_ascii!(None, Ok(None));
        Ok(())
    }
}
