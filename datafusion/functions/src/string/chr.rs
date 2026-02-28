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

use std::any::Any;
use std::sync::Arc;

use arrow::array::{ArrayRef, GenericStringBuilder, Int64Array};
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Int64;
use arrow::datatypes::DataType::Utf8;

use datafusion_common::cast::as_int64_array;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{ColumnarValue, Documentation, Volatility};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_macros::user_doc;

/// Returns the character with the given code.
/// chr(65) = 'A'
fn chr_array(integer_array: &Int64Array) -> Result<ArrayRef> {
    let mut builder = GenericStringBuilder::<i32>::with_capacity(
        integer_array.len(),
        // 1 byte per character, assuming that is the common case
        integer_array.len(),
    );

    let mut buf = [0u8; 4];

    for integer in integer_array {
        match integer {
            Some(integer) => {
                if let Ok(u) = u32::try_from(integer)
                    && let Some(c) = core::char::from_u32(u)
                {
                    builder.append_value(c.encode_utf8(&mut buf));
                    continue;
                }

                return exec_err!("invalid Unicode scalar value: {integer}");
            }
            None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Returns a string containing the character with the specified Unicode scalar value.",
    syntax_example = "chr(expression)",
    sql_example = r#"```sql
> select chr(128640);
+--------------------+
| chr(Int64(128640)) |
+--------------------+
| 🚀                 |
+--------------------+
```"#,
    standard_argument(name = "expression", prefix = "String"),
    related_udf(name = "ascii")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ChrFunc {
    signature: Signature,
}

impl Default for ChrFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ChrFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, vec![Int64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ChrFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "chr"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = take_function_args(self.name(), args.args)?;

        match arg {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(code_point))) => {
                if let Ok(u) = u32::try_from(code_point)
                    && let Some(c) = core::char::from_u32(u)
                {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                        c.to_string(),
                    ))))
                } else {
                    exec_err!("invalid Unicode scalar value: {code_point}")
                }
            }
            ColumnarValue::Scalar(ScalarValue::Int64(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
            }
            ColumnarValue::Array(array) => {
                let integer_array = as_int64_array(&array)?;
                Ok(ColumnarValue::Array(chr_array(integer_array)?))
            }
            other => internal_err!(
                "Unexpected data type {:?} for function chr",
                other.data_type()
            ),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::Field;
    use datafusion_common::assert_contains;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};

    fn invoke_chr(arg: ColumnarValue, number_rows: usize) -> Result<ColumnarValue> {
        ChrFunc::new().invoke_with_args(ScalarFunctionArgs {
            args: vec![arg],
            arg_fields: vec![Field::new("a", Int64, true).into()],
            number_rows,
            return_field: Field::new("f", Utf8, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    #[test]
    fn test_chr_normal() {
        let input = Arc::new(Int64Array::from(vec![
            Some(0),        // \u{0000}
            Some(65),       // A
            Some(66),       // B
            Some(67),       // C
            Some(128640),   // 🚀
            Some(8364),     // €
            Some(945),      // α
            None,           // NULL
            Some(32),       // space
            Some(10),       // newline
            Some(9),        // tab
            Some(0x10FFFF), // 0x10FFFF, the largest Unicode code point
        ]));

        let result = invoke_chr(ColumnarValue::Array(input), 12).unwrap();
        let ColumnarValue::Array(arr) = result else {
            panic!("Expected array");
        };
        let string_array = arr.as_any().downcast_ref::<StringArray>().unwrap();

        let expected = [
            "\u{0000}",
            "A",
            "B",
            "C",
            "🚀",
            "€",
            "α",
            "",
            " ",
            "\n",
            "\t",
            "\u{10ffff}",
        ];

        assert_eq!(string_array.len(), expected.len());
        for (i, e) in expected.iter().enumerate() {
            assert_eq!(string_array.value(i), *e);
        }
    }

    #[test]
    fn test_chr_error() {
        let input = Arc::new(Int64Array::from(vec![i64::MAX]));
        let result = invoke_chr(ColumnarValue::Array(input), 1);
        assert!(result.is_err());
        assert_contains!(
            result.err().unwrap().to_string(),
            "invalid Unicode scalar value: 9223372036854775807"
        );

        let input = Arc::new(Int64Array::from(vec![0x10FFFF + 1]));
        let result = invoke_chr(ColumnarValue::Array(input), 1);
        assert!(result.is_err());
        assert_contains!(
            result.err().unwrap().to_string(),
            "invalid Unicode scalar value: 1114112"
        );

        let input = Arc::new(Int64Array::from(vec![0xD800 + 1]));
        let result = invoke_chr(ColumnarValue::Array(input), 1);
        assert!(result.is_err());
        assert_contains!(
            result.err().unwrap().to_string(),
            "invalid Unicode scalar value: 55297"
        );

        let input = Arc::new(Int64Array::from(vec![i64::MIN + 2i64]));
        let result = invoke_chr(ColumnarValue::Array(input), 1);
        assert!(result.is_err());
        assert_contains!(
            result.err().unwrap().to_string(),
            "invalid Unicode scalar value: -9223372036854775806"
        );

        let input = Arc::new(Int64Array::from(vec![-1]));
        let result = invoke_chr(ColumnarValue::Array(input), 1);
        assert!(result.is_err());
        assert_contains!(
            result.err().unwrap().to_string(),
            "invalid Unicode scalar value: -1"
        );

        let input = Arc::new(Int64Array::from(vec![65, -1, 66]));
        let result = invoke_chr(ColumnarValue::Array(input), 3);
        assert!(result.is_err());
        assert_contains!(
            result.err().unwrap().to_string(),
            "invalid Unicode scalar value: -1"
        );
    }

    #[test]
    fn test_chr_empty() {
        let input = Arc::new(Int64Array::from(Vec::<i64>::new()));
        let result = invoke_chr(ColumnarValue::Array(input), 0).unwrap();
        let ColumnarValue::Array(arr) = result else {
            panic!("Expected array");
        };
        let string_array = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(string_array.len(), 0);
    }

    #[test]
    fn test_chr_scalar() {
        let result =
            invoke_chr(ColumnarValue::Scalar(ScalarValue::Int64(Some(65))), 1).unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                assert_eq!(s, "A");
            }
            other => panic!("Unexpected result: {other:?}"),
        }
    }

    #[test]
    fn test_chr_scalar_null() {
        let result =
            invoke_chr(ColumnarValue::Scalar(ScalarValue::Int64(None)), 1).unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {}
            other => panic!("Unexpected result: {other:?}"),
        }
    }
}
