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

use arrow::array::ArrayRef;
use arrow::array::GenericStringBuilder;
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Int64;
use arrow::datatypes::DataType::Utf8;

use crate::utils::make_scalar_function;
use datafusion_common::cast::as_int64_array;
use datafusion_common::{exec_err, Result};
use datafusion_expr::{ColumnarValue, Documentation, Volatility};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_macros::user_doc;

/// Returns the character with the given code.
/// chr(65) = 'A'
pub fn chr(args: &[ArrayRef]) -> Result<ArrayRef> {
    let integer_array = as_int64_array(&args[0])?;

    let mut builder = GenericStringBuilder::<i32>::with_capacity(
        integer_array.len(),
        // 1 byte per character, assuming that is the common case
        integer_array.len(),
    );

    let mut buf = [0u8; 4];

    for integer in integer_array {
        match integer {
            Some(integer) => {
                if let Ok(u) = u32::try_from(integer) {
                    if let Some(c) = core::char::from_u32(u) {
                        builder.append_value(c.encode_utf8(&mut buf));
                        continue;
                    }
                }

                return exec_err!("invalid Unicode scalar value: {integer}");
            }
            None => {
                builder.append_null();
            }
        }
    }

    let result = builder.finish();

    Ok(Arc::new(result) as ArrayRef)
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
        make_scalar_function(chr, vec![])(&args.args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray};
    use datafusion_common::assert_contains;

    #[test]
    fn test_chr_normal() {
        let input = Arc::new(Int64Array::from(vec![
            Some(0),        // null
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
        let result = chr(&[input]).unwrap();
        let string_array = result.as_any().downcast_ref::<StringArray>().unwrap();
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
        // invalid Unicode code points (too large)
        let input = Arc::new(Int64Array::from(vec![i64::MAX]));
        let result = chr(&[input]);
        assert!(result.is_err());
        assert_contains!(
            result.err().unwrap().to_string(),
            "invalid Unicode scalar value: 9223372036854775807"
        );

        // invalid Unicode code points (too large) case 2
        let input = Arc::new(Int64Array::from(vec![0x10FFFF + 1]));
        let result = chr(&[input]);
        assert!(result.is_err());
        assert_contains!(
            result.err().unwrap().to_string(),
            "invalid Unicode scalar value: 1114112"
        );

        // invalid Unicode code points (surrogate code point)
        // link: <https://learn.microsoft.com/en-us/globalization/encoding/unicode-standard#surrogate-pairs>
        let input = Arc::new(Int64Array::from(vec![0xD800 + 1]));
        let result = chr(&[input]);
        assert!(result.is_err());
        assert_contains!(
            result.err().unwrap().to_string(),
            "invalid Unicode scalar value: 55297"
        );

        // negative input
        let input = Arc::new(Int64Array::from(vec![i64::MIN + 2i64])); // will be 2 if cast to u32
        let result = chr(&[input]);
        assert!(result.is_err());
        assert_contains!(
            result.err().unwrap().to_string(),
            "invalid Unicode scalar value: -9223372036854775806"
        );

        // negative input case 2
        let input = Arc::new(Int64Array::from(vec![-1]));
        let result = chr(&[input]);
        assert!(result.is_err());
        assert_contains!(
            result.err().unwrap().to_string(),
            "invalid Unicode scalar value: -1"
        );

        // one error with valid values after
        let input = Arc::new(Int64Array::from(vec![65, -1, 66])); // A, -1, B
        let result = chr(&[input]);
        assert!(result.is_err());
        assert_contains!(
            result.err().unwrap().to_string(),
            "invalid Unicode scalar value: -1"
        );
    }

    #[test]
    fn test_chr_empty() {
        // empty input array
        let input = Arc::new(Int64Array::from(Vec::<i64>::new()));
        let result = chr(&[input]).unwrap();
        let string_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(string_array.len(), 0);
    }
}
