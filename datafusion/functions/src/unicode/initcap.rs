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

use arrow::array::{
    Array, ArrayRef, GenericStringArray, GenericStringBuilder, OffsetSizeTrait,
    StringViewBuilder,
};
use arrow::buffer::Buffer;
use arrow::datatypes::DataType;

use crate::utils::{make_scalar_function, utf8_to_str_type};
use datafusion_common::cast::{as_generic_string_array, as_string_view_array};
use datafusion_common::types::logical_string;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ScalarUDFImpl, Signature, TypeSignatureClass,
    Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Capitalizes the first character in each word in the input string. \
            Words are delimited by non-alphanumeric characters.",
    syntax_example = "initcap(str)",
    sql_example = r#"```sql
> select initcap('apache datafusion');
+------------------------------------+
| initcap(Utf8("apache datafusion")) |
+------------------------------------+
| Apache Datafusion                  |
+------------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    related_udf(name = "lower"),
    related_udf(name = "upper")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct InitcapFunc {
    signature: Signature,
}

impl Default for InitcapFunc {
    fn default() -> Self {
        InitcapFunc::new()
    }
}

impl InitcapFunc {
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

impl ScalarUDFImpl for InitcapFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "initcap"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if let DataType::Utf8View = arg_types[0] {
            Ok(DataType::Utf8View)
        } else {
            utf8_to_str_type(&arg_types[0], "initcap")
        }
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let arg = &args.args[0];

        // Scalar fast path - handle directly without array conversion
        if let ColumnarValue::Scalar(scalar) = arg {
            return match scalar {
                ScalarValue::Utf8(None)
                | ScalarValue::LargeUtf8(None)
                | ScalarValue::Utf8View(None) => Ok(arg.clone()),
                ScalarValue::Utf8(Some(s)) => {
                    let mut result = String::new();
                    initcap_string(s, &mut result);
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
                }
                ScalarValue::LargeUtf8(Some(s)) => {
                    let mut result = String::new();
                    initcap_string(s, &mut result);
                    Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(result))))
                }
                ScalarValue::Utf8View(Some(s)) => {
                    let mut result = String::new();
                    initcap_string(s, &mut result);
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(Some(result))))
                }
                other => {
                    exec_err!(
                        "Unsupported data type {:?} for function `initcap`",
                        other.data_type()
                    )
                }
            };
        }

        // Array path
        let args = &args.args;
        match args[0].data_type() {
            DataType::Utf8 => make_scalar_function(initcap::<i32>, vec![])(args),
            DataType::LargeUtf8 => make_scalar_function(initcap::<i64>, vec![])(args),
            DataType::Utf8View => make_scalar_function(initcap_utf8view, vec![])(args),
            other => {
                exec_err!("Unsupported data type {other:?} for function `initcap`")
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Converts the first letter of each word to uppercase and the rest to
/// lowercase. Words are sequences of alphanumeric characters separated by
/// non-alphanumeric characters.
///
/// Example:
/// ```sql
/// initcap('hi THOMAS') = 'Hi Thomas'
/// ```
fn initcap<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = as_generic_string_array::<T>(&args[0])?;

    if string_array.value_data().is_ascii() {
        return Ok(initcap_ascii_array(string_array));
    }

    let mut builder = GenericStringBuilder::<T>::with_capacity(
        string_array.len(),
        string_array.value_data().len(),
    );

    let mut container = String::new();
    string_array.iter().for_each(|str| match str {
        Some(s) => {
            initcap_string(s, &mut container);
            builder.append_value(&container);
        }
        None => builder.append_null(),
    });

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

/// Fast path for initcap of `Utf8` or `LargeUtf8` arrays that are
/// ASCII-only. We can operate on the entire buffer in a single pass, and
/// operate on bytes directly. Since ASCII case conversion preserves byte
/// length, the original offsets and nulls also don't need to be recomputed.
fn initcap_ascii_array<T: OffsetSizeTrait>(
    string_array: &GenericStringArray<T>,
) -> ArrayRef {
    let offsets = string_array.offsets();
    let src = string_array.value_data();
    let first_offset = offsets.first().unwrap().as_usize();
    let last_offset = offsets.last().unwrap().as_usize();
    let mut out = Vec::with_capacity(src.len());

    // Preserve bytes before the first offset unchanged.
    out.extend_from_slice(&src[..first_offset]);

    for window in offsets.windows(2) {
        let start = window[0].as_usize();
        let end = window[1].as_usize();

        let mut prev_is_alnum = false;
        for &b in &src[start..end] {
            let converted = if prev_is_alnum {
                b.to_ascii_lowercase()
            } else {
                b.to_ascii_uppercase()
            };
            out.push(converted);
            prev_is_alnum = b.is_ascii_alphanumeric();
        }
    }

    // Preserve bytes after the last offset unchanged.
    out.extend_from_slice(&src[last_offset..]);

    let values = Buffer::from_vec(out);
    // SAFETY: ASCII case conversion preserves byte length, so the original
    // offsets and nulls remain valid.
    Arc::new(unsafe {
        GenericStringArray::<T>::new_unchecked(
            offsets.clone(),
            values,
            string_array.nulls().cloned(),
        )
    })
}

fn initcap_utf8view(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_view_array = as_string_view_array(&args[0])?;
    let mut builder = StringViewBuilder::with_capacity(string_view_array.len());
    let mut container = String::new();

    string_view_array.iter().for_each(|str| match str {
        Some(s) => {
            initcap_string(s, &mut container);
            builder.append_value(&container);
        }
        None => builder.append_null(),
    });

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn initcap_string(input: &str, container: &mut String) {
    container.clear();
    let mut prev_is_alphanumeric = false;

    if input.is_ascii() {
        container.reserve(input.len());
        // SAFETY: each byte is ASCII, so the result is valid UTF-8.
        let out = unsafe { container.as_mut_vec() };
        for &b in input.as_bytes() {
            if prev_is_alphanumeric {
                out.push(b.to_ascii_lowercase());
            } else {
                out.push(b.to_ascii_uppercase());
            }
            prev_is_alphanumeric = b.is_ascii_alphanumeric();
        }
    } else {
        for c in input.chars() {
            if prev_is_alphanumeric {
                container.extend(c.to_lowercase());
            } else {
                container.extend(c.to_uppercase());
            }
            prev_is_alphanumeric = c.is_alphanumeric();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::unicode::initcap::InitcapFunc;
    use crate::utils::test::test_function;
    use arrow::array::{Array, ArrayRef, LargeStringArray, StringArray, StringViewArray};
    use arrow::datatypes::DataType::{Utf8, Utf8View};
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};
    use std::sync::Arc;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            InitcapFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::from("hi THOMAS"))],
            Ok(Some("Hi Thomas")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            InitcapFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                "êM ả ñAnDÚ ÁrBOL ОлЕГ ИвАНОВИч ÍslENsku ÞjóðaRiNNaR εΛλΗΝΙκΉ"
                    .to_string()
            )))],
            Ok(Some(
                "Êm Ả Ñandú Árbol Олег Иванович Íslensku Þjóðarinnar Ελληνική"
            )),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            InitcapFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::from(""))],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            InitcapFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::from(""))],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            InitcapFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8(None))],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );

        test_function!(
            InitcapFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                "hi THOMAS".to_string()
            )))],
            Ok(Some("Hi Thomas")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            InitcapFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                "hi THOMAS wIth M0re ThAN 12 ChaRs".to_string()
            )))],
            Ok(Some("Hi Thomas With M0re Than 12 Chars")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            InitcapFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                "đẸp đẼ êM ả ñAnDÚ ÁrBOL ОлЕГ ИвАНОВИч ÍslENsku ÞjóðaRiNNaR εΛλΗΝΙκΉ"
                    .to_string()
            )))],
            Ok(Some(
                "Đẹp Đẽ Êm Ả Ñandú Árbol Олег Иванович Íslensku Þjóðarinnar Ελληνική"
            )),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            InitcapFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                "".to_string()
            )))],
            Ok(Some("")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            InitcapFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8View(None))],
            Ok(None),
            &str,
            Utf8View,
            StringViewArray
        );

        Ok(())
    }

    #[test]
    fn test_initcap_ascii_array() -> Result<()> {
        let array = StringArray::from(vec![
            Some("hello world"),
            None,
            Some("foo-bar_baz/baX"),
            Some(""),
            Some("123 abc 456DEF"),
            Some("ALL CAPS"),
            Some("already correct"),
        ]);
        let args: Vec<ArrayRef> = vec![Arc::new(array)];
        let result = super::initcap::<i32>(&args)?;
        let result = result.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(result.len(), 7);
        assert_eq!(result.value(0), "Hello World");
        assert!(result.is_null(1));
        assert_eq!(result.value(2), "Foo-Bar_Baz/Bax");
        assert_eq!(result.value(3), "");
        assert_eq!(result.value(4), "123 Abc 456def");
        assert_eq!(result.value(5), "All Caps");
        assert_eq!(result.value(6), "Already Correct");
        Ok(())
    }

    #[test]
    fn test_initcap_ascii_large_array() -> Result<()> {
        let array = LargeStringArray::from(vec![
            Some("hello world"),
            None,
            Some("foo-bar_baz/baX"),
            Some(""),
            Some("123 abc 456DEF"),
            Some("ALL CAPS"),
            Some("already correct"),
        ]);
        let args: Vec<ArrayRef> = vec![Arc::new(array)];
        let result = super::initcap::<i64>(&args)?;
        let result = result.as_any().downcast_ref::<LargeStringArray>().unwrap();

        assert_eq!(result.len(), 7);
        assert_eq!(result.value(0), "Hello World");
        assert!(result.is_null(1));
        assert_eq!(result.value(2), "Foo-Bar_Baz/Bax");
        assert_eq!(result.value(3), "");
        assert_eq!(result.value(4), "123 Abc 456def");
        assert_eq!(result.value(5), "All Caps");
        assert_eq!(result.value(6), "Already Correct");
        Ok(())
    }

    /// Test that initcap works correctly on a sliced ASCII StringArray.
    #[test]
    fn test_initcap_sliced_ascii_array() -> Result<()> {
        let array = StringArray::from(vec![
            Some("hello world"),
            Some("foo bar"),
            Some("baz qux"),
        ]);
        // Slice to get only the last two elements. The resulting array's
        // offsets are [11, 18, 25] (non-zero start), but value_data still
        // contains the full original buffer.
        let sliced = array.slice(1, 2);
        let args: Vec<ArrayRef> = vec![Arc::new(sliced)];
        let result = super::initcap::<i32>(&args)?;
        let result = result.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result.value(0), "Foo Bar");
        assert_eq!(result.value(1), "Baz Qux");
        Ok(())
    }

    /// Test that initcap works correctly on a sliced ASCII LargeStringArray.
    #[test]
    fn test_initcap_sliced_ascii_large_array() -> Result<()> {
        let array = LargeStringArray::from(vec![
            Some("hello world"),
            Some("foo bar"),
            Some("baz qux"),
        ]);
        // Slice to get only the last two elements. The resulting array's
        // offsets are [11, 18, 25] (non-zero start), but value_data still
        // contains the full original buffer.
        let sliced = array.slice(1, 2);
        let args: Vec<ArrayRef> = vec![Arc::new(sliced)];
        let result = super::initcap::<i64>(&args)?;
        let result = result.as_any().downcast_ref::<LargeStringArray>().unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result.value(0), "Foo Bar");
        assert_eq!(result.value(1), "Baz Qux");
        Ok(())
    }
}
