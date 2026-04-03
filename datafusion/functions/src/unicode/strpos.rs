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

use crate::utils::{make_scalar_function, utf8_to_int_type};
use arrow::array::{
    ArrayRef, ArrowPrimitiveType, AsArray, PrimitiveArray, StringArrayType,
};
use arrow::datatypes::{
    ArrowNativeType, DataType, Field, FieldRef, Int32Type, Int64Type,
};
use datafusion_common::types::logical_string;
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;
use memchr::{memchr, memmem};

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Returns the starting position of a specified substring in a string. Positions begin at 1. If the substring does not exist in the string, the function returns 0.",
    syntax_example = "strpos(str, substr)",
    alternative_syntax = "position(substr in origstr)",
    sql_example = r#"```sql
> select strpos('datafusion', 'fus');
+----------------------------------------+
| strpos(Utf8("datafusion"),Utf8("fus")) |
+----------------------------------------+
| 5                                      |
+----------------------------------------+ 
```"#,
    standard_argument(name = "str", prefix = "String"),
    argument(name = "substr", description = "Substring expression to search for.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct StrposFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for StrposFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl StrposFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("instr"), String::from("position")],
        }
    }
}

impl ScalarUDFImpl for StrposFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "strpos"
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
        utf8_to_int_type(args.arg_fields[0].data_type(), "strpos/instr/position").map(
            |data_type| {
                Field::new(
                    self.name(),
                    data_type,
                    args.arg_fields.iter().any(|x| x.is_nullable()),
                )
                .into()
            },
        )
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Fast path for array haystack and scalar needle
        if let (
            ColumnarValue::Array(haystack_array),
            ColumnarValue::Scalar(needle_scalar),
        ) = (&args.args[0], &args.args[1])
        {
            return strpos_scalar_needle(haystack_array, needle_scalar);
        }
        make_scalar_function(strpos, vec![])(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn strpos(args: &[ArrayRef]) -> Result<ArrayRef> {
    /// Dispatches the needle array to the correct string type and calls
    /// `strpos_general` with the given haystack and result type.
    macro_rules! dispatch_needle {
        ($haystack:expr, $result_type:ty, $args:expr) => {
            match $args[1].data_type() {
                DataType::Utf8 => strpos_general::<_, _, $result_type>(
                    $haystack,
                    $args[1].as_string::<i32>(),
                ),
                DataType::LargeUtf8 => strpos_general::<_, _, $result_type>(
                    $haystack,
                    $args[1].as_string::<i64>(),
                ),
                DataType::Utf8View => strpos_general::<_, _, $result_type>(
                    $haystack,
                    $args[1].as_string_view(),
                ),
                other => exec_err!("Unsupported data type {other:?} for strpos needle"),
            }
        };
    }

    match args[0].data_type() {
        DataType::Utf8 => dispatch_needle!(args[0].as_string::<i32>(), Int32Type, args),
        DataType::LargeUtf8 => {
            dispatch_needle!(args[0].as_string::<i64>(), Int64Type, args)
        }
        DataType::Utf8View => dispatch_needle!(args[0].as_string_view(), Int32Type, args),
        other => {
            exec_err!("Unsupported data type {other:?} for strpos haystack")
        }
    }
}

/// Find `needle` in `haystack` using `memchr` to quickly skip to positions
/// where the first byte matches, then verify the remaining bytes. Returns
/// the 0-based byte offset of the match, or `None` if not found. An empty
/// `needle` matches at offset 0.
fn find_substring_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    let needle_len = needle.len();
    let haystack_len = haystack.len();

    if needle_len == 0 {
        return Some(0);
    }
    if needle_len > haystack_len {
        return None;
    }

    let first_byte = needle[0];
    let mut offset = 0;

    while let Some(pos) = memchr(first_byte, &haystack[offset..]) {
        let start = offset + pos;
        if start + needle_len > haystack.len() {
            return None;
        }
        if haystack[start..start + needle_len] == *needle {
            return Some(start);
        }
        offset = start + 1;
    }

    None
}

/// Converts a byte offset within a haystack to a 1-based character position.
/// For ASCII data, byte offset == char offset so we just add 1. For non-ASCII,
/// we count UTF-8 characters in the prefix before the match.
#[inline]
fn byte_offset_to_char_pos<T: ArrowPrimitiveType>(
    haystack: &str,
    byte_offset: usize,
    ascii_only: bool,
) -> Option<T::Native> {
    if ascii_only {
        return T::Native::from_usize(byte_offset + 1);
    }
    // SAFETY: byte_offset is at a UTF-8 char boundary because both haystack
    // and needle are valid UTF-8, and UTF-8 is self-synchronizing: a valid
    // needle byte sequence can only match starting at a char boundary in a
    // valid haystack.
    debug_assert!(haystack.is_char_boundary(byte_offset));
    let prefix =
        unsafe { std::str::from_utf8_unchecked(&haystack.as_bytes()[..byte_offset]) };
    T::Native::from_usize(prefix.chars().count() + 1)
}

/// Fallback strpos implementation for when both haystack and needle are arrays.
/// Building a new `memmem::Finder` for every row is too expensive; it is faster
/// to use `memchr::memchr`.
fn strpos_general<'a, V1, V2, T: ArrowPrimitiveType>(
    haystack_array: V1,
    needle_array: V2,
) -> Result<ArrayRef>
where
    V1: StringArrayType<'a, Item = &'a str> + Copy,
    V2: StringArrayType<'a, Item = &'a str> + Copy,
{
    let ascii_only = needle_array.is_ascii() && haystack_array.is_ascii();
    let haystack_iter = haystack_array.iter();
    let needle_iter = needle_array.iter();

    let result = haystack_iter
        .zip(needle_iter)
        .map(|(haystack, needle)| match (haystack, needle) {
            (Some(haystack), Some(needle)) => {
                let haystack_bytes = haystack.as_bytes();
                let needle_bytes = needle.as_bytes();

                match find_substring_bytes(haystack_bytes, needle_bytes) {
                    None => T::Native::from_usize(0),
                    Some(byte_offset) => {
                        byte_offset_to_char_pos::<T>(haystack, byte_offset, ascii_only)
                    }
                }
            }
            _ => None,
        })
        .collect::<PrimitiveArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Fast-path strpos implementation for when the haystack is an array and the
/// needle is a scalar.  We can pre-build a `memmem::Finder` once and reuse it
/// for every haystack row.
fn strpos_scalar_needle(
    haystack_array: &ArrayRef,
    needle_scalar: &ScalarValue,
) -> Result<ColumnarValue> {
    let Some(needle_str) = needle_scalar.try_as_str() else {
        return exec_err!(
            "Unsupported data type {:?} for strpos needle",
            needle_scalar.data_type()
        );
    };

    // Null needle => null result for every row
    let Some(needle_str) = needle_str else {
        return match haystack_array.data_type() {
            DataType::LargeUtf8 => {
                Ok(ColumnarValue::Array(Arc::new(
                    PrimitiveArray::<Int64Type>::new_null(haystack_array.len()),
                )))
            }
            DataType::Utf8 | DataType::Utf8View => Ok(ColumnarValue::Array(Arc::new(
                PrimitiveArray::<Int32Type>::new_null(haystack_array.len()),
            ))),
            other => exec_err!("Unsupported data type {other:?} for strpos haystack"),
        };
    };

    let result = match haystack_array.data_type() {
        DataType::Utf8 => strpos_with_finder::<_, Int32Type>(
            haystack_array.as_string::<i32>(),
            needle_str,
        ),
        DataType::LargeUtf8 => strpos_with_finder::<_, Int64Type>(
            haystack_array.as_string::<i64>(),
            needle_str,
        ),
        DataType::Utf8View => strpos_with_finder::<_, Int32Type>(
            haystack_array.as_string_view(),
            needle_str,
        ),
        other => {
            exec_err!("Unsupported data type {other:?} for strpos haystack")
        }
    }?;
    Ok(ColumnarValue::Array(result))
}

fn strpos_with_finder<'a, V, T: ArrowPrimitiveType>(
    haystack_array: V,
    needle: &str,
) -> Result<ArrayRef>
where
    V: StringArrayType<'a, Item = &'a str> + Copy,
{
    let needle_bytes = needle.as_bytes();
    let ascii_haystack = haystack_array.is_ascii();
    let finder = memmem::Finder::new(needle_bytes);

    let result = haystack_array
        .iter()
        .map(|string| match string {
            Some(string) => {
                let haystack_bytes = string.as_bytes();
                match finder.find(haystack_bytes) {
                    None => T::Native::from_usize(0),
                    Some(byte_offset) => {
                        byte_offset_to_char_pos::<T>(string, byte_offset, ascii_haystack)
                    }
                }
            }
            None => None,
        })
        .collect::<PrimitiveArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Int32Array, Int64Array};
    use arrow::datatypes::DataType::{Int32, Int64};

    use arrow::datatypes::{DataType, Field};
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::unicode::strpos::StrposFunc;
    use crate::utils::test::test_function;

    macro_rules! test_strpos {
        ($lhs:literal, $rhs:literal -> $result:literal; $t1:ident $t2:ident $t3:ident $t4:ident $t5:ident) => {
            test_function!(
                StrposFunc::new(),
                vec![
                    ColumnarValue::Scalar(ScalarValue::$t1(Some($lhs.to_owned()))),
                    ColumnarValue::Scalar(ScalarValue::$t2(Some($rhs.to_owned()))),
                ],
                Ok(Some($result)),
                $t3,
                $t4,
                $t5
            )
        };
    }

    #[test]
    fn test_strpos_functions() {
        // Utf8 and Utf8 combinations
        test_strpos!("alphabet", "ph" -> 3; Utf8 Utf8 i32 Int32 Int32Array);
        test_strpos!("alphabet", "a" -> 1; Utf8 Utf8 i32 Int32 Int32Array);
        test_strpos!("alphabet", "z" -> 0; Utf8 Utf8 i32 Int32 Int32Array);
        test_strpos!("alphabet", "" -> 1; Utf8 Utf8 i32 Int32 Int32Array);
        test_strpos!("", "a" -> 0; Utf8 Utf8 i32 Int32 Int32Array);
        test_strpos!("", "" -> 1; Utf8 Utf8 i32 Int32 Int32Array);
        test_strpos!("ДатаФусион数据融合📊🔥", "📊" -> 15; Utf8 Utf8 i32 Int32 Int32Array);

        // LargeUtf8 and LargeUtf8 combinations
        test_strpos!("alphabet", "ph" -> 3; LargeUtf8 LargeUtf8 i64 Int64 Int64Array);
        test_strpos!("alphabet", "a" -> 1; LargeUtf8 LargeUtf8 i64 Int64 Int64Array);
        test_strpos!("alphabet", "z" -> 0; LargeUtf8 LargeUtf8 i64 Int64 Int64Array);
        test_strpos!("alphabet", "" -> 1; LargeUtf8 LargeUtf8 i64 Int64 Int64Array);
        test_strpos!("", "a" -> 0; LargeUtf8 LargeUtf8 i64 Int64 Int64Array);
        test_strpos!("", "" -> 1; LargeUtf8 LargeUtf8 i64 Int64 Int64Array);
        test_strpos!("ДатаФусион数据融合📊🔥", "📊" -> 15; LargeUtf8 LargeUtf8 i64 Int64 Int64Array);

        // Utf8 and LargeUtf8 combinations
        test_strpos!("alphabet", "ph" -> 3; Utf8 LargeUtf8 i32 Int32 Int32Array);
        test_strpos!("alphabet", "a" -> 1; Utf8 LargeUtf8 i32 Int32 Int32Array);
        test_strpos!("alphabet", "z" -> 0; Utf8 LargeUtf8 i32 Int32 Int32Array);
        test_strpos!("alphabet", "" -> 1; Utf8 LargeUtf8 i32 Int32 Int32Array);
        test_strpos!("", "a" -> 0; Utf8 LargeUtf8 i32 Int32 Int32Array);
        test_strpos!("", "" -> 1; Utf8 LargeUtf8 i32 Int32 Int32Array);
        test_strpos!("ДатаФусион数据融合📊🔥", "📊" -> 15; Utf8 LargeUtf8 i32 Int32 Int32Array);

        // LargeUtf8 and Utf8 combinations
        test_strpos!("alphabet", "ph" -> 3; LargeUtf8 Utf8 i64 Int64 Int64Array);
        test_strpos!("alphabet", "a" -> 1; LargeUtf8 Utf8 i64 Int64 Int64Array);
        test_strpos!("alphabet", "z" -> 0; LargeUtf8 Utf8 i64 Int64 Int64Array);
        test_strpos!("alphabet", "" -> 1; LargeUtf8 Utf8 i64 Int64 Int64Array);
        test_strpos!("", "a" -> 0; LargeUtf8 Utf8 i64 Int64 Int64Array);
        test_strpos!("", "" -> 1; LargeUtf8 Utf8 i64 Int64 Int64Array);
        test_strpos!("ДатаФусион数据融合📊🔥", "📊" -> 15; LargeUtf8 Utf8 i64 Int64 Int64Array);

        // Utf8View and Utf8View combinations
        test_strpos!("alphabet", "ph" -> 3; Utf8View Utf8View i32 Int32 Int32Array);
        test_strpos!("alphabet", "a" -> 1; Utf8View Utf8View i32 Int32 Int32Array);
        test_strpos!("alphabet", "z" -> 0; Utf8View Utf8View i32 Int32 Int32Array);
        test_strpos!("alphabet", "" -> 1; Utf8View Utf8View i32 Int32 Int32Array);
        test_strpos!("", "a" -> 0; Utf8View Utf8View i32 Int32 Int32Array);
        test_strpos!("", "" -> 1; Utf8View Utf8View i32 Int32 Int32Array);
        test_strpos!("ДатаФусион数据融合📊🔥", "📊" -> 15; Utf8View Utf8View i32 Int32 Int32Array);

        // Utf8View and Utf8 combinations
        test_strpos!("alphabet", "ph" -> 3; Utf8View Utf8 i32 Int32 Int32Array);
        test_strpos!("alphabet", "a" -> 1; Utf8View Utf8 i32 Int32 Int32Array);
        test_strpos!("alphabet", "z" -> 0; Utf8View Utf8 i32 Int32 Int32Array);
        test_strpos!("alphabet", "" -> 1; Utf8View Utf8 i32 Int32 Int32Array);
        test_strpos!("", "a" -> 0; Utf8View Utf8 i32 Int32 Int32Array);
        test_strpos!("", "" -> 1; Utf8View Utf8 i32 Int32 Int32Array);
        test_strpos!("ДатаФусион数据融合📊🔥", "📊" -> 15; Utf8View Utf8 i32 Int32 Int32Array);

        // Utf8View and LargeUtf8 combinations
        test_strpos!("alphabet", "ph" -> 3; Utf8View LargeUtf8 i32 Int32 Int32Array);
        test_strpos!("alphabet", "a" -> 1; Utf8View LargeUtf8 i32 Int32 Int32Array);
        test_strpos!("alphabet", "z" -> 0; Utf8View LargeUtf8 i32 Int32 Int32Array);
        test_strpos!("alphabet", "" -> 1; Utf8View LargeUtf8 i32 Int32 Int32Array);
        test_strpos!("", "a" -> 0; Utf8View LargeUtf8 i32 Int32 Int32Array);
        test_strpos!("", "" -> 1; Utf8View LargeUtf8 i32 Int32 Int32Array);
        test_strpos!("ДатаФусион数据融合📊🔥", "📊" -> 15; Utf8View LargeUtf8 i32 Int32 Int32Array);
    }

    #[test]
    fn nullable_return_type() {
        fn get_nullable(string_array_nullable: bool, substring_nullable: bool) -> bool {
            let strpos = StrposFunc::new();
            let args = datafusion_expr::ReturnFieldArgs {
                arg_fields: &[
                    Field::new("f1", DataType::Utf8, string_array_nullable).into(),
                    Field::new("f2", DataType::Utf8, substring_nullable).into(),
                ],
                scalar_arguments: &[None::<&ScalarValue>, None::<&ScalarValue>],
            };

            strpos.return_field_from_args(args).unwrap().is_nullable()
        }

        assert!(!get_nullable(false, false));

        // If any of the arguments is nullable, the result is nullable
        assert!(get_nullable(true, false));
        assert!(get_nullable(false, true));
        assert!(get_nullable(true, true));
    }
}
