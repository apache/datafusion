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

use crate::utils::{make_scalar_function, utf8_to_int_type};
use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, GenericStringArray, OffsetSizeTrait,
    PrimitiveArray, StringViewArray,
};
use arrow::datatypes::{ArrowNativeType, DataType, Int32Type, Int64Type};
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;
use std::sync::Arc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Returns the number of characters in a string.",
    syntax_example = "character_length(str)",
    sql_example = r#"```sql
> select character_length('Ångström');
+------------------------------------+
| character_length(Utf8("Ångström")) |
+------------------------------------+
| 8                                  |
+------------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    related_udf(name = "bit_length"),
    related_udf(name = "octet_length")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CharacterLengthFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for CharacterLengthFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl CharacterLengthFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::uniform(
                1,
                vec![Utf8, LargeUtf8, Utf8View],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("length"), String::from("char_length")],
        }
    }
}

impl ScalarUDFImpl for CharacterLengthFunc {
    fn name(&self) -> &str {
        "character_length"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_int_type(&arg_types[0], "character_length")
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(character_length, vec![])(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Returns number of characters in the string.
/// character_length('josé') = 4
/// The implementation counts UTF-8 code points to count the number of characters
fn character_length(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Utf8 => {
            let string_array = args[0].as_string::<i32>();
            character_length_offsets::<Int32Type, i32>(string_array)
        }
        DataType::LargeUtf8 => {
            let string_array = args[0].as_string::<i64>();
            character_length_offsets::<Int64Type, i64>(string_array)
        }
        DataType::Utf8View => {
            let string_array = args[0].as_string_view();
            character_length_string_view::<Int32Type>(string_array)
        }
        _ => unreachable!("CharacterLengthFunc"),
    }
}

/// Optimized character_length for offset-based string arrays (Utf8/LargeUtf8).
/// For ASCII-only arrays, computes lengths directly from the offsets buffer
/// without touching the string data at all.
fn character_length_offsets<T, O>(array: &GenericStringArray<O>) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType,
    T::Native: OffsetSizeTrait,
    O: OffsetSizeTrait,
{
    let nulls = array.nulls().cloned();
    let offsets = array.offsets();

    if array.is_ascii() {
        // ASCII: byte length == char length, compute from offsets only
        let values: Vec<T::Native> = offsets
            .windows(2)
            .map(|w| T::Native::usize_as((w[1] - w[0]).as_usize()))
            .collect();
        Ok(Arc::new(PrimitiveArray::<T>::new(values.into(), nulls)))
    } else {
        let values: Vec<T::Native> = (0..array.len())
            .map(|i| {
                // Safety: i is within bounds
                let value = unsafe { array.value_unchecked(i) };
                if value.is_empty() {
                    T::default_value()
                } else if value.is_ascii() {
                    T::Native::usize_as(value.len())
                } else {
                    T::Native::usize_as(value.chars().count())
                }
            })
            .collect();
        Ok(Arc::new(PrimitiveArray::<T>::new(values.into(), nulls)))
    }
}

/// Optimized character_length for StringViewArray.
/// For ASCII-only arrays, reads string lengths directly from the view metadata
/// without touching string data.
fn character_length_string_view<T>(array: &StringViewArray) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType,
    T::Native: OffsetSizeTrait,
{
    let nulls = array.nulls().cloned();
    let views = array.views();

    if array.is_ascii() {
        // ASCII: byte length == char length, read length from view (first 4 bytes)
        let values: Vec<T::Native> = views
            .iter()
            .map(|view| {
                let len = (*view as u32) as usize;
                T::Native::usize_as(len)
            })
            .collect();
        Ok(Arc::new(PrimitiveArray::<T>::new(values.into(), nulls)))
    } else {
        let values: Vec<T::Native> = views
            .iter()
            .enumerate()
            .map(|(i, raw_view)| {
                let len = (*raw_view as u32) as usize;
                if len == 0 {
                    T::default_value()
                } else if len <= 12 {
                    // Inlined string: count UTF-8 chars directly from the u128 view.
                    // Bytes are at positions 4..4+len in the view (little-endian).
                    // Shift right by 32 bits to get the string bytes in the low bits.
                    let data = *raw_view >> 32;
                    // Create a mask of just the high bit of each byte (0x80)
                    // and the bit below it (0x40) to detect continuation bytes (10xxxxxx).
                    // A continuation byte has bit7=1 and bit6=0.
                    // ~data inverts: continuation bytes get bit7=0, bit6=1
                    // (data >> 6) shifts bit7 into bit1 and bit6 into bit0
                    // OR with ~data: for continuation bytes, bit6 is guaranteed 1
                    // For non-continuation bytes, at least one of these will have bit7=1
                    // We only need to check the high bit of each byte after the OR.
                    let not_continuation =
                        (data | (!data >> 1)) & 0x0080_0080_0080_0080_0080_0080u128;
                    T::Native::usize_as(not_continuation.count_ones() as usize)
                } else {
                    // Non-inlined string: must access buffer data
                    // Safety: i is within bounds
                    let value = unsafe { array.value_unchecked(i) };
                    if value.is_ascii() {
                        T::Native::usize_as(len)
                    } else {
                        T::Native::usize_as(value.chars().count())
                    }
                }
            })
            .collect();
        Ok(Arc::new(PrimitiveArray::<T>::new(values.into(), nulls)))
    }
}

#[cfg(test)]
mod tests {
    use crate::unicode::character_length::CharacterLengthFunc;
    use crate::utils::test::test_function;
    use arrow::array::{Array, Int32Array, Int64Array};
    use arrow::datatypes::DataType::{Int32, Int64};
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    macro_rules! test_character_length {
        ($INPUT:expr, $EXPECTED:expr) => {
            test_function!(
                CharacterLengthFunc::new(),
                vec![ColumnarValue::Scalar(ScalarValue::Utf8($INPUT))],
                $EXPECTED,
                i32,
                Int32,
                Int32Array
            );

            test_function!(
                CharacterLengthFunc::new(),
                vec![ColumnarValue::Scalar(ScalarValue::LargeUtf8($INPUT))],
                $EXPECTED,
                i64,
                Int64,
                Int64Array
            );

            test_function!(
                CharacterLengthFunc::new(),
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
        #[cfg(feature = "unicode_expressions")]
        {
            test_character_length!(Some(String::from("chars")), Ok(Some(5)));
            test_character_length!(Some(String::from("josé")), Ok(Some(4)));
            // test long strings (more than 12 bytes for StringView)
            test_character_length!(Some(String::from("joséjoséjoséjosé")), Ok(Some(16)));
            test_character_length!(Some(String::from("")), Ok(Some(0)));
            test_character_length!(None, Ok(None));
        }

        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            CharacterLengthFunc::new(),
            &[ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                String::from("josé")
            )))],
            internal_err!(
                "function character_length requires compilation with feature flag: unicode_expressions."
            ),
            i32,
            Int32,
            Int32Array
        );

        Ok(())
    }
}
