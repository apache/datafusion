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

use arrow::array::{ArrayRef, GenericStringBuilder, OffsetSizeTrait};
use arrow::datatypes::DataType;
use datafusion_common::cast::{as_generic_string_array, as_string_view_array};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, exec_err};
use datafusion_expr::{ColumnarValue, Signature, Volatility};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions::utils::make_scalar_function;
use std::sync::Arc;

/// Spark-compatible `soundex` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#soundex>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSoundex {
    signature: Signature,
}

impl Default for SparkSoundex {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSoundex {
    pub fn new() -> Self {
        Self {
            signature: Signature::string(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkSoundex {
    fn name(&self) -> &str {
        "soundex"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::LargeUtf8 => Ok(DataType::LargeUtf8),
            _ => Ok(DataType::Utf8),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_soundex_inner, vec![])(&args.args)
    }
}

fn spark_soundex_inner(arg: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = take_function_args("soundex", arg)?;
    match &array.data_type() {
        DataType::Utf8 => soundex_array::<i32>(array),
        DataType::LargeUtf8 => soundex_array::<i64>(array),
        DataType::Utf8View => soundex_view(array),
        other => {
            exec_err!("unsupported data type {other:?} for function `soundex`")
        }
    }
}

fn soundex_array<T: OffsetSizeTrait>(array: &ArrayRef) -> Result<ArrayRef> {
    Ok(soundex_impl::<T, _>(
        as_generic_string_array::<T>(array)?.iter(),
    ))
}

fn soundex_view(str_view: &ArrayRef) -> Result<ArrayRef> {
    Ok(soundex_impl::<i32, _>(
        as_string_view_array(str_view)?.iter(),
    ))
}

fn soundex_impl<'a, O: OffsetSizeTrait, I: Iterator<Item = Option<&'a str>>>(
    input: I,
) -> ArrayRef {
    let len = input.size_hint().0;
    // A soundex code is always exactly 4 ASCII characters.
    let mut builder = GenericStringBuilder::<O>::with_capacity(len, len * SOUNDEX_LEN);
    for value in input {
        match value {
            Some(value) => append_soundex(&mut builder, value),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

fn classify_char(c: char) -> Option<char> {
    match c.to_ascii_uppercase() {
        'B' | 'F' | 'P' | 'V' => Some('1'),
        'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => Some('2'),
        'D' | 'T' => Some('3'),
        'L' => Some('4'),
        'M' | 'N' => Some('5'),
        'R' => Some('6'),
        _ => None,
    }
}

fn is_ignored(c: char) -> bool {
    matches!(c.to_ascii_uppercase(), 'H' | 'W')
}

/// Length of a soundex code: an initial letter plus three digits.
const SOUNDEX_LEN: usize = 4;

/// Appends the soundex code of `s` to `builder`.
///
/// Strings that do not start with an ASCII letter are passed through unchanged.
/// Otherwise the code is built in a stack buffer, so no row allocates.
fn append_soundex<O: OffsetSizeTrait>(builder: &mut GenericStringBuilder<O>, s: &str) {
    let mut chars = s.chars();

    let first_char = match chars.next() {
        Some(c) if c.is_ascii_alphabetic() => c.to_ascii_uppercase(),
        _ => {
            builder.append_value(s);
            return;
        }
    };

    // Codes shorter than four characters are right-padded with '0'.
    let mut soundex_code = [b'0'; SOUNDEX_LEN];
    soundex_code[0] = first_char as u8;
    let mut written = 1;
    let mut last_code = classify_char(first_char);

    for c in chars {
        if written >= SOUNDEX_LEN {
            break;
        }

        if is_ignored(c) {
            continue;
        }

        match classify_char(c) {
            Some(code) => {
                if last_code != Some(code) {
                    soundex_code[written] = code as u8;
                    written += 1;
                }
                last_code = Some(code);
            }
            None => {
                last_code = None;
            }
        }
    }

    // `soundex_code` holds an ASCII letter followed by ASCII digits, so the
    // validation here is a four-byte check that never fails.
    builder
        .append_value(std::str::from_utf8(&soundex_code).expect("soundex code is ASCII"));
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{LargeStringArray, StringArray, StringViewArray};

    fn soundex(array: ArrayRef) -> ArrayRef {
        spark_soundex_inner(&[array]).unwrap()
    }

    fn as_strings(array: &ArrayRef) -> Vec<Option<&str>> {
        as_generic_string_array::<i32>(array)
            .unwrap()
            .iter()
            .collect()
    }

    #[test]
    fn soundex_preserves_the_offset_width() {
        let utf8 = soundex(Arc::new(StringArray::from(vec!["Miller"])) as ArrayRef);
        assert_eq!(utf8.data_type(), &DataType::Utf8);
        assert_eq!(as_strings(&utf8), vec![Some("M460")]);

        let large = soundex(Arc::new(LargeStringArray::from(vec!["Miller"])) as ArrayRef);
        assert_eq!(large.data_type(), &DataType::LargeUtf8);
        assert_eq!(
            as_generic_string_array::<i64>(&large).unwrap().value(0),
            "M460"
        );

        // A view input has no offsets to preserve, so it narrows to `Utf8`.
        let view = soundex(Arc::new(StringViewArray::from(vec!["Miller"])) as ArrayRef);
        assert_eq!(view.data_type(), &DataType::Utf8);
        assert_eq!(as_strings(&view), vec![Some("M460")]);
    }

    /// Values whose first character is not an ASCII letter are passed through
    /// unchanged, so the output is not always the four-byte code.
    #[test]
    fn soundex_multi_row_batch() {
        let array = Arc::new(StringArray::from(vec![
            Some("Miller"),
            None,
            Some(""),
            // Non-ASCII alphabetic first character: passthrough, not a code.
            Some("Ñoño"),
            Some("Éclair"),
            Some("123"),
            Some("Robert"),
        ])) as ArrayRef;

        assert_eq!(
            as_strings(&soundex(array)),
            vec![
                Some("M460"),
                None,
                Some(""),
                Some("Ñoño"),
                Some("Éclair"),
                Some("123"),
                Some("R163"),
            ]
        );
    }

    #[test]
    fn soundex_sliced_array() {
        let array = Arc::new(StringArray::from(vec![
            Some("Miller"),
            Some("Ñoño"),
            None,
            Some("Robert"),
        ])) as ArrayRef;

        assert_eq!(
            as_strings(&soundex(array.slice(1, 3))),
            vec![Some("Ñoño"), None, Some("R163")]
        );
    }

    #[test]
    fn soundex_empty_array() {
        let empty = soundex(Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef);
        assert_eq!(empty.len(), 0);

        let empty_view =
            soundex(Arc::new(StringViewArray::from(Vec::<&str>::new())) as ArrayRef);
        assert_eq!(empty_view.len(), 0);
    }
}
