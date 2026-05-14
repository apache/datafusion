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

use std::sync::Arc;

use arrow::array::{ArrayRef, GenericStringArray, OffsetSizeTrait};
use arrow::datatypes::DataType;

use crate::utils::{make_scalar_function, utf8_to_str_type};
use datafusion_common::cast::{
    as_generic_string_array, as_int64_array, as_string_view_array,
};
use datafusion_common::{Result, exec_err};
use datafusion_expr::{ColumnarValue, Documentation, TypeSignature, Volatility};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Returns the string which is replaced by another string from the specified position and specified count length.",
    syntax_example = "overlay(str PLACING substr FROM pos [FOR count])",
    sql_example = r#"```sql
> select overlay('Txxxxas' placing 'hom' from 2 for 4);
+--------------------------------------------------------+
| overlay(Utf8("Txxxxas"),Utf8("hom"),Int64(2),Int64(4)) |
+--------------------------------------------------------+
| Thomas                                                 |
+--------------------------------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    argument(name = "substr", description = "Substring to replace in str."),
    argument(
        name = "pos",
        description = "The start position to start the replace in str."
    ),
    argument(
        name = "count",
        description = "The count of characters to be replaced from start position of str. If not specified, will use substr length instead."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct OverlayFunc {
    signature: Signature,
}

impl Default for OverlayFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl OverlayFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![Utf8View, Utf8View, Int64, Int64]),
                    TypeSignature::Exact(vec![Utf8, Utf8, Int64, Int64]),
                    TypeSignature::Exact(vec![LargeUtf8, LargeUtf8, Int64, Int64]),
                    TypeSignature::Exact(vec![Utf8View, Utf8View, Int64]),
                    TypeSignature::Exact(vec![Utf8, Utf8, Int64]),
                    TypeSignature::Exact(vec![LargeUtf8, LargeUtf8, Int64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for OverlayFunc {
    fn name(&self) -> &str {
        "overlay"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_str_type(&arg_types[0], "overlay")
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.args[0].data_type() {
            DataType::Utf8View | DataType::Utf8 => {
                make_scalar_function(overlay::<i32>, vec![])(&args.args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(overlay::<i64>, vec![])(&args.args)
            }
            other => exec_err!("Unsupported data type {other:?} for function overlay"),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Converts a 0-based character index into a byte index suitable for UTF-8
/// slicing.
fn byte_index_for_char(string: &str, char_idx: usize, is_ascii: bool) -> usize {
    if is_ascii {
        char_idx.min(string.len())
    } else {
        string
            .char_indices()
            .nth(char_idx)
            .map_or(string.len(), |(byte_idx, _)| byte_idx)
    }
}

/// Builds the OVERLAY result for a single (non-null) row.
///
/// `start_pos` is a 1-based character position; `replace_len` is the number
/// of characters of `string` to replace with `characters`.
fn overlay_one(
    string: &str,
    characters: &str,
    start_pos: i64,
    replace_len: i64,
) -> Result<String> {
    if start_pos < 1 {
        return exec_err!("negative substring length not allowed");
    }

    let is_ascii = string.is_ascii();
    let string_char_len = if is_ascii {
        string.len() as i64
    } else {
        string.chars().count() as i64
    };

    // Convert SQL's 1-based character position into 0-based character indexes.
    // `start_char_idx` is the first replaced character; `end_char_idx` is the
    // first character after the replaced span.
    //
    // No upper-bound check on `start_char_idx`: when it exceeds `string_char_len`
    // we want the whole string as the prefix (PostgreSQL-compatible "insert past
    // end" semantics).
    let start_char_idx = start_pos - 1;
    let end_char_idx = start_char_idx.saturating_add(replace_len);

    let prefix_char_idx = usize::try_from(start_char_idx).unwrap_or(usize::MAX);
    let prefix_end_byte = byte_index_for_char(string, prefix_char_idx, is_ascii);

    let mut res = String::with_capacity(string.len() + characters.len());
    res.push_str(&string[..prefix_end_byte]);
    res.push_str(characters);

    if end_char_idx < string_char_len {
        let suffix_char_idx = usize::try_from(end_char_idx.max(0)).unwrap_or(usize::MAX);
        let suffix_start_byte = byte_index_for_char(string, suffix_char_idx, is_ascii);
        res.push_str(&string[suffix_start_byte..]);
    }
    Ok(res)
}

macro_rules! process_overlay {
    // Three argument case
    ($string_array:expr, $characters_array:expr, $pos_array:expr) => {{
        $string_array
            .iter()
            .zip($characters_array.iter())
            .zip($pos_array.iter())
            .map(|((string, characters), start_pos)| {
                match (string, characters, start_pos) {
                    (Some(string), Some(characters), Some(start_pos)) => {
                        let replace_len = characters.chars().count() as i64;
                        overlay_one(string, characters, start_pos, replace_len).map(Some)
                    }
                    _ => Ok(None),
                }
            })
            .collect::<Result<GenericStringArray<T>>>()
    }};

    // Four argument case
    ($string_array:expr, $characters_array:expr, $pos_array:expr, $len_array:expr) => {{
        $string_array
            .iter()
            .zip($characters_array.iter())
            .zip($pos_array.iter())
            .zip($len_array.iter())
            .map(|(((string, characters), start_pos), replace_len)| {
                match (string, characters, start_pos, replace_len) {
                    (
                        Some(string),
                        Some(characters),
                        Some(start_pos),
                        Some(replace_len),
                    ) => {
                        overlay_one(string, characters, start_pos, replace_len).map(Some)
                    }
                    _ => Ok(None),
                }
            })
            .collect::<Result<GenericStringArray<T>>>()
    }};
}

/// `OVERLAY(string PLACING substring FROM start [FOR count])`
///
/// Replaces a region of `string` with `substring`, starting at the 1-based
/// character position `start`. If `count` is supplied, that many characters
/// of `string` are replaced; otherwise `count` defaults to the character
/// length of `substring`.
///
/// ```text
/// overlay('Txxxxas' placing 'hom' from 2 for 4) → 'Thomas'
/// overlay('Txxxxas' placing 'hom' from 2)       → 'Thomxas'
/// ```
fn overlay<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    if !matches!(args.len(), 3 | 4) {
        return exec_err!(
            "overlay was called with {} arguments. It requires 3 or 4.",
            args.len()
        );
    }
    if args[0].data_type() == &DataType::Utf8View {
        string_view_overlay::<T>(args)
    } else {
        string_overlay::<T>(args)
    }
}

fn string_overlay<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = as_generic_string_array::<T>(&args[0])?;
    let characters_array = as_generic_string_array::<T>(&args[1])?;
    let pos_array = as_int64_array(&args[2])?;

    let result = if args.len() == 4 {
        let len_array = as_int64_array(&args[3])?;
        process_overlay!(string_array, characters_array, pos_array, len_array)?
    } else {
        process_overlay!(string_array, characters_array, pos_array)?
    };
    Ok(Arc::new(result) as ArrayRef)
}

fn string_view_overlay<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = as_string_view_array(&args[0])?;
    let characters_array = as_string_view_array(&args[1])?;
    let pos_array = as_int64_array(&args[2])?;

    let result = if args.len() == 4 {
        let len_array = as_int64_array(&args[3])?;
        process_overlay!(string_array, characters_array, pos_array, len_array)?
    } else {
        process_overlay!(string_array, characters_array, pos_array)?
    };
    Ok(Arc::new(result) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int64Array, StringArray};

    use super::*;

    #[test]
    fn to_overlay() -> Result<()> {
        let string =
            Arc::new(StringArray::from(vec!["123", "abcdefg", "xyz", "Txxxxas"]));
        let replace_string =
            Arc::new(StringArray::from(vec!["abc", "qwertyasdfg", "ijk", "hom"]));
        let start = Arc::new(Int64Array::from(vec![4, 1, 1, 2])); // start
        let end = Arc::new(Int64Array::from(vec![5, 7, 2, 4])); // replace len

        let res = overlay::<i32>(&[string, replace_string, start, end]).unwrap();
        let result = as_generic_string_array::<i32>(&res).unwrap();
        // First row: start=4 is past the end of "123" (len 3). PostgreSQL
        // takes the whole string as prefix and appends the replacement.
        let expected = StringArray::from(vec!["123abc", "qwertyasdfg", "ijkz", "Thomas"]);
        assert_eq!(&expected, result);

        Ok(())
    }
}
