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

use arrow::array::{
    Array, ArrayRef, GenericStringArray, Int64Array, OffsetSizeTrait, StringArrayType,
    StringViewArray,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::DataType;

use crate::strings::{
    BulkNullStringArrayBuilder, GenericStringArrayBuilder, StringWriter,
};
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

/// Computes the byte ranges of `string` to keep around the replaced span: the
/// prefix is `string[..prefix_end]` and the suffix is `string[suffix_start..]`.
///
/// `start_pos` is a 1-based character position; the caller must ensure it is
/// `>= 1`. `replace_len` is the number of characters of `string` to replace,
/// and may be negative (in which case `suffix_start <= prefix_end` and the
/// result re-emits part of the original string).
///
/// Matches PostgreSQL semantics for codepoint indices past the end of
/// `string`: `prefix_end` and `suffix_start` clamp to `string.len()`.
fn overlay_bounds(string: &str, start_pos: i64, replace_len: i64) -> (usize, usize) {
    let start_char_idx = start_pos - 1;
    let end_char_idx = start_char_idx.saturating_add(replace_len);

    if string.is_ascii() {
        // ASCII fast path: byte index == codepoint index.
        let len = string.len() as i64;
        let prefix_end = start_char_idx.clamp(0, len) as usize;
        let suffix_start = end_char_idx.clamp(0, len) as usize;
        return (prefix_end, suffix_start);
    }

    let prefix_target = usize::try_from(start_char_idx).unwrap_or(usize::MAX);
    let suffix_target = usize::try_from(end_char_idx.max(0)).unwrap_or(usize::MAX);
    let target_max = prefix_target.max(suffix_target);

    // Single forward pass over codepoint boundaries records both targets.
    // Either target falls through to `string.len()` if past the codepoint
    // count.
    let mut prefix_byte = string.len();
    let mut suffix_byte = string.len();
    for (count, (byte_idx, _)) in string.char_indices().enumerate() {
        if count == prefix_target {
            prefix_byte = byte_idx;
        }
        if count == suffix_target {
            suffix_byte = byte_idx;
        }
        if count == target_max {
            break;
        }
    }
    (prefix_byte, suffix_byte)
}

/// Appends the overlay result for one non-null row into `builder`.
#[inline]
fn apply_overlay<B: BulkNullStringArrayBuilder>(
    string: &str,
    characters: &str,
    start_pos: i64,
    replace_len: i64,
    builder: &mut B,
) -> Result<()> {
    if start_pos < 1 {
        return exec_err!("overlay start position must be at least 1: {start_pos}");
    }
    let (prefix_end, suffix_start) = overlay_bounds(string, start_pos, replace_len);
    builder.append_with(|w| {
        w.write_str(&string[..prefix_end]);
        w.write_str(characters);
        w.write_str(&string[suffix_start..]);
    });
    Ok(())
}

#[inline]
fn char_count(characters: &str) -> i64 {
    if characters.is_ascii() {
        characters.len() as i64
    } else {
        characters.chars().count() as i64
    }
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
    let pos_array = as_int64_array(&args[2])?;
    let len_array = if args.len() == 4 {
        Some(as_int64_array(&args[3])?)
    } else {
        None
    };

    if args[0].data_type() == &DataType::Utf8View {
        let string_array = as_string_view_array(&args[0])?;
        let characters_array = as_string_view_array(&args[1])?;
        let data_capacity = visible_view_bytes(string_array)
            .saturating_add(visible_view_bytes(characters_array));
        let builder = GenericStringArrayBuilder::<i32>::with_capacity(
            string_array.len(),
            data_capacity,
        );
        overlay_inner(
            string_array,
            characters_array,
            pos_array,
            len_array,
            builder,
        )
    } else {
        let string_array = as_generic_string_array::<T>(&args[0])?;
        let characters_array = as_generic_string_array::<T>(&args[1])?;
        let data_capacity = visible_offset_bytes(string_array)
            .saturating_add(visible_offset_bytes(characters_array));
        let builder = GenericStringArrayBuilder::<T>::with_capacity(
            string_array.len(),
            data_capacity,
        );
        overlay_inner(
            string_array,
            characters_array,
            pos_array,
            len_array,
            builder,
        )
    }
}

/// Drives the per-row OVERLAY computation. A null in any input array
/// produces a null output.
fn overlay_inner<'a, V, B>(
    string_array: V,
    characters_array: V,
    pos_array: &Int64Array,
    len_array: Option<&Int64Array>,
    mut builder: B,
) -> Result<ArrayRef>
where
    V: StringArrayType<'a, Item = &'a str> + Copy,
    B: BulkNullStringArrayBuilder,
{
    let len = string_array.len();
    let nulls = NullBuffer::union_many([
        string_array.nulls(),
        characters_array.nulls(),
        pos_array.nulls(),
        len_array.and_then(|a| a.nulls()),
    ]);

    if let Some(nulls_ref) = nulls.as_ref() {
        for i in 0..len {
            if nulls_ref.is_null(i) {
                builder.append_placeholder();
                continue;
            }
            // SAFETY: `i < len`, and null bitmap check implies not-null
            let string = unsafe { string_array.value_unchecked(i) };
            let characters = unsafe { characters_array.value_unchecked(i) };
            let start_pos = unsafe { pos_array.value_unchecked(i) };
            let replace_len = match len_array {
                Some(arr) => unsafe { arr.value_unchecked(i) },
                None => char_count(characters),
            };
            apply_overlay(string, characters, start_pos, replace_len, &mut builder)?;
        }
    } else {
        for i in 0..len {
            // SAFETY: `i < len`, and no null bitmap means no nulls
            let string = unsafe { string_array.value_unchecked(i) };
            let characters = unsafe { characters_array.value_unchecked(i) };
            let start_pos = unsafe { pos_array.value_unchecked(i) };
            let replace_len = match len_array {
                Some(arr) => unsafe { arr.value_unchecked(i) },
                None => char_count(characters),
            };
            apply_overlay(string, characters, start_pos, replace_len, &mut builder)?;
        }
    }
    builder.finish(nulls)
}

/// Bytes referenced by the visible window of `array`, computed from the
/// per-view lengths.
fn visible_view_bytes(array: &StringViewArray) -> usize {
    array.lengths().map(|l| l as usize).sum()
}

/// Bytes referenced by the visible window of `array`, derived from the offset
/// buffer.
fn visible_offset_bytes<T: OffsetSizeTrait>(array: &GenericStringArray<T>) -> usize {
    let offsets = array.value_offsets();
    // `value_offsets()` always has `array.len() + 1` entries (≥1).
    let first = offsets.first().copied().unwrap_or_default();
    let last = offsets.last().copied().unwrap_or_default();
    last.as_usize() - first.as_usize()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::StringArray;

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
