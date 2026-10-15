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

//! Shared implementation of the `lpad` and `rpad` functions

use std::fmt::Write;
use std::iter;
use std::sync::Arc;

use DataType::{LargeUtf8, Utf8, Utf8View};
use arrow::array::{
    Array, ArrayRef, AsArray, GenericStringBuilder, Int64Array, OffsetSizeTrait,
    StringArrayType,
};
use arrow::datatypes::DataType;
use datafusion_common::cast::as_int64_array;
use datafusion_common::{Result, exec_err};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};

use super::common::{
    StringCharLen, byte_offset_of_char, char_count_or_boundary, pad_data_capacity,
    try_as_scalar_i64, try_as_scalar_str,
};
use crate::utils::make_scalar_function;

/// The side of the string that padding is written on
pub(crate) trait PadSide {
    /// Name of the SQL function, used in error messages
    const NAME: &'static str;
    /// `true` to pad on the left (`lpad`), `false` to pad on the right (`rpad`)
    const LEFT: bool;
}

pub(crate) struct LeftPad {}

impl PadSide for LeftPad {
    const NAME: &'static str = "lpad";
    const LEFT: bool = true;
}

pub(crate) struct RightPad {}

impl PadSide for RightPad {
    const NAME: &'static str = "rpad";
    const LEFT: bool = false;
}

/// Signature shared by `lpad` and `rpad`
pub(crate) fn pad_signature() -> Signature {
    use DataType::*;
    Signature::one_of(
        vec![
            Exact(vec![Utf8View, Int64]),
            Exact(vec![Utf8View, Int64, Utf8View]),
            Exact(vec![Utf8View, Int64, Utf8]),
            Exact(vec![Utf8View, Int64, LargeUtf8]),
            Exact(vec![Utf8, Int64]),
            Exact(vec![Utf8, Int64, Utf8View]),
            Exact(vec![Utf8, Int64, Utf8]),
            Exact(vec![Utf8, Int64, LargeUtf8]),
            Exact(vec![LargeUtf8, Int64]),
            Exact(vec![LargeUtf8, Int64, Utf8View]),
            Exact(vec![LargeUtf8, Int64, Utf8]),
            Exact(vec![LargeUtf8, Int64, LargeUtf8]),
        ],
        Volatility::Immutable,
    )
}

/// `invoke_with_args` for `lpad` and `rpad`
pub(crate) fn invoke_pad<S: PadSide>(args: ScalarFunctionArgs) -> Result<ColumnarValue> {
    let ScalarFunctionArgs {
        args, number_rows, ..
    } = args;

    const MAX_SCALAR_TARGET_LEN: usize = 16384;

    // If target_len and fill (if specified) are constants, use the scalar
    // fast path.
    if let Some(target_len) = try_as_scalar_i64(&args[1]) {
        let target_len = parse_target_len(target_len, S::NAME)?;

        let fill_str = if args.len() == 3 {
            try_as_scalar_str(&args[2])
        } else {
            Some(" ")
        };

        // Skip the fast path for very large `target_len` values to avoid
        // consuming too much memory. Such large padding values are uncommon
        // in practice.
        if target_len <= MAX_SCALAR_TARGET_LEN
            && let Some(fill) = fill_str
        {
            let string_array = args[0].to_array_of_size(number_rows)?;
            let result = match string_array.data_type() {
                Utf8View => pad_scalar_args::<S, _, i32>(
                    string_array.as_string_view(),
                    target_len,
                    fill,
                ),
                Utf8 => pad_scalar_args::<S, _, i32>(
                    string_array.as_string::<i32>(),
                    target_len,
                    fill,
                ),
                LargeUtf8 => pad_scalar_args::<S, _, i64>(
                    string_array.as_string::<i64>(),
                    target_len,
                    fill,
                ),
                other => {
                    exec_err!("Unsupported data type {other:?} for function {}", S::NAME)
                }
            }?;
            return Ok(ColumnarValue::Array(result));
        }
    }

    match args[0].data_type() {
        Utf8 | Utf8View => make_scalar_function(pad::<S, i32>, vec![])(&args),
        LargeUtf8 => make_scalar_function(pad::<S, i64>, vec![])(&args),
        other => exec_err!("Unsupported data type {other:?} for function {}", S::NAME),
    }
}

/// Validates a requested pad length. Lengths above `i32::MAX` are an error and
/// negative lengths are treated as 0.
#[inline(always)]
fn parse_target_len(target_len: i64, fn_name: &str) -> Result<usize> {
    if target_len > i32::MAX as i64 {
        return target_len_too_large(target_len, fn_name);
    }
    Ok(target_len.max(0) as usize)
}

/// Kept out of line so the per-row length check stays small.
#[cold]
#[inline(never)]
fn target_len_too_large(target_len: i64, fn_name: &str) -> Result<usize> {
    exec_err!(
        "{fn_name} requested length {target_len} too large, maximum allowed length is {}",
        i32::MAX
    )
}

/// Appends one output row made of `string` and the padding written by
/// `write_padding`, placing the padding on the side given by `S`.
#[inline(always)]
fn append_padded<S: PadSide, T: OffsetSizeTrait>(
    builder: &mut GenericStringBuilder<T>,
    string: &str,
    write_padding: impl FnOnce(&mut GenericStringBuilder<T>) -> std::fmt::Result,
) -> Result<()> {
    if S::LEFT {
        write_padding(builder)?;
        builder.append_value(string);
    } else {
        builder.write_str(string)?;
        write_padding(builder)?;
        builder.append_value("");
    }
    Ok(())
}

/// Writes `pad_chars` characters of `fill`, repeated cyclically. Whole copies
/// of `fill` are written at once rather than one character at a time.
#[inline(always)]
fn write_fill_chars(
    w: &mut impl Write,
    fill: &str,
    pad_chars: usize,
) -> std::fmt::Result {
    let fill_chars = if fill.is_ascii() {
        fill.len()
    } else {
        fill.chars().count()
    };
    if fill_chars == 0 {
        return Ok(());
    }
    let mut remaining = pad_chars;
    while remaining >= fill_chars {
        w.write_str(fill)?;
        remaining -= fill_chars;
    }
    if remaining > 0 {
        w.write_str(&fill[..byte_offset_of_char(fill, remaining)])?;
    }
    Ok(())
}

/// Optimized pad for constant target_len and fill arguments.
fn pad_scalar_args<'a, S: PadSide, V: StringArrayType<'a> + Copy, T: OffsetSizeTrait>(
    string_array: V,
    target_len: usize,
    fill: &str,
) -> Result<ArrayRef> {
    if string_array.is_ascii() && fill.is_ascii() {
        pad_scalar_ascii::<S, V, T>(string_array, target_len, fill)
    } else {
        pad_scalar_unicode::<S, V, T>(string_array, target_len, fill)
    }
}

fn pad_scalar_ascii<'a, S: PadSide, V: StringArrayType<'a> + Copy, T: OffsetSizeTrait>(
    string_array: V,
    target_len: usize,
    fill: &str,
) -> Result<ArrayRef> {
    // With a scalar `target_len` and `fill`, we can precompute a padding
    // buffer of `target_len` fill characters repeated cyclically.
    let padding_buf = if !fill.is_empty() {
        let mut buf = String::with_capacity(target_len);
        while buf.len() < target_len {
            let remaining = target_len - buf.len();
            if remaining >= fill.len() {
                buf.push_str(fill);
            } else {
                buf.push_str(&fill[..remaining]);
            }
        }
        buf
    } else {
        String::new()
    };

    // Each output row is exactly `target_len` ASCII bytes (string + padding).
    let data_capacity = string_array.len().saturating_mul(target_len);
    let mut builder =
        GenericStringBuilder::<T>::with_capacity(string_array.len(), data_capacity);

    for maybe_string in string_array.iter() {
        match maybe_string {
            Some(string) => {
                let str_len = string.len();
                if target_len <= str_len {
                    builder.append_value(&string[..target_len]);
                } else if fill.is_empty() {
                    builder.append_value(string);
                } else {
                    let pad_needed = target_len - str_len;
                    append_padded::<S, T>(&mut builder, string, |b| {
                        b.write_str(&padding_buf[..pad_needed])
                    })?;
                }
            }
            None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn pad_scalar_unicode<
    'a,
    S: PadSide,
    V: StringArrayType<'a> + Copy,
    T: OffsetSizeTrait,
>(
    string_array: V,
    target_len: usize,
    fill: &str,
) -> Result<ArrayRef> {
    let fill_chars: Vec<char> = fill.chars().collect();

    // With a scalar `target_len` and `fill`, we can precompute a padding buffer
    // of `target_len` fill characters repeated cyclically. Because Unicode
    // characters are variable-width, we build a byte-offset table to map from
    // character count to the corresponding byte position in the padding buffer.
    let (padding_buf, char_byte_offsets) = if !fill_chars.is_empty() {
        let mut buf = String::new();
        let mut offsets = Vec::with_capacity(target_len + 1);
        offsets.push(0usize);
        for i in 0..target_len {
            buf.push(fill_chars[i % fill_chars.len()]);
            offsets.push(buf.len());
        }
        (buf, offsets)
    } else {
        (String::new(), vec![0])
    };

    // Each output row is `target_len` chars; multiply by 4 (max UTF-8 bytes
    // per char) for an upper bound in bytes.
    let data_capacity = string_array.len().saturating_mul(target_len * 4);
    let mut builder =
        GenericStringBuilder::<T>::with_capacity(string_array.len(), data_capacity);

    for maybe_string in string_array.iter() {
        match maybe_string {
            Some(string) => match char_count_or_boundary(string, target_len) {
                StringCharLen::ByteOffset(offset) => {
                    builder.append_value(&string[..offset]);
                }
                StringCharLen::CharCount(char_count) => {
                    append_padded::<S, T>(&mut builder, string, |b| {
                        if fill_chars.is_empty() {
                            return Ok(());
                        }
                        let pad_chars = target_len - char_count;
                        let pad_bytes = char_byte_offsets[pad_chars];
                        b.write_str(&padding_buf[..pad_bytes])
                    })?;
                }
            },
            None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

/// Pads `string` to `target_len` using the fill string (default: space), on
/// the side given by `S`. Truncates from the right if `string` is already
/// longer than `target_len`.
/// lpad('hi', 5, 'xy') = 'xyxhi'
/// rpad('hi', 5, 'xy') = 'hixyx'
fn pad<S: PadSide, T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() <= 1 || args.len() > 3 {
        return exec_err!(
            "{} was called with {} arguments. It requires at least 2 and at most 3.",
            S::NAME,
            args.len()
        );
    }

    let length_array = as_int64_array(&args[1])?;

    // Without a fill argument, pad with spaces. `repeat_with` keeps the fill a
    // compile-time constant in `pad_impl`, so this case is as fast as a
    // dedicated space-padding loop.
    let spaces = || iter::repeat_with(|| Some(" "));

    match (args.len(), args[0].data_type()) {
        (2, Utf8View) => {
            pad_impl::<S, _, T>(&args[0].as_string_view(), length_array, spaces())
        }
        (2, Utf8 | LargeUtf8) => {
            pad_impl::<S, _, T>(&args[0].as_string::<T>(), length_array, spaces())
        }
        (3, Utf8View) => {
            pad_with_fill::<S, _, T>(&args[0].as_string_view(), length_array, &args[2])
        }
        (3, Utf8 | LargeUtf8) => {
            pad_with_fill::<S, _, T>(&args[0].as_string::<T>(), length_array, &args[2])
        }
        (len, dt) => {
            unreachable!("{}: unexpected arg count ({len}) or type ({dt})", S::NAME)
        }
    }
}

fn pad_with_fill<'a, S: PadSide, V: StringArrayType<'a>, T: OffsetSizeTrait>(
    string_array: &V,
    length_array: &Int64Array,
    fill_array: &'a ArrayRef,
) -> Result<ArrayRef> {
    match fill_array.data_type() {
        Utf8View => pad_impl::<S, V, T>(
            string_array,
            length_array,
            fill_array.as_string_view().iter(),
        ),
        LargeUtf8 => pad_impl::<S, V, T>(
            string_array,
            length_array,
            fill_array.as_string::<i64>().iter(),
        ),
        Utf8 => pad_impl::<S, V, T>(
            string_array,
            length_array,
            fill_array.as_string::<i32>().iter(),
        ),
        other => {
            exec_err!("Unsupported data type {other:?} for function {}", S::NAME)
        }
    }
}

fn pad_impl<'a, S, V, T>(
    string_array: &V,
    length_array: &Int64Array,
    fills: impl Iterator<Item = Option<&'a str>>,
) -> Result<ArrayRef>
where
    S: PadSide,
    V: StringArrayType<'a>,
    T: OffsetSizeTrait,
{
    let mut builder: GenericStringBuilder<T> = GenericStringBuilder::with_capacity(
        string_array.len(),
        pad_data_capacity(length_array),
    );
    for ((string, target_len), fill) in
        string_array.iter().zip(length_array.iter()).zip(fills)
    {
        let (Some(string), Some(target_len), Some(fill)) = (string, target_len, fill)
        else {
            builder.append_null();
            continue;
        };

        let target_len = parse_target_len(target_len, S::NAME)?;
        if target_len == 0 {
            builder.append_value("");
            continue;
        }

        if string.is_ascii() && fill.is_ascii() {
            // ASCII fast path: byte length == character length.
            let str_len = string.len();
            if target_len < str_len {
                builder.append_value(&string[..target_len]);
            } else if fill.is_empty() {
                builder.append_value(string);
            } else {
                append_padded::<S, T>(&mut builder, string, |b| {
                    let mut remaining = target_len - str_len;
                    while remaining >= fill.len() {
                        b.write_str(fill)?;
                        remaining -= fill.len();
                    }
                    if remaining > 0 {
                        b.write_str(&fill[..remaining])?;
                    }
                    Ok(())
                })?;
            }
        } else {
            match char_count_or_boundary(string, target_len) {
                StringCharLen::ByteOffset(offset) => {
                    builder.append_value(&string[..offset]);
                }
                StringCharLen::CharCount(char_count) => {
                    append_padded::<S, T>(&mut builder, string, |b| {
                        write_fill_chars(b, fill, target_len - char_count)
                    })?;
                }
            }
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, AsArray, Int64Array, StringArray, StringViewArray,
    };
    use arrow::datatypes::{DataType, Field};
    use datafusion_common::Result;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};

    use crate::unicode::lpad::LPadFunc;
    use crate::unicode::rpad::RPadFunc;

    /// Invokes `func` with every argument passed as an array, so the
    /// per-row path is used instead of the scalar fast path.
    fn invoke(func: &dyn ScalarUDFImpl, args: Vec<ArrayRef>) -> Result<StringArray> {
        let number_rows = args[0].len();
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type().clone(), true).into()
            })
            .collect();
        let result = func.invoke_with_args(ScalarFunctionArgs {
            args: args.into_iter().map(ColumnarValue::Array).collect(),
            arg_fields,
            number_rows,
            return_field: Field::new("f", DataType::Utf8, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        })?;
        Ok(result.into_array(number_rows)?.as_string::<i32>().clone())
    }

    #[test]
    fn pad_array_args() -> Result<()> {
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("hi"),
            Some("josé"),
            Some("hello"),
            None,
            Some("hi"),
            Some("hi"),
        ]));
        let lengths: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(5),
            Some(6),
            Some(3),
            Some(5),
            None,
            Some(0),
        ]));
        let fills: ArrayRef = Arc::new(StringArray::from(vec![
            Some("xy"),
            Some("é"),
            Some("x"),
            Some("x"),
            Some("x"),
            Some("x"),
        ]));
        let no_fill = vec![Arc::clone(&strings), Arc::clone(&lengths)];
        let with_fill = vec![strings, lengths, fills];

        assert_eq!(
            invoke(&LPadFunc::new(), no_fill.clone())?,
            StringArray::from(vec![
                Some("   hi"),
                Some("  josé"),
                Some("hel"),
                None,
                None,
                Some(""),
            ])
        );
        assert_eq!(
            invoke(&RPadFunc::new(), no_fill)?,
            StringArray::from(vec![
                Some("hi   "),
                Some("josé  "),
                Some("hel"),
                None,
                None,
                Some(""),
            ])
        );
        assert_eq!(
            invoke(&LPadFunc::new(), with_fill.clone())?,
            StringArray::from(vec![
                Some("xyxhi"),
                Some("ééjosé"),
                Some("hel"),
                None,
                None,
                Some(""),
            ])
        );
        assert_eq!(
            invoke(&RPadFunc::new(), with_fill)?,
            StringArray::from(vec![
                Some("hixyx"),
                Some("josééé"),
                Some("hel"),
                None,
                None,
                Some(""),
            ])
        );
        Ok(())
    }

    #[test]
    fn pad_string_view_array_args() -> Result<()> {
        let strings: ArrayRef =
            Arc::new(StringViewArray::from(vec![Some("hi"), Some("josé"), None]));
        let lengths: ArrayRef =
            Arc::new(Int64Array::from(vec![Some(4), Some(5), Some(4)]));
        let fills: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("ab"),
            Some("ñ"),
            Some("ab"),
        ]));

        assert_eq!(
            invoke(
                &LPadFunc::new(),
                vec![
                    Arc::clone(&strings),
                    Arc::clone(&lengths),
                    Arc::clone(&fills)
                ]
            )?,
            StringArray::from(vec![Some("abhi"), Some("ñjosé"), None])
        );
        assert_eq!(
            invoke(&RPadFunc::new(), vec![strings, lengths, fills])?,
            StringArray::from(vec![Some("hiab"), Some("joséñ"), None])
        );
        Ok(())
    }

    #[test]
    fn pad_partial_unicode_fill() -> Result<()> {
        // Only part of the multi-character fill fits in the remaining space.
        let strings: ArrayRef = Arc::new(StringArray::from(vec!["josé", "josé"]));
        let lengths: ArrayRef = Arc::new(Int64Array::from(vec![6, 9]));
        let fills: ArrayRef = Arc::new(StringArray::from(vec!["ñéx", "ñéx"]));

        assert_eq!(
            invoke(
                &LPadFunc::new(),
                vec![
                    Arc::clone(&strings),
                    Arc::clone(&lengths),
                    Arc::clone(&fills)
                ]
            )?,
            StringArray::from(vec!["ñéjosé", "ñéxñéjosé"])
        );
        assert_eq!(
            invoke(&RPadFunc::new(), vec![strings, lengths, fills])?,
            StringArray::from(vec!["joséñé", "joséñéxñé"])
        );
        Ok(())
    }

    #[test]
    fn pad_array_length_too_large() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec!["hi"]));
        let lengths: ArrayRef = Arc::new(Int64Array::from(vec![i32::MAX as i64 + 1]));

        for (func, name) in [
            (&LPadFunc::new() as &dyn ScalarUDFImpl, "lpad"),
            (&RPadFunc::new() as &dyn ScalarUDFImpl, "rpad"),
        ] {
            let err = invoke(func, vec![Arc::clone(&strings), Arc::clone(&lengths)])
                .unwrap_err()
                .strip_backtrace();
            assert_eq!(
                err,
                format!(
                    "Execution error: {name} requested length 2147483648 too large, maximum allowed length is 2147483647"
                )
            );
        }
    }
}
