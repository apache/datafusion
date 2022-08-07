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

// Some of these functions reference the Postgres documentation
// or implementation to ensure compatibility and are subject to
// the Postgres license.

//! Unicode expressions

use arrow::{
    array::{ArrayRef, GenericStringArray, Int64Array, OffsetSizeTrait, PrimitiveArray},
    datatypes::{ArrowNativeType, ArrowPrimitiveType},
};
use datafusion_common::{DataFusionError, Result};
use hashbrown::HashMap;
use std::cmp::Ordering;
use std::sync::Arc;
use std::{any::type_name, cmp::max};
use unicode_segmentation::UnicodeSegmentation;

macro_rules! downcast_string_arg {
    ($ARG:expr, $NAME:expr, $T:ident) => {{
        $ARG.as_any()
            .downcast_ref::<GenericStringArray<T>>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "could not cast {} to {}",
                    $NAME,
                    type_name::<GenericStringArray<T>>()
                ))
            })?
    }};
}

macro_rules! downcast_arg {
    ($ARG:expr, $NAME:expr, $ARRAY_TYPE:ident) => {{
        $ARG.as_any().downcast_ref::<$ARRAY_TYPE>().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "could not cast {} to {}",
                $NAME,
                type_name::<$ARRAY_TYPE>()
            ))
        })?
    }};
}

/// Returns number of characters in the string.
/// character_length('josé') = 4
/// The implementation counts UTF-8 code points to count the number of characters
pub fn character_length<T: ArrowPrimitiveType>(args: &[ArrayRef]) -> Result<ArrayRef>
where
    T::Native: OffsetSizeTrait,
{
    let string_array: &GenericStringArray<T::Native> = args[0]
        .as_any()
        .downcast_ref::<GenericStringArray<T::Native>>()
        .ok_or_else(|| {
            DataFusionError::Internal("could not cast string to StringArray".to_string())
        })?;

    let result = string_array
        .iter()
        .map(|string| {
            string.map(|string: &str| {
                T::Native::from_usize(string.chars().count())
                    .expect("should not fail as string.chars will always return integer")
            })
        })
        .collect::<PrimitiveArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Returns first n characters in the string, or when n is negative, returns all but last |n| characters.
/// left('abcde', 2) = 'ab'
/// The implementation uses UTF-8 code points as characters
pub fn left<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = downcast_string_arg!(args[0], "string", T);
    let n_array = downcast_arg!(args[1], "n", Int64Array);
    let result = string_array
        .iter()
        .zip(n_array.iter())
        .map(|(string, n)| match (string, n) {
            (Some(string), Some(n)) => match n.cmp(&0) {
                Ordering::Less => {
                    let len = string.chars().count() as i64;
                    Some(if n.abs() < len {
                        string.chars().take((len + n) as usize).collect::<String>()
                    } else {
                        "".to_string()
                    })
                }
                Ordering::Equal => Some("".to_string()),
                Ordering::Greater => {
                    Some(string.chars().take(n as usize).collect::<String>())
                }
            },
            _ => None,
        })
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Extends the string to length 'length' by prepending the characters fill (a space by default). If the string is already longer than length then it is truncated (on the right).
/// lpad('hi', 5, 'xy') = 'xyxhi'
pub fn lpad<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args.len() {
        2 => {
            let string_array = downcast_string_arg!(args[0], "string", T);
            let length_array = downcast_arg!(args[1], "length", Int64Array);

            let result = string_array
                .iter()
                .zip(length_array.iter())
                .map(|(string, length)| match (string, length) {
                    (Some(string), Some(length)) => {
                        let length = length as usize;
                        if length == 0 {
                            Some("".to_string())
                        } else {
                            let graphemes = string.graphemes(true).collect::<Vec<&str>>();
                            if length < graphemes.len() {
                                Some(graphemes[..length].concat())
                            } else {
                                let mut s: String = " ".repeat(length - graphemes.len());
                                s.push_str(string);
                                Some(s)
                            }
                        }
                    }
                    _ => None,
                })
                .collect::<GenericStringArray<T>>();

            Ok(Arc::new(result) as ArrayRef)
        }
        3 => {
            let string_array = downcast_string_arg!(args[0], "string", T);
            let length_array = downcast_arg!(args[1], "length", Int64Array);
            let fill_array = downcast_string_arg!(args[2], "fill", T);

            let result = string_array
                .iter()
                .zip(length_array.iter())
                .zip(fill_array.iter())
                .map(|((string, length), fill)| match (string, length, fill) {
                    (Some(string), Some(length), Some(fill)) => {
                        let length = length as usize;

                        if length == 0 {
                            Some("".to_string())
                        } else {
                            let graphemes = string.graphemes(true).collect::<Vec<&str>>();
                            let fill_chars = fill.chars().collect::<Vec<char>>();

                            if length < graphemes.len() {
                                Some(graphemes[..length].concat())
                            } else if fill_chars.is_empty() {
                                Some(string.to_string())
                            } else {
                                let mut s = string.to_string();
                                let mut char_vector =
                                    Vec::<char>::with_capacity(length - graphemes.len());
                                for l in 0..length - graphemes.len() {
                                    char_vector.push(
                                        *fill_chars.get(l % fill_chars.len()).unwrap(),
                                    );
                                }
                                s.insert_str(
                                    0,
                                    char_vector.iter().collect::<String>().as_str(),
                                );
                                Some(s)
                            }
                        }
                    }
                    _ => None,
                })
                .collect::<GenericStringArray<T>>();

            Ok(Arc::new(result) as ArrayRef)
        }
        other => Err(DataFusionError::Internal(format!(
            "lpad was called with {} arguments. It requires at least 2 and at most 3.",
            other
        ))),
    }
}

/// Reverses the order of the characters in the string.
/// reverse('abcde') = 'edcba'
/// The implementation uses UTF-8 code points as characters
pub fn reverse<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = downcast_string_arg!(args[0], "string", T);

    let result = string_array
        .iter()
        .map(|string| string.map(|string: &str| string.chars().rev().collect::<String>()))
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Returns last n characters in the string, or when n is negative, returns all but first |n| characters.
/// right('abcde', 2) = 'de'
/// The implementation uses UTF-8 code points as characters
pub fn right<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = downcast_string_arg!(args[0], "string", T);
    let n_array = downcast_arg!(args[1], "n", Int64Array);

    let result = string_array
        .iter()
        .zip(n_array.iter())
        .map(|(string, n)| match (string, n) {
            (Some(string), Some(n)) => match n.cmp(&0) {
                Ordering::Less => Some(
                    string
                        .chars()
                        .skip(n.unsigned_abs() as usize)
                        .collect::<String>(),
                ),
                Ordering::Equal => Some("".to_string()),
                Ordering::Greater => Some(
                    string
                        .chars()
                        .skip(max(string.chars().count() as i64 - n, 0) as usize)
                        .collect::<String>(),
                ),
            },
            _ => None,
        })
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Extends the string to length 'length' by appending the characters fill (a space by default). If the string is already longer than length then it is truncated.
/// rpad('hi', 5, 'xy') = 'hixyx'
pub fn rpad<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args.len() {
        2 => {
            let string_array = downcast_string_arg!(args[0], "string", T);
            let length_array = downcast_arg!(args[1], "length", Int64Array);

            let result = string_array
                .iter()
                .zip(length_array.iter())
                .map(|(string, length)| match (string, length) {
                    (Some(string), Some(length)) => {
                        let length = length as usize;
                        if length == 0 {
                            Some("".to_string())
                        } else {
                            let graphemes = string.graphemes(true).collect::<Vec<&str>>();
                            if length < graphemes.len() {
                                Some(graphemes[..length].concat())
                            } else {
                                let mut s = string.to_string();
                                s.push_str(" ".repeat(length - graphemes.len()).as_str());
                                Some(s)
                            }
                        }
                    }
                    _ => None,
                })
                .collect::<GenericStringArray<T>>();

            Ok(Arc::new(result) as ArrayRef)
        }
        3 => {
            let string_array = downcast_string_arg!(args[0], "string", T);
            let length_array = downcast_arg!(args[1], "length", Int64Array);
            let fill_array = downcast_string_arg!(args[2], "fill", T);

            let result = string_array
                .iter()
                .zip(length_array.iter())
                .zip(fill_array.iter())
                .map(|((string, length), fill)| match (string, length, fill) {
                    (Some(string), Some(length), Some(fill)) => {
                        let length = length as usize;
                        let graphemes = string.graphemes(true).collect::<Vec<&str>>();
                        let fill_chars = fill.chars().collect::<Vec<char>>();

                        if length < graphemes.len() {
                            Some(graphemes[..length].concat())
                        } else if fill_chars.is_empty() {
                            Some(string.to_string())
                        } else {
                            let mut s = string.to_string();
                            let mut char_vector =
                                Vec::<char>::with_capacity(length - graphemes.len());
                            for l in 0..length - graphemes.len() {
                                char_vector
                                    .push(*fill_chars.get(l % fill_chars.len()).unwrap());
                            }
                            s.push_str(char_vector.iter().collect::<String>().as_str());
                            Some(s)
                        }
                    }
                    _ => None,
                })
                .collect::<GenericStringArray<T>>();

            Ok(Arc::new(result) as ArrayRef)
        }
        other => Err(DataFusionError::Internal(format!(
            "rpad was called with {} arguments. It requires at least 2 and at most 3.",
            other
        ))),
    }
}

/// Returns starting index of specified substring within string, or zero if it's not present. (Same as position(substring in string), but note the reversed argument order.)
/// strpos('high', 'ig') = 2
/// The implementation uses UTF-8 code points as characters
pub fn strpos<T: ArrowPrimitiveType>(args: &[ArrayRef]) -> Result<ArrayRef>
where
    T::Native: OffsetSizeTrait,
{
    let string_array: &GenericStringArray<T::Native> = args[0]
        .as_any()
        .downcast_ref::<GenericStringArray<T::Native>>()
        .ok_or_else(|| {
            DataFusionError::Internal("could not cast string to StringArray".to_string())
        })?;

    let substring_array: &GenericStringArray<T::Native> = args[1]
        .as_any()
        .downcast_ref::<GenericStringArray<T::Native>>()
        .ok_or_else(|| {
            DataFusionError::Internal(
                "could not cast substring to StringArray".to_string(),
            )
        })?;

    let result = string_array
        .iter()
        .zip(substring_array.iter())
        .map(|(string, substring)| match (string, substring) {
            (Some(string), Some(substring)) => {
                // the find method returns the byte index of the substring
                // Next, we count the number of the chars until that byte
                T::Native::from_usize(
                    string
                        .find(substring)
                        .map(|x| string[..x].chars().count() + 1)
                        .unwrap_or(0),
                )
            }
            _ => None,
        })
        .collect::<PrimitiveArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Extracts the substring of string starting at the start'th character, and extending for count characters if that is specified. (Same as substring(string from start for count).)
/// substr('alphabet', 3) = 'phabet'
/// substr('alphabet', 3, 2) = 'ph'
/// The implementation uses UTF-8 code points as characters
pub fn substr<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args.len() {
        2 => {
            let string_array = downcast_string_arg!(args[0], "string", T);
            let start_array = downcast_arg!(args[1], "start", Int64Array);

            let result = string_array
                .iter()
                .zip(start_array.iter())
                .map(|(string, start)| match (string, start) {
                    (Some(string), Some(start)) => {
                        if start <= 0 {
                            Some(string.to_string())
                        } else {
                            Some(string.chars().skip(start as usize - 1).collect())
                        }
                    }
                    _ => None,
                })
                .collect::<GenericStringArray<T>>();

            Ok(Arc::new(result) as ArrayRef)
        }
        3 => {
            let string_array = downcast_string_arg!(args[0], "string", T);
            let start_array = downcast_arg!(args[1], "start", Int64Array);
            let count_array = downcast_arg!(args[2], "count", Int64Array);

            let result = string_array
                .iter()
                .zip(start_array.iter())
                .zip(count_array.iter())
                .map(|((string, start), count)| match (string, start, count) {
                    (Some(string), Some(start), Some(count)) => {
                        if count < 0 {
                            Err(DataFusionError::Execution(format!(
                                "negative substring length not allowed: substr(<str>, {}, {})",
                                start,
                                count
                            )))
                        } else {
                            let skip = max(0, start - 1);
                            let count = max(0, count + (if start < 1 {start - 1} else {0}));
                            Ok(Some(string.chars().skip(skip as usize).take(count as usize).collect::<String>()))
                        }
                    }
                    _ => Ok(None),
                })
                .collect::<Result<GenericStringArray<T>>>()?;

            Ok(Arc::new(result) as ArrayRef)
        }
        other => Err(DataFusionError::Internal(format!(
            "substr was called with {} arguments. It requires 2 or 3.",
            other
        ))),
    }
}

/// Replaces each character in string that matches a character in the from set with the corresponding character in the to set. If from is longer than to, occurrences of the extra characters in from are deleted.
/// translate('12345', '143', 'ax') = 'a2x5'
pub fn translate<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = downcast_string_arg!(args[0], "string", T);
    let from_array = downcast_string_arg!(args[1], "from", T);
    let to_array = downcast_string_arg!(args[2], "to", T);

    let result = string_array
        .iter()
        .zip(from_array.iter())
        .zip(to_array.iter())
        .map(|((string, from), to)| match (string, from, to) {
            (Some(string), Some(from), Some(to)) => {
                // create a hashmap of [char, index] to change from O(n) to O(1) for from list
                let from_map: HashMap<&str, usize> = from
                    .graphemes(true)
                    .collect::<Vec<&str>>()
                    .iter()
                    .enumerate()
                    .map(|(index, c)| (c.to_owned(), index))
                    .collect();

                let to = to.graphemes(true).collect::<Vec<&str>>();

                Some(
                    string
                        .graphemes(true)
                        .collect::<Vec<&str>>()
                        .iter()
                        .flat_map(|c| match from_map.get(*c) {
                            Some(n) => to.get(*n).copied(),
                            None => Some(*c),
                        })
                        .collect::<Vec<&str>>()
                        .concat(),
                )
            }
            _ => None,
        })
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}
