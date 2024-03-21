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

use arrow::{
    array::{Array, ArrayRef, GenericStringArray, OffsetSizeTrait},
    datatypes::DataType,
};
use datafusion_common::{
    cast::as_generic_string_array, exec_err, plan_err, Result, ScalarValue,
};
use datafusion_expr::{ColumnarValue, ScalarFunctionImplementation};
use datafusion_physical_expr::functions::Hint;
use std::{
    fmt::{Display, Formatter},
    sync::Arc,
};

/// Creates a function to identify the optimal return type of a string function given
/// the type of its first argument.
///
/// If the input type is `LargeUtf8` or `LargeBinary` the return type is
/// `$largeUtf8Type`,
///
/// If the input type is `Utf8` or `Binary` the return type is `$utf8Type`,
macro_rules! get_optimal_return_type {
    ($FUNC:ident, $largeUtf8Type:expr, $utf8Type:expr) => {
        fn $FUNC(arg_type: &DataType, name: &str) -> Result<DataType> {
            Ok(match arg_type {
                // LargeBinary inputs are automatically coerced to Utf8
                DataType::LargeUtf8 | DataType::LargeBinary => $largeUtf8Type,
                // Binary inputs are automatically coerced to Utf8
                DataType::Utf8 | DataType::Binary => $utf8Type,
                DataType::Null => DataType::Null,
                DataType::Dictionary(_, value_type) => match **value_type {
                    DataType::LargeUtf8 | DataType::LargeBinary => $largeUtf8Type,
                    DataType::Utf8 | DataType::Binary => $utf8Type,
                    DataType::Null => DataType::Null,
                    _ => {
                        return plan_err!(
                            "The {} function can only accept strings, but got {:?}.",
                            name.to_uppercase(),
                            **value_type
                        );
                    }
                },
                data_type => {
                    return plan_err!(
                        "The {} function can only accept strings, but got {:?}.",
                        name.to_uppercase(),
                        data_type
                    );
                }
            })
        }
    };
}

// `utf8_to_str_type`: returns either a Utf8 or LargeUtf8 based on the input type size.
get_optimal_return_type!(utf8_to_str_type, DataType::LargeUtf8, DataType::Utf8);

/// applies a unary expression to `args[0]` that is expected to be downcastable to
/// a `GenericStringArray` and returns a `GenericStringArray` (which may have a different offset)
/// # Errors
/// This function errors when:
/// * the number of arguments is not 1
/// * the first argument is not castable to a `GenericStringArray`
pub(crate) fn unary_string_function<'a, T, O, F, R>(
    args: &[&'a dyn Array],
    op: F,
    name: &str,
) -> Result<GenericStringArray<O>>
where
    R: AsRef<str>,
    O: OffsetSizeTrait,
    T: OffsetSizeTrait,
    F: Fn(&'a str) -> R,
{
    if args.len() != 1 {
        return exec_err!(
            "{:?} args were supplied but {} takes exactly one argument",
            args.len(),
            name
        );
    }

    let string_array = as_generic_string_array::<T>(args[0])?;

    // first map is the iterator, second is for the `Option<_>`
    Ok(string_array.iter().map(|string| string.map(&op)).collect())
}

fn handle<'a, F, R>(args: &'a [ColumnarValue], op: F, name: &str) -> Result<ColumnarValue>
where
    R: AsRef<str>,
    F: Fn(&'a str) -> R,
{
    match &args[0] {
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Utf8 => {
                Ok(ColumnarValue::Array(Arc::new(unary_string_function::<
                    i32,
                    i32,
                    _,
                    _,
                >(
                    &[a.as_ref()], op, name
                )?)))
            }
            DataType::LargeUtf8 => {
                Ok(ColumnarValue::Array(Arc::new(unary_string_function::<
                    i64,
                    i64,
                    _,
                    _,
                >(
                    &[a.as_ref()], op, name
                )?)))
            }
            other => exec_err!("Unsupported data type {other:?} for function {name}"),
        },
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Utf8(a) => {
                let result = a.as_ref().map(|x| (op)(x).as_ref().to_string());
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
            ScalarValue::LargeUtf8(a) => {
                let result = a.as_ref().map(|x| (op)(x).as_ref().to_string());
                Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(result)))
            }
            other => exec_err!("Unsupported data type {other:?} for function {name}"),
        },
    }
}

// TODO: mode allow[(dead_code)] after move ltrim and rtrim
enum TrimType {
    #[allow(dead_code)]
    Left,
    #[allow(dead_code)]
    Right,
    Both,
}

impl Display for TrimType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TrimType::Left => write!(f, "ltrim"),
            TrimType::Right => write!(f, "rtrim"),
            TrimType::Both => write!(f, "btrim"),
        }
    }
}

fn general_trim<T: OffsetSizeTrait>(
    args: &[ArrayRef],
    trim_type: TrimType,
) -> Result<ArrayRef> {
    let func = match trim_type {
        TrimType::Left => |input, pattern: &str| {
            let pattern = pattern.chars().collect::<Vec<char>>();
            str::trim_start_matches::<&[char]>(input, pattern.as_ref())
        },
        TrimType::Right => |input, pattern: &str| {
            let pattern = pattern.chars().collect::<Vec<char>>();
            str::trim_end_matches::<&[char]>(input, pattern.as_ref())
        },
        TrimType::Both => |input, pattern: &str| {
            let pattern = pattern.chars().collect::<Vec<char>>();
            str::trim_end_matches::<&[char]>(
                str::trim_start_matches::<&[char]>(input, pattern.as_ref()),
                pattern.as_ref(),
            )
        },
    };

    let string_array = as_generic_string_array::<T>(&args[0])?;

    match args.len() {
        1 => {
            let result = string_array
                .iter()
                .map(|string| string.map(|string: &str| func(string, " ")))
                .collect::<GenericStringArray<T>>();

            Ok(Arc::new(result) as ArrayRef)
        }
        2 => {
            let characters_array = as_generic_string_array::<T>(&args[1])?;

            let result = string_array
                .iter()
                .zip(characters_array.iter())
                .map(|(string, characters)| match (string, characters) {
                    (Some(string), Some(characters)) => Some(func(string, characters)),
                    _ => None,
                })
                .collect::<GenericStringArray<T>>();

            Ok(Arc::new(result) as ArrayRef)
        }
        other => {
            exec_err!(
            "{trim_type} was called with {other} arguments. It requires at least 1 and at most 2."
        )
        }
    }
}

pub(super) fn make_scalar_function<F>(
    inner: F,
    hints: Vec<Hint>,
) -> ScalarFunctionImplementation
where
    F: Fn(&[ArrayRef]) -> Result<ArrayRef> + Sync + Send + 'static,
{
    Arc::new(move |args: &[ColumnarValue]| {
        // first, identify if any of the arguments is an Array. If yes, store its `len`,
        // as any scalar will need to be converted to an array of len `len`.
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let is_scalar = len.is_none();

        let inferred_length = len.unwrap_or(1);
        let args = args
            .iter()
            .zip(hints.iter().chain(std::iter::repeat(&Hint::Pad)))
            .map(|(arg, hint)| {
                // Decide on the length to expand this scalar to depending
                // on the given hints.
                let expansion_len = match hint {
                    Hint::AcceptsSingular => 1,
                    Hint::Pad => inferred_length,
                };
                arg.clone().into_array(expansion_len)
            })
            .collect::<Result<Vec<_>>>()?;

        let result = (inner)(&args);
        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    })
}

mod starts_with;
mod to_hex;
mod trim;
mod upper;
// create UDFs
make_udf_function!(starts_with::StartsWithFunc, STARTS_WITH, starts_with);
make_udf_function!(to_hex::ToHexFunc, TO_HEX, to_hex);
make_udf_function!(trim::TrimFunc, TRIM, trim);
make_udf_function!(upper::UpperFunc, UPPER, upper);

export_functions!(
    (
    starts_with,
    arg1 arg2,
    "Returns true if string starts with prefix."),
    (
    to_hex,
    arg1,
    "Converts an integer to a hexadecimal string."),
    (trim,
    arg1,
    "removes all characters, space by default from the string"),
    (upper,
    arg1,
    "Converts a string to uppercase."));
