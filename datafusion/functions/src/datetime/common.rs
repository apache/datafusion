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

use arrow::array::{
    Array, ArrowPrimitiveType, AsArray, GenericStringArray, PrimitiveArray,
    StringArrayType, StringViewArray,
};
use arrow::compute::DecimalCast;
use arrow::datatypes::{DataType, TimeUnit};
use arrow_buffer::ArrowNativeType;
use datafusion_common::cast::as_generic_string_array;
use datafusion_common::{
    Result, ScalarValue, exec_err, internal_datafusion_err, unwrap_or_internal_err,
};
use datafusion_expr::ColumnarValue;

/// Checks that all the arguments from the second are of type [Utf8], [LargeUtf8] or [Utf8View]
///
/// [Utf8]: DataType::Utf8
/// [LargeUtf8]: DataType::LargeUtf8
/// [Utf8View]: DataType::Utf8View
pub(crate) fn validate_data_types(args: &[ColumnarValue], name: &str) -> Result<()> {
    for (idx, a) in args.iter().skip(1).enumerate() {
        match a.data_type() {
            DataType::Utf8View | DataType::LargeUtf8 | DataType::Utf8 => {
                // all good
            }
            _ => {
                return exec_err!(
                    "{name} function unsupported data type at index {}: {}",
                    idx + 1,
                    a.data_type()
                );
            }
        }
    }

    Ok(())
}

pub(crate) fn handle<O, F>(
    args: &[ColumnarValue],
    op: F,
    name: &str,
    dt: &DataType,
) -> Result<ColumnarValue>
where
    O: ArrowPrimitiveType,
    F: Fn(&str) -> Result<O::Native>,
{
    match &args[0] {
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Utf8View => Ok(ColumnarValue::Array(Arc::new(
                unary_string_to_primitive_function::<&StringViewArray, O, _>(
                    &a.as_string_view(),
                    op,
                )?,
            ))),
            DataType::LargeUtf8 => Ok(ColumnarValue::Array(Arc::new(
                unary_string_to_primitive_function::<&GenericStringArray<i64>, O, _>(
                    &a.as_string::<i64>(),
                    op,
                )?,
            ))),
            DataType::Utf8 => Ok(ColumnarValue::Array(Arc::new(
                unary_string_to_primitive_function::<&GenericStringArray<i32>, O, _>(
                    &a.as_string::<i32>(),
                    op,
                )?,
            ))),
            other => exec_err!("Unsupported data type {other:?} for function {name}"),
        },
        ColumnarValue::Scalar(scalar) => match scalar.try_as_str() {
            Some(a) => {
                let result = a
                    .as_ref()
                    .map(|x| op(x))
                    .transpose()?
                    .and_then(|v| v.to_i64());
                let s = scalar_value(dt, result)?;
                Ok(ColumnarValue::Scalar(s))
            }
            _ => exec_err!("Unsupported data type {scalar:?} for function {name}"),
        },
    }
}

// Given a function that maps a `&str`, `&str` to an arrow native type,
// returns a `ColumnarValue` where the function is applied to either a `ArrayRef` or `ScalarValue`
// depending on the `args`'s variant.
pub(crate) fn handle_multiple<O, F, M>(
    args: &[ColumnarValue],
    op: F,
    op2: M,
    name: &str,
    dt: &DataType,
) -> Result<ColumnarValue>
where
    O: ArrowPrimitiveType,
    F: Fn(&str, &[&str]) -> Result<O::Native>,
    M: Fn(O::Native) -> O::Native,
{
    match &args[0] {
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Utf8View | DataType::LargeUtf8 | DataType::Utf8 => {
                // validate the column types
                for (pos, arg) in args.iter().enumerate() {
                    match arg {
                        ColumnarValue::Array(arg) => match arg.data_type() {
                            DataType::Utf8View | DataType::LargeUtf8 | DataType::Utf8 => {
                                // all good
                            }
                            other => {
                                return exec_err!(
                                    "Unsupported data type {other:?} for function {name}, arg # {pos}"
                                );
                            }
                        },
                        ColumnarValue::Scalar(arg) => {
                            match arg.data_type() {
                                DataType::Utf8View
                                | DataType::LargeUtf8
                                | DataType::Utf8 => {
                                    // all good
                                }
                                other => {
                                    return exec_err!(
                                        "Unsupported data type {other:?} for function {name}, arg # {pos}"
                                    );
                                }
                            }
                        }
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(
                    strings_to_primitive_function::<O, _, _>(args, op, op2, name)?,
                )))
            }
            other => {
                exec_err!("Unsupported data type {other:?} for function {name}")
            }
        },
        // if the first argument is a scalar utf8 all arguments are expected to be scalar utf8
        ColumnarValue::Scalar(scalar) => match scalar.try_as_str() {
            Some(a) => {
                let mut vals = vec![];

                for (pos, v) in args.iter().enumerate().skip(1) {
                    let ColumnarValue::Scalar(
                        ScalarValue::Utf8View(x)
                        | ScalarValue::LargeUtf8(x)
                        | ScalarValue::Utf8(x),
                    ) = v
                    else {
                        return exec_err!(
                            "Unsupported data type {v:?} for function {name}, arg # {pos}"
                        );
                    };

                    if let Some(s) = x {
                        vals.push(s.as_str());
                    }
                }

                let a = a.as_ref();
                // ASK: Why do we trust `a` to be non-null at this point?
                let a = unwrap_or_internal_err!(a);

                let ret = match op(a, &vals) {
                    Ok(r) => {
                        let result = op2(r).to_i64();
                        let s = scalar_value(dt, result)?;
                        Some(Ok(ColumnarValue::Scalar(s)))
                    }
                    Err(e) => Some(Err(e)),
                };

                unwrap_or_internal_err!(ret)
            }
            other => {
                exec_err!("Unsupported data type {other:?} for function {name}")
            }
        },
    }
}

/// given a function `op` that maps `&str`, `&str` to the first successful Result
/// of an arrow native type, returns a `PrimitiveArray` after the application of the
/// function to `args` and the subsequence application of the `op2` function to any
/// successful result. This function calls the `op` function with the first and second
/// argument and if not successful continues with first and third, first and fourth,
/// etc until the result was successful or no more arguments are present.
/// # Errors
/// This function errors iff:
/// * the number of arguments is not > 1 or
/// * the function `op` errors for all input
pub(crate) fn strings_to_primitive_function<O, F, F2>(
    args: &[ColumnarValue],
    op: F,
    op2: F2,
    name: &str,
) -> Result<PrimitiveArray<O>>
where
    O: ArrowPrimitiveType,
    F: Fn(&str, &[&str]) -> Result<O::Native>,
    F2: Fn(O::Native) -> O::Native,
{
    if args.len() < 2 {
        return exec_err!(
            "{:?} args were supplied but {} takes 2 or more arguments",
            args.len(),
            name
        );
    }

    match &args[0] {
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Utf8View => {
                let string_array = a.as_string_view();
                handle_array_op::<O, &StringViewArray, F, F2>(
                    &string_array,
                    &args[1..],
                    op,
                    op2,
                )
            }
            DataType::LargeUtf8 => {
                let string_array = as_generic_string_array::<i64>(&a)?;
                handle_array_op::<O, &GenericStringArray<i64>, F, F2>(
                    &string_array,
                    &args[1..],
                    op,
                    op2,
                )
            }
            DataType::Utf8 => {
                let string_array = as_generic_string_array::<i32>(&a)?;
                handle_array_op::<O, &GenericStringArray<i32>, F, F2>(
                    &string_array,
                    &args[1..],
                    op,
                    op2,
                )
            }
            other => exec_err!(
                "Unsupported data type {other:?} for function substr,\
                    expected Utf8View, Utf8 or LargeUtf8."
            ),
        },
        other => exec_err!(
            "Received {} data type, expected only array",
            other.data_type()
        ),
    }
}

fn handle_array_op<'a, O, V, F, F2>(
    first: &V,
    args: &[ColumnarValue],
    op: F,
    op2: F2,
) -> Result<PrimitiveArray<O>>
where
    V: StringArrayType<'a>,
    O: ArrowPrimitiveType,
    F: Fn(&str, &[&str]) -> Result<O::Native>,
    F2: Fn(O::Native) -> O::Native,
{
    first
        .iter()
        .enumerate()
        .map(|(pos, x)| {
            let mut val = None;
            if let Some(x) = x {
                let mut v = vec![];

                for arg in args {
                    match arg {
                        ColumnarValue::Array(a) => match a.data_type() {
                            DataType::Utf8View => v.push(a.as_string_view().value(pos)),
                            DataType::LargeUtf8 => {
                                v.push(a.as_string::<i64>().value(pos))
                            }
                            DataType::Utf8 => v.push(a.as_string::<i32>().value(pos)),
                            other => {
                                return exec_err!(
                                    "Unexpected type encountered '{other}'"
                                );
                            }
                        },
                        ColumnarValue::Scalar(s) => match s.try_as_str() {
                            Some(Some(s)) => v.push(s),
                            Some(None) => continue, // null string
                            None => {
                                return exec_err!(
                                    "Unexpected scalar type encountered '{s}'"
                                );
                            }
                        },
                    };
                }

                if !v.is_empty() {
                    let r = op(x, &v);
                    if let Ok(inner) = r {
                        val = Some(Ok(op2(inner)));
                    } else {
                        val = Some(r);
                    }
                }
            };

            val.transpose()
        })
        .collect()
}

/// given a function `op` that maps a `&str` to a Result of an arrow native type,
/// returns a `PrimitiveArray` after the application
/// of the function to `args[0]`.
/// # Errors
/// This function errors iff:
/// * the number of arguments is not 1 or
/// * the function `op` errors
fn unary_string_to_primitive_function<'a, StringArrType, O, F>(
    array: &StringArrType,
    op: F,
) -> Result<PrimitiveArray<O>>
where
    StringArrType: StringArrayType<'a>,
    O: ArrowPrimitiveType,
    F: Fn(&'a str) -> Result<O::Native>,
{
    // first map is the iterator, second is for the `Option<_>`
    array.iter().map(|x| x.map(&op).transpose()).collect()
}

fn scalar_value(dt: &DataType, r: Option<i64>) -> Result<ScalarValue> {
    match dt {
        DataType::Date32 => Ok(ScalarValue::Date32(r.and_then(|v| v.to_i32()))),
        DataType::Timestamp(u, tz) => match u {
            TimeUnit::Second => Ok(ScalarValue::TimestampSecond(r, tz.clone())),
            TimeUnit::Millisecond => Ok(ScalarValue::TimestampMillisecond(r, tz.clone())),
            TimeUnit::Microsecond => Ok(ScalarValue::TimestampMicrosecond(r, tz.clone())),
            TimeUnit::Nanosecond => Ok(ScalarValue::TimestampNanosecond(r, tz.clone())),
        },
        t => Err(internal_datafusion_err!("Unsupported data type: {t:?}")),
    }
}
