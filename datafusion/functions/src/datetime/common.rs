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
    Array, ArrowPrimitiveType, GenericStringArray, OffsetSizeTrait, PrimitiveArray,
};
use arrow::compute::kernels::cast_utils::string_to_timestamp_nanos;
use arrow::datatypes::DataType;
use chrono::format::{parse, Parsed, StrftimeItems};
use chrono::LocalResult::Single;
use chrono::{DateTime, TimeZone, Utc};
use itertools::Either;

use datafusion_common::cast::as_generic_string_array;
use datafusion_common::{
    exec_err, unwrap_or_internal_err, DataFusionError, Result, ScalarType, ScalarValue,
};
use datafusion_expr::ColumnarValue;

/// Error message if nanosecond conversion request beyond supported interval
const ERR_NANOSECONDS_NOT_SUPPORTED: &str = "The dates that can be represented as nanoseconds have to be between 1677-09-21T00:12:44.0 and 2262-04-11T23:47:16.854775804";

/// Calls string_to_timestamp_nanos and converts the error type
pub(crate) fn string_to_timestamp_nanos_shim(s: &str) -> Result<i64> {
    string_to_timestamp_nanos(s).map_err(|e| e.into())
}

/// Checks that all the arguments from the second are of type [Utf8] or [LargeUtf8]
///
/// [Utf8]: DataType::Utf8
/// [LargeUtf8]: DataType::LargeUtf8
pub(crate) fn validate_data_types(args: &[ColumnarValue], name: &str) -> Result<()> {
    for (idx, a) in args.iter().skip(1).enumerate() {
        match a.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => {
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

/// Accepts a string and parses it using the [`chrono::format::strftime`] specifiers
/// relative to the provided `timezone`
///
/// [IANA timezones] are only supported if the `arrow-array/chrono-tz` feature is enabled
///
/// * `2023-01-01 040506 America/Los_Angeles`
///
/// If a timestamp is ambiguous, for example as a result of daylight-savings time, an error
/// will be returned
///
/// [`chrono::format::strftime`]: https://docs.rs/chrono/latest/chrono/format/strftime/index.html
/// [IANA timezones]: https://www.iana.org/time-zones
pub(crate) fn string_to_datetime_formatted<T: TimeZone>(
    timezone: &T,
    s: &str,
    format: &str,
) -> Result<DateTime<T>, DataFusionError> {
    let err = |err_ctx: &str| {
        DataFusionError::Execution(format!(
            "Error parsing timestamp from '{s}' using format '{format}': {err_ctx}"
        ))
    };

    let mut parsed = Parsed::new();
    parse(&mut parsed, s, StrftimeItems::new(format)).map_err(|e| err(&e.to_string()))?;

    // attempt to parse the string assuming it has a timezone
    let dt = parsed.to_datetime();

    if let Err(e) = &dt {
        // no timezone or other failure, try without a timezone
        let ndt = parsed
            .to_naive_datetime_with_offset(0)
            .or_else(|_| parsed.to_naive_date().map(|nd| nd.into()));
        if let Err(e) = &ndt {
            return Err(err(&e.to_string()));
        }

        if let Single(e) = &timezone.from_local_datetime(&ndt.unwrap()) {
            Ok(e.to_owned())
        } else {
            Err(err(&e.to_string()))
        }
    } else {
        Ok(dt.unwrap().with_timezone(timezone))
    }
}

/// Accepts a string with a `chrono` format and converts it to a
/// nanosecond precision timestamp.
///
/// See [`chrono::format::strftime`] for the full set of supported formats.
///
/// Implements the `to_timestamp` function to convert a string to a
/// timestamp, following the model of spark SQL’s to_`timestamp`.
///
/// Internally, this function uses the `chrono` library for the
/// datetime parsing
///
/// ## Timestamp Precision
///
/// Function uses the maximum precision timestamps supported by
/// Arrow (nanoseconds stored as a 64-bit integer) timestamps. This
/// means the range of dates that timestamps can represent is ~1677 AD
/// to 2262 AM
///
/// ## Timezone / Offset Handling
///
/// Numerical values of timestamps are stored compared to offset UTC.
///
/// Any timestamp in the formatting string is handled according to the rules
/// defined by `chrono`.
///
/// [`chrono::format::strftime`]: https://docs.rs/chrono/latest/chrono/format/strftime/index.html
///
#[inline]
pub(crate) fn string_to_timestamp_nanos_formatted(
    s: &str,
    format: &str,
) -> Result<i64, DataFusionError> {
    string_to_datetime_formatted(&Utc, s, format)?
        .naive_utc()
        .and_utc()
        .timestamp_nanos_opt()
        .ok_or_else(|| {
            DataFusionError::Execution(ERR_NANOSECONDS_NOT_SUPPORTED.to_string())
        })
}

/// Accepts a string with a `chrono` format and converts it to a
/// millisecond precision timestamp.
///
/// See [`chrono::format::strftime`] for the full set of supported formats.
///
/// Internally, this function uses the `chrono` library for the
/// datetime parsing
///
/// ## Timezone / Offset Handling
///
/// Numerical values of timestamps are stored compared to offset UTC.
///
/// Any timestamp in the formatting string is handled according to the rules
/// defined by `chrono`.
///
/// [`chrono::format::strftime`]: https://docs.rs/chrono/latest/chrono/format/strftime/index.html
///
#[inline]
pub(crate) fn string_to_timestamp_millis_formatted(s: &str, format: &str) -> Result<i64> {
    Ok(string_to_datetime_formatted(&Utc, s, format)?
        .naive_utc()
        .and_utc()
        .timestamp_millis())
}

pub(crate) fn handle<'a, O, F, S>(
    args: &'a [ColumnarValue],
    op: F,
    name: &str,
) -> Result<ColumnarValue>
where
    O: ArrowPrimitiveType,
    S: ScalarType<O::Native>,
    F: Fn(&'a str) -> Result<O::Native>,
{
    match &args[0] {
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => Ok(ColumnarValue::Array(Arc::new(
                unary_string_to_primitive_function::<i32, O, _>(&[a.as_ref()], op, name)?,
            ))),
            other => exec_err!("Unsupported data type {other:?} for function {name}"),
        },
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Utf8(a) | ScalarValue::LargeUtf8(a) => {
                let result = a.as_ref().map(|x| (op)(x)).transpose()?;
                Ok(ColumnarValue::Scalar(S::scalar(result)))
            }
            other => exec_err!("Unsupported data type {other:?} for function {name}"),
        },
    }
}

// given an function that maps a `&str`, `&str` to an arrow native type,
// returns a `ColumnarValue` where the function is applied to either a `ArrayRef` or `ScalarValue`
// depending on the `args`'s variant.
pub(crate) fn handle_multiple<'a, O, F, S, M>(
    args: &'a [ColumnarValue],
    op: F,
    op2: M,
    name: &str,
) -> Result<ColumnarValue>
where
    O: ArrowPrimitiveType,
    S: ScalarType<O::Native>,
    F: Fn(&'a str, &'a str) -> Result<O::Native>,
    M: Fn(O::Native) -> O::Native,
{
    match &args[0] {
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => {
                // validate the column types
                for (pos, arg) in args.iter().enumerate() {
                    match arg {
                        ColumnarValue::Array(arg) => match arg.data_type() {
                            DataType::Utf8 | DataType::LargeUtf8 => {
                                // all good
                            }
                            other => return exec_err!("Unsupported data type {other:?} for function {name}, arg # {pos}"),
                        },
                        ColumnarValue::Scalar(arg) => {
                            match arg.data_type() {
                                DataType::Utf8 | DataType::LargeUtf8 => {
                                    // all good
                                }
                                other => return exec_err!("Unsupported data type {other:?} for function {name}, arg # {pos}"),
                            }
                        }
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(
                    strings_to_primitive_function::<i32, O, _, _>(args, op, op2, name)?,
                )))
            }
            other => {
                exec_err!("Unsupported data type {other:?} for function {name}")
            }
        },
        // if the first argument is a scalar utf8 all arguments are expected to be scalar utf8
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Utf8(a) | ScalarValue::LargeUtf8(a) => {
                let a = a.as_ref();
                // ASK: Why do we trust `a` to be non-null at this point?
                let a = unwrap_or_internal_err!(a);

                let mut ret = None;

                for (pos, v) in args.iter().enumerate().skip(1) {
                    let ColumnarValue::Scalar(
                        ScalarValue::Utf8(x) | ScalarValue::LargeUtf8(x),
                    ) = v
                    else {
                        return exec_err!("Unsupported data type {v:?} for function {name}, arg # {pos}");
                    };

                    if let Some(s) = x {
                        match op(a.as_str(), s.as_str()) {
                            Ok(r) => {
                                ret = Some(Ok(ColumnarValue::Scalar(S::scalar(Some(
                                    op2(r),
                                )))));
                                break;
                            }
                            Err(e) => ret = Some(Err(e)),
                        }
                    }
                }

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
/// * the array arguments are not castable to a `GenericStringArray` or
/// * the function `op` errors for all input
pub(crate) fn strings_to_primitive_function<'a, T, O, F, F2>(
    args: &'a [ColumnarValue],
    op: F,
    op2: F2,
    name: &str,
) -> Result<PrimitiveArray<O>>
where
    O: ArrowPrimitiveType,
    T: OffsetSizeTrait,
    F: Fn(&'a str, &'a str) -> Result<O::Native>,
    F2: Fn(O::Native) -> O::Native,
{
    if args.len() < 2 {
        return exec_err!(
            "{:?} args were supplied but {} takes 2 or more arguments",
            args.len(),
            name
        );
    }

    // this will throw the error if any of the array args are not castable to GenericStringArray
    let data = args
        .iter()
        .map(|a| match a {
            ColumnarValue::Array(a) => {
                Ok(Either::Left(as_generic_string_array::<T>(a.as_ref())?))
            }
            ColumnarValue::Scalar(s) => match s {
                ScalarValue::Utf8(a) | ScalarValue::LargeUtf8(a) => Ok(Either::Right(a)),
                other => exec_err!(
                    "Unexpected scalar type encountered '{other}' for function '{name}'"
                ),
            },
        })
        .collect::<Result<Vec<Either<&GenericStringArray<T>, &Option<String>>>>>()?;

    let first_arg = &data.first().unwrap().left().unwrap();

    first_arg
        .iter()
        .enumerate()
        .map(|(pos, x)| {
            let mut val = None;

            if let Some(x) = x {
                let param_args = data.iter().skip(1);

                // go through the args and find the first successful result. Only the last
                // failure will be returned if no successful result was received.
                for param_arg in param_args {
                    // param_arg is an array, use the corresponding index into the array as the arg
                    // we're currently parsing
                    let p = *param_arg;
                    let r = if p.is_left() {
                        let p = p.left().unwrap();
                        op(x, p.value(pos))
                    }
                    // args is a scalar, use it directly
                    else if let Some(p) = p.right().unwrap() {
                        op(x, p.as_str())
                    } else {
                        continue;
                    };

                    if r.is_ok() {
                        val = Some(Ok(op2(r.unwrap())));
                        break;
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
/// * the first argument is not castable to a `GenericStringArray` or
/// * the function `op` errors
fn unary_string_to_primitive_function<'a, T, O, F>(
    args: &[&'a dyn Array],
    op: F,
    name: &str,
) -> Result<PrimitiveArray<O>>
where
    O: ArrowPrimitiveType,
    T: OffsetSizeTrait,
    F: Fn(&'a str) -> Result<O::Native>,
{
    if args.len() != 1 {
        return exec_err!(
            "{:?} args were supplied but {} takes exactly one argument",
            args.len(),
            name
        );
    }

    let array = as_generic_string_array::<T>(args[0])?;

    // first map is the iterator, second is for the `Option<_>`
    array.iter().map(|x| x.map(&op).transpose()).collect()
}
