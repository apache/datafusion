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

use std::iter::repeat;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Datum, GenericStringArray, Int64Array, OffsetSizeTrait,
    Scalar,
};
use arrow::datatypes::DataType::{self, Int64, LargeUtf8, Utf8};
use arrow::datatypes::Int64Type;
use arrow::error::ArrowError;
use datafusion_common::cast::{as_generic_string_array, as_primitive_array};
use datafusion_common::{
    arrow_err, exec_err, internal_err, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use itertools::izip;
use regex::Regex;

/// regexp_count(string, pattern [, start [, flags ]]) -> int
#[derive(Debug)]
pub struct RegexpCountFunc {
    signature: Signature,
}

impl Default for RegexpCountFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexpCountFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Utf8, Utf8]),
                    Exact(vec![Utf8, Utf8, Int64]),
                    Exact(vec![Utf8, Utf8, Int64, Utf8]),
                    Exact(vec![Utf8, Utf8, Int64, LargeUtf8]),
                    Exact(vec![LargeUtf8, LargeUtf8]),
                    Exact(vec![LargeUtf8, LargeUtf8, Int64]),
                    Exact(vec![LargeUtf8, LargeUtf8, Int64, Utf8]),
                    Exact(vec![LargeUtf8, LargeUtf8, Int64, LargeUtf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpCountFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_count"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;

        Ok(match &arg_types[0] {
            Null => Null,
            _ => Int64,
        })
    }

    fn invoke(&self, args: &[datafusion_expr::ColumnarValue]) -> Result<ColumnarValue> {
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
            .map(|arg| arg.clone().into_array(inferred_length))
            .collect::<Result<Vec<_>>>()?;

        let result = regexp_count_func(&args);
        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    }
}

fn regexp_count_func(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Utf8 => regexp_count::<i32>(args),
        DataType::LargeUtf8 => regexp_count::<i64>(args),
        other => {
            internal_err!("Unsupported data type {other:?} for function regexp_like")
        }
    }
}

pub fn regexp_count<O: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let arg_len = args.len();

    match arg_len {
        2..=4 => {
            let values = as_generic_string_array::<O>(&args[0])?;
            let regex = as_generic_string_array::<O>(&args[1])?;
            let regex_datum: &dyn Datum = if regex.len() != 1 {
                regex
            } else {
                &Scalar::new(regex)
            };
            let start_scalar: Scalar<&Int64Array>;
            let start_array_datum: Option<&dyn Datum> = if arg_len > 2 {
                let start_array = as_primitive_array::<Int64Type>(&args[2])?;
                if start_array.len() != 1 {
                    Some(start_array as &dyn Datum)
                } else {
                    start_scalar = Scalar::new(start_array);
                    Some(&start_scalar as &dyn Datum)
                }
            } else {
                None
            };

            let flags_scalar: Scalar<&GenericStringArray<O>>;
            let flags_array_datum: Option<&dyn Datum> = if arg_len > 3 {
                let flags_array = as_generic_string_array::<O>(&args[3])?;
                if flags_array.len() != 1 {
                    Some(flags_array as &dyn Datum)
                } else {
                    flags_scalar = Scalar::new(flags_array);
                    Some(&flags_scalar as &dyn Datum)
                }
            } else {
                None
            };

            Ok(regexp_array_count(
                values,
                regex_datum,
                start_array_datum,
                flags_array_datum,
            )
            .map(|x| Arc::new(x) as ArrayRef)?)
        }
        other => {
            exec_err!(
            "regexp_count was called with {other} arguments. It requires at least 2 and at most 4."
             )
        }
    }
}

pub fn regexp_array_count<O: OffsetSizeTrait>(
    array: &GenericStringArray<O>,
    regex_array: &dyn Datum,
    start_array: Option<&dyn Datum>,
    flags_array: Option<&dyn Datum>,
) -> Result<Int64Array> {
    let (rhs, is_rhs_scalar) = regex_array.get();

    if array.data_type() != rhs.data_type() {
        return arrow_err!(ArrowError::ComputeError(
            "regexp_count() requires pattern to be either Utf8 or LargeUtf8".to_string(),
        ));
    }

    if !is_rhs_scalar && array.len() != rhs.len() {
        return arrow_err!(
            ArrowError::ComputeError(
                "regexp_count() requires pattern to be either a scalar or the same length as the input array".to_string(),
            )
        );
    }

    let (starts, is_starts_scalar) = match start_array {
        Some(starts) => starts.get(),
        None => (&Int64Array::from(vec![1]) as &dyn Array, true),
    };

    if *starts.data_type() != Int64 {
        return arrow_err!(ArrowError::ComputeError(
            "regexp_count() requires start to be Int64".to_string(),
        ));
    }

    if !is_starts_scalar && array.len() != starts.len() {
        return arrow_err!(
            ArrowError::ComputeError(
                "regexp_count() requires start to be either a scalar or the same length as the input array".to_string(),
            )
        );
    }

    let (flags, is_flags_scalar) = match flags_array {
        Some(flags) => flags.get(),
        None => (
            &GenericStringArray::<O>::from(vec![None as Option<&str>]) as &dyn Array,
            true,
        ),
    };

    if !is_flags_scalar && array.len() != flags.len() {
        return arrow_err!(
            ArrowError::ComputeError(
                "regexp_count() requires flags to be either a scalar or the same length as the input array".to_string(),
            )
        );
    }

    let regex_iter: Box<dyn Iterator<Item = Option<&str>>> = if is_rhs_scalar {
        let regex: &arrow::array::GenericByteArray<
            arrow::datatypes::GenericStringType<O>,
        > = rhs.as_string::<O>();
        let regex = regex.is_valid(0).then(|| regex.value(0));
        if regex.is_none() {
            return Ok(Int64Array::from(
                repeat(0).take(array.len()).collect::<Vec<_>>(),
            ));
        }

        Box::new(repeat(regex))
    } else {
        Box::new(rhs.as_string::<O>().iter())
    };

    let start_iter: Box<dyn Iterator<Item = i64>> = if is_starts_scalar {
        let start = starts.as_primitive::<Int64Type>();
        let start = start.is_valid(0).then(|| start.value(0));
        Box::new(repeat(start.unwrap_or(1)))
    } else {
        Box::new(
            starts
                .as_primitive::<Int64Type>()
                .iter()
                .map(|x| x.unwrap_or(1)),
        )
    };

    let flags_iter: Box<dyn Iterator<Item = Option<&str>>> = if is_flags_scalar {
        let flags = flags.as_string::<O>();
        let flags = flags
            .is_valid(0)
            .then(|| flags.value(0))
            .map(|x| {
                if x.contains('g') {
                    return arrow_err!(ArrowError::ComputeError(
                        "regexp_count() does not support global flag".to_string(),
                    ));
                }
                Ok(x)
            })
            .transpose()?;

        Box::new(repeat(flags))
    } else {
        Box::new(flags.as_string::<O>().iter())
    };

    regex_iter_count(array.iter(), regex_iter, start_iter, flags_iter)
}

fn regex_iter_count<'a>(
    array: impl Iterator<Item = Option<&'a str>>,
    regex: impl Iterator<Item = Option<&'a str>>,
    start: impl Iterator<Item = i64>,
    flags: impl Iterator<Item = Option<&'a str>>,
) -> Result<Int64Array> {
    Ok(Int64Array::from(
        izip!(array, regex, start, flags)
            .map(|(array, regex, start, flags)| {
                if array.is_none() || regex.is_none() {
                    return Ok(0);
                }

                let regex = regex.unwrap();
                if regex.is_empty() {
                    return Ok(0);
                }

                if start <= 0 {
                    return Err(ArrowError::ComputeError(
                        "regexp_count() requires start to be 1 based".to_string(),
                    ));
                }

                let array = array.unwrap();
                let start = start as usize;
                if start > array.len() {
                    return Ok(0);
                }

                let pattern = if let Some(Some(flags)) =
                    flags.map(|x| if x.is_empty() { None } else { Some(x) })
                {
                    if flags.contains('g') {
                        return Err(ArrowError::ComputeError(
                            "regexp_count() does not support global flag".to_string(),
                        ));
                    }

                    format!("(?{flags}){regex}")
                } else {
                    regex.to_string()
                };

                let Ok(re) = Regex::new(pattern.as_str()) else {
                    return Err(ArrowError::ComputeError(format!(
                        "Regular expression did not compile: {}",
                        pattern
                    )));
                };

                Ok(re
                    .find_iter(&array.chars().skip(start - 1).collect::<String>())
                    .count() as i64)
            })
            .collect::<Result<Vec<i64>, ArrowError>>()?,
    ))
}

#[cfg(test)]
mod tests {
    use crate::regex::regexpcount::regexp_count;
    use arrow::array::{ArrayRef, GenericStringArray, Int64Array, OffsetSizeTrait};
    use std::sync::Arc;

    #[test]
    fn test_regexp_count() {
        test_case_sensitive_regexp_count_scalar::<i32>();
        test_case_sensitive_regexp_count_scalar::<i64>();

        test_case_sensitive_regexp_count_scalar_start::<i32>();
        test_case_sensitive_regexp_count_scalar_start::<i64>();

        test_case_insensitive_regexp_count_scalar_flags::<i32>();
        test_case_insensitive_regexp_count_scalar_flags::<i64>();

        test_case_sensitive_regexp_count_array::<i32>();
        test_case_sensitive_regexp_count_array::<i64>();

        test_case_sensitive_regexp_count_array_start::<i32>();
        test_case_sensitive_regexp_count_array_start::<i64>();

        test_case_insensitive_regexp_count_array_flags::<i32>();
        test_case_insensitive_regexp_count_array_flags::<i64>();

        test_case_sensitive_regexp_count_start_scalar_complex::<i32>();
        test_case_sensitive_regexp_count_start_scalar_complex::<i64>();

        test_case_sensitive_regexp_count_array_complex::<i32>();
        test_case_sensitive_regexp_count_array_complex::<i64>();
    }

    fn test_case_sensitive_regexp_count_scalar<O: OffsetSizeTrait>() {
        let values = GenericStringArray::<O>::from(vec![
            "",
            "aabca",
            "abcabc",
            "abcAbcab",
            "abcabcabc",
        ]);
        let regex = GenericStringArray::<O>::from(vec!["abc"; 1]);

        let expected = Int64Array::from(vec![0, 1, 2, 1, 3]);

        let re = regexp_count::<O>(&[
            Arc::new(values) as ArrayRef,
            Arc::new(regex) as ArrayRef,
        ])
        .unwrap();
        assert_eq!(re.as_ref(), &expected);
    }

    fn test_case_sensitive_regexp_count_scalar_start<O: OffsetSizeTrait>() {
        let values = GenericStringArray::<O>::from(vec![
            "",
            "aabca",
            "abcabc",
            "abcAbcab",
            "abcabcabc",
        ]);
        let regex = GenericStringArray::<O>::from(vec!["abc"; 1]);
        let start = Int64Array::from(vec![2]);

        let expected = Int64Array::from(vec![0, 1, 1, 0, 2]);

        let re = regexp_count::<O>(&[
            Arc::new(values) as ArrayRef,
            Arc::new(regex) as ArrayRef,
            Arc::new(start) as ArrayRef,
        ])
        .unwrap();
        assert_eq!(re.as_ref(), &expected);
    }

    fn test_case_insensitive_regexp_count_scalar_flags<O: OffsetSizeTrait>() {
        let values = GenericStringArray::<O>::from(vec![
            "",
            "aabca",
            "abcabc",
            "abcAbcab",
            "abcabcabc",
        ]);
        let regex = GenericStringArray::<O>::from(vec!["abc"; 1]);
        let start = Int64Array::from(vec![1]);
        let flags = GenericStringArray::<O>::from(vec!["i"]);

        let expected = Int64Array::from(vec![0, 1, 2, 2, 3]);

        let re = regexp_count::<O>(&[
            Arc::new(values) as ArrayRef,
            Arc::new(regex) as ArrayRef,
            Arc::new(start) as ArrayRef,
            Arc::new(flags) as ArrayRef,
        ])
        .unwrap();
        assert_eq!(re.as_ref(), &expected);
    }

    fn test_case_sensitive_regexp_count_array<O: OffsetSizeTrait>() {
        let values = GenericStringArray::<O>::from(vec![
            "",
            "aabca",
            "abcabc",
            "abcAbcab",
            "abcabcAbc",
        ]);
        let regex = GenericStringArray::<O>::from(vec!["", "abc", "a", "bc", "ab"]);

        let expected = Int64Array::from(vec![0, 1, 2, 2, 2]);

        let re = regexp_count::<O>(&[
            Arc::new(values) as ArrayRef,
            Arc::new(regex) as ArrayRef,
        ])
        .unwrap();
        assert_eq!(re.as_ref(), &expected);
    }

    fn test_case_sensitive_regexp_count_array_start<O: OffsetSizeTrait>() {
        let values = GenericStringArray::<O>::from(vec![
            "",
            "aAbca",
            "abcabc",
            "abcAbcab",
            "abcabcAbc",
        ]);
        let regex = GenericStringArray::<O>::from(vec!["", "abc", "a", "bc", "ab"]);
        let start = Int64Array::from(vec![1, 2, 3, 4, 5]);

        let expected = Int64Array::from(vec![0, 0, 1, 1, 0]);

        let re = regexp_count::<O>(&[
            Arc::new(values) as ArrayRef,
            Arc::new(regex) as ArrayRef,
            Arc::new(start) as ArrayRef,
        ])
        .unwrap();
        assert_eq!(re.as_ref(), &expected);
    }

    fn test_case_insensitive_regexp_count_array_flags<O: OffsetSizeTrait>() {
        let values = GenericStringArray::<O>::from(vec![
            "",
            "aAbca",
            "abcabc",
            "abcAbcab",
            "abcabcAbc",
        ]);
        let regex = GenericStringArray::<O>::from(vec!["", "abc", "a", "bc", "ab"]);
        let start = Int64Array::from(vec![1]);
        let flags = GenericStringArray::<O>::from(vec!["", "i", "", "", "i"]);

        let expected = Int64Array::from(vec![0, 1, 2, 2, 3]);

        let re = regexp_count::<O>(&[
            Arc::new(values) as ArrayRef,
            Arc::new(regex) as ArrayRef,
            Arc::new(start) as ArrayRef,
            Arc::new(flags) as ArrayRef,
        ])
        .unwrap();
        assert_eq!(re.as_ref(), &expected);
    }

    fn test_case_sensitive_regexp_count_start_scalar_complex<O: OffsetSizeTrait>() {
        let values = GenericStringArray::<O>::from(vec![
            "",
            "aAbca",
            "abcabc",
            "abcAbcabc",
            "abcabcAbc",
        ]);
        let regex = GenericStringArray::<O>::from(vec!["", "abc", "a", "bc", "ab"]);
        let start = Int64Array::from(vec![5]);
        let flags = GenericStringArray::<O>::from(vec!["", "i", "", "", "i"]);

        let expected = Int64Array::from(vec![0, 0, 0, 2, 1]);

        let re = regexp_count::<O>(&[
            Arc::new(values) as ArrayRef,
            Arc::new(regex) as ArrayRef,
            Arc::new(start) as ArrayRef,
            Arc::new(flags) as ArrayRef,
        ])
        .unwrap();
        assert_eq!(re.as_ref(), &expected);
    }

    fn test_case_sensitive_regexp_count_array_complex<O: OffsetSizeTrait>() {
        let values = GenericStringArray::<O>::from(vec![
            "",
            "aAbca",
            "abcabc",
            "abcAbcab",
            "abcabcAbc",
        ]);
        let regex = GenericStringArray::<O>::from(vec!["", "abc", "a", "bc", "ab"]);
        let start = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let flags = GenericStringArray::<O>::from(vec!["", "i", "", "", "i"]);

        let expected = Int64Array::from(vec![0, 1, 1, 1, 1]);

        let re = regexp_count::<O>(&[
            Arc::new(values) as ArrayRef,
            Arc::new(regex) as ArrayRef,
            Arc::new(start) as ArrayRef,
            Arc::new(flags) as ArrayRef,
        ])
        .unwrap();
        assert_eq!(re.as_ref(), &expected);
    }
}
