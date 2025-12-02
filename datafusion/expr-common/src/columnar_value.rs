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

//! [`ColumnarValue`] represents the result of evaluating an expression.

use arrow::{
    array::{Array, ArrayRef, Date32Array, Date64Array, NullArray},
    compute::{CastOptions, kernels, max, min},
    datatypes::DataType,
    util::pretty::pretty_format_columns,
};
use datafusion_common::internal_datafusion_err;
use datafusion_common::{
    Result, ScalarValue,
    format::DEFAULT_CAST_OPTIONS,
    internal_err,
    scalar::{date_to_timestamp_multiplier, ensure_timestamp_in_bounds},
};
use std::fmt;
use std::sync::Arc;

/// The result of evaluating an expression.
///
/// [`ColumnarValue::Scalar`] represents a single value repeated any number of
/// times. This is an important performance optimization for handling values
/// that do not change across rows.
///
/// [`ColumnarValue::Array`] represents a column of data, stored as an  Arrow
/// [`ArrayRef`]
///
/// A slice of `ColumnarValue`s logically represents a table, with each column
/// having the same number of rows. This means that all `Array`s are the same
/// length.
///
/// # Example
///
/// A `ColumnarValue::Array` with an array of 5 elements and a
/// `ColumnarValue::Scalar` with the value 100
///
/// ```text
/// ┌──────────────┐
/// │ ┌──────────┐ │
/// │ │   "A"    │ │
/// │ ├──────────┤ │
/// │ │   "B"    │ │
/// │ ├──────────┤ │
/// │ │   "C"    │ │
/// │ ├──────────┤ │
/// │ │   "D"    │ │        ┌──────────────┐
/// │ ├──────────┤ │        │ ┌──────────┐ │
/// │ │   "E"    │ │        │ │   100    │ │
/// │ └──────────┘ │        │ └──────────┘ │
/// └──────────────┘        └──────────────┘
///
///  ColumnarValue::        ColumnarValue::
///       Array                 Scalar
/// ```
///
/// Logically represents the following table:
///
/// | Column 1| Column 2 |
/// | ------- | -------- |
/// | A | 100 |
/// | B | 100 |
/// | C | 100 |
/// | D | 100 |
/// | E | 100 |
///
/// # Performance Notes
///
/// When implementing functions or operators, it is important to consider the
/// performance implications of handling scalar values.
///
/// Because all functions must handle [`ArrayRef`], it is
/// convenient to convert [`ColumnarValue::Scalar`]s using
/// [`Self::into_array`]. For example,  [`ColumnarValue::values_to_arrays`]
/// converts multiple columnar values into arrays of the same length.
///
/// However, it is often much more performant to provide a different,
/// implementation that handles scalar values differently
#[derive(Clone, Debug)]
pub enum ColumnarValue {
    /// Array of values
    Array(ArrayRef),
    /// A single value
    Scalar(ScalarValue),
}

impl From<ArrayRef> for ColumnarValue {
    fn from(value: ArrayRef) -> Self {
        ColumnarValue::Array(value)
    }
}

impl From<ScalarValue> for ColumnarValue {
    fn from(value: ScalarValue) -> Self {
        ColumnarValue::Scalar(value)
    }
}

impl ColumnarValue {
    pub fn data_type(&self) -> DataType {
        match self {
            ColumnarValue::Array(array_value) => array_value.data_type().clone(),
            ColumnarValue::Scalar(scalar_value) => scalar_value.data_type(),
        }
    }

    /// Convert any [`Self::Scalar`] into an Arrow [`ArrayRef`] with the specified
    /// number of rows  by repeating the same scalar multiple times,
    /// which is not as efficient as handling the scalar directly.
    /// [`Self::Array`] will just be returned as is.
    ///
    /// See [`Self::into_array_of_size`] if you need to validate the length of the output array.
    ///
    /// See [`Self::values_to_arrays`] to convert multiple columnar values into
    /// arrays of the same length.
    ///
    /// # Errors
    ///
    /// Errors if `self` is a Scalar that fails to be converted into an array of size
    pub fn into_array(self, num_rows: usize) -> Result<ArrayRef> {
        Ok(match self {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(num_rows)?,
        })
    }

    /// Convert a columnar value into an Arrow [`ArrayRef`] with the specified
    /// number of rows. [`Self::Scalar`] is converted by repeating the same
    /// scalar multiple times which is not as efficient as handling the scalar
    /// directly.
    /// This validates that if this is [`Self::Array`], it has the expected length.
    ///
    /// See [`Self::values_to_arrays`] to convert multiple columnar values into
    /// arrays of the same length.
    ///
    /// # Errors
    ///
    /// Errors if `self` is a Scalar that fails to be converted into an array of size or
    /// if the array length does not match the expected length
    pub fn into_array_of_size(self, num_rows: usize) -> Result<ArrayRef> {
        match self {
            ColumnarValue::Array(array) => {
                if array.len() == num_rows {
                    Ok(array)
                } else {
                    internal_err!(
                        "Array length {} does not match expected length {}",
                        array.len(),
                        num_rows
                    )
                }
            }
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(num_rows),
        }
    }

    /// Convert any [`Self::Scalar`] into an Arrow [`ArrayRef`] with the specified
    /// number of rows  by repeating the same scalar multiple times,
    /// which is not as efficient as handling the scalar directly.
    /// [`Self::Array`] will just be returned as is.
    ///
    /// See [`Self::to_array_of_size`] if you need to validate the length of the output array.
    ///
    /// See [`Self::values_to_arrays`] to convert multiple columnar values into
    /// arrays of the same length.
    ///
    /// # Errors
    ///
    /// Errors if `self` is a Scalar that fails to be converted into an array of size
    pub fn to_array(&self, num_rows: usize) -> Result<ArrayRef> {
        Ok(match self {
            ColumnarValue::Array(array) => Arc::clone(array),
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(num_rows)?,
        })
    }

    /// Convert a columnar value into an Arrow [`ArrayRef`] with the specified
    /// number of rows. [`Self::Scalar`] is converted by repeating the same
    /// scalar multiple times which is not as efficient as handling the scalar
    /// directly.
    /// This validates that if this is [`Self::Array`], it has the expected length.
    ///
    /// See [`Self::values_to_arrays`] to convert multiple columnar values into
    /// arrays of the same length.
    ///
    /// # Errors
    ///
    /// Errors if `self` is a Scalar that fails to be converted into an array of size or
    /// if the array length does not match the expected length
    pub fn to_array_of_size(&self, num_rows: usize) -> Result<ArrayRef> {
        match self {
            ColumnarValue::Array(array) => {
                if array.len() == num_rows {
                    Ok(Arc::clone(array))
                } else {
                    internal_err!(
                        "Array length {} does not match expected length {}",
                        array.len(),
                        num_rows
                    )
                }
            }
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(num_rows),
        }
    }

    /// Null columnar values are implemented as a null array in order to pass batch
    /// num_rows
    pub fn create_null_array(num_rows: usize) -> Self {
        ColumnarValue::Array(Arc::new(NullArray::new(num_rows)))
    }

    /// Converts  [`ColumnarValue`]s to [`ArrayRef`]s with the same length.
    ///
    /// # Performance Note
    ///
    /// This function expands any [`ScalarValue`] to an array. This expansion
    /// permits using a single function in terms of arrays, but it can be
    /// inefficient compared to handling the scalar value directly.
    ///
    /// Thus, it is recommended to provide specialized implementations for
    /// scalar values if performance is a concern.
    ///
    /// # Errors
    ///
    /// If there are multiple array arguments that have different lengths
    pub fn values_to_arrays(args: &[ColumnarValue]) -> Result<Vec<ArrayRef>> {
        if args.is_empty() {
            return Ok(vec![]);
        }

        let mut array_len = None;
        for arg in args {
            array_len = match (arg, array_len) {
                (ColumnarValue::Array(a), None) => Some(a.len()),
                (ColumnarValue::Array(a), Some(array_len)) => {
                    if array_len == a.len() {
                        Some(array_len)
                    } else {
                        return internal_err!(
                            "Arguments has mixed length. Expected length: {array_len}, found length: {}",
                            a.len()
                        );
                    }
                }
                (ColumnarValue::Scalar(_), array_len) => array_len,
            }
        }

        // If array_len is none, it means there are only scalars, so make a 1 element array
        let inferred_length = array_len.unwrap_or(1);

        let args = args
            .iter()
            .map(|arg| arg.to_array(inferred_length))
            .collect::<Result<Vec<_>>>()?;

        Ok(args)
    }

    /// Cast's this [ColumnarValue] to the specified `DataType`
    pub fn cast_to(
        &self,
        cast_type: &DataType,
        cast_options: Option<&CastOptions<'static>>,
    ) -> Result<ColumnarValue> {
        let cast_options = cast_options.cloned().unwrap_or(DEFAULT_CAST_OPTIONS);
        match self {
            ColumnarValue::Array(array) => {
                ensure_date_array_timestamp_bounds(array, cast_type)?;
                Ok(ColumnarValue::Array(kernels::cast::cast_with_options(
                    array,
                    cast_type,
                    &cast_options,
                )?))
            }
            ColumnarValue::Scalar(scalar) => Ok(ColumnarValue::Scalar(
                scalar.cast_to_with_options(cast_type, &cast_options)?,
            )),
        }
    }
}

fn ensure_date_array_timestamp_bounds(
    array: &ArrayRef,
    cast_type: &DataType,
) -> Result<()> {
    let source_type = array.data_type().clone();
    let Some(multiplier) = date_to_timestamp_multiplier(&source_type, cast_type) else {
        return Ok(());
    };

    if multiplier <= 1 {
        return Ok(());
    }

    // Use compute kernels to find min/max instead of iterating all elements
    let (min_val, max_val): (Option<i64>, Option<i64>) = match &source_type {
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    internal_datafusion_err!(
                        "Expected Date32Array but found {}",
                        array.data_type()
                    )
                })?;
            (min(arr).map(|v| v as i64), max(arr).map(|v| v as i64))
        }
        DataType::Date64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date64Array>()
                .ok_or_else(|| {
                    internal_datafusion_err!(
                        "Expected Date64Array but found {}",
                        array.data_type()
                    )
                })?;
            (min(arr), max(arr))
        }
        _ => return Ok(()), // Not a date type, nothing to do
    };

    // Only validate the min and max values instead of all elements
    if let Some(min) = min_val {
        ensure_timestamp_in_bounds(min, multiplier, &source_type, cast_type)?;
    }
    if let Some(max) = max_val {
        ensure_timestamp_in_bounds(max, multiplier, &source_type, cast_type)?;
    }

    Ok(())
}

// Implement Display trait for ColumnarValue
impl fmt::Display for ColumnarValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let formatted = match self {
            ColumnarValue::Array(array) => {
                pretty_format_columns("ColumnarValue(ArrayRef)", &[Arc::clone(array)])
            }
            ColumnarValue::Scalar(_) => {
                if let Ok(array) = self.to_array(1) {
                    pretty_format_columns("ColumnarValue(ScalarValue)", &[array])
                } else {
                    return write!(f, "Error formatting columnar value");
                }
            }
        };

        if let Ok(formatted) = formatted {
            write!(f, "{formatted}")
        } else {
            write!(f, "Error formatting columnar value")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Date64Array, Int32Array},
        datatypes::TimeUnit,
    };

    #[test]
    fn into_array_of_size() {
        // Array case
        let arr = make_array(1, 3);
        let arr_columnar_value = ColumnarValue::Array(Arc::clone(&arr));
        assert_eq!(&arr_columnar_value.into_array_of_size(3).unwrap(), &arr);

        // Scalar case
        let scalar_columnar_value = ColumnarValue::Scalar(ScalarValue::Int32(Some(42)));
        let expected_array = make_array(42, 100);
        assert_eq!(
            &scalar_columnar_value.into_array_of_size(100).unwrap(),
            &expected_array
        );

        // Array case with wrong size
        let arr = make_array(1, 3);
        let arr_columnar_value = ColumnarValue::Array(Arc::clone(&arr));
        let result = arr_columnar_value.into_array_of_size(5);
        let err = result.unwrap_err();
        assert!(
            err.to_string().starts_with(
                "Internal error: Array length 3 does not match expected length 5"
            ),
            "Found: {err}"
        );
    }

    #[test]
    fn values_to_arrays() {
        // (input, expected)
        let cases = vec![
            // empty
            TestCase {
                input: vec![],
                expected: vec![],
            },
            // one array of length 3
            TestCase {
                input: vec![ColumnarValue::Array(make_array(1, 3))],
                expected: vec![make_array(1, 3)],
            },
            // two arrays length 3
            TestCase {
                input: vec![
                    ColumnarValue::Array(make_array(1, 3)),
                    ColumnarValue::Array(make_array(2, 3)),
                ],
                expected: vec![make_array(1, 3), make_array(2, 3)],
            },
            // array and scalar
            TestCase {
                input: vec![
                    ColumnarValue::Array(make_array(1, 3)),
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(100))),
                ],
                expected: vec![
                    make_array(1, 3),
                    make_array(100, 3), // scalar is expanded
                ],
            },
            // scalar and array
            TestCase {
                input: vec![
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(100))),
                    ColumnarValue::Array(make_array(1, 3)),
                ],
                expected: vec![
                    make_array(100, 3), // scalar is expanded
                    make_array(1, 3),
                ],
            },
            // multiple scalars and array
            TestCase {
                input: vec![
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(100))),
                    ColumnarValue::Array(make_array(1, 3)),
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(200))),
                ],
                expected: vec![
                    make_array(100, 3), // scalar is expanded
                    make_array(1, 3),
                    make_array(200, 3), // scalar is expanded
                ],
            },
        ];
        for case in cases {
            case.run();
        }
    }

    #[test]
    #[should_panic(
        expected = "Arguments has mixed length. Expected length: 3, found length: 4"
    )]
    fn values_to_arrays_mixed_length() {
        ColumnarValue::values_to_arrays(&[
            ColumnarValue::Array(make_array(1, 3)),
            ColumnarValue::Array(make_array(2, 4)),
        ])
        .unwrap();
    }

    #[test]
    #[should_panic(
        expected = "Arguments has mixed length. Expected length: 3, found length: 7"
    )]
    fn values_to_arrays_mixed_length_and_scalar() {
        ColumnarValue::values_to_arrays(&[
            ColumnarValue::Array(make_array(1, 3)),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(100))),
            ColumnarValue::Array(make_array(2, 7)),
        ])
        .unwrap();
    }

    struct TestCase {
        input: Vec<ColumnarValue>,
        expected: Vec<ArrayRef>,
    }

    impl TestCase {
        fn run(self) {
            let Self { input, expected } = self;

            assert_eq!(
                ColumnarValue::values_to_arrays(&input).unwrap(),
                expected,
                "\ninput: {input:?}\nexpected: {expected:?}"
            );
        }
    }

    /// Makes an array of length `len` with all elements set to `val`
    fn make_array(val: i32, len: usize) -> ArrayRef {
        Arc::new(Int32Array::from(vec![val; len]))
    }

    #[test]
    fn test_display_scalar() {
        let column = ColumnarValue::from(ScalarValue::from("foo"));
        assert_eq!(
            column.to_string(),
            concat!(
                "+----------------------------+\n",
                "| ColumnarValue(ScalarValue) |\n",
                "+----------------------------+\n",
                "| foo                        |\n",
                "+----------------------------+"
            )
        );
    }

    #[test]
    fn test_display_array() {
        let array: ArrayRef = Arc::new(Int32Array::from_iter_values(vec![1, 2, 3]));
        let column = ColumnarValue::from(array);
        assert_eq!(
            column.to_string(),
            concat!(
                "+-------------------------+\n",
                "| ColumnarValue(ArrayRef) |\n",
                "+-------------------------+\n",
                "| 1                       |\n",
                "| 2                       |\n",
                "| 3                       |\n",
                "+-------------------------+"
            )
        );
    }

    #[test]
    fn cast_date64_array_to_timestamp_overflow() {
        let overflow_value = i64::MAX / 1_000_000 + 1;
        let array: ArrayRef = Arc::new(Date64Array::from(vec![Some(overflow_value)]));
        let value = ColumnarValue::Array(array);
        let result =
            value.cast_to(&DataType::Timestamp(TimeUnit::Nanosecond, None), None);
        let err = result.expect_err("expected overflow to be detected");
        assert!(
            err.to_string()
                .contains("converted value exceeds the representable i64 range"),
            "unexpected error: {err}"
        );
    }
}
