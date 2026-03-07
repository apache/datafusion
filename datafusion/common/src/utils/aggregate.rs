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

//! Array-level and scalar-level aggregation utilities (sum, min, max).
//!
//! These functions compute aggregate values over Arrow arrays or slices of
//! [`ScalarValue`]s, returning the result as a [`ScalarValue`]. They use
//! native Arrow compute kernels for primitive numeric types and fall back
//! to element-wise [`ScalarValue`] operations for other types.

use arrow::array::ArrayRef;

use crate::stats::Precision;
use crate::{Result, ScalarValue};
use arrow::array::*;
use arrow::compute::{min as arrow_min, sum as arrow_sum};
use arrow::datatypes::*;

/// Returns true if the [`ScalarValue`] is a primitive numeric type that
/// benefits from Arrow vectorized kernels over direct `ScalarValue`
/// comparison.
pub(crate) fn is_primitive_scalar(value: &ScalarValue) -> bool {
    matches!(
        value,
        ScalarValue::Int8(_)
            | ScalarValue::Int16(_)
            | ScalarValue::Int32(_)
            | ScalarValue::Int64(_)
            | ScalarValue::UInt8(_)
            | ScalarValue::UInt16(_)
            | ScalarValue::UInt32(_)
            | ScalarValue::UInt64(_)
            | ScalarValue::Float16(_)
            | ScalarValue::Float32(_)
            | ScalarValue::Float64(_)
            | ScalarValue::Decimal32(_, _, _)
            | ScalarValue::Decimal64(_, _, _)
            | ScalarValue::Decimal128(_, _, _)
            | ScalarValue::Decimal256(_, _, _)
            | ScalarValue::Date32(_)
            | ScalarValue::Date64(_)
    )
}

/// Compute the minimum of [`ScalarValue`]s using direct `PartialOrd`
/// comparison.
pub(crate) fn scalar_min(values: &[ScalarValue]) -> ScalarValue {
    debug_assert!(!values.is_empty());
    let mut result = values[0].clone();
    for v in &values[1..] {
        if v.is_null() {
            continue;
        }
        if result.is_null() || v < &result {
            result = v.clone();
        }
    }
    result
}

/// Compute the maximum of [`ScalarValue`]s using direct `PartialOrd`
/// comparison.
pub(crate) fn scalar_max(values: &[ScalarValue]) -> ScalarValue {
    debug_assert!(!values.is_empty());
    let mut result = values[0].clone();
    for v in &values[1..] {
        if v.is_null() {
            continue;
        }
        if result.is_null() || v > &result {
            result = v.clone();
        }
    }
    result
}

/// Wrap a [`ScalarValue`] result with the appropriate [`Precision`] level.
pub(crate) fn wrap_precision(
    value: ScalarValue,
    all_exact: bool,
) -> Precision<ScalarValue> {
    if value.is_null() {
        return Precision::Absent;
    }
    if all_exact {
        Precision::Exact(value)
    } else {
        Precision::Inexact(value)
    }
}

/// Compute the sum of a collection of [`ScalarValue`]s using vectorized
/// Arrow kernels. The values are converted to an Arrow array once, then
/// the sum kernel is applied in a single call.
pub(crate) fn vectorized_sum(
    values: &[ScalarValue],
    all_exact: bool,
) -> Result<Precision<ScalarValue>> {
    debug_assert!(!values.is_empty());

    let array = ScalarValue::iter_to_array(values.iter().cloned())?;
    let result = sum_array(&array)?;

    Ok(wrap_precision(result, all_exact))
}

/// Compute minimum of a collection of [`ScalarValue`]s with arrows vector kernels.
pub(crate) fn vectorized_min(
    values: &[ScalarValue],
    all_exact: bool,
) -> Result<Precision<ScalarValue>> {
    debug_assert!(!values.is_empty());

    let result = if is_primitive_scalar(&values[0]) {
        let array = ScalarValue::iter_to_array(values.iter().cloned())?;
        min_array(&array)?
    } else {
        scalar_min(values)
    };

    Ok(wrap_precision(result, all_exact))
}

/// Compute the maximum of a collection of [`ScalarValue`]s with vector kernels.
pub(crate) fn vectorized_max(
    values: &[ScalarValue],
    all_exact: bool,
) -> Result<Precision<ScalarValue>> {
    debug_assert!(!values.is_empty());

    let result = if is_primitive_scalar(&values[0]) {
        let array = ScalarValue::iter_to_array(values.iter().cloned())?;
        max_array(&array)?
    } else {
        scalar_max(values)
    };

    Ok(wrap_precision(result, all_exact))
}

/// Compute sum of all elements in an Arrow array, returning the result as
/// a [`ScalarValue`].
pub(crate) fn sum_array(array: &ArrayRef) -> Result<ScalarValue> {
    macro_rules! sum_primitive {
        ($array:expr, $array_type:ty, $scalar_variant:ident $(, $extra:expr)*) => {{
            let typed = $array.as_any().downcast_ref::<$array_type>().unwrap();
            match arrow_sum(typed) {
                Some(v) => ScalarValue::$scalar_variant(Some(v) $(, $extra)*),
                None => ScalarValue::try_from(array.data_type())?,
            }
        }};
    }

    let result = match array.data_type() {
        DataType::Int8 => sum_primitive!(array, Int8Array, Int8),
        DataType::Int16 => sum_primitive!(array, Int16Array, Int16),
        DataType::Int32 => sum_primitive!(array, Int32Array, Int32),
        DataType::Int64 => sum_primitive!(array, Int64Array, Int64),
        DataType::UInt8 => sum_primitive!(array, UInt8Array, UInt8),
        DataType::UInt16 => sum_primitive!(array, UInt16Array, UInt16),
        DataType::UInt32 => sum_primitive!(array, UInt32Array, UInt32),
        DataType::UInt64 => sum_primitive!(array, UInt64Array, UInt64),
        DataType::Float16 => sum_primitive!(array, Float16Array, Float16),
        DataType::Float32 => sum_primitive!(array, Float32Array, Float32),
        DataType::Float64 => sum_primitive!(array, Float64Array, Float64),
        DataType::Decimal32(p, s) => {
            let p = *p;
            let s = *s;
            sum_primitive!(array, Decimal32Array, Decimal32, p, s)
        }
        DataType::Decimal64(p, s) => {
            let p = *p;
            let s = *s;
            sum_primitive!(array, Decimal64Array, Decimal64, p, s)
        }
        DataType::Decimal128(p, s) => {
            let p = *p;
            let s = *s;
            sum_primitive!(array, Decimal128Array, Decimal128, p, s)
        }
        DataType::Decimal256(p, s) => {
            let p = *p;
            let s = *s;
            sum_primitive!(array, Decimal256Array, Decimal256, p, s)
        }
        _ => {
            let mut acc = ScalarValue::try_from_array(array, 0)?;
            for i in 1..array.len() {
                let v = ScalarValue::try_from_array(array, i)?;
                if !v.is_null() {
                    if acc.is_null() {
                        acc = v;
                    } else {
                        acc = acc.add(&v)?;
                    }
                }
            }
            acc
        }
    };

    Ok(result)
}

/// Compute the minimum of all elements in an Arrow array, returning the
/// result as a [`ScalarValue`].
pub(crate) fn min_array(array: &ArrayRef) -> Result<ScalarValue> {
    macro_rules! min_primitive {
        ($array:expr, $array_type:ty, $scalar_variant:ident $(, $extra:expr)*) => {{
            let typed = $array.as_any().downcast_ref::<$array_type>().unwrap();
            match arrow_min(typed) {
                Some(v) => ScalarValue::$scalar_variant(Some(v) $(, $extra)*),
                None => ScalarValue::try_from(array.data_type())?,
            }
        }};
    }

    macro_rules! min_string {
        ($array:expr, $array_type:ty, $scalar_variant:ident) => {{
            let typed = $array.as_any().downcast_ref::<$array_type>().unwrap();
            match arrow::compute::min_string(typed) {
                Some(v) => ScalarValue::$scalar_variant(Some(v.to_owned())),
                None => ScalarValue::try_from(array.data_type())?,
            }
        }};
    }

    let result = match array.data_type() {
        DataType::Int8 => min_primitive!(array, Int8Array, Int8),
        DataType::Int16 => min_primitive!(array, Int16Array, Int16),
        DataType::Int32 => min_primitive!(array, Int32Array, Int32),
        DataType::Int64 => min_primitive!(array, Int64Array, Int64),
        DataType::UInt8 => min_primitive!(array, UInt8Array, UInt8),
        DataType::UInt16 => min_primitive!(array, UInt16Array, UInt16),
        DataType::UInt32 => min_primitive!(array, UInt32Array, UInt32),
        DataType::UInt64 => min_primitive!(array, UInt64Array, UInt64),
        DataType::Float16 => min_primitive!(array, Float16Array, Float16),
        DataType::Float32 => min_primitive!(array, Float32Array, Float32),
        DataType::Float64 => min_primitive!(array, Float64Array, Float64),
        DataType::Decimal32(p, s) => {
            let p = *p;
            let s = *s;
            min_primitive!(array, Decimal32Array, Decimal32, p, s)
        }
        DataType::Decimal64(p, s) => {
            let p = *p;
            let s = *s;
            min_primitive!(array, Decimal64Array, Decimal64, p, s)
        }
        DataType::Decimal128(p, s) => {
            let p = *p;
            let s = *s;
            min_primitive!(array, Decimal128Array, Decimal128, p, s)
        }
        DataType::Decimal256(p, s) => {
            let p = *p;
            let s = *s;
            min_primitive!(array, Decimal256Array, Decimal256, p, s)
        }
        DataType::Date32 => min_primitive!(array, Date32Array, Date32),
        DataType::Date64 => min_primitive!(array, Date64Array, Date64),
        DataType::Utf8 => min_string!(array, StringArray, Utf8),
        DataType::LargeUtf8 => min_string!(array, LargeStringArray, LargeUtf8),
        DataType::Utf8View => {
            let typed = array.as_any().downcast_ref::<StringViewArray>().unwrap();
            match arrow::compute::min_string_view(typed) {
                Some(v) => ScalarValue::Utf8View(Some(v.to_owned())),
                None => ScalarValue::try_from(array.data_type())?,
            }
        }
        DataType::Boolean => {
            let typed = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            match arrow::compute::min_boolean(typed) {
                Some(v) => ScalarValue::Boolean(Some(v)),
                None => ScalarValue::try_from(array.data_type())?,
            }
        }
        _ => scalar_min_max_fallback(array, true)?,
    };

    Ok(result)
}

/// Compute the maximum of all elements in an Arrow array, returning the
/// result as a [`ScalarValue`].
///
/// Uses native Arrow aggregate kernels for primitive numeric types, and
/// falls back to [`ScalarValue`] comparison for other types.
pub(crate) fn max_array(array: &ArrayRef) -> Result<ScalarValue> {
    use arrow::array::*;
    use arrow::compute::max as arrow_max;
    use arrow::datatypes::*;

    macro_rules! max_primitive {
        ($array:expr, $array_type:ty, $scalar_variant:ident $(, $extra:expr)*) => {{
            let typed = $array.as_any().downcast_ref::<$array_type>().unwrap();
            match arrow_max(typed) {
                Some(v) => ScalarValue::$scalar_variant(Some(v) $(, $extra)*),
                None => ScalarValue::try_from(array.data_type())?,
            }
        }};
    }

    macro_rules! max_string {
        ($array:expr, $array_type:ty, $scalar_variant:ident) => {{
            let typed = $array.as_any().downcast_ref::<$array_type>().unwrap();
            match arrow::compute::max_string(typed) {
                Some(v) => ScalarValue::$scalar_variant(Some(v.to_owned())),
                None => ScalarValue::try_from(array.data_type())?,
            }
        }};
    }

    let result = match array.data_type() {
        DataType::Int8 => max_primitive!(array, Int8Array, Int8),
        DataType::Int16 => max_primitive!(array, Int16Array, Int16),
        DataType::Int32 => max_primitive!(array, Int32Array, Int32),
        DataType::Int64 => max_primitive!(array, Int64Array, Int64),
        DataType::UInt8 => max_primitive!(array, UInt8Array, UInt8),
        DataType::UInt16 => max_primitive!(array, UInt16Array, UInt16),
        DataType::UInt32 => max_primitive!(array, UInt32Array, UInt32),
        DataType::UInt64 => max_primitive!(array, UInt64Array, UInt64),
        DataType::Float16 => max_primitive!(array, Float16Array, Float16),
        DataType::Float32 => max_primitive!(array, Float32Array, Float32),
        DataType::Float64 => max_primitive!(array, Float64Array, Float64),
        DataType::Decimal32(p, s) => {
            let p = *p;
            let s = *s;
            max_primitive!(array, Decimal32Array, Decimal32, p, s)
        }
        DataType::Decimal64(p, s) => {
            let p = *p;
            let s = *s;
            max_primitive!(array, Decimal64Array, Decimal64, p, s)
        }
        DataType::Decimal128(p, s) => {
            let p = *p;
            let s = *s;
            max_primitive!(array, Decimal128Array, Decimal128, p, s)
        }
        DataType::Decimal256(p, s) => {
            let p = *p;
            let s = *s;
            max_primitive!(array, Decimal256Array, Decimal256, p, s)
        }
        DataType::Date32 => max_primitive!(array, Date32Array, Date32),
        DataType::Date64 => max_primitive!(array, Date64Array, Date64),
        DataType::Utf8 => max_string!(array, StringArray, Utf8),
        DataType::LargeUtf8 => max_string!(array, LargeStringArray, LargeUtf8),
        DataType::Utf8View => {
            let typed = array.as_any().downcast_ref::<StringViewArray>().unwrap();
            match arrow::compute::max_string_view(typed) {
                Some(v) => ScalarValue::Utf8View(Some(v.to_owned())),
                None => ScalarValue::try_from(array.data_type())?,
            }
        }
        DataType::Boolean => {
            let typed = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            match arrow::compute::max_boolean(typed) {
                Some(v) => ScalarValue::Boolean(Some(v)),
                None => ScalarValue::try_from(array.data_type())?,
            }
        }
        _ => scalar_min_max_fallback(array, false)?,
    };

    Ok(result)
}
