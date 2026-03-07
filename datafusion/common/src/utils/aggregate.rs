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

//! Scalar-level aggregation utilities (sum, min, max).
//!
//! These functions compute aggregate values over slices of [`ScalarValue`]s
//! by directly extracting the inner primitive values and accumulating.

use arrow::datatypes::i256;
use half::f16;

use crate::stats::Precision;
use crate::{Result, ScalarValue};

/// Returns true if the [`ScalarValue`] is a primitive numeric type.
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

/// Compute the sum of [`ScalarValue`]s by directly extracting and
/// accumulating primitive values without any Arrow array allocation.
///
/// For non-primitive types, falls back to `ScalarValue::add`.
pub(crate) fn scalar_sum(values: &[ScalarValue]) -> Result<ScalarValue> {
    debug_assert!(!values.is_empty());

    macro_rules! sum_wrapping {
        ($values:expr, $VARIANT:ident, $T:ty) => {{
            let mut has_value = false;
            let mut acc: $T = Default::default();
            for sv in $values {
                if let ScalarValue::$VARIANT(Some(v)) = sv {
                    if has_value {
                        acc = acc.wrapping_add(*v);
                    } else {
                        acc = *v;
                        has_value = true;
                    }
                }
            }
            if has_value {
                ScalarValue::$VARIANT(Some(acc))
            } else {
                $values[0].clone() // all null
            }
        }};
    }

    /// Accumulate a wrapping sum for a decimal ScalarValue variant that
    /// carries precision and scale fields.
    macro_rules! sum_decimal {
        ($values:expr, $VARIANT:ident, $T:ty) => {{
            let (p, s) = match &$values[0] {
                ScalarValue::$VARIANT(_, p, s) => (*p, *s),
                _ => unreachable!(),
            };
            let mut has_value = false;
            let mut acc: $T = Default::default();
            for sv in $values {
                if let ScalarValue::$VARIANT(Some(v), _, _) = sv {
                    if has_value {
                        acc = acc.wrapping_add(*v);
                    } else {
                        acc = *v;
                        has_value = true;
                    }
                }
            }
            if has_value {
                ScalarValue::$VARIANT(Some(acc), p, s)
            } else {
                $values[0].clone() // all null
            }
        }};
    }

    macro_rules! sum_float {
        ($values:expr, $VARIANT:ident, $T:ty) => {{
            let mut has_value = false;
            let mut acc: $T = Default::default();
            for sv in $values {
                if let ScalarValue::$VARIANT(Some(v)) = sv {
                    if has_value {
                        acc = acc + *v;
                    } else {
                        acc = *v;
                        has_value = true;
                    }
                }
            }
            if has_value {
                ScalarValue::$VARIANT(Some(acc))
            } else {
                $values[0].clone() // all null
            }
        }};
    }

    let result = match &values[0] {
        ScalarValue::Int8(_) => sum_wrapping!(values, Int8, i8),
        ScalarValue::Int16(_) => sum_wrapping!(values, Int16, i16),
        ScalarValue::Int32(_) => sum_wrapping!(values, Int32, i32),
        ScalarValue::Int64(_) => sum_wrapping!(values, Int64, i64),
        ScalarValue::UInt8(_) => sum_wrapping!(values, UInt8, u8),
        ScalarValue::UInt16(_) => sum_wrapping!(values, UInt16, u16),
        ScalarValue::UInt32(_) => sum_wrapping!(values, UInt32, u32),
        ScalarValue::UInt64(_) => sum_wrapping!(values, UInt64, u64),
        ScalarValue::Float16(_) => sum_float!(values, Float16, f16),
        ScalarValue::Float32(_) => sum_float!(values, Float32, f32),
        ScalarValue::Float64(_) => sum_float!(values, Float64, f64),
        ScalarValue::Decimal32(_, _, _) => sum_decimal!(values, Decimal32, i32),
        ScalarValue::Decimal64(_, _, _) => sum_decimal!(values, Decimal64, i64),
        ScalarValue::Decimal128(_, _, _) => sum_decimal!(values, Decimal128, i128),
        ScalarValue::Decimal256(_, _, _) => sum_decimal!(values, Decimal256, i256),
        _ => {
            // Fallback for non-primitive types: use ScalarValue::add
            let mut acc = values[0].clone();
            for v in &values[1..] {
                if !v.is_null() {
                    if acc.is_null() {
                        acc = v.clone();
                    } else {
                        acc = acc.add(v)?;
                    }
                }
            }
            acc
        }
    };

    Ok(result)
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

/// Compute the sum of a collection of [`ScalarValue`]s by directly
/// extracting primitive values and accumulating without array allocation.
pub(crate) fn vectorized_sum(
    values: &[ScalarValue],
    all_exact: bool,
) -> Result<Precision<ScalarValue>> {
    debug_assert!(!values.is_empty());
    let result = scalar_sum(values)?;
    Ok(wrap_precision(result, all_exact))
}

/// Compute minimum of a collection of [`ScalarValue`]s using direct
/// `PartialOrd` comparison.
pub(crate) fn vectorized_min(
    values: &[ScalarValue],
    all_exact: bool,
) -> Result<Precision<ScalarValue>> {
    debug_assert!(!values.is_empty());
    let result = scalar_min(values);
    Ok(wrap_precision(result, all_exact))
}

/// Compute the maximum of a collection of [`ScalarValue`]s using direct
/// `PartialOrd` comparison.
pub(crate) fn vectorized_max(
    values: &[ScalarValue],
    all_exact: bool,
) -> Result<Precision<ScalarValue>> {
    debug_assert!(!values.is_empty());
    let result = scalar_max(values);
    Ok(wrap_precision(result, all_exact))
}
