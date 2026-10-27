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

//! Utilities for casting scalar literals to different data types
//!
//! This module contains functions for casting ScalarValue literals
//! to different data types, originally extracted from the optimizer's
//! unwrap_cast module to be shared between logical and physical layers.

use std::cmp::Ordering;
use std::sync::Arc;

use crate::interval_arithmetic::Interval;
use crate::operator::Operator;

use arrow::datatypes::{
    DataType, MAX_DECIMAL32_FOR_EACH_PRECISION, MAX_DECIMAL64_FOR_EACH_PRECISION,
    MAX_DECIMAL128_FOR_EACH_PRECISION, MIN_DECIMAL32_FOR_EACH_PRECISION,
    MIN_DECIMAL64_FOR_EACH_PRECISION, MIN_DECIMAL128_FOR_EACH_PRECISION, TimeUnit,
};
use arrow::temporal_conversions::{
    MICROSECONDS, MILLISECONDS, MILLISECONDS_IN_DAY, NANOSECONDS,
};
use datafusion_common::{Result, ScalarValue};

/// Source-domain preimage of `CAST(source_expr AS target_type) OP literal`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CastPredicatePreimage {
    /// An exact same-operator source bound or literal, i.e. the rewrite is
    /// `source_expr OP value` with the original comparison operator.
    Exact(ScalarValue),
    /// A half-open source-domain interval `[lower, upper)`; the caller maps the
    /// comparison operator onto its bounds. Equal bounds mean no single source
    /// value maps to the literal, an empty equality preimage, while ordered
    /// comparisons still compare against that bound as in `< bound` or
    /// `>= bound`.
    Range(Interval),
}

impl CastPredicatePreimage {
    /// Returns whether this preimage and comparison operator produce multiple
    /// predicates that each evaluate the input expression.
    pub fn duplicates_input(&self, op: Operator) -> bool {
        matches!(self, Self::Range(_))
            && matches!(
                op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::IsDistinctFrom
                    | Operator::IsNotDistinctFrom
            )
    }
}

/// Convert a literal [`ScalarValue`] to `target_type`, preserving the exact value.
///
/// Returns `None` if the value cannot be represented in `target_type`
/// *exactly*.
///
/// This is a restricted, value-preserving cast used to rewrite comparison
/// predicates of the form `CAST(col AS target_type) <op> literal` into
/// `col <op> try_cast_literal_to_type(literal, col_type)`. The rewrite requires
/// an exact literal preimage and an accepted cast family; for integer or
/// decimal sources with a decimal target of equal or greater scale, it may
/// change cast overflow errors or NULLs to a boolean result for values outside
/// the target range.
///
/// # Supported Casts
/// * numeric → numeric, including integers, decimals, `Date32`/`Date64` and
///   `Timestamps`, rejecting literal conversions outside the target's range or
///   that would lose decimal digits
/// * string → string between `Utf8`, `LargeUtf8` and `Utf8View`
/// * wrapping a value into, or unwrapping it out of, a `Dictionary` whose value
///   type matches the literal's type
/// * `Binary` → `FixedSizeBinary` of the matching length
/// * `Timestamp` → `Timestamp` cast between different time units is allowed even
///   though it can truncate (for example nanoseconds → seconds), and a unit
///   conversion that overflows yields a `NULL` literal rather than `None`.
///
/// # See Also
/// - [`ScalarValue::cast_to`]: a general-purpose cast that can lose information
///   or change a value's meaning.
pub fn try_cast_literal_to_type(
    lit_value: &ScalarValue,
    target_type: &DataType,
) -> Option<ScalarValue> {
    let lit_data_type = lit_value.data_type();
    if !is_supported_type(&lit_data_type) || !is_supported_type(target_type) {
        return None;
    }
    if lit_value.is_null() {
        // null value can be cast to any type of null value
        return ScalarValue::try_from(target_type).ok();
    }
    try_cast_numeric_literal(lit_value, target_type)
        .or_else(|| try_cast_string_literal(lit_value, target_type))
        .or_else(|| try_cast_dictionary(lit_value, target_type))
        .or_else(|| try_cast_binary(lit_value, target_type))
}

/// Computes a source-domain preimage for `CAST(source AS target_type) OP literal`.
///
/// This is the shared semantic core for logical and physical cast-predicate
/// rewrites. It returns an exact same-operator source bound or literal as
/// [`CastPredicatePreimage::Exact`] where moving the cast preserves comparison
/// semantics subject to the documented family-level overflow exceptions, and a
/// [`CastPredicatePreimage::Range`] for many-to-one casts with known preimages
/// (timestamp precision narrowing) or for non-aligned timestamp widening
/// literals.
pub fn cast_predicate_preimage(
    source_type: &DataType,
    target_type: &DataType,
    op: Operator,
    lit_value: &ScalarValue,
) -> Result<Option<CastPredicatePreimage>> {
    // Timestamp casts have their own rules: a truncation bucket when the cast
    // narrows, and the source values equal to the literal when it does not. A
    // rejected timestamp cast keeps its `CAST` instead of falling back to
    // another cast family.
    if let Some((source, target)) = timestamp_cast_types(source_type, target_type) {
        return Ok(timestamp_preimage(
            source_type,
            source,
            target,
            op,
            lit_value,
        ));
    }

    if let Some(value) =
        exact_preimage_int_to_str_eq_like(source_type, target_type, op, lit_value)
    {
        return Ok(Some(CastPredicatePreimage::Exact(value)));
    }

    Ok(exact_preimage_cast(source_type, target_type, lit_value)
        .map(CastPredicatePreimage::Exact))
}

/// Computes a singleton source-domain literal for exact cast-predicate rewrites.
///
/// This intentionally returns `None` for timestamp precision narrowing and for
/// non-aligned timestamp widening: those casts need range preimages instead.
pub fn exact_preimage_cast(
    source_type: &DataType,
    target_type: &DataType,
    lit_value: &ScalarValue,
) -> Option<ScalarValue> {
    // Timestamp casts need unit arithmetic rather than a literal round trip; only
    // their singleton preimages are valid here.
    if let Some((source, target)) = timestamp_cast_types(source_type, target_type) {
        return match timestamp_preimage(
            source_type,
            source,
            target,
            Operator::Eq,
            lit_value,
        )? {
            CastPredicatePreimage::Exact(value) => Some(value),
            CastPredicatePreimage::Range(_) => None,
        };
    }

    // Apply a family-level gate: the source→target cast must normally preserve
    // the full source domain. Integer or decimal sources to a decimal target
    // are admitted by scale instead (see `is_exact_cast_safe`), accepting
    // precision/width overflow as a boolean result for out-of-range rows.
    if !is_exact_cast_safe(source_type, target_type) {
        return None;
    }

    try_cast_literal_to_type(lit_value, source_type)
}

/// Returns true when casting a timestamp from `source_type` to `target_type`
/// loses timestamp precision.
pub fn is_timestamp_precision_narrowing_cast(
    source_type: &DataType,
    target_type: &DataType,
) -> bool {
    let (DataType::Timestamp(source_unit, _), DataType::Timestamp(target_unit, _)) =
        (source_type, target_type)
    else {
        return false;
    };

    timestamp_unit_scale(source_unit) > timestamp_unit_scale(target_unit)
}

/// Returns the source and target types of a timestamp cast, each unwrapped from
/// at most one `Dictionary` level.
fn timestamp_cast_types<'a>(
    source_type: &'a DataType,
    target_type: &'a DataType,
) -> Option<(&'a DataType, &'a DataType)> {
    let unwrap = |data_type: &'a DataType| match data_type {
        DataType::Dictionary(_, value_type) => value_type.as_ref(),
        data_type => data_type,
    };
    let (source, target) = (unwrap(source_type), unwrap(target_type));
    (matches!(source, DataType::Timestamp(_, _))
        && matches!(target, DataType::Timestamp(_, _)))
    .then_some((source, target))
}

/// Returns `true` when casting `source_type` to `target_type` is exact and
/// order-preserving over the source values the cast succeeds on. Integer or
/// decimal sources to a decimal target are admitted when the source scale is
/// nonnegative and the target scale is at least the source scale: a successful
/// cast only multiplies the unscaled value by an integral power of ten.
/// Precision or width overflow is the explicit exception: `CAST` errors and
/// `TRY_CAST` yields NULL for such source values, while the rewritten predicate
/// compares the source value and returns TRUE/FALSE. This rule is type-based
/// and does not distinguish implicit coercions from explicit casts.
///
/// This is a conservative gate used by [`exact_preimage_cast`] for the non
/// timestamp cast families; timestamp casts are handled by
/// [`timestamp_preimage`] instead.
fn is_exact_cast_safe(source_type: &DataType, target_type: &DataType) -> bool {
    // Unwrap at most one level of dictionary for family-level checking.
    let (src, tgt) = match (source_type, target_type) {
        (DataType::Dictionary(_, v_src), DataType::Dictionary(_, v_tgt)) => {
            (v_src.as_ref(), v_tgt.as_ref())
        }
        (DataType::Dictionary(_, v_src), tgt) => (v_src.as_ref(), tgt),
        (src, DataType::Dictionary(_, v_tgt)) => (src, v_tgt.as_ref()),
        _ => (source_type, target_type),
    };

    // Integer or decimal sources to a decimal target are accepted when the
    // source scale is nonnegative and the target only adds fractional digits.
    // This precedes identity so negative-scale decimals are rejected by the
    // scale check instead of reaching the scale arithmetic and panicking.
    if is_decimal_type(tgt) && (is_decimal_type(src) || is_integer_type(src)) {
        let s_src = if is_decimal_type(src) {
            decimal_precision_scale(src).1
        } else {
            0
        };
        return s_src >= 0 && decimal_precision_scale(tgt).1 >= s_src;
    }

    if src == tgt {
        return true;
    }

    // Whitelist of family-level safe casts.  Anything not listed here is
    // considered potentially information-losing and blocked.
    if is_integer_type(src) && is_integer_type(tgt) {
        return is_safe_integer_cast(src, tgt);
    }
    if matches!(
        (src, tgt),
        (DataType::Date32, DataType::Int32)
            | (DataType::Int32, DataType::Date32)
            | (DataType::Date64, DataType::Int64)
            | (DataType::Int64, DataType::Date64)
    ) {
        // Date32 ↔ Int32 and Date64 ↔ Int64 share the same internal
        // representation.
        return true;
    }
    if matches!((src, tgt), (DataType::Date32, DataType::Date64)) {
        // Date32 → Date64 is injective: every source day maps to its midnight
        // millisecond value. The literal conversion below enforces the exact
        // day-boundary requirement in the reverse direction.
        return true;
    }
    if is_supported_string_type(src) && is_supported_string_type(tgt) {
        // String width casts among Utf8 / LargeUtf8 / Utf8View.
        return true;
    }
    if matches!((src, tgt), (DataType::FixedSizeBinary(_), DataType::Binary)) {
        // FixedSizeBinary(n) → Binary is always widening.
        return true;
    }
    if matches!(
        (src, tgt),
        (DataType::FixedSizeBinary(n1), DataType::FixedSizeBinary(n2)) if n1 == n2
    ) {
        return true;
    }
    false
}

/// Returns `true` for integer types (signed and unsigned, excluding Date and
/// Decimal).
fn is_integer_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

/// Integer cast that is value-preserving over the full source domain.
fn is_safe_integer_cast(src: &DataType, tgt: &DataType) -> bool {
    use DataType::*;
    match (src, tgt) {
        // Signed → wider signed
        (Int8, Int16 | Int32 | Int64) => true,
        (Int16, Int32 | Int64) => true,
        (Int32, Int64) => true,
        // Unsigned → wider unsigned
        (UInt8, UInt16 | UInt32 | UInt64) => true,
        (UInt16, UInt32 | UInt64) => true,
        (UInt32, UInt64) => true,
        // Unsigned → wider signed (full source range fits in target)
        (UInt8, Int16 | Int32 | Int64) => true,
        (UInt16, Int32 | Int64) => true,
        (UInt32, Int64) => true,
        // Anything else is narrowing, partial, or crosses signedness
        // without guaranteed domain containment.
        _ => false,
    }
}

fn is_decimal_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Decimal128(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal32(_, _)
    )
}

fn decimal_precision_scale(dt: &DataType) -> (u8, i8) {
    match dt {
        DataType::Decimal128(p, s)
        | DataType::Decimal64(p, s)
        | DataType::Decimal32(p, s) => (*p, *s),
        _ => unreachable!(),
    }
}

/// Computes a singleton preimage for equality-like predicates over casts whose
/// target value is a string representation of an integer source value.
///
/// For example, `CAST(int_col AS Utf8) = '123'` can be rewritten to
/// `int_col = 123`, but `CAST(int_col AS Utf8) = '0123'` cannot be rewritten to
/// `int_col = 123` because casting `123` back to a string yields `'123'`, not
/// `'0123'`.
fn exact_preimage_int_to_str_eq_like(
    source_type: &DataType,
    target_type: &DataType,
    op: Operator,
    lit_value: &ScalarValue,
) -> Option<ScalarValue> {
    if !matches!(
        target_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        return None;
    }

    match (op, lit_value) {
        (
            Operator::Eq
            | Operator::NotEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom,
            ScalarValue::Utf8(Some(_))
            | ScalarValue::Utf8View(Some(_))
            | ScalarValue::LargeUtf8(Some(_)),
        ) => {
            // Only try for integer types (TODO can we do this for other types
            // like timestamps)?
            use DataType::*;
            if matches!(
                source_type,
                Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64
            ) {
                let casted = lit_value.cast_to(source_type).ok()?;
                let round_tripped = casted.cast_to(&lit_value.data_type()).ok()?;
                if lit_value != &round_tripped {
                    return None;
                }
                Some(casted)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Computes the source-domain preimage of `CAST(source AS target) OP literal`
/// for a timestamp precision cast.
///
/// `source` and `target` are already unwrapped from one `Dictionary` level;
/// `source_type` is the original source type, whose dictionary key is applied to
/// every bound together with the source unit and timezone, so nothing from the
/// target type leaks into the rewritten predicate. Rejected casts return `None`.
///
/// Retained policy: at extreme source values where the cast itself overflows,
/// `CAST` can error and `TRY_CAST` can return `NULL` while the rewritten
/// source-unit comparison returns a Boolean.
fn timestamp_preimage(
    source_type: &DataType,
    source: &DataType,
    target: &DataType,
    op: Operator,
    lit_value: &ScalarValue,
) -> Option<CastPredicatePreimage> {
    let (
        DataType::Timestamp(source_unit, source_tz),
        DataType::Timestamp(target_unit, target_tz),
    ) = (source, target)
    else {
        return None;
    };

    // Arrow reads a naive value as local wall-clock time in the target timezone
    // and shifts it to UTC (`adjust_timestamp_to_timezone`), which this unit
    // arithmetic does not model, so the cast is kept.
    if source_tz.is_none() && target_tz.is_some() {
        return None;
    }

    // The literal must already be a timestamp in the target unit and timezone; it
    // is never converted, and at most one dictionary wrapper is unwrapped.
    let literal = match lit_value {
        ScalarValue::Dictionary(_, inner) => inner.as_ref(),
        literal => literal,
    };
    if literal.data_type() != *target {
        return None;
    }
    let (ScalarValue::TimestampSecond(literal_value, _)
    | ScalarValue::TimestampMillisecond(literal_value, _)
    | ScalarValue::TimestampMicrosecond(literal_value, _)
    | ScalarValue::TimestampNanosecond(literal_value, _)) = literal
    else {
        return None;
    };

    let bound = |value| {
        let scalar = timestamp_scalar(source_unit, source_tz.clone(), value);
        match source_type {
            DataType::Dictionary(key_type, _) => {
                ScalarValue::Dictionary(key_type.clone(), Box::new(scalar))
            }
            _ => scalar,
        }
    };

    let source_scale = i128::from(timestamp_unit_scale(source_unit));
    let target_scale = i128::from(timestamp_unit_scale(target_unit));

    let Some(literal_value) = *literal_value else {
        // A narrowing NULL has no bucket; widening and same-unit casts keep the
        // NULL on the source side.
        return (source_scale <= target_scale)
            .then(|| CastPredicatePreimage::Exact(bound(None)));
    };

    // A single source-domain half-open interval `[lower, upper)`: narrowing casts
    // many values onto the literal, so it is the truncation bucket; a widening or
    // same-unit cast maps the literal from one source value when it is aligned and
    // from none otherwise.
    let (lower, upper) = if source_scale > target_scale {
        trunc_toward_zero_bucket(literal_value, source_scale / target_scale)?
    } else {
        let ratio = target_scale / source_scale;
        let literal_value = i128::from(literal_value);
        let floor = literal_value.div_euclid(ratio);
        let ceil = floor + i128::from(literal_value.rem_euclid(ratio) != 0);
        (ceil, floor + 1)
    };

    if lower == upper {
        // No source value maps to the literal, so only ordered comparisons can
        // move the cast, and only when both sides share the source timezone.
        if source_tz != target_tz
            || !matches!(
                op,
                Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
            )
        {
            return None;
        }
    } else if upper == lower + 1 {
        // Exactly one source value maps to the literal. Its exclusive upper bound
        // can sit one past `i64::MAX`, so only `lower` is range-checked.
        return Some(CastPredicatePreimage::Exact(bound(Some(
            i64::try_from(lower).ok()?,
        ))));
    }

    let (lower, upper) = (i64::try_from(lower).ok()?, i64::try_from(upper).ok()?);
    Interval::try_new(bound(Some(lower)), bound(Some(upper)))
        .ok()
        .map(CastPredicatePreimage::Range)
}

/// Returns the half-open source-domain bucket `[lower, upper)` that truncates
/// toward zero to `value` when divided by `bucket_width`.
///
/// Timestamp precision narrowing follows integer truncation toward zero rather
/// than mathematical floor. For example, when `bucket_width = 1_000_000`, both
/// `999_999` and `-999_999` truncate to `0`, while `-1_000_000` truncates to
/// `-1`.
///
/// This makes the inverse bucket depend on the sign of `value`:
///
/// * `value > 0`: `[value * width, (value + 1) * width)`
/// * `value == 0`: `[1 - width, width)`, spanning small negative and positive
///   values that both truncate to zero
/// * `value < 0`: `[(value - 1) * width + 1, value * width + 1)`
///
/// The arithmetic uses `checked_*` operations and returns `None` if an
/// intermediate bound cannot be represented as `i128`.
fn trunc_toward_zero_bucket(value: i64, bucket_width: i128) -> Option<(i128, i128)> {
    let value = value as i128;
    match value.cmp(&0) {
        Ordering::Greater => {
            let lower = value.checked_mul(bucket_width)?;
            let upper = value.checked_add(1)?.checked_mul(bucket_width)?;
            Some((lower, upper))
        }
        Ordering::Equal => Some((1_i128.checked_sub(bucket_width)?, bucket_width)),
        Ordering::Less => {
            let lower = value
                .checked_sub(1)?
                .checked_mul(bucket_width)?
                .checked_add(1)?;
            let upper = value.checked_mul(bucket_width)?.checked_add(1)?;
            Some((lower, upper))
        }
    }
}

fn timestamp_unit_scale(unit: &TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => MILLISECONDS,
        TimeUnit::Microsecond => MICROSECONDS,
        TimeUnit::Nanosecond => NANOSECONDS,
    }
}

/// Builds a timestamp scalar in `unit` and `tz`.
fn timestamp_scalar(
    unit: &TimeUnit,
    tz: Option<Arc<str>>,
    value: Option<i64>,
) -> ScalarValue {
    match unit {
        TimeUnit::Second => ScalarValue::TimestampSecond(value, tz),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(value, tz),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(value, tz),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(value, tz),
    }
}

/// Returns true if unwrap_cast_in_comparison supports this data type
pub fn is_supported_type(data_type: &DataType) -> bool {
    is_supported_numeric_type(data_type)
        || is_supported_string_type(data_type)
        || is_supported_dictionary_type(data_type)
        || is_supported_binary_type(data_type)
}

fn is_date_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Date32 | DataType::Date64)
}

/// Returns true when unwrapping a date/timestamp cast could change comparison
/// semantics.
///
/// A `Date` stores only a calendar day, while a `Timestamp` stores a specific
/// instant or wall-clock time. `Timestamp -> Date` is lossy because it drops the
/// time-of-day. `Date -> Timestamp` is also lossy in this optimizer context
/// because there is no unique inverse: converting a date to a timestamp has to
/// invent a time component such as midnight.
///
/// For example, `CAST(ts AS DATE) = DATE '2024-01-01'` means "any timestamp
/// during that day", but unwrapping it to `ts = TIMESTAMP '2024-01-01
/// 00:00:00'` matches only midnight.
///
/// An identity cast (`from_type == to_type`, e.g. `Date32 -> Date32`) never
/// changes comparison semantics and is therefore not lossy.
///
/// A cast between the two date types (`Date32` <-> `Date64`) is not pre-filtered
/// as lossy here, because whether it loses information is a per-value question
/// rather than a per-type one. `Date32` -> `Date64` is always exact (a day scaled
/// to midnight in milliseconds). `Date64` -> `Date32` is exact only when the value
/// lands on a day boundary: Arrow nominally defines `Date64` as whole days encoded
/// in milliseconds, but arrow-rs does not enforce that (see arrow-rs#5288), so a
/// `Date64` carrying sub-day milliseconds would lose them. This is not a licence to
/// drop them - [`try_cast_numeric_literal`] returns `None` for a `Date64` value not
/// divisible by 86_400_000, so an inexact `Date64` -> `Date32` fold never happens.
fn is_lossy_temporal_cast(from_type: &DataType, to_type: &DataType) -> bool {
    if from_type == to_type {
        return false;
    }
    if is_date_type(from_type) && is_date_type(to_type) {
        return false;
    }
    (is_date_type(from_type) && to_type.is_temporal())
        || (is_date_type(to_type) && from_type.is_temporal())
}

/// Returns true when casting a date column from `from_type` to `to_type` narrows
/// `Date64` (milliseconds) to `Date32` (days).
///
/// `CAST(date64 AS Date32) = lit_day` matches any millisecond within that day,
/// but the rewritten `date64 = lit_ms` matches only midnight. Arrow does not
/// require `Date64` values to be whole days (see arrow-rs#5288), so the column
/// may carry sub-day values the planner cannot see; the widening direction
/// (`Date32 -> Date64`) is injective and stays allowed.
///
/// This pair is already rejected by the `is_exact_cast_safe` allowlist
/// (including when wrapped in a `Dictionary`), so predicate rewrites do not need
/// a separate `Date64 -> Date32` early return.
#[deprecated(
    since = "56.0.0",
    note = "Date64 -> Date32 is rejected by the cast_predicate_preimage allowlist"
)]
pub fn is_date_narrowing_cast(from_type: &DataType, to_type: &DataType) -> bool {
    matches!((from_type, to_type), (DataType::Date64, DataType::Date32))
}
/// Returns true if unwrap_cast_in_comparison supports this numeric type
fn is_supported_numeric_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Date32
            | DataType::Date64
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Timestamp(_, _)
    )
}

/// Returns true if unwrap_cast_in_comparison supports casting this value as a string
fn is_supported_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

/// Returns true if unwrap_cast_in_comparison supports casting this value as a dictionary
fn is_supported_dictionary_type(data_type: &DataType) -> bool {
    matches!(data_type,
                    DataType::Dictionary(_, inner) if is_supported_type(inner))
}

fn is_supported_binary_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Binary | DataType::FixedSizeBinary(_))
}

/// Scale a `Date32`/`Date64` literal value into the units of `target_type`,
/// returning `None` when the conversion is not exact.
///
/// `Date32` counts **days** since the Unix epoch while `Date64` counts
/// **milliseconds** since the Unix epoch, so a cross conversion scales by
/// [`MILLISECONDS_IN_DAY`]:
/// * `Date32` -> `Date64` is always exact: `days * MILLISECONDS_IN_DAY`
///   (guarded against `i64`/`i128` overflow).
/// * `Date64` -> `Date32` is exact only when the millisecond value lands on a
///   whole-day boundary; otherwise it returns `None` so the cast unwrap is
///   skipped (correct for every operator, including `=`).
///
/// For a same-type date cast or a date/integer cast the generic `mul`
/// multiplier already applies, so this returns `value * mul`.
fn scale_date_literal(
    value: i128,
    from_type: &DataType,
    target_type: &DataType,
    mul: i128,
) -> Option<i128> {
    const MILLIS_PER_DAY: i128 = MILLISECONDS_IN_DAY as i128;
    match (from_type, target_type) {
        (DataType::Date32, DataType::Date64) => value.checked_mul(MILLIS_PER_DAY),
        (DataType::Date64, DataType::Date32) => {
            (value % MILLIS_PER_DAY == 0).then_some(value / MILLIS_PER_DAY)
        }
        _ => value.checked_mul(mul),
    }
}

/// Convert a numeric value from one numeric data type to another
fn try_cast_numeric_literal(
    lit_value: &ScalarValue,
    target_type: &DataType,
) -> Option<ScalarValue> {
    let lit_data_type = lit_value.data_type();
    if !is_supported_numeric_type(&lit_data_type)
        || !is_supported_numeric_type(target_type)
    {
        return None;
    }

    if is_lossy_temporal_cast(&lit_data_type, target_type) {
        return None;
    }

    let mul = match target_type {
        DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Date32
        | DataType::Date64 => 1_i128,
        DataType::Timestamp(_, _) => 1_i128,
        DataType::Decimal32(_, scale) => 10_i128.pow(*scale as u32),
        DataType::Decimal64(_, scale) => 10_i128.pow(*scale as u32),
        DataType::Decimal128(_, scale) => 10_i128.pow(*scale as u32),
        _ => return None,
    };
    let (target_min, target_max) = match target_type {
        DataType::UInt8 => (u8::MIN as i128, u8::MAX as i128),
        DataType::UInt16 => (u16::MIN as i128, u16::MAX as i128),
        DataType::UInt32 => (u32::MIN as i128, u32::MAX as i128),
        DataType::UInt64 => (u64::MIN as i128, u64::MAX as i128),
        DataType::Int8 => (i8::MIN as i128, i8::MAX as i128),
        DataType::Int16 => (i16::MIN as i128, i16::MAX as i128),
        DataType::Int32 | DataType::Date32 => (i32::MIN as i128, i32::MAX as i128),
        DataType::Int64 | DataType::Date64 => (i64::MIN as i128, i64::MAX as i128),
        DataType::Timestamp(_, _) => (i64::MIN as i128, i64::MAX as i128),
        DataType::Decimal32(precision, _) => (
            // Different precision for decimal32 can store different range of value.
            // For example, the precision is 3, the max of value is `999` and the min
            // value is `-999`
            MIN_DECIMAL32_FOR_EACH_PRECISION[*precision as usize] as i128,
            MAX_DECIMAL32_FOR_EACH_PRECISION[*precision as usize] as i128,
        ),
        DataType::Decimal64(precision, _) => (
            // Different precision for decimal64 can store different range of value.
            // For example, the precision is 3, the max of value is `999` and the min
            // value is `-999`
            MIN_DECIMAL64_FOR_EACH_PRECISION[*precision as usize] as i128,
            MAX_DECIMAL64_FOR_EACH_PRECISION[*precision as usize] as i128,
        ),
        DataType::Decimal128(precision, _) => (
            // Different precision for decimal128 can store different range of value.
            // For example, the precision is 3, the max of value is `999` and the min
            // value is `-999`
            MIN_DECIMAL128_FOR_EACH_PRECISION[*precision as usize],
            MAX_DECIMAL128_FOR_EACH_PRECISION[*precision as usize],
        ),
        _ => return None,
    };
    let lit_value_target_type = match lit_value {
        ScalarValue::Int8(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::Int16(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::Int32(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::Int64(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::UInt8(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::UInt16(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::UInt32(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::UInt64(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::Date32(Some(v)) => {
            scale_date_literal(*v as i128, &lit_data_type, target_type, mul)
        }
        ScalarValue::Date64(Some(v)) => {
            scale_date_literal(*v as i128, &lit_data_type, target_type, mul)
        }
        ScalarValue::TimestampSecond(Some(v), _) => (*v as i128).checked_mul(mul),
        ScalarValue::TimestampMillisecond(Some(v), _) => (*v as i128).checked_mul(mul),
        ScalarValue::TimestampMicrosecond(Some(v), _) => (*v as i128).checked_mul(mul),
        ScalarValue::TimestampNanosecond(Some(v), _) => (*v as i128).checked_mul(mul),
        ScalarValue::Decimal32(Some(v), _, scale) => {
            let v = *v as i128;
            let lit_scale_mul = 10_i128.pow(*scale as u32);
            if mul >= lit_scale_mul {
                // Example:
                // lit is decimal(123,3,2)
                // target type is decimal(5,3)
                // the lit can be converted to the decimal(1230,5,3)
                v.checked_mul(mul / lit_scale_mul)
            } else if v % (lit_scale_mul / mul) == 0 {
                // Example:
                // lit is decimal(123000,10,3)
                // target type is int32: the lit can be converted to INT32(123)
                // target type is decimal(10,2): the lit can be converted to decimal(12300,10,2)
                Some(v / (lit_scale_mul / mul))
            } else {
                // can't convert the lit decimal to the target data type
                None
            }
        }
        ScalarValue::Decimal64(Some(v), _, scale) => {
            let v = *v as i128;
            let lit_scale_mul = 10_i128.pow(*scale as u32);
            if mul >= lit_scale_mul {
                // Example:
                // lit is decimal(123,3,2)
                // target type is decimal(5,3)
                // the lit can be converted to the decimal(1230,5,3)
                v.checked_mul(mul / lit_scale_mul)
            } else if v % (lit_scale_mul / mul) == 0 {
                // Example:
                // lit is decimal(123000,10,3)
                // target type is int32: the lit can be converted to INT32(123)
                // target type is decimal(10,2): the lit can be converted to decimal(12300,10,2)
                Some(v / (lit_scale_mul / mul))
            } else {
                // can't convert the lit decimal to the target data type
                None
            }
        }
        ScalarValue::Decimal128(Some(v), _, scale) => {
            let lit_scale_mul = 10_i128.pow(*scale as u32);
            if mul >= lit_scale_mul {
                // Example:
                // lit is decimal(123,3,2)
                // target type is decimal(5,3)
                // the lit can be converted to the decimal(1230,5,3)
                (*v).checked_mul(mul / lit_scale_mul)
            } else if (*v) % (lit_scale_mul / mul) == 0 {
                // Example:
                // lit is decimal(123000,10,3)
                // target type is int32: the lit can be converted to INT32(123)
                // target type is decimal(10,2): the lit can be converted to decimal(12300,10,2)
                Some(*v / (lit_scale_mul / mul))
            } else {
                // can't convert the lit decimal to the target data type
                None
            }
        }
        _ => None,
    };

    match lit_value_target_type {
        None => None,
        Some(value) => {
            if value >= target_min && value <= target_max {
                // the value casted from lit to the target type is in the range of target type.
                // return the target type of scalar value
                let result_scalar = match target_type {
                    DataType::Int8 => ScalarValue::Int8(Some(value as i8)),
                    DataType::Int16 => ScalarValue::Int16(Some(value as i16)),
                    DataType::Int32 => ScalarValue::Int32(Some(value as i32)),
                    DataType::Int64 => ScalarValue::Int64(Some(value as i64)),
                    DataType::Date32 => ScalarValue::Date32(Some(value as i32)),
                    DataType::Date64 => ScalarValue::Date64(Some(value as i64)),
                    DataType::UInt8 => ScalarValue::UInt8(Some(value as u8)),
                    DataType::UInt16 => ScalarValue::UInt16(Some(value as u16)),
                    DataType::UInt32 => ScalarValue::UInt32(Some(value as u32)),
                    DataType::UInt64 => ScalarValue::UInt64(Some(value as u64)),
                    DataType::Timestamp(TimeUnit::Second, tz) => {
                        let value = cast_between_timestamp(
                            &lit_data_type,
                            &DataType::Timestamp(TimeUnit::Second, tz.clone()),
                            value,
                        );
                        ScalarValue::TimestampSecond(value, tz.clone())
                    }
                    DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                        let value = cast_between_timestamp(
                            &lit_data_type,
                            &DataType::Timestamp(TimeUnit::Millisecond, tz.clone()),
                            value,
                        );
                        ScalarValue::TimestampMillisecond(value, tz.clone())
                    }
                    DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                        let value = cast_between_timestamp(
                            &lit_data_type,
                            &DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                            value,
                        );
                        ScalarValue::TimestampMicrosecond(value, tz.clone())
                    }
                    DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                        let value = cast_between_timestamp(
                            &lit_data_type,
                            &DataType::Timestamp(TimeUnit::Nanosecond, tz.clone()),
                            value,
                        );
                        ScalarValue::TimestampNanosecond(value, tz.clone())
                    }
                    DataType::Decimal32(p, s) => {
                        ScalarValue::Decimal32(Some(value as i32), *p, *s)
                    }
                    DataType::Decimal64(p, s) => {
                        ScalarValue::Decimal64(Some(value as i64), *p, *s)
                    }
                    DataType::Decimal128(p, s) => {
                        ScalarValue::Decimal128(Some(value), *p, *s)
                    }
                    _ => {
                        return None;
                    }
                };
                Some(result_scalar)
            } else {
                None
            }
        }
    }
}

fn try_cast_string_literal(
    lit_value: &ScalarValue,
    target_type: &DataType,
) -> Option<ScalarValue> {
    let string_value = lit_value.try_as_str()?.map(|s| s.to_string());
    let scalar_value = match target_type {
        DataType::Utf8 => ScalarValue::Utf8(string_value),
        DataType::LargeUtf8 => ScalarValue::LargeUtf8(string_value),
        DataType::Utf8View => ScalarValue::Utf8View(string_value),
        _ => return None,
    };
    Some(scalar_value)
}

/// Attempt to cast to/from a dictionary type by wrapping/unwrapping the dictionary
fn try_cast_dictionary(
    lit_value: &ScalarValue,
    target_type: &DataType,
) -> Option<ScalarValue> {
    let lit_value_type = lit_value.data_type();
    let result_scalar = match (lit_value, target_type) {
        // Unwrap dictionary when inner type matches target type
        (ScalarValue::Dictionary(_, inner_value), _)
            if inner_value.data_type() == *target_type =>
        {
            (**inner_value).clone()
        }
        // Wrap type when target type is dictionary
        (_, DataType::Dictionary(index_type, inner_type))
            if **inner_type == lit_value_type =>
        {
            ScalarValue::Dictionary(index_type.clone(), Box::new(lit_value.clone()))
        }
        _ => {
            return None;
        }
    };
    Some(result_scalar)
}

/// Cast a timestamp value from one unit to another
fn cast_between_timestamp(from: &DataType, to: &DataType, value: i128) -> Option<i64> {
    let value = value as i64;
    let from_scale = match from {
        DataType::Timestamp(unit, _) => timestamp_unit_scale(unit),
        _ => return Some(value),
    };

    let to_scale = match to {
        DataType::Timestamp(unit, _) => timestamp_unit_scale(unit),
        _ => return Some(value),
    };

    match from_scale.cmp(&to_scale) {
        Ordering::Less => value.checked_mul(to_scale / from_scale),
        Ordering::Greater => Some(value / (from_scale / to_scale)),
        Ordering::Equal => Some(value),
    }
}

fn try_cast_binary(
    lit_value: &ScalarValue,
    target_type: &DataType,
) -> Option<ScalarValue> {
    match (lit_value, target_type) {
        (ScalarValue::Binary(Some(v)), DataType::FixedSizeBinary(n))
            if v.len() == *n as usize =>
        {
            Some(ScalarValue::FixedSizeBinary(*n, Some(v.clone())))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::compute::{CastOptions, cast_with_options};
    use arrow::datatypes::{Field, Fields};
    use std::sync::Arc;

    #[derive(Debug, Clone)]
    enum ExpectedCast {
        /// test successfully cast value and it is as specified
        Value(ScalarValue),
        /// test returned OK, but could not cast the value
        NoValue,
    }

    /// Runs try_cast_literal_to_type with the specified inputs and
    /// ensure it computes the expected output, and ensures the
    /// casting is consistent with the Arrow kernels
    fn expect_cast(
        literal: ScalarValue,
        target_type: DataType,
        expected_result: ExpectedCast,
    ) {
        let actual_value = try_cast_literal_to_type(&literal, &target_type);

        println!("expect_cast: ");
        println!("  {literal:?} --> {target_type}");
        println!("  expected_result: {expected_result:?}");
        println!("  actual_result:   {actual_value:?}");

        match expected_result {
            ExpectedCast::Value(expected_value) => {
                let actual_value =
                    actual_value.expect("Expected cast value but got None");

                assert_eq!(actual_value, expected_value);

                // Verify that calling the arrow
                // cast kernel yields the same results
                // input array
                let literal_array = literal
                    .to_array_of_size(1)
                    .expect("Failed to convert to array of size");
                let expected_array = expected_value
                    .to_array_of_size(1)
                    .expect("Failed to convert to array of size");
                let cast_array = cast_with_options(
                    &literal_array,
                    &target_type,
                    &CastOptions::default(),
                )
                .expect("Expected to be cast array with arrow cast kernel");

                assert_eq!(
                    &expected_array, &cast_array,
                    "Result of casting {literal:?} with arrow was\n {cast_array:#?}\nbut expected\n{expected_array:#?}"
                );

                // Verify that for timestamp types the timezones are the same
                // (ScalarValue::cmp doesn't account for timezones);
                if let (
                    DataType::Timestamp(left_unit, left_tz),
                    DataType::Timestamp(right_unit, right_tz),
                ) = (actual_value.data_type(), expected_value.data_type())
                {
                    assert_eq!(left_unit, right_unit);
                    assert_eq!(left_tz, right_tz);
                }
            }
            ExpectedCast::NoValue => {
                assert!(
                    actual_value.is_none(),
                    "Expected no cast value, but got {actual_value:?}"
                );
            }
        }
    }

    #[test]
    fn test_try_cast_to_type_nulls() {
        // test that nulls can be cast to/from all integer types
        let scalars = vec![
            ScalarValue::Int8(None),
            ScalarValue::Int16(None),
            ScalarValue::Int32(None),
            ScalarValue::Int64(None),
            ScalarValue::UInt8(None),
            ScalarValue::UInt16(None),
            ScalarValue::UInt32(None),
            ScalarValue::UInt64(None),
            ScalarValue::Decimal128(None, 3, 0),
            ScalarValue::Decimal128(None, 8, 2),
            ScalarValue::Utf8(None),
            ScalarValue::LargeUtf8(None),
        ];

        for s1 in &scalars {
            for s2 in &scalars {
                let expected_value = ExpectedCast::Value(s2.clone());

                expect_cast(s1.clone(), s2.data_type(), expected_value);
            }
        }
    }

    #[test]
    fn test_try_cast_to_type_int_in_range() {
        // test values that can be cast to/from all integer types
        let scalars = vec![
            ScalarValue::Int8(Some(123)),
            ScalarValue::Int16(Some(123)),
            ScalarValue::Int32(Some(123)),
            ScalarValue::Int64(Some(123)),
            ScalarValue::UInt8(Some(123)),
            ScalarValue::UInt16(Some(123)),
            ScalarValue::UInt32(Some(123)),
            ScalarValue::UInt64(Some(123)),
            ScalarValue::Decimal128(Some(123), 3, 0),
            ScalarValue::Decimal128(Some(12300), 8, 2),
        ];

        for s1 in &scalars {
            for s2 in &scalars {
                let expected_value = ExpectedCast::Value(s2.clone());

                expect_cast(s1.clone(), s2.data_type(), expected_value);
            }
        }

        let max_i32 = ScalarValue::Int32(Some(i32::MAX));
        expect_cast(
            max_i32,
            DataType::UInt64,
            ExpectedCast::Value(ScalarValue::UInt64(Some(i32::MAX as u64))),
        );

        let min_i32 = ScalarValue::Int32(Some(i32::MIN));
        expect_cast(
            min_i32,
            DataType::Int64,
            ExpectedCast::Value(ScalarValue::Int64(Some(i32::MIN as i64))),
        );

        let max_i64 = ScalarValue::Int64(Some(i64::MAX));
        expect_cast(
            max_i64,
            DataType::UInt64,
            ExpectedCast::Value(ScalarValue::UInt64(Some(i64::MAX as u64))),
        );
    }

    #[test]
    fn test_try_cast_to_type_int_out_of_range() {
        let min_i32 = ScalarValue::Int32(Some(i32::MIN));
        let min_i64 = ScalarValue::Int64(Some(i64::MIN));
        let max_i64 = ScalarValue::Int64(Some(i64::MAX));
        let max_u64 = ScalarValue::UInt64(Some(u64::MAX));

        expect_cast(max_i64.clone(), DataType::Int8, ExpectedCast::NoValue);

        expect_cast(max_i64.clone(), DataType::Int16, ExpectedCast::NoValue);

        expect_cast(max_i64, DataType::Int32, ExpectedCast::NoValue);

        expect_cast(max_u64, DataType::Int64, ExpectedCast::NoValue);

        expect_cast(min_i64, DataType::UInt64, ExpectedCast::NoValue);

        expect_cast(min_i32, DataType::UInt64, ExpectedCast::NoValue);

        // decimal out of range
        expect_cast(
            ScalarValue::Decimal128(Some(99999999999999999999999999999999999900), 38, 0),
            DataType::Int64,
            ExpectedCast::NoValue,
        );

        expect_cast(
            ScalarValue::Decimal128(Some(-9999999999999999999999999999999999), 37, 1),
            DataType::Int64,
            ExpectedCast::NoValue,
        );
    }

    #[test]
    fn test_try_decimal_cast_in_range() {
        expect_cast(
            ScalarValue::Decimal128(Some(12300), 5, 2),
            DataType::Decimal128(3, 0),
            ExpectedCast::Value(ScalarValue::Decimal128(Some(123), 3, 0)),
        );

        expect_cast(
            ScalarValue::Decimal128(Some(12300), 5, 2),
            DataType::Decimal128(8, 0),
            ExpectedCast::Value(ScalarValue::Decimal128(Some(123), 8, 0)),
        );

        expect_cast(
            ScalarValue::Decimal128(Some(12300), 5, 2),
            DataType::Decimal128(8, 5),
            ExpectedCast::Value(ScalarValue::Decimal128(Some(12300000), 8, 5)),
        );
    }

    #[test]
    fn test_try_decimal_cast_out_of_range() {
        // decimal would lose precision
        expect_cast(
            ScalarValue::Decimal128(Some(12345), 5, 2),
            DataType::Decimal128(3, 0),
            ExpectedCast::NoValue,
        );

        // decimal would lose precision
        expect_cast(
            ScalarValue::Decimal128(Some(12300), 5, 2),
            DataType::Decimal128(2, 0),
            ExpectedCast::NoValue,
        );
    }

    #[test]
    fn test_try_cast_to_type_timestamps() {
        for time_unit in [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ] {
            let utc = Some("+00:00".into());
            // No timezone, utc timezone
            let (lit_tz_none, lit_tz_utc) = match time_unit {
                TimeUnit::Second => (
                    ScalarValue::TimestampSecond(Some(12345), None),
                    ScalarValue::TimestampSecond(Some(12345), utc),
                ),

                TimeUnit::Millisecond => (
                    ScalarValue::TimestampMillisecond(Some(12345), None),
                    ScalarValue::TimestampMillisecond(Some(12345), utc),
                ),

                TimeUnit::Microsecond => (
                    ScalarValue::TimestampMicrosecond(Some(12345), None),
                    ScalarValue::TimestampMicrosecond(Some(12345), utc),
                ),

                TimeUnit::Nanosecond => (
                    ScalarValue::TimestampNanosecond(Some(12345), None),
                    ScalarValue::TimestampNanosecond(Some(12345), utc),
                ),
            };

            // DataFusion ignores timezones for comparisons of ScalarValue
            // so double check it here
            assert_eq!(lit_tz_none, lit_tz_utc);

            // e.g. DataType::Timestamp(_, None)
            let dt_tz_none = lit_tz_none.data_type();

            // e.g. DataType::Timestamp(_, Some(utc))
            let dt_tz_utc = lit_tz_utc.data_type();

            // None <--> None
            expect_cast(
                lit_tz_none.clone(),
                dt_tz_none.clone(),
                ExpectedCast::Value(lit_tz_none.clone()),
            );

            // None <--> Utc
            expect_cast(
                lit_tz_none.clone(),
                dt_tz_utc.clone(),
                ExpectedCast::Value(lit_tz_utc.clone()),
            );

            // Utc <--> None
            expect_cast(
                lit_tz_utc.clone(),
                dt_tz_none.clone(),
                ExpectedCast::Value(lit_tz_none.clone()),
            );

            // Utc <--> Utc
            expect_cast(
                lit_tz_utc.clone(),
                dt_tz_utc.clone(),
                ExpectedCast::Value(lit_tz_utc.clone()),
            );

            // timestamp to int64
            expect_cast(
                lit_tz_utc.clone(),
                DataType::Int64,
                ExpectedCast::Value(ScalarValue::Int64(Some(12345))),
            );

            // int64 to timestamp
            expect_cast(
                ScalarValue::Int64(Some(12345)),
                dt_tz_none.clone(),
                ExpectedCast::Value(lit_tz_none.clone()),
            );

            // int64 to timestamp
            expect_cast(
                ScalarValue::Int64(Some(12345)),
                dt_tz_utc.clone(),
                ExpectedCast::Value(lit_tz_utc.clone()),
            );

            // timestamp to string (not supported yet)
            expect_cast(
                lit_tz_utc.clone(),
                DataType::LargeUtf8,
                ExpectedCast::NoValue,
            );
        }
    }

    #[test]
    fn test_try_cast_to_type_date_timestamp_lossy_not_allowed() {
        expect_cast(
            ScalarValue::Date32(Some(1)),
            DataType::Timestamp(TimeUnit::Second, None),
            ExpectedCast::NoValue,
        );

        expect_cast(
            ScalarValue::Date64(Some(86_400_000)),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            ExpectedCast::NoValue,
        );

        expect_cast(
            ScalarValue::TimestampSecond(Some(86_400), None),
            DataType::Date32,
            ExpectedCast::NoValue,
        );

        expect_cast(
            ScalarValue::TimestampMillisecond(Some(86_400_000), None),
            DataType::Date64,
            ExpectedCast::NoValue,
        );
    }

    #[test]
    fn test_try_cast_identity_date_allowed() {
        // An identity Date cast (e.g. `CAST(date_col AS DATE)` where the column
        // is already Date32) must fold: it never changes comparison semantics,
        // so `try_cast_literal_to_type` should return the same value rather than
        // treating it as a lossy temporal cast.
        expect_cast(
            ScalarValue::Date32(Some(19_723)),
            DataType::Date32,
            ExpectedCast::Value(ScalarValue::Date32(Some(19_723))),
        );

        expect_cast(
            ScalarValue::Date64(Some(1_704_067_200_000)),
            DataType::Date64,
            ExpectedCast::Value(ScalarValue::Date64(Some(1_704_067_200_000))),
        );

        // is_lossy_temporal_cast must classify an identity cast as non-lossy.
        assert!(!is_lossy_temporal_cast(
            &DataType::Date32,
            &DataType::Date32
        ));
        assert!(!is_lossy_temporal_cast(
            &DataType::Date64,
            &DataType::Date64
        ));
    }

    #[test]
    fn test_try_cast_between_date32_and_date64() {
        // 2025-01-01 is day 20089 since the Unix epoch, which is
        // 20089 * 86_400_000 = 1_735_689_600_000 milliseconds.
        const DAY_2025_01_01: i32 = 20089;
        const MS_2025_01_01: i64 = 1_735_689_600_000;
        assert_eq!(DAY_2025_01_01 as i64 * MILLISECONDS_IN_DAY, MS_2025_01_01);

        // Date32 -> Date64 is always exact (days scaled up to milliseconds).
        expect_cast(
            ScalarValue::Date32(Some(DAY_2025_01_01)),
            DataType::Date64,
            ExpectedCast::Value(ScalarValue::Date64(Some(MS_2025_01_01))),
        );

        // Date64 -> Date32 is exact only on a whole-day boundary.
        expect_cast(
            ScalarValue::Date64(Some(MS_2025_01_01)),
            DataType::Date32,
            ExpectedCast::Value(ScalarValue::Date32(Some(DAY_2025_01_01))),
        );

        // A Date64 value that is not on a day boundary cannot be represented as
        // a Date32 exactly, so no rewrite is produced.
        expect_cast(
            ScalarValue::Date64(Some(MS_2025_01_01 + 1)),
            DataType::Date32,
            ExpectedCast::NoValue,
        );
        expect_cast(
            ScalarValue::Date64(Some(MS_2025_01_01 - 1)),
            DataType::Date32,
            ExpectedCast::NoValue,
        );

        // The epoch and negative (pre-epoch) days round-trip exactly.
        expect_cast(
            ScalarValue::Date32(Some(0)),
            DataType::Date64,
            ExpectedCast::Value(ScalarValue::Date64(Some(0))),
        );
        expect_cast(
            ScalarValue::Date32(Some(-1)),
            DataType::Date64,
            ExpectedCast::Value(ScalarValue::Date64(Some(-MILLISECONDS_IN_DAY))),
        );
        expect_cast(
            ScalarValue::Date64(Some(-MILLISECONDS_IN_DAY)),
            DataType::Date32,
            ExpectedCast::Value(ScalarValue::Date32(Some(-1))),
        );

        // Same-type date casts remain identity conversions.
        expect_cast(
            ScalarValue::Date32(Some(DAY_2025_01_01)),
            DataType::Date32,
            ExpectedCast::Value(ScalarValue::Date32(Some(DAY_2025_01_01))),
        );
        expect_cast(
            ScalarValue::Date64(Some(MS_2025_01_01)),
            DataType::Date64,
            ExpectedCast::Value(ScalarValue::Date64(Some(MS_2025_01_01))),
        );
    }

    #[test]
    fn test_is_lossy_temporal_cast_date_pairs() {
        // Date <-> Date is let through the pre-filter (per-value exactness is
        // enforced downstream in try_cast_numeric_literal, not here).
        assert!(!is_lossy_temporal_cast(
            &DataType::Date32,
            &DataType::Date64
        ));
        assert!(!is_lossy_temporal_cast(
            &DataType::Date64,
            &DataType::Date32
        ));
        // Identity is not lossy.
        assert!(!is_lossy_temporal_cast(
            &DataType::Date32,
            &DataType::Date32
        ));
        // Date <-> Timestamp remains lossy.
        let ts = DataType::Timestamp(TimeUnit::Millisecond, None);
        assert!(is_lossy_temporal_cast(&DataType::Date32, &ts));
        assert!(is_lossy_temporal_cast(&ts, &DataType::Date32));
    }

    #[test]
    fn test_timestamp_precision_narrowing_cast() {
        let ts_ns = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let ts_us = DataType::Timestamp(TimeUnit::Microsecond, None);
        let ts_ms = DataType::Timestamp(TimeUnit::Millisecond, None);
        let ts_s = DataType::Timestamp(TimeUnit::Second, None);

        assert!(is_timestamp_precision_narrowing_cast(&ts_ns, &ts_ms));
        assert!(is_timestamp_precision_narrowing_cast(&ts_us, &ts_s));
        assert!(!is_timestamp_precision_narrowing_cast(&ts_ms, &ts_ns));
        assert!(!is_timestamp_precision_narrowing_cast(&ts_ms, &ts_ms));
        assert!(!is_timestamp_precision_narrowing_cast(
            &DataType::Int64,
            &ts_ms
        ));
    }

    #[test]
    fn test_cast_predicate_preimage_duplicates_input() {
        let exact = CastPredicatePreimage::Exact(ScalarValue::Int32(Some(1)));
        let range = CastPredicatePreimage::Range(
            Interval::try_new(ScalarValue::Int32(Some(0)), ScalarValue::Int32(Some(2)))
                .unwrap(),
        );
        let cases = [
            (Operator::Eq, true),
            (Operator::NotEq, true),
            (Operator::IsDistinctFrom, true),
            (Operator::IsNotDistinctFrom, true),
            (Operator::Lt, false),
            (Operator::LtEq, false),
            (Operator::Gt, false),
            (Operator::GtEq, false),
        ];

        for (op, range_duplicates) in cases {
            assert!(!exact.duplicates_input(op), "Exact with {op:?}");
            assert_eq!(
                range.duplicates_input(op),
                range_duplicates,
                "Range with {op:?}"
            );
        }
    }

    #[test]
    #[expect(
        deprecated,
        reason = "kept to verify the deprecated helper's semantics"
    )]
    fn test_is_date_narrowing_cast() {
        // Only Date64 -> Date32 narrows (ms -> days, many-to-one).
        assert!(is_date_narrowing_cast(&DataType::Date64, &DataType::Date32));
        // The widening direction is injective and must not be flagged.
        assert!(!is_date_narrowing_cast(
            &DataType::Date32,
            &DataType::Date64
        ));
        // Identity and non-date pairs are not date-narrowing casts.
        assert!(!is_date_narrowing_cast(
            &DataType::Date32,
            &DataType::Date32
        ));
        assert!(!is_date_narrowing_cast(
            &DataType::Date64,
            &DataType::Date64
        ));
        assert!(!is_date_narrowing_cast(&DataType::Int64, &DataType::Date32));
    }

    #[test]
    fn test_scale_date_literal_exactness_and_overflow() {
        const MS_PER_DAY: i128 = MILLISECONDS_IN_DAY as i128;

        // Date32 -> Date64 is always exact: days scaled to midnight milliseconds.
        // 2025-01-01 is day 20089 = 1_735_689_600_000 ms.
        assert_eq!(
            scale_date_literal(20089, &DataType::Date32, &DataType::Date64, 1),
            Some(1_735_689_600_000)
        );
        assert_eq!(
            scale_date_literal(0, &DataType::Date32, &DataType::Date64, 1),
            Some(0)
        );
        // Negative (pre-epoch) whole day: 1969-12-31 is day -1 = -86_400_000 ms.
        assert_eq!(
            scale_date_literal(-1, &DataType::Date32, &DataType::Date64, 1),
            Some(-86_400_000)
        );

        // Date64 -> Date32 is exact only on a whole-day boundary.
        assert_eq!(
            scale_date_literal(
                1_735_689_600_000,
                &DataType::Date64,
                &DataType::Date32,
                1
            ),
            Some(20089)
        );
        assert_eq!(
            scale_date_literal(-86_400_000, &DataType::Date64, &DataType::Date32, 1),
            Some(-1)
        );
        // Sub-day values are not exactly representable as a Date32, in both the
        // positive and the pre-epoch negative direction -> None (no fold).
        assert_eq!(
            scale_date_literal(
                1_735_732_800_000,
                &DataType::Date64,
                &DataType::Date32,
                1
            ),
            None
        );
        assert_eq!(
            scale_date_literal(-43_200_000, &DataType::Date64, &DataType::Date32, 1),
            None
        );

        // Extremes: a Date32 at i32::MIN / i32::MAX widens with checked i128
        // arithmetic, producing the exact millisecond value without overflow or
        // panic.
        assert_eq!(
            scale_date_literal(i32::MAX as i128, &DataType::Date32, &DataType::Date64, 1),
            Some(i32::MAX as i128 * MS_PER_DAY)
        );
        assert_eq!(
            scale_date_literal(i32::MIN as i128, &DataType::Date32, &DataType::Date64, 1),
            Some(i32::MIN as i128 * MS_PER_DAY)
        );
    }

    #[test]
    fn test_try_cast_to_type_unsupported() {
        // int64 to list
        expect_cast(
            ScalarValue::Int64(Some(12345)),
            DataType::List(Arc::new(Field::new("f", DataType::Int32, true))),
            ExpectedCast::NoValue,
        );
    }

    #[test]
    fn test_try_cast_literal_to_timestamp() {
        // same timestamp
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampNanosecond(Some(123456), None),
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .unwrap();

        assert_eq!(
            new_scalar,
            ScalarValue::TimestampNanosecond(Some(123456), None)
        );

        // TimestampNanosecond to TimestampMicrosecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampNanosecond(Some(123456), None),
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .unwrap();

        assert_eq!(
            new_scalar,
            ScalarValue::TimestampMicrosecond(Some(123), None)
        );

        // TimestampNanosecond to TimestampMillisecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampNanosecond(Some(123456), None),
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )
        .unwrap();

        assert_eq!(new_scalar, ScalarValue::TimestampMillisecond(Some(0), None));

        // TimestampNanosecond to TimestampSecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampNanosecond(Some(123456), None),
            &DataType::Timestamp(TimeUnit::Second, None),
        )
        .unwrap();

        assert_eq!(new_scalar, ScalarValue::TimestampSecond(Some(0), None));

        // TimestampMicrosecond to TimestampNanosecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampMicrosecond(Some(123), None),
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .unwrap();

        assert_eq!(
            new_scalar,
            ScalarValue::TimestampNanosecond(Some(123000), None)
        );

        // TimestampMicrosecond to TimestampMillisecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampMicrosecond(Some(123), None),
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )
        .unwrap();

        assert_eq!(new_scalar, ScalarValue::TimestampMillisecond(Some(0), None));

        // TimestampMicrosecond to TimestampSecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampMicrosecond(Some(123456789), None),
            &DataType::Timestamp(TimeUnit::Second, None),
        )
        .unwrap();
        assert_eq!(new_scalar, ScalarValue::TimestampSecond(Some(123), None));

        // TimestampMillisecond to TimestampNanosecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampMillisecond(Some(123), None),
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .unwrap();
        assert_eq!(
            new_scalar,
            ScalarValue::TimestampNanosecond(Some(123000000), None)
        );

        // TimestampMillisecond to TimestampMicrosecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampMillisecond(Some(123), None),
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .unwrap();
        assert_eq!(
            new_scalar,
            ScalarValue::TimestampMicrosecond(Some(123000), None)
        );
        // TimestampMillisecond to TimestampSecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampMillisecond(Some(123456789), None),
            &DataType::Timestamp(TimeUnit::Second, None),
        )
        .unwrap();
        assert_eq!(new_scalar, ScalarValue::TimestampSecond(Some(123456), None));

        // TimestampSecond to TimestampNanosecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampSecond(Some(123), None),
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .unwrap();
        assert_eq!(
            new_scalar,
            ScalarValue::TimestampNanosecond(Some(123000000000), None)
        );

        // TimestampSecond to TimestampMicrosecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampSecond(Some(123), None),
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .unwrap();
        assert_eq!(
            new_scalar,
            ScalarValue::TimestampMicrosecond(Some(123000000), None)
        );

        // TimestampSecond to TimestampMillisecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampSecond(Some(123), None),
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )
        .unwrap();
        assert_eq!(
            new_scalar,
            ScalarValue::TimestampMillisecond(Some(123000), None)
        );

        // overflow
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampSecond(Some(i64::MAX), None),
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )
        .unwrap();
        assert_eq!(new_scalar, ScalarValue::TimestampMillisecond(None, None));
    }

    #[test]
    fn test_cast_predicate_preimage_exact() {
        assert_preimage_exact(
            &DataType::Int32,
            &DataType::Int64,
            Operator::Gt,
            &ScalarValue::Int64(Some(10)),
            ScalarValue::Int32(Some(10)),
        );

        assert_preimage_exact(
            &DataType::Int32,
            &DataType::Utf8,
            Operator::Eq,
            &ScalarValue::Utf8(Some("123".to_string())),
            ScalarValue::Int32(Some(123)),
        );

        assert_preimage_none(
            &DataType::Int32,
            &DataType::Utf8,
            Operator::Eq,
            &ScalarValue::Utf8(Some("0123".to_string())),
        );
    }

    #[test]
    fn test_cast_predicate_preimage_timestamp_narrowing_range() {
        // Truncation toward zero decides the bucket by sign: positive, zero spans
        // both sides, and negative buckets shift by one.
        for (source_unit, target_unit, lit, lower, upper) in [
            (
                TimeUnit::Nanosecond,
                TimeUnit::Millisecond,
                1000,
                1_000_000_000,
                1_001_000_000,
            ),
            (
                TimeUnit::Nanosecond,
                TimeUnit::Millisecond,
                0,
                -999_999,
                1_000_000,
            ),
            (
                TimeUnit::Nanosecond,
                TimeUnit::Millisecond,
                -1,
                -1_999_999,
                -999_999,
            ),
            (
                TimeUnit::Microsecond,
                TimeUnit::Second,
                1,
                1_000_000,
                2_000_000,
            ),
            (
                TimeUnit::Nanosecond,
                TimeUnit::Second,
                -2,
                -2_999_999_999,
                -1_999_999_999,
            ),
            (TimeUnit::Nanosecond, TimeUnit::Microsecond, 0, -999, 1_000),
            (TimeUnit::Millisecond, TimeUnit::Second, 1, 1000, 2000),
            (TimeUnit::Millisecond, TimeUnit::Second, -1, -1999, -999),
            (TimeUnit::Microsecond, TimeUnit::Millisecond, 0, -999, 1000),
        ] {
            assert_timestamp_narrowing_range(source_unit, target_unit, lit, lower, upper);
        }
    }

    #[test]
    fn test_timestamp_narrowing_range_rejects_naive_to_timezone() {
        let source = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let target = DataType::Timestamp(TimeUnit::Millisecond, Some("+01:00".into()));
        let literal = ScalarValue::TimestampMillisecond(Some(0), Some("+01:00".into()));
        assert_preimage_none(&source, &target, Operator::Eq, &literal);
    }

    #[test]
    fn test_naive_to_timezone_timestamp_rejected() {
        // Same unit, widening and narrowing targets: Arrow adjusts naive
        // values to UTC, which the precision arithmetic does not model. The
        // literal is unit-aligned (0), so the rejection comes from the timezone
        // adjustment and not from an unaligned widening bound.
        for (source_unit, target_unit) in [
            (TimeUnit::Nanosecond, TimeUnit::Nanosecond),
            (TimeUnit::Millisecond, TimeUnit::Nanosecond),
            (TimeUnit::Nanosecond, TimeUnit::Millisecond),
        ] {
            let source = DataType::Timestamp(source_unit, None);
            let target = DataType::Timestamp(target_unit, Some("+07:00".into()));
            let literal = timestamp_scalar(&target_unit, Some("+07:00".into()), Some(0));

            assert!(
                exact_preimage_cast(&source, &target, &literal).is_none(),
                "{source:?} -> {target:?} must not have an exact preimage"
            );
            for op in [
                Operator::Eq,
                Operator::Gt,
                Operator::LtEq,
                Operator::IsDistinctFrom,
            ] {
                assert_preimage_none(&source, &target, op, &literal);
            }
        }

        // Conservative for every timezone, including fixed zero offsets:
        // `Some("UTC")` is rejected like `Some("+07:00")`.
        let source = DataType::Timestamp(TimeUnit::Nanosecond, None);
        for tz in ["UTC", "+00:00"] {
            let target = DataType::Timestamp(TimeUnit::Nanosecond, Some(tz.into()));
            assert_preimage_none(
                &source,
                &target,
                Operator::Eq,
                &ScalarValue::TimestampNanosecond(Some(0), Some(tz.into())),
            );
        }

        // Positive control: a timezone-aware source keeps the accepted same-unit
        // and widening policies.
        let aware_ms = DataType::Timestamp(TimeUnit::Millisecond, Some("+07:00".into()));
        let aware_ns = DataType::Timestamp(TimeUnit::Nanosecond, Some("+07:00".into()));
        assert_preimage_exact(
            &aware_ns,
            &aware_ns,
            Operator::Eq,
            &ScalarValue::TimestampNanosecond(Some(7), Some("+07:00".into())),
            ScalarValue::TimestampNanosecond(Some(7), Some("+07:00".into())),
        );
        assert_preimage_exact(
            &aware_ms,
            &aware_ns,
            Operator::Eq,
            &ScalarValue::TimestampNanosecond(Some(7_000_000), Some("+07:00".into())),
            ScalarValue::TimestampMillisecond(Some(7), Some("+07:00".into())),
        );
    }

    #[test]
    fn test_naive_to_timezone_dictionary_wrapped_rejected() {
        let dict = |inner: DataType| {
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(inner))
        };
        let naive_ns = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let aware_ns = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
        let literal = ScalarValue::TimestampNanosecond(Some(0), Some("UTC".into()));

        // Timestamp preimages unwrap one level of dictionary, so a wrapped
        // naive -> aware pair is rejected like the bare pair.
        assert_preimage_none(&naive_ns, &aware_ns, Operator::Eq, &literal);
        assert_preimage_none(
            &dict(naive_ns.clone()),
            &dict(aware_ns.clone()),
            Operator::Eq,
            &literal,
        );
        assert_preimage_none(&dict(naive_ns.clone()), &aware_ns, Operator::Eq, &literal);
        assert_preimage_none(&naive_ns, &dict(aware_ns.clone()), Operator::Eq, &literal);

        // Safe controls stay allowed: aware -> aware unit widening and identity.
        assert_preimage_exact(
            &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            Operator::Eq,
            &ScalarValue::TimestampNanosecond(Some(1_000_000), Some("UTC".into())),
            ScalarValue::TimestampMillisecond(Some(1), Some("UTC".into())),
        );
        assert_preimage_exact(
            &aware_ns,
            &aware_ns,
            Operator::Eq,
            &literal,
            ScalarValue::TimestampNanosecond(Some(0), Some("UTC".into())),
        );
    }

    #[test]
    fn test_cast_predicate_preimage_timestamp_widening_ordered() {
        let ts_ms = DataType::Timestamp(TimeUnit::Millisecond, None);
        let ts_ns = DataType::Timestamp(TimeUnit::Nanosecond, None);

        assert_preimage_exact(
            &ts_ms,
            &ts_ns,
            Operator::Eq,
            &ScalarValue::TimestampNanosecond(Some(123_000_000), None),
            ScalarValue::TimestampMillisecond(Some(123), None),
        );

        for (source_unit, target_unit) in [
            (TimeUnit::Second, TimeUnit::Millisecond),
            (TimeUnit::Second, TimeUnit::Microsecond),
            (TimeUnit::Second, TimeUnit::Nanosecond),
            (TimeUnit::Millisecond, TimeUnit::Microsecond),
            (TimeUnit::Millisecond, TimeUnit::Nanosecond),
            (TimeUnit::Microsecond, TimeUnit::Nanosecond),
        ] {
            assert_timestamp_widening_ordered(source_unit, target_unit);
        }

        assert_preimage_none(
            &ts_ms,
            &ts_ns,
            Operator::Eq,
            &ScalarValue::TimestampNanosecond(Some(123_456_789), None),
        );
    }

    #[test]
    fn test_timestamp_widening_ordered_rejections_and_timezone() {
        let source_type = DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
        let target_type = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
        let literal =
            ScalarValue::TimestampNanosecond(Some(123_456_789), Some("UTC".into()));

        // A non-aligned literal has no singleton source value, so ordered
        // comparisons use the empty interval and equality-like ones keep the
        // cast. `123_456_789 / 1_000_000` floors to 123, so the interval is
        // `[124, 124)`.
        let empty = Some(CastPredicatePreimage::Range(
            Interval::try_new(
                ScalarValue::TimestampMillisecond(Some(124), Some("UTC".into())),
                ScalarValue::TimestampMillisecond(Some(124), Some("UTC".into())),
            )
            .unwrap(),
        ));
        for op in [Operator::GtEq, Operator::Gt, Operator::Lt, Operator::LtEq] {
            assert_eq!(
                cast_predicate_preimage(&source_type, &target_type, op, &literal)
                    .unwrap(),
                empty
            );
        }
        for op in [
            Operator::Eq,
            Operator::NotEq,
            Operator::IsDistinctFrom,
            Operator::IsNotDistinctFrom,
        ] {
            assert_preimage_none(&source_type, &target_type, op, &literal);
        }

        // A literal of the wrong unit or the wrong timezone is not converted.
        for literal in [
            ScalarValue::TimestampMicrosecond(Some(123_456), Some("UTC".into())),
            ScalarValue::TimestampNanosecond(Some(123_456_789), None),
            ScalarValue::Int64(Some(123_456_789)),
        ] {
            for op in [Operator::Eq, Operator::GtEq, Operator::LtEq] {
                assert_preimage_none(&source_type, &target_type, op, &literal);
            }
        }

        // A source/target timezone change has no source-domain bound.
        assert_preimage_none(
            &source_type,
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
            Operator::GtEq,
            &ScalarValue::TimestampNanosecond(Some(123_456_789), None),
        );
    }

    #[test]
    fn test_cast_predicate_preimage_timestamp_null_literal() {
        let ts_ns = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let ts_ms = DataType::Timestamp(TimeUnit::Millisecond, None);
        let null_ms = ScalarValue::TimestampMillisecond(None, None);

        // Narrowing a typed NULL literal has no source-domain bucket.
        for op in [
            Operator::Eq,
            Operator::NotEq,
            Operator::IsDistinctFrom,
            Operator::IsNotDistinctFrom,
            Operator::Gt,
            Operator::LtEq,
        ] {
            assert_preimage_none(&ts_ns, &ts_ms, op, &null_ms);
        }

        // Widening and same-unit casts keep the NULL result on the source side.
        // The literal must be typed in the target unit, as SQL coercion produces.
        for target_unit in [TimeUnit::Millisecond, TimeUnit::Nanosecond] {
            let target_type = DataType::Timestamp(target_unit, None);
            assert_eq!(
                cast_predicate_preimage(
                    &ts_ms,
                    &target_type,
                    Operator::Eq,
                    &timestamp_scalar(&target_unit, None, None),
                )
                .unwrap(),
                Some(CastPredicatePreimage::Exact(
                    ScalarValue::TimestampMillisecond(None, None)
                ))
            );
        }
    }

    #[test]
    fn test_try_cast_to_string_type() {
        let scalars = vec![
            ScalarValue::from("string"),
            ScalarValue::LargeUtf8(Some("string".to_owned())),
        ];

        for s1 in &scalars {
            for s2 in &scalars {
                let expected_value = ExpectedCast::Value(s2.clone());

                expect_cast(s1.clone(), s2.data_type(), expected_value);
            }
        }
    }

    #[test]
    fn test_try_cast_to_dictionary_type() {
        fn dictionary_type(t: DataType) -> DataType {
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(t))
        }
        fn dictionary_value(value: ScalarValue) -> ScalarValue {
            ScalarValue::Dictionary(Box::new(DataType::Int32), Box::new(value))
        }
        let scalars = vec![
            ScalarValue::from("string"),
            ScalarValue::LargeUtf8(Some("string".to_owned())),
        ];
        for s in &scalars {
            expect_cast(
                s.clone(),
                dictionary_type(s.data_type()),
                ExpectedCast::Value(dictionary_value(s.clone())),
            );
            expect_cast(
                dictionary_value(s.clone()),
                s.data_type(),
                ExpectedCast::Value(s.clone()),
            )
        }
    }

    #[test]
    fn test_try_cast_to_fixed_size_binary() {
        expect_cast(
            ScalarValue::Binary(Some(vec![1, 2, 3])),
            DataType::FixedSizeBinary(3),
            ExpectedCast::Value(ScalarValue::FixedSizeBinary(3, Some(vec![1, 2, 3]))),
        )
    }

    #[test]
    fn test_numeric_boundary_values() {
        // Test exact boundary values for signed integers
        expect_cast(
            ScalarValue::Int8(Some(i8::MAX)),
            DataType::UInt8,
            ExpectedCast::Value(ScalarValue::UInt8(Some(i8::MAX as u8))),
        );

        expect_cast(
            ScalarValue::Int8(Some(i8::MIN)),
            DataType::UInt8,
            ExpectedCast::NoValue,
        );

        expect_cast(
            ScalarValue::UInt8(Some(u8::MAX)),
            DataType::Int8,
            ExpectedCast::NoValue,
        );

        // Test cross-type boundary scenarios
        expect_cast(
            ScalarValue::Int32(Some(i32::MAX)),
            DataType::Int64,
            ExpectedCast::Value(ScalarValue::Int64(Some(i32::MAX as i64))),
        );

        expect_cast(
            ScalarValue::Int64(Some(i64::MIN)),
            DataType::UInt64,
            ExpectedCast::NoValue,
        );

        // Test unsigned to signed edge cases
        expect_cast(
            ScalarValue::UInt32(Some(u32::MAX)),
            DataType::Int32,
            ExpectedCast::NoValue,
        );

        expect_cast(
            ScalarValue::UInt64(Some(u64::MAX)),
            DataType::Int64,
            ExpectedCast::NoValue,
        );
    }

    #[test]
    fn test_decimal_precision_limits() {
        use arrow::datatypes::{
            MAX_DECIMAL128_FOR_EACH_PRECISION, MIN_DECIMAL128_FOR_EACH_PRECISION,
        };

        // Test maximum precision values
        expect_cast(
            ScalarValue::Decimal128(Some(MAX_DECIMAL128_FOR_EACH_PRECISION[3]), 3, 0),
            DataType::Decimal128(5, 0),
            ExpectedCast::Value(ScalarValue::Decimal128(
                Some(MAX_DECIMAL128_FOR_EACH_PRECISION[3]),
                5,
                0,
            )),
        );

        // Test minimum precision values
        expect_cast(
            ScalarValue::Decimal128(Some(MIN_DECIMAL128_FOR_EACH_PRECISION[3]), 3, 0),
            DataType::Decimal128(5, 0),
            ExpectedCast::Value(ScalarValue::Decimal128(
                Some(MIN_DECIMAL128_FOR_EACH_PRECISION[3]),
                5,
                0,
            )),
        );

        // Test scale increase
        expect_cast(
            ScalarValue::Decimal128(Some(123), 3, 0),
            DataType::Decimal128(5, 2),
            ExpectedCast::Value(ScalarValue::Decimal128(Some(12300), 5, 2)),
        );

        // Test precision overflow (value too large for target precision)
        expect_cast(
            ScalarValue::Decimal128(Some(MAX_DECIMAL128_FOR_EACH_PRECISION[10]), 10, 0),
            DataType::Decimal128(3, 0),
            ExpectedCast::NoValue,
        );

        // Test non-divisible decimal conversion (should fail)
        expect_cast(
            ScalarValue::Decimal128(Some(12345), 5, 3), // 12.345
            DataType::Int32,
            ExpectedCast::NoValue, // Can't convert 12.345 to integer without loss
        );

        // Test edge case: scale reduction with precision loss
        expect_cast(
            ScalarValue::Decimal128(Some(12345), 5, 2), // 123.45
            DataType::Decimal128(3, 0),                 // Can only hold up to 999
            ExpectedCast::NoValue,
        );
    }

    #[test]
    fn test_timestamp_overflow_scenarios() {
        // Test overflow in timestamp conversions
        let max_seconds = i64::MAX / 1_000_000_000; // Avoid overflow when converting to nanos

        // This should work - within safe range
        expect_cast(
            ScalarValue::TimestampSecond(Some(max_seconds), None),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            ExpectedCast::Value(ScalarValue::TimestampNanosecond(
                Some(max_seconds * 1_000_000_000),
                None,
            )),
        );

        // Test very large nanosecond value conversion to smaller units
        expect_cast(
            ScalarValue::TimestampNanosecond(Some(i64::MAX), None),
            DataType::Timestamp(TimeUnit::Second, None),
            ExpectedCast::Value(ScalarValue::TimestampSecond(
                Some(i64::MAX / 1_000_000_000),
                None,
            )),
        );

        // Test precision loss in downscaling
        expect_cast(
            ScalarValue::TimestampNanosecond(Some(1), None),
            DataType::Timestamp(TimeUnit::Second, None),
            ExpectedCast::Value(ScalarValue::TimestampSecond(Some(0), None)),
        );

        expect_cast(
            ScalarValue::TimestampMicrosecond(Some(999), None),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            ExpectedCast::Value(ScalarValue::TimestampMillisecond(Some(0), None)),
        );
    }

    #[test]
    fn test_string_view() {
        // Test Utf8View to other string types
        expect_cast(
            ScalarValue::Utf8View(Some("test".to_string())),
            DataType::Utf8,
            ExpectedCast::Value(ScalarValue::Utf8(Some("test".to_string()))),
        );

        expect_cast(
            ScalarValue::Utf8View(Some("test".to_string())),
            DataType::LargeUtf8,
            ExpectedCast::Value(ScalarValue::LargeUtf8(Some("test".to_string()))),
        );

        // Test other string types to Utf8View
        expect_cast(
            ScalarValue::Utf8(Some("hello".to_string())),
            DataType::Utf8View,
            ExpectedCast::Value(ScalarValue::Utf8View(Some("hello".to_string()))),
        );

        expect_cast(
            ScalarValue::LargeUtf8(Some("world".to_string())),
            DataType::Utf8View,
            ExpectedCast::Value(ScalarValue::Utf8View(Some("world".to_string()))),
        );

        // Test empty string
        expect_cast(
            ScalarValue::Utf8(Some("".to_string())),
            DataType::Utf8View,
            ExpectedCast::Value(ScalarValue::Utf8View(Some("".to_string()))),
        );

        // Test large string
        let large_string = "x".repeat(1000);
        expect_cast(
            ScalarValue::LargeUtf8(Some(large_string.clone())),
            DataType::Utf8View,
            ExpectedCast::Value(ScalarValue::Utf8View(Some(large_string))),
        );
    }

    #[test]
    fn test_binary_size_edge_cases() {
        // Test size mismatch - too small
        expect_cast(
            ScalarValue::Binary(Some(vec![1, 2])),
            DataType::FixedSizeBinary(3),
            ExpectedCast::NoValue,
        );

        // Test size mismatch - too large
        expect_cast(
            ScalarValue::Binary(Some(vec![1, 2, 3, 4])),
            DataType::FixedSizeBinary(3),
            ExpectedCast::NoValue,
        );

        // Test empty binary
        expect_cast(
            ScalarValue::Binary(Some(vec![])),
            DataType::FixedSizeBinary(0),
            ExpectedCast::Value(ScalarValue::FixedSizeBinary(0, Some(vec![]))),
        );

        // Test exact size match
        expect_cast(
            ScalarValue::Binary(Some(vec![1, 2, 3])),
            DataType::FixedSizeBinary(3),
            ExpectedCast::Value(ScalarValue::FixedSizeBinary(3, Some(vec![1, 2, 3]))),
        );

        // Test single byte
        expect_cast(
            ScalarValue::Binary(Some(vec![42])),
            DataType::FixedSizeBinary(1),
            ExpectedCast::Value(ScalarValue::FixedSizeBinary(1, Some(vec![42]))),
        );
    }

    #[test]
    fn test_dictionary_index_types() {
        // Test different dictionary index types
        let string_value = ScalarValue::Utf8(Some("test".to_string()));

        // Int8 index dictionary
        let dict_int8 =
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
        expect_cast(
            string_value.clone(),
            dict_int8,
            ExpectedCast::Value(ScalarValue::Dictionary(
                Box::new(DataType::Int8),
                Box::new(string_value.clone()),
            )),
        );

        // Int16 index dictionary
        let dict_int16 =
            DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8));
        expect_cast(
            string_value.clone(),
            dict_int16,
            ExpectedCast::Value(ScalarValue::Dictionary(
                Box::new(DataType::Int16),
                Box::new(string_value.clone()),
            )),
        );

        // Int64 index dictionary
        let dict_int64 =
            DataType::Dictionary(Box::new(DataType::Int64), Box::new(DataType::Utf8));
        expect_cast(
            string_value.clone(),
            dict_int64,
            ExpectedCast::Value(ScalarValue::Dictionary(
                Box::new(DataType::Int64),
                Box::new(string_value.clone()),
            )),
        );

        // Test dictionary unwrapping
        let dict_value = ScalarValue::Dictionary(
            Box::new(DataType::Int32),
            Box::new(ScalarValue::LargeUtf8(Some("unwrap_test".to_string()))),
        );
        expect_cast(
            dict_value,
            DataType::LargeUtf8,
            ExpectedCast::Value(ScalarValue::LargeUtf8(Some("unwrap_test".to_string()))),
        );
    }

    #[test]
    fn test_type_support_functions() {
        // Test numeric type support
        assert!(is_supported_numeric_type(&DataType::Int8));
        assert!(is_supported_numeric_type(&DataType::UInt64));
        assert!(is_supported_numeric_type(&DataType::Decimal128(10, 2)));
        assert!(is_supported_numeric_type(&DataType::Timestamp(
            TimeUnit::Nanosecond,
            None
        )));
        assert!(!is_supported_numeric_type(&DataType::Float32));
        assert!(!is_supported_numeric_type(&DataType::Float64));

        // Test string type support
        assert!(is_supported_string_type(&DataType::Utf8));
        assert!(is_supported_string_type(&DataType::LargeUtf8));
        assert!(is_supported_string_type(&DataType::Utf8View));
        assert!(!is_supported_string_type(&DataType::Binary));

        // Test binary type support
        assert!(is_supported_binary_type(&DataType::Binary));
        assert!(is_supported_binary_type(&DataType::FixedSizeBinary(10)));
        assert!(!is_supported_binary_type(&DataType::Utf8));

        // Test dictionary type support with nested types
        assert!(is_supported_dictionary_type(&DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Utf8)
        )));
        assert!(is_supported_dictionary_type(&DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Int64)
        )));
        assert!(!is_supported_dictionary_type(&DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Int32,
                true
            ))))
        )));

        // Test overall type support
        assert!(is_supported_type(&DataType::Int32));
        assert!(is_supported_type(&DataType::Utf8));
        assert!(is_supported_type(&DataType::Binary));
        assert!(is_supported_type(&DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Utf8)
        )));
        assert!(!is_supported_type(&DataType::List(Arc::new(Field::new(
            "item",
            DataType::Int32,
            true
        )))));
        assert!(!is_supported_type(&DataType::Struct(Fields::empty())));
    }

    #[test]
    fn test_error_conditions() {
        // Test unsupported source type
        expect_cast(
            ScalarValue::Float32(Some(1.5)),
            DataType::Int32,
            ExpectedCast::NoValue,
        );

        // Test unsupported target type
        expect_cast(
            ScalarValue::Int32(Some(123)),
            DataType::Float64,
            ExpectedCast::NoValue,
        );

        // Test both types unsupported
        expect_cast(
            ScalarValue::Float64(Some(1.5)),
            DataType::Float32,
            ExpectedCast::NoValue,
        );

        // Test complex unsupported types
        let list_type =
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        expect_cast(
            ScalarValue::Int32(Some(123)),
            list_type,
            ExpectedCast::NoValue,
        );

        // Test dictionary with unsupported inner type
        let bad_dict = DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Int32,
                true,
            )))),
        );
        expect_cast(
            ScalarValue::Int32(Some(123)),
            bad_dict,
            ExpectedCast::NoValue,
        );
    }

    #[test]
    fn test_cast_predicate_preimage_extreme_literals() {
        let ts_ns = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let ts_ms = DataType::Timestamp(TimeUnit::Millisecond, None);

        // These millisecond values expand beyond i64 range in nanoseconds, so
        // the bucket has no representable endpoint and the preimage is None
        // rather than a clamped or unbounded interval.
        for value in [i64::MAX, i64::MIN] {
            assert_preimage_none(
                &ts_ns,
                &ts_ms,
                Operator::Eq,
                &ScalarValue::TimestampMillisecond(Some(value), None),
            );
        }

        // A widening literal at the i64 boundary produces a checked source
        // endpoint: `i64::MAX` ns floors to 9223372036s with a 854775807ns
        // remainder, so the empty-interval bound `floor + 1` is 9223372037s,
        // still a representable second.
        let source_type = DataType::Timestamp(TimeUnit::Second, None);
        let target_type = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let empty = Some(CastPredicatePreimage::Range(
            Interval::try_new(
                ScalarValue::TimestampSecond(Some(9_223_372_037), None),
                ScalarValue::TimestampSecond(Some(9_223_372_037), None),
            )
            .unwrap(),
        ));
        assert_eq!(
            cast_predicate_preimage(
                &source_type,
                &target_type,
                Operator::Lt,
                &ScalarValue::TimestampNanosecond(Some(i64::MAX), None),
            )
            .unwrap(),
            empty
        );

        // A bucket whose upper endpoint is not representable in the source unit
        // yields `None` rather than a clamped or unbounded interval.
        assert_preimage_none(
            &ts_ns,
            &ts_ms,
            Operator::Eq,
            &ScalarValue::TimestampMillisecond(Some(9_223_372_036_854), None),
        );
    }

    #[test]
    fn test_cast_predicate_preimage_timestamp_identity_anchors() {
        // Same-unit casts anchor both ends of the domain, zero and NULL: the
        // literal passes through untouched, including `i64::MAX`, whose exclusive
        // upper bound would not fit in an `i64`.
        for (unit, scalar) in [
            (TimeUnit::Second, Some(i64::MIN)),
            (TimeUnit::Second, None),
            (TimeUnit::Second, Some(0)),
            (TimeUnit::Second, Some(i64::MAX)),
            (TimeUnit::Millisecond, Some(i64::MIN)),
            (TimeUnit::Millisecond, Some(0)),
            (TimeUnit::Millisecond, Some(i64::MAX)),
            (TimeUnit::Microsecond, Some(i64::MIN)),
            (TimeUnit::Microsecond, Some(0)),
            (TimeUnit::Microsecond, Some(i64::MAX)),
            (TimeUnit::Nanosecond, Some(i64::MIN)),
            (TimeUnit::Nanosecond, Some(0)),
            (TimeUnit::Nanosecond, Some(i64::MAX)),
        ] {
            let data_type = DataType::Timestamp(unit, None);
            let literal = timestamp_scalar(&unit, None, scalar);
            assert_preimage_exact(
                &data_type,
                &data_type,
                Operator::Eq,
                &literal,
                literal.clone(),
            );
            assert_eq!(
                exact_preimage_cast(&data_type, &data_type, &literal),
                Some(literal)
            );
        }
    }

    #[test]
    fn test_cast_predicate_preimage_timestamp_rejects_nested_wrapper() {
        let dict = |inner: ScalarValue| {
            ScalarValue::Dictionary(Box::new(DataType::Int32), Box::new(inner))
        };
        let source_type = DataType::Timestamp(TimeUnit::Millisecond, None);
        let target_type = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let literal = ScalarValue::TimestampNanosecond(Some(123_000_000), None);

        // One dictionary level is unwrapped.
        assert_preimage_exact(
            &source_type,
            &target_type,
            Operator::Eq,
            &dict(literal.clone()),
            ScalarValue::TimestampMillisecond(Some(123), None),
        );
        // A second wrapper is rejected rather than recursively unwrapped.
        assert_preimage_none(
            &source_type,
            &target_type,
            Operator::Eq,
            &dict(dict(literal)),
        );
    }

    #[test]
    fn test_cast_predicate_preimage_timezone_preservation() {
        let source_type =
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+05:30".into()));
        let target_type = DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
        let lit = ScalarValue::TimestampMillisecond(Some(1000), Some("UTC".into()));

        let result =
            cast_predicate_preimage(&source_type, &target_type, Operator::Eq, &lit)
                .unwrap();
        assert_preimage_data_type(result.as_ref(), &source_type);

        match result {
            Some(CastPredicatePreimage::Range(interval)) => {
                let (lower, upper) = interval.into_bounds();
                assert_eq!(
                    lower,
                    ScalarValue::TimestampNanosecond(
                        Some(1_000_000_000),
                        Some("+05:30".into())
                    ),
                    "lower bound should preserve source timezone +05:30"
                );
                assert_eq!(
                    upper,
                    ScalarValue::TimestampNanosecond(
                        Some(1_001_000_000),
                        Some("+05:30".into())
                    ),
                    "upper bound should preserve source timezone +05:30"
                );
            }
            other => panic!("Expected CastPredicatePreimage::Range but got {other:?}"),
        }
    }

    #[test]
    fn test_cast_predicate_preimage_timestamp_dictionary() {
        let key_type = Box::new(DataType::Int32);
        let dict = |key: &DataType, inner: DataType| {
            DataType::Dictionary(Box::new(key.clone()), Box::new(inner))
        };
        let wrap = |value: ScalarValue| {
            ScalarValue::Dictionary(key_type.clone(), Box::new(value))
        };
        let source_tz: Option<Arc<str>> = Some("+05:30".into());

        // The target dictionary key (`Int16`) differs from the source key
        // (`Int32`) so a leaked target key cannot go unnoticed.
        let source_type = dict(
            &DataType::Int32,
            DataType::Timestamp(TimeUnit::Nanosecond, source_tz.clone()),
        );
        let target_type = dict(
            &DataType::Int16,
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        );
        let lit = ScalarValue::TimestampMillisecond(Some(1000), Some("UTC".into()));

        let result =
            cast_predicate_preimage(&source_type, &target_type, Operator::Eq, &lit)
                .unwrap();
        // `ScalarValue` equality ignores timestamp timezones, so the data type
        // (dictionary key + source unit + source timezone) is asserted too.
        assert_preimage_data_type(result.as_ref(), &source_type);
        assert_eq!(
            result,
            Some(CastPredicatePreimage::Range(
                Interval::try_new(
                    wrap(ScalarValue::TimestampNanosecond(
                        Some(1_000_000_000),
                        source_tz.clone(),
                    )),
                    wrap(ScalarValue::TimestampNanosecond(
                        Some(1_001_000_000),
                        source_tz,
                    )),
                )
                .unwrap()
            ))
        );

        // Exact preimages are dictionary-wrapped the same way.
        let ms_source = dict(
            &DataType::Int32,
            DataType::Timestamp(TimeUnit::Millisecond, None),
        );
        let ns_target = dict(
            &DataType::Int16,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        );
        let result = cast_predicate_preimage(
            &ms_source,
            &ns_target,
            Operator::Eq,
            &ScalarValue::TimestampNanosecond(Some(123_000_000), None),
        )
        .unwrap();
        assert_preimage_data_type(result.as_ref(), &ms_source);
        assert_eq!(
            result,
            Some(CastPredicatePreimage::Exact(wrap(
                ScalarValue::TimestampMillisecond(Some(123), None)
            )))
        );

        // A dictionary-wrapped literal is accepted for the same cast.
        let result = cast_predicate_preimage(
            &ms_source,
            &ns_target,
            Operator::Eq,
            &wrap(ScalarValue::TimestampNanosecond(Some(123_000_000), None)),
        )
        .unwrap();
        assert_preimage_data_type(result.as_ref(), &ms_source);
        assert_eq!(
            result,
            Some(CastPredicatePreimage::Exact(wrap(
                ScalarValue::TimestampMillisecond(Some(123), None)
            )))
        );

        // `exact_preimage_cast` (the IN-list path) returns the same source-domain
        // scalar, still dictionary-wrapped with the source key.
        let scalar = exact_preimage_cast(
            &ms_source,
            &ns_target,
            &ScalarValue::TimestampNanosecond(Some(123_000_000), None),
        )
        .unwrap();
        assert_eq!(scalar.data_type(), ms_source);
        assert_eq!(
            scalar,
            wrap(ScalarValue::TimestampMillisecond(Some(123), None))
        );
    }

    // ── Gate safety tests ──────────────────────────────────────────────

    #[test]
    fn test_exact_predicate_gate_safe() {
        // integer widening
        assert_preimage_exact(
            &DataType::Int32,
            &DataType::Int64,
            Operator::Eq,
            &ScalarValue::Int64(Some(10)),
            ScalarValue::Int32(Some(10)),
        );
        // unsigned → wider signed
        assert_preimage_exact(
            &DataType::UInt32,
            &DataType::Int64,
            Operator::Eq,
            &ScalarValue::Int64(Some(100)),
            ScalarValue::UInt32(Some(100)),
        );
        // int → decimal scale-up
        assert_preimage_exact(
            &DataType::Int32,
            &DataType::Decimal128(12, 2),
            Operator::Eq,
            &ScalarValue::Decimal128(Some(10000), 12, 2),
            ScalarValue::Int32(Some(100)),
        );
        // uint → decimal at the same scale
        assert_preimage_exact(
            &DataType::UInt64,
            &DataType::Decimal128(20, 0),
            Operator::Eq,
            &ScalarValue::Decimal128(Some(123), 20, 0),
            ScalarValue::UInt64(Some(123)),
        );
        // decimal scale-up at sub-maximum precision
        assert_preimage_exact(
            &DataType::Decimal128(10, 2),
            &DataType::Decimal128(18, 4),
            Operator::Eq,
            &ScalarValue::Decimal128(Some(1230000), 18, 4),
            ScalarValue::Decimal128(Some(12300), 10, 2),
        );
        // Date32 ↔ Int32
        assert_preimage_exact(
            &DataType::Date32,
            &DataType::Int32,
            Operator::Eq,
            &ScalarValue::Int32(Some(19000)),
            ScalarValue::Date32(Some(19000)),
        );
        assert_preimage_exact(
            &DataType::Int32,
            &DataType::Date32,
            Operator::Eq,
            &ScalarValue::Date32(Some(19000)),
            ScalarValue::Int32(Some(19000)),
        );
        // Date32 → Date64 is injective; literals must still land on a day
        // boundary to be exactly convertible back to the source type.
        assert_preimage_exact(
            &DataType::Date32,
            &DataType::Date64,
            Operator::Eq,
            &ScalarValue::Date64(Some(1_641_600_000_000)),
            ScalarValue::Date32(Some(19000)),
        );
        // FixedSizeBinary(n) → Binary
        assert_preimage_exact(
            &DataType::FixedSizeBinary(4),
            &DataType::Binary,
            Operator::Eq,
            &ScalarValue::Binary(Some(vec![1, 2, 3, 4])),
            ScalarValue::FixedSizeBinary(4, Some(vec![1, 2, 3, 4])),
        );
    }

    #[test]
    fn test_exact_predicate_gate_blocked() {
        // numeric narrowing
        assert_preimage_none(
            &DataType::Int64,
            &DataType::Int32,
            Operator::Eq,
            &ScalarValue::Int32(Some(10)),
        );
        // signed → unsigned (even small values)
        assert_preimage_none(
            &DataType::Int32,
            &DataType::UInt32,
            Operator::Eq,
            &ScalarValue::UInt32(Some(10)),
        );
        // unsigned → narrower signed
        assert_preimage_none(
            &DataType::UInt64,
            &DataType::Int64,
            Operator::Eq,
            &ScalarValue::Int64(Some(10)),
        );
        // decimal → int
        assert_preimage_none(
            &DataType::Decimal128(18, 2),
            &DataType::Int64,
            Operator::Eq,
            &ScalarValue::Int64(Some(123)),
        );
        // decimal scale narrowing
        assert_preimage_none(
            &DataType::Decimal128(18, 2),
            &DataType::Decimal128(18, 1),
            Operator::Eq,
            &ScalarValue::Decimal128(Some(1230), 18, 1),
        );
        // negative decimal scale is not accepted for exact predicate rewrites
        assert_preimage_none(
            &DataType::Decimal128(10, -1),
            &DataType::Decimal128(18, 0),
            Operator::Eq,
            &ScalarValue::Decimal128(Some(120), 18, 0),
        );
        assert_preimage_none(
            &DataType::Int32,
            &DataType::Decimal128(12, -1),
            Operator::Eq,
            &ScalarValue::Decimal128(Some(120), 12, -1),
        );
        // Binary → FixedSizeBinary (not a family-safe source cast)
        assert_preimage_none(
            &DataType::Binary,
            &DataType::FixedSizeBinary(4),
            Operator::Eq,
            &ScalarValue::FixedSizeBinary(4, Some(vec![1, 2, 3, 4])),
        );
    }

    #[test]
    fn test_decimal_scale_up_preimages() {
        let ops = [
            Operator::Eq,
            Operator::NotEq,
            Operator::Lt,
            Operator::LtEq,
            Operator::Gt,
            Operator::GtEq,
            Operator::IsDistinctFrom,
            Operator::IsNotDistinctFrom,
        ];

        // One same-variant representative per decimal variant. Positive,
        // negative and zero literals use all eight comparison operators, and a
        // NULL literal casts to a NULL preimage.
        let cases = [
            (
                DataType::Decimal32(9, 1),
                DataType::Decimal32(9, 2),
                ScalarValue::Decimal32(Some(1200), 9, 2),
                ScalarValue::Decimal32(Some(120), 9, 1),
            ),
            (
                DataType::Decimal64(18, 1),
                DataType::Decimal64(18, 2),
                ScalarValue::Decimal64(Some(1200), 18, 2),
                ScalarValue::Decimal64(Some(120), 18, 1),
            ),
            (
                DataType::Decimal128(38, 1),
                DataType::Decimal128(38, 2),
                ScalarValue::Decimal128(Some(1200), 38, 2),
                ScalarValue::Decimal128(Some(120), 38, 1),
            ),
        ];
        for (source, target, literal, expected) in &cases {
            for op in ops {
                assert_preimage_exact(source, target, op, literal, expected.clone());
            }
            let (negative, negative_expected, zero, zero_expected) = match source {
                DataType::Decimal32(_, _) => (
                    ScalarValue::Decimal32(Some(-1200), 9, 2),
                    ScalarValue::Decimal32(Some(-120), 9, 1),
                    ScalarValue::Decimal32(Some(0), 9, 2),
                    ScalarValue::Decimal32(Some(0), 9, 1),
                ),
                DataType::Decimal64(_, _) => (
                    ScalarValue::Decimal64(Some(-1200), 18, 2),
                    ScalarValue::Decimal64(Some(-120), 18, 1),
                    ScalarValue::Decimal64(Some(0), 18, 2),
                    ScalarValue::Decimal64(Some(0), 18, 1),
                ),
                _ => (
                    ScalarValue::Decimal128(Some(-1200), 38, 2),
                    ScalarValue::Decimal128(Some(-120), 38, 1),
                    ScalarValue::Decimal128(Some(0), 38, 2),
                    ScalarValue::Decimal128(Some(0), 38, 1),
                ),
            };
            for (literal, expected) in
                [(negative, negative_expected), (zero, zero_expected)]
            {
                for op in ops {
                    assert_preimage_exact(source, target, op, &literal, expected.clone());
                }
            }
            let null = ScalarValue::try_from(target).unwrap();
            assert_preimage_exact(
                source,
                target,
                Operator::Eq,
                &null,
                ScalarValue::try_from(source).unwrap(),
            );
        }

        // Additional shapes admitted by the scale-only rule: the motivating
        // sub-maximum 30,2 / 28,0 / 36,4 sources, a precision-37 target,
        // cross-width scale-up, cross-variant widening and signed / unsigned
        // integer sources.
        let extra_cases = [
            (
                DataType::Decimal128(30, 2),
                DataType::Decimal128(38, 15),
                ScalarValue::Decimal128(Some(4 * 10_i128.pow(15)), 38, 15),
                ScalarValue::Decimal128(Some(400), 30, 2),
            ),
            (
                DataType::Decimal128(28, 0),
                DataType::Decimal128(38, 15),
                ScalarValue::Decimal128(Some(4 * 10_i128.pow(15)), 38, 15),
                ScalarValue::Decimal128(Some(4), 28, 0),
            ),
            (
                DataType::Decimal128(36, 4),
                DataType::Decimal128(38, 15),
                ScalarValue::Decimal128(Some(4 * 10_i128.pow(15)), 38, 15),
                ScalarValue::Decimal128(Some(40_000), 36, 4),
            ),
            (
                DataType::Decimal128(37, 0),
                DataType::Decimal128(38, 2),
                ScalarValue::Decimal128(Some(1200), 38, 2),
                ScalarValue::Decimal128(Some(12), 37, 0),
            ),
            (
                DataType::Decimal128(38, 1),
                DataType::Decimal128(37, 2),
                ScalarValue::Decimal128(Some(1200), 37, 2),
                ScalarValue::Decimal128(Some(120), 38, 1),
            ),
            (
                DataType::Decimal64(18, 0),
                DataType::Decimal128(38, 30),
                ScalarValue::Decimal128(Some(10_i128.pow(30)), 38, 30),
                ScalarValue::Decimal64(Some(1), 18, 0),
            ),
            (
                DataType::Decimal128(38, 1),
                DataType::Decimal64(18, 2),
                ScalarValue::Decimal64(Some(1200), 18, 2),
                ScalarValue::Decimal128(Some(120), 38, 1),
            ),
            (
                DataType::Int32,
                DataType::Decimal128(10, 2),
                ScalarValue::Decimal128(Some(10000), 10, 2),
                ScalarValue::Int32(Some(100)),
            ),
            (
                DataType::UInt64,
                DataType::Decimal128(20, 2),
                ScalarValue::Decimal128(Some(12300), 20, 2),
                ScalarValue::UInt64(Some(123)),
            ),
        ];
        for (source, target, literal, expected) in &extra_cases {
            for op in ops {
                assert_preimage_exact(source, target, op, literal, expected.clone());
            }
        }

        // The scale-only rule still rejects reduced or negative target scales,
        // nonaligned literals, decimal → integer and Decimal256.
        assert_preimage_none(
            &DataType::Decimal128(38, 1),
            &DataType::Decimal128(38, 0),
            Operator::Eq,
            &ScalarValue::Decimal128(Some(12), 38, 0),
        );
        assert_preimage_none(
            &DataType::Decimal128(18, 0),
            &DataType::Decimal128(10, -1),
            Operator::Eq,
            &ScalarValue::Decimal128(Some(12), 10, -1),
        );
        // Negative source scales are rejected by the gate instead of reaching
        // the scale arithmetic: both plain and one-level dictionary identity
        // return None.
        let negative_scale = DataType::Decimal128(10, -1);
        assert_preimage_none(
            &negative_scale,
            &negative_scale,
            Operator::Eq,
            &ScalarValue::Decimal128(Some(12), 10, -1),
        );
        let dict_negative_scale =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(negative_scale));
        assert_preimage_none(
            &dict_negative_scale,
            &dict_negative_scale,
            Operator::Eq,
            &ScalarValue::Decimal128(Some(12), 10, -1),
        );
        for literal in [
            ScalarValue::Decimal128(Some(1201), 38, 2),
            ScalarValue::Decimal128(Some(-1201), 38, 2),
        ] {
            assert_preimage_none(
                &DataType::Decimal128(38, 1),
                &DataType::Decimal128(38, 2),
                Operator::Eq,
                &literal,
            );
        }
        assert_preimage_none(
            &DataType::Decimal128(18, 2),
            &DataType::Int64,
            Operator::Eq,
            &ScalarValue::Int64(Some(123)),
        );
        assert_preimage_none(
            &DataType::Decimal256(76, 1),
            &DataType::Decimal256(76, 2),
            Operator::Eq,
            &ScalarValue::Decimal256(Some(1200.into()), 76, 2),
        );

        // The literal still has to convert exactly into the source type and
        // into its range: a negative literal is not a UInt64 preimage, and a
        // cross-width decimal literal with sub-unit digits has no decimal128
        // preimage at the source scale.
        assert_preimage_none(
            &DataType::UInt64,
            &DataType::Decimal128(20, 2),
            Operator::Eq,
            &ScalarValue::Decimal128(Some(-12300), 20, 2),
        );
        assert_preimage_none(
            &DataType::Decimal128(38, 1),
            &DataType::Decimal64(18, 2),
            Operator::Eq,
            &ScalarValue::Decimal64(Some(1201), 18, 2),
        );

        // Arrow's strict cast errors and TRY_CAST returns NULL when this valid
        // Decimal128 source exceeds the target's integer capacity.
        let target_type = DataType::Decimal128(38, 2);
        let source = ScalarValue::Decimal128(Some(i128::MAX / 10), 38, 1);
        let array = source.to_array_of_size(1).unwrap();
        assert!(
            cast_with_options(
                &array,
                &target_type,
                &CastOptions {
                    safe: false,
                    ..Default::default()
                },
            )
            .is_err()
        );
        assert_eq!(
            cast_with_options(&array, &target_type, &CastOptions::default())
                .unwrap()
                .null_count(),
            1,
        );

        // Newly admitted integer source: i32::MAX is a valid Int32 value but
        // overflows decimal(10, 2). The rewritten predicate compares the
        // integer preimage instead, so an out-of-range row is simply FALSE for
        // `=` rather than an error or NULL.
        let int_target = DataType::Decimal128(10, 2);
        let int_literal = ScalarValue::Decimal128(Some(10000), 10, 2);
        let int_preimage =
            exact_preimage_cast(&DataType::Int32, &int_target, &int_literal)
                .expect("integer scale-up preimage");
        assert_eq!(int_preimage, ScalarValue::Int32(Some(100)));
        let int_max = ScalarValue::Int32(Some(i32::MAX));
        let int_max_array = int_max.to_array_of_size(1).unwrap();
        assert!(
            cast_with_options(
                &int_max_array,
                &int_target,
                &CastOptions {
                    safe: false,
                    ..Default::default()
                },
            )
            .is_err()
        );
        assert_eq!(
            cast_with_options(&int_max_array, &int_target, &CastOptions::default())
                .unwrap()
                .null_count(),
            1,
        );
        assert_eq!(int_max.partial_cmp(&int_preimage), Some(Ordering::Greater));

        // Newly admitted sub-maximum decimal source: a valid decimal(30, 2)
        // value outside decimal(38, 15)'s integer capacity errors under a
        // strict cast and is NULL under TRY_CAST, while the rewritten equality
        // predicate is a boolean.
        let wide_source = DataType::Decimal128(30, 2);
        let wide_target = DataType::Decimal128(38, 15);
        let wide_literal = ScalarValue::Decimal128(Some(4 * 10_i128.pow(15)), 38, 15);
        let wide_preimage =
            exact_preimage_cast(&wide_source, &wide_target, &wide_literal)
                .expect("sub-maximum scale-up preimage");
        assert_eq!(wide_preimage, ScalarValue::Decimal128(Some(400), 30, 2));
        let out_of_range = ScalarValue::Decimal128(Some(10_i128.pow(25)), 30, 2);
        let out_of_range_array = out_of_range.to_array_of_size(1).unwrap();
        assert!(
            cast_with_options(
                &out_of_range_array,
                &wide_target,
                &CastOptions {
                    safe: false,
                    ..Default::default()
                },
            )
            .is_err()
        );
        assert_eq!(
            cast_with_options(&out_of_range_array, &wide_target, &CastOptions::default())
                .unwrap()
                .null_count(),
            1,
        );
        assert_eq!(
            out_of_range.partial_cmp(&wide_preimage),
            Some(Ordering::Greater)
        );

        // Newly admitted cross-width decimal source: decimal128 to a narrower
        // decimal64, where a large but valid source value overflows.
        let cross_source = ScalarValue::Decimal128(Some(i128::MAX / 10), 38, 1);
        let cross_target = DataType::Decimal64(18, 2);
        let cross_preimage = exact_preimage_cast(
            &cross_source.data_type(),
            &cross_target,
            &ScalarValue::Decimal64(Some(1200), 18, 2),
        )
        .expect("cross-width scale-up preimage");
        assert_eq!(cross_preimage, ScalarValue::Decimal128(Some(120), 38, 1));
        let cross_array = cross_source.to_array_of_size(1).unwrap();
        assert!(
            cast_with_options(
                &cross_array,
                &cross_target,
                &CastOptions {
                    safe: false,
                    ..Default::default()
                },
            )
            .is_err()
        );
        assert_eq!(
            cast_with_options(&cross_array, &cross_target, &CastOptions::default())
                .unwrap()
                .null_count(),
            1,
        );
        assert_eq!(
            cross_source.partial_cmp(&cross_preimage),
            Some(Ordering::Greater)
        );

        // This larger-scale Decimal128 value overflows the physical i128
        // representation during the cast, not only the target precision.
        let extreme = ScalarValue::Decimal128(Some(10_i128.pow(37)), 38, 2);
        let extreme_array = extreme.to_array_of_size(1).unwrap();
        let extreme_target = DataType::Decimal128(38, 15);
        assert_preimage_exact(
            &DataType::Decimal128(38, 2),
            &extreme_target,
            Operator::Eq,
            &ScalarValue::Decimal128(Some(4 * 10_i128.pow(15)), 38, 15),
            ScalarValue::Decimal128(Some(400), 38, 2),
        );
        assert!(
            cast_with_options(
                &extreme_array,
                &extreme_target,
                &CastOptions {
                    safe: false,
                    ..Default::default()
                },
            )
            .is_err()
        );
        assert_eq!(
            cast_with_options(&extreme_array, &extreme_target, &CastOptions::default())
                .unwrap()
                .null_count(),
            1,
        );
    }

    // ── Integer → string distinctness tests ─────────────────────────────

    #[test]
    fn test_cast_to_string_is_distinct_from_round_trip() {
        // IS DISTINCT FROM with round-trippable string
        assert_preimage_exact(
            &DataType::Int32,
            &DataType::Utf8,
            Operator::IsDistinctFrom,
            &ScalarValue::Utf8(Some("123".to_string())),
            ScalarValue::Int32(Some(123)),
        );
        // IS NOT DISTINCT FROM with round-trippable string
        assert_preimage_exact(
            &DataType::Int32,
            &DataType::Utf8,
            Operator::IsNotDistinctFrom,
            &ScalarValue::Utf8(Some("123".to_string())),
            ScalarValue::Int32(Some(123)),
        );
        // IS NOT DISTINCT FROM with non-round-trippable string (leading zero)
        assert_preimage_none(
            &DataType::Int32,
            &DataType::Utf8,
            Operator::IsNotDistinctFrom,
            &ScalarValue::Utf8(Some("0123".to_string())),
        );
        // IS DISTINCT FROM with non-round-trippable string
        assert_preimage_none(
            &DataType::Int32,
            &DataType::Utf8,
            Operator::IsDistinctFrom,
            &ScalarValue::Utf8(Some("0123".to_string())),
        );
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    fn assert_preimage_exact(
        source_type: &DataType,
        target_type: &DataType,
        op: Operator,
        lit_value: &ScalarValue,
        expected_source: ScalarValue,
    ) {
        let result =
            cast_predicate_preimage(source_type, target_type, op, lit_value).unwrap();
        // The preimage must be a source-domain value: same unit, timezone and
        // dictionary key as the source, never the target.
        assert_preimage_data_type(result.as_ref(), source_type);
        assert_eq!(
            result,
            Some(CastPredicatePreimage::Exact(expected_source)),
            "expected Exact preimage for {source_type:?} → {target_type:?} {op:?} {lit_value:?}"
        );
    }

    fn assert_preimage_none(
        source_type: &DataType,
        target_type: &DataType,
        op: Operator,
        lit_value: &ScalarValue,
    ) {
        let result =
            cast_predicate_preimage(source_type, target_type, op, lit_value).unwrap();
        assert_eq!(
            result, None,
            "expected None preimage for {source_type:?} → {target_type:?} {op:?} {lit_value:?}"
        );
    }

    /// Asserts that every endpoint of a preimage has the expected data type.
    ///
    /// [`ScalarValue`] equality ignores a timestamp's timezone metadata, and a
    /// dictionary wrapper is compared only by key type plus inner value, so
    /// value-based assertions alone cannot catch a leaked target timezone, unit
    /// or dictionary key. This check must accompany them.
    fn assert_preimage_data_type(
        preimage: Option<&CastPredicatePreimage>,
        expected: &DataType,
    ) {
        let (lower, upper) = match preimage {
            Some(CastPredicatePreimage::Exact(value)) => (value, value),
            Some(CastPredicatePreimage::Range(interval)) => {
                (interval.lower(), interval.upper())
            }
            None => return,
        };
        assert_eq!(&lower.data_type(), expected);
        assert_eq!(&upper.data_type(), expected);
    }

    /// Asserts the widening preimage shapes for a `source → target` unit pair
    /// with `target` finer than `source`.
    fn assert_timestamp_widening_ordered(source_unit: TimeUnit, target_unit: TimeUnit) {
        let source_type = DataType::Timestamp(source_unit, None);
        let target_type = DataType::Timestamp(target_unit, None);
        let quotient = i128::from(timestamp_unit_scale(&target_unit))
            / i128::from(timestamp_unit_scale(&source_unit));
        assert!(quotient > 1, "expected a widening unit pair");

        // Aligned literals have a singleton preimage for every operator.
        let aligned = timestamp_scalar(
            &target_unit,
            None,
            Some(i64::try_from(123 * quotient).unwrap()),
        );
        for op in [
            Operator::Eq,
            Operator::NotEq,
            Operator::IsDistinctFrom,
            Operator::IsNotDistinctFrom,
            Operator::GtEq,
            Operator::Gt,
            Operator::Lt,
            Operator::LtEq,
        ] {
            assert_preimage_exact(
                &source_type,
                &target_type,
                op,
                &aligned,
                timestamp_scalar(&source_unit, None, Some(123)),
            );
        }

        // A non-aligned literal has no singleton preimage: ordered comparisons
        // get the empty interval `[floor + 1, floor + 1)` and equality-like ones
        // keep the cast. The bounds are checked real source-unit values, so the
        // extreme literal is covered without any clamping.
        for target_value in [
            quotient + 1,
            1 - quotient,
            i128::from(i64::MAX),
            i128::from(i64::MIN),
        ] {
            let literal = timestamp_scalar(&target_unit, None, Some(target_value as i64));
            let bound = i64::try_from(target_value.div_euclid(quotient) + 1).unwrap();
            let empty = Some(CastPredicatePreimage::Range(
                Interval::try_new(
                    timestamp_scalar(&source_unit, None, Some(bound)),
                    timestamp_scalar(&source_unit, None, Some(bound)),
                )
                .unwrap(),
            ));

            for op in [Operator::GtEq, Operator::Gt, Operator::Lt, Operator::LtEq] {
                let result =
                    cast_predicate_preimage(&source_type, &target_type, op, &literal)
                        .unwrap();
                assert_preimage_data_type(result.as_ref(), &source_type);
                assert_eq!(
                    result, empty,
                    "unexpected preimage for {source_unit:?} → {target_unit:?} {op:?} {target_value}"
                );
            }
            for op in [
                Operator::Eq,
                Operator::NotEq,
                Operator::IsDistinctFrom,
                Operator::IsNotDistinctFrom,
            ] {
                assert_preimage_none(&source_type, &target_type, op, &literal);
            }
        }
    }

    /// Asserts the truncation bucket `[lower, upper)` of a narrowing cast.
    fn assert_timestamp_narrowing_range(
        source_unit: TimeUnit,
        target_unit: TimeUnit,
        lit: i64,
        lower: i64,
        upper: i64,
    ) {
        let source_type = DataType::Timestamp(source_unit, None);
        let target_type = DataType::Timestamp(target_unit, None);
        let expected = Some(CastPredicatePreimage::Range(
            Interval::try_new(
                timestamp_scalar(&source_unit, None, Some(lower)),
                timestamp_scalar(&source_unit, None, Some(upper)),
            )
            .unwrap(),
        ));
        let result = cast_predicate_preimage(
            &source_type,
            &target_type,
            Operator::Eq,
            &timestamp_scalar(&target_unit, None, Some(lit)),
        )
        .unwrap();
        assert_preimage_data_type(result.as_ref(), &source_type);
        assert_eq!(
            result, expected,
            "unexpected narrowing bucket for {source_unit:?} → {target_unit:?} {lit}"
        );
    }
}
