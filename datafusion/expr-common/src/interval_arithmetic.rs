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

//! Interval arithmetic library

use std::borrow::Borrow;
use std::fmt::{self, Display, Formatter};
use std::ops::{AddAssign, SubAssign};

use crate::operator::Operator;
use crate::type_coercion::binary::{BinaryTypeCoercer, comparison_coercion_numeric};

use arrow::compute::{CastOptions, cast_with_options};
use arrow::datatypes::{
    DataType, IntervalDayTime, IntervalMonthDayNano, IntervalUnit,
    MAX_DECIMAL128_FOR_EACH_PRECISION, MAX_DECIMAL256_FOR_EACH_PRECISION,
    MIN_DECIMAL128_FOR_EACH_PRECISION, MIN_DECIMAL256_FOR_EACH_PRECISION, TimeUnit,
};
use datafusion_common::rounding::{alter_fp_rounding_mode, next_down, next_up};
use datafusion_common::{
    DataFusionError, Result, ScalarValue, assert_eq_or_internal_err,
    assert_or_internal_err, internal_err,
};

macro_rules! get_extreme_value {
    ($extreme:ident, $value:expr) => {
        match $value {
            DataType::UInt8 => ScalarValue::UInt8(Some(u8::$extreme)),
            DataType::UInt16 => ScalarValue::UInt16(Some(u16::$extreme)),
            DataType::UInt32 => ScalarValue::UInt32(Some(u32::$extreme)),
            DataType::UInt64 => ScalarValue::UInt64(Some(u64::$extreme)),
            DataType::Int8 => ScalarValue::Int8(Some(i8::$extreme)),
            DataType::Int16 => ScalarValue::Int16(Some(i16::$extreme)),
            DataType::Int32 => ScalarValue::Int32(Some(i32::$extreme)),
            DataType::Int64 => ScalarValue::Int64(Some(i64::$extreme)),
            DataType::Float32 => ScalarValue::Float32(Some(f32::$extreme)),
            DataType::Float64 => ScalarValue::Float64(Some(f64::$extreme)),
            DataType::Duration(TimeUnit::Second) => {
                ScalarValue::DurationSecond(Some(i64::$extreme))
            }
            DataType::Duration(TimeUnit::Millisecond) => {
                ScalarValue::DurationMillisecond(Some(i64::$extreme))
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                ScalarValue::DurationMicrosecond(Some(i64::$extreme))
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                ScalarValue::DurationNanosecond(Some(i64::$extreme))
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                ScalarValue::TimestampSecond(Some(i64::$extreme), None)
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                ScalarValue::TimestampMillisecond(Some(i64::$extreme), None)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                ScalarValue::TimestampMicrosecond(Some(i64::$extreme), None)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                ScalarValue::TimestampNanosecond(Some(i64::$extreme), None)
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                ScalarValue::IntervalYearMonth(Some(i32::$extreme))
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                ScalarValue::IntervalDayTime(Some(IntervalDayTime::$extreme))
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano::$extreme))
            }
            DataType::Decimal128(precision, scale) => ScalarValue::Decimal128(
                Some(
                    paste::paste! {[<$extreme _DECIMAL128_FOR_EACH_PRECISION>]}
                        [*precision as usize],
                ),
                *precision,
                *scale,
            ),
            DataType::Decimal256(precision, scale) => ScalarValue::Decimal256(
                Some(
                    paste::paste! {[<$extreme _DECIMAL256_FOR_EACH_PRECISION>]}
                        [*precision as usize],
                ),
                *precision,
                *scale,
            ),
            _ => unreachable!(),
        }
    };
}

macro_rules! value_transition {
    ($bound:ident, $direction:expr, $value:expr) => {
        match $value {
            UInt8(Some(value)) if value == u8::$bound => UInt8(None),
            UInt16(Some(value)) if value == u16::$bound => UInt16(None),
            UInt32(Some(value)) if value == u32::$bound => UInt32(None),
            UInt64(Some(value)) if value == u64::$bound => UInt64(None),
            Int8(Some(value)) if value == i8::$bound => Int8(None),
            Int16(Some(value)) if value == i16::$bound => Int16(None),
            Int32(Some(value)) if value == i32::$bound => Int32(None),
            Int64(Some(value)) if value == i64::$bound => Int64(None),
            Float32(Some(value)) if value == f32::$bound => Float32(None),
            Float64(Some(value)) if value == f64::$bound => Float64(None),
            DurationSecond(Some(value)) if value == i64::$bound => DurationSecond(None),
            DurationMillisecond(Some(value)) if value == i64::$bound => {
                DurationMillisecond(None)
            }
            DurationMicrosecond(Some(value)) if value == i64::$bound => {
                DurationMicrosecond(None)
            }
            DurationNanosecond(Some(value)) if value == i64::$bound => {
                DurationNanosecond(None)
            }
            TimestampSecond(Some(value), tz) if value == i64::$bound => {
                TimestampSecond(None, tz)
            }
            TimestampMillisecond(Some(value), tz) if value == i64::$bound => {
                TimestampMillisecond(None, tz)
            }
            TimestampMicrosecond(Some(value), tz) if value == i64::$bound => {
                TimestampMicrosecond(None, tz)
            }
            TimestampNanosecond(Some(value), tz) if value == i64::$bound => {
                TimestampNanosecond(None, tz)
            }
            IntervalYearMonth(Some(value)) if value == i32::$bound => {
                IntervalYearMonth(None)
            }
            IntervalDayTime(Some(value))
                if value == arrow::datatypes::IntervalDayTime::$bound =>
            {
                IntervalDayTime(None)
            }
            IntervalMonthDayNano(Some(value))
                if value == arrow::datatypes::IntervalMonthDayNano::$bound =>
            {
                IntervalMonthDayNano(None)
            }
            _ => next_value_helper::<$direction>($value),
        }
    };
}

/// The `Interval` type represents a closed interval used for computing
/// reliable bounds for mathematical expressions.
///
/// Conventions:
///
/// 1. **Closed bounds**: The interval always encompasses its endpoints. We
///    accommodate operations resulting in open intervals by incrementing or
///    decrementing the interval endpoint value to its successor/predecessor.
///
/// 2. **Unbounded endpoints**: If the `lower` or `upper` bounds are indeterminate,
///    they are labeled as *unbounded*. This is represented using a `NULL`.
///
/// 3. **Overflow handling**: If the `lower` or `upper` endpoints exceed their
///    limits after any operation, they either become unbounded or they are fixed
///    to the maximum/minimum value of the datatype, depending on the direction
///    of the overflowing endpoint, opting for the safer choice.
///
/// 4. **Floating-point special cases**:
///    - `INF` values are converted to `NULL`s while constructing an interval to
///      ensure consistency, with other data types.
///    - `NaN` (Not a Number) results are conservatively result in unbounded
///      endpoints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Interval {
    lower: ScalarValue,
    upper: ScalarValue,
}

/// This macro handles the `NaN` and `INF` floating point values.
///
/// - `NaN` values are always converted to unbounded i.e. `NULL` values.
/// - For lower bounds:
///     - A `NEG_INF` value is converted to a `NULL`.
///     - An `INF` value is conservatively converted to the maximum representable
///       number for the floating-point type in question. In this case, converting
///       to `NULL` doesn't make sense as it would be interpreted as a `NEG_INF`.
/// - For upper bounds:
///     - An `INF` value is converted to a `NULL`.
///     - An `NEG_INF` value is conservatively converted to the minimum representable
///       number for the floating-point type in question. In this case, converting
///       to `NULL` doesn't make sense as it would be interpreted as an `INF`.
macro_rules! handle_float_intervals {
    ($scalar_type:ident, $primitive_type:ident, $lower:expr, $upper:expr) => {{
        let lower = match $lower {
            ScalarValue::$scalar_type(Some(l_val))
                if l_val == $primitive_type::NEG_INFINITY || l_val.is_nan() =>
            {
                ScalarValue::$scalar_type(None)
            }
            ScalarValue::$scalar_type(Some(l_val))
                if l_val == $primitive_type::INFINITY =>
            {
                ScalarValue::$scalar_type(Some($primitive_type::MAX))
            }
            value @ ScalarValue::$scalar_type(Some(_)) => value,
            _ => ScalarValue::$scalar_type(None),
        };

        let upper = match $upper {
            ScalarValue::$scalar_type(Some(r_val))
                if r_val == $primitive_type::INFINITY || r_val.is_nan() =>
            {
                ScalarValue::$scalar_type(None)
            }
            ScalarValue::$scalar_type(Some(r_val))
                if r_val == $primitive_type::NEG_INFINITY =>
            {
                ScalarValue::$scalar_type(Some($primitive_type::MIN))
            }
            value @ ScalarValue::$scalar_type(Some(_)) => value,
            _ => ScalarValue::$scalar_type(None),
        };

        Interval { lower, upper }
    }};
}

/// Ordering floating-point numbers according to their binary representations
/// contradicts with their natural ordering. Floating-point number ordering
/// after unsigned integer transmutation looks like:
///
/// ```text
/// 0, 1, 2, 3, ..., MAX, -0, -1, -2, ..., -MAX
/// ```
///
/// This macro applies a one-to-one map that fixes the ordering above.
macro_rules! map_floating_point_order {
    ($value:expr, $ty:ty) => {{
        let num_bits = std::mem::size_of::<$ty>() * 8;
        let sign_bit = 1 << (num_bits - 1);
        if $value & sign_bit == sign_bit {
            // Negative numbers:
            !$value
        } else {
            // Positive numbers:
            $value | sign_bit
        }
    }};
}

impl Interval {
    /// Attempts to create a new `Interval` from the given lower and upper bounds.
    ///
    /// # Notes
    ///
    /// This constructor creates intervals in a "canonical" form where:
    /// - **Boolean intervals**:
    ///   - Unboundedness (`NULL`) for boolean endpoints is converted to `false`
    ///     for lower and `true` for upper bounds.
    /// - **Floating-point intervals**:
    ///   - Floating-point endpoints with `NaN`, `INF`, or `NEG_INF` are converted
    ///     to `NULL`s.
    pub fn try_new(lower: ScalarValue, upper: ScalarValue) -> Result<Self> {
        assert_eq_or_internal_err!(
            lower.data_type(),
            upper.data_type(),
            "Endpoints of an Interval should have the same type"
        );

        let interval = Self::new(lower, upper);

        assert_or_internal_err!(
            interval.lower.is_null()
                || interval.upper.is_null()
                || interval.lower <= interval.upper,
            "Interval's lower bound {} is greater than the upper bound {}",
            interval.lower,
            interval.upper
        );
        Ok(interval)
    }

    /// Only for internal usage. Responsible for standardizing booleans and
    /// floating-point values, as well as fixing NaNs. It doesn't validate
    /// the given bounds for ordering, or verify that they have the same data
    /// type. For its user-facing counterpart and more details, see
    /// [`Interval::try_new`].
    fn new(lower: ScalarValue, upper: ScalarValue) -> Self {
        if let ScalarValue::Boolean(lower_bool) = lower {
            let ScalarValue::Boolean(upper_bool) = upper else {
                // We are sure that upper and lower bounds have the same type.
                unreachable!();
            };
            // Standardize boolean interval endpoints:
            return Self {
                lower: ScalarValue::Boolean(Some(lower_bool.unwrap_or(false))),
                upper: ScalarValue::Boolean(Some(upper_bool.unwrap_or(true))),
            };
        }
        match lower.data_type() {
            // Standardize floating-point endpoints:
            DataType::Float32 => handle_float_intervals!(Float32, f32, lower, upper),
            DataType::Float64 => handle_float_intervals!(Float64, f64, lower, upper),
            // Unsigned null values for lower bounds are set to zero:
            DataType::UInt8 if lower.is_null() => Self {
                lower: ScalarValue::UInt8(Some(0)),
                upper,
            },
            DataType::UInt16 if lower.is_null() => Self {
                lower: ScalarValue::UInt16(Some(0)),
                upper,
            },
            DataType::UInt32 if lower.is_null() => Self {
                lower: ScalarValue::UInt32(Some(0)),
                upper,
            },
            DataType::UInt64 if lower.is_null() => Self {
                lower: ScalarValue::UInt64(Some(0)),
                upper,
            },
            // Other data types do not require standardization:
            _ => Self { lower, upper },
        }
    }

    /// Convenience function to create a new `Interval` from the given (optional)
    /// bounds, for use in tests only. Absence of either endpoint indicates
    /// unboundedness on that side. See [`Interval::try_new`] for more information.
    pub fn make<T>(lower: Option<T>, upper: Option<T>) -> Result<Self>
    where
        ScalarValue: From<Option<T>>,
    {
        Self::try_new(ScalarValue::from(lower), ScalarValue::from(upper))
    }

    /// Creates a singleton zero interval if the datatype supported.
    pub fn make_zero(data_type: &DataType) -> Result<Self> {
        let zero_endpoint = ScalarValue::new_zero(data_type)?;
        Ok(Self::new(zero_endpoint.clone(), zero_endpoint))
    }

    /// Creates an unbounded interval from both sides if the datatype supported.
    pub fn make_unbounded(data_type: &DataType) -> Result<Self> {
        let unbounded_endpoint = ScalarValue::try_from(data_type)?;
        Ok(Self::new(unbounded_endpoint.clone(), unbounded_endpoint))
    }

    /// Creates an interval between -1 to 1.
    pub fn make_symmetric_unit_interval(data_type: &DataType) -> Result<Self> {
        Self::try_new(
            ScalarValue::new_negative_one(data_type)?,
            ScalarValue::new_one(data_type)?,
        )
    }

    /// Create an interval from -π to π.
    pub fn make_symmetric_pi_interval(data_type: &DataType) -> Result<Self> {
        Self::try_new(
            ScalarValue::new_negative_pi_lower(data_type)?,
            ScalarValue::new_pi_upper(data_type)?,
        )
    }

    /// Create an interval from -π/2 to π/2.
    pub fn make_symmetric_half_pi_interval(data_type: &DataType) -> Result<Self> {
        Self::try_new(
            ScalarValue::new_neg_frac_pi_2_lower(data_type)?,
            ScalarValue::new_frac_pi_2_upper(data_type)?,
        )
    }

    /// Create an interval from 0 to infinity.
    pub fn make_non_negative_infinity_interval(data_type: &DataType) -> Result<Self> {
        Self::try_new(
            ScalarValue::new_zero(data_type)?,
            ScalarValue::try_from(data_type)?,
        )
    }

    /// Returns a reference to the lower bound.
    pub fn lower(&self) -> &ScalarValue {
        &self.lower
    }

    /// Returns a reference to the upper bound.
    pub fn upper(&self) -> &ScalarValue {
        &self.upper
    }

    /// Converts this `Interval` into its boundary scalar values. It's useful
    /// when you need to work with the individual bounds directly.
    pub fn into_bounds(self) -> (ScalarValue, ScalarValue) {
        (self.lower, self.upper)
    }

    /// This function returns the data type of this interval.
    pub fn data_type(&self) -> DataType {
        let lower_type = self.lower.data_type();
        let upper_type = self.upper.data_type();

        // There must be no way to create an interval whose endpoints have
        // different types.
        debug_assert!(
            lower_type == upper_type,
            "Interval bounds have different types: {lower_type} != {upper_type}"
        );
        lower_type
    }

    /// Checks if the interval is unbounded (on either side).
    pub fn is_unbounded(&self) -> bool {
        self.lower.is_null() || self.upper.is_null()
    }

    /// Casts this interval to `data_type` using `cast_options`.
    pub fn cast_to(
        &self,
        data_type: &DataType,
        cast_options: &CastOptions,
    ) -> Result<Self> {
        Self::try_new(
            cast_scalar_value(&self.lower, data_type, cast_options)?,
            cast_scalar_value(&self.upper, data_type, cast_options)?,
        )
    }

    /// An interval containing only the 'false' truth value.
    pub const FALSE: Self = Self {
        lower: ScalarValue::Boolean(Some(false)),
        upper: ScalarValue::Boolean(Some(false)),
    };

    #[deprecated(since = "52.0.0", note = "Use `FALSE` instead")]
    pub const CERTAINLY_FALSE: Self = Self::FALSE;

    /// An interval containing both the 'true', and 'false' truth values.
    pub const TRUE_OR_FALSE: Self = Self {
        lower: ScalarValue::Boolean(Some(false)),
        upper: ScalarValue::Boolean(Some(true)),
    };

    #[deprecated(since = "52.0.0", note = "Use `TRUE_OR_FALSE` instead")]
    pub const UNCERTAIN: Self = Self::TRUE_OR_FALSE;

    /// An interval containing only the 'true' truth value.
    pub const TRUE: Self = Self {
        lower: ScalarValue::Boolean(Some(true)),
        upper: ScalarValue::Boolean(Some(true)),
    };

    #[deprecated(since = "52.0.0", note = "Use `TRUE` instead")]
    pub const CERTAINLY_TRUE: Self = Self::TRUE;

    /// Decide if this interval is certainly greater than, possibly greater than,
    /// or can't be greater than `other` by returning `[true, true]`,
    /// `[false, true]` or `[false, false]` respectively.
    ///
    /// NOTE: This function only works with intervals of the same data type.
    ///       Attempting to compare intervals of different data types will lead
    ///       to an error.
    pub fn gt<T: Borrow<Self>>(&self, other: T) -> Result<Self> {
        let rhs = other.borrow();
        let lhs_type = self.data_type();
        let rhs_type = rhs.data_type();
        assert_eq_or_internal_err!(
            lhs_type,
            rhs_type,
            "Only intervals with the same data type are comparable, lhs:{}, rhs:{}",
            self.data_type(),
            rhs.data_type()
        );
        if !(self.upper.is_null() || rhs.lower.is_null()) && self.upper <= rhs.lower {
            // Values in this interval are certainly less than or equal to
            // those in the given interval.
            Ok(Self::FALSE)
        } else if !(self.lower.is_null() || rhs.upper.is_null())
            && (self.lower > rhs.upper)
        {
            // Values in this interval are certainly greater than those in the
            // given interval.
            Ok(Self::TRUE)
        } else {
            // All outcomes are possible.
            Ok(Self::TRUE_OR_FALSE)
        }
    }

    /// Decide if this interval is certainly greater than or equal to, possibly
    /// greater than or equal to, or can't be greater than or equal to `other`
    /// by returning `[true, true]`, `[false, true]` or `[false, false]` respectively.
    ///
    /// NOTE: This function only works with intervals of the same data type.
    ///       Attempting to compare intervals of different data types will lead
    ///       to an error.
    pub fn gt_eq<T: Borrow<Self>>(&self, other: T) -> Result<Self> {
        let rhs = other.borrow();
        let lhs_type = self.data_type();
        let rhs_type = rhs.data_type();
        assert_eq_or_internal_err!(
            lhs_type,
            rhs_type,
            "Only intervals with the same data type are comparable, lhs:{}, rhs:{}",
            self.data_type(),
            rhs.data_type()
        );
        if !(self.lower.is_null() || rhs.upper.is_null()) && self.lower >= rhs.upper {
            // Values in this interval are certainly greater than or equal to
            // those in the given interval.
            Ok(Self::TRUE)
        } else if !(self.upper.is_null() || rhs.lower.is_null())
            && (self.upper < rhs.lower)
        {
            // Values in this interval are certainly less than those in the
            // given interval.
            Ok(Self::FALSE)
        } else {
            // All outcomes are possible.
            Ok(Self::TRUE_OR_FALSE)
        }
    }

    /// Decide if this interval is certainly less than, possibly less than, or
    /// can't be less than `other` by returning `[true, true]`, `[false, true]`
    /// or `[false, false]` respectively.
    ///
    /// NOTE: This function only works with intervals of the same data type.
    ///       Attempting to compare intervals of different data types will lead
    ///       to an error.
    pub fn lt<T: Borrow<Self>>(&self, other: T) -> Result<Self> {
        other.borrow().gt(self)
    }

    /// Decide if this interval is certainly less than or equal to, possibly
    /// less than or equal to, or can't be less than or equal to `other` by
    /// returning `[true, true]`, `[false, true]` or `[false, false]` respectively.
    ///
    /// NOTE: This function only works with intervals of the same data type.
    ///       Attempting to compare intervals of different data types will lead
    ///       to an error.
    pub fn lt_eq<T: Borrow<Self>>(&self, other: T) -> Result<Self> {
        other.borrow().gt_eq(self)
    }

    /// Decide if this interval is certainly equal to, possibly equal to, or
    /// can't be equal to `other` by returning `[true, true]`, `[false, true]`
    /// or `[false, false]` respectively.
    ///
    /// NOTE: This function only works with intervals of the same data type.
    ///       Attempting to compare intervals of different data types will lead
    ///       to an error.
    pub fn equal<T: Borrow<Self>>(&self, other: T) -> Result<Self> {
        let rhs = other.borrow();
        let types_compatible =
            BinaryTypeCoercer::new(&self.data_type(), &Operator::Eq, &rhs.data_type())
                .get_result_type()
                .is_ok();
        assert_or_internal_err!(
            types_compatible,
            "Interval data types must be compatible for equality checks, lhs:{}, rhs:{}",
            self.data_type(),
            rhs.data_type()
        );
        if !self.lower.is_null()
            && (self.lower == self.upper)
            && (rhs.lower == rhs.upper)
            && (self.lower == rhs.lower)
        {
            Ok(Self::TRUE)
        } else if self.intersect(rhs)?.is_none() {
            Ok(Self::FALSE)
        } else {
            Ok(Self::TRUE_OR_FALSE)
        }
    }

    /// Compute the logical conjunction of this (boolean) interval with the
    /// given boolean interval.
    pub fn and<T: Borrow<Self>>(&self, other: T) -> Result<Self> {
        let rhs = other.borrow();
        match (&self.lower, &self.upper, &rhs.lower, &rhs.upper) {
            (
                &ScalarValue::Boolean(Some(self_lower)),
                &ScalarValue::Boolean(Some(self_upper)),
                &ScalarValue::Boolean(Some(other_lower)),
                &ScalarValue::Boolean(Some(other_upper)),
            ) => {
                let lower = self_lower && other_lower;
                let upper = self_upper && other_upper;

                Ok(Self {
                    lower: ScalarValue::Boolean(Some(lower)),
                    upper: ScalarValue::Boolean(Some(upper)),
                })
            }

            // Return TRUE_OR_FALSE when intervals don't have concrete boolean bounds
            _ => Ok(Self::TRUE_OR_FALSE),
        }
    }

    /// Compute the logical disjunction of this boolean interval with the
    /// given boolean interval.
    pub fn or<T: Borrow<Self>>(&self, other: T) -> Result<Self> {
        let rhs = other.borrow();
        match (&self.lower, &self.upper, &rhs.lower, &rhs.upper) {
            (
                &ScalarValue::Boolean(Some(self_lower)),
                &ScalarValue::Boolean(Some(self_upper)),
                &ScalarValue::Boolean(Some(other_lower)),
                &ScalarValue::Boolean(Some(other_upper)),
            ) => {
                let lower = self_lower || other_lower;
                let upper = self_upper || other_upper;

                Ok(Self {
                    lower: ScalarValue::Boolean(Some(lower)),
                    upper: ScalarValue::Boolean(Some(upper)),
                })
            }

            // Return TRUE_OR_FALSE when intervals don't have concrete boolean bounds
            _ => Ok(Self::TRUE_OR_FALSE),
        }
    }

    /// Compute the logical negation of this (boolean) interval.
    pub fn not(&self) -> Result<Self> {
        assert_eq_or_internal_err!(
            self.data_type(),
            DataType::Boolean,
            "Cannot apply logical negation to a non-boolean interval"
        );
        if self == &Self::TRUE {
            Ok(Self::FALSE)
        } else if self == &Self::FALSE {
            Ok(Self::TRUE)
        } else {
            Ok(Self::TRUE_OR_FALSE)
        }
    }

    /// Compute the intersection of this interval with the given interval.
    /// If the intersection is empty, return `None`.
    ///
    /// NOTE: This function only works with intervals of the same data type.
    ///       Attempting to compare intervals of different data types will lead
    ///       to an error.
    pub fn intersect<T: Borrow<Self>>(&self, other: T) -> Result<Option<Self>> {
        let rhs = other.borrow();
        let lhs_type = self.data_type();
        let rhs_type = rhs.data_type();
        assert_eq_or_internal_err!(
            lhs_type,
            rhs_type,
            "Only intervals with the same data type are intersectable, lhs:{}, rhs:{}",
            self.data_type(),
            rhs.data_type()
        );

        // If it is evident that the result is an empty interval, short-circuit
        // and directly return `None`.
        if (!(self.lower.is_null() || rhs.upper.is_null()) && self.lower > rhs.upper)
            || (!(self.upper.is_null() || rhs.lower.is_null()) && self.upper < rhs.lower)
        {
            return Ok(None);
        }

        let lower = max_of_bounds(&self.lower, &rhs.lower);
        let upper = min_of_bounds(&self.upper, &rhs.upper);

        // New lower and upper bounds must always construct a valid interval.
        debug_assert!(
            (lower.is_null() || upper.is_null() || (lower <= upper)),
            "The intersection of two intervals can not be an invalid interval"
        );

        Ok(Some(Self { lower, upper }))
    }

    /// Compute the union of this interval with the given interval.
    ///
    /// NOTE: This function only works with intervals of the same data type.
    ///       Attempting to compare intervals of different data types will lead
    ///       to an error.
    pub fn union<T: Borrow<Self>>(&self, other: T) -> Result<Self> {
        let rhs = other.borrow();
        let lhs_type = self.data_type();
        let rhs_type = rhs.data_type();
        assert_eq_or_internal_err!(
            lhs_type,
            rhs_type,
            "Cannot calculate the union of intervals with different data types, lhs:{}, rhs:{}",
            self.data_type(),
            rhs.data_type()
        );

        let lower = if self.lower.is_null()
            || (!rhs.lower.is_null() && self.lower <= rhs.lower)
        {
            self.lower.clone()
        } else {
            rhs.lower.clone()
        };
        let upper = if self.upper.is_null()
            || (!rhs.upper.is_null() && self.upper >= rhs.upper)
        {
            self.upper.clone()
        } else {
            rhs.upper.clone()
        };

        // New lower and upper bounds must always construct a valid interval.
        debug_assert!(
            (lower.is_null() || upper.is_null() || (lower <= upper)),
            "The union of two intervals can not be an invalid interval"
        );

        Ok(Self { lower, upper })
    }

    /// Decide if this interval contains a [`ScalarValue`] (`other`) by returning `true` or `false`.
    pub fn contains_value<T: Borrow<ScalarValue>>(&self, other: T) -> Result<bool> {
        let rhs = other.borrow();

        let (lhs_lower, lhs_upper, rhs_value) = if self.data_type().eq(&rhs.data_type()) {
            (self.lower.clone(), self.upper.clone(), rhs.clone())
        } else {
            let maybe_common_type =
                comparison_coercion_numeric(&self.data_type(), &rhs.data_type());
            assert_or_internal_err!(
                maybe_common_type.is_some(),
                "Data types must be compatible for containment checks, lhs:{}, rhs:{}",
                self.data_type(),
                rhs.data_type()
            );
            let common_type = maybe_common_type.expect("checked for Some");
            (
                self.lower.cast_to(&common_type)?,
                self.upper.cast_to(&common_type)?,
                rhs.cast_to(&common_type)?,
            )
        };

        // We only check the upper bound for a `None` value because `None`
        // values are less than `Some` values according to Rust.
        Ok(lhs_lower <= rhs_value && (lhs_upper.is_null() || rhs_value <= lhs_upper))
    }

    /// Decide if this interval is a superset of, overlaps with, or
    /// disjoint with `other` by returning `[true, true]`, `[false, true]` or
    /// `[false, false]` respectively.
    ///
    /// NOTE: This function only works with intervals of the same data type.
    ///       Attempting to compare intervals of different data types will lead
    ///       to an error.
    pub fn contains<T: Borrow<Self>>(&self, other: T) -> Result<Self> {
        let rhs = other.borrow();
        let lhs_type = self.data_type();
        let rhs_type = rhs.data_type();
        assert_eq_or_internal_err!(
            lhs_type,
            rhs_type,
            "Interval data types must match for containment checks, lhs:{}, rhs:{}",
            self.data_type(),
            rhs.data_type()
        );

        match self.intersect(rhs)? {
            Some(intersection) => {
                if &intersection == rhs {
                    Ok(Self::TRUE)
                } else {
                    Ok(Self::TRUE_OR_FALSE)
                }
            }
            None => Ok(Self::FALSE),
        }
    }

    /// Decide if this interval is a superset of `other`. If argument `strict`
    /// is `true`, only returns `true` if this interval is a strict superset.
    ///
    /// NOTE: This function only works with intervals of the same data type.
    ///       Attempting to compare intervals of different data types will lead
    ///       to an error.
    pub fn is_superset(&self, other: &Interval, strict: bool) -> Result<bool> {
        Ok(!(strict && self.eq(other)) && (self.contains(other)? == Interval::TRUE))
    }

    /// Add the given interval (`other`) to this interval. Say we have intervals
    /// `[a1, b1]` and `[a2, b2]`, then their sum is `[a1 + a2, b1 + b2]`. Note
    /// that this represents all possible values the sum can take if one can
    /// choose single values arbitrarily from each of the operands.
    pub fn add<T: Borrow<Self>>(&self, other: T) -> Result<Self> {
        let rhs = other.borrow();
        let dt =
            BinaryTypeCoercer::new(&self.data_type(), &Operator::Plus, &rhs.data_type())
                .get_result_type()?;

        Ok(Self::new(
            add_bounds::<false>(&dt, &self.lower, &rhs.lower),
            add_bounds::<true>(&dt, &self.upper, &rhs.upper),
        ))
    }

    /// Subtract the given interval (`other`) from this interval. Say we have
    /// intervals `[a1, b1]` and `[a2, b2]`, then their difference is
    /// `[a1 - b2, b1 - a2]`. Note that this represents all possible values the
    /// difference can take if one can choose single values arbitrarily from
    /// each of the operands.
    pub fn sub<T: Borrow<Interval>>(&self, other: T) -> Result<Self> {
        let rhs = other.borrow();
        let dt =
            BinaryTypeCoercer::new(&self.data_type(), &Operator::Minus, &rhs.data_type())
                .get_result_type()?;

        Ok(Self::new(
            sub_bounds::<false>(&dt, &self.lower, &rhs.upper),
            sub_bounds::<true>(&dt, &self.upper, &rhs.lower),
        ))
    }

    /// Multiply the given interval (`other`) with this interval. Say we have
    /// intervals `[a1, b1]` and `[a2, b2]`, then their product is `[min(a1 * a2,
    /// a1 * b2, b1 * a2, b1 * b2), max(a1 * a2, a1 * b2, b1 * a2, b1 * b2)]`.
    /// Note that this represents all possible values the product can take if
    /// one can choose single values arbitrarily from each of the operands.
    ///
    /// NOTE: This function only works with intervals of the same data type.
    ///       Attempting to compare intervals of different data types will lead
    ///       to an error.
    pub fn mul<T: Borrow<Self>>(&self, other: T) -> Result<Self> {
        let rhs = other.borrow();
        let dt = self.data_type();
        let rhs_type = rhs.data_type();
        assert_eq_or_internal_err!(
            dt.clone(),
            rhs_type.clone(),
            "Intervals must have the same data type for multiplication, lhs:{}, rhs:{}",
            dt.clone(),
            rhs_type.clone()
        );

        let zero = ScalarValue::new_zero(&dt)?;

        let result = match (
            self.contains_value(&zero)?,
            rhs.contains_value(&zero)?,
            dt.is_unsigned_integer(),
        ) {
            (true, true, false) => mul_helper_multi_zero_inclusive(&dt, self, rhs),
            (true, false, false) => {
                mul_helper_single_zero_inclusive(&dt, self, rhs, &zero)
            }
            (false, true, false) => {
                mul_helper_single_zero_inclusive(&dt, rhs, self, &zero)
            }
            _ => mul_helper_zero_exclusive(&dt, self, rhs, &zero),
        };
        Ok(result)
    }

    /// Divide this interval by the given interval (`other`). Say we have intervals
    /// `[a1, b1]` and `[a2, b2]`, then their division is `[a1, b1] * [1 / b2, 1 / a2]`
    /// if `0 ∉ [a2, b2]` and `[NEG_INF, INF]` otherwise. Note that this represents
    /// all possible values the quotient can take if one can choose single values
    /// arbitrarily from each of the operands.
    ///
    /// NOTE: This function only works with intervals of the same data type.
    ///       Attempting to compare intervals of different data types will lead
    ///       to an error.
    ///
    /// **TODO**: Once interval sets are supported, cases where the divisor contains
    ///           zero should result in an interval set, not the universal set.
    pub fn div<T: Borrow<Self>>(&self, other: T) -> Result<Self> {
        let rhs = other.borrow();
        let dt = self.data_type();
        let rhs_type = rhs.data_type();
        assert_eq_or_internal_err!(
            dt.clone(),
            rhs_type.clone(),
            "Intervals must have the same data type for division, lhs:{}, rhs:{}",
            dt.clone(),
            rhs_type.clone()
        );

        let zero = ScalarValue::new_zero(&dt)?;
        // We want 0 to be approachable from both negative and positive sides.
        let zero_point = match &dt {
            DataType::Float32 | DataType::Float64 => Self::new(zero.clone(), zero),
            _ => Self::new(prev_value(zero.clone()), next_value(zero)),
        };

        // Exit early with an unbounded interval if zero is strictly inside the
        // right hand side:
        if rhs.contains(&zero_point)? == Self::TRUE && !dt.is_unsigned_integer() {
            Self::make_unbounded(&dt)
        }
        // At this point, we know that only one endpoint of the right hand side
        // can be zero.
        else if self.contains(&zero_point)? == Self::TRUE && !dt.is_unsigned_integer() {
            Ok(div_helper_lhs_zero_inclusive(&dt, self, rhs, &zero_point))
        } else {
            Ok(div_helper_zero_exclusive(&dt, self, rhs, &zero_point))
        }
    }

    /// Computes the width of this interval; i.e. the difference between its
    /// bounds. For unbounded intervals, this function will return a `NULL`
    /// `ScalarValue` If the underlying data type doesn't support subtraction,
    /// this function will return an error.
    pub fn width(&self) -> Result<ScalarValue> {
        let dt = self.data_type();
        let width_dt =
            BinaryTypeCoercer::new(&dt, &Operator::Minus, &dt).get_result_type()?;
        Ok(sub_bounds::<true>(&width_dt, &self.upper, &self.lower))
    }

    /// Returns the cardinality of this interval, which is the number of all
    /// distinct points inside it. This function returns `None` if:
    /// - The interval is unbounded from either side, or
    /// - Cardinality calculations for the datatype in question is not
    ///   implemented yet, or
    /// - An overflow occurs during the calculation: This case can only arise
    ///   when the calculated cardinality does not fit in an `u64`.
    pub fn cardinality(&self) -> Option<u64> {
        let data_type = self.data_type();
        if data_type.is_integer() {
            self.upper.distance(&self.lower).map(|diff| diff as u64)
        } else if data_type.is_floating() {
            // Negative numbers are sorted in the reverse order. To
            // always have a positive difference after the subtraction,
            // we perform following transformation:
            match (&self.lower, &self.upper) {
                // Exploit IEEE 754 ordering properties to calculate the correct
                // cardinality in all cases (including subnormals).
                (
                    ScalarValue::Float32(Some(lower)),
                    ScalarValue::Float32(Some(upper)),
                ) => {
                    let lower_bits = map_floating_point_order!(lower.to_bits(), u32);
                    let upper_bits = map_floating_point_order!(upper.to_bits(), u32);
                    Some((upper_bits - lower_bits) as u64)
                }
                (
                    ScalarValue::Float64(Some(lower)),
                    ScalarValue::Float64(Some(upper)),
                ) => {
                    let lower_bits = map_floating_point_order!(lower.to_bits(), u64);
                    let upper_bits = map_floating_point_order!(upper.to_bits(), u64);
                    let count = upper_bits - lower_bits;
                    (count != u64::MAX).then_some(count)
                }
                _ => None,
            }
        } else {
            // Cardinality calculations are not implemented for this data type yet:
            None
        }
        .map(|result| result + 1)
    }

    /// Reflects an [`Interval`] around the point zero.
    ///
    /// This method computes the arithmetic negation of the interval, reflecting
    /// it about the origin of the number line. This operation swaps and negates
    /// the lower and upper bounds of the interval.
    pub fn arithmetic_negate(&self) -> Result<Self> {
        Ok(Self {
            lower: self.upper.arithmetic_negate()?,
            upper: self.lower.arithmetic_negate()?,
        })
    }
}

impl Display for Interval {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "[{}, {}]", self.lower, self.upper)
    }
}

impl From<ScalarValue> for Interval {
    fn from(value: ScalarValue) -> Self {
        Self::new(value.clone(), value)
    }
}

impl From<&ScalarValue> for Interval {
    fn from(value: &ScalarValue) -> Self {
        Self::new(value.to_owned(), value.to_owned())
    }
}

/// Applies the given binary operator the `lhs` and `rhs` arguments.
pub fn apply_operator(op: &Operator, lhs: &Interval, rhs: &Interval) -> Result<Interval> {
    match *op {
        Operator::Eq => lhs.equal(rhs),
        Operator::NotEq => lhs.equal(rhs)?.not(),
        Operator::Gt => lhs.gt(rhs),
        Operator::GtEq => lhs.gt_eq(rhs),
        Operator::Lt => lhs.lt(rhs),
        Operator::LtEq => lhs.lt_eq(rhs),
        Operator::And => lhs.and(rhs),
        Operator::Or => lhs.or(rhs),
        Operator::Plus => lhs.add(rhs),
        Operator::Minus => lhs.sub(rhs),
        Operator::Multiply => lhs.mul(rhs),
        Operator::Divide => lhs.div(rhs),
        _ => internal_err!("Interval arithmetic does not support the operator {op}"),
    }
}

/// Helper function used for adding the end-point values of intervals.
///
/// **Caution:** This function contains multiple calls to `unwrap()`, and may
/// return non-standardized interval bounds. Therefore, it should be used
/// with caution. Currently, it is used in contexts where the `DataType`
/// (`dt`) is validated prior to calling this function, and the following
/// interval creation is standardized with `Interval::new`.
fn add_bounds<const UPPER: bool>(
    dt: &DataType,
    lhs: &ScalarValue,
    rhs: &ScalarValue,
) -> ScalarValue {
    if lhs.is_null() || rhs.is_null() {
        return ScalarValue::try_from(dt).unwrap();
    }

    match dt {
        DataType::Float64 | DataType::Float32 => {
            alter_fp_rounding_mode::<UPPER, _>(lhs, rhs, |lhs, rhs| lhs.add_checked(rhs))
        }
        _ => lhs.add_checked(rhs),
    }
    .unwrap_or_else(|_| handle_overflow::<UPPER>(dt, Operator::Plus, lhs, rhs))
}

/// Helper function used for subtracting the end-point values of intervals.
///
/// **Caution:** This function contains multiple calls to `unwrap()`, and may
/// return non-standardized interval bounds. Therefore, it should be used
/// with caution. Currently, it is used in contexts where the `DataType`
/// (`dt`) is validated prior to calling this function, and the following
/// interval creation is standardized with `Interval::new`.
fn sub_bounds<const UPPER: bool>(
    dt: &DataType,
    lhs: &ScalarValue,
    rhs: &ScalarValue,
) -> ScalarValue {
    if lhs.is_null() || rhs.is_null() {
        return ScalarValue::try_from(dt).unwrap();
    }

    match dt {
        DataType::Float64 | DataType::Float32 => {
            alter_fp_rounding_mode::<UPPER, _>(lhs, rhs, |lhs, rhs| lhs.sub_checked(rhs))
        }
        _ => lhs.sub_checked(rhs),
    }
    .unwrap_or_else(|_| handle_overflow::<UPPER>(dt, Operator::Minus, lhs, rhs))
}

/// Helper function used for multiplying the end-point values of intervals.
///
/// **Caution:** This function contains multiple calls to `unwrap()`, and may
/// return non-standardized interval bounds. Therefore, it should be used
/// with caution. Currently, it is used in contexts where the `DataType`
/// (`dt`) is validated prior to calling this function, and the following
/// interval creation is standardized with `Interval::new`.
fn mul_bounds<const UPPER: bool>(
    dt: &DataType,
    lhs: &ScalarValue,
    rhs: &ScalarValue,
) -> ScalarValue {
    if lhs.is_null() || rhs.is_null() {
        return ScalarValue::try_from(dt).unwrap();
    }

    match dt {
        DataType::Float64 | DataType::Float32 => {
            alter_fp_rounding_mode::<UPPER, _>(lhs, rhs, |lhs, rhs| lhs.mul_checked(rhs))
        }
        _ => lhs.mul_checked(rhs),
    }
    .unwrap_or_else(|_| handle_overflow::<UPPER>(dt, Operator::Multiply, lhs, rhs))
}

/// Helper function used for dividing the end-point values of intervals.
///
/// **Caution:** This function contains multiple calls to `unwrap()`, and may
/// return non-standardized interval bounds. Therefore, it should be used
/// with caution. Currently, it is used in contexts where the `DataType`
/// (`dt`) is validated prior to calling this function, and the following
/// interval creation is standardized with `Interval::new`.
fn div_bounds<const UPPER: bool>(
    dt: &DataType,
    lhs: &ScalarValue,
    rhs: &ScalarValue,
) -> ScalarValue {
    let zero = ScalarValue::new_zero(dt).unwrap();

    if (lhs.is_null() || rhs.eq(&zero)) || (dt.is_unsigned_integer() && rhs.is_null()) {
        return ScalarValue::try_from(dt).unwrap();
    } else if rhs.is_null() {
        return zero;
    }

    match dt {
        DataType::Float64 | DataType::Float32 => {
            alter_fp_rounding_mode::<UPPER, _>(lhs, rhs, |lhs, rhs| lhs.div(rhs))
        }
        _ => lhs.div(rhs),
    }
    .unwrap_or_else(|_| handle_overflow::<UPPER>(dt, Operator::Divide, lhs, rhs))
}

/// This function handles cases where an operation results in an overflow. Such
/// results are converted to an *unbounded endpoint* if:
///   - We are calculating an upper bound and we have a positive overflow.
///   - We are calculating a lower bound and we have a negative overflow.
///
/// Otherwise, the function sets the endpoint as:
///   - The minimum representable number with the given datatype (`dt`) if
///     we are calculating an upper bound and we have a negative overflow.
///   - The maximum representable number with the given datatype (`dt`) if
///     we are calculating a lower bound and we have a positive overflow.
///
/// **Caution:** This function contains multiple calls to `unwrap()`, and may
/// return non-standardized interval bounds. Therefore, it should be used
/// with caution. Currently, it is used in contexts where the `DataType`
/// (`dt`) is validated prior to calling this function,  `op` is supported by
/// interval library, and the following interval creation is standardized with
/// `Interval::new`.
fn handle_overflow<const UPPER: bool>(
    dt: &DataType,
    op: Operator,
    lhs: &ScalarValue,
    rhs: &ScalarValue,
) -> ScalarValue {
    let lhs_zero = ScalarValue::new_zero(&lhs.data_type()).unwrap();
    let rhs_zero = ScalarValue::new_zero(&rhs.data_type()).unwrap();
    let positive_sign = match op {
        Operator::Multiply | Operator::Divide => {
            lhs.lt(&lhs_zero) && rhs.lt(&rhs_zero)
                || lhs.gt(&lhs_zero) && rhs.gt(&rhs_zero)
        }
        Operator::Plus => lhs.ge(&lhs_zero),
        Operator::Minus => lhs.ge(rhs),
        _ => {
            unreachable!()
        }
    };

    match (UPPER, positive_sign) {
        (true, true) | (false, false) => ScalarValue::try_from(dt).unwrap(),
        (true, false) => {
            get_extreme_value!(MIN, dt)
        }
        (false, true) => {
            get_extreme_value!(MAX, dt)
        }
    }
}

// This function should remain private since it may corrupt the an interval if
// used without caution.
fn next_value(value: ScalarValue) -> ScalarValue {
    use ScalarValue::*;
    value_transition!(MAX, true, value)
}

// This function should remain private since it may corrupt the an interval if
// used without caution.
fn prev_value(value: ScalarValue) -> ScalarValue {
    use ScalarValue::*;
    value_transition!(MIN, false, value)
}

trait OneTrait: Sized + std::ops::Add + std::ops::Sub {
    fn one() -> Self;
}
macro_rules! impl_OneTrait{
    ($($m:ty),*) => {$( impl OneTrait for $m  { fn one() -> Self { 1 as $m } })*}
}
impl_OneTrait! {u8, u16, u32, u64, i8, i16, i32, i64, i128}

impl OneTrait for IntervalDayTime {
    fn one() -> Self {
        IntervalDayTime {
            days: 0,
            milliseconds: 1,
        }
    }
}

impl OneTrait for IntervalMonthDayNano {
    fn one() -> Self {
        IntervalMonthDayNano {
            months: 0,
            days: 0,
            nanoseconds: 1,
        }
    }
}

/// This function either increments or decrements its argument, depending on
/// the `INC` value (where a `true` value corresponds to the increment).
fn increment_decrement<const INC: bool, T: OneTrait + SubAssign + AddAssign>(
    mut value: T,
) -> T {
    if INC {
        value.add_assign(T::one());
    } else {
        value.sub_assign(T::one());
    }
    value
}

/// This function returns the next/previous value depending on the `INC` value.
/// If `true`, it returns the next value; otherwise it returns the previous value.
fn next_value_helper<const INC: bool>(value: ScalarValue) -> ScalarValue {
    use ScalarValue::*;
    match value {
        // f32/f64::NEG_INF/INF and f32/f64::NaN values should not emerge at this point.
        Float32(Some(val)) => {
            debug_assert!(val.is_finite(), "Non-standardized floating point usage");
            Float32(Some(if INC { next_up(val) } else { next_down(val) }))
        }
        Float64(Some(val)) => {
            debug_assert!(val.is_finite(), "Non-standardized floating point usage");
            Float64(Some(if INC { next_up(val) } else { next_down(val) }))
        }
        Int8(Some(val)) => Int8(Some(increment_decrement::<INC, i8>(val))),
        Int16(Some(val)) => Int16(Some(increment_decrement::<INC, i16>(val))),
        Int32(Some(val)) => Int32(Some(increment_decrement::<INC, i32>(val))),
        Int64(Some(val)) => Int64(Some(increment_decrement::<INC, i64>(val))),
        UInt8(Some(val)) => UInt8(Some(increment_decrement::<INC, u8>(val))),
        UInt16(Some(val)) => UInt16(Some(increment_decrement::<INC, u16>(val))),
        UInt32(Some(val)) => UInt32(Some(increment_decrement::<INC, u32>(val))),
        UInt64(Some(val)) => UInt64(Some(increment_decrement::<INC, u64>(val))),
        DurationSecond(Some(val)) => {
            DurationSecond(Some(increment_decrement::<INC, i64>(val)))
        }
        DurationMillisecond(Some(val)) => {
            DurationMillisecond(Some(increment_decrement::<INC, i64>(val)))
        }
        DurationMicrosecond(Some(val)) => {
            DurationMicrosecond(Some(increment_decrement::<INC, i64>(val)))
        }
        DurationNanosecond(Some(val)) => {
            DurationNanosecond(Some(increment_decrement::<INC, i64>(val)))
        }
        TimestampSecond(Some(val), tz) => {
            TimestampSecond(Some(increment_decrement::<INC, i64>(val)), tz)
        }
        TimestampMillisecond(Some(val), tz) => {
            TimestampMillisecond(Some(increment_decrement::<INC, i64>(val)), tz)
        }
        TimestampMicrosecond(Some(val), tz) => {
            TimestampMicrosecond(Some(increment_decrement::<INC, i64>(val)), tz)
        }
        TimestampNanosecond(Some(val), tz) => {
            TimestampNanosecond(Some(increment_decrement::<INC, i64>(val)), tz)
        }
        IntervalYearMonth(Some(val)) => {
            IntervalYearMonth(Some(increment_decrement::<INC, i32>(val)))
        }
        IntervalDayTime(Some(val)) => IntervalDayTime(Some(increment_decrement::<
            INC,
            arrow::datatypes::IntervalDayTime,
        >(val))),
        IntervalMonthDayNano(Some(val)) => {
            IntervalMonthDayNano(Some(increment_decrement::<
                INC,
                arrow::datatypes::IntervalMonthDayNano,
            >(val)))
        }
        _ => value, // Unbounded values return without change.
    }
}

/// Returns the greater of the given interval bounds. Assumes that a `NULL`
/// value represents `NEG_INF`.
fn max_of_bounds(first: &ScalarValue, second: &ScalarValue) -> ScalarValue {
    if !first.is_null() && (second.is_null() || first >= second) {
        first.clone()
    } else {
        second.clone()
    }
}

/// Returns the lesser of the given interval bounds. Assumes that a `NULL`
/// value represents `INF`.
fn min_of_bounds(first: &ScalarValue, second: &ScalarValue) -> ScalarValue {
    if !first.is_null() && (second.is_null() || first <= second) {
        first.clone()
    } else {
        second.clone()
    }
}

/// This function updates the given intervals by enforcing (i.e. propagating)
/// the inequality `left > right` (or the `left >= right` inequality, if `strict`
/// is `true`).
///
/// Returns a `Result` wrapping an `Option` containing the tuple of resulting
/// intervals. If the comparison is infeasible, returns `None`.
///
/// Example usage:
/// ```
/// use datafusion_common::DataFusionError;
/// use datafusion_expr_common::interval_arithmetic::{satisfy_greater, Interval};
///
/// let left = Interval::make(Some(-1000.0_f32), Some(1000.0_f32))?;
/// let right = Interval::make(Some(500.0_f32), Some(2000.0_f32))?;
/// let strict = false;
/// assert_eq!(
///     satisfy_greater(&left, &right, strict)?,
///     Some((
///         Interval::make(Some(500.0_f32), Some(1000.0_f32))?,
///         Interval::make(Some(500.0_f32), Some(1000.0_f32))?
///     ))
/// );
/// Ok::<(), DataFusionError>(())
/// ```
///
/// NOTE: This function only works with intervals of the same data type.
///       Attempting to compare intervals of different data types will lead
///       to an error.
pub fn satisfy_greater(
    left: &Interval,
    right: &Interval,
    strict: bool,
) -> Result<Option<(Interval, Interval)>> {
    let lhs_type = left.data_type();
    let rhs_type = right.data_type();
    assert_eq_or_internal_err!(
        lhs_type.clone(),
        rhs_type.clone(),
        "Intervals must have the same data type, lhs:{}, rhs:{}",
        lhs_type,
        rhs_type
    );

    if !left.upper.is_null() && left.upper <= right.lower {
        if !strict && left.upper == right.lower {
            // Singleton intervals:
            return Ok(Some((
                Interval::new(left.upper.clone(), left.upper.clone()),
                Interval::new(left.upper.clone(), left.upper.clone()),
            )));
        } else {
            // Left-hand side:  <--======----0------------>
            // Right-hand side: <------------0--======---->
            // No intersection, infeasible to propagate:
            return Ok(None);
        }
    }

    // Only the lower bound of left-hand side and the upper bound of the right-hand
    // side can change after propagating the greater-than operation.
    let new_left_lower = if left.lower.is_null() || left.lower <= right.lower {
        if strict {
            next_value(right.lower.clone())
        } else {
            right.lower.clone()
        }
    } else {
        left.lower.clone()
    };
    // Below code is asymmetric relative to the above if statement, because
    // `None` compares less than `Some` in Rust.
    let new_right_upper = if right.upper.is_null()
        || (!left.upper.is_null() && left.upper <= right.upper)
    {
        if strict {
            prev_value(left.upper.clone())
        } else {
            left.upper.clone()
        }
    } else {
        right.upper.clone()
    };
    // No possibility to create an invalid interval:
    Ok(Some((
        Interval::new(new_left_lower, left.upper.clone()),
        Interval::new(right.lower.clone(), new_right_upper),
    )))
}

/// Multiplies two intervals that both contain zero.
///
/// This function takes in two intervals (`lhs` and `rhs`) as arguments and
/// returns their product (whose data type is known to be `dt`). It is
/// specifically designed to handle intervals that contain zero within their
/// ranges. Returns an error if the multiplication of bounds fails.
///
/// ```text
/// Left-hand side:  <-------=====0=====------->
/// Right-hand side: <-------=====0=====------->
/// ```
///
/// **Caution:** This function contains multiple calls to `unwrap()`. Therefore,
/// it should be used with caution. Currently, it is used in contexts where the
/// `DataType` (`dt`) is validated prior to calling this function.
fn mul_helper_multi_zero_inclusive(
    dt: &DataType,
    lhs: &Interval,
    rhs: &Interval,
) -> Interval {
    if lhs.lower.is_null()
        || lhs.upper.is_null()
        || rhs.lower.is_null()
        || rhs.upper.is_null()
    {
        return Interval::make_unbounded(dt).unwrap();
    }
    // Since unbounded cases are handled above, we can safely
    // use the utility functions here to eliminate code duplication.
    let lower = min_of_bounds(
        &mul_bounds::<false>(dt, &lhs.lower, &rhs.upper),
        &mul_bounds::<false>(dt, &rhs.lower, &lhs.upper),
    );
    let upper = max_of_bounds(
        &mul_bounds::<true>(dt, &lhs.upper, &rhs.upper),
        &mul_bounds::<true>(dt, &lhs.lower, &rhs.lower),
    );
    // There is no possibility to create an invalid interval.
    Interval::new(lower, upper)
}

/// Multiplies two intervals when only left-hand side interval contains zero.
///
/// This function takes in two intervals (`lhs` and `rhs`) as arguments and
/// returns their product (whose data type is known to be `dt`). This function
/// serves as a subroutine that handles the specific case when only `lhs` contains
/// zero within its range. The interval not containing zero, i.e. rhs, can lie
/// on either side of zero. Returns an error if the multiplication of bounds fails.
///
/// ``` text
/// Left-hand side:  <-------=====0=====------->
/// Right-hand side: <--======----0------------>
///
///                    or
///
/// Left-hand side:  <-------=====0=====------->
/// Right-hand side: <------------0--======---->
/// ```
///
/// **Caution:** This function contains multiple calls to `unwrap()`. Therefore,
/// it should be used with caution. Currently, it is used in contexts where the
/// `DataType` (`dt`) is validated prior to calling this function.
fn mul_helper_single_zero_inclusive(
    dt: &DataType,
    lhs: &Interval,
    rhs: &Interval,
    zero: &ScalarValue,
) -> Interval {
    // With the following interval bounds, there is no possibility to create an invalid interval.
    if rhs.upper <= *zero && !rhs.upper.is_null() {
        // <-------=====0=====------->
        // <--======----0------------>
        let lower = mul_bounds::<false>(dt, &lhs.upper, &rhs.lower);
        let upper = mul_bounds::<true>(dt, &lhs.lower, &rhs.lower);
        Interval::new(lower, upper)
    } else {
        // <-------=====0=====------->
        // <------------0--======---->
        let lower = mul_bounds::<false>(dt, &lhs.lower, &rhs.upper);
        let upper = mul_bounds::<true>(dt, &lhs.upper, &rhs.upper);
        Interval::new(lower, upper)
    }
}

/// Multiplies two intervals when neither of them contains zero.
///
/// This function takes in two intervals (`lhs` and `rhs`) as arguments and
/// returns their product (whose data type is known to be `dt`). It is
/// specifically designed to handle intervals that do not contain zero within
/// their ranges. Returns an error if the multiplication of bounds fails.
///
/// ``` text
/// Left-hand side:  <--======----0------------>
/// Right-hand side: <--======----0------------>
///
///                    or
///
/// Left-hand side:  <--======----0------------>
/// Right-hand side: <------------0--======---->
///
///                    or
///
/// Left-hand side:  <------------0--======---->
/// Right-hand side: <--======----0------------>
///
///                    or
///
/// Left-hand side:  <------------0--======---->
/// Right-hand side: <------------0--======---->
/// ```
///
/// **Caution:** This function contains multiple calls to `unwrap()`. Therefore,
/// it should be used with caution. Currently, it is used in contexts where the
/// `DataType` (`dt`) is validated prior to calling this function.
fn mul_helper_zero_exclusive(
    dt: &DataType,
    lhs: &Interval,
    rhs: &Interval,
    zero: &ScalarValue,
) -> Interval {
    let (lower, upper) = match (
        lhs.upper <= *zero && !lhs.upper.is_null(),
        rhs.upper <= *zero && !rhs.upper.is_null(),
    ) {
        // With the following interval bounds, there is no possibility to create an invalid interval.
        (true, true) => (
            // <--======----0------------>
            // <--======----0------------>
            mul_bounds::<false>(dt, &lhs.upper, &rhs.upper),
            mul_bounds::<true>(dt, &lhs.lower, &rhs.lower),
        ),
        (true, false) => (
            // <--======----0------------>
            // <------------0--======---->
            mul_bounds::<false>(dt, &lhs.lower, &rhs.upper),
            mul_bounds::<true>(dt, &lhs.upper, &rhs.lower),
        ),
        (false, true) => (
            // <------------0--======---->
            // <--======----0------------>
            mul_bounds::<false>(dt, &rhs.lower, &lhs.upper),
            mul_bounds::<true>(dt, &rhs.upper, &lhs.lower),
        ),
        (false, false) => (
            // <------------0--======---->
            // <------------0--======---->
            mul_bounds::<false>(dt, &lhs.lower, &rhs.lower),
            mul_bounds::<true>(dt, &lhs.upper, &rhs.upper),
        ),
    };
    Interval::new(lower, upper)
}

/// Divides the left-hand side interval by the right-hand side interval when
/// the former contains zero.
///
/// This function takes in two intervals (`lhs` and `rhs`) as arguments and
/// returns their quotient (whose data type is known to be `dt`). This function
/// serves as a subroutine that handles the specific case when only `lhs` contains
/// zero within its range. Returns an error if the division of bounds fails.
///
/// ``` text
/// Left-hand side:  <-------=====0=====------->
/// Right-hand side: <--======----0------------>
///
///                    or
///
/// Left-hand side:  <-------=====0=====------->
/// Right-hand side: <------------0--======---->
/// ```
///
/// **Caution:** This function contains multiple calls to `unwrap()`. Therefore,
/// it should be used with caution. Currently, it is used in contexts where the
/// `DataType` (`dt`) is validated prior to calling this function.
fn div_helper_lhs_zero_inclusive(
    dt: &DataType,
    lhs: &Interval,
    rhs: &Interval,
    zero_point: &Interval,
) -> Interval {
    // With the following interval bounds, there is no possibility to create an invalid interval.
    if rhs.upper <= zero_point.lower && !rhs.upper.is_null() {
        // <-------=====0=====------->
        // <--======----0------------>
        let lower = div_bounds::<false>(dt, &lhs.upper, &rhs.upper);
        let upper = div_bounds::<true>(dt, &lhs.lower, &rhs.upper);
        Interval::new(lower, upper)
    } else {
        // <-------=====0=====------->
        // <------------0--======---->
        let lower = div_bounds::<false>(dt, &lhs.lower, &rhs.lower);
        let upper = div_bounds::<true>(dt, &lhs.upper, &rhs.lower);
        Interval::new(lower, upper)
    }
}

/// Divides the left-hand side interval by the right-hand side interval when
/// neither interval contains zero.
///
/// This function takes in two intervals (`lhs` and `rhs`) as arguments and
/// returns their quotient (whose data type is known to be `dt`). It is
/// specifically designed to handle intervals that do not contain zero within
/// their ranges. Returns an error if the division of bounds fails.
///
/// ``` text
/// Left-hand side:  <--======----0------------>
/// Right-hand side: <--======----0------------>
///
///                    or
///
/// Left-hand side:  <--======----0------------>
/// Right-hand side: <------------0--======---->
///
///                    or
///
/// Left-hand side:  <------------0--======---->
/// Right-hand side: <--======----0------------>
///
///                    or
///
/// Left-hand side:  <------------0--======---->
/// Right-hand side: <------------0--======---->
/// ```
///
/// **Caution:** This function contains multiple calls to `unwrap()`. Therefore,
/// it should be used with caution. Currently, it is used in contexts where the
/// `DataType` (`dt`) is validated prior to calling this function.
fn div_helper_zero_exclusive(
    dt: &DataType,
    lhs: &Interval,
    rhs: &Interval,
    zero_point: &Interval,
) -> Interval {
    let (lower, upper) = match (
        lhs.upper <= zero_point.lower && !lhs.upper.is_null(),
        rhs.upper <= zero_point.lower && !rhs.upper.is_null(),
    ) {
        // With the following interval bounds, there is no possibility to create an invalid interval.
        (true, true) => (
            // <--======----0------------>
            // <--======----0------------>
            div_bounds::<false>(dt, &lhs.upper, &rhs.lower),
            div_bounds::<true>(dt, &lhs.lower, &rhs.upper),
        ),
        (true, false) => (
            // <--======----0------------>
            // <------------0--======---->
            div_bounds::<false>(dt, &lhs.lower, &rhs.lower),
            div_bounds::<true>(dt, &lhs.upper, &rhs.upper),
        ),
        (false, true) => (
            // <------------0--======---->
            // <--======----0------------>
            div_bounds::<false>(dt, &lhs.upper, &rhs.upper),
            div_bounds::<true>(dt, &lhs.lower, &rhs.lower),
        ),
        (false, false) => (
            // <------------0--======---->
            // <------------0--======---->
            div_bounds::<false>(dt, &lhs.lower, &rhs.upper),
            div_bounds::<true>(dt, &lhs.upper, &rhs.lower),
        ),
    };
    Interval::new(lower, upper)
}

/// This function computes the selectivity of an operation by computing the
/// cardinality ratio of the given input/output intervals. If this can not be
/// calculated for some reason, it returns `1.0` meaning fully selective (no
/// filtering).
pub fn cardinality_ratio(initial_interval: &Interval, final_interval: &Interval) -> f64 {
    match (final_interval.cardinality(), initial_interval.cardinality()) {
        (Some(final_interval), Some(initial_interval)) => {
            (final_interval as f64) / (initial_interval as f64)
        }
        _ => 1.0,
    }
}

/// Cast scalar value to the given data type using an arrow kernel.
fn cast_scalar_value(
    value: &ScalarValue,
    data_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ScalarValue> {
    let cast_array = cast_with_options(&value.to_array()?, data_type, cast_options)?;
    ScalarValue::try_from_array(&cast_array, 0)
}

/// An [Interval] that also tracks null status using a boolean interval.
///
/// This represents values that may be in a particular range or be null.
///
/// # Examples
///
/// ```
/// use arrow::datatypes::DataType;
/// use datafusion_common::ScalarValue;
/// use datafusion_expr_common::interval_arithmetic::Interval;
/// use datafusion_expr_common::interval_arithmetic::NullableInterval;
///
/// // [1, 2) U {NULL}
/// let maybe_null = NullableInterval::MaybeNull {
///     values: Interval::try_new(
///         ScalarValue::Int32(Some(1)),
///         ScalarValue::Int32(Some(2)),
///     )
///     .unwrap(),
/// };
///
/// // (0, ∞)
/// let not_null = NullableInterval::NotNull {
///     values: Interval::try_new(ScalarValue::Int32(Some(0)), ScalarValue::Int32(None))
///         .unwrap(),
/// };
///
/// // {NULL}
/// let null_interval = NullableInterval::Null {
///     datatype: DataType::Int32,
/// };
///
/// // {4}
/// let single_value = NullableInterval::from(ScalarValue::Int32(Some(4)));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NullableInterval {
    /// The value is always null. This is typed so it can be used in physical
    /// expressions, which don't do type coercion.
    Null { datatype: DataType },
    /// The value may or may not be null. If it is non-null, its is within the
    /// specified range.
    MaybeNull { values: Interval },
    /// The value is definitely not null, and is within the specified range.
    NotNull { values: Interval },
}

impl Display for NullableInterval {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null { .. } => write!(f, "NullableInterval: {{NULL}}"),
            Self::MaybeNull { values } => {
                write!(f, "NullableInterval: {values} U {{NULL}}")
            }
            Self::NotNull { values } => write!(f, "NullableInterval: {values}"),
        }
    }
}

impl From<ScalarValue> for NullableInterval {
    /// Create an interval that represents a single value.
    fn from(value: ScalarValue) -> Self {
        if value.is_null() {
            Self::Null {
                datatype: value.data_type(),
            }
        } else {
            Self::NotNull {
                values: Interval {
                    lower: value.clone(),
                    upper: value,
                },
            }
        }
    }
}

impl NullableInterval {
    /// An interval containing only the 'false' truth value.
    /// This interval is semantically equivalent to [Interval::FALSE].
    pub const FALSE: Self = NullableInterval::NotNull {
        values: Interval::FALSE,
    };

    /// An interval containing only the 'true' truth value.
    /// This interval is semantically equivalent to [Interval::TRUE].
    pub const TRUE: Self = NullableInterval::NotNull {
        values: Interval::TRUE,
    };

    /// An interval containing only the 'unknown' truth value.
    pub const UNKNOWN: Self = NullableInterval::Null {
        datatype: DataType::Boolean,
    };

    /// An interval containing both the 'true', and 'false' truth values.
    /// This interval is semantically equivalent to [Interval::TRUE_OR_FALSE].
    pub const TRUE_OR_FALSE: Self = NullableInterval::NotNull {
        values: Interval::TRUE_OR_FALSE,
    };

    /// An interval containing both the 'true' and 'unknown' truth values.
    pub const TRUE_OR_UNKNOWN: Self = NullableInterval::MaybeNull {
        values: Interval::TRUE,
    };

    /// An interval containing both the 'false' and 'unknown' truth values.
    pub const FALSE_OR_UNKNOWN: Self = NullableInterval::MaybeNull {
        values: Interval::FALSE,
    };

    /// An interval that contains all possible truth values: 'true', 'false' and 'unknown'.
    pub const ANY_TRUTH_VALUE: Self = NullableInterval::MaybeNull {
        values: Interval::TRUE_OR_FALSE,
    };

    /// Get the values interval, or None if this interval is definitely null.
    pub fn values(&self) -> Option<&Interval> {
        match self {
            Self::Null { .. } => None,
            Self::MaybeNull { values } | Self::NotNull { values } => Some(values),
        }
    }

    /// Get the data type
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Null { datatype } => datatype.clone(),
            Self::MaybeNull { values } | Self::NotNull { values } => values.data_type(),
        }
    }

    /// Return true if the value is definitely true (and not null).
    pub fn is_certainly_true(&self) -> bool {
        self == &Self::TRUE
    }

    /// Returns the set of possible values after applying the `is true` test on all
    /// values in this set.
    /// The resulting set can only contain 'TRUE' and/or 'FALSE', never 'UNKNOWN'.
    pub fn is_true(&self) -> Result<Self> {
        let (t, f, u) = self.is_true_false_unknown()?;

        match (t, f, u) {
            (true, false, false) => Ok(Self::TRUE),
            (true, _, _) => Ok(Self::TRUE_OR_FALSE),
            (false, _, _) => Ok(Self::FALSE),
        }
    }

    /// Return true if the value is definitely false (and not null).
    pub fn is_certainly_false(&self) -> bool {
        self == &Self::FALSE
    }

    /// Returns the set of possible values after applying the `is false` test on all
    /// values in this set.
    /// The resulting set can only contain 'TRUE' and/or 'FALSE', never 'UNKNOWN'.
    pub fn is_false(&self) -> Result<Self> {
        let (t, f, u) = self.is_true_false_unknown()?;

        match (t, f, u) {
            (false, true, false) => Ok(Self::TRUE),
            (_, true, _) => Ok(Self::TRUE_OR_FALSE),
            (_, false, _) => Ok(Self::FALSE),
        }
    }

    /// Return true if the value is definitely null (and not true or false).
    pub fn is_certainly_unknown(&self) -> bool {
        self == &Self::UNKNOWN
    }

    /// Returns the set of possible values after applying the `is unknown` test on all
    /// values in this set.
    /// The resulting set can only contain 'TRUE' and/or 'FALSE', never 'UNKNOWN'.
    pub fn is_unknown(&self) -> Result<Self> {
        let (t, f, u) = self.is_true_false_unknown()?;

        match (t, f, u) {
            (false, false, true) => Ok(Self::TRUE),
            (_, _, true) => Ok(Self::TRUE_OR_FALSE),
            (_, _, false) => Ok(Self::FALSE),
        }
    }

    /// Returns a tuple of booleans indicating if this interval contains the
    /// true, false, and unknown truth values respectively.
    fn is_true_false_unknown(&self) -> Result<(bool, bool, bool), DataFusionError> {
        Ok(match self {
            NullableInterval::Null { .. } => (false, false, true),
            NullableInterval::MaybeNull { values } => (
                values.contains_value(ScalarValue::Boolean(Some(true)))?,
                values.contains_value(ScalarValue::Boolean(Some(false)))?,
                true,
            ),
            NullableInterval::NotNull { values } => (
                values.contains_value(ScalarValue::Boolean(Some(true)))?,
                values.contains_value(ScalarValue::Boolean(Some(false)))?,
                false,
            ),
        })
    }

    /// Returns an interval representing the set of possible values after applying
    /// SQL three-valued logical NOT on possible value in this interval.
    ///
    /// This method uses the following truth table.
    ///
    /// ```text
    ///  A  | ¬A
    /// ----|----
    ///  F  |  T
    ///  U  |  U
    ///  T  |  F
    /// ```
    pub fn not(&self) -> Result<Self> {
        match self {
            Self::Null { datatype } => {
                assert_eq_or_internal_err!(
                    datatype,
                    &DataType::Boolean,
                    "Cannot apply logical negation to a non-boolean interval"
                );
                Ok(Self::UNKNOWN)
            }
            Self::MaybeNull { values } => Ok(Self::MaybeNull {
                values: values.not()?,
            }),
            Self::NotNull { values } => Ok(Self::NotNull {
                values: values.not()?,
            }),
        }
    }

    /// Returns an interval representing the set of possible values after applying SQL
    /// three-valued logical AND on each combination of possible values from `self` and `other`.
    ///
    /// This method uses the following truth table.
    ///
    /// ```text
    ///       │   B
    /// A ∧ B ├──────
    ///       │ F U T
    /// ──┬───┼──────
    ///   │ F │ F F F
    /// A │ U │ F U U
    ///   │ T │ F U T
    /// ```
    pub fn and<T: Borrow<Self>>(&self, rhs: T) -> Result<Self> {
        if self == &Self::FALSE || rhs.borrow() == &Self::FALSE {
            return Ok(Self::FALSE);
        }

        match (self.values(), rhs.borrow().values()) {
            (Some(l), Some(r)) => {
                let values = l.and(r)?;
                match (self, rhs.borrow()) {
                    (Self::NotNull { .. }, Self::NotNull { .. }) => {
                        Ok(Self::NotNull { values })
                    }
                    _ => Ok(Self::MaybeNull { values }),
                }
            }
            (Some(v), None) | (None, Some(v)) => {
                if v.contains_value(ScalarValue::Boolean(Some(false)))? {
                    Ok(Self::FALSE_OR_UNKNOWN)
                } else {
                    Ok(Self::UNKNOWN)
                }
            }
            _ => Ok(Self::UNKNOWN),
        }
    }

    /// Returns an interval representing the set of possible values after applying SQL three-valued
    /// logical OR on each combination of possible values from `self` and `other`.
    ///
    /// This method uses the following truth table.
    ///
    /// ```text
    ///       │   B
    /// A ∨ B ├──────
    ///       │ F U T
    /// ──┬───┼──────
    ///   │ F │ F U T
    /// A │ U │ U U T
    ///   │ T │ T T T
    /// ```
    pub fn or<T: Borrow<Self>>(&self, rhs: T) -> Result<Self> {
        if self == &Self::TRUE || rhs.borrow() == &Self::TRUE {
            return Ok(Self::TRUE);
        }

        match (self.values(), rhs.borrow().values()) {
            (Some(l), Some(r)) => {
                let values = l.or(r)?;
                match (self, rhs.borrow()) {
                    (Self::NotNull { .. }, Self::NotNull { .. }) => {
                        Ok(Self::NotNull { values })
                    }
                    _ => Ok(Self::MaybeNull { values }),
                }
            }
            (Some(v), None) | (None, Some(v)) => {
                if v.contains_value(ScalarValue::Boolean(Some(true)))? {
                    Ok(Self::TRUE_OR_UNKNOWN)
                } else {
                    Ok(Self::UNKNOWN)
                }
            }
            _ => Ok(Self::UNKNOWN),
        }
    }

    /// Apply the given operator to this interval and the given interval.
    ///
    /// # Examples
    ///
    /// ```
    /// use datafusion_common::ScalarValue;
    /// use datafusion_expr_common::interval_arithmetic::Interval;
    /// use datafusion_expr_common::interval_arithmetic::NullableInterval;
    /// use datafusion_expr_common::operator::Operator;
    ///
    /// // 4 > 3 -> true
    /// let lhs = NullableInterval::from(ScalarValue::Int32(Some(4)));
    /// let rhs = NullableInterval::from(ScalarValue::Int32(Some(3)));
    /// let result = lhs.apply_operator(&Operator::Gt, &rhs).unwrap();
    /// assert_eq!(
    ///     result,
    ///     NullableInterval::from(ScalarValue::Boolean(Some(true)))
    /// );
    ///
    /// // [1, 3) > NULL -> NULL
    /// let lhs = NullableInterval::NotNull {
    ///     values: Interval::try_new(
    ///         ScalarValue::Int32(Some(1)),
    ///         ScalarValue::Int32(Some(3)),
    ///     )
    ///     .unwrap(),
    /// };
    /// let rhs = NullableInterval::from(ScalarValue::Int32(None));
    /// let result = lhs.apply_operator(&Operator::Gt, &rhs).unwrap();
    /// assert_eq!(result.single_value(), Some(ScalarValue::Boolean(None)));
    ///
    /// // [1, 3] > [2, 4] -> [false, true]
    /// let lhs = NullableInterval::NotNull {
    ///     values: Interval::try_new(
    ///         ScalarValue::Int32(Some(1)),
    ///         ScalarValue::Int32(Some(3)),
    ///     )
    ///     .unwrap(),
    /// };
    /// let rhs = NullableInterval::NotNull {
    ///     values: Interval::try_new(
    ///         ScalarValue::Int32(Some(2)),
    ///         ScalarValue::Int32(Some(4)),
    ///     )
    ///     .unwrap(),
    /// };
    /// let result = lhs.apply_operator(&Operator::Gt, &rhs).unwrap();
    /// // Both inputs are valid (non-null), so result must be non-null
    /// assert_eq!(
    ///     result,
    ///     NullableInterval::NotNull {
    ///         // Uncertain whether inequality is true or false
    ///         values: Interval::TRUE_OR_FALSE,
    ///     }
    /// );
    /// ```
    pub fn apply_operator(&self, op: &Operator, rhs: &Self) -> Result<Self> {
        match op {
            Operator::IsDistinctFrom => {
                let values = match (self, rhs) {
                    // NULL is distinct from NULL -> False
                    (Self::Null { .. }, Self::Null { .. }) => Interval::FALSE,
                    // x is distinct from y -> x != y,
                    // if at least one of them is never null.
                    (Self::NotNull { .. }, _) | (_, Self::NotNull { .. }) => {
                        let lhs_values = self.values();
                        let rhs_values = rhs.values();
                        match (lhs_values, rhs_values) {
                            (Some(lhs_values), Some(rhs_values)) => {
                                lhs_values.equal(rhs_values)?.not()?
                            }
                            (Some(_), None) | (None, Some(_)) => Interval::TRUE,
                            (None, None) => unreachable!("Null case handled above"),
                        }
                    }
                    _ => Interval::TRUE_OR_FALSE,
                };
                // IsDistinctFrom never returns null.
                Ok(Self::NotNull { values })
            }
            Operator::IsNotDistinctFrom => self
                .apply_operator(&Operator::IsDistinctFrom, rhs)
                .map(|i| i.not())?,
            Operator::And => self.and(rhs),
            Operator::Or => self.or(rhs),
            _ => {
                if let (Some(left_values), Some(right_values)) =
                    (self.values(), rhs.values())
                {
                    let values = apply_operator(op, left_values, right_values)?;
                    match (self, rhs) {
                        (Self::NotNull { .. }, Self::NotNull { .. }) => {
                            Ok(Self::NotNull { values })
                        }
                        _ => Ok(Self::MaybeNull { values }),
                    }
                } else if op.supports_propagation() {
                    Ok(Self::Null {
                        datatype: DataType::Boolean,
                    })
                } else {
                    Ok(Self::Null {
                        datatype: self.data_type(),
                    })
                }
            }
        }
    }

    /// Decide if this interval is a superset of, overlaps with, or
    /// disjoint with `other` by returning `[true, true]`, `[false, true]` or
    /// `[false, false]` respectively.
    ///
    /// NOTE: This function only works with intervals of the same data type.
    ///       Attempting to compare intervals of different data types will lead
    ///       to an error.
    pub fn contains<T: Borrow<Self>>(&self, other: T) -> Result<Self> {
        let rhs = other.borrow();
        if let (Some(left_values), Some(right_values)) = (self.values(), rhs.values()) {
            left_values
                .contains(right_values)
                .map(|values| match (self, rhs) {
                    (Self::NotNull { .. }, Self::NotNull { .. }) => {
                        Self::NotNull { values }
                    }
                    _ => Self::MaybeNull { values },
                })
        } else {
            Ok(Self::Null {
                datatype: DataType::Boolean,
            })
        }
    }

    /// Determines if this interval contains a [`ScalarValue`] or not.
    pub fn contains_value<T: Borrow<ScalarValue>>(&self, value: T) -> Result<bool> {
        match value.borrow() {
            ScalarValue::Null => match self {
                NullableInterval::Null { .. } | NullableInterval::MaybeNull { .. } => {
                    Ok(true)
                }
                NullableInterval::NotNull { .. } => Ok(false),
            },
            s if s.is_null() => match self {
                NullableInterval::Null { datatype } => Ok(datatype.eq(&s.data_type())),
                NullableInterval::MaybeNull { values } => {
                    Ok(values.data_type().eq(&s.data_type()))
                }
                NullableInterval::NotNull { .. } => Ok(false),
            },
            s => match self {
                NullableInterval::Null { .. } => Ok(false),
                NullableInterval::MaybeNull { values }
                | NullableInterval::NotNull { values } => values.contains_value(s),
            },
        }
    }

    /// If the interval has collapsed to a single value, return that value.
    /// Otherwise, returns `None`.
    ///
    /// # Examples
    ///
    /// ```
    /// use datafusion_common::ScalarValue;
    /// use datafusion_expr_common::interval_arithmetic::Interval;
    /// use datafusion_expr_common::interval_arithmetic::NullableInterval;
    ///
    /// let interval = NullableInterval::from(ScalarValue::Int32(Some(4)));
    /// assert_eq!(interval.single_value(), Some(ScalarValue::Int32(Some(4))));
    ///
    /// let interval = NullableInterval::from(ScalarValue::Int32(None));
    /// assert_eq!(interval.single_value(), Some(ScalarValue::Int32(None)));
    ///
    /// let interval = NullableInterval::MaybeNull {
    ///     values: Interval::try_new(
    ///         ScalarValue::Int32(Some(1)),
    ///         ScalarValue::Int32(Some(4)),
    ///     )
    ///     .unwrap(),
    /// };
    /// assert_eq!(interval.single_value(), None);
    /// ```
    pub fn single_value(&self) -> Option<ScalarValue> {
        match self {
            Self::Null { datatype } => {
                Some(ScalarValue::try_from(datatype).unwrap_or(ScalarValue::Null))
            }
            Self::MaybeNull { values } | Self::NotNull { values }
                if values.lower == values.upper && !values.lower.is_null() =>
            {
                Some(values.lower.clone())
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        interval_arithmetic::{
            Interval, handle_overflow, next_value, prev_value, satisfy_greater,
        },
        operator::Operator,
    };

    use crate::interval_arithmetic::NullableInterval;
    use arrow::datatypes::DataType;
    use datafusion_common::rounding::{next_down, next_up};
    use datafusion_common::{Result, ScalarValue};

    #[test]
    fn test_next_prev_value() -> Result<()> {
        let zeros = vec![
            ScalarValue::new_zero(&DataType::UInt8)?,
            ScalarValue::new_zero(&DataType::UInt16)?,
            ScalarValue::new_zero(&DataType::UInt32)?,
            ScalarValue::new_zero(&DataType::UInt64)?,
            ScalarValue::new_zero(&DataType::Int8)?,
            ScalarValue::new_zero(&DataType::Int16)?,
            ScalarValue::new_zero(&DataType::Int32)?,
            ScalarValue::new_zero(&DataType::Int64)?,
        ];
        let ones = vec![
            ScalarValue::new_one(&DataType::UInt8)?,
            ScalarValue::new_one(&DataType::UInt16)?,
            ScalarValue::new_one(&DataType::UInt32)?,
            ScalarValue::new_one(&DataType::UInt64)?,
            ScalarValue::new_one(&DataType::Int8)?,
            ScalarValue::new_one(&DataType::Int16)?,
            ScalarValue::new_one(&DataType::Int32)?,
            ScalarValue::new_one(&DataType::Int64)?,
        ];
        zeros.into_iter().zip(ones).for_each(|(z, o)| {
            assert_eq!(next_value(z.clone()), o);
            assert_eq!(prev_value(o), z);
        });

        let values = vec![
            ScalarValue::new_zero(&DataType::Float32)?,
            ScalarValue::new_zero(&DataType::Float64)?,
        ];
        let eps = vec![
            ScalarValue::Float32(Some(1e-6)),
            ScalarValue::Float64(Some(1e-6)),
        ];
        values.into_iter().zip(eps).for_each(|(value, eps)| {
            assert!(
                next_value(value.clone())
                    .sub(value.clone())
                    .unwrap()
                    .lt(&eps)
            );
            assert!(value.sub(prev_value(value.clone())).unwrap().lt(&eps));
            assert_ne!(next_value(value.clone()), value);
            assert_ne!(prev_value(value.clone()), value);
        });

        let min_max = vec![
            (
                ScalarValue::UInt64(Some(u64::MIN)),
                ScalarValue::UInt64(Some(u64::MAX)),
            ),
            (
                ScalarValue::Int8(Some(i8::MIN)),
                ScalarValue::Int8(Some(i8::MAX)),
            ),
            (
                ScalarValue::Float32(Some(f32::MIN)),
                ScalarValue::Float32(Some(f32::MAX)),
            ),
            (
                ScalarValue::Float64(Some(f64::MIN)),
                ScalarValue::Float64(Some(f64::MAX)),
            ),
        ];
        let inf = vec![
            ScalarValue::UInt64(None),
            ScalarValue::Int8(None),
            ScalarValue::Float32(None),
            ScalarValue::Float64(None),
        ];
        min_max.into_iter().zip(inf).for_each(|((min, max), inf)| {
            assert_eq!(next_value(max.clone()), inf);
            assert_ne!(prev_value(max.clone()), max);
            assert_ne!(prev_value(max), inf);

            assert_eq!(prev_value(min.clone()), inf);
            assert_ne!(next_value(min.clone()), min);
            assert_ne!(next_value(min), inf);

            assert_eq!(next_value(inf.clone()), inf);
            assert_eq!(prev_value(inf.clone()), inf);
        });

        Ok(())
    }

    #[test]
    fn test_new_interval() -> Result<()> {
        use ScalarValue::*;

        let cases = vec![
            (
                (Boolean(None), Boolean(Some(false))),
                Boolean(Some(false)),
                Boolean(Some(false)),
            ),
            (
                (Boolean(Some(false)), Boolean(None)),
                Boolean(Some(false)),
                Boolean(Some(true)),
            ),
            (
                (Boolean(Some(false)), Boolean(Some(true))),
                Boolean(Some(false)),
                Boolean(Some(true)),
            ),
            (
                (UInt16(Some(u16::MAX)), UInt16(None)),
                UInt16(Some(u16::MAX)),
                UInt16(None),
            ),
            (
                (Int16(None), Int16(Some(-1000))),
                Int16(None),
                Int16(Some(-1000)),
            ),
            (
                (Float32(Some(f32::MAX)), Float32(Some(f32::MAX))),
                Float32(Some(f32::MAX)),
                Float32(Some(f32::MAX)),
            ),
            (
                (Float32(Some(f32::NAN)), Float32(Some(f32::MIN))),
                Float32(None),
                Float32(Some(f32::MIN)),
            ),
            (
                (
                    Float64(Some(f64::NEG_INFINITY)),
                    Float64(Some(f64::INFINITY)),
                ),
                Float64(None),
                Float64(None),
            ),
        ];
        for (inputs, lower, upper) in cases {
            let result = Interval::try_new(inputs.0, inputs.1)?;
            assert_eq!(result.clone().lower(), &lower);
            assert_eq!(result.upper(), &upper);
        }

        let invalid_intervals = vec![
            (Float32(Some(f32::INFINITY)), Float32(Some(100_f32))),
            (Float64(Some(0_f64)), Float64(Some(f64::NEG_INFINITY))),
            (Boolean(Some(true)), Boolean(Some(false))),
            (Int32(Some(1000)), Int32(Some(-2000))),
            (UInt64(Some(1)), UInt64(Some(0))),
        ];
        for (lower, upper) in invalid_intervals {
            Interval::try_new(lower, upper).expect_err(
                "Given parameters should have given an invalid interval error",
            );
        }

        Ok(())
    }

    #[test]
    fn test_make_unbounded() -> Result<()> {
        use ScalarValue::*;

        let unbounded_cases = vec![
            (DataType::Boolean, Boolean(Some(false)), Boolean(Some(true))),
            (DataType::UInt8, UInt8(Some(0)), UInt8(None)),
            (DataType::UInt16, UInt16(Some(0)), UInt16(None)),
            (DataType::UInt32, UInt32(Some(0)), UInt32(None)),
            (DataType::UInt64, UInt64(Some(0)), UInt64(None)),
            (DataType::Int8, Int8(None), Int8(None)),
            (DataType::Int16, Int16(None), Int16(None)),
            (DataType::Int32, Int32(None), Int32(None)),
            (DataType::Int64, Int64(None), Int64(None)),
            (DataType::Float32, Float32(None), Float32(None)),
            (DataType::Float64, Float64(None), Float64(None)),
        ];
        for (dt, lower, upper) in unbounded_cases {
            let inf = Interval::make_unbounded(&dt)?;
            assert_eq!(inf.clone().lower(), &lower);
            assert_eq!(inf.upper(), &upper);
        }

        Ok(())
    }

    #[test]
    fn gt_lt_test() -> Result<()> {
        let exactly_gt_cases = vec![
            (
                Interval::make(Some(1000_i64), None)?,
                Interval::make(None, Some(999_i64))?,
            ),
            (
                Interval::make(Some(1000_i64), Some(1000_i64))?,
                Interval::make(None, Some(999_i64))?,
            ),
            (
                Interval::make(Some(501_i64), Some(1000_i64))?,
                Interval::make(Some(500_i64), Some(500_i64))?,
            ),
            (
                Interval::make(Some(-1000_i64), Some(1000_i64))?,
                Interval::make(None, Some(-1500_i64))?,
            ),
            (
                Interval::try_new(
                    next_value(ScalarValue::Float32(Some(0.0))),
                    next_value(ScalarValue::Float32(Some(0.0))),
                )?,
                Interval::make(Some(0.0_f32), Some(0.0_f32))?,
            ),
            (
                Interval::make(Some(-1.0_f32), Some(-1.0_f32))?,
                Interval::try_new(
                    prev_value(ScalarValue::Float32(Some(-1.0))),
                    prev_value(ScalarValue::Float32(Some(-1.0))),
                )?,
            ),
        ];
        for (first, second) in exactly_gt_cases {
            assert_eq!(first.gt(second.clone())?, Interval::TRUE);
            assert_eq!(second.lt(first)?, Interval::TRUE);
        }

        let possibly_gt_cases = vec![
            (
                Interval::make(Some(1000_i64), Some(2000_i64))?,
                Interval::make(Some(1000_i64), Some(1000_i64))?,
            ),
            (
                Interval::make(Some(500_i64), Some(1000_i64))?,
                Interval::make(Some(500_i64), Some(1000_i64))?,
            ),
            (
                Interval::make(Some(1000_i64), None)?,
                Interval::make(Some(1000_i64), None)?,
            ),
            (
                Interval::make::<i64>(None, None)?,
                Interval::make::<i64>(None, None)?,
            ),
            (
                Interval::try_new(
                    ScalarValue::Float32(Some(0.0_f32)),
                    next_value(ScalarValue::Float32(Some(0.0_f32))),
                )?,
                Interval::make(Some(0.0_f32), Some(0.0_f32))?,
            ),
            (
                Interval::make(Some(-1.0_f32), Some(-1.0_f32))?,
                Interval::try_new(
                    prev_value(ScalarValue::Float32(Some(-1.0_f32))),
                    ScalarValue::Float32(Some(-1.0_f32)),
                )?,
            ),
        ];
        for (first, second) in possibly_gt_cases {
            assert_eq!(first.gt(second.clone())?, Interval::TRUE_OR_FALSE);
            assert_eq!(second.lt(first)?, Interval::TRUE_OR_FALSE);
        }

        let not_gt_cases = vec![
            (
                Interval::make(Some(1000_i64), Some(1000_i64))?,
                Interval::make(Some(1000_i64), Some(1000_i64))?,
            ),
            (
                Interval::make(Some(500_i64), Some(1000_i64))?,
                Interval::make(Some(1000_i64), None)?,
            ),
            (
                Interval::make(None, Some(1000_i64))?,
                Interval::make(Some(1000_i64), Some(1500_i64))?,
            ),
            (
                Interval::make(Some(0_u8), Some(0_u8))?,
                Interval::make::<u8>(None, None)?,
            ),
            (
                Interval::try_new(
                    prev_value(ScalarValue::Float32(Some(0.0_f32))),
                    ScalarValue::Float32(Some(0.0_f32)),
                )?,
                Interval::make(Some(0.0_f32), Some(0.0_f32))?,
            ),
            (
                Interval::make(Some(-1.0_f32), Some(-1.0_f32))?,
                Interval::try_new(
                    ScalarValue::Float32(Some(-1.0_f32)),
                    next_value(ScalarValue::Float32(Some(-1.0_f32))),
                )?,
            ),
        ];
        for (first, second) in not_gt_cases {
            assert_eq!(first.gt(second.clone())?, Interval::FALSE);
            assert_eq!(second.lt(first)?, Interval::FALSE);
        }

        Ok(())
    }

    #[test]
    fn gteq_lteq_test() -> Result<()> {
        let exactly_gteq_cases = vec![
            (
                Interval::make(Some(1000_i64), None)?,
                Interval::make(None, Some(1000_i64))?,
            ),
            (
                Interval::make(Some(1000_i64), Some(1000_i64))?,
                Interval::make(None, Some(1000_i64))?,
            ),
            (
                Interval::make(Some(500_i64), Some(1000_i64))?,
                Interval::make(Some(500_i64), Some(500_i64))?,
            ),
            (
                Interval::make(Some(-1000_i64), Some(1000_i64))?,
                Interval::make(None, Some(-1500_i64))?,
            ),
            (
                Interval::make::<u64>(None, None)?,
                Interval::make(Some(0_u64), Some(0_u64))?,
            ),
            (
                Interval::make(Some(0.0_f32), Some(0.0_f32))?,
                Interval::make(Some(0.0_f32), Some(0.0_f32))?,
            ),
            (
                Interval::try_new(
                    ScalarValue::Float32(Some(-1.0)),
                    next_value(ScalarValue::Float32(Some(-1.0))),
                )?,
                Interval::try_new(
                    prev_value(ScalarValue::Float32(Some(-1.0))),
                    ScalarValue::Float32(Some(-1.0)),
                )?,
            ),
        ];
        for (first, second) in exactly_gteq_cases {
            assert_eq!(first.gt_eq(second.clone())?, Interval::TRUE);
            assert_eq!(second.lt_eq(first)?, Interval::TRUE);
        }

        let possibly_gteq_cases = vec![
            (
                Interval::make(Some(999_i64), Some(2000_i64))?,
                Interval::make(Some(1000_i64), Some(1000_i64))?,
            ),
            (
                Interval::make(Some(500_i64), Some(1000_i64))?,
                Interval::make(Some(500_i64), Some(1001_i64))?,
            ),
            (
                Interval::make(Some(0_i64), None)?,
                Interval::make(Some(1000_i64), None)?,
            ),
            (
                Interval::make::<i64>(None, None)?,
                Interval::make::<i64>(None, None)?,
            ),
            (
                Interval::try_new(
                    prev_value(ScalarValue::Float32(Some(0.0))),
                    ScalarValue::Float32(Some(0.0)),
                )?,
                Interval::make(Some(0.0_f32), Some(0.0_f32))?,
            ),
            (
                Interval::make(Some(-1.0_f32), Some(-1.0_f32))?,
                Interval::try_new(
                    prev_value(ScalarValue::Float32(Some(-1.0_f32))),
                    next_value(ScalarValue::Float32(Some(-1.0_f32))),
                )?,
            ),
        ];
        for (first, second) in possibly_gteq_cases {
            assert_eq!(first.gt_eq(second.clone())?, Interval::TRUE_OR_FALSE);
            assert_eq!(second.lt_eq(first)?, Interval::TRUE_OR_FALSE);
        }

        let not_gteq_cases = vec![
            (
                Interval::make(Some(1000_i64), Some(1000_i64))?,
                Interval::make(Some(2000_i64), Some(2000_i64))?,
            ),
            (
                Interval::make(Some(500_i64), Some(999_i64))?,
                Interval::make(Some(1000_i64), None)?,
            ),
            (
                Interval::make(None, Some(1000_i64))?,
                Interval::make(Some(1001_i64), Some(1500_i64))?,
            ),
            (
                Interval::try_new(
                    prev_value(ScalarValue::Float32(Some(0.0_f32))),
                    prev_value(ScalarValue::Float32(Some(0.0_f32))),
                )?,
                Interval::make(Some(0.0_f32), Some(0.0_f32))?,
            ),
            (
                Interval::make(Some(-1.0_f32), Some(-1.0_f32))?,
                Interval::try_new(
                    next_value(ScalarValue::Float32(Some(-1.0))),
                    next_value(ScalarValue::Float32(Some(-1.0))),
                )?,
            ),
        ];
        for (first, second) in not_gteq_cases {
            assert_eq!(first.gt_eq(second.clone())?, Interval::FALSE);
            assert_eq!(second.lt_eq(first)?, Interval::FALSE);
        }

        Ok(())
    }

    #[test]
    fn equal_test() -> Result<()> {
        let exactly_eq_cases = vec![
            (
                Interval::make(Some(1000_i64), Some(1000_i64))?,
                Interval::make(Some(1000_i64), Some(1000_i64))?,
            ),
            (
                Interval::make(Some(0_u64), Some(0_u64))?,
                Interval::make(Some(0_u64), Some(0_u64))?,
            ),
            (
                Interval::make(Some(f32::MAX), Some(f32::MAX))?,
                Interval::make(Some(f32::MAX), Some(f32::MAX))?,
            ),
            (
                Interval::make(Some(f64::MIN), Some(f64::MIN))?,
                Interval::make(Some(f64::MIN), Some(f64::MIN))?,
            ),
        ];
        for (first, second) in exactly_eq_cases {
            assert_eq!(first.equal(second.clone())?, Interval::TRUE);
            assert_eq!(second.equal(first)?, Interval::TRUE);
        }

        let possibly_eq_cases = vec![
            (
                Interval::make::<i64>(None, None)?,
                Interval::make::<i64>(None, None)?,
            ),
            (
                Interval::make(Some(0_i64), Some(0_i64))?,
                Interval::make(Some(0_i64), Some(1000_i64))?,
            ),
            (
                Interval::make(Some(0_i64), Some(0_i64))?,
                Interval::make(Some(0_i64), Some(1000_i64))?,
            ),
            (
                Interval::make(Some(100.0_f32), Some(200.0_f32))?,
                Interval::make(Some(0.0_f32), Some(1000.0_f32))?,
            ),
            (
                Interval::try_new(
                    prev_value(ScalarValue::Float32(Some(0.0))),
                    ScalarValue::Float32(Some(0.0)),
                )?,
                Interval::make(Some(0.0_f32), Some(0.0_f32))?,
            ),
            (
                Interval::make(Some(-1.0_f32), Some(-1.0_f32))?,
                Interval::try_new(
                    prev_value(ScalarValue::Float32(Some(-1.0))),
                    next_value(ScalarValue::Float32(Some(-1.0))),
                )?,
            ),
        ];
        for (first, second) in possibly_eq_cases {
            assert_eq!(first.equal(second.clone())?, Interval::TRUE_OR_FALSE);
            assert_eq!(second.equal(first)?, Interval::TRUE_OR_FALSE);
        }

        let not_eq_cases = vec![
            (
                Interval::make(Some(1000_i64), Some(1000_i64))?,
                Interval::make(Some(2000_i64), Some(2000_i64))?,
            ),
            (
                Interval::make(Some(500_i64), Some(999_i64))?,
                Interval::make(Some(1000_i64), None)?,
            ),
            (
                Interval::make(None, Some(1000_i64))?,
                Interval::make(Some(1001_i64), Some(1500_i64))?,
            ),
            (
                Interval::try_new(
                    prev_value(ScalarValue::Float32(Some(0.0))),
                    prev_value(ScalarValue::Float32(Some(0.0))),
                )?,
                Interval::make(Some(0.0_f32), Some(0.0_f32))?,
            ),
            (
                Interval::make(Some(-1.0_f32), Some(-1.0_f32))?,
                Interval::try_new(
                    next_value(ScalarValue::Float32(Some(-1.0))),
                    next_value(ScalarValue::Float32(Some(-1.0))),
                )?,
            ),
        ];
        for (first, second) in not_eq_cases {
            assert_eq!(first.equal(second.clone())?, Interval::FALSE);
            assert_eq!(second.equal(first)?, Interval::FALSE);
        }

        Ok(())
    }

    #[test]
    fn and_test() -> Result<()> {
        let cases = vec![
            (Interval::TRUE_OR_FALSE, Interval::FALSE, Interval::FALSE),
            (
                Interval::TRUE_OR_FALSE,
                Interval::TRUE_OR_FALSE,
                Interval::TRUE_OR_FALSE,
            ),
            (
                Interval::TRUE_OR_FALSE,
                Interval::TRUE,
                Interval::TRUE_OR_FALSE,
            ),
            (Interval::FALSE, Interval::FALSE, Interval::FALSE),
            (Interval::FALSE, Interval::TRUE_OR_FALSE, Interval::FALSE),
            (Interval::FALSE, Interval::TRUE, Interval::FALSE),
            (Interval::TRUE, Interval::FALSE, Interval::FALSE),
            (
                Interval::TRUE,
                Interval::TRUE_OR_FALSE,
                Interval::TRUE_OR_FALSE,
            ),
            (Interval::TRUE, Interval::TRUE, Interval::TRUE),
        ];

        for case in cases {
            assert_eq!(
                case.0.and(&case.1)?,
                case.2,
                "Failed for {} AND {}",
                case.0,
                case.1
            );
        }
        Ok(())
    }

    #[test]
    fn or_test() -> Result<()> {
        let cases = vec![
            (
                Interval::TRUE_OR_FALSE,
                Interval::FALSE,
                Interval::TRUE_OR_FALSE,
            ),
            (
                Interval::TRUE_OR_FALSE,
                Interval::TRUE_OR_FALSE,
                Interval::TRUE_OR_FALSE,
            ),
            (Interval::TRUE_OR_FALSE, Interval::TRUE, Interval::TRUE),
            (Interval::FALSE, Interval::FALSE, Interval::FALSE),
            (
                Interval::FALSE,
                Interval::TRUE_OR_FALSE,
                Interval::TRUE_OR_FALSE,
            ),
            (Interval::FALSE, Interval::TRUE, Interval::TRUE),
            (Interval::TRUE, Interval::FALSE, Interval::TRUE),
            (Interval::TRUE, Interval::TRUE_OR_FALSE, Interval::TRUE),
            (Interval::TRUE, Interval::TRUE, Interval::TRUE),
        ];

        for case in cases {
            assert_eq!(
                case.0.or(&case.1)?,
                case.2,
                "Failed for {} OR {}",
                case.0,
                case.1
            );
        }
        Ok(())
    }

    #[test]
    fn not_test() -> Result<()> {
        let cases = vec![
            (Interval::TRUE_OR_FALSE, Interval::TRUE_OR_FALSE),
            (Interval::FALSE, Interval::TRUE),
            (Interval::TRUE, Interval::FALSE),
        ];

        for case in cases {
            assert_eq!(case.0.not()?, case.1, "Failed for NOT {}", case.0);
        }
        Ok(())
    }

    #[test]
    fn test_and_or_with_normalized_boolean_intervals() -> Result<()> {
        // Verify that NULL boolean bounds are normalized and don't cause errors
        let from_nulls =
            Interval::try_new(ScalarValue::Boolean(None), ScalarValue::Boolean(None))?;

        assert!(from_nulls.or(&Interval::TRUE).is_ok());
        assert!(from_nulls.and(&Interval::FALSE).is_ok());

        Ok(())
    }

    // Tests that there's no such thing as a 'null' boolean interval.
    // An interval with two `Boolean(None)` boundaries is normalised to `Interval::TRUE_OR_FALSE`.
    #[test]
    fn test_null_boolean_interval() {
        let null_interval =
            Interval::try_new(ScalarValue::Boolean(None), ScalarValue::Boolean(None))
                .unwrap();

        assert_eq!(null_interval, Interval::TRUE_OR_FALSE);
    }

    // Asserts that `Interval::TRUE_OR_FALSE` represents a set that contains `true`, `false`, and does
    // not contain `null`.
    #[test]
    fn test_uncertain_boolean_interval() {
        assert!(
            Interval::TRUE_OR_FALSE
                .contains_value(ScalarValue::Boolean(Some(true)))
                .unwrap()
        );
        assert!(
            Interval::TRUE_OR_FALSE
                .contains_value(ScalarValue::Boolean(Some(false)))
                .unwrap()
        );
        assert!(
            !Interval::TRUE_OR_FALSE
                .contains_value(ScalarValue::Boolean(None))
                .unwrap()
        );
        assert!(
            !Interval::TRUE_OR_FALSE
                .contains_value(ScalarValue::Null)
                .unwrap()
        );
    }

    #[test]
    fn test_and_uncertain_boolean_intervals() -> Result<()> {
        let and_result = Interval::TRUE_OR_FALSE.and(&Interval::FALSE)?;
        assert_eq!(and_result, Interval::FALSE);

        let and_result = Interval::FALSE.and(&Interval::TRUE_OR_FALSE)?;
        assert_eq!(and_result, Interval::FALSE);

        let and_result = Interval::TRUE_OR_FALSE.and(&Interval::TRUE)?;
        assert_eq!(and_result, Interval::TRUE_OR_FALSE);

        let and_result = Interval::TRUE.and(&Interval::TRUE_OR_FALSE)?;
        assert_eq!(and_result, Interval::TRUE_OR_FALSE);

        let and_result = Interval::TRUE_OR_FALSE.and(&Interval::TRUE_OR_FALSE)?;
        assert_eq!(and_result, Interval::TRUE_OR_FALSE);

        Ok(())
    }

    #[test]
    fn test_or_uncertain_boolean_intervals() -> Result<()> {
        let or_result = Interval::TRUE_OR_FALSE.or(&Interval::FALSE)?;
        assert_eq!(or_result, Interval::TRUE_OR_FALSE);

        let or_result = Interval::FALSE.or(&Interval::TRUE_OR_FALSE)?;
        assert_eq!(or_result, Interval::TRUE_OR_FALSE);

        let or_result = Interval::TRUE_OR_FALSE.or(&Interval::TRUE)?;
        assert_eq!(or_result, Interval::TRUE);

        let or_result = Interval::TRUE.or(&Interval::TRUE_OR_FALSE)?;
        assert_eq!(or_result, Interval::TRUE);

        let or_result = Interval::TRUE_OR_FALSE.or(&Interval::TRUE_OR_FALSE)?;
        assert_eq!(or_result, Interval::TRUE_OR_FALSE);

        Ok(())
    }

    #[test]
    fn intersect_test() -> Result<()> {
        let possible_cases = vec![
            (
                Interval::make(Some(1000_i64), None)?,
                Interval::make::<i64>(None, None)?,
                Interval::make(Some(1000_i64), None)?,
            ),
            (
                Interval::make(Some(1000_i64), None)?,
                Interval::make(None, Some(1000_i64))?,
                Interval::make(Some(1000_i64), Some(1000_i64))?,
            ),
            (
                Interval::make(Some(1000_i64), None)?,
                Interval::make(None, Some(2000_i64))?,
                Interval::make(Some(1000_i64), Some(2000_i64))?,
            ),
            (
                Interval::make(Some(1000_i64), Some(2000_i64))?,
                Interval::make(Some(1000_i64), None)?,
                Interval::make(Some(1000_i64), Some(2000_i64))?,
            ),
            (
                Interval::make(Some(1000_i64), Some(2000_i64))?,
                Interval::make(Some(1000_i64), Some(1500_i64))?,
                Interval::make(Some(1000_i64), Some(1500_i64))?,
            ),
            (
                Interval::make(Some(1000_i64), Some(2000_i64))?,
                Interval::make(Some(500_i64), Some(1500_i64))?,
                Interval::make(Some(1000_i64), Some(1500_i64))?,
            ),
            (
                Interval::make::<i64>(None, None)?,
                Interval::make::<i64>(None, None)?,
                Interval::make::<i64>(None, None)?,
            ),
            (
                Interval::make(None, Some(2000_u64))?,
                Interval::make(Some(500_u64), None)?,
                Interval::make(Some(500_u64), Some(2000_u64))?,
            ),
            (
                Interval::make(Some(0_u64), Some(0_u64))?,
                Interval::make(Some(0_u64), None)?,
                Interval::make(Some(0_u64), Some(0_u64))?,
            ),
            (
                Interval::make(Some(1000.0_f32), None)?,
                Interval::make(None, Some(1000.0_f32))?,
                Interval::make(Some(1000.0_f32), Some(1000.0_f32))?,
            ),
            (
                Interval::make(Some(1000.0_f32), Some(1500.0_f32))?,
                Interval::make(Some(0.0_f32), Some(1500.0_f32))?,
                Interval::make(Some(1000.0_f32), Some(1500.0_f32))?,
            ),
            (
                Interval::make(Some(-1000.0_f64), Some(1500.0_f64))?,
                Interval::make(Some(-1500.0_f64), Some(2000.0_f64))?,
                Interval::make(Some(-1000.0_f64), Some(1500.0_f64))?,
            ),
            (
                Interval::make(Some(16.0_f64), Some(32.0_f64))?,
                Interval::make(Some(32.0_f64), Some(64.0_f64))?,
                Interval::make(Some(32.0_f64), Some(32.0_f64))?,
            ),
        ];
        for (first, second, expected) in possible_cases {
            assert_eq!(first.intersect(second)?.unwrap(), expected)
        }

        let empty_cases = vec![
            (
                Interval::make(Some(1000_i64), None)?,
                Interval::make(None, Some(0_i64))?,
            ),
            (
                Interval::make(Some(1000_i64), None)?,
                Interval::make(None, Some(999_i64))?,
            ),
            (
                Interval::make(Some(1500_i64), Some(2000_i64))?,
                Interval::make(Some(1000_i64), Some(1499_i64))?,
            ),
            (
                Interval::make(Some(0_i64), Some(1000_i64))?,
                Interval::make(Some(2000_i64), Some(3000_i64))?,
            ),
            (
                Interval::try_new(
                    prev_value(ScalarValue::Float32(Some(1.0))),
                    prev_value(ScalarValue::Float32(Some(1.0))),
                )?,
                Interval::make(Some(1.0_f32), Some(1.0_f32))?,
            ),
            (
                Interval::try_new(
                    next_value(ScalarValue::Float32(Some(1.0))),
                    next_value(ScalarValue::Float32(Some(1.0))),
                )?,
                Interval::make(Some(1.0_f32), Some(1.0_f32))?,
            ),
        ];
        for (first, second) in empty_cases {
            assert_eq!(first.intersect(second)?, None)
        }

        Ok(())
    }

    #[test]
    fn union_test() -> Result<()> {
        let possible_cases = vec![
            (
                Interval::make(Some(1000_i64), None)?,
                Interval::make::<i64>(None, None)?,
                Interval::make_unbounded(&DataType::Int64)?,
            ),
            (
                Interval::make(Some(1000_i64), None)?,
                Interval::make(None, Some(1000_i64))?,
                Interval::make_unbounded(&DataType::Int64)?,
            ),
            (
                Interval::make(Some(1000_i64), None)?,
                Interval::make(None, Some(2000_i64))?,
                Interval::make_unbounded(&DataType::Int64)?,
            ),
            (
                Interval::make(Some(1000_i64), Some(2000_i64))?,
                Interval::make(Some(1000_i64), None)?,
                Interval::make(Some(1000_i64), None)?,
            ),
            (
                Interval::make(Some(1000_i64), Some(2000_i64))?,
                Interval::make(Some(1000_i64), Some(1500_i64))?,
                Interval::make(Some(1000_i64), Some(2000_i64))?,
            ),
            (
                Interval::make(Some(1000_i64), Some(2000_i64))?,
                Interval::make(Some(500_i64), Some(1500_i64))?,
                Interval::make(Some(500_i64), Some(2000_i64))?,
            ),
            (
                Interval::make::<i64>(None, None)?,
                Interval::make::<i64>(None, None)?,
                Interval::make::<i64>(None, None)?,
            ),
            (
                Interval::make(Some(1000_i64), None)?,
                Interval::make(None, Some(0_i64))?,
                Interval::make_unbounded(&DataType::Int64)?,
            ),
            (
                Interval::make(Some(1000_i64), None)?,
                Interval::make(None, Some(999_i64))?,
                Interval::make_unbounded(&DataType::Int64)?,
            ),
            (
                Interval::make(Some(1500_i64), Some(2000_i64))?,
                Interval::make(Some(1000_i64), Some(1499_i64))?,
                Interval::make(Some(1000_i64), Some(2000_i64))?,
            ),
            (
                Interval::make(Some(0_i64), Some(1000_i64))?,
                Interval::make(Some(2000_i64), Some(3000_i64))?,
                Interval::make(Some(0_i64), Some(3000_i64))?,
            ),
            (
                Interval::make(None, Some(2000_u64))?,
                Interval::make(Some(500_u64), None)?,
                Interval::make(Some(0_u64), None)?,
            ),
            (
                Interval::make(Some(0_u64), Some(0_u64))?,
                Interval::make(Some(0_u64), None)?,
                Interval::make(Some(0_u64), None)?,
            ),
            (
                Interval::make(Some(1000.0_f32), None)?,
                Interval::make(None, Some(1000.0_f32))?,
                Interval::make_unbounded(&DataType::Float32)?,
            ),
            (
                Interval::make(Some(1000.0_f32), Some(1500.0_f32))?,
                Interval::make(Some(0.0_f32), Some(1500.0_f32))?,
                Interval::make(Some(0.0_f32), Some(1500.0_f32))?,
            ),
            (
                Interval::try_new(
                    prev_value(ScalarValue::Float32(Some(1.0))),
                    prev_value(ScalarValue::Float32(Some(1.0))),
                )?,
                Interval::make(Some(1.0_f32), Some(1.0_f32))?,
                Interval::try_new(
                    prev_value(ScalarValue::Float32(Some(1.0))),
                    ScalarValue::Float32(Some(1.0)),
                )?,
            ),
            (
                Interval::try_new(
                    next_value(ScalarValue::Float32(Some(1.0))),
                    next_value(ScalarValue::Float32(Some(1.0))),
                )?,
                Interval::make(Some(1.0_f32), Some(1.0_f32))?,
                Interval::try_new(
                    ScalarValue::Float32(Some(1.0)),
                    next_value(ScalarValue::Float32(Some(1.0))),
                )?,
            ),
            (
                Interval::make(Some(-1000.0_f64), Some(1500.0_f64))?,
                Interval::make(Some(-1500.0_f64), Some(2000.0_f64))?,
                Interval::make(Some(-1500.0_f64), Some(2000.0_f64))?,
            ),
            (
                Interval::make(Some(16.0_f64), Some(32.0_f64))?,
                Interval::make(Some(32.0_f64), Some(64.0_f64))?,
                Interval::make(Some(16.0_f64), Some(64.0_f64))?,
            ),
        ];
        for (first, second, expected) in possible_cases {
            println!("{first}");
            println!("{second}");
            assert_eq!(first.union(second)?, expected)
        }

        Ok(())
    }

    #[test]
    fn test_contains() -> Result<()> {
        let possible_cases = vec![
            (
                Interval::make::<i64>(None, None)?,
                Interval::make::<i64>(None, None)?,
                Interval::TRUE,
            ),
            (
                Interval::make(Some(1500_i64), Some(2000_i64))?,
                Interval::make(Some(1501_i64), Some(1999_i64))?,
                Interval::TRUE,
            ),
            (
                Interval::make(Some(1000_i64), None)?,
                Interval::make::<i64>(None, None)?,
                Interval::TRUE_OR_FALSE,
            ),
            (
                Interval::make(Some(1000_i64), Some(2000_i64))?,
                Interval::make(Some(500), Some(1500_i64))?,
                Interval::TRUE_OR_FALSE,
            ),
            (
                Interval::make(Some(16.0), Some(32.0))?,
                Interval::make(Some(32.0), Some(64.0))?,
                Interval::TRUE_OR_FALSE,
            ),
            (
                Interval::make(Some(1000_i64), None)?,
                Interval::make(None, Some(0_i64))?,
                Interval::FALSE,
            ),
            (
                Interval::make(Some(1500_i64), Some(2000_i64))?,
                Interval::make(Some(1000_i64), Some(1499_i64))?,
                Interval::FALSE,
            ),
            (
                Interval::try_new(
                    prev_value(ScalarValue::Float32(Some(1.0))),
                    prev_value(ScalarValue::Float32(Some(1.0))),
                )?,
                Interval::make(Some(1.0_f32), Some(1.0_f32))?,
                Interval::FALSE,
            ),
            (
                Interval::try_new(
                    next_value(ScalarValue::Float32(Some(1.0))),
                    next_value(ScalarValue::Float32(Some(1.0))),
                )?,
                Interval::make(Some(1.0_f32), Some(1.0_f32))?,
                Interval::FALSE,
            ),
        ];
        for (first, second, expected) in possible_cases {
            assert_eq!(first.contains(second)?, expected)
        }

        Ok(())
    }

    #[test]
    fn test_contains_value() -> Result<()> {
        let possible_cases = vec![
            (
                Interval::make(Some(0), Some(100))?,
                ScalarValue::Int32(Some(50)),
                true,
            ),
            (
                Interval::make(Some(0), Some(100))?,
                ScalarValue::Int32(Some(150)),
                false,
            ),
            (
                Interval::make(Some(0), Some(100))?,
                ScalarValue::Float64(Some(50.)),
                true,
            ),
            (
                Interval::make(Some(0), Some(100))?,
                ScalarValue::Float64(Some(next_down(100.))),
                true,
            ),
            (
                Interval::make(Some(0), Some(100))?,
                ScalarValue::Float64(Some(next_up(100.))),
                false,
            ),
        ];

        for (interval, value, expected) in possible_cases {
            assert_eq!(interval.contains_value(value)?, expected)
        }

        Ok(())
    }

    #[test]
    fn test_add() -> Result<()> {
        let cases = vec![
            (
                Interval::make(Some(100_i64), Some(200_i64))?,
                Interval::make(None, Some(200_i64))?,
                Interval::make(None, Some(400_i64))?,
            ),
            (
                Interval::make(Some(100_i64), Some(200_i64))?,
                Interval::make(Some(200_i64), None)?,
                Interval::make(Some(300_i64), None)?,
            ),
            (
                Interval::make(None, Some(200_i64))?,
                Interval::make(Some(100_i64), Some(200_i64))?,
                Interval::make(None, Some(400_i64))?,
            ),
            (
                Interval::make(Some(200_i64), None)?,
                Interval::make(Some(100_i64), Some(200_i64))?,
                Interval::make(Some(300_i64), None)?,
            ),
            (
                Interval::make(Some(100_i64), Some(200_i64))?,
                Interval::make(Some(-300_i64), Some(150_i64))?,
                Interval::make(Some(-200_i64), Some(350_i64))?,
            ),
            (
                Interval::make(Some(f32::MAX), Some(f32::MAX))?,
                Interval::make(Some(11_f32), Some(11_f32))?,
                Interval::make(Some(f32::MAX), None)?,
            ),
            (
                Interval::make(Some(f32::MIN), Some(f32::MIN))?,
                Interval::make(Some(-10_f32), Some(10_f32))?,
                // Since rounding mode is up, the result would be much greater than f32::MIN
                // (f32::MIN = -3.4_028_235e38, the result is -3.4_028_233e38)
                Interval::make(
                    None,
                    Some(-340282330000000000000000000000000000000.0_f32),
                )?,
            ),
            (
                Interval::make(Some(f32::MIN), Some(f32::MIN))?,
                Interval::make(Some(-10_f32), Some(-10_f32))?,
                Interval::make(None, Some(f32::MIN))?,
            ),
            (
                Interval::make(Some(1.0), Some(f32::MAX))?,
                Interval::make(Some(f32::MAX), Some(f32::MAX))?,
                Interval::make(Some(f32::MAX), None)?,
            ),
            (
                Interval::make(Some(f32::MIN), Some(f32::MIN))?,
                Interval::make(Some(f32::MAX), Some(f32::MAX))?,
                Interval::make(Some(-0.0_f32), Some(0.0_f32))?,
            ),
            (
                Interval::make(Some(100_f64), None)?,
                Interval::make(None, Some(200_f64))?,
                Interval::make::<i64>(None, None)?,
            ),
            (
                Interval::make(None, Some(100_f64))?,
                Interval::make(None, Some(200_f64))?,
                Interval::make(None, Some(300_f64))?,
            ),
        ];
        for case in cases {
            let result = case.0.add(case.1)?;
            if case.0.data_type().is_floating() {
                assert!(
                    result.lower().is_null() && case.2.lower().is_null()
                        || result.lower().le(case.2.lower())
                );
                assert!(
                    result.upper().is_null() && case.2.upper().is_null()
                        || result.upper().ge(case.2.upper())
                );
            } else {
                assert_eq!(result, case.2);
            }
        }

        Ok(())
    }

    #[test]
    fn test_sub() -> Result<()> {
        let cases = vec![
            (
                Interval::make(Some(i32::MAX), Some(i32::MAX))?,
                Interval::make(Some(11_i32), Some(11_i32))?,
                Interval::make(Some(i32::MAX - 11), Some(i32::MAX - 11))?,
            ),
            (
                Interval::make(Some(100_i64), Some(200_i64))?,
                Interval::make(None, Some(200_i64))?,
                Interval::make(Some(-100_i64), None)?,
            ),
            (
                Interval::make(Some(100_i64), Some(200_i64))?,
                Interval::make(Some(200_i64), None)?,
                Interval::make(None, Some(0_i64))?,
            ),
            (
                Interval::make(None, Some(200_i64))?,
                Interval::make(Some(100_i64), Some(200_i64))?,
                Interval::make(None, Some(100_i64))?,
            ),
            (
                Interval::make(Some(200_i64), None)?,
                Interval::make(Some(100_i64), Some(200_i64))?,
                Interval::make(Some(0_i64), None)?,
            ),
            (
                Interval::make(Some(100_i64), Some(200_i64))?,
                Interval::make(Some(-300_i64), Some(150_i64))?,
                Interval::make(Some(-50_i64), Some(500_i64))?,
            ),
            (
                Interval::make(Some(i64::MIN), Some(i64::MIN))?,
                Interval::make(Some(-10_i64), Some(-10_i64))?,
                Interval::make(Some(i64::MIN + 10), Some(i64::MIN + 10))?,
            ),
            (
                Interval::make(Some(1), Some(i64::MAX))?,
                Interval::make(Some(i64::MAX), Some(i64::MAX))?,
                Interval::make(Some(1 - i64::MAX), Some(0))?,
            ),
            (
                Interval::make(Some(i64::MIN), Some(i64::MIN))?,
                Interval::make(Some(i64::MAX), Some(i64::MAX))?,
                Interval::make(None, Some(i64::MIN))?,
            ),
            (
                Interval::make(Some(2_u32), Some(10_u32))?,
                Interval::make(Some(4_u32), Some(6_u32))?,
                Interval::make(None, Some(6_u32))?,
            ),
            (
                Interval::make(Some(2_u32), Some(10_u32))?,
                Interval::make(Some(20_u32), Some(30_u32))?,
                Interval::make(None, Some(0_u32))?,
            ),
            (
                Interval::make(Some(f32::MIN), Some(f32::MIN))?,
                Interval::make(Some(-10_f32), Some(10_f32))?,
                // Since rounding mode is up, the result would be much larger than f32::MIN
                // (f32::MIN = -3.4_028_235e38, the result is -3.4_028_233e38)
                Interval::make(
                    None,
                    Some(-340282330000000000000000000000000000000.0_f32),
                )?,
            ),
            (
                Interval::make(Some(100_f64), None)?,
                Interval::make(None, Some(200_f64))?,
                Interval::make(Some(-100_f64), None)?,
            ),
            (
                Interval::make(None, Some(100_f64))?,
                Interval::make(None, Some(200_f64))?,
                Interval::make::<i64>(None, None)?,
            ),
        ];
        for case in cases {
            let result = case.0.sub(case.1)?;
            if case.0.data_type().is_floating() {
                assert!(
                    result.lower().is_null() && case.2.lower().is_null()
                        || result.lower().le(case.2.lower())
                );
                assert!(
                    result.upper().is_null() && case.2.upper().is_null()
                        || result.upper().ge(case.2.upper(),)
                );
            } else {
                assert_eq!(result, case.2);
            }
        }

        Ok(())
    }

    #[test]
    fn test_mul() -> Result<()> {
        let cases = vec![
            (
                Interval::make(Some(1_i64), Some(2_i64))?,
                Interval::make(None, Some(2_i64))?,
                Interval::make(None, Some(4_i64))?,
            ),
            (
                Interval::make(Some(1_i64), Some(2_i64))?,
                Interval::make(Some(2_i64), None)?,
                Interval::make(Some(2_i64), None)?,
            ),
            (
                Interval::make(None, Some(2_i64))?,
                Interval::make(Some(1_i64), Some(2_i64))?,
                Interval::make(None, Some(4_i64))?,
            ),
            (
                Interval::make(Some(2_i64), None)?,
                Interval::make(Some(1_i64), Some(2_i64))?,
                Interval::make(Some(2_i64), None)?,
            ),
            (
                Interval::make(Some(1_i64), Some(2_i64))?,
                Interval::make(Some(-3_i64), Some(15_i64))?,
                Interval::make(Some(-6_i64), Some(30_i64))?,
            ),
            (
                Interval::make(Some(-0.0), Some(0.0))?,
                Interval::make(None, Some(0.0))?,
                Interval::make::<i64>(None, None)?,
            ),
            (
                Interval::make(Some(f32::MIN), Some(f32::MIN))?,
                Interval::make(Some(-10_f32), Some(10_f32))?,
                Interval::make::<i64>(None, None)?,
            ),
            (
                Interval::make(Some(1_u32), Some(2_u32))?,
                Interval::make(Some(0_u32), Some(1_u32))?,
                Interval::make(Some(0_u32), Some(2_u32))?,
            ),
            (
                Interval::make(None, Some(2_u32))?,
                Interval::make(Some(0_u32), Some(1_u32))?,
                Interval::make(None, Some(2_u32))?,
            ),
            (
                Interval::make(None, Some(2_u32))?,
                Interval::make(Some(1_u32), Some(2_u32))?,
                Interval::make(None, Some(4_u32))?,
            ),
            (
                Interval::make(None, Some(2_u32))?,
                Interval::make(Some(1_u32), None)?,
                Interval::make::<u32>(None, None)?,
            ),
            (
                Interval::make::<u32>(None, None)?,
                Interval::make(Some(0_u32), None)?,
                Interval::make::<u32>(None, None)?,
            ),
            (
                Interval::make(Some(f32::MAX), Some(f32::MAX))?,
                Interval::make(Some(11_f32), Some(11_f32))?,
                Interval::make(Some(f32::MAX), None)?,
            ),
            (
                Interval::make(Some(f32::MIN), Some(f32::MIN))?,
                Interval::make(Some(-10_f32), Some(-10_f32))?,
                Interval::make(Some(f32::MAX), None)?,
            ),
            (
                Interval::make(Some(1.0), Some(f32::MAX))?,
                Interval::make(Some(f32::MAX), Some(f32::MAX))?,
                Interval::make(Some(f32::MAX), None)?,
            ),
            (
                Interval::make(Some(f32::MIN), Some(f32::MIN))?,
                Interval::make(Some(f32::MAX), Some(f32::MAX))?,
                Interval::make(None, Some(f32::MIN))?,
            ),
            (
                Interval::make(Some(-0.0_f32), Some(0.0_f32))?,
                Interval::make(Some(f32::MAX), None)?,
                Interval::make::<f32>(None, None)?,
            ),
            (
                Interval::make(Some(0.0_f32), Some(0.0_f32))?,
                Interval::make(Some(f32::MAX), None)?,
                Interval::make(Some(0.0_f32), None)?,
            ),
            (
                Interval::make(Some(1_f64), None)?,
                Interval::make(None, Some(2_f64))?,
                Interval::make::<f64>(None, None)?,
            ),
            (
                Interval::make(None, Some(1_f64))?,
                Interval::make(None, Some(2_f64))?,
                Interval::make::<f64>(None, None)?,
            ),
            (
                Interval::make(Some(-0.0_f64), Some(-0.0_f64))?,
                Interval::make(Some(1_f64), Some(2_f64))?,
                Interval::make(Some(-0.0_f64), Some(-0.0_f64))?,
            ),
            (
                Interval::make(Some(0.0_f64), Some(0.0_f64))?,
                Interval::make(Some(1_f64), Some(2_f64))?,
                Interval::make(Some(0.0_f64), Some(0.0_f64))?,
            ),
            (
                Interval::make(Some(-0.0_f64), Some(0.0_f64))?,
                Interval::make(Some(1_f64), Some(2_f64))?,
                Interval::make(Some(-0.0_f64), Some(0.0_f64))?,
            ),
            (
                Interval::make(Some(-0.0_f64), Some(1.0_f64))?,
                Interval::make(Some(1_f64), Some(2_f64))?,
                Interval::make(Some(-0.0_f64), Some(2.0_f64))?,
            ),
            (
                Interval::make(Some(0.0_f64), Some(1.0_f64))?,
                Interval::make(Some(1_f64), Some(2_f64))?,
                Interval::make(Some(0.0_f64), Some(2.0_f64))?,
            ),
            (
                Interval::make(Some(-0.0_f64), Some(1.0_f64))?,
                Interval::make(Some(-1_f64), Some(2_f64))?,
                Interval::make(Some(-1.0_f64), Some(2.0_f64))?,
            ),
            (
                Interval::make::<f64>(None, None)?,
                Interval::make(Some(-0.0_f64), Some(0.0_f64))?,
                Interval::make::<f64>(None, None)?,
            ),
            (
                Interval::make::<f64>(None, Some(10.0_f64))?,
                Interval::make(Some(-0.0_f64), Some(0.0_f64))?,
                Interval::make::<f64>(None, None)?,
            ),
        ];
        for case in cases {
            let result = case.0.mul(case.1)?;
            if case.0.data_type().is_floating() {
                assert!(
                    result.lower().is_null() && case.2.lower().is_null()
                        || result.lower().le(case.2.lower())
                );
                assert!(
                    result.upper().is_null() && case.2.upper().is_null()
                        || result.upper().ge(case.2.upper())
                );
            } else {
                assert_eq!(result, case.2);
            }
        }

        Ok(())
    }

    #[test]
    fn test_div() -> Result<()> {
        let cases = vec![
            (
                Interval::make(Some(100_i64), Some(200_i64))?,
                Interval::make(Some(1_i64), Some(2_i64))?,
                Interval::make(Some(50_i64), Some(200_i64))?,
            ),
            (
                Interval::make(Some(-200_i64), Some(-100_i64))?,
                Interval::make(Some(-2_i64), Some(-1_i64))?,
                Interval::make(Some(50_i64), Some(200_i64))?,
            ),
            (
                Interval::make(Some(100_i64), Some(200_i64))?,
                Interval::make(Some(-2_i64), Some(-1_i64))?,
                Interval::make(Some(-200_i64), Some(-50_i64))?,
            ),
            (
                Interval::make(Some(-200_i64), Some(-100_i64))?,
                Interval::make(Some(1_i64), Some(2_i64))?,
                Interval::make(Some(-200_i64), Some(-50_i64))?,
            ),
            (
                Interval::make(Some(-200_i64), Some(100_i64))?,
                Interval::make(Some(1_i64), Some(2_i64))?,
                Interval::make(Some(-200_i64), Some(100_i64))?,
            ),
            (
                Interval::make(Some(-100_i64), Some(200_i64))?,
                Interval::make(Some(1_i64), Some(2_i64))?,
                Interval::make(Some(-100_i64), Some(200_i64))?,
            ),
            (
                Interval::make(Some(10_i64), Some(20_i64))?,
                Interval::make::<i64>(None, None)?,
                Interval::make::<i64>(None, None)?,
            ),
            (
                Interval::make(Some(-100_i64), Some(200_i64))?,
                Interval::make(Some(-1_i64), Some(2_i64))?,
                Interval::make::<i64>(None, None)?,
            ),
            (
                Interval::make(Some(-100_i64), Some(200_i64))?,
                Interval::make(Some(-2_i64), Some(1_i64))?,
                Interval::make::<i64>(None, None)?,
            ),
            (
                Interval::make(Some(100_i64), Some(200_i64))?,
                Interval::make(Some(0_i64), Some(1_i64))?,
                Interval::make(Some(100_i64), None)?,
            ),
            (
                Interval::make(Some(100_i64), Some(200_i64))?,
                Interval::make(None, Some(0_i64))?,
                Interval::make(None, Some(0_i64))?,
            ),
            (
                Interval::make(Some(100_i64), Some(200_i64))?,
                Interval::make(Some(0_i64), Some(0_i64))?,
                Interval::make::<i64>(None, None)?,
            ),
            (
                Interval::make(Some(0_i64), Some(1_i64))?,
                Interval::make(Some(100_i64), Some(200_i64))?,
                Interval::make(Some(0_i64), Some(0_i64))?,
            ),
            (
                Interval::make(Some(0_i64), Some(1_i64))?,
                Interval::make(Some(100_i64), Some(200_i64))?,
                Interval::make(Some(0_i64), Some(0_i64))?,
            ),
            (
                Interval::make(Some(1_u32), Some(2_u32))?,
                Interval::make(Some(0_u32), Some(0_u32))?,
                Interval::make::<u32>(None, None)?,
            ),
            (
                Interval::make(Some(10_u32), Some(20_u32))?,
                Interval::make(None, Some(2_u32))?,
                Interval::make(Some(5_u32), None)?,
            ),
            (
                Interval::make(Some(10_u32), Some(20_u32))?,
                Interval::make(Some(0_u32), Some(2_u32))?,
                Interval::make(Some(5_u32), None)?,
            ),
            (
                Interval::make(Some(10_u32), Some(20_u32))?,
                Interval::make(Some(0_u32), Some(0_u32))?,
                Interval::make::<u32>(None, None)?,
            ),
            (
                Interval::make(Some(12_u64), Some(48_u64))?,
                Interval::make(Some(10_u64), Some(20_u64))?,
                Interval::make(Some(0_u64), Some(4_u64))?,
            ),
            (
                Interval::make(Some(12_u64), Some(48_u64))?,
                Interval::make(None, Some(2_u64))?,
                Interval::make(Some(6_u64), None)?,
            ),
            (
                Interval::make(Some(12_u64), Some(48_u64))?,
                Interval::make(Some(0_u64), Some(2_u64))?,
                Interval::make(Some(6_u64), None)?,
            ),
            (
                Interval::make(None, Some(48_u64))?,
                Interval::make(Some(0_u64), Some(2_u64))?,
                Interval::make::<u64>(None, None)?,
            ),
            (
                Interval::make(Some(f32::MAX), Some(f32::MAX))?,
                Interval::make(Some(-0.1_f32), Some(0.1_f32))?,
                Interval::make::<f32>(None, None)?,
            ),
            (
                Interval::make(Some(f32::MIN), None)?,
                Interval::make(Some(0.1_f32), Some(0.1_f32))?,
                Interval::make::<f32>(None, None)?,
            ),
            (
                Interval::make(Some(-10.0_f32), Some(10.0_f32))?,
                Interval::make(Some(-0.1_f32), Some(-0.1_f32))?,
                Interval::make(Some(-100.0_f32), Some(100.0_f32))?,
            ),
            (
                Interval::make(Some(-10.0_f32), Some(f32::MAX))?,
                Interval::make::<f32>(None, None)?,
                Interval::make::<f32>(None, None)?,
            ),
            (
                Interval::make(Some(f32::MIN), Some(10.0_f32))?,
                Interval::make(Some(1.0_f32), None)?,
                Interval::make(Some(f32::MIN), Some(10.0_f32))?,
            ),
            (
                Interval::make(Some(-0.0_f32), Some(0.0_f32))?,
                Interval::make(Some(f32::MAX), None)?,
                Interval::make(Some(-0.0_f32), Some(0.0_f32))?,
            ),
            (
                Interval::make(Some(-0.0_f32), Some(0.0_f32))?,
                Interval::make(None, Some(-0.0_f32))?,
                Interval::make::<f32>(None, None)?,
            ),
            (
                Interval::make(Some(0.0_f32), Some(0.0_f32))?,
                Interval::make(Some(f32::MAX), None)?,
                Interval::make(Some(0.0_f32), Some(0.0_f32))?,
            ),
            (
                Interval::make(Some(1.0_f32), Some(2.0_f32))?,
                Interval::make(Some(0.0_f32), Some(4.0_f32))?,
                Interval::make(Some(0.25_f32), None)?,
            ),
            (
                Interval::make(Some(1.0_f32), Some(2.0_f32))?,
                Interval::make(Some(-4.0_f32), Some(-0.0_f32))?,
                Interval::make(None, Some(-0.25_f32))?,
            ),
            (
                Interval::make(Some(-4.0_f64), Some(2.0_f64))?,
                Interval::make(Some(10.0_f64), Some(20.0_f64))?,
                Interval::make(Some(-0.4_f64), Some(0.2_f64))?,
            ),
            (
                Interval::make(Some(-0.0_f64), Some(-0.0_f64))?,
                Interval::make(None, Some(-0.0_f64))?,
                Interval::make(Some(0.0_f64), None)?,
            ),
            (
                Interval::make(Some(1.0_f64), Some(2.0_f64))?,
                Interval::make::<f64>(None, None)?,
                Interval::make(Some(0.0_f64), None)?,
            ),
        ];
        for case in cases {
            let result = case.0.div(case.1)?;
            if case.0.data_type().is_floating() {
                assert!(
                    result.lower().is_null() && case.2.lower().is_null()
                        || result.lower().le(case.2.lower())
                );
                assert!(
                    result.upper().is_null() && case.2.upper().is_null()
                        || result.upper().ge(case.2.upper())
                );
            } else {
                assert_eq!(result, case.2);
            }
        }

        Ok(())
    }

    #[test]
    fn test_overflow_handling() -> Result<()> {
        // Test integer overflow handling:
        let dt = DataType::Int32;
        let op = Operator::Plus;
        let lhs = ScalarValue::Int32(Some(i32::MAX));
        let rhs = ScalarValue::Int32(Some(1));
        let result = handle_overflow::<true>(&dt, op, &lhs, &rhs);
        assert_eq!(result, ScalarValue::Int32(None));
        let result = handle_overflow::<false>(&dt, op, &lhs, &rhs);
        assert_eq!(result, ScalarValue::Int32(Some(i32::MAX)));

        // Test float overflow handling:
        let dt = DataType::Float32;
        let op = Operator::Multiply;
        let lhs = ScalarValue::Float32(Some(f32::MAX));
        let rhs = ScalarValue::Float32(Some(2.0));
        let result = handle_overflow::<true>(&dt, op, &lhs, &rhs);
        assert_eq!(result, ScalarValue::Float32(None));
        let result = handle_overflow::<false>(&dt, op, &lhs, &rhs);
        assert_eq!(result, ScalarValue::Float32(Some(f32::MAX)));

        // Test float underflow handling:
        let lhs = ScalarValue::Float32(Some(f32::MIN));
        let rhs = ScalarValue::Float32(Some(2.0));
        let result = handle_overflow::<true>(&dt, op, &lhs, &rhs);
        assert_eq!(result, ScalarValue::Float32(Some(f32::MIN)));
        let result = handle_overflow::<false>(&dt, op, &lhs, &rhs);
        assert_eq!(result, ScalarValue::Float32(None));

        // Test integer underflow handling:
        let dt = DataType::Int64;
        let op = Operator::Minus;
        let lhs = ScalarValue::Int64(Some(i64::MIN));
        let rhs = ScalarValue::Int64(Some(1));
        let result = handle_overflow::<true>(&dt, op, &lhs, &rhs);
        assert_eq!(result, ScalarValue::Int64(Some(i64::MIN)));
        let result = handle_overflow::<false>(&dt, op, &lhs, &rhs);
        assert_eq!(result, ScalarValue::Int64(None));

        // Test unsigned integer handling:
        let dt = DataType::UInt32;
        let op = Operator::Minus;
        let lhs = ScalarValue::UInt32(Some(0));
        let rhs = ScalarValue::UInt32(Some(1));
        let result = handle_overflow::<true>(&dt, op, &lhs, &rhs);
        assert_eq!(result, ScalarValue::UInt32(Some(0)));
        let result = handle_overflow::<false>(&dt, op, &lhs, &rhs);
        assert_eq!(result, ScalarValue::UInt32(None));

        // Test decimal handling:
        let dt = DataType::Decimal128(38, 35);
        let op = Operator::Plus;
        let lhs =
            ScalarValue::Decimal128(Some(54321543215432154321543215432154321), 35, 35);
        let rhs = ScalarValue::Decimal128(Some(10000), 20, 0);
        let result = handle_overflow::<true>(&dt, op, &lhs, &rhs);
        assert_eq!(result, ScalarValue::Decimal128(None, 38, 35));
        let result = handle_overflow::<false>(&dt, op, &lhs, &rhs);
        assert_eq!(
            result,
            ScalarValue::Decimal128(Some(99999999999999999999999999999999999999), 38, 35)
        );

        Ok(())
    }

    #[test]
    fn test_width_of_intervals() -> Result<()> {
        let intervals = [
            (
                Interval::make(Some(0.25_f64), Some(0.50_f64))?,
                ScalarValue::from(0.25_f64),
            ),
            (
                Interval::make(Some(0.5_f64), Some(1.0_f64))?,
                ScalarValue::from(0.5_f64),
            ),
            (
                Interval::make(Some(1.0_f64), Some(2.0_f64))?,
                ScalarValue::from(1.0_f64),
            ),
            (
                Interval::make(Some(32.0_f64), Some(64.0_f64))?,
                ScalarValue::from(32.0_f64),
            ),
            (
                Interval::make(Some(-0.50_f64), Some(-0.25_f64))?,
                ScalarValue::from(0.25_f64),
            ),
            (
                Interval::make(Some(-32.0_f64), Some(-16.0_f64))?,
                ScalarValue::from(16.0_f64),
            ),
            (
                Interval::make(Some(-0.50_f64), Some(0.25_f64))?,
                ScalarValue::from(0.75_f64),
            ),
            (
                Interval::make(Some(-32.0_f64), Some(16.0_f64))?,
                ScalarValue::from(48.0_f64),
            ),
            (
                Interval::make(Some(-32_i64), Some(16_i64))?,
                ScalarValue::from(48_i64),
            ),
        ];
        for (interval, expected) in intervals {
            assert_eq!(interval.width()?, expected);
        }

        Ok(())
    }

    #[test]
    fn test_cardinality_of_intervals() -> Result<()> {
        // In IEEE 754 standard for floating-point arithmetic, if we keep the sign and exponent fields same,
        // we can represent 4503599627370496+1 different numbers by changing the mantissa
        // (4503599627370496 = 2^52, since there are 52 bits in mantissa, and 2^23 = 8388608 for f32).
        // TODO: Add tests for non-exponential boundary aligned intervals too.
        let distinct_f64 = 4503599627370497;
        let distinct_f32 = 8388609;
        let intervals = [
            Interval::make(Some(0.25_f64), Some(0.50_f64))?,
            Interval::make(Some(0.5_f64), Some(1.0_f64))?,
            Interval::make(Some(1.0_f64), Some(2.0_f64))?,
            Interval::make(Some(32.0_f64), Some(64.0_f64))?,
            Interval::make(Some(-0.50_f64), Some(-0.25_f64))?,
            Interval::make(Some(-32.0_f64), Some(-16.0_f64))?,
        ];
        for interval in intervals {
            assert_eq!(interval.cardinality().unwrap(), distinct_f64);
        }

        let intervals = [
            Interval::make(Some(0.25_f32), Some(0.50_f32))?,
            Interval::make(Some(-1_f32), Some(-0.5_f32))?,
        ];
        for interval in intervals {
            assert_eq!(interval.cardinality().unwrap(), distinct_f32);
        }

        // The regular logarithmic distribution of floating-point numbers are
        // only applicable outside of the `(-phi, phi)` interval where `phi`
        // denotes the largest positive subnormal floating-point number. Since
        // the following intervals include such subnormal points, we cannot use
        // a simple powers-of-two type formula for our expectations. Therefore,
        // we manually supply the actual expected cardinality.
        let interval = Interval::make(Some(-0.0625), Some(0.0625))?;
        assert_eq!(interval.cardinality().unwrap(), 9178336040581070850);

        let interval = Interval::try_new(
            ScalarValue::UInt64(Some(u64::MIN + 1)),
            ScalarValue::UInt64(Some(u64::MAX)),
        )?;
        assert_eq!(interval.cardinality().unwrap(), u64::MAX);

        let interval = Interval::try_new(
            ScalarValue::Int64(Some(i64::MIN + 1)),
            ScalarValue::Int64(Some(i64::MAX)),
        )?;
        assert_eq!(interval.cardinality().unwrap(), u64::MAX);

        let interval = Interval::try_new(
            ScalarValue::Float32(Some(-0.0_f32)),
            ScalarValue::Float32(Some(0.0_f32)),
        )?;
        assert_eq!(interval.cardinality().unwrap(), 2);

        Ok(())
    }

    #[test]
    fn test_satisfy_comparison() -> Result<()> {
        let cases = vec![
            (
                Interval::make(Some(1000_i64), None)?,
                Interval::make(None, Some(1000_i64))?,
                true,
                Interval::make(Some(1000_i64), None)?,
                Interval::make(None, Some(1000_i64))?,
            ),
            (
                Interval::make(None, Some(1000_i64))?,
                Interval::make(Some(1000_i64), None)?,
                true,
                Interval::make(Some(1000_i64), Some(1000_i64))?,
                Interval::make(Some(1000_i64), Some(1000_i64))?,
            ),
            (
                Interval::make(Some(1000_i64), None)?,
                Interval::make(None, Some(1000_i64))?,
                false,
                Interval::make(Some(1000_i64), None)?,
                Interval::make(None, Some(1000_i64))?,
            ),
            (
                Interval::make(Some(0_i64), Some(1000_i64))?,
                Interval::make(Some(500_i64), Some(1500_i64))?,
                true,
                Interval::make(Some(500_i64), Some(1000_i64))?,
                Interval::make(Some(500_i64), Some(1000_i64))?,
            ),
            (
                Interval::make(Some(500_i64), Some(1500_i64))?,
                Interval::make(Some(0_i64), Some(1000_i64))?,
                true,
                Interval::make(Some(500_i64), Some(1500_i64))?,
                Interval::make(Some(0_i64), Some(1000_i64))?,
            ),
            (
                Interval::make(Some(0_i64), Some(1000_i64))?,
                Interval::make(Some(500_i64), Some(1500_i64))?,
                false,
                Interval::make(Some(501_i64), Some(1000_i64))?,
                Interval::make(Some(500_i64), Some(999_i64))?,
            ),
            (
                Interval::make(Some(500_i64), Some(1500_i64))?,
                Interval::make(Some(0_i64), Some(1000_i64))?,
                false,
                Interval::make(Some(500_i64), Some(1500_i64))?,
                Interval::make(Some(0_i64), Some(1000_i64))?,
            ),
            (
                Interval::make::<i64>(None, None)?,
                Interval::make(Some(1_i64), Some(1_i64))?,
                false,
                Interval::make(Some(2_i64), None)?,
                Interval::make(Some(1_i64), Some(1_i64))?,
            ),
            (
                Interval::make::<i64>(None, None)?,
                Interval::make(Some(1_i64), Some(1_i64))?,
                true,
                Interval::make(Some(1_i64), None)?,
                Interval::make(Some(1_i64), Some(1_i64))?,
            ),
            (
                Interval::make(Some(1_i64), Some(1_i64))?,
                Interval::make::<i64>(None, None)?,
                false,
                Interval::make(Some(1_i64), Some(1_i64))?,
                Interval::make(None, Some(0_i64))?,
            ),
            (
                Interval::make(Some(1_i64), Some(1_i64))?,
                Interval::make::<i64>(None, None)?,
                true,
                Interval::make(Some(1_i64), Some(1_i64))?,
                Interval::make(None, Some(1_i64))?,
            ),
            (
                Interval::make(Some(1_i64), Some(1_i64))?,
                Interval::make::<i64>(None, None)?,
                false,
                Interval::make(Some(1_i64), Some(1_i64))?,
                Interval::make(None, Some(0_i64))?,
            ),
            (
                Interval::make(Some(1_i64), Some(1_i64))?,
                Interval::make::<i64>(None, None)?,
                true,
                Interval::make(Some(1_i64), Some(1_i64))?,
                Interval::make(None, Some(1_i64))?,
            ),
            (
                Interval::make::<i64>(None, None)?,
                Interval::make(Some(1_i64), Some(1_i64))?,
                false,
                Interval::make(Some(2_i64), None)?,
                Interval::make(Some(1_i64), Some(1_i64))?,
            ),
            (
                Interval::make::<i64>(None, None)?,
                Interval::make(Some(1_i64), Some(1_i64))?,
                true,
                Interval::make(Some(1_i64), None)?,
                Interval::make(Some(1_i64), Some(1_i64))?,
            ),
            (
                Interval::make(Some(-1000.0_f32), Some(1000.0_f32))?,
                Interval::make(Some(-500.0_f32), Some(500.0_f32))?,
                false,
                Interval::try_new(
                    next_value(ScalarValue::Float32(Some(-500.0))),
                    ScalarValue::Float32(Some(1000.0)),
                )?,
                Interval::make(Some(-500_f32), Some(500.0_f32))?,
            ),
            (
                Interval::make(Some(-500.0_f32), Some(500.0_f32))?,
                Interval::make(Some(-1000.0_f32), Some(1000.0_f32))?,
                true,
                Interval::make(Some(-500.0_f32), Some(500.0_f32))?,
                Interval::make(Some(-1000.0_f32), Some(500.0_f32))?,
            ),
            (
                Interval::make(Some(-500.0_f32), Some(500.0_f32))?,
                Interval::make(Some(-1000.0_f32), Some(1000.0_f32))?,
                false,
                Interval::make(Some(-500.0_f32), Some(500.0_f32))?,
                Interval::try_new(
                    ScalarValue::Float32(Some(-1000.0_f32)),
                    prev_value(ScalarValue::Float32(Some(500.0_f32))),
                )?,
            ),
            (
                Interval::make(Some(-1000.0_f64), Some(1000.0_f64))?,
                Interval::make(Some(-500.0_f64), Some(500.0_f64))?,
                true,
                Interval::make(Some(-500.0_f64), Some(1000.0_f64))?,
                Interval::make(Some(-500.0_f64), Some(500.0_f64))?,
            ),
            (
                Interval::make(Some(0_i64), Some(0_i64))?,
                Interval::make(Some(-0_i64), Some(0_i64))?,
                true,
                Interval::make(Some(0_i64), Some(0_i64))?,
                Interval::make(Some(-0_i64), Some(0_i64))?,
            ),
            (
                Interval::make(Some(-0_i64), Some(0_i64))?,
                Interval::make(Some(-0_i64), Some(-0_i64))?,
                true,
                Interval::make(Some(-0_i64), Some(0_i64))?,
                Interval::make(Some(-0_i64), Some(-0_i64))?,
            ),
            (
                Interval::make(Some(0.0_f64), Some(0.0_f64))?,
                Interval::make(Some(-0.0_f64), Some(0.0_f64))?,
                true,
                Interval::make(Some(0.0_f64), Some(0.0_f64))?,
                Interval::make(Some(-0.0_f64), Some(0.0_f64))?,
            ),
            (
                Interval::make(Some(0.0_f64), Some(0.0_f64))?,
                Interval::make(Some(-0.0_f64), Some(0.0_f64))?,
                false,
                Interval::make(Some(0.0_f64), Some(0.0_f64))?,
                Interval::make(Some(-0.0_f64), Some(-0.0_f64))?,
            ),
            (
                Interval::make(Some(-0.0_f64), Some(0.0_f64))?,
                Interval::make(Some(-0.0_f64), Some(-0.0_f64))?,
                true,
                Interval::make(Some(-0.0_f64), Some(0.0_f64))?,
                Interval::make(Some(-0.0_f64), Some(-0.0_f64))?,
            ),
            (
                Interval::make(Some(-0.0_f64), Some(0.0_f64))?,
                Interval::make(Some(-0.0_f64), Some(-0.0_f64))?,
                false,
                Interval::make(Some(0.0_f64), Some(0.0_f64))?,
                Interval::make(Some(-0.0_f64), Some(-0.0_f64))?,
            ),
            (
                Interval::make(Some(0_i64), None)?,
                Interval::make(Some(-0_i64), None)?,
                true,
                Interval::make(Some(0_i64), None)?,
                Interval::make(Some(-0_i64), None)?,
            ),
            (
                Interval::make(Some(0_i64), None)?,
                Interval::make(Some(-0_i64), None)?,
                false,
                Interval::make(Some(1_i64), None)?,
                Interval::make(Some(-0_i64), None)?,
            ),
            (
                Interval::make(Some(0.0_f64), None)?,
                Interval::make(Some(-0.0_f64), None)?,
                true,
                Interval::make(Some(0.0_f64), None)?,
                Interval::make(Some(-0.0_f64), None)?,
            ),
            (
                Interval::make(Some(0.0_f64), None)?,
                Interval::make(Some(-0.0_f64), None)?,
                false,
                Interval::make(Some(0.0_f64), None)?,
                Interval::make(Some(-0.0_f64), None)?,
            ),
        ];
        for (first, second, includes_endpoints, left_modified, right_modified) in cases {
            assert_eq!(
                satisfy_greater(&first, &second, !includes_endpoints)?.unwrap(),
                (left_modified, right_modified)
            );
        }

        let infeasible_cases = vec![
            (
                Interval::make(None, Some(1000_i64))?,
                Interval::make(Some(1000_i64), None)?,
                false,
            ),
            (
                Interval::make(Some(-1000.0_f32), Some(1000.0_f32))?,
                Interval::make(Some(1500.0_f32), Some(2000.0_f32))?,
                false,
            ),
            (
                Interval::make(Some(0_i64), Some(0_i64))?,
                Interval::make(Some(-0_i64), Some(0_i64))?,
                false,
            ),
            (
                Interval::make(Some(-0_i64), Some(0_i64))?,
                Interval::make(Some(-0_i64), Some(-0_i64))?,
                false,
            ),
        ];
        for (first, second, includes_endpoints) in infeasible_cases {
            assert_eq!(satisfy_greater(&first, &second, !includes_endpoints)?, None);
        }

        Ok(())
    }

    #[test]
    fn test_interval_display() {
        let interval = Interval::make(Some(0.25_f32), Some(0.50_f32)).unwrap();
        assert_eq!(format!("{interval}"), "[0.25, 0.5]");

        let interval = Interval::try_new(
            ScalarValue::Float32(Some(f32::NEG_INFINITY)),
            ScalarValue::Float32(Some(f32::INFINITY)),
        )
        .unwrap();
        assert_eq!(format!("{interval}"), "[NULL, NULL]");
    }

    macro_rules! capture_mode_change {
        ($TYPE:ty) => {
            paste::item! {
                capture_mode_change_helper!([<capture_mode_change_ $TYPE>],
                                            [<create_interval_ $TYPE>],
                                            $TYPE);
            }
        };
    }

    macro_rules! capture_mode_change_helper {
        ($TEST_FN_NAME:ident, $CREATE_FN_NAME:ident, $TYPE:ty) => {
            fn $CREATE_FN_NAME(lower: $TYPE, upper: $TYPE) -> Interval {
                Interval::try_new(
                    ScalarValue::try_from(Some(lower as $TYPE)).unwrap(),
                    ScalarValue::try_from(Some(upper as $TYPE)).unwrap(),
                )
                .unwrap()
            }

            fn $TEST_FN_NAME(input: ($TYPE, $TYPE), expect_low: bool, expect_high: bool) {
                assert!(expect_low || expect_high);
                let interval1 = $CREATE_FN_NAME(input.0, input.0);
                let interval2 = $CREATE_FN_NAME(input.1, input.1);
                let result = interval1.add(&interval2).unwrap();
                let without_fe = $CREATE_FN_NAME(input.0 + input.1, input.0 + input.1);
                assert!(
                    (!expect_low || result.lower < without_fe.lower)
                        && (!expect_high || result.upper > without_fe.upper)
                );
            }
        };
    }

    capture_mode_change!(f32);
    capture_mode_change!(f64);

    #[cfg(all(
        any(target_arch = "x86_64", target_arch = "aarch64"),
        not(target_os = "windows")
    ))]
    #[test]
    fn test_add_intervals_lower_affected_f32() {
        // Lower is affected
        let lower = f32::from_bits(1073741887); //1000000000000000000000000111111
        let upper = f32::from_bits(1098907651); //1000001100000000000000000000011
        capture_mode_change_f32((lower, upper), true, false);

        // Upper is affected
        let lower = f32::from_bits(1072693248); //111111111100000000000000000000
        let upper = f32::from_bits(715827883); //101010101010101010101010101011
        capture_mode_change_f32((lower, upper), false, true);

        // Lower is affected
        let lower = 1.0; // 0x3FF0000000000000
        let upper = 0.3; // 0x3FD3333333333333
        capture_mode_change_f64((lower, upper), true, false);

        // Upper is affected
        let lower = 1.4999999999999998; // 0x3FF7FFFFFFFFFFFF
        let upper = 0.000_000_000_000_000_022_044_604_925_031_31; // 0x3C796A6B413BB21F
        capture_mode_change_f64((lower, upper), false, true);
    }

    #[cfg(any(
        not(any(target_arch = "x86_64", target_arch = "aarch64")),
        target_os = "windows"
    ))]
    #[test]
    fn test_next_impl_add_intervals_f64() {
        let lower = 1.5;
        let upper = 1.5;
        capture_mode_change_f64((lower, upper), true, true);

        let lower = 1.5;
        let upper = 1.5;
        capture_mode_change_f32((lower, upper), true, true);
    }

    #[test]
    fn test_is_superset() -> Result<()> {
        // Test cases: (interval1, interval2, strict, expected)
        let test_cases = vec![
            // Equal intervals - non-strict should be true, strict should be false
            (
                Interval::make(Some(10_i32), Some(50_i32))?,
                Interval::make(Some(10_i32), Some(50_i32))?,
                false,
                true,
            ),
            (
                Interval::make(Some(10_i32), Some(50_i32))?,
                Interval::make(Some(10_i32), Some(50_i32))?,
                true,
                false,
            ),
            // Unbounded intervals
            (
                Interval::make::<i32>(None, None)?,
                Interval::make(Some(10_i32), Some(50_i32))?,
                false,
                true,
            ),
            (
                Interval::make::<i32>(None, None)?,
                Interval::make::<i32>(None, None)?,
                false,
                true,
            ),
            (
                Interval::make::<i32>(None, None)?,
                Interval::make::<i32>(None, None)?,
                true,
                false,
            ),
            // Half-bounded intervals
            (
                Interval::make(Some(0_i32), None)?,
                Interval::make(Some(10_i32), Some(50_i32))?,
                false,
                true,
            ),
            (
                Interval::make(None, Some(100_i32))?,
                Interval::make(Some(10_i32), Some(50_i32))?,
                false,
                true,
            ),
            // Non-superset cases - partial overlap
            (
                Interval::make(Some(0_i32), Some(50_i32))?,
                Interval::make(Some(25_i32), Some(75_i32))?,
                false,
                false,
            ),
            (
                Interval::make(Some(0_i32), Some(50_i32))?,
                Interval::make(Some(25_i32), Some(75_i32))?,
                true,
                false,
            ),
            // Non-superset cases - disjoint intervals
            (
                Interval::make(Some(0_i32), Some(50_i32))?,
                Interval::make(Some(60_i32), Some(100_i32))?,
                false,
                false,
            ),
            // Subset relationship (reversed)
            (
                Interval::make(Some(20_i32), Some(80_i32))?,
                Interval::make(Some(0_i32), Some(100_i32))?,
                false,
                false,
            ),
            // Float cases
            (
                Interval::make(Some(0.0_f32), Some(100.0_f32))?,
                Interval::make(Some(25.5_f32), Some(75.5_f32))?,
                false,
                true,
            ),
            (
                Interval::make(Some(0.0_f64), Some(100.0_f64))?,
                Interval::make(Some(0.0_f64), Some(100.0_f64))?,
                true,
                false,
            ),
            // Edge cases with single point intervals
            (
                Interval::make(Some(0_i32), Some(100_i32))?,
                Interval::make(Some(50_i32), Some(50_i32))?,
                false,
                true,
            ),
            (
                Interval::make(Some(50_i32), Some(50_i32))?,
                Interval::make(Some(50_i32), Some(50_i32))?,
                false,
                true,
            ),
            (
                Interval::make(Some(50_i32), Some(50_i32))?,
                Interval::make(Some(50_i32), Some(50_i32))?,
                true,
                false,
            ),
            // Boundary touch cases
            (
                Interval::make(Some(0_i32), Some(50_i32))?,
                Interval::make(Some(0_i32), Some(25_i32))?,
                false,
                true,
            ),
            (
                Interval::make(Some(0_i32), Some(50_i32))?,
                Interval::make(Some(25_i32), Some(50_i32))?,
                false,
                true,
            ),
        ];

        for (interval1, interval2, strict, expected) in test_cases {
            let result = interval1.is_superset(&interval2, strict)?;
            assert_eq!(
                result, expected,
                "Failed for interval1: {interval1}, interval2: {interval2}, strict: {strict}",
            );
        }

        Ok(())
    }

    #[test]
    fn nullable_and_test() -> Result<()> {
        // Test cases: (lhs, rhs, expected) => lhs AND rhs = expected
        #[rustfmt::skip]
        let cases = vec![
            (NullableInterval::TRUE, NullableInterval::TRUE, NullableInterval::TRUE),
            (NullableInterval::TRUE, NullableInterval::FALSE, NullableInterval::FALSE),
            (NullableInterval::TRUE, NullableInterval::UNKNOWN, NullableInterval::UNKNOWN),
            (NullableInterval::TRUE, NullableInterval::TRUE_OR_FALSE, NullableInterval::TRUE_OR_FALSE),
            (NullableInterval::TRUE, NullableInterval::TRUE_OR_UNKNOWN, NullableInterval::TRUE_OR_UNKNOWN),
            (NullableInterval::TRUE, NullableInterval::FALSE_OR_UNKNOWN, NullableInterval::FALSE_OR_UNKNOWN),
            (NullableInterval::TRUE, NullableInterval::ANY_TRUTH_VALUE, NullableInterval::ANY_TRUTH_VALUE),
            (NullableInterval::FALSE, NullableInterval::TRUE, NullableInterval::FALSE),
            (NullableInterval::FALSE, NullableInterval::FALSE, NullableInterval::FALSE),
            (NullableInterval::FALSE, NullableInterval::UNKNOWN, NullableInterval::FALSE),
            (NullableInterval::FALSE, NullableInterval::TRUE_OR_FALSE, NullableInterval::FALSE),
            (NullableInterval::FALSE, NullableInterval::TRUE_OR_UNKNOWN, NullableInterval::FALSE),
            (NullableInterval::FALSE, NullableInterval::FALSE_OR_UNKNOWN, NullableInterval::FALSE),
            (NullableInterval::FALSE, NullableInterval::ANY_TRUTH_VALUE, NullableInterval::FALSE),
            (NullableInterval::UNKNOWN, NullableInterval::TRUE, NullableInterval::UNKNOWN),
            (NullableInterval::UNKNOWN, NullableInterval::FALSE, NullableInterval::FALSE),
            (NullableInterval::UNKNOWN, NullableInterval::UNKNOWN, NullableInterval::UNKNOWN),
            (NullableInterval::UNKNOWN, NullableInterval::TRUE_OR_FALSE, NullableInterval::FALSE_OR_UNKNOWN),
            (NullableInterval::UNKNOWN, NullableInterval::TRUE_OR_UNKNOWN, NullableInterval::UNKNOWN),
            (NullableInterval::UNKNOWN, NullableInterval::FALSE_OR_UNKNOWN, NullableInterval::FALSE_OR_UNKNOWN),
            (NullableInterval::UNKNOWN, NullableInterval::ANY_TRUTH_VALUE, NullableInterval::FALSE_OR_UNKNOWN),
            (NullableInterval::ANY_TRUTH_VALUE, NullableInterval::TRUE, NullableInterval::ANY_TRUTH_VALUE),
            (NullableInterval::ANY_TRUTH_VALUE, NullableInterval::FALSE, NullableInterval::FALSE),
            (NullableInterval::ANY_TRUTH_VALUE, NullableInterval::UNKNOWN, NullableInterval::FALSE_OR_UNKNOWN),
            (NullableInterval::ANY_TRUTH_VALUE, NullableInterval::TRUE_OR_FALSE, NullableInterval::ANY_TRUTH_VALUE),
            (NullableInterval::ANY_TRUTH_VALUE, NullableInterval::TRUE_OR_UNKNOWN, NullableInterval::ANY_TRUTH_VALUE),
            (NullableInterval::ANY_TRUTH_VALUE, NullableInterval::FALSE_OR_UNKNOWN, NullableInterval::FALSE_OR_UNKNOWN),
            (NullableInterval::ANY_TRUTH_VALUE, NullableInterval::ANY_TRUTH_VALUE, NullableInterval::ANY_TRUTH_VALUE),
            (NullableInterval::TRUE_OR_FALSE, NullableInterval::TRUE, NullableInterval::TRUE_OR_FALSE),
            (NullableInterval::TRUE_OR_FALSE, NullableInterval::FALSE, NullableInterval::FALSE),
            (NullableInterval::TRUE_OR_FALSE, NullableInterval::UNKNOWN, NullableInterval::FALSE_OR_UNKNOWN),
            (NullableInterval::TRUE_OR_FALSE, NullableInterval::TRUE_OR_FALSE, NullableInterval::TRUE_OR_FALSE),
            (NullableInterval::TRUE_OR_FALSE, NullableInterval::TRUE_OR_UNKNOWN, NullableInterval::ANY_TRUTH_VALUE),
            (NullableInterval::TRUE_OR_FALSE, NullableInterval::FALSE_OR_UNKNOWN, NullableInterval::FALSE_OR_UNKNOWN),
            (NullableInterval::TRUE_OR_FALSE, NullableInterval::ANY_TRUTH_VALUE, NullableInterval::ANY_TRUTH_VALUE),
            (NullableInterval::TRUE_OR_UNKNOWN, NullableInterval::TRUE, NullableInterval::TRUE_OR_UNKNOWN),
            (NullableInterval::TRUE_OR_UNKNOWN, NullableInterval::FALSE, NullableInterval::FALSE),
            (NullableInterval::TRUE_OR_UNKNOWN, NullableInterval::UNKNOWN, NullableInterval::UNKNOWN),
            (NullableInterval::TRUE_OR_UNKNOWN, NullableInterval::TRUE_OR_FALSE, NullableInterval::ANY_TRUTH_VALUE),
            (NullableInterval::TRUE_OR_UNKNOWN, NullableInterval::TRUE_OR_UNKNOWN, NullableInterval::TRUE_OR_UNKNOWN),
            (NullableInterval::TRUE_OR_UNKNOWN, NullableInterval::FALSE_OR_UNKNOWN, NullableInterval::FALSE_OR_UNKNOWN),
            (NullableInterval::TRUE_OR_UNKNOWN, NullableInterval::ANY_TRUTH_VALUE, NullableInterval::ANY_TRUTH_VALUE),
            (NullableInterval::FALSE_OR_UNKNOWN, NullableInterval::TRUE, NullableInterval::FALSE_OR_UNKNOWN),
            (NullableInterval::FALSE_OR_UNKNOWN, NullableInterval::FALSE, NullableInterval::FALSE),
            (NullableInterval::FALSE_OR_UNKNOWN, NullableInterval::UNKNOWN, NullableInterval::FALSE_OR_UNKNOWN),
            (NullableInterval::FALSE_OR_UNKNOWN, NullableInterval::TRUE_OR_FALSE, NullableInterval::FALSE_OR_UNKNOWN),
            (NullableInterval::FALSE_OR_UNKNOWN, NullableInterval::TRUE_OR_UNKNOWN, NullableInterval::FALSE_OR_UNKNOWN),
            (NullableInterval::FALSE_OR_UNKNOWN, NullableInterval::FALSE_OR_UNKNOWN, NullableInterval::FALSE_OR_UNKNOWN),
            (NullableInterval::FALSE_OR_UNKNOWN, NullableInterval::ANY_TRUTH_VALUE, NullableInterval::FALSE_OR_UNKNOWN),
        ];

        for case in cases {
            assert_eq!(
                case.0.apply_operator(&Operator::And, &case.1).unwrap(),
                case.2,
                "Failed for {} AND {}",
                case.0,
                case.1
            );
        }
        Ok(())
    }

    #[test]
    fn nullable_or_test() -> Result<()> {
        // Test cases: (lhs, rhs, expected) => lhs OR rhs = expected
        #[rustfmt::skip]
        let cases = vec![
            (NullableInterval::TRUE, NullableInterval::TRUE, NullableInterval::TRUE),
            (NullableInterval::TRUE, NullableInterval::FALSE, NullableInterval::TRUE),
            (NullableInterval::TRUE, NullableInterval::UNKNOWN, NullableInterval::TRUE),
            (NullableInterval::TRUE, NullableInterval::TRUE_OR_FALSE, NullableInterval::TRUE),
            (NullableInterval::TRUE, NullableInterval::TRUE_OR_UNKNOWN, NullableInterval::TRUE),
            (NullableInterval::TRUE, NullableInterval::FALSE_OR_UNKNOWN, NullableInterval::TRUE),
            (NullableInterval::TRUE, NullableInterval::ANY_TRUTH_VALUE, NullableInterval::TRUE),
            (NullableInterval::FALSE, NullableInterval::TRUE, NullableInterval::TRUE),
            (NullableInterval::FALSE, NullableInterval::FALSE, NullableInterval::FALSE),
            (NullableInterval::FALSE, NullableInterval::UNKNOWN, NullableInterval::UNKNOWN),
            (NullableInterval::FALSE, NullableInterval::TRUE_OR_FALSE, NullableInterval::TRUE_OR_FALSE),
            (NullableInterval::FALSE, NullableInterval::TRUE_OR_UNKNOWN, NullableInterval::TRUE_OR_UNKNOWN),
            (NullableInterval::FALSE, NullableInterval::FALSE_OR_UNKNOWN, NullableInterval::FALSE_OR_UNKNOWN),
            (NullableInterval::FALSE, NullableInterval::ANY_TRUTH_VALUE, NullableInterval::ANY_TRUTH_VALUE),
            (NullableInterval::UNKNOWN, NullableInterval::TRUE, NullableInterval::TRUE),
            (NullableInterval::UNKNOWN, NullableInterval::FALSE, NullableInterval::UNKNOWN),
            (NullableInterval::UNKNOWN, NullableInterval::UNKNOWN, NullableInterval::UNKNOWN),
            (NullableInterval::UNKNOWN, NullableInterval::TRUE_OR_FALSE, NullableInterval::TRUE_OR_UNKNOWN),
            (NullableInterval::UNKNOWN, NullableInterval::TRUE_OR_UNKNOWN, NullableInterval::TRUE_OR_UNKNOWN),
            (NullableInterval::UNKNOWN, NullableInterval::FALSE_OR_UNKNOWN, NullableInterval::UNKNOWN),
            (NullableInterval::UNKNOWN, NullableInterval::ANY_TRUTH_VALUE, NullableInterval::TRUE_OR_UNKNOWN),
            (NullableInterval::ANY_TRUTH_VALUE, NullableInterval::TRUE, NullableInterval::TRUE),
            (NullableInterval::ANY_TRUTH_VALUE, NullableInterval::FALSE, NullableInterval::ANY_TRUTH_VALUE),
            (NullableInterval::ANY_TRUTH_VALUE, NullableInterval::UNKNOWN, NullableInterval::TRUE_OR_UNKNOWN),
            (NullableInterval::ANY_TRUTH_VALUE, NullableInterval::TRUE_OR_FALSE, NullableInterval::ANY_TRUTH_VALUE),
            (NullableInterval::ANY_TRUTH_VALUE, NullableInterval::TRUE_OR_UNKNOWN, NullableInterval::TRUE_OR_UNKNOWN),
            (NullableInterval::ANY_TRUTH_VALUE, NullableInterval::FALSE_OR_UNKNOWN, NullableInterval::ANY_TRUTH_VALUE),
            (NullableInterval::ANY_TRUTH_VALUE, NullableInterval::ANY_TRUTH_VALUE, NullableInterval::ANY_TRUTH_VALUE),
            (NullableInterval::TRUE_OR_FALSE, NullableInterval::TRUE, NullableInterval::TRUE),
            (NullableInterval::TRUE_OR_FALSE, NullableInterval::FALSE, NullableInterval::TRUE_OR_FALSE),
            (NullableInterval::TRUE_OR_FALSE, NullableInterval::UNKNOWN, NullableInterval::TRUE_OR_UNKNOWN),
            (NullableInterval::TRUE_OR_FALSE, NullableInterval::TRUE_OR_FALSE, NullableInterval::TRUE_OR_FALSE),
            (NullableInterval::TRUE_OR_FALSE, NullableInterval::TRUE_OR_UNKNOWN, NullableInterval::TRUE_OR_UNKNOWN),
            (NullableInterval::TRUE_OR_FALSE, NullableInterval::FALSE_OR_UNKNOWN, NullableInterval::ANY_TRUTH_VALUE),
            (NullableInterval::TRUE_OR_FALSE, NullableInterval::ANY_TRUTH_VALUE, NullableInterval::ANY_TRUTH_VALUE),
            (NullableInterval::TRUE_OR_UNKNOWN, NullableInterval::TRUE, NullableInterval::TRUE),
            (NullableInterval::TRUE_OR_UNKNOWN, NullableInterval::FALSE, NullableInterval::TRUE_OR_UNKNOWN),
            (NullableInterval::TRUE_OR_UNKNOWN, NullableInterval::UNKNOWN, NullableInterval::TRUE_OR_UNKNOWN),
            (NullableInterval::TRUE_OR_UNKNOWN, NullableInterval::TRUE_OR_FALSE, NullableInterval::TRUE_OR_UNKNOWN),
            (NullableInterval::TRUE_OR_UNKNOWN, NullableInterval::TRUE_OR_UNKNOWN, NullableInterval::TRUE_OR_UNKNOWN),
            (NullableInterval::TRUE_OR_UNKNOWN, NullableInterval::FALSE_OR_UNKNOWN, NullableInterval::TRUE_OR_UNKNOWN),
            (NullableInterval::TRUE_OR_UNKNOWN, NullableInterval::ANY_TRUTH_VALUE, NullableInterval::TRUE_OR_UNKNOWN),
            (NullableInterval::FALSE_OR_UNKNOWN, NullableInterval::TRUE, NullableInterval::TRUE),
            (NullableInterval::FALSE_OR_UNKNOWN, NullableInterval::FALSE, NullableInterval::FALSE_OR_UNKNOWN),
            (NullableInterval::FALSE_OR_UNKNOWN, NullableInterval::UNKNOWN, NullableInterval::UNKNOWN),
            (NullableInterval::FALSE_OR_UNKNOWN, NullableInterval::TRUE_OR_FALSE, NullableInterval::ANY_TRUTH_VALUE),
            (NullableInterval::FALSE_OR_UNKNOWN, NullableInterval::TRUE_OR_UNKNOWN, NullableInterval::TRUE_OR_UNKNOWN),
            (NullableInterval::FALSE_OR_UNKNOWN, NullableInterval::FALSE_OR_UNKNOWN, NullableInterval::FALSE_OR_UNKNOWN),
            (NullableInterval::FALSE_OR_UNKNOWN, NullableInterval::ANY_TRUTH_VALUE, NullableInterval::ANY_TRUTH_VALUE),
        ];

        for case in cases {
            assert_eq!(
                case.0.apply_operator(&Operator::Or, &case.1).unwrap(),
                case.2,
                "Failed for {} OR {}",
                case.0,
                case.1
            );
        }
        Ok(())
    }

    #[test]
    fn nullable_not_test() -> Result<()> {
        // Test cases: (interval, expected) => NOT interval = expected
        #[rustfmt::skip]
        let cases = vec![
            (NullableInterval::TRUE, NullableInterval::FALSE),
            (NullableInterval::FALSE, NullableInterval::TRUE),
            (NullableInterval::UNKNOWN, NullableInterval::UNKNOWN),
            (NullableInterval::TRUE_OR_FALSE,NullableInterval::TRUE_OR_FALSE),
            (NullableInterval::TRUE_OR_UNKNOWN,NullableInterval::FALSE_OR_UNKNOWN),
            (NullableInterval::FALSE_OR_UNKNOWN,NullableInterval::TRUE_OR_UNKNOWN),
            (NullableInterval::ANY_TRUTH_VALUE, NullableInterval::ANY_TRUTH_VALUE),
        ];

        for case in cases {
            assert_eq!(case.0.not().unwrap(), case.1, "Failed for NOT {}", case.0,);
        }
        Ok(())
    }

    #[test]
    fn nullable_interval_is_certainly_true() {
        // Test cases: (interval, expected) => interval.is_certainly_true() = expected
        #[rustfmt::skip]
        let test_cases = vec![
            (NullableInterval::TRUE, true),
            (NullableInterval::FALSE, false),
            (NullableInterval::UNKNOWN, false),
            (NullableInterval::TRUE_OR_FALSE, false),
            (NullableInterval::TRUE_OR_UNKNOWN, false),
            (NullableInterval::FALSE_OR_UNKNOWN, false),
            (NullableInterval::ANY_TRUTH_VALUE, false),
        ];

        for (interval, expected) in test_cases {
            let result = interval.is_certainly_true();
            assert_eq!(result, expected, "Failed for interval: {interval}",);
        }
    }

    #[test]
    fn nullable_interval_is_true() {
        // Test cases: (interval, expected) => interval.is_true() = expected
        #[rustfmt::skip]
        let test_cases = vec![
            (NullableInterval::TRUE, NullableInterval::TRUE),
            (NullableInterval::FALSE, NullableInterval::FALSE),
            (NullableInterval::UNKNOWN, NullableInterval::FALSE),
            (NullableInterval::TRUE_OR_FALSE,NullableInterval::TRUE_OR_FALSE),
            (NullableInterval::TRUE_OR_UNKNOWN,NullableInterval::TRUE_OR_FALSE),
            (NullableInterval::FALSE_OR_UNKNOWN, NullableInterval::FALSE),
            (NullableInterval::ANY_TRUTH_VALUE,NullableInterval::TRUE_OR_FALSE),
        ];

        for (interval, expected) in test_cases {
            let result = interval.is_true().unwrap();
            assert_eq!(result, expected, "Failed for interval: {interval}",);
        }
    }

    #[test]
    fn nullable_interval_is_certainly_false() {
        // Test cases: (interval, expected) => interval.is_certainly_false() = expected
        #[rustfmt::skip]
        let test_cases = vec![
            (NullableInterval::TRUE, false),
            (NullableInterval::FALSE, true),
            (NullableInterval::UNKNOWN, false),
            (NullableInterval::TRUE_OR_FALSE, false),
            (NullableInterval::TRUE_OR_UNKNOWN, false),
            (NullableInterval::FALSE_OR_UNKNOWN, false),
            (NullableInterval::ANY_TRUTH_VALUE, false),
        ];

        for (interval, expected) in test_cases {
            let result = interval.is_certainly_false();
            assert_eq!(result, expected, "Failed for interval: {interval}",);
        }
    }

    #[test]
    fn nullable_interval_is_false() {
        // Test cases: (interval, expected) => interval.is_false() = expected
        #[rustfmt::skip]
        let test_cases = vec![
            (NullableInterval::TRUE, NullableInterval::FALSE),
            (NullableInterval::FALSE, NullableInterval::TRUE),
            (NullableInterval::UNKNOWN, NullableInterval::FALSE),
            (NullableInterval::TRUE_OR_FALSE,NullableInterval::TRUE_OR_FALSE),
            (NullableInterval::TRUE_OR_UNKNOWN, NullableInterval::FALSE),
            (NullableInterval::FALSE_OR_UNKNOWN,NullableInterval::TRUE_OR_FALSE),
            (NullableInterval::ANY_TRUTH_VALUE,NullableInterval::TRUE_OR_FALSE),
        ];

        for (interval, expected) in test_cases {
            let result = interval.is_false().unwrap();
            assert_eq!(result, expected, "Failed for interval: {interval}",);
        }
    }

    #[test]
    fn nullable_interval_is_certainly_unknown() {
        // Test cases: (interval, expected) => interval.is_certainly_unknown() = expected
        #[rustfmt::skip]
        let test_cases = vec![
            (NullableInterval::TRUE, false),
            (NullableInterval::FALSE, false),
            (NullableInterval::UNKNOWN, true),
            (NullableInterval::TRUE_OR_FALSE, false),
            (NullableInterval::TRUE_OR_UNKNOWN, false),
            (NullableInterval::FALSE_OR_UNKNOWN, false),
            (NullableInterval::ANY_TRUTH_VALUE, false),
        ];

        for (interval, expected) in test_cases {
            let result = interval.is_certainly_unknown();
            assert_eq!(result, expected, "Failed for interval: {interval}",);
        }
    }

    #[test]
    fn nullable_interval_is_unknown() {
        // Test cases: (interval, expected) => interval.is_unknown() = expected
        #[rustfmt::skip]
        let test_cases = vec![
            (NullableInterval::TRUE, NullableInterval::FALSE),
            (NullableInterval::FALSE, NullableInterval::FALSE),
            (NullableInterval::UNKNOWN, NullableInterval::TRUE),
            (NullableInterval::TRUE_OR_FALSE, NullableInterval::FALSE),
            (NullableInterval::TRUE_OR_UNKNOWN,NullableInterval::TRUE_OR_FALSE),
            (NullableInterval::FALSE_OR_UNKNOWN,NullableInterval::TRUE_OR_FALSE),
            (NullableInterval::ANY_TRUTH_VALUE,NullableInterval::TRUE_OR_FALSE),
        ];

        for (interval, expected) in test_cases {
            let result = interval.is_unknown().unwrap();
            assert_eq!(result, expected, "Failed for interval: {interval}",);
        }
    }

    #[test]
    fn nullable_interval_contains_value() {
        // Test cases: (interval, value, expected) => interval.contains_value(value) = expected
        #[rustfmt::skip]
        let test_cases = vec![
            (NullableInterval::TRUE, ScalarValue::Boolean(Some(true)), true),
            (NullableInterval::TRUE, ScalarValue::Boolean(Some(false)), false),
            (NullableInterval::TRUE, ScalarValue::Boolean(None), false),
            (NullableInterval::TRUE, ScalarValue::Null, false),
            (NullableInterval::TRUE, ScalarValue::UInt32(None), false),
            (NullableInterval::FALSE, ScalarValue::Boolean(Some(true)), false),
            (NullableInterval::FALSE, ScalarValue::Boolean(Some(false)), true),
            (NullableInterval::FALSE, ScalarValue::Boolean(None), false),
            (NullableInterval::FALSE, ScalarValue::Null, false),
            (NullableInterval::FALSE, ScalarValue::UInt32(None), false),
            (NullableInterval::UNKNOWN, ScalarValue::Boolean(Some(true)), false),
            (NullableInterval::UNKNOWN, ScalarValue::Boolean(Some(false)), false),
            (NullableInterval::UNKNOWN, ScalarValue::Boolean(None), true),
            (NullableInterval::UNKNOWN, ScalarValue::Null, true),
            (NullableInterval::UNKNOWN, ScalarValue::UInt32(None), false),
            (NullableInterval::TRUE_OR_FALSE, ScalarValue::Boolean(Some(true)), true),
            (NullableInterval::TRUE_OR_FALSE, ScalarValue::Boolean(Some(false)), true),
            (NullableInterval::TRUE_OR_FALSE, ScalarValue::Boolean(None), false),
            (NullableInterval::TRUE_OR_FALSE, ScalarValue::Null, false),
            (NullableInterval::TRUE_OR_FALSE, ScalarValue::UInt32(None), false),
            (NullableInterval::TRUE_OR_UNKNOWN, ScalarValue::Boolean(Some(true)), true),
            (NullableInterval::TRUE_OR_UNKNOWN, ScalarValue::Boolean(Some(false)), false),
            (NullableInterval::TRUE_OR_UNKNOWN, ScalarValue::Boolean(None), true),
            (NullableInterval::TRUE_OR_UNKNOWN, ScalarValue::Null, true),
            (NullableInterval::TRUE_OR_UNKNOWN, ScalarValue::UInt32(None), false),
            (NullableInterval::FALSE_OR_UNKNOWN, ScalarValue::Boolean(Some(true)), false),
            (NullableInterval::FALSE_OR_UNKNOWN, ScalarValue::Boolean(Some(false)), true),
            (NullableInterval::FALSE_OR_UNKNOWN, ScalarValue::Boolean(None), true),
            (NullableInterval::FALSE_OR_UNKNOWN, ScalarValue::Null, true),
            (NullableInterval::FALSE_OR_UNKNOWN, ScalarValue::UInt32(None), false),
            (NullableInterval::ANY_TRUTH_VALUE, ScalarValue::Boolean(Some(true)), true),
            (NullableInterval::ANY_TRUTH_VALUE, ScalarValue::Boolean(Some(false)), true),
            (NullableInterval::ANY_TRUTH_VALUE, ScalarValue::Boolean(None), true),
            (NullableInterval::ANY_TRUTH_VALUE, ScalarValue::Null, true),
            (NullableInterval::ANY_TRUTH_VALUE, ScalarValue::UInt32(None), false),
        ];

        for (interval, value, expected) in test_cases {
            let result = interval.contains_value(value.clone()).unwrap();
            assert_eq!(
                result, expected,
                "Failed for interval: {interval} and value {value:?}",
            );
        }
    }
}
