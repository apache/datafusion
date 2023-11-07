// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Interval arithmetic library

use crate::type_coercion::binary::get_result_type;
use crate::Operator;
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::DataType;
use arrow_array::ArrowNativeTypeOp;
use datafusion_common::rounding::{alter_fp_rounding_mode, next_down, next_up};
use datafusion_common::{internal_err, DataFusionError, Result, ScalarValue};

use std::borrow::Borrow;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::ops::{AddAssign, SubAssign};

/// The `Interval` type represents a closed interval used for computing
/// reliable bounds for mathematical expressions.
///
/// Features:
///
/// 1. **Closed Bounds**: The interval always encompasses its endpoints.
///
/// 2. **Unbounded Endpoints**: If the `lower` or `upper` bounds are indeterminate,
///    they are labeled as *unbounded*. This is represented using a `NULL`.
///
/// 3. **Overflow Handling**: If after certain operations, the `lower` or `upper`
///    bounds exceed their limits, they either become unbounded or they are fixed to
///    the maximum/minimum value of the datatype, depending on the direction of the
///    bound overflowed, opting for the safer choice. The system treats overflowed
///    bounds the same as unbounded ones.
///  
/// 4. **Float Special Cases**: For floating-point numbers:
///    - `inf` values are converted to `NULL` to ensure consistency, as floats have
///      their own maximum values.
///    - NaN (Not a Number) values are interpreted as unbounded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Interval {
    lower: ScalarValue,
    upper: ScalarValue,
}

macro_rules! handle_float_intervals {
    ($variant:ident, $primitive_type:ident, $lower:expr, $upper:expr) => {{
        let lower = match $lower {
            ScalarValue::$variant(Some(l_val))
                if l_val == $primitive_type::NEG_INFINITY || l_val.is_nan() =>
            {
                ScalarValue::$variant(None)
            }
            ScalarValue::$variant(Some(l_val)) if l_val == $primitive_type::INFINITY => {
                return internal_err!("Interval lower value cannot be positive infinity");
            }
            ScalarValue::$variant(Some(l_val)) => ScalarValue::$variant(Some(l_val)),
            _ => ScalarValue::$variant(None),
        };

        let upper = match $upper {
            ScalarValue::$variant(Some(r_val))
                if r_val == $primitive_type::INFINITY || r_val.is_nan() =>
            {
                ScalarValue::$variant(None)
            }
            ScalarValue::$variant(Some(r_val))
                if r_val == $primitive_type::NEG_INFINITY =>
            {
                return internal_err!("Interval upper value cannot be negative infinity");
            }
            ScalarValue::$variant(Some(r_val)) => ScalarValue::$variant(Some(r_val)),
            _ => ScalarValue::$variant(None),
        };

        Interval { lower, upper }
    }};
}

impl Interval {
    /// Attempts to create a new `Interval` from the given lower and upper bounds.
    ///
    /// # Notes
    ///
    /// - **Boolean Intervals**:
    ///   - Unboundedness (`NULL`) for boolean values is interpreted as `false` for lower and `true` for upper bounds.
    ///
    /// - **Floating Point Intervals**:
    ///   - Floating-point values with `NaN`, `Inf`, or `-Inf` are represented as `NULL` within the interval.
    ///   - An error is returned if the lower bound is positive infinity or if the upper bound is negative infinity.
    pub fn try_new(lower: ScalarValue, upper: ScalarValue) -> Result<Interval> {
        if lower.data_type() != upper.data_type() {
            return internal_err!(
                "Interval bounds must be in the same datatype to create a valid Interval"
            );
        }

        let interval = if let ScalarValue::Boolean(_) = lower {
            // Handle boolean type:
            let standardized_lower = match lower {
                ScalarValue::Boolean(None) => ScalarValue::Boolean(Some(false)),
                _ => lower.clone(),
            };
            let standardized_upper: ScalarValue = match upper {
                ScalarValue::Boolean(None) => ScalarValue::Boolean(Some(true)),
                _ => upper.clone(),
            };
            Interval {
                lower: standardized_lower,
                upper: standardized_upper,
            }
        }
        // Handle risky floating point values:
        else if let ScalarValue::Float32(Some(_)) = lower {
            handle_float_intervals!(Float32, f32, lower, upper)
        } else if let ScalarValue::Float64(Some(_)) = lower {
            handle_float_intervals!(Float64, f64, lower, upper)
        } else {
            // Others:
            Interval {
                lower: lower.clone(),
                upper: upper.clone(),
            }
        };

        if interval.lower <= interval.upper
            || interval.upper.is_null()
            || interval.lower.is_null()
        {
            Ok(interval)
        } else {
            Err(DataFusionError::Internal(format!(
                "Interval's lower bound {:?} is greater than the upper bound {:?}",
                lower, upper
            )))
        }
    }

    /// Creates an unbounded interval from both sides if the datatype supported.
    pub fn make_unbounded(data_type: &DataType) -> Result<Interval> {
        let inf = ScalarValue::try_from(data_type)?;
        Interval::try_new(inf.clone(), inf)
    }

    pub fn lower(&self) -> &ScalarValue {
        &self.lower
    }

    pub fn upper(&self) -> &ScalarValue {
        &self.upper
    }

    /// This function returns the data type of this interval.
    pub fn get_datatype(&self) -> DataType {
        let lower_type = self.lower.data_type();
        let upper_type = self.upper.data_type();

        // There must be no way to create an interval having different types.
        assert!(
            lower_type == upper_type,
            "Interval bounds have different types: {lower_type} != {upper_type}"
        );
        lower_type
    }

    /// Casts this interval to `data_type` using `cast_options`.
    pub fn cast_to(
        &self,
        data_type: &DataType,
        cast_options: &CastOptions,
    ) -> Result<Interval> {
        Interval::try_new(
            cast_scalar_value(&self.lower, data_type, cast_options)?,
            cast_scalar_value(&self.lower, data_type, cast_options)?,
        )
    }

    pub const CERTAINLY_FALSE: Interval = Interval {
        lower: ScalarValue::Boolean(Some(false)),
        upper: ScalarValue::Boolean(Some(false)),
    };

    pub const UNCERTAIN: Interval = Interval {
        lower: ScalarValue::Boolean(Some(false)),
        upper: ScalarValue::Boolean(Some(true)),
    };

    pub const CERTAINLY_TRUE: Interval = Interval {
        lower: ScalarValue::Boolean(Some(true)),
        upper: ScalarValue::Boolean(Some(true)),
    };

    /// Decide if this interval is certainly greater than, possibly greater than,
    /// or can't be greater than `other` by returning [true, true],
    /// [false, true] or [false, false] respectively.
    pub(crate) fn gt<T: Borrow<Interval>>(&self, other: T) -> Result<Interval> {
        let rhs = other.borrow();
        if self.get_datatype() != rhs.get_datatype() {
            return internal_err!("Intervals must be in the same type to be compared");
        }

        let flags = if !(self.upper.is_null() || rhs.lower.is_null())
            && self.upper <= rhs.lower
        {
            // Values in this interval are certainly less than or
            // equal to those in the given interval.
            (false, false)
        } else if !(self.lower.is_null() || rhs.upper.is_null())
            && (self.lower > rhs.upper)
        {
            // Values in this interval are certainly greater
            // than those in the  given interval.
            (true, true)
        } else {
            // All outcomes are possible.
            (false, true)
        };

        Ok(Interval {
            lower: ScalarValue::from(flags.0),
            upper: ScalarValue::from(flags.1),
        })
    }

    /// Decide if this interval is certainly greater than or equal to, possibly greater than
    /// or equal to, or can't be greater than or equal to `other` by returning [true, true],
    /// [false, true] or [false, false] respectively.
    pub(crate) fn gt_eq<T: Borrow<Interval>>(&self, other: T) -> Result<Interval> {
        let rhs = other.borrow();
        if self.get_datatype() != rhs.get_datatype() {
            return internal_err!("Intervals must be in the same type to be compared");
        };

        let flags = if !(self.lower.is_null() || rhs.upper.is_null())
            && self.lower >= rhs.upper
        {
            // Values in this interval are certainly greater than or
            // equal to those in the given interval.
            (true, true)
        } else if !(self.upper.is_null() || rhs.lower.is_null())
            && (self.upper < rhs.lower)
        {
            // Values in this interval are certainly less
            // than those in the given interval.
            (false, false)
        } else {
            // All outcomes are possible.
            (false, true)
        };

        Ok(Interval {
            lower: ScalarValue::from(flags.0),
            upper: ScalarValue::from(flags.1),
        })
    }

    /// Decide if this interval is certainly less than, possibly less than,
    /// or can't be less than `other` by returning [true, true],
    /// [false, true] or [false, false] respectively.
    pub(crate) fn lt<T: Borrow<Interval>>(&self, other: T) -> Result<Interval> {
        other.borrow().gt(self)
    }

    /// Decide if this interval is certainly less than or equal to, possibly
    /// less than or equal to, or can't be less than or equal to `other` by returning
    /// [true, true], [false, true] or [false, false] respectively.
    pub(crate) fn lt_eq<T: Borrow<Interval>>(&self, other: T) -> Result<Interval> {
        other.borrow().gt_eq(self)
    }

    /// Decide if this interval is certainly equal to, possibly equal to,
    /// or can't be equal to `other` by returning [true, true],
    /// [false, true] or [false, false] respectively.
    pub(crate) fn equal<T: Borrow<Interval>>(&self, other: T) -> Result<Interval> {
        let rhs = other.borrow();
        if self.get_datatype() != rhs.get_datatype() {
            return internal_err!(
                "Intervals must be in the same type to be checked equality"
            );
        };

        let flags = if !self.lower.is_null()
            && (self.lower == self.upper)
            && (rhs.lower == rhs.upper)
            && (self.lower == rhs.lower)
        {
            (true, true)
        } else if self.intersect(rhs)?.is_none() {
            (false, false)
        } else {
            (false, true)
        };

        Ok(Interval {
            lower: ScalarValue::from(flags.0),
            upper: ScalarValue::from(flags.1),
        })
    }

    /// Compute the logical conjunction of this (boolean) interval with the given boolean interval.
    pub(crate) fn and<T: Borrow<Interval>>(&self, other: T) -> Result<Interval> {
        let rhs = other.borrow();
        match (&self.lower, &self.upper, &rhs.lower, &rhs.upper) {
            (
                ScalarValue::Boolean(Some(self_lower)),
                ScalarValue::Boolean(Some(self_upper)),
                ScalarValue::Boolean(Some(other_lower)),
                ScalarValue::Boolean(Some(other_upper)),
            ) => {
                let lower = *self_lower && *other_lower;
                let upper = *self_upper && *other_upper;

                Ok(Interval {
                    lower: ScalarValue::Boolean(Some(lower)),
                    upper: ScalarValue::Boolean(Some(upper)),
                })
            }
            _ => internal_err!("Incompatible types for logical AND operator"),
        }
    }

    /// Compute the logical negation of this (boolean) interval.
    pub(crate) fn not(&self) -> Result<Self> {
        if self.get_datatype().ne(&DataType::Boolean) {
            return internal_err!(
                "Cannot apply logical negation to non-boolean interval"
            );
        }
        if self == &Interval::CERTAINLY_TRUE {
            Ok(Interval::CERTAINLY_FALSE)
        } else if self == &Interval::CERTAINLY_FALSE {
            Ok(Interval::CERTAINLY_TRUE)
        } else {
            Ok(Interval::UNCERTAIN)
        }
    }

    /// Compute the intersection of the interval with the given interval.
    /// If the intersection is empty, return None.
    pub fn intersect<T: Borrow<Interval>>(&self, other: T) -> Result<Option<Interval>> {
        let rhs = other.borrow();
        if self.get_datatype() != rhs.get_datatype() {
            return internal_err!("Intervals must be in the same type to be intersected");
        };

        // If it is evident that the result is an empty interval,
        // do not make any calculation and directly return None.
        if (!(self.lower.is_null() || rhs.upper.is_null()) && self.lower > rhs.upper)
            || (!(self.upper.is_null() || rhs.lower.is_null()) && self.upper < rhs.lower)
        {
            // This None value signals an empty interval.
            return Ok(None);
        }

        let lower = max_of_bounds(&self.lower, &rhs.lower);
        let upper = min_of_bounds(&self.upper, &rhs.upper);

        // New lower and upper bounds must always construct a valid interval.
        assert!(
            (lower.is_null() || upper.is_null() || (lower <= upper)),
            "Result of the intersection of two intervals cannot be an invalid interval"
        );

        Ok(Some(Interval { lower, upper }))
    }

    /// Decide if this interval is certainly contains, possibly contains,
    /// or can't contain `other` by returning [true, true],
    /// [false, true] or [false, false] respectively.
    pub fn contains<T: Borrow<Self>>(&self, other: T) -> Result<Self> {
        let rhs = other.borrow();
        if self.get_datatype() != rhs.get_datatype() {
            return internal_err!(
                "Intervals must be in the same type to check containment"
            );
        };

        Ok(match self.intersect(other.borrow())? {
            Some(intersection) => {
                if &intersection == other.borrow() {
                    Interval::CERTAINLY_TRUE
                } else {
                    Interval::UNCERTAIN
                }
            }
            None => Interval::CERTAINLY_FALSE,
        })
    }

    /// Add the given interval (`other`) to this interval. Say we have
    /// intervals [a1, b1] and [a2, b2], then their sum is [a1 + a2, b1 + b2].
    /// Note that this represents all possible values the sum can take if
    /// one can choose single values arbitrarily from each of the operands.
    pub fn add<T: Borrow<Interval>>(&self, other: T) -> Result<Interval> {
        let rhs = other.borrow();

        Interval::try_new(
            add_bounds::<false>(&self.lower, &rhs.lower)?,
            add_bounds::<true>(&self.upper, &rhs.upper)?,
        )
    }

    /// Subtract the given interval (`other`) from this interval. Say we have
    /// intervals [a1, b1] and [a2, b2], then their sum is [a1 - b2, b1 - a2].
    /// Note that this represents all possible values the difference can take
    /// if one can choose single values arbitrarily from each of the operands.
    pub fn sub<T: Borrow<Interval>>(&self, other: T) -> Result<Interval> {
        let rhs = other.borrow();

        Interval::try_new(
            sub_bounds::<false>(&self.lower, &rhs.upper)?,
            sub_bounds::<true>(&self.upper, &rhs.lower)?,
        )
    }

    /// Multiply the given interval (`other`) with this interval. Say we have
    /// intervals [a1, b1] and [a2, b2], then their multiplication is
    /// [min(a1*a2, a1*b2, b1*a2, b1*b2), max(a1*a2, a1*b2, b1*a2, b1*b2)].
    /// Note that this represents all possible values the multiplication can take
    /// if one can choose single values arbitrarily from each of the operands.
    pub fn mul<T: Borrow<Interval>>(&self, other: T) -> Result<Interval> {
        let rhs = other.borrow();

        let zero_point = Interval::try_new(
            ScalarValue::new_zero(&self.get_datatype())?,
            ScalarValue::new_zero(&self.get_datatype())?,
        )?;
        match (
            Interval::CERTAINLY_TRUE == self.contains(&zero_point)?,
            Interval::CERTAINLY_TRUE == rhs.contains(&zero_point)?,
        ) {
            // Since the parameter of contains function is a singleton,
            // it is impossible to have an UNCERTAIN case.
            (true, true) => mul_helper_multi_zero_inclusive(self, rhs),
            // only option is CERTAINLY_FALSE for rhs
            (true, false) => mul_helper_single_zero_inclusive(self, rhs, &zero_point),
            // only option is CERTAINLY_FALSE for lhs
            (false, true) => mul_helper_single_zero_inclusive(rhs, self, &zero_point),
            // only option is CERTAINLY_FALSE for both sides
            (false, false) => mul_helper_zero_exclusive(self, rhs, &zero_point),
        }
    }

    /// Divide this interval by the given interval (`other`). Say we have
    /// intervals [a1, b1] and [a2, b2], then their division is
    /// [a1, b1]*[1/b2, 1/a2] if 0 ∉ [a2, b2], [-Inf, +Inf] otherwise.
    /// Note that this represents all possible values the division can take
    /// if one can choose single values arbitrarily from each of the operands.
    /// TODO: Once the interval sets are supported, cases where divisor contains 0
    /// give an interval set, not the all space.
    pub fn div<T: Borrow<Interval>>(&self, other: T) -> Result<Interval> {
        let rhs = other.borrow();

        let zero = ScalarValue::new_zero(&self.get_datatype())?;
        let zero_point = Interval::try_new(zero.clone(), zero.clone())?;

        if rhs.contains(&zero_point)? == Interval::CERTAINLY_TRUE
        // The purpose is early exit with unbounded interval if the interval
        // is in the form of [some_negative, some_positive] or [0,0].
            && !(rhs.lower.eq(&zero) ^ rhs.upper.eq(&zero))
        {
            return Interval::make_unbounded(&get_result_type(
                &self.get_datatype(),
                &Operator::Divide,
                &rhs.get_datatype(),
            )?);
        }

        // 1) Since the parameter of contains function is a singleton,
        // it is impossible to have an UNCERTAIN case.
        // 2) We handle the case of rhs reaches 0 from both negative
        // and positive directions, so rhs can only contain zero as
        // edge point of the bound.
        if Interval::CERTAINLY_TRUE == self.contains(&zero_point)? {
            div_helper_lhs_zero_inclusive(self, rhs, &zero_point)
        } else {
            // Only option is CERTAINLY_FALSE for rhs.
            div_helper_zero_exclusive(self, rhs, &zero_point)
        }
    }

    /// Returns the cardinality of this interval, which is the number of all
    /// distinct points inside it. This function returns `None` if:
    /// - The interval is unbounded from either side, or
    /// - Cardinality calculations for the datatype in question is not
    ///   implemented yet, or
    /// - An overflow occurs during the calculation.
    pub fn cardinality(&self) -> Option<u64> {
        let data_type = self.get_datatype();
        if data_type.is_integer() {
            self.upper.distance(&self.lower).map(|diff| diff as u64)
        }
        // Ordering floating-point numbers according to their binary representations
        // coincide with their natural ordering. Therefore, we can consider their
        // binary representations as "indices" and subtract them. For details, see:
        // https://stackoverflow.com/questions/8875064/how-many-distinct-floating-point-numbers-in-a-specific-range
        else if data_type.is_floating() {
            match (&self.lower, &self.upper) {
                (
                    ScalarValue::Float32(Some(lower)),
                    ScalarValue::Float32(Some(upper)),
                ) => {
                    // Negative numbers are sorted in the reverse order.
                    // To always have a positive difference after the subtraction,
                    // we perform following transformation:
                    let lower_bits = lower.to_bits() as i32;
                    let upper_bits = upper.to_bits() as i32;
                    let transformed_lower =
                        lower_bits ^ ((lower_bits >> 31) & 0x7fffffff);
                    let transformed_upper =
                        upper_bits ^ ((upper_bits >> 31) & 0x7fffffff);
                    let Ok(count) = transformed_upper.sub_checked(transformed_lower)
                    else {
                        return None;
                    };
                    Some(count as u64)
                }
                (
                    ScalarValue::Float64(Some(lower)),
                    ScalarValue::Float64(Some(upper)),
                ) => {
                    let lower_bits = lower.to_bits() as i64;
                    let upper_bits = upper.to_bits() as i64;
                    let transformed_lower =
                        lower_bits ^ ((lower_bits >> 63) & 0x7fffffffffffffff);
                    let transformed_upper =
                        upper_bits ^ ((upper_bits >> 63) & 0x7fffffffffffffff);
                    let Ok(count) = transformed_upper.sub_checked(transformed_lower)
                    else {
                        return None;
                    };
                    Some(count as u64)
                }
                _ => None,
            }
        } else {
            // Cardinality calculations are not implemented for this data type yet:
            None
        }
        .map(|result| (result + 1))
    }
}

impl Default for Interval {
    fn default() -> Self {
        Interval {
            lower: ScalarValue::Null,
            upper: ScalarValue::Null,
        }
    }
}

impl Display for Interval {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "[{}, {}]", self.lower, self.upper)
    }
}

pub fn apply_operator(op: &Operator, lhs: &Interval, rhs: &Interval) -> Result<Interval> {
    match *op {
        Operator::Eq => lhs.equal(rhs),
        Operator::NotEq => lhs.equal(rhs)?.not(),
        Operator::Gt => lhs.gt(rhs),
        Operator::GtEq => lhs.gt_eq(rhs),
        Operator::Lt => lhs.lt(rhs),
        Operator::LtEq => lhs.lt_eq(rhs),
        Operator::And => lhs.and(rhs),
        Operator::Plus => lhs.add(rhs),
        Operator::Minus => lhs.sub(rhs),
        Operator::Multiply => lhs.mul(rhs),
        Operator::Divide => lhs.div(rhs),
        _ => Err(DataFusionError::Internal(format!(
            "Interval arithmetic does not support the operator {}",
            op
        ))),
    }
}

// Cannot be used other than add method of intervals since
// it may return non-standardized interval bounds.
fn add_bounds<const UPPER: bool>(
    lhs: &ScalarValue,
    rhs: &ScalarValue,
) -> Result<ScalarValue> {
    let dt = get_result_type(&lhs.data_type(), &Operator::Plus, &rhs.data_type())?;

    if lhs.is_null() || rhs.is_null() {
        return ScalarValue::try_from(dt);
    }

    Ok(match lhs.data_type() {
        DataType::Float64 | DataType::Float32 => {
            alter_fp_rounding_mode::<UPPER, _>(lhs, rhs, |lhs, rhs| lhs.add_checked(rhs))
        }
        _ => lhs.add_checked(rhs),
    }
    .unwrap_or(handle_overflow::<UPPER>(lhs, rhs, dt)?))
}

// Cannot be used other than add method of intervals since
// it may return non-standardized interval bounds.
fn sub_bounds<const UPPER: bool>(
    lhs: &ScalarValue,
    rhs: &ScalarValue,
) -> Result<ScalarValue> {
    let dt = get_result_type(&lhs.data_type(), &Operator::Minus, &rhs.data_type())?;

    if lhs.is_null() || rhs.is_null() {
        return ScalarValue::try_from(dt);
    }

    Ok(match lhs.data_type() {
        DataType::Float64 | DataType::Float32 => {
            alter_fp_rounding_mode::<UPPER, _>(lhs, rhs, |lhs, rhs| lhs.sub_checked(rhs))
        }
        _ => lhs.sub_checked(rhs),
    }
    // We cannot represent the overflow cases.
    // Set the bound as unbounded.
    .unwrap_or(handle_overflow::<UPPER>(lhs, rhs, dt)?))
}

// Cannot be used other than add method of intervals since
// it may return non-standardized interval bounds.
fn mul_bounds<const UPPER: bool>(
    lhs: &ScalarValue,
    rhs: &ScalarValue,
) -> Result<ScalarValue> {
    let dt = get_result_type(&lhs.data_type(), &Operator::Multiply, &rhs.data_type())?;

    if lhs.is_null() || rhs.is_null() {
        return ScalarValue::try_from(dt);
    }

    Ok(match lhs.data_type() {
        DataType::Float64 | DataType::Float32 => {
            alter_fp_rounding_mode::<UPPER, _>(lhs, rhs, |lhs, rhs| lhs.mul_checked(rhs))
        }
        _ => lhs.mul_checked(rhs),
    }
    // We cannot represent the overflow cases.
    // Set the bound as unbounded.
    .unwrap_or(handle_overflow::<UPPER>(lhs, rhs, dt)?))
}

// Cannot be used other than add method of intervals since
// it may return non-standardized interval bounds.
fn div_bounds<const UPPER: bool>(
    lhs: &ScalarValue,
    rhs: &ScalarValue,
) -> Result<ScalarValue> {
    let dt = get_result_type(&lhs.data_type(), &Operator::Divide, &rhs.data_type())?;
    let zero = ScalarValue::new_zero(&dt)?;

    if lhs.is_null() || rhs.eq(&zero) {
        return ScalarValue::try_from(dt);
    } else if rhs.is_null() {
        return Ok(zero);
    }

    Ok(match lhs.data_type() {
        DataType::Float64 | DataType::Float32 => {
            alter_fp_rounding_mode::<UPPER, _>(lhs, rhs, |lhs, rhs| lhs.div(rhs))
        }
        _ => lhs.div(rhs),
    }
    // We cannot represent the overflow cases.
    // Set the bound as unbounded.
    .unwrap_or(handle_overflow::<UPPER>(lhs, rhs, dt)?))
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
            IntervalDayTime(Some(value)) if value == i64::$bound => IntervalDayTime(None),
            IntervalMonthDayNano(Some(value)) if value == i128::$bound => {
                IntervalMonthDayNano(None)
            }
            _ => next_value_helper::<$direction>($value),
        }
    };
}

// It should remain private since it may malform the intervals if used randomly.
fn next_value(value: ScalarValue) -> ScalarValue {
    use ScalarValue::*;
    value_transition!(MAX, true, value)
}

// It should remain private since it may malform the intervals if used randomly.
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

/// This function either increments or decrements its argument, depending on the `INC` value.
/// If `true`, it increments; otherwise it decrements the argument.
fn increment_decrement<const INC: bool, T: OneTrait + SubAssign + AddAssign>(
    mut val: T,
) -> T {
    if INC {
        val.add_assign(T::one());
    } else {
        val.sub_assign(T::one());
    }
    val
}

/// This function returns the next/previous value depending on the `ADD` value.
/// If `true`, it returns the next value; otherwise it returns the previous value.
fn next_value_helper<const INC: bool>(value: ScalarValue) -> ScalarValue {
    use ScalarValue::*;
    match value {
        // f32/64::-Inf/Inf and f32/64::NaN values should not emerge at this point.
        Float32(Some(val)) => {
            assert!(val.is_finite(), "Non-standardized floating point usage");
            Float32(Some(if INC { next_up(val) } else { next_down(val) }))
        }
        Float64(Some(val)) => {
            assert!(val.is_finite(), "Non-standardized floating point usage");
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
        IntervalDayTime(Some(val)) => {
            IntervalDayTime(Some(increment_decrement::<INC, i64>(val)))
        }
        IntervalMonthDayNano(Some(val)) => {
            IntervalMonthDayNano(Some(increment_decrement::<INC, i128>(val)))
        }
        _ => value, // Unbounded values return without change.
    }
}

/// Returns the greater one of two interval bounds.
/// When comparing unbounded values, they are assumed to be lower bounds.
fn max_of_bounds(first: &ScalarValue, second: &ScalarValue) -> ScalarValue {
    if first.is_null() {
        second.clone()
    } else if second.is_null() || first >= second {
        first.clone()
    } else {
        second.clone()
    }
}

/// Returns the smaller one of two interval bounds.
/// When comparing unbounded values, they are assumed to be upper bounds.
fn min_of_bounds(first: &ScalarValue, second: &ScalarValue) -> ScalarValue {
    if first.is_null() {
        second.clone()
    } else if second.is_null() || first <= second {
        first.clone()
    } else {
        second.clone()
    }
}

/// Finds the equatable part of two intervals and updates them with that new result.
/// None means they are disjoint intervals.
pub fn equalize_intervals(
    first: &Interval,
    second: &Interval,
) -> Result<Option<Vec<Interval>>> {
    if let Some(intersection) = first.intersect(second)? {
        Ok(Some(vec![intersection.clone(), intersection]))
    } else {
        // Disjoint intervals result in invalid intervals.
        Ok(None)
    }
}

/// It either updates one or both of the intervals, or does not modify
/// the intervals, or it can return None meaning infeasible result.
/// op => includes_endpoints / op_gt
///
/// GtEq => true / true
///
/// Gt => false / true
///
/// LtEq => true / false
///
/// Lt => false / false
pub fn satisfy_comparison(
    left: &Interval,
    right: &Interval,
    includes_endpoints: bool,
    op_gt: bool,
) -> Result<Option<Vec<Interval>>> {
    // The algorithm is implemented to work with Gt or GtEq operators. In case of Lt or LtEq
    // operator, we can switch the sides at the initial and final steps.
    let (left, right) = if op_gt { (left, right) } else { (right, left) };

    if left.upper <= right.lower && !left.upper.is_null() {
        if includes_endpoints && left.upper == right.lower {
            // Singleton intervals
            return Ok(Some(vec![
                Interval::try_new(left.upper.clone(), left.upper.clone())?,
                Interval::try_new(left.upper.clone(), left.upper.clone())?,
            ]));
        } else {
            // left:  <--======----0------------>
            // right: <------------0--======---->
            // No intersection, infeasible to propagate
            return Ok(None);
        }
    }

    // Gt operator can only change left lower bound and right upper bound.
    let new_left_lower = if left.lower < right.lower || left.lower.is_null() {
        if includes_endpoints {
            right.lower.clone()
        } else {
            next_value(right.lower.clone())
        }
    } else if (left.lower == right.lower) && includes_endpoints {
        right.lower.clone()
    } else if (left.lower == right.lower) && !includes_endpoints {
        next_value(right.lower.clone())
    } else {
        left.lower.clone()
    };
    let new_right_upper =
        if (left.upper < right.upper && !left.upper.is_null()) || right.upper.is_null() {
            if includes_endpoints {
                left.upper.clone()
            } else {
                prev_value(left.upper.clone())
            }
        } else if (left.upper == right.upper) && includes_endpoints {
            left.upper.clone()
        } else if (left.lower == right.lower) && !includes_endpoints {
            prev_value(left.upper.clone())
        } else {
            right.upper.clone()
        };

    if op_gt {
        Ok(Some(vec![
            Interval::try_new(new_left_lower, left.upper.clone())?,
            Interval::try_new(right.lower.clone(), new_right_upper)?,
        ]))
    } else {
        Ok(Some(vec![
            Interval::try_new(right.lower.clone(), new_right_upper)?,
            Interval::try_new(new_left_lower, left.upper.clone())?,
        ]))
    }
}

/// Multiplies two intervals that both include zero.
///
/// This function takes in two intervals (`lhs` and `rhs`) as arguments and returns their product.
/// It is specifically designed to handle intervals that both contain zero within their range.
/// Returns an error if the multiplication of bounds fails.
///
/// left:  <-------=====0=====------->
///
/// right: <-------=====0=====------->
fn mul_helper_multi_zero_inclusive(lhs: &Interval, rhs: &Interval) -> Result<Interval> {
    if lhs.lower.is_null()
        || lhs.upper.is_null()
        || rhs.lower.is_null()
        || rhs.upper.is_null()
    {
        return Interval::make_unbounded(&lhs.get_datatype());
    }
    // Since unbounded cases are handled above, we can safely
    // use the utility functions here to eliminate code duplication.
    let lower = min_of_bounds(
        &mul_bounds::<false>(&lhs.lower, &rhs.upper)?,
        &mul_bounds::<false>(&rhs.lower, &lhs.upper)?,
    );
    let upper = max_of_bounds(
        &mul_bounds::<true>(&lhs.upper, &rhs.upper)?,
        &mul_bounds::<true>(&lhs.lower, &rhs.lower)?,
    );
    Interval::try_new(lower, upper)
}

/// Multiplies two intervals where only left-hand side interval includes zero.
///
/// This function calculates the product of two intervals (`lhs` and `rhs`), when only lhs
/// contains zero within its range. The interval not containing zero, i.e. rhs, can either be
/// on the left side or right side of zero. Returns an error if the multiplication of bounds
/// fails or if there is a problem with determining the result type.
///
/// left:  <-------=====0=====------->
///
/// right: <--======----0------------>
///
///                    or
///
/// left:  <-------=====0=====------->
///
/// right: <------------0--======---->
fn mul_helper_single_zero_inclusive(
    lhs: &Interval,
    rhs: &Interval,
    zero_point: &Interval,
) -> Result<Interval> {
    let inf = ScalarValue::try_from(get_result_type(
        &lhs.get_datatype(),
        &Operator::Multiply,
        &rhs.get_datatype(),
    )?)?;
    if rhs.upper < zero_point.upper && !rhs.upper.is_null() {
        // <-------=====0=====------->
        // <--======----0------------>
        let lower = if lhs.upper.is_null() || rhs.lower.is_null() {
            inf.clone()
        } else {
            mul_bounds::<false>(&lhs.upper, &rhs.lower)?
        };
        let upper = if lhs.lower.is_null() || rhs.lower.is_null() {
            inf
        } else {
            mul_bounds::<true>(&lhs.lower, &rhs.lower)?
        };
        Interval::try_new(lower, upper)
    } else {
        // <-------=====0=====------->
        // <------------0--======---->
        let lower = if lhs.lower.is_null() || rhs.upper.is_null() {
            inf.clone()
        } else {
            mul_bounds::<false>(&lhs.lower, &rhs.upper)?
        };
        let upper = if lhs.upper.is_null() || rhs.upper.is_null() {
            inf
        } else {
            mul_bounds::<true>(&lhs.upper, &rhs.upper)?
        };
        Interval::try_new(lower, upper)
    }
}

/// Multiplies two intervals where neither of them includes zero.
///
/// This function calculates the product of two intervals (`lhs` and `rhs`), when none of
/// the intervals includes zero. Returns an error if the multiplication of bounds
/// fails or if there is a problem with determining the result type.
///
/// left:  <--======----0------------>
///
/// right: <--======----0------------>
///
///                    or
///
/// left:  <--======----0------------>
///
/// right: <------------0--======---->
///
///                    or
///
/// left:  <------------0--======---->
///
/// right: <--======----0------------>
///
///                    or
///
/// left:  <------------0--======---->
///
/// right: <------------0--======---->
fn mul_helper_zero_exclusive(
    lhs: &Interval,
    rhs: &Interval,
    zero_point: &Interval,
) -> Result<Interval> {
    let inf = ScalarValue::try_from(get_result_type(
        &lhs.get_datatype(),
        &Operator::Multiply,
        &rhs.get_datatype(),
    )?)?;
    match (
        lhs.upper <= zero_point.upper && !lhs.upper.is_null(),
        rhs.upper <= zero_point.upper && !rhs.upper.is_null(),
    ) {
        (true, true) => {
            // <--======----0------------>
            // <--======----0------------>
            let lower = mul_bounds::<false>(&lhs.upper, &rhs.upper)?;
            let upper = if lhs.lower.is_null() || rhs.lower.is_null() {
                inf
            } else {
                mul_bounds::<true>(&lhs.lower, &rhs.lower)?
            };
            Ok(Interval::try_new(lower, upper)?)
        }
        (true, false) => {
            // <--======----0------------>
            // <------------0--======---->
            mul_helper_zero_exclusive_opposite(lhs, rhs)
        }
        (false, true) => {
            // <------------0--======---->
            // <--======----0------------>
            mul_helper_zero_exclusive_opposite(rhs, lhs)
        }
        (false, false) => {
            // <------------0--======---->
            // <------------0--======---->
            let lower = mul_bounds::<false>(&lhs.lower, &rhs.lower)?;
            let upper = if lhs.upper.is_null() || rhs.upper.is_null() {
                inf
            } else {
                mul_bounds::<true>(&lhs.upper, &rhs.upper)?
            };
            Ok(Interval::try_new(lower, upper)?)
        }
    }
}

// <--======----0------------>
// <------------0--======---->
fn mul_helper_zero_exclusive_opposite(
    lhs: &Interval,
    rhs: &Interval,
) -> Result<Interval> {
    let inf = ScalarValue::try_from(get_result_type(
        &lhs.get_datatype(),
        &Operator::Multiply,
        &rhs.get_datatype(),
    )?)?;
    let lower = if lhs.lower.is_null() || rhs.upper.is_null() {
        inf
    } else {
        mul_bounds::<false>(&lhs.lower, &rhs.upper)?
    };
    let upper = mul_bounds::<true>(&lhs.upper, &rhs.lower)?;
    Interval::try_new(lower, upper)
}

/// Divides left-hand side interval by right-hand side interval when left-hand side includes zero.
///
/// This function calculates the divison of two intervals (`lhs` and `rhs`), when lhs of
/// the intervals includes zero. Returns an error if the division of bounds fails or
/// if there is a problem with determining the result type.
///
/// left:  <-------=====0=====------->
///
/// right: <--======----0------------>
///
///                    or
///
/// left:  <-------=====0=====------->
///
/// right: <------------0--======---->
fn div_helper_lhs_zero_inclusive(
    lhs: &Interval,
    rhs: &Interval,
    zero_point: &Interval,
) -> Result<Interval> {
    let inf = ScalarValue::try_from(get_result_type(
        &lhs.get_datatype(),
        &Operator::Divide,
        &rhs.get_datatype(),
    )?)?;
    if rhs.upper < zero_point.upper && !rhs.upper.is_null() {
        // <-------=====0=====------->
        // <--======----0------------>
        let lower = if lhs.upper.is_null() {
            inf.clone()
        } else {
            div_bounds::<false>(&lhs.upper, &rhs.upper)?
        };
        let upper = if lhs.lower.is_null() {
            inf
        } else {
            div_bounds::<true>(&lhs.lower, &rhs.upper)?
        };
        Interval::try_new(lower, upper)
    } else {
        // <-------=====0=====------->
        // <------------0--======---->
        let lower = if lhs.lower.is_null() {
            inf.clone()
        } else {
            div_bounds::<false>(&lhs.lower, &rhs.lower)?
        };
        let upper = if lhs.upper.is_null() {
            inf
        } else {
            div_bounds::<true>(&lhs.upper, &rhs.lower)?
        };
        Interval::try_new(lower, upper)
    }
}

/// Divides left-hand side interval by right-hand side interval when left-hand side includes zero.
///
/// This function calculates the divison of two intervals (`lhs` and `rhs`), when lhs of
/// the intervals includes zero. Returns an error if the division of bounds fails or
/// if there is a problem with determining the result type.
///
/// left:  <--======----0------------>
///
/// right: <--======----0------------>
///
///                    or
///
/// left:  <--======----0------------>
///
/// right: <------------0--======---->
///
///                    or
///
/// left:  <------------0--======---->
///
/// right: <--======----0------------>
///
///                    or
///
/// left:  <------------0--======---->
///
/// right: <------------0--======---->
fn div_helper_zero_exclusive(
    lhs: &Interval,
    rhs: &Interval,
    zero_point: &Interval,
) -> Result<Interval> {
    let inf = ScalarValue::try_from(get_result_type(
        &lhs.get_datatype(),
        &Operator::Divide,
        &rhs.get_datatype(),
    )?)?;
    match (
        lhs.upper <= zero_point.upper && !lhs.upper.is_null(),
        rhs.upper <= zero_point.upper && !rhs.upper.is_null(),
    ) {
        (true, true) => {
            // <--======----0------------>
            // <--======----0------------>
            let lower = div_bounds::<false>(&lhs.upper, &rhs.lower)?;
            let upper = if lhs.lower.is_null() {
                inf
            } else {
                div_bounds::<true>(&lhs.lower, &rhs.upper)?
            };
            Ok(Interval::try_new(lower, upper)?)
        }
        (true, false) => {
            // <--======----0------------>
            // <------------0--======---->
            let lower = if lhs.lower.is_null() {
                inf
            } else {
                div_bounds::<false>(&lhs.lower, &rhs.lower)?
            };
            let upper = div_bounds::<true>(&lhs.upper, &rhs.upper)?;
            Ok(Interval::try_new(lower, upper)?)
        }
        (false, true) => {
            // <------------0--======---->
            // <--======----0------------>
            let lower = if lhs.upper.is_null() {
                inf
            } else {
                div_bounds::<false>(&lhs.upper, &rhs.upper)?
            };
            let upper = div_bounds::<true>(&lhs.lower, &rhs.lower)?;
            Ok(Interval::try_new(lower, upper)?)
        }
        (false, false) => {
            // <------------0--======---->
            // <------------0--======---->
            let lower = div_bounds::<false>(&lhs.lower, &rhs.upper)?;
            let upper = if lhs.upper.is_null() {
                inf
            } else {
                div_bounds::<true>(&lhs.upper, &rhs.lower)?
            };
            Ok(Interval::try_new(lower, upper)?)
        }
    }
}

/// This function computes the selectivity of an operation by computing the
/// cardinality ratio of the given input/output intervals. If this can not be
/// calculated for some reason, it returns `1.0` meaning fullly selective (no
/// filtering).
pub fn cardinality_ratio(
    initial_interval: &Interval,
    final_interval: &Interval,
) -> Result<f64> {
    Ok(
        match (final_interval.cardinality(), initial_interval.cardinality()) {
            (Some(final_interval), Some(initial_interval)) => {
                final_interval as f64 / initial_interval as f64
            }
            _ => 1.0,
        },
    )
}

/// Cast scalar value to the given data type using an arrow kernel.
fn cast_scalar_value(
    value: &ScalarValue,
    data_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ScalarValue> {
    let cast_array = cast_with_options(&value.to_array(), data_type, cast_options)?;
    ScalarValue::try_from_array(&cast_array, 0)
}

/// Since we cannot represent the overflow cases, this function
/// sets the bound as unbounded for positive upper bounds and negative lower bounds,
/// For other cases, select the one that narrows the most.
/// TODO: When ScalarValue supports such methods like ScalarValue::max(data_type) and
/// ScalarValue::min(data_type), we can even narrow more.
fn handle_overflow<const UPPER: bool>(
    lhs: &ScalarValue,
    rhs: &ScalarValue,
    dt: DataType,
) -> Result<ScalarValue> {
    // No need to check operator since knowing lhs sign is enough for subtraction and division cases also.
    match (UPPER, lhs > &ScalarValue::new_zero(&lhs.data_type())?) {
        (true, true) | (false, false) => ScalarValue::try_from(dt),
        (true, false) => {
            if lhs < rhs {
                Ok(lhs.clone())
            } else {
                Ok(rhs.clone())
            }
        }
        (false, true) => {
            if lhs > rhs {
                Ok(lhs.clone())
            } else {
                Ok(rhs.clone())
            }
        }
    }
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
/// use datafusion_expr::interval_aritmetic::{Interval, NullableInterval};
///
/// // [1, 2) U {NULL}
/// NullableInterval::MaybeNull {
///    values: Interval::try_new(
///            ScalarValue::Int32(Some(1)).unwrap(),
///            ScalarValue::Int32(Some(2)).unwrap(),
///        )?;
///
/// // (0, ∞)
/// NullableInterval::NotNull {
///   values:Interval::try_new(
///            ScalarValue::Int32(Some(0)).unwrap(),
///            ScalarValue::Int32(None).unwrap(),
///        )?;
///
/// // {NULL}
/// NullableInterval::Null { datatype: DataType::Int32 };
///
/// // {4}
/// NullableInterval::from(ScalarValue::Int32(Some(4)));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NullableInterval {
    /// The value is always null in this interval
    ///
    /// This is typed so it can be used in physical expressions, which don't do
    /// type coercion.
    Null { datatype: DataType },
    /// The value may or may not be null in this interval. If it is non null its value is within
    /// the specified values interval
    MaybeNull { values: Interval },
    /// The value is definitely not null in this interval and is within values
    NotNull { values: Interval },
}

impl Default for NullableInterval {
    fn default() -> Self {
        NullableInterval::MaybeNull {
            values: Interval::default(),
        }
    }
}

impl Display for NullableInterval {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null { .. } => write!(f, "NullableInterval: {{NULL}}"),
            Self::MaybeNull { values } => {
                write!(f, "NullableInterval: {} U {{NULL}}", values)
            }
            Self::NotNull { values } => write!(f, "NullableInterval: {}", values),
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
    /// Get the values interval, or None if this interval is definitely null.
    pub fn values(&self) -> Option<&Interval> {
        match self {
            Self::Null { .. } => None,
            Self::MaybeNull { values } | Self::NotNull { values } => Some(values),
        }
    }

    /// Get the data type
    pub fn get_datatype(&self) -> Result<DataType> {
        match self {
            Self::Null { datatype } => Ok(datatype.clone()),
            Self::MaybeNull { values } | Self::NotNull { values } => {
                Ok(values.get_datatype())
            }
        }
    }

    /// Return true if the value is definitely true (and not null).
    pub fn is_certainly_true(&self) -> bool {
        match self {
            Self::Null { .. } | Self::MaybeNull { .. } => false,
            Self::NotNull { values } => values == &Interval::CERTAINLY_TRUE,
        }
    }

    /// Return true if the value is definitely false (and not null).
    pub fn is_certainly_false(&self) -> bool {
        match self {
            Self::Null { .. } => false,
            Self::MaybeNull { .. } => false,
            Self::NotNull { values } => values == &Interval::CERTAINLY_FALSE,
        }
    }

    /// Perform logical negation on a boolean nullable interval.
    fn not(&self) -> Result<Self> {
        match self {
            Self::Null { datatype } => Ok(Self::Null {
                datatype: datatype.clone(),
            }),
            Self::MaybeNull { values } => Ok(Self::MaybeNull {
                values: values.not()?,
            }),
            Self::NotNull { values } => Ok(Self::NotNull {
                values: values.not()?,
            }),
        }
    }

    /// Apply the given operator to this interval and the given interval.
    ///
    /// # Examples
    ///
    /// ```
    /// use datafusion_common::ScalarValue;
    /// use datafusion_expr::Operator;
    /// use datafusion_expr::interval_aritmetic::{Interval, NullableInterval};
    ///
    /// // 4 > 3 -> true
    /// let lhs = NullableInterval::from(ScalarValue::Int32(Some(4)));
    /// let rhs = NullableInterval::from(ScalarValue::Int32(Some(3)));
    /// let result = lhs.apply_operator(&Operator::Gt, &rhs).unwrap();
    /// assert_eq!(result, NullableInterval::from(ScalarValue::Boolean(Some(true))));
    ///
    /// // [1, 3) > NULL -> NULL
    /// let lhs = NullableInterval::NotNull {
    ///     values: Interval::try_new(
    ///            ScalarValue::Int32(Some(1)).unwrap(),
    ///            ScalarValue::Int32(Some(3)).unwrap(),
    ///        )?;
    /// let rhs = NullableInterval::from(ScalarValue::Int32(None));
    /// let result = lhs.apply_operator(&Operator::Gt, &rhs).unwrap();
    /// assert_eq!(result.single_value(), Some(ScalarValue::Boolean(None)));
    ///
    /// // [1, 3] > [2, 4] -> [false, true]
    /// let lhs = NullableInterval::NotNull {
    ///     values: Interval::try_new(
    ///            ScalarValue::Int32(Some(1)).unwrap(),
    ///            ScalarValue::Int32(Some(3)).unwrap(),
    ///        )?;
    /// let rhs = NullableInterval::NotNull {
    ///    values: Interval::try_new(
    ///            ScalarValue::Int32(Some(2)).unwrap(),
    ///            ScalarValue::Int32(Some(4)).unwrap(),
    ///        )?;
    /// let result = lhs.apply_operator(&Operator::Gt, &rhs).unwrap();
    /// // Both inputs are valid (non-null), so result must be non-null
    /// assert_eq!(result, NullableInterval::NotNull {
    ///    // Uncertain whether inequality is true or false
    ///    values: Interval::UNCERTAIN,
    /// });
    ///
    /// ```
    pub fn apply_operator(&self, op: &Operator, rhs: &Self) -> Result<Self> {
        match op {
            Operator::IsDistinctFrom => {
                let values = match (self, rhs) {
                    // NULL is distinct from NULL -> False
                    (Self::Null { .. }, Self::Null { .. }) => Interval::CERTAINLY_FALSE,
                    // x is distinct from y -> x != y,
                    // if at least one of them is never null.
                    (Self::NotNull { .. }, _) | (_, Self::NotNull { .. }) => {
                        let lhs_values = self.values();
                        let rhs_values = rhs.values();
                        match (lhs_values, rhs_values) {
                            (Some(lhs_values), Some(rhs_values)) => {
                                lhs_values.equal(rhs_values)?.not()?
                            }
                            (Some(_), None) | (None, Some(_)) => Interval::CERTAINLY_TRUE,
                            (None, None) => unreachable!("Null case handled above"),
                        }
                    }
                    _ => Interval::UNCERTAIN,
                };
                // IsDistinctFrom never returns null.
                Ok(Self::NotNull { values })
            }
            Operator::IsNotDistinctFrom => self
                .apply_operator(&Operator::IsDistinctFrom, rhs)
                .map(|i| i.not())?,
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
                } else if op.is_comparison_operator() {
                    Ok(Self::Null {
                        datatype: DataType::Boolean,
                    })
                } else {
                    Ok(Self::Null {
                        datatype: self.get_datatype()?,
                    })
                }
            }
        }
    }

    /// Determine if this interval contains the given interval. Returns a boolean
    /// interval that is [true, true] if this interval is a superset of the
    /// given interval, [false, false] if this interval is disjoint from the
    /// given interval, and [false, true] otherwise.
    pub fn contains<T: Borrow<Self>>(&self, other: T) -> Result<Self> {
        let rhs = other.borrow();
        if let (Some(left_values), Some(right_values)) = (self.values(), rhs.values()) {
            let values = left_values.contains(right_values)?;
            match (self, rhs) {
                (Self::NotNull { .. }, Self::NotNull { .. }) => {
                    Ok(Self::NotNull { values })
                }
                _ => Ok(Self::MaybeNull { values }),
            }
        } else {
            Ok(Self::Null {
                datatype: DataType::Boolean,
            })
        }
    }

    /// If the interval has collapsed to a single value, return that value.
    ///
    /// Otherwise returns None.
    ///
    /// # Examples
    ///
    /// ```
    /// use datafusion_common::ScalarValue;
    /// use datafusion_expr::interval_aritmetic::{Interval, NullableInterval};
    ///
    /// let interval = NullableInterval::from(ScalarValue::Int32(Some(4)));
    /// assert_eq!(interval.single_value(), Some(ScalarValue::Int32(Some(4))));
    ///
    /// let interval = NullableInterval::from(ScalarValue::Int32(None));
    /// assert_eq!(interval.single_value(), Some(ScalarValue::Int32(None)));
    ///
    /// let interval = NullableInterval::MaybeNull {
    ///     values: Interval::try_new(
    ///            ScalarValue::Int32(Some(1)).unwrap(),
    ///            ScalarValue::Int32(Some(4)).unwrap(),
    ///        )?;
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
    use super::{next_value, Interval};
    use crate::interval_aritmetic::{equalize_intervals, prev_value, satisfy_comparison};
    use arrow::datatypes::DataType;
    use datafusion_common::{Result, ScalarValue};

    #[test]
    fn test_next_previous_value() -> Result<()> {
        let zeros = vec![
            ScalarValue::new_zero(&DataType::UInt8)?,
            ScalarValue::new_zero(&DataType::UInt16)?,
            ScalarValue::new_zero(&DataType::UInt32)?,
            ScalarValue::new_zero(&DataType::UInt64)?,
            ScalarValue::new_zero(&DataType::Int8)?,
            ScalarValue::new_zero(&DataType::Int8)?,
            ScalarValue::new_zero(&DataType::Int8)?,
            ScalarValue::new_zero(&DataType::Int8)?,
        ];
        let ones = vec![
            ScalarValue::new_one(&DataType::UInt8)?,
            ScalarValue::new_one(&DataType::UInt16)?,
            ScalarValue::new_one(&DataType::UInt32)?,
            ScalarValue::new_one(&DataType::UInt64)?,
            ScalarValue::new_one(&DataType::Int8)?,
            ScalarValue::new_one(&DataType::Int8)?,
            ScalarValue::new_one(&DataType::Int8)?,
            ScalarValue::new_one(&DataType::Int8)?,
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
            assert!(next_value(value.clone())
                .sub(value.clone())
                .unwrap()
                .lt(&eps));
            assert!(value
                .clone()
                .sub(prev_value(value.clone()))
                .unwrap()
                .lt(&eps));
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

            assert_eq!(prev_value(min.clone()), inf);
            assert_ne!(next_value(min.clone()), min);

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
            (Float32(Some(f32::INFINITY)), Float32(Some(0_f32))),
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
            (DataType::UInt8, UInt8(None), UInt8(None)),
            (DataType::UInt16, UInt16(None), UInt16(None)),
            (DataType::UInt32, UInt32(None), UInt32(None)),
            (DataType::UInt64, UInt64(None), UInt64(None)),
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
        use ScalarValue::*;

        let exactly_gt_cases = vec![
            (
                Interval::try_new(Int64(Some(1000_i64)), Int64(None))?,
                Interval::try_new(Int64(None), Int64(Some(999_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(1000_i64)), Int64(Some(1000_i64)))?,
                Interval::try_new(Int64(None), Int64(Some(999_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(501_i64)), Int64(Some(1000_i64)))?,
                Interval::try_new(Int64(Some(500_i64)), Int64(Some(500_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(-1000_i64)), Int64(Some(1000_i64)))?,
                Interval::try_new(Int64(None), Int64(Some(-1500_i64)))?,
            ),
        ];
        for (first, second) in exactly_gt_cases {
            assert_eq!(first.gt(second.clone())?, Interval::CERTAINLY_TRUE);
            assert_eq!(second.lt(first)?, Interval::CERTAINLY_TRUE);
        }
        assert_eq!(
            Interval::try_new(
                next_value(Float32(Some(0.0))),
                next_value(Float32(Some(0.0))),
            )?
            .gt(Interval::try_new(Float32(Some(0.0)), Float32(Some(0.0)),)?)?,
            Interval::CERTAINLY_TRUE
        );
        assert_eq!(
            Interval::try_new(Float32(Some(-1.0)), Float32(Some(-1.0)),)?.gt(
                Interval::try_new(
                    prev_value(Float32(Some(-1.0))),
                    prev_value(Float32(Some(-1.0))),
                )?
            )?,
            Interval::CERTAINLY_TRUE
        );

        let possibly_gt_cases = vec![
            (
                Interval::try_new(Int64(Some(1000_i64)), Int64(Some(2000_i64)))?,
                Interval::try_new(Int64(Some(1000_i64)), Int64(Some(1000_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(500_i64)), Int64(Some(1000_i64)))?,
                Interval::try_new(Int64(Some(500_i64)), Int64(Some(1000_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(1000_i64)), Int64(None))?,
                Interval::try_new(Int64(Some(1000_i64)), Int64(None))?,
            ),
            (
                Interval::try_new(Int64(None), Int64(None))?,
                Interval::try_new(Int64(None), Int64(None))?,
            ),
        ];
        for (first, second) in possibly_gt_cases {
            assert_eq!(first.gt(second.clone())?, Interval::UNCERTAIN);
            assert_eq!(second.lt(first)?, Interval::UNCERTAIN);
        }
        assert_eq!(
            Interval::try_new(Float32(Some(0.0)), next_value(Float32(Some(0.0))),)?
                .gt(Interval::try_new(Float32(Some(0.0)), Float32(Some(0.0)),)?,)?,
            Interval::UNCERTAIN
        );
        assert_eq!(
            Interval::try_new(Float32(Some(-1.0)), Float32(Some(-1.0)),)?.gt(
                Interval::try_new(prev_value(Float32(Some(-1.0))), Float32(Some(-1.0)),)?,
            )?,
            Interval::UNCERTAIN
        );

        let not_gt_cases = vec![
            (
                Interval::try_new(Int64(Some(1000_i64)), Int64(Some(1000_i64)))?,
                Interval::try_new(Int64(Some(1000_i64)), Int64(Some(1000_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(500_i64)), Int64(Some(1000_i64)))?,
                Interval::try_new(Int64(Some(1000_i64)), Int64(None))?,
            ),
            (
                Interval::try_new(Int64(None), Int64(Some(1000_i64)))?,
                Interval::try_new(Int64(Some(1000_i64)), Int64(Some(1500_i64)))?,
            ),
        ];
        for (first, second) in not_gt_cases {
            assert_eq!(first.gt(second.clone())?, Interval::CERTAINLY_FALSE);
            assert_eq!(second.lt(first)?, Interval::CERTAINLY_FALSE);
        }
        assert_eq!(
            Interval::try_new(prev_value(Float32(Some(0.0))), Float32(Some(0.0)),)?
                .gt(Interval::try_new(Float32(Some(0.0)), Float32(Some(0.0)),)?,)?,
            Interval::CERTAINLY_FALSE
        );
        assert_eq!(
            Interval::try_new(Float32(Some(-1.0)), Float32(Some(-1.0)),)?.gt(
                Interval::try_new(Float32(Some(-1.0)), next_value(Float32(Some(-1.0))),)?,
            )?,
            Interval::CERTAINLY_FALSE
        );

        Ok(())
    }

    #[test]
    fn gteq_lteq_test() -> Result<()> {
        use ScalarValue::*;
        let exactly_gteq_cases = vec![
            (
                Interval::try_new(Int64(Some(1000_i64)), Int64(None))?,
                Interval::try_new(Int64(None), Int64(Some(1000_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(1000_i64)), Int64(Some(1000_i64)))?,
                Interval::try_new(Int64(None), Int64(Some(1000_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(500_i64)), Int64(Some(1000_i64)))?,
                Interval::try_new(Int64(Some(500_i64)), Int64(Some(500_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(-1000_i64)), Int64(Some(1000_i64)))?,
                Interval::try_new(Int64(None), Int64(Some(-1500_i64)))?,
            ),
        ];
        for (first, second) in exactly_gteq_cases {
            assert_eq!(first.gt_eq(second.clone())?, Interval::CERTAINLY_TRUE);
            assert_eq!(second.lt_eq(first)?, Interval::CERTAINLY_TRUE);
        }
        assert_eq!(
            Interval::try_new(Float32(Some(0.0)), Float32(Some(0.0)),)?
                .gt_eq(Interval::try_new(Float32(Some(0.0)), Float32(Some(0.0)),)?)?,
            Interval::CERTAINLY_TRUE
        );
        assert_eq!(
            Interval::try_new(Float32(Some(-1.0)), next_value(Float32(Some(-1.0))),)?
                .gt_eq(Interval::try_new(
                    prev_value(Float32(Some(-1.0))),
                    Float32(Some(-1.0)),
                )?)?,
            Interval::CERTAINLY_TRUE
        );

        let possibly_gteq_cases = vec![
            (
                Interval::try_new(Int64(Some(999_i64)), Int64(Some(2000_i64)))?,
                Interval::try_new(Int64(Some(1000_i64)), Int64(Some(1000_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(500_i64)), Int64(Some(1000_i64)))?,
                Interval::try_new(Int64(Some(500_i64)), Int64(Some(1001_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(0_i64)), Int64(None))?,
                Interval::try_new(Int64(Some(1000_i64)), Int64(None))?,
            ),
            (
                Interval::try_new(Int64(None), Int64(None))?,
                Interval::try_new(Int64(None), Int64(None))?,
            ),
        ];
        for (first, second) in possibly_gteq_cases {
            assert_eq!(first.gt_eq(second.clone())?, Interval::UNCERTAIN);
            assert_eq!(second.lt_eq(first)?, Interval::UNCERTAIN);
        }
        assert_eq!(
            Interval::try_new(prev_value(Float32(Some(0.0))), Float32(Some(0.0)),)?
                .gt_eq(Interval::try_new(Float32(Some(0.0)), Float32(Some(0.0)),)?,)?,
            Interval::UNCERTAIN
        );
        assert_eq!(
            Interval::try_new(Float32(Some(-1.0)), Float32(Some(-1.0)),)?.gt_eq(
                Interval::try_new(
                    prev_value(Float32(Some(-1.0))),
                    next_value(Float32(Some(-1.0))),
                )?,
            )?,
            Interval::UNCERTAIN
        );

        let not_gteq_cases = vec![
            (
                Interval::try_new(Int64(Some(1000_i64)), Int64(Some(1000_i64)))?,
                Interval::try_new(Int64(Some(2000_i64)), Int64(Some(2000_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(500_i64)), Int64(Some(999_i64)))?,
                Interval::try_new(Int64(Some(1000_i64)), Int64(None))?,
            ),
            (
                Interval::try_new(Int64(None), Int64(Some(1000_i64)))?,
                Interval::try_new(Int64(Some(1001_i64)), Int64(Some(1500_i64)))?,
            ),
        ];
        for (first, second) in not_gteq_cases {
            assert_eq!(first.gt_eq(second.clone())?, Interval::CERTAINLY_FALSE);
            assert_eq!(second.lt_eq(first)?, Interval::CERTAINLY_FALSE);
        }
        assert_eq!(
            Interval::try_new(
                prev_value(Float32(Some(0.0))),
                prev_value(Float32(Some(0.0))),
            )?
            .gt_eq(Interval::try_new(Float32(Some(0.0)), Float32(Some(0.0)),)?,)?,
            Interval::CERTAINLY_FALSE
        );
        assert_eq!(
            Interval::try_new(Float32(Some(-1.0)), Float32(Some(-1.0)),)?.gt_eq(
                Interval::try_new(
                    next_value(Float32(Some(-1.0))),
                    next_value(Float32(Some(-1.0))),
                )?,
            )?,
            Interval::CERTAINLY_FALSE
        );

        Ok(())
    }

    #[test]
    fn equal_test() -> Result<()> {
        use ScalarValue::*;
        let exactly_eq_cases = vec![
            (
                Interval::try_new(Int64(Some(1000_i64)), Int64(Some(1000_i64)))?,
                Interval::try_new(Int64(Some(1000_i64)), Int64(Some(1000_i64)))?,
            ),
            (
                Interval::try_new(Float32(Some(f32::MAX)), Float32(Some(f32::MAX)))?,
                Interval::try_new(Float32(Some(f32::MAX)), Float32(Some(f32::MAX)))?,
            ),
            (
                Interval::try_new(Float64(Some(f64::MIN)), Float64(Some(f64::MIN)))?,
                Interval::try_new(Float64(Some(f64::MIN)), Float64(Some(f64::MIN)))?,
            ),
            (
                Interval::try_new(UInt64(Some(0_u64)), UInt64(Some(0_u64)))?,
                Interval::try_new(UInt64(Some(0_u64)), UInt64(Some(0_u64)))?,
            ),
        ];
        for (first, second) in exactly_eq_cases {
            assert_eq!(first.equal(second.clone())?, Interval::CERTAINLY_TRUE);
            assert_eq!(second.equal(first)?, Interval::CERTAINLY_TRUE);
        }

        let possibly_eq_cases = vec![
            (
                Interval::try_new(UInt64(None), UInt64(None))?,
                Interval::try_new(UInt64(None), UInt64(None))?,
            ),
            (
                Interval::try_new(UInt64(Some(0)), UInt64(Some(0)))?,
                Interval::try_new(UInt64(Some(0)), UInt64(Some(1000)))?,
            ),
            (
                Interval::try_new(UInt64(Some(0)), UInt64(Some(0)))?,
                Interval::try_new(UInt64(Some(0)), UInt64(Some(1000)))?,
            ),
            (
                Interval::try_new(Float32(Some(100.0)), Float32(Some(200.0)))?,
                Interval::try_new(Float32(Some(0.0)), Float32(Some(1000.0)))?,
            ),
        ];
        for (first, second) in possibly_eq_cases {
            assert_eq!(first.equal(second.clone())?, Interval::UNCERTAIN);
            assert_eq!(second.equal(first)?, Interval::UNCERTAIN);
        }
        assert_eq!(
            Interval::try_new(prev_value(Float32(Some(0.0))), Float32(Some(0.0)),)?
                .equal(Interval::try_new(Float32(Some(0.0)), Float32(Some(0.0)),)?,)?,
            Interval::UNCERTAIN
        );
        assert_eq!(
            Interval::try_new(Float32(Some(-1.0)), Float32(Some(-1.0)),)?.equal(
                Interval::try_new(
                    prev_value(Float32(Some(-1.0))),
                    next_value(Float32(Some(-1.0))),
                )?,
            )?,
            Interval::UNCERTAIN
        );

        let not_eq_cases = vec![
            (
                Interval::try_new(Int64(Some(1000_i64)), Int64(Some(1000_i64)))?,
                Interval::try_new(Int64(Some(2000_i64)), Int64(Some(2000_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(500_i64)), Int64(Some(999_i64)))?,
                Interval::try_new(Int64(Some(1000_i64)), Int64(None))?,
            ),
            (
                Interval::try_new(Int64(None), Int64(Some(1000_i64)))?,
                Interval::try_new(Int64(Some(1001_i64)), Int64(Some(1500_i64)))?,
            ),
        ];
        for (first, second) in not_eq_cases {
            assert_eq!(first.equal(second.clone())?, Interval::CERTAINLY_FALSE);
            assert_eq!(second.equal(first)?, Interval::CERTAINLY_FALSE);
        }
        assert_eq!(
            Interval::try_new(
                prev_value(Float32(Some(0.0))),
                prev_value(Float32(Some(0.0))),
            )?
            .equal(Interval::try_new(Float32(Some(0.0)), Float32(Some(0.0)),)?,)?,
            Interval::CERTAINLY_FALSE
        );
        assert_eq!(
            Interval::try_new(Float32(Some(-1.0)), Float32(Some(-1.0)),)?.equal(
                Interval::try_new(
                    next_value(Float32(Some(-1.0))),
                    next_value(Float32(Some(-1.0))),
                )?,
            )?,
            Interval::CERTAINLY_FALSE
        );

        Ok(())
    }

    #[test]
    fn and_test() -> Result<()> {
        let cases = vec![
            (false, true, false, false, false, false),
            (false, false, false, true, false, false),
            (false, true, false, true, false, true),
            (false, true, true, true, false, true),
            (false, false, false, false, false, false),
            (true, true, true, true, true, true),
        ];

        for case in cases {
            assert_eq!(
                Interval::try_new(
                    ScalarValue::Boolean(Some(case.0)),
                    ScalarValue::Boolean(Some(case.1))
                )?
                .and(Interval::try_new(
                    ScalarValue::Boolean(Some(case.2)),
                    ScalarValue::Boolean(Some(case.3))
                )?)?,
                Interval::try_new(
                    ScalarValue::Boolean(Some(case.4)),
                    ScalarValue::Boolean(Some(case.5))
                )?
            );
        }
        Ok(())
    }

    #[test]
    fn not_test() -> Result<()> {
        let cases = vec![
            (false, true, false, true),
            (false, false, true, true),
            (true, true, false, false),
        ];

        for case in cases {
            assert_eq!(
                Interval::try_new(
                    ScalarValue::Boolean(Some(case.0)),
                    ScalarValue::Boolean(Some(case.1))
                )?
                .not()?,
                Interval::try_new(
                    ScalarValue::Boolean(Some(case.2)),
                    ScalarValue::Boolean(Some(case.3))
                )?
            );
        }
        Ok(())
    }

    #[test]
    fn intersect_test() -> Result<()> {
        use ScalarValue::*;

        let possible_cases = vec![
            (
                Interval::try_new(Int64(Some(1000_i64)), Int64(None))?,
                Interval::try_new(Int64(None), Int64(None))?,
                Interval::try_new(Int64(Some(1000_i64)), Int64(None))?,
            ),
            (
                Interval::try_new(Int64(Some(1000_i64)), Int64(Some(2000_i64)))?,
                Interval::try_new(Int64(Some(1000_i64)), Int64(Some(1500_i64)))?,
                Interval::try_new(Int64(Some(1000_i64)), Int64(Some(1500_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(1000_i64)), Int64(Some(2000_i64)))?,
                Interval::try_new(Int64(Some(500)), Int64(Some(1500_i64)))?,
                Interval::try_new(Int64(Some(1000_i64)), Int64(Some(1500_i64)))?,
            ),
            (
                Interval::try_new(UInt64(None), UInt64(Some(2000_u64)))?,
                Interval::try_new(UInt64(Some(500_u64)), UInt64(None))?,
                Interval::try_new(UInt64(Some(500_u64)), UInt64(Some(2000_u64)))?,
            ),
            (
                Interval::try_new(Float64(None), Float64(None))?,
                Interval::try_new(Float64(None), Float64(None))?,
                Interval::try_new(Float64(None), Float64(None))?,
            ),
            (
                Interval::try_new(Float64(Some(16.0)), Float64(Some(32.0)))?,
                Interval::try_new(Float64(Some(32.0)), Float64(Some(64.0)))?,
                Interval::try_new(Float64(Some(32.0)), Float64(Some(32.0)))?,
            ),
            (
                Interval::try_new(UInt64(Some(0_u64)), UInt64(Some(0_u64)))?,
                Interval::try_new(UInt64(Some(0_u64)), UInt64(None))?,
                Interval::try_new(UInt64(Some(0_u64)), UInt64(Some(0_u64)))?,
            ),
        ];
        for (first, second, expected) in possible_cases {
            assert_eq!(first.intersect(second)?.unwrap(), expected)
        }

        let empty_cases = vec![
            (
                Interval::try_new(Int64(Some(1000_i64)), Int64(None))?,
                Interval::try_new(Int64(None), Int64(Some(0_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(1500_i64)), Int64(Some(2000_i64)))?,
                Interval::try_new(Int64(Some(1000_i64)), Int64(Some(1499_i64)))?,
            ),
            (
                Interval::try_new(
                    prev_value(Float32(Some(1.0))),
                    prev_value(Float32(Some(1.0))),
                )?,
                Interval::try_new(Float32(Some(1.0)), Float32(Some(1.0)))?,
            ),
            (
                Interval::try_new(
                    next_value(Float32(Some(1.0))),
                    next_value(Float32(Some(1.0))),
                )?,
                Interval::try_new(Float32(Some(1.0)), Float32(Some(1.0)))?,
            ),
        ];
        for (first, second) in empty_cases {
            assert_eq!(first.intersect(second)?, None)
        }

        Ok(())
    }

    #[test]
    fn test_contains() -> Result<()> {
        use ScalarValue::*;

        let possible_cases = vec![
            (
                Interval::try_new(Float64(None), Float64(None))?,
                Interval::try_new(Float64(None), Float64(None))?,
                Interval::CERTAINLY_TRUE,
            ),
            (
                Interval::try_new(Int64(Some(1500_i64)), Int64(Some(2000_i64)))?,
                Interval::try_new(Int64(Some(1501_i64)), Int64(Some(1999_i64)))?,
                Interval::CERTAINLY_TRUE,
            ),
            (
                Interval::try_new(Int64(Some(1000_i64)), Int64(None))?,
                Interval::try_new(Int64(None), Int64(None))?,
                Interval::UNCERTAIN,
            ),
            (
                Interval::try_new(Int64(Some(1000_i64)), Int64(Some(2000_i64)))?,
                Interval::try_new(Int64(Some(500)), Int64(Some(1500_i64)))?,
                Interval::UNCERTAIN,
            ),
            (
                Interval::try_new(Float64(Some(16.0)), Float64(Some(32.0)))?,
                Interval::try_new(Float64(Some(32.0)), Float64(Some(64.0)))?,
                Interval::UNCERTAIN,
            ),
            (
                Interval::try_new(Int64(Some(1000_i64)), Int64(None))?,
                Interval::try_new(Int64(None), Int64(Some(0_i64)))?,
                Interval::CERTAINLY_FALSE,
            ),
            (
                Interval::try_new(Int64(Some(1500_i64)), Int64(Some(2000_i64)))?,
                Interval::try_new(Int64(Some(1000_i64)), Int64(Some(1499_i64)))?,
                Interval::CERTAINLY_FALSE,
            ),
            (
                Interval::try_new(
                    prev_value(Float32(Some(1.0))),
                    prev_value(Float32(Some(1.0))),
                )?,
                Interval::try_new(Float32(Some(1.0)), Float32(Some(1.0)))?,
                Interval::CERTAINLY_FALSE,
            ),
            (
                Interval::try_new(
                    next_value(Float32(Some(1.0))),
                    next_value(Float32(Some(1.0))),
                )?,
                Interval::try_new(Float32(Some(1.0)), Float32(Some(1.0)))?,
                Interval::CERTAINLY_FALSE,
            ),
        ];
        for (first, second, expected) in possible_cases {
            assert_eq!(first.contains(second)?, expected)
        }

        Ok(())
    }

    #[test]
    fn test_add() -> Result<()> {
        use ScalarValue::*;

        let cases = vec![
            (
                Interval::try_new(Int64(Some(100_i64)), Int64(Some(200_i64)))?,
                Interval::try_new(Int64(None), Int64(Some(200_i64)))?,
                Interval::try_new(Int64(None), Int64(Some(400_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(100_i64)), Int64(Some(200_i64)))?,
                Interval::try_new(Int64(Some(200_i64)), Int64(None))?,
                Interval::try_new(Int64(Some(300_i64)), Int64(None))?,
            ),
            (
                Interval::try_new(Int64(None), Int64(Some(200_i64)))?,
                Interval::try_new(Int64(Some(100_i64)), Int64(Some(200_i64)))?,
                Interval::try_new(Int64(None), Int64(Some(400_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(200_i64)), Int64(None))?,
                Interval::try_new(Int64(Some(100_i64)), Int64(Some(200_i64)))?,
                Interval::try_new(Int64(Some(300_i64)), Int64(None))?,
            ),
            (
                Interval::try_new(Int64(Some(100_i64)), Int64(Some(200_i64)))?,
                Interval::try_new(Int64(Some(-300_i64)), Int64(Some(150_i64)))?,
                Interval::try_new(Int64(Some(-200_i64)), Int64(Some(350_i64)))?,
            ),
            (
                Interval::try_new(Float64(Some(100_f64)), Float64(None))?,
                Interval::try_new(Float64(None), Float64(Some(200_f64)))?,
                Interval::try_new(Float64(None), Float64(None))?,
            ),
            (
                Interval::try_new(Float64(None), Float64(Some(100_f64)))?,
                Interval::try_new(Float64(None), Float64(Some(200_f64)))?,
                Interval::try_new(Float64(None), Float64(Some(300_f64)))?,
            ),
            (
                Interval::try_new(Float32(Some(f32::MAX)), Float32(Some(f32::MAX)))?,
                Interval::try_new(Float32(Some(11_f32)), Float32(Some(11_f32)))?,
                Interval::try_new(Float32(Some(f32::MAX)), Float32(None))?,
            ),
            (
                Interval::try_new(Float32(Some(f32::MIN)), Float32(Some(f32::MIN)))?,
                Interval::try_new(Float32(Some(-10_f32)), Float32(Some(10_f32)))?,
                // Since rounding mode is up, the result would be much greater than f32::MIN
                // (f32::MIN = -3.4_028_235e38, the result is -3.4_028_233e38)
                Interval::try_new(
                    Float32(None),
                    Float32(Some(-340282330000000000000000000000000000000.0)),
                )?,
            ),
            (
                Interval::try_new(Float32(Some(f32::MIN)), Float32(Some(f32::MIN)))?,
                Interval::try_new(Float32(Some(-10_f32)), Float32(Some(-10_f32)))?,
                Interval::try_new(Float32(None), Float32(Some(f32::MIN)))?,
            ),
            (
                Interval::try_new(Float32(Some(1.0)), Float32(Some(f32::MAX)))?,
                Interval::try_new(Float32(Some(f32::MAX)), Float32(Some(f32::MAX)))?,
                Interval::try_new(Float32(Some(f32::MAX)), Float32(None))?,
            ),
            (
                Interval::try_new(Float32(Some(f32::MIN)), Float32(Some(f32::MIN)))?,
                Interval::try_new(Float32(Some(f32::MAX)), Float32(Some(f32::MAX)))?,
                Interval::try_new(Float32(Some(-0.0)), Float32(Some(0.0)))?,
            ),
        ];
        for case in cases {
            assert_eq!(case.0.add(case.1)?, case.2)
        }

        Ok(())
    }

    #[test]
    fn test_sub() -> Result<()> {
        use ScalarValue::*;

        let cases = vec![
            (
                Interval::try_new(Int64(Some(100_i64)), Int64(Some(200_i64)))?,
                Interval::try_new(Int64(None), Int64(Some(200_i64)))?,
                Interval::try_new(Int64(Some(-100_i64)), Int64(None))?,
            ),
            (
                Interval::try_new(Int64(Some(100_i64)), Int64(Some(200_i64)))?,
                Interval::try_new(Int64(Some(200_i64)), Int64(None))?,
                Interval::try_new(Int64(None), Int64(Some(0)))?,
            ),
            (
                Interval::try_new(Int64(None), Int64(Some(200_i64)))?,
                Interval::try_new(Int64(Some(100_i64)), Int64(Some(200_i64)))?,
                Interval::try_new(Int64(None), Int64(Some(100_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(200_i64)), Int64(None))?,
                Interval::try_new(Int64(Some(100_i64)), Int64(Some(200_i64)))?,
                Interval::try_new(Int64(Some(0_i64)), Int64(None))?,
            ),
            (
                Interval::try_new(Int64(Some(100_i64)), Int64(Some(200_i64)))?,
                Interval::try_new(Int64(Some(-300_i64)), Int64(Some(150_i64)))?,
                Interval::try_new(Int64(Some(-50_i64)), Int64(Some(500_i64)))?,
            ),
            (
                Interval::try_new(Float64(Some(100_f64)), Float64(None))?,
                Interval::try_new(Float64(None), Float64(Some(200_f64)))?,
                Interval::try_new(Float64(Some(-100_f64)), Float64(None))?,
            ),
            (
                Interval::try_new(Float64(None), Float64(Some(100_f64)))?,
                Interval::try_new(Float64(None), Float64(Some(200_f64)))?,
                Interval::try_new(Float64(None), Float64(None))?,
            ),
            (
                Interval::try_new(Float32(Some(f32::MAX)), Float32(Some(f32::MAX)))?,
                Interval::try_new(Float32(Some(11_f32)), Float32(Some(11_f32)))?,
                // Since rounding mode is down, the result would be much smaller than f32::MAX
                // (f32::MAX = 3.4_028_235e38, the result is 3.4_028_233e38)
                Interval::try_new(
                    Float32(Some(340282330000000000000000000000000000000.0)),
                    Float32(Some(f32::MAX)),
                )?,
            ),
            (
                Interval::try_new(Float32(Some(f32::MIN)), Float32(Some(f32::MIN)))?,
                Interval::try_new(Float32(Some(-10_f32)), Float32(Some(10_f32)))?,
                Interval::try_new(
                    Float32(None),
                    Float32(Some(-340282330000000000000000000000000000000.0)),
                )?,
            ),
            (
                Interval::try_new(Float32(Some(f32::MIN)), Float32(Some(f32::MIN)))?,
                Interval::try_new(Float32(Some(-10_f32)), Float32(Some(-10_f32)))?,
                Interval::try_new(
                    Float32(Some(f32::MIN)),
                    Float32(Some(-340282330000000000000000000000000000000.0)),
                )?,
            ),
            (
                Interval::try_new(Float32(Some(1.0)), Float32(Some(f32::MAX)))?,
                Interval::try_new(Float32(Some(f32::MAX)), Float32(Some(f32::MAX)))?,
                Interval::try_new(Float32(Some(f32::MIN)), Float32(Some(0.0)))?,
            ),
            (
                Interval::try_new(Float32(Some(f32::MIN)), Float32(Some(f32::MIN)))?,
                Interval::try_new(Float32(Some(f32::MAX)), Float32(Some(f32::MAX)))?,
                Interval::try_new(Float32(None), Float32(Some(f32::MIN)))?,
            ),
        ];
        for case in cases {
            assert_eq!(case.0.sub(case.1)?, case.2)
        }

        Ok(())
    }

    #[test]
    fn test_mul() -> Result<()> {
        use ScalarValue::*;

        let cases = vec![
            (
                Interval::try_new(Int64(Some(1_i64)), Int64(Some(2_i64)))?,
                Interval::try_new(Int64(None), Int64(Some(2_i64)))?,
                Interval::try_new(Int64(None), Int64(Some(4_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(1_i64)), Int64(Some(2_i64)))?,
                Interval::try_new(Int64(Some(2_i64)), Int64(None))?,
                Interval::try_new(Int64(Some(2_i64)), Int64(None))?,
            ),
            (
                Interval::try_new(Int64(None), Int64(Some(2_i64)))?,
                Interval::try_new(Int64(Some(1_i64)), Int64(Some(2_i64)))?,
                Interval::try_new(Int64(None), Int64(Some(4_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(2_i64)), Int64(None))?,
                Interval::try_new(Int64(Some(1_i64)), Int64(Some(2_i64)))?,
                Interval::try_new(Int64(Some(2_i64)), Int64(None))?,
            ),
            (
                Interval::try_new(Int64(Some(1_i64)), Int64(Some(2_i64)))?,
                Interval::try_new(Int64(Some(-3_i64)), Int64(Some(15_i64)))?,
                Interval::try_new(Int64(Some(-6_i64)), Int64(Some(30_i64)))?,
            ),
            (
                Interval::try_new(Float64(Some(1_f64)), Float64(None))?,
                Interval::try_new(Float64(None), Float64(Some(2_f64)))?,
                Interval::try_new(Float64(None), Float64(None))?,
            ),
            (
                Interval::try_new(Float64(None), Float64(Some(1_f64)))?,
                Interval::try_new(Float64(None), Float64(Some(2_f64)))?,
                Interval::try_new(Float64(None), Float64(None))?,
            ),
            (
                Interval::try_new(Float32(Some(f32::MAX)), Float32(Some(f32::MAX)))?,
                Interval::try_new(Float32(Some(11_f32)), Float32(Some(11_f32)))?,
                Interval::try_new(Float32(Some(f32::MAX)), Float32(None))?,
            ),
            (
                Interval::try_new(Float32(Some(f32::MIN)), Float32(Some(f32::MIN)))?,
                Interval::try_new(Float32(Some(-10_f32)), Float32(Some(10_f32)))?,
                Interval::try_new(Float32(None), Float32(None))?,
            ),
            (
                Interval::try_new(Float32(Some(f32::MIN)), Float32(Some(f32::MIN)))?,
                Interval::try_new(Float32(Some(-10_f32)), Float32(Some(-10_f32)))?,
                Interval::try_new(Float32(Some(f32::MAX)), Float32(None))?,
            ),
            (
                Interval::try_new(Float32(Some(1.0)), Float32(Some(f32::MAX)))?,
                Interval::try_new(Float32(Some(f32::MAX)), Float32(Some(f32::MAX)))?,
                Interval::try_new(Float32(Some(f32::MAX)), Float32(None))?,
            ),
            (
                Interval::try_new(Float32(Some(f32::MIN)), Float32(Some(f32::MIN)))?,
                Interval::try_new(Float32(Some(f32::MAX)), Float32(Some(f32::MAX)))?,
                Interval::try_new(Float32(None), Float32(Some(f32::MIN)))?,
            ),
        ];
        for case in cases {
            assert_eq!(case.0.mul(case.1)?, case.2)
        }

        Ok(())
    }

    #[test]
    fn test_div() -> Result<()> {
        use ScalarValue::*;

        let cases = vec![
            (
                Interval::try_new(Int64(Some(100_i64)), Int64(Some(200_i64)))?,
                Interval::try_new(Int64(Some(1_i64)), Int64(Some(2_i64)))?,
                Interval::try_new(Int64(Some(50_i64)), Int64(Some(200_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(-200_i64)), Int64(Some(-100_i64)))?,
                Interval::try_new(Int64(Some(-2_i64)), Int64(Some(-1_i64)))?,
                Interval::try_new(Int64(Some(50_i64)), Int64(Some(200_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(100_i64)), Int64(Some(200_i64)))?,
                Interval::try_new(Int64(Some(-2_i64)), Int64(Some(-1_i64)))?,
                Interval::try_new(Int64(Some(-200_i64)), Int64(Some(-50_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(-200_i64)), Int64(Some(-100_i64)))?,
                Interval::try_new(Int64(Some(1_i64)), Int64(Some(2_i64)))?,
                Interval::try_new(Int64(Some(-200_i64)), Int64(Some(-50_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(-200_i64)), Int64(Some(100_i64)))?,
                Interval::try_new(Int64(Some(1_i64)), Int64(Some(2_i64)))?,
                Interval::try_new(Int64(Some(-200_i64)), Int64(Some(100_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(-100_i64)), Int64(Some(200_i64)))?,
                Interval::try_new(Int64(Some(1_i64)), Int64(Some(2_i64)))?,
                Interval::try_new(Int64(Some(-100_i64)), Int64(Some(200_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(-100_i64)), Int64(Some(200_i64)))?,
                Interval::try_new(Int64(Some(-1_i64)), Int64(Some(2_i64)))?,
                Interval::try_new(Int64(None), Int64(None))?,
            ),
            (
                Interval::try_new(Int64(Some(-100_i64)), Int64(Some(200_i64)))?,
                Interval::try_new(Int64(Some(-2_i64)), Int64(Some(1_i64)))?,
                Interval::try_new(Int64(None), Int64(None))?,
            ),
            (
                Interval::try_new(Int64(Some(100_i64)), Int64(Some(200_i64)))?,
                Interval::try_new(Int64(Some(0)), Int64(Some(1_i64)))?,
                Interval::try_new(Int64(Some(100_i64)), Int64(None))?,
            ),
            (
                Interval::try_new(Int64(Some(100_i64)), Int64(Some(200_i64)))?,
                Interval::try_new(Int64(None), Int64(Some(0)))?,
                Interval::try_new(Int64(None), Int64(Some(0)))?,
            ),
            (
                Interval::try_new(Int64(Some(100_i64)), Int64(Some(200_i64)))?,
                Interval::try_new(Int64(Some(0)), Int64(Some(0)))?,
                Interval::try_new(Int64(None), Int64(None))?,
            ),
            (
                Interval::try_new(Int64(Some(0)), Int64(Some(1_i64)))?,
                Interval::try_new(Int64(Some(100_i64)), Int64(Some(200_i64)))?,
                Interval::try_new(Int64(Some(0)), Int64(Some(0)))?,
            ),
            (
                Interval::try_new(Int64(Some(0)), Int64(Some(1_i64)))?,
                Interval::try_new(Int64(Some(100_i64)), Int64(Some(200_i64)))?,
                Interval::try_new(Int64(Some(0)), Int64(Some(0)))?,
            ),
            (
                Interval::try_new(Float32(Some(f32::MAX)), Float32(Some(f32::MAX)))?,
                Interval::try_new(Float32(Some(-0.1)), Float32(Some(0.1)))?,
                Interval::try_new(Float32(None), Float32(None))?,
            ),
            (
                Interval::try_new(Float32(Some(f32::MIN)), Float32(None))?,
                Interval::try_new(Float32(Some(0.1)), Float32(Some(0.1)))?,
                Interval::try_new(Float32(None), Float32(None))?,
            ),
            (
                Interval::try_new(Float32(Some(-10.0)), Float32(Some(10.0)))?,
                Interval::try_new(Float32(Some(-0.1)), Float32(Some(-0.1)))?,
                Interval::try_new(Float32(Some(-100.0)), Float32(Some(100.0)))?,
            ),
            (
                Interval::try_new(Float32(Some(-10.0)), Float32(Some(f32::MAX)))?,
                Interval::try_new(Float32(None), Float32(None))?,
                Interval::try_new(Float32(None), Float32(None))?,
            ),
            (
                Interval::try_new(Float32(Some(f32::MIN)), Float32(Some(10.0)))?,
                Interval::try_new(Float32(Some(1.0)), Float32(None))?,
                Interval::try_new(Float32(Some(f32::MIN)), Float32(Some(10.0)))?,
            ),
            (
                Interval::try_new(Int64(Some(12)), Int64(Some(48)))?,
                Interval::try_new(Int64(Some(10)), Int64(Some(20)))?,
                Interval::try_new(Int64(Some(0)), Int64(Some(4)))?,
            ),
            (
                Interval::try_new(Float32(Some(-4.0)), Float32(Some(2.0)))?,
                Interval::try_new(Float32(Some(10.0)), Float32(Some(20.0)))?,
                Interval::try_new(Float32(Some(-0.4)), Float32(Some(0.2)))?,
            ),
        ];
        for case in cases {
            assert_eq!(case.0.div(case.1)?, case.2)
        }

        Ok(())
    }

    #[test]
    fn test_cardinality_of_intervals() -> Result<()> {
        // In IEEE 754 standard for floating-point arithmetic, if we keep the sign and exponent fields same,
        // we can represent 4503599627370496+1 different numbers by changing the mantissa
        // (4503599627370496 = 2^52, since there are 52 bits in mantissa, and 2^23 = 8388608 for f32).
        let distinct_f64 = 4503599627370497;
        let distinct_f32 = 8388609;
        let intervals = [
            Interval::try_new(ScalarValue::from(0.25), ScalarValue::from(0.50))?,
            Interval::try_new(ScalarValue::from(0.5), ScalarValue::from(1.0))?,
            Interval::try_new(ScalarValue::from(1.0), ScalarValue::from(2.0))?,
            Interval::try_new(ScalarValue::from(32.0), ScalarValue::from(64.0))?,
            Interval::try_new(ScalarValue::from(-0.50), ScalarValue::from(-0.25))?,
            Interval::try_new(ScalarValue::from(-32.0), ScalarValue::from(-16.0))?,
        ];
        for interval in intervals {
            assert_eq!(interval.cardinality().unwrap(), distinct_f64);
        }

        let intervals = [
            Interval::try_new(ScalarValue::from(0.25_f32), ScalarValue::from(0.50_f32))?,
            Interval::try_new(ScalarValue::from(-1_f32), ScalarValue::from(-0.5_f32))?,
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
        let interval =
            Interval::try_new(ScalarValue::from(-0.0625), ScalarValue::from(0.0625))?;
        assert_eq!(interval.cardinality().unwrap(), 9178336040581070850);

        let interval = Interval::try_new(
            ScalarValue::from(-0.0625_f32),
            ScalarValue::from(0.0625_f32),
        )?;
        assert_eq!(interval.cardinality().unwrap(), 2063597570);

        Ok(())
    }

    #[test]
    fn test_equalize_intervals() -> Result<()> {
        use ScalarValue::*;
        let cases = vec![
            (
                Interval::try_new(Int64(Some(1000_i64)), Int64(None))?,
                Interval::try_new(Int64(None), Int64(Some(1000_i64)))?,
                Interval::try_new(Int64(Some(1000_i64)), Int64(Some(1000_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(1000_i64)), Int64(None))?,
                Interval::try_new(Int64(None), Int64(Some(2000_i64)))?,
                Interval::try_new(Int64(Some(1000_i64)), Int64(Some(2000_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(1000_i64)), Int64(Some(2000_i64)))?,
                Interval::try_new(Int64(Some(1000_i64)), Int64(None))?,
                Interval::try_new(Int64(Some(1000_i64)), Int64(Some(2000_i64)))?,
            ),
            (
                Interval::try_new(Float64(Some(1000.0)), Float64(None))?,
                Interval::try_new(Float64(None), Float64(Some(1000.0)))?,
                Interval::try_new(Float64(Some(1000.0)), Float64(Some(1000.0)))?,
            ),
            (
                Interval::try_new(Float64(Some(1000.0)), Float64(Some(1500.0)))?,
                Interval::try_new(Float64(Some(0.0)), Float64(Some(1500.0)))?,
                Interval::try_new(Float64(Some(1000.0)), Float64(Some(1500.0)))?,
            ),
            (
                Interval::try_new(Float64(Some(-1000.0)), Float64(Some(1500.0)))?,
                Interval::try_new(Float64(Some(-1500.0)), Float64(Some(2000.0)))?,
                Interval::try_new(Float64(Some(-1000.0)), Float64(Some(1500.0)))?,
            ),
            (
                Interval::try_new(Float64(None), Float64(None))?,
                Interval::try_new(Float64(None), Float64(None))?,
                Interval::try_new(Float64(None), Float64(None))?,
            ),
        ];
        for (first, second, result) in cases {
            assert_eq!(
                equalize_intervals(&first, &second)?.unwrap(),
                vec![result.clone(), result.clone()]
            );
            assert_eq!(
                equalize_intervals(&second, &first)?.unwrap(),
                vec![result.clone(), result]
            );
        }

        let non_equitable_cases = vec![
            (
                Interval::try_new(Int64(Some(1000_i64)), Int64(None))?,
                Interval::try_new(Int64(None), Int64(Some(999_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(0_i64)), Int64(Some(1000_i64)))?,
                Interval::try_new(Int64(Some(2000_i64)), Int64(Some(3000_i64)))?,
            ),
        ];
        for (first, second) in non_equitable_cases {
            assert_eq!(equalize_intervals(&first, &second)?, None);
            assert_eq!(equalize_intervals(&second, &first)?, None);
        }

        Ok(())
    }

    #[test]
    fn test_satisfy_comparison() -> Result<()> {
        use ScalarValue::*;
        let cases = vec![
            (
                Interval::try_new(Int64(Some(1000_i64)), Int64(None))?,
                Interval::try_new(Int64(None), Int64(Some(1000_i64)))?,
                true,
                true,
                Interval::try_new(Int64(Some(1000_i64)), Int64(None))?,
                Interval::try_new(Int64(None), Int64(Some(1000_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(1000_i64)), Int64(None))?,
                Interval::try_new(Int64(None), Int64(Some(1000_i64)))?,
                true,
                false,
                Interval::try_new(Int64(Some(1000_i64)), Int64(Some(1000_i64)))?,
                Interval::try_new(Int64(Some(1000_i64)), Int64(Some(1000_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(1000_i64)), Int64(None))?,
                Interval::try_new(Int64(None), Int64(Some(1000_i64)))?,
                false,
                true,
                Interval::try_new(Int64(Some(1000_i64)), Int64(None))?,
                Interval::try_new(Int64(None), Int64(Some(1000_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(0_i64)), Int64(Some(1000_i64)))?,
                Interval::try_new(Int64(Some(500_i64)), Int64(Some(1500_i64)))?,
                true,
                true,
                Interval::try_new(Int64(Some(500_i64)), Int64(Some(1000_i64)))?,
                Interval::try_new(Int64(Some(500_i64)), Int64(Some(1000_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(0_i64)), Int64(Some(1000_i64)))?,
                Interval::try_new(Int64(Some(500_i64)), Int64(Some(1500_i64)))?,
                true,
                false,
                Interval::try_new(Int64(Some(0_i64)), Int64(Some(1000_i64)))?,
                Interval::try_new(Int64(Some(500_i64)), Int64(Some(1500_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(0_i64)), Int64(Some(1000_i64)))?,
                Interval::try_new(Int64(Some(500_i64)), Int64(Some(1500_i64)))?,
                false,
                true,
                Interval::try_new(Int64(Some(501_i64)), Int64(Some(1000_i64)))?,
                Interval::try_new(Int64(Some(500_i64)), Int64(Some(999_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(0_i64)), Int64(Some(1000_i64)))?,
                Interval::try_new(Int64(Some(500_i64)), Int64(Some(1500_i64)))?,
                false,
                false,
                Interval::try_new(Int64(Some(0_i64)), Int64(Some(1000_i64)))?,
                Interval::try_new(Int64(Some(500_i64)), Int64(Some(1500_i64)))?,
            ),
            (
                Interval::try_new(Int64(None), Int64(None))?,
                Interval::try_new(Int64(Some(1_i64)), Int64(Some(1_i64)))?,
                false,
                true,
                Interval::try_new(Int64(Some(2_i64)), Int64(None))?,
                Interval::try_new(Int64(Some(1_i64)), Int64(Some(1_i64)))?,
            ),
            (
                Interval::try_new(Int64(None), Int64(None))?,
                Interval::try_new(Int64(Some(1_i64)), Int64(Some(1_i64)))?,
                true,
                true,
                Interval::try_new(Int64(Some(1_i64)), Int64(None))?,
                Interval::try_new(Int64(Some(1_i64)), Int64(Some(1_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(1_i64)), Int64(Some(1_i64)))?,
                Interval::try_new(Int64(None), Int64(None))?,
                false,
                true,
                Interval::try_new(Int64(Some(1_i64)), Int64(Some(1_i64)))?,
                Interval::try_new(Int64(None), Int64(Some(0_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(1_i64)), Int64(Some(1_i64)))?,
                Interval::try_new(Int64(None), Int64(None))?,
                true,
                true,
                Interval::try_new(Int64(Some(1_i64)), Int64(Some(1_i64)))?,
                Interval::try_new(Int64(None), Int64(Some(1_i64)))?,
            ),
            (
                Interval::try_new(Int64(None), Int64(None))?,
                Interval::try_new(Int64(Some(1_i64)), Int64(Some(1_i64)))?,
                false,
                false,
                Interval::try_new(Int64(None), Int64(Some(0_i64)))?,
                Interval::try_new(Int64(Some(1_i64)), Int64(Some(1_i64)))?,
            ),
            (
                Interval::try_new(Int64(None), Int64(None))?,
                Interval::try_new(Int64(Some(1_i64)), Int64(Some(1_i64)))?,
                true,
                false,
                Interval::try_new(Int64(None), Int64(Some(1_i64)))?,
                Interval::try_new(Int64(Some(1_i64)), Int64(Some(1_i64)))?,
            ),
            (
                Interval::try_new(Int64(Some(1_i64)), Int64(Some(1_i64)))?,
                Interval::try_new(Int64(None), Int64(None))?,
                false,
                false,
                Interval::try_new(Int64(Some(1_i64)), Int64(Some(1_i64)))?,
                Interval::try_new(Int64(Some(2_i64)), Int64(None))?,
            ),
            (
                Interval::try_new(Int64(Some(1_i64)), Int64(Some(1_i64)))?,
                Interval::try_new(Int64(None), Int64(None))?,
                true,
                false,
                Interval::try_new(Int64(Some(1_i64)), Int64(Some(1_i64)))?,
                Interval::try_new(Int64(Some(1_i64)), Int64(None))?,
            ),
            (
                Interval::try_new(Float64(Some(-1000.0)), Float64(Some(1000.0)))?,
                Interval::try_new(Float64(Some(-500.0)), Float64(Some(500.0)))?,
                true,
                true,
                Interval::try_new(Float64(Some(-500.0)), Float64(Some(1000.0)))?,
                Interval::try_new(Float64(Some(-500.0)), Float64(Some(500.0)))?,
            ),
            (
                Interval::try_new(Float64(Some(-1000.0)), Float64(Some(1000.0)))?,
                Interval::try_new(Float64(Some(-500.0)), Float64(Some(500.0)))?,
                false,
                true,
                Interval::try_new(
                    next_value(Float64(Some(-500.0))),
                    Float64(Some(1000.0)),
                )?,
                Interval::try_new(Float64(Some(-500.0)), Float64(Some(500.0)))?,
            ),
            (
                Interval::try_new(Float64(Some(-1000.0)), Float64(Some(1000.0)))?,
                Interval::try_new(Float64(Some(-500.0)), Float64(Some(500.0)))?,
                true,
                false,
                Interval::try_new(Float64(Some(-1000.0)), Float64(Some(500.0)))?,
                Interval::try_new(Float64(Some(-500.0)), Float64(Some(500.0)))?,
            ),
            (
                Interval::try_new(Float64(Some(-1000.0)), Float64(Some(1000.0)))?,
                Interval::try_new(Float64(Some(-500.0)), Float64(Some(500.0)))?,
                false,
                false,
                Interval::try_new(
                    Float64(Some(-1000.0)),
                    prev_value(Float64(Some(500.0))),
                )?,
                Interval::try_new(Float64(Some(-500.0)), Float64(Some(500.0)))?,
            ),
        ];
        for (first, second, includes_endpoints, op_gt, left_modified, right_modified) in
            cases
        {
            assert_eq!(
                satisfy_comparison(&first, &second, includes_endpoints, op_gt)?.unwrap(),
                vec![left_modified, right_modified]
            );
        }

        let infeasible_cases = vec![
            (
                Interval::try_new(Int64(Some(1000_i64)), Int64(None))?,
                Interval::try_new(Int64(None), Int64(Some(1000_i64)))?,
                false,
                false,
            ),
            (
                Interval::try_new(Float64(Some(-1000.0)), Float64(Some(1000.0)))?,
                Interval::try_new(Float64(Some(1500.0)), Float64(Some(2000.0)))?,
                false,
                true,
            ),
        ];
        for (first, second, includes_endpoints, op_gt) in infeasible_cases {
            assert_eq!(
                satisfy_comparison(&first, &second, includes_endpoints, op_gt)?,
                None
            );
        }

        Ok(())
    }

    #[test]
    fn test_interval_display() {
        let interval =
            Interval::try_new(ScalarValue::from(0.25_f32), ScalarValue::from(0.50_f32))
                .unwrap();
        assert_eq!(format!("{}", interval), "[0.25, 0.5]");

        let interval = Interval::try_new(
            ScalarValue::Float32(Some(f32::NEG_INFINITY)),
            ScalarValue::Float32(Some(f32::INFINITY)),
        )
        .unwrap();
        assert_eq!(format!("{}", interval), "[NULL, NULL]");
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
}
