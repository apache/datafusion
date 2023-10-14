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

use std::borrow::Borrow;
use std::fmt::{self, Display, Formatter};
use std::ops::{AddAssign, SubAssign};

use crate::aggregate::min_max::{max, min};
use crate::intervals::rounding::{alter_fp_rounding_mode, next_down, next_up};

use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::DataType;
use arrow_array::ArrowNativeTypeOp;
use datafusion_common::{internal_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::type_coercion::binary::get_result_type;
use datafusion_expr::Operator;

/// This type represents a single endpoint of an [`Interval`]. An
/// endpoint can be open (does not include the endpoint) or closed
/// (includes the endpoint).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntervalBound {
    pub value: ScalarValue,
    /// If true, interval does not include `value`
    pub open: bool,
}

impl IntervalBound {
    /// Creates a new `IntervalBound` object using the given value.
    pub const fn new(value: ScalarValue, open: bool) -> IntervalBound {
        IntervalBound { value, open }
    }

    /// Creates a new "open" interval (does not include the `value`
    /// bound)
    pub const fn new_open(value: ScalarValue) -> IntervalBound {
        IntervalBound::new(value, true)
    }

    /// Creates a new "closed" interval (includes the `value`
    /// bound)
    pub const fn new_closed(value: ScalarValue) -> IntervalBound {
        IntervalBound::new(value, false)
    }

    /// This convenience function creates an unbounded interval endpoint.
    pub fn make_unbounded<T: Borrow<DataType>>(data_type: T) -> Result<Self> {
        ScalarValue::try_from(data_type.borrow()).map(|v| IntervalBound::new(v, true))
    }

    /// This convenience function returns the data type associated with this
    /// `IntervalBound`.
    pub fn get_datatype(&self) -> DataType {
        self.value.data_type()
    }

    /// This convenience function checks whether the `IntervalBound` represents
    /// an unbounded interval endpoint.
    pub fn is_unbounded(&self) -> bool {
        self.value.is_null()
    }

    /// This function casts the `IntervalBound` to the given data type.
    pub(crate) fn cast_to(
        &self,
        data_type: &DataType,
        cast_options: &CastOptions,
    ) -> Result<IntervalBound> {
        cast_scalar_value(&self.value, data_type, cast_options)
            .map(|value| IntervalBound::new(value, self.open))
    }

    /// Returns a new bound with a negated value, if any, and the same open/closed.
    /// For example negating `[5` would return `[-5`, or `-1)` would return `1)`.
    pub fn negate(&self) -> Result<IntervalBound> {
        self.value.arithmetic_negate().map(|value| IntervalBound {
            value,
            open: self.open,
        })
    }

    /// This function adds the given `IntervalBound` to this `IntervalBound`.
    /// The result is unbounded if either is; otherwise, their values are
    /// added. The result is closed if both original bounds are closed, or open
    /// otherwise.
    pub fn add<const UPPER: bool, T: Borrow<IntervalBound>>(
        &self,
        other: T,
    ) -> Result<IntervalBound> {
        let rhs = other.borrow();
        if self.is_unbounded() || rhs.is_unbounded() {
            return IntervalBound::make_unbounded(get_result_type(
                &self.get_datatype(),
                &Operator::Plus,
                &rhs.get_datatype(),
            )?);
        }
        match self.get_datatype() {
            DataType::Float64 | DataType::Float32 => {
                alter_fp_rounding_mode::<UPPER, _>(&self.value, &rhs.value, |lhs, rhs| {
                    lhs.add(rhs)
                })
            }
            _ => self.value.add(&rhs.value),
        }
        .map(|v| IntervalBound::new(v, self.open || rhs.open))
    }

    /// This function subtracts the given `IntervalBound` from `self`.
    /// The result is unbounded if either is; otherwise, their values are
    /// subtracted. The result is closed if both original bounds are closed,
    /// or open otherwise.
    pub fn sub<const UPPER: bool, T: Borrow<IntervalBound>>(
        &self,
        other: T,
    ) -> Result<IntervalBound> {
        let rhs = other.borrow();
        if self.is_unbounded() || rhs.is_unbounded() {
            return IntervalBound::make_unbounded(get_result_type(
                &self.get_datatype(),
                &Operator::Minus,
                &rhs.get_datatype(),
            )?);
        }
        match self.get_datatype() {
            DataType::Float64 | DataType::Float32 => {
                alter_fp_rounding_mode::<UPPER, _>(&self.value, &rhs.value, |lhs, rhs| {
                    lhs.sub(rhs)
                })
            }
            _ => self.value.sub(&rhs.value),
        }
        .map(|v| IntervalBound::new(v, self.open || rhs.open))
    }

    /// This function chooses one of the given `IntervalBound`s according to
    /// the given function `decide`. The result is unbounded if both are. If
    /// only one of the arguments is unbounded, the other one is chosen by
    /// default. If neither is unbounded, the function `decide` is used.
    pub fn choose(
        first: &IntervalBound,
        second: &IntervalBound,
        decide: fn(&ScalarValue, &ScalarValue) -> Result<ScalarValue>,
    ) -> Result<IntervalBound> {
        Ok(if first.is_unbounded() {
            second.clone()
        } else if second.is_unbounded() {
            first.clone()
        } else if first.value != second.value {
            let chosen = decide(&first.value, &second.value)?;
            if chosen.eq(&first.value) {
                first.clone()
            } else {
                second.clone()
            }
        } else {
            IntervalBound::new(second.value.clone(), first.open || second.open)
        })
    }
}

impl Display for IntervalBound {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "IntervalBound [{}]", self.value)
    }
}

/// This type represents an interval, which is used to calculate reliable
/// bounds for expressions:
///
/// * An *open* interval does not include the endpoint and is written using a
/// `(` or `)`.
///
/// * A *closed* interval does include the endpoint and is written using `[` or
/// `]`.
///
/// * If the interval's `lower` and/or `upper` bounds are not known, they are
/// called *unbounded* endpoint and represented using a `NULL` and written using
/// `∞`.
///
/// # Examples
///
/// A `Int64` `Interval` of `[10, 20)` represents the values `10, 11, ... 18,
/// 19` (includes 10, but does not include 20).
///
/// A `Int64` `Interval` of  `[10, ∞)` represents a value known to be either
/// `10` or higher.
///
/// An `Interval` of `(-∞, ∞)` represents that the range is entirely unknown.
///
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Interval {
    pub lower: IntervalBound,
    pub upper: IntervalBound,
}

impl Default for Interval {
    fn default() -> Self {
        Interval::new(
            IntervalBound::new(ScalarValue::Null, true),
            IntervalBound::new(ScalarValue::Null, true),
        )
    }
}

impl Display for Interval {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}, {}{}",
            if self.lower.open { "(" } else { "[" },
            self.lower.value,
            self.upper.value,
            if self.upper.open { ")" } else { "]" }
        )
    }
}

impl Interval {
    /// Creates a new interval object using the given bounds.
    ///
    /// # Boolean intervals need special handling
    ///
    /// For boolean intervals, having an open false lower bound is equivalent to
    /// having a true closed lower bound. Similarly, open true upper bound is
    /// equivalent to having a false closed upper bound. Also for boolean
    /// intervals, having an unbounded left endpoint is equivalent to having a
    /// false closed lower bound, while having an unbounded right endpoint is
    /// equivalent to having a true closed upper bound. Therefore; input
    /// parameters to construct an Interval can have different types, but they
    /// all result in `[false, false]`, `[false, true]` or `[true, true]`.
    pub fn new(lower: IntervalBound, upper: IntervalBound) -> Interval {
        // Boolean intervals need a special handling.
        if let ScalarValue::Boolean(_) = lower.value {
            let standardized_lower = match lower.value {
                ScalarValue::Boolean(None) if lower.open => {
                    ScalarValue::Boolean(Some(false))
                }
                ScalarValue::Boolean(Some(false)) if lower.open => {
                    ScalarValue::Boolean(Some(true))
                }
                // The rest may include some invalid interval cases. The validation of
                // interval construction parameters will be implemented later.
                // For now, let's return them unchanged.
                _ => lower.value,
            };
            let standardized_upper = match upper.value {
                ScalarValue::Boolean(None) if upper.open => {
                    ScalarValue::Boolean(Some(true))
                }
                ScalarValue::Boolean(Some(true)) if upper.open => {
                    ScalarValue::Boolean(Some(false))
                }
                _ => upper.value,
            };
            Interval {
                lower: IntervalBound::new(standardized_lower, false),
                upper: IntervalBound::new(standardized_upper, false),
            }
        } else {
            Interval { lower, upper }
        }
    }

    pub fn make<T>(lower: Option<T>, upper: Option<T>, open: (bool, bool)) -> Interval
    where
        ScalarValue: From<Option<T>>,
    {
        Interval::new(
            IntervalBound::new(ScalarValue::from(lower), open.0),
            IntervalBound::new(ScalarValue::from(upper), open.1),
        )
    }

    /// Casts this interval to `data_type` using `cast_options`.
    pub(crate) fn cast_to(
        &self,
        data_type: &DataType,
        cast_options: &CastOptions,
    ) -> Result<Interval> {
        let lower = self.lower.cast_to(data_type, cast_options)?;
        let upper = self.upper.cast_to(data_type, cast_options)?;
        Ok(Interval::new(lower, upper))
    }

    /// This function returns the data type of this interval. If both endpoints
    /// do not have the same data type, returns an error.
    pub fn get_datatype(&self) -> Result<DataType> {
        let lower_type = self.lower.get_datatype();
        let upper_type = self.upper.get_datatype();
        if lower_type == upper_type {
            Ok(lower_type)
        } else {
            internal_err!(
                "Interval bounds have different types: {lower_type} != {upper_type}"
            )
        }
    }

    /// Decide if this interval is certainly greater than, possibly greater than,
    /// or can't be greater than `other` by returning [true, true],
    /// [false, true] or [false, false] respectively.
    pub(crate) fn gt<T: Borrow<Interval>>(&self, other: T) -> Interval {
        let rhs = other.borrow();
        let flags = if !self.upper.is_unbounded()
            && !rhs.lower.is_unbounded()
            && self.upper.value <= rhs.lower.value
        {
            // Values in this interval are certainly less than or equal to those
            // in the given interval.
            (false, false)
        } else if !self.lower.is_unbounded()
            && !rhs.upper.is_unbounded()
            && self.lower.value >= rhs.upper.value
            && (self.lower.value > rhs.upper.value || self.lower.open || rhs.upper.open)
        {
            // Values in this interval are certainly greater than those in the
            // given interval.
            (true, true)
        } else {
            // All outcomes are possible.
            (false, true)
        };

        Interval::make(Some(flags.0), Some(flags.1), (false, false))
    }

    /// Decide if this interval is certainly greater than or equal to, possibly greater than
    /// or equal to, or can't be greater than or equal to `other` by returning [true, true],
    /// [false, true] or [false, false] respectively.
    pub(crate) fn gt_eq<T: Borrow<Interval>>(&self, other: T) -> Interval {
        let rhs = other.borrow();
        let flags = if !self.lower.is_unbounded()
            && !rhs.upper.is_unbounded()
            && self.lower.value >= rhs.upper.value
        {
            // Values in this interval are certainly greater than or equal to those
            // in the given interval.
            (true, true)
        } else if !self.upper.is_unbounded()
            && !rhs.lower.is_unbounded()
            && self.upper.value <= rhs.lower.value
            && (self.upper.value < rhs.lower.value || self.upper.open || rhs.lower.open)
        {
            // Values in this interval are certainly less than those in the
            // given interval.
            (false, false)
        } else {
            // All outcomes are possible.
            (false, true)
        };

        Interval::make(Some(flags.0), Some(flags.1), (false, false))
    }

    /// Decide if this interval is certainly less than, possibly less than,
    /// or can't be less than `other` by returning [true, true],
    /// [false, true] or [false, false] respectively.
    pub(crate) fn lt<T: Borrow<Interval>>(&self, other: T) -> Interval {
        other.borrow().gt(self)
    }

    /// Decide if this interval is certainly less than or equal to, possibly
    /// less than or equal to, or can't be less than or equal to `other` by returning
    /// [true, true], [false, true] or [false, false] respectively.
    pub(crate) fn lt_eq<T: Borrow<Interval>>(&self, other: T) -> Interval {
        other.borrow().gt_eq(self)
    }

    /// Decide if this interval is certainly equal to, possibly equal to,
    /// or can't be equal to `other` by returning [true, true],
    /// [false, true] or [false, false] respectively.
    pub(crate) fn equal<T: Borrow<Interval>>(&self, other: T) -> Interval {
        let rhs = other.borrow();
        let flags = if !self.lower.is_unbounded()
            && (self.lower.value == self.upper.value)
            && (rhs.lower.value == rhs.upper.value)
            && (self.lower.value == rhs.lower.value)
        {
            (true, true)
        } else if self.gt(rhs) == Interval::CERTAINLY_TRUE
            || self.lt(rhs) == Interval::CERTAINLY_TRUE
        {
            (false, false)
        } else {
            (false, true)
        };

        Interval::make(Some(flags.0), Some(flags.1), (false, false))
    }

    /// Compute the logical conjunction of this (boolean) interval with the given boolean interval.
    pub(crate) fn and<T: Borrow<Interval>>(&self, other: T) -> Result<Interval> {
        let rhs = other.borrow();
        match (
            &self.lower.value,
            &self.upper.value,
            &rhs.lower.value,
            &rhs.upper.value,
        ) {
            (
                ScalarValue::Boolean(Some(self_lower)),
                ScalarValue::Boolean(Some(self_upper)),
                ScalarValue::Boolean(Some(other_lower)),
                ScalarValue::Boolean(Some(other_upper)),
            ) => {
                let lower = *self_lower && *other_lower;
                let upper = *self_upper && *other_upper;

                Ok(Interval {
                    lower: IntervalBound::new(ScalarValue::Boolean(Some(lower)), false),
                    upper: IntervalBound::new(ScalarValue::Boolean(Some(upper)), false),
                })
            }
            _ => internal_err!("Incompatible types for logical conjunction"),
        }
    }

    /// Compute the logical negation of this (boolean) interval.
    pub(crate) fn not(&self) -> Result<Self> {
        if !matches!(self.get_datatype()?, DataType::Boolean) {
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
    pub(crate) fn intersect<T: Borrow<Interval>>(
        &self,
        other: T,
    ) -> Result<Option<Interval>> {
        let rhs = other.borrow();
        // If it is evident that the result is an empty interval,
        // do not make any calculation and directly return None.
        if (!self.lower.is_unbounded()
            && !rhs.upper.is_unbounded()
            && self.lower.value > rhs.upper.value)
            || (!self.upper.is_unbounded()
                && !rhs.lower.is_unbounded()
                && self.upper.value < rhs.lower.value)
        {
            // This None value signals an empty interval.
            return Ok(None);
        }

        let lower = IntervalBound::choose(&self.lower, &rhs.lower, max)?;
        let upper = IntervalBound::choose(&self.upper, &rhs.upper, min)?;

        let non_empty = lower.is_unbounded()
            || upper.is_unbounded()
            || lower.value != upper.value
            || (!lower.open && !upper.open);
        Ok(non_empty.then_some(Interval::new(lower, upper)))
    }

    /// Decide if this interval is certainly contains, possibly contains,
    /// or can't can't `other` by returning [true, true],
    /// [false, true] or [false, false] respectively.
    pub fn contains<T: Borrow<Self>>(&self, other: T) -> Result<Self> {
        match self.intersect(other.borrow())? {
            Some(intersection) => {
                // Need to compare with same bounds close-ness.
                if intersection.close_bounds() == other.borrow().clone().close_bounds() {
                    Ok(Interval::CERTAINLY_TRUE)
                } else {
                    Ok(Interval::UNCERTAIN)
                }
            }
            None => Ok(Interval::CERTAINLY_FALSE),
        }
    }

    /// Add the given interval (`other`) to this interval. Say we have
    /// intervals [a1, b1] and [a2, b2], then their sum is [a1 + a2, b1 + b2].
    /// Note that this represents all possible values the sum can take if
    /// one can choose single values arbitrarily from each of the operands.
    pub fn add<T: Borrow<Interval>>(&self, other: T) -> Result<Interval> {
        let rhs = other.borrow();
        Ok(Interval::new(
            self.lower.add::<false, _>(&rhs.lower)?,
            self.upper.add::<true, _>(&rhs.upper)?,
        ))
    }

    /// Subtract the given interval (`other`) from this interval. Say we have
    /// intervals [a1, b1] and [a2, b2], then their sum is [a1 - b2, b1 - a2].
    /// Note that this represents all possible values the difference can take
    /// if one can choose single values arbitrarily from each of the operands.
    pub fn sub<T: Borrow<Interval>>(&self, other: T) -> Result<Interval> {
        let rhs = other.borrow();
        Ok(Interval::new(
            self.lower.sub::<false, _>(&rhs.upper)?,
            self.upper.sub::<true, _>(&rhs.lower)?,
        ))
    }

    pub const CERTAINLY_FALSE: Interval = Interval {
        lower: IntervalBound::new_closed(ScalarValue::Boolean(Some(false))),
        upper: IntervalBound::new_closed(ScalarValue::Boolean(Some(false))),
    };

    pub const UNCERTAIN: Interval = Interval {
        lower: IntervalBound::new_closed(ScalarValue::Boolean(Some(false))),
        upper: IntervalBound::new_closed(ScalarValue::Boolean(Some(true))),
    };

    pub const CERTAINLY_TRUE: Interval = Interval {
        lower: IntervalBound::new_closed(ScalarValue::Boolean(Some(true))),
        upper: IntervalBound::new_closed(ScalarValue::Boolean(Some(true))),
    };

    /// Returns the cardinality of this interval, which is the number of all
    /// distinct points inside it. This function returns `None` if:
    /// - The interval is unbounded from either side, or
    /// - Cardinality calculations for the datatype in question is not
    ///   implemented yet, or
    /// - An overflow occurs during the calculation.
    ///
    /// This function returns an error if the given interval is malformed.
    pub fn cardinality(&self) -> Result<Option<u64>> {
        let data_type = self.get_datatype()?;
        if data_type.is_integer() {
            Ok(self.upper.value.distance(&self.lower.value).map(|diff| {
                calculate_cardinality_based_on_bounds(
                    self.lower.open,
                    self.upper.open,
                    diff as u64,
                )
            }))
        }
        // Ordering floating-point numbers according to their binary representations
        // coincide with their natural ordering. Therefore, we can consider their
        // binary representations as "indices" and subtract them. For details, see:
        // https://stackoverflow.com/questions/8875064/how-many-distinct-floating-point-numbers-in-a-specific-range
        else if data_type.is_floating() {
            match (&self.lower.value, &self.upper.value) {
                (
                    ScalarValue::Float32(Some(lower)),
                    ScalarValue::Float32(Some(upper)),
                ) => {
                    // Negative numbers are sorted in the reverse order. To always have a positive difference after the subtraction,
                    // we perform following transformation:
                    let lower_bits = lower.to_bits() as i32;
                    let upper_bits = upper.to_bits() as i32;
                    let transformed_lower =
                        lower_bits ^ ((lower_bits >> 31) & 0x7fffffff);
                    let transformed_upper =
                        upper_bits ^ ((upper_bits >> 31) & 0x7fffffff);
                    let Ok(count) = transformed_upper.sub_checked(transformed_lower)
                    else {
                        return Ok(None);
                    };
                    Ok(Some(calculate_cardinality_based_on_bounds(
                        self.lower.open,
                        self.upper.open,
                        count as u64,
                    )))
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
                        return Ok(None);
                    };
                    Ok(Some(calculate_cardinality_based_on_bounds(
                        self.lower.open,
                        self.upper.open,
                        count as u64,
                    )))
                }
                _ => Ok(None),
            }
        } else {
            // Cardinality calculations are not implemented for this data type yet:
            Ok(None)
        }
    }

    /// This function "closes" this interval; i.e. it modifies the endpoints so
    /// that we end up with the narrowest possible closed interval containing
    /// the original interval.
    pub fn close_bounds(mut self) -> Interval {
        if self.lower.open {
            // Get next value
            self.lower.value = next_value::<true>(self.lower.value);
            self.lower.open = false;
        }

        if self.upper.open {
            // Get previous value
            self.upper.value = next_value::<false>(self.upper.value);
            self.upper.open = false;
        }

        self
    }
}

trait OneTrait: Sized + std::ops::Add + std::ops::Sub {
    fn one() -> Self;
}

macro_rules! impl_OneTrait{
    ($($m:ty),*) => {$( impl OneTrait for $m  { fn one() -> Self { 1 as $m } })*}
}
impl_OneTrait! {u8, u16, u32, u64, i8, i16, i32, i64}

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

macro_rules! check_infinite_bounds {
    ($value:expr, $val:expr, $type:ident, $inc:expr) => {
        if ($val == $type::MAX && $inc) || ($val == $type::MIN && !$inc) {
            return $value;
        }
    };
}

/// This function returns the next/previous value depending on the `ADD` value.
/// If `true`, it returns the next value; otherwise it returns the previous value.
fn next_value<const INC: bool>(value: ScalarValue) -> ScalarValue {
    use ScalarValue::*;
    match value {
        Float32(Some(val)) => {
            let new_float = if INC { next_up(val) } else { next_down(val) };
            Float32(Some(new_float))
        }
        Float64(Some(val)) => {
            let new_float = if INC { next_up(val) } else { next_down(val) };
            Float64(Some(new_float))
        }
        Int8(Some(val)) => {
            check_infinite_bounds!(value, val, i8, INC);
            Int8(Some(increment_decrement::<INC, i8>(val)))
        }
        Int16(Some(val)) => {
            check_infinite_bounds!(value, val, i16, INC);
            Int16(Some(increment_decrement::<INC, i16>(val)))
        }
        Int32(Some(val)) => {
            check_infinite_bounds!(value, val, i32, INC);
            Int32(Some(increment_decrement::<INC, i32>(val)))
        }
        Int64(Some(val)) => {
            check_infinite_bounds!(value, val, i64, INC);
            Int64(Some(increment_decrement::<INC, i64>(val)))
        }
        UInt8(Some(val)) => {
            check_infinite_bounds!(value, val, u8, INC);
            UInt8(Some(increment_decrement::<INC, u8>(val)))
        }
        UInt16(Some(val)) => {
            check_infinite_bounds!(value, val, u16, INC);
            UInt16(Some(increment_decrement::<INC, u16>(val)))
        }
        UInt32(Some(val)) => {
            check_infinite_bounds!(value, val, u32, INC);
            UInt32(Some(increment_decrement::<INC, u32>(val)))
        }
        UInt64(Some(val)) => {
            check_infinite_bounds!(value, val, u64, INC);
            UInt64(Some(increment_decrement::<INC, u64>(val)))
        }
        _ => value, // Unsupported datatypes
    }
}

/// This function computes the selectivity by computing the cardinality ratio of the given intervals.
/// If this can not be calculated for some reasons, returns `1.0` meaning full selective / no filtering.
pub fn cardinality_ratio(
    initial_interval: &Interval,
    final_interval: &Interval,
) -> Result<f64> {
    Ok(
        match (
            final_interval.cardinality()?,
            initial_interval.cardinality()?,
        ) {
            (Some(final_interval), Some(initial_interval)) => {
                final_interval as f64 / initial_interval as f64
            }
            _ => 1.0,
        },
    )
}

pub fn apply_operator(op: &Operator, lhs: &Interval, rhs: &Interval) -> Result<Interval> {
    match *op {
        Operator::Eq => Ok(lhs.equal(rhs)),
        Operator::NotEq => Ok(lhs.equal(rhs).not()?),
        Operator::Gt => Ok(lhs.gt(rhs)),
        Operator::GtEq => Ok(lhs.gt_eq(rhs)),
        Operator::Lt => Ok(lhs.lt(rhs)),
        Operator::LtEq => Ok(lhs.lt_eq(rhs)),
        Operator::And => lhs.and(rhs),
        Operator::Plus => lhs.add(rhs),
        Operator::Minus => lhs.sub(rhs),
        _ => Ok(Interval::default()),
    }
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

/// This function calculates the final cardinality result by inspecting the endpoints of the interval.
fn calculate_cardinality_based_on_bounds(
    lower_open: bool,
    upper_open: bool,
    diff: u64,
) -> u64 {
    match (lower_open, upper_open) {
        (false, false) => diff + 1,
        (true, true) => diff - 1,
        _ => diff,
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
/// use datafusion_physical_expr::intervals::{Interval, NullableInterval};
/// use datafusion_common::ScalarValue;
///
/// // [1, 2) U {NULL}
/// NullableInterval::MaybeNull {
///    values: Interval::make(Some(1), Some(2), (false, true)),
/// };
///
/// // (0, ∞)
/// NullableInterval::NotNull {
///   values: Interval::make(Some(0), None, (true, true)),
/// };
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
                values: Interval::new(
                    IntervalBound::new(value.clone(), false),
                    IntervalBound::new(value, false),
                ),
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
                values.get_datatype()
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
    /// use datafusion_physical_expr::intervals::{Interval, NullableInterval};
    ///
    /// // 4 > 3 -> true
    /// let lhs = NullableInterval::from(ScalarValue::Int32(Some(4)));
    /// let rhs = NullableInterval::from(ScalarValue::Int32(Some(3)));
    /// let result = lhs.apply_operator(&Operator::Gt, &rhs).unwrap();
    /// assert_eq!(result, NullableInterval::from(ScalarValue::Boolean(Some(true))));
    ///
    /// // [1, 3) > NULL -> NULL
    /// let lhs = NullableInterval::NotNull {
    ///     values: Interval::make(Some(1), Some(3), (false, true)),
    /// };
    /// let rhs = NullableInterval::from(ScalarValue::Int32(None));
    /// let result = lhs.apply_operator(&Operator::Gt, &rhs).unwrap();
    /// assert_eq!(result.single_value(), Some(ScalarValue::Boolean(None)));
    ///
    /// // [1, 3] > [2, 4] -> [false, true]
    /// let lhs = NullableInterval::NotNull {
    ///     values: Interval::make(Some(1), Some(3), (false, false)),
    /// };
    /// let rhs = NullableInterval::NotNull {
    ///    values: Interval::make(Some(2), Some(4), (false, false)),
    /// };
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
                                lhs_values.equal(rhs_values).not()?
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
    /// use datafusion_physical_expr::intervals::{Interval, NullableInterval};
    ///
    /// let interval = NullableInterval::from(ScalarValue::Int32(Some(4)));
    /// assert_eq!(interval.single_value(), Some(ScalarValue::Int32(Some(4))));
    ///
    /// let interval = NullableInterval::from(ScalarValue::Int32(None));
    /// assert_eq!(interval.single_value(), Some(ScalarValue::Int32(None)));
    ///
    /// let interval = NullableInterval::MaybeNull {
    ///     values: Interval::make(Some(1), Some(4), (false, true)),
    /// };
    /// assert_eq!(interval.single_value(), None);
    /// ```
    pub fn single_value(&self) -> Option<ScalarValue> {
        match self {
            Self::Null { datatype } => {
                Some(ScalarValue::try_from(datatype).unwrap_or(ScalarValue::Null))
            }
            Self::MaybeNull { values } | Self::NotNull { values }
                if values.lower.value == values.upper.value
                    && !values.lower.is_unbounded() =>
            {
                Some(values.lower.value.clone())
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::next_value;
    use crate::intervals::{Interval, IntervalBound};
    use arrow_schema::DataType;
    use datafusion_common::{Result, ScalarValue};

    fn open_open<T>(lower: Option<T>, upper: Option<T>) -> Interval
    where
        ScalarValue: From<Option<T>>,
    {
        Interval::make(lower, upper, (true, true))
    }

    fn open_closed<T>(lower: Option<T>, upper: Option<T>) -> Interval
    where
        ScalarValue: From<Option<T>>,
    {
        Interval::make(lower, upper, (true, false))
    }

    fn closed_open<T>(lower: Option<T>, upper: Option<T>) -> Interval
    where
        ScalarValue: From<Option<T>>,
    {
        Interval::make(lower, upper, (false, true))
    }

    fn closed_closed<T>(lower: Option<T>, upper: Option<T>) -> Interval
    where
        ScalarValue: From<Option<T>>,
    {
        Interval::make(lower, upper, (false, false))
    }

    #[test]
    fn intersect_test() -> Result<()> {
        let possible_cases = vec![
            (Some(1000_i64), None, None, None, Some(1000_i64), None),
            (None, Some(1000_i64), None, None, None, Some(1000_i64)),
            (None, None, Some(1000_i64), None, Some(1000_i64), None),
            (None, None, None, Some(1000_i64), None, Some(1000_i64)),
            (
                Some(1000_i64),
                None,
                Some(1000_i64),
                None,
                Some(1000_i64),
                None,
            ),
            (
                None,
                Some(1000_i64),
                Some(999_i64),
                Some(1002_i64),
                Some(999_i64),
                Some(1000_i64),
            ),
            (None, None, None, None, None, None),
        ];

        for case in possible_cases {
            assert_eq!(
                open_open(case.0, case.1).intersect(open_open(case.2, case.3))?,
                Some(open_open(case.4, case.5))
            )
        }

        let empty_cases = vec![
            (None, Some(1000_i64), Some(1001_i64), None),
            (Some(1001_i64), None, None, Some(1000_i64)),
            (None, Some(1000_i64), Some(1001_i64), Some(1002_i64)),
            (Some(1001_i64), Some(1002_i64), None, Some(1000_i64)),
        ];

        for case in empty_cases {
            assert_eq!(
                open_open(case.0, case.1).intersect(open_open(case.2, case.3))?,
                None
            )
        }

        Ok(())
    }

    #[test]
    fn gt_test() {
        let cases = vec![
            (Some(1000_i64), None, None, None, false, true),
            (None, Some(1000_i64), None, None, false, true),
            (None, None, Some(1000_i64), None, false, true),
            (None, None, None, Some(1000_i64), false, true),
            (None, Some(1000_i64), Some(1000_i64), None, false, false),
            (None, Some(1000_i64), Some(1001_i64), None, false, false),
            (Some(1000_i64), None, Some(1000_i64), None, false, true),
            (
                None,
                Some(1000_i64),
                Some(1001_i64),
                Some(1002_i64),
                false,
                false,
            ),
            (
                None,
                Some(1000_i64),
                Some(999_i64),
                Some(1002_i64),
                false,
                true,
            ),
            (
                Some(1002_i64),
                None,
                Some(999_i64),
                Some(1002_i64),
                true,
                true,
            ),
            (
                Some(1003_i64),
                None,
                Some(999_i64),
                Some(1002_i64),
                true,
                true,
            ),
            (None, None, None, None, false, true),
        ];

        for case in cases {
            assert_eq!(
                open_open(case.0, case.1).gt(open_open(case.2, case.3)),
                closed_closed(Some(case.4), Some(case.5))
            );
        }
    }

    #[test]
    fn lt_test() {
        let cases = vec![
            (Some(1000_i64), None, None, None, false, true),
            (None, Some(1000_i64), None, None, false, true),
            (None, None, Some(1000_i64), None, false, true),
            (None, None, None, Some(1000_i64), false, true),
            (None, Some(1000_i64), Some(1000_i64), None, true, true),
            (None, Some(1000_i64), Some(1001_i64), None, true, true),
            (Some(1000_i64), None, Some(1000_i64), None, false, true),
            (
                None,
                Some(1000_i64),
                Some(1001_i64),
                Some(1002_i64),
                true,
                true,
            ),
            (
                None,
                Some(1000_i64),
                Some(999_i64),
                Some(1002_i64),
                false,
                true,
            ),
            (None, None, None, None, false, true),
        ];

        for case in cases {
            assert_eq!(
                open_open(case.0, case.1).lt(open_open(case.2, case.3)),
                closed_closed(Some(case.4), Some(case.5))
            );
        }
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
                open_open(Some(case.0), Some(case.1))
                    .and(open_open(Some(case.2), Some(case.3)))?,
                open_open(Some(case.4), Some(case.5))
            );
        }
        Ok(())
    }

    #[test]
    fn add_test() -> Result<()> {
        let cases = vec![
            (Some(1000_i64), None, None, None, None, None),
            (None, Some(1000_i64), None, None, None, None),
            (None, None, Some(1000_i64), None, None, None),
            (None, None, None, Some(1000_i64), None, None),
            (
                Some(1000_i64),
                None,
                Some(1000_i64),
                None,
                Some(2000_i64),
                None,
            ),
            (
                None,
                Some(1000_i64),
                Some(999_i64),
                Some(1002_i64),
                None,
                Some(2002_i64),
            ),
            (None, Some(1000_i64), Some(1000_i64), None, None, None),
            (
                Some(2001_i64),
                Some(1_i64),
                Some(1005_i64),
                Some(-999_i64),
                Some(3006_i64),
                Some(-998_i64),
            ),
            (None, None, None, None, None, None),
        ];

        for case in cases {
            assert_eq!(
                open_open(case.0, case.1).add(open_open(case.2, case.3))?,
                open_open(case.4, case.5)
            );
        }
        Ok(())
    }

    #[test]
    fn sub_test() -> Result<()> {
        let cases = vec![
            (Some(1000_i64), None, None, None, None, None),
            (None, Some(1000_i64), None, None, None, None),
            (None, None, Some(1000_i64), None, None, None),
            (None, None, None, Some(1000_i64), None, None),
            (Some(1000_i64), None, Some(1000_i64), None, None, None),
            (
                None,
                Some(1000_i64),
                Some(999_i64),
                Some(1002_i64),
                None,
                Some(1_i64),
            ),
            (
                None,
                Some(1000_i64),
                Some(1000_i64),
                None,
                None,
                Some(0_i64),
            ),
            (
                Some(2001_i64),
                Some(1000_i64),
                Some(1005),
                Some(999_i64),
                Some(1002_i64),
                Some(-5_i64),
            ),
            (None, None, None, None, None, None),
        ];

        for case in cases {
            assert_eq!(
                open_open(case.0, case.1).sub(open_open(case.2, case.3))?,
                open_open(case.4, case.5)
            );
        }
        Ok(())
    }

    #[test]
    fn sub_test_various_bounds() -> Result<()> {
        let cases = vec![
            (
                closed_closed(Some(100_i64), Some(200_i64)),
                closed_open(Some(200_i64), None),
                open_closed(None, Some(0_i64)),
            ),
            (
                closed_open(Some(100_i64), Some(200_i64)),
                open_closed(Some(300_i64), Some(150_i64)),
                closed_open(Some(-50_i64), Some(-100_i64)),
            ),
            (
                closed_open(Some(100_i64), Some(200_i64)),
                open_open(Some(200_i64), None),
                open_open(None, Some(0_i64)),
            ),
            (
                closed_closed(Some(1_i64), Some(1_i64)),
                closed_closed(Some(11_i64), Some(11_i64)),
                closed_closed(Some(-10_i64), Some(-10_i64)),
            ),
        ];
        for case in cases {
            assert_eq!(case.0.sub(case.1)?, case.2)
        }
        Ok(())
    }

    #[test]
    fn add_test_various_bounds() -> Result<()> {
        let cases = vec![
            (
                closed_closed(Some(100_i64), Some(200_i64)),
                open_closed(None, Some(200_i64)),
                open_closed(None, Some(400_i64)),
            ),
            (
                closed_open(Some(100_i64), Some(200_i64)),
                closed_open(Some(-300_i64), Some(150_i64)),
                closed_open(Some(-200_i64), Some(350_i64)),
            ),
            (
                closed_open(Some(100_i64), Some(200_i64)),
                open_open(Some(200_i64), None),
                open_open(Some(300_i64), None),
            ),
            (
                closed_closed(Some(1_i64), Some(1_i64)),
                closed_closed(Some(11_i64), Some(11_i64)),
                closed_closed(Some(12_i64), Some(12_i64)),
            ),
        ];
        for case in cases {
            assert_eq!(case.0.add(case.1)?, case.2)
        }
        Ok(())
    }

    #[test]
    fn lt_test_various_bounds() -> Result<()> {
        let cases = vec![
            (
                closed_closed(Some(100_i64), Some(200_i64)),
                open_closed(None, Some(100_i64)),
                closed_closed(Some(false), Some(false)),
            ),
            (
                closed_closed(Some(100_i64), Some(200_i64)),
                open_open(None, Some(100_i64)),
                closed_closed(Some(false), Some(false)),
            ),
            (
                open_open(Some(100_i64), Some(200_i64)),
                closed_closed(Some(0_i64), Some(100_i64)),
                closed_closed(Some(false), Some(false)),
            ),
            (
                closed_closed(Some(2_i64), Some(2_i64)),
                closed_closed(Some(1_i64), Some(2_i64)),
                closed_closed(Some(false), Some(false)),
            ),
            (
                closed_closed(Some(2_i64), Some(2_i64)),
                closed_open(Some(1_i64), Some(2_i64)),
                closed_closed(Some(false), Some(false)),
            ),
            (
                closed_closed(Some(1_i64), Some(1_i64)),
                open_open(Some(1_i64), Some(2_i64)),
                closed_closed(Some(true), Some(true)),
            ),
        ];
        for case in cases {
            assert_eq!(case.0.lt(case.1), case.2)
        }
        Ok(())
    }

    #[test]
    fn gt_test_various_bounds() -> Result<()> {
        let cases = vec![
            (
                closed_closed(Some(100_i64), Some(200_i64)),
                open_closed(None, Some(100_i64)),
                closed_closed(Some(false), Some(true)),
            ),
            (
                closed_closed(Some(100_i64), Some(200_i64)),
                open_open(None, Some(100_i64)),
                closed_closed(Some(true), Some(true)),
            ),
            (
                open_open(Some(100_i64), Some(200_i64)),
                closed_closed(Some(0_i64), Some(100_i64)),
                closed_closed(Some(true), Some(true)),
            ),
            (
                closed_closed(Some(2_i64), Some(2_i64)),
                closed_closed(Some(1_i64), Some(2_i64)),
                closed_closed(Some(false), Some(true)),
            ),
            (
                closed_closed(Some(2_i64), Some(2_i64)),
                closed_open(Some(1_i64), Some(2_i64)),
                closed_closed(Some(true), Some(true)),
            ),
            (
                closed_closed(Some(1_i64), Some(1_i64)),
                open_open(Some(1_i64), Some(2_i64)),
                closed_closed(Some(false), Some(false)),
            ),
        ];
        for case in cases {
            assert_eq!(case.0.gt(case.1), case.2)
        }
        Ok(())
    }

    #[test]
    fn lt_eq_test_various_bounds() -> Result<()> {
        let cases = vec![
            (
                closed_closed(Some(100_i64), Some(200_i64)),
                open_closed(None, Some(100_i64)),
                closed_closed(Some(false), Some(true)),
            ),
            (
                closed_closed(Some(100_i64), Some(200_i64)),
                open_open(None, Some(100_i64)),
                closed_closed(Some(false), Some(false)),
            ),
            (
                closed_closed(Some(2_i64), Some(2_i64)),
                closed_closed(Some(1_i64), Some(2_i64)),
                closed_closed(Some(false), Some(true)),
            ),
            (
                closed_closed(Some(2_i64), Some(2_i64)),
                closed_open(Some(1_i64), Some(2_i64)),
                closed_closed(Some(false), Some(false)),
            ),
            (
                closed_closed(Some(1_i64), Some(1_i64)),
                closed_open(Some(1_i64), Some(2_i64)),
                closed_closed(Some(true), Some(true)),
            ),
            (
                closed_closed(Some(1_i64), Some(1_i64)),
                open_open(Some(1_i64), Some(2_i64)),
                closed_closed(Some(true), Some(true)),
            ),
        ];
        for case in cases {
            assert_eq!(case.0.lt_eq(case.1), case.2)
        }
        Ok(())
    }

    #[test]
    fn gt_eq_test_various_bounds() -> Result<()> {
        let cases = vec![
            (
                closed_closed(Some(100_i64), Some(200_i64)),
                open_closed(None, Some(100_i64)),
                closed_closed(Some(true), Some(true)),
            ),
            (
                closed_closed(Some(100_i64), Some(200_i64)),
                open_open(None, Some(100_i64)),
                closed_closed(Some(true), Some(true)),
            ),
            (
                closed_closed(Some(2_i64), Some(2_i64)),
                closed_closed(Some(1_i64), Some(2_i64)),
                closed_closed(Some(true), Some(true)),
            ),
            (
                closed_closed(Some(2_i64), Some(2_i64)),
                closed_open(Some(1_i64), Some(2_i64)),
                closed_closed(Some(true), Some(true)),
            ),
            (
                closed_closed(Some(1_i64), Some(1_i64)),
                closed_open(Some(1_i64), Some(2_i64)),
                closed_closed(Some(false), Some(true)),
            ),
            (
                closed_closed(Some(1_i64), Some(1_i64)),
                open_open(Some(1_i64), Some(2_i64)),
                closed_closed(Some(false), Some(false)),
            ),
        ];
        for case in cases {
            assert_eq!(case.0.gt_eq(case.1), case.2)
        }
        Ok(())
    }

    #[test]
    fn intersect_test_various_bounds() -> Result<()> {
        let cases = vec![
            (
                closed_closed(Some(100_i64), Some(200_i64)),
                open_closed(None, Some(100_i64)),
                Some(closed_closed(Some(100_i64), Some(100_i64))),
            ),
            (
                closed_closed(Some(100_i64), Some(200_i64)),
                open_open(None, Some(100_i64)),
                None,
            ),
            (
                open_open(Some(100_i64), Some(200_i64)),
                closed_closed(Some(0_i64), Some(100_i64)),
                None,
            ),
            (
                closed_closed(Some(2_i64), Some(2_i64)),
                closed_closed(Some(1_i64), Some(2_i64)),
                Some(closed_closed(Some(2_i64), Some(2_i64))),
            ),
            (
                closed_closed(Some(2_i64), Some(2_i64)),
                closed_open(Some(1_i64), Some(2_i64)),
                None,
            ),
            (
                closed_closed(Some(1_i64), Some(1_i64)),
                open_open(Some(1_i64), Some(2_i64)),
                None,
            ),
            (
                closed_closed(Some(1_i64), Some(3_i64)),
                open_open(Some(1_i64), Some(2_i64)),
                Some(open_open(Some(1_i64), Some(2_i64))),
            ),
        ];
        for case in cases {
            assert_eq!(case.0.intersect(case.1)?, case.2)
        }
        Ok(())
    }

    // This function tests if valid constructions produce standardized objects
    // ([false, false], [false, true], [true, true]) for boolean intervals.
    #[test]
    fn non_standard_interval_constructs() {
        use ScalarValue::Boolean;
        let cases = vec![
            (
                IntervalBound::new(Boolean(None), true),
                IntervalBound::new(Boolean(Some(true)), false),
                closed_closed(Some(false), Some(true)),
            ),
            (
                IntervalBound::new(Boolean(None), true),
                IntervalBound::new(Boolean(Some(true)), true),
                closed_closed(Some(false), Some(false)),
            ),
            (
                IntervalBound::new(Boolean(Some(false)), false),
                IntervalBound::new(Boolean(None), true),
                closed_closed(Some(false), Some(true)),
            ),
            (
                IntervalBound::new(Boolean(Some(true)), false),
                IntervalBound::new(Boolean(None), true),
                closed_closed(Some(true), Some(true)),
            ),
            (
                IntervalBound::new(Boolean(None), true),
                IntervalBound::new(Boolean(None), true),
                closed_closed(Some(false), Some(true)),
            ),
            (
                IntervalBound::new(Boolean(Some(false)), true),
                IntervalBound::new(Boolean(None), true),
                closed_closed(Some(true), Some(true)),
            ),
        ];

        for case in cases {
            assert_eq!(Interval::new(case.0, case.1), case.2)
        }
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
                Interval::make(Some(lower as $TYPE), Some(upper as $TYPE), (true, true))
            }

            fn $TEST_FN_NAME(input: ($TYPE, $TYPE), expect_low: bool, expect_high: bool) {
                assert!(expect_low || expect_high);
                let interval1 = $CREATE_FN_NAME(input.0, input.0);
                let interval2 = $CREATE_FN_NAME(input.1, input.1);
                let result = interval1.add(&interval2).unwrap();
                let without_fe = $CREATE_FN_NAME(input.0 + input.1, input.0 + input.1);
                assert!(
                    (!expect_low || result.lower.value < without_fe.lower.value)
                        && (!expect_high || result.upper.value > without_fe.upper.value)
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
    fn test_cardinality_of_intervals() -> Result<()> {
        // In IEEE 754 standard for floating-point arithmetic, if we keep the sign and exponent fields same,
        // we can represent 4503599627370496 different numbers by changing the mantissa
        // (4503599627370496 = 2^52, since there are 52 bits in mantissa, and 2^23 = 8388608 for f32).
        let distinct_f64 = 4503599627370496;
        let distinct_f32 = 8388608;
        let intervals = [
            Interval::new(
                IntervalBound::new(ScalarValue::from(0.25), false),
                IntervalBound::new(ScalarValue::from(0.50), true),
            ),
            Interval::new(
                IntervalBound::new(ScalarValue::from(0.5), false),
                IntervalBound::new(ScalarValue::from(1.0), true),
            ),
            Interval::new(
                IntervalBound::new(ScalarValue::from(1.0), false),
                IntervalBound::new(ScalarValue::from(2.0), true),
            ),
            Interval::new(
                IntervalBound::new(ScalarValue::from(32.0), false),
                IntervalBound::new(ScalarValue::from(64.0), true),
            ),
            Interval::new(
                IntervalBound::new(ScalarValue::from(-0.50), false),
                IntervalBound::new(ScalarValue::from(-0.25), true),
            ),
            Interval::new(
                IntervalBound::new(ScalarValue::from(-32.0), false),
                IntervalBound::new(ScalarValue::from(-16.0), true),
            ),
        ];
        for interval in intervals {
            assert_eq!(interval.cardinality()?.unwrap(), distinct_f64);
        }

        let intervals = [
            Interval::new(
                IntervalBound::new(ScalarValue::from(0.25_f32), false),
                IntervalBound::new(ScalarValue::from(0.50_f32), true),
            ),
            Interval::new(
                IntervalBound::new(ScalarValue::from(-1_f32), false),
                IntervalBound::new(ScalarValue::from(-0.5_f32), true),
            ),
        ];
        for interval in intervals {
            assert_eq!(interval.cardinality()?.unwrap(), distinct_f32);
        }

        // The regular logarithmic distribution of floating-point numbers are only applicable
        // outside of the `(-phi, phi)` interval, where `phi` denotes the largest positive
        // subnormal floating-point number. Since the following intervals include these subnormal
        // points, we cannot use the constant number that remains the same in powers of 2. Therefore,
        // we manually supply the actual expected cardinality.
        let interval = Interval::new(
            IntervalBound::new(ScalarValue::from(-0.0625), false),
            IntervalBound::new(ScalarValue::from(0.0625), true),
        );
        assert_eq!(interval.cardinality()?.unwrap(), 9178336040581070849);

        let interval = Interval::new(
            IntervalBound::new(ScalarValue::from(-0.0625_f32), false),
            IntervalBound::new(ScalarValue::from(0.0625_f32), true),
        );
        assert_eq!(interval.cardinality()?.unwrap(), 2063597569);

        Ok(())
    }

    #[test]
    fn test_next_value() -> Result<()> {
        // integer increment / decrement
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
            assert_eq!(next_value::<true>(z.clone()), o);
            assert_eq!(next_value::<false>(o), z);
        });

        // floating value increment / decrement
        let values = vec![
            ScalarValue::new_zero(&DataType::Float32)?,
            ScalarValue::new_zero(&DataType::Float64)?,
        ];

        let eps = vec![
            ScalarValue::Float32(Some(1e-6)),
            ScalarValue::Float64(Some(1e-6)),
        ];

        values.into_iter().zip(eps).for_each(|(v, e)| {
            assert!(next_value::<true>(v.clone()).sub(v.clone()).unwrap().lt(&e));
            assert!(v.clone().sub(next_value::<false>(v)).unwrap().lt(&e));
        });

        // Min / Max values do not change for integer values
        let min = vec![
            ScalarValue::UInt64(Some(u64::MIN)),
            ScalarValue::Int8(Some(i8::MIN)),
        ];
        let max = vec![
            ScalarValue::UInt64(Some(u64::MAX)),
            ScalarValue::Int8(Some(i8::MAX)),
        ];

        min.into_iter().zip(max).for_each(|(min, max)| {
            assert_eq!(next_value::<true>(max.clone()), max);
            assert_eq!(next_value::<false>(min.clone()), min);
        });

        // Min / Max values results in infinity for floating point values
        assert_eq!(
            next_value::<true>(ScalarValue::Float32(Some(f32::MAX))),
            ScalarValue::Float32(Some(f32::INFINITY))
        );
        assert_eq!(
            next_value::<false>(ScalarValue::Float64(Some(f64::MIN))),
            ScalarValue::Float64(Some(f64::NEG_INFINITY))
        );

        Ok(())
    }

    #[test]
    fn test_interval_display() {
        let interval = Interval::new(
            IntervalBound::new(ScalarValue::from(0.25_f32), true),
            IntervalBound::new(ScalarValue::from(0.50_f32), false),
        );
        assert_eq!(format!("{}", interval), "(0.25, 0.5]");

        let interval = Interval::new(
            IntervalBound::new(ScalarValue::from(0.25_f32), false),
            IntervalBound::new(ScalarValue::from(0.50_f32), true),
        );
        assert_eq!(format!("{}", interval), "[0.25, 0.5)");

        let interval = Interval::new(
            IntervalBound::new(ScalarValue::from(0.25_f32), true),
            IntervalBound::new(ScalarValue::from(0.50_f32), true),
        );
        assert_eq!(format!("{}", interval), "(0.25, 0.5)");

        let interval = Interval::new(
            IntervalBound::new(ScalarValue::from(0.25_f32), false),
            IntervalBound::new(ScalarValue::from(0.50_f32), false),
        );
        assert_eq!(format!("{}", interval), "[0.25, 0.5]");
    }
}
