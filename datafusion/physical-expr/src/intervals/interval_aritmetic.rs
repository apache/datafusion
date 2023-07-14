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
use std::fmt;
use std::fmt::{Display, Formatter};
use std::ops::{AddAssign, SubAssign};

use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::DataType;
use arrow_array::ArrowNativeTypeOp;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::type_coercion::binary::get_result_type;
use datafusion_expr::Operator;

use crate::aggregate::min_max::{max, min};
use crate::intervals::rounding::alter_fp_rounding_mode;

/// This type represents a single endpoint of an [`Interval`]. An endpoint can
/// be open or closed, denoting whether the interval includes or excludes the
/// endpoint itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntervalBound {
    pub value: ScalarValue,
    pub open: bool,
}

impl IntervalBound {
    /// Creates a new `IntervalBound` object using the given value.
    pub const fn new(value: ScalarValue, open: bool) -> IntervalBound {
        IntervalBound { value, open }
    }

    /// This convenience function creates an unbounded interval endpoint.
    pub fn make_unbounded<T: Borrow<DataType>>(data_type: T) -> Result<Self> {
        ScalarValue::try_from(data_type.borrow()).map(|v| IntervalBound::new(v, true))
    }

    /// This convenience function returns the data type associated with this
    /// `IntervalBound`.
    pub fn get_datatype(&self) -> DataType {
        self.value.get_datatype()
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
/// bounds for expressions. Currently, we only support addition and
/// subtraction, but more capabilities will be added in the future.
/// Upper/lower bounds having NULL values indicate an unbounded side. For
/// example; [10, 20], [10, ∞), (-∞, 100] and (-∞, ∞) are all valid intervals.
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
        write!(f, "Interval [{}, {}]", self.lower, self.upper)
    }
}

impl Interval {
    /// Creates a new interval object using the given bounds.
    /// For boolean intervals, having an open false lower bound is equivalent
    /// to having a true closed lower bound. Similarly, open true upper bound
    /// is equivalent to having a false closed upper bound. Also for boolean
    /// intervals, having an unbounded left endpoint is equivalent to having a
    /// false closed lower bound, while having an unbounded right endpoint is
    /// equivalent to having a true closed upper bound. Therefore; input
    /// parameters to construct an Interval can have different types, but they
    /// all result in [false, false], [false, true] or [true, true].
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
    pub(crate) fn get_datatype(&self) -> Result<DataType> {
        let lower_type = self.lower.get_datatype();
        let upper_type = self.upper.get_datatype();
        if lower_type == upper_type {
            Ok(lower_type)
        } else {
            Err(DataFusionError::Internal(format!(
                "Interval bounds have different types: {lower_type} != {upper_type}",
            )))
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
            _ => Err(DataFusionError::Internal(
                "Incompatible types for logical conjunction".to_string(),
            )),
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
        lower: IntervalBound::new(ScalarValue::Boolean(Some(false)), false),
        upper: IntervalBound::new(ScalarValue::Boolean(Some(false)), false),
    };

    pub const UNCERTAIN: Interval = Interval {
        lower: IntervalBound::new(ScalarValue::Boolean(Some(false)), false),
        upper: IntervalBound::new(ScalarValue::Boolean(Some(true)), false),
    };

    pub const CERTAINLY_TRUE: Interval = Interval {
        lower: IntervalBound::new(ScalarValue::Boolean(Some(true)), false),
        upper: IntervalBound::new(ScalarValue::Boolean(Some(true)), false),
    };

    // Cardinality is the number of all points included by the interval, considering its bounds.
    pub fn cardinality(&self) -> Result<u64> {
        match self.get_datatype() {
            Ok(data_type) if data_type.is_integer() => {
                if let Some(diff) = self.upper.value.distance(&self.lower.value) {
                    Ok(calculate_cardinality_based_on_bounds(
                        self.lower.open,
                        self.upper.open,
                        diff as u64,
                    ))
                } else {
                    Err(DataFusionError::Execution(format!(
                        "Cardinality cannot be calculated for {:?}",
                        self
                    )))
                }
            }
            // Since the floating-point numbers are ordered in the same order as their binary representation,
            // we can consider their binary representations as "indices" and subtract them.
            // https://stackoverflow.com/questions/8875064/how-many-distinct-floating-point-numbers-in-a-specific-range
            Ok(data_type) if data_type.is_floating() => {
                // If the minimum value is a negative number, we need to
                // switch sides to ensure an unsigned result.
                let (min, max) = if self.lower.value
                    < ScalarValue::new_zero(&self.lower.value.get_datatype())?
                {
                    (self.upper.value.clone(), self.lower.value.clone())
                } else {
                    (self.lower.value.clone(), self.upper.value.clone())
                };

                match (min, max) {
                    (
                        ScalarValue::Float32(Some(lower)),
                        ScalarValue::Float32(Some(upper)),
                    ) => Ok(calculate_cardinality_based_on_bounds(
                        self.lower.open,
                        self.upper.open,
                        (upper.to_bits().sub_checked(lower.to_bits()))? as u64,
                    )),
                    (
                        ScalarValue::Float64(Some(lower)),
                        ScalarValue::Float64(Some(upper)),
                    ) => Ok(calculate_cardinality_based_on_bounds(
                        self.lower.open,
                        self.upper.open,
                        upper.to_bits().sub_checked(lower.to_bits())?,
                    )),
                    _ => Err(DataFusionError::Execution(format!(
                        "Cardinality cannot be calculated for the datatype {:?}",
                        data_type
                    ))),
                }
            }
            // If the cardinality cannot be calculated anyway, give an error.
            _ => Err(DataFusionError::Execution(format!(
                "Cardinality cannot be calculated for {:?}",
                self
            ))),
        }
    }
}

pub fn cardinality_ratio(
    initial_interval: &Interval,
    final_interval: &Interval,
) -> Result<f64> {
    Ok(final_interval.cardinality()? as f64 / initial_interval.cardinality()? as f64)
}

/// Indicates whether interval arithmetic is supported for the given operator.
pub fn is_operator_supported(op: &Operator) -> bool {
    matches!(
        op,
        &Operator::Plus
            | &Operator::Minus
            | &Operator::And
            | &Operator::Gt
            | &Operator::GtEq
            | &Operator::Lt
            | &Operator::LtEq
            | &Operator::Eq
    )
}

/// Indicates whether interval arithmetic is supported for the given data type.
pub fn is_datatype_supported(data_type: &DataType) -> bool {
    matches!(
        data_type,
        &DataType::Int64
            | &DataType::Int32
            | &DataType::Int16
            | &DataType::Int8
            | &DataType::UInt64
            | &DataType::UInt32
            | &DataType::UInt16
            | &DataType::UInt8
            | &DataType::Float64
            | &DataType::Float32
    )
}

pub fn apply_operator(op: &Operator, lhs: &Interval, rhs: &Interval) -> Result<Interval> {
    match *op {
        Operator::Eq => Ok(lhs.equal(rhs)),
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

trait OneTrait: Sized + std::ops::Add + std::ops::Sub {
    fn one() -> Self;
}

macro_rules! impl_OneTrait{
    ($($m:ty),*) => {$( impl OneTrait for $m  { fn one() -> Self { 1 as $m } })*}
}
impl_OneTrait! {u8, u16, u32, u64, i8, i16, i32, i64, f32, f64}

/// This function either increments or decrements its argument, depending on the `DIR` value. If `true`, it increments; otherwise it decrements the argument.
fn next_value<const DIR: bool, T: OneTrait + SubAssign + AddAssign>(mut val: T) -> T {
    if DIR {
        val.add_assign(T::one());
    } else {
        val.sub_assign(T::one());
    }
    val
}

/// This function returns the next/previous value depending on the `DIR` value.
/// If `true`, it returns the next value; otherwise it returns the previous value.
fn get_interval_with_next_value<const DIR: bool>(value: ScalarValue) -> ScalarValue {
    use ScalarValue::*;
    match value {
        Float32(Some(val)) => {
            let incremented_bits = next_value::<DIR, u32>(val.to_bits());
            Float32(Some(f32::from_bits(incremented_bits)))
        }
        Float64(Some(val)) => {
            let incremented_bits = next_value::<DIR, u64>(val.to_bits());
            Float64(Some(f64::from_bits(incremented_bits)))
        }
        Int8(Some(val)) => Int8(Some(next_value::<DIR, i8>(val))),
        Int16(Some(val)) => Int16(Some(next_value::<DIR, i16>(val))),
        Int32(Some(val)) => Int32(Some(next_value::<DIR, i32>(val))),
        Int64(Some(val)) => Int64(Some(next_value::<DIR, i64>(val))),
        UInt8(Some(val)) => UInt8(Some(next_value::<DIR, u8>(val))),
        UInt16(Some(val)) => UInt16(Some(next_value::<DIR, u16>(val))),
        UInt32(Some(val)) => UInt32(Some(next_value::<DIR, u32>(val))),
        UInt64(Some(val)) => UInt64(Some(next_value::<DIR, u64>(val))),
        _ => value, // Infinite bounds or unsupported datatypes
    }
}

/// This function takes an interval, and if it has open bound/s, the function
/// converts them to closed bounds preserving the interval values.
pub fn interval_with_closed_bounds(mut interval: Interval) -> Interval {
    if interval.lower.open {
        // Get next value
        interval.lower.value = get_interval_with_next_value::<true>(interval.lower.value);
        interval.lower.open = false;
    }

    if interval.upper.open {
        // Get previous value
        interval.upper.value =
            get_interval_with_next_value::<false>(interval.upper.value);
        interval.upper.open = false;
    }

    interval
}

#[cfg(test)]
mod tests {
    use crate::intervals::{Interval, IntervalBound};
    use datafusion_common::{Result, ScalarValue};
    use ScalarValue::Boolean;

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
            assert_eq!(interval.cardinality()?, distinct_f64);
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
            assert_eq!(interval.cardinality()?, distinct_f32);
        }

        let interval = Interval::new(
            IntervalBound::new(ScalarValue::from(-0.0625), false),
            IntervalBound::new(ScalarValue::from(0.0625), true),
        );
        assert_eq!(interval.cardinality()?, distinct_f64 * 2_048);

        let interval = Interval::new(
            IntervalBound::new(ScalarValue::from(-0.0625_f32), false),
            IntervalBound::new(ScalarValue::from(0.0625_f32), true),
        );
        assert_eq!(interval.cardinality()?, distinct_f32 * 256);

        Ok(())
    }
}
