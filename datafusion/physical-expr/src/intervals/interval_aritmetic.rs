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

use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::Operator;

use crate::aggregate::min_max::{max, min};

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
    pub fn add<T: Borrow<IntervalBound>>(&self, other: T) -> Result<IntervalBound> {
        let rhs = other.borrow();
        if self.is_unbounded() || rhs.is_unbounded() {
            IntervalBound::make_unbounded(self.get_datatype())
        } else {
            self.value
                .add(&rhs.value)
                .map(|v| IntervalBound::new(v, self.open || rhs.open))
        }
    }

    /// This function subtracts the given `IntervalBound` from `self`.
    /// The result is unbounded if either is; otherwise, their values are
    /// subtracted. The result is closed if both original bounds are closed,
    /// or open otherwise.
    pub fn sub<T: Borrow<IntervalBound>>(&self, other: T) -> Result<IntervalBound> {
        let rhs = other.borrow();
        if self.is_unbounded() || rhs.is_unbounded() {
            IntervalBound::make_unbounded(self.get_datatype())
        } else {
            self.value
                .sub(&rhs.value)
                .map(|v| IntervalBound::new(v, self.open || rhs.open))
        }
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
    pub fn new(lower: IntervalBound, upper: IntervalBound) -> Interval {
        Interval { lower, upper }
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
                "Interval bounds have different types: {} != {}",
                lower_type, upper_type,
            )))
        }
    }

    /// Decide if this interval is certainly greater than, possibly greater than,
    /// or can't be greater than `other` by returning [true, true],
    /// [false, true] or [false, false] respectively.
    pub(crate) fn gt(&self, other: &Interval) -> Interval {
        let flags = if !self.upper.is_unbounded()
            && !other.lower.is_unbounded()
            && self.upper.value <= other.lower.value
        {
            // Values in this interval are certainly less than or equal to those
            // in the given interval.
            (false, false)
        } else if !self.lower.is_unbounded()
            && !other.upper.is_unbounded()
            && self.lower.value >= other.upper.value
            && (self.lower.value > other.upper.value
                || self.lower.open
                || other.upper.open)
        {
            // Values in this interval are certainly greater than those in the
            // given interval.
            (true, true)
        } else {
            // All outcomes are possible.
            (false, true)
        };

        Interval::new(
            IntervalBound::new(ScalarValue::Boolean(Some(flags.0)), false),
            IntervalBound::new(ScalarValue::Boolean(Some(flags.1)), false),
        )
    }

    /// Decide if this interval is certainly greater than or equal to, possibly greater than
    /// or equal to, or can't be greater than or equal to `other` by returning [true, true],
    /// [false, true] or [false, false] respectively.
    pub(crate) fn gt_eq(&self, other: &Interval) -> Interval {
        let flags = if !self.lower.is_unbounded()
            && !other.upper.is_unbounded()
            && self.lower.value >= other.upper.value
        {
            // Values in this interval are certainly greater than or equal to those
            // in the given interval.
            (true, true)
        } else if !self.upper.is_unbounded()
            && !other.lower.is_unbounded()
            && self.upper.value <= other.lower.value
            && (self.upper.value < other.lower.value
                || self.upper.open
                || other.lower.open)
        {
            // Values in this interval are certainly less than those in the
            // given interval.
            (false, false)
        } else {
            // All outcomes are possible.
            (false, true)
        };

        Interval::new(
            IntervalBound::new(ScalarValue::Boolean(Some(flags.0)), false),
            IntervalBound::new(ScalarValue::Boolean(Some(flags.1)), false),
        )
    }

    /// Decide if this interval is certainly less than, possibly less than,
    /// or can't be less than `other` by returning [true, true],
    /// [false, true] or [false, false] respectively.
    pub(crate) fn lt(&self, other: &Interval) -> Interval {
        other.gt(self)
    }

    /// Decide if this interval is certainly less than or equal to, possibly
    /// less than or equal to, or can't be less than or equal to `other` by returning
    /// [true, true], [false, true] or [false, false] respectively.
    pub(crate) fn lt_eq(&self, other: &Interval) -> Interval {
        other.gt_eq(self)
    }

    /// Decide if this interval is certainly equal to, possibly equal to,
    /// or can't be equal to `other` by returning [true, true],
    /// [false, true] or [false, false] respectively.    
    pub(crate) fn equal(&self, other: &Interval) -> Interval {
        let flags = if !self.lower.is_unbounded()
            && (self.lower.value == self.upper.value)
            && (other.lower.value == other.upper.value)
            && (self.lower.value == other.lower.value)
        {
            (true, true)
        } else if self.gt(other) == Interval::CERTAINLY_TRUE
            || self.lt(other) == Interval::CERTAINLY_TRUE
        {
            (false, false)
        } else {
            (false, true)
        };

        Interval::new(
            IntervalBound::new(ScalarValue::Boolean(Some(flags.0)), false),
            IntervalBound::new(ScalarValue::Boolean(Some(flags.1)), false),
        )
    }

    /// Compute the logical conjunction of this (boolean) interval with the given boolean interval.
    pub(crate) fn and(&self, other: &Interval) -> Result<Interval> {
        let (self_lower_value, self_upper_value) =
            standardize_boolean_bound(&self.lower, &self.upper)?;
        let (other_lower_value, other_upper_value) =
            standardize_boolean_bound(&other.lower, &other.upper)?;

        let lower = self_lower_value && other_lower_value;
        let upper = self_upper_value && other_upper_value;

        Ok(Interval {
            lower: IntervalBound::new(ScalarValue::Boolean(Some(lower)), false),
            upper: IntervalBound::new(ScalarValue::Boolean(Some(upper)), false),
        })
    }

    /// Compute the intersection of the interval with the given interval.
    /// If the intersection is empty, return None.
    pub(crate) fn intersect(&self, other: &Interval) -> Result<Option<Interval>> {
        // If it is evident that the result is an empty interval,
        // do not make any calculation and directly return None.
        if (!self.lower.is_unbounded()
            && !other.upper.is_unbounded()
            && self.lower.value > other.upper.value)
            || (!self.upper.is_unbounded()
                && !other.lower.is_unbounded()
                && self.upper.value < other.lower.value)
        {
            // This None value signals an empty interval.
            return Ok(None);
        }

        let lower = IntervalBound::choose(&self.lower, &other.lower, max)?;
        let upper = IntervalBound::choose(&self.upper, &other.upper, min)?;

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
            self.lower.add(&rhs.lower)?,
            self.upper.add(&rhs.upper)?,
        ))
    }

    /// Subtract the given interval (`other`) from this interval. Say we have
    /// intervals [a1, b1] and [a2, b2], then their sum is [a1 - b2, b1 - a2].
    /// Note that this represents all possible values the difference can take
    /// if one can choose single values arbitrarily from each of the operands.
    pub fn sub<T: Borrow<Interval>>(&self, other: T) -> Result<Interval> {
        let rhs = other.borrow();
        Ok(Interval::new(
            self.lower.sub(&rhs.upper)?,
            self.upper.sub(&rhs.lower)?,
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

/// For boolean intervals, having an "(false, ..." (open false lower bound) is
/// equivalent to having a "[true, ..."  (true closed lower bound). Similarly,
/// having an "..., true)" (open true upper bound) is equivalent to having
/// a "..., false] (false closed upper bound). Open true lower bounds and open false
/// upper bounds are impossible. Also for boolean intervals, having a None (infinite)
/// IntervalBound should mean "[false, ..." for lower bounds and  "..., true]" for
/// upper bounds. This function standardizes all acceptable matches to [lhs, rhs],
/// where lhs cannot be true while rhs is false.
#[inline]
fn standardize_boolean_bound(
    lhs: &IntervalBound,
    rhs: &IntervalBound,
) -> Result<(bool, bool)> {
    let err = Err(DataFusionError::Internal(
        "Open true lower bounds and open false upper bounds are impossible for an interval."
            .to_string(),
    ));
    let err_none = Err(DataFusionError::Internal(
        "Intervals cannot have a None (infinite) value bounded by a closed bound."
            .to_string(),
    ));
    match (lhs.open, lhs.value.clone(), rhs.value.clone(), rhs.open) {
        (
            false,
            ScalarValue::Boolean(Some(false)),
            ScalarValue::Boolean(Some(false)),
            false,
        )
        | (
            false,
            ScalarValue::Boolean(Some(false)),
            ScalarValue::Boolean(Some(true)),
            true,
        ) => Ok((false, false)),
        (
            false,
            ScalarValue::Boolean(Some(false)),
            ScalarValue::Boolean(Some(true)),
            false,
        ) => Ok((false, true)),
        (
            false,
            ScalarValue::Boolean(Some(true)),
            ScalarValue::Boolean(Some(true)),
            false,
        )
        | (
            true,
            ScalarValue::Boolean(Some(false)),
            ScalarValue::Boolean(Some(true)),
            false,
        ) => Ok((true, true)),
        (true, ScalarValue::Boolean(None), value, open) => Ok(standardize_boolean_bound(
            &IntervalBound::new(ScalarValue::Boolean(Some(false)), false),
            &IntervalBound::new(value, open),
        )?),
        (open, value, ScalarValue::Boolean(None), true) => Ok(standardize_boolean_bound(
            &IntervalBound::new(value, open),
            &IntervalBound::new(ScalarValue::Boolean(Some(true)), false),
        )?),
        (false, ScalarValue::Boolean(None), _, _)
        | (_, _, ScalarValue::Boolean(None), false) => err_none,
        (_, _, _, _) => err,
    }
}

#[cfg(test)]
mod tests {
    use crate::intervals::{Interval, IntervalBound};
    use datafusion_common::{Result, ScalarValue};
    use ScalarValue::{Boolean, Int64};

    #[test]
    fn intersect_test() -> Result<()> {
        let possible_cases = vec![
            (Some(1000), None, None, None, Some(1000), None),
            (None, Some(1000), None, None, None, Some(1000)),
            (None, None, Some(1000), None, Some(1000), None),
            (None, None, None, Some(1000), None, Some(1000)),
            (Some(1000), None, Some(1000), None, Some(1000), None),
            (
                None,
                Some(1000),
                Some(999),
                Some(1002),
                Some(999),
                Some(1000),
            ),
            (None, None, None, None, None, None),
        ];

        for case in possible_cases {
            assert_eq!(
                Interval::new(
                    IntervalBound::new(ScalarValue::Int64(case.0), true),
                    IntervalBound::new(ScalarValue::Int64(case.1), true),
                )
                .intersect(&Interval::new(
                    IntervalBound::new(ScalarValue::Int64(case.2), true),
                    IntervalBound::new(ScalarValue::Int64(case.3), true)
                ))?
                .unwrap(),
                Interval::new(
                    IntervalBound::new(ScalarValue::Int64(case.4), true),
                    IntervalBound::new(ScalarValue::Int64(case.5), true),
                )
            )
        }

        let empty_cases = vec![
            (None, Some(1000), Some(1001), None),
            (Some(1001), None, None, Some(1000)),
            (None, Some(1000), Some(1001), Some(1002)),
            (Some(1001), Some(1002), None, Some(1000)),
        ];

        for case in empty_cases {
            assert_eq!(
                Interval::new(
                    IntervalBound::new(ScalarValue::Int64(case.0), true),
                    IntervalBound::new(ScalarValue::Int64(case.1), true),
                )
                .intersect(&Interval::new(
                    IntervalBound::new(ScalarValue::Int64(case.2), true),
                    IntervalBound::new(ScalarValue::Int64(case.3), true),
                ))?,
                None
            )
        }

        Ok(())
    }

    #[test]
    fn gt_test() {
        let cases = vec![
            (Some(1000), None, None, None, false, true),
            (None, Some(1000), None, None, false, true),
            (None, None, Some(1000), None, false, true),
            (None, None, None, Some(1000), false, true),
            (None, Some(1000), Some(1000), None, false, false),
            (None, Some(1000), Some(1001), None, false, false),
            (Some(1000), None, Some(1000), None, false, true),
            (None, Some(1000), Some(1001), Some(1002), false, false),
            (None, Some(1000), Some(999), Some(1002), false, true),
            (Some(1002), None, Some(999), Some(1002), true, true),
            (Some(1003), None, Some(999), Some(1002), true, true),
            (None, None, None, None, false, true),
        ];

        for case in cases {
            assert_eq!(
                Interval::new(
                    IntervalBound::new(ScalarValue::Int64(case.0), true),
                    IntervalBound::new(ScalarValue::Int64(case.1), true),
                )
                .gt(&Interval::new(
                    IntervalBound::new(ScalarValue::Int64(case.2), true),
                    IntervalBound::new(ScalarValue::Int64(case.3), true),
                )),
                Interval::new(
                    IntervalBound::new(ScalarValue::Boolean(Some(case.4)), false),
                    IntervalBound::new(ScalarValue::Boolean(Some(case.5)), false),
                )
            )
        }
    }

    #[test]
    fn lt_test() {
        let cases = vec![
            (Some(1000), None, None, None, false, true),
            (None, Some(1000), None, None, false, true),
            (None, None, Some(1000), None, false, true),
            (None, None, None, Some(1000), false, true),
            (None, Some(1000), Some(1000), None, true, true),
            (None, Some(1000), Some(1001), None, true, true),
            (Some(1000), None, Some(1000), None, false, true),
            (None, Some(1000), Some(1001), Some(1002), true, true),
            (None, Some(1000), Some(999), Some(1002), false, true),
            (None, None, None, None, false, true),
        ];

        for case in cases {
            assert_eq!(
                Interval::new(
                    IntervalBound::new(ScalarValue::Int64(case.0), true),
                    IntervalBound::new(ScalarValue::Int64(case.1), true),
                )
                .lt(&Interval::new(
                    IntervalBound::new(ScalarValue::Int64(case.2), true),
                    IntervalBound::new(ScalarValue::Int64(case.3), true),
                )),
                Interval::new(
                    IntervalBound::new(ScalarValue::Boolean(Some(case.4)), false),
                    IntervalBound::new(ScalarValue::Boolean(Some(case.5)), false),
                ),
            )
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
                Interval::new(
                    IntervalBound::new(ScalarValue::Boolean(Some(case.0)), false),
                    IntervalBound::new(ScalarValue::Boolean(Some(case.1)), false),
                )
                .and(&Interval::new(
                    IntervalBound::new(ScalarValue::Boolean(Some(case.2)), false),
                    IntervalBound::new(ScalarValue::Boolean(Some(case.3)), false),
                ))?,
                Interval::new(
                    IntervalBound::new(ScalarValue::Boolean(Some(case.4)), false),
                    IntervalBound::new(ScalarValue::Boolean(Some(case.5)), false),
                )
            )
        }
        Ok(())
    }

    #[test]
    fn add_test() -> Result<()> {
        let cases = vec![
            (Some(1000), None, None, None, None, None),
            (None, Some(1000), None, None, None, None),
            (None, None, Some(1000), None, None, None),
            (None, None, None, Some(1000), None, None),
            (Some(1000), None, Some(1000), None, Some(2000), None),
            (None, Some(1000), Some(999), Some(1002), None, Some(2002)),
            (None, Some(1000), Some(1000), None, None, None),
            (
                Some(2001),
                Some(1),
                Some(1005),
                Some(-999),
                Some(3006),
                Some(-998),
            ),
            (None, None, None, None, None, None),
        ];

        for case in cases {
            assert_eq!(
                Interval::new(
                    IntervalBound::new(ScalarValue::Int64(case.0), true),
                    IntervalBound::new(ScalarValue::Int64(case.1), true),
                )
                .add(&Interval::new(
                    IntervalBound::new(ScalarValue::Int64(case.2), true),
                    IntervalBound::new(ScalarValue::Int64(case.3), true),
                ))?,
                Interval::new(
                    IntervalBound::new(ScalarValue::Int64(case.4), true),
                    IntervalBound::new(ScalarValue::Int64(case.5), true),
                )
            )
        }
        Ok(())
    }

    #[test]
    fn sub_test() -> Result<()> {
        let cases = vec![
            (Some(1000), None, None, None, None, None),
            (None, Some(1000), None, None, None, None),
            (None, None, Some(1000), None, None, None),
            (None, None, None, Some(1000), None, None),
            (Some(1000), None, Some(1000), None, None, None),
            (None, Some(1000), Some(999), Some(1002), None, Some(1)),
            (None, Some(1000), Some(1000), None, None, Some(0)),
            (
                Some(2001),
                Some(1000),
                Some(1005),
                Some(999),
                Some(1002),
                Some(-5),
            ),
            (None, None, None, None, None, None),
        ];

        for case in cases {
            assert_eq!(
                Interval::new(
                    IntervalBound::new(ScalarValue::Int64(case.0), true),
                    IntervalBound::new(ScalarValue::Int64(case.1), true),
                )
                .sub(&Interval::new(
                    IntervalBound::new(ScalarValue::Int64(case.2), true),
                    IntervalBound::new(ScalarValue::Int64(case.3), true),
                ))?,
                Interval::new(
                    IntervalBound::new(ScalarValue::Int64(case.4), true),
                    IntervalBound::new(ScalarValue::Int64(case.5), true),
                )
            )
        }
        Ok(())
    }

    #[test]
    fn sub_test_various_bounds() -> Result<()> {
        let cases = vec![
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(100)), false),
                    IntervalBound::new(Int64(Some(200)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(200)), false),
                    IntervalBound::new(Int64(None), true),
                ),
                Interval::new(
                    IntervalBound::new(Int64(None), true),
                    IntervalBound::new(Int64(Some(0)), false),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(100)), false),
                    IntervalBound::new(Int64(Some(200)), true),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(300)), true),
                    IntervalBound::new(Int64(Some(150)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(-50)), false),
                    IntervalBound::new(Int64(Some(-100)), true),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(100)), false),
                    IntervalBound::new(Int64(Some(200)), true),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(200)), true),
                    IntervalBound::new(Int64(None), true),
                ),
                Interval::new(
                    IntervalBound::new(Int64(None), true),
                    IntervalBound::new(Int64(Some(0)), true),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(1)), false),
                    IntervalBound::new(Int64(Some(1)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(11)), false),
                    IntervalBound::new(Int64(Some(11)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(-10)), false),
                    IntervalBound::new(Int64(Some(-10)), false),
                ),
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
                Interval::new(
                    IntervalBound::new(Int64(Some(100)), false),
                    IntervalBound::new(Int64(Some(200)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(None), true),
                    IntervalBound::new(Int64(Some(200)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(None), true),
                    IntervalBound::new(Int64(Some(400)), false),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(100)), false),
                    IntervalBound::new(Int64(Some(200)), true),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(-300)), false),
                    IntervalBound::new(Int64(Some(150)), true),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(-200)), false),
                    IntervalBound::new(Int64(Some(350)), true),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(100)), false),
                    IntervalBound::new(Int64(Some(200)), true),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(200)), true),
                    IntervalBound::new(Int64(None), true),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(300)), true),
                    IntervalBound::new(Int64(None), true),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(1)), false),
                    IntervalBound::new(Int64(Some(1)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(11)), false),
                    IntervalBound::new(Int64(Some(11)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(12)), false),
                    IntervalBound::new(Int64(Some(12)), false),
                ),
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
                Interval::new(
                    IntervalBound::new(Int64(Some(100)), false),
                    IntervalBound::new(Int64(Some(200)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(None), true),
                    IntervalBound::new(Int64(Some(100)), false),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(false)), false),
                    IntervalBound::new(Boolean(Some(false)), false),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(100)), false),
                    IntervalBound::new(Int64(Some(200)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(None), true),
                    IntervalBound::new(Int64(Some(100)), true),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(false)), false),
                    IntervalBound::new(Boolean(Some(false)), false),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(100)), true),
                    IntervalBound::new(Int64(Some(200)), true),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(0)), false),
                    IntervalBound::new(Int64(Some(100)), false),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(false)), false),
                    IntervalBound::new(Boolean(Some(false)), false),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(2)), false),
                    IntervalBound::new(Int64(Some(2)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(1)), false),
                    IntervalBound::new(Int64(Some(2)), false),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(false)), false),
                    IntervalBound::new(Boolean(Some(false)), false),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(2)), false),
                    IntervalBound::new(Int64(Some(2)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(1)), false),
                    IntervalBound::new(Int64(Some(2)), true),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(false)), false),
                    IntervalBound::new(Boolean(Some(false)), false),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(1)), false),
                    IntervalBound::new(Int64(Some(1)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(1)), true),
                    IntervalBound::new(Int64(Some(2)), true),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(true)), false),
                    IntervalBound::new(Boolean(Some(true)), false),
                ),
            ),
        ];

        for case in cases {
            assert_eq!(case.0.lt(&case.1), case.2)
        }
        Ok(())
    }

    #[test]
    fn gt_test_various_bounds() -> Result<()> {
        let cases = vec![
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(100)), false),
                    IntervalBound::new(Int64(Some(200)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(None), true),
                    IntervalBound::new(Int64(Some(100)), false),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(false)), false),
                    IntervalBound::new(Boolean(Some(true)), false),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(100)), false),
                    IntervalBound::new(Int64(Some(200)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(None), true),
                    IntervalBound::new(Int64(Some(100)), true),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(true)), false),
                    IntervalBound::new(Boolean(Some(true)), false),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(100)), true),
                    IntervalBound::new(Int64(Some(200)), true),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(0)), false),
                    IntervalBound::new(Int64(Some(100)), false),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(true)), false),
                    IntervalBound::new(Boolean(Some(true)), false),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(2)), false),
                    IntervalBound::new(Int64(Some(2)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(1)), false),
                    IntervalBound::new(Int64(Some(2)), false),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(false)), false),
                    IntervalBound::new(Boolean(Some(true)), false),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(2)), false),
                    IntervalBound::new(Int64(Some(2)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(1)), false),
                    IntervalBound::new(Int64(Some(2)), true),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(true)), false),
                    IntervalBound::new(Boolean(Some(true)), false),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(1)), false),
                    IntervalBound::new(Int64(Some(1)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(1)), true),
                    IntervalBound::new(Int64(Some(2)), true),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(false)), false),
                    IntervalBound::new(Boolean(Some(false)), false),
                ),
            ),
        ];

        for case in cases {
            assert_eq!(case.0.gt(&case.1), case.2)
        }
        Ok(())
    }

    #[test]
    fn lt_eq_test_various_bounds() -> Result<()> {
        let cases = vec![
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(100)), false),
                    IntervalBound::new(Int64(Some(200)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(None), true),
                    IntervalBound::new(Int64(Some(100)), false),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(false)), false),
                    IntervalBound::new(Boolean(Some(true)), false),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(100)), false),
                    IntervalBound::new(Int64(Some(200)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(None), true),
                    IntervalBound::new(Int64(Some(100)), true),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(false)), false),
                    IntervalBound::new(Boolean(Some(false)), false),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(2)), false),
                    IntervalBound::new(Int64(Some(2)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(1)), false),
                    IntervalBound::new(Int64(Some(2)), false),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(false)), false),
                    IntervalBound::new(Boolean(Some(true)), false),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(2)), false),
                    IntervalBound::new(Int64(Some(2)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(1)), false),
                    IntervalBound::new(Int64(Some(2)), true),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(false)), false),
                    IntervalBound::new(Boolean(Some(false)), false),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(1)), false),
                    IntervalBound::new(Int64(Some(1)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(1)), false),
                    IntervalBound::new(Int64(Some(2)), true),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(true)), false),
                    IntervalBound::new(Boolean(Some(true)), false),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(1)), false),
                    IntervalBound::new(Int64(Some(1)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(1)), true),
                    IntervalBound::new(Int64(Some(2)), true),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(true)), false),
                    IntervalBound::new(Boolean(Some(true)), false),
                ),
            ),
        ];

        for case in cases {
            assert_eq!(case.0.lt_eq(&case.1), case.2)
        }
        Ok(())
    }

    #[test]
    fn gt_eq_test_various_bounds() -> Result<()> {
        let cases = vec![
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(100)), false),
                    IntervalBound::new(Int64(Some(200)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(None), true),
                    IntervalBound::new(Int64(Some(100)), false),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(true)), false),
                    IntervalBound::new(Boolean(Some(true)), false),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(100)), false),
                    IntervalBound::new(Int64(Some(200)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(None), true),
                    IntervalBound::new(Int64(Some(100)), true),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(true)), false),
                    IntervalBound::new(Boolean(Some(true)), false),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(2)), false),
                    IntervalBound::new(Int64(Some(2)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(1)), false),
                    IntervalBound::new(Int64(Some(2)), false),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(true)), false),
                    IntervalBound::new(Boolean(Some(true)), false),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(2)), false),
                    IntervalBound::new(Int64(Some(2)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(1)), false),
                    IntervalBound::new(Int64(Some(2)), true),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(true)), false),
                    IntervalBound::new(Boolean(Some(true)), false),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(1)), false),
                    IntervalBound::new(Int64(Some(1)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(1)), false),
                    IntervalBound::new(Int64(Some(2)), true),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(false)), false),
                    IntervalBound::new(Boolean(Some(true)), false),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(1)), false),
                    IntervalBound::new(Int64(Some(1)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(1)), true),
                    IntervalBound::new(Int64(Some(2)), true),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(false)), false),
                    IntervalBound::new(Boolean(Some(false)), false),
                ),
            ),
        ];

        for case in cases {
            assert_eq!(case.0.gt_eq(&case.1), case.2)
        }
        Ok(())
    }

    #[test]
    fn intersect_test_various_bounds() -> Result<()> {
        let cases = vec![
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(100)), false),
                    IntervalBound::new(Int64(Some(200)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(None), true),
                    IntervalBound::new(Int64(Some(100)), false),
                ),
                Some(Interval::new(
                    IntervalBound::new(Int64(Some(100)), false),
                    IntervalBound::new(Int64(Some(100)), false),
                )),
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(100)), false),
                    IntervalBound::new(Int64(Some(200)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(None), true),
                    IntervalBound::new(Int64(Some(100)), true),
                ),
                None,
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(100)), true),
                    IntervalBound::new(Int64(Some(200)), true),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(0)), false),
                    IntervalBound::new(Int64(Some(100)), false),
                ),
                None,
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(2)), false),
                    IntervalBound::new(Int64(Some(2)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(1)), false),
                    IntervalBound::new(Int64(Some(2)), false),
                ),
                Some(Interval::new(
                    IntervalBound::new(Int64(Some(2)), false),
                    IntervalBound::new(Int64(Some(2)), false),
                )),
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(2)), false),
                    IntervalBound::new(Int64(Some(2)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(1)), false),
                    IntervalBound::new(Int64(Some(2)), true),
                ),
                None,
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(1)), false),
                    IntervalBound::new(Int64(Some(1)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(1)), true),
                    IntervalBound::new(Int64(Some(2)), true),
                ),
                None,
            ),
            (
                Interval::new(
                    IntervalBound::new(Int64(Some(1)), false),
                    IntervalBound::new(Int64(Some(3)), false),
                ),
                Interval::new(
                    IntervalBound::new(Int64(Some(1)), true),
                    IntervalBound::new(Int64(Some(2)), true),
                ),
                Some(Interval::new(
                    IntervalBound::new(Int64(Some(1)), true),
                    IntervalBound::new(Int64(Some(2)), true),
                )),
            ),
        ];

        for case in cases {
            assert_eq!(case.0.intersect(&case.1)?, case.2)
        }
        Ok(())
    }

    #[test]
    fn non_standard_boolean_intervals() -> Result<()> {
        let cases = vec![
            (
                Interval::new(
                    IntervalBound::new(Boolean(None), true),
                    IntervalBound::new(Boolean(Some(true)), false),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(false)), false),
                    IntervalBound::new(Boolean(Some(true)), true),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(false)), false),
                    IntervalBound::new(Boolean(Some(false)), false),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Boolean(Some(false)), false),
                    IntervalBound::new(Boolean(Some(true)), true),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(None), true),
                    IntervalBound::new(Boolean(None), true),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(false)), false),
                    IntervalBound::new(Boolean(Some(false)), false),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Boolean(Some(false)), false),
                    IntervalBound::new(Boolean(Some(true)), false),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(None), true),
                    IntervalBound::new(Boolean(None), true),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(false)), false),
                    IntervalBound::new(Boolean(Some(true)), false),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Boolean(None), true),
                    IntervalBound::new(Boolean(None), true),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(false)), true),
                    IntervalBound::new(Boolean(Some(true)), false),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(false)), false),
                    IntervalBound::new(Boolean(Some(true)), false),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Boolean(Some(false)), false),
                    IntervalBound::new(Boolean(Some(true)), true),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(None), true),
                    IntervalBound::new(Boolean(Some(true)), true),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(false)), false),
                    IntervalBound::new(Boolean(Some(false)), false),
                ),
            ),
            (
                Interval::new(
                    IntervalBound::new(Boolean(Some(true)), false),
                    IntervalBound::new(Boolean(None), true),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(false)), true),
                    IntervalBound::new(Boolean(Some(true)), false),
                ),
                Interval::new(
                    IntervalBound::new(Boolean(Some(true)), false),
                    IntervalBound::new(Boolean(Some(true)), false),
                ),
            ),
        ];

        for case in cases {
            assert_eq!(case.0.and(&case.1)?, case.2)
        }
        Ok(())
    }
}
