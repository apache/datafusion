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
use std::cmp::Ordering;
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
#[derive(Debug, Clone, Eq)]
pub enum IntervalBound {
    Open(ScalarValue),
    Closed(ScalarValue),
}

impl IntervalBound {
    /// This convenience function creates an unbounded interval endpoint.
    pub fn make_unbounded<T: Borrow<DataType>>(data_type: T) -> Result<Self> {
        ScalarValue::try_from(data_type.borrow()).map(IntervalBound::Open)
    }

    /// This convenience function returns the data type associated with this
    /// `IntervalBound`.
    pub fn get_datatype(&self) -> DataType {
        match self {
            IntervalBound::Open(value) | IntervalBound::Closed(value) => {
                value.get_datatype()
            }
        }
    }

    /// This convenience function returns the scalar value associated with this
    /// `IntervalBound`.
    pub fn get_value(&self) -> &ScalarValue {
        match self {
            IntervalBound::Open(value) | IntervalBound::Closed(value) => value,
        }
    }

    /// This convenience function checks whether the `IntervalBound` represents
    /// an unbounded interval endpoint.
    pub fn is_null(&self) -> bool {
        self.get_value().is_null()
    }

    /// This convenience function checks whether the `IntervalBound` represents
    /// a closed (i.e. inclusive) endpoint.
    pub fn is_closed(&self) -> bool {
        matches!(self, IntervalBound::Closed(_))
    }

    /// This function casts the `IntervalBound` to the given data type.
    pub(crate) fn cast_to(
        &self,
        data_type: &DataType,
        cast_options: &CastOptions,
    ) -> Result<IntervalBound> {
        Ok(match &self {
            IntervalBound::Open(scalar) => {
                IntervalBound::Open(cast_scalar_value(scalar, data_type, cast_options)?)
            }
            IntervalBound::Closed(scalar) => {
                IntervalBound::Closed(cast_scalar_value(scalar, data_type, cast_options)?)
            }
        })
    }

    /// This function adds the given `IntervalBound` to this `IntervalBound`.
    /// The result is unbounded if either is; otherwise, their values are
    /// added. The result is closed if both original bounds are closed, or open
    /// otherwise.
    pub fn add<T: Borrow<IntervalBound>>(&self, other: T) -> Result<IntervalBound> {
        let rhs = other.borrow();
        if self.is_null() || rhs.is_null() {
            IntervalBound::make_unbounded(self.get_datatype())
        } else {
            let result = self.get_value().add(rhs.get_value());
            if self.is_closed() && rhs.is_closed() {
                result.map(IntervalBound::Closed)
            } else {
                result.map(IntervalBound::Open)
            }
        }
    }

    /// This function subtracts the given `IntervalBound` from `self`.
    /// The result is unbounded if either is; otherwise, their values are
    /// subtracted. The result is closed if both original bounds are closed,
    /// or open otherwise.
    pub fn sub<T: Borrow<IntervalBound>>(&self, other: T) -> Result<IntervalBound> {
        let rhs = other.borrow();
        if self.is_null() || rhs.is_null() {
            IntervalBound::make_unbounded(self.get_datatype())
        } else {
            let result = self.get_value().sub(rhs.get_value());
            if self.is_closed() && rhs.is_closed() {
                result.map(IntervalBound::Closed)
            } else {
                result.map(IntervalBound::Open)
            }
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
        Ok(if first.is_null() {
            second.clone()
        } else if second.is_null() {
            first.clone()
        } else if first != second {
            let chosen = decide(first.get_value(), second.get_value())?;
            if chosen.eq(first.get_value()) {
                first.clone()
            } else {
                second.clone()
            }
        } else if first.is_closed() && second.is_closed() {
            IntervalBound::Closed(second.get_value().clone())
        } else {
            IntervalBound::Open(second.get_value().clone())
        })
    }
}

impl Display for IntervalBound {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "IntervalBound [{}]", self.get_value())
    }
}

impl PartialOrd for IntervalBound {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.get_value().partial_cmp(other.get_value())
    }
}

impl PartialEq for IntervalBound {
    fn eq(&self, other: &Self) -> bool {
        self.get_value() == other.get_value()
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
        Interval {
            lower: IntervalBound::Open(ScalarValue::Null),
            upper: IntervalBound::Open(ScalarValue::Null),
        }
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
        let lower_type = self.lower.get_value().get_datatype();
        let upper_type = self.upper.get_value().get_datatype();
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
        let flags = if !self.upper.is_null()
            && !other.lower.is_null()
            && self.upper <= other.lower
        {
            // Values in this interval are certainly less than or equal to those
            // in the given interval.
            (false, false)
        } else if !self.lower.is_null()
            && !other.upper.is_null()
            && self.lower >= other.upper
            && (self.lower > other.upper
                || !self.lower.is_closed()
                || !other.upper.is_closed())
        {
            // Values in this interval are certainly greater than those in the
            // given interval.
            (true, true)
        } else {
            // All outcomes are possible.
            (false, true)
        };

        Interval::new(
            IntervalBound::Closed(ScalarValue::Boolean(Some(flags.0))),
            IntervalBound::Closed(ScalarValue::Boolean(Some(flags.1))),
        )
    }

    /// Decide if this interval is certainly greater than or equal to, possibly greater than
    /// or equal to, or can't be greater than or equal to `other` by returning [true, true],
    /// [false, true] or [false, false] respectively.
    pub(crate) fn gt_eq(&self, other: &Interval) -> Interval {
        let flags = if !self.lower.is_null()
            && !other.upper.is_null()
            && self.lower >= other.upper
        {
            // Values in this interval are certainly greater than or equal to those
            // in the given interval.
            (true, true)
        } else if !self.upper.is_null()
            && !other.lower.is_null()
            && self.upper <= other.lower
            && (self.upper < other.lower
                || !self.upper.is_closed()
                || !other.lower.is_closed())
        {
            // Values in this interval are certainly less than those in the
            // given interval.
            (false, false)
        } else {
            // All outcomes are possible.
            (false, true)
        };

        Interval::new(
            IntervalBound::Closed(ScalarValue::Boolean(Some(flags.0))),
            IntervalBound::Closed(ScalarValue::Boolean(Some(flags.1))),
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
        let flags = if !self.lower.is_null()
            && (self.lower == self.upper)
            && (other.lower == other.upper)
            && (self.lower == other.lower)
        {
            (true, true)
        } else if self.gt(other) == CERTAINLY_TRUE || self.gt(other) == CERTAINLY_FALSE {
            (false, false)
        } else {
            (false, true)
        };

        Interval::new(
            IntervalBound::Closed(ScalarValue::Boolean(Some(flags.0))),
            IntervalBound::Closed(ScalarValue::Boolean(Some(flags.1))),
        )
    }

    /// Compute the logical conjunction of this (boolean) interval with the
    /// given boolean interval. Boolean intervals always have closed bounds.
    pub(crate) fn and(&self, other: &Interval) -> Result<Interval> {
        match (
            self.lower.get_value(),
            self.upper.get_value(),
            other.lower.get_value(),
            other.upper.get_value(),
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
                    lower: IntervalBound::Closed(ScalarValue::Boolean(Some(lower))),
                    upper: IntervalBound::Closed(ScalarValue::Boolean(Some(upper))),
                })
            }
            _ => Err(DataFusionError::Internal(
                "Incompatible types for logical conjunction".to_string(),
            )),
        }
    }

    /// Compute the intersection of the interval with the given interval.
    /// If the intersection is empty, return None.
    pub(crate) fn intersect(&self, other: &Interval) -> Result<Option<Interval>> {
        // If it is evident that the result is an empty interval,
        // do not make any calculation and directly return None.
        if (!self.lower.is_null() && !other.upper.is_null() && self.lower > other.upper)
            || (!self.upper.is_null()
                && !other.lower.is_null()
                && self.upper < other.lower)
        {
            // This None value signals an empty interval.
            return Ok(None);
        }

        let lower = IntervalBound::choose(&self.lower, &other.lower, max)?;
        let upper = IntervalBound::choose(&self.upper, &other.upper, min)?;

        let non_empty = lower.is_null()
            || upper.is_null()
            || lower != upper
            || (lower.is_closed() && upper.is_closed());
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
}

const CERTAINLY_TRUE: Interval = Interval {
    lower: IntervalBound::Closed(ScalarValue::Boolean(Some(true))),
    upper: IntervalBound::Closed(ScalarValue::Boolean(Some(true))),
};

const CERTAINLY_FALSE: Interval = Interval {
    lower: IntervalBound::Closed(ScalarValue::Boolean(Some(false))),
    upper: IntervalBound::Closed(ScalarValue::Boolean(Some(false))),
};

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
        _ => Ok(Interval::new(
            IntervalBound::Open(ScalarValue::Null),
            IntervalBound::Open(ScalarValue::Null),
        )),
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

#[cfg(test)]
mod tests {
    use crate::intervals::{Interval, IntervalBound};
    use datafusion_common::{Result, ScalarValue};
    use IntervalBound::{Closed, Open};
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
                    Open(ScalarValue::Int64(case.0)),
                    Open(ScalarValue::Int64(case.1)),
                )
                .intersect(&Interval::new(
                    Open(ScalarValue::Int64(case.2)),
                    Open(ScalarValue::Int64(case.3))
                ))?
                .unwrap(),
                Interval::new(
                    Open(ScalarValue::Int64(case.4)),
                    Open(ScalarValue::Int64(case.5)),
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
                    Open(ScalarValue::Int64(case.0)),
                    Open(ScalarValue::Int64(case.1)),
                )
                .intersect(&Interval::new(
                    Open(ScalarValue::Int64(case.2)),
                    Open(ScalarValue::Int64(case.3)),
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
                    Open(ScalarValue::Int64(case.0)),
                    Open(ScalarValue::Int64(case.1)),
                )
                .gt(&Interval::new(
                    Open(ScalarValue::Int64(case.2)),
                    Open(ScalarValue::Int64(case.3)),
                )),
                Interval::new(
                    Closed(ScalarValue::Boolean(Some(case.4))),
                    Closed(ScalarValue::Boolean(Some(case.5))),
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
                    Open(ScalarValue::Int64(case.0)),
                    Open(ScalarValue::Int64(case.1)),
                )
                .lt(&Interval::new(
                    Open(ScalarValue::Int64(case.2)),
                    Open(ScalarValue::Int64(case.3)),
                )),
                Interval::new(
                    Closed(ScalarValue::Boolean(Some(case.4))),
                    Closed(ScalarValue::Boolean(Some(case.5))),
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
                    Closed(ScalarValue::Boolean(Some(case.0))),
                    Closed(ScalarValue::Boolean(Some(case.1))),
                )
                .and(&Interval::new(
                    Closed(ScalarValue::Boolean(Some(case.2))),
                    Closed(ScalarValue::Boolean(Some(case.3))),
                ))?,
                Interval::new(
                    Closed(ScalarValue::Boolean(Some(case.4))),
                    Closed(ScalarValue::Boolean(Some(case.5))),
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
                    IntervalBound::Open(ScalarValue::Int64(case.0)),
                    IntervalBound::Open(ScalarValue::Int64(case.1)),
                )
                .add(&Interval::new(
                    IntervalBound::Open(ScalarValue::Int64(case.2)),
                    IntervalBound::Open(ScalarValue::Int64(case.3)),
                ))?,
                Interval::new(
                    IntervalBound::Open(ScalarValue::Int64(case.4)),
                    IntervalBound::Open(ScalarValue::Int64(case.5)),
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
                    IntervalBound::Open(ScalarValue::Int64(case.0)),
                    IntervalBound::Open(ScalarValue::Int64(case.1)),
                )
                .sub(&Interval::new(
                    IntervalBound::Open(ScalarValue::Int64(case.2)),
                    IntervalBound::Open(ScalarValue::Int64(case.3)),
                ))?,
                Interval::new(
                    IntervalBound::Open(ScalarValue::Int64(case.4)),
                    IntervalBound::Open(ScalarValue::Int64(case.5)),
                )
            )
        }
        Ok(())
    }

    #[test]
    fn sub_test_various_bounds() -> Result<()> {
        let cases = vec![
            (
                Interval::new(Closed(Int64(Some(100))), Closed(Int64(Some(200)))),
                Interval::new(Closed(Int64(Some(200))), Open(Int64(None))),
                Interval::new(Open(Int64(None)), Closed(Int64(Some(0)))),
            ),
            (
                Interval::new(Closed(Int64(Some(100))), Open(Int64(Some(200)))),
                Interval::new(Open(Int64(Some(300))), Closed(Int64(Some(150)))),
                Interval::new(Closed(Int64(Some(-50))), Open(Int64(Some(-100)))),
            ),
            (
                Interval::new(Closed(Int64(Some(100))), Open(Int64(Some(200)))),
                Interval::new(Open(Int64(Some(200))), Open(Int64(None))),
                Interval::new(Open(Int64(None)), Open(Int64(Some(0)))),
            ),
            (
                Interval::new(Closed(Int64(Some(1))), Closed(Int64(Some(1)))),
                Interval::new(Closed(Int64(Some(11))), Closed(Int64(Some(11)))),
                Interval::new(Closed(Int64(Some(-10))), Closed(Int64(Some(-10)))),
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
                Interval::new(Closed(Int64(Some(100))), Closed(Int64(Some(200)))),
                Interval::new(Open(Int64(None)), Closed(Int64(Some(200)))),
                Interval::new(Open(Int64(None)), Closed(Int64(Some(400)))),
            ),
            (
                Interval::new(Closed(Int64(Some(100))), Open(Int64(Some(200)))),
                Interval::new(Closed(Int64(Some(-300))), Open(Int64(Some(150)))),
                Interval::new(Closed(Int64(Some(-200))), Open(Int64(Some(350)))),
            ),
            (
                Interval::new(Closed(Int64(Some(100))), Open(Int64(Some(200)))),
                Interval::new(Open(Int64(Some(200))), Open(Int64(None))),
                Interval::new(Open(Int64(Some(300))), Open(Int64(None))),
            ),
            (
                Interval::new(Closed(Int64(Some(1))), Closed(Int64(Some(1)))),
                Interval::new(Closed(Int64(Some(11))), Closed(Int64(Some(11)))),
                Interval::new(Closed(Int64(Some(12))), Closed(Int64(Some(12)))),
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
                Interval::new(Closed(Int64(Some(100))), Closed(Int64(Some(200)))),
                Interval::new(Open(Int64(None)), Closed(Int64(Some(100)))),
                Interval::new(Closed(Boolean(Some(false))), Closed(Boolean(Some(false)))),
            ),
            (
                Interval::new(Closed(Int64(Some(100))), Closed(Int64(Some(200)))),
                Interval::new(Open(Int64(None)), Open(Int64(Some(100)))),
                Interval::new(Closed(Boolean(Some(false))), Closed(Boolean(Some(false)))),
            ),
            (
                Interval::new(Open(Int64(Some(100))), Open(Int64(Some(200)))),
                Interval::new(Closed(Int64(Some(0))), Closed(Int64(Some(100)))),
                Interval::new(Closed(Boolean(Some(false))), Closed(Boolean(Some(false)))),
            ),
            (
                Interval::new(Closed(Int64(Some(2))), Closed(Int64(Some(2)))),
                Interval::new(Closed(Int64(Some(1))), Closed(Int64(Some(2)))),
                Interval::new(Closed(Boolean(Some(false))), Closed(Boolean(Some(false)))),
            ),
            (
                Interval::new(Closed(Int64(Some(2))), Closed(Int64(Some(2)))),
                Interval::new(Closed(Int64(Some(1))), Open(Int64(Some(2)))),
                Interval::new(Closed(Boolean(Some(false))), Closed(Boolean(Some(false)))),
            ),
            (
                Interval::new(Closed(Int64(Some(1))), Closed(Int64(Some(1)))),
                Interval::new(Open(Int64(Some(1))), Open(Int64(Some(2)))),
                Interval::new(Closed(Boolean(Some(true))), Closed(Boolean(Some(true)))),
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
                Interval::new(Closed(Int64(Some(100))), Closed(Int64(Some(200)))),
                Interval::new(Open(Int64(None)), Closed(Int64(Some(100)))),
                Interval::new(Closed(Boolean(Some(false))), Closed(Boolean(Some(true)))),
            ),
            (
                Interval::new(Closed(Int64(Some(100))), Closed(Int64(Some(200)))),
                Interval::new(Open(Int64(None)), Open(Int64(Some(100)))),
                Interval::new(Closed(Boolean(Some(true))), Closed(Boolean(Some(true)))),
            ),
            (
                Interval::new(Open(Int64(Some(100))), Open(Int64(Some(200)))),
                Interval::new(Closed(Int64(Some(0))), Closed(Int64(Some(100)))),
                Interval::new(Closed(Boolean(Some(true))), Closed(Boolean(Some(true)))),
            ),
            (
                Interval::new(Closed(Int64(Some(2))), Closed(Int64(Some(2)))),
                Interval::new(Closed(Int64(Some(1))), Closed(Int64(Some(2)))),
                Interval::new(Closed(Boolean(Some(false))), Closed(Boolean(Some(true)))),
            ),
            (
                Interval::new(Closed(Int64(Some(2))), Closed(Int64(Some(2)))),
                Interval::new(Closed(Int64(Some(1))), Open(Int64(Some(2)))),
                Interval::new(Closed(Boolean(Some(true))), Closed(Boolean(Some(true)))),
            ),
            (
                Interval::new(Closed(Int64(Some(1))), Closed(Int64(Some(1)))),
                Interval::new(Open(Int64(Some(1))), Open(Int64(Some(2)))),
                Interval::new(Closed(Boolean(Some(false))), Closed(Boolean(Some(false)))),
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
                Interval::new(Closed(Int64(Some(100))), Closed(Int64(Some(200)))),
                Interval::new(Open(Int64(None)), Closed(Int64(Some(100)))),
                Interval::new(Closed(Boolean(Some(false))), Closed(Boolean(Some(true)))),
            ),
            (
                Interval::new(Closed(Int64(Some(100))), Closed(Int64(Some(200)))),
                Interval::new(Open(Int64(None)), Open(Int64(Some(100)))),
                Interval::new(Closed(Boolean(Some(false))), Closed(Boolean(Some(false)))),
            ),
            (
                Interval::new(Closed(Int64(Some(2))), Closed(Int64(Some(2)))),
                Interval::new(Closed(Int64(Some(1))), Closed(Int64(Some(2)))),
                Interval::new(Closed(Boolean(Some(false))), Closed(Boolean(Some(true)))),
            ),
            (
                Interval::new(Closed(Int64(Some(2))), Closed(Int64(Some(2)))),
                Interval::new(Closed(Int64(Some(1))), Open(Int64(Some(2)))),
                Interval::new(Closed(Boolean(Some(false))), Closed(Boolean(Some(false)))),
            ),
            (
                Interval::new(Closed(Int64(Some(1))), Closed(Int64(Some(1)))),
                Interval::new(Closed(Int64(Some(1))), Open(Int64(Some(2)))),
                Interval::new(Closed(Boolean(Some(true))), Closed(Boolean(Some(true)))),
            ),
            (
                Interval::new(Closed(Int64(Some(1))), Closed(Int64(Some(1)))),
                Interval::new(Open(Int64(Some(1))), Open(Int64(Some(2)))),
                Interval::new(Closed(Boolean(Some(true))), Closed(Boolean(Some(true)))),
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
                Interval::new(Closed(Int64(Some(100))), Closed(Int64(Some(200)))),
                Interval::new(Open(Int64(None)), Closed(Int64(Some(100)))),
                Interval::new(Closed(Boolean(Some(true))), Closed(Boolean(Some(true)))),
            ),
            (
                Interval::new(Closed(Int64(Some(100))), Closed(Int64(Some(200)))),
                Interval::new(Open(Int64(None)), Open(Int64(Some(100)))),
                Interval::new(Closed(Boolean(Some(true))), Closed(Boolean(Some(true)))),
            ),
            (
                Interval::new(Closed(Int64(Some(2))), Closed(Int64(Some(2)))),
                Interval::new(Closed(Int64(Some(1))), Closed(Int64(Some(2)))),
                Interval::new(Closed(Boolean(Some(true))), Closed(Boolean(Some(true)))),
            ),
            (
                Interval::new(Closed(Int64(Some(2))), Closed(Int64(Some(2)))),
                Interval::new(Closed(Int64(Some(1))), Open(Int64(Some(2)))),
                Interval::new(Closed(Boolean(Some(true))), Closed(Boolean(Some(true)))),
            ),
            (
                Interval::new(Closed(Int64(Some(1))), Closed(Int64(Some(1)))),
                Interval::new(Closed(Int64(Some(1))), Open(Int64(Some(2)))),
                Interval::new(Closed(Boolean(Some(false))), Closed(Boolean(Some(true)))),
            ),
            (
                Interval::new(Closed(Int64(Some(1))), Closed(Int64(Some(1)))),
                Interval::new(Open(Int64(Some(1))), Open(Int64(Some(2)))),
                Interval::new(Closed(Boolean(Some(false))), Closed(Boolean(Some(false)))),
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
                Interval::new(Closed(Int64(Some(100))), Closed(Int64(Some(200)))),
                Interval::new(Open(Int64(None)), Closed(Int64(Some(100)))),
                Some(Interval::new(
                    Closed(Int64(Some(100))),
                    Closed(Int64(Some(100))),
                )),
            ),
            (
                Interval::new(Closed(Int64(Some(100))), Closed(Int64(Some(200)))),
                Interval::new(Open(Int64(None)), Open(Int64(Some(100)))),
                None,
            ),
            (
                Interval::new(Open(Int64(Some(100))), Open(Int64(Some(200)))),
                Interval::new(Closed(Int64(Some(0))), Closed(Int64(Some(100)))),
                None,
            ),
            (
                Interval::new(Closed(Int64(Some(2))), Closed(Int64(Some(2)))),
                Interval::new(Closed(Int64(Some(1))), Closed(Int64(Some(2)))),
                Some(Interval::new(
                    Closed(Int64(Some(2))),
                    Closed(Int64(Some(2))),
                )),
            ),
            (
                Interval::new(Closed(Int64(Some(2))), Closed(Int64(Some(2)))),
                Interval::new(Closed(Int64(Some(1))), Open(Int64(Some(2)))),
                None,
            ),
            (
                Interval::new(Closed(Int64(Some(1))), Closed(Int64(Some(1)))),
                Interval::new(Open(Int64(Some(1))), Open(Int64(Some(2)))),
                None,
            ),
            (
                Interval::new(Closed(Int64(Some(1))), Closed(Int64(Some(3)))),
                Interval::new(Open(Int64(Some(1))), Open(Int64(Some(2)))),
                Some(Interval::new(Open(Int64(Some(1))), Open(Int64(Some(2))))),
            ),
        ];

        for (i, case) in cases.iter().enumerate() {
            assert_eq!(case.0.intersect(&case.1)?, case.2, "{}", i)
        }
        Ok(())
    }
}
