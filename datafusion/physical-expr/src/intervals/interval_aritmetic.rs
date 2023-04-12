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

/// This type represents an interval, which is used to calculate reliable
/// bounds for expressions. Currently, we only support addition and
/// subtraction, but more capabilities will be added in the future.
/// Upper/lower bounds having NULL values indicate an unbounded side. For
/// example; [10, 20], [10, ∞], [-∞, 100] and [-∞, ∞] are all valid intervals.
#[derive(Debug, Clone, Eq)]
pub enum IntervalBound {
    Open(ScalarValue),
    Closed(ScalarValue),
}

impl IntervalBound {
    pub fn get_bound_scalar(&self) -> &ScalarValue {
        match self {
            IntervalBound::Open(scalar) | IntervalBound::Closed(scalar) => scalar,
        }
    }

    pub fn is_null(&self) -> bool {
        self.get_bound_scalar().is_null()
    }

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

    /// Add the given IntervalBound to this IntervalBound.
    /// If either bound is null, the result is an open bound with the same data type.
    /// Otherwise, the bounds are added, and the result is closed if both original bounds are closed,
    /// or open otherwise.
    pub fn add<T: Borrow<IntervalBound>>(&self, other: T) -> Result<IntervalBound> {
        let rhs = other.borrow();
        let res = if self.is_null() || rhs.is_null() {
            IntervalBound::Open(ScalarValue::try_from(
                self.get_bound_scalar().get_datatype(),
            )?)
        } else {
            let res = self.get_bound_scalar().add(rhs.get_bound_scalar())?;
            if is_bound_closed(self, rhs) {
                IntervalBound::Closed(res)
            } else {
                IntervalBound::Open(res)
            }
        };
        Ok(res)
    }

    /// Subtract the given IntervalBound from this IntervalBound.
    /// If either bound is null, the result is an open bound with the same data type.
    /// Otherwise, the bounds are subtracted, and the result is closed if both original bounds are closed,
    /// or open otherwise.
    pub fn sub<T: Borrow<IntervalBound>>(&self, other: T) -> Result<IntervalBound> {
        let rhs = other.borrow();
        let res = if self.is_null() || rhs.is_null() {
            IntervalBound::Open(ScalarValue::try_from(
                self.get_bound_scalar().get_datatype(),
            )?)
        } else {
            let res = self.get_bound_scalar().sub(rhs.get_bound_scalar())?;
            if is_bound_closed(self, rhs) {
                IntervalBound::Closed(res)
            } else {
                IntervalBound::Open(res)
            }
        };
        Ok(res)
    }

    pub fn intersect_bounds(
        self_: &IntervalBound,
        other: &IntervalBound,
        min_max: fn(&ScalarValue, &ScalarValue) -> Result<ScalarValue>,
    ) -> Result<IntervalBound> {
        let val = if self_.is_null() {
            other.clone()
        } else if other.is_null() {
            self_.clone()
        } else {
            let inner = min_max(self_.get_bound_scalar(), other.get_bound_scalar())?;
            if self_ != other {
                if inner == *self_.get_bound_scalar() {
                    self_.clone()
                } else {
                    other.clone()
                }
            } else if is_bound_closed(self_, other) {
                IntervalBound::Closed(other.get_bound_scalar().clone())
            } else {
                IntervalBound::Open(other.get_bound_scalar().clone())
            }
        };
        Ok(val)
    }
}

impl Display for IntervalBound {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "IntervalBound [{}]", self.get_bound_scalar())
    }
}

impl PartialOrd for IntervalBound {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.get_bound_scalar()
            .partial_cmp(other.get_bound_scalar())
    }
}

impl PartialEq for IntervalBound {
    fn eq(&self, other: &Self) -> bool {
        self.get_bound_scalar() == other.get_bound_scalar()
    }
}

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
    fn new(lower: IntervalBound, upper: IntervalBound) -> Interval {
        Interval { lower, upper }
    }

    pub(crate) fn cast_to(
        &self,
        data_type: &DataType,
        cast_options: &CastOptions,
    ) -> Result<Interval> {
        let lower = self.lower.cast_to(data_type, cast_options)?;
        let upper = self.upper.cast_to(data_type, cast_options)?;
        Ok(Interval::new(lower, upper))
    }

    // If the scalar type of bounds are not the same, return error.
    pub(crate) fn get_datatype(&self) -> Result<DataType> {
        let lower_type = self.lower.get_bound_scalar().get_datatype();
        let upper_type = self.upper.get_bound_scalar().get_datatype();
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
            && (self.upper <= other.lower)
        {
            // If self.upper is less than or equal to other.lower, self can't be greater than other.
            (false, false)
        } else if !self.lower.is_null()
            && !other.upper.is_null()
            && (self.lower >= other.upper)
        {
            // If self.lower is greater than or equal to other.upper, self is certainly greater than other.
            if self.lower > other.upper {
                (true, true)
            } else if is_bound_closed(&self.lower, &other.upper) {
                (false, true)
            } else {
                (true, true)
            }
        } else {
            // Otherwise, self is possibly greater than other.
            (false, true)
        };

        Interval {
            lower: IntervalBound::Closed(ScalarValue::Boolean(Some(flags.0))),
            upper: IntervalBound::Closed(ScalarValue::Boolean(Some(flags.1))),
        }
    }

    /// Decide if this interval is certainly greater than or equal to, possibly greater than
    /// or equal to, or can't be greater than or equal to `other` by returning [true, true],
    /// [false, true] or [false, false] respectively.
    pub(crate) fn gt_eq(&self, other: &Interval) -> Interval {
        let flags = if !self.upper.is_null()
            && !other.lower.is_null()
            && (self.upper <= other.lower)
        {
            // If self.upper is less than or equal to other.lower, self can't be greater than or equal to other.
            if self.upper < other.lower {
                (false, false)
            } else if is_bound_closed(&self.upper, &other.lower) {
                (false, true)
            } else {
                (false, false)
            }
        } else if !self.lower.is_null()
            && !other.upper.is_null()
            && (self.lower >= other.upper)
        {
            // If self.lower is greater than or equal to other.upper, self is certainly greater than or equal to other.
            (true, true)
        } else {
            // Otherwise, self is possibly greater than or equal to other.
            (false, true)
        };

        Interval {
            lower: IntervalBound::Closed(ScalarValue::Boolean(Some(flags.0))),
            upper: IntervalBound::Closed(ScalarValue::Boolean(Some(flags.1))),
        }
    }

    /// Decide if this interval is certainly less than, possibly less than,
    /// or can't be less than `other` by returning [true, true],
    /// [false, true] or [false, false] respectively.
    pub(crate) fn lt(&self, other: &Interval) -> Interval {
        other.gt(self)
    }

    /// Decide if this interval is certainly less than or equal to, possibly
    /// less than or equal to, or can't be less than or equal to `other` by returning
    ///  [true, true], [false, true] or [false, false] respectively.
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
        } else if self.gt(other)
            == (Interval {
                lower: IntervalBound::Closed(ScalarValue::Boolean(Some(true))),
                upper: IntervalBound::Closed(ScalarValue::Boolean(Some(true))),
            })
            || self.gt(other)
                == (Interval {
                    lower: IntervalBound::Closed(ScalarValue::Boolean(Some(false))),
                    upper: IntervalBound::Closed(ScalarValue::Boolean(Some(false))),
                })
        {
            (false, false)
        } else {
            (false, true)
        };

        Interval {
            lower: IntervalBound::Closed(ScalarValue::Boolean(Some(flags.0))),
            upper: IntervalBound::Closed(ScalarValue::Boolean(Some(flags.1))),
        }
    }

    /// Compute the logical conjunction of this (boolean) interval with the
    /// given boolean interval. Boolean intervals always have closed bounds.
    pub(crate) fn and(&self, other: &Interval) -> Result<Interval> {
        match (
            self.lower.get_bound_scalar(),
            self.upper.get_bound_scalar(),
            other.lower.get_bound_scalar(),
            other.upper.get_bound_scalar(),
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

        let lower = IntervalBound::intersect_bounds(&self.lower, &other.lower, max)?;

        let upper = IntervalBound::intersect_bounds(&self.upper, &other.upper, min)?;

        Ok(if !lower.is_null() && !upper.is_null() && lower == upper {
            match (&lower, &upper) {
                (IntervalBound::Closed(_), IntervalBound::Closed(_)) => {
                    Some(Interval { lower, upper })
                }
                (_, _) => None,
            }
        } else {
            Some(Interval { lower, upper })
        })
    }

    /// Add the given interval (`other`) to this interval. Say we have
    /// intervals [a1, b1] and [a2, b2], then their sum is [a1 + a2, b1 + b2].
    /// Note that this represents all possible values the sum can take if
    /// one can choose single values arbitrarily from each of the operands.
    pub fn add<T: Borrow<Interval>>(&self, other: T) -> Result<Interval> {
        let rhs = other.borrow();

        let lower = self.lower.add(&rhs.lower)?;
        let upper = self.upper.add(&rhs.upper)?;

        Ok(Interval { lower, upper })
    }

    /// Subtract the given interval (`other`) from this interval. Say we have
    /// intervals [a1, b1] and [a2, b2], then their sum is [a1 - b2, b1 - a2].
    /// Note that this represents all possible values the difference can take
    /// if one can choose single values arbitrarily from each of the operands.
    pub fn sub<T: Borrow<Interval>>(&self, other: T) -> Result<Interval> {
        let rhs = other.borrow();
        let lower = self.lower.sub(&rhs.upper)?;
        let upper = self.upper.sub(&rhs.lower)?;
        Ok(Interval { lower, upper })
    }
}

/// Indicates whether interval arithmetic is supported for the given operator.
pub fn is_operator_supported(op: &Operator) -> bool {
    matches!(
        op,
        &Operator::Plus
            | &Operator::Minus
            | &Operator::And
            | &Operator::Gt
            | &Operator::Lt
    )
}

pub fn is_bound_closed(bound1: &IntervalBound, bound2: &IntervalBound) -> bool {
    match (bound1, bound2) {
        (IntervalBound::Closed(_), IntervalBound::Closed(_)) => true,
        (_, _) => false,
    }
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
        _ => Ok(Interval {
            lower: IntervalBound::Open(ScalarValue::Null),
            upper: IntervalBound::Open(ScalarValue::Null),
        }),
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
                Interval {
                    lower: IntervalBound::Open(ScalarValue::Int64(case.0)),
                    upper: IntervalBound::Open(ScalarValue::Int64(case.1))
                }
                .intersect(&Interval {
                    lower: IntervalBound::Open(ScalarValue::Int64(case.2)),
                    upper: IntervalBound::Open(ScalarValue::Int64(case.3))
                })?
                .unwrap(),
                Interval {
                    lower: IntervalBound::Open(ScalarValue::Int64(case.4)),
                    upper: IntervalBound::Open(ScalarValue::Int64(case.5))
                }
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
                Interval {
                    lower: IntervalBound::Open(ScalarValue::Int64(case.0)),
                    upper: IntervalBound::Open(ScalarValue::Int64(case.1))
                }
                .intersect(&Interval {
                    lower: IntervalBound::Open(ScalarValue::Int64(case.2)),
                    upper: IntervalBound::Open(ScalarValue::Int64(case.3))
                })?,
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
                Interval {
                    lower: IntervalBound::Open(ScalarValue::Int64(case.0)),
                    upper: IntervalBound::Open(ScalarValue::Int64(case.1))
                }
                .gt(&Interval {
                    lower: IntervalBound::Open(ScalarValue::Int64(case.2)),
                    upper: IntervalBound::Open(ScalarValue::Int64(case.3))
                }),
                Interval {
                    lower: IntervalBound::Closed(ScalarValue::Boolean(Some(case.4))),
                    upper: IntervalBound::Closed(ScalarValue::Boolean(Some(case.5)))
                }
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
                Interval {
                    lower: IntervalBound::Open(ScalarValue::Int64(case.0)),
                    upper: IntervalBound::Open(ScalarValue::Int64(case.1))
                }
                .lt(&Interval {
                    lower: IntervalBound::Open(ScalarValue::Int64(case.2)),
                    upper: IntervalBound::Open(ScalarValue::Int64(case.3))
                }),
                Interval {
                    lower: IntervalBound::Closed(ScalarValue::Boolean(Some(case.4))),
                    upper: IntervalBound::Closed(ScalarValue::Boolean(Some(case.5)))
                },
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
                Interval {
                    lower: IntervalBound::Closed(ScalarValue::Boolean(Some(case.0))),
                    upper: IntervalBound::Closed(ScalarValue::Boolean(Some(case.1)))
                }
                .and(&Interval {
                    lower: IntervalBound::Closed(ScalarValue::Boolean(Some(case.2))),
                    upper: IntervalBound::Closed(ScalarValue::Boolean(Some(case.3)))
                })?,
                Interval {
                    lower: IntervalBound::Closed(ScalarValue::Boolean(Some(case.4))),
                    upper: IntervalBound::Closed(ScalarValue::Boolean(Some(case.5)))
                }
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
                Interval {
                    lower: IntervalBound::Open(ScalarValue::Int64(case.0)),
                    upper: IntervalBound::Open(ScalarValue::Int64(case.1))
                }
                .add(&Interval {
                    lower: IntervalBound::Open(ScalarValue::Int64(case.2)),
                    upper: IntervalBound::Open(ScalarValue::Int64(case.3))
                })?,
                Interval {
                    lower: IntervalBound::Open(ScalarValue::Int64(case.4)),
                    upper: IntervalBound::Open(ScalarValue::Int64(case.5))
                }
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
                Interval {
                    lower: IntervalBound::Open(ScalarValue::Int64(case.0)),
                    upper: IntervalBound::Open(ScalarValue::Int64(case.1))
                }
                .sub(&Interval {
                    lower: IntervalBound::Open(ScalarValue::Int64(case.2)),
                    upper: IntervalBound::Open(ScalarValue::Int64(case.3))
                })?,
                Interval {
                    lower: IntervalBound::Open(ScalarValue::Int64(case.4)),
                    upper: IntervalBound::Open(ScalarValue::Int64(case.5))
                }
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
