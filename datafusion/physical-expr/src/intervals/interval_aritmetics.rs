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
//!
use std::borrow::Borrow;
use std::fmt;
use std::fmt::{Display, Formatter};

use arrow::compute::{kernels, CastOptions};
use arrow::datatypes::DataType;
use datafusion_common::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::Operator;

use crate::aggregate::min_max::{max, min};

/// This type represents an interval, which is used to calculate reliable
/// bounds for expressions. Currently, we only support addition and
/// subtraction, but more capabilities will be added in the future.
#[derive(Debug, PartialEq, Clone, Eq, Hash)]
pub struct Interval {
    pub lower: ScalarValue,
    pub upper: ScalarValue,
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
        write!(f, "Interval [{}, {}]", self.lower, self.upper)
    }
}

/// Casting data type with arrow kernel
fn cast_scalar_value(
    value: &ScalarValue,
    data_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ScalarValue> {
    let scalar_array = value.to_array();
    let cast_array =
        kernels::cast::cast_with_options(&scalar_array, data_type, cast_options)?;
    ScalarValue::try_from_array(&cast_array, 0)
}

impl Interval {
    pub(crate) fn cast_to(
        &self,
        data_type: &DataType,
        cast_options: &CastOptions,
    ) -> Result<Interval> {
        Ok(Interval {
            lower: cast_scalar_value(&self.lower, data_type, cast_options)?,
            upper: cast_scalar_value(&self.upper, data_type, cast_options)?,
        })
    }

    pub(crate) fn is_boolean(&self) -> bool {
        matches!(
            self,
            Interval {
                lower: ScalarValue::Boolean(_),
                upper: ScalarValue::Boolean(_),
            }
        )
    }

    pub(crate) fn get_datatype(&self) -> DataType {
        self.lower.get_datatype()
    }

    pub(crate) fn gt(&self, other: &Interval) -> Interval {
        let flags = if self.upper < other.lower {
            (false, false)
        } else if self.lower > other.upper {
            (true, true)
        } else {
            (false, true)
        };
        Interval {
            lower: ScalarValue::Boolean(Some(flags.0)),
            upper: ScalarValue::Boolean(Some(flags.1)),
        }
    }

    pub(crate) fn and(&self, other: &Interval) -> Result<Interval> {
        let flags = match (self, other) {
            (
                Interval {
                    lower: ScalarValue::Boolean(Some(lower)),
                    upper: ScalarValue::Boolean(Some(upper)),
                },
                Interval {
                    lower: ScalarValue::Boolean(Some(lower2)),
                    upper: ScalarValue::Boolean(Some(upper2)),
                },
            ) => {
                if *lower && *lower2 {
                    (true, true)
                } else if *upper || *upper2 {
                    (false, true)
                } else {
                    (false, false)
                }
            }
            _ => {
                return Err(DataFusionError::Internal(
                    "Incompatible types for logical conjunction".to_string(),
                ))
            }
        };
        Ok(Interval {
            lower: ScalarValue::Boolean(Some(flags.0)),
            upper: ScalarValue::Boolean(Some(flags.1)),
        })
    }

    pub(crate) fn equal(&self, other: &Interval) -> Interval {
        let flags = if (self.lower == self.upper)
            && (other.lower == other.upper)
            && (self.lower == other.lower)
        {
            (true, true)
        } else if (self.lower > other.upper) || (self.upper < other.lower) {
            (false, false)
        } else {
            (false, true)
        };
        Interval {
            lower: ScalarValue::Boolean(Some(flags.0)),
            upper: ScalarValue::Boolean(Some(flags.1)),
        }
    }

    pub(crate) fn lt(&self, other: &Interval) -> Interval {
        other.gt(self)
    }

    pub(crate) fn intersect(&self, other: &Interval) -> Result<Option<Interval>> {
        Ok(match (self, other) {
            (
                Interval {
                    lower: ScalarValue::Boolean(Some(low)),
                    upper: ScalarValue::Boolean(Some(high)),
                },
                Interval {
                    lower: ScalarValue::Boolean(Some(other_low)),
                    upper: ScalarValue::Boolean(Some(other_high)),
                },
            ) => {
                if low > other_high || high < other_low {
                    // This None value signals an empty interval.
                    None
                } else if (low == high) && (other_low == other_high) && (low == other_low)
                {
                    Some(self.clone())
                } else {
                    Some(Interval {
                        lower: ScalarValue::Boolean(Some(false)),
                        upper: ScalarValue::Boolean(Some(true)),
                    })
                }
            }
            (
                Interval {
                    lower: lower1,
                    upper: upper1,
                },
                Interval {
                    lower: lower2,
                    upper: upper2,
                },
            ) => {
                let lower = if lower1.is_null() {
                    lower2.clone()
                } else if lower2.is_null() {
                    lower1.clone()
                } else {
                    max(lower1, lower2)?
                };
                let upper = if upper1.is_null() {
                    upper2.clone()
                } else if upper2.is_null() {
                    upper1.clone()
                } else {
                    min(upper1, upper2)?
                };
                if !upper.is_null() && lower > upper {
                    // This None value signals an empty interval.
                    None
                } else {
                    Some(Interval { lower, upper })
                }
            }
        })
    }

    pub(crate) fn arithmetic_negate(&self) -> Result<Interval> {
        Ok(Interval {
            lower: self.upper.arithmetic_negate()?,
            upper: self.lower.arithmetic_negate()?,
        })
    }

    /// The interval addition operation takes two intervals, say [a1, b1] and [a2, b2],
    /// and returns a new interval that represents the range of possible values obtained by adding
    /// any value from the first interval to any value from the second interval.
    /// The resulting interval is defined as [a1 + a2, b1 + b2]. For example,
    /// if we have the intervals [1, 2] and [3, 4], the interval addition
    /// operation would return the interval [4, 6].
    pub fn add<T: Borrow<Interval>>(&self, other: T) -> Result<Interval> {
        let rhs = other.borrow();
        let lower = if self.lower.is_null() || rhs.lower.is_null() {
            ScalarValue::try_from(self.lower.get_datatype())
        } else {
            self.lower.add(&rhs.lower)
        }?;
        let upper = if self.upper.is_null() || rhs.upper.is_null() {
            ScalarValue::try_from(self.upper.get_datatype())
        } else {
            self.upper.add(&rhs.upper)
        }?;
        Ok(Interval { lower, upper })
    }

    /// The interval subtraction operation is similar to the addition operation,
    /// but it subtracts one interval from another. The resulting interval is defined as [a1 - b2, b1 - a2].
    /// For example, if we have the intervals [1, 2] and [3, 4], the interval subtraction operation
    /// would return the interval [-3, -1].
    pub fn sub<T: Borrow<Interval>>(&self, other: T) -> Result<Interval> {
        let rhs = other.borrow();
        let lower = if self.lower.is_null() || rhs.upper.is_null() {
            ScalarValue::try_from(self.lower.get_datatype())
        } else {
            self.lower.sub(&rhs.upper)
        }?;
        let upper = if self.upper.is_null() || rhs.lower.is_null() {
            ScalarValue::try_from(self.upper.get_datatype())
        } else {
            self.upper.sub(&rhs.lower)
        }?;
        Ok(Interval { lower, upper })
    }
}

pub fn apply_operator(lhs: &Interval, op: &Operator, rhs: &Interval) -> Result<Interval> {
    match *op {
        Operator::Eq => Ok(lhs.equal(rhs)),
        Operator::Gt => Ok(lhs.gt(rhs)),
        Operator::Lt => Ok(lhs.lt(rhs)),
        Operator::And => lhs.and(rhs),
        Operator::Plus => lhs.add(rhs),
        Operator::Minus => lhs.sub(rhs),
        _ => Ok(Interval {
            lower: ScalarValue::Null,
            upper: ScalarValue::Null,
        }),
    }
}

pub fn target_parent_interval(datatype: &DataType, op: &Operator) -> Result<Interval> {
    let unbounded = ScalarValue::try_from(datatype)?;
    let zero = ScalarValue::try_from_string("0".to_string(), datatype)?;
    Ok(match *op {
        // TODO: Tidy these comments, write a text that explains what this function does.
        // [x1, y1] > [x2, y2] ise
        // [x1, y1] + [-y2, -x2] = [0, inf]
        // [x1_new, x2_new] = ([0, inf] - [-y2, -x2]) intersect [x1, y1]
        // [-y2_new, -x2_new] = ([0, inf] - [x1_new, x2_new]) intersect [-y2, -x2]
        Operator::Gt => Interval {
            lower: zero,
            upper: unbounded,
        },
        // TODO: Tidy these comments.
        // [x1, y1] < [x2, y2] ise
        // [x1, y1] + [-y2, -x2] = [-inf, 0]
        Operator::Lt => Interval {
            lower: unbounded,
            upper: zero,
        },
        _ => unreachable!(),
    })
}

pub fn propagate_logical_operators(
    left_interval: &Interval,
    op: &Operator,
    right_interval: &Interval,
) -> Result<(Interval, Interval)> {
    if left_interval.is_boolean() || right_interval.is_boolean() {
        return Ok((
            Interval {
                lower: ScalarValue::Boolean(Some(true)),
                upper: ScalarValue::Boolean(Some(true)),
            },
            Interval {
                lower: ScalarValue::Boolean(Some(true)),
                upper: ScalarValue::Boolean(Some(true)),
            },
        ));
    }
    let intersection = left_interval.intersect(right_interval)?;
    // TODO: Is this right with the new "intersect" semantics?
    if intersection.is_none() {
        return Ok((left_interval.clone(), right_interval.clone()));
    }
    let parent_interval = target_parent_interval(&left_interval.get_datatype(), op)?;
    let negate_right = right_interval.arithmetic_negate()?;
    // TODO: Fix the following unwraps.
    let new_left = parent_interval
        .sub(&negate_right)?
        .intersect(left_interval)?
        .unwrap();
    let new_right = parent_interval
        .sub(&new_left)?
        .intersect(&negate_right)?
        .unwrap();
    let range = (new_left, new_right.arithmetic_negate()?);
    Ok(range)
}

pub fn negate_ops(op: Operator) -> Operator {
    match op {
        Operator::Plus => Operator::Minus,
        Operator::Minus => Operator::Plus,
        _ => unreachable!(),
    }
}
