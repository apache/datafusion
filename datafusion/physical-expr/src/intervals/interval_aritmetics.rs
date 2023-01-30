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

use arrow::compute::CastOptions;
use arrow::datatypes::DataType;
use datafusion_common::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::Operator;

use crate::aggregate::min_max::{max, min};
use crate::intervals::cp_solver;

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

impl Interval {
    pub(crate) fn cast_to(
        &self,
        data_type: &DataType,
        cast_options: &CastOptions,
    ) -> Result<Interval> {
        Ok(Interval {
            lower: cp_solver::cast_scalar_value(&self.lower, data_type, cast_options)?,
            upper: cp_solver::cast_scalar_value(&self.upper, data_type, cast_options)?,
        })
    }

    pub(crate) fn get_datatype(&self) -> DataType {
        self.lower.get_datatype()
    }

    /// Null is treated as infinite.
    /// [a , b] > [c, d]
    /// where a, b, c or d can be infinite or a real value. We use None if a or c is minus infinity or
    /// b or d is infinity. [10,20], [10, ∞], [-∞, 100] or [-∞, ∞] are all valid intervals.
    /// While we are calculating correct comparison, we take the PartialOrd implementation of ScalarValue,
    /// which is
    ///     None < a true
    ///     a < None false
    ///     None > a false
    ///     a > None true
    ///     None < None false
    ///     None > None false
    pub(crate) fn gt(&self, other: &Interval) -> Interval {
        let flags = if !self.upper.is_null()
            && !other.lower.is_null()
            && self.upper < other.lower
        {
            (false, false)
            // If lhs has lower negative infinity or rhs has upper infinity, this can not be completely true.
        } else if !other.upper.is_null()
            && !self.lower.is_null()
            && (self.lower > other.upper)
        {
            (true, true)
        } else {
            (false, true)
        };
        Interval {
            lower: ScalarValue::Boolean(Some(flags.0)),
            upper: ScalarValue::Boolean(Some(flags.1)),
        }
    }
    /// We define vague result as (false, true). If we "AND" a vague value with another,
    /// result is
    ///  vague AND false -> false
    ///  vague AND true -> vague,
    ///  vague AND vague -> vague
    ///  false AND true -> true
    ///
    /// where "false" represented with (false, false), "true" represented with (true, true) and
    /// vague represented with (false, true).
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
                } else if *upper && *upper2 {
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
    /// Null is treated as infinite.
    /// [a , b] < [c, d]
    /// where a, b, c or d can be infinite or a real value. We use None if a or c is minus infinity or
    /// b or d is infinity. [10,20], [10, ∞], [-∞, 100] or [-∞, ∞] are all valid intervals.
    /// Implemented as complementary of greater than.
    pub(crate) fn lt(&self, other: &Interval) -> Interval {
        other.gt(self)
    }

    /// Null is treated as infinite.
    /// [a , b] < [c, d]
    /// where a, b, c or d can be infinite or a real value. We use None if a or c is minus infinity or
    /// b or d is infinity. [10,20], [10, ∞], [-∞, 100] or [-∞, ∞] are all valid intervals.
    pub(crate) fn intersect(&self, other: &Interval) -> Result<Option<Interval>> {
        Ok(match (self, other) {
            (
                Interval {
                    lower: ScalarValue::Boolean(_),
                    upper: ScalarValue::Boolean(_),
                },
                Interval {
                    lower: ScalarValue::Boolean(_),
                    upper: ScalarValue::Boolean(_),
                },
            ) => None,
            (
                Interval {
                    lower: low,
                    upper: high,
                },
                Interval {
                    lower: other_low,
                    upper: other_high,
                },
            ) => {
                let lower = if low.is_null() {
                    other_low.clone()
                } else if other_low.is_null() {
                    low.clone()
                } else {
                    max(low, other_low)?
                };
                let upper = if high.is_null() {
                    other_high.clone()
                } else if other_high.is_null() {
                    high.clone()
                } else {
                    min(high, other_high)?
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

pub fn negate_ops(op: Operator) -> Operator {
    match op {
        Operator::Plus => Operator::Minus,
        Operator::Minus => Operator::Plus,
        _ => unreachable!(),
    }
}
#[cfg(test)]
mod tests {
    use crate::intervals::interval_aritmetics::Interval;
    use datafusion_common::Result;
    use datafusion_common::ScalarValue;

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
            (None, Some(1000), Some(1000), None, Some(1000), Some(1000)), // singleton
            (None, None, None, None, None, None),
        ];

        for case in possible_cases {
            assert_eq!(
                Interval {
                    lower: ScalarValue::Int64(case.0),
                    upper: ScalarValue::Int64(case.1)
                }
                .intersect(&Interval {
                    lower: ScalarValue::Int64(case.2),
                    upper: ScalarValue::Int64(case.3)
                })?
                .unwrap(),
                Interval {
                    lower: ScalarValue::Int64(case.4),
                    upper: ScalarValue::Int64(case.5)
                }
            )
        }

        let not_possible_cases = vec![
            (None, Some(1000), Some(1001), None),
            (Some(1001), None, None, Some(1000)),
            (None, Some(1000), Some(1001), Some(1002)),
            (Some(1001), Some(1002), None, Some(1000)),
        ];
        //(None, Some(1000), Some(1001), None, --),
        // (None, Some(1000), Some(1000), None, false, true),
        // (None, Some(1000), Some(1001), Some(1002), false, false),

        for case in not_possible_cases {
            assert_eq!(
                Interval {
                    lower: ScalarValue::Int64(case.0),
                    upper: ScalarValue::Int64(case.1)
                }
                .intersect(&Interval {
                    lower: ScalarValue::Int64(case.2),
                    upper: ScalarValue::Int64(case.3)
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
            (None, Some(1000), Some(1000), None, false, true),
            (None, Some(1000), Some(1001), None, false, false),
            (Some(1000), None, Some(1000), None, false, true),
            (None, Some(1000), Some(1001), Some(1002), false, false),
            (None, Some(1000), Some(999), Some(1002), false, true),
            (None, None, None, None, false, true),
        ];

        for case in cases {
            assert_eq!(
                Interval {
                    lower: ScalarValue::Int64(case.0),
                    upper: ScalarValue::Int64(case.1)
                }
                .gt(&Interval {
                    lower: ScalarValue::Int64(case.2),
                    upper: ScalarValue::Int64(case.3)
                }),
                Interval {
                    lower: ScalarValue::Boolean(Some(case.4)),
                    upper: ScalarValue::Boolean(Some(case.5))
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
            (None, Some(1000), Some(1000), None, false, true),
            (None, Some(1000), Some(1001), None, true, true),
            (Some(1000), None, Some(1000), None, false, true),
            (None, Some(1000), Some(1001), Some(1002), true, true),
            (None, Some(1000), Some(999), Some(1002), false, true),
            (None, None, None, None, false, true),
        ];

        for case in cases {
            assert_eq!(
                Interval {
                    lower: ScalarValue::Int64(case.0),
                    upper: ScalarValue::Int64(case.1)
                }
                .lt(&Interval {
                    lower: ScalarValue::Int64(case.2),
                    upper: ScalarValue::Int64(case.3)
                }),
                Interval {
                    lower: ScalarValue::Boolean(Some(case.4)),
                    upper: ScalarValue::Boolean(Some(case.5))
                }
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
                    lower: ScalarValue::Boolean(Some(case.0)),
                    upper: ScalarValue::Boolean(Some(case.1))
                }
                .and(&Interval {
                    lower: ScalarValue::Boolean(Some(case.2)),
                    upper: ScalarValue::Boolean(Some(case.3))
                })?,
                Interval {
                    lower: ScalarValue::Boolean(Some(case.4)),
                    upper: ScalarValue::Boolean(Some(case.5))
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
                    lower: ScalarValue::Int64(case.0),
                    upper: ScalarValue::Int64(case.1)
                }
                .add(&Interval {
                    lower: ScalarValue::Int64(case.2),
                    upper: ScalarValue::Int64(case.3)
                })?,
                Interval {
                    lower: ScalarValue::Int64(case.4),
                    upper: ScalarValue::Int64(case.5)
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
                    lower: ScalarValue::Int64(case.0),
                    upper: ScalarValue::Int64(case.1)
                }
                .sub(&Interval {
                    lower: ScalarValue::Int64(case.2),
                    upper: ScalarValue::Int64(case.3)
                })?,
                Interval {
                    lower: ScalarValue::Int64(case.4),
                    upper: ScalarValue::Int64(case.5)
                }
            )
        }
        Ok(())
    }
}
