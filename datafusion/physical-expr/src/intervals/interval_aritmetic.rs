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
use datafusion_expr::type_coercion::binary::coerce_types;
use datafusion_expr::Operator;

use crate::aggregate::min_max::{max, min};

/// This type represents an interval, which is used to calculate reliable
/// bounds for expressions. Currently, we only support addition and
/// subtraction, but more capabilities will be added in the future.
/// Upper/lower bounds having NULL values indicate an unbounded side. For
/// example; [10, 20], [10, ∞], [-∞, 100] and [-∞, ∞] are all valid intervals.
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
            lower: cast_scalar_value(&self.lower, data_type, cast_options)?,
            upper: cast_scalar_value(&self.upper, data_type, cast_options)?,
        })
    }

    pub(crate) fn get_datatype(&self) -> DataType {
        self.lower.get_datatype()
    }

    /// Decide if this interval is certainly greater than, possibly greater than,
    /// or can't be greater than `other` by returning [true, true],
    /// [false, true] or [false, false] respectively.
    pub(crate) fn gt(&self, other: &Interval) -> Interval {
        let flags = if !self.upper.is_null()
            && !other.lower.is_null()
            && (self.upper <= other.lower)
        {
            (false, false)
        } else if !self.lower.is_null()
            && !other.upper.is_null()
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

    /// Decide if this interval is certainly less than, possibly less than,
    /// or can't be less than `other` by returning [true, true],
    /// [false, true] or [false, false] respectively.
    pub(crate) fn lt(&self, other: &Interval) -> Interval {
        other.gt(self)
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
        } else if (!self.lower.is_null()
            && !other.upper.is_null()
            && (self.lower > other.upper))
            || (!self.upper.is_null()
                && !other.lower.is_null()
                && (self.upper < other.lower))
        {
            (false, false)
        } else {
            (false, true)
        };
        Interval {
            lower: ScalarValue::Boolean(Some(flags.0)),
            upper: ScalarValue::Boolean(Some(flags.1)),
        }
    }

    /// Compute the logical conjunction of this (boolean) interval with the
    /// given boolean interval.
    pub(crate) fn and(&self, other: &Interval) -> Result<Interval> {
        let flags = match (self, other) {
            (
                Interval {
                    lower: ScalarValue::Boolean(Some(lower)),
                    upper: ScalarValue::Boolean(Some(upper)),
                },
                Interval {
                    lower: ScalarValue::Boolean(Some(other_lower)),
                    upper: ScalarValue::Boolean(Some(other_upper)),
                },
            ) => {
                if *lower && *other_lower {
                    (true, true)
                } else if *upper && *other_upper {
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

    /// Compute the intersection of the interval with the given interval.
    /// If the intersection is empty, return None.
    pub(crate) fn intersect(&self, other: &Interval) -> Result<Option<Interval>> {
        let lower = if self.lower.is_null() {
            other.lower.clone()
        } else if other.lower.is_null() {
            self.lower.clone()
        } else {
            max(&self.lower, &other.lower)?
        };
        let upper = if self.upper.is_null() {
            other.upper.clone()
        } else if other.upper.is_null() {
            self.upper.clone()
        } else {
            min(&self.upper, &other.upper)?
        };
        Ok(if !lower.is_null() && !upper.is_null() && lower > upper {
            // This None value signals an empty interval.
            None
        } else {
            Some(Interval { lower, upper })
        })
    }

    // Compute the negation of the interval.
    #[allow(dead_code)]
    pub(crate) fn arithmetic_negate(&self) -> Result<Interval> {
        Ok(Interval {
            lower: self.upper.arithmetic_negate()?,
            upper: self.lower.arithmetic_negate()?,
        })
    }

    /// Add the given interval (`other`) to this interval. Say we have
    /// intervals [a1, b1] and [a2, b2], then their sum is [a1 + a2, b1 + b2].
    /// Note that this represents all possible values the sum can take if
    /// one can choose single values arbitrarily from each of the operands.
    pub fn add<T: Borrow<Interval>>(&self, other: T) -> Result<Interval> {
        let rhs = other.borrow();
        let mut datatype =
            coerce_types(&self.get_datatype(), &Operator::Minus, &rhs.get_datatype());
        if datatype.is_err() {
            datatype =
                coerce_types(&rhs.get_datatype(), &Operator::Minus, &self.get_datatype());
        }
        let datatype = datatype?;
        let lower = if self.lower.is_null() || rhs.lower.is_null() {
            ScalarValue::try_from(&datatype)
        } else {
            self.lower.add(&rhs.lower)
        }?;
        let upper = if self.upper.is_null() || rhs.upper.is_null() {
            ScalarValue::try_from(&datatype)
        } else {
            self.upper.add(&rhs.upper)
        }?;
        Ok(Interval { lower, upper })
    }

    /// Subtract the given interval (`other`) from this interval. Say we have
    /// intervals [a1, b1] and [a2, b2], then their sum is [a1 - b2, b1 - a2].
    /// Note that this represents all possible values the difference can take
    /// if one can choose single values arbitrarily from each of the operands.
    pub fn sub<T: Borrow<Interval>>(&self, other: T) -> Result<Interval> {
        let rhs = other.borrow();
        let lower = if self.lower.is_null() || rhs.upper.is_null() {
            ScalarValue::try_from(coerce_types(
                &self.lower.get_datatype(),
                &Operator::Minus,
                &rhs.lower.get_datatype(),
            )?)
        } else {
            self.lower.sub(&rhs.upper)
        }?;
        let upper = if self.upper.is_null() || rhs.lower.is_null() {
            ScalarValue::try_from(coerce_types(
                &self.upper.get_datatype(),
                &Operator::Minus,
                &rhs.upper.get_datatype(),
            )?)
        } else {
            self.upper.sub(&rhs.lower)
        }?;
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
    use crate::intervals::Interval;
    use datafusion_common::{Result, ScalarValue};

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

        let empty_cases = vec![
            (None, Some(1000), Some(1001), None),
            (Some(1001), None, None, Some(1000)),
            (None, Some(1000), Some(1001), Some(1002)),
            (Some(1001), Some(1002), None, Some(1000)),
        ];

        for case in empty_cases {
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
            (None, Some(1000), Some(1000), None, false, false),
            (None, Some(1000), Some(1001), None, false, false),
            (Some(1000), None, Some(1000), None, false, true),
            (None, Some(1000), Some(1001), Some(1002), false, false),
            (None, Some(1000), Some(999), Some(1002), false, true),
            (Some(1002), None, Some(999), Some(1002), false, true),
            (Some(1003), None, Some(999), Some(1002), true, true),
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
