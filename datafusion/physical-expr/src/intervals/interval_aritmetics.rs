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

//! Interval arithmetics library
//!
use std::borrow::Borrow;

use arrow::datatypes::DataType;

use datafusion_common::ScalarValue;
use datafusion_common::{DataFusionError, Result};

use datafusion_expr::Operator;

use std::fmt;
use std::fmt::{Display, Formatter};

use crate::aggregate::min_max::{max, min};

use arrow::compute::{kernels, CastOptions};
use std::ops::{Add, Sub};

#[derive(Debug, PartialEq, Clone, Eq, Hash)]
pub struct Range {
    pub lower: ScalarValue,
    pub upper: ScalarValue,
}

impl Range {
    fn new(lower: ScalarValue, upper: ScalarValue) -> Range {
        Range { lower, upper }
    }
}

impl Default for Range {
    fn default() -> Self {
        Range {
            lower: ScalarValue::Null,
            upper: ScalarValue::Null,
        }
    }
}

impl Add for Range {
    type Output = Range;

    fn add(self, other: Range) -> Range {
        Range::new(
            self.lower.add(&other.lower).unwrap(),
            self.upper.add(&other.upper).unwrap(),
        )
    }
}

impl Sub for Range {
    type Output = Range;

    fn sub(self, other: Range) -> Range {
        Range::new(
            self.lower.add(&other.lower).unwrap(),
            self.upper.sub(&other.lower).unwrap(),
        )
    }
}

impl Display for Interval {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Interval::Singleton(value) => write!(f, "Singleton [{}]", value),
            Interval::Range(Range { lower, upper }) => {
                write!(f, "Range [{}, {}]", lower, upper)
            }
        }
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Interval {
    Singleton(ScalarValue),
    Range(Range),
}

impl Default for Interval {
    fn default() -> Self {
        Interval::Range(Range::default())
    }
}

impl Interval {
    pub(crate) fn cast_to(
        &self,
        data_type: &DataType,
        cast_options: &CastOptions,
    ) -> Result<Interval> {
        Ok(match self {
            Interval::Range(Range { lower, upper }) => Interval::Range(Range {
                lower: cast_scalar_value(lower, data_type, cast_options)?,
                upper: cast_scalar_value(upper, data_type, cast_options)?,
            }),
            Interval::Singleton(value) => {
                Interval::Singleton(cast_scalar_value(value, data_type, cast_options)?)
            }
        })
    }
    pub(crate) fn is_boolean(&self) -> bool {
        matches!(
            self,
            &Interval::Range(Range {
                lower: ScalarValue::Boolean(_),
                upper: ScalarValue::Boolean(_),
            })
        )
    }

    pub fn lower_value(&self) -> ScalarValue {
        match self {
            Interval::Range(Range { lower, .. }) => lower.clone(),
            Interval::Singleton(val) => val.clone(),
        }
    }

    pub fn upper_value(&self) -> ScalarValue {
        match self {
            Interval::Range(Range { upper, .. }) => upper.clone(),
            Interval::Singleton(val) => val.clone(),
        }
    }

    pub(crate) fn get_datatype(&self) -> DataType {
        match self {
            Interval::Range(Range { lower, .. }) => lower.get_datatype(),
            Interval::Singleton(val) => val.get_datatype(),
        }
    }
    pub(crate) fn is_singleton(&self) -> bool {
        match self {
            Interval::Range(_) => false,
            Interval::Singleton(_) => true,
        }
    }
    pub(crate) fn is_strict_false(&self) -> bool {
        matches!(
            self,
            &Interval::Range(Range {
                lower: ScalarValue::Boolean(Some(false)),
                upper: ScalarValue::Boolean(Some(false)),
            })
        )
    }
    pub(crate) fn gt(&self, other: &Interval) -> Interval {
        let bools = match (self, other) {
            (Interval::Singleton(val), Interval::Singleton(val2)) => {
                let equal = val > val2;
                (equal, equal)
            }
            (Interval::Singleton(val), Interval::Range(Range { lower, upper })) => {
                if val < lower {
                    (false, false)
                } else if val > upper {
                    (true, true)
                } else {
                    (false, true)
                }
            }
            (Interval::Range(Range { lower, upper }), Interval::Singleton(val)) => {
                if val > upper {
                    (false, false)
                } else if lower > val {
                    (true, true)
                } else {
                    (false, true)
                }
            }
            (
                Interval::Range(Range { lower, upper }),
                Interval::Range(Range {
                    lower: lower2,
                    upper: upper2,
                }),
            ) => {
                if upper < lower2 {
                    (false, false)
                } else if lower > upper2 {
                    (true, true)
                } else {
                    (false, true)
                }
            }
        };
        Interval::Range(Range {
            lower: ScalarValue::Boolean(Some(bools.0)),
            upper: ScalarValue::Boolean(Some(bools.1)),
        })
    }

    pub(crate) fn and(&self, other: &Interval) -> Result<Interval> {
        let bools = match (self, other) {
            (
                Interval::Singleton(ScalarValue::Boolean(Some(val))),
                Interval::Singleton(ScalarValue::Boolean(Some(val2))),
            ) => {
                let equal = *val && *val2;
                (equal, equal)
            }
            (
                Interval::Singleton(ScalarValue::Boolean(Some(val))),
                Interval::Range(Range {
                    lower: ScalarValue::Boolean(Some(lower)),
                    upper: ScalarValue::Boolean(Some(upper)),
                }),
            )
            | (
                Interval::Range(Range {
                    lower: ScalarValue::Boolean(Some(lower)),
                    upper: ScalarValue::Boolean(Some(upper)),
                }),
                Interval::Singleton(ScalarValue::Boolean(Some(val))),
            ) => {
                if (*val && *lower) && *upper {
                    (true, true)
                } else if !(*val && *lower) && *upper {
                    (false, false)
                } else {
                    (false, true)
                }
            }
            (
                Interval::Range(Range {
                    lower: ScalarValue::Boolean(Some(lower)),
                    upper: ScalarValue::Boolean(Some(upper)),
                }),
                Interval::Range(Range {
                    lower: ScalarValue::Boolean(Some(lower2)),
                    upper: ScalarValue::Boolean(Some(upper2)),
                }),
            ) => {
                if (*lower && *lower2) && (*upper && *upper2) {
                    (true, true)
                } else if (*lower || *lower2) || (*upper || *upper2) {
                    (false, true)
                } else {
                    (false, false)
                }
            }
            _ => {
                return Err(DataFusionError::Internal(
                    "Booleans cannot be None.".to_string(),
                ))
            }
        };
        Ok(Interval::Range(Range {
            lower: ScalarValue::Boolean(Some(bools.0)),
            upper: ScalarValue::Boolean(Some(bools.1)),
        }))
    }

    pub(crate) fn equal(&self, other: &Interval) -> Interval {
        let bool = match (self, other) {
            (Interval::Singleton(val), Interval::Singleton(val2)) => {
                let equal = val == val2;
                (equal, equal)
            }
            (Interval::Singleton(val), Interval::Range(Range { lower, upper })) => {
                let equal = lower >= val && val <= upper;
                (equal, equal)
            }
            (Interval::Range(Range { lower, upper }), Interval::Singleton(val)) => {
                let equal = lower >= val && val <= upper;
                (equal, equal)
            }
            (
                Interval::Range(Range { lower, upper }),
                Interval::Range(Range {
                    lower: lower2,
                    upper: upper2,
                }),
            ) => {
                if lower == lower2 && upper == upper2 {
                    (true, true)
                } else if (lower < lower2 && upper < upper2)
                    || (lower > lower2 && upper > upper2)
                {
                    (false, false)
                } else {
                    (false, true)
                }
            }
        };
        Interval::Range(Range {
            lower: ScalarValue::Boolean(Some(bool.0)),
            upper: ScalarValue::Boolean(Some(bool.1)),
        })
    }

    pub(crate) fn lt(&self, other: &Interval) -> Interval {
        let bool = match (self, other) {
            (Interval::Singleton(val), Interval::Singleton(val2)) => {
                let result = val < val2;
                (result, result)
            }
            (Interval::Singleton(ref val), Interval::Range(Range { lower, upper })) => {
                if val > upper {
                    (false, false)
                } else if val < lower {
                    (true, true)
                } else {
                    (true, false)
                }
            }
            (
                Interval::Range(Range {
                    ref lower,
                    ref upper,
                }),
                Interval::Singleton(val),
            ) => {
                if val < upper {
                    (false, false)
                } else if lower > val {
                    (true, true)
                } else {
                    (true, false)
                }
            }
            (
                Interval::Range(Range {
                    ref lower,
                    ref upper,
                }),
                Interval::Range(Range {
                    lower: lower2,
                    upper: upper2,
                }),
            ) => {
                if upper < lower2 {
                    (true, true)
                } else if lower >= upper2 {
                    (false, false)
                } else {
                    (true, false)
                }
            }
        };
        Interval::Range(Range {
            lower: ScalarValue::Boolean(Some(bool.0)),
            upper: ScalarValue::Boolean(Some(bool.1)),
        })
    }

    pub(crate) fn intersect(&self, other: &Interval) -> Result<Interval> {
        let result = match (self, other) {
            (Interval::Singleton(val1), Interval::Singleton(val2)) => {
                if val1 == val2 {
                    Interval::Singleton(val1.clone())
                } else {
                    Interval::Range(Range {
                        lower: ScalarValue::Boolean(Some(false)),
                        upper: ScalarValue::Boolean(Some(false)),
                    })
                }
            }
            (Interval::Singleton(val), Interval::Range(range)) => {
                if val >= &range.lower && val <= &range.upper {
                    Interval::Singleton(val.clone())
                } else {
                    Interval::Range(Range {
                        lower: ScalarValue::Boolean(Some(false)),
                        upper: ScalarValue::Boolean(Some(false)),
                    })
                }
            }
            (Interval::Range(Range { lower, upper }), Interval::Singleton(val)) => {
                if (lower.is_null() && upper.is_null()) || (val >= lower && val <= upper)
                {
                    Interval::Singleton(val.clone())
                } else {
                    Interval::Range(Range {
                        lower: ScalarValue::Boolean(Some(false)),
                        upper: ScalarValue::Boolean(Some(false)),
                    })
                }
            }
            (
                Interval::Range(Range {
                    lower: ScalarValue::Boolean(Some(val)),
                    upper: ScalarValue::Boolean(Some(val2)),
                }),
                Interval::Range(Range {
                    lower: ScalarValue::Boolean(Some(val3)),
                    upper: ScalarValue::Boolean(Some(val4)),
                }),
            ) => Interval::Range(Range {
                lower: ScalarValue::Boolean(Some(*val || *val3)),
                upper: ScalarValue::Boolean(Some(*val2 || *val4)),
            }),
            (
                Interval::Range(Range {
                    lower: lower1,
                    upper: upper1,
                }),
                Interval::Range(Range {
                    lower: lower2,
                    upper: upper2,
                }),
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
                    Interval::Range(Range {
                        lower: ScalarValue::Boolean(Some(false)),
                        upper: ScalarValue::Boolean(Some(false)),
                    })
                } else {
                    Interval::Range(Range { lower, upper })
                }
            }
        };
        Ok(result)
    }

    pub(crate) fn arithmetic_negate(&self) -> Result<Interval> {
        Ok(match self {
            Interval::Singleton(value) => Interval::Singleton(value.arithmetic_negate()?),
            Interval::Range(Range { lower, upper }) => Interval::Range(Range {
                lower: upper.arithmetic_negate()?,
                upper: lower.arithmetic_negate()?,
            }),
        })
    }

    pub fn add<T: Borrow<Interval>>(&self, other: T) -> Result<Interval> {
        let rhs = other.borrow();
        let result = match (self, rhs) {
            (Interval::Singleton(val), Interval::Singleton(val2)) => {
                Interval::Singleton(val.add(val2).unwrap())
            }
            (Interval::Singleton(val), Interval::Range(Range { lower, upper }))
            | (Interval::Range(Range { lower, upper }), Interval::Singleton(val)) => {
                let new_lower = if lower.is_null() {
                    lower.clone()
                } else {
                    lower.add(val).unwrap()
                };
                let new_upper = if upper.is_null() {
                    upper.clone()
                } else {
                    upper.add(val).unwrap()
                };
                Interval::Range(Range {
                    lower: new_lower,
                    upper: new_upper,
                })
            }

            (
                Interval::Range(Range { lower, upper }),
                Interval::Range(Range {
                    lower: lower2,
                    upper: upper2,
                }),
            ) => {
                let new_lower = if lower.is_null() || lower2.is_null() {
                    ScalarValue::try_from(lower.get_datatype()).unwrap()
                } else {
                    lower.add(lower2).unwrap()
                };

                let new_upper = if upper.is_null() || upper2.is_null() {
                    ScalarValue::try_from(upper.get_datatype()).unwrap()
                } else {
                    upper.add(upper2).unwrap()
                };
                Interval::Range(Range {
                    lower: new_lower,
                    upper: new_upper,
                })
            }
        };
        Ok(result)
    }

    pub fn sub<T: Borrow<Interval>>(&self, other: T) -> Result<Interval> {
        let rhs = other.borrow();
        let result = match (self, rhs) {
            (Interval::Singleton(val), Interval::Singleton(val2)) => {
                Interval::Singleton(val.sub(val2).unwrap())
            }
            (Interval::Singleton(val), Interval::Range(Range { lower, upper })) => {
                let new_lower = if lower.is_null() {
                    lower.clone()
                } else {
                    lower.add(val)?.arithmetic_negate()?
                };
                let new_upper = if upper.is_null() {
                    upper.clone()
                } else {
                    upper.add(val)?.arithmetic_negate()?
                };
                Interval::Range(Range {
                    lower: new_lower,
                    upper: new_upper,
                })
            }
            (Interval::Range(Range { lower, upper }), Interval::Singleton(val)) => {
                let new_lower = if lower.is_null() {
                    lower.clone()
                } else {
                    lower.sub(val)?
                };
                let new_upper = if upper.is_null() {
                    upper.clone()
                } else {
                    upper.sub(val)?
                };
                Interval::Range(Range {
                    lower: new_lower,
                    upper: new_upper,
                })
            }
            (
                Interval::Range(Range { lower, upper }),
                Interval::Range(Range {
                    lower: lower2,
                    upper: upper2,
                }),
            ) => {
                let new_lower = if lower.is_null() || upper2.is_null() {
                    ScalarValue::try_from(&lower.get_datatype())?
                } else {
                    lower.sub(upper2)?
                };

                let new_upper = if upper.is_null() || lower2.is_null() {
                    ScalarValue::try_from(&upper.get_datatype())?
                } else {
                    upper.sub(lower2)?
                };

                Interval::Range(Range {
                    lower: new_lower,
                    upper: new_upper,
                })
            }
        };
        Ok(result)
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
        _ => Ok(Interval::Singleton(ScalarValue::Null)),
    }
}

pub fn target_parent_interval(datatype: &DataType, op: &Operator) -> Result<Interval> {
    let inf = ScalarValue::try_from(datatype)?;
    let zero = ScalarValue::try_from_string("0".to_string(), datatype)?;
    let parent_interval = match *op {
        // [x1, y1] > [x2, y2] ise
        // [x1, y1] + [-y2, -x2] = [0, inf]
        // [x1_new, x2_new] = ([0, inf] - [-y2, -x2]) intersect [x1, y1]
        // [-y2_new, -x2_new] = ([0, inf] - [x1_new, x2_new]) intersect [-y2, -x2]
        Operator::Gt => {
            // [0, inf]
            Interval::Range(Range {
                lower: zero,
                upper: inf,
            })
        }
        // [x1, y1] < [x2, y2] ise
        // [x1, y1] + [-y2, -x2] = [-inf, 0]
        Operator::Lt => {
            // [-inf, 0]
            Interval::Range(Range {
                lower: inf,
                upper: zero,
            })
        }
        _ => unreachable!(),
    };
    Ok(parent_interval)
}

pub fn propagate_logical_operators(
    left_interval: &Interval,
    op: &Operator,
    right_interval: &Interval,
) -> Result<(Interval, Interval)> {
    if left_interval.is_boolean() || right_interval.is_boolean() {
        return Ok((
            Interval::Range(Range {
                lower: ScalarValue::Boolean(Some(true)),
                upper: ScalarValue::Boolean(Some(true)),
            }),
            Interval::Range(Range {
                lower: ScalarValue::Boolean(Some(true)),
                upper: ScalarValue::Boolean(Some(true)),
            }),
        ));
    }

    let intersection = left_interval.clone().intersect(right_interval)?;
    if intersection.is_strict_false() {
        return Ok((left_interval.clone(), right_interval.clone()));
    }
    let parent_interval = target_parent_interval(&left_interval.get_datatype(), op)?;
    let negate_right = right_interval.arithmetic_negate()?;
    let new_left = parent_interval
        .sub(&negate_right)?
        .intersect(left_interval)?;
    let new_right = parent_interval.sub(&new_left)?.intersect(&negate_right)?;
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
