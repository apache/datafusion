// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{
    interval_arithmetic::Interval,
    sort_properties::{ExprProperties, SortProperties},
};

/// Non-increasing on the interval [−1,1]. Undefined outside this interval.
pub fn acos_monotonicity(input: &[ExprProperties]) -> Result<SortProperties> {
    let x = &input[0];

    let valid_domain = Interval::try_new(
        ScalarValue::new_negative_one(&x.range.lower().data_type())?,
        ScalarValue::new_one(&x.range.upper().data_type())?,
    )?;

    if valid_domain.contains(&x.range)? == Interval::CERTAINLY_TRUE {
        Ok(match x.sort_properties {
            SortProperties::Ordered(opt) => -SortProperties::Ordered(opt),
            other => other,
        })
    } else {
        exec_err!("A value from undefined range is the input of ACOS()")
    }
}

/// Non-decreasing for x≥1. Undefined for x<1.
pub fn acosh_monotonicity(input: &[ExprProperties]) -> Result<SortProperties> {
    let x = &input[0];

    let valid_domain = Interval::try_new(
        ScalarValue::new_one(&x.range.lower().data_type())?,
        ScalarValue::new_null(&x.range.upper().data_type())?,
    )?;

    if valid_domain.contains(&x.range)? == Interval::CERTAINLY_TRUE {
        Ok(x.sort_properties)
    } else {
        exec_err!("A value from undefined range is the input of ACOSH()")
    }
}

/// Non-decreasing on the interval [−1,1]. Undefined outside this interval.
pub fn asin_monotonicity(input: &[ExprProperties]) -> Result<SortProperties> {
    let x = &input[0];

    let valid_domain = Interval::try_new(
        ScalarValue::new_negative_one(&x.range.lower().data_type())?,
        ScalarValue::new_one(&x.range.upper().data_type())?,
    )?;

    if valid_domain.contains(&x.range)? == Interval::CERTAINLY_TRUE {
        Ok(x.sort_properties)
    } else {
        exec_err!("A value from undefined range is the input of ASIN()")
    }
}

/// Non-decreasing for all real numbers x.
pub fn asinh_monotonicity(input: &[ExprProperties]) -> Result<SortProperties> {
    let x = &input[0];

    Ok(x.sort_properties)
}

/// Non-decreasing for all real numbers x.
pub fn atan_monotonicity(input: &[ExprProperties]) -> Result<SortProperties> {
    let x = &input[0];

    Ok(x.sort_properties)
}

/// Non-decreasing on the interval [−1,1]. Undefined outside this interval.
pub fn atanh_monotonicity(input: &[ExprProperties]) -> Result<SortProperties> {
    let x = &input[0];

    let valid_domain = Interval::try_new(
        ScalarValue::new_negative_one(&x.range.lower().data_type())?,
        ScalarValue::new_one(&x.range.upper().data_type())?,
    )?;

    if valid_domain.contains(&x.range)? == Interval::CERTAINLY_TRUE {
        Ok(x.sort_properties)
    } else {
        exec_err!("A value from undefined range is the input of ATANH()")
    }
}

/// Non-decreasing for all real numbers x.
pub fn atan2_monotonicity(_input: &[ExprProperties]) -> Result<SortProperties> {
    // TODO
    Ok(SortProperties::Unordered)
}

/// Non-decreasing for all real numbers x.
pub fn cbrt_monotonicity(input: &[ExprProperties]) -> Result<SortProperties> {
    let x = &input[0];

    Ok(x.sort_properties)
}

/// Non-decreasing for all real numbers x.
pub fn ceil_monotonicity(input: &[ExprProperties]) -> Result<SortProperties> {
    let x = &input[0];

    Ok(x.sort_properties)
}

/// Non-increasing on \[0,π\] and then non-decreasing on \[π,2π\].
/// This pattern repeats periodically with a period of 2π.
pub fn cos_monotonicity(_input: &[ExprProperties]) -> Result<SortProperties> {
    // TODO
    Ok(SortProperties::Unordered)
}

/// Non-decreasing for x≥0 and symmetrically non-increasing for x≤0.
pub fn cosh_monotonicity(input: &[ExprProperties]) -> Result<SortProperties> {
    let x = &input[0];

    let zero_point = Interval::try_new(
        ScalarValue::new_zero(&x.range.lower().data_type())?,
        ScalarValue::new_zero(&x.range.upper().data_type())?,
    )?;

    if x.range.gt_eq(&zero_point)? == Interval::CERTAINLY_TRUE {
        Ok(match x.sort_properties {
            SortProperties::Ordered(opt) => SortProperties::Ordered(opt),
            other => other,
        })
    } else if x.range.lt_eq(&zero_point)? == Interval::CERTAINLY_TRUE {
        Ok(match x.sort_properties {
            SortProperties::Ordered(opt) => -SortProperties::Ordered(opt),
            other => other,
        })
    } else {
        Ok(SortProperties::Unordered)
    }
}

/// Non-decreasing function that converts radians to degrees.
pub fn degrees_monotonicity(input: &[ExprProperties]) -> Result<SortProperties> {
    let x = &input[0];

    Ok(x.sort_properties)
}

/// Non-decreasing for all real numbers x.
pub fn exp_monotonicity(input: &[ExprProperties]) -> Result<SortProperties> {
    let x = &input[0];

    Ok(x.sort_properties)
}

/// Non-decreasing for all real numbers x.
pub fn floor_monotonicity(input: &[ExprProperties]) -> Result<SortProperties> {
    let x = &input[0];

    Ok(x.sort_properties)
}

/// Non-decreasing for x>0. Undefined for x≤0.
pub fn ln_monotonicity(input: &[ExprProperties]) -> Result<SortProperties> {
    let x = &input[0];

    let zero_point = Interval::try_new(
        ScalarValue::new_zero(&x.range.lower().data_type())?,
        ScalarValue::new_zero(&x.range.upper().data_type())?,
    )?;

    if x.range.gt_eq(&zero_point)? == Interval::CERTAINLY_TRUE {
        Ok(match x.sort_properties {
            SortProperties::Ordered(opt) => SortProperties::Ordered(opt),
            other => other,
        })
    } else {
        exec_err!("A value from undefined range is the input of LN()")
    }
}

/// Non-decreasing for x>0. Undefined for x≤0.
pub fn log2_monotonicity(input: &[ExprProperties]) -> Result<SortProperties> {
    let x = &input[0];

    let zero_point = Interval::try_new(
        ScalarValue::new_zero(&x.range.lower().data_type())?,
        ScalarValue::new_zero(&x.range.upper().data_type())?,
    )?;

    if x.range.gt_eq(&zero_point)? == Interval::CERTAINLY_TRUE {
        Ok(match x.sort_properties {
            SortProperties::Ordered(opt) => SortProperties::Ordered(opt),
            other => other,
        })
    } else {
        exec_err!("A value from undefined range is the input of LOG2()")
    }
}

/// Non-decreasing for x>0. Undefined for x≤0.
pub fn log10_monotonicity(input: &[ExprProperties]) -> Result<SortProperties> {
    let x = &input[0];

    let zero_point = Interval::try_new(
        ScalarValue::new_zero(&x.range.lower().data_type())?,
        ScalarValue::new_zero(&x.range.upper().data_type())?,
    )?;

    if x.range.gt(&zero_point)? == Interval::CERTAINLY_TRUE {
        Ok(match x.sort_properties {
            SortProperties::Ordered(opt) => SortProperties::Ordered(opt),
            other => other,
        })
    } else {
        exec_err!("A value from undefined range is the input of LOG10()")
    }
}

/// Non-decreasing for all real numbers x.
pub fn radians_monotonicity(input: &[ExprProperties]) -> Result<SortProperties> {
    let x = &input[0];

    Ok(x.sort_properties)
}

/// Non-decreasing for all real numbers x.
pub fn signum_monotonicity(input: &[ExprProperties]) -> Result<SortProperties> {
    let x = &input[0];

    Ok(x.sort_properties)
}

/// Non-decreasing on \[0,π\] and then non-increasing on \[π,2π\].
/// This pattern repeats periodically with a period of 2π.
pub fn sin_monotonicity(_input: &[ExprProperties]) -> Result<SortProperties> {
    // TODO
    Ok(SortProperties::Unordered)
}

/// Non-decreasing for all real numbers x.
pub fn sinh_monotonicity(input: &[ExprProperties]) -> Result<SortProperties> {
    let x = &input[0];

    Ok(x.sort_properties)
}

/// Non-decreasing for x≥0. Undefined for x<0.
pub fn sqrt_monotonicity(input: &[ExprProperties]) -> Result<SortProperties> {
    let x = &input[0];

    let zero_point = Interval::try_new(
        ScalarValue::new_zero(&x.range.lower().data_type())?,
        ScalarValue::new_zero(&x.range.upper().data_type())?,
    )?;

    if x.range.gt_eq(&zero_point)? == Interval::CERTAINLY_TRUE {
        Ok(match x.sort_properties {
            SortProperties::Ordered(opt) => SortProperties::Ordered(opt),
            other => other,
        })
    } else {
        exec_err!("A value from undefined range is the input of SQRT()")
    }
}

/// Non-decreasing between its vertical asymptotes at x=π2+kπ for any integer k, where it is undefined.
///
pub fn tan_monotonicity(_input: &[ExprProperties]) -> Result<SortProperties> {
    // TODO
    Ok(SortProperties::Unordered)
}

/// Non-decreasing for all real numbers x.
pub fn tanh_monotonicity(input: &[ExprProperties]) -> Result<SortProperties> {
    let x = &input[0];

    Ok(x.sort_properties)
}
