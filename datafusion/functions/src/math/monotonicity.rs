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

use arrow::datatypes::DataType;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};

fn symmetric_unit_interval(data_type: &DataType) -> Result<Interval> {
    Interval::try_new(
        ScalarValue::new_negative_one(data_type)?,
        ScalarValue::new_one(data_type)?,
    )
}

/// Non-increasing on the interval \[−1, 1\], undefined otherwise.
pub fn acos_order(input: &[ExprProperties]) -> Result<SortProperties> {
    let arg = &input[0];
    let range = &arg.range;

    let valid_domain = symmetric_unit_interval(&range.lower().data_type())?;

    if valid_domain.contains(range)? == Interval::CERTAINLY_TRUE {
        Ok(-arg.sort_properties)
    } else {
        exec_err!("Input range of ACOS contains out-of-domain values")
    }
}

/// Non-decreasing for x ≥ 1, undefined otherwise.
pub fn acosh_order(input: &[ExprProperties]) -> Result<SortProperties> {
    let arg = &input[0];
    let range = &arg.range;

    let valid_domain = Interval::try_new(
        ScalarValue::new_one(&range.lower().data_type())?,
        ScalarValue::try_from(&range.upper().data_type())?,
    )?;

    if valid_domain.contains(range)? == Interval::CERTAINLY_TRUE {
        Ok(arg.sort_properties)
    } else {
        exec_err!("Input range of ACOSH contains out-of-domain values")
    }
}

/// Non-decreasing on the interval \[−1, 1\], undefined otherwise.
pub fn asin_order(input: &[ExprProperties]) -> Result<SortProperties> {
    let arg = &input[0];
    let range = &arg.range;

    let valid_domain = symmetric_unit_interval(&range.lower().data_type())?;

    if valid_domain.contains(range)? == Interval::CERTAINLY_TRUE {
        Ok(arg.sort_properties)
    } else {
        exec_err!("Input range of ASIN contains out-of-domain values")
    }
}

/// Non-decreasing for all real numbers.
pub fn asinh_order(input: &[ExprProperties]) -> Result<SortProperties> {
    Ok(input[0].sort_properties)
}

/// Non-decreasing for all real numbers.
pub fn atan_order(input: &[ExprProperties]) -> Result<SortProperties> {
    Ok(input[0].sort_properties)
}

/// Non-decreasing on the interval \[−1, 1\], undefined otherwise.
pub fn atanh_order(input: &[ExprProperties]) -> Result<SortProperties> {
    let arg = &input[0];
    let range = &arg.range;

    let valid_domain = symmetric_unit_interval(&range.lower().data_type())?;

    if valid_domain.contains(range)? == Interval::CERTAINLY_TRUE {
        Ok(arg.sort_properties)
    } else {
        exec_err!("Input range of ATANH contains out-of-domain values")
    }
}

/// Order depends on the quadrant.
// TODO: Implement ordering rule of the ATAN2 function.
pub fn atan2_order(_input: &[ExprProperties]) -> Result<SortProperties> {
    Ok(SortProperties::Unordered)
}

/// Non-decreasing for all real numbers.
pub fn cbrt_order(input: &[ExprProperties]) -> Result<SortProperties> {
    Ok(input[0].sort_properties)
}

/// Non-decreasing for all real numbers.
pub fn ceil_order(input: &[ExprProperties]) -> Result<SortProperties> {
    Ok(input[0].sort_properties)
}

/// Non-increasing on \[0, π\] and then non-decreasing on \[π, 2π\].
/// This pattern repeats periodically with a period of 2π.
// TODO: Implement ordering rule of the ATAN2 function.
pub fn cos_order(_input: &[ExprProperties]) -> Result<SortProperties> {
    Ok(SortProperties::Unordered)
}

/// Non-decreasing for x ≥ 0 and symmetrically non-increasing for x ≤ 0.
pub fn cosh_order(input: &[ExprProperties]) -> Result<SortProperties> {
    let arg = &input[0];
    let range = &arg.range;

    let zero_point = Interval::make_zero(&range.lower().data_type())?;

    if range.gt_eq(&zero_point)? == Interval::CERTAINLY_TRUE {
        Ok(arg.sort_properties)
    } else if range.lt_eq(&zero_point)? == Interval::CERTAINLY_TRUE {
        Ok(-arg.sort_properties)
    } else {
        Ok(SortProperties::Unordered)
    }
}

/// Non-decreasing function that converts radians to degrees.
pub fn degrees_order(input: &[ExprProperties]) -> Result<SortProperties> {
    Ok(input[0].sort_properties)
}

/// Non-decreasing for all real numbers.
pub fn exp_order(input: &[ExprProperties]) -> Result<SortProperties> {
    Ok(input[0].sort_properties)
}

/// Non-decreasing for all real numbers.
pub fn floor_order(input: &[ExprProperties]) -> Result<SortProperties> {
    Ok(input[0].sort_properties)
}

/// Non-decreasing for x ≥ 0, undefined otherwise.
pub fn ln_order(input: &[ExprProperties]) -> Result<SortProperties> {
    let arg = &input[0];
    let range = &arg.range;

    let zero_point = Interval::make_zero(&range.lower().data_type())?;

    if range.gt_eq(&zero_point)? == Interval::CERTAINLY_TRUE {
        Ok(arg.sort_properties)
    } else {
        exec_err!("Input range of LN contains out-of-domain values")
    }
}

/// Non-decreasing for x ≥ 0, undefined otherwise.
pub fn log2_order(input: &[ExprProperties]) -> Result<SortProperties> {
    let arg = &input[0];
    let range = &arg.range;

    let zero_point = Interval::make_zero(&range.lower().data_type())?;

    if range.gt_eq(&zero_point)? == Interval::CERTAINLY_TRUE {
        Ok(arg.sort_properties)
    } else {
        exec_err!("Input range of LOG2 contains out-of-domain values")
    }
}

/// Non-decreasing for x ≥ 0, undefined otherwise.
pub fn log10_order(input: &[ExprProperties]) -> Result<SortProperties> {
    let arg = &input[0];
    let range = &arg.range;

    let zero_point = Interval::make_zero(&range.lower().data_type())?;

    if range.gt_eq(&zero_point)? == Interval::CERTAINLY_TRUE {
        Ok(arg.sort_properties)
    } else {
        exec_err!("Input range of LOG10 contains out-of-domain values")
    }
}

/// Non-decreasing for all real numbers x.
pub fn radians_order(input: &[ExprProperties]) -> Result<SortProperties> {
    Ok(input[0].sort_properties)
}

/// Non-decreasing for all real numbers x.
pub fn signum_order(input: &[ExprProperties]) -> Result<SortProperties> {
    Ok(input[0].sort_properties)
}

/// Non-decreasing on \[0, π\] and then non-increasing on \[π, 2π\].
/// This pattern repeats periodically with a period of 2π.
// TODO: Implement ordering rule of the SIN function.
pub fn sin_order(_input: &[ExprProperties]) -> Result<SortProperties> {
    Ok(SortProperties::Unordered)
}

/// Non-decreasing for all real numbers.
pub fn sinh_order(input: &[ExprProperties]) -> Result<SortProperties> {
    Ok(input[0].sort_properties)
}

/// Non-decreasing for x ≥ 0, undefined otherwise.
pub fn sqrt_order(input: &[ExprProperties]) -> Result<SortProperties> {
    let arg = &input[0];
    let range = &arg.range;

    let zero_point = Interval::make_zero(&range.lower().data_type())?;

    if range.gt_eq(&zero_point)? == Interval::CERTAINLY_TRUE {
        Ok(arg.sort_properties)
    } else {
        exec_err!("Input range of SQRT contains out-of-domain values")
    }
}

/// Non-decreasing between vertical asymptotes at x = k * π ± π / 2 for any
/// integer k.
// TODO: Implement ordering rule of the TAN function.
pub fn tan_order(_input: &[ExprProperties]) -> Result<SortProperties> {
    Ok(SortProperties::Unordered)
}

/// Non-decreasing for all real numbers.
pub fn tanh_order(input: &[ExprProperties]) -> Result<SortProperties> {
    Ok(input[0].sort_properties)
}
