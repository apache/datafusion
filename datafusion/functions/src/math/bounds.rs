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

use datafusion_common::ScalarValue;
use datafusion_expr::interval_arithmetic::Interval;

pub(super) fn unbounded_bounds(input: &[&Interval]) -> crate::Result<Interval> {
    let data_type = input[0].data_type();

    Interval::make_unbounded(&data_type)
}

pub(super) fn sin_bounds(input: &[&Interval]) -> crate::Result<Interval> {
    // sin(x) is bounded by [-1, 1]
    let data_type = input[0].data_type();

    Interval::make_symmetric_unit_interval(&data_type)
}

pub(super) fn asin_bounds(input: &[&Interval]) -> crate::Result<Interval> {
    // asin(x) is bounded by [-π/2, π/2]
    let data_type = input[0].data_type();

    Interval::make_symmetric_half_pi_interval(&data_type)
}

pub(super) fn atan_bounds(input: &[&Interval]) -> crate::Result<Interval> {
    // atan(x) is bounded by [-π/2, π/2]
    let data_type = input[0].data_type();

    Interval::make_symmetric_half_pi_interval(&data_type)
}

pub(super) fn acos_bounds(input: &[&Interval]) -> crate::Result<Interval> {
    // acos(x) is bounded by [0, π]
    let data_type = input[0].data_type();

    Interval::try_new(
        ScalarValue::new_zero(&data_type)?,
        ScalarValue::new_pi_upper(&data_type)?,
    )
}

pub(super) fn acosh_bounds(input: &[&Interval]) -> crate::Result<Interval> {
    // acosh(x) is bounded by [0, ∞)
    let data_type = input[0].data_type();

    Interval::make_non_negative_infinity_interval(&data_type)
}

pub(super) fn cos_bounds(input: &[&Interval]) -> crate::Result<Interval> {
    // cos(x) is bounded by [-1, 1]
    let data_type = input[0].data_type();

    Interval::make_symmetric_unit_interval(&data_type)
}

pub(super) fn cosh_bounds(input: &[&Interval]) -> crate::Result<Interval> {
    // cosh(x) is bounded by [1, ∞)
    let data_type = input[0].data_type();

    Interval::try_new(
        ScalarValue::new_one(&data_type)?,
        ScalarValue::try_from(&data_type)?,
    )
}

pub(super) fn exp_bounds(input: &[&Interval]) -> crate::Result<Interval> {
    // exp(x) is bounded by [0, ∞)
    let data_type = input[0].data_type();

    Interval::make_non_negative_infinity_interval(&data_type)
}

pub(super) fn radians_bounds(input: &[&Interval]) -> crate::Result<Interval> {
    // radians(x) is bounded by (-π, π)
    let data_type = input[0].data_type();

    Interval::make_symmetric_pi_interval(&data_type)
}

pub(super) fn sqrt_bounds(input: &[&Interval]) -> crate::Result<Interval> {
    // sqrt(x) is bounded by [0, ∞)
    let data_type = input[0].data_type();

    Interval::make_non_negative_infinity_interval(&data_type)
}

pub(super) fn tanh_bounds(input: &[&Interval]) -> crate::Result<Interval> {
    // tanh(x) is bounded by (-1, 1)
    let data_type = input[0].data_type();

    Interval::make_symmetric_unit_interval(&data_type)
}
