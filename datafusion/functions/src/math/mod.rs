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

//! "math" DataFusion functions

use crate::math::monotonicity::*;
use datafusion_expr::ScalarUDF;
use std::sync::Arc;

pub mod abs;
pub mod bounds;
pub mod ceil;
pub mod cot;
mod decimal;
pub mod factorial;
pub mod floor;
pub mod gcd;
pub mod iszero;
pub mod lcm;
pub mod log;
pub mod monotonicity;
pub mod nans;
pub mod nanvl;
pub mod pi;
pub mod power;
pub mod random;
pub mod round;
pub mod signum;
pub mod trunc;

// Create UDFs
make_udf_function!(abs::AbsFunc, abs);
make_math_unary_udf!(
    AcosFunc,
    acos,
    acos,
    super::acos_order,
    super::bounds::acos_bounds,
    super::get_acos_doc
);
make_math_unary_udf!(
    AcoshFunc,
    acosh,
    acosh,
    super::acosh_order,
    super::bounds::acosh_bounds,
    super::get_acosh_doc
);
make_math_unary_udf!(
    AsinFunc,
    asin,
    asin,
    super::asin_order,
    super::bounds::asin_bounds,
    super::get_asin_doc
);
make_math_unary_udf!(
    AsinhFunc,
    asinh,
    asinh,
    super::asinh_order,
    super::bounds::unbounded_bounds,
    super::get_asinh_doc
);
make_math_unary_udf!(
    AtanFunc,
    atan,
    atan,
    super::atan_order,
    super::bounds::atan_bounds,
    super::get_atan_doc
);
make_math_unary_udf!(
    AtanhFunc,
    atanh,
    atanh,
    super::atanh_order,
    super::bounds::unbounded_bounds,
    super::get_atanh_doc
);
make_math_binary_udf!(
    Atan2,
    atan2,
    atan2,
    super::atan2_order,
    super::get_atan2_doc
);
make_math_unary_udf!(
    CbrtFunc,
    cbrt,
    cbrt,
    super::cbrt_order,
    super::bounds::unbounded_bounds,
    super::get_cbrt_doc
);
make_udf_function!(ceil::CeilFunc, ceil);
make_math_unary_udf!(
    CosFunc,
    cos,
    cos,
    super::cos_order,
    super::bounds::cos_bounds,
    super::get_cos_doc
);
make_math_unary_udf!(
    CoshFunc,
    cosh,
    cosh,
    super::cosh_order,
    super::bounds::cosh_bounds,
    super::get_cosh_doc
);
make_udf_function!(cot::CotFunc, cot);
make_math_unary_udf!(
    DegreesFunc,
    degrees,
    to_degrees,
    super::degrees_order,
    super::bounds::unbounded_bounds,
    super::get_degrees_doc
);
make_math_unary_udf!(
    ExpFunc,
    exp,
    exp,
    super::exp_order,
    super::bounds::exp_bounds,
    super::get_exp_doc
);
make_udf_function!(factorial::FactorialFunc, factorial);
make_udf_function!(floor::FloorFunc, floor);
make_udf_function!(log::LogFunc, log);
make_udf_function!(gcd::GcdFunc, gcd);
make_udf_function!(nans::IsNanFunc, isnan);
make_udf_function!(iszero::IsZeroFunc, iszero);
make_udf_function!(lcm::LcmFunc, lcm);
make_math_unary_udf!(
    LnFunc,
    ln,
    ln,
    super::ln_order,
    super::bounds::unbounded_bounds,
    super::get_ln_doc
);
make_math_unary_udf!(
    Log2Func,
    log2,
    log2,
    super::log2_order,
    super::bounds::unbounded_bounds,
    super::get_log2_doc
);
make_math_unary_udf!(
    Log10Func,
    log10,
    log10,
    super::log10_order,
    super::bounds::unbounded_bounds,
    super::get_log10_doc
);
make_udf_function!(nanvl::NanvlFunc, nanvl);
make_udf_function!(pi::PiFunc, pi);
make_udf_function!(power::PowerFunc, power);
make_math_unary_udf!(
    RadiansFunc,
    radians,
    to_radians,
    super::radians_order,
    super::bounds::radians_bounds,
    super::get_radians_doc
);
make_udf_function!(random::RandomFunc, random);
make_udf_function!(round::RoundFunc, round);
make_udf_function!(signum::SignumFunc, signum);
make_math_unary_udf!(
    SinFunc,
    sin,
    sin,
    super::sin_order,
    super::bounds::sin_bounds,
    super::get_sin_doc
);
make_math_unary_udf!(
    SinhFunc,
    sinh,
    sinh,
    super::sinh_order,
    super::bounds::unbounded_bounds,
    super::get_sinh_doc
);
make_math_unary_udf!(
    SqrtFunc,
    sqrt,
    sqrt,
    super::sqrt_order,
    super::bounds::sqrt_bounds,
    super::get_sqrt_doc
);
make_math_unary_udf!(
    TanFunc,
    tan,
    tan,
    super::tan_order,
    super::bounds::unbounded_bounds,
    super::get_tan_doc
);
make_math_unary_udf!(
    TanhFunc,
    tanh,
    tanh,
    super::tanh_order,
    super::bounds::tanh_bounds,
    super::get_tanh_doc
);
make_udf_function!(trunc::TruncFunc, trunc);

pub mod expr_fn {
    export_functions!(
        (abs, "returns the absolute value of a given number", num),
        (acos, "returns the arc cosine or inverse cosine of a number", num),
        (acosh, "returns inverse hyperbolic cosine", num),
        (asin, "returns the arc sine or inverse sine of a number", num),
        (asinh, "returns inverse hyperbolic sine", num),
        (atan, "returns inverse tangent", num),
        (atan2, "returns inverse tangent of a division given in the argument", y x),
        (atanh, "returns inverse hyperbolic tangent", num),
        (cbrt, "cube root of a number", num),
        (ceil, "nearest integer greater than or equal to argument", num),
        (cos, "cosine", num),
        (cosh, "hyperbolic cosine", num),
        (cot, "cotangent of a number", num),
        (degrees, "converts radians to degrees", num),
        (exp, "exponential", num),
        (factorial, "factorial", num),
        (floor, "nearest integer less than or equal to argument", num),
        (gcd, "greatest common divisor", x y),
        (isnan, "returns true if a given number is +NaN or -NaN otherwise returns false", num),
        (iszero, "returns true if a given number is +0.0 or -0.0 otherwise returns false", num),
        (lcm, "least common multiple", x y),
        (ln, "natural logarithm (base e) of a number", num),
        (log, "logarithm of a number for a particular `base`", base num),
        (log2, "base 2 logarithm of a number", num),
        (log10, "base 10 logarithm of a number", num),
        (nanvl, "returns x if x is not NaN otherwise returns y", x y),
        (pi, "Returns an approximate value of π",),
        (power, "`base` raised to the power of `exponent`", base exponent),
        (radians, "converts degrees to radians", num),
        (random, "Returns a random value in the range 0.0 <= x < 1.0",),
        (signum, "sign of the argument (-1, 0, +1)", num),
        (sin, "sine", num),
        (sinh, "hyperbolic sine", num),
        (sqrt, "square root of a number", num),
        (tan, "returns the tangent of a number", num),
        (tanh, "returns the hyperbolic tangent of a number", num),
        (round, "round to nearest integer", args,),
        (trunc, "truncate toward zero, with optional precision", args,)
    );
}

/// Returns all DataFusion functions defined in this package
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        abs(),
        acos(),
        acosh(),
        asin(),
        asinh(),
        atan(),
        atan2(),
        atanh(),
        cbrt(),
        ceil(),
        cos(),
        cosh(),
        cot(),
        degrees(),
        exp(),
        factorial(),
        floor(),
        gcd(),
        isnan(),
        iszero(),
        lcm(),
        ln(),
        log(),
        log2(),
        log10(),
        nanvl(),
        pi(),
        power(),
        radians(),
        random(),
        signum(),
        sin(),
        sinh(),
        sqrt(),
        tan(),
        tanh(),
        round(),
        trunc(),
    ]
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use datafusion_common::ScalarValue;
    use datafusion_expr::interval_arithmetic::Interval;

    fn unbounded_interval(data_type: &DataType) -> Interval {
        Interval::make_unbounded(data_type).unwrap()
    }

    fn one_to_inf_interval(data_type: &DataType) -> Interval {
        Interval::try_new(
            ScalarValue::new_one(data_type).unwrap(),
            ScalarValue::try_from(data_type).unwrap(),
        )
        .unwrap()
    }

    fn zero_to_pi_interval(data_type: &DataType) -> Interval {
        Interval::try_new(
            ScalarValue::new_zero(data_type).unwrap(),
            ScalarValue::new_pi_upper(data_type).unwrap(),
        )
        .unwrap()
    }

    fn assert_udf_evaluates_to_bounds(
        udf: &datafusion_expr::ScalarUDF,
        interval: Interval,
        expected: Interval,
    ) {
        let input = vec![&interval];
        let result = udf.evaluate_bounds(&input).unwrap();
        assert_eq!(
            result,
            expected,
            "Bounds check failed on UDF: {:?}",
            udf.name()
        );
    }

    #[test]
    fn test_cases() -> crate::Result<()> {
        let datatypes = [DataType::Float32, DataType::Float64];
        let cases = datatypes
            .iter()
            .flat_map(|data_type| {
                vec![
                    (
                        super::acos(),
                        unbounded_interval(data_type),
                        zero_to_pi_interval(data_type),
                    ),
                    (
                        super::acosh(),
                        unbounded_interval(data_type),
                        Interval::make_non_negative_infinity_interval(data_type).unwrap(),
                    ),
                    (
                        super::asin(),
                        unbounded_interval(data_type),
                        Interval::make_symmetric_half_pi_interval(data_type).unwrap(),
                    ),
                    (
                        super::atan(),
                        unbounded_interval(data_type),
                        Interval::make_symmetric_half_pi_interval(data_type).unwrap(),
                    ),
                    (
                        super::cos(),
                        unbounded_interval(data_type),
                        Interval::make_symmetric_unit_interval(data_type).unwrap(),
                    ),
                    (
                        super::cosh(),
                        unbounded_interval(data_type),
                        one_to_inf_interval(data_type),
                    ),
                    (
                        super::sin(),
                        unbounded_interval(data_type),
                        Interval::make_symmetric_unit_interval(data_type).unwrap(),
                    ),
                    (
                        super::exp(),
                        unbounded_interval(data_type),
                        Interval::make_non_negative_infinity_interval(data_type).unwrap(),
                    ),
                    (
                        super::sqrt(),
                        unbounded_interval(data_type),
                        Interval::make_non_negative_infinity_interval(data_type).unwrap(),
                    ),
                    (
                        super::radians(),
                        unbounded_interval(data_type),
                        Interval::make_symmetric_pi_interval(data_type).unwrap(),
                    ),
                    (
                        super::sqrt(),
                        unbounded_interval(data_type),
                        Interval::make_non_negative_infinity_interval(data_type).unwrap(),
                    ),
                ]
            })
            .collect::<Vec<_>>();

        for (udf, interval, expected) in cases {
            assert_udf_evaluates_to_bounds(&udf, interval, expected);
        }

        Ok(())
    }
}
