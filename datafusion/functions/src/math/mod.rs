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
pub mod cot;
pub mod factorial;
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
pub mod trunc;

// Create UDFs
make_udf_function!(abs::AbsFunc, ABS, abs);
make_math_unary_udf!(AcosFunc, ACOS, acos, acos, super::acos_order);
make_math_unary_udf!(AcoshFunc, ACOSH, acosh, acosh, super::acosh_order);
make_math_unary_udf!(AsinFunc, ASIN, asin, asin, super::asin_order);
make_math_unary_udf!(AsinhFunc, ASINH, asinh, asinh, super::asinh_order);
make_math_unary_udf!(AtanFunc, ATAN, atan, atan, super::atan_order);
make_math_unary_udf!(AtanhFunc, ATANH, atanh, atanh, super::atanh_order);
make_math_binary_udf!(Atan2, ATAN2, atan2, atan2, super::atan2_order);
make_math_unary_udf!(CbrtFunc, CBRT, cbrt, cbrt, super::cbrt_order);
make_math_unary_udf!(CeilFunc, CEIL, ceil, ceil, super::ceil_order);
make_math_unary_udf!(CosFunc, COS, cos, cos, super::cos_order);
make_math_unary_udf!(CoshFunc, COSH, cosh, cosh, super::cosh_order);
make_udf_function!(cot::CotFunc, COT, cot);
make_math_unary_udf!(
    DegreesFunc,
    DEGREES,
    degrees,
    to_degrees,
    super::degrees_order
);
make_math_unary_udf!(ExpFunc, EXP, exp, exp, super::exp_order);
make_udf_function!(factorial::FactorialFunc, FACTORIAL, factorial);
make_math_unary_udf!(FloorFunc, FLOOR, floor, floor, super::floor_order);
make_udf_function!(log::LogFunc, LOG, log);
make_udf_function!(gcd::GcdFunc, GCD, gcd);
make_udf_function!(nans::IsNanFunc, ISNAN, isnan);
make_udf_function!(iszero::IsZeroFunc, ISZERO, iszero);
make_udf_function!(lcm::LcmFunc, LCM, lcm);
make_math_unary_udf!(LnFunc, LN, ln, ln, super::ln_order);
make_math_unary_udf!(Log2Func, LOG2, log2, log2, super::log2_order);
make_math_unary_udf!(Log10Func, LOG10, log10, log10, super::log10_order);
make_udf_function!(nanvl::NanvlFunc, NANVL, nanvl);
make_udf_function!(pi::PiFunc, PI, pi);
make_udf_function!(power::PowerFunc, POWER, power);
make_math_unary_udf!(
    RadiansFunc,
    RADIANS,
    radians,
    to_radians,
    super::radians_order
);
make_udf_function!(random::RandomFunc, RANDOM, random);
make_udf_function!(round::RoundFunc, ROUND, round);
make_math_unary_udf!(SignumFunc, SIGNUM, signum, signum, super::signum_order);
make_math_unary_udf!(SinFunc, SIN, sin, sin, super::sin_order);
make_math_unary_udf!(SinhFunc, SINH, sinh, sinh, super::sinh_order);
make_math_unary_udf!(SqrtFunc, SQRT, sqrt, sqrt, super::sqrt_order);
make_math_unary_udf!(TanFunc, TAN, tan, tan, super::tan_order);
make_math_unary_udf!(TanhFunc, TANH, tanh, tanh, super::tanh_order);
make_udf_function!(trunc::TruncFunc, TRUNC, trunc);

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
