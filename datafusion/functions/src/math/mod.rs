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

use datafusion_expr::ScalarUDF;
use std::sync::Arc;

pub mod abs;
pub mod cot;
pub mod gcd;
pub mod iszero;
pub mod lcm;
pub mod log;
pub mod nans;
pub mod nanvl;
pub mod pi;
pub mod power;
pub mod random;
pub mod round;
pub mod trunc;

// Create UDFs
make_udf_function!(abs::AbsFunc, ABS, abs);
make_math_unary_udf!(AcosFunc, ACOS, acos, acos, None);
make_math_unary_udf!(AcoshFunc, ACOSH, acosh, acosh, Some(vec![Some(true)]));
make_math_unary_udf!(AsinFunc, ASIN, asin, asin, None);
make_math_unary_udf!(AsinhFunc, ASINH, asinh, asinh, Some(vec![Some(true)]));
make_math_unary_udf!(AtanFunc, ATAN, atan, atan, Some(vec![Some(true)]));
make_math_unary_udf!(AtanhFunc, ATANH, atanh, atanh, Some(vec![Some(true)]));
make_math_binary_udf!(Atan2, ATAN2, atan2, atan2, Some(vec![Some(true)]));
make_math_unary_udf!(CbrtFunc, CBRT, cbrt, cbrt, None);
make_math_unary_udf!(CosFunc, COS, cos, cos, None);
make_math_unary_udf!(CoshFunc, COSH, cosh, cosh, None);
make_udf_function!(cot::CotFunc, COT, cot);
make_math_unary_udf!(DegreesFunc, DEGREES, degrees, to_degrees, None);
make_math_unary_udf!(FloorFunc, FLOOR, floor, floor, Some(vec![Some(true)]));
make_udf_function!(log::LogFunc, LOG, log);
make_udf_function!(gcd::GcdFunc, GCD, gcd);
make_udf_function!(nans::IsNanFunc, ISNAN, isnan);
make_udf_function!(iszero::IsZeroFunc, ISZERO, iszero);
make_udf_function!(lcm::LcmFunc, LCM, lcm);
make_math_unary_udf!(LnFunc, LN, ln, ln, Some(vec![Some(true)]));
make_math_unary_udf!(Log2Func, LOG2, log2, log2, Some(vec![Some(true)]));
make_math_unary_udf!(Log10Func, LOG10, log10, log10, Some(vec![Some(true)]));
make_udf_function!(nanvl::NanvlFunc, NANVL, nanvl);
make_udf_function!(pi::PiFunc, PI, pi);
make_udf_function!(power::PowerFunc, POWER, power);
make_math_unary_udf!(RadiansFunc, RADIANS, radians, to_radians, None);
make_udf_function!(random::RandomFunc, RANDOM, random);
make_udf_function!(round::RoundFunc, ROUND, round);
make_math_unary_udf!(SignumFunc, SIGNUM, signum, signum, None);
make_math_unary_udf!(SinFunc, SIN, sin, sin, None);
make_math_unary_udf!(SinhFunc, SINH, sinh, sinh, None);
make_math_unary_udf!(SqrtFunc, SQRT, sqrt, sqrt, None);
make_math_unary_udf!(TanFunc, TAN, tan, tan, None);
make_math_unary_udf!(TanhFunc, TANH, tanh, tanh, None);
make_udf_function!(trunc::TruncFunc, TRUNC, trunc);

pub mod expr_fn {
    use datafusion_expr::Expr;

    #[doc = "returns the absolute value of a given number"]
    pub fn abs(num: Expr) -> Expr {
        super::abs().call(vec![num])
    }

    #[doc = "returns the arc cosine or inverse cosine of a number"]
    pub fn acos(num: Expr) -> Expr {
        super::acos().call(vec![num])
    }

    #[doc = "returns inverse hyperbolic cosine"]
    pub fn acosh(num: Expr) -> Expr {
        super::acosh().call(vec![num])
    }

    #[doc = "returns the arc sine or inverse sine of a number"]
    pub fn asin(num: Expr) -> Expr {
        super::asin().call(vec![num])
    }

    #[doc = "returns inverse hyperbolic sine"]
    pub fn asinh(num: Expr) -> Expr {
        super::asinh().call(vec![num])
    }

    #[doc = "returns inverse tangent"]
    pub fn atan(num: Expr) -> Expr {
        super::atan().call(vec![num])
    }

    #[doc = "returns inverse tangent of a division given in the argument"]
    pub fn atan2(y: Expr, x: Expr) -> Expr {
        super::atan2().call(vec![y, x])
    }

    #[doc = "returns inverse hyperbolic tangent"]
    pub fn atanh(num: Expr) -> Expr {
        super::atanh().call(vec![num])
    }

    #[doc = "cube root of a number"]
    pub fn cbrt(num: Expr) -> Expr {
        super::cbrt().call(vec![num])
    }

    #[doc = "cosine"]
    pub fn cos(num: Expr) -> Expr {
        super::cos().call(vec![num])
    }

    #[doc = "hyperbolic cosine"]
    pub fn cosh(num: Expr) -> Expr {
        super::cosh().call(vec![num])
    }

    #[doc = "cotangent of a number"]
    pub fn cot(num: Expr) -> Expr {
        super::cot().call(vec![num])
    }

    #[doc = "converts radians to degrees"]
    pub fn degrees(num: Expr) -> Expr {
        super::degrees().call(vec![num])
    }

    #[doc = "nearest integer less than or equal to argument"]
    pub fn floor(num: Expr) -> Expr {
        super::floor().call(vec![num])
    }

    #[doc = "greatest common divisor"]
    pub fn gcd(x: Expr, y: Expr) -> Expr {
        super::gcd().call(vec![x, y])
    }

    #[doc = "returns true if a given number is +NaN or -NaN otherwise returns false"]
    pub fn isnan(num: Expr) -> Expr {
        super::isnan().call(vec![num])
    }

    #[doc = "returns true if a given number is +0.0 or -0.0 otherwise returns false"]
    pub fn iszero(num: Expr) -> Expr {
        super::iszero().call(vec![num])
    }

    #[doc = "least common multiple"]
    pub fn lcm(x: Expr, y: Expr) -> Expr {
        super::lcm().call(vec![x, y])
    }

    #[doc = "natural logarithm (base e) of a number"]
    pub fn ln(num: Expr) -> Expr {
        super::ln().call(vec![num])
    }

    #[doc = "logarithm of a number for a particular `base`"]
    pub fn log(base: Expr, num: Expr) -> Expr {
        super::log().call(vec![base, num])
    }

    #[doc = "base 2 logarithm of a number"]
    pub fn log2(num: Expr) -> Expr {
        super::log2().call(vec![num])
    }

    #[doc = "base 10 logarithm of a number"]
    pub fn log10(num: Expr) -> Expr {
        super::log10().call(vec![num])
    }

    #[doc = "returns x if x is not NaN otherwise returns y"]
    pub fn nanvl(x: Expr, y: Expr) -> Expr {
        super::nanvl().call(vec![x, y])
    }

    #[doc = "Returns an approximate value of π"]
    pub fn pi() -> Expr {
        super::pi().call(vec![])
    }

    #[doc = "`base` raised to the power of `exponent`"]
    pub fn power(base: Expr, exponent: Expr) -> Expr {
        super::power().call(vec![base, exponent])
    }

    #[doc = "converts degrees to radians"]
    pub fn radians(num: Expr) -> Expr {
        super::radians().call(vec![num])
    }

    #[doc = "Returns a random value in the range 0.0 <= x < 1.0"]
    pub fn random() -> Expr {
        super::random().call(vec![])
    }

    #[doc = "round to nearest integer"]
    pub fn round(args: Vec<Expr>) -> Expr {
        super::round().call(args)
    }

    #[doc = "sign of the argument (-1, 0, +1)"]
    pub fn signum(num: Expr) -> Expr {
        super::signum().call(vec![num])
    }

    #[doc = "sine"]
    pub fn sin(num: Expr) -> Expr {
        super::sin().call(vec![num])
    }

    #[doc = "hyperbolic sine"]
    pub fn sinh(num: Expr) -> Expr {
        super::sinh().call(vec![num])
    }

    #[doc = "square root of a number"]
    pub fn sqrt(num: Expr) -> Expr {
        super::sqrt().call(vec![num])
    }

    #[doc = "returns the tangent of a number"]
    pub fn tan(num: Expr) -> Expr {
        super::tan().call(vec![num])
    }

    #[doc = "returns the hyperbolic tangent of a number"]
    pub fn tanh(num: Expr) -> Expr {
        super::tanh().call(vec![num])
    }

    #[doc = "truncate toward zero, with optional precision"]
    pub fn trunc(args: Vec<Expr>) -> Expr {
        super::trunc().call(args)
    }
}

///   Return a list of all functions in this package
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
        cos(),
        cosh(),
        cot(),
        degrees(),
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
        round(),
        signum(),
        sin(),
        sinh(),
        sqrt(),
        tan(),
        tanh(),
        trunc(),
    ]
}
