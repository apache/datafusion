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

pub mod abs;
pub mod gcd;
pub mod lcm;
pub mod log;
pub mod nans;
pub mod pi;
pub mod power;

// Create UDFs
make_udf_function!(nans::IsNanFunc, ISNAN, isnan);
make_udf_function!(abs::AbsFunc, ABS, abs);
make_udf_function!(log::LogFunc, LOG, log);
make_udf_function!(power::PowerFunc, POWER, power);
make_udf_function!(gcd::GcdFunc, GCD, gcd);
make_udf_function!(lcm::LcmFunc, LCM, lcm);
make_udf_function!(pi::PiFunc, PI, pi);

make_math_unary_udf!(Log2Func, LOG2, log2, log2, Some(vec![Some(true)]));
make_math_unary_udf!(Log10Func, LOG10, log10, log10, Some(vec![Some(true)]));
make_math_unary_udf!(LnFunc, LN, ln, ln, Some(vec![Some(true)]));

make_math_unary_udf!(TanhFunc, TANH, tanh, tanh, None);
make_math_unary_udf!(AcosFunc, ACOS, acos, acos, None);
make_math_unary_udf!(AsinFunc, ASIN, asin, asin, None);
make_math_unary_udf!(TanFunc, TAN, tan, tan, None);

make_math_unary_udf!(AtanhFunc, ATANH, atanh, atanh, Some(vec![Some(true)]));
make_math_unary_udf!(AsinhFunc, ASINH, asinh, asinh, Some(vec![Some(true)]));
make_math_unary_udf!(AcoshFunc, ACOSH, acosh, acosh, Some(vec![Some(true)]));
make_math_unary_udf!(AtanFunc, ATAN, atan, atan, Some(vec![Some(true)]));
make_math_binary_udf!(Atan2, ATAN2, atan2, atan2, Some(vec![Some(true)]));

make_math_unary_udf!(RadiansFunc, RADIANS, radians, to_radians, None);
make_math_unary_udf!(SignumFunc, SIGNUM, signum, signum, None);
make_math_unary_udf!(SinFunc, SIN, sin, sin, None);
make_math_unary_udf!(SinhFunc, SINH, sinh, sinh, None);
make_math_unary_udf!(SqrtFunc, SQRT, sqrt, sqrt, None);

make_math_unary_udf!(CbrtFunc, CBRT, cbrt, cbrt, None);
make_math_unary_udf!(CosFunc, COS, cos, cos, None);
make_math_unary_udf!(CoshFunc, COSH, cosh, cosh, None);
make_math_unary_udf!(DegreesFunc, DEGREES, degrees, to_degrees, None);

make_math_unary_udf!(FloorFunc, FLOOR, floor, floor, Some(vec![Some(true)]));

// Export the functions out of this package, both as expr_fn as well as a list of functions
export_functions!(
    (
        isnan,
        num,
        "returns true if a given number is +NaN or -NaN otherwise returns false"
    ),
    (abs, num, "returns the absolute value of a given number"),
    (power, base exponent, "`base` raised to the power of `exponent`"),
    (log, base num, "logarithm of a number for a particular `base`"),
    (log2, num, "base 2 logarithm of a number"),
    (log10, num, "base 10 logarithm of a number"),
    (ln, num, "natural logarithm (base e) of a number"),
    (
        acos,
        num,
        "returns the arc cosine or inverse cosine of a number"
    ),
    (
        asin,
        num,
        "returns the arc sine or inverse sine of a number"
    ),
    (tan, num, "returns the tangent of a number"),
    (tanh, num, "returns the hyperbolic tangent of a number"),
    (atanh, num, "returns inverse hyperbolic tangent"),
    (asinh, num, "returns inverse hyperbolic sine"),
    (acosh, num, "returns inverse hyperbolic cosine"),
    (atan, num, "returns inverse tangent"),
    (atan2, y x, "returns inverse tangent of a division given in the argument"),
    (radians, num, "converts degrees to radians"),
    (signum, num, "sign of the argument (-1, 0, +1)"),
    (sin, num, "sine"),
    (sinh, num, "hyperbolic sine"),
    (sqrt, num, "square root of a number"),
    (cbrt, num, "cube root of a number"),
    (cos, num, "cosine"),
    (cosh, num, "hyperbolic cosine"),
    (degrees, num, "converts radians to degrees"),
    (gcd, x y, "greatest common divisor"),
    (lcm, x y, "least common multiple"),
    (floor, num, "nearest integer less than or equal to argument"),
    (pi, , "Returns an approximate value of π")
);
