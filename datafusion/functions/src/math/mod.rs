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

mod abs;
mod nans;

// Create UDFs
make_udf_function!(nans::IsNanFunc, ISNAN, isnan);
make_udf_function!(abs::AbsFunc, ABS, abs);

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

// Export the functions out of this package, both as expr_fn as well as a list of functions
export_functions!(
    (
        isnan,
        num,
        "returns true if a given number is +NaN or -NaN otherwise returns false"
    ),
    (abs, num, "returns the absolute value of a given number"),
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
    (atan2, y x, "returns inverse tangent of a division given in the argument")
);
