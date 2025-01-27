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

use std::sync::OnceLock;

use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::scalar_doc_sections::DOC_SECTION_MATH;
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::Documentation;

/// Non-increasing on the interval \[−1, 1\], undefined otherwise.
pub fn acos_order(input: &[ExprProperties]) -> Result<SortProperties> {
    let arg = &input[0];
    let range = &arg.range;

    let valid_domain =
        Interval::make_symmetric_unit_interval(&range.lower().data_type())?;

    if valid_domain.contains(range)? == Interval::CERTAINLY_TRUE {
        Ok(-arg.sort_properties)
    } else {
        exec_err!("Input range of ACOS contains out-of-domain values")
    }
}

static DOCUMENTATION_ACOS: OnceLock<Documentation> = OnceLock::new();

pub fn get_acos_doc() -> &'static Documentation {
    DOCUMENTATION_ACOS.get_or_init(|| {
        Documentation::builder(
            DOC_SECTION_MATH,
            "Returns the arc cosine or inverse cosine of a number.",
            "acos(numeric_expression)",
        )
        .with_standard_argument("numeric_expression", Some("Numeric"))
        .build()
    })
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

static DOCUMENTATION_ACOSH: OnceLock<Documentation> = OnceLock::new();

pub fn get_acosh_doc() -> &'static Documentation {
    DOCUMENTATION_ACOSH.get_or_init(|| {
        Documentation::builder(
            DOC_SECTION_MATH,
                "Returns the area hyperbolic cosine or inverse hyperbolic cosine of a number.",

            "acosh(numeric_expression)")
            .with_standard_argument("numeric_expression", Some("Numeric"))
            .build()
    })
}

/// Non-decreasing on the interval \[−1, 1\], undefined otherwise.
pub fn asin_order(input: &[ExprProperties]) -> Result<SortProperties> {
    let arg = &input[0];
    let range = &arg.range;

    let valid_domain =
        Interval::make_symmetric_unit_interval(&range.lower().data_type())?;

    if valid_domain.contains(range)? == Interval::CERTAINLY_TRUE {
        Ok(arg.sort_properties)
    } else {
        exec_err!("Input range of ASIN contains out-of-domain values")
    }
}

static DOCUMENTATION_ASIN: OnceLock<Documentation> = OnceLock::new();

pub fn get_asin_doc() -> &'static Documentation {
    DOCUMENTATION_ASIN.get_or_init(|| {
        Documentation::builder(
            DOC_SECTION_MATH,
            "Returns the arc sine or inverse sine of a number.",
            "asin(numeric_expression)",
        )
        .with_standard_argument("numeric_expression", Some("Numeric"))
        .build()
    })
}

/// Non-decreasing for all real numbers.
pub fn asinh_order(input: &[ExprProperties]) -> Result<SortProperties> {
    Ok(input[0].sort_properties)
}

static DOCUMENTATION_ASINH: OnceLock<Documentation> = OnceLock::new();

pub fn get_asinh_doc() -> &'static Documentation {
    DOCUMENTATION_ASINH.get_or_init(|| {
        Documentation::builder(
            DOC_SECTION_MATH,
            "Returns the area hyperbolic sine or inverse hyperbolic sine of a number.",
            "asinh(numeric_expression)",
        )
        .with_standard_argument("numeric_expression", Some("Numeric"))
        .build()
    })
}

/// Non-decreasing for all real numbers.
pub fn atan_order(input: &[ExprProperties]) -> Result<SortProperties> {
    Ok(input[0].sort_properties)
}

static DOCUMENTATION_ATAN: OnceLock<Documentation> = OnceLock::new();

pub fn get_atan_doc() -> &'static Documentation {
    DOCUMENTATION_ATAN.get_or_init(|| {
        Documentation::builder(
            DOC_SECTION_MATH,
            "Returns the arc tangent or inverse tangent of a number.",
            "atan(numeric_expression)",
        )
        .with_standard_argument("numeric_expression", Some("Numeric"))
        .build()
    })
}

/// Non-decreasing on the interval \[−1, 1\], undefined otherwise.
pub fn atanh_order(input: &[ExprProperties]) -> Result<SortProperties> {
    let arg = &input[0];
    let range = &arg.range;

    let valid_domain =
        Interval::make_symmetric_unit_interval(&range.lower().data_type())?;

    if valid_domain.contains(range)? == Interval::CERTAINLY_TRUE {
        Ok(arg.sort_properties)
    } else {
        exec_err!("Input range of ATANH contains out-of-domain values")
    }
}

static DOCUMENTATION_ATANH: OnceLock<Documentation> = OnceLock::new();

pub fn get_atanh_doc() -> &'static Documentation {
    DOCUMENTATION_ATANH.get_or_init(|| {
        Documentation::builder(
            DOC_SECTION_MATH,
                "Returns the area hyperbolic tangent or inverse hyperbolic tangent of a number.",

            "atanh(numeric_expression)")
            .with_standard_argument("numeric_expression", Some("Numeric"))
            .build()
    })
}

/// Order depends on the quadrant.
// TODO: Implement ordering rule of the ATAN2 function.
pub fn atan2_order(_input: &[ExprProperties]) -> Result<SortProperties> {
    Ok(SortProperties::Unordered)
}

static DOCUMENTATION_ATANH2: OnceLock<Documentation> = OnceLock::new();

pub fn get_atan2_doc() -> &'static Documentation {
    DOCUMENTATION_ATANH2.get_or_init(|| {
        Documentation::builder(
            DOC_SECTION_MATH,
                "Returns the arc tangent or inverse tangent of `expression_y / expression_x`.",

            "atan2(expression_y, expression_x)")
            .with_argument("expression_y", r#"First numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators."#)
            .with_argument("expression_x", r#"Second numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators."#)
            .build()
    })
}

/// Non-decreasing for all real numbers.
pub fn cbrt_order(input: &[ExprProperties]) -> Result<SortProperties> {
    Ok(input[0].sort_properties)
}

static DOCUMENTATION_CBRT: OnceLock<Documentation> = OnceLock::new();

pub fn get_cbrt_doc() -> &'static Documentation {
    DOCUMENTATION_CBRT.get_or_init(|| {
        Documentation::builder(
            DOC_SECTION_MATH,
            "Returns the cube root of a number.",
            "cbrt(numeric_expression)",
        )
        .with_standard_argument("numeric_expression", Some("Numeric"))
        .build()
    })
}

/// Non-decreasing for all real numbers.
pub fn ceil_order(input: &[ExprProperties]) -> Result<SortProperties> {
    Ok(input[0].sort_properties)
}

static DOCUMENTATION_CEIL: OnceLock<Documentation> = OnceLock::new();

pub fn get_ceil_doc() -> &'static Documentation {
    DOCUMENTATION_CEIL.get_or_init(|| {
        Documentation::builder(
            DOC_SECTION_MATH,
            "Returns the nearest integer greater than or equal to a number.",
            "ceil(numeric_expression)",
        )
        .with_standard_argument("numeric_expression", Some("Numeric"))
        .build()
    })
}

/// Non-increasing on \[0, π\] and then non-decreasing on \[π, 2π\].
/// This pattern repeats periodically with a period of 2π.
// TODO: Implement ordering rule of the ATAN2 function.
pub fn cos_order(_input: &[ExprProperties]) -> Result<SortProperties> {
    Ok(SortProperties::Unordered)
}

static DOCUMENTATION_COS: OnceLock<Documentation> = OnceLock::new();

pub fn get_cos_doc() -> &'static Documentation {
    DOCUMENTATION_COS.get_or_init(|| {
        Documentation::builder(
            DOC_SECTION_MATH,
            "Returns the cosine of a number.",
            "cos(numeric_expression)",
        )
        .with_standard_argument("numeric_expression", Some("Numeric"))
        .build()
    })
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

static DOCUMENTATION_COSH: OnceLock<Documentation> = OnceLock::new();

pub fn get_cosh_doc() -> &'static Documentation {
    DOCUMENTATION_COSH.get_or_init(|| {
        Documentation::builder(
            DOC_SECTION_MATH,
            "Returns the hyperbolic cosine of a number.",
            "cosh(numeric_expression)",
        )
        .with_standard_argument("numeric_expression", Some("Numeric"))
        .build()
    })
}

/// Non-decreasing function that converts radians to degrees.
pub fn degrees_order(input: &[ExprProperties]) -> Result<SortProperties> {
    Ok(input[0].sort_properties)
}

static DOCUMENTATION_DEGREES: OnceLock<Documentation> = OnceLock::new();

pub fn get_degrees_doc() -> &'static Documentation {
    DOCUMENTATION_DEGREES.get_or_init(|| {
        Documentation::builder(
            DOC_SECTION_MATH,
            "Converts radians to degrees.",
            "degrees(numeric_expression)",
        )
        .with_standard_argument("numeric_expression", Some("Numeric"))
        .build()
    })
}

/// Non-decreasing for all real numbers.
pub fn exp_order(input: &[ExprProperties]) -> Result<SortProperties> {
    Ok(input[0].sort_properties)
}

static DOCUMENTATION_EXP: OnceLock<Documentation> = OnceLock::new();

pub fn get_exp_doc() -> &'static Documentation {
    DOCUMENTATION_EXP.get_or_init(|| {
        Documentation::builder(
            DOC_SECTION_MATH,
            "Returns the base-e exponential of a number.",
            "exp(numeric_expression)",
        )
        .with_standard_argument("numeric_expression", Some("Numeric"))
        .build()
    })
}

/// Non-decreasing for all real numbers.
pub fn floor_order(input: &[ExprProperties]) -> Result<SortProperties> {
    Ok(input[0].sort_properties)
}

static DOCUMENTATION_FLOOR: OnceLock<Documentation> = OnceLock::new();

pub fn get_floor_doc() -> &'static Documentation {
    DOCUMENTATION_FLOOR.get_or_init(|| {
        Documentation::builder(
            DOC_SECTION_MATH,
            "Returns the nearest integer less than or equal to a number.",
            "floor(numeric_expression)",
        )
        .with_standard_argument("numeric_expression", Some("Numeric"))
        .build()
    })
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

static DOCUMENTATION_LN: OnceLock<Documentation> = OnceLock::new();

pub fn get_ln_doc() -> &'static Documentation {
    DOCUMENTATION_LN.get_or_init(|| {
        Documentation::builder(
            DOC_SECTION_MATH,
            "Returns the natural logarithm of a number.",
            "ln(numeric_expression)",
        )
        .with_standard_argument("numeric_expression", Some("Numeric"))
        .build()
    })
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

static DOCUMENTATION_LOG2: OnceLock<Documentation> = OnceLock::new();

pub fn get_log2_doc() -> &'static Documentation {
    DOCUMENTATION_LOG2.get_or_init(|| {
        Documentation::builder(
            DOC_SECTION_MATH,
            "Returns the base-2 logarithm of a number.",
            "log2(numeric_expression)",
        )
        .with_standard_argument("numeric_expression", Some("Numeric"))
        .build()
    })
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

static DOCUMENTATION_LOG10: OnceLock<Documentation> = OnceLock::new();

pub fn get_log10_doc() -> &'static Documentation {
    DOCUMENTATION_LOG10.get_or_init(|| {
        Documentation::builder(
            DOC_SECTION_MATH,
            "Returns the base-10 logarithm of a number.",
            "log10(numeric_expression)",
        )
        .with_standard_argument("numeric_expression", Some("Numeric"))
        .build()
    })
}

/// Non-decreasing for all real numbers x.
pub fn radians_order(input: &[ExprProperties]) -> Result<SortProperties> {
    Ok(input[0].sort_properties)
}

static DOCUMENTATION_RADIONS: OnceLock<Documentation> = OnceLock::new();

pub fn get_radians_doc() -> &'static Documentation {
    DOCUMENTATION_RADIONS.get_or_init(|| {
        Documentation::builder(
            DOC_SECTION_MATH,
            "Converts degrees to radians.",
            "radians(numeric_expression)",
        )
        .with_standard_argument("numeric_expression", Some("Numeric"))
        .build()
    })
}

/// Non-decreasing on \[0, π\] and then non-increasing on \[π, 2π\].
/// This pattern repeats periodically with a period of 2π.
// TODO: Implement ordering rule of the SIN function.
pub fn sin_order(_input: &[ExprProperties]) -> Result<SortProperties> {
    Ok(SortProperties::Unordered)
}

static DOCUMENTATION_SIN: OnceLock<Documentation> = OnceLock::new();

pub fn get_sin_doc() -> &'static Documentation {
    DOCUMENTATION_SIN.get_or_init(|| {
        Documentation::builder(
            DOC_SECTION_MATH,
            "Returns the sine of a number.",
            "sin(numeric_expression)",
        )
        .with_standard_argument("numeric_expression", Some("Numeric"))
        .build()
    })
}

/// Non-decreasing for all real numbers.
pub fn sinh_order(input: &[ExprProperties]) -> Result<SortProperties> {
    Ok(input[0].sort_properties)
}

static DOCUMENTATION_SINH: OnceLock<Documentation> = OnceLock::new();

pub fn get_sinh_doc() -> &'static Documentation {
    DOCUMENTATION_SINH.get_or_init(|| {
        Documentation::builder(
            DOC_SECTION_MATH,
            "Returns the hyperbolic sine of a number.",
            "sinh(numeric_expression)",
        )
        .with_standard_argument("numeric_expression", Some("Numeric"))
        .build()
    })
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

static DOCUMENTATION_SQRT: OnceLock<Documentation> = OnceLock::new();

pub fn get_sqrt_doc() -> &'static Documentation {
    DOCUMENTATION_SQRT.get_or_init(|| {
        Documentation::builder(
            DOC_SECTION_MATH,
            "Returns the square root of a number.",
            "sqrt(numeric_expression)",
        )
        .with_standard_argument("numeric_expression", Some("Numeric"))
        .build()
    })
}

/// Non-decreasing between vertical asymptotes at x = k * π ± π / 2 for any
/// integer k.
// TODO: Implement ordering rule of the TAN function.
pub fn tan_order(_input: &[ExprProperties]) -> Result<SortProperties> {
    Ok(SortProperties::Unordered)
}

static DOCUMENTATION_TAN: OnceLock<Documentation> = OnceLock::new();

pub fn get_tan_doc() -> &'static Documentation {
    DOCUMENTATION_TAN.get_or_init(|| {
        Documentation::builder(
            DOC_SECTION_MATH,
            "Returns the tangent of a number.",
            "tan(numeric_expression)",
        )
        .with_standard_argument("numeric_expression", Some("Numeric"))
        .build()
    })
}

/// Non-decreasing for all real numbers.
pub fn tanh_order(input: &[ExprProperties]) -> Result<SortProperties> {
    Ok(input[0].sort_properties)
}

static DOCUMENTATION_TANH: OnceLock<Documentation> = OnceLock::new();

pub fn get_tanh_doc() -> &'static Documentation {
    DOCUMENTATION_TANH.get_or_init(|| {
        Documentation::builder(
            DOC_SECTION_MATH,
            "Returns the hyperbolic tangent of a number.",
            "tanh(numeric_expression)",
        )
        .with_standard_argument("numeric_expression", Some("Numeric"))
        .build()
    })
}

#[cfg(test)]
mod tests {
    use arrow::compute::SortOptions;
    use datafusion_common::Result;

    use super::*;

    #[derive(Debug)]
    struct MonotonicityTestCase {
        name: &'static str,
        func: fn(&[ExprProperties]) -> Result<SortProperties>,
        lower: f64,
        upper: f64,
        input_sort: SortProperties,
        expected: Result<SortProperties>,
    }

    #[test]
    fn test_monotonicity_table() {
        fn create_ep(lower: f64, upper: f64, sp: SortProperties) -> ExprProperties {
            ExprProperties {
                range: Interval::try_new(
                    ScalarValue::from(lower),
                    ScalarValue::from(upper),
                )
                .unwrap(),
                sort_properties: sp,
                preserves_lex_ordering: false,
            }
        }

        let test_cases = vec![
            MonotonicityTestCase {
                name: "acos_order within domain",
                func: acos_order,
                lower: -0.5,
                upper: 0.5,
                input_sort: SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                expected: Ok(SortProperties::Ordered(SortOptions {
                    descending: true,
                    nulls_first: false,
                })),
            },
            MonotonicityTestCase {
                name: "acos_order out of domain",
                func: acos_order,
                lower: -2.0,
                upper: 1.0,
                input_sort: SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                expected: exec_err!("Input range of ACOS contains out-of-domain values"),
            },
            MonotonicityTestCase {
                name: "acosh_order within domain",
                func: acosh_order,
                lower: 2.0,
                upper: 100.0,
                input_sort: SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
                expected: Ok(SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: true,
                })),
            },
            MonotonicityTestCase {
                name: "acosh_order out of domain",
                func: acosh_order,
                lower: 0.5,
                upper: 1.0,
                input_sort: SortProperties::Ordered(SortOptions {
                    descending: true,
                    nulls_first: false,
                }),
                expected: exec_err!("Input range of ACOSH contains out-of-domain values"),
            },
            MonotonicityTestCase {
                name: "asin_order within domain",
                func: asin_order,
                lower: -0.5,
                upper: 0.5,
                input_sort: SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                expected: Ok(SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                })),
            },
            MonotonicityTestCase {
                name: "asin_order out of domain",
                func: asin_order,
                lower: -2.0,
                upper: 1.0,
                input_sort: SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                expected: exec_err!("Input range of ASIN contains out-of-domain values"),
            },
            MonotonicityTestCase {
                name: "asinh_order within domain",
                func: asinh_order,
                lower: -1.0,
                upper: 1.0,
                input_sort: SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                expected: Ok(SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                })),
            },
            MonotonicityTestCase {
                name: "asinh_order out of domain",
                func: asinh_order,
                lower: -2.0,
                upper: 1.0,
                input_sort: SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                expected: Ok(SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                })),
            },
            MonotonicityTestCase {
                name: "atan_order within domain",
                func: atan_order,
                lower: -1.0,
                upper: 1.0,
                input_sort: SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                expected: Ok(SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                })),
            },
            MonotonicityTestCase {
                name: "atan_order out of domain",
                func: atan_order,
                lower: -2.0,
                upper: 1.0,
                input_sort: SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                expected: Ok(SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                })),
            },
            MonotonicityTestCase {
                name: "atanh_order within domain",
                func: atanh_order,
                lower: -0.6,
                upper: 0.6,
                input_sort: SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                expected: Ok(SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                })),
            },
            MonotonicityTestCase {
                name: "atanh_order out of domain",
                func: atanh_order,
                lower: -2.0,
                upper: 1.0,
                input_sort: SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                expected: exec_err!("Input range of ATANH contains out-of-domain values"),
            },
            MonotonicityTestCase {
                name: "cbrt_order within domain",
                func: cbrt_order,
                lower: -1.0,
                upper: 1.0,
                input_sort: SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                expected: Ok(SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                })),
            },
            MonotonicityTestCase {
                name: "cbrt_order out of domain",
                func: cbrt_order,
                lower: -2.0,
                upper: 1.0,
                input_sort: SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                expected: Ok(SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                })),
            },
            MonotonicityTestCase {
                name: "ceil_order within domain",
                func: ceil_order,
                lower: -1.0,
                upper: 1.0,
                input_sort: SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                expected: Ok(SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                })),
            },
            MonotonicityTestCase {
                name: "ceil_order out of domain",
                func: ceil_order,
                lower: -2.0,
                upper: 1.0,
                input_sort: SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                expected: Ok(SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                })),
            },
            MonotonicityTestCase {
                name: "cos_order within domain",
                func: cos_order,
                lower: 0.0,
                upper: 2.0 * std::f64::consts::PI,
                input_sort: SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                expected: Ok(SortProperties::Unordered),
            },
            MonotonicityTestCase {
                name: "cos_order out of domain",
                func: cos_order,
                lower: -2.0,
                upper: 1.0,
                input_sort: SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                expected: Ok(SortProperties::Unordered),
            },
            MonotonicityTestCase {
                name: "cosh_order within domain positive",
                func: cosh_order,
                lower: 5.0,
                upper: 100.0,
                input_sort: SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                expected: Ok(SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                })),
            },
            MonotonicityTestCase {
                name: "cosh_order within domain negative",
                func: cosh_order,
                lower: -100.0,
                upper: -5.0,
                input_sort: SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                expected: Ok(SortProperties::Ordered(SortOptions {
                    descending: true,
                    nulls_first: false,
                })),
            },
            MonotonicityTestCase {
                name: "cosh_order out of domain so unordered",
                func: cosh_order,
                lower: -1.0,
                upper: 1.0,
                input_sort: SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                expected: Ok(SortProperties::Unordered),
            },
            MonotonicityTestCase {
                name: "degrees_order",
                func: degrees_order,
                lower: -1.0,
                upper: 1.0,
                input_sort: SortProperties::Ordered(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
                expected: Ok(SortProperties::Ordered(SortOptions {
                    descending: true,
                    nulls_first: true,
                })),
            },
            MonotonicityTestCase {
                name: "exp_order",
                func: exp_order,
                lower: -1000.0,
                upper: 1000.0,
                input_sort: SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                expected: Ok(SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                })),
            },
            MonotonicityTestCase {
                name: "floor_order",
                func: floor_order,
                lower: -1.0,
                upper: 1.0,
                input_sort: SortProperties::Ordered(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
                expected: Ok(SortProperties::Ordered(SortOptions {
                    descending: true,
                    nulls_first: true,
                })),
            },
            MonotonicityTestCase {
                name: "ln_order within domain",
                func: ln_order,
                lower: 1.0,
                upper: 2.0,
                input_sort: SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                expected: Ok(SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                })),
            },
            MonotonicityTestCase {
                name: "ln_order out of domain",
                func: ln_order,
                lower: -5.0,
                upper: -4.0,
                input_sort: SortProperties::Ordered(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                expected: exec_err!("Input range of LN contains out-of-domain values"),
            },
        ];

        for tcase in test_cases {
            let input = vec![create_ep(tcase.lower, tcase.upper, tcase.input_sort)];
            let actual = (tcase.func)(&input);
            match (&actual, &tcase.expected) {
                (Ok(a), Ok(e)) => assert_eq!(
                    a, e,
                    "Test '{}' failed: got {:?}, expected {:?}",
                    tcase.name, a, e
                ),
                (Err(e1), Err(e2)) => {
                    assert_eq!(
                        e1.strip_backtrace().to_string(),
                        e2.strip_backtrace().to_string(),
                        "Test '{}' failed: got {:?}, expected {:?}",
                        tcase.name,
                        e1,
                        e2
                    )
                } // Both are errors, so it's fine
                _ => panic!(
                    "Test '{}' failed: got {:?}, expected {:?}",
                    tcase.name, actual, tcase.expected
                ),
            }
        }
    }
}
