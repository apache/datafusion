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

//! Arithmetic whose result precision and scale come from a Substrait call.
//!
//! Arrow's native decimal kernels derive their own result types. Casting their
//! output cannot recover discarded digits or avoid an intermediate overflow.
//! This function computes at the declared scale using checked integer arithmetic,
//! with i256 intermediates when needed. DataFusion's native SQL operators are
//! unchanged.
//!
//! Supports Decimal128 operands and outputs with nonnegative scales, including
//! all standard Substrait decimal precisions (1 through 38). Division truncates
//! toward zero, like native division. Reducing the scale of other operations
//! rounds to nearest, with ties away from zero, like decimal casts. Results are
//! checked against the output precision. NULL inputs propagate; overflow and
//! zero divisors return errors.
//! An explicit overflow option must allow ERROR. Other decimal representations
//! and function options require separate implementations.

use std::sync::Arc;

use datafusion::arrow::array::Decimal128Array;
use datafusion::arrow::compute::kernels::arity::{try_binary, try_unary};
use datafusion::arrow::datatypes::i256;
use datafusion::arrow::datatypes::validate_decimal_precision_and_scale;
use datafusion::arrow::datatypes::{DataType, Decimal128Type, Field, FieldRef};
use datafusion::arrow::error::ArrowError;
use datafusion::common::cast::as_primitive_array;
use datafusion::common::{
    Result, ScalarValue, exec_datafusion_err, exec_err, substrait_err,
};
use datafusion::logical_expr::interval_arithmetic::Interval;
use datafusion::logical_expr::{
    ColumnarValue, Operator, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, Volatility,
};
use substrait::proto::FunctionOption;

/// A consumer-created function, with the declared output type included in its
/// identity so optimizations cannot merge calls with different decimal types.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct DecimalArithmetic {
    function_signature: String,
    signature: Signature,
    op: Operator,
    precision: u8,
    scale: i8,
    left_multiplier: i256,
    right_multiplier: i256,
    result_multiplier: i256,
    result_divisor: i256,
    max: i128,
    narrow: Option<DecimalArithmetic128>,
}

impl DecimalArithmetic {
    pub(crate) fn try_new(
        function_signature: &str,
        op: Operator,
        input_types: [DataType; 2],
        output_type: &DataType,
        options: &[FunctionOption],
    ) -> Result<Self> {
        let decimal = |dt: &DataType| -> Result<(u8, i8)> {
            if let DataType::Decimal128(p, s) = dt
                && *s >= 0
            {
                validate_decimal_precision_and_scale::<Decimal128Type>(*p, *s)?;
                return Ok((*p, *s));
            }
            substrait_err!(
                "Unsupported decimal arithmetic type {dt:?} for {function_signature}: \
                 expected Decimal128 with nonnegative scale"
            )
        };
        let (_, left_scale) = decimal(&input_types[0])?;
        let (_, right_scale) = decimal(&input_types[1])?;
        let (precision, scale) = decimal(output_type)?;
        // An omitted option leaves behavior to the consumer. We choose ERROR.
        // For a specified option, use the first supported preference (ERROR is
        // currently the only supported overflow behavior), or reject the call.
        let mut overflow_seen = false;
        for option in options {
            if !option.name.eq_ignore_ascii_case("overflow") || overflow_seen {
                return substrait_err!(
                    "Unsupported or duplicate option {} for {function_signature}",
                    option.name
                );
            }
            overflow_seen = true;
            if !option
                .preference
                .iter()
                .any(|v| v.eq_ignore_ascii_case("ERROR"))
            {
                return substrait_err!(
                    "Unsupported overflow preferences {:?} for {function_signature}; supported: ERROR",
                    option.preference
                );
            }
        }
        // Scale factors depend only on types. Compute them once when importing
        // the call, rather than repeating wide powers of ten for every row.
        let [s1, s2, output_scale] = [left_scale, right_scale, scale].map(i16::from);
        let (left_shift, right_shift, result_shift) = match op {
            Operator::Plus | Operator::Minus | Operator::Modulo => {
                let common_scale = s1.max(s2);
                (
                    common_scale - s1,
                    common_scale - s2,
                    output_scale - common_scale,
                )
            }
            Operator::Multiply => (0, 0, output_scale - s1 - s2),
            Operator::Divide => {
                let shift = output_scale + s2 - s1;
                (shift.max(0), (-shift).max(0), 0)
            }
            _ => unreachable!("only decimal arithmetic operators are constructed"),
        };
        let factors = [
            power_of_ten(left_shift),
            power_of_ten(right_shift),
            power_of_ten(result_shift.max(0)),
            power_of_ten((-result_shift).max(0)),
        ];
        Ok(Self {
            function_signature: function_signature.to_owned(),
            signature: Signature::exact(Vec::from(input_types), Volatility::Immutable),
            op,
            precision,
            scale,
            left_multiplier: factors[0],
            right_multiplier: factors[1],
            result_multiplier: factors[2],
            result_divisor: factors[3],
            max: 10_i128.pow(u32::from(precision)) - 1,
            narrow: DecimalArithmetic128::new(factors),
        })
    }

    pub(crate) fn function_signature(&self) -> &str {
        &self.function_signature
    }

    fn output_type(&self) -> DataType {
        DataType::Decimal128(self.precision, self.scale)
    }

    fn evaluate_value(&self, left: i128, right: i128) -> Result<i128> {
        if matches!(self.op, Operator::Divide | Operator::Modulo) && right == 0 {
            return exec_err!("Divide by zero in Substrait {}", self.name());
        }
        // Precision describes a column's capacity, not the size of every value.
        // Most values fit in i128 even in decimal(38, s) columns. Retry in i256
        // if any intermediate overflows; never narrow or round the inputs.
        if let Some(narrow) = &self.narrow
            && let Some(value) = narrow.evaluate(self.op, left, right)
        {
            return self.check_result(Some(value));
        }
        let left = i256::from_i128(left);
        let right = i256::from_i128(right);
        // Each input has at most 38 digits. Exact products and operands aligned
        // to max(s1, s2) fit in i256; rescale only after the operation so neither
        // cancellation nor fractional carries are lost.
        // For division the factors calculate directly at the output scale.
        // If scaling the numerator overflows i256, dividing by a <=38-digit
        // denominator cannot yield a <=38-digit result either.
        let result = multiply(left, self.left_multiplier)
            .and_then(|left| {
                let right = multiply(right, self.right_multiplier)?;
                match self.op {
                    Operator::Plus => left.checked_add(right),
                    Operator::Minus => left.checked_sub(right),
                    Operator::Multiply => left.checked_mul(right),
                    // Keep division's rounding independent of overflow options
                    // and precision-only changes to the declared result type.
                    Operator::Divide => left.checked_div(right),
                    Operator::Modulo => left.checked_rem(right),
                    _ => {
                        unreachable!("only decimal arithmetic operators are constructed")
                    }
                }
            })
            .and_then(|value| multiply(value, self.result_multiplier))
            .and_then(|value| {
                if self.result_divisor == i256::ONE {
                    Some(value)
                } else {
                    rounded_div(value, self.result_divisor)
                }
            });
        self.check_result(result.and_then(i256::to_i128))
    }

    fn check_result(&self, result: Option<i128>) -> Result<i128> {
        result
            .filter(|v| *v >= -self.max && *v <= self.max)
            .ok_or_else(|| {
                exec_datafusion_err!(
                    "Decimal overflow in Substrait {}: result does not fit {:?}",
                    self.name(),
                    self.output_type()
                )
            })
    }
}

/// The common case uses native integers. `None` means the exact intermediate
/// needs wider arithmetic, including when it would fit after scale reduction.
#[derive(Debug, PartialEq, Eq, Hash)]
struct DecimalArithmetic128 {
    left_multiplier: i128,
    right_multiplier: i128,
    result_multiplier: i128,
    result_divisor: i128,
}

impl DecimalArithmetic128 {
    fn new(factors: [i256; 4]) -> Option<Self> {
        Some(Self {
            left_multiplier: factors[0].to_i128()?,
            right_multiplier: factors[1].to_i128()?,
            result_multiplier: factors[2].to_i128()?,
            result_divisor: factors[3].to_i128()?,
        })
    }

    fn evaluate(&self, op: Operator, left: i128, right: i128) -> Option<i128> {
        let left = left.checked_mul(self.left_multiplier)?;
        let right = right.checked_mul(self.right_multiplier)?;
        let value = match op {
            Operator::Plus => left.checked_add(right),
            Operator::Minus => left.checked_sub(right),
            Operator::Multiply => left.checked_mul(right),
            Operator::Divide => left.checked_div(right),
            Operator::Modulo => left.checked_rem(right),
            _ => unreachable!("only decimal arithmetic operators are constructed"),
        }?
        .checked_mul(self.result_multiplier)?;
        if self.result_divisor == 1 {
            return Some(value);
        }
        // The divisor is a positive power of ten, at least 10, so neither
        // division by zero nor MIN / -1 is possible here.
        let quotient = value / self.result_divisor;
        let remainder = value % self.result_divisor;
        if remainder.unsigned_abs() >= (self.result_divisor / 2) as u128 {
            quotient.checked_add(value.signum())
        } else {
            Some(quotient)
        }
    }
}

/// Exponents are bounded by 76 by the validated input and output types.
fn power_of_ten(exponent: i16) -> i256 {
    i256::from_i128(10).wrapping_pow(exponent as u32)
}

fn multiply(value: i256, factor: i256) -> Option<i256> {
    if factor == i256::ONE {
        Some(value)
    } else {
        value.checked_mul(factor)
    }
}

/// Round once, to nearest with ties away from zero, as Arrow's decimal casts do.
/// The decimal arithmetic extension does not specify a rounding option.
fn rounded_div(numerator: i256, denominator: i256) -> Option<i256> {
    let quotient = numerator.checked_div(denominator)?;
    let remainder = numerator.checked_rem(denominator)?.checked_abs()?;
    let divisor = denominator.checked_abs()?;
    // ceil(divisor / 2), without doubling a potentially large remainder.
    let half = divisor / i256::from_i128(2) + divisor % i256::from_i128(2);
    if remainder >= half {
        quotient.checked_add(if (numerator < i256::ZERO) != (denominator < i256::ZERO) {
            i256::MINUS_ONE
        } else {
            i256::ONE
        })
    } else {
        Some(quotient)
    }
}

impl ScalarUDFImpl for DecimalArithmetic {
    fn name(&self) -> &str {
        self.function_signature
            .split(':')
            .next()
            .unwrap_or(&self.function_signature)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.output_type())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            self.output_type(),
            args.arg_fields.iter().any(|f| f.is_nullable()),
        )))
    }

    fn is_strict(&self) -> bool {
        true
    }

    fn evaluate_bounds(&self, _inputs: &[&Interval]) -> Result<Interval> {
        // Keep the decimal type when a parent expression asks for a range.
        // Tighter bounds would also need to account for rounding and overflow.
        Interval::make_unbounded(&self.output_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let evaluate = |left, right| {
            self.evaluate_value(left, right)
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))
        };
        // Scalar operands stay scalar: common expressions such as price * 0.9
        // should not allocate a repeated column for the constant.
        let result: Decimal128Array = match (&args.args[0], &args.args[1]) {
            (
                ColumnarValue::Scalar(ScalarValue::Decimal128(left, _, _)),
                ColumnarValue::Scalar(ScalarValue::Decimal128(right, _, _)),
            ) => {
                let value = match (left, right) {
                    (Some(left), Some(right)) => {
                        Some(self.evaluate_value(*left, *right)?)
                    }
                    _ => None,
                };
                return Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                    value,
                    self.precision,
                    self.scale,
                )));
            }
            (ColumnarValue::Array(left), ColumnarValue::Array(right)) => try_binary(
                as_primitive_array::<Decimal128Type>(left)?,
                as_primitive_array::<Decimal128Type>(right)?,
                evaluate,
            )?,
            (
                ColumnarValue::Array(array),
                ColumnarValue::Scalar(ScalarValue::Decimal128(value, _, _)),
            ) => {
                let array = as_primitive_array::<Decimal128Type>(array)?;
                match value {
                    Some(value) => try_unary(array, |left| evaluate(left, *value))?,
                    None => Decimal128Array::new_null(array.len()),
                }
            }
            (
                ColumnarValue::Scalar(ScalarValue::Decimal128(value, _, _)),
                ColumnarValue::Array(array),
            ) => {
                let array = as_primitive_array::<Decimal128Type>(array)?;
                match value {
                    Some(value) => try_unary(array, |right| evaluate(*value, right))?,
                    None => Decimal128Array::new_null(array.len()),
                }
            }
            _ => {
                return exec_err!(
                    "Expected two Decimal128 arguments for {}",
                    self.name()
                );
            }
        };
        Ok(ColumnarValue::Array(Arc::new(
            result.with_precision_and_scale(self.precision, self.scale)?,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigdecimal::{BigDecimal, RoundingMode};

    fn function(op: Operator, scales: [i8; 2], output: (u8, i8)) -> DecimalArithmetic {
        DecimalArithmetic::try_new(
            "decimal_test:dec_dec",
            op,
            scales.map(|s| DataType::Decimal128(38, s)),
            &DataType::Decimal128(output.0, output.1),
            &[],
        )
        .unwrap()
    }

    #[test]
    fn decimal_values_at_declared_scale() -> Result<()> {
        use Operator::*;
        // Raw integers encode value * 10^scale. Division truncates; scale
        // reduction after the other operations rounds the exact result.
        for (op, scales, output, left, right, expected) in [
            (Divide, [2, 1], (21, 8), 100, 30, 33_333_333),
            (Divide, [2, 1], (21, 8), 200, 30, 66_666_666),
            (Divide, [2, 1], (21, 8), -200, 30, -66_666_666),
            (Divide, [2, 1], (21, 8), 200, -30, -66_666_666),
            (Divide, [2, 1], (21, 8), -200, -30, 66_666_666),
            (Divide, [0, 0], (3, 0), 5, 2, 2),
            (Divide, [0, 0], (3, 0), -5, 2, -2),
            (Divide, [0, 0], (3, 0), 4, 3, 1),
            (Divide, [0, 0], (3, 0), 5, 3, 1),
            (Plus, [2, 2], (3, 1), 4, 4, 1),
            (Plus, [2, 2], (3, 1), -4, -4, -1),
            (Minus, [2, 2], (3, 1), 104, 96, 1),
            (Multiply, [2, 2], (5, 2), 125, 250, 313),
            (Multiply, [2, 2], (5, 2), -125, 250, -313),
            (Modulo, [3, 1], (5, 2), 12_345, 20, 35),
            (Modulo, [3, 1], (5, 2), -12_345, 20, -35),
            // Precision-only changes and increasing scale are supported.
            (Plus, [2, 1], (20, 2), 100, 30, 400),
            (Multiply, [1, 1], (20, 8), 15, 20, 300_000_000),
        ] {
            assert_eq!(
                function(op, scales, output).evaluate_value(left, right)?,
                expected,
                "{left} {op} {right}, scales={scales:?}, output={output:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn decimal_wide_intermediates() -> Result<()> {
        // 0.1 / 0.1 = 1.000000. Using Arrow Decimal256 division at scale
        // s1 + 4 would overflow while scaling the numerator for this example.
        let tenth = 10_i128.pow(37);
        assert_eq!(
            function(Operator::Divide, [38, 38], (38, 6)).evaluate_value(tenth, tenth)?,
            1_000_000
        );
        assert_eq!(
            function(Operator::Divide, [38, 0], (38, 6)).evaluate_value(tenth, 1)?,
            100_000
        );
        assert_eq!(
            function(Operator::Multiply, [38, 38], (38, 6))
                .evaluate_value(tenth, tenth)?,
            10_000
        );
        // The exact sum exceeds i128, but fits after reducing scale.
        assert_eq!(
            function(Operator::Plus, [10, 10], (38, 9))
                .evaluate_value(9 * tenth, 9 * tenth)?,
            18 * 10_i128.pow(36)
        );
        // Keep all digits before cancellation and before multiplication's
        // final scale reduction.
        assert_eq!(
            function(Operator::Minus, [10, 10], (38, 9))
                .evaluate_value(9 * tenth, 9 * tenth - 6)?,
            1
        );
        assert_eq!(
            function(Operator::Multiply, [10, 10], (38, 6))
                .evaluate_value(9 * tenth, 10_i128.pow(10))?,
            9 * 10_i128.pow(33)
        );
        // Maximum scale adjustment, including zero numerators.
        assert_eq!(
            function(Operator::Divide, [0, 38], (38, 38)).evaluate_value(0, 1)?,
            0
        );
        Ok(())
    }

    #[test]
    fn decimal_overflow_and_zero_divisors() {
        for (op, scales, output, left, right) in [
            (Operator::Plus, [0, 0], (2, 0), 99, 1),
            (Operator::Divide, [0, 0], (2, 0), 200, 2),
            (Operator::Divide, [0, 0], (2, 0), -200, 2),
            // Rounding after addition can itself make the result overflow.
            (Operator::Plus, [1, 1], (2, 0), 994, 1),
            (Operator::Plus, [1, 1], (2, 0), -994, -1),
            (
                Operator::Multiply,
                [10, 10],
                (38, 6),
                9 * 10_i128.pow(37),
                9 * 10_i128.pow(37),
            ),
            // The scaled numerator exceeds i256, and the final result really
            // does exceed the declared precision as well.
            (Operator::Divide, [0, 38], (38, 38), 10_i128.pow(37), 1),
        ] {
            let err = function(op, scales, output)
                .evaluate_value(left, right)
                .unwrap_err();
            assert!(err.to_string().contains("Decimal overflow"), "{err}");
        }
        for op in [Operator::Divide, Operator::Modulo] {
            let err = function(op, [2, 2], (21, 8))
                .evaluate_value(0, 0)
                .unwrap_err();
            assert!(err.to_string().contains("Divide by zero"), "{err}");
        }
    }

    #[test]
    fn decimal_option_negotiation() {
        for (name, preferences, supported) in [
            ("overflow", vec!["ERROR"], true),
            ("OVERFLOW", vec!["SILENT", "error"], true),
            ("overflow", vec!["SATURATE"], false),
            ("overflow", vec!["SILENT"], false),
            ("overflow", vec![], false),
            ("rounding", vec!["HALF_UP"], false),
        ] {
            let result = DecimalArithmetic::try_new(
                "divide:dec_dec",
                Operator::Divide,
                [DataType::Decimal128(10, 2), DataType::Decimal128(5, 1)],
                &DataType::Decimal128(21, 8),
                &[FunctionOption {
                    name: name.into(),
                    preference: preferences.into_iter().map(str::to_owned).collect(),
                }],
            );
            assert_eq!(result.is_ok(), supported, "{result:?}");
        }
    }

    #[test]
    fn decimal_matches_arbitrary_precision_arithmetic() -> Result<()> {
        // Use a separate decimal implementation to check values and overflow
        // across the supported scales, including intermediates wider than i128.
        let mut seed = 42_u64;
        let mut next = || {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            seed
        };
        for _ in 0..3000 {
            let scales = [(next() % 39) as i8, (next() % 39) as i8];
            let precision = (next() % 38 + 1) as u8;
            let scale = (next() % (u64::from(precision) + 1)) as i8;
            let left = i128::from(next() as i64) * 10_i128.pow((next() % 19) as u32);
            let right = i128::from(next() as i64) * 10_i128.pow((next() % 19) as u32);
            let l = BigDecimal::new(left.into(), i64::from(scales[0]));
            let r = BigDecimal::new(right.into(), i64::from(scales[1]));
            for op in [
                Operator::Plus,
                Operator::Minus,
                Operator::Multiply,
                Operator::Divide,
                Operator::Modulo,
            ] {
                let (exact, rounding) = match op {
                    Operator::Plus => (&l + &r, RoundingMode::HalfUp),
                    Operator::Minus => (&l - &r, RoundingMode::HalfUp),
                    Operator::Multiply => (&l * &r, RoundingMode::HalfUp),
                    Operator::Divide => (&l / &r, RoundingMode::Down),
                    Operator::Modulo => (&l % &r, RoundingMode::HalfUp),
                    _ => unreachable!(),
                };
                let (unscaled, _) = exact
                    .with_scale_round(i64::from(scale), rounding)
                    .as_bigint_and_exponent();
                let digits = unscaled.to_string();
                let expected =
                    if digits.trim_start_matches('-').len() > usize::from(precision) {
                        None
                    } else {
                        Some(digits.parse::<i128>().unwrap())
                    };
                let result =
                    function(op, scales, (precision, scale)).evaluate_value(left, right);
                match expected {
                    Some(expected) => assert_eq!(
                        result?, expected,
                        "{left} {op} {right}, scales {scales:?}, output ({precision},{scale})"
                    ),
                    None => assert!(
                        result.unwrap_err().to_string().contains("Decimal overflow")
                    ),
                }
            }
        }
        Ok(())
    }
}
