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
//! This function instead computes at the declared scale using checked i256
//! arithmetic, without changing DataFusion's native SQL operators.
//!
//! Supports Decimal128 operands and outputs with nonnegative scales, including
//! all standard Substrait decimal precisions (1 through 38). Values are rounded
//! once to nearest, with ties away from zero, and checked against the output
//! precision. NULL inputs propagate; overflow and zero divisors return errors.
//! An explicit overflow option must allow ERROR. Other decimal representations
//! and function options require separate implementations.

use std::sync::Arc;

use datafusion::arrow::array::Decimal128Array;
use datafusion::arrow::datatypes::i256;
use datafusion::arrow::datatypes::validate_decimal_precision_and_scale;
use datafusion::arrow::datatypes::{DataType, Decimal128Type, Field, FieldRef};
use datafusion::common::cast::as_primitive_array;
use datafusion::common::{
    Result, ScalarValue, exec_datafusion_err, exec_err, substrait_err,
};
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
    input_scales: [i8; 2],
    precision: u8,
    scale: i8,
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
        Ok(Self {
            function_signature: function_signature.to_owned(),
            signature: Signature::exact(Vec::from(input_types), Volatility::Immutable),
            op,
            input_scales: [left_scale, right_scale],
            precision,
            scale,
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
        let left = i256::from_i128(left);
        let right = i256::from_i128(right);
        let [s1, s2] = self.input_scales.map(i16::from);
        let output_scale = i16::from(self.scale);
        // Each input has at most 38 digits. Exact products and operands aligned
        // to max(s1, s2) fit in i256; rescale only after the operation so neither
        // cancellation nor fractional carries are lost.
        let result = match self.op {
            Operator::Plus | Operator::Minus | Operator::Modulo => {
                let scale = s1.max(s2);
                left.checked_mul(power_of_ten(scale - s1)).and_then(|l| {
                    let r = right.checked_mul(power_of_ten(scale - s2))?;
                    let value = match self.op {
                        Operator::Plus => l.checked_add(r),
                        Operator::Minus => l.checked_sub(r),
                        _ => l.checked_rem(r),
                    }?;
                    rescale(value, scale, output_scale)
                })
            }
            Operator::Multiply => left
                .checked_mul(right)
                .and_then(|value| rescale(value, s1 + s2, output_scale)),
            Operator::Divide => {
                // Work directly at the output scale. In particular, do not use
                // Arrow's s1 + 4 intermediate scale for high-scale operands.
                let exponent = output_scale + s2 - s1;
                if exponent >= 0 {
                    left.checked_mul(power_of_ten(exponent))
                        .and_then(|numerator| rounded_div(numerator, right))
                } else {
                    right
                        .checked_mul(power_of_ten(-exponent))
                        .and_then(|denominator| rounded_div(left, denominator))
                }
                // If scaling the numerator overflows i256, dividing by a
                // <=38-digit denominator cannot yield a <=38-digit result.
                // Thus this failure cannot reject a representable output.
            }
            _ => unreachable!("only decimal arithmetic operators are constructed"),
        };
        let max = power_of_ten(i16::from(self.precision)) - i256::ONE;
        result
            .filter(|v| *v >= -max && *v <= max)
            .and_then(i256::to_i128)
            .ok_or_else(|| {
                exec_datafusion_err!(
                    "Decimal overflow in Substrait {}: result does not fit {:?}",
                    self.name(),
                    self.output_type()
                )
            })
    }
}

/// Exponents are bounded by 76 by the validated input and output types.
fn power_of_ten(exponent: i16) -> i256 {
    i256::from_i128(10).wrapping_pow(exponent as u32)
}

fn rescale(value: i256, from: i16, to: i16) -> Option<i256> {
    if to >= from {
        value.checked_mul(power_of_ten(to - from))
    } else {
        rounded_div(value, power_of_ten(from - to))
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let left = as_primitive_array::<Decimal128Type>(&arrays[0])?;
        let right = as_primitive_array::<Decimal128Type>(&arrays[1])?;
        let values = left
            .iter()
            .zip(right.iter())
            .map(|(left, right)| match (left, right) {
                (Some(left), Some(right)) => self.evaluate_value(left, right).map(Some),
                _ => Ok(None),
            })
            .collect::<Result<Vec<_>>>()?;
        let result = Decimal128Array::from(values)
            .with_precision_and_scale(self.precision, self.scale)?;
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        // Raw integers encode value * 10^scale. Expected answers are rounded
        // mathematical results, not results obtained from Arrow's kernels.
        for (op, scales, output, left, right, expected) in [
            (Divide, [2, 1], (21, 8), 100, 30, 33_333_333),
            (Divide, [2, 1], (21, 8), 200, 30, 66_666_667),
            (Divide, [2, 1], (21, 8), -200, 30, -66_666_667),
            (Divide, [2, 1], (21, 8), 200, -30, -66_666_667),
            (Divide, [2, 1], (21, 8), -200, -30, 66_666_667),
            (Divide, [0, 0], (3, 0), 5, 2, 3),
            (Divide, [0, 0], (3, 0), -5, 2, -3),
            (Divide, [0, 0], (3, 0), 4, 3, 1),
            // The half threshold must work for odd denominators too.
            (Divide, [0, 0], (3, 0), 5, 3, 2),
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
            // Rounding can itself make the result overflow.
            (Operator::Divide, [0, 0], (2, 0), 199, 2),
            (Operator::Divide, [0, 0], (2, 0), -199, 2),
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
}
