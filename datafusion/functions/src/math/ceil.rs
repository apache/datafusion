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

use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray};
use arrow::datatypes::{
    DataType, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, Float32Type,
    Float64Type,
};
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;

use super::decimal::{apply_decimal_op, ceil_decimal_value};

#[user_doc(
    doc_section(label = "Math Functions"),
    description = "Returns the nearest integer greater than or equal to a number.",
    syntax_example = "ceil(numeric_expression)",
    standard_argument(name = "numeric_expression", prefix = "Numeric"),
    sql_example = r#"```sql
> SELECT ceil(3.14);
+------------+
| ceil(3.14) |
+------------+
| 4.0        |
+------------+
```"#
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CeilFunc {
    signature: Signature,
}

impl Default for CeilFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl CeilFunc {
    pub fn new() -> Self {
        let decimal_sig = Coercion::new_exact(TypeSignatureClass::Decimal);
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![decimal_sig]),
                    TypeSignature::Uniform(1, vec![DataType::Float64, DataType::Float32]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for CeilFunc {
    fn name(&self) -> &str {
        "ceil"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::Null => Ok(DataType::Float64),
            other => Ok(other.clone()),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arg = &args.args[0];

        // Scalar fast path for float types - avoid array conversion overhead entirely
        if let ColumnarValue::Scalar(scalar) = arg {
            match scalar {
                ScalarValue::Float64(v) => {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Float64(
                        v.map(f64::ceil),
                    )));
                }
                ScalarValue::Float32(v) => {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Float32(
                        v.map(f32::ceil),
                    )));
                }
                ScalarValue::Null => {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Float64(None)));
                }
                // For decimals: convert to array of size 1, process, then extract scalar
                // This ensures we don't expand the array while reusing overflow validation
                _ => {}
            }
        }

        // Track if input was a scalar to convert back at the end
        let is_scalar = matches!(arg, ColumnarValue::Scalar(_));

        // Array path (also handles decimal scalars converted to size-1 arrays)
        let value = arg.to_array(args.number_rows)?;

        let result: ArrayRef = match value.data_type() {
            DataType::Float64 => Arc::new(
                value
                    .as_primitive::<Float64Type>()
                    .unary::<_, Float64Type>(f64::ceil),
            ),
            DataType::Float32 => Arc::new(
                value
                    .as_primitive::<Float32Type>()
                    .unary::<_, Float32Type>(f32::ceil),
            ),
            DataType::Null => {
                return Ok(ColumnarValue::Scalar(ScalarValue::Float64(None)));
            }
            DataType::Decimal32(precision, scale) => {
                apply_decimal_op::<Decimal32Type, _>(
                    &value,
                    *precision,
                    *scale,
                    self.name(),
                    ceil_decimal_value,
                )?
            }
            DataType::Decimal64(precision, scale) => {
                apply_decimal_op::<Decimal64Type, _>(
                    &value,
                    *precision,
                    *scale,
                    self.name(),
                    ceil_decimal_value,
                )?
            }
            DataType::Decimal128(precision, scale) => {
                apply_decimal_op::<Decimal128Type, _>(
                    &value,
                    *precision,
                    *scale,
                    self.name(),
                    ceil_decimal_value,
                )?
            }
            DataType::Decimal256(precision, scale) => {
                apply_decimal_op::<Decimal256Type, _>(
                    &value,
                    *precision,
                    *scale,
                    self.name(),
                    ceil_decimal_value,
                )?
            }
            other => {
                return exec_err!(
                    "Unsupported data type {other:?} for function {}",
                    self.name()
                );
            }
        };

        // If input was a scalar, convert result back to scalar
        if is_scalar {
            ScalarValue::try_from_array(&result, 0).map(ColumnarValue::Scalar)
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        Ok(input[0].sort_properties)
    }

    fn evaluate_bounds(&self, inputs: &[&Interval]) -> Result<Interval> {
        let [input] = inputs else {
            return exec_err!(
                "ceil expected 1 argument for bounds evaluation, got {}",
                inputs.len()
            );
        };
        let data_type = input.data_type();
        match (ceil_scalar(input.lower()), ceil_scalar(input.upper())) {
            (Some(lo), Some(hi)) => Interval::try_new(lo, hi)
                .or_else(|_| Interval::make_unbounded(&data_type)),
            _ => Interval::make_unbounded(&data_type),
        }
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        inputs: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        let [input_interval] = inputs else {
            return exec_err!(
                "ceil expected 1 argument for constraint propagation, got {}",
                inputs.len()
            );
        };
        // ceil(x) ∈ [N, M] → x ∈ (ceil(N)−1, floor(M)]
        // Normalize bounds to integers ceil can actually take before mapping back.
        let lo = match interval.lower() {
            ScalarValue::Float64(Some(n)) if n.is_finite() => {
                Some(ScalarValue::Float64(Some(n.ceil() - 1.0)))
            }
            ScalarValue::Float32(Some(n)) if n.is_finite() => {
                Some(ScalarValue::Float32(Some(n.ceil() - 1.0)))
            }
            _ => None,
        };
        let hi = match interval.upper() {
            ScalarValue::Float64(Some(n)) if n.is_finite() => {
                Some(ScalarValue::Float64(Some(n.floor())))
            }
            ScalarValue::Float32(Some(n)) if n.is_finite() => {
                Some(ScalarValue::Float32(Some(n.floor())))
            }
            _ => None,
        };
        match (lo, hi) {
            (Some(lo), Some(hi)) => {
                let constraint = Interval::try_new(lo, hi)?;
                Ok(input_interval.intersect(constraint)?.map(|r| vec![r]))
            }
            _ => Ok(Some(vec![(*input_interval).clone()])),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn ceil_scalar(v: &ScalarValue) -> Option<ScalarValue> {
    match v {
        ScalarValue::Float64(Some(f)) if f.is_finite() => {
            Some(ScalarValue::Float64(Some(f.ceil())))
        }
        ScalarValue::Float32(Some(f)) if f.is_finite() => {
            Some(ScalarValue::Float32(Some(f.ceil())))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ceil() -> CeilFunc {
        CeilFunc::new()
    }

    fn f64_interval(lo: f64, hi: f64) -> Interval {
        Interval::try_new(
            ScalarValue::Float64(Some(lo)),
            ScalarValue::Float64(Some(hi)),
        )
        .unwrap()
    }

    fn f32_interval(lo: f32, hi: f32) -> Interval {
        Interval::try_new(
            ScalarValue::Float32(Some(lo)),
            ScalarValue::Float32(Some(hi)),
        )
        .unwrap()
    }

    fn unbounded_f64() -> Interval {
        Interval::make_unbounded(&DataType::Float64).unwrap()
    }

    fn unbounded_f32() -> Interval {
        Interval::make_unbounded(&DataType::Float32).unwrap()
    }

    // --- evaluate_bounds ---

    #[test]
    fn test_evaluate_bounds_basic() {
        // ceil([1.2, 3.7]) = [2.0, 4.0]
        let input = f64_interval(1.2, 3.7);
        let result = ceil().evaluate_bounds(&[&input]).unwrap();
        assert_eq!(result, f64_interval(2.0, 4.0));
    }

    #[test]
    fn test_evaluate_bounds_already_integer() {
        // ceil([2.0, 4.0]) = [2.0, 4.0]
        let input = f64_interval(2.0, 4.0);
        let result = ceil().evaluate_bounds(&[&input]).unwrap();
        assert_eq!(result, f64_interval(2.0, 4.0));
    }

    #[test]
    fn test_evaluate_bounds_f32() {
        // ceil([1.1f32, 2.9f32]) = [2.0f32, 3.0f32]
        let input = f32_interval(1.1, 2.9);
        let result = ceil().evaluate_bounds(&[&input]).unwrap();
        assert_eq!(result, f32_interval(2.0, 3.0));
    }

    #[test]
    fn test_evaluate_bounds_unbounded_returns_unbounded() {
        let input = unbounded_f64();
        let result = ceil().evaluate_bounds(&[&input]).unwrap();
        assert_eq!(result, unbounded_f64());
    }

    #[test]
    fn test_evaluate_bounds_negative() {
        // ceil([-3.7, -1.2]) = [-3.0, -1.0]
        let input = f64_interval(-3.7, -1.2);
        let result = ceil().evaluate_bounds(&[&input]).unwrap();
        assert_eq!(result, f64_interval(-3.0, -1.0));
    }

    // --- propagate_constraints ---

    #[test]
    fn test_propagate_constraints_basic() {
        // ceil(x) ∈ [13.0, 15.0] → x ∈ (12.0, 15.0]
        let output = f64_interval(13.0, 15.0);
        let input = unbounded_f64();
        let result = ceil()
            .propagate_constraints(&output, &[&input])
            .unwrap()
            .unwrap();
        assert_eq!(result[0], f64_interval(12.0, 15.0));
    }

    #[test]
    fn test_propagate_constraints_non_integer_bounds() {
        // ceil(x) ∈ [12.3, 14.7] — non-integer bounds are normalized:
        // lower: ceil(12.3)-1 = 13-1 = 12.0, upper: floor(14.7) = 14.0
        // → x ∈ (12.0, 14.0]
        let output = f64_interval(12.3, 14.7);
        let input = unbounded_f64();
        let result = ceil()
            .propagate_constraints(&output, &[&input])
            .unwrap()
            .unwrap();
        assert_eq!(result[0], f64_interval(12.0, 14.0));
    }

    #[test]
    fn test_propagate_constraints_f32() {
        // Same as basic but with Float32
        let output = f32_interval(5.0, 8.0);
        let input = unbounded_f32();
        let result = ceil()
            .propagate_constraints(&output, &[&input])
            .unwrap()
            .unwrap();
        assert_eq!(result[0], f32_interval(4.0, 8.0));
    }

    #[test]
    fn test_propagate_constraints_unbounded_output_no_change() {
        // No output constraint → input unchanged
        let output = unbounded_f64();
        let input = f64_interval(1.0, 10.0);
        let result = ceil()
            .propagate_constraints(&output, &[&input])
            .unwrap()
            .unwrap();
        assert_eq!(result[0], input);
    }

    #[test]
    fn test_propagate_constraints_nan_output_no_change() {
        // NaN bounds → conservative: input unchanged
        let output = Interval::try_new(
            ScalarValue::Float64(Some(f64::NAN)),
            ScalarValue::Float64(Some(f64::NAN)),
        )
        .unwrap();
        let input = f64_interval(0.0, 100.0);
        let result = ceil()
            .propagate_constraints(&output, &[&input])
            .unwrap()
            .unwrap();
        assert_eq!(result[0], input);
    }

    #[test]
    fn test_propagate_constraints_negative_range() {
        // ceil(x) ∈ [-3.0, -1.0] → x ∈ (-4.0, -1.0]
        let output = f64_interval(-3.0, -1.0);
        let input = unbounded_f64();
        let result = ceil()
            .propagate_constraints(&output, &[&input])
            .unwrap()
            .unwrap();
        assert_eq!(result[0], f64_interval(-4.0, -1.0));
    }
}
