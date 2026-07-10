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

use arrow::array::builder::NullBufferBuilder;
use arrow::array::{Array, ArrayRef, AsArray, PrimitiveArray};
use arrow::datatypes::DataType::{Float16, Float32, Float64};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Float16Type, Float32Type, Float64Type,
};
use datafusion_common::types::{NativeType, logical_float64};
use datafusion_common::{Result, ScalarValue, exec_err, utils::take_function_args};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;
use num_traits::Float;

#[user_doc(
    doc_section(label = "Math Functions"),
    description = r#"Returns the first argument if it's not _NaN_.
Returns the second argument otherwise."#,
    syntax_example = "nanvl(expression_x, expression_y)",
    sql_example = r#"```sql
> SELECT nanvl(0, 5);
+------------+
| nanvl(0,5) |
+------------+
| 0          |
+------------+
```"#,
    argument(
        name = "expression_x",
        description = "Numeric expression to return if it's not _NaN_. Can be a constant, column, or function, and any combination of arithmetic operators."
    ),
    argument(
        name = "expression_y",
        description = "Numeric expression to return if the first expression is _NaN_. Can be a constant, column, or function, and any combination of arithmetic operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct NanvlFunc {
    signature: Signature,
}

impl Default for NanvlFunc {
    fn default() -> Self {
        NanvlFunc::new()
    }
}

impl NanvlFunc {
    pub fn new() -> Self {
        // Non-float numerics (integers, decimals) and NULL coerce to Float64,
        // which represents as many inputs as possible before rounding.
        let non_float = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_float64()),
            vec![TypeSignatureClass::Integer, TypeSignatureClass::Decimal],
            NativeType::Float64,
        );
        // Any numeric (including floats) coerces to Float64.
        let to_float64 = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_float64()),
            vec![TypeSignatureClass::Numeric],
            NativeType::Float64,
        );
        Self {
            signature: Signature::one_of(
                vec![
                    // If either argument is a non-float numeric (or NULL), both
                    // are computed in Float64. Two arms cover either argument
                    // order.
                    TypeSignature::Coercible(vec![non_float.clone(), to_float64.clone()]),
                    TypeSignature::Coercible(vec![to_float64, non_float]),
                    // Otherwise both arguments are floats; preserve their
                    // (widest common) precision rather than widening to Float64.
                    TypeSignature::Exact(vec![Float16, Float16]),
                    TypeSignature::Exact(vec![Float32, Float32]),
                    TypeSignature::Exact(vec![Float64, Float64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for NanvlFunc {
    fn name(&self) -> &str {
        "nanvl"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match (&arg_types[0], &arg_types[1]) {
            (Float16, Float16) => Ok(Float16),
            (Float32, Float32) => Ok(Float32),
            _ => Ok(Float64),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [x, y] = take_function_args(self.name(), args.args)?;

        match (x, y) {
            // Scalar x: return y if x is NaN, otherwise x (which may be NULL).
            (ColumnarValue::Scalar(ref x), y) if scalar_is_nan(x) => Ok(y),
            (x @ ColumnarValue::Scalar(_), _) => Ok(x),
            // At least one argument is an array: evaluate element-wise.
            (x, y) => {
                let args = ColumnarValue::values_to_arrays(&[x, y])?;
                Ok(ColumnarValue::Array(nanvl(&args)?))
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn scalar_is_nan(scalar: &ScalarValue) -> bool {
    match scalar {
        ScalarValue::Float16(Some(v)) => v.is_nan(),
        ScalarValue::Float32(Some(v)) => v.is_nan(),
        ScalarValue::Float64(Some(v)) => v.is_nan(),
        _ => false,
    }
}

/// Nanvl SQL function
///
/// - x is NaN -> output is y (which may itself be NULL)
/// - otherwise -> output is x (which may itself be NULL)
fn nanvl(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        Float64 => Ok(Arc::new(nanvl_impl::<Float64Type>(
            args[0].as_primitive(),
            args[1].as_primitive(),
        ))),
        Float32 => Ok(Arc::new(nanvl_impl::<Float32Type>(
            args[0].as_primitive(),
            args[1].as_primitive(),
        ))),
        Float16 => Ok(Arc::new(nanvl_impl::<Float16Type>(
            args[0].as_primitive(),
            args[1].as_primitive(),
        ))),
        other => exec_err!("Unsupported data type {other:?} for function nanvl"),
    }
}

/// Element-wise `nanvl`: selects `y[i]` where `x[i]` is `NaN`, otherwise `x[i]`
/// (a null `x` selects `x`, i.e. propagates null).
///
/// This produces output identical to collecting an iterator of `Option`s but
/// splits out a null-free fast path that iterates the raw value slices,
/// skipping per-element validity checks and `Option` handling. The null-aware
/// path builds its null buffer lazily via [`NullBufferBuilder`] exactly as the
/// `FromIterator` implementation does, so the result is byte-for-byte the same.
fn nanvl_impl<T>(x: &PrimitiveArray<T>, y: &PrimitiveArray<T>) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    T::Native: Float,
{
    let xv = x.values();
    let yv = y.values();

    match (x.nulls(), y.nulls()) {
        // No nulls in either input means no nulls in the output, so we can
        // iterate values directly and avoid the null bookkeeping entirely.
        (None, None) => {
            let values: Vec<T::Native> = xv
                .iter()
                .zip(yv.iter())
                .map(
                    |(&x_value, &y_value)| {
                        if x_value.is_nan() { y_value } else { x_value }
                    },
                )
                .collect();
            PrimitiveArray::<T>::new(values.into(), None)
        }
        _ => {
            let len = x.len();
            let mut nulls = NullBufferBuilder::new(len);
            let mut values = Vec::with_capacity(len);
            for i in 0..len {
                // `y` is only consulted when `x` is a (non-null) NaN, matching
                // the original short-circuiting match.
                if x.is_valid(i) {
                    let x_value = xv[i];
                    if x_value.is_nan() {
                        if y.is_valid(i) {
                            values.push(yv[i]);
                            nulls.append_non_null();
                        } else {
                            values.push(T::Native::default());
                            nulls.append_null();
                        }
                    } else {
                        values.push(x_value);
                        nulls.append_non_null();
                    }
                } else {
                    values.push(T::Native::default());
                    nulls.append_null();
                }
            }
            PrimitiveArray::<T>::new(values.into(), nulls.finish())
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::math::nanvl::nanvl;

    use arrow::array::{Array, ArrayRef, Float32Array, Float64Array};
    use datafusion_common::cast::{as_float32_array, as_float64_array};

    #[test]
    fn test_nanvl_f64() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![1.0, f64::NAN, 3.0, f64::NAN])), // x
            Arc::new(Float64Array::from(vec![5.0, 6.0, f64::NAN, f64::NAN])), // y
        ];

        let result = nanvl(&args).expect("failed to initialize function nanvl");
        let floats =
            as_float64_array(&result).expect("failed to initialize function nanvl");

        assert_eq!(floats.len(), 4);
        assert_eq!(floats.value(0), 1.0);
        assert_eq!(floats.value(1), 6.0);
        assert_eq!(floats.value(2), 3.0);
        assert!(floats.value(3).is_nan());
    }

    #[test]
    fn test_nanvl_f32() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from(vec![1.0, f32::NAN, 3.0, f32::NAN])), // x
            Arc::new(Float32Array::from(vec![5.0, 6.0, f32::NAN, f32::NAN])), // y
        ];

        let result = nanvl(&args).expect("failed to initialize function nanvl");
        let floats =
            as_float32_array(&result).expect("failed to initialize function nanvl");

        assert_eq!(floats.len(), 4);
        assert_eq!(floats.value(0), 1.0);
        assert_eq!(floats.value(1), 6.0);
        assert_eq!(floats.value(2), 3.0);
        assert!(floats.value(3).is_nan());
    }

    #[test]
    fn test_nanvl_f64_with_nulls() {
        // Covers the null-aware path and null propagation:
        // - x null            -> null (regardless of y)
        // - x NaN, y non-null -> y
        // - x NaN, y null     -> null
        // - x non-NaN         -> x
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![
                None,
                Some(f64::NAN),
                Some(f64::NAN),
                Some(2.5),
            ])), // x
            Arc::new(Float64Array::from(vec![
                Some(9.0),
                Some(6.0),
                None,
                Some(7.0),
            ])), // y
        ];

        let result = nanvl(&args).expect("failed to initialize function nanvl");
        let floats =
            as_float64_array(&result).expect("failed to initialize function nanvl");

        assert_eq!(floats.len(), 4);
        assert!(floats.is_null(0));
        assert_eq!(floats.value(1), 6.0);
        assert!(floats.is_null(2));
        assert_eq!(floats.value(3), 2.5);
    }
}
