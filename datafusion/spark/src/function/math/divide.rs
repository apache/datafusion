// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::array::{ArrayRef, Scalar, new_null_array};
use arrow::compute::kernels::boolean::{and, is_not_null};
use arrow::compute::kernels::numeric::div;
use arrow::compute::kernels::zip::zip;
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use datafusion_common::{Result, assert_eq_or_internal_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

/// Computes `dividend / divisor` with Spark's semantics for a non-decimal pair.
///
/// A zero divisor yields NULL, with `-0.0` counting as zero, unless ANSI mode is
/// enabled, where it raises `DivideByZero`. The kernel only sees `Float64`
/// arguments because [`SparkDivide`] coerces them.
fn try_div(
    left: &ArrayRef,
    right: &ArrayRef,
    enable_ansi_mode: bool,
) -> Result<ArrayRef> {
    let divisor_is_zero = super::modulus::is_zero(right)?;

    if enable_ansi_mode {
        // Spark's division is null intolerant, so a row whose dividend is NULL
        // evaluates to NULL and never raises, even when the divisor is zero.
        let raises = and(&divisor_is_zero, &is_not_null(left.as_ref())?)?;
        if raises.iter().flatten().any(|raises| raises) {
            return Err(ArrowError::DivideByZero.into());
        }
    }

    // Null out zero divisors so the division kernel never sees one, which would
    // produce an infinity instead of the NULL Spark returns.
    let null = Scalar::new(new_null_array(right.data_type(), 1));
    let safe_right = zip(&divisor_is_zero, &null, right)?;
    Ok(div(left, &safe_right)?)
}

/// Spark compatible `divide` operator: casts both operands to `Float64` and
/// returns NULL for a zero divisor in legacy mode, or raises in ANSI mode.
pub fn spark_divide(
    args: &[ColumnarValue],
    enable_ansi_mode: bool,
) -> Result<ColumnarValue> {
    assert_eq_or_internal_err!(args.len(), 2, "divide expects exactly two arguments");
    let args = ColumnarValue::values_to_arrays(args)?;
    let result = try_div(&args[0], &args[1], enable_ansi_mode)?;
    Ok(ColumnarValue::Array(result))
}

/// Implements Spark's `/` on operands that are not `DECIMAL`: Spark casts both
/// sides to `DOUBLE`, and a zero divisor yields NULL unless ANSI mode is on.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDivide {
    signature: Signature,
}

impl Default for SparkDivide {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkDivide {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkDivide {
    fn name(&self) -> &str {
        "spark_divide"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        assert_eq_or_internal_err!(
            arg_types.len(),
            2,
            "divide expects exactly two arguments"
        );
        Ok(vec![DataType::Float64; 2])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_divide(&args.args, args.config_options.execution.enable_ansi_mode)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::*;
    use arrow::array::Float64Array;

    fn run(left: ArrayRef, right: ArrayRef, ansi: bool) -> Result<ArrayRef> {
        match spark_divide(
            &[ColumnarValue::Array(left), ColumnarValue::Array(right)],
            ansi,
        )? {
            ColumnarValue::Array(array) => Ok(array),
            other => panic!("expected an array, got {other:?}"),
        }
    }

    #[test]
    fn divides_decimal_digits_as_double() {
        let left = Arc::new(Float64Array::from(vec![1.0, 10.0]));
        let right = Arc::new(Float64Array::from(vec![2.0, 4.0]));
        let result = run(left, right, false).unwrap();
        let expected: ArrayRef = Arc::new(Float64Array::from(vec![0.5, 2.5]));
        assert_eq!(&result, &expected);
    }

    #[test]
    fn zero_divisor_is_null_in_legacy_mode() {
        let left = Arc::new(Float64Array::from(vec![Some(1.0), Some(2.0), None]));
        let right = Arc::new(Float64Array::from(vec![Some(0.0), Some(-0.0), Some(0.0)]));
        let result = run(left, right, false).unwrap();
        let expected: ArrayRef = Arc::new(Float64Array::from(vec![None, None, None]));
        assert_eq!(&result, &expected);
    }

    #[test]
    fn zero_divisor_raises_in_ansi_mode() {
        let left = Arc::new(Float64Array::from(vec![1.0, 2.0]));
        let right = Arc::new(Float64Array::from(vec![0.0, 1.0]));
        let err = run(left, right, true).unwrap_err();
        assert!(
            err.to_string().contains("Divide by zero"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn null_dividend_with_zero_divisor_does_not_raise_in_ansi_mode() {
        let left = Arc::new(Float64Array::from(vec![None]));
        let right = Arc::new(Float64Array::from(vec![Some(0.0)]));
        let result = run(left, right, true).unwrap();
        let expected: ArrayRef = Arc::new(Float64Array::from(vec![None]));
        assert_eq!(&result, &expected);
    }
}
