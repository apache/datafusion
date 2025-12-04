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

use arrow::compute::kernels::numeric::add;
use arrow::compute::kernels::{cmp::lt, numeric::rem, zip::zip};
use arrow::datatypes::DataType;
use datafusion_common::{assert_eq_or_internal_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;

/// Spark-compatible `mod` function
/// This function directly uses Arrow's arithmetic_op function for modulo operations
pub fn spark_mod(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    assert_eq_or_internal_err!(args.len(), 2, "mod expects exactly two arguments");
    let args = ColumnarValue::values_to_arrays(args)?;
    let result = rem(&args[0], &args[1])?;
    Ok(ColumnarValue::Array(result))
}

/// Spark-compatible `pmod` function
/// This function directly uses Arrow's arithmetic_op function for modulo operations
pub fn spark_pmod(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    assert_eq_or_internal_err!(args.len(), 2, "pmod expects exactly two arguments");
    let args = ColumnarValue::values_to_arrays(args)?;
    let left = &args[0];
    let right = &args[1];
    let zero = ScalarValue::new_zero(left.data_type())?.to_array_of_size(left.len())?;
    let result = rem(left, right)?;
    let neg = lt(&result, &zero)?;
    let plus = zip(&neg, right, &zero)?;
    let result = add(&plus, &result)?;
    let result = rem(&result, right)?;
    Ok(ColumnarValue::Array(result))
}

/// SparkMod implements the Spark-compatible modulo function
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMod {
    signature: Signature,
}

impl Default for SparkMod {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMod {
    pub fn new() -> Self {
        Self {
            signature: Signature::numeric(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkMod {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "mod"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        assert_eq_or_internal_err!(
            arg_types.len(),
            2,
            "mod expects exactly two arguments"
        );

        // Return the same type as the first argument for simplicity
        // Arrow's rem function handles type promotion internally
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_mod(&args.args)
    }
}

/// SparkMod implements the Spark-compatible modulo function
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkPmod {
    signature: Signature,
}

impl Default for SparkPmod {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkPmod {
    pub fn new() -> Self {
        Self {
            signature: Signature::numeric(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkPmod {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "pmod"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        assert_eq_or_internal_err!(
            arg_types.len(),
            2,
            "pmod expects exactly two arguments"
        );

        // Return the same type as the first argument for simplicity
        // Arrow's rem function handles type promotion internally
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_pmod(&args.args)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::*;
    use arrow::array::*;
    use datafusion_common::ScalarValue;

    #[test]
    fn test_mod_int32() {
        let left = Int32Array::from(vec![Some(10), Some(7), Some(15), None]);
        let right = Int32Array::from(vec![Some(3), Some(2), Some(4), Some(5)]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_mod(&[left_value, right_value]).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_int32 =
                result_array.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(result_int32.value(0), 1); // 10 % 3 = 1
            assert_eq!(result_int32.value(1), 1); // 7 % 2 = 1
            assert_eq!(result_int32.value(2), 3); // 15 % 4 = 3
            assert!(result_int32.is_null(3)); // None % 5 = None
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_mod_int64() {
        let left = Int64Array::from(vec![Some(100), Some(50), Some(200)]);
        let right = Int64Array::from(vec![Some(30), Some(25), Some(60)]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_mod(&[left_value, right_value]).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_int64 =
                result_array.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(result_int64.value(0), 10); // 100 % 30 = 10
            assert_eq!(result_int64.value(1), 0); // 50 % 25 = 0
            assert_eq!(result_int64.value(2), 20); // 200 % 60 = 20
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_mod_float64() {
        let left = Float64Array::from(vec![
            Some(10.5),
            Some(7.2),
            Some(15.8),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(5.0),
            Some(5.0),
            Some(f64::NAN),
            Some(f64::INFINITY),
        ]);
        let right = Float64Array::from(vec![
            Some(3.0),
            Some(2.5),
            Some(4.2),
            Some(2.0),
            Some(2.0),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::INFINITY),
            Some(f64::NAN),
        ]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_mod(&[left_value, right_value]).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_float64 = result_array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            // Regular cases
            assert!((result_float64.value(0) - 1.5).abs() < f64::EPSILON); // 10.5 % 3.0 = 1.5
            assert!((result_float64.value(1) - 2.2).abs() < f64::EPSILON); // 7.2 % 2.5 = 2.2
            assert!((result_float64.value(2) - 3.2).abs() < f64::EPSILON); // 15.8 % 4.2 = 3.2
                                                                           // nan % 2.0 = nan
            assert!(result_float64.value(3).is_nan());
            // inf % 2.0 = nan (IEEE 754)
            assert!(result_float64.value(4).is_nan());
            // 5.0 % nan = nan
            assert!(result_float64.value(5).is_nan());
            // 5.0 % inf = 5.0
            assert!((result_float64.value(6) - 5.0).abs() < f64::EPSILON);
            // nan % inf = nan
            assert!(result_float64.value(7).is_nan());
            // inf % nan = nan
            assert!(result_float64.value(8).is_nan());
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_mod_float32() {
        let left = Float32Array::from(vec![
            Some(10.5),
            Some(7.2),
            Some(15.8),
            Some(f32::NAN),
            Some(f32::INFINITY),
            Some(5.0),
            Some(5.0),
            Some(f32::NAN),
            Some(f32::INFINITY),
        ]);
        let right = Float32Array::from(vec![
            Some(3.0),
            Some(2.5),
            Some(4.2),
            Some(2.0),
            Some(2.0),
            Some(f32::NAN),
            Some(f32::INFINITY),
            Some(f32::INFINITY),
            Some(f32::NAN),
        ]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_mod(&[left_value, right_value]).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_float32 = result_array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap();
            // Regular cases
            assert!((result_float32.value(0) - 1.5).abs() < f32::EPSILON); // 10.5 % 3.0 = 1.5
            assert!((result_float32.value(1) - 2.2).abs() < f32::EPSILON * 3.0); // 7.2 % 2.5 = 2.2
            assert!((result_float32.value(2) - 3.2).abs() < f32::EPSILON * 10.0); // 15.8 % 4.2 = 3.2
                                                                                  // nan % 2.0 = nan
            assert!(result_float32.value(3).is_nan());
            // inf % 2.0 = nan (IEEE 754)
            assert!(result_float32.value(4).is_nan());
            // 5.0 % nan = nan
            assert!(result_float32.value(5).is_nan());
            // 5.0 % inf = 5.0
            assert!((result_float32.value(6) - 5.0).abs() < f32::EPSILON);
            // nan % inf = nan
            assert!(result_float32.value(7).is_nan());
            // inf % nan = nan
            assert!(result_float32.value(8).is_nan());
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_mod_scalar() {
        let left = Int32Array::from(vec![Some(10), Some(7), Some(15)]);
        let right_value = ColumnarValue::Scalar(ScalarValue::Int32(Some(3)));

        let left_value = ColumnarValue::Array(Arc::new(left));

        let result = spark_mod(&[left_value, right_value]).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_int32 =
                result_array.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(result_int32.value(0), 1); // 10 % 3 = 1
            assert_eq!(result_int32.value(1), 1); // 7 % 3 = 1
            assert_eq!(result_int32.value(2), 0); // 15 % 3 = 0
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_mod_wrong_arg_count() {
        let left = Int32Array::from(vec![Some(10)]);
        let left_value = ColumnarValue::Array(Arc::new(left));

        let result = spark_mod(&[left_value]);
        assert!(result.is_err());
    }

    #[test]
    fn test_mod_zero_division() {
        let left = Int32Array::from(vec![Some(10), Some(7), Some(15)]);
        let right = Int32Array::from(vec![Some(0), Some(2), Some(4)]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_mod(&[left_value, right_value]);
        assert!(result.is_err()); // Division by zero should error
    }

    // PMOD tests
    #[test]
    fn test_pmod_int32() {
        let left = Int32Array::from(vec![Some(10), Some(-7), Some(15), Some(-15), None]);
        let right = Int32Array::from(vec![Some(3), Some(3), Some(4), Some(4), Some(5)]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_pmod(&[left_value, right_value]).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_int32 =
                result_array.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(result_int32.value(0), 1); // 10 pmod 3 = 1
            assert_eq!(result_int32.value(1), 2); // -7 pmod 3 = 2 (positive remainder)
            assert_eq!(result_int32.value(2), 3); // 15 pmod 4 = 3
            assert_eq!(result_int32.value(3), 1); // -15 pmod 4 = 1 (positive remainder)
            assert!(result_int32.is_null(4)); // None pmod 5 = None
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_pmod_int64() {
        let left = Int64Array::from(vec![Some(100), Some(-50), Some(200), Some(-200)]);
        let right = Int64Array::from(vec![Some(30), Some(30), Some(60), Some(60)]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_pmod(&[left_value, right_value]).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_int64 =
                result_array.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(result_int64.value(0), 10); // 100 pmod 30 = 10
            assert_eq!(result_int64.value(1), 10); // -50 pmod 30 = 10 (positive remainder)
            assert_eq!(result_int64.value(2), 20); // 200 pmod 60 = 20
            assert_eq!(result_int64.value(3), 40); // -200 pmod 60 = 40 (positive remainder)
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_pmod_float64() {
        let left = Float64Array::from(vec![
            Some(10.5),
            Some(-7.2),
            Some(15.8),
            Some(-15.8),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(5.0),
            Some(-5.0),
        ]);
        let right = Float64Array::from(vec![
            Some(3.0),
            Some(3.0),
            Some(4.2),
            Some(4.2),
            Some(2.0),
            Some(2.0),
            Some(f64::INFINITY),
            Some(f64::INFINITY),
        ]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_pmod(&[left_value, right_value]).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_float64 = result_array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            // Regular cases
            assert!((result_float64.value(0) - 1.5).abs() < f64::EPSILON); // 10.5 pmod 3.0 = 1.5
            assert!((result_float64.value(1) - 1.8).abs() < f64::EPSILON * 3.0); // -7.2 pmod 3.0 = 1.8 (positive)
            assert!((result_float64.value(2) - 3.2).abs() < f64::EPSILON * 3.0); // 15.8 pmod 4.2 = 3.2
            assert!((result_float64.value(3) - 1.0).abs() < f64::EPSILON * 3.0); // -15.8 pmod 4.2 = 1.0 (positive)
                                                                                 // nan pmod 2.0 = nan
            assert!(result_float64.value(4).is_nan());
            // inf pmod 2.0 = nan (IEEE 754)
            assert!(result_float64.value(5).is_nan());
            // 5.0 pmod inf = 5.0
            assert!((result_float64.value(6) - 5.0).abs() < f64::EPSILON);
            // -5.0 pmod inf = NaN
            assert!(result_float64.value(7).is_nan());
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_pmod_float32() {
        let left = Float32Array::from(vec![
            Some(10.5),
            Some(-7.2),
            Some(15.8),
            Some(-15.8),
            Some(f32::NAN),
            Some(f32::INFINITY),
            Some(5.0),
            Some(-5.0),
        ]);
        let right = Float32Array::from(vec![
            Some(3.0),
            Some(3.0),
            Some(4.2),
            Some(4.2),
            Some(2.0),
            Some(2.0),
            Some(f32::INFINITY),
            Some(f32::INFINITY),
        ]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_pmod(&[left_value, right_value]).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_float32 = result_array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap();
            // Regular cases
            assert!((result_float32.value(0) - 1.5).abs() < f32::EPSILON); // 10.5 pmod 3.0 = 1.5
            assert!((result_float32.value(1) - 1.8).abs() < f32::EPSILON * 3.0); // -7.2 pmod 3.0 = 1.8 (positive)
            assert!((result_float32.value(2) - 3.2).abs() < f32::EPSILON * 10.0); // 15.8 pmod 4.2 = 3.2
            assert!((result_float32.value(3) - 1.0).abs() < f32::EPSILON * 10.0); // -15.8 pmod 4.2 = 1.0 (positive)
                                                                                  // nan pmod 2.0 = nan
            assert!(result_float32.value(4).is_nan());
            // inf pmod 2.0 = nan (IEEE 754)
            assert!(result_float32.value(5).is_nan());
            // 5.0 pmod inf = 5.0
            assert!((result_float32.value(6) - 5.0).abs() < f32::EPSILON * 10.0);
            // -5.0 pmod inf = NaN
            assert!(result_float32.value(7).is_nan());
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_pmod_scalar() {
        let left = Int32Array::from(vec![Some(10), Some(-7), Some(15), Some(-15)]);
        let right_value = ColumnarValue::Scalar(ScalarValue::Int32(Some(3)));

        let left_value = ColumnarValue::Array(Arc::new(left));

        let result = spark_pmod(&[left_value, right_value]).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_int32 =
                result_array.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(result_int32.value(0), 1); // 10 pmod 3 = 1
            assert_eq!(result_int32.value(1), 2); // -7 pmod 3 = 2 (positive remainder)
            assert_eq!(result_int32.value(2), 0); // 15 pmod 3 = 0
            assert_eq!(result_int32.value(3), 0); // -15 pmod 3 = 0 (positive remainder)
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_pmod_wrong_arg_count() {
        let left = Int32Array::from(vec![Some(10)]);
        let left_value = ColumnarValue::Array(Arc::new(left));

        let result = spark_pmod(&[left_value]);
        assert!(result.is_err());
    }

    #[test]
    fn test_pmod_zero_division() {
        let left = Int32Array::from(vec![Some(10), Some(-7), Some(15)]);
        let right = Int32Array::from(vec![Some(0), Some(0), Some(4)]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_pmod(&[left_value, right_value]);
        assert!(result.is_err()); // Division by zero should error
    }

    #[test]
    fn test_pmod_negative_divisor() {
        // PMOD with negative divisor should still work like regular mod
        let left = Int32Array::from(vec![Some(10), Some(-7), Some(15)]);
        let right = Int32Array::from(vec![Some(-3), Some(-3), Some(-4)]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_pmod(&[left_value, right_value]).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_int32 =
                result_array.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(result_int32.value(0), 1); // 10 pmod -3 = 1
            assert_eq!(result_int32.value(1), -1); // -7 pmod -3 = -1
            assert_eq!(result_int32.value(2), 3); // 15 pmod -4 = 3
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_pmod_edge_cases() {
        // Test edge cases for PMOD
        let left = Int32Array::from(vec![
            Some(0),  // 0 pmod 5 = 0
            Some(-1), // -1 pmod 5 = 4
            Some(1),  // 1 pmod 5 = 1
            Some(-5), // -5 pmod 5 = 0
            Some(5),  // 5 pmod 5 = 0
            Some(-6), // -6 pmod 5 = 4
            Some(6),  // 6 pmod 5 = 1
        ]);
        let right = Int32Array::from(vec![
            Some(5),
            Some(5),
            Some(5),
            Some(5),
            Some(5),
            Some(5),
            Some(5),
        ]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_pmod(&[left_value, right_value]).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_int32 =
                result_array.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(result_int32.value(0), 0); // 0 pmod 5 = 0
            assert_eq!(result_int32.value(1), 4); // -1 pmod 5 = 4
            assert_eq!(result_int32.value(2), 1); // 1 pmod 5 = 1
            assert_eq!(result_int32.value(3), 0); // -5 pmod 5 = 0
            assert_eq!(result_int32.value(4), 0); // 5 pmod 5 = 0
            assert_eq!(result_int32.value(5), 4); // -6 pmod 5 = 4
            assert_eq!(result_int32.value(6), 1); // 6 pmod 5 = 1
        } else {
            panic!("Expected array result");
        }
    }
}
