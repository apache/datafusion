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

use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::compute::cast;
use arrow::datatypes::DataType::{
    Float32, Float64, Int16, Int32, Int64, Int8, UInt16, UInt32, UInt64, UInt8,
};
use arrow::datatypes::{DataType, Float32Type, Float64Type};
use datafusion_common::{assert_eq_or_internal_err, exec_err, Result};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions::utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkRint {
    signature: Signature,
}

impl Default for SparkRint {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkRint {
    pub fn new() -> Self {
        Self {
            signature: Signature::numeric(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkRint {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "rint"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_rint, vec![])(&args.args)
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        // round preserves the order of the first argument
        if input.len() == 1 {
            let value = &input[0];
            Ok(value.sort_properties)
        } else {
            Ok(SortProperties::default())
        }
    }
}

pub fn spark_rint(args: &[ArrayRef]) -> Result<ArrayRef> {
    assert_eq_or_internal_err!(args.len(), 1, "`rint` expects exactly one argument");

    let array: &dyn Array = args[0].as_ref();
    match args[0].data_type() {
        Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => {
            Ok(cast(array, &Float64)?)
        }
        Float64 => {
            let array = array
                .as_primitive::<Float64Type>()
                .unary::<_, Float64Type>(|value: f64| value.round_ties_even());
            Ok(Arc::new(array))
        }
        Float32 => {
            let array = array
                .as_primitive::<Float32Type>()
                .unary::<_, Float64Type>(|value: f32| value.round_ties_even() as f64);
            Ok(Arc::new(array))
        }
        _ => {
            exec_err!(
                "rint expects a numeric argument, got {}",
                args[0].data_type()
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Float64Array;

    #[test]
    fn test_rint_positive_decimals() {
        // Test positive decimal rounding
        let result = spark_rint(&[Arc::new(Float64Array::from(vec![12.3456]))]).unwrap();
        assert_eq!(result.as_ref(), &Float64Array::from(vec![12.0]));

        // Test rounding to nearest even (banker's rounding)
        let result = spark_rint(&[Arc::new(Float64Array::from(vec![2.5]))]).unwrap();
        assert_eq!(result.as_ref(), &Float64Array::from(vec![2.0]));

        let result = spark_rint(&[Arc::new(Float64Array::from(vec![3.5]))]).unwrap();
        assert_eq!(result.as_ref(), &Float64Array::from(vec![4.0]));
    }

    #[test]
    fn test_rint_negative_decimals() {
        // Test negative decimal rounding
        let result = spark_rint(&[Arc::new(Float64Array::from(vec![-12.3456]))]).unwrap();
        assert_eq!(result.as_ref(), &Float64Array::from(vec![-12.0]));

        // Test negative rounding to nearest even
        let result = spark_rint(&[Arc::new(Float64Array::from(vec![-2.5]))]).unwrap();
        assert_eq!(result.as_ref(), &Float64Array::from(vec![-2.0]));
    }

    #[test]
    fn test_rint_integers() {
        // Test integer input (should return as float64)
        let result = spark_rint(&[Arc::new(Float64Array::from(vec![42.0]))]).unwrap();
        assert_eq!(result.as_ref(), &Float64Array::from(vec![42.0]));
    }

    #[test]
    fn test_rint_null() {
        let result = spark_rint(&[Arc::new(Float64Array::from(vec![None]))]).unwrap();
        assert_eq!(result.as_ref(), &Float64Array::from(vec![None]));
    }

    #[test]
    fn test_rint_zero() {
        // Test zero
        let result = spark_rint(&[Arc::new(Float64Array::from(vec![0.0]))]).unwrap();
        assert_eq!(result.as_ref(), &Float64Array::from(vec![0.0]));
    }
}
