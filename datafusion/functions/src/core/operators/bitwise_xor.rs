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

//! Operator bitwise xor: `#` `^`

use std::any::Any;

use crate::core::operators::common::to_result_type_array;
use arrow::array::*;
use arrow::compute::kernels::bitwise::{bitwise_xor, bitwise_xor_scalar};
use arrow::datatypes::DataType;
use datafusion_common::plan_err;
use datafusion_common::{Result, ScalarValue, utils::take_function_args};
use datafusion_expr::{
    ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

create_left_integral_dyn_scalar_kernel!(bitwise_xor_dyn_scalar, bitwise_xor_scalar);
create_left_integral_dyn_kernel!(bitwise_xor_dyn, bitwise_xor);

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct BitwiseXorFunc {
    signature: Signature,
}

impl BitwiseXorFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                2,
                vec![
                    DataType::Int8,
                    DataType::Int16,
                    DataType::Int32,
                    DataType::Int64,
                    DataType::UInt8,
                    DataType::UInt16,
                    DataType::UInt32,
                    DataType::UInt64,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for BitwiseXorFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for BitwiseXorFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "bitwise_xor"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [lhs, rhs] = take_function_args(self.name(), &args.args)?;

        if let (ColumnarValue::Array(array), ColumnarValue::Scalar(scalar)) = (&lhs, &rhs)
            && let Some(res) = bitwise_xor_dyn_scalar(array, scalar.clone())
        {
            return res
                .and_then(|a| {
                    to_result_type_array(&Operator::BitwiseXor, a, &lhs.data_type())
                })
                .map(ColumnarValue::Array);
        }

        let left = lhs.to_array(args.number_rows)?;
        let right = rhs.to_array(args.number_rows)?;
        bitwise_xor_dyn(left, right).map(ColumnarValue::Array)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;
    use datafusion_common::{Result, ScalarValue, config::ConfigOptions};

    #[test]
    fn bitwise_array_test() -> Result<()> {
        let left = Arc::new(Int32Array::from(vec![Some(12), None, Some(11)])) as ArrayRef;
        let right =
            Arc::new(Int32Array::from(vec![Some(1), Some(3), Some(7)])) as ArrayRef;
        let result = bitwise_xor_dyn(Arc::clone(&left), Arc::clone(&right))?;
        let expected = Int32Array::from(vec![Some(13), None, Some(12)]);
        assert_eq!(result.as_ref(), &expected);
        
        let left = Arc::new(UInt32Array::from(vec![Some(12), None, Some(11)])) as ArrayRef;
        let right =
            Arc::new(UInt32Array::from(vec![Some(1), Some(3), Some(7)])) as ArrayRef;
        let result = bitwise_xor_dyn(Arc::clone(&left), Arc::clone(&right))?;
        let expected = UInt32Array::from(vec![Some(13), None, Some(12)]);
        assert_eq!(result.as_ref(), &expected);
        Ok(())
    }

    #[test]
    fn bitwise_scalar_test() -> Result<()> {
        let left = Arc::new(Int32Array::from(vec![Some(12), None, Some(11)])) as ArrayRef;
        let right = ScalarValue::from(3i32);
        let result = bitwise_xor_dyn_scalar(&left, right).unwrap()?;
        let expected = Int32Array::from(vec![Some(15), None, Some(8)]);
        assert_eq!(result.as_ref(), &expected);
        
        let left = Arc::new(UInt32Array::from(vec![Some(12), None, Some(11)])) as ArrayRef;
        let right = ScalarValue::from(3u32);
        let result = bitwise_xor_dyn_scalar(&left, right).unwrap()?;
        let expected = UInt32Array::from(vec![Some(15), None, Some(8)]);
        assert_eq!(result.as_ref(), &expected);
        
        Ok(())
    }

    #[test]
    fn test_bitwise_xor_scalar_null() -> Result<()> {
        // Test with null scalar
        let array = UInt32Array::from(vec![1, 2, 3]);
        let scalar = ScalarValue::UInt32(None);
        let result = bitwise_xor_dyn_scalar(&array, scalar).unwrap()?;
        assert_eq!(result.len(), 3);
        assert!(result.is_null(0));
        assert!(result.is_null(1));
        assert!(result.is_null(2));
        Ok(())
    }

    #[test]
    fn test_bitwise_xor_func_invoke() -> Result<()> {
        // Test the ScalarUDFImpl invoke_with_args method
        let func = BitwiseXorFunc::new();
        let left = ColumnarValue::Array(Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef);
        let right = ColumnarValue::Array(Arc::new(Int32Array::from(vec![30, 20, 10])) as ArrayRef);
        let args = ScalarFunctionArgs {
            args: vec![left, right],
            arg_fields: vec![Field::new("a", DataType::Int32, false).into(), Field::new("b", DataType::Int32, false).into()],
            number_rows: 3,
            return_field: Field::new("f", DataType::Int32, false).into(),
            config_options: Arc::new(ConfigOptions::new()),
        };
        let result = func.invoke_with_args(args)?;
        match result {
            ColumnarValue::Array(arr) => {
                let arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(arr.values(), &[10 ^ 30, 20 ^ 20, 30 ^ 10]);
            }
            _ => panic!("Expected array result"),
        }
        Ok(())
    }
}
