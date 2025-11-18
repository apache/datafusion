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
use std::mem::size_of;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, ArrowPrimitiveType, AsArray, PrimitiveArray};
use arrow::compute::try_binary;
use arrow::datatypes::DataType::{
    Int16, Int32, Int64, Int8, UInt16, UInt32, UInt64, UInt8,
};
use arrow::datatypes::{
    ArrowNativeType, DataType, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};
use datafusion_common::{exec_err, Result};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions::utils::make_scalar_function;

use crate::function::error_utils::{
    invalid_arg_count_exec_err, unsupported_data_type_exec_err,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkBitGet {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkBitGet {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkBitGet {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec!["getbit".to_string()],
        }
    }
}

impl ScalarUDFImpl for SparkBitGet {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return Err(invalid_arg_count_exec_err(
                "bit_get",
                (2, 2),
                arg_types.len(),
            ));
        }
        if !arg_types[0].is_integer() && !arg_types[0].is_null() {
            return Err(unsupported_data_type_exec_err(
                "bit_get",
                "Integer Type",
                &arg_types[0],
            ));
        }
        if !arg_types[1].is_integer() && !arg_types[1].is_null() {
            return Err(unsupported_data_type_exec_err(
                "bit_get",
                "Integer Type",
                &arg_types[1],
            ));
        }
        if arg_types[0].is_null() {
            return Ok(vec![Int8, Int32]);
        }
        Ok(vec![arg_types[0].clone(), Int32])
    }

    fn name(&self) -> &str {
        "bit_get"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Int8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_bit_get, vec![])(&args.args)
    }
}

fn spark_bit_get_inner<T: ArrowPrimitiveType>(
    value: &PrimitiveArray<T>,
    pos: &PrimitiveArray<Int32Type>,
) -> Result<PrimitiveArray<Int8Type>> {
    let bit_length = (size_of::<T::Native>() * 8) as i32;

    let result: PrimitiveArray<Int8Type> = try_binary(value, pos, |value, pos| {
        if pos < 0 || pos >= bit_length {
            return Err(arrow::error::ArrowError::ComputeError(format!(
                "bit_get: position {pos} is out of bounds. Expected pos < {bit_length} and pos >= 0"
            )));
        }
        Ok(((value.to_i64().unwrap() >> pos) & 1) as i8)
    })?;
    Ok(result)
}

pub fn spark_bit_get(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("`bit_get` expects exactly two arguments");
    }

    if args[1].data_type() != &Int32 {
        return exec_err!("`bit_get` expects Int32 as the second argument");
    }

    let pos_arg = args[1].as_primitive::<Int32Type>();

    let ret = match &args[0].data_type() {
        Int64 => {
            let value_arg = args[0].as_primitive::<Int64Type>();
            spark_bit_get_inner(value_arg, pos_arg)
        }
        Int32 => {
            let value_arg = args[0].as_primitive::<Int32Type>();
            spark_bit_get_inner(value_arg, pos_arg)
        }
        Int16 => {
            let value_arg = args[0].as_primitive::<Int16Type>();
            spark_bit_get_inner(value_arg, pos_arg)
        }
        Int8 => {
            let value_arg = args[0].as_primitive::<Int8Type>();
            spark_bit_get_inner(value_arg, pos_arg)
        }
        UInt64 => {
            let value_arg = args[0].as_primitive::<UInt64Type>();
            spark_bit_get_inner(value_arg, pos_arg)
        }
        UInt32 => {
            let value_arg = args[0].as_primitive::<UInt32Type>();
            spark_bit_get_inner(value_arg, pos_arg)
        }
        UInt16 => {
            let value_arg = args[0].as_primitive::<UInt16Type>();
            spark_bit_get_inner(value_arg, pos_arg)
        }
        UInt8 => {
            let value_arg = args[0].as_primitive::<UInt8Type>();
            spark_bit_get_inner(value_arg, pos_arg)
        }
        _ => {
            exec_err!(
                "`bit_get` expects Int64, Int32, Int16, or Int8 as the first argument"
            )
        }
    }?;
    Ok(Arc::new(ret))
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int32Array, Int64Array};

    use super::*;

    #[test]
    fn test_bit_get_basic() {
        // Test bit_get(11, 0) - 11 = 1011 in binary, bit 0 = 1
        let result = spark_bit_get(&[
            Arc::new(Int64Array::from(vec![11])),
            Arc::new(Int32Array::from(vec![0])),
        ])
        .unwrap();

        assert_eq!(result.as_primitive::<Int8Type>().value(0), 1);

        // Test bit_get(11, 2) - 11 = 1011 in binary, bit 2 = 0
        let result = spark_bit_get(&[
            Arc::new(Int64Array::from(vec![11])),
            Arc::new(Int32Array::from(vec![2])),
        ])
        .unwrap();

        assert_eq!(result.as_primitive::<Int8Type>().value(0), 0);

        // Test bit_get(11, 3) - 11 = 1011 in binary, bit 3 = 1
        let result = spark_bit_get(&[
            Arc::new(Int64Array::from(vec![11])),
            Arc::new(Int32Array::from(vec![3])),
        ])
        .unwrap();

        assert_eq!(result.as_primitive::<Int8Type>().value(0), 1);
    }

    #[test]
    fn test_bit_get_edge_cases() {
        // Test with 0
        let result = spark_bit_get(&[
            Arc::new(Int64Array::from(vec![0])),
            Arc::new(Int32Array::from(vec![0])),
        ])
        .unwrap();

        assert_eq!(result.as_primitive::<Int8Type>().value(0), 0);

        let result = spark_bit_get(&[
            Arc::new(Int64Array::from(vec![11])),
            Arc::new(Int32Array::from(vec![-1])),
        ]);
        assert_eq!(
            result.unwrap_err().message().lines().next().unwrap(),
            "Compute error: bit_get: position -1 is out of bounds. Expected pos < 64 and pos >= 0"
        );

        let result = spark_bit_get(&[
            Arc::new(Int64Array::from(vec![11])),
            Arc::new(Int32Array::from(vec![64])),
        ]);

        assert_eq!(
            result.unwrap_err().message().lines().next().unwrap(),
            "Compute error: bit_get: position 64 is out of bounds. Expected pos < 64 and pos >= 0"
        );
    }

    #[test]
    fn test_bit_get_null_inputs() {
        // Test with NULL value
        let result = spark_bit_get(&[
            Arc::new(Int64Array::from(vec![None])),
            Arc::new(Int32Array::from(vec![0])),
        ])
        .unwrap();

        assert_eq!(result.as_primitive::<Int8Type>().value(0), 0);

        // Test with NULL position
        let result = spark_bit_get(&[
            Arc::new(Int64Array::from(vec![11])),
            Arc::new(Int32Array::from(vec![None])),
        ])
        .unwrap();

        assert_eq!(result.as_primitive::<Int8Type>().value(0), 0);
    }

    #[test]
    fn test_bit_get_large_numbers() {
        // Test with larger number
        let result = spark_bit_get(&[
            Arc::new(Int64Array::from(vec![255])), // 11111111 in binary
            Arc::new(Int32Array::from(vec![7])),   // bit 7 = 1
        ])
        .unwrap();

        assert_eq!(result.as_primitive::<Int8Type>().value(0), 1);

        let result = spark_bit_get(&[
            Arc::new(Int64Array::from(vec![255])), // 11111111 in binary
            Arc::new(Int32Array::from(vec![8])),   // bit 8 = 0
        ])
        .unwrap();

        assert_eq!(result.as_primitive::<Int8Type>().value(0), 0);
    }
}
