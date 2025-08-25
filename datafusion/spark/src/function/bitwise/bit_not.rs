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

use arrow::array::{ArrayRef, AsArray, Int16Array, Int32Array, Int64Array, Int8Array};
use arrow::datatypes::{DataType, Int16Type, Int32Type, Int64Type, Int8Type};
use datafusion_common::{exec_err, plan_err, Result};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions::utils::make_scalar_function;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct SparkBitNot {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkBitNot {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkBitNot {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![],
        }
    }
}

impl ScalarUDFImpl for SparkBitNot {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "bit_not"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match arg_types[0] {
            DataType::Int8 => DataType::Int8,
            DataType::Int16 => DataType::Int16,
            DataType::Int32 => DataType::Int32,
            DataType::Int64 => DataType::Int64,
            DataType::Null => DataType::Null,
            _ => {
                return exec_err!(
                    "{} function can only accept integral arrays",
                    self.name()
                )
            }
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return plan_err!("bit_not expects exactly 1 argument");
        }
        make_scalar_function(spark_bit_not, vec![])(&args.args)
    }
}

macro_rules! bit_not_op {
    ($OPERAND:expr, $PT:ident, $AT:ident) => {{
        let result: $AT = $OPERAND
            .as_primitive::<$PT>()
            .iter()
            .map(|x| x.map(|y| !y))
            .collect();
        Ok(Arc::new(result))
    }};
}

pub fn spark_bit_not(value_array: &[ArrayRef]) -> Result<ArrayRef> {
    let value_array = value_array[0].as_ref();
    match value_array.data_type() {
        DataType::Int8 => bit_not_op!(value_array, Int8Type, Int8Array),
        DataType::Int16 => bit_not_op!(value_array, Int16Type, Int16Array),
        DataType::Int32 => bit_not_op!(value_array, Int32Type, Int32Array),
        DataType::Int64 => bit_not_op!(value_array, Int64Type, Int64Array),
        _ => exec_err!("bit_not can't be evaluated because the expression's type is {:?}, not signed int", value_array.data_type()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spark_bit_not_int32() -> Result<()> {
        let int32_array = Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            Some(12345),
            Some(89),
            Some(-3456),
        ]);
        let expected = &Int32Array::from(vec![
            Some(-2),
            Some(-3),
            None,
            Some(-12346),
            Some(-90),
            Some(3455),
        ]);

        let result = spark_bit_not(&[Arc::new(int32_array)])?;
        let result = result.as_primitive::<Int32Type>();

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn test_spark_bit_not_int8() -> Result<()> {
        let int8_array = Int8Array::from(vec![
            Some(0),
            Some(127),
            Some(-128),
            None,
            Some(42),
            Some(-73),
            Some(1),
        ]);
        let expected = &Int8Array::from(vec![
            Some(-1),
            Some(-128),
            Some(127),
            None,
            Some(-43),
            Some(72),
            Some(-2),
        ]);

        let result = spark_bit_not(&[Arc::new(int8_array)])?;
        let result = result.as_primitive::<Int8Type>();

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn test_spark_bit_not_int16() -> Result<()> {
        let int16_array = Int16Array::from(vec![
            Some(0),
            Some(32767),
            Some(-32768),
            None,
            Some(12345),
            Some(-24680),
            Some(0x5555),
        ]);
        let expected = &Int16Array::from(vec![
            Some(-1),
            Some(-32768),
            Some(32767),
            None,
            Some(-12346),
            Some(24679),
            Some(-0x5556),
        ]);

        let result = spark_bit_not(&[Arc::new(int16_array)])?;
        let result = result.as_primitive::<Int16Type>();

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn test_spark_bit_not_int64() -> Result<()> {
        let int64_array = Int64Array::from(vec![
            Some(0),
            Some(i64::MAX),
            Some(i64::MIN),
            None,
            Some(1234567890123456),
            Some(-9876543210987654),
            Some(0x5555555555555555),
        ]);
        let expected = &Int64Array::from(vec![
            Some(-1),
            Some(i64::MIN),
            Some(i64::MAX),
            None,
            Some(-1234567890123457),
            Some(9876543210987653),
            Some(-0x5555555555555556),
        ]);

        let result = spark_bit_not(&[Arc::new(int64_array)])?;
        let result = result.as_primitive::<Int64Type>();

        assert_eq!(result, expected);

        Ok(())
    }
}
