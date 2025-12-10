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

use arrow::array::{ArrayRef, AsArray, Int32Array};
use arrow::datatypes::{
    DataType, FieldRef, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use datafusion_common::cast::as_boolean_array;
use datafusion_common::{Result, internal_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion_functions::utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkBitCount {
    signature: Signature,
}

impl Default for SparkBitCount {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkBitCount {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Boolean]),
                    TypeSignature::Exact(vec![DataType::Int8]),
                    TypeSignature::Exact(vec![DataType::Int16]),
                    TypeSignature::Exact(vec![DataType::Int32]),
                    TypeSignature::Exact(vec![DataType::Int64]),
                    TypeSignature::Exact(vec![DataType::UInt8]),
                    TypeSignature::Exact(vec![DataType::UInt16]),
                    TypeSignature::Exact(vec![DataType::UInt32]),
                    TypeSignature::Exact(vec![DataType::UInt64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkBitCount {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "bit_count"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(
        &self,
        args: datafusion_expr::ReturnFieldArgs,
    ) -> Result<FieldRef> {
        use arrow::datatypes::Field;
        // bit_count returns Int32 with the same nullability as the input
        Ok(Arc::new(Field::new(
            args.arg_fields[0].name(),
            DataType::Int32,
            args.arg_fields[0].is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return plan_err!("bit_count expects exactly 1 argument");
        }

        make_scalar_function(spark_bit_count, vec![])(&args.args)
    }
}

fn spark_bit_count(value_array: &[ArrayRef]) -> Result<ArrayRef> {
    let value_array = value_array[0].as_ref();
    match value_array.data_type() {
        DataType::Boolean => {
            let result: Int32Array = as_boolean_array(value_array)?
                .iter()
                .map(|x| x.map(|y| y as i32))
                .collect();
            Ok(Arc::new(result))
        }
        DataType::Int8 => {
            let result: Int32Array = value_array
                .as_primitive::<Int8Type>()
                .unary(|v| (v as i64).count_ones() as i32);
            Ok(Arc::new(result))
        }
        DataType::Int16 => {
            let result: Int32Array = value_array
                .as_primitive::<Int16Type>()
                .unary(|v| (v as i64).count_ones() as i32);
            Ok(Arc::new(result))
        }
        DataType::Int32 => {
            let result: Int32Array = value_array
                .as_primitive::<Int32Type>()
                .unary(|v| (v as i64).count_ones() as i32);
            Ok(Arc::new(result))
        }
        DataType::Int64 => {
            let result: Int32Array = value_array
                .as_primitive::<Int64Type>()
                .unary(|v| v.count_ones() as i32);
            Ok(Arc::new(result))
        }
        DataType::UInt8 => {
            let result: Int32Array = value_array
                .as_primitive::<UInt8Type>()
                .unary(|v| v.count_ones() as i32);
            Ok(Arc::new(result))
        }
        DataType::UInt16 => {
            let result: Int32Array = value_array
                .as_primitive::<UInt16Type>()
                .unary(|v| v.count_ones() as i32);
            Ok(Arc::new(result))
        }
        DataType::UInt32 => {
            let result: Int32Array = value_array
                .as_primitive::<UInt32Type>()
                .unary(|v| v.count_ones() as i32);
            Ok(Arc::new(result))
        }
        DataType::UInt64 => {
            let result: Int32Array = value_array
                .as_primitive::<UInt64Type>()
                .unary(|v| v.count_ones() as i32);
            Ok(Arc::new(result))
        }
        _ => {
            plan_err!(
                "bit_count function does not support data type: {}",
                value_array.data_type()
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, BooleanArray, Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array,
    };
    use arrow::datatypes::{Field, Int32Type};

    #[test]
    fn test_bit_count_basic() {
        // Test bit_count(0) - no bits set
        let result = spark_bit_count(&[Arc::new(Int32Array::from(vec![0]))]).unwrap();

        assert_eq!(result.as_primitive::<Int32Type>().value(0), 0);

        // Test bit_count(1) - 1 bit set
        let result = spark_bit_count(&[Arc::new(Int32Array::from(vec![1]))]).unwrap();

        assert_eq!(result.as_primitive::<Int32Type>().value(0), 1);

        // Test bit_count(7) - 7 = 111 in binary, 3 bits set
        let result = spark_bit_count(&[Arc::new(Int32Array::from(vec![7]))]).unwrap();

        assert_eq!(result.as_primitive::<Int32Type>().value(0), 3);

        // Test bit_count(15) - 15 = 1111 in binary, 4 bits set
        let result = spark_bit_count(&[Arc::new(Int32Array::from(vec![15]))]).unwrap();

        assert_eq!(result.as_primitive::<Int32Type>().value(0), 4);
    }

    #[test]
    fn test_bit_count_int8() {
        // Test bit_count on Int8Array
        let result =
            spark_bit_count(&[Arc::new(Int8Array::from(vec![0i8, 1, 3, 7, 15, -1]))])
                .unwrap();

        let arr = result.as_primitive::<Int32Type>();
        assert_eq!(arr.value(0), 0);
        assert_eq!(arr.value(1), 1);
        assert_eq!(arr.value(2), 2);
        assert_eq!(arr.value(3), 3);
        assert_eq!(arr.value(4), 4);
        assert_eq!(arr.value(5), 64);
    }

    #[test]
    fn test_bit_count_boolean() {
        // Test bit_count on BooleanArray
        let result =
            spark_bit_count(&[Arc::new(BooleanArray::from(vec![true, false]))]).unwrap();

        let arr = result.as_primitive::<Int32Type>();
        assert_eq!(arr.value(0), 1);
        assert_eq!(arr.value(1), 0);
    }

    #[test]
    fn test_bit_count_int16() {
        // Test bit_count on Int16Array
        let result =
            spark_bit_count(&[Arc::new(Int16Array::from(vec![0i16, 1, 255, 1023, -1]))])
                .unwrap();

        let arr = result.as_primitive::<Int32Type>();
        assert_eq!(arr.value(0), 0);
        assert_eq!(arr.value(1), 1);
        assert_eq!(arr.value(2), 8);
        assert_eq!(arr.value(3), 10);
        assert_eq!(arr.value(4), 64);
    }

    #[test]
    fn test_bit_count_int32() {
        // Test bit_count on Int32Array
        let result =
            spark_bit_count(&[Arc::new(Int32Array::from(vec![0i32, 1, 255, 1023, -1]))])
                .unwrap();

        let arr = result.as_primitive::<Int32Type>();
        assert_eq!(arr.value(0), 0); // 0b00000000000000000000000000000000 = 0
        assert_eq!(arr.value(1), 1); // 0b00000000000000000000000000000001 = 1
        assert_eq!(arr.value(2), 8); // 0b00000000000000000000000011111111 = 8
        assert_eq!(arr.value(3), 10); // 0b00000000000000000000001111111111 = 10
        assert_eq!(arr.value(4), 64); // -1 in two's complement = all 32 bits set
    }

    #[test]
    fn test_bit_count_int64() {
        // Test bit_count on Int64Array
        let result =
            spark_bit_count(&[Arc::new(Int64Array::from(vec![0i64, 1, 255, 1023, -1]))])
                .unwrap();

        let arr = result.as_primitive::<Int32Type>();
        assert_eq!(arr.value(0), 0); // 0b0000000000000000000000000000000000000000000000000000000000000000 = 0
        assert_eq!(arr.value(1), 1); // 0b0000000000000000000000000000000000000000000000000000000000000001 = 1
        assert_eq!(arr.value(2), 8); // 0b0000000000000000000000000000000000000000000000000000000011111111 = 8
        assert_eq!(arr.value(3), 10); // 0b0000000000000000000000000000000000000000000000000000001111111111 = 10
        assert_eq!(arr.value(4), 64); // -1 in two's complement = all 64 bits set
    }

    #[test]
    fn test_bit_count_uint8() {
        // Test bit_count on UInt8Array
        let result =
            spark_bit_count(&[Arc::new(UInt8Array::from(vec![0u8, 1, 255]))]).unwrap();

        let arr = result.as_primitive::<Int32Type>();
        assert_eq!(arr.value(0), 0); // 0b00000000 = 0
        assert_eq!(arr.value(1), 1); // 0b00000001 = 1
        assert_eq!(arr.value(2), 8); // 0b11111111 = 8
    }

    #[test]
    fn test_bit_count_uint16() {
        // Test bit_count on UInt16Array
        let result =
            spark_bit_count(&[Arc::new(UInt16Array::from(vec![0u16, 1, 255, 65535]))])
                .unwrap();

        let arr = result.as_primitive::<Int32Type>();
        assert_eq!(arr.value(0), 0); // 0b0000000000000000 = 0
        assert_eq!(arr.value(1), 1); // 0b0000000000000001 = 1
        assert_eq!(arr.value(2), 8); // 0b0000000011111111 = 8
        assert_eq!(arr.value(3), 16); // 0b1111111111111111 = 16
    }

    #[test]
    fn test_bit_count_uint32() {
        // Test bit_count on UInt32Array
        let result = spark_bit_count(&[Arc::new(UInt32Array::from(vec![
            0u32, 1, 255, 4294967295,
        ]))])
        .unwrap();

        let arr = result.as_primitive::<Int32Type>();
        assert_eq!(arr.value(0), 0); // 0b00000000000000000000000000000000 = 0
        assert_eq!(arr.value(1), 1); // 0b00000000000000000000000000000001 = 1
        assert_eq!(arr.value(2), 8); // 0b00000000000000000000000011111111 = 8
        assert_eq!(arr.value(3), 32); // 0b11111111111111111111111111111111 = 32
    }

    #[test]
    fn test_bit_count_uint64() {
        // Test bit_count on UInt64Array
        let result = spark_bit_count(&[Arc::new(UInt64Array::from(vec![
            0u64,
            1,
            255,
            256,
            u64::MAX,
        ]))])
        .unwrap();

        let arr = result.as_primitive::<Int32Type>();
        // 0b0 = 0
        assert_eq!(arr.value(0), 0);
        // 0b1 = 1
        assert_eq!(arr.value(1), 1);
        // 0b11111111 = 8
        assert_eq!(arr.value(2), 8);
        // 0b100000000 = 1
        assert_eq!(arr.value(3), 1);
        // u64::MAX = all 64 bits set
        assert_eq!(arr.value(4), 64);
    }

    #[test]
    fn test_bit_count_nulls() {
        // Test bit_count with nulls
        let arr = Int32Array::from(vec![Some(3), None, Some(7)]);
        let result = spark_bit_count(&[Arc::new(arr)]).unwrap();
        let arr = result.as_primitive::<Int32Type>();
        assert_eq!(arr.value(0), 2); // 0b11
        assert!(arr.is_null(1));
        assert_eq!(arr.value(2), 3); // 0b111
    }

    #[test]
    fn test_bit_count_nullability() -> Result<()> {
        use datafusion_expr::ReturnFieldArgs;

        let bit_count = SparkBitCount::new();

        // Test with non-nullable Int32 field
        let non_nullable_field = Arc::new(Field::new("num", DataType::Int32, false));

        let result = bit_count.return_field_from_args(ReturnFieldArgs {
            arg_fields: &[Arc::clone(&non_nullable_field)],
            scalar_arguments: &[None],
        })?;

        // The result should not be nullable (same as input)
        assert!(!result.is_nullable());
        assert_eq!(result.data_type(), &DataType::Int32);

        // Test with nullable Int32 field
        let nullable_field = Arc::new(Field::new("num", DataType::Int32, true));

        let result = bit_count.return_field_from_args(ReturnFieldArgs {
            arg_fields: &[Arc::clone(&nullable_field)],
            scalar_arguments: &[None],
        })?;

        // The result should be nullable (same as input)
        assert!(result.is_nullable());
        assert_eq!(result.data_type(), &DataType::Int32);

        Ok(())
    }
}
