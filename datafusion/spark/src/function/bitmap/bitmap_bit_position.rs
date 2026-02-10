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

use arrow::array::{ArrayRef, AsArray, Int64Array};
use arrow::datatypes::Field;
use arrow::datatypes::{DataType, FieldRef, Int8Type, Int16Type, Int32Type, Int64Type};
use datafusion::logical_expr::{ColumnarValue, Signature, TypeSignature, Volatility};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, internal_err, plan_err};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions::utils::make_scalar_function;
use std::any::Any;
use std::sync::Arc;

/// Spark-compatible `bitmap_bit_position` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#bitmap_bit_position>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct BitmapBitPosition {
    signature: Signature,
}

impl Default for BitmapBitPosition {
    fn default() -> Self {
        Self::new()
    }
}

impl BitmapBitPosition {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Int8]),
                    TypeSignature::Exact(vec![DataType::Int16]),
                    TypeSignature::Exact(vec![DataType::Int32]),
                    TypeSignature::Exact(vec![DataType::Int64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for BitmapBitPosition {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "bitmap_bit_position"
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
        Ok(Arc::new(Field::new(
            args.arg_fields[0].name(),
            DataType::Int64,
            args.arg_fields[0].is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return plan_err!("bitmap_bit_position expects exactly 1 argument");
        }
        make_scalar_function(bitmap_bit_position_inner, vec![])(&args.args)
    }
}

pub fn bitmap_bit_position_inner(arg: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = take_function_args("bitmap_bit_position", arg)?;
    match &array.data_type() {
        DataType::Int8 => {
            let result: Int64Array = array
                .as_primitive::<Int8Type>()
                .iter()
                .map(|opt| opt.map(|value| bitmap_bit_position(value.into())))
                .collect();
            Ok(Arc::new(result))
        }
        DataType::Int16 => {
            let result: Int64Array = array
                .as_primitive::<Int16Type>()
                .iter()
                .map(|opt| opt.map(|value| bitmap_bit_position(value.into())))
                .collect();
            Ok(Arc::new(result))
        }
        DataType::Int32 => {
            let result: Int64Array = array
                .as_primitive::<Int32Type>()
                .iter()
                .map(|opt| opt.map(|value| bitmap_bit_position(value.into())))
                .collect();
            Ok(Arc::new(result))
        }
        DataType::Int64 => {
            let result: Int64Array = array
                .as_primitive::<Int64Type>()
                .iter()
                .map(|opt| opt.map(|value| bitmap_bit_position(value.into())))
                .collect();
            Ok(Arc::new(result))
        }
        data_type => {
            internal_err!("bitmap_bit_position does not support {data_type}")
        }
    }
}

const NUM_BYTES: i64 = 4 * 1024;
const NUM_BITS: i64 = NUM_BYTES * 8;

fn bitmap_bit_position(value: i64) -> i64 {
    if value > 0 {
        (value - 1) % NUM_BITS
    } else {
        (-value) % NUM_BITS
    }
}

#[cfg(test)]
mod tests {
    use crate::function::bitmap::bitmap_bit_position::bitmap_bit_position_inner;
    use arrow::array::{AsArray, Int8Array, Int16Array, Int32Array, Int64Array};
    use arrow::datatypes::Int64Type;
    use std::sync::Arc;

    #[test]
    fn test_bitmap_bit_position_int8() {
        let result = bitmap_bit_position_inner(&[Arc::new(Int8Array::from(vec![
            1, 3, 7, 15, -1,
        ]))])
        .unwrap();
        let result = result.as_primitive::<Int64Type>();
        assert_eq!(result.value(0), 0);
        assert_eq!(result.value(1), 2);
        assert_eq!(result.value(2), 6);
        assert_eq!(result.value(3), 14);
        assert_eq!(result.value(4), 1);
    }

    #[test]
    fn test_bitmap_bit_position_int16() {
        let result = bitmap_bit_position_inner(&[Arc::new(Int16Array::from(vec![
            256, 1024, -32768, 16384, -1,
        ]))])
        .unwrap();
        let result = result.as_primitive::<Int64Type>();
        assert_eq!(result.value(0), 255);
        assert_eq!(result.value(1), 1023);
        assert_eq!(result.value(2), 0);
        assert_eq!(result.value(3), 16383);
        assert_eq!(result.value(4), 1);
    }

    #[test]
    fn test_bitmap_bit_position_int32() {
        let result = bitmap_bit_position_inner(&[Arc::new(Int32Array::from(vec![
            65536,
            1048576,
            -2147483648,
            1073741824,
            -1,
        ]))])
        .unwrap();
        let result = result.as_primitive::<Int64Type>();
        assert_eq!(result.value(0), 32767);
        assert_eq!(result.value(1), 32767);
        assert_eq!(result.value(2), 0);
        assert_eq!(result.value(3), 32767);
        assert_eq!(result.value(4), 1);
    }

    #[test]
    fn test_bitmap_bit_position_int64() {
        let result = bitmap_bit_position_inner(&[Arc::new(Int64Array::from(vec![
            4294967296, -1,
        ]))])
        .unwrap();
        let result = result.as_primitive::<Int64Type>();
        assert_eq!(result.value(0), 32767);
        assert_eq!(result.value(1), 1);
    }
}
