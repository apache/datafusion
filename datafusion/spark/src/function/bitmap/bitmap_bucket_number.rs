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
use datafusion_common::{Result, internal_err};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions::utils::make_scalar_function;
use std::any::Any;
use std::sync::Arc;

/// Spark-compatible `bitmap_bucket_number` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#bitmap_bucket_number>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct BitmapBucketNumber {
    signature: Signature,
}

impl Default for BitmapBucketNumber {
    fn default() -> Self {
        Self::new()
    }
}

impl BitmapBucketNumber {
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

impl ScalarUDFImpl for BitmapBucketNumber {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "bitmap_bucket_number"
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
        make_scalar_function(bitmap_bucket_number_inner, vec![])(&args.args)
    }
}

pub fn bitmap_bucket_number_inner(arg: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = take_function_args("bitmap_bucket_number", arg)?;
    match &array.data_type() {
        DataType::Int8 => {
            let result: Int64Array = array
                .as_primitive::<Int8Type>()
                .iter()
                .map(|opt| opt.map(|value| bitmap_bucket_number(value.into())))
                .collect();
            Ok(Arc::new(result))
        }
        DataType::Int16 => {
            let result: Int64Array = array
                .as_primitive::<Int16Type>()
                .iter()
                .map(|opt| opt.map(|value| bitmap_bucket_number(value.into())))
                .collect();
            Ok(Arc::new(result))
        }
        DataType::Int32 => {
            let result: Int64Array = array
                .as_primitive::<Int32Type>()
                .iter()
                .map(|opt| opt.map(|value| bitmap_bucket_number(value.into())))
                .collect();
            Ok(Arc::new(result))
        }
        DataType::Int64 => {
            let result: Int64Array = array
                .as_primitive::<Int64Type>()
                .iter()
                .map(|opt| opt.map(bitmap_bucket_number))
                .collect();
            Ok(Arc::new(result))
        }
        data_type => {
            internal_err!("bitmap_bucket_number does not support {data_type}")
        }
    }
}

const NUM_BYTES: i64 = 4 * 1024;
const NUM_BITS: i64 = NUM_BYTES * 8;

fn bitmap_bucket_number(value: i64) -> i64 {
    if value > 0 {
        1 + (value - 1) / NUM_BITS
    } else {
        value / NUM_BITS
    }
}

#[cfg(test)]
mod tests {
    use super::bitmap_bucket_number;
    use arrow::array::{Int8Array, Int16Array, Int32Array, Int64Array};

    #[test]
    fn test_bitmap_bucket_number_int8() {
        let input = Int8Array::from(vec![1, 127, -1, -64, -65]);
        let result: Int64Array = input
            .iter()
            .map(|opt| opt.map(|v| bitmap_bucket_number(v as i64)))
            .collect();

        assert_eq!(result.value(0), 1);
        assert_eq!(result.value(1), 1);
        assert_eq!(result.value(2), 0);
        assert_eq!(result.value(3), 0);
        assert_eq!(result.value(4), 0);
    }

    #[test]
    fn test_bitmap_bucket_number_int16() {
        let input = Int16Array::from(vec![1, 257, 32767, -1, -256]);
        let result: Int64Array = input
            .iter()
            .map(|opt| opt.map(|v| bitmap_bucket_number(v as i64)))
            .collect();

        assert_eq!(result.value(0), 1);
        assert_eq!(result.value(1), 1);
        assert_eq!(result.value(2), 1);
        assert_eq!(result.value(3), 0);
        assert_eq!(result.value(4), 0);
    }

    #[test]
    fn test_bitmap_bucket_number_int32() {
        let input = Int32Array::from(vec![1, 65537, 2147483647, -1, -65536]);
        let result: Int64Array = input
            .iter()
            .map(|opt| opt.map(|v| bitmap_bucket_number(v as i64)))
            .collect();

        assert_eq!(result.value(0), 1);
        assert_eq!(result.value(1), 3);
        assert_eq!(result.value(2), 65536);
        assert_eq!(result.value(3), 0);
        assert_eq!(result.value(4), -2);
    }

    #[test]
    fn test_bitmap_bucket_number_int64() {
        let input =
            Int64Array::from(vec![1, 4294967297, i64::MAX, -1, -4294967296, i64::MIN]);
        let result: Int64Array = input
            .iter()
            .map(|opt| opt.map(bitmap_bucket_number))
            .collect();

        assert_eq!(result.value(0), 1);
        assert_eq!(result.value(1), 131073);
        assert_eq!(result.value(2), 281474976710656);
        assert_eq!(result.value(3), 0);
        assert_eq!(result.value(4), -131072);
        assert_eq!(result.value(5), -281474976710656);
    }
}
