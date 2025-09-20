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

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, FixedSizeBinaryArray, Int64Array,
    LargeBinaryArray,
};
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{
    Binary, BinaryView, FixedSizeBinary, Int64, LargeBinary,
};
use datafusion_common::utils::take_function_args;
use datafusion_common::{internal_err, Result};
use datafusion_expr::{
    Coercion, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
};
use datafusion_functions::downcast_arg;
use datafusion_functions::utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct BitmapCount {
    signature: Signature,
}

impl Default for BitmapCount {
    fn default() -> Self {
        Self::new()
    }
}

impl BitmapCount {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![Coercion::new_exact(TypeSignatureClass::Binary)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for BitmapCount {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "bitmap_count"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(bitmap_count_inner, vec![])(&args.args)
    }
}

fn binary_count_ones(opt: Option<&[u8]>) -> Option<i64> {
    opt.map(|value| value.iter().map(|b| b.count_ones() as i64).sum())
}

macro_rules! downcast_and_count_ones {
    ($input_array:expr, $array_type:ident) => {{
        let arr = downcast_arg!($input_array, $array_type);
        Ok(arr.iter().map(binary_count_ones).collect::<Int64Array>())
    }};
}

pub fn bitmap_count_inner(arg: &[ArrayRef]) -> Result<ArrayRef> {
    let [input_array] = take_function_args("bitmap_count", arg)?;

    let res: Result<Int64Array> = match &input_array.data_type() {
        Binary => downcast_and_count_ones!(input_array, BinaryArray),
        BinaryView => downcast_and_count_ones!(input_array, BinaryViewArray),
        LargeBinary => downcast_and_count_ones!(input_array, LargeBinaryArray),
        FixedSizeBinary(_size) => {
            downcast_and_count_ones!(input_array, FixedSizeBinaryArray)
        }
        data_type => {
            internal_err!("bitmap_count does not support {data_type}")
        }
    };

    Ok(Arc::new(res?))
}

#[cfg(test)]
mod tests {
    use crate::function::bitmap::bitmap_count::BitmapCount;
    use crate::function::utils::test::test_scalar_function;
    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::DataType::Int64;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    macro_rules! test_bitmap_count_binary_invoke {
        ($INPUT:expr, $EXPECTED:expr) => {
            test_scalar_function!(
                BitmapCount::new(),
                vec![ColumnarValue::Scalar(ScalarValue::Binary($INPUT))],
                $EXPECTED,
                i64,
                Int64,
                Int64Array
            );

            test_scalar_function!(
                BitmapCount::new(),
                vec![ColumnarValue::Scalar(ScalarValue::LargeBinary($INPUT))],
                $EXPECTED,
                i64,
                Int64,
                Int64Array
            );

            test_scalar_function!(
                BitmapCount::new(),
                vec![ColumnarValue::Scalar(ScalarValue::BinaryView($INPUT))],
                $EXPECTED,
                i64,
                Int64,
                Int64Array
            );

            test_scalar_function!(
                BitmapCount::new(),
                vec![ColumnarValue::Scalar(ScalarValue::FixedSizeBinary(
                    $INPUT.map(|a| a.len()).unwrap_or(0) as i32,
                    $INPUT
                ))],
                $EXPECTED,
                i64,
                Int64,
                Int64Array
            );
        };
    }

    #[test]
    fn test_bitmap_count_invoke() -> Result<()> {
        test_bitmap_count_binary_invoke!(None::<Vec<u8>>, Ok(None));
        test_bitmap_count_binary_invoke!(Some(vec![0x0Au8]), Ok(Some(2)));
        test_bitmap_count_binary_invoke!(Some(vec![0xFFu8, 0xFFu8]), Ok(Some(16)));
        test_bitmap_count_binary_invoke!(
            Some(vec![0x0Au8, 0xB0u8, 0xCDu8]),
            Ok(Some(10))
        );
        Ok(())
    }
}
