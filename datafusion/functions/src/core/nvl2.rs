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

use arrow::datatypes::DataType;
use datafusion_common::{internal_err, plan_datafusion_err, DataFusionError, Result};
use datafusion_expr::{utils, ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use arrow::compute::kernels::zip::zip;
use arrow::compute::is_not_null;
use arrow::array::Array;

#[derive(Debug)]
pub(super) struct NVL2Func {
    signature: Signature,
}

/// Currently supported types by the nvl/ifnull function.
/// The order of these types correspond to the order on which coercion applies
/// This should thus be from least informative to most informative
static SUPPORTED_NVL2_TYPES: &[DataType] = &[
    DataType::Boolean,
    DataType::UInt8,
    DataType::UInt16,
    DataType::UInt32,
    DataType::UInt64,
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
    DataType::Float32,
    DataType::Float64,
    DataType::Utf8,
    DataType::LargeUtf8,
];

impl NVL2Func {
    pub fn new() -> Self {
        Self {
            signature:
            Signature::uniform(3, SUPPORTED_NVL2_TYPES.to_vec(),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for NVL2Func {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "nvl2"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 3 {
            return Err(plan_datafusion_err!(
                "{}",
                utils::generate_signature_error_msg(
                    self.name(),
                    self.signature().clone(),
                    arg_types,
                )
            ));
        }
        Ok(arg_types[1].clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        nvl2_func(args)
    }
}

fn nvl2_func(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 3 {
        return internal_err!(
            "{:?} args were supplied but NVL2 takes exactly three args",
            args.len()
        );
    }
    let mut len = 1;
    let mut is_array = false;
    for arg in args {
        if let ColumnarValue::Array(array) = arg {
            len = array.len();
            is_array = true;
            break;
        }
    }
    if is_array {
        let args = args.iter().map(|arg| match arg {
            ColumnarValue::Scalar(scalar) => {
                scalar.to_array_of_size(len)
            }
            ColumnarValue::Array(array) => {
                Ok(array.clone())
            }
        }).collect::<Result<Vec<_>>>()?;
        let to_apply = is_not_null(&args[0])?;
        let value = zip(&to_apply, &args[1], &args[2])?;
        Ok(ColumnarValue::Array(value))
    } else {
        let mut current_value = &args[1];
        match &args[0] {
            ColumnarValue::Array(_) => {
                internal_err!("except Scalar value, but got Array")
            }
            ColumnarValue::Scalar(scalar) => {
                if scalar.is_null() {
                    current_value = &args[2];
                }
                Ok(current_value.clone())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::*;

    use super::*;
    use datafusion_common::{Result, ScalarValue};

    #[test]
    fn nvl2_int32() -> Result<()> {
        let a = Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            None,
            Some(3),
            None,
            None,
            Some(4),
            Some(5),
        ]);
        let a = ColumnarValue::Array(Arc::new(a));

        let b = ColumnarValue::Scalar(ScalarValue::Int32(Some(6i32)));
        let c = ColumnarValue::Scalar(ScalarValue::Int32(Some(10i32)));

        let result = nvl2_func(&[a, b, c])?;
        let result = result.into_array(0).expect("Failed to convert to array");

        let expected = Arc::new(Int32Array::from(vec![
            Some(6),
            Some(6),
            Some(10),
            Some(10),
            Some(6),
            Some(10),
            Some(10),
            Some(6),
            Some(6),
        ])) as ArrayRef;
        assert_eq!(expected.as_ref(), result.as_ref());
        Ok(())
    }

    #[test]
    // Ensure that arrays with no nulls can also invoke nvl() correctly
    fn nvl2_int32_nonulls() -> Result<()> {
        let a = Int32Array::from(vec![1, 3, 10, 7, 8, 1, 2, 4, 5]);
        let a = ColumnarValue::Array(Arc::new(a));

        let b = ColumnarValue::Scalar(ScalarValue::Int32(Some(20i32)));
        let c = ColumnarValue::Scalar(ScalarValue::Int32(Some(30i32)));

        let result = nvl2_func(&[a, b, c])?;
        let result = result.into_array(0).expect("Failed to convert to array");

        let expected = Arc::new(Int32Array::from(vec![
            Some(20),
            Some(20),
            Some(20),
            Some(20),
            Some(20),
            Some(20),
            Some(20),
            Some(20),
            Some(20),
        ])) as ArrayRef;
        assert_eq!(expected.as_ref(), result.as_ref());
        Ok(())
    }

    #[test]
    fn nvl2_boolean() -> Result<()> {
        let a = BooleanArray::from(vec![Some(true), Some(false), None]);
        let a = ColumnarValue::Array(Arc::new(a));

        let b = ColumnarValue::Scalar(ScalarValue::Boolean(Some(false)));
        let c = BooleanArray::from(vec![Some(true), Some(false), Some(true)]);
        let c = ColumnarValue::Array(Arc::new(c));

        let result = nvl2_func(&[a, b, c])?;
        let result = result.into_array(0).expect("Failed to convert to array");

        let expected =
            Arc::new(BooleanArray::from(vec![Some(false), Some(false), Some(true)])) as ArrayRef;

        assert_eq!(expected.as_ref(), result.as_ref());
        Ok(())
    }

    #[test]
    fn nvl2_string() -> Result<()> {
        let a = StringArray::from(vec![Some("foo"), Some("bar"), None, Some("baz")]);
        let a = ColumnarValue::Array(Arc::new(a));

        let b = StringArray::from(vec![Some("bar"), Some("xyz"), Some("baz"), Some("foo")]);
        let b = ColumnarValue::Array(Arc::new(b));
        let c = ColumnarValue::Scalar(ScalarValue::from("bax"));

        let result = nvl2_func(&[a, b, c])?;
        let result = result.into_array(0).expect("Failed to convert to array");

        let expected = Arc::new(StringArray::from(vec![
            Some("bar"),
            Some("xyz"),
            Some("bax"),
            Some("foo"),
        ])) as ArrayRef;

        assert_eq!(expected.as_ref(), result.as_ref());
        Ok(())
    }

    #[test]
    fn nvl2_literal_first() -> Result<()> {
        let a = Int32Array::from(vec![Some(1), Some(2), None, None, Some(3), Some(4)]);
        let a = ColumnarValue::Array(Arc::new(a));

        let b = ColumnarValue::Scalar(ScalarValue::Int32(Some(2i32)));

        let c = Int32Array::from(vec![Some(11), Some(12), None, None, Some(13), Some(14)]);
        let c = ColumnarValue::Array(Arc::new(c));

        let result = nvl2_func(&[b, a, c])?;
        let result = result.into_array(0).expect("Failed to convert to array");

        let expected = Arc::new(Int32Array::from(vec![Some(1), Some(2), None, None, Some(3), Some(4)])) as ArrayRef;
        assert_eq!(expected.as_ref(), result.as_ref());
        Ok(())
    }

    #[test]
    fn nvl2_all_array() -> Result<()> {
        let a = Int32Array::from(vec![Some(1), Some(2), None, None, Some(3), Some(4)]);
        let a = ColumnarValue::Array(Arc::new(a));

        let b = Int32Array::from(vec![Some(5), Some(6), Some(7), Some(8), Some(9), None]);
        let b = ColumnarValue::Array(Arc::new(b));

        let c = Int32Array::from(vec![Some(11), Some(12), Some(13), Some(14), Some(15), None]);
        let c = ColumnarValue::Array(Arc::new(c));

        let result = nvl2_func(&[a, b, c])?;
        let result = result.into_array(0).expect("Failed to convert to array");

        let expected = Arc::new(Int32Array::from(vec![
            Some(5),
            Some(6),
            Some(13),
            Some(14),
            Some(9),
            None,
        ])) as ArrayRef;
        assert_eq!(expected.as_ref(), result.as_ref());
        Ok(())
    }

    #[test]
    fn nvl_scalar() -> Result<()> {
        let a_scalar = ColumnarValue::Scalar(ScalarValue::Int32(None));
        let b_scalar = ColumnarValue::Scalar(ScalarValue::Int32(Some(2i32)));
        let c_scalar = ColumnarValue::Scalar(ScalarValue::Int32(Some(3i32)));

        let result_null = nvl2_func(&[a_scalar, b_scalar, c_scalar])?;
        let result_null = result_null.into_array(1).expect("Failed to convert to array");

        let expected_null = Arc::new(Int32Array::from(vec![Some(3i32)])) as ArrayRef;

        assert_eq!(expected_null.as_ref(), result_null.as_ref());

        let a_scalar = ColumnarValue::Scalar(ScalarValue::Int32(Some(2i32)));
        let b_scalar = ColumnarValue::Scalar(ScalarValue::Int32(Some(3i32)));
        let c_scalar = ColumnarValue::Scalar(ScalarValue::Int32(Some(4i32)));

        let result_nnull = nvl2_func(&[a_scalar, b_scalar, c_scalar])?;
        let result_nnull = result_nnull
            .into_array(1)
            .expect("Failed to convert to array");

        let expected_nnull = Arc::new(Int32Array::from(vec![Some(3i32)])) as ArrayRef;
        assert_eq!(expected_nnull.as_ref(), result_nnull.as_ref());

        Ok(())
    }
}
