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

use std::hash::Hasher;
use std::sync::Arc;

use arrow::array::{
    Array, AsArray, BinaryArray, LargeBinaryArray, LargeStringArray, StringArray,
    StringBuilder,
};
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{
    Binary, BinaryView, LargeBinary, LargeUtf8, Null, UInt64, Utf8, Utf8View,
};
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use twox_hash::XxHash64;

#[derive(Debug)]
pub struct XXHash64Func {
    signature: Signature,
}

impl Default for XXHash64Func {
    fn default() -> Self {
        Self::new()
    }
}

impl XXHash64Func {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![Utf8]),
                    TypeSignature::Exact(vec![Utf8View]),
                    TypeSignature::Exact(vec![LargeUtf8]),
                    TypeSignature::Exact(vec![Binary]),
                    TypeSignature::Exact(vec![BinaryView]),
                    TypeSignature::Exact(vec![LargeBinary]),
                    TypeSignature::Exact(vec![Utf8, UInt64]),
                    TypeSignature::Exact(vec![Utf8View, UInt64]),
                    TypeSignature::Exact(vec![LargeUtf8, UInt64]),
                    TypeSignature::Exact(vec![Binary, UInt64]),
                    TypeSignature::Exact(vec![LargeBinary, UInt64]),
                    TypeSignature::Exact(vec![BinaryView, UInt64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for XXHash64Func {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "xxhash"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() == 1 {
            return Ok(match &arg_types[0] {
                Utf8 | Utf8View | LargeUtf8 => Utf8View,
                Binary | BinaryView | LargeBinary => BinaryView,
                Null => Null,
                other => {
                    return plan_err!(
                        "The xxhash64 function can only accept strings or binary types. Got {other}"
                    );
                }
            });
        }
        Ok(match (&arg_types[0], &arg_types[1]) {
            (Utf8 | Utf8View | LargeUtf8, UInt64) => Utf8View,
            (Binary | BinaryView | LargeBinary, UInt64) => BinaryView,
            (Null, Null) => Null,
            other => {
                return plan_err!(
                    "The xxhash64 function can only accept strings or binary types. Got {:?}",
                    other
                );
            }
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if !(args.args.len() == 1 || args.args.len() == 2) {
            return exec_err!(
                "{} expects 1 or 2 arguments, got {}",
                self.name(),
                args.args.len()
            );
        }

        let input_data = &args.args[0];
        let seed = if args.args.len() > 1 {
            if let ColumnarValue::Scalar(ScalarValue::UInt32(Some(seed))) = &args.args[1]
            {
                *seed as u64
            } else {
                let actual_type = format!("{:?}", &args.args[1]);
                return exec_err!(
                    "Expected a UInt64 seed value, but got {}",
                    actual_type
                );
            }
        } else {
            42 // Default seed value
        };

        let result = match input_data {
            ColumnarValue::Array(array) => {
                let hash_results =
                    process_array(array, XxHash64::with_seed(seed), HashType::U64)?;
                let hash_array = StringArray::from(hash_results);
                Arc::new(hash_array) as Arc<dyn Array>
            }
            ColumnarValue::Scalar(scalar) => match scalar {
                ScalarValue::Utf8(None)
                | ScalarValue::Utf8View(None)
                | ScalarValue::LargeUtf8(None) => {
                    let hash_array = StringArray::from(vec![String::new()]);
                    Arc::new(hash_array) as Arc<dyn Array>
                }
                ScalarValue::Utf8(Some(ref v))
                | ScalarValue::Utf8View(Some(ref v))
                | ScalarValue::LargeUtf8(Some(ref v)) => {
                    let hash_result = hash_value(
                        v.as_bytes(),
                        XxHash64::with_seed(seed),
                        HashType::U64,
                    )?;
                    let hash_array = StringArray::from(vec![hash_result]);
                    Arc::new(hash_array) as Arc<dyn Array>
                }
                ScalarValue::Binary(Some(ref v))
                | ScalarValue::LargeBinary(Some(ref v)) => {
                    let hash_result =
                        hash_value(v, XxHash64::with_seed(seed), HashType::U64)?;
                    let hash_array = StringArray::from(vec![hash_result]);
                    Arc::new(hash_array) as Arc<dyn Array>
                }
                _ => {
                    let actual_type = format!("{:?}", scalar);
                    return exec_err!("Unsupported scalar type: {}", actual_type);
                }
            },
        };
        Ok(ColumnarValue::Array(result))
    }
}

#[derive(Clone)]
pub enum HashType {
    U32,
    U64,
}

fn hash_value<T: Hasher>(
    value_bytes: &[u8],
    mut hasher: T,
    hash_type: HashType,
) -> Result<String> {
    hasher.write(value_bytes);
    let hash = hasher.finish();
    match hash_type {
        HashType::U32 => {
            let hash_u32 = hash as u32;
            Ok(hex::encode(hash_u32.to_be_bytes()))
        }
        HashType::U64 => {
            let hash_u64 = hash;
            Ok(hex::encode(hash_u64.to_be_bytes()))
        }
    }
}

fn process_array<T: Hasher>(
    array: &dyn Array,
    mut hasher: T,
    hash_type: HashType,
) -> Result<StringArray> {
    let mut hash_results = StringBuilder::new();

    match array.data_type() {
        Utf8View => {
            let string_view_array = array.as_string_view();
            for i in 0..array.len() {
                if array.is_null(i) {
                    hash_results.append_value(String::new());
                    continue;
                }
                let value = string_view_array.value(i);
                hash_results.append_value(hash_value(
                    value.as_bytes(),
                    &mut hasher,
                    hash_type.clone(),
                )?);
            }
        }

        Utf8 => {
            let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..array.len() {
                if array.is_null(i) {
                    hash_results.append_value(String::new());
                    continue;
                }
                let value = string_array.value(i);
                hash_results.append_value(hash_value(
                    value.as_bytes(),
                    &mut hasher,
                    hash_type.clone(),
                )?);
            }
        }

        LargeUtf8 => {
            let large_string_array =
                array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            for i in 0..array.len() {
                if array.is_null(i) {
                    hash_results.append_value(String::new());
                    continue;
                }
                let value = large_string_array.value(i);
                hash_results.append_value(hash_value(
                    value.as_bytes(),
                    &mut hasher,
                    hash_type.clone(),
                )?);
            }
        }

        Binary | LargeBinary => {
            let binary_array: &dyn Array = if array.data_type() == &Binary {
                array.as_any().downcast_ref::<BinaryArray>().unwrap()
            } else {
                array.as_any().downcast_ref::<LargeBinaryArray>().unwrap()
            };
            for i in 0..array.len() {
                if array.is_null(i) {
                    hash_results.append_value(String::new());
                    continue;
                }
                let value = if let Some(binary_array) =
                    binary_array.as_any().downcast_ref::<BinaryArray>()
                {
                    binary_array.value(i)
                } else {
                    binary_array
                        .as_any()
                        .downcast_ref::<LargeBinaryArray>()
                        .unwrap()
                        .value(i)
                };
                hash_results.append_value(hash_value(
                    value,
                    &mut hasher,
                    hash_type.clone(),
                )?);
            }
        }

        Null => {
            for _ in 0..array.len() {
                hash_results.append_value(String::new());
            }
        }
        _ => {
            let actual_type = format!("{:?}", array.data_type());
            return exec_err!("Unsupported array type: {}", actual_type);
        }
    }

    Ok(hash_results.finish())
}
