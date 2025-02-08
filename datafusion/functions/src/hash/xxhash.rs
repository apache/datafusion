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

use arrow::array::{
    Array, AsArray, BinaryArray, LargeBinaryArray, LargeStringArray, StringArray,
    StringBuilder,
};
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{Binary, Int64, LargeBinary, LargeUtf8, Utf8, Utf8View};
use datafusion_common::{exec_err, internal_err, plan_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;
use std::hash::Hasher;
use std::sync::Arc;
use twox_hash::{XxHash32, XxHash64};

#[user_doc(
    doc_section(label = "Hashing Functions"),
    description = "Computes the XXHash32 hash of a binary string.",
    syntax_example = "xxhash32(expression [,seed])",
    sql_example = r#"```sql
> select xxhash32('foo');
+-------------------------------------------+
| xxhash32(Utf8("foo"))                     |
+-------------------------------------------+
| <xxhash32_result>                         |
+-------------------------------------------+
```"#,
    standard_argument(name = "expression", prefix = "String")
)]
#[derive(Debug)]
pub struct XxHash32Func {
    signature: Signature,
}

impl Default for XxHash32Func {
    fn default() -> Self {
        Self::new()
    }
}

impl XxHash32Func {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![Utf8View]),
                    TypeSignature::Exact(vec![Utf8]),
                    TypeSignature::Exact(vec![LargeUtf8]),
                    TypeSignature::Exact(vec![Binary]),
                    TypeSignature::Exact(vec![LargeBinary]),
                    TypeSignature::Exact(vec![Utf8View, Int64]),
                    TypeSignature::Exact(vec![Utf8, Int64]),
                    TypeSignature::Exact(vec![LargeUtf8, Int64]),
                    TypeSignature::Exact(vec![Binary, Int64]),
                    TypeSignature::Exact(vec![LargeBinary, Int64]),
                ],
                Volatility::Immutable,
            ),
        }
    }

    pub fn hash_scalar(&self, value: &[u8]) -> Result<String> {
        // let value_str = to_string_from_scalar(value)?;
        hash_value(value, XxHash32::default(), HashType::U32)
    }
}

impl ScalarUDFImpl for XxHash32Func {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "xxhash32"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;
        Ok(match &arg_types[0] {
            LargeUtf8 | LargeBinary => Utf8,
            Utf8View | Utf8 | Binary => Utf8,
            Null => Null,
            Dictionary(_, t) => match **t {
                LargeUtf8 | LargeBinary => Utf8,
                Utf8 | Binary => Utf8,
                Null => Null,
                _ => {
                    return plan_err!(
                        "The xxhash32 can only accept Utf8, Utf8View, LargeUtf8, Binary and LargeBinary but got {:?}",
                        **t
                    );
                }
            },
            other => {
                return plan_err!(
                    "The xxhash32 can only accept Utf8, Utf8View, LargeUtf8, Binary and LargeBinary but got {other}"
                );
            }
        })
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        _number_rows: usize,
    ) -> Result<ColumnarValue> {
        let input_data = &args[0];

        let seed = if args.len() > 1 {
            if let ColumnarValue::Scalar(ScalarValue::Int64(Some(seed))) = &args[1] {
                if *seed >= 0 && *seed <= u32::MAX as i64 {
                    *seed as u32
                } else {
                    return exec_err!("Seed value out of range for UInt32: {}", seed);
                }
            } else {
                let actual_type = format!("{:?}", &args[1]);
                return exec_err!("Expected a Int64 seed value, but got {}", actual_type);
            }
        } else {
            0 // Default seed value
        };

        let result = match input_data {
            ColumnarValue::Array(array) => {
                let hash_results =
                    process_array(array, XxHash32::with_seed(seed), HashType::U32)?;
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
                        XxHash32::with_seed(seed),
                        HashType::U32,
                    )?;
                    let hash_array = StringArray::from(vec![hash_result]);
                    Arc::new(hash_array) as Arc<dyn Array>
                }
                ScalarValue::Binary(Some(ref v))
                | ScalarValue::LargeBinary(Some(ref v)) => {
                    let hash_result =
                        hash_value(v, XxHash32::with_seed(seed), HashType::U32)?;
                    let hash_array = StringArray::from(vec![hash_result]);
                    Arc::new(hash_array) as Arc<dyn Array>
                }
                _ => {
                    return internal_err!("Unsupported scalar type: {:?}", scalar);
                }
            },
        };

        Ok(ColumnarValue::Array(result))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[user_doc(
    doc_section(label = "Hashing Functions"),
    description = "Computes the XXHash64 hash of a binary string.",
    syntax_example = "xxhash64(expression [,seed])",
    sql_example = r#"```sql
> select xxhash64('foo');
+-------------------------------------------+
| xxhash64(Utf8("foo"))                     |
+-------------------------------------------+
| <xxhash64_result>                         |
+-------------------------------------------+
```"#,
    standard_argument(name = "expression", prefix = "String")
)]
#[derive(Debug)]
pub struct XxHash64Func {
    signature: Signature,
}

impl Default for XxHash64Func {
    fn default() -> Self {
        Self::new()
    }
}

impl XxHash64Func {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![Utf8View]),
                    TypeSignature::Exact(vec![Utf8]),
                    TypeSignature::Exact(vec![LargeUtf8]),
                    TypeSignature::Exact(vec![Binary]),
                    TypeSignature::Exact(vec![LargeBinary]),
                    TypeSignature::Exact(vec![Utf8View, Int64]),
                    TypeSignature::Exact(vec![Utf8, Int64]),
                    TypeSignature::Exact(vec![LargeUtf8, Int64]),
                    TypeSignature::Exact(vec![Binary, Int64]),
                    TypeSignature::Exact(vec![LargeBinary, Int64]),
                ],
                Volatility::Immutable,
            ),
        }
    }

    pub fn hash_scalar(&self, value: &[u8]) -> Result<String> {
        // let value_str = to_string_from_scalar(value)?;
        hash_value(value, XxHash64::default(), HashType::U64)
    }
}

impl ScalarUDFImpl for XxHash64Func {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "xxhash64"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;
        Ok(match &arg_types[0] {
            LargeUtf8 | LargeBinary => Utf8,
            Utf8View | Utf8 | Binary => Utf8,
            Null => Null,
            Dictionary(_, t) => match **t {
                LargeUtf8 | LargeBinary => Utf8,
                Utf8 | Binary => Utf8,
                Null => Null,
                _ => {
                    return plan_err!(
                        "The xxhash64 can only accept Utf8, Utf8View, LargeUtf8, Binary and LargeBinary but got {:?}",
                        **t
                    );
                }
            },
            other => {
                return plan_err!(
                    "The xxhash64 can only accept Utf8, Utf8View, LargeUtf8, Binary and LargeBinary but {other}"
                );
            }
        })
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        _number_rows: usize,
    ) -> Result<ColumnarValue> {
        let input_data = &args[0];

        let seed = if args.len() > 1 {
            if let ColumnarValue::Scalar(ScalarValue::Int64(Some(seed))) = &args[1] {
                if *seed >= 0 {
                    *seed as u64
                } else {
                    return exec_err!("Seed value out of range for UInt64: {}", seed);
                }
            } else {
                let actual_type = format!("{:?}", &args[1]);
                return exec_err!("Expected a Int64 seed value, but got {}", actual_type);
            }
        } else {
            0 // Default seed value
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

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

// Helper functions

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

        DataType::Null => {
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
