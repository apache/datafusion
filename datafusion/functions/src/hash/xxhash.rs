// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::array::{Array, StringArray, Int32Array, Int64Array, UInt32Array, UInt64Array};
use arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use twox_hash::{XxHash64, XxHash32};
use datafusion_macros::user_doc;
use std::any::Any;
use std::hash::Hasher;
use datafusion_common::DataFusionError;
use std::sync::Arc;

#[user_doc(
    doc_section(label = "Hashing Functions"),
    description = "Computes the XXHash64 hash of a binary string.",
    syntax_example = "xxhash64(expression)",
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
        use DataType::*;
        Self {
            signature: Signature::uniform(
                1,
                vec![Utf8View, Utf8, LargeUtf8, Binary, LargeBinary],
                Volatility::Immutable,
            ),
        }
    }

    pub fn hash_scalar(&self, value: &ColumnarValue) -> Result<String> {
        let value_str = to_string_from_scalar(value)?;
        hash_value(&value_str, XxHash64::default(), HashType::U64)
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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        _number_rows: usize,
    ) -> Result<ColumnarValue> {
        let input_data = &args[0];

        let result = match input_data {
            ColumnarValue::Array(array) => {
                let hash_results = process_array(array, XxHash64::default(), HashType::U64)?;
                let hash_array = StringArray::from(hash_results);
                Arc::new(hash_array) as Arc<dyn Array>
            },
            ColumnarValue::Scalar(scalar) => {
                let hash_result = self.hash_scalar(&ColumnarValue::Scalar(scalar.clone()))?;
                let hash_array = StringArray::from(vec![hash_result]);
                Arc::new(hash_array) as Arc<dyn Array>
            }
        };

        Ok(ColumnarValue::Array(result))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[user_doc(
    doc_section(label = "Hashing Functions"),
    description = "Computes the XXHash32 hash of a binary string.",
    syntax_example = "xxhash32(expression)",
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
        use DataType::*;
        Self {
            signature: Signature::uniform(
                1,
                vec![Utf8View, Utf8, LargeUtf8, Binary, LargeBinary],
                Volatility::Immutable,
            ),
        }
    }

    pub fn hash_scalar(&self, value: &ColumnarValue) -> Result<String> {
        let value_str = to_string_from_scalar(value)?;
        hash_value(&value_str, XxHash32::default(), HashType::U32)
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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        _number_rows: usize,
    ) -> Result<ColumnarValue> {
        let input_data = &args[0];

        let result = match input_data {
            ColumnarValue::Array(array) => {
                let hash_results = process_array(array, XxHash32::default(), HashType::U32)?;
                let hash_array = StringArray::from(hash_results);
                Arc::new(hash_array) as Arc<dyn Array>
            },
            ColumnarValue::Scalar(scalar) => {
                let hash_result = self.hash_scalar(&ColumnarValue::Scalar(scalar.clone()))?;
                let hash_array = StringArray::from(vec![hash_result]);
                Arc::new(hash_array) as Arc<dyn Array>
            }
        };

        Ok(ColumnarValue::Array(result))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

// Helper functions

fn to_string_from_scalar(value: &ColumnarValue) -> Result<String> {
    match value {
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Utf8(Some(v)) => Ok(v.clone()),
            ScalarValue::Int32(Some(v)) => Ok(v.to_string()),
            ScalarValue::Int64(Some(v)) => Ok(v.to_string()),
            ScalarValue::UInt32(Some(v)) => Ok(v.to_string()),
            ScalarValue::UInt64(Some(v)) => Ok(v.to_string()),
            _ => Err(DataFusionError::Internal("Unsupported scalar type".to_string())),
        },
        _ => Err(DataFusionError::Internal("Expected a scalar value".to_string())),
    }
}

#[derive(Clone)]
pub enum HashType {
    U32,
    U64,
}

fn hash_value<T: Hasher>(value_str: &str, mut hasher: T, hash_type: HashType) -> Result<String, DataFusionError> {
    hasher.write(value_str.as_bytes());
    let hash = hasher.finish();
    match hash_type {
        HashType::U32 => {
            let hash_u32 = hash as u32;
            Ok(hex::encode(hash_u32.to_be_bytes()))
        },
        HashType::U64 => {
            let hash_u64 = hash as u64;
            Ok(hex::encode(hash_u64.to_be_bytes()))
        },
    }
}

fn process_array<T: Hasher>(array: &dyn Array, mut hasher: T, hash_type: HashType) -> Result<Vec<String>> {
    let mut hash_results: Vec<String> = Vec::with_capacity(array.len());
    for i in 0..array.len() {
        if array.is_null(i) {
            hash_results.push(String::from("00000000")); // Handle null values
            continue;
        }

        let value_str = match array.data_type() {
            DataType::Utf8 => {
                let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                string_array.value(i).to_string()
            }
            DataType::Int32 => {
                let int_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                int_array.value(i).to_string()
            }
            DataType::Int64 => {
                let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                int_array.value(i).to_string()
            }
            DataType::UInt32 => {
                let uint_array = array.as_any().downcast_ref::<UInt32Array>().unwrap();
                uint_array.value(i).to_string()
            }
            DataType::UInt64 => {
                let uint_array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                uint_array.value(i).to_string()
            }
            _ => return Err(DataFusionError::Internal("Unsupported array type".to_string())),
        };

        hash_results.push(hash_value(&value_str, &mut hasher, hash_type.clone())?);
    }
    Ok(hash_results)
}