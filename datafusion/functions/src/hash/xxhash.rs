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

use arrow::array::{Array, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::DataType;
use datafusion_common::Result;
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
        Ok(DataType::Utf8)
    }

        fn invoke_batch(
            &self,
            args: &[ColumnarValue],
            _number_rows: usize,
        ) -> Result<ColumnarValue> {
            // Assuming that the argument is either a StringArray or BinaryArray
            let input_data = &args[0];
    
            // Collect the output hash results
            let result = match input_data {
                ColumnarValue::Array(array) => {
                    // Check if the array is a StringArray (or another type you expect)
                    if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
                        let mut hash_results: Vec<String> = Vec::with_capacity(string_array.len());
                        for i in 0..string_array.len() {
                            if !string_array.is_null(i) {
                                let value = string_array.value(i); // returns &str
                                let mut hasher = XxHash64::default();
                                hasher.write(value.as_bytes()); // as_bytes() on &str returns &[u8]
                                let hash = hasher.finish();
                                let hash_hex = hex::encode(hash.to_be_bytes());
                                hash_results.push(hash_hex);
                            } else {
                                hash_results.push(String::from("00000000")); // or handle null values differently
                            }
                        }
    
                        // Create an UInt64Array from the hash results
                        let hash_array = StringArray::from(hash_results);
                        Arc::new(hash_array) as Arc<dyn Array>
                    } else {
                        return Err(DataFusionError::Internal("Unsupported array type".to_string()));
                    }
                },
                _ => return Err(DataFusionError::Internal("Unsupported input type".to_string())),
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
        Ok(DataType::Utf8)
    }

        fn invoke_batch(
            &self,
            args: &[ColumnarValue],
            _number_rows: usize,
        ) -> Result<ColumnarValue> {
            // Assuming that the argument is either a StringArray or BinaryArray
            let input_data = &args[0];

            // Collect the output hash results
            let result = match input_data {
                ColumnarValue::Array(array) => {
                    // Check if the array is a StringArray (or another type you expect)
                    if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
                        let mut hash_results: Vec<String> = Vec::with_capacity(string_array.len());
                        for i in 0..string_array.len() {
                            if !string_array.is_null(i) {
                                let value = string_array.value(i); // returns &str
                                let mut hasher = XxHash32::default();
                                hasher.write(value.as_bytes()); // as_bytes() on &str returns &[u8]
                                let hash: u32 = hasher.finish() as u32;
                                let hash_hex = hex::encode(hash.to_be_bytes());
                                hash_results.push(hash_hex);
                            } else {
                                hash_results.push(String::from("00000000")); // or handle null values differently
                            }
                        }

                        // Create a StringArray from the hash results
                        let hash_array = StringArray::from(hash_results);
                        Arc::new(hash_array) as Arc<dyn Array>
                    } else {
                        return Err(DataFusionError::Internal("Unsupported array type".to_string()));
                    }
                },
                _ => return Err(DataFusionError::Internal("Unsupported input type".to_string())),
            };
    
            Ok(ColumnarValue::Array(result))
        }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
