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

//! Hash Arrays UDF: A scalar function that hashes one or more arrays using the same algorithm as DataFusion's join operations

use std::any::Any;
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{Array, UInt64Array};
use arrow::datatypes::DataType;
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::{plan_err, Result};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Hashing Functions"),
    description = "Computes hash values for one or more arrays using DataFusion's internal hash algorithm. When multiple arrays are provided, their hash values are combined using the same logic as multi-column joins.",
    syntax_example = "hash(array1 [, array2, ...])",
    sql_example = r#"```sql
-- Hash a single array
SELECT hash([1, 2, 3]);

-- Hash multiple arrays (combines their values)
SELECT hash([1, 2, 3], ['a', 'b', 'c']);

-- Hash arrays from table columns
SELECT hash(col1, col2) FROM table;
```"#,
    standard_argument(name = "array1", prefix = "First array to hash (any supported type)")
)]
#[derive(Debug)]
pub struct Hash {
    signature: Signature,
    /// RandomState for consistent hashing - using the same seed as hash joins
    random_state: RandomState,
}

impl PartialEq for Hash {
    fn eq(&self, other: &Self) -> bool {
        // RandomState doesn't implement PartialEq, so we just compare signatures
        self.signature == other.signature
    }
}

impl Eq for Hash {}

impl std::hash::Hash for Hash {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Only hash the signature since RandomState doesn't implement Hash
        self.signature.hash(state);
    }
}

impl Default for Hash {
    fn default() -> Self {
        Self::new()
    }
}

impl Hash {
    pub fn new() -> Self {
        // Use the same seed as hash joins for consistency
        let random_state = RandomState::with_seeds('H' as u64, 'A' as u64, 'S' as u64, 'H' as u64);

        Self {
            signature: Signature::one_of(
                vec![
                    // Accept any number of arrays (1 or more)
                    TypeSignature::VariadicAny,
                ],
                Volatility::Immutable,
            ),
            random_state,
        }
    }

    /// Create a new HashArraysFunc with a custom RandomState
    pub fn new_with_random_state(random_state: RandomState) -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::VariadicAny],
                Volatility::Immutable,
            ),
            random_state,
        }
    }
}

impl ScalarUDFImpl for Hash {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "hash"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // Always return UInt64Array regardless of input types
        Ok(DataType::UInt64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            return plan_err!("hash requires at least one argument");
        }

        // Convert all arguments to arrays
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;

        // Check that all arrays have the same length
        let array_len = arrays[0].len();
        for (i, array) in arrays.iter().enumerate() {
            if array.len() != array_len {
                return plan_err!(
                    "All input arrays must have the same length. Array 0 has length {}, but array {} has length {}",
                    array_len, i, array.len()
                );
            }
        }

        // If no rows, return an empty UInt64Array
        if array_len == 0 {
            return Ok(ColumnarValue::Array(Arc::new(UInt64Array::from(
                Vec::<u64>::new(),
            ))));
        }

        // Create hash buffer and compute hashes using DataFusion's internal algorithm
        let mut hashes_buffer = vec![0u64; array_len];
        create_hashes(&arrays, &self.random_state, &mut hashes_buffer)?;

        // Return the hash values as a UInt64Array
        Ok(ColumnarValue::Array(Arc::new(UInt64Array::from(
            hashes_buffer,
        ))))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, FieldRef};
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::ScalarFunctionArgs;
    use std::sync::Arc;

    fn create_test_args(args: Vec<ColumnarValue>) -> ScalarFunctionArgs {
        // For the mismatched length test, we need to use the first array's length
        // for number_rows even though the arrays have different lengths
        let array_len = if let Some(ColumnarValue::Array(first_array)) = args.first() {
            first_array.len()
        } else {
            0
        };

        let arg_fields: Vec<FieldRef> = args.iter().enumerate().map(|(i, _)| {
            Arc::new(Field::new(format!("arg{}", i), DataType::Int32, true))
        }).collect();

        let return_field = Arc::new(Field::new("result", DataType::UInt64, false));

        ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: array_len,
            return_field,
            config_options: Arc::new(ConfigOptions::new()),
        }
    }

    #[test]
    fn test_hash_single_array() {
        let func = Hash::new();
        let array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let args = create_test_args(vec![ColumnarValue::Array(array)]);

        let result = func.invoke_with_args(args).unwrap();
        if let ColumnarValue::Array(result_array) = result {
            let hash_array = result_array.as_any().downcast_ref::<UInt64Array>().unwrap();
            assert_eq!(hash_array.len(), 3);
            // Hash values should be non-zero for non-null values
            assert_ne!(hash_array.value(0), 0);
            assert_ne!(hash_array.value(1), 0);
            assert_ne!(hash_array.value(2), 0);
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_hash_multiple_arrays() {
        let func = Hash::new();
        let int_array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let str_array = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;

        let args = create_test_args(vec![
            ColumnarValue::Array(int_array),
            ColumnarValue::Array(str_array),
        ]);

        let result = func.invoke_with_args(args).unwrap();
        if let ColumnarValue::Array(result_array) = result {
            let hash_array = result_array.as_any().downcast_ref::<UInt64Array>().unwrap();
            assert_eq!(hash_array.len(), 3);
            // Hash values should be non-zero
            assert_ne!(hash_array.value(0), 0);
            assert_ne!(hash_array.value(1), 0);
            assert_ne!(hash_array.value(2), 0);
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_hash_empty_input() {
        let func = Hash::new();
        let empty_array = Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef;
        let args = create_test_args(vec![ColumnarValue::Array(empty_array)]);

        let result = func.invoke_with_args(args).unwrap();
        if let ColumnarValue::Array(result_array) = result {
            let hash_array = result_array.as_any().downcast_ref::<UInt64Array>().unwrap();
            assert_eq!(hash_array.len(), 0);
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_hash_no_arguments() {
        let func = Hash::new();
        let args = create_test_args(vec![]);

        let result = func.invoke_with_args(args);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires at least one argument"));
    }

    #[test]
    fn test_hash_mismatched_lengths() {
        let func = Hash::new();
        let array1 = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let array2 = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef; // Different length

        let args = create_test_args(vec![
            ColumnarValue::Array(array1),
            ColumnarValue::Array(array2),
        ]);

        let result = func.invoke_with_args(args);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Arguments has mixed length") ||
                error_msg.contains("All input arrays must have the same length"));
    }

    #[test]
    fn test_hash_with_nulls() {
        let func = Hash::new();
        let array = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef;
        let args = create_test_args(vec![ColumnarValue::Array(array)]);

        let result = func.invoke_with_args(args).unwrap();
        if let ColumnarValue::Array(result_array) = result {
            let hash_array = result_array.as_any().downcast_ref::<UInt64Array>().unwrap();
            assert_eq!(hash_array.len(), 3);
            // Non-null values should have non-zero hashes
            assert_ne!(hash_array.value(0), 0);
            assert_ne!(hash_array.value(2), 0);
            // Null value should have zero hash (DataFusion convention)
            assert_eq!(hash_array.value(1), 0);
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_hash_consistency() {
        let func = Hash::new();
        let array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;

        // Hash the same array multiple times
        let result1 = func.invoke_with_args(create_test_args(vec![ColumnarValue::Array(array.clone())])).unwrap();
        let result2 = func.invoke_with_args(create_test_args(vec![ColumnarValue::Array(array)])).unwrap();

        // Results should be identical
        if let (ColumnarValue::Array(arr1), ColumnarValue::Array(arr2)) = (result1, result2) {
            let hash_array1 = arr1.as_any().downcast_ref::<UInt64Array>().unwrap();
            let hash_array2 = arr2.as_any().downcast_ref::<UInt64Array>().unwrap();

            assert_eq!(hash_array1.len(), hash_array2.len());
            for i in 0..hash_array1.len() {
                assert_eq!(hash_array1.value(i), hash_array2.value(i));
            }
        } else {
            panic!("Expected array results");
        }
    }
}