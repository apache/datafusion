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

use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, OffsetSizeTrait};
use arrow::datatypes::DataType;
use datafusion_common::cast::{
    as_generic_string_array, as_int32_array, as_string_view_array,
};
use datafusion_common::types::{NativeType, logical_int32, logical_string};
use datafusion_common::utils::datafusion_strsim;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::type_coercion::binary::{
    binary_to_string_coercion, string_coercion,
};
use datafusion_expr::{
    Coercion, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    TypeSignatureClass, Volatility,
};
use datafusion_functions::utils::make_scalar_function;

/// Spark-compatible `levenshtein` function.
///
/// Differs from DataFusion core's `levenshtein` in that it supports an optional
/// third argument `threshold`. When the computed Levenshtein distance exceeds
/// the threshold, the function returns -1 instead of the actual distance.
///
/// ```sql
/// levenshtein('kitten', 'sitting')     -- returns 3
/// levenshtein('kitten', 'sitting', 2)  -- returns -1 (distance 3 > threshold 2)
/// levenshtein('kitten', 'sitting', 4)  -- returns 3  (distance 3 <= threshold 4)
/// ```
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkLevenshtein {
    signature: Signature,
}

impl Default for SparkLevenshtein {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkLevenshtein {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_implicit(
                            TypeSignatureClass::Native(logical_int32()),
                            vec![TypeSignatureClass::Integer],
                            NativeType::Int32,
                        ),
                    ]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkLevenshtein {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "levenshtein"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if let Some(coercion_data_type) = string_coercion(&arg_types[0], &arg_types[1])
            .or_else(|| binary_to_string_coercion(&arg_types[0], &arg_types[1]))
        {
            match coercion_data_type {
                DataType::LargeUtf8 => Ok(DataType::Int64),
                DataType::Utf8 | DataType::Utf8View => Ok(DataType::Int32),
                other => exec_err!(
                    "levenshtein requires Utf8, LargeUtf8 or Utf8View, got {other}"
                ),
            }
        } else {
            exec_err!(
                "Unsupported data types for levenshtein. Expected Utf8, LargeUtf8 or Utf8View"
            )
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        // Determine the coerced string type (handles mixed Utf8 + LargeUtf8)
        let coerced_type = string_coercion(&args[0].data_type(), &args[1].data_type())
            .or_else(|| {
                binary_to_string_coercion(&args[0].data_type(), &args[1].data_type())
            })
            .unwrap_or(DataType::Utf8);

        // Spark returns NULL when any scalar argument is NULL.
        let null_int = |dt: &DataType| match dt {
            DataType::LargeUtf8 => ColumnarValue::Scalar(ScalarValue::Int64(None)),
            _ => ColumnarValue::Scalar(ScalarValue::Int32(None)),
        };
        for arg in &args {
            if matches!(arg, ColumnarValue::Scalar(s) if s.is_null()) {
                return Ok(null_int(&coerced_type));
            }
        }

        match coerced_type {
            DataType::Utf8View | DataType::Utf8 => {
                make_scalar_function(spark_levenshtein::<i32>, vec![])(&args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(spark_levenshtein::<i64>, vec![])(&args)
            }
            other => {
                exec_err!("Unsupported data type {other:?} for function levenshtein")
            }
        }
    }
}

/// Spark-compatible Levenshtein distance with optional threshold.
fn spark_levenshtein<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!("levenshtein expects 2 or 3 arguments, got {}", args.len());
    }

    let str1 = &args[0];
    let str2 = &args[1];
    let threshold = if args.len() == 3 {
        Some(as_int32_array(&args[2])?)
    } else {
        None
    };

    if let Some(coercion_data_type) = string_coercion(str1.data_type(), str2.data_type())
        .or_else(|| binary_to_string_coercion(str1.data_type(), str2.data_type()))
    {
        let str1 = if str1.data_type() == &coercion_data_type {
            Arc::clone(str1)
        } else {
            arrow::compute::kernels::cast::cast(str1, &coercion_data_type)?
        };
        let str2 = if str2.data_type() == &coercion_data_type {
            Arc::clone(str2)
        } else {
            arrow::compute::kernels::cast::cast(str2, &coercion_data_type)?
        };

        match coercion_data_type {
            DataType::Utf8View => {
                let str1_array = as_string_view_array(&str1)?;
                let str2_array = as_string_view_array(&str2)?;
                let mut cache = Vec::new();

                let result = str1_array
                    .iter()
                    .zip(str2_array.iter())
                    .enumerate()
                    .map(|(i, (string1, string2))| match (string1, string2) {
                        (Some(string1), Some(string2)) => {
                            let dist = datafusion_strsim::levenshtein_with_buffer(
                                string1, string2, &mut cache,
                            ) as i32;
                            match &threshold {
                                Some(t) => {
                                    // Spark: null threshold is treated as 0
                                    let thresh =
                                        if t.is_null(i) { 0 } else { t.value(i) };
                                    if dist > thresh {
                                        Some(-1i32)
                                    } else {
                                        Some(dist)
                                    }
                                }
                                None => Some(dist),
                            }
                        }
                        _ => None,
                    })
                    .collect::<Int32Array>();
                Ok(Arc::new(result) as ArrayRef)
            }
            DataType::Utf8 => {
                let str1_array = as_generic_string_array::<T>(&str1)?;
                let str2_array = as_generic_string_array::<T>(&str2)?;
                let mut cache = Vec::new();

                let result = str1_array
                    .iter()
                    .zip(str2_array.iter())
                    .enumerate()
                    .map(|(i, (string1, string2))| match (string1, string2) {
                        (Some(string1), Some(string2)) => {
                            let dist = datafusion_strsim::levenshtein_with_buffer(
                                string1, string2, &mut cache,
                            ) as i32;
                            match &threshold {
                                Some(t) => {
                                    // Spark: null threshold is treated as 0
                                    let thresh =
                                        if t.is_null(i) { 0 } else { t.value(i) };
                                    if dist > thresh {
                                        Some(-1i32)
                                    } else {
                                        Some(dist)
                                    }
                                }
                                None => Some(dist),
                            }
                        }
                        _ => None,
                    })
                    .collect::<Int32Array>();
                Ok(Arc::new(result) as ArrayRef)
            }
            DataType::LargeUtf8 => {
                let str1_array = as_generic_string_array::<T>(&str1)?;
                let str2_array = as_generic_string_array::<T>(&str2)?;
                let mut cache = Vec::new();

                let result = str1_array
                    .iter()
                    .zip(str2_array.iter())
                    .enumerate()
                    .map(|(i, (string1, string2))| match (string1, string2) {
                        (Some(string1), Some(string2)) => {
                            let dist = datafusion_strsim::levenshtein_with_buffer(
                                string1, string2, &mut cache,
                            ) as i64;
                            match &threshold {
                                Some(t) => {
                                    let thresh =
                                        if t.is_null(i) { 0 } else { t.value(i) as i64 };
                                    if dist > thresh {
                                        Some(-1i64)
                                    } else {
                                        Some(dist)
                                    }
                                }
                                None => Some(dist),
                            }
                        }
                        _ => None,
                    })
                    .collect::<Int64Array>();
                Ok(Arc::new(result) as ArrayRef)
            }
            other => {
                exec_err!(
                    "levenshtein was called with {other} datatype arguments. It requires Utf8View, Utf8 or LargeUtf8."
                )
            }
        }
    } else {
        exec_err!(
            "Unsupported data types for levenshtein. Expected Utf8, LargeUtf8 or Utf8View"
        )
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::StringArray;
    use datafusion_common::cast::as_int32_array;

    use super::*;

    // ── Basic usage ─────────────────────────────────────────────

    #[test]
    fn test_basic_distance() -> Result<()> {
        let s1 = Arc::new(StringArray::from(vec!["kitten"]));
        let s2 = Arc::new(StringArray::from(vec!["sitting"]));
        let res = spark_levenshtein::<i32>(&[s1, s2])?;
        let result = as_int32_array(&res)?;
        assert_eq!(&Int32Array::from(vec![3]), result);
        Ok(())
    }

    #[test]
    fn test_identical_strings() -> Result<()> {
        let s1 = Arc::new(StringArray::from(vec!["hello"]));
        let s2 = Arc::new(StringArray::from(vec!["hello"]));
        let res = spark_levenshtein::<i32>(&[s1, s2])?;
        let result = as_int32_array(&res)?;
        assert_eq!(&Int32Array::from(vec![0]), result);
        Ok(())
    }

    #[test]
    fn test_empty_vs_nonempty() -> Result<()> {
        let s1 = Arc::new(StringArray::from(vec![""]));
        let s2 = Arc::new(StringArray::from(vec!["abc"]));
        let res = spark_levenshtein::<i32>(&[s1, s2])?;
        let result = as_int32_array(&res)?;
        assert_eq!(&Int32Array::from(vec![3]), result);
        Ok(())
    }

    #[test]
    fn test_both_empty() -> Result<()> {
        let s1 = Arc::new(StringArray::from(vec![""]));
        let s2 = Arc::new(StringArray::from(vec![""]));
        let res = spark_levenshtein::<i32>(&[s1, s2])?;
        let result = as_int32_array(&res)?;
        assert_eq!(&Int32Array::from(vec![0]), result);
        Ok(())
    }

    #[test]
    fn test_single_char_difference() -> Result<()> {
        let s1 = Arc::new(StringArray::from(vec!["abc"]));
        let s2 = Arc::new(StringArray::from(vec!["adc"]));
        let res = spark_levenshtein::<i32>(&[s1, s2])?;
        let result = as_int32_array(&res)?;
        assert_eq!(&Int32Array::from(vec![1]), result);
        Ok(())
    }

    #[test]
    fn test_basic_batch() -> Result<()> {
        let s1 = Arc::new(StringArray::from(vec!["123", "abc", "xyz", "kitten"]));
        let s2 = Arc::new(StringArray::from(vec!["321", "def", "zyx", "sitting"]));
        let res = spark_levenshtein::<i32>(&[s1, s2])?;
        let result = as_int32_array(&res)?;
        assert_eq!(&Int32Array::from(vec![2, 3, 2, 3]), result);
        Ok(())
    }

    // ── Threshold (3-argument form) ─────────────────────────────

    #[test]
    fn test_threshold_within() -> Result<()> {
        // distance 3 <= threshold 4 → returns 3
        let s1 = Arc::new(StringArray::from(vec!["kitten"]));
        let s2 = Arc::new(StringArray::from(vec!["sitting"]));
        let t = Arc::new(Int32Array::from(vec![4]));
        let res = spark_levenshtein::<i32>(&[s1, s2, t])?;
        let result = as_int32_array(&res)?;
        assert_eq!(&Int32Array::from(vec![3]), result);
        Ok(())
    }

    #[test]
    fn test_threshold_exceeded() -> Result<()> {
        // distance 3 > threshold 2 → returns -1
        let s1 = Arc::new(StringArray::from(vec!["kitten"]));
        let s2 = Arc::new(StringArray::from(vec!["sitting"]));
        let t = Arc::new(Int32Array::from(vec![2]));
        let res = spark_levenshtein::<i32>(&[s1, s2, t])?;
        let result = as_int32_array(&res)?;
        assert_eq!(&Int32Array::from(vec![-1]), result);
        Ok(())
    }

    #[test]
    fn test_threshold_equals_distance() -> Result<()> {
        // distance 3 == threshold 3 → returns 3 (not -1)
        let s1 = Arc::new(StringArray::from(vec!["kitten"]));
        let s2 = Arc::new(StringArray::from(vec!["sitting"]));
        let t = Arc::new(Int32Array::from(vec![3]));
        let res = spark_levenshtein::<i32>(&[s1, s2, t])?;
        let result = as_int32_array(&res)?;
        assert_eq!(&Int32Array::from(vec![3]), result);
        Ok(())
    }

    #[test]
    fn test_threshold_zero_different_strings() -> Result<()> {
        // distance 3 > threshold 0 → -1
        let s1 = Arc::new(StringArray::from(vec!["abc"]));
        let s2 = Arc::new(StringArray::from(vec!["def"]));
        let t = Arc::new(Int32Array::from(vec![0]));
        let res = spark_levenshtein::<i32>(&[s1, s2, t])?;
        let result = as_int32_array(&res)?;
        assert_eq!(&Int32Array::from(vec![-1]), result);
        Ok(())
    }

    #[test]
    fn test_threshold_zero_identical_strings() -> Result<()> {
        // distance 0 <= threshold 0 → 0
        let s1 = Arc::new(StringArray::from(vec!["abc"]));
        let s2 = Arc::new(StringArray::from(vec!["abc"]));
        let t = Arc::new(Int32Array::from(vec![0]));
        let res = spark_levenshtein::<i32>(&[s1, s2, t])?;
        let result = as_int32_array(&res)?;
        assert_eq!(&Int32Array::from(vec![0]), result);
        Ok(())
    }

    #[test]
    fn test_threshold_batch() -> Result<()> {
        // distances [2, 3, 2, 3], threshold=2 → [2, -1, 2, -1]
        let s1 = Arc::new(StringArray::from(vec!["123", "abc", "xyz", "kitten"]));
        let s2 = Arc::new(StringArray::from(vec!["321", "def", "zyx", "sitting"]));
        let t = Arc::new(Int32Array::from(vec![2, 2, 2, 2]));
        let res = spark_levenshtein::<i32>(&[s1, s2, t])?;
        let result = as_int32_array(&res)?;
        assert_eq!(&Int32Array::from(vec![2, -1, 2, -1]), result);
        Ok(())
    }

    #[test]
    fn test_threshold_boundary_batch() -> Result<()> {
        // distance=3 for both, threshold [3, 2] → [3, -1]
        let s1 = Arc::new(StringArray::from(vec!["abc", "abc"]));
        let s2 = Arc::new(StringArray::from(vec!["def", "def"]));
        let t = Arc::new(Int32Array::from(vec![3, 2]));
        let res = spark_levenshtein::<i32>(&[s1, s2, t])?;
        let result = as_int32_array(&res)?;
        assert_eq!(&Int32Array::from(vec![3, -1]), result);
        Ok(())
    }

    // ── Null handling ───────────────────────────────────────────

    #[test]
    fn test_null_first_arg() -> Result<()> {
        let s1 = Arc::new(StringArray::from(vec![None::<&str>]));
        let s2 = Arc::new(StringArray::from(vec![Some("hello")]));
        let res = spark_levenshtein::<i32>(&[s1, s2])?;
        let result = as_int32_array(&res)?;
        assert_eq!(&Int32Array::from(vec![None::<i32>]), result);
        Ok(())
    }

    #[test]
    fn test_null_second_arg() -> Result<()> {
        let s1 = Arc::new(StringArray::from(vec![Some("hello")]));
        let s2 = Arc::new(StringArray::from(vec![None::<&str>]));
        let res = spark_levenshtein::<i32>(&[s1, s2])?;
        let result = as_int32_array(&res)?;
        assert_eq!(&Int32Array::from(vec![None::<i32>]), result);
        Ok(())
    }

    #[test]
    fn test_both_null() -> Result<()> {
        let s1 = Arc::new(StringArray::from(vec![None::<&str>]));
        let s2 = Arc::new(StringArray::from(vec![None::<&str>]));
        let res = spark_levenshtein::<i32>(&[s1, s2])?;
        let result = as_int32_array(&res)?;
        assert_eq!(&Int32Array::from(vec![None::<i32>]), result);
        Ok(())
    }

    #[test]
    fn test_null_strings_batch() -> Result<()> {
        let s1 = Arc::new(StringArray::from(vec![Some("hello"), None, Some("abc")]));
        let s2 = Arc::new(StringArray::from(vec![Some("hallo"), Some("xyz"), None]));
        let res = spark_levenshtein::<i32>(&[s1, s2])?;
        let result = as_int32_array(&res)?;
        assert_eq!(&Int32Array::from(vec![Some(1), None, None]), result);
        Ok(())
    }

    #[test]
    fn test_null_threshold_treated_as_zero() -> Result<()> {
        // Spark: NULL threshold → treated as 0
        // distances: [3, 3, 0], thresholds: [NULL(→0), 1, NULL(→0)]
        // results:   [-1, -1, 0]
        let s1 = Arc::new(StringArray::from(vec!["kitten", "abc", "abc"]));
        let s2 = Arc::new(StringArray::from(vec!["sitting", "def", "abc"]));
        let t = Arc::new(Int32Array::from(vec![None, Some(1), None]));
        let res = spark_levenshtein::<i32>(&[s1, s2, t])?;
        let result = as_int32_array(&res)?;
        assert_eq!(&Int32Array::from(vec![Some(-1), Some(-1), Some(0)]), result);
        Ok(())
    }

    // ── Unicode and special characters ──────────────────────────

    #[test]
    fn test_unicode_strings() -> Result<()> {
        let s1 = Arc::new(StringArray::from(vec!["café"]));
        let s2 = Arc::new(StringArray::from(vec!["cafe"]));
        let res = spark_levenshtein::<i32>(&[s1, s2])?;
        let result = as_int32_array(&res)?;
        assert_eq!(&Int32Array::from(vec![1]), result);
        Ok(())
    }

    #[test]
    fn test_strings_with_spaces() -> Result<()> {
        let s1 = Arc::new(StringArray::from(vec!["hello world"]));
        let s2 = Arc::new(StringArray::from(vec!["hello world!"]));
        let res = spark_levenshtein::<i32>(&[s1, s2])?;
        let result = as_int32_array(&res)?;
        assert_eq!(&Int32Array::from(vec![1]), result);
        Ok(())
    }

    // ── Column expressions ──────────────────────────────────────

    #[test]
    fn test_columns_without_threshold() -> Result<()> {
        let s1 = Arc::new(StringArray::from(vec!["abc", "abc", "kitten"]));
        let s2 = Arc::new(StringArray::from(vec!["abc", "def", "sitting"]));
        let res = spark_levenshtein::<i32>(&[s1, s2])?;
        let result = as_int32_array(&res)?;
        assert_eq!(&Int32Array::from(vec![0, 3, 3]), result);
        Ok(())
    }

    #[test]
    fn test_columns_with_constant_threshold() -> Result<()> {
        // threshold=2 for all rows
        let s1 = Arc::new(StringArray::from(vec!["abc", "abc", "kitten"]));
        let s2 = Arc::new(StringArray::from(vec!["abc", "def", "sitting"]));
        let t = Arc::new(Int32Array::from(vec![2, 2, 2]));
        let res = spark_levenshtein::<i32>(&[s1, s2, t])?;
        let result = as_int32_array(&res)?;
        assert_eq!(&Int32Array::from(vec![0, -1, -1]), result);
        Ok(())
    }

    // ── Per-row threshold ───────────────────────────────────────

    #[test]
    fn test_per_row_threshold() -> Result<()> {
        // dist=3 for all, threshold varies: [2, 5, 3] → [-1, 3, 3]
        let s1 = Arc::new(StringArray::from(vec!["abc", "abc", "abc"]));
        let s2 = Arc::new(StringArray::from(vec!["def", "def", "def"]));
        let t = Arc::new(Int32Array::from(vec![2, 5, 3]));
        let res = spark_levenshtein::<i32>(&[s1, s2, t])?;
        let result = as_int32_array(&res)?;
        assert_eq!(&Int32Array::from(vec![-1, 3, 3]), result);
        Ok(())
    }

    #[test]
    fn test_per_row_null_threshold_treated_as_zero() -> Result<()> {
        // Spark: null threshold per row → treated as 0 → dist > 0 returns -1
        // ('kitten','sitting',NULL) → dist=3 > 0 → -1
        // ('abc','def',1)           → dist=3 > 1 → -1
        let s1 = Arc::new(StringArray::from(vec!["kitten", "abc"]));
        let s2 = Arc::new(StringArray::from(vec!["sitting", "def"]));
        let t = Arc::new(Int32Array::from(vec![None, Some(1)]));
        let res = spark_levenshtein::<i32>(&[s1, s2, t])?;
        let result = as_int32_array(&res)?;
        assert_eq!(&Int32Array::from(vec![Some(-1), Some(-1)]), result);
        Ok(())
    }

    // ── Mixed types ─────────────────────────────────────────────

    #[test]
    fn test_mixed_utf8_largeutf8() -> Result<()> {
        use arrow::array::LargeStringArray;
        use datafusion_common::cast::as_int64_array;
        // Utf8 + LargeUtf8 → coerces to LargeUtf8, returns Int64
        let s1: ArrayRef = Arc::new(StringArray::from(vec!["kitten"]));
        let s2: ArrayRef = Arc::new(LargeStringArray::from(vec!["sitting"]));
        let res = spark_levenshtein::<i64>(&[s1, s2])?;
        let result = as_int64_array(&res)?;
        assert_eq!(&Int64Array::from(vec![3i64]), result);
        Ok(())
    }
}
