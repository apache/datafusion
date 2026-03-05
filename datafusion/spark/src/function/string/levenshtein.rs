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

use arrow::array::{Array, ArrayRef, Int32Array, OffsetSizeTrait};
use arrow::datatypes::DataType;

use datafusion_common::cast::{
    as_generic_string_array, as_int32_array, as_string_view_array,
};
use datafusion_common::types::{NativeType, logical_int32, logical_int64, logical_string};
use datafusion_common::{Result, exec_err};
use datafusion_expr::type_coercion::binary::{
    binary_to_string_coercion, string_coercion,
};
use datafusion_expr::{
    Coercion, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_functions::utils::make_scalar_function;

/// Spark-compatible `levenshtein` function.
///
/// Extends the standard 2-argument `levenshtein(str1, str2)` with an optional
/// third `threshold` parameter: `levenshtein(str1, str2, threshold)`.
/// When provided, if the Levenshtein distance exceeds the threshold, the function
/// returns -1 with early termination for better performance.
///
/// <https://spark.apache.org/docs/latest/api/sql/index.html#levenshtein>
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
                        Coercion::new_exact(TypeSignatureClass::Native(
                            logical_string(),
                        )),
                        Coercion::new_exact(TypeSignatureClass::Native(
                            logical_string(),
                        )),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(
                            logical_string(),
                        )),
                        Coercion::new_exact(TypeSignatureClass::Native(
                            logical_string(),
                        )),
                        Coercion::new_implicit(
                            TypeSignatureClass::Native(logical_int32()),
                            vec![TypeSignatureClass::Native(logical_int64())],
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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.args[0].data_type() {
            DataType::Utf8View | DataType::Utf8 => {
                make_scalar_function(levenshtein::<i32>, vec![])(&args.args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(levenshtein::<i64>, vec![])(&args.args)
            }
            other => {
                exec_err!(
                    "Unsupported data type {other:?} for function levenshtein"
                )
            }
        }
    }
}

/// Computes the Levenshtein distance between two string arrays,
/// with an optional threshold (3rd argument). If the threshold is provided
/// and the distance exceeds it, returns -1.
fn levenshtein<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let str1 = &args[0];
    let str2 = &args[1];
    let threshold_array = if args.len() == 3 {
        Some(as_int32_array(&args[2])?)
    } else {
        None
    };

    let coercion_data_type = string_coercion(str1.data_type(), str2.data_type())
        .or_else(|| binary_to_string_coercion(str1.data_type(), str2.data_type()))
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "Unsupported data types for levenshtein. Expected Utf8, LargeUtf8 or Utf8View"
                    .to_string(),
            )
        })?;

    let coerce = |arr: &ArrayRef| -> Result<ArrayRef> {
        if arr.data_type() == &coercion_data_type {
            Ok(Arc::clone(arr))
        } else {
            Ok(arrow::compute::kernels::cast::cast(arr, &coercion_data_type)?)
        }
    };
    let str1 = coerce(str1)?;
    let str2 = coerce(str2)?;

    // Reusable buffers to avoid allocating for each row
    let mut bufs = LevenshteinBuffers::default();

    let pairs: Box<dyn Iterator<Item = (Option<&str>, Option<&str>)>> =
        match coercion_data_type {
            DataType::Utf8View => {
                let str1_array = as_string_view_array(&str1)?;
                let str2_array = as_string_view_array(&str2)?;
                Box::new(str1_array.iter().zip(str2_array.iter()))
            }
            DataType::Utf8 | DataType::LargeUtf8 => {
                let str1_array = as_generic_string_array::<T>(&str1)?;
                let str2_array = as_generic_string_array::<T>(&str2)?;
                Box::new(str1_array.iter().zip(str2_array.iter()))
            }
            other => {
                return exec_err!(
                    "levenshtein was called with {other} datatype arguments. It requires Utf8View, Utf8 or LargeUtf8."
                );
            }
        };

    let result: Int32Array = pairs
        .enumerate()
        .map(|(idx, (s1, s2))| {
            compute_distance(
                s1,
                s2,
                threshold_array.as_ref().and_then(|a| {
                    if a.is_null(idx) {
                        return None;
                    }
                    Some(a.value(idx))
                }),
                threshold_array.is_some(),
                &mut bufs,
            )
        })
        .collect();
    Ok(Arc::new(result) as ArrayRef)
}

/// Reusable buffers for Levenshtein distance computation to avoid
/// per-row allocation.
#[derive(Default)]
struct LevenshteinBuffers {
    cache: Vec<usize>,
    p: Vec<i32>,
    d: Vec<i32>,
    a: Vec<char>,
    b: Vec<char>,
}

/// Computes the Levenshtein distance for a single pair of strings,
/// with an optional threshold. If the threshold is provided, uses a
/// banded DP algorithm with early termination for better performance.
fn compute_distance(
    s1: Option<&str>,
    s2: Option<&str>,
    threshold: Option<i32>,
    has_threshold_arg: bool,
    bufs: &mut LevenshteinBuffers,
) -> Option<i32> {
    // Null threshold → null output
    if has_threshold_arg && threshold.is_none() {
        return None;
    }
    match (s1, s2) {
        (Some(s1), Some(s2)) => match threshold {
            Some(t) => Some(levenshtein_with_threshold(
                s1,
                s2,
                t,
                &mut bufs.p,
                &mut bufs.d,
                &mut bufs.a,
                &mut bufs.b,
            )),
            None => {
                use datafusion_common::utils::datafusion_strsim;
                Some(
                    datafusion_strsim::levenshtein_with_buffer(
                        s1,
                        s2,
                        &mut bufs.cache,
                    ) as i32,
                )
            }
        },
        _ => None,
    }
}

/// Calculates the Levenshtein distance between two strings with a threshold.
/// If the distance exceeds the threshold, returns -1 (early termination).
///
/// Uses a banded dynamic programming approach that only computes cells
/// within `threshold` distance of the diagonal, providing better performance
/// than the full algorithm when the threshold is small relative to string lengths.
///
/// Ported from Apache Spark's `UTF8String.levenshteinDistance`, which itself is
/// based on Apache Commons Text `LevenshteinDistance.limitedCompare`.
fn levenshtein_with_threshold(
    a: &str,
    b: &str,
    threshold: i32,
    p: &mut Vec<i32>,
    d: &mut Vec<i32>,
    a_buf: &mut Vec<char>,
    b_buf: &mut Vec<char>,
) -> i32 {
    a_buf.clear();
    a_buf.extend(a.chars());
    b_buf.clear();
    b_buf.extend(b.chars());

    let (s, t, n, m) = if a_buf.len() <= b_buf.len() {
        (a_buf.as_slice(), b_buf.as_slice(), a_buf.len(), b_buf.len())
    } else {
        (b_buf.as_slice(), a_buf.as_slice(), b_buf.len(), a_buf.len())
    };
    // n <= m is guaranteed

    let threshold_usize = if threshold < 0 {
        return -1;
    } else {
        threshold as usize
    };

    if n == 0 {
        return if m <= threshold_usize { m as i32 } else { -1 };
    }
    if m - n > threshold_usize {
        return -1;
    }

    // Initialize previous row (p) with boundary values
    let size = n + 1;
    p.clear();
    p.resize(size, i32::MAX);
    d.clear();
    d.resize(size, i32::MAX);

    let boundary = n.min(threshold_usize) + 1;
    for (i, p_val) in p.iter_mut().enumerate().take(boundary) {
        *p_val = i as i32;
    }

    for (j, t_char) in t.iter().enumerate() {
        d[0] = (j + 1) as i32;

        let range_min = 1.max((j + 1).saturating_sub(threshold_usize));
        let range_max = if j + 1 > (i32::MAX as usize - threshold_usize) {
            n
        } else {
            n.min(j + 1 + threshold_usize)
        };

        if range_min > 1 {
            d[range_min - 1] = i32::MAX;
        }

        let mut lower_bound = i32::MAX;

        for i in range_min..=range_max {
            if s[i - 1] == *t_char {
                d[i] = p[i - 1];
            } else {
                d[i] = 1 + d[i - 1].min(p[i]).min(p[i - 1]);
            }
            lower_bound = lower_bound.min(d[i]);
        }

        if lower_bound > threshold {
            return -1;
        }

        std::mem::swap(p, d);
    }

    if p[n] <= threshold { p[n] } else { -1 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, StringArray};
    use datafusion_common::cast::as_int32_array;

    #[test]
    fn test_levenshtein_basic() -> Result<()> {
        let string1_array =
            Arc::new(StringArray::from(vec!["123", "abc", "xyz", "kitten"]));
        let string2_array =
            Arc::new(StringArray::from(vec!["321", "def", "zyx", "sitting"]));
        let res = levenshtein::<i32>(&[string1_array, string2_array]).unwrap();
        let result = as_int32_array(&res)?;
        let expected = Int32Array::from(vec![2, 3, 2, 3]);
        assert_eq!(&expected, result);
        Ok(())
    }

    #[test]
    fn test_levenshtein_with_threshold() -> Result<()> {
        let string1_array =
            Arc::new(StringArray::from(vec!["kitten", "kitten", "kitten"]));
        let string2_array =
            Arc::new(StringArray::from(vec!["sitting", "sitting", "sitting"]));
        let threshold_array = Arc::new(Int32Array::from(vec![5, 3, 2]));
        let res =
            levenshtein::<i32>(&[string1_array, string2_array, threshold_array])?;
        let result = as_int32_array(&res)?;
        let expected = Int32Array::from(vec![3, 3, -1]);
        assert_eq!(&expected, result);
        Ok(())
    }

    #[test]
    fn test_levenshtein_with_threshold_nulls() -> Result<()> {
        let string1_array = Arc::new(StringArray::from(vec![
            Some("kitten"),
            None,
            Some("kitten"),
        ]));
        let string2_array = Arc::new(StringArray::from(vec![
            None,
            Some("sitting"),
            Some("sitting"),
        ]));
        let threshold_array = Arc::new(Int32Array::from(vec![Some(5), Some(3), None]));
        let res =
            levenshtein::<i32>(&[string1_array, string2_array, threshold_array])?;
        let result = as_int32_array(&res)?;
        assert!(result.is_null(0)); // null str2
        assert!(result.is_null(1)); // null str1
        assert!(result.is_null(2)); // null threshold
        Ok(())
    }

    #[test]
    fn test_levenshtein_with_threshold_edge_cases() -> Result<()> {
        let string1_array =
            Arc::new(StringArray::from(vec!["", "", "abc", "same", "kitten"]));
        let string2_array =
            Arc::new(StringArray::from(vec!["", "abc", "", "same", "sitting"]));
        let threshold_array = Arc::new(Int32Array::from(vec![0, 2, 2, 0, 0]));
        let res =
            levenshtein::<i32>(&[string1_array, string2_array, threshold_array])?;
        let result = as_int32_array(&res)?;
        let expected = Int32Array::from(vec![
            0,  // "" vs "" threshold=0 → 0
            -1, // "" vs "abc" threshold=2 → -1 (distance 3 > 2)
            -1, // "abc" vs "" threshold=2 → -1 (distance 3 > 2)
            0,  // "same" vs "same" threshold=0 → 0
            -1, // "kitten" vs "sitting" threshold=0 → -1
        ]);
        assert_eq!(&expected, result);
        Ok(())
    }
}
