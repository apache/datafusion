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
    Array, AsArray, BooleanArray, BooleanBufferBuilder, GenericListArray, OffsetSizeTrait,
};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::DataType;
use datafusion_common::{Result, exec_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions_nested::array_has::array_has_any_udf;
use std::any::Any;
use std::sync::Arc;

/// Spark-compatible `arrays_overlap` function.
///
/// Wraps DataFusion's `array_has_any` and applies Spark's three-valued null logic:
/// - If `array_has_any` returns `true`, return `true` (definite overlap found).
/// - If `array_has_any` returns `false` and either input array contains null elements,
///   return `null` (overlap is unknown because nulls could match).
/// - If `array_has_any` returns `false` and neither array contains null elements,
///   return `false` (definitively no overlap).
///
/// DataFusion's built-in `array_has_any` does not implement three-valued null logic —
/// it returns `false` instead of `null` when arrays contain nulls with no definite match.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkArraysOverlap {
    signature: Signature,
}

impl Default for SparkArraysOverlap {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArraysOverlap {
    pub fn new() -> Self {
        Self {
            signature: Signature::arrays(2, None, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkArraysOverlap {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "arrays_overlap"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let left = args.args[0].clone();
        let right = args.args[1].clone();

        // Delegate to DataFusion's array_has_any for the core comparison
        let has_any_result = array_has_any_udf().invoke_with_args(args)?;

        let result_array = has_any_result.to_array(1)?;
        let result = result_array.as_boolean();

        // If all results are true or null, no patching needed
        if result.false_count() == 0 {
            return Ok(ColumnarValue::Array(Arc::new(result.clone())));
        }

        let patched = apply_spark_overlap_null_semantics(result, &left, &right)?;
        Ok(ColumnarValue::Array(Arc::new(patched)))
    }
}

/// For each row where `array_has_any` returned `false`, set the output to null
/// if either input array contains any null elements.
fn apply_spark_overlap_null_semantics(
    result: &BooleanArray,
    left: &ColumnarValue,
    right: &ColumnarValue,
) -> Result<BooleanArray> {
    let len = result.len();
    let left_arr = left.to_array_of_size(len)?;
    let right_arr = right.to_array_of_size(len)?;

    let left_has_nulls = compute_row_has_nulls(left_arr.as_ref())?;
    let right_has_nulls = compute_row_has_nulls(right_arr.as_ref())?;
    let either_has_nulls = &left_has_nulls | &right_has_nulls;

    // A row keeps its validity when the result is true OR neither array has nulls.
    // When result is false AND either array has nulls, validity is cleared (output = null).
    let keep_mask = result.values() | &!&either_has_nulls;
    let new_validity = match result.nulls() {
        Some(n) => n.inner() & &keep_mask,
        None => keep_mask,
    };

    Ok(BooleanArray::new(
        result.values().clone(),
        Some(NullBuffer::new(new_validity)),
    ))
}

/// Returns a per-row bitmap where bit i is set if row i's list contains any null element.
fn compute_row_has_nulls(haystack: &dyn Array) -> Result<BooleanBuffer> {
    match haystack.data_type() {
        DataType::Null => Ok(BooleanBuffer::new_unset(haystack.len())),
        DataType::List(_) => generic_list_row_has_nulls(haystack.as_list::<i32>()),
        DataType::LargeList(_) => generic_list_row_has_nulls(haystack.as_list::<i64>()),
        DataType::FixedSizeList(_, _) => {
            let list = haystack.as_fixed_size_list();
            let buf = match list.values().nulls() {
                Some(nulls) => {
                    let validity = nulls.inner();
                    let vl = list.value_length() as usize;
                    let mut builder = BooleanBufferBuilder::new(list.len());
                    for i in 0..list.len() {
                        builder.append(validity.slice(i * vl, vl).count_set_bits() < vl);
                    }
                    builder.finish()
                }
                None => BooleanBuffer::new_unset(list.len()),
            };
            Ok(mask_with_list_nulls(buf, list.nulls()))
        }
        dt => exec_err!("compute_row_has_nulls: unsupported data type {dt}"),
    }
}

/// Computes per-row null presence for `List` and `LargeList` arrays.
fn generic_list_row_has_nulls<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
) -> Result<BooleanBuffer> {
    let buf = match list.values().nulls() {
        Some(nulls) => {
            let validity = nulls.inner();
            let offsets = list.offsets();
            let mut builder = BooleanBufferBuilder::new(list.len());
            for i in 0..list.len() {
                let s = offsets[i].as_usize();
                let len = offsets[i + 1].as_usize() - s;
                builder.append(validity.slice(s, len).count_set_bits() < len);
            }
            builder.finish()
        }
        None => BooleanBuffer::new_unset(list.len()),
    };
    Ok(mask_with_list_nulls(buf, list.nulls()))
}

/// Rows where the list itself is null should not be marked as "has nulls".
fn mask_with_list_nulls(
    buf: BooleanBuffer,
    list_nulls: Option<&NullBuffer>,
) -> BooleanBuffer {
    match list_nulls {
        Some(n) => &buf & n.inner(),
        None => buf,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, ListArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::Field;
    use datafusion_common::config::ConfigOptions;

    fn make_list_array(data: Vec<Option<Vec<Option<i32>>>>) -> ListArray {
        let mut values = Vec::new();
        let mut offsets = vec![0i32];
        let mut list_nulls = Vec::new();

        for row in &data {
            match row {
                Some(elements) => {
                    for elem in elements {
                        values.push(*elem);
                    }
                    offsets.push(values.len() as i32);
                    list_nulls.push(true);
                }
                None => {
                    offsets.push(*offsets.last().unwrap());
                    list_nulls.push(false);
                }
            }
        }

        let values_array = Int32Array::from(values);
        let field = Arc::new(Field::new_list_field(DataType::Int32, true));

        ListArray::new(
            field,
            OffsetBuffer::new(offsets.into()),
            Arc::new(values_array),
            Some(NullBuffer::new(list_nulls.into())),
        )
    }

    fn invoke_arrays_overlap(left: ListArray, right: ListArray) -> Result<BooleanArray> {
        let len = left.len();
        let left_field = Arc::new(Field::new("left", left.data_type().clone(), true));
        let right_field = Arc::new(Field::new("right", right.data_type().clone(), true));
        let return_field = Arc::new(Field::new("return", DataType::Boolean, true));

        let result = SparkArraysOverlap::new().invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(left)),
                ColumnarValue::Array(Arc::new(right)),
            ],
            arg_fields: vec![left_field, right_field],
            number_rows: len,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        })?;

        let arr = result.into_array(len)?;
        Ok(arr.as_boolean().clone())
    }

    #[test]
    fn test_definite_overlap() -> Result<()> {
        // [1, 2] and [2, 3] -> true (definite overlap on 2)
        let left = make_list_array(vec![Some(vec![Some(1), Some(2)])]);
        let right = make_list_array(vec![Some(vec![Some(2), Some(3)])]);
        let result = invoke_arrays_overlap(left, right)?;
        assert!(!result.is_null(0));
        assert!(result.value(0));
        Ok(())
    }

    #[test]
    fn test_no_overlap_no_nulls() -> Result<()> {
        // [1, 2] and [3, 4] -> false (no overlap, no nulls)
        let left = make_list_array(vec![Some(vec![Some(1), Some(2)])]);
        let right = make_list_array(vec![Some(vec![Some(3), Some(4)])]);
        let result = invoke_arrays_overlap(left, right)?;
        assert!(!result.is_null(0));
        assert!(!result.value(0));
        Ok(())
    }

    #[test]
    fn test_no_overlap_with_null_in_left() -> Result<()> {
        // [1, NULL] and [3] -> null (no definite overlap, but left has null)
        let left = make_list_array(vec![Some(vec![Some(1), None])]);
        let right = make_list_array(vec![Some(vec![Some(3)])]);
        let result = invoke_arrays_overlap(left, right)?;
        assert!(
            result.is_null(0),
            "Expected null but got {:?}",
            result.value(0)
        );
        Ok(())
    }

    #[test]
    fn test_no_overlap_with_null_in_right() -> Result<()> {
        // [1, 2] and [3, NULL] -> null (no definite overlap, but right has null)
        let left = make_list_array(vec![Some(vec![Some(1), Some(2)])]);
        let right = make_list_array(vec![Some(vec![Some(3), None])]);
        let result = invoke_arrays_overlap(left, right)?;
        assert!(
            result.is_null(0),
            "Expected null but got {:?}",
            result.value(0)
        );
        Ok(())
    }

    #[test]
    fn test_overlap_with_nulls_in_both() -> Result<()> {
        // [1, NULL] and [1, 3] -> true (definite overlap on 1, null doesn't matter)
        let left = make_list_array(vec![Some(vec![Some(1), None])]);
        let right = make_list_array(vec![Some(vec![Some(1), Some(3)])]);
        let result = invoke_arrays_overlap(left, right)?;
        assert!(!result.is_null(0));
        assert!(result.value(0));
        Ok(())
    }

    #[test]
    fn test_null_list() -> Result<()> {
        // NULL and [1, 2] -> null (whole list is null)
        let left = make_list_array(vec![None]);
        let right = make_list_array(vec![Some(vec![Some(1), Some(2)])]);
        let result = invoke_arrays_overlap(left, right)?;
        assert!(result.is_null(0));
        Ok(())
    }

    #[test]
    fn test_multiple_rows() -> Result<()> {
        let left = make_list_array(vec![
            Some(vec![Some(1), Some(2)]), // row 0: no overlap, no nulls -> false
            Some(vec![Some(1), None]),    // row 1: no overlap, has null -> null
            Some(vec![Some(1), Some(2)]), // row 2: overlap on 2 -> true
            None,                         // row 3: null list -> null
            Some(vec![Some(1), None]),    // row 4: overlap on 1, has null -> true
        ]);
        let right = make_list_array(vec![
            Some(vec![Some(3), Some(4)]), // row 0
            Some(vec![Some(3), Some(4)]), // row 1
            Some(vec![Some(2), Some(3)]), // row 2
            Some(vec![Some(1)]),          // row 3
            Some(vec![Some(1), Some(3)]), // row 4
        ]);

        let result = invoke_arrays_overlap(left, right)?;

        // row 0: false
        assert!(!result.is_null(0));
        assert!(!result.value(0));

        // row 1: null
        assert!(
            result.is_null(1),
            "Row 1: expected null but got {:?}",
            result.value(1)
        );

        // row 2: true
        assert!(!result.is_null(2));
        assert!(result.value(2));

        // row 3: null (list-level null)
        assert!(result.is_null(3));

        // row 4: true (definite overlap trumps null)
        assert!(!result.is_null(4));
        assert!(result.value(4));

        Ok(())
    }
}
