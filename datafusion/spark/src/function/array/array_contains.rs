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

use arrow::array::{Array, AsArray, BooleanArray, GenericListArray, OffsetSizeTrait};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::DataType;
use datafusion_common::utils::rows_have_nulls;
use datafusion_common::{Result, exec_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions_nested::array_has::array_has_udf;
use std::sync::Arc;

/// Spark-compatible `array_contains` function.
///
/// Calls DataFusion's `array_has` and then applies Spark's null semantics:
/// - If the result from `array_has` is `true`, return `true`.
/// - If the result is `false` and the input array row contains any null elements,
///   return `null` (because the element might have been the null).
/// - If the result is `false` and the input array row has no null elements,
///   return `false`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkArrayContains {
    signature: Signature,
}

impl Default for SparkArrayContains {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArrayContains {
    pub fn new() -> Self {
        Self {
            signature: Signature::array_and_element(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkArrayContains {
    fn name(&self) -> &str {
        "array_contains"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let haystack = args.args[0].clone();
        let array_has_result = array_has_udf().invoke_with_args(args)?;

        let result_array = array_has_result.to_array(1)?;
        let patched = apply_spark_null_semantics(result_array.as_boolean(), &haystack)?;
        Ok(ColumnarValue::Array(Arc::new(patched)))
    }
}

/// For each row where `array_has` returned `false`, set the output to null
/// if that row's input array contains any null elements.
fn apply_spark_null_semantics(
    result: &BooleanArray,
    haystack_arg: &ColumnarValue,
) -> Result<BooleanArray> {
    // happy path
    if haystack_arg.data_type() == DataType::Null || !result.has_false() {
        return Ok(result.clone());
    }

    let haystack = haystack_arg.to_array_of_size(result.len())?;

    let row_has_nulls = compute_row_has_nulls(&haystack)?;

    // A row keeps its validity when result is true OR the row has no nulls.
    let keep_mask = result.values() | &!&row_has_nulls;
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
        DataType::List(_) => generic_list_row_has_nulls(haystack.as_list::<i32>()),
        DataType::LargeList(_) => generic_list_row_has_nulls(haystack.as_list::<i64>()),
        DataType::FixedSizeList(_, _) => {
            let list = haystack.as_fixed_size_list();
            let buf = match list.values().nulls() {
                Some(nulls) => {
                    let vl = list.value_length() as usize;
                    rows_have_nulls(nulls, 0, (0..list.len()).map(|row| (row + 1) * vl))
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
            let offsets = list.offsets();
            rows_have_nulls(
                nulls,
                offsets[0].as_usize(),
                offsets[1..].iter().map(|end| end.as_usize()),
            )
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
    use arrow::array::{ArrayRef, FixedSizeListArray, Int32Array, ListArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::Field;

    #[test]
    fn row_has_nulls_on_sliced_lists() -> Result<()> {
        // Every 7th element and every 6th list row are NULL.
        let values = Arc::new(Int32Array::from_iter(
            (0..300).map(|i| (i % 7 != 3).then_some(i)),
        ));
        let field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let list = ListArray::new(
            Arc::clone(&field),
            OffsetBuffer::from_lengths((0..100).map(|row| row % 5)),
            Arc::clone(&values) as ArrayRef,
            Some(NullBuffer::from_iter((0..100).map(|row| row % 6 != 0))),
        );
        let fixed = FixedSizeListArray::new(field, 3, values, None);
        for array in [Arc::new(list) as ArrayRef, Arc::new(fixed)] {
            // The first visible element is past the first 64-bit word.
            let full = compute_row_has_nulls(array.as_ref())?;
            let sliced = compute_row_has_nulls(array.slice(37, 50).as_ref())?;
            assert_eq!(sliced, full.slice(37, 50), "{}", array.data_type());
        }
        Ok(())
    }
}
