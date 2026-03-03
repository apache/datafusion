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

use arrow::array::{Array, AsArray, BooleanArray, BooleanBufferBuilder};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions_nested::array_has::array_has_udf;
use std::any::Any;
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
    fn as_any(&self) -> &dyn Any {
        self
    }

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
    let haystack = match haystack_arg {
        ColumnarValue::Array(arr) => Arc::clone(arr),
        ColumnarValue::Scalar(s) => s.to_array_of_size(result.len())?,
    };

    if haystack.data_type() == &DataType::Null {
        return Ok(result.clone());
    }

    // If every result is already true or null, nothing to nullify.
    if result.false_count() == 0 {
        return Ok(result.clone());
    }

    // Build a per-row bitmap: true if that row's list contains any null element.
    // Works directly on the values' null bitmap + offsets, no per-row array allocation.
    let row_has_nulls = match haystack.data_type() {
        DataType::List(_) => {
            let list = haystack.as_list::<i32>();
            if list.values().null_count() == 0 {
                return Ok(result.clone());
            }
            build_row_has_nulls(
                list.values().nulls().unwrap().inner(),
                list.offsets().iter().map(|o| *o as usize),
                list.len(),
                list.nulls(),
            )
        }
        DataType::LargeList(_) => {
            let list = haystack.as_list::<i64>();
            if list.values().null_count() == 0 {
                return Ok(result.clone());
            }
            build_row_has_nulls(
                list.values().nulls().unwrap().inner(),
                list.offsets().iter().map(|o| *o as usize),
                list.len(),
                list.nulls(),
            )
        }
        DataType::FixedSizeList(_, _) => {
            let list = haystack.as_fixed_size_list();
            if list.values().null_count() == 0 {
                return Ok(result.clone());
            }
            let vl = list.value_length() as usize;
            build_row_has_nulls(
                list.values().nulls().unwrap().inner(),
                (0..=list.len()).map(|i| i * vl),
                list.len(),
                list.nulls(),
            )
        }
        _ => return Ok(result.clone()),
    };

    // A row should be nullified when: result is false AND row has nulls.
    // nullify_mask = !result_values & row_has_nulls
    // new_validity = old_validity & !nullify_mask
    let nullify_mask = &(!result.values()) & &row_has_nulls;
    let old_validity = match result.nulls() {
        Some(n) => n.inner().clone(),
        None => BooleanBuffer::new_set(result.len()),
    };
    let new_validity = &old_validity & &(!&nullify_mask);

    Ok(BooleanArray::new(
        result.values().clone(),
        Some(NullBuffer::new(new_validity)),
    ))
}

/// Build a BooleanBuffer where bit `i` is set if list row `i` contains
/// any null element, using the flat values null bitmap and row offsets.
fn build_row_has_nulls<I>(
    values_validity: &BooleanBuffer,
    offsets: I,
    num_rows: usize,
    list_nulls: Option<&NullBuffer>,
) -> BooleanBuffer
where
    I: Iterator<Item = usize>,
{
    let mut builder = BooleanBufferBuilder::new(num_rows);
    let mut prev = None;
    for offset in offsets {
        if let Some(start) = prev {
            let len = offset - start;
            // count_set_bits on the validity bitmap = number of valid (non-null) elements
            let valid_count = values_validity.slice(start, len).count_set_bits();
            builder.append(valid_count < len);
        }
        prev = Some(offset);
    }

    let buf = builder.finish();
    // Mask out rows where the list itself is null (those shouldn't count as "has nulls").
    match list_nulls {
        Some(n) => &buf & n.inner(),
        None => buf,
    }
}
