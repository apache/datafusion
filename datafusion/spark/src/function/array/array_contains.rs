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

use arrow::array::{Array, AsArray, BooleanArray};
use arrow::buffer::NullBuffer;
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
        let array_has_result = array_has_udf().invoke_with_args(args.clone())?;
        let first_arg = &args.args[0];

        let result_array = array_has_result.to_array(args.number_rows)?;
        let patched = apply_spark_null_semantics(result_array.as_boolean(), first_arg)?;
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

    // For each row i, check if the list at row i has any null elements.
    // If result[i] is false and the list has nulls, set output[i] to null.
    let row_has_nulls: Box<dyn Fn(usize) -> bool> = match haystack.data_type() {
        DataType::List(_) => {
            let list = haystack.as_list::<i32>();
            Box::new(move |i| !list.is_null(i) && list.value(i).null_count() > 0)
        }
        DataType::LargeList(_) => {
            let list = haystack.as_list::<i64>();
            Box::new(move |i| !list.is_null(i) && list.value(i).null_count() > 0)
        }
        DataType::FixedSizeList(_, _) => {
            let list = haystack.as_fixed_size_list();
            Box::new(move |i| !list.is_null(i) && list.value(i).null_count() > 0)
        }
        _ => return Ok(result.clone()),
    };

    let validity: Vec<bool> = (0..result.len())
        .map(|i| {
            if result.is_null(i) {
                return false;
            }
            if !result.value(i) && row_has_nulls(i) {
                return false;
            }
            true
        })
        .collect();

    Ok(BooleanArray::new(
        result.values().clone(),
        Some(NullBuffer::from(validity)),
    ))
}
