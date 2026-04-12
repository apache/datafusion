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

//! Spark-compatible `concat_ws` function.
//!
//! Differences with DataFusion core `concat_ws`:
//! - Accepts array arguments and expands their elements
//! - Allows zero value arguments: `concat_ws(',')` → `""`
//! - Null array elements are skipped (same as null scalars)

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, GenericListArray, OffsetSizeTrait, StringBuilder,
};
use arrow::datatypes::DataType;
use datafusion_common::cast::as_generic_string_array;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

/// Spark-compatible `concat_ws` expression.
///
/// In Spark, `concat_ws(sep, a, b, ...)` joins strings with separator.
/// If any argument is an array, its elements are joined with the separator.
/// Null values (both scalar and array elements) are skipped.
/// Null separator produces null result.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkConcatWs {
    signature: Signature,
}

impl Default for SparkConcatWs {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkConcatWs {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkConcatWs {
    fn name(&self) -> &str {
        "concat_ws"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // Spark's concat_ws always returns STRING (Utf8)
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Zero value args: concat_ws(',') → ""
        if args.args.len() <= 1 {
            if args.args.is_empty() {
                return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    Some(String::new()),
                )));
            }
            // Only separator — return "" or NULL depending on separator
            return match &args.args[0] {
                ColumnarValue::Scalar(s) if s.is_null() => {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
                }
                ColumnarValue::Scalar(_) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    Some(String::new()),
                ))),
                ColumnarValue::Array(arr) => {
                    // Separator is a column: return "" for non-null, NULL for null
                    let mut builder = StringBuilder::with_capacity(arr.len(), 0);
                    for row_idx in 0..arr.len() {
                        if arr.is_null(row_idx) {
                            builder.append_null();
                        } else {
                            builder.append_value("");
                        }
                    }
                    Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
                }
            };
        }

        // Use our implementation for all cases to guarantee consistent Utf8 return type.
        // Core's concat_ws may return Utf8View which conflicts with our return_type.
        spark_concat_ws_with_arrays(&args.args, args.number_rows)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() {
            return Ok(vec![]);
        }

        let mut coerced = Vec::with_capacity(arg_types.len());
        // First arg is separator — must be string
        coerced.push(DataType::Utf8);

        for dt in &arg_types[1..] {
            match dt {
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                    coerced.push(dt.clone());
                }
                DataType::List(_) | DataType::LargeList(_) => {
                    // Keep list types as-is; elements will be extracted at runtime
                    coerced.push(dt.clone());
                }
                DataType::Null => {
                    coerced.push(DataType::Utf8);
                }
                _ => {
                    // Cast other types to string
                    coerced.push(DataType::Utf8);
                }
            }
        }

        Ok(coerced)
    }
}

/// Implementation of concat_ws that supports array arguments.
fn spark_concat_ws_with_arrays(
    args: &[ColumnarValue],
    num_rows: usize,
) -> Result<ColumnarValue> {
    // Convert all to arrays for uniform processing
    let arrays: Vec<ArrayRef> = args
        .iter()
        .map(|arg| arg.to_array(num_rows))
        .collect::<Result<Vec<_>>>()?;

    // If separator is Null type, all results are null
    if *arrays[0].data_type() == DataType::Null {
        let mut builder = StringBuilder::with_capacity(num_rows, 0);
        for _ in 0..num_rows {
            builder.append_null();
        }
        return Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef));
    }

    let mut builder = StringBuilder::new();

    for row_idx in 0..num_rows {
        // Null separator → null result
        if arrays[0].is_null(row_idx) {
            builder.append_null();
            continue;
        }

        let separator = get_string_value(&arrays[0], row_idx)?;
        let mut parts: Vec<String> = Vec::new();

        // Process remaining arguments
        for arr in &arrays[1..] {
            collect_parts(arr, row_idx, &mut parts)?;
        }

        builder.append_value(parts.join(&separator));
    }

    Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
}

/// Get a string value from an array at a given row index, supporting Utf8/LargeUtf8/Utf8View.
fn get_string_value(arr: &ArrayRef, row_idx: usize) -> Result<String> {
    match arr.data_type() {
        DataType::Utf8 => {
            let str_arr = as_generic_string_array::<i32>(arr)?;
            Ok(str_arr.value(row_idx).to_string())
        }
        DataType::LargeUtf8 => {
            let str_arr = as_generic_string_array::<i64>(arr)?;
            Ok(str_arr.value(row_idx).to_string())
        }
        DataType::Utf8View => {
            let str_arr = arr.as_string_view();
            Ok(str_arr.value(row_idx).to_string())
        }
        other => exec_err!("concat_ws separator must be a string, got {other:?}"),
    }
}

/// Collect string parts from an array at a given row index.
/// For scalar string: adds the value if non-null.
/// For list arrays: expands elements, skipping nulls.
fn collect_parts(arr: &ArrayRef, row_idx: usize, parts: &mut Vec<String>) -> Result<()> {
    if arr.is_null(row_idx) {
        return Ok(());
    }

    match arr.data_type() {
        DataType::Null => {}
        DataType::Utf8 => {
            let str_arr = as_generic_string_array::<i32>(arr)?;
            parts.push(str_arr.value(row_idx).to_string());
        }
        DataType::LargeUtf8 => {
            let str_arr = as_generic_string_array::<i64>(arr)?;
            parts.push(str_arr.value(row_idx).to_string());
        }
        DataType::Utf8View => {
            let str_arr = arr.as_string_view();
            parts.push(str_arr.value(row_idx).to_string());
        }
        DataType::List(_) => {
            collect_parts_from_list::<i32>(arr.as_list::<i32>(), row_idx, parts)?;
        }
        DataType::LargeList(_) => {
            collect_parts_from_list::<i64>(arr.as_list::<i64>(), row_idx, parts)?;
        }
        other => {
            return exec_err!("concat_ws does not support data type {other:?}");
        }
    }
    Ok(())
}

/// Collect string parts from a list array at a given row index.
fn collect_parts_from_list<O: OffsetSizeTrait>(
    arr: &GenericListArray<O>,
    row_idx: usize,
    parts: &mut Vec<String>,
) -> Result<()> {
    if arr.is_null(row_idx) {
        return Ok(());
    }

    let values = arr.value(row_idx);
    collect_string_elements(&values, parts)
}

/// Extract non-null string elements from an array.
fn collect_string_elements(values: &ArrayRef, parts: &mut Vec<String>) -> Result<()> {
    match values.data_type() {
        DataType::Utf8 => {
            let str_arr = as_generic_string_array::<i32>(values)?;
            for i in 0..str_arr.len() {
                if !str_arr.is_null(i) {
                    parts.push(str_arr.value(i).to_string());
                }
            }
        }
        DataType::LargeUtf8 => {
            let str_arr = as_generic_string_array::<i64>(values)?;
            for i in 0..str_arr.len() {
                if !str_arr.is_null(i) {
                    parts.push(str_arr.value(i).to_string());
                }
            }
        }
        DataType::Utf8View => {
            let str_arr = values.as_string_view();
            for i in 0..str_arr.len() {
                if !str_arr.is_null(i) {
                    parts.push(str_arr.value(i).to_string());
                }
            }
        }
        other => {
            return exec_err!("concat_ws array elements must be strings, got {other:?}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ListArray, StringArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::Field;
    use datafusion_common::Result;

    /// Helper to build a ListArray of strings from Vec<Option<Vec<Option<&str>>>>
    fn make_list_array(data: Vec<Option<Vec<Option<&str>>>>) -> ListArray {
        let mut offsets: Vec<i32> = vec![0];
        let mut values: Vec<Option<&str>> = Vec::new();
        let mut nulls: Vec<bool> = Vec::new();

        for item in &data {
            match item {
                Some(list) => {
                    for v in list {
                        values.push(*v);
                    }
                    offsets.push(values.len() as i32);
                    nulls.push(true);
                }
                None => {
                    offsets.push(values.len() as i32);
                    nulls.push(false);
                }
            }
        }

        let string_array = StringArray::from(values);
        let field = Arc::new(Field::new_list_field(DataType::Utf8, true));
        let null_buffer = arrow::buffer::NullBuffer::from(nulls);

        ListArray::try_new(
            field,
            OffsetBuffer::new(offsets.into()),
            Arc::new(string_array),
            Some(null_buffer),
        )
        .unwrap()
    }

    #[test]
    fn test_basic_concat_ws() -> Result<()> {
        let sep: ArrayRef = Arc::new(StringArray::from(vec![","]));
        let a: ArrayRef = Arc::new(StringArray::from(vec!["a"]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["b"]));
        let c: ArrayRef = Arc::new(StringArray::from(vec!["c"]));

        let result = spark_concat_ws_with_arrays(
            &[
                ColumnarValue::Array(sep),
                ColumnarValue::Array(a),
                ColumnarValue::Array(b),
                ColumnarValue::Array(c),
            ],
            1,
        )?;

        match result {
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(str_arr.value(0), "a,b,c");
            }
            _ => panic!("Expected array"),
        }
        Ok(())
    }

    #[test]
    fn test_null_values_skipped() -> Result<()> {
        let sep: ArrayRef = Arc::new(StringArray::from(vec![","]));
        let a: ArrayRef = Arc::new(StringArray::from(vec![Some("a")]));
        let b: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>]));
        let c: ArrayRef = Arc::new(StringArray::from(vec![Some("c")]));

        let result = spark_concat_ws_with_arrays(
            &[
                ColumnarValue::Array(sep),
                ColumnarValue::Array(a),
                ColumnarValue::Array(b),
                ColumnarValue::Array(c),
            ],
            1,
        )?;

        match result {
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(str_arr.value(0), "a,c");
            }
            _ => panic!("Expected array"),
        }
        Ok(())
    }

    #[test]
    fn test_null_separator() -> Result<()> {
        let sep: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>]));
        let a: ArrayRef = Arc::new(StringArray::from(vec![Some("a")]));

        let result = spark_concat_ws_with_arrays(
            &[ColumnarValue::Array(sep), ColumnarValue::Array(a)],
            1,
        )?;

        match result {
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
                assert!(str_arr.is_null(0));
            }
            _ => panic!("Expected array"),
        }
        Ok(())
    }

    #[test]
    fn test_with_list_array() -> Result<()> {
        let sep: ArrayRef = Arc::new(StringArray::from(vec![","]));
        let list = make_list_array(vec![Some(vec![Some("a"), Some("b"), Some("c")])]);
        let list_ref: ArrayRef = Arc::new(list);

        let result = spark_concat_ws_with_arrays(
            &[ColumnarValue::Array(sep), ColumnarValue::Array(list_ref)],
            1,
        )?;

        match result {
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(str_arr.value(0), "a,b,c");
            }
            _ => panic!("Expected array"),
        }
        Ok(())
    }

    #[test]
    fn test_with_list_nulls_skipped() -> Result<()> {
        let sep: ArrayRef = Arc::new(StringArray::from(vec![","]));
        let list = make_list_array(vec![Some(vec![Some("a"), None, Some("c")])]);
        let list_ref: ArrayRef = Arc::new(list);

        let result = spark_concat_ws_with_arrays(
            &[ColumnarValue::Array(sep), ColumnarValue::Array(list_ref)],
            1,
        )?;

        match result {
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(str_arr.value(0), "a,c");
            }
            _ => panic!("Expected array"),
        }
        Ok(())
    }

    #[test]
    fn test_mixed_scalar_and_list() -> Result<()> {
        let sep: ArrayRef = Arc::new(StringArray::from(vec![","]));
        let x: ArrayRef = Arc::new(StringArray::from(vec!["x"]));
        let list = make_list_array(vec![Some(vec![Some("a"), Some("b")])]);
        let list_ref: ArrayRef = Arc::new(list);
        let y: ArrayRef = Arc::new(StringArray::from(vec!["y"]));

        let result = spark_concat_ws_with_arrays(
            &[
                ColumnarValue::Array(sep),
                ColumnarValue::Array(x),
                ColumnarValue::Array(list_ref),
                ColumnarValue::Array(y),
            ],
            1,
        )?;

        match result {
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(str_arr.value(0), "x,a,b,y");
            }
            _ => panic!("Expected array"),
        }
        Ok(())
    }

    #[test]
    fn test_multiple_rows() -> Result<()> {
        let sep: ArrayRef = Arc::new(StringArray::from(vec![",", "-", "|"]));
        let a: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None, Some("x")]));
        let b: ArrayRef =
            Arc::new(StringArray::from(vec![Some("b"), Some("y"), Some("z")]));

        let result = spark_concat_ws_with_arrays(
            &[
                ColumnarValue::Array(sep),
                ColumnarValue::Array(a),
                ColumnarValue::Array(b),
            ],
            3,
        )?;

        match result {
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(str_arr.value(0), "a,b");
                assert_eq!(str_arr.value(1), "y");
                assert_eq!(str_arr.value(2), "x|z");
            }
            _ => panic!("Expected array"),
        }
        Ok(())
    }
}
