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

use crate::function::conversion::cast::cast_array;
use crate::function::conversion::cast_utils::SparkCastOptions;
use arrow::array::builder::StringBuilder;
use arrow::array::{
    Array, ArrayRef, AsArray, BinaryBuilder, GenericByteArray, GenericStringArray,
    Int8Array, Int16Array, Int32Array, Int64Array, ListArray, OffsetSizeTrait,
    StringArray, StructArray,
};
use arrow::datatypes::{DataType, GenericBinaryType};
use arrow::error::ArrowError;
use datafusion_common::Result;
use datafusion_expr::ColumnarValue;
use std::sync::Arc;

// --- Int → Binary ---

macro_rules! cast_whole_num_to_binary {
    ($array:expr, $primitive_type:ty, $byte_size:expr) => {{
        let input_arr = $array
            .as_any()
            .downcast_ref::<$primitive_type>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "Expected numeric array".to_string(),
                )
            })?;

        let len = input_arr.len();
        let mut builder = BinaryBuilder::with_capacity(len, len * $byte_size);

        for i in 0..input_arr.len() {
            if input_arr.is_null(i) {
                builder.append_null();
            } else {
                builder.append_value(input_arr.value(i).to_be_bytes());
            }
        }

        Ok(Arc::new(builder.finish()) as ArrayRef)
    }};
}

pub(crate) fn cast_int_to_binary(
    array: &ArrayRef,
    from_type: &DataType,
) -> Result<ArrayRef> {
    match from_type {
        DataType::Int8 => cast_whole_num_to_binary!(array, Int8Array, 1),
        DataType::Int16 => cast_whole_num_to_binary!(array, Int16Array, 2),
        DataType::Int32 => cast_whole_num_to_binary!(array, Int32Array, 4),
        DataType::Int64 => cast_whole_num_to_binary!(array, Int64Array, 8),
        _ => datafusion_common::internal_err!(
            "Unsupported type for cast_int_to_binary: {:?}",
            from_type
        ),
    }
}

// --- Struct → Struct ---

/// Cast between struct types based on logic in
/// `org.apache.spark.sql.catalyst.expressions.Cast#castStruct`.
pub(crate) fn cast_struct_to_struct(
    array: &StructArray,
    from_type: &DataType,
    to_type: &DataType,
    cast_options: &SparkCastOptions,
) -> Result<ArrayRef> {
    match (from_type, to_type) {
        (DataType::Struct(from_fields), DataType::Struct(to_fields)) => {
            let cast_fields: Vec<ArrayRef> = from_fields
                .iter()
                .enumerate()
                .zip(to_fields.iter())
                .map(|((idx, _from), to)| {
                    let from_field = Arc::clone(array.column(idx));
                    let array_length = from_field.len();
                    let cast_result = spark_cast_columnar(
                        ColumnarValue::from(from_field),
                        to.data_type(),
                        cast_options,
                    )
                    .unwrap();
                    cast_result.to_array(array_length).unwrap()
                })
                .collect();

            Ok(Arc::new(StructArray::new(
                to_fields.clone(),
                cast_fields,
                array.nulls().cloned(),
            )))
        }
        _ => unreachable!(),
    }
}

// --- Struct → String ---

pub(crate) fn casts_struct_to_string(
    array: &StructArray,
    spark_cast_options: &SparkCastOptions,
) -> Result<ArrayRef> {
    let string_arrays: Vec<ArrayRef> = array
        .columns()
        .iter()
        .map(|arr| {
            spark_cast_columnar(
                ColumnarValue::Array(Arc::clone(arr)),
                &DataType::Utf8,
                spark_cast_options,
            )
            .and_then(|cv| cv.into_array(arr.len()))
        })
        .collect::<Result<Vec<_>>>()?;
    let string_arrays: Vec<&StringArray> =
        string_arrays.iter().map(|arr| arr.as_string()).collect();

    let mut builder = StringBuilder::with_capacity(array.len(), array.len() * 16);
    let mut str = String::with_capacity(array.len() * 16);
    for row_index in 0..array.len() {
        if array.is_null(row_index) {
            builder.append_null();
        } else {
            str.clear();
            let mut any_fields_written = false;
            str.push('{');
            for field in &string_arrays {
                if any_fields_written {
                    str.push_str(", ");
                }
                if field.is_null(row_index) {
                    str.push_str("null");
                } else {
                    str.push_str(field.value(row_index));
                }
                any_fields_written = true;
            }
            str.push('}');
            builder.append_value(&str);
        }
    }
    Ok(Arc::new(builder.finish()))
}

// --- List → String ---

pub(crate) fn cast_array_to_string(
    array: &ListArray,
    spark_cast_options: &SparkCastOptions,
) -> Result<ArrayRef> {
    let mut builder = StringBuilder::with_capacity(array.len(), array.len() * 16);
    let mut str = String::with_capacity(array.len() * 16);

    let casted_values = cast_array(
        Arc::clone(array.values()),
        &DataType::Utf8,
        spark_cast_options,
    )?;
    let string_values = casted_values
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Casted values should be StringArray");

    let offsets = array.offsets();
    for row_index in 0..array.len() {
        if array.is_null(row_index) {
            builder.append_null();
        } else {
            str.clear();
            let start = offsets[row_index] as usize;
            let end = offsets[row_index + 1] as usize;

            str.push('[');
            let mut first = true;
            for idx in start..end {
                if !first {
                    str.push_str(", ");
                }
                if string_values.is_null(idx) {
                    str.push_str("null");
                } else {
                    str.push_str(string_values.value(idx));
                }
                first = false;
            }
            str.push(']');
            builder.append_value(&str);
        }
    }
    Ok(Arc::new(builder.finish()))
}

// --- Binary → String ---

pub(crate) fn cast_binary_to_string<O: OffsetSizeTrait>(
    array: &dyn Array,
    _spark_cast_options: &SparkCastOptions,
) -> std::result::Result<ArrayRef, ArrowError> {
    let input = array
        .as_any()
        .downcast_ref::<GenericByteArray<GenericBinaryType<O>>>()
        .unwrap();

    let output_array = input
        .iter()
        .map(|value| match value {
            Some(value) => Ok(Some(cast_binary_formatter(value))),
            _ => Ok(None),
        })
        .collect::<std::result::Result<GenericStringArray<O>, ArrowError>>()?;
    Ok(Arc::new(output_array))
}

fn cast_binary_formatter(value: &[u8]) -> String {
    match String::from_utf8(value.to_vec()) {
        Ok(value) => value,
        Err(_) => unsafe { String::from_utf8_unchecked(value.to_vec()) },
    }
}

/// Helper: apply spark_cast to a ColumnarValue (used by struct/list casting).
fn spark_cast_columnar(
    arg: ColumnarValue,
    data_type: &DataType,
    cast_options: &SparkCastOptions,
) -> Result<ColumnarValue> {
    match arg {
        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(cast_array(
            array,
            data_type,
            cast_options,
        )?)),
        ColumnarValue::Scalar(scalar) => {
            let array = scalar.to_array()?;
            let scalar = datafusion_common::ScalarValue::try_from_array(
                &cast_array(array, data_type, cast_options)?,
                0,
            )?;
            Ok(ColumnarValue::Scalar(scalar))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::Field;

    #[test]
    fn test_cast_int_to_binary() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(256), None]));
        let result = cast_int_to_binary(&array, &DataType::Int32).unwrap();
        let binary = result
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .unwrap();
        assert_eq!(binary.value(0), &[0, 0, 0, 1]);
        assert_eq!(binary.value(1), &[0, 0, 1, 0]);
        assert!(binary.is_null(2));
    }

    #[test]
    fn test_cast_struct_to_utf8() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(2), None]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let c: ArrayRef = Arc::new(StructArray::from(vec![
            (Arc::new(Field::new("a", DataType::Int32, true)), a),
            (Arc::new(Field::new("b", DataType::Utf8, true)), b),
        ]));
        let cast_opts = SparkCastOptions::new(
            crate::function::conversion::cast_utils::EvalMode::Legacy,
            "UTC",
        );
        let string_array = casts_struct_to_string(c.as_struct(), &cast_opts).unwrap();
        let string_array = string_array.as_string::<i32>();
        assert_eq!(string_array.value(0), "{1, a}");
        assert_eq!(string_array.value(1), "{2, b}");
        assert_eq!(string_array.value(2), "{null, c}");
    }

    #[test]
    fn test_cast_list_to_string() {
        let values_array = StringArray::from(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            Some("a"),
            None,
            None,
        ]);
        let offsets_buffer = OffsetBuffer::<i32>::new(vec![0, 3, 5, 6, 6].into());
        let item_field = Arc::new(Field::new("item", DataType::Utf8, true));
        let list_array = Arc::new(ListArray::new(
            item_field,
            offsets_buffer,
            Arc::new(values_array),
            None,
        ));
        let cast_opts = SparkCastOptions::new(
            crate::function::conversion::cast_utils::EvalMode::Legacy,
            "UTC",
        );
        let string_array = cast_array_to_string(&list_array, &cast_opts).unwrap();
        let string_array = string_array.as_string::<i32>();
        assert_eq!(string_array.value(0), "[a, b, c]");
        assert_eq!(string_array.value(1), "[a, null]");
        assert_eq!(string_array.value(2), "[null]");
        assert_eq!(string_array.value(3), "[]");
    }

    #[test]
    fn test_cast_i32_list_to_string() {
        let values_array =
            Int32Array::from(vec![Some(1), Some(2), Some(3), Some(1), None, None]);
        let offsets_buffer = OffsetBuffer::<i32>::new(vec![0, 3, 5, 6, 6].into());
        let item_field = Arc::new(Field::new("item", DataType::Int32, true));
        let list_array = Arc::new(ListArray::new(
            item_field,
            offsets_buffer,
            Arc::new(values_array),
            None,
        ));
        let cast_opts = SparkCastOptions::new(
            crate::function::conversion::cast_utils::EvalMode::Legacy,
            "UTC",
        );
        let string_array = cast_array_to_string(&list_array, &cast_opts).unwrap();
        let string_array = string_array.as_string::<i32>();
        assert_eq!(string_array.value(0), "[1, 2, 3]");
        assert_eq!(string_array.value(1), "[1, null]");
        assert_eq!(string_array.value(2), "[null]");
        assert_eq!(string_array.value(3), "[]");
    }
}
