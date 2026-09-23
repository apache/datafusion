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

//! [`ScalarUDFImpl`] definitions for array_compact function.

use crate::utils::{empty_list_values, make_scalar_function};
use arrow::array::types::ByteArrayType;
use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, BooleanArray, BooleanBufferBuilder,
    GenericByteArray, GenericListArray, OffsetSizeTrait, PrimitiveArray,
    downcast_primitive_array,
};
use arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer};
use arrow::compute::filter;
use arrow::datatypes::DataType::{LargeList, List, Null};
use arrow::datatypes::{DataType, FieldRef};
use datafusion_common::cast::as_generic_list_array;
use datafusion_common::utils::offset_span_len;
use datafusion_common::{Result, exec_err, utils::take_function_args};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;
use std::sync::Arc;

make_udf_expr_and_func!(
    ArrayCompact,
    array_compact,
    array,
    "removes null values from the array.",
    array_compact_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Removes null values from the array.",
    syntax_example = "array_compact(array)",
    sql_example = r#"```sql
> select array_compact([1, NULL, 2, NULL, 3]) arr;
+-----------+
| arr       |
+-----------+
| [1, 2, 3] |
+-----------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayCompact {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayCompact {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayCompact {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
            aliases: vec!["list_compact".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayCompact {
    fn name(&self) -> &str {
        "array_compact"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(array_compact_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// array_compact SQL function
fn array_compact_inner(arg: &[ArrayRef]) -> Result<ArrayRef> {
    let [input_array] = take_function_args("array_compact", arg)?;

    match &input_array.data_type() {
        List(field) => compact_list::<i32>(input_array, field),
        LargeList(field) => compact_list::<i64>(input_array, field),
        Null => Ok(Arc::clone(input_array)),
        array_type => exec_err!("array_compact does not support type '{array_type}'."),
    }
}

/// Remove null elements from each row of a list array.
///
/// Each row is a range in a shared child array. Compaction removes null child
/// values and rebuilds the row offsets, preserving null list rows.
fn compact_list<O: OffsetSizeTrait>(
    input_array: &ArrayRef,
    field: &FieldRef,
) -> Result<ArrayRef> {
    let list_array = as_generic_list_array::<O>(input_array.as_ref())?;
    let visible_len = offset_span_len(list_array.offsets());
    if visible_len == 0 || list_array.null_count() == list_array.len() {
        return Ok(Arc::clone(input_array));
    }
    // Restrict the child to the visible values before computing logical nulls,
    // which can be expensive.
    // Borrow fully visible values to avoid an Arc clone on the fast paths.
    let values = list_array.values();
    let start = list_array.offsets()[0].as_usize();
    let sliced_values = (start != 0 || visible_len != values.len())
        .then(|| values.slice(start, visible_len));
    let values = sliced_values.as_ref().unwrap_or(values);
    // Use logical nulls so element types without a validity buffer
    // (e.g. NullArray) are still treated as null.
    let Some(values_nulls) = values
        .logical_nulls()
        .filter(|nulls| nulls.null_count() != 0)
    else {
        return Ok(Arc::clone(input_array));
    };
    if values_nulls.null_count() == values_nulls.len() {
        return Ok(empty_list_values(list_array, Arc::clone(field)));
    }

    build_compacted_list(list_array, field, values.as_ref(), &values_nulls)
}

/// Copy primitive values while computing result row offsets. For other types,
/// build row offsets and a keep bitmap, then copy variable-length strings/binary
/// in valid spans or use Arrow's filter kernel for the remaining types.
///
/// `values` covers the visible range of list elements, and `values_nulls` is its
/// logical validity. List offsets still index the original element array, so
/// subtract the first list offset when indexing `values`.
fn build_compacted_list<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
    field: &FieldRef,
    values: &dyn Array,
    values_nulls: &NullBuffer,
) -> Result<ArrayRef> {
    let (offsets, new_values) = downcast_primitive_array! {
        values => Ok(compact_primitive(values, list_array, values_nulls)),
        _ => compact_non_primitive(values, list_array, values_nulls),
    }?;
    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::clone(field),
        offsets,
        new_values,
        list_array.nulls().cloned(),
    )?))
}

fn compact_primitive<T: ArrowPrimitiveType, O: OffsetSizeTrait>(
    values: &PrimitiveArray<T>,
    list_array: &GenericListArray<O>,
    values_nulls: &NullBuffer,
) -> (OffsetBuffer<O>, ArrayRef) {
    let list_offsets = list_array.offsets();
    let first_offset = list_offsets[0].as_usize();
    let mut kept_values =
        Vec::with_capacity(values_nulls.len() - values_nulls.null_count());
    let mut offsets = Vec::with_capacity(list_array.len() + 1);
    offsets.push(O::zero());
    for (row, window) in list_offsets.windows(2).enumerate() {
        if list_array.is_valid(row) {
            let start = window[0].as_usize() - first_offset;
            let end = window[1].as_usize() - first_offset;
            for i in start..end {
                if values_nulls.is_valid(i) {
                    kept_values.push(values.value(i));
                }
            }
        }
        offsets.push(O::usize_as(kept_values.len()));
    }
    // Release capacity reserved for elements hidden by NULL list rows.
    kept_values.shrink_to_fit();
    let output = PrimitiveArray::<T>::new(kept_values.into(), None)
        .with_data_type(values.data_type().clone());
    (OffsetBuffer::new(offsets.into()), Arc::new(output))
}

fn compact_non_primitive<O: OffsetSizeTrait>(
    values: &dyn Array,
    list_array: &GenericListArray<O>,
    values_nulls: &NullBuffer,
) -> Result<(OffsetBuffer<O>, ArrayRef)> {
    let list_offsets = list_array.offsets();
    let first_offset = list_offsets[0].as_usize();
    let mut offsets = Vec::with_capacity(list_array.len() + 1);
    offsets.push(O::zero());
    let mut kept_count = 0;
    // Element validity is already the keep bitmap unless NULL rows hide elements.
    let mut keep_mask =
        (list_array.null_count() != 0).then(|| BooleanBufferBuilder::new(values.len()));
    for (row, window) in list_offsets.windows(2).enumerate() {
        let start = window[0].as_usize() - first_offset;
        let len = window[1].as_usize() - window[0].as_usize();
        if list_array.is_valid(row) {
            let bit_start = values_nulls.offset() + start;
            kept_count += values_nulls.buffer().count_set_bits_offset(bit_start, len);
            if let Some(keep_mask) = &mut keep_mask {
                // Skip elements belonging to preceding NULL rows.
                keep_mask.resize(start);
                keep_mask.append_packed_range(
                    bit_start..bit_start + len,
                    values_nulls.validity(),
                );
            }
        }
        offsets.push(O::usize_as(kept_count));
    }
    let keep_mask = keep_mask
        .map(|mut keep_mask| {
            keep_mask.resize(values.len());
            keep_mask.finish()
        })
        .unwrap_or_else(|| values_nulls.inner().clone());
    let output = match values.data_type() {
        DataType::Utf8 => {
            copy_byte_spans(values.as_string::<i32>(), &keep_mask, kept_count)
        }
        DataType::LargeUtf8 => {
            copy_byte_spans(values.as_string::<i64>(), &keep_mask, kept_count)
        }
        DataType::Binary => {
            copy_byte_spans(values.as_binary::<i32>(), &keep_mask, kept_count)
        }
        DataType::LargeBinary => {
            copy_byte_spans(values.as_binary::<i64>(), &keep_mask, kept_count)
        }
        _ => filter(values, &BooleanArray::new(keep_mask, None))?,
    };
    Ok((OffsetBuffer::new(offsets.into()), output))
}

/// Copy spans of retained strings or binary values. The keep bitmap excludes
/// NULL elements, so the output does not need a validity bitmap.
fn copy_byte_spans<S: OffsetSizeTrait, T: ByteArrayType<Offset = S>>(
    values: &GenericByteArray<T>,
    keep_mask: &BooleanBuffer,
    kept_count: usize,
) -> ArrayRef {
    let source_offsets = values.value_offsets();
    let mut offsets = Vec::with_capacity(kept_count + 1);
    offsets.push(S::zero());
    // Count retained bytes first to avoid reserving space for hidden NULL payloads.
    let mut byte_len = S::zero();
    for (start, end) in keep_mask.set_slices() {
        let adjustment = byte_len - source_offsets[start];
        offsets.extend(
            source_offsets[start + 1..=end]
                .iter()
                .map(|offset| *offset + adjustment),
        );
        byte_len = source_offsets[end] + adjustment;
    }
    let mut bytes = Vec::with_capacity(byte_len.as_usize());
    for (start, end) in keep_mask.set_slices() {
        bytes.extend_from_slice(
            &values.value_data()
                [source_offsets[start].as_usize()..source_offsets[end].as_usize()],
        );
    }
    // SAFETY: the keep bitmap selects only non-null elements, copied unchanged
    // from the input. This preserves UTF-8 validity for string arrays. Rebasing
    // offsets preserves their order, and the last offset equals bytes.len().
    // The checked constructor would rescan unchanged bytes for UTF-8 validity.
    let output = unsafe {
        GenericByteArray::<T>::new_unchecked(
            OffsetBuffer::new(offsets.into()),
            bytes.into(),
            None,
        )
    };
    Arc::new(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        AsArray, BinaryArray, BooleanArray, DictionaryArray, FixedSizeListArray,
        Int32Array, ListArray, MapArray, NullArray, RunArray, StringArray,
        StringViewArray, StructArray, UInt32Array, new_empty_array,
    };
    use arrow::compute::take;
    use arrow::datatypes::{Field, Int32Type, TimeUnit};
    use datafusion_common::utils::list_values;

    #[test]
    fn test_compact_child_types() -> Result<()> {
        let input = Int32Array::from(vec![
            Some(1),
            None,
            Some(2),  // Padding before the child slice.
            Some(90), // Padding before the outer slice.
            Some(10),
            None,
            Some(20),
            Some(100),
            Some(110), // Values belonging to a null list row.
            None,
            None, // A valid list with only null elements.
            Some(30),
            None,
            Some(40),
            Some(99), // Padding after the outer slice.
        ]);
        let mut children = vec![];
        for data_type in [
            DataType::UInt64,
            DataType::Int32,
            DataType::Float64,
            DataType::Decimal256(40, 4),
            DataType::Timestamp(TimeUnit::Microsecond, Some("America/Toronto".into())),
            DataType::Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Int32)),
        ] {
            children.push(arrow::compute::cast(&input, &data_type)?);
        }
        // Include empty/multibyte strings and nonempty storage behind nulls.
        let strings = StringArray::from_iter_values(input.iter().map(|v| match v {
            Some(10) => String::new(),
            Some(v) => format!("café 🦀 {v}"),
            None => "hidden null payload".to_string(),
        }));
        let strings = StringArray::new(
            strings.offsets().clone(),
            strings.values().clone(),
            input.nulls().cloned(),
        );
        let binary = BinaryArray::from_iter(
            input.iter().map(|v| v.map(|v| [0xff, v as u8, 0x80])),
        );
        children.extend([
            Arc::new(BooleanArray::from_iter(
                input.iter().map(|v| v.map(|v| v % 3 == 0)),
            )) as ArrayRef,
            Arc::new(strings.clone()),
            arrow::compute::cast(&strings, &DataType::LargeUtf8)?,
            Arc::new(binary.clone()),
            arrow::compute::cast(&binary, &DataType::LargeBinary)?,
            arrow::compute::cast(&binary, &DataType::BinaryView)?,
            arrow::compute::cast(&binary, &DataType::FixedSizeBinary(3))?,
            Arc::new(StringViewArray::from_iter(
                input
                    .iter()
                    .map(|v| v.map(|v| format!("a longer string containing {v}"))),
            )),
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
                input.iter().map(|v| v.map(|v| vec![Some(v), None])),
            )),
            Arc::new(FixedSizeListArray::new(
                Arc::new(Field::new_list_field(DataType::Int32, true)),
                2,
                Arc::new(Int32Array::from_iter(input.iter().flat_map(|v| [v, None]))),
                input.nulls().cloned(),
            )),
            // Non-nullable struct fields may contain nulls masked by the struct.
            Arc::new(StructArray::new(
                vec![Field::new("value", DataType::Int32, false)].into(),
                vec![Arc::new(input.clone())],
                input.nulls().cloned(),
            )),
            // Non-null keys referring to null dictionary values are logical nulls.
            Arc::new(DictionaryArray::<Int32Type>::try_new(
                Int32Array::from_iter_values(0..input.len() as i32),
                Arc::new(input.clone()),
            )?),
            // NULL keys can contain arbitrary indices; their validity must not
            // be stripped before filtering the dictionary.
            Arc::new(DictionaryArray::<Int32Type>::try_new(
                Int32Array::new(
                    input
                        .iter()
                        .enumerate()
                        .map(|(i, value)| if value.is_some() { i as i32 } else { -1 })
                        .collect::<Vec<_>>()
                        .into(),
                    input.nulls().cloned(),
                ),
                Arc::new(input.clone()),
            )?),
            Arc::new(RunArray::<Int32Type>::try_new(
                &Int32Array::from_iter_values(1..=input.len() as i32),
                &input,
            )?),
        ]);
        let entries = StructArray::new(
            vec![
                Field::new("key", DataType::Int32, false),
                Field::new("value", DataType::Int32, true),
            ]
            .into(),
            vec![
                Arc::new(Int32Array::from(vec![1; input.len()])),
                Arc::new(input.clone()),
            ],
            None,
        );
        children.push(Arc::new(MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::from_repeated_length(1, input.len()),
            entries,
            input.nulls().cloned(),
            true,
        )));
        for child in children {
            // Both child validity and outer-list validity have non-zero offsets.
            let child = child.slice(3, 12);
            let field = Arc::new(
                Field::new_list_field(child.data_type().clone(), true)
                    .with_metadata([("test".to_string(), "preserved".to_string())]),
            );
            let expected_values =
                take(child.as_ref(), &UInt32Array::from(vec![1, 3, 8, 10]), None)?;
            let input = ListArray::new(
                Arc::clone(&field),
                OffsetBuffer::from_lengths([1, 3, 0, 2, 2, 3, 1]),
                child,
                Some(NullBuffer::from(vec![
                    true, true, true, false, true, true, true,
                ])),
            );
            let expected = ListArray::new(
                Arc::clone(&field),
                OffsetBuffer::from_lengths([2, 0, 0, 0, 2]),
                Arc::clone(&expected_values),
                Some(NullBuffer::from(vec![true, true, false, true, true])),
            );
            for data_type in [input.data_type().clone(), LargeList(field)] {
                let input = arrow::compute::cast(&input, &data_type)?.slice(1, 5);
                let expected = arrow::compute::cast(&expected, &data_type)?;
                let result = array_compact_inner(&[input])?;
                assert_eq!(result.data_type(), &data_type);
                assert_eq!(result.as_ref(), expected.as_ref(), "{data_type}");
                // Also check that no hidden children of null parents were copied.
                assert_eq!(
                    list_values(result.as_ref())?.as_ref(),
                    expected_values.as_ref(),
                    "{data_type}"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn test_compact_byte_spans() -> Result<()> {
        let rows = [
            (true, vec![Some("outer padding")]),
            (false, vec![Some("hidden leading")]),
            (true, vec![Some("a"), Some("")]),
            (true, vec![]),
            (true, vec![Some("bc"), None, Some("é🦀"), Some("tail")]),
            (false, vec![]),
            (false, vec![Some("hidden"), Some("hidden")]),
            (true, vec![Some(""), Some("終"), None]),
            (false, vec![Some("hidden trailing")]),
            (true, vec![Some("outer padding")]),
        ];
        let elements = std::iter::repeat_n(Some("element padding"), 3)
            .chain(rows.iter().flat_map(|(_, row)| row.iter().copied()))
            .collect::<Vec<_>>();
        // Keep bytes behind NULL strings, and start the element bitmap mid-byte.
        let strings = StringArray::from_iter_values(
            elements.iter().map(|s| s.unwrap_or("hidden NULL bytes")),
        );
        let strings = StringArray::new(
            strings.offsets().clone(),
            strings.values().clone(),
            Some(NullBuffer::from_iter(elements.iter().map(Option::is_some))),
        )
        .slice(3, elements.len() - 3);
        let expected_strings =
            StringArray::from(vec!["a", "", "bc", "é🦀", "tail", "", "終"]);
        for data_type in [
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Binary,
            DataType::LargeBinary,
        ] {
            let values = arrow::compute::cast(&strings, &data_type)?;
            let expected_values = arrow::compute::cast(&expected_strings, &data_type)?;
            let field = Arc::new(Field::new_list_field(data_type, true));
            let input = ListArray::new(
                Arc::clone(&field),
                OffsetBuffer::from_lengths(rows.iter().map(|(_, row)| row.len())),
                values,
                Some(NullBuffer::from_iter(rows.iter().map(|(valid, _)| *valid))),
            )
            .slice(1, 8);
            // The first span crosses a row boundary and an empty row. NULL rows
            // must break spans even when they contain non-NULL strings.
            let expected = ListArray::new(
                Arc::clone(&field),
                OffsetBuffer::from_lengths([0, 2, 0, 3, 0, 0, 2, 0]),
                expected_values,
                input.nulls().cloned(),
            );
            for data_type in [input.data_type().clone(), LargeList(field)] {
                let input = arrow::compute::cast(&input, &data_type)?;
                let expected = arrow::compute::cast(&expected, &data_type)?;
                let result = array_compact_inner(&[input])?;
                result.to_data().validate_full()?;
                assert_eq!(result.as_ref(), expected.as_ref());
                assert_eq!(
                    list_values(result.as_ref())?.to_data(),
                    list_values(expected.as_ref())?.to_data()
                );
            }
        }
        Ok(())
    }

    #[test]
    fn test_compact_nested_rows() -> Result<()> {
        let field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let child = ListArray::new(
            Arc::clone(&field),
            OffsetBuffer::from_repeated_length(2, 8),
            Arc::new(Int32Array::from_iter(
                (0..16).map(|i| (i % 2 == 0).then_some(i)),
            )),
            Some(NullBuffer::from(vec![
                true, true, false, true, true, true, true, true,
            ])),
        );
        for child_type in [child.data_type().clone(), LargeList(field)] {
            let child = arrow::compute::cast(&child, &child_type)?;
            let expected = take(
                child.as_ref(),
                &UInt32Array::from(vec![0, 1, 3, 4, 7]),
                None,
            )?;
            let field = Arc::new(Field::new_list_field(child_type, true));
            // An empty row separates adjacent selected children. The null
            // parent has a nonempty range of children that must be skipped.
            let input = ListArray::new(
                Arc::clone(&field),
                OffsetBuffer::from_lengths([1, 0, 2, 2, 2, 1]),
                child,
                Some(NullBuffer::from(vec![true, true, true, true, false, true])),
            );
            for data_type in [input.data_type().clone(), LargeList(field)] {
                let arg = arrow::compute::cast(&input, &data_type)?;
                let result = array_compact_inner(&[arg])?;
                assert_eq!(result.data_type(), &data_type);
                assert_eq!(result.nulls(), input.nulls());
                let result = arrow::compute::cast(&result, input.data_type())?;
                let list = result.as_list::<i32>();
                assert_eq!(list.value_offsets(), &[0, 1, 1, 2, 4, 4, 5]);
                assert_eq!(list.values().as_ref(), expected.as_ref());
            }
        }
        Ok(())
    }

    #[test]
    fn test_compact_all_null_children() -> Result<()> {
        let children: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::new_null(5)),
            Arc::new(NullArray::new(5)),
            Arc::new(DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![0; 5]),
                Arc::new(Int32Array::new_null(1)),
            )?),
            Arc::new(RunArray::<Int32Type>::try_new(
                &Int32Array::from(vec![5]),
                &Int32Array::new_null(1),
            )?),
        ];
        for child in children {
            let field = Arc::new(
                Field::new_list_field(child.data_type().clone(), true)
                    .with_metadata([("test".to_string(), "preserved".to_string())]),
            );
            let nulls = Some(NullBuffer::from(vec![true, false, true]));
            let input = ListArray::new(
                Arc::clone(&field),
                OffsetBuffer::from_lengths([2, 0, 3]),
                child,
                nulls.clone(),
            );
            let expected = ListArray::new(
                Arc::clone(&field),
                OffsetBuffer::new_zeroed(3),
                new_empty_array(field.data_type()),
                nulls.clone(),
            );
            for data_type in [input.data_type().clone(), LargeList(field)] {
                let input = arrow::compute::cast(&input, &data_type)?;
                let expected = arrow::compute::cast(&expected, &data_type)?;
                assert_eq!(array_compact_inner(&[input])?.as_ref(), expected.as_ref());
            }
        }
        Ok(())
    }

    #[test]
    fn test_compact_null_row_capacity() -> Result<()> {
        let padding = 8192;
        let values = Int32Array::from_iter(
            std::iter::repeat_n(Some(1), padding)
                .chain([Some(2), None, Some(3)])
                .chain(std::iter::repeat_n(Some(1), padding)),
        );
        for data_type in [DataType::Int32, DataType::Utf8, DataType::LargeUtf8] {
            let values = arrow::compute::cast(&values, &data_type)?;
            let expected =
                arrow::compute::cast(&Int32Array::from(vec![2, 3]), &data_type)?;
            let field = Arc::new(Field::new_list_field(data_type, true));
            let input = ListArray::new(
                Arc::clone(&field),
                OffsetBuffer::from_lengths([padding, 3, padding]),
                values,
                Some(NullBuffer::from(vec![false, true, false])),
            );
            for list_type in [input.data_type().clone(), LargeList(field)] {
                let input = arrow::compute::cast(&input, &list_type)?;
                let result = array_compact_inner(&[input])?;
                assert_eq!(list_values(result.as_ref())?.as_ref(), expected.as_ref());
                assert!(result.get_buffer_memory_size() < 1024);
            }
        }
        Ok(())
    }

    #[test]
    fn test_sliced_capacity() -> Result<()> {
        crate::utils::tests::check_sliced_list_behavior(|input| {
            array_compact_inner(std::slice::from_ref(input))
        })
    }

    #[test]
    fn test_compact_hidden_nulls_zero_copy() -> Result<()> {
        let primitive =
            Int32Array::from(vec![None, Some(10), Some(20), None, Some(30), None]);
        let dictionary = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0, 1, 2, 3, 4, 5]),
            Arc::new(primitive.clone()),
        )?;
        let runs = RunArray::<Int32Type>::try_new(
            &Int32Array::from(vec![1, 2, 3, 4, 5, 6]),
            &primitive,
        )?;
        for values in [
            Arc::new(primitive) as ArrayRef,
            Arc::new(dictionary),
            Arc::new(runs),
        ] {
            let field = Arc::new(Field::new_list_field(values.data_type().clone(), true));
            let input = ListArray::new(
                Arc::clone(&field),
                OffsetBuffer::from_lengths([1, 2, 2, 1]),
                values,
                None,
            );
            for data_type in [input.data_type().clone(), LargeList(field)] {
                // Nulls outside the selected row must not prevent the zero-copy fast path.
                let sliced = arrow::compute::cast(&input, &data_type)?.slice(1, 1);
                let result = array_compact_inner(std::slice::from_ref(&sliced))?;
                assert!(Arc::ptr_eq(&result, &sliced));
            }
        }
        Ok(())
    }
}
