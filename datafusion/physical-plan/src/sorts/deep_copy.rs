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

//! Copies of record batch ranges that keep only the rows of the range: a slice of
//! an array shares the buffers of the whole array, and keeps all of them alive.

use arrow::array::RecordBatch;

/// The rows `start..start + len` of `batch` in buffers of their own, holding only
/// the values those rows reference
pub(crate) fn deep_copy_batch(
    batch: &RecordBatch,
    start: usize,
    len: usize,
) -> RecordBatch {
    let batch = batch.slice(start, len);
    let (schema, columns, _row_count) = batch.into_parts();
    let cols = columns
        .into_iter()
        .map(|col| deep_copy_array::deep_copy_array(col))
        .collect();

    unsafe { RecordBatch::new_unchecked(schema, cols, len) }
}

mod deep_copy_array {
    use arrow::array::{
        Array, ArrayRef, ArrowPrimitiveType, AsArray, BooleanArray, GenericByteArray,
        GenericListArray, MapArray, OffsetSizeTrait, PrimitiveArray, StructArray,
        make_array,
    };
    use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow::datatypes::{ByteArrayType, DataType};
    use arrow::downcast_primitive_array;
    use std::sync::Arc;

    pub fn deep_copy_array(array: ArrayRef) -> ArrayRef {
        {
            let array_r = &array;

            // This needs a ref, so we bound it to here so we can try to change ownership
            downcast_primitive_array!(
                array_r => {
                    let array_r = array_r.clone();
                    drop(array);

                    let copied = deep_copy_primitive(array_r);

                    return Arc::new(copied) as ArrayRef
                }
                _ => {

                }
            )
        }

        match array.data_type() {
            DataType::Boolean => {
                let array = array.as_boolean().clone();

                let copied = deep_copy_boolean(array);

                Arc::new(copied) as ArrayRef
            }
            DataType::Utf8 => {
                let array = array.as_string::<i32>().clone();

                let copied = deep_copy_byte_array(array);

                Arc::new(copied) as ArrayRef
            }
            DataType::LargeUtf8 => {
                let array = array.as_string::<i64>().clone();

                let copied = deep_copy_byte_array(array);

                Arc::new(copied) as ArrayRef
            }
            DataType::Binary => {
                let array = array.as_binary::<i32>().clone();

                let copied = deep_copy_byte_array(array);

                Arc::new(copied) as ArrayRef
            }
            DataType::LargeBinary => {
                let array = array.as_binary::<i64>().clone();

                let copied = deep_copy_byte_array(array);

                Arc::new(copied) as ArrayRef
            }
            DataType::Struct(_) => {
                let array = array.as_struct().clone();

                let copied = deep_copy_struct(array);

                Arc::new(copied) as ArrayRef
            }
            DataType::List(_) => {
                let array = array.as_list::<i32>().clone();

                let copied = deep_copy_list(array);

                Arc::new(copied) as ArrayRef
            }
            DataType::LargeList(_) => {
                let array = array.as_list::<i64>().clone();

                let copied = deep_copy_list(array);

                Arc::new(copied) as ArrayRef
            }
            DataType::Map(_, _) => {
                let array = array.as_map().clone();

                let copied = deep_copy_map(array);

                Arc::new(copied) as ArrayRef
            }
            _ => fallback_deep_copy_array(&array),
        }
    }

    fn deep_copy_primitive<T: ArrowPrimitiveType>(
        arr: PrimitiveArray<T>,
    ) -> PrimitiveArray<T> {
        let (dt, values, nulls) = arr.into_parts();

        let mut values = values
            .into_inner()
            .into_vec::<T::Native>()
            .unwrap_or_else(|e| e.typed_data::<T::Native>().to_vec());

        values.shrink_to_fit();

        let values = ScalarBuffer::<T::Native>::from(values);

        let nulls = deep_copy_null_buffer(nulls);

        // SAFETY: we copied the entire data as is
        unsafe { PrimitiveArray::<T>::new_unchecked(values, nulls).with_data_type(dt) }
    }

    fn deep_copy_struct(arr: StructArray) -> StructArray {
        let (fields, values, nulls) = arr.into_parts();

        let values = values
            .into_iter()
            .map(|v| deep_copy_array(v))
            .collect::<Vec<_>>();

        let nulls = deep_copy_null_buffer(nulls);

        // SAFETY: we copied the entire data as is
        unsafe { StructArray::new_unchecked(fields, values, nulls) }
    }

    fn deep_copy_list<O: OffsetSizeTrait>(
        arr: GenericListArray<O>,
    ) -> GenericListArray<O> {
        let (field, offsets, child, nulls) = arr.into_parts();

        // A sliced list keeps its whole child, copy only the referenced values
        let (start, len) = offsets_range(&offsets);
        let child = deep_copy_array(child.slice(start, len));
        let offsets = deep_copy_offset_buffer(&offsets);
        let nulls = deep_copy_null_buffer(nulls);

        // SAFETY: we copied the entire data as is
        unsafe { GenericListArray::<O>::new_unchecked(field, offsets, child, nulls) }
    }
    fn deep_copy_map(arr: MapArray) -> MapArray {
        let (field, offsets, entries, nulls, ordered) = arr.into_parts();

        // A sliced map keeps its whole entries, copy only the referenced ones
        let (start, len) = offsets_range(&offsets);
        let entries = deep_copy_struct(entries.slice(start, len));
        let offsets = deep_copy_offset_buffer(&offsets);
        let nulls = deep_copy_null_buffer(nulls);

        // SAFETY: we copied the entire data as is
        unsafe { MapArray::new_unchecked(field, offsets, entries, nulls, ordered) }
    }

    fn deep_copy_boolean(arr: BooleanArray) -> BooleanArray {
        let (buffer, nulls) = arr.into_parts();

        let buffer = deep_copy_boolean_buffer(&buffer);
        let nulls = deep_copy_null_buffer(nulls);

        // SAFETY: we copied the entire data as is
        unsafe { BooleanArray::new_unchecked(buffer, nulls) }
    }

    fn deep_copy_byte_array<T: ByteArrayType>(
        arr: GenericByteArray<T>,
    ) -> GenericByteArray<T> {
        let (offsets, bytes, nulls) = arr.into_parts();

        // A sliced array keeps its whole values buffer, copy only the referenced bytes
        let (start, len) = offsets_range(&offsets);
        let bytes = deep_copy_buffer(&bytes.slice_with_length(start, len));
        let offsets = deep_copy_offset_buffer(&offsets);
        let nulls = deep_copy_null_buffer(nulls);

        // SAFETY: we copied the entire data as is
        unsafe { GenericByteArray::<T>::new_unchecked(offsets, bytes, nulls) }
    }

    /// Start and length of the values the offsets reference
    fn offsets_range<O: OffsetSizeTrait>(offsets: &OffsetBuffer<O>) -> (usize, usize) {
        let start = offsets[0].as_usize();
        let end = offsets[offsets.len() - 1].as_usize();
        (start, end - start)
    }

    fn deep_copy_offset_buffer<O: OffsetSizeTrait>(
        offsets: &OffsetBuffer<O>,
    ) -> OffsetBuffer<O> {
        // Always a new buffer: shifting by a first offset of 0 would keep the original,
        // and with it the whole buffer it may be a slice of
        let first_val = offsets[0];
        let shifted: Vec<O> = offsets.iter().map(|offset| *offset - first_val).collect();

        // SAFETY: shifting every offset by the first keeps them non-decreasing, from 0
        unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(shifted)) }
    }

    fn deep_copy_null_buffer(nulls: Option<NullBuffer>) -> Option<NullBuffer> {
        let nulls = nulls.filter(|n| n.null_count() > 0)?;

        let null_count = nulls.null_count();

        let boolean_buffer = deep_copy_boolean_buffer(nulls.inner());

        // SAFETY: this is safe since we copied the entire data as is
        let copied = unsafe { NullBuffer::new_unchecked(boolean_buffer, null_count) };
        Some(copied)
    }

    fn deep_copy_boolean_buffer(boolean_buffer: &BooleanBuffer) -> BooleanBuffer {
        let len = boolean_buffer.len();
        let offset = boolean_buffer.offset();

        // Byte range that contains bits [offset, offset + len)
        let start_byte = offset / 8;
        let end_byte = (offset + len).div_ceil(8);

        // Single memcpy of only the used bytes into a fresh, uniquely-owned allocation
        let bytes = &boolean_buffer.inner().as_slice()[start_byte..end_byte];
        let owned = Buffer::from_slice_ref(bytes);

        // Keep the sub-byte bit offset instead of shifting bits
        BooleanBuffer::new(owned, offset % 8, len)
    }

    fn deep_copy_buffer(buffer: &Buffer) -> Buffer {
        Buffer::from_slice_ref(buffer.as_slice())
    }

    fn fallback_deep_copy_array(array: &ArrayRef) -> ArrayRef {
        use arrow::array::MutableArrayData;

        let data = array.to_data();
        let mut a = MutableArrayData::new(vec![&data], false, array.len());
        a.try_extend(0, 0, array.len())
            .expect("an array copied whole fits its own offsets");
        make_array(a.freeze())
    }
}

#[cfg(test)]
mod deep_copy_tests {
    use super::*;
    use arrow::array::{
        Array, ArrayRef, BinaryArray, Int32Array, Int32Builder, ListArray, MapBuilder,
        StringArray, StringBuilder, StructArray,
    };
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use std::sync::Arc;

    fn batch() -> RecordBatch {
        let rows = 50;
        let strings: StringArray = (0..rows)
            .map(|i| (i % 7 != 0).then(|| format!("value-{i}-{}", "x".repeat(i % 5))))
            .collect();
        let binary: BinaryArray = (0..rows)
            .map(|i| (i % 5 != 0).then(|| vec![i as u8; i % 4]))
            .collect();
        let ints: Int32Array = (0..rows as i32)
            .map(|i| (i % 3 != 0).then_some(i))
            .collect();
        let list =
            ListArray::from_iter_primitive::<Int32Type, _, _>((0..rows).map(|i| {
                (i % 6 != 0).then(|| {
                    (0..i % 4)
                        .map(|j| Some((i * 10 + j) as i32))
                        .collect::<Vec<_>>()
                })
            }));
        let mut map = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        for i in 0..rows {
            for j in 0..i % 3 {
                map.keys().append_value(format!("k{i}-{j}"));
                map.values().append_value((i * j) as i32);
            }
            map.append(i % 8 != 0).unwrap();
        }
        let map = map.finish();
        let structs = StructArray::from(vec![
            (
                Arc::new(Field::new("s", DataType::Utf8, true)),
                Arc::new(strings.clone()) as ArrayRef,
            ),
            (
                Arc::new(Field::new("i", DataType::Int32, true)),
                Arc::new(ints.clone()) as ArrayRef,
            ),
        ]);
        let columns: Vec<ArrayRef> = vec![
            Arc::new(strings),
            Arc::new(binary),
            Arc::new(ints),
            Arc::new(list),
            Arc::new(map),
            Arc::new(structs),
        ];
        let fields: Vec<Field> = columns
            .iter()
            .enumerate()
            .map(|(i, c)| Field::new(format!("c{i}"), c.data_type().clone(), true))
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    #[test]
    fn deep_copy_of_a_leading_range_does_not_keep_the_whole_buffers() {
        let strings: StringArray =
            (0..10_000).map(|i| Some(format!("value-{i}"))).collect();
        let whole = strings.get_array_memory_size();
        let batch =
            RecordBatch::try_from_iter(vec![("s", Arc::new(strings) as ArrayRef)])
                .unwrap();
        let copy = deep_copy_batch(&batch, 0, 10);
        assert_eq!(copy, batch.slice(0, 10));
        let copied = copy.column(0).get_array_memory_size();
        assert!(
            copied < 1024,
            "{copied} bytes copied for 10 rows of {whole}"
        );
    }

    #[test]
    fn deep_copy_of_any_range_keeps_the_values() {
        let batch = batch();
        for (start, len) in [(0, 50), (0, 13), (7, 20), (13, 37), (49, 1), (20, 0)] {
            let copy = deep_copy_batch(&batch, start, len);
            assert_eq!(
                copy,
                batch.slice(start, len),
                "range {start}..{}",
                start + len
            );
        }
    }
}
