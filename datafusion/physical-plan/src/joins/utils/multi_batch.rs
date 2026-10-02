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

//! Gather and compare logical build rows without concatenating the build side.

use std::borrow::Cow;
use std::iter::once;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, GenericListArray, ListArray, MapArray, MutableArrayData,
    OffsetSizeTrait, RecordBatch, StructArray, UInt32Array, UInt64Array, downcast_array,
    make_array, new_empty_array, new_null_array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::{self, take};
use arrow::datatypes::{FieldRef, Schema};
use arrow_schema::{ArrowError, DataType, SortOptions};
use datafusion_common::cast::as_boolean_array;
use datafusion_common::{JoinSide, JoinType, NullEquality, Result, internal_err};
use hashbrown::HashMap;

use super::{
    ColumnIndex, JoinFilter, JoinKeyComparator, PreparedJoinKeyProbe,
    new_empty_schema_batch,
};

/// Referenced sources in first-use order. Source zero denotes a synthetic null
/// row; other source IDs are one-based indexes into the retained build batches.
struct SelectedBuildSources {
    sources: Vec<usize>,
    indices: Vec<(usize, usize)>,
    single_source_indices: Option<UInt64Array>,
}

impl SelectedBuildSources {
    fn new(indices: &[(usize, usize)]) -> Self {
        let mut sources = Vec::new();
        let mut source_indices = HashMap::new();
        let mut last_source = 0;
        let mut last_index = 0;
        let indices = indices
            .iter()
            .map(|&(source, row)| {
                if source == 0 {
                    return (0, 0);
                }
                if source != last_source {
                    last_index = *source_indices.entry(source).or_insert_with(|| {
                        sources.push(source - 1);
                        sources.len()
                    });
                    last_source = source;
                }
                (last_index, row)
            })
            .collect();
        Self {
            sources,
            indices,
            single_source_indices: None,
        }
    }

    fn gather<'a>(
        &mut self,
        data_type: &DataType,
        mut source: impl FnMut(usize) -> &'a dyn Array,
    ) -> Result<ArrayRef> {
        if self.sources.is_empty() {
            return Ok(new_null_array(data_type, self.indices.len()));
        }
        if self.sources.len() == 1
            && !matches!(
                data_type,
                DataType::Struct(_)
                    | DataType::List(_)
                    | DataType::LargeList(_)
                    | DataType::Map(_, _)
            )
        {
            let indices = self.single_source_indices.get_or_insert_with(|| {
                self.indices
                    .iter()
                    .map(|&(source, row)| (source != 0).then_some(row as u64))
                    .collect()
            });
            return Ok(take(source(self.sources[0]), indices, None)?);
        }
        let arrays = self
            .sources
            .iter()
            .map(|&index| source(index))
            .collect::<Vec<_>>();
        interleave_payload(data_type, &arrays, &self.indices)
    }
}

/// Gather nested nulls without copying their hidden child values. Other Arrow
/// layouts use the same interleave/take kernels as the single-batch join.
fn interleave_payload(
    data_type: &DataType,
    values: &[&dyn Array],
    indices: &[(usize, usize)],
) -> Result<ArrayRef> {
    if indices.is_empty() {
        return Ok(new_empty_array(data_type));
    }
    // Arrow handles primitive nulls directly. Variable-width nulls still need
    // normalization to avoid copying hidden payload, as do nested null parents.
    let normalize_nulls = match data_type {
        DataType::Struct(_)
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::Map(_, _) => true,
        DataType::Boolean => false,
        data_type if data_type.primitive_width().is_some() => false,
        _ => values.iter().any(|array| array.null_count() > 0),
    };
    let (indices, nulls): (Cow<'_, [(usize, usize)]>, _) = if normalize_nulls {
        let nulls: NullBuffer = indices
            .iter()
            .map(|&(source, row)| source != 0 && values[source - 1].is_valid(row))
            .collect();
        let indices = if nulls.null_count() == 0 {
            Cow::Borrowed(indices)
        } else {
            Cow::Owned(
                indices
                    .iter()
                    .enumerate()
                    .map(|(index, &row)| if nulls.is_valid(index) { row } else { (0, 0) })
                    .collect(),
            )
        };
        (indices, (nulls.null_count() != 0).then_some(nulls))
    } else {
        (Cow::Borrowed(indices), None)
    };
    match data_type {
        DataType::Struct(fields) => {
            let arrays = values.iter().map(|a| a.as_struct()).collect::<Vec<_>>();
            let children = fields
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    let children = arrays
                        .iter()
                        .map(|array| array.column(index).as_ref())
                        .collect::<Vec<_>>();
                    interleave_payload(field.data_type(), &children, &indices)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StructArray::try_new_with_length(
                fields.clone(),
                children,
                nulls,
                indices.len(),
            )?))
        }
        DataType::List(field) => {
            let arrays = values
                .iter()
                .map(|a| a.as_list::<i32>())
                .collect::<Vec<_>>();
            Ok(Arc::new(interleave_list(field, &arrays, &indices, nulls)?))
        }
        DataType::LargeList(field) => {
            let arrays = values
                .iter()
                .map(|a| a.as_list::<i64>())
                .collect::<Vec<_>>();
            Ok(Arc::new(interleave_list(field, &arrays, &indices, nulls)?))
        }
        DataType::Map(field, ordered) => {
            let lists = values
                .iter()
                .map(|array| ListArray::from(array.as_map().clone()))
                .collect::<Vec<_>>();
            let arrays = lists.iter().collect::<Vec<_>>();
            let (_, offsets, entries, nulls) =
                interleave_list(field, &arrays, &indices, nulls)?.into_parts();
            Ok(Arc::new(MapArray::try_new(
                Arc::clone(field),
                offsets,
                entries.as_struct().clone(),
                nulls,
                *ordered,
            )?))
        }
        _ => {
            // An unused null sentinel would force Arrow to build a null bitmap
            // even when all selected arrays and output rows are non-null.
            let sentinel = if indices.iter().any(|(source, _)| *source == 0) {
                new_null_array(data_type, 1)
            } else {
                new_empty_array(data_type)
            };
            let arrays = once(sentinel.as_ref())
                .chain(values.iter().copied())
                .collect::<Vec<_>>();
            Ok(compute::interleave(&arrays, &indices)?)
        }
    }
}

/// Fixed-width list children can be copied by range without allocating an index
/// for every child, which would dwarf Boolean and zero-width child payloads.
fn fixed_width_max_buffer(data_type: &DataType, rows: usize) -> Result<Option<usize>> {
    let overflow =
        || ArrowError::MemoryError(format!("Gather capacity overflow for {data_type}"));
    let bytes = match data_type {
        DataType::Struct(fields) => {
            let mut maximum = 0;
            for field in fields {
                let Some(bytes) = fixed_width_max_buffer(field.data_type(), rows)? else {
                    return Ok(None);
                };
                maximum = maximum.max(bytes);
            }
            maximum
        }
        DataType::Null | DataType::Boolean => 0,
        DataType::FixedSizeBinary(width) => {
            let width = usize::try_from(*width).map_err(|_| overflow())?;
            rows.checked_mul(width).ok_or_else(overflow)?
        }
        _ => {
            let Some(width) = data_type.primitive_width() else {
                return Ok(None);
            };
            rows.checked_mul(width).ok_or_else(overflow)?
        }
    };
    let bytes = bytes.max(rows.div_ceil(8));
    if bytes > (isize::MAX as usize & !63) {
        return Err(overflow().into());
    }
    Ok(Some(bytes))
}

fn interleave_list<O: OffsetSizeTrait>(
    field: &FieldRef,
    arrays: &[&GenericListArray<O>],
    indices: &[(usize, usize)],
    nulls: Option<NullBuffer>,
) -> Result<GenericListArray<O>> {
    let mut offsets = Vec::with_capacity(indices.len() + 1);
    let mut child_count = 0usize;
    offsets.push(O::usize_as(0));
    for &(source, row) in indices {
        if source != 0 {
            let source_offsets = arrays[source - 1].value_offsets();
            let len = source_offsets[row + 1].as_usize() - source_offsets[row].as_usize();
            child_count = child_count
                .checked_add(len)
                .ok_or(ArrowError::OffsetOverflowError(usize::MAX))?;
        }
        offsets.push(
            O::from_usize(child_count)
                .ok_or(ArrowError::OffsetOverflowError(child_count))?,
        );
    }
    let children = if fixed_width_max_buffer(field.data_type(), child_count)?.is_some() {
        let data = arrays
            .iter()
            .map(|array| array.values().to_data())
            .collect::<Vec<_>>();
        let mut output = MutableArrayData::new(data.iter().collect(), false, child_count);
        for &(source, row) in indices {
            if source != 0 {
                let source_offsets = arrays[source - 1].value_offsets();
                output.try_extend(
                    source - 1,
                    source_offsets[row].as_usize(),
                    source_offsets[row + 1].as_usize(),
                )?;
            }
        }
        make_array(output.freeze())
    } else {
        let mut child_indices = Vec::with_capacity(child_count);
        for &(source, row) in indices {
            if source != 0 {
                let source_offsets = arrays[source - 1].value_offsets();
                let start = source_offsets[row].as_usize();
                let end = source_offsets[row + 1].as_usize();
                child_indices.extend((start..end).map(|row| (source, row)));
            }
        }
        let children = arrays
            .iter()
            .map(|array| array.values().as_ref())
            .collect::<Vec<_>>();
        interleave_payload(field.data_type(), &children, &child_indices)?
    };
    Ok(GenericListArray::<O>::try_new(
        Arc::clone(field),
        OffsetBuffer::new(offsets.into()),
        children,
        nulls,
    )?)
}

#[expect(clippy::too_many_arguments)]
pub(crate) fn build_batch_from_indices_multi(
    schema: &Schema,
    build_batches: &[RecordBatch],
    gather_indices: &[(usize, usize)],
    probe_batch: &RecordBatch,
    build_indices: &UInt64Array,
    probe_indices: &UInt32Array,
    column_indices: &[ColumnIndex],
    join_type: JoinType,
    mark_column: Option<&ArrayRef>,
) -> Result<RecordBatch> {
    if schema.fields().is_empty() {
        let row_count = match join_type {
            JoinType::RightAnti | JoinType::RightSemi => probe_indices.len(),
            _ => build_indices.len(),
        };
        return new_empty_schema_batch(schema, row_count);
    }
    let mut selection = None;
    let columns = column_indices
        .iter()
        .map(|column| {
            if column.side == JoinSide::None {
                return match mark_column {
                    Some(mark) => Ok(Arc::clone(mark)),
                    None => {
                        Ok(Arc::new(compute::is_not_null(probe_indices)?) as ArrayRef)
                    }
                };
            }
            if column.side == JoinSide::Left {
                let data_type = build_batches[0].column(column.index).data_type();
                if build_indices.null_count() == build_indices.len() {
                    Ok(new_null_array(data_type, build_indices.len()))
                } else {
                    selection
                        .get_or_insert_with(|| SelectedBuildSources::new(gather_indices))
                        .gather(data_type, |index| {
                            build_batches[index].column(column.index).as_ref()
                        })
                }
            } else {
                let array = probe_batch.column(column.index);
                if probe_indices.null_count() == probe_indices.len() {
                    Ok(new_null_array(array.data_type(), probe_indices.len()))
                } else {
                    Ok(take(array.as_ref(), probe_indices, None)?)
                }
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
}

pub(crate) fn apply_join_filter_to_indices_multi(
    build_batches: &[RecordBatch],
    gather_indices: &[(usize, usize)],
    probe_batch: &RecordBatch,
    build_indices: UInt64Array,
    probe_indices: UInt32Array,
    filter: &JoinFilter,
    join_type: JoinType,
) -> Result<(UInt64Array, UInt32Array)> {
    if build_indices.is_empty() && probe_indices.is_empty() {
        return Ok((build_indices, probe_indices));
    }
    let intermediate = build_batch_from_indices_multi(
        filter.schema(),
        build_batches,
        gather_indices,
        probe_batch,
        &build_indices,
        &probe_indices,
        filter.column_indices(),
        join_type,
        None,
    )?;
    let filter_result = filter
        .expression()
        .evaluate(&intermediate)?
        .into_array(intermediate.num_rows())?;
    let mask = as_boolean_array(&filter_result)?;
    let left = compute::filter(&build_indices, mask)?;
    let right = compute::filter(&probe_indices, mask)?;
    Ok((
        downcast_array(left.as_ref()),
        downcast_array(right.as_ref()),
    ))
}

/// Compare keys directly in the referenced batches using the existing join
/// comparator, including its floating-point and logical-null semantics.
pub(crate) fn equal_rows_arr_multi(
    indices_left: &UInt64Array,
    indices_right: &UInt32Array,
    left_arrays: &[Vec<ArrayRef>],
    right_arrays: &[ArrayRef],
    gather_indices: &[(usize, usize)],
    null_equality: NullEquality,
) -> Result<(UInt64Array, UInt32Array)> {
    if indices_left.len() != indices_right.len()
        || indices_left.len() != gather_indices.len()
    {
        return internal_err!("Cannot compare join indices with different lengths");
    }
    if indices_left.is_empty() || right_arrays.is_empty() {
        return Ok((Vec::<u64>::new().into(), Vec::<u32>::new().into()));
    }
    let selection = SelectedBuildSources::new(gather_indices);
    let sort_options = vec![SortOptions::default(); right_arrays.len()];
    let probe = PreparedJoinKeyProbe::new(right_arrays, null_equality);

    // Link candidate positions by source, then compact in original order.
    // Arrow comparators may build their own logical-null masks even with our
    // prepared probe metadata. Retaining one comparator at a time bounds that
    // scratch memory, though Arrow still computes those masks for each source.
    let mut source_heads = vec![usize::MAX; selection.sources.len()];
    let mut next_positions = Vec::with_capacity(selection.indices.len());
    for (position, &(source, _)) in selection.indices.iter().enumerate() {
        // Equality candidates come from the hash table, before outer padding.
        debug_assert_ne!(source, 0);
        next_positions.push(source_heads[source - 1]);
        source_heads[source - 1] = position;
    }
    let mut equal = vec![false; indices_left.len()];
    for (&index, mut position) in selection.sources.iter().zip(source_heads) {
        let arrays = &left_arrays[index];
        if arrays.len() != right_arrays.len() {
            return internal_err!(
                "Cannot compare join keys with different column counts"
            );
        }
        let comparator =
            JoinKeyComparator::new_with_prepared_probe(arrays, &probe, &sort_options)?;
        while position != usize::MAX {
            let (_, row) = selection.indices[position];
            equal[position] =
                comparator.is_equal(row, indices_right.value(position) as usize);
            position = next_positions[position];
        }
    }
    let mut left_filtered = Vec::with_capacity(indices_left.len());
    let mut right_filtered = Vec::with_capacity(indices_right.len());
    for ((&left, &right), equal) in indices_left
        .values()
        .iter()
        .zip(indices_right.values())
        .zip(equal)
    {
        if equal {
            left_filtered.push(left);
            right_filtered.push(right);
        }
    }
    Ok((left_filtered.into(), right_filtered.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BooleanArray, DictionaryArray, FixedSizeListArray, Float64Array, Int8Array,
        Int32Array, ListViewArray, PrimitiveRunBuilder, StringArray, UnionArray,
    };
    use arrow::datatypes::{Field, Float64Type, Int8Type, Int32Type, UnionFields};

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn fixed_width_children_do_not_have_a_32_bit_byte_offset_limit() -> Result<()> {
        assert_eq!(
            fixed_width_max_buffer(&DataType::FixedSizeBinary(16), 1 << 27)?,
            Some(1 << 31),
        );
        Ok(())
    }

    #[test]
    fn gather_flat_nonnull_omits_null_bitmap() -> Result<()> {
        let first = Int32Array::from(vec![1, 2]);
        let second = Int32Array::from(vec![3, 4]);
        let indices = [(2, 1), (1, 0), (2, 0)];
        let result = interleave_payload(&DataType::Int32, &[&first, &second], &indices)?;
        assert_eq!(
            result.as_ref(),
            &Int32Array::from(vec![4, 1, 3]) as &dyn Array
        );
        assert!(result.nulls().is_none());

        let first = StringArray::from(vec!["a", "b"]);
        let second = StringArray::from(vec!["c", "d"]);
        let result = interleave_payload(&DataType::Utf8, &[&first, &second], &indices)?;
        assert_eq!(
            result.as_ref(),
            &StringArray::from(vec!["d", "a", "c"]) as &dyn Array
        );
        assert!(result.nulls().is_none());
        Ok(())
    }

    #[test]
    fn gather_flat_physical_nulls_and_padding() -> Result<()> {
        let first = Int32Array::from(vec![Some(1), None]);
        let second = Int32Array::from(vec![None, Some(4)]);
        for indices in [
            vec![(1, 1), (2, 1), (2, 0), (1, 0)],
            vec![(1, 1), (2, 1), (0, 0), (1, 0)],
        ] {
            let result =
                interleave_payload(&DataType::Int32, &[&first, &second], &indices)?;
            assert_eq!(
                result.as_ref(),
                &Int32Array::from(vec![None, Some(4), None, Some(1)]) as &dyn Array
            );
        }

        let first = BooleanArray::from(vec![Some(true), None]);
        let second = BooleanArray::from(vec![Some(false)]);
        let result = interleave_payload(
            &DataType::Boolean,
            &[&first, &second],
            &[(1, 1), (2, 0), (0, 0), (1, 0)],
        )?;
        assert_eq!(
            result.as_ref(),
            &BooleanArray::from(vec![None, Some(false), None, Some(true)]) as &dyn Array
        );
        Ok(())
    }

    #[test]
    fn gather_flat_variable_width_nulls_omit_hidden_payload() -> Result<()> {
        let first = StringArray::new(
            OffsetBuffer::new(vec![0, 6, 13].into()),
            arrow::buffer::Buffer::from(b"hiddenvisible".as_slice()),
            Some(NullBuffer::from(vec![false, true])),
        );
        let second = StringArray::from(vec!["other"]);
        let result = interleave_payload(
            &DataType::Utf8,
            &[&first, &second],
            &[(1, 0), (2, 0), (1, 1), (0, 0)],
        )?;
        assert_eq!(
            result.as_ref(),
            &StringArray::from(vec![None, Some("other"), Some("visible"), None])
                as &dyn Array
        );
        assert_eq!(result.as_string::<i32>().value_data(), b"othervisible");
        Ok(())
    }

    #[test]
    fn gather_only_borrows_selected_sources() -> Result<()> {
        let first = StringArray::from(vec!["a", "b"]);
        let second = StringArray::from(vec!["c", "d"]);
        let mut borrowed = Vec::new();
        let result = SelectedBuildSources::new(&[(4097, 1), (2, 0), (4097, 0), (0, 0)])
            .gather(&DataType::Utf8, |source| {
            borrowed.push(source);
            match source {
                4096 => &first,
                1 => &second,
                _ => panic!("unreferenced source {source}"),
            }
        })?;
        assert_eq!(borrowed, vec![4096, 1]);
        assert_eq!(
            result.as_ref(),
            &StringArray::from(vec![Some("b"), Some("c"), Some("a"), None]) as &dyn Array
        );
        Ok(())
    }

    #[test]
    fn gather_nested_nulls_omits_hidden_children() -> Result<()> {
        let child: ArrayRef = Arc::new(StringArray::from(vec!["hidden", "visible"]));
        let field = Arc::new(Field::new("item", DataType::Utf8, true));
        let list = ListArray::try_new(
            field,
            OffsetBuffer::new(vec![0, 1, 2].into()),
            child,
            Some(NullBuffer::from(vec![false, true])),
        )?;
        let result = SelectedBuildSources::new(&[(1, 0), (1, 1), (0, 0), (1, 0)])
            .gather(list.data_type(), |_| &list)?;
        let result = result.as_list::<i32>();
        assert_eq!(result.value_offsets(), &[0, 0, 1, 1, 1]);
        assert_eq!(result.null_count(), 3);
        assert_eq!(
            result.values().as_ref(),
            &StringArray::from(vec!["visible"]) as &dyn Array
        );
        Ok(())
    }

    #[test]
    fn gather_dictionary_payload_matches_concat_take() -> Result<()> {
        let first: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), None, Some(1)]),
            Arc::new(StringArray::from(vec!["first", "shared"])),
        )?);
        let second: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), Some(1), Some(0)]),
            Arc::new(StringArray::from(vec!["shared", "second"])),
        )?);
        let arrays = [first, second];
        let combined =
            compute::concat(&arrays.iter().map(AsRef::as_ref).collect::<Vec<_>>())?;
        let indices = UInt64Array::from(vec![Some(4), Some(2), None, Some(1), Some(3)]);
        let expected = take(combined.as_ref(), &indices, None)?;
        let actual = SelectedBuildSources::new(&[(2, 1), (1, 2), (0, 0), (1, 1), (2, 0)])
            .gather(arrays[0].data_type(), |source| arrays[source].as_ref())?;
        let actual = compute::cast(actual.as_ref(), &DataType::Utf8)?;
        let expected = compute::cast(expected.as_ref(), &DataType::Utf8)?;
        assert_eq!(actual.as_ref(), expected.as_ref());
        Ok(())
    }

    #[test]
    fn gather_dictionary_value_nulls_without_padding() -> Result<()> {
        let first: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![0, 1]),
            Arc::new(StringArray::from(vec![Some("a"), None])),
        )?);
        let second: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![0, 1]),
            Arc::new(StringArray::from(vec![None, Some("b")])),
        )?);
        for array in [&first, &second] {
            assert_eq!(array.null_count(), 0);
            assert_eq!(array.logical_nulls().unwrap().null_count(), 1);
        }
        let actual = interleave_payload(
            first.data_type(),
            &[first.as_ref(), second.as_ref()],
            &[(2, 0), (1, 0), (2, 1), (1, 1)],
        )?;
        let actual = compute::cast(actual.as_ref(), &DataType::Utf8)?;
        assert_eq!(
            actual.as_ref(),
            &StringArray::from(vec![None, Some("a"), Some("b"), None]) as &dyn Array
        );
        Ok(())
    }

    #[test]
    fn gather_encoded_payloads_matches_concat_take() -> Result<()> {
        let fixed: ArrayRef =
            Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
                [
                    Some(vec![Some(1), Some(2)]),
                    None,
                    Some(vec![Some(3), None]),
                    Some(vec![Some(4), Some(5)]),
                ],
                2,
            ));
        let lists = ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1), Some(2)]),
            None,
            Some(vec![Some(3)]),
            Some(vec![Some(4), Some(5)]),
        ]);
        let views: ArrayRef = Arc::new(ListViewArray::from(lists));
        let mut builder = PrimitiveRunBuilder::<Int32Type, Int32Type>::new();
        builder.extend([Some(1), Some(1), None, Some(2)]);
        let runs: ArrayRef = Arc::new(builder.finish());
        let union: ArrayRef = Arc::new(UnionArray::try_new(
            UnionFields::try_new(
                vec![0, 1],
                vec![
                    Field::new("int", DataType::Int32, true),
                    Field::new("str", DataType::Utf8, true),
                ],
            )?,
            vec![0, 1, 0, 1].into(),
            Some(vec![0, 0, 1, 1].into()),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )?);
        for array in [fixed, views, runs, union] {
            let sources = [array.slice(0, 2), array.slice(2, 2)];
            let combined =
                compute::concat(&sources.iter().map(AsRef::as_ref).collect::<Vec<_>>())?;
            // Without padding, logical nulls in encoded arrays and non-null
            // selections use an empty sentinel instead of a synthetic null row.
            for indices in [
                vec![Some(3), Some(0), None, Some(2), Some(1)],
                vec![Some(3), Some(0), Some(2), Some(1)],
                vec![Some(3), Some(0)],
            ] {
                let indices = UInt64Array::from(indices);
                let expected = take(combined.as_ref(), &indices, None)?;
                let gather = indices
                    .iter()
                    .map(|index| {
                        index.map_or((0, 0), |row| {
                            (row as usize / 2 + 1, row as usize % 2)
                        })
                    })
                    .collect::<Vec<_>>();
                let actual = SelectedBuildSources::new(&gather)
                    .gather(array.data_type(), |source| sources[source].as_ref())?;
                assert_eq!(actual.as_ref(), expected.as_ref(), "{}", array.data_type());
            }
        }
        Ok(())
    }

    #[test]
    fn equality_matches_contiguous_float_dictionary_and_composite_keys() -> Result<()> {
        let floats: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(-0.0),
            Some(f64::NAN),
            None,
            Some(0.0),
            Some(-f64::NAN),
            None,
        ]));
        let dictionary: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), Some(1), Some(2), Some(0), Some(1), None]),
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
        )?);
        let second_key: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 1, 0, 3]));
        let build_indices = UInt64Array::from(vec![0, 1, 2, 3, 4, 5, 0, 4]);
        let probe_indices = UInt32Array::from(vec![3, 4, 5, 0, 1, 2, 0, 4]);
        let gather = [
            (1, 0),
            (1, 1),
            (1, 2),
            (2, 0),
            (2, 1),
            (2, 2),
            (1, 0),
            (2, 1),
        ];
        for first_key in [floats, dictionary] {
            for composite in [false, true] {
                let mut keys = vec![Arc::clone(&first_key)];
                if composite {
                    keys.push(Arc::clone(&second_key));
                }
                let batches = [0, 3]
                    .iter()
                    .map(|&start| {
                        keys.iter().map(|array| array.slice(start, 3)).collect()
                    })
                    .collect::<Vec<_>>();
                for null_equality in [
                    NullEquality::NullEqualsNothing,
                    NullEquality::NullEqualsNull,
                ] {
                    let expected = super::super::equal_rows_arr(
                        &build_indices,
                        &probe_indices,
                        &keys,
                        &keys,
                        null_equality,
                    )?;
                    let actual = equal_rows_arr_multi(
                        &build_indices,
                        &probe_indices,
                        &batches,
                        &keys,
                        &gather,
                        null_equality,
                    )?;
                    assert_eq!(actual, expected);
                }
            }
        }
        Ok(())
    }

    #[test]
    fn probe_preprocessing_is_shared_across_many_build_sources() -> Result<()> {
        const SOURCES: usize = 64;
        let dictionary: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![0, 1, 0, 2]),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("b")])),
        )?);
        let right = vec![
            Arc::new(Float64Array::from(vec![
                Some(-0.0),
                Some(2.0),
                None,
                Some(0.0),
            ])) as ArrayRef,
            Arc::clone(&dictionary),
        ];
        let left = vec![
            Arc::new(Float64Array::from(vec![
                Some(0.0),
                Some(2.0),
                None,
                Some(-0.0),
            ])) as ArrayRef,
            dictionary,
        ];
        let probe = PreparedJoinKeyProbe::new(&right, NullEquality::NullEqualsNothing);
        let normalized_values = probe.columns[0]
            .0
            .as_primitive::<Float64Type>()
            .values()
            .inner()
            .clone();
        assert!(
            !normalized_values
                .ptr_eq(right[0].as_primitive::<Float64Type>().values().inner())
        );
        let dictionary_nulls = probe.columns[1].1.as_ref().unwrap().buffer().clone();
        let value_refs = normalized_values.strong_count();
        let null_refs = dictionary_nulls.strong_count();
        let options = vec![SortOptions::default(); right.len()];
        for _ in 0..SOURCES {
            let comparator =
                JoinKeyComparator::new_with_prepared_probe(&left, &probe, &options)?;
            assert_eq!(normalized_values.strong_count(), value_refs + 1);
            assert_eq!(dictionary_nulls.strong_count(), null_refs + 1);
            assert!(comparator.is_equal(0, 0));
            assert!(!comparator.is_equal(1, 1));
            assert!(!comparator.is_equal(2, 2));
            assert!(comparator.is_equal(3, 3));
            drop(comparator);
            assert_eq!(normalized_values.strong_count(), value_refs);
            assert_eq!(dictionary_nulls.strong_count(), null_refs);
        }

        let batches = vec![left; SOURCES];
        let contiguous = (0..right.len())
            .map(|column| {
                Ok(compute::concat(
                    &batches
                        .iter()
                        .map(|keys| keys[column].as_ref())
                        .collect::<Vec<_>>(),
                )?)
            })
            .collect::<Result<Vec<_>>>()?;
        let mut build_indices = Vec::new();
        let mut probe_indices = Vec::new();
        let mut gather = Vec::new();
        for round in 0..4 {
            for source in (0..SOURCES).rev() {
                let row = (source + round) % 4;
                build_indices.push((source * 4 + row) as u64);
                probe_indices.push(round as u32);
                gather.push((source + 1, row));
            }
        }
        let build_indices = UInt64Array::from(build_indices);
        let probe_indices = UInt32Array::from(probe_indices);
        for null_equality in [
            NullEquality::NullEqualsNothing,
            NullEquality::NullEqualsNull,
        ] {
            let expected = super::super::equal_rows_arr(
                &build_indices,
                &probe_indices,
                &contiguous,
                &right,
                null_equality,
            )?;
            let actual = equal_rows_arr_multi(
                &build_indices,
                &probe_indices,
                &batches,
                &right,
                &gather,
                null_equality,
            )?;
            assert_eq!(actual, expected);
        }
        Ok(())
    }
}
