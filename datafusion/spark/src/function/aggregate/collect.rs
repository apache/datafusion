// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::array::{
    Array, ArrayData, ArrayRef, FixedSizeListArray, GenericListArray,
    GenericListViewArray, MapArray, OffsetSizeTrait, PrimitiveArray, RunArray,
    StructArray, UInt64Array, UnionArray, cast::AsArray, downcast_run_array, make_array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::compute::{cast, interleave, take};
use arrow::datatypes::{DataType, Field, FieldRef, RunEndIndexType, UnionMode};
use arrow_select::dictionary::garbage_collect_any_dictionary;
use datafusion_common::utils::SingleRowListArrayBuilder;
use datafusion_common::{Result, ScalarValue, internal_datafusion_err, internal_err};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion_functions_aggregate::array_agg::{
    ArrayAggAccumulator, DistinctArrayAggAccumulator,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

// Spark implementation of collect_list/collect_set aggregate function.
// Differs from DataFusion ArrayAgg in the following ways:
// - ignores NULL inputs
// - returns an empty list when all inputs are NULL
// - does not support ordering

/// Build an empty list `ScalarValue` for a `List(element_type)` data type.
/// Used as the result for empty window frames and for groups whose inputs
/// were all NULL, matching Spark's `collect_list` / `collect_set` semantics.
fn empty_list_scalar(list_type: &DataType) -> Result<ScalarValue> {
    let DataType::List(field) = list_type else {
        return internal_err!(
            "collect_list/collect_set expected List return type, got {list_type:?}"
        );
    };
    let empty = arrow::array::new_empty_array(field.data_type());
    Ok(SingleRowListArrayBuilder::new(empty)
        .with_field(field)
        .build_list_scalar())
}

fn collect_type(element_type: DataType) -> DataType {
    DataType::List(Arc::new(Field::new_list_field(element_type, false)))
}

/// Whether an exact-type array still needs normalization to discard physical
/// storage that is not part of its logical value.
fn has_compactable_storage(value: &ArrayRef) -> bool {
    match value.data_type() {
        DataType::Dictionary(_, _)
        | DataType::RunEndEncoded(_, _)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::Union(_, _) => true,
        DataType::List(_) => {
            let list = value.as_list::<i32>();
            list.null_count() != 0
                || list
                    .value_offsets()
                    .first()
                    .is_some_and(|offset| *offset != 0)
                || list
                    .value_offsets()
                    .last()
                    .is_some_and(|offset| *offset as usize != list.values().len())
                || has_compactable_storage(list.values())
        }
        DataType::LargeList(_) => {
            let list = value.as_list::<i64>();
            list.null_count() != 0
                || list
                    .value_offsets()
                    .first()
                    .is_some_and(|offset| *offset != 0)
                || list
                    .value_offsets()
                    .last()
                    .is_some_and(|offset| *offset as usize != list.values().len())
                || has_compactable_storage(list.values())
        }
        DataType::FixedSizeList(_, size) => {
            let list = value.as_fixed_size_list();
            let has_exact_child_length = usize::try_from(*size)
                .ok()
                .and_then(|size| list.len().checked_mul(size))
                .is_some_and(|length| length == list.values().len());
            list.null_count() != 0
                || list.values().offset() != 0
                || !has_exact_child_length
                || has_compactable_storage(list.values())
        }
        DataType::Struct(_) => {
            let structure = value.as_struct();
            structure.null_count() != 0
                || structure.columns().iter().any(has_compactable_storage)
        }
        DataType::Map(_, _) => {
            let map = value.as_map();
            map.null_count() != 0
                || map
                    .value_offsets()
                    .first()
                    .is_some_and(|offset| *offset != 0)
                || map
                    .value_offsets()
                    .last()
                    .is_some_and(|offset| *offset as usize != map.entries().len())
                || map.entries().columns().iter().any(has_compactable_storage)
        }
        _ => false,
    }
}

/// Materialize only logically reachable values and align their nested type with
/// `target_type`.
///
/// Arrow container arrays may retain unreachable null payload in null lists,
/// unused dictionary values, sparse-union children, and view backing buffers.
/// That payload is valid, but it makes `Array::is_nullable` conservative and
/// prevents the result from being embedded below Spark's non-null list field.
fn normalize_array(
    value: &ArrayRef,
    target_type: &DataType,
    require_non_null: bool,
) -> Result<ArrayRef> {
    if value.data_type() == target_type
        && !has_compactable_storage(value)
        && (!require_non_null || !value.is_nullable())
    {
        return Ok(Arc::clone(value));
    }
    let normalized = match (value.data_type(), target_type) {
        (DataType::List(_), DataType::List(field)) => {
            normalize_list::<i32>(value, field)?
        }
        (DataType::LargeList(_), DataType::LargeList(field)) => {
            normalize_list::<i64>(value, field)?
        }
        (DataType::ListView(_), DataType::ListView(field)) => {
            normalize_list_view::<i32>(value, field)?
        }
        (DataType::LargeListView(_), DataType::LargeListView(field)) => {
            normalize_list_view::<i64>(value, field)?
        }
        (
            DataType::FixedSizeList(_, source_size),
            DataType::FixedSizeList(field, target_size),
        ) if source_size == target_size => {
            normalize_fixed_size_list(value, field, *target_size)?
        }
        (
            DataType::Dictionary(source_key, _),
            DataType::Dictionary(target_key, target_value),
        ) if source_key == target_key => {
            let compact = garbage_collect_any_dictionary(value.as_any_dictionary())?;
            let dictionary =
                datafusion_common::utils::map_dictionary_values(&compact, |values| {
                    normalize_array(values, target_value, require_non_null)
                })?;
            if dictionary.null_count() == 0 && dictionary.to_data().nulls().is_some() {
                let data = dictionary.to_data().into_builder().nulls(None).build()?;
                make_array(data)
            } else {
                dictionary
            }
        }
        (
            DataType::RunEndEncoded(source_run_ends, _),
            DataType::RunEndEncoded(target_run_ends, target_value),
        ) if source_run_ends.data_type() == target_run_ends.data_type() => {
            // Work at the physical run count, not the potentially enormous
            // logical length represented by those runs.
            let value = value.as_ref();
            arrow::array::downcast_run_array! {
                value => normalize_run_end(
                    value,
                    target_type,
                    target_value.data_type(),
                    require_non_null,
                ),
                _ => internal_err!("collect_list/collect_set expected a run-end encoded array"),
            }?
        }
        (DataType::Struct(_), DataType::Struct(fields)) => {
            let source = value.as_struct();
            if source.null_count() == source.len() {
                return Ok(arrow::array::new_null_array(target_type, source.len()));
            }
            let columns = fields
                .iter()
                .zip(source.columns())
                .map(|(field, column)| {
                    let column = materialize_masked_values(column, source.nulls())?;
                    normalize_array(&column, field.data_type(), !field.is_nullable())
                })
                .collect::<Result<Vec<_>>>()?;
            Arc::new(StructArray::try_new_with_length(
                fields.clone(),
                columns,
                source.nulls().cloned(),
                source.len(),
            )?) as ArrayRef
        }
        (DataType::Map(_, _), DataType::Map(field, ordered)) => {
            normalize_map(value, field, *ordered)?
        }
        (
            DataType::Union(source_fields, UnionMode::Sparse),
            DataType::Union(fields, UnionMode::Sparse),
        ) => normalize_sparse_union(value, source_fields, fields, require_non_null)?,
        (
            DataType::Union(source_fields, UnionMode::Dense),
            DataType::Union(fields, UnionMode::Dense),
        ) => normalize_dense_union(value, source_fields, fields, require_non_null)?,
        _ => {
            let value = if value.data_type() == target_type {
                Arc::clone(value)
            } else {
                cast(value.as_ref(), target_type)?
            };
            if require_non_null
                && value.logical_null_count() == 0
                && value.is_nullable()
                && value.to_data().nulls().is_some()
            {
                let data = value.to_data().into_builder().nulls(None).build()?;
                make_array(data)
            } else {
                value
            }
        }
    };
    Ok(normalized)
}

fn materialize_masked_values(
    value: &ArrayRef,
    parent_nulls: Option<&NullBuffer>,
) -> Result<ArrayRef> {
    let Some(parent_nulls) = parent_nulls.filter(|nulls| nulls.null_count() != 0) else {
        return Ok(Arc::clone(value));
    };
    // Use an inexpensive default, not a retained value: copying a visible string
    // or list into every masked slot multiplies its payload by the row count.
    // Visible nulls are left untouched so narrowing still rejects invalid data.
    let default = masked_default(value.data_type())?.to_array()?;
    let indices = (0..value.len())
        .map(|index| {
            if parent_nulls.is_valid(index) {
                (0, index)
            } else {
                (1, 0)
            }
        })
        .collect::<Vec<_>>();
    interleave_rows(value, &default, &indices)
}

/// A cheap replacement for an unreachable value. Prefer a non-null union arm:
/// the first arm may be NULL even when a required union has valid values.
fn masked_default(data_type: &DataType) -> Result<ScalarValue> {
    match data_type {
        DataType::Union(fields, mode) => {
            for (id, field) in fields.iter() {
                let value = masked_default(field.data_type())?;
                if !value.is_null() {
                    return Ok(ScalarValue::Union(
                        Some((id, Box::new(value))),
                        fields.clone(),
                        *mode,
                    ));
                }
            }
            ScalarValue::new_default(data_type)
        }
        DataType::Struct(fields) => {
            let columns = fields
                .iter()
                .map(|field| masked_default(field.data_type())?.to_array())
                .collect::<Result<Vec<_>>>()?;
            let nulls = fields
                .iter()
                .zip(&columns)
                .any(|(field, column)| {
                    !field.is_nullable() && column.logical_null_count() != 0
                })
                .then(|| NullBuffer::new_null(1));
            Ok(ScalarValue::Struct(Arc::new(
                StructArray::try_new_with_length(fields.clone(), columns, nulls, 1)?,
            )))
        }
        DataType::FixedSizeList(field, size) => {
            let len = usize::try_from(*size)
                .map_err(|_| internal_datafusion_err!("negative fixed-size-list size"))?;
            let values = masked_default(field.data_type())?.to_array_of_size(len)?;
            let nulls = (!field.is_nullable() && values.logical_null_count() != 0)
                .then(|| NullBuffer::new_null(1));
            Ok(ScalarValue::FixedSizeList(Arc::new(
                FixedSizeListArray::try_new_with_length(
                    Arc::clone(field),
                    *size,
                    values,
                    nulls,
                    1,
                )?,
            )))
        }
        DataType::Dictionary(key, value) => Ok(ScalarValue::Dictionary(
            key.clone(),
            Box::new(masked_default(value)?),
        )),
        DataType::RunEndEncoded(run_ends, value) => Ok(ScalarValue::RunEndEncoded(
            Arc::clone(run_ends),
            Arc::clone(value),
            Box::new(masked_default(value.data_type())?),
        )),
        _ => ScalarValue::new_default(data_type),
    }
}

/// Preserve shared dense-union offsets when selecting from masked structures.
/// Arrow's generic selection kernels materialize a union child per selected row.
fn interleave_rows(
    left: &ArrayRef,
    right: &ArrayRef,
    indices: &[(usize, usize)],
) -> Result<ArrayRef> {
    match left.data_type() {
        DataType::Map(field, ordered) => {
            let sources = [left.as_map(), right.as_map()];
            let mut offsets = vec![0_i32];
            let mut selected = vec![];
            for &(source, row) in indices {
                if sources[source].is_valid(row) {
                    let range = &sources[source].value_offsets()[row..=row + 1];
                    selected.extend(
                        (range[0] as usize..range[1] as usize)
                            .map(|index| (source, index)),
                    );
                }
                offsets.push(i32::try_from(selected.len()).map_err(|_| {
                    internal_datafusion_err!("map child length exceeds i32")
                })?);
            }
            let entries = interleave_rows(
                &(Arc::new(sources[0].entries().clone()) as ArrayRef),
                &(Arc::new(sources[1].entries().clone()) as ArrayRef),
                &selected,
            )?;
            let nulls = NullBuffer::from_iter(
                indices
                    .iter()
                    .map(|(source, row)| sources[*source].is_valid(*row)),
            );
            Ok(Arc::new(MapArray::try_new(
                Arc::clone(field),
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                entries.as_struct().clone(),
                Some(nulls),
                *ordered,
            )?))
        }
        DataType::List(field) => interleave_lists::<i32>(left, right, field, indices),
        DataType::LargeList(field) => {
            interleave_lists::<i64>(left, right, field, indices)
        }
        DataType::FixedSizeList(field, size) => {
            let sources = [left.as_fixed_size_list(), right.as_fixed_size_list()];
            let size = usize::try_from(*size)
                .map_err(|_| internal_datafusion_err!("negative fixed-size-list size"))?;
            let mut selected = Vec::new();
            for &(source, row) in indices {
                let start = row.checked_mul(size).ok_or_else(|| {
                    internal_datafusion_err!("fixed-size-list child offset overflow")
                })?;
                selected.extend((start..start + size).map(|index| (source, index)));
            }
            let values =
                interleave_rows(sources[0].values(), sources[1].values(), &selected)?;
            let nulls = NullBuffer::from_iter(
                indices
                    .iter()
                    .map(|(source, row)| sources[*source].is_valid(*row)),
            );
            Ok(Arc::new(FixedSizeListArray::try_new_with_length(
                Arc::clone(field),
                size.try_into().map_err(|_| {
                    internal_datafusion_err!("fixed-size-list size exceeds i32")
                })?,
                values,
                Some(nulls),
                indices.len(),
            )?))
        }
        DataType::Struct(fields) => {
            let sources = [left.as_struct(), right.as_struct()];
            let columns = (0..fields.len())
                .map(|index| {
                    interleave_rows(
                        sources[0].column(index),
                        sources[1].column(index),
                        indices,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let nulls = NullBuffer::from_iter(
                indices
                    .iter()
                    .map(|(source, row)| sources[*source].is_valid(*row)),
            );
            Ok(Arc::new(StructArray::try_new_with_length(
                fields.clone(),
                columns,
                Some(nulls),
                indices.len(),
            )?))
        }
        DataType::Union(fields, UnionMode::Dense) => {
            let sources = [left.as_union(), right.as_union()];
            let mut referenced = vec![BTreeMap::new(); fields.len()];
            let mut selected = vec![vec![]; fields.len()];
            let mut type_ids = Vec::with_capacity(indices.len());
            let mut offsets = Vec::with_capacity(indices.len());
            for &(source, row) in indices {
                let type_id = sources[source].type_id(row);
                let field =
                    fields
                        .iter()
                        .position(|(id, _)| id == type_id)
                        .ok_or_else(|| {
                            internal_datafusion_err!(
                                "unknown dense union type id {type_id}"
                            )
                        })?;
                let key = (source, sources[source].value_offset(row));
                let offset = if let Some(offset) = referenced[field].get(&key) {
                    *offset
                } else {
                    let offset = i32::try_from(selected[field].len()).map_err(|_| {
                        internal_datafusion_err!("dense union child length exceeds i32")
                    })?;
                    selected[field].push(key);
                    referenced[field].insert(key, offset);
                    offset
                };
                type_ids.push(type_id);
                offsets.push(offset);
            }
            let children = fields
                .iter()
                .zip(selected)
                .map(|((id, _), selected)| {
                    interleave_rows(sources[0].child(id), sources[1].child(id), &selected)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(UnionArray::try_new(
                fields.clone(),
                ScalarBuffer::from(type_ids),
                Some(ScalarBuffer::from(offsets)),
                children,
            )?))
        }
        DataType::Union(fields, UnionMode::Sparse) => {
            let sources = [left.as_union(), right.as_union()];
            let children = fields
                .iter()
                .map(|(id, _)| {
                    interleave_rows(sources[0].child(id), sources[1].child(id), indices)
                })
                .collect::<Result<Vec<_>>>()?;
            let type_ids = indices
                .iter()
                .map(|(source, row)| sources[*source].type_id(*row))
                .collect::<Vec<_>>();
            Ok(Arc::new(UnionArray::try_new(
                fields.clone(),
                ScalarBuffer::from(type_ids),
                None,
                children,
            )?))
        }
        _ => Ok(interleave(&[left.as_ref(), right.as_ref()], indices)?),
    }
}

fn interleave_lists<Offset: OffsetSizeTrait>(
    left: &ArrayRef,
    right: &ArrayRef,
    field: &FieldRef,
    indices: &[(usize, usize)],
) -> Result<ArrayRef> {
    let sources = [left.as_list::<Offset>(), right.as_list::<Offset>()];
    let mut offsets = Vec::with_capacity(indices.len() + 1);
    let mut selected = vec![];
    offsets.push(Offset::zero());
    for &(source, row) in indices {
        if sources[source].is_valid(row) {
            let range = &sources[source].value_offsets()[row..=row + 1];
            selected.extend(
                (range[0].as_usize()..range[1].as_usize()).map(|index| (source, index)),
            );
        }
        offsets.push(Offset::from_usize(selected.len()).ok_or_else(|| {
            internal_datafusion_err!("list child length exceeds offset type")
        })?);
    }
    let values = interleave_rows(sources[0].values(), sources[1].values(), &selected)?;
    let nulls = NullBuffer::from_iter(
        indices
            .iter()
            .map(|(source, row)| sources[*source].is_valid(*row)),
    );
    Ok(Arc::new(GenericListArray::<Offset>::try_new(
        Arc::clone(field),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        values,
        Some(nulls),
    )?))
}

/// Select dense-union rows without expanding a shared child offset once per
/// occurrence. Child normalization subsequently retains each referenced offset
/// once, including when logical null rows were removed.
fn take_rows(value: &ArrayRef, indices: &UInt64Array) -> Result<ArrayRef> {
    if matches!(
        value.data_type(),
        DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Map(_, _)
            | DataType::Union(_, UnionMode::Sparse)
    ) {
        let indices = indices
            .values()
            .iter()
            .map(|index| {
                usize::try_from(*index)
                    .map(|index| (0, index))
                    .map_err(|_| internal_datafusion_err!("row index exceeds usize"))
            })
            .collect::<Result<Vec<_>>>()?;
        return interleave_rows(
            value,
            &arrow::array::new_empty_array(value.data_type()),
            &indices,
        );
    }
    if let DataType::Struct(fields) = value.data_type() {
        let source = value.as_struct();
        let columns = source
            .columns()
            .iter()
            .map(|column| take_rows(column, indices))
            .collect::<Result<Vec<_>>>()?;
        let nulls = source.nulls().map(|nulls| {
            NullBuffer::from_iter(
                indices
                    .values()
                    .iter()
                    .map(|index| nulls.is_valid(*index as usize)),
            )
        });
        return Ok(Arc::new(StructArray::try_new_with_length(
            fields.clone(),
            columns,
            nulls,
            indices.len(),
        )?));
    }
    if let DataType::Union(fields, UnionMode::Dense) = value.data_type() {
        let source = value.as_union();
        let mut type_ids = Vec::with_capacity(indices.len());
        let mut offsets = Vec::with_capacity(indices.len());
        for index in indices.values() {
            let index = usize::try_from(*index)
                .map_err(|_| internal_datafusion_err!("union row index exceeds usize"))?;
            type_ids.push(source.type_id(index));
            offsets.push(source.offsets().expect("dense union has offsets")[index]);
        }
        return Ok(Arc::new(UnionArray::try_new(
            fields.clone(),
            ScalarBuffer::from(type_ids),
            Some(ScalarBuffer::from(offsets)),
            fields
                .iter()
                .map(|(id, _)| Arc::clone(source.child(id)))
                .collect(),
        )?));
    }
    Ok(take(value.as_ref(), indices, None)?)
}

fn normalize_run_end<R: RunEndIndexType>(
    source: &RunArray<R>,
    target_type: &DataType,
    target_value_type: &DataType,
    require_non_null: bool,
) -> Result<ArrayRef> {
    let run_ends =
        PrimitiveArray::<R>::from_iter_values(source.run_ends().sliced_values());
    let values = source.values_slice();
    let values = normalize_array(&values, target_value_type, require_non_null)?;

    let data = ArrayData::builder(target_type.clone())
        .len(source.len())
        .add_child_data(run_ends.to_data())
        .add_child_data(values.to_data())
        .build()?;
    data.validate_full()?;
    Ok(make_array(data))
}

fn normalize_list<Offset: OffsetSizeTrait>(
    value: &ArrayRef,
    field: &FieldRef,
) -> Result<ArrayRef> {
    let source = value.as_list::<Offset>();
    let source_offsets = source.value_offsets();
    let mut indices = Vec::new();
    let mut offsets = Vec::with_capacity(source.len() + 1);
    offsets.push(Offset::zero());
    for index in 0..source.len() {
        if source.is_valid(index) {
            let start = source_offsets[index].as_usize();
            let end = source_offsets[index + 1].as_usize();
            indices.extend((start..end).map(|index| index as u64));
        }
        offsets.push(Offset::from_usize(indices.len()).ok_or_else(|| {
            internal_datafusion_err!("list offset exceeds target offset type")
        })?);
    }
    let values = take_rows(source.values(), &UInt64Array::from_iter_values(indices))?;
    let values = normalize_array(&values, field.data_type(), !field.is_nullable())?;
    Ok(Arc::new(GenericListArray::<Offset>::try_new(
        Arc::clone(field),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        values,
        source.nulls().cloned(),
    )?))
}

fn normalize_list_view<Offset: OffsetSizeTrait>(
    value: &ArrayRef,
    field: &FieldRef,
) -> Result<ArrayRef> {
    let source = value.as_list_view::<Offset>();
    let mut ranges = Vec::new();
    ranges.try_reserve(source.len()).map_err(|error| {
        internal_datafusion_err!("failed to reserve list-view ranges: {error}")
    })?;
    for index in 0..source.len() {
        if source.is_valid(index) {
            let start = source.value_offsets()[index].as_usize();
            let size = source.value_sizes()[index].as_usize();
            let end = start.checked_add(size).ok_or_else(|| {
                internal_datafusion_err!("list-view offset plus size overflow")
            })?;
            if end > source.values().len() {
                return internal_err!(
                    "list-view range {start}..{end} exceeds child length {}",
                    source.values().len()
                );
            }
            if start != end {
                ranges.push((start, end));
            }
        }
    }

    // Copy the union of referenced ranges once. Overlapping views therefore
    // remain bounded by the original backing array instead of sum(view_sizes).
    ranges.sort_unstable_by_key(|(start, _)| *start);
    let mut merged: Vec<(usize, usize, usize)> = Vec::new();
    merged.try_reserve(ranges.len()).map_err(|error| {
        internal_datafusion_err!("failed to reserve merged list-view ranges: {error}")
    })?;
    for (start, end) in ranges {
        match merged.last_mut() {
            Some((_, merged_end, _)) if start <= *merged_end => {
                *merged_end = (*merged_end).max(end);
            }
            _ => {
                let new_start = match merged.last() {
                    Some((previous_start, previous_end, previous_new_start)) => {
                        previous_new_start
                            .checked_add(previous_end - previous_start)
                            .ok_or_else(|| {
                                internal_datafusion_err!(
                                    "compacted list-view length overflow"
                                )
                            })?
                    }
                    None => 0,
                };
                merged.push((start, end, new_start));
            }
        }
    }

    let retained_len = match merged.last() {
        Some((start, end, new_start)) => {
            new_start.checked_add(end - start).ok_or_else(|| {
                internal_datafusion_err!("compacted list-view length overflow")
            })?
        }
        None => 0,
    };
    let mut indices = Vec::new();
    indices.try_reserve(retained_len).map_err(|error| {
        internal_datafusion_err!("failed to reserve compacted list-view values: {error}")
    })?;
    for (start, end, _) in &merged {
        indices.extend((*start..*end).map(|index| index as u64));
    }

    let values = take_rows(source.values(), &UInt64Array::from_iter_values(indices))?;
    let values = normalize_array(&values, field.data_type(), !field.is_nullable())?;

    let mut offsets = Vec::new();
    let mut sizes = Vec::new();
    offsets.try_reserve(source.len()).map_err(|error| {
        internal_datafusion_err!("failed to reserve list-view offsets: {error}")
    })?;
    sizes.try_reserve(source.len()).map_err(|error| {
        internal_datafusion_err!("failed to reserve list-view sizes: {error}")
    })?;
    for index in 0..source.len() {
        if source.is_null(index) {
            offsets.push(Offset::zero());
            sizes.push(Offset::zero());
            continue;
        }
        let start = source.value_offsets()[index].as_usize();
        let size = source.value_sizes()[index].as_usize();
        if size == 0 {
            offsets.push(Offset::zero());
            sizes.push(Offset::zero());
            continue;
        }
        let merged_index = merged.partition_point(|(_, end, _)| *end <= start);
        let &(range_start, range_end, new_start) =
            merged.get(merged_index).ok_or_else(|| {
                internal_datafusion_err!("list-view range was not retained")
            })?;
        let end = start.checked_add(size).ok_or_else(|| {
            internal_datafusion_err!("list-view offset plus size overflow")
        })?;
        if start < range_start || end > range_end {
            return internal_err!("list-view range was not retained contiguously");
        }
        let offset = new_start
            .checked_add(start - range_start)
            .ok_or_else(|| internal_datafusion_err!("list-view offset overflow"))?;
        offsets.push(Offset::from_usize(offset).ok_or_else(|| {
            internal_datafusion_err!("list-view offset exceeds target offset type")
        })?);
        sizes.push(Offset::from_usize(size).ok_or_else(|| {
            internal_datafusion_err!("list-view size exceeds target offset type")
        })?);
    }

    Ok(Arc::new(GenericListViewArray::<Offset>::try_new(
        Arc::clone(field),
        ScalarBuffer::from(offsets),
        ScalarBuffer::from(sizes),
        values,
        source.nulls().cloned(),
    )?))
}

fn normalize_fixed_size_list(
    value: &ArrayRef,
    field: &FieldRef,
    size: i32,
) -> Result<ArrayRef> {
    let source = value.as_fixed_size_list();
    let size = usize::try_from(size).map_err(|_| {
        internal_datafusion_err!("fixed-size-list size cannot be negative")
    })?;
    let values_len = source.len().checked_mul(size).ok_or_else(|| {
        internal_datafusion_err!("fixed-size-list child length overflow")
    })?;
    if source.values().len() != values_len {
        return internal_err!(
            "fixed-size-list has {} child values, expected {values_len}",
            source.values().len()
        );
    }

    let child_nulls = source.nulls().map(|nulls| nulls.expand(size));
    let values = materialize_masked_values(source.values(), child_nulls.as_ref())?;
    let values = normalize_array(&values, field.data_type(), !field.is_nullable())?;
    Ok(Arc::new(FixedSizeListArray::try_new_with_length(
        Arc::clone(field),
        i32::try_from(size)
            .map_err(|_| internal_datafusion_err!("fixed-size-list size exceeds i32"))?,
        values,
        source.nulls().cloned(),
        source.len(),
    )?))
}

fn normalize_map(value: &ArrayRef, field: &FieldRef, ordered: bool) -> Result<ArrayRef> {
    let source = value.as_map();
    let source_offsets = source.value_offsets();
    let requires_entry_compaction = source.null_count() != 0
        || source_offsets.first().is_some_and(|offset| *offset != 0)
        || source_offsets
            .last()
            .is_some_and(|offset| *offset as usize != source.entries().len());

    let (offsets, entries) = if requires_entry_compaction {
        let retained_len = (0..source.len())
            .filter(|index| source.is_valid(*index))
            .try_fold(0_usize, |retained, index| {
                let start = source_offsets[index] as usize;
                let end = source_offsets[index + 1] as usize;
                retained.checked_add(end - start).ok_or_else(|| {
                    internal_datafusion_err!("compacted map length overflow")
                })
            })?;
        let mut indices = Vec::new();
        indices.try_reserve(retained_len).map_err(|error| {
            internal_datafusion_err!(
                "failed to reserve compacted map entry indices: {error}"
            )
        })?;
        let mut offsets = Vec::new();
        let offsets_len = source.len().checked_add(1).ok_or_else(|| {
            internal_datafusion_err!("compacted map offset length overflow")
        })?;
        offsets.try_reserve(offsets_len).map_err(|error| {
            internal_datafusion_err!("failed to reserve compacted map offsets: {error}")
        })?;
        offsets.push(0_i32);
        for index in 0..source.len() {
            if source.is_valid(index) {
                let start = source_offsets[index] as usize;
                let end = source_offsets[index + 1] as usize;
                indices.extend((start..end).map(|index| index as u64));
            }
            offsets.push(i32::try_from(indices.len()).map_err(|_| {
                internal_datafusion_err!("compacted map offset exceeds i32")
            })?);
        }
        let entries = take_rows(
            &(Arc::new(source.entries().clone()) as ArrayRef),
            &UInt64Array::from_iter_values(indices),
        )?;
        (OffsetBuffer::new(ScalarBuffer::from(offsets)), entries)
    } else {
        (
            source.offsets().clone(),
            Arc::new(source.entries().clone()) as ArrayRef,
        )
    };

    let entries = normalize_array(&entries, field.data_type(), true)?;
    Ok(Arc::new(MapArray::try_new(
        Arc::clone(field),
        offsets,
        entries.as_struct().clone(),
        source.nulls().cloned(),
        ordered,
    )?))
}

fn normalize_sparse_union(
    value: &ArrayRef,
    source_fields: &arrow::datatypes::UnionFields,
    target_fields: &arrow::datatypes::UnionFields,
    require_non_null: bool,
) -> Result<ArrayRef> {
    let source = value
        .as_any()
        .downcast_ref::<UnionArray>()
        .ok_or_else(|| internal_datafusion_err!("expected sparse union array"))?;
    if source_fields.len() != target_fields.len() {
        return internal_err!(
            "cannot normalize sparse union with {} runtime fields to {} declared fields",
            source_fields.len(),
            target_fields.len()
        );
    }
    let field_mapping = source_fields
        .iter()
        .zip(target_fields.iter())
        .collect::<Vec<_>>();
    let type_ids = source
        .type_ids()
        .iter()
        .map(|source_type_id| {
            field_mapping
                .iter()
                .find_map(|((source_id, _), (target_id, _))| {
                    (*source_id == *source_type_id).then_some(*target_id)
                })
                .ok_or_else(|| {
                    internal_datafusion_err!(
                        "sparse union contains unknown runtime type id {source_type_id}"
                    )
                })
        })
        .collect::<Result<Vec<_>>>()?;
    let children = field_mapping
        .iter()
        .map(|((source_type_id, _), (_, target_field))| {
            let child = source.child(*source_type_id);
            let first_active =
                (0..source.len()).find(|index| source.type_id(*index) == *source_type_id);
            if require_non_null
                && (0..source.len()).any(|index| {
                    source.type_id(index) == *source_type_id && child.is_null(index)
                })
            {
                return internal_err!(
                    "Found unmasked nulls for non-nullable sparse union field {}",
                    target_field.name()
                );
            }
            let active = NullBuffer::from_iter(
                (0..source.len()).map(|index| source.type_id(index) == *source_type_id),
            );
            let child = materialize_masked_values(child, Some(&active))?;
            let child =
                normalize_array(&child, target_field.data_type(), require_non_null)?;
            if require_non_null
                && first_active.is_some()
                && child.logical_null_count() != 0
            {
                return internal_err!(
                    "Found unmasked nulls for non-nullable sparse union field {}",
                    target_field.name()
                );
            }
            Ok(child)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(UnionArray::try_new(
        target_fields.clone(),
        ScalarBuffer::from(type_ids),
        None,
        children,
    )?))
}

fn normalize_dense_union(
    value: &ArrayRef,
    source_fields: &arrow::datatypes::UnionFields,
    target_fields: &arrow::datatypes::UnionFields,
    require_non_null: bool,
) -> Result<ArrayRef> {
    let source = value
        .as_any()
        .downcast_ref::<UnionArray>()
        .ok_or_else(|| internal_datafusion_err!("expected dense union array"))?;
    if source_fields.len() != target_fields.len() {
        return internal_err!(
            "cannot normalize dense union with {} runtime fields to {} declared fields",
            source_fields.len(),
            target_fields.len()
        );
    }
    let field_mapping = source_fields
        .iter()
        .zip(target_fields.iter())
        .collect::<Vec<_>>();

    let mut referenced_offsets = vec![BTreeSet::new(); field_mapping.len()];
    for index in 0..source.len() {
        let source_type_id = source.type_id(index);
        let field_index = field_mapping
            .iter()
            .position(|((field_type_id, _), _)| *field_type_id == source_type_id)
            .ok_or_else(|| {
                internal_datafusion_err!(
                    "dense union contains unknown runtime type id {source_type_id}"
                )
            })?;
        referenced_offsets[field_index].insert(source.value_offset(index));
    }

    let mut children = Vec::new();
    let mut compacted_offsets = Vec::new();
    children.try_reserve(field_mapping.len()).map_err(|error| {
        internal_datafusion_err!("failed to reserve dense union children: {error}")
    })?;
    compacted_offsets
        .try_reserve(field_mapping.len())
        .map_err(|error| {
            internal_datafusion_err!("failed to reserve dense union offsets: {error}")
        })?;
    for (((source_type_id, _), (_, target_field)), referenced) in
        field_mapping.iter().zip(referenced_offsets)
    {
        let child = source.child(*source_type_id);
        if let Some(offset) = referenced.last()
            && *offset >= child.len()
        {
            return internal_err!(
                "dense union offset {offset} exceeds child length {}",
                child.len()
            );
        }
        let referenced = referenced.into_iter().collect::<Vec<_>>();
        let indices =
            UInt64Array::from_iter_values(referenced.iter().map(|offset| *offset as u64));
        let child = take_rows(child, &indices)?;
        let child = normalize_array(&child, target_field.data_type(), require_non_null)?;
        if require_non_null && child.logical_null_count() != 0 {
            return internal_err!(
                "Found unmasked nulls for non-nullable dense union field {}",
                target_field.name()
            );
        }
        children.push(child);
        compacted_offsets.push(referenced);
    }

    let mut type_ids = Vec::new();
    let mut offsets = Vec::new();
    type_ids.try_reserve(source.len()).map_err(|error| {
        internal_datafusion_err!("failed to reserve dense union type ids: {error}")
    })?;
    offsets.try_reserve(source.len()).map_err(|error| {
        internal_datafusion_err!("failed to reserve dense union offsets: {error}")
    })?;
    for index in 0..source.len() {
        let source_type_id = source.type_id(index);
        let field_index = field_mapping
            .iter()
            .position(|((field_type_id, _), _)| *field_type_id == source_type_id)
            .ok_or_else(|| {
                internal_datafusion_err!(
                    "dense union contains unknown runtime type id {source_type_id}"
                )
            })?;
        let target_type_id = field_mapping[field_index].1.0;
        let source_offset = source.value_offset(index);
        let compacted_offset = compacted_offsets[field_index]
            .binary_search(&source_offset)
            .map_err(|_| {
                internal_datafusion_err!(
                    "dense union offset {source_offset} was not retained"
                )
            })?;
        type_ids.push(target_type_id);
        offsets.push(
            i32::try_from(compacted_offset).map_err(|_| {
                internal_datafusion_err!("dense union offset exceeds i32")
            })?,
        );
    }

    Ok(Arc::new(UnionArray::try_new(
        target_fields.clone(),
        ScalarBuffer::from(type_ids),
        Some(ScalarBuffer::from(offsets)),
        children,
    )?))
}

/// Rebuild an accumulator result with the aggregate's declared list field.
///
/// The shared array aggregate accumulators use a nullable list field and can
/// derive nested fields from runtime arrays. Spark collect aggregates always
/// drop null inputs, so their element field is non-nullable. Reusing the
/// declared field also keeps nested types consistent with planning.
fn normalize_list_scalar(
    value: ScalarValue,
    list_type: &DataType,
) -> Result<ScalarValue> {
    let DataType::List(field) = list_type else {
        return internal_err!(
            "collect_list/collect_set expected List return type, got {list_type:?}"
        );
    };
    let ScalarValue::List(array) = value else {
        return internal_err!(
            "collect_list/collect_set accumulator returned a non-List value"
        );
    };
    if array.len() != 1 {
        return internal_err!(
            "collect_list/collect_set accumulator returned {} rows, expected one",
            array.len()
        );
    }
    if array.is_null(0) {
        return Ok(ScalarValue::new_null_list(
            field.data_type().clone(),
            field.is_nullable(),
            1,
        ));
    }

    let values = normalize_array(&array.value(0), field.data_type(), true)?;
    if values.logical_null_count() != 0 {
        return internal_err!(
            "Found unmasked nulls for non-nullable collect_list/collect_set element"
        );
    }
    Ok(SingleRowListArrayBuilder::new(values)
        .with_field(field)
        .build_list_scalar())
}

// <https://spark.apache.org/docs/latest/api/sql/index.html#collect_list>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCollectList {
    signature: Signature,
}

impl Default for SparkCollectList {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkCollectList {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for SparkCollectList {
    fn name(&self) -> &str {
        "collect_list"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(collect_type(arg_types[0].clone()))
    }

    fn is_nullable(&self) -> bool {
        false
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Field::new_list(
                format_state_name(args.name, "collect_list"),
                Field::new_list_field(args.input_fields[0].data_type().clone(), false),
                true,
            )
            .into(),
        ])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let element_type = acc_args.expr_fields[0].data_type().clone();
        let ignore_nulls = true;
        Ok(Box::new(NullToEmptyListAccumulator::new(
            ArrayAggAccumulator::try_new(&element_type, ignore_nulls)?,
            acc_args.return_type().clone(),
        )))
    }

    fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {
        empty_list_scalar(data_type)
    }
}

// <https://spark.apache.org/docs/latest/api/sql/index.html#collect_set>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCollectSet {
    signature: Signature,
}

impl Default for SparkCollectSet {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkCollectSet {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for SparkCollectSet {
    fn name(&self) -> &str {
        "collect_set"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(collect_type(arg_types[0].clone()))
    }

    fn is_nullable(&self) -> bool {
        false
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Field::new_list(
                format_state_name(args.name, "collect_set"),
                Field::new_list_field(args.input_fields[0].data_type().clone(), false),
                true,
            )
            .into(),
        ])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let element_type = acc_args.expr_fields[0].data_type().clone();
        let ignore_nulls = true;
        Ok(Box::new(NullToEmptyListAccumulator::new(
            DistinctArrayAggAccumulator::try_new(&element_type, None, ignore_nulls)?,
            acc_args.return_type().clone(),
        )))
    }

    fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {
        empty_list_scalar(data_type)
    }
}

/// Wrapper accumulator that returns an empty list instead of NULL when all inputs are NULL.
/// This implements Spark's behavior for collect_list and collect_set.
#[derive(Debug)]
struct NullToEmptyListAccumulator<T: Accumulator> {
    inner: T,
    list_type: DataType,
}

impl<T: Accumulator> NullToEmptyListAccumulator<T> {
    pub fn new(inner: T, list_type: DataType) -> Self {
        Self { inner, list_type }
    }

    fn normalize_input(&self, value: &ArrayRef) -> Result<ArrayRef> {
        let DataType::List(field) = &self.list_type else {
            return internal_err!(
                "collect_list/collect_set expected List return type, got {:?}",
                self.list_type
            );
        };
        if value.data_type() == field.data_type() && !has_compactable_storage(value) {
            Ok(Arc::clone(value))
        } else if matches!(
            (value.data_type(), field.data_type()),
            (
                DataType::RunEndEncoded(source_run_ends, _),
                DataType::RunEndEncoded(target_run_ends, _)
            ) if source_run_ends.data_type() == target_run_ends.data_type()
        ) {
            // Normalizing a run-end encoded array through `take` requires an
            // index for every logical row. Dispatch it directly so work stays
            // bounded by the physical run count.
            normalize_array(value, field.data_type(), false)
        } else {
            // Materialize only retained rows before narrowing nested fields.
            // A slice can still reference null payload outside its logical rows.
            let value = match value.logical_nulls() {
                Some(nulls) if nulls.null_count() != 0 => {
                    let indices = UInt64Array::from_iter_values(
                        nulls.valid_indices().map(|index| index as u64),
                    );
                    take_rows(value, &indices)?
                }
                _ => Arc::clone(value),
            };
            normalize_array(&value, field.data_type(), true)
        }
    }
}

impl<T: Accumulator> Accumulator for NullToEmptyListAccumulator<T> {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let [value] = values else {
            return self.inner.update_batch(values);
        };
        let value = self.normalize_input(value)?;
        self.inner.update_batch(&[value])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner
            .state()?
            .into_iter()
            .map(|value| normalize_list_scalar(value, &self.list_type))
            .collect()
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let result = self.inner.evaluate()?;
        if result.is_null() {
            empty_list_scalar(&self.list_type)
        } else {
            normalize_list_scalar(result, &self.list_type)
        }
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let [value] = values else {
            return self.inner.retract_batch(values);
        };
        let value = self.normalize_input(value)?;
        self.inner.retract_batch(&[value])
    }

    fn supports_retract_batch(&self) -> bool {
        self.inner.supports_retract_batch()
    }

    fn size(&self) -> usize {
        self.inner.size() + self.list_type.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        DictionaryArray, FixedSizeListArray, Int8Array, Int16Array, Int32Array,
        Int64Array, ListArray, ListViewArray, MapArray, NullArray, RunArray, StringArray,
        StructArray, UnionArray,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow::datatypes::{
        Fields, Int8Type, Int16Type, Int32Type, Int64Type, Schema, UnionFields,
    };
    use arrow::record_batch::RecordBatch;
    use arrow::util::display::array_value_to_string;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_expr::AggregateUDF;
    use std::collections::HashMap;

    fn list_type(element_type: DataType) -> DataType {
        DataType::List(Arc::new(Field::new_list_field(element_type, false)))
    }

    fn accumulator(
        element_type: &DataType,
        distinct: bool,
    ) -> Result<Box<dyn Accumulator>> {
        let return_type = list_type(element_type.clone());
        if distinct {
            Ok(Box::new(NullToEmptyListAccumulator::new(
                DistinctArrayAggAccumulator::try_new(element_type, None, true)?,
                return_type,
            )))
        } else {
            Ok(Box::new(NullToEmptyListAccumulator::new(
                ArrayAggAccumulator::try_new(element_type, true)?,
                return_type,
            )))
        }
    }

    fn assert_empty_list(value: &ScalarValue) {
        let ScalarValue::List(array) = value else {
            panic!("expected a list scalar")
        };
        assert_eq!(array.value(0).len(), 0);
    }

    fn assert_nested_values(value: &ScalarValue) {
        let ScalarValue::List(array) = value else {
            panic!("expected a list scalar")
        };
        let values = array.value(0);
        let structs = values
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("expected struct values");
        let integers = structs
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("expected Int32 struct field");
        let mut actual: Vec<i32> = integers.iter().map(Option::unwrap).collect();
        actual.sort_unstable();
        assert_eq!(actual, vec![1, 2]);
    }

    fn assert_list_values(
        value: &ScalarValue,
        element_type: &DataType,
        expected: &[&str],
    ) -> Result<()> {
        assert_eq!(value.data_type(), list_type(element_type.clone()));
        let ScalarValue::List(array) = value else {
            panic!("expected a list scalar")
        };
        assert_list_array_values(array, element_type, expected)
    }

    fn assert_list_array_values(
        array: &ListArray,
        element_type: &DataType,
        expected: &[&str],
    ) -> Result<()> {
        assert_eq!(array.data_type(), &list_type(element_type.clone()));
        array.to_data().validate_full()?;

        let values = array.value(0);
        assert_eq!(values.logical_null_count(), 0);
        let mut actual = (0..values.len())
            .map(|index| array_value_to_string(values.as_ref(), index))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        actual.sort_unstable();

        let mut expected = expected
            .iter()
            .map(|value| (*value).to_string())
            .collect::<Vec<_>>();
        expected.sort_unstable();
        assert_eq!(actual, expected);
        Ok(())
    }

    fn assert_accumulator_outputs(
        values: ArrayRef,
        distinct: bool,
        expected: &[&str],
    ) -> Result<()> {
        let element_type = values.data_type().clone();
        assert_accumulator_outputs_with_type(values, &element_type, distinct, expected)
    }

    fn assert_accumulator_outputs_with_type(
        values: ArrayRef,
        element_type: &DataType,
        distinct: bool,
        expected: &[&str],
    ) -> Result<()> {
        let mut partial = accumulator(element_type, distinct)?;
        partial.update_batch(std::slice::from_ref(&values))?;
        let state = partial.state()?;
        assert_list_values(&state[0], element_type, expected)?;
        assert_list_values(&state[0].clone().compacted(), element_type, expected)?;

        let mut final_accumulator = accumulator(element_type, distinct)?;
        final_accumulator.merge_batch(&[state[0].to_array()?])?;
        let merged = final_accumulator.evaluate()?;
        assert_list_values(&merged, element_type, expected)?;
        assert_list_values(&merged.compacted(), element_type, expected)?;

        let mut single = accumulator(element_type, distinct)?;
        single.update_batch(&[values])?;
        let value = single.evaluate()?;
        assert_list_values(&value, element_type, expected)?;
        assert_list_values(&value.compacted(), element_type, expected)
    }

    #[test]
    fn review_regression_masked_map_defaults() -> Result<()> {
        let entries = StructArray::try_new(
            Fields::from(vec![
                Field::new("key", DataType::Int32, false),
                Field::new("value", DataType::Int32, false),
            ]),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![7])),
            ],
            None,
        )?;
        let map = Arc::new(MapArray::try_new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1])),
            entries,
            None,
            false,
        )?) as ArrayRef;
        let optional = Arc::new(StructArray::try_new(
            Fields::from(vec![Field::new("map", map.data_type().clone(), false)]),
            vec![Arc::clone(&map)],
            Some(NullBuffer::new_null(1)),
        )?) as ArrayRef;
        let values = Arc::new(StructArray::try_new(
            Fields::from(vec![Field::new(
                "optional",
                optional.data_type().clone(),
                true,
            )]),
            vec![optional],
            None,
        )?) as ArrayRef;
        values.to_data().validate_full()?;
        for distinct in [false, true] {
            let mut accumulator = accumulator(values.data_type(), distinct)?;
            accumulator.update_batch(std::slice::from_ref(&values))?;
            let ScalarValue::List(result) = accumulator.evaluate()? else {
                panic!("expected list")
            };
            result.to_data().validate_full()?;
            let rows = result.value(0);
            assert!(rows.as_struct().column(0).is_null(0));
        }

        let fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("integer", DataType::Int32, false),
                Field::new("map", map.data_type().clone(), false),
            ],
        )?;
        let union = Arc::new(UnionArray::try_new(
            fields,
            ScalarBuffer::from(vec![0_i8]),
            None,
            vec![Arc::new(Int32Array::from(vec![1])), map],
        )?) as ArrayRef;
        assert_accumulator_outputs(union, false, &["{integer=1}"])
    }

    #[test]
    fn review_regression_masked_payload_is_not_replicated() -> Result<()> {
        const ROWS: usize = 1_024;
        const BYTES: usize = 4_096;
        let payload = "x".repeat(BYTES);
        let strings = Arc::new(StringArray::from_iter(
            std::iter::once(Some(payload.as_str()))
                .chain(std::iter::repeat_n(None, ROWS - 1)),
        )) as ArrayRef;
        let optional = Arc::new(StructArray::try_new(
            Fields::from(vec![Field::new("payload", DataType::Utf8, true)]),
            vec![strings],
            Some(NullBuffer::from_iter((0..ROWS).map(|row| row == 0))),
        )?) as ArrayRef;
        let values = Arc::new(StructArray::try_new(
            Fields::from(vec![Field::new(
                "optional",
                optional.data_type().clone(),
                true,
            )]),
            vec![optional],
            None,
        )?) as ArrayRef;
        let mut accumulator = accumulator(values.data_type(), false)?;
        accumulator.update_batch(&[values])?;
        let ScalarValue::List(result) = accumulator.evaluate()? else {
            panic!("expected list")
        };
        result.to_data().validate_full()?;
        let rows = result.value(0);
        let optional = rows.as_struct().column(0).as_struct();
        assert_eq!(optional.null_count(), ROWS - 1);
        assert!(optional.is_valid(0));
        assert_eq!(optional.column(0).as_string::<i32>().value(0), payload);
        assert_eq!(
            optional.column(0).as_string::<i32>().value_data().len(),
            BYTES
        );

        let fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("integer", DataType::Int32, false),
                Field::new("string", DataType::Utf8, false),
            ],
        )?;
        let union = Arc::new(UnionArray::try_new(
            fields,
            ScalarBuffer::from(
                (0..ROWS).map(|row| i8::from(row == 0)).collect::<Vec<_>>(),
            ),
            None,
            vec![
                Arc::new(Int32Array::from_iter_values(0..ROWS as i32)),
                Arc::new(StringArray::from_iter_values(
                    std::iter::once(payload.as_str())
                        .chain(std::iter::repeat_n("", ROWS - 1)),
                )),
            ],
        )?) as ArrayRef;
        let normalized = normalize_array(&union, union.data_type(), true)?;
        assert_eq!(
            normalized.as_union().type_ids(),
            union.as_union().type_ids()
        );
        assert_eq!(
            normalized.as_union().child(1).as_string::<i32>().value(0),
            payload
        );
        assert_eq!(
            normalized
                .as_union()
                .child(1)
                .as_string::<i32>()
                .value_data()
                .len(),
            BYTES
        );
        normalized.to_data().validate_full()?;

        let fixed = Arc::new(FixedSizeListArray::try_new(
            Arc::new(Field::new_list_field(DataType::Utf8, true)),
            1,
            Arc::new(StringArray::from_iter(
                std::iter::once(Some(payload.as_str()))
                    .chain(std::iter::repeat_n(None, ROWS - 1)),
            )),
            Some(NullBuffer::from_iter((0..ROWS).map(|row| row == 0))),
        )?) as ArrayRef;
        let normalized = normalize_array(&fixed, fixed.data_type(), false)?;
        normalized.to_data().validate_full()?;
        let fixed = normalized.as_fixed_size_list();
        assert_eq!(fixed.null_count(), ROWS - 1);
        assert_eq!(fixed.values().as_string::<i32>().value_data().len(), BYTES);
        assert_eq!(fixed.values().as_string::<i32>().value(0), payload);
        Ok(())
    }

    #[test]
    fn review_regression_dense_union_keeps_shared_offsets() -> Result<()> {
        const ROWS: usize = 1_024;
        const BYTES: usize = 4_096;
        let fields = UnionFields::try_new(
            vec![0],
            vec![Field::new("string", DataType::Utf8, false)],
        )?;
        let values = Arc::new(UnionArray::try_new(
            fields,
            ScalarBuffer::from(vec![0_i8; ROWS]),
            Some(ScalarBuffer::from(vec![0_i32; ROWS])),
            vec![Arc::new(StringArray::from(vec!["x".repeat(BYTES)]))],
        )?) as ArrayRef;
        values.to_data().validate_full()?;
        let mut acc = accumulator(values.data_type(), false)?;
        acc.update_batch(&[values])?;
        let ScalarValue::List(result) = acc.evaluate()? else {
            panic!("expected list")
        };
        result.to_data().validate_full()?;
        let rows = result.value(0);
        assert_eq!(rows.len(), ROWS);
        let union = rows.as_union();
        assert_eq!(union.child(0).len(), 1);
        assert_eq!(union.child(0).as_string::<i32>().value_data().len(), BYTES);
        assert_eq!(
            union.child(0).as_string::<i32>().value(0),
            "x".repeat(BYTES)
        );
        assert!(union.offsets().unwrap().iter().all(|offset| *offset == 0));

        // Removing a logical null must select only union metadata, rather than
        // expanding the retained shared offset through Arrow's generic take.
        let fields = UnionFields::try_new(
            vec![0],
            vec![Field::new("string", DataType::Utf8, true)],
        )?;
        let mut offsets = vec![0_i32; ROWS];
        offsets[1] = 1;
        let values = Arc::new(UnionArray::try_new(
            fields,
            ScalarBuffer::from(vec![0_i8; ROWS]),
            Some(ScalarBuffer::from(offsets)),
            vec![Arc::new(StringArray::from(vec![
                Some("x".repeat(BYTES)),
                None,
            ]))],
        )?) as ArrayRef;
        values.to_data().validate_full()?;
        let mut acc = accumulator(values.data_type(), false)?;
        acc.update_batch(&[values])?;
        let ScalarValue::List(result) = acc.evaluate()? else {
            panic!("expected list")
        };
        result.to_data().validate_full()?;
        let rows = result.value(0);
        assert_eq!(rows.len(), ROWS - 1);
        let union = rows.as_union();
        assert_eq!(union.child(0).len(), 1);
        assert_eq!(union.child(0).as_string::<i32>().value_data().len(), BYTES);
        assert_eq!(
            union.child(0).as_string::<i32>().value(0),
            "x".repeat(BYTES)
        );
        Ok(())
    }

    #[test]
    fn review_regression_zero_field_struct_keeps_length() -> Result<()> {
        let optional = Arc::new(StructArray::new_empty_fields(
            1,
            Some(NullBuffer::new_null(1)),
        )) as ArrayRef;
        let values = Arc::new(StructArray::try_new(
            Fields::from(vec![Field::new(
                "optional",
                optional.data_type().clone(),
                true,
            )]),
            vec![optional],
            None,
        )?) as ArrayRef;
        values.to_data().validate_full()?;
        for distinct in [false, true] {
            let mut accumulator = accumulator(values.data_type(), distinct)?;
            accumulator.update_batch(std::slice::from_ref(&values))?;
            let ScalarValue::List(result) = accumulator.evaluate()? else {
                panic!("expected list")
            };
            result.to_data().validate_full()?;
            let rows = result.value(0);
            assert_eq!(rows.len(), 1);
            let optional = rows.as_struct().column(0).as_struct();
            assert_eq!(optional.len(), 1);
            assert!(optional.is_null(0));
        }
        Ok(())
    }

    #[test]
    fn review_regression_masked_list_preserves_item_field() -> Result<()> {
        let item = Arc::new(
            Field::new("custom_item", DataType::Int32, false).with_metadata(
                HashMap::from([("tag".to_string(), "preserved".to_string())]),
            ),
        );
        let lists = vec![
            Arc::new(ListArray::try_new(
                Arc::clone(&item),
                OffsetBuffer::from_lengths([1, 1]),
                Arc::new(Int32Array::from(vec![7, 8])),
                None,
            )?) as ArrayRef,
            Arc::new(arrow::array::LargeListArray::try_new(
                Arc::clone(&item),
                OffsetBuffer::from_lengths([1, 1]),
                Arc::new(Int32Array::from(vec![7, 8])),
                None,
            )?) as ArrayRef,
        ];
        for list in lists {
            let optional = Arc::new(StructArray::try_new(
                Fields::from(vec![Field::new("items", list.data_type().clone(), false)]),
                vec![list],
                Some(NullBuffer::from(vec![true, false])),
            )?) as ArrayRef;
            let input = Arc::new(StructArray::try_new(
                Fields::from(vec![Field::new(
                    "optional",
                    optional.data_type().clone(),
                    true,
                )]),
                vec![optional],
                None,
            )?) as ArrayRef;
            for distinct in [false, true] {
                let mut acc = accumulator(input.data_type(), distinct)?;
                acc.update_batch(std::slice::from_ref(&input))?;
                let ScalarValue::List(result) = acc.evaluate()? else {
                    panic!("expected list")
                };
                result.to_data().validate_full()?;
                let rows = result.value(0);
                assert_eq!(rows.data_type(), input.data_type());
                let optional = rows.as_struct().column(0).as_struct();
                assert_eq!(optional.len(), 2);
                assert_eq!(optional.null_count(), 1);
                let visible = (0..2).find(|index| optional.is_valid(*index)).unwrap();
                assert_eq!(
                    array_value_to_string(optional.column(0).as_ref(), visible)?,
                    "[7]"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn review_regression_nested_dense_union_keeps_shared_offsets() -> Result<()> {
        const ROWS: usize = 1_024;
        const BYTES: usize = 4_096;
        let fields = UnionFields::try_new(
            vec![0],
            vec![Field::new("string", DataType::Utf8, false)],
        )?;
        let union = Arc::new(UnionArray::try_new(
            fields,
            ScalarBuffer::from(vec![0_i8; ROWS]),
            Some(ScalarBuffer::from(vec![0_i32; ROWS])),
            vec![Arc::new(StringArray::from(vec!["x".repeat(BYTES)]))],
        )?) as ArrayRef;
        let structure = Arc::new(StructArray::try_new(
            Fields::from(vec![Field::new("value", union.data_type().clone(), false)]),
            vec![union],
            Some(NullBuffer::from_iter((0..ROWS).map(|row| row != ROWS - 1))),
        )?) as ArrayRef;
        let outer = Arc::new(StructArray::try_new(
            Fields::from(vec![Field::new(
                "optional",
                structure.data_type().clone(),
                true,
            )]),
            vec![Arc::clone(&structure)],
            None,
        )?) as ArrayRef;
        let normalized = normalize_array(&outer, outer.data_type(), true)?;
        normalized.to_data().validate_full()?;
        let optional = normalized.as_struct().column(0).as_struct();
        assert_eq!(optional.null_count(), 1);
        assert_eq!(
            optional
                .column(0)
                .as_union()
                .child(0)
                .as_string::<i32>()
                .value_data()
                .len(),
            BYTES
        );

        let mut acc = accumulator(structure.data_type(), false)?;
        acc.update_batch(&[structure])?;
        let ScalarValue::List(result) = acc.evaluate()? else {
            panic!("expected list")
        };
        result.to_data().validate_full()?;
        let rows = result.value(0);
        assert_eq!(rows.len(), ROWS - 1);
        let union = rows.as_struct().column(0).as_union();
        assert_eq!(union.child(0).as_string::<i32>().value_data().len(), BYTES);
        assert!(union.offsets().unwrap().iter().all(|offset| *offset == 0));
        Ok(())
    }

    #[test]
    fn review_regression_masked_null_first_union() -> Result<()> {
        for mode in [UnionMode::Dense, UnionMode::Sparse] {
            let fields = UnionFields::try_new(
                vec![3, 7],
                vec![
                    Field::new("null", DataType::Null, true),
                    Field::new("integer", DataType::Int32, false),
                ],
            )?;
            let union = Arc::new(UnionArray::try_new(
                fields,
                ScalarBuffer::from(vec![7_i8; 3]),
                (mode == UnionMode::Dense).then(|| ScalarBuffer::from(vec![0_i32, 1, 0])),
                vec![
                    Arc::new(NullArray::new(3)),
                    Arc::new(Int32Array::from(vec![42, 99, 12])),
                ],
            )?) as ArrayRef;
            let source = Arc::new(StructArray::try_new(
                Fields::from(vec![Field::new("value", union.data_type().clone(), false)]),
                vec![union],
                Some(NullBuffer::from(vec![true, false, true])),
            )?) as ArrayRef;
            let normalized = normalize_array(&source, source.data_type(), true)?;
            normalized.to_data().validate_full()?;
            assert!(normalized.is_null(1));
            let union = normalized.as_struct().column(0).as_union();
            assert_eq!(union.type_id(0), 7);
            assert_eq!(union.type_id(2), 7);
            assert_eq!(
                union
                    .child(7)
                    .as_primitive::<Int32Type>()
                    .value(union.value_offset(0)),
                42
            );
            assert_eq!(
                union
                    .child(7)
                    .as_primitive::<Int32Type>()
                    .value(union.value_offset(2)),
                if mode == UnionMode::Dense { 42 } else { 12 }
            );
        }
        Ok(())
    }

    #[test]
    fn review_regression_wrapped_dense_union_keeps_shared_offsets() -> Result<()> {
        const ROWS: usize = 1024;
        const BYTES: usize = 4096;
        for wrapper in [2, 0, 1] {
            let fields = UnionFields::try_new(
                vec![3, 7],
                vec![
                    Field::new("text", DataType::Utf8, false),
                    Field::new("integer", DataType::Int32, false),
                ],
            )?;
            let union = Arc::new(UnionArray::try_new(
                fields,
                ScalarBuffer::from(
                    (0..ROWS)
                        .map(|row| if row % 2 == 0 { 3_i8 } else { 7_i8 })
                        .collect::<Vec<_>>(),
                ),
                Some(ScalarBuffer::from(vec![0_i32; ROWS])),
                vec![
                    Arc::new(StringArray::from(vec!["x".repeat(BYTES)])),
                    Arc::new(Int32Array::from(vec![42])),
                ],
            )?) as ArrayRef;
            let field = Arc::new(Field::new("item", union.data_type().clone(), false));
            let wrapped = if wrapper == 1 {
                Arc::new(FixedSizeListArray::try_new(field, 1, union, None)?) as ArrayRef
            } else if wrapper == 2 {
                let entries = StructArray::try_new(
                    Fields::from(vec![
                        Field::new("key", DataType::Int32, false),
                        Field::new("value", union.data_type().clone(), false),
                    ]),
                    vec![
                        Arc::new(Int32Array::from_iter_values(0..ROWS as i32)),
                        union,
                    ],
                    None,
                )?;
                let field =
                    Arc::new(Field::new("entries", entries.data_type().clone(), false));
                Arc::new(MapArray::try_new(
                    field,
                    OffsetBuffer::new(ScalarBuffer::from(
                        (0..=ROWS as i32).collect::<Vec<_>>(),
                    )),
                    entries,
                    None,
                    true,
                )?) as ArrayRef
            } else {
                Arc::new(ListArray::try_new(
                    field,
                    OffsetBuffer::new(ScalarBuffer::from(
                        (0..=ROWS as i32).collect::<Vec<_>>(),
                    )),
                    union,
                    None,
                )?) as ArrayRef
            };
            let source = Arc::new(StructArray::try_new(
                Fields::from(vec![Field::new(
                    "value",
                    wrapped.data_type().clone(),
                    false,
                )]),
                vec![wrapped],
                Some(NullBuffer::from_iter((0..ROWS).map(|row| row != ROWS - 1))),
            )?) as ArrayRef;
            let mut acc = accumulator(source.data_type(), false)?;
            acc.update_batch(&[source])?;
            let ScalarValue::List(result) = acc.evaluate()? else {
                panic!("expected list")
            };
            result.to_data().validate_full()?;
            let rows = result.value(0);
            assert_eq!(rows.len(), ROWS - 1);
            let wrapped = rows.as_struct().column(0);
            let values = if wrapper == 1 {
                wrapped.as_fixed_size_list().values()
            } else if wrapper == 2 {
                assert!(matches!(wrapped.data_type(), DataType::Map(_, true)));
                wrapped.as_map().values()
            } else {
                wrapped.as_list::<i32>().values()
            };
            let union = values.as_union();
            assert_eq!(union.child(3).as_string::<i32>().value_data().len(), BYTES);
            for row in 0..ROWS - 1 {
                assert_eq!(union.type_id(row), if row % 2 == 0 { 3 } else { 7 });
                assert_eq!(union.value_offset(row), 0);
                if row % 2 == 0 {
                    assert_eq!(
                        union.child(3).as_string::<i32>().value(0),
                        "x".repeat(BYTES)
                    );
                } else {
                    assert_eq!(union.child(7).as_primitive::<Int32Type>().value(0), 42);
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn review_regression_inactive_null_union_grouped_and_window_sql() -> Result<()>
    {
        let fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("integer", DataType::Int32, false),
                Field::new("null", DataType::Null, true),
            ],
        )?;
        let values = Arc::new(UnionArray::try_new(
            fields,
            ScalarBuffer::from(vec![0_i8; 4]),
            None,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 1, 2])),
                Arc::new(NullArray::new(4)),
            ],
        )?) as ArrayRef;
        values.to_data().validate_full()?;
        let element_type = values.data_type().clone();
        for target_partitions in [1, 4] {
            let ctx = SessionContext::new_with_config(
                SessionConfig::new().with_target_partitions(target_partitions),
            );
            ctx.register_udaf(AggregateUDF::new_from_impl(SparkCollectList::new()));
            ctx.register_udaf(AggregateUDF::new_from_impl(SparkCollectSet::new()));
            ctx.register_batch(
                "sparse_null_arm",
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("g", DataType::Int32, false),
                        Field::new("id", DataType::Int32, false),
                        Field::new("x", element_type.clone(), false),
                    ])),
                    vec![
                        Arc::new(Int32Array::from(vec![0, 0, 1, 1])),
                        Arc::new(Int32Array::from(vec![0, 1, 2, 3])),
                        Arc::clone(&values),
                    ],
                )?,
            )?;
            for aggregate in ["collect_list", "collect_set"] {
                for (query, expected) in [
                    (
                        format!(
                            "SELECT g, {aggregate}(x) FROM sparse_null_arm GROUP BY g ORDER BY g"
                        ),
                        vec![(0, vec![1, 2]), (1, vec![1, 2])],
                    ),
                    (
                        format!(
                            "SELECT id, {aggregate}(x) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM sparse_null_arm ORDER BY id"
                        ),
                        if aggregate == "collect_list" {
                            vec![
                                (0, vec![1]),
                                (1, vec![1, 2]),
                                (2, vec![1, 1, 2]),
                                (3, vec![1, 2, 2]),
                            ]
                        } else {
                            vec![
                                (0, vec![1]),
                                (1, vec![1, 2]),
                                (2, vec![1, 2]),
                                (3, vec![1, 2]),
                            ]
                        },
                    ),
                ] {
                    let output = ctx.sql(&query).await?.collect().await?;
                    let mut actual = vec![];
                    for batch in output {
                        assert_eq!(
                            batch.schema().field(1).data_type(),
                            &list_type(element_type.clone())
                        );
                        assert!(!batch.schema().field(1).is_nullable());
                        for column in batch.columns() {
                            column.to_data().validate_full()?;
                        }
                        for row in 0..batch.num_rows() {
                            let ScalarValue::List(result) =
                                ScalarValue::try_from_array(batch.column(1), row)?
                                    .compacted()
                            else {
                                panic!("expected list")
                            };
                            result.to_data().validate_full()?;
                            let values = result.value(0);
                            assert_eq!(values.data_type(), &element_type);
                            assert_eq!(values.logical_null_count(), 0);
                            let union = values.as_union();
                            let mut collected = (0..union.len())
                                .map(|index| {
                                    assert_eq!(union.type_id(index), 0);
                                    union
                                        .child(0)
                                        .as_primitive::<Int32Type>()
                                        .value(union.value_offset(index))
                                })
                                .collect::<Vec<_>>();
                            collected.sort_unstable();
                            actual.push((
                                batch.column(0).as_primitive::<Int32Type>().value(row),
                                collected,
                            ));
                        }
                    }
                    assert_eq!(
                        actual, expected,
                        "{query}, target_partitions={target_partitions}"
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn collect_types_have_non_nullable_elements() -> Result<()> {
        let element_type = DataType::Int32;
        let expected = list_type(element_type.clone());

        for aggregate in [
            &SparkCollectList::new() as &dyn AggregateUDFImpl,
            &SparkCollectSet::new() as &dyn AggregateUDFImpl,
        ] {
            assert_eq!(
                aggregate.return_type(std::slice::from_ref(&element_type))?,
                expected
            );

            let input_field = Arc::new(Field::new("input", element_type.clone(), true));
            let return_field =
                aggregate.return_field(std::slice::from_ref(&input_field))?;
            assert_eq!(return_field.data_type(), &expected);
            assert!(!return_field.is_nullable());

            let state_fields = aggregate.state_fields(StateFieldsArgs {
                name: aggregate.name(),
                input_fields: &[input_field],
                return_field: Arc::new(Field::new("result", expected.clone(), false)),
                ordering_fields: &[],
                is_distinct: false,
            })?;
            assert_eq!(state_fields[0].data_type(), &expected);
        }

        Ok(())
    }

    #[test]
    fn empty_partial_state_has_non_nullable_elements() -> Result<()> {
        let element_type = DataType::Int32;
        let expected = list_type(element_type.clone());

        for distinct in [false, true] {
            let mut partial = accumulator(&element_type, distinct)?;
            let state = partial.state()?;
            assert_eq!(state.len(), 1);
            assert!(state[0].is_null());
            assert_eq!(state[0].data_type(), expected);

            let ScalarValue::List(array) = &state[0] else {
                panic!("expected a list scalar")
            };
            let DataType::List(field) = array.data_type() else {
                panic!("expected a list data type")
            };
            assert_eq!(field.name(), "item");
            assert!(!field.is_nullable());

            let mut final_accumulator = accumulator(&element_type, distinct)?;
            final_accumulator.merge_batch(&[state[0].to_array()?])?;
            let value = final_accumulator.evaluate()?;
            assert!(!value.is_null());
            assert_eq!(value.data_type(), expected);
            assert_empty_list(&value);
        }

        Ok(())
    }

    #[test]
    fn empty_results_have_non_nullable_elements() -> Result<()> {
        let expected = list_type(DataType::Int32);

        for aggregate in [
            &SparkCollectList::new() as &dyn AggregateUDFImpl,
            &SparkCollectSet::new() as &dyn AggregateUDFImpl,
        ] {
            let value = aggregate.default_value(&expected)?;
            assert!(!value.is_null());
            assert_eq!(value.data_type(), expected);
            assert_empty_list(&value);
        }

        for distinct in [false, true] {
            let value = accumulator(&DataType::Int32, distinct)?.evaluate()?;
            assert!(!value.is_null());
            assert_eq!(value.data_type(), expected);
            assert_empty_list(&value);
        }

        Ok(())
    }

    #[test]
    fn accumulator_state_and_output_preserve_nested_type() -> Result<()> {
        let declared_fields =
            Fields::from(vec![Field::new("required", DataType::Int32, false)]);
        let element_type = DataType::Struct(declared_fields.clone());
        let expected = list_type(element_type.clone());

        // Exercise the downstream case from the issue: runtime arrays can carry
        // different nested nullability than the aggregate's declared type.
        let runtime_fields =
            Fields::from(vec![Field::new("required", DataType::Int32, true)]);
        let values = Arc::new(StructArray::new(
            runtime_fields,
            vec![Arc::new(Int32Array::from(vec![Some(1), Some(2)]))],
            None,
        )) as ArrayRef;

        let scalar =
            SingleRowListArrayBuilder::new(Arc::clone(&values)).build_list_scalar();
        let normalized = normalize_list_scalar(scalar, &expected)?;
        assert_eq!(normalized.data_type(), expected);
        assert_nested_values(&normalized);

        for distinct in [false, true] {
            let mut partial = accumulator(&element_type, distinct)?;
            partial.update_batch(std::slice::from_ref(&values))?;

            let state = partial.state()?;
            assert_eq!(state[0].data_type(), expected);
            assert_nested_values(&state[0]);

            let value = partial.evaluate()?;
            assert_eq!(value.data_type(), expected);
            assert_nested_values(&value);

            let mut final_accumulator = accumulator(&element_type, distinct)?;
            final_accumulator.merge_batch(&[state[0].to_array()?])?;
            let merged = final_accumulator.evaluate()?;
            assert_eq!(merged.data_type(), expected);
            assert_nested_values(&merged);
        }

        Ok(())
    }

    #[test]
    fn nested_runtime_null_in_non_nullable_declared_field_is_rejected() -> Result<()> {
        // Widening the output type would violate the aggregate's declared schema and
        // reintroduce the AggregateExec schema mismatch this normalization prevents.
        let declared_fields =
            Fields::from(vec![Field::new("required", DataType::Int32, false)]);
        let element_type = DataType::Struct(declared_fields);
        let runtime_fields =
            Fields::from(vec![Field::new("required", DataType::Int32, true)]);
        let values = Arc::new(StructArray::new(
            runtime_fields,
            vec![Arc::new(Int32Array::from(vec![Some(1), None]))],
            None,
        )) as ArrayRef;

        let scalar =
            SingleRowListArrayBuilder::new(Arc::clone(&values)).build_list_scalar();
        let error =
            normalize_list_scalar(scalar, &list_type(element_type.clone())).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Found unmasked nulls for non-nullable"),
            "unexpected error: {error}"
        );

        for distinct in [false, true] {
            let mut partial = accumulator(&element_type, distinct)?;
            let error = partial
                .update_batch(std::slice::from_ref(&values))
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("Found unmasked nulls for non-nullable"),
                "unexpected error: {error}"
            );
        }

        Ok(())
    }

    #[test]
    fn all_valid_struct_rows_with_only_null_required_values_are_rejected() -> Result<()> {
        let declared_fields =
            Fields::from(vec![Field::new("required", DataType::Int32, false)]);
        let element_type = DataType::Struct(declared_fields);
        let runtime_fields =
            Fields::from(vec![Field::new("required", DataType::Int32, true)]);
        let values = Arc::new(StructArray::new(
            runtime_fields,
            vec![Arc::new(Int32Array::from(vec![None, None]))],
            None,
        )) as ArrayRef;

        let error = normalize_array(&values, &element_type, true).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Found unmasked nulls for non-nullable"),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[test]
    fn retract_preserves_nested_type() -> Result<()> {
        let element_type = DataType::Struct(Fields::from(vec![Field::new(
            "required",
            DataType::Int32,
            false,
        )]));
        let values = Arc::new(StructArray::new(
            Fields::from(vec![Field::new("required", DataType::Int32, true)]),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
            None,
        )) as ArrayRef;

        for distinct in [false, true] {
            let mut acc = accumulator(&element_type, distinct)?;
            acc.update_batch(std::slice::from_ref(&values))?;
            acc.retract_batch(&[values.slice(0, 1)])?;
            assert_list_values(&acc.evaluate()?, &element_type, &["{required: 2}"])?;
            let state = acc.state()?;
            assert_list_values(&state[0], &element_type, &["{required: 2}"])?;
            let mut merged = accumulator(&element_type, distinct)?;
            merged.merge_batch(&[state[0].to_array()?])?;
            assert_list_values(&merged.evaluate()?, &element_type, &["{required: 2}"])?;
            acc.retract_batch(&[values.slice(1, 1)])?;
            assert_empty_list(&acc.evaluate()?);
        }
        Ok(())
    }

    #[test]
    fn ignored_null_rows_do_not_narrow_nested_payload() -> Result<()> {
        let element_type =
            DataType::List(Arc::new(Field::new_list_field(DataType::Int32, false)));
        // A null list can retain a null child, even though every retained list
        // satisfies the declared non-nullable item field.
        let values = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1, 2])),
            Arc::new(Int32Array::from(vec![None, Some(1)])),
            Some(NullBuffer::from(vec![false, true])),
        )) as ArrayRef;
        values.to_data().validate_full()?;

        for distinct in [false, true] {
            let mut acc = accumulator(&element_type, distinct)?;
            acc.update_batch(std::slice::from_ref(&values))?;
            assert_list_values(&acc.evaluate()?, &element_type, &["[1]"])?;
            let state = acc.state()?;
            assert_list_values(&state[0], &element_type, &["[1]"])?;
            let mut merged = accumulator(&element_type, distinct)?;
            merged.merge_batch(&[state[0].to_array()?])?;
            assert_list_values(&merged.evaluate()?, &element_type, &["[1]"])?;

            acc.retract_batch(&[values.slice(0, 1)])?;
            assert_list_values(&acc.evaluate()?, &element_type, &["[1]"])?;
            acc.retract_batch(&[values.slice(1, 1)])?;
            assert_empty_list(&acc.evaluate()?);
            acc.update_batch(&[values.slice(0, 1)])?;
            assert_empty_list(&acc.evaluate()?);
        }
        Ok(())
    }

    #[test]
    fn nested_null_lists_do_not_narrow_unreachable_payload() -> Result<()> {
        let element_type = DataType::List(Arc::new(Field::new_list_field(
            DataType::List(Arc::new(Field::new_list_field(DataType::Int32, false))),
            true,
        )));
        let runtime_inner = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1, 2])),
            Arc::new(Int32Array::from(vec![None, Some(1)])),
            Some(NullBuffer::from(vec![false, true])),
        ));
        let values = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(
                runtime_inner.data_type().clone(),
                true,
            )),
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 2])),
            runtime_inner,
            None,
        )) as ArrayRef;
        values.to_data().validate_full()?;

        for distinct in [false, true] {
            let mut partial = accumulator(&element_type, distinct)?;
            partial.update_batch(std::slice::from_ref(&values))?;
            let state = partial.state()?;
            assert_eq!(state[0].data_type(), list_type(element_type.clone()));

            let ScalarValue::List(array) = &state[0] else {
                panic!("expected collect state")
            };
            let collected_values = array.value(0);
            let collected = collected_values.as_list::<i32>();
            let inner_values = collected.value(0);
            let inner = inner_values.as_list::<i32>();
            assert!(inner.is_null(0));
            assert_eq!(inner.value(1).as_primitive::<Int32Type>().value(0), 1);

            let mut merged = accumulator(&element_type, distinct)?;
            merged.merge_batch(&[state[0].to_array()?])?;
            assert_eq!(
                merged.evaluate()?.data_type(),
                list_type(element_type.clone())
            );
            partial.retract_batch(std::slice::from_ref(&values))?;
            assert_empty_list(&partial.evaluate()?);
        }
        Ok(())
    }

    #[test]
    fn normalization_reuses_matching_primitive_values() -> Result<()> {
        let values = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let values_ptr = values.values().as_ptr();
        let scalar = SingleRowListArrayBuilder::new(values).build_list_scalar();

        let normalized = normalize_list_scalar(scalar, &list_type(DataType::Int32))?;
        let ScalarValue::List(array) = normalized else {
            panic!("expected a list scalar")
        };
        let normalized_values = array.value(0);
        let normalized_values = normalized_values
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("expected Int32 values");

        assert_eq!(normalized_values.values().as_ptr(), values_ptr);
        Ok(())
    }

    #[test]
    fn collect_list_handles_dictionary_with_unused_null() -> Result<()> {
        let keys = Int8Array::new(
            ScalarBuffer::from(vec![0_i8, 0]),
            Some(NullBuffer::new_valid(2)),
        );
        let dictionary_values = Arc::new(StringArray::from(vec![Some("a"), None]));
        let values = Arc::new(DictionaryArray::<Int8Type>::try_new(
            keys,
            dictionary_values,
        )?) as ArrayRef;

        assert_eq!(values.logical_null_count(), 0);
        assert!(values.is_nullable());
        assert!(values.nulls().is_some());

        assert_accumulator_outputs(Arc::clone(&values), false, &["a", "a"])?;
        assert_accumulator_outputs(values, true, &["a"])?;

        Ok(())
    }

    #[test]
    fn exact_type_input_discards_unreferenced_dictionary_values() -> Result<()> {
        let values = Arc::new(DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![1]),
            Arc::new(StringArray::from(vec![
                "secret-before",
                "visible",
                "secret-after",
            ])),
        )?) as ArrayRef;
        let element_type = values.data_type().clone();
        let accumulator = NullToEmptyListAccumulator::new(
            ArrayAggAccumulator::try_new(&element_type, true)?,
            list_type(element_type.clone()),
        );

        let normalized = accumulator.normalize_input(&values)?;
        assert_eq!(normalized.data_type(), &element_type);
        let dictionary = normalized
            .as_any()
            .downcast_ref::<DictionaryArray<Int8Type>>()
            .expect("expected Int8 dictionary");
        assert_eq!(dictionary.values().len(), 1);
        assert_eq!(dictionary.values().as_string::<i32>().value(0), "visible");
        Ok(())
    }

    #[test]
    fn required_encoded_logical_null_returns_error() -> Result<()> {
        let values = Arc::new(DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![0]),
            Arc::new(StringArray::from(vec![None::<&str>])),
        )?) as ArrayRef;
        let element_type = values.data_type().clone();
        let scalar = SingleRowListArrayBuilder::new(values).build_list_scalar();

        let error = normalize_list_scalar(scalar, &list_type(element_type)).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Found unmasked nulls for non-nullable"),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[test]
    fn nullable_nested_dictionary_preserves_logical_null_union_value() -> Result<()> {
        let runtime_union_fields = UnionFields::try_new(
            vec![4, 9],
            vec![
                Field::new("integer", DataType::Int32, false),
                Field::new("null", DataType::Null, true),
            ],
        )?;
        let declared_union_fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("integer", DataType::Int32, false),
                Field::new("null", DataType::Null, false),
            ],
        )?;
        let runtime_union = Arc::new(UnionArray::try_new(
            runtime_union_fields,
            ScalarBuffer::from(vec![4_i8, 9]),
            None,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None])),
                Arc::new(NullArray::new(2)),
            ],
        )?) as ArrayRef;
        assert_eq!(runtime_union.logical_null_count(), 1);

        let dictionary = Arc::new(DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![0, 1]),
            runtime_union,
        )?) as ArrayRef;
        assert_eq!(dictionary.logical_null_count(), 1);

        let runtime_fields = Fields::from(vec![Field::new(
            "optional",
            dictionary.data_type().clone(),
            true,
        )]);
        let values = Arc::new(StructArray::try_new(
            runtime_fields,
            vec![dictionary],
            None,
        )?) as ArrayRef;
        let target_dictionary_type = DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::Union(declared_union_fields, UnionMode::Sparse)),
        );
        let element_type = DataType::Struct(Fields::from(vec![Field::new(
            "optional",
            target_dictionary_type.clone(),
            true,
        )]));

        let assert_output = |value: ScalarValue,
                             expected_len: usize,
                             expected_null_count: usize|
         -> Result<()> {
            let ScalarValue::List(array) = value else {
                panic!("expected a list scalar")
            };
            let collected = array.value(0);
            assert_eq!(collected.len(), expected_len);
            let dictionary = collected.as_struct().column(0);
            assert_eq!(dictionary.data_type(), &target_dictionary_type);
            assert_eq!(dictionary.logical_null_count(), expected_null_count);
            Ok(())
        };

        let mut accumulator = accumulator(&element_type, false)?;
        accumulator.update_batch(std::slice::from_ref(&values))?;
        assert_output(accumulator.evaluate()?, 2, 1)?;

        accumulator.retract_batch(&[values.slice(0, 1)])?;
        assert_output(accumulator.evaluate()?, 1, 1)?;
        accumulator.retract_batch(&[values.slice(1, 1)])?;
        assert_empty_list(&accumulator.evaluate()?);
        Ok(())
    }

    #[test]
    fn collect_aggregates_handle_sparse_union_inactive_nulls() -> Result<()> {
        let fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("integer", DataType::Int32, false),
                Field::new("string", DataType::Utf8, false),
            ],
        )?;
        let values = Arc::new(UnionArray::try_new(
            fields,
            ScalarBuffer::from(vec![0_i8, 1, 0]),
            None,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None, Some(1)])),
                Arc::new(StringArray::from(vec![None, Some("a"), None])),
            ],
        )?) as ArrayRef;

        assert_eq!(values.logical_null_count(), 0);
        assert!(values.is_nullable());
        assert_accumulator_outputs(
            Arc::clone(&values),
            false,
            &["{integer=1}", "{string=a}", "{integer=1}"],
        )?;
        assert_accumulator_outputs(values, true, &["{integer=1}", "{string=a}"])?;

        Ok(())
    }

    #[test]
    fn collect_aggregates_handle_inactive_sparse_union_null_variant() -> Result<()> {
        let runtime_fields = UnionFields::try_new(
            vec![4, 9],
            vec![
                Field::new("null", DataType::Null, true),
                Field::new("integer", DataType::Int32, false),
            ],
        )?;
        let declared_fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("null", DataType::Null, false),
                Field::new("integer", DataType::Int32, false),
            ],
        )?;
        let element_type = DataType::Union(declared_fields, UnionMode::Sparse);
        let values = Arc::new(UnionArray::try_new(
            runtime_fields,
            ScalarBuffer::from(vec![9_i8, 9]),
            None,
            vec![
                Arc::new(NullArray::new(2)),
                Arc::new(Int32Array::from(vec![1, 2])),
            ],
        )?) as ArrayRef;

        assert_eq!(values.logical_null_count(), 0);
        assert!(values.is_nullable());
        for distinct in [false, true] {
            assert_accumulator_outputs_with_type(
                Arc::clone(&values),
                &element_type,
                distinct,
                &["{integer=1}", "{integer=2}"],
            )?;
        }
        Ok(())
    }

    #[test]
    fn collect_aggregates_remap_sparse_union_type_ids() -> Result<()> {
        let runtime_fields = UnionFields::try_new(
            vec![4, 9],
            vec![
                Field::new("integer", DataType::Int32, false),
                Field::new("string", DataType::Utf8, false),
            ],
        )?;
        let declared_fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("integer", DataType::Int32, false),
                Field::new("string", DataType::Utf8, false),
            ],
        )?;
        let element_type = DataType::Union(declared_fields, UnionMode::Sparse);
        let values = Arc::new(UnionArray::try_new(
            runtime_fields,
            ScalarBuffer::from(vec![4_i8, 9, 4]),
            None,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None, Some(1)])),
                Arc::new(StringArray::from(vec![None, Some("a"), None])),
            ],
        )?) as ArrayRef;

        for distinct in [false, true] {
            let expected = if distinct {
                vec!["{integer=1}", "{string=a}"]
            } else {
                vec!["{integer=1}", "{string=a}", "{integer=1}"]
            };
            let mut partial = accumulator(&element_type, distinct)?;
            partial.update_batch(std::slice::from_ref(&values))?;
            let state = partial.state()?;
            assert_list_values(&state[0], &element_type, &expected)?;

            let mut merged = accumulator(&element_type, distinct)?;
            merged.merge_batch(&[state[0].to_array()?])?;
            assert_list_values(&merged.evaluate()?, &element_type, &expected)?;
        }
        Ok(())
    }

    #[test]
    fn active_sparse_union_nulls_are_not_replaced_with_defaults() -> Result<()> {
        let runtime_fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("integer", DataType::Int32, true),
                Field::new("string", DataType::Utf8, true),
            ],
        )?;
        let declared_fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("integer", DataType::Int32, false),
                Field::new("string", DataType::Utf8, false),
            ],
        )?;
        let element_type = DataType::Union(declared_fields, UnionMode::Sparse);
        let values = Arc::new(UnionArray::try_new(
            runtime_fields,
            ScalarBuffer::from(vec![0_i8, 0]),
            None,
            vec![
                Arc::new(Int32Array::from(vec![None, None])),
                Arc::new(StringArray::from(vec![None::<&str>, None])),
            ],
        )?) as ArrayRef;

        let error = normalize_array(&values, &element_type, true).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Found unmasked nulls for non-nullable"),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[test]
    fn exact_type_unions_reject_active_nulls() -> Result<()> {
        let fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("integer", DataType::Int32, false),
                Field::new("string", DataType::Utf8, false),
            ],
        )?;
        let children = || -> Vec<ArrayRef> {
            vec![
                Arc::new(Int32Array::from(vec![None])),
                Arc::new(StringArray::from(vec![Some("unused")])),
            ]
        };

        let sparse = Arc::new(UnionArray::try_new(
            fields.clone(),
            ScalarBuffer::from(vec![0_i8]),
            None,
            children(),
        )?) as ArrayRef;
        assert!(sparse.is_nullable());
        assert_eq!(sparse.logical_null_count(), 1);
        let error = normalize_array(&sparse, sparse.data_type(), true).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("non-nullable sparse union field integer"),
            "unexpected error: {error}"
        );

        let dense = Arc::new(UnionArray::try_new(
            fields,
            ScalarBuffer::from(vec![0_i8]),
            Some(ScalarBuffer::from(vec![0_i32])),
            children(),
        )?) as ArrayRef;
        assert!(dense.is_nullable());
        assert_eq!(dense.logical_null_count(), 1);
        let error = normalize_array(&dense, dense.data_type(), true).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("non-nullable dense union field integer"),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[test]
    fn exact_type_encoded_wrappers_reject_active_union_nulls() -> Result<()> {
        let union = || -> Result<ArrayRef> {
            let fields = UnionFields::try_new(
                vec![0],
                vec![Field::new("integer", DataType::Int32, false)],
            )?;
            Ok(Arc::new(UnionArray::try_new(
                fields,
                ScalarBuffer::from(vec![0_i8]),
                None,
                vec![Arc::new(Int32Array::from(vec![None]))],
            )?) as ArrayRef)
        };

        let dictionary = Arc::new(DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![0]),
            union()?,
        )?) as ArrayRef;
        assert!(dictionary.is_nullable());
        assert!(normalize_array(&dictionary, dictionary.data_type(), true).is_err());

        let run_ends = Int16Array::from(vec![1]);
        let run = Arc::new(RunArray::<Int16Type>::try_new(
            &run_ends,
            union()?.as_ref(),
        )?) as ArrayRef;
        assert!(run.is_nullable());
        assert!(normalize_array(&run, run.data_type(), true).is_err());
        Ok(())
    }

    fn assert_active_sparse_union_logical_null_is_rejected(
        active_child: ArrayRef,
    ) -> Result<()> {
        assert!(active_child.is_valid(0));
        assert_eq!(active_child.logical_null_count(), 1);

        let runtime_fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("logical", active_child.data_type().clone(), true),
                Field::new("integer", DataType::Int32, true),
            ],
        )?;
        let declared_fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("logical", active_child.data_type().clone(), false),
                Field::new("integer", DataType::Int32, false),
            ],
        )?;
        let element_type = DataType::Union(declared_fields, UnionMode::Sparse);
        let values = Arc::new(UnionArray::try_new(
            runtime_fields,
            ScalarBuffer::from(vec![0_i8]),
            None,
            vec![active_child, Arc::new(Int32Array::from(vec![Some(1)]))],
        )?) as ArrayRef;

        let error = normalize_array(&values, &element_type, true).unwrap_err();
        assert!(
            error.to_string().contains(
                "Found unmasked nulls for non-nullable sparse union field logical"
            ),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[test]
    fn active_sparse_union_dictionary_logical_null_is_rejected() -> Result<()> {
        let keys = Int8Array::from(vec![0]);
        let values = Arc::new(StringArray::from(vec![None::<&str>]));
        let dictionary =
            Arc::new(DictionaryArray::<Int8Type>::try_new(keys, values)?) as ArrayRef;
        assert_active_sparse_union_logical_null_is_rejected(dictionary)
    }

    #[test]
    fn active_sparse_union_run_end_logical_null_is_rejected() -> Result<()> {
        let run_ends = Int16Array::from(vec![1]);
        let values = StringArray::from(vec![None::<&str>]);
        let run =
            Arc::new(RunArray::<Int16Type>::try_new(&run_ends, &values)?) as ArrayRef;
        assert_active_sparse_union_logical_null_is_rejected(run)
    }

    #[test]
    fn collect_aggregates_compact_dense_union_children() -> Result<()> {
        let fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("integer", DataType::Int32, false),
                Field::new("string", DataType::Utf8, false),
            ],
        )?;
        let values = Arc::new(UnionArray::try_new(
            fields,
            ScalarBuffer::from(vec![0_i8, 1, 0]),
            Some(ScalarBuffer::from(vec![0_i32, 0, 0])),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
            ],
        )?) as ArrayRef;

        assert_eq!(values.logical_null_count(), 0);
        assert!(values.is_nullable());
        assert_accumulator_outputs(
            Arc::clone(&values),
            false,
            &["{integer=1}", "{string=a}", "{integer=1}"],
        )?;
        assert_accumulator_outputs(values, true, &["{integer=1}", "{string=a}"])?;
        Ok(())
    }

    #[test]
    fn collect_aggregates_handle_sliced_run_array_unused_null() -> Result<()> {
        let run_ends = Int16Array::from(vec![2, 4]);
        let run_values = StringArray::from(vec![Some("a"), None]);
        let values =
            Arc::new(RunArray::<Int16Type>::try_new(&run_ends, &run_values)?.slice(0, 2))
                as ArrayRef;

        assert_eq!(values.logical_null_count(), 0);
        assert!(values.is_nullable());
        assert_accumulator_outputs(Arc::clone(&values), false, &["a", "a"])?;
        assert_accumulator_outputs(values, true, &["a"])?;

        Ok(())
    }

    #[test]
    fn run_end_normalization_is_bounded_by_physical_runs() -> Result<()> {
        let logical_len = i32::MAX as i64;
        let run_ends = Int64Array::from(vec![logical_len]);
        let values = StringArray::from(vec!["a"]);
        let values =
            Arc::new(RunArray::<Int64Type>::try_new(&run_ends, &values)?) as ArrayRef;
        let data_type = values.data_type().clone();

        let normalized = normalize_array(&values, &data_type, true)?;
        assert_eq!(normalized.len(), logical_len as usize);
        assert_eq!(normalized.as_run::<Int64Type>().values().len(), 1);
        Ok(())
    }

    #[test]
    fn collect_input_run_end_normalization_is_bounded_by_physical_runs() -> Result<()> {
        let logical_len = i32::MAX as i64;
        let run_ends = Int64Array::from(vec![logical_len]);
        let values = StringArray::from(vec!["a"]);
        let values =
            Arc::new(RunArray::<Int64Type>::try_new(&run_ends, &values)?) as ArrayRef;
        let DataType::RunEndEncoded(run_ends_field, values_field) = values.data_type()
        else {
            panic!("expected run-end encoded values")
        };
        let target_type = DataType::RunEndEncoded(
            Arc::clone(run_ends_field),
            Arc::new(Field::new(
                values_field.name(),
                DataType::LargeUtf8,
                values_field.is_nullable(),
            )),
        );
        let accumulator = NullToEmptyListAccumulator::new(
            ArrayAggAccumulator::try_new(&target_type, true)?,
            list_type(target_type.clone()),
        );

        let normalized = accumulator.normalize_input(&values)?;
        assert_eq!(normalized.data_type(), &target_type);
        assert_eq!(normalized.len(), logical_len as usize);
        let normalized = normalized.as_run::<Int64Type>();
        assert_eq!(normalized.values().len(), 1);
        assert_eq!(normalized.values().data_type(), &DataType::LargeUtf8);

        let exact_type = values.data_type().clone();
        let accumulator = NullToEmptyListAccumulator::new(
            ArrayAggAccumulator::try_new(&exact_type, true)?,
            list_type(exact_type.clone()),
        );
        let normalized = accumulator.normalize_input(&values)?;
        assert_eq!(normalized.data_type(), &exact_type);
        assert_eq!(normalized.len(), logical_len as usize);
        assert_eq!(normalized.as_run::<Int64Type>().values().len(), 1);
        Ok(())
    }

    #[test]
    fn collect_input_run_end_normalization_preserves_null_runs() -> Result<()> {
        let runtime_fields = UnionFields::try_new(
            vec![4, 9],
            vec![
                Field::new("integer", DataType::Int32, false),
                Field::new("null", DataType::Null, true),
            ],
        )?;
        let declared_fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("integer", DataType::Int32, false),
                Field::new("null", DataType::Null, false),
            ],
        )?;
        let runtime_values = UnionArray::try_new(
            runtime_fields,
            ScalarBuffer::from(vec![4_i8, 9]),
            None,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None])),
                Arc::new(NullArray::new(2)),
            ],
        )?;
        assert_eq!(runtime_values.logical_null_count(), 1);

        let run_ends = Int16Array::from(vec![2, 4]);
        let values = Arc::new(RunArray::<Int16Type>::try_new(&run_ends, &runtime_values)?)
            as ArrayRef;
        assert_eq!(values.logical_null_count(), 2);

        let DataType::RunEndEncoded(run_ends_field, values_field) = values.data_type()
        else {
            panic!("expected run-end encoded values")
        };
        let element_type = DataType::RunEndEncoded(
            Arc::clone(run_ends_field),
            Arc::new(Field::new(
                values_field.name(),
                DataType::Union(declared_fields, UnionMode::Sparse),
                true,
            )),
        );
        let mut accumulator = NullToEmptyListAccumulator::new(
            ArrayAggAccumulator::try_new(&element_type, true)?,
            list_type(element_type.clone()),
        );

        let normalized = accumulator.normalize_input(&values)?;
        assert_eq!(normalized.data_type(), &element_type);
        assert_eq!(normalized.len(), 4);
        assert_eq!(normalized.as_run::<Int16Type>().values().len(), 2);

        accumulator.update_batch(std::slice::from_ref(&values))?;
        assert_list_values(
            &accumulator.evaluate()?,
            &element_type,
            &["{integer=1}", "{integer=1}"],
        )?;

        accumulator.retract_batch(&[values.slice(2, 2)])?;
        assert_list_values(
            &accumulator.evaluate()?,
            &element_type,
            &["{integer=1}", "{integer=1}"],
        )?;
        accumulator.retract_batch(&[values.slice(0, 2)])?;
        assert_empty_list(&accumulator.evaluate()?);
        Ok(())
    }

    #[test]
    fn list_view_normalization_copies_overlapping_backing_once() -> Result<()> {
        const VIEW_COUNT: usize = 8_192;
        const VALUE_COUNT: usize = 1_024;
        let field = Arc::new(Field::new_list_field(DataType::Int32, false));
        let values = Arc::new(ListViewArray::new(
            Arc::clone(&field),
            ScalarBuffer::from(vec![0_i32; VIEW_COUNT]),
            ScalarBuffer::from(vec![VALUE_COUNT as i32; VIEW_COUNT]),
            Arc::new(Int32Array::from_iter_values(0..VALUE_COUNT as i32)),
            None,
        )) as ArrayRef;
        let data_type = values.data_type().clone();

        let normalized = normalize_array(&values, &data_type, true)?;
        let normalized = normalized.as_list_view::<i32>();
        assert_eq!(normalized.len(), VIEW_COUNT);
        assert_eq!(normalized.values().len(), VALUE_COUNT);
        assert_eq!(normalized.value_offsets()[VIEW_COUNT - 1], 0);
        assert_eq!(normalized.value_sizes()[VIEW_COUNT - 1], VALUE_COUNT as i32);
        Ok(())
    }

    #[test]
    fn exact_list_view_discards_unreferenced_backing() -> Result<()> {
        const VALUE_COUNT: usize = 8_192;
        const RETAINED_INDEX: usize = 4_096;
        let field = Arc::new(Field::new_list_field(DataType::Int32, false));
        let values = Arc::new(ListViewArray::new(
            Arc::clone(&field),
            ScalarBuffer::from(vec![RETAINED_INDEX as i32]),
            ScalarBuffer::from(vec![1_i32]),
            Arc::new(Int32Array::from_iter_values(0..VALUE_COUNT as i32)),
            None,
        )) as ArrayRef;
        let data_type = values.data_type().clone();

        let normalized = normalize_array(&values, &data_type, true)?;
        let normalized = normalized.as_list_view::<i32>();
        assert_eq!(normalized.values().len(), 1);
        assert_eq!(normalized.value_offsets(), &[0]);
        assert_eq!(normalized.value_sizes(), &[1]);
        assert_eq!(
            normalized.values().as_primitive::<Int32Type>().value(0),
            RETAINED_INDEX as i32
        );
        Ok(())
    }

    #[test]
    fn nullable_nested_sparse_union_scrubs_inactive_payload() -> Result<()> {
        let union_fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("integer", DataType::Int32, true),
                Field::new("string", DataType::Utf8, true),
            ],
        )?;
        let union = Arc::new(UnionArray::try_new(
            union_fields,
            ScalarBuffer::from(vec![0_i8, 1, 0]),
            None,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(999), Some(2)])),
                Arc::new(StringArray::from(vec![
                    Some("inactive-secret-before"),
                    Some("visible"),
                    Some("inactive-secret-after"),
                ])),
            ],
        )?) as ArrayRef;
        let fields = Fields::from(vec![Field::new(
            "optional",
            union.data_type().clone(),
            true,
        )]);
        let value =
            Arc::new(StructArray::try_new(fields, vec![union], None)?) as ArrayRef;
        let data_type = value.data_type().clone();

        let normalized = normalize_array(&value, &data_type, true)?;
        let union = normalized
            .as_struct()
            .column(0)
            .as_any()
            .downcast_ref::<UnionArray>()
            .expect("expected union");
        let strings = union.child(1).as_string::<i32>();
        assert_eq!(strings.value(0), "");
        assert_eq!(strings.value(1), "visible");
        assert_eq!(strings.value(2), "");
        Ok(())
    }

    #[test]
    fn nullable_struct_child_scrubs_parent_masked_payload() -> Result<()> {
        let inner_fields =
            Fields::from(vec![Field::new("optional_value", DataType::Utf8, true)]);
        let inner = Arc::new(StructArray::try_new(
            inner_fields,
            vec![Arc::new(StringArray::from(vec![
                "masked-secret",
                "visible",
            ]))],
            Some(NullBuffer::from(vec![false, true])),
        )?) as ArrayRef;
        let outer_fields = Fields::from(vec![Field::new(
            "optional_struct",
            inner.data_type().clone(),
            true,
        )]);
        let value =
            Arc::new(StructArray::try_new(outer_fields, vec![inner], None)?) as ArrayRef;
        let data_type = value.data_type().clone();

        let normalized = normalize_array(&value, &data_type, true)?;
        let structure = normalized.as_struct().column(0).as_struct();
        assert!(structure.is_null(0));
        let child = structure.column(0).as_string::<i32>();
        assert_eq!(child.value(0), "");
        assert_eq!(child.value(1), "visible");
        Ok(())
    }

    #[tokio::test]
    async fn collect_list_dictionary_sql() -> Result<()> {
        let keys = Int8Array::from(vec![0, 0]);
        let dictionary_values = Arc::new(StringArray::from(vec![Some("a"), None]));
        let values = Arc::new(DictionaryArray::<Int8Type>::try_new(
            keys,
            dictionary_values,
        )?) as ArrayRef;
        let element_type = values.data_type().clone();

        let ctx = SessionContext::new();
        ctx.register_udaf(AggregateUDF::new_from_impl(SparkCollectList::new()));
        ctx.register_batch(
            "dictionary_input",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "x",
                    element_type.clone(),
                    false,
                )])),
                vec![values],
            )?,
        )?;

        let batches = ctx
            .sql("SELECT collect_list(x) AS values FROM dictionary_input")
            .await?
            .collect()
            .await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let value = ScalarValue::try_from_array(batches[0].column(0), 0)?;
        assert_list_values(&value, &element_type, &["a", "a"])?;

        Ok(())
    }

    #[tokio::test]
    async fn collect_list_map_discards_unreferenced_entries_sql() -> Result<()> {
        let key_field = Arc::new(Field::new("key", DataType::Int32, false));
        let value_field = Arc::new(Field::new("value", DataType::Utf8, false));
        let entries = StructArray::try_new(
            Fields::from(vec![Arc::clone(&key_field), Arc::clone(&value_field)]),
            vec![
                Arc::new(Int32Array::from(vec![0, 1, 2])),
                Arc::new(StringArray::from(vec![
                    "secret-before",
                    "visible",
                    "secret-after",
                ])),
            ],
            None,
        )?;
        let entry_field = Arc::new(
            Field::new("entries", entries.data_type().clone(), false).with_metadata(
                HashMap::from([("test-metadata".to_string(), "preserved".to_string())]),
            ),
        );
        let values = Arc::new(MapArray::try_new(
            Arc::clone(&entry_field),
            OffsetBuffer::new(ScalarBuffer::from(vec![1_i32, 2_i32])),
            entries,
            None,
            true,
        )?) as ArrayRef;
        let element_type = values.data_type().clone();

        let ctx = SessionContext::new();
        ctx.register_udaf(AggregateUDF::new_from_impl(SparkCollectList::new()));
        ctx.register_batch(
            "map_input",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "x",
                    element_type.clone(),
                    false,
                )])),
                vec![values],
            )?,
        )?;

        let batches = ctx
            .sql("SELECT collect_list(x) AS values FROM map_input")
            .await?
            .collect()
            .await?;
        let list = batches[0].column(0).as_list::<i32>();
        let values = list.value(0);
        let map = values.as_map();
        assert_eq!(map.data_type(), &element_type);
        assert_eq!(map.value_offsets(), &[0, 1]);
        assert_eq!(map.entries().len(), 1);
        assert_eq!(
            map.values().as_string::<i32>().iter().collect::<Vec<_>>(),
            vec![Some("visible")]
        );
        Ok(())
    }

    #[tokio::test]
    async fn collect_list_sliced_fixed_size_list_compacts_nested_dictionary_sql()
    -> Result<()> {
        let dictionary = Arc::new(DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![0, 1, 2]),
            Arc::new(StringArray::from(vec![
                "secret-before",
                "visible",
                "secret-after",
            ])),
        )?) as ArrayRef;
        let item_field = Arc::new(
            Field::new_list_field(dictionary.data_type().clone(), false).with_metadata(
                HashMap::from([("test-metadata".to_string(), "preserved".to_string())]),
            ),
        );
        let values =
            FixedSizeListArray::try_new(Arc::clone(&item_field), 1, dictionary, None)?
                .slice(1, 1);
        assert_eq!(values.values().len(), 1);
        let values = Arc::new(values) as ArrayRef;
        let element_type = values.data_type().clone();

        let ctx = SessionContext::new();
        ctx.register_udaf(AggregateUDF::new_from_impl(SparkCollectList::new()));
        ctx.register_batch(
            "fixed_size_list_input",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "x",
                    element_type.clone(),
                    false,
                )])),
                vec![values],
            )?,
        )?;

        let batches = ctx
            .sql("SELECT collect_list(x) AS values FROM fixed_size_list_input")
            .await?
            .collect()
            .await?;
        let list = batches[0].column(0).as_list::<i32>();
        let values = list.value(0);
        let fixed = values.as_fixed_size_list();
        assert_eq!(fixed.data_type(), &element_type);
        assert_eq!(fixed.values().len(), 1);
        let dictionary = fixed
            .values()
            .as_any()
            .downcast_ref::<DictionaryArray<Int8Type>>()
            .expect("expected Int8 dictionary");
        assert_eq!(dictionary.keys().offset(), 0);
        let dictionary = dictionary.values().as_string::<i32>();
        assert_eq!(dictionary.len(), 1);
        assert_eq!(dictionary.value(0), "visible");
        Ok(())
    }

    #[tokio::test]
    async fn collect_list_preserves_null_nested_fixed_size_list_sql() -> Result<()> {
        let inner_field = Arc::new(Field::new_list_field(DataType::Int32, false));
        let inner = Arc::new(FixedSizeListArray::try_new(
            inner_field,
            2,
            Arc::new(Int32Array::from(vec![1, 2])),
            None,
        )?) as ArrayRef;
        let outer_field =
            Arc::new(Field::new_list_field(inner.data_type().clone(), false));
        let outer = Arc::new(FixedSizeListArray::try_new(
            outer_field,
            1,
            inner,
            Some(NullBuffer::new_null(1)),
        )?) as ArrayRef;
        let element_fields = Fields::from(vec![Field::new(
            "optional",
            outer.data_type().clone(),
            true,
        )]);
        let element_type = DataType::Struct(element_fields.clone());
        let values = Arc::new(StructArray::try_new(element_fields, vec![outer], None)?)
            as ArrayRef;
        values.to_data().validate_full()?;

        let ctx = SessionContext::new();
        ctx.register_udaf(AggregateUDF::new_from_impl(SparkCollectList::new()));
        ctx.register_batch(
            "nested_fixed_size_list_input",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "x",
                    element_type.clone(),
                    false,
                )])),
                vec![values],
            )?,
        )?;

        let batches = ctx
            .sql("SELECT collect_list(x) AS values FROM nested_fixed_size_list_input")
            .await?
            .collect()
            .await?;
        let list = batches[0].column(0).as_list::<i32>();
        assert_eq!(list.data_type(), &list_type(element_type));
        let collected = list.value(0);
        let outer = collected.as_struct().column(0).as_fixed_size_list();
        assert!(outer.is_null(0));
        outer.to_data().validate_full()?;
        Ok(())
    }

    #[tokio::test]
    async fn collect_list_dictionary_grouped_and_window_sql() -> Result<()> {
        for target_partitions in [1, 4] {
            check_collect_list_dictionary_sql(target_partitions).await?;
        }
        Ok(())
    }

    async fn check_collect_list_dictionary_sql(target_partitions: usize) -> Result<()> {
        let keys = Int8Array::new(
            ScalarBuffer::from(vec![0_i8, 0, 1, 1]),
            Some(NullBuffer::new_valid(4)),
        );
        let dictionary_values =
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), None]));
        let values = Arc::new(DictionaryArray::<Int8Type>::try_new(
            keys,
            dictionary_values,
        )?) as ArrayRef;
        let element_type = values.data_type().clone();
        assert!(values.nulls().is_some());

        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_target_partitions(target_partitions),
        );
        ctx.register_udaf(AggregateUDF::new_from_impl(SparkCollectList::new()));
        ctx.register_batch(
            "dictionary_input",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("g", DataType::Int32, false),
                    Field::new("id", DataType::Int32, false),
                    Field::new("x", element_type.clone(), false),
                ])),
                vec![
                    Arc::new(Int32Array::from(vec![0, 0, 1, 1])),
                    Arc::new(Int32Array::from(vec![0, 1, 2, 3])),
                    values,
                ],
            )?,
        )?;

        let grouped = ctx
            .sql(
                "SELECT g, collect_list(x) AS values \
                 FROM dictionary_input GROUP BY g ORDER BY g",
            )
            .await?
            .collect()
            .await?;
        assert_eq!(grouped.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        let grouped =
            arrow::compute::concat_batches(&grouped[0].schema(), grouped.iter())?;
        for (row, expected) in [["a", "a"], ["b", "b"]].iter().enumerate() {
            let value = ScalarValue::try_from_array(grouped.column(1), row)?;
            assert_list_values(&value, &element_type, expected)?;
        }

        let window = ctx
            .sql(
                "SELECT id, collect_list(x) OVER (\
                   ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW\
                 ) AS values FROM dictionary_input ORDER BY id",
            )
            .await?
            .collect()
            .await?;
        let window = arrow::compute::concat_batches(&window[0].schema(), window.iter())?;
        let expected = [vec!["a"], vec!["a", "a"], vec!["a", "b"], vec!["b", "b"]];
        for (row, expected) in expected.iter().enumerate() {
            let value = ScalarValue::try_from_array(window.column(1), row)?;
            assert_list_values(&value, &element_type, expected)?;
        }

        let outer = ctx
            .sql(
                "SELECT array_agg(values ORDER BY g) FROM (\
                   SELECT g, collect_list(x) AS values \
                   FROM dictionary_input GROUP BY g\
                 )",
            )
            .await?
            .collect()
            .await?;
        let outer = arrow::compute::concat_batches(&outer[0].schema(), outer.iter())?;
        assert_eq!(outer.num_rows(), 1);
        let value = ScalarValue::try_from_array(outer.column(0), 0)?.compacted();
        let ScalarValue::List(array) = value else {
            panic!("expected ordered outer list")
        };
        array.to_data().validate_full()?;
        let values = array.value(0);
        assert_eq!(values.len(), 2);
        for (row, expected) in [["a", "a"], ["b", "b"]].iter().enumerate() {
            let value = ScalarValue::try_from_array(&values, row)?.compacted();
            assert_list_values(&value, &element_type, expected)?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn collect_list_dense_union_grouped_sql() -> Result<()> {
        let fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("integer", DataType::Int32, false),
                Field::new("string", DataType::Utf8, false),
            ],
        )?;
        let values = Arc::new(UnionArray::try_new(
            fields,
            ScalarBuffer::from(vec![0_i8, 1, 0]),
            Some(ScalarBuffer::from(vec![0_i32, 0, 0])),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
            ],
        )?) as ArrayRef;
        let element_type = values.data_type().clone();

        let ctx = SessionContext::new();
        ctx.register_udaf(AggregateUDF::new_from_impl(SparkCollectList::new()));
        ctx.register_batch(
            "dense_union_input",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("g", DataType::Int32, false),
                    Field::new("x", element_type.clone(), false),
                ])),
                vec![Arc::new(Int32Array::from(vec![0, 0, 1])), values],
            )?,
        )?;

        let grouped = ctx
            .sql(
                "SELECT g, collect_list(x) AS values \
                 FROM dense_union_input GROUP BY g ORDER BY g",
            )
            .await?
            .collect()
            .await?;
        let grouped =
            arrow::compute::concat_batches(&grouped[0].schema(), grouped.iter())?;
        for (row, expected) in [vec!["{integer=1}", "{string=a}"], vec!["{integer=1}"]]
            .iter()
            .enumerate()
        {
            let value = ScalarValue::try_from_array(grouped.column(1), row)?;
            assert_list_values(&value, &element_type, expected)?;
        }
        Ok(())
    }
}
