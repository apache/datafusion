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
    Array, ArrayData, ArrayRef, GenericListArray, GenericListViewArray, OffsetSizeTrait,
    PrimitiveArray, RunArray, StructArray, UInt64Array, UnionArray, cast::AsArray,
    downcast_run_array, make_array,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::compute::{cast, take};
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
use std::collections::BTreeSet;
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
    if value.data_type() == target_type && (!require_non_null || !value.is_nullable()) {
        return Ok(Arc::clone(value));
    }
    match (value.data_type(), target_type) {
        (DataType::List(_), DataType::List(field)) => normalize_list::<i32>(value, field),
        (DataType::LargeList(_), DataType::LargeList(field)) => {
            normalize_list::<i64>(value, field)
        }
        (DataType::ListView(_), DataType::ListView(field)) => {
            normalize_list_view::<i32>(value, field)
        }
        (DataType::LargeListView(_), DataType::LargeListView(field)) => {
            normalize_list_view::<i64>(value, field)
        }
        (
            DataType::Dictionary(source_key, _),
            DataType::Dictionary(target_key, target_value),
        ) if source_key == target_key => {
            let compact = garbage_collect_any_dictionary(value.as_any_dictionary())?;
            let dictionary = compact.as_any_dictionary();
            let values = normalize_array(dictionary.values(), target_value, true)?;
            let dictionary = dictionary.with_values(values);
            if dictionary.null_count() == 0 && dictionary.to_data().nulls().is_some() {
                let data = dictionary.to_data().into_builder().nulls(None).build()?;
                Ok(make_array(data))
            } else {
                Ok(dictionary)
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
                value => normalize_run_end(value, target_type, target_value.data_type()),
                _ => internal_err!("collect_list/collect_set expected a run-end encoded array"),
            }
        }
        (DataType::Struct(_), DataType::Struct(fields)) => {
            let source = value.as_struct();
            let columns = fields
                .iter()
                .zip(source.columns())
                .map(|(field, column)| {
                    let column = if !field.is_nullable() {
                        materialize_masked_values(
                            column,
                            source.nulls(),
                            field.data_type(),
                        )?
                    } else {
                        Arc::clone(column)
                    };
                    normalize_array(&column, field.data_type(), !field.is_nullable())
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StructArray::try_new(
                fields.clone(),
                columns,
                source.nulls().cloned(),
            )?))
        }
        (
            DataType::Union(source_fields, UnionMode::Sparse),
            DataType::Union(fields, UnionMode::Sparse),
        ) => normalize_sparse_union(value, source_fields, fields, require_non_null),
        (
            DataType::Union(source_fields, UnionMode::Dense),
            DataType::Union(fields, UnionMode::Dense),
        ) => normalize_dense_union(value, source_fields, fields, require_non_null),
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
                Ok(make_array(data))
            } else {
                Ok(value)
            }
        }
    }
}

fn materialize_masked_values(
    value: &ArrayRef,
    parent_nulls: Option<&arrow::buffer::NullBuffer>,
    target_type: &DataType,
) -> Result<ArrayRef> {
    let Some(parent_nulls) = parent_nulls else {
        return Ok(Arc::clone(value));
    };
    let mut valid_parent_indices = parent_nulls.valid_indices();
    let Some(first_valid_parent) = valid_parent_indices.next() else {
        return ScalarValue::new_default(target_type)?.to_array_of_size(value.len());
    };
    let fallback = std::iter::once(first_valid_parent)
        .chain(valid_parent_indices)
        .find(|index| value.is_valid(*index));
    let Some(fallback) = fallback else {
        // The nulls are logically visible because the parent rows are valid.
        // Preserve them so the declared non-null field validation rejects the
        // input instead of silently replacing user data with defaults.
        return Ok(Arc::clone(value));
    };
    let indices = UInt64Array::from_iter_values((0..value.len()).map(|index| {
        if parent_nulls.is_valid(index) {
            index as u64
        } else {
            fallback as u64
        }
    }));
    Ok(take(value.as_ref(), &indices, None)?)
}

fn normalize_run_end<R: RunEndIndexType>(
    source: &RunArray<R>,
    target_type: &DataType,
    target_value_type: &DataType,
) -> Result<ArrayRef> {
    let run_ends =
        PrimitiveArray::<R>::from_iter_values(source.run_ends().sliced_values());
    let values = source.values_slice();
    let values = normalize_array(&values, target_value_type, true)?;

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
    let values = take(
        source.values().as_ref(),
        &UInt64Array::from_iter_values(indices),
        None,
    )?;
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

    let values = take(
        source.values().as_ref(),
        &UInt64Array::from_iter_values(indices),
        None,
    )?;
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
            let child = if require_non_null {
                let first_active =
                    (0..source.len()).find(|index| source.type_id(*index) == *source_type_id);
                match first_active {
                    None => ScalarValue::new_default(target_field.data_type())?
                        .to_array_of_size(source.len())?,
                    Some(first_active) => {
                        if (first_active..source.len()).any(|index| {
                            source.type_id(index) == *source_type_id
                                && child.is_null(index)
                        }) {
                            return internal_err!(
                                "Found unmasked nulls for non-nullable sparse union field {}",
                                target_field.name()
                            );
                        }
                        let indices = UInt64Array::from_iter_values(
                            (0..source.len()).map(|index| {
                                if source.type_id(index) == *source_type_id {
                                    index as u64
                                } else {
                                    first_active as u64
                                }
                            }),
                        );
                        take(child.as_ref(), &indices, None)?
                    }
                }
            } else {
                Arc::clone(child)
            };
            normalize_array(&child, target_field.data_type(), require_non_null)
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
        let child = take(child.as_ref(), &indices, None)?;
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
        if value.data_type() == field.data_type() {
            Ok(Arc::clone(value))
        } else {
            // Materialize only retained rows before narrowing nested fields.
            // A slice can still reference null payload outside its logical rows.
            let indices = match value.logical_nulls() {
                Some(nulls) => UInt64Array::from_iter_values(
                    nulls.valid_indices().map(|index| index as u64),
                ),
                None => UInt64Array::from_iter_values(0..value.len() as u64),
            };
            let value = take(value.as_ref(), &indices, None)?;
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
        DictionaryArray, Int8Array, Int16Array, Int32Array, Int64Array, ListArray,
        ListViewArray, RunArray, StringArray, StructArray, UnionArray,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow::datatypes::{
        Fields, Int8Type, Int16Type, Int32Type, Int64Type, Schema, UnionFields,
    };
    use arrow::record_batch::RecordBatch;
    use arrow::util::display::array_value_to_string;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::AggregateUDF;

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

        let mut partial = accumulator(&element_type, distinct)?;
        partial.update_batch(std::slice::from_ref(&values))?;
        let state = partial.state()?;
        assert_list_values(&state[0], &element_type, expected)?;
        assert_list_values(&state[0].clone().compacted(), &element_type, expected)?;

        let mut final_accumulator = accumulator(&element_type, distinct)?;
        final_accumulator.merge_batch(&[state[0].to_array()?])?;
        let merged = final_accumulator.evaluate()?;
        assert_list_values(&merged, &element_type, expected)?;
        assert_list_values(&merged.compacted(), &element_type, expected)?;

        let mut single = accumulator(&element_type, distinct)?;
        single.update_batch(&[values])?;
        let value = single.evaluate()?;
        assert_list_values(&value, &element_type, expected)?;
        assert_list_values(&value.compacted(), &element_type, expected)
    }

    #[test]
    fn collect_types_have_non_nullable_elements() -> Result<()> {
        let element_type = DataType::Int32;
        let expected = list_type(element_type.clone());

        for aggregate in [
            &SparkCollectList::new() as &dyn AggregateUDFImpl,
            &SparkCollectSet::new() as &dyn AggregateUDFImpl,
        ] {
            assert!(!aggregate.is_nullable());
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
    async fn collect_list_dictionary_grouped_and_window_sql() -> Result<()> {
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

        let ctx = SessionContext::new();
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
