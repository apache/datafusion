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

use crate::error::{_plan_err, Result};
use arrow::{
    array::{
        Array, ArrayRef, AsArray, DictionaryArray, FixedSizeListArray, GenericListArray,
        GenericListViewArray, MapArray, RecordBatch, StructArray, UInt64Array,
        UnionArray, downcast_integer, make_array, new_null_array,
    },
    buffer::{NullBuffer, ScalarBuffer},
    compute::{CastOptions, can_cast_types, cast_with_options, take},
    datatypes::{
        DataType, DataType::Struct, Field, FieldRef, SchemaRef, UnionFields, UnionMode,
    },
    error::ArrowError,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

/// Cast a struct column to match target struct fields, handling nested structs recursively.
///
/// This function implements struct-to-struct casting with the assumption that **structs should
/// always be allowed to cast to other structs**. However, the source column must already be
/// a struct type - non-struct sources will result in an error.
///
/// ## Field Matching Strategy
/// - **By Name**: Source struct fields are matched to target fields by name (case-sensitive)
/// - **No Positional Mapping**: Structs with no overlapping field names are rejected
/// - **Type Adaptation**: When a matching field is found, it is recursively cast to the target field's type
/// - **Missing Fields**: Target fields not present in the source are filled with null values
/// - **Extra Fields**: Source fields not present in the target are ignored
///
/// ## Nested Struct Handling
/// - Nested structs are handled recursively using the same casting rules
/// - Each level of nesting follows the same field matching and null-filling strategy
/// - This allows for complex struct transformations while maintaining data integrity
///
/// # Arguments
/// * `source_col` - The source array to cast (must be a struct array)
/// * `target_fields` - The target struct field definitions to cast to
///
/// # Returns
/// A `Result<ArrayRef>` containing the cast struct array
///
/// # Errors
/// Returns a `DataFusionError::Plan` if the source column is not a struct type
fn cast_struct_column(
    source_col: &ArrayRef,
    target_fields: &[Arc<Field>],
    cast_options: &CastOptions,
) -> Result<ArrayRef> {
    if source_col.data_type() == &DataType::Null {
        return Ok(new_null_array(
            &Struct(target_fields.to_vec().into()),
            source_col.len(),
        ));
    }

    if let Some(source_struct) = source_col.as_any().downcast_ref::<StructArray>() {
        let source_fields = source_struct.fields();
        validate_struct_compatibility(source_fields, target_fields)?;

        if !source_col.is_empty() && source_col.null_count() == source_col.len() {
            return Ok(new_null_array(
                &Struct(target_fields.to_vec().into()),
                source_col.len(),
            ));
        }

        let mut fields: Vec<Arc<Field>> = Vec::with_capacity(target_fields.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(target_fields.len());
        let num_rows = source_col.len();

        // Iterate target fields and pick source child by name when present.
        for target_child_field in target_fields.iter() {
            fields.push(Arc::clone(target_child_field));

            let source_child_opt =
                source_struct.column_by_name(target_child_field.name());

            match source_child_opt {
                Some(source_child_col) => {
                    let adapted_child = cast_column(
                        source_child_col,
                        target_child_field.data_type(),
                        cast_options,
                    )
                    .map_err(|e| {
                        e.context(format!(
                            "While casting struct field '{}'",
                            target_child_field.name()
                        ))
                    })?;
                    arrays.push(adapted_child);
                }
                None => {
                    arrays.push(new_null_array(target_child_field.data_type(), num_rows));
                }
            }
        }

        let struct_array =
            StructArray::try_new(fields.into(), arrays, source_struct.nulls().cloned())?;
        Ok(Arc::new(struct_array))
    } else {
        // Return error if source is not a struct type
        _plan_err!(
            "Cannot cast column of type {} to struct type. Source must be a struct to cast to struct.",
            source_col.data_type()
        )
    }
}

/// Cast a union column to match target union fields, handling child fields recursively.
///
/// ## Casting Behavior
/// - Preserves union mode (sparse or dense). Incompatible modes are rejected.
/// - Requires exact matching union type ID sets (order may differ).
/// - Recursively adapts each matching child array using `cast_column`.
/// - Preserves row-level `type_ids` and dense `offsets` buffers without copying primitive data.
fn cast_union_column(
    source_col: &ArrayRef,
    source_fields: &UnionFields,
    source_mode: &UnionMode,
    target_fields: &UnionFields,
    target_mode: &UnionMode,
    cast_options: &CastOptions,
) -> Result<ArrayRef> {
    validate_union_schema_compatibility(
        source_fields,
        source_mode,
        target_fields,
        target_mode,
    )?;

    let source_union = source_col
        .as_any()
        .downcast_ref::<UnionArray>()
        .ok_or_else(|| {
            crate::error::DataFusionError::Plan(format!(
                "Expected UnionArray for Union data type, got {}",
                source_col.data_type()
            ))
        })?;

    let mut children = Vec::with_capacity(target_fields.len());

    for (target_type_id, target_field) in target_fields.iter() {
        let source_child = source_union.child(target_type_id);

        children.push(
            cast_column(source_child, target_field.data_type(), cast_options).map_err(
                |e| {
                    e.context(format!(
                        "While adapting Union child type ID {target_type_id} ('{}')",
                        target_field.name()
                    ))
                },
            )?,
        );
    }

    Ok(Arc::new(UnionArray::try_new(
        target_fields.clone(),
        source_union.type_ids().clone(),
        source_union.offsets().cloned(),
        children,
    )?))
}

/// Cast a column to match the target field type, with special handling for nested structs.
///
/// This function serves as the main entry point for column casting operations. For struct
/// types, it enforces that **only struct columns can be cast to struct types**.
///
/// ## Casting Behavior
/// - **Struct Types**: Delegates to `cast_struct_column` for struct-to-struct casting only
/// - **Non-Struct Types**: Uses Arrow's standard `cast` function for primitive type conversions
///
/// ## Cast Options
/// The `cast_options` argument controls how Arrow handles values that cannot be represented
/// in the target type. When `safe` is `false` (DataFusion's default) the cast will return an
/// error if such a value is encountered. Setting `safe` to `true` instead produces `NULL`
/// for out-of-range or otherwise invalid values. The options also allow customizing how
/// temporal values are formatted when cast to strings.
///
/// ```
/// use arrow::array::{ArrayRef, Int64Array};
/// use arrow::compute::CastOptions;
/// use arrow::datatypes::DataType;
/// use datafusion_common::nested_struct::cast_column;
/// use std::sync::Arc;
///
/// let source: ArrayRef = Arc::new(Int64Array::from(vec![1, i64::MAX]));
/// // Permit lossy conversions by producing NULL on overflow instead of erroring
/// let options = CastOptions {
///     safe: true,
///     ..Default::default()
/// };
/// let result = cast_column(&source, &DataType::Int32, &options).unwrap();
/// assert!(result.is_null(1));
/// ```
///
/// ## Struct Casting Requirements
/// The struct casting logic requires that the source column must already be a struct type.
/// This makes the function useful for:
/// - Schema evolution scenarios where struct layouts change over time
/// - Data migration between different struct schemas
/// - Type-safe data processing pipelines that maintain struct type integrity
///
/// # Arguments
/// * `source_col` - The source array to cast
/// * `target_type` - The target data type to cast to
/// * `cast_options` - Options that govern strictness and formatting of the cast
///
/// # Returns
/// A `Result<ArrayRef>` containing the cast array
///
/// # Errors
/// Returns an error if:
/// - Attempting to cast a non-struct column to a struct type
/// - Arrow's cast function fails for non-struct types
/// - Memory allocation fails during struct construction
/// - Invalid data type combinations are encountered
pub fn cast_column(
    source_col: &ArrayRef,
    target_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef> {
    match (source_col.data_type(), target_type) {
        (_, Struct(target_fields)) => {
            cast_struct_column(source_col, target_fields, cast_options)
        }
        (DataType::List(_), DataType::List(target_inner)) => {
            cast_list_column::<i32>(source_col, target_inner, cast_options)
        }
        (DataType::LargeList(_), DataType::LargeList(target_inner)) => {
            cast_list_column::<i64>(source_col, target_inner, cast_options)
        }
        (
            DataType::FixedSizeList(_, source_list_size),
            DataType::FixedSizeList(target_inner, target_list_size),
        ) if source_list_size == target_list_size => cast_fixed_size_list_column(
            source_col,
            target_inner,
            *target_list_size,
            cast_options,
        ),
        (DataType::ListView(_), DataType::ListView(target_inner)) => {
            cast_list_view_column::<i32>(source_col, target_inner, cast_options)
        }
        (DataType::LargeListView(_), DataType::LargeListView(target_inner)) => {
            cast_list_view_column::<i64>(source_col, target_inner, cast_options)
        }
        (DataType::Map(_, _), DataType::Map(target_entries, target_sorted)) => {
            if requires_nested_struct_cast(source_col.data_type(), target_type) {
                cast_map_column(
                    source_col.as_map(),
                    target_entries,
                    *target_sorted,
                    cast_options,
                )
            } else {
                Ok(cast_with_options(source_col, target_type, cast_options)?)
            }
        }
        (
            DataType::Dictionary(source_key_type, _),
            DataType::Dictionary(target_key_type, target_value_type),
        ) => cast_dictionary_column(
            source_col,
            source_key_type,
            target_key_type,
            target_value_type,
            cast_options,
        ),
        (
            DataType::Union(source_fields, source_mode),
            DataType::Union(target_fields, target_mode),
        ) => cast_union_column(
            source_col,
            source_fields,
            source_mode,
            target_fields,
            target_mode,
            cast_options,
        ),
        _ => Ok(cast_with_options(source_col, target_type, cast_options)?),
    }
}

fn cast_list_column<O: arrow::array::OffsetSizeTrait>(
    source_col: &ArrayRef,
    target_inner_field: &FieldRef,
    cast_options: &CastOptions,
) -> Result<ArrayRef> {
    let source_list = source_col.as_list::<O>();
    let offsets = source_list.value_offsets();
    let needs_compaction = offsets[0] != O::usize_as(0)
        || offsets[offsets.len() - 1].as_usize() != source_list.values().len()
        || source_list
            .offsets()
            .has_non_empty_nulls(source_list.nulls());
    let compacted_list = if needs_compaction {
        Some(compact_list_values(source_list)?)
    } else {
        None
    };
    let source_list = compacted_list.as_ref().unwrap_or(source_list);

    let cast_values = cast_column(
        source_list.values(),
        target_inner_field.data_type(),
        cast_options,
    )?;

    let result = GenericListArray::<O>::new(
        Arc::clone(target_inner_field),
        source_list.offsets().clone(),
        cast_values,
        source_list.nulls().cloned(),
    );
    Ok(Arc::new(result))
}

fn cast_list_view_column<O: arrow::array::OffsetSizeTrait>(
    source_col: &ArrayRef,
    target_inner_field: &FieldRef,
    cast_options: &CastOptions,
) -> Result<ArrayRef> {
    let source_list = source_col.as_list_view::<O>();
    let compacted_values = compact_list_view_values(source_list)?;
    let (offsets, sizes, values) = match compacted_values.as_ref() {
        Some((offsets, sizes, values)) => (offsets, sizes, values),
        None => (
            source_list.offsets(),
            source_list.sizes(),
            source_list.values(),
        ),
    };

    let cast_values = cast_column(values, target_inner_field.data_type(), cast_options)?;

    let result = GenericListViewArray::<O>::try_new(
        Arc::clone(target_inner_field),
        offsets.clone(),
        sizes.clone(),
        cast_values,
        source_list.nulls().cloned(),
    )?;
    Ok(Arc::new(result))
}

fn compact_list_values<O: arrow::array::OffsetSizeTrait>(
    list: &GenericListArray<O>,
) -> Result<GenericListArray<O>> {
    let indices = UInt64Array::from_iter_values(0..list.len() as u64);
    Ok(take(list, &indices, None)?.as_list::<O>().clone())
}

type CompactedListView<O> = (ScalarBuffer<O>, ScalarBuffer<O>, ArrayRef);

/// Selects the union of child ranges reachable from valid ListView rows.
///
/// ListView ranges may overlap or appear in any order, so selecting each row
/// independently could duplicate a large number of child values. Merging the
/// ranges first preserves sharing while excluding unreachable values.
fn compact_list_view_values<O: arrow::array::OffsetSizeTrait>(
    list: &GenericListViewArray<O>,
) -> Result<Option<CompactedListView<O>>> {
    let mut dense_end = 0;
    let mut is_dense = true;
    for row in 0..list.len() {
        if list.is_null(row) || list.value_sizes()[row] == O::usize_as(0) {
            continue;
        }
        let start = list.value_offsets()[row].as_usize();
        if start != dense_end {
            is_dense = false;
            break;
        }
        dense_end = start + list.value_sizes()[row].as_usize();
    }
    if is_dense && dense_end == list.values().len() {
        return Ok(None);
    }

    let mut ranges = list
        .value_offsets()
        .iter()
        .zip(list.value_sizes())
        .enumerate()
        .filter(|(row, (_, size))| list.is_valid(*row) && **size != O::usize_as(0))
        .map(|(_, (offset, size))| {
            let start = offset.as_usize();
            (start, start + size.as_usize())
        })
        .collect::<Vec<_>>();
    ranges.sort_unstable_by_key(|range| range.0);

    let mut merged_ranges: Vec<(usize, usize)> = Vec::with_capacity(ranges.len());
    for (start, end) in ranges {
        if let Some((_, previous_end)) = merged_ranges.last_mut()
            && start <= *previous_end
        {
            *previous_end = (*previous_end).max(end);
        } else {
            merged_ranges.push((start, end));
        }
    }

    if merged_ranges.as_slice() == [(0, list.values().len())] {
        return Ok(None);
    }

    let mut range_bases = Vec::with_capacity(merged_ranges.len());
    let mut selected_len = 0;
    for &(start, end) in &merged_ranges {
        range_bases.push(selected_len);
        selected_len += end - start;
    }

    let mut offsets = Vec::with_capacity(list.len());
    let mut sizes = Vec::with_capacity(list.len());
    for row in 0..list.len() {
        let size = list.value_sizes()[row];
        if list.is_null(row) || size == O::usize_as(0) {
            offsets.push(O::usize_as(0));
            sizes.push(O::usize_as(0));
            continue;
        }

        let source_offset = list.value_offsets()[row].as_usize();
        let range_index =
            merged_ranges.partition_point(|range| range.0 <= source_offset) - 1;
        let new_offset =
            range_bases[range_index] + source_offset - merged_ranges[range_index].0;
        offsets.push(O::from_usize(new_offset).ok_or_else(|| {
            ArrowError::ComputeError("ListView offset overflow during compaction".into())
        })?);
        sizes.push(size);
    }

    let child_indices = UInt64Array::from_iter_values(
        merged_ranges
            .iter()
            .flat_map(|&(start, end)| (start..end).map(|index| index as u64)),
    );
    let values = take(list.values(), &child_indices, None)?;
    Ok(Some((offsets.into(), sizes.into(), values)))
}

fn cast_fixed_size_list_column(
    source_col: &ArrayRef,
    target_inner_field: &FieldRef,
    target_list_size: i32,
    cast_options: &CastOptions,
) -> Result<ArrayRef> {
    let source_list = source_col.as_fixed_size_list();

    let source_values = source_list.values();
    let target_type = target_inner_field.data_type();

    let cast_values = match cast_column(source_values, target_type, cast_options) {
        Ok(cast_values) => cast_values,
        Err(error) => match cast_fixed_size_list_values_with_parent_nulls(
            source_values,
            target_type,
            cast_options,
            source_list.nulls(),
            target_list_size,
        ) {
            Some(masked_cast) => masked_cast?,
            None => return Err(error),
        },
    };

    Ok(Arc::new(FixedSizeListArray::try_new(
        Arc::clone(target_inner_field),
        target_list_size,
        cast_values,
        source_list.nulls().cloned(),
    )?))
}

fn cast_fixed_size_list_values_with_parent_nulls(
    source_values: &ArrayRef,
    target_type: &DataType,
    cast_options: &CastOptions,
    parent_nulls: Option<&NullBuffer>,
    list_size: i32,
) -> Option<Result<ArrayRef>> {
    let parent_nulls = parent_nulls.filter(|nulls| nulls.null_count() > 0)?;

    // FixedSizeList stores child slots for null parent lists. Those child
    // values are semantically hidden, but recursive casts still inspect them.
    let hidden_child_nulls = parent_nulls.expand(list_size as usize);
    let masked_values = mask_array_values(source_values, &hidden_child_nulls);
    Some(masked_values.and_then(|values| cast_column(&values, target_type, cast_options)))
}

fn mask_array_values(
    values: &ArrayRef,
    additional_nulls: &NullBuffer,
) -> Result<ArrayRef> {
    let nulls = NullBuffer::union(values.nulls(), Some(additional_nulls));

    if let Some(struct_array) = values.as_any().downcast_ref::<StructArray>() {
        let struct_nulls = nulls
            .as_ref()
            .expect("additional nulls always produce nulls");
        let arrays = struct_array
            .columns()
            .iter()
            .map(|child| mask_array_values(child, struct_nulls))
            .collect::<Result<Vec<_>>>()?;
        return Ok(Arc::new(StructArray::new(
            struct_array.fields().clone(),
            arrays,
            nulls,
        )));
    }

    Ok(make_array(
        values.to_data().into_builder().nulls(nulls).build()?,
    ))
}

/// Casts Map children by their semantic positions: key at index 0 and value at
/// index 1. Technical entry field names are taken from the target schema.
///
/// Nested Struct fields within keys and values are still matched by name. Key
/// evolution is restricted by [`validate_map_key_compatibility`] so it cannot
/// remove identity-bearing fields; sorted Maps require an unchanged key type.
///
/// Map-specific compaction is necessary because sliced Maps retain unreachable
/// entries and null parents hide entries in their backing array, which `keys()`
/// and `values()` expose to recursive casts. List-family casts currently
/// preserve offsets and cast their backing values directly; consistent handling
/// is tracked in #24506.
fn cast_map_column(
    source_map: &MapArray,
    target_entries: &FieldRef,
    target_sorted: bool,
    cast_options: &CastOptions,
) -> Result<ArrayRef> {
    let DataType::Map(source_entries, source_sorted) = source_map.data_type() else {
        unreachable!("MapArray data type must be Map")
    };
    let (target_key, target_value) = validate_map_compatibility(
        source_entries,
        *source_sorted,
        target_entries,
        target_sorted,
        None,
    )?;

    let offsets = source_map.value_offsets();
    let has_unreachable_entries = offsets[0] != 0
        || offsets[offsets.len() - 1] as usize != source_map.entries().len();
    let needs_compaction = has_unreachable_entries
        || source_map.offsets().has_non_empty_nulls(source_map.nulls());
    let compacted_map = if needs_compaction {
        Some(compact_map_entries(source_map)?)
    } else {
        None
    };
    let source_map = compacted_map.as_ref().unwrap_or(source_map);

    let cast_keys = cast_column(source_map.keys(), target_key.data_type(), cast_options)
        .map_err(|error| error.context("While casting Map keys"))?;
    let cast_values =
        cast_column(source_map.values(), target_value.data_type(), cast_options)
            .map_err(|error| error.context("While casting Map values"))?;
    let Struct(target_fields) = target_entries.data_type() else {
        unreachable!("validated Map entries must be Struct")
    };
    let cast_entries =
        StructArray::try_new(target_fields.clone(), vec![cast_keys, cast_values], None)?;

    Ok(Arc::new(MapArray::try_new(
        Arc::clone(target_entries),
        source_map.offsets().clone(),
        cast_entries,
        source_map.nulls().cloned(),
        target_sorted,
    )?))
}

/// Returns an equivalent MapArray whose entries contain only values reachable
/// from visible Map rows.
///
/// Arrow Map arrays can contain unreachable entries after slicing, or entries
/// hidden behind null parent rows. An identity `take` rebuilds the Map through
/// Arrow's selection kernel, normalizing offsets and dropping those unreachable
/// child entries before recursive key/value casts are applied.
fn compact_map_entries(map: &MapArray) -> Result<MapArray> {
    let indices = UInt64Array::from_iter_values(0..map.len() as u64);
    Ok(take(map, &indices, None)?.as_map().clone())
}

fn cast_dictionary_column(
    source_col: &ArrayRef,
    source_key_type: &DataType,
    target_key_type: &DataType,
    target_value_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef> {
    // Dispatch on source key type to access keys/values, then recursively
    // cast values. Rebuild with the source key type first.
    macro_rules! cast_dict_values {
        ($t:ty) => {{
            let source_dict = source_col
                .as_any()
                .downcast_ref::<DictionaryArray<$t>>()
                .expect("downcast must succeed");
            let cast_values =
                cast_column(source_dict.values(), target_value_type, cast_options)?;
            Ok(Arc::new(DictionaryArray::<$t>::new(
                source_dict.keys().clone(),
                cast_values,
            )) as ArrayRef)
        }};
    }

    let result: Result<ArrayRef> = downcast_integer! {
        source_key_type => (cast_dict_values),
        k => _plan_err!("Unsupported dictionary key type: {k}")
    };
    let result = result?;

    // If key types differ, delegate key casting to Arrow.
    if source_key_type != target_key_type {
        let target_dict_type = DataType::Dictionary(
            Box::new(target_key_type.clone()),
            Box::new(target_value_type.clone()),
        );
        Ok(cast_with_options(&result, &target_dict_type, cast_options)?)
    } else {
        Ok(result)
    }
}

/// Validates compatibility between source and target struct fields for casting operations.
///
/// This function implements comprehensive struct compatibility checking by examining:
/// - Field name matching between source and target structs
/// - Type castability for each matching field (including recursive struct validation)
/// - Proper handling of missing fields (target fields not in source are allowed - filled with nulls)
/// - Proper handling of extra fields (source fields not in target are allowed - ignored)
///
/// # Compatibility Rules
/// - **Field Matching**: Fields are matched by name (case-sensitive)
/// - **Missing Target Fields**: Allowed - will be filled with null values during casting
/// - **Extra Source Fields**: Allowed - will be ignored during casting
/// - **Type Compatibility**: Each matching field must be castable using Arrow's type system
/// - **Nested Structs**: Recursively validates nested struct compatibility
///
/// # Arguments
/// * `source_fields` - Fields from the source struct type
/// * `target_fields` - Fields from the target struct type
///
/// # Returns
/// * `Ok(())` if the structs are compatible for casting
/// * `Err(DataFusionError)` with detailed error message if incompatible
///
/// # Examples
/// ```text
/// // Compatible: source has extra field, target has missing field
/// // Source: {a: i32, b: string, c: f64}
/// // Target: {a: i64, d: bool}
/// // Result: Ok(()) - 'a' can cast i32->i64, 'b','c' ignored, 'd' filled with nulls
///
/// // Incompatible: matching field has incompatible types
/// // Source: {a: string}
/// // Target: {a: binary}
/// // Result: Err(...) - string cannot cast to binary
/// ```
///
pub fn validate_struct_compatibility(
    source_fields: &[FieldRef],
    target_fields: &[FieldRef],
) -> Result<()> {
    let has_overlap = has_one_of_more_common_fields(source_fields, target_fields);
    if !has_overlap {
        return _plan_err!(
            "Cannot cast struct with {} fields to {} fields because there is no field name overlap",
            source_fields.len(),
            target_fields.len()
        );
    }

    // Check compatibility for each target field
    for target_field in target_fields {
        // Look for matching field in source by name
        if let Some(source_field) = source_fields
            .iter()
            .find(|f| f.name() == target_field.name())
        {
            validate_field_compatibility(source_field, target_field)?;
        } else {
            // Target field is missing from source
            // If it's non-nullable, we cannot fill it with NULL
            if !target_field.is_nullable() {
                return _plan_err!(
                    "Cannot cast struct: target field '{}' is non-nullable but missing from source. \
                     Cannot fill with NULL.",
                    target_field.name()
                );
            }
        }
    }

    // Extra fields in source are OK - they'll be ignored
    Ok(())
}

fn validate_field_compatibility(
    source_field: &Field,
    target_field: &Field,
) -> Result<()> {
    if source_field.data_type() == &DataType::Null {
        // Validate that target allows nulls before returning early.
        // It is invalid to cast a NULL source field to a non-nullable target field.
        if !target_field.is_nullable() {
            return _plan_err!(
                "Cannot cast NULL struct field '{}' to non-nullable field '{}'",
                source_field.name(),
                target_field.name()
            );
        }
        return Ok(());
    }

    // Ensure nullability is compatible. It is invalid to cast a nullable
    // source field to a non-nullable target field as this may discard
    // null values.
    if source_field.is_nullable() && !target_field.is_nullable() {
        return _plan_err!(
            "Cannot cast nullable struct field '{}' to non-nullable field",
            target_field.name()
        );
    }

    validate_data_type_compatibility(
        target_field.name(),
        source_field.data_type(),
        target_field.data_type(),
    )
}

fn validate_map_compatibility<'a>(
    source_entries: &Field,
    source_sorted: bool,
    target_entries: &'a Field,
    target_sorted: bool,
    field_name: Option<&str>,
) -> Result<(&'a FieldRef, &'a FieldRef)> {
    if source_sorted != target_sorted {
        if let Some(field_name) = field_name {
            return _plan_err!(
                "Cannot change Map sorted flag for field '{}' during schema adaptation",
                field_name
            );
        }
        return _plan_err!("Cannot change Map sorted flag during schema adaptation");
    }
    let (source_key, source_value) = validate_map_entries_field(source_entries)?;
    let (target_key, target_value) = validate_map_entries_field(target_entries)?;
    validate_map_key_compatibility(source_key, target_key, target_sorted)?;
    validate_field_compatibility(source_value, target_value)?;
    Ok((target_key, target_value))
}

fn validate_map_key_compatibility(
    source_key: &Field,
    target_key: &Field,
    sorted: bool,
) -> Result<()> {
    if sorted && source_key.data_type() != target_key.data_type() {
        return _plan_err!("Cannot evolve key type of a sorted Map");
    }
    validate_map_key_data_type(source_key.data_type(), target_key.data_type())
}

fn validate_map_key_data_type(
    source_type: &DataType,
    target_type: &DataType,
) -> Result<()> {
    if source_type == target_type {
        return Ok(());
    }

    match (source_type, target_type) {
        (Struct(source_fields), Struct(target_fields)) => {
            if !has_one_of_more_common_fields(source_fields, target_fields) {
                return _plan_err!(
                    "Cannot cast Map key Struct because there is no field name overlap"
                );
            }

            let target_by_name: HashMap<&str, &FieldRef> = target_fields
                .iter()
                .map(|field| (field.name().as_str(), field))
                .collect();
            let source_names: HashSet<&str> = source_fields
                .iter()
                .map(|field| field.name().as_str())
                .collect();

            for source_field in source_fields {
                let Some(target_field) = target_by_name.get(source_field.name().as_str())
                else {
                    return _plan_err!(
                        "Cannot remove field '{}' from a Map key Struct",
                        source_field.name()
                    );
                };
                if source_field.is_nullable() && !target_field.is_nullable() {
                    return _plan_err!(
                        "Cannot cast nullable Map key field '{}' to non-nullable field",
                        source_field.name()
                    );
                }
                validate_map_key_data_type(
                    source_field.data_type(),
                    target_field.data_type(),
                )?;
            }
            for target_field in target_fields {
                if !source_names.contains(target_field.name().as_str())
                    && !target_field.is_nullable()
                {
                    return _plan_err!(
                        "Cannot add non-nullable field '{}' to a Map key Struct",
                        target_field.name()
                    );
                }
            }
            Ok(())
        }
        _ if is_injective_map_key_cast(source_type, target_type) => Ok(()),
        _ => _plan_err!(
            "Cannot safely evolve Map key type from {} to {}",
            source_type,
            target_type
        ),
    }
}

/// Returns true when casting a Map key from `source_type` to `target_type` is
/// known to preserve key identity.
///
/// Keep this list conservative. Map keys define equality/lookup identity, so
/// only representation widenings that cannot merge distinct source keys belong
/// here. Other Arrow-supported casts, including decimal/timestamp widening and
/// dictionaries, should stay rejected until their identity semantics are
/// deliberately reviewed.
fn is_injective_map_key_cast(source_type: &DataType, target_type: &DataType) -> bool {
    matches!(
        (source_type, target_type),
        (
            DataType::Int8,
            DataType::Int16 | DataType::Int32 | DataType::Int64
        ) | (DataType::Int16, DataType::Int32 | DataType::Int64)
            | (DataType::Int32, DataType::Int64)
            | (
                DataType::UInt8,
                DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
            )
            | (
                DataType::UInt16,
                DataType::Int32 | DataType::Int64 | DataType::UInt32 | DataType::UInt64
            )
            | (DataType::UInt32, DataType::Int64 | DataType::UInt64)
            | (DataType::Utf8, DataType::LargeUtf8 | DataType::Utf8View)
            | (
                DataType::Binary,
                DataType::LargeBinary | DataType::BinaryView
            )
    )
}

fn validate_map_entries_field(entries: &Field) -> Result<(&FieldRef, &FieldRef)> {
    if entries.is_nullable() {
        return _plan_err!("Map entries field must be non-nullable");
    }

    let Struct(fields) = entries.data_type() else {
        return _plan_err!("Map entries field must be a struct");
    };
    if fields.len() != 2 {
        return _plan_err!(
            "Map entries struct must contain exactly two fields, found {}",
            fields.len()
        );
    }
    if fields[0].is_nullable() {
        return _plan_err!("Map key field must be non-nullable");
    }
    Ok((&fields[0], &fields[1]))
}

/// Validates that `source_type` can be cast to `target_type`, recursively
/// handling container types that wrap structs.
///
/// # Map evolution
///
/// Map entry children are matched by position (key, then value), regardless of
/// their technical field names. Nested Struct fields within each child are
/// matched by name. Values use the standard Struct evolution rules: nullable
/// target fields may be added and extra source fields are omitted.
///
/// For Maps using specialized nested Struct adaptation, unsorted Struct keys may
/// add nullable fields and use only injective primitive widening casts, but may
/// not remove source fields. Sorted Maps require an unchanged key type. Map
/// entries and keys must remain non-nullable. Source and target sorted flags
/// must match. Maps without semantic Struct children use Arrow's normal cast.
fn validate_union_schema_compatibility(
    source_fields: &UnionFields,
    source_mode: &UnionMode,
    target_fields: &UnionFields,
    target_mode: &UnionMode,
) -> Result<()> {
    if source_mode != target_mode {
        return _plan_err!(
            "Cannot adapt Union from mode {source_mode:?} to {target_mode:?}"
        );
    }

    // This adapter is for schema conformance, not general Union variant-set evolution.
    if source_fields.len() != target_fields.len() {
        return _plan_err!(
            "Cannot adapt Union schema with different field sets:              source has {} fields, target has {}",
            source_fields.len(),
            target_fields.len()
        );
    }

    for (target_type_id, target_field) in target_fields.iter() {
        let Some((_, source_field)) = source_fields
            .iter()
            .find(|(source_type_id, _)| *source_type_id == target_type_id)
        else {
            return _plan_err!(
                "Cannot adapt Union schema: target type ID {target_type_id}                  ('{}') is missing from source",
                target_field.name()
            );
        };

        if !target_field.contains(source_field) {
            return _plan_err!(
                "Cannot adapt Union child with type ID {target_type_id}:                  source field {source_field} is not contained by target field {target_field}"
            );
        }
    }

    Ok(())
}

pub fn validate_data_type_compatibility(
    field_name: &str,
    source_type: &DataType,
    target_type: &DataType,
) -> Result<()> {
    match (source_type, target_type) {
        (Struct(source_nested), Struct(target_nested)) => {
            validate_struct_compatibility(source_nested, target_nested)?;
        }
        (
            DataType::FixedSizeList(s, source_list_size),
            DataType::FixedSizeList(t, target_list_size),
        ) if source_list_size == target_list_size => {
            validate_field_compatibility(s, t)?;
        }
        (DataType::List(s), DataType::List(t))
        | (DataType::LargeList(s), DataType::LargeList(t))
        | (DataType::ListView(s), DataType::ListView(t))
        | (DataType::LargeListView(s), DataType::LargeListView(t)) => {
            validate_field_compatibility(s, t)?;
        }
        (DataType::Map(s, source_sorted), DataType::Map(t, target_sorted)) => {
            if requires_nested_struct_cast(source_type, target_type) {
                validate_map_compatibility(
                    s,
                    *source_sorted,
                    t,
                    *target_sorted,
                    Some(field_name),
                )?;
            } else if !can_cast_types(source_type, target_type) {
                return _plan_err!(
                    "Cannot cast struct field '{}' from type {} to type {}",
                    field_name,
                    source_type,
                    target_type
                );
            }
        }
        (DataType::Dictionary(s_key, s_val), DataType::Dictionary(t_key, t_val)) => {
            if !can_cast_types(s_key, t_key) {
                return _plan_err!(
                    "Cannot cast dictionary key type {} to {} for field '{}'",
                    s_key,
                    t_key,
                    field_name
                );
            }
            validate_data_type_compatibility(field_name, s_val, t_val)?;
        }
        (
            DataType::Union(source_fields, source_mode),
            DataType::Union(target_fields, target_mode),
        ) => {
            validate_union_schema_compatibility(
                source_fields,
                source_mode,
                target_fields,
                target_mode,
            )?;
        }
        _ => {
            if !can_cast_types(source_type, target_type) {
                return _plan_err!(
                    "Cannot cast struct field '{}' from type {} to type {}",
                    field_name,
                    source_type,
                    target_type
                );
            }
        }
    }
    Ok(())
}

/// Returns true if casting from `source_type` to `target_type` requires
/// DataFusion's specialized nested/container casting logic rather than Arrow's
/// standard cast.
///
/// This is the case when both types are struct types, or both are the same
/// container type (List, LargeList, equal-width FixedSizeList, ListView,
/// LargeListView, Map, Dictionary) wrapping types that recursively contain structs.
///
/// Map entries are technically Struct-backed, but that implementation detail does
/// not require name-based adaptation. A Map uses the specialized path only when
/// its semantic key or value child recursively contains a Struct.
///
/// Use this predicate at both planning time (to decide whether to apply nested
/// compatibility validation) and execution time (to decide whether to route
/// through [`cast_column`] instead of Arrow's generic cast).
pub fn requires_nested_struct_cast(
    source_type: &DataType,
    target_type: &DataType,
) -> bool {
    match (source_type, target_type) {
        (Struct(_), Struct(_)) => true,
        (
            DataType::FixedSizeList(s, source_list_size),
            DataType::FixedSizeList(t, target_list_size),
        ) if source_list_size == target_list_size => {
            requires_nested_struct_cast(s.data_type(), t.data_type())
        }
        (DataType::List(s), DataType::List(t))
        | (DataType::LargeList(s), DataType::LargeList(t))
        | (DataType::ListView(s), DataType::ListView(t))
        | (DataType::LargeListView(s), DataType::LargeListView(t)) => {
            requires_nested_struct_cast(s.data_type(), t.data_type())
        }
        (DataType::Map(source_entries, _), DataType::Map(target_entries, _)) => {
            map_entries_require_nested_struct_cast(source_entries, target_entries)
        }
        (DataType::Dictionary(_, s_val), DataType::Dictionary(_, t_val)) => {
            requires_nested_struct_cast(s_val, t_val)
        }
        _ => false,
    }
}

/// Returns whether corresponding semantic Map children require nested Struct casting.
///
/// Invalid Map entry layouts return `false`; normal Map validation/casting reports
/// those layouts through its usual error path.
fn map_entries_require_nested_struct_cast(
    source_entries: &FieldRef,
    target_entries: &FieldRef,
) -> bool {
    let (Struct(source_fields), Struct(target_fields)) =
        (source_entries.data_type(), target_entries.data_type())
    else {
        return false;
    };

    source_fields.len() == 2
        && target_fields.len() == 2
        && (requires_nested_struct_cast(
            source_fields[0].data_type(),
            target_fields[0].data_type(),
        ) || requires_nested_struct_cast(
            source_fields[1].data_type(),
            target_fields[1].data_type(),
        ))
}

/// Check if two field lists have at least one common field by name.
///
/// This is useful for validating struct compatibility when casting between structs,
/// ensuring that source and target fields have overlapping names.
pub fn has_one_of_more_common_fields(
    source_fields: &[FieldRef],
    target_fields: &[FieldRef],
) -> bool {
    let source_names: HashSet<&str> = source_fields
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    target_fields
        .iter()
        .any(|field| source_names.contains(field.name().as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{assert_contains, format::DEFAULT_CAST_OPTIONS};
    use arrow::{
        array::{
            BinaryArray, FixedSizeListArray, Int32Array, Int32Builder, Int64Array,
            Int64Builder, ListArray, ListViewArray, MapArray, MapBuilder, NullArray,
            StringArray, StringBuilder, UInt32Array,
        },
        buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
        datatypes::{DataType, Field, FieldRef, Int32Type},
    };
    /// Macro to extract and downcast a column from a StructArray
    macro_rules! get_column_as {
        ($struct_array:expr, $column_name:expr, $array_type:ty) => {
            $struct_array
                .column_by_name($column_name)
                .unwrap()
                .as_any()
                .downcast_ref::<$array_type>()
                .unwrap()
        };
    }

    fn field(name: &str, data_type: DataType) -> Field {
        Field::new(name, data_type, true)
    }

    fn non_null_field(name: &str, data_type: DataType) -> Field {
        Field::new(name, data_type, false)
    }

    fn arc_field(name: &str, data_type: DataType) -> FieldRef {
        Arc::new(field(name, data_type))
    }

    fn struct_type(fields: Vec<Field>) -> DataType {
        Struct(fields.into())
    }

    fn struct_field(name: &str, fields: Vec<Field>) -> Field {
        field(name, struct_type(fields))
    }

    fn arc_struct_field(name: &str, fields: Vec<Field>) -> FieldRef {
        Arc::new(struct_field(name, fields))
    }

    #[test]
    fn test_cast_simple_column() {
        let source = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let target_field = field("ints", DataType::Int64);
        let result =
            cast_column(&source, target_field.data_type(), &DEFAULT_CAST_OPTIONS)
                .unwrap();
        let result = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), 1);
        assert_eq!(result.value(1), 2);
        assert_eq!(result.value(2), 3);
    }

    #[test]
    fn test_cast_column_with_options() {
        let source = Arc::new(Int64Array::from(vec![1, i64::MAX])) as ArrayRef;
        let target_field = field("ints", DataType::Int32);

        let safe_opts = CastOptions {
            // safe: false - return Err for failure
            safe: false,
            ..DEFAULT_CAST_OPTIONS
        };
        assert!(cast_column(&source, target_field.data_type(), &safe_opts).is_err());

        let unsafe_opts = CastOptions {
            // safe: true - return Null for failure
            safe: true,
            ..DEFAULT_CAST_OPTIONS
        };
        let result =
            cast_column(&source, target_field.data_type(), &unsafe_opts).unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(result.value(0), 1);
        assert!(result.is_null(1));
    }

    #[test]
    fn test_cast_struct_with_missing_field() {
        let a_array = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
        let source_struct = StructArray::from(vec![(
            arc_field("a", DataType::Int32),
            Arc::clone(&a_array),
        )]);
        let source_col = Arc::new(source_struct) as ArrayRef;

        let target_field = struct_field(
            "s",
            vec![field("a", DataType::Int32), field("b", DataType::Utf8)],
        );

        let result =
            cast_column(&source_col, target_field.data_type(), &DEFAULT_CAST_OPTIONS)
                .unwrap();
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_array.fields().len(), 2);
        let a_result = get_column_as!(&struct_array, "a", Int32Array);
        assert_eq!(a_result.value(0), 1);
        assert_eq!(a_result.value(1), 2);

        let b_result = get_column_as!(&struct_array, "b", StringArray);
        assert_eq!(b_result.len(), 2);
        assert!(b_result.is_null(0));
        assert!(b_result.is_null(1));
    }

    #[test]
    fn test_cast_struct_source_not_struct() {
        let source = Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef;
        let target_field = struct_field("s", vec![field("a", DataType::Int32)]);

        let result =
            cast_column(&source, target_field.data_type(), &DEFAULT_CAST_OPTIONS);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Cannot cast column of type"));
        assert!(error_msg.contains("to struct type"));
        assert!(error_msg.contains("Source must be a struct"));
    }

    #[test]
    fn test_cast_struct_incompatible_child_type() {
        let a_array = Arc::new(BinaryArray::from(vec![
            Some(b"a".as_ref()),
            Some(b"b".as_ref()),
        ])) as ArrayRef;
        let source_struct =
            StructArray::from(vec![(arc_field("a", DataType::Binary), a_array)]);
        let source_col = Arc::new(source_struct) as ArrayRef;

        let target_field = struct_field("s", vec![field("a", DataType::Int32)]);

        let result =
            cast_column(&source_col, target_field.data_type(), &DEFAULT_CAST_OPTIONS);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Cannot cast struct field 'a'"));
    }

    #[test]
    fn test_validate_struct_compatibility_incompatible_types() {
        // Source struct: {field1: Binary, field2: String}
        let source_fields = vec![
            arc_field("field1", DataType::Binary),
            arc_field("field2", DataType::Utf8),
        ];

        // Target struct: {field1: Int32}
        let target_fields = vec![arc_field("field1", DataType::Int32)];

        let result = validate_struct_compatibility(&source_fields, &target_fields);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Cannot cast struct field 'field1'"));
        assert!(error_msg.contains("Binary"));
        assert!(error_msg.contains("Int32"));
    }

    #[test]
    fn test_validate_struct_compatibility_compatible_types() {
        // Source struct: {field1: Int32, field2: String}
        let source_fields = vec![
            arc_field("field1", DataType::Int32),
            arc_field("field2", DataType::Utf8),
        ];

        // Target struct: {field1: Int64} (Int32 can cast to Int64)
        let target_fields = vec![arc_field("field1", DataType::Int64)];

        let result = validate_struct_compatibility(&source_fields, &target_fields);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_struct_compatibility_missing_field_in_source() {
        // Source struct: {field1: Int32} (missing field2)
        let source_fields = vec![arc_field("field1", DataType::Int32)];

        // Target struct: {field1: Int32, field2: Utf8}
        let target_fields = vec![
            arc_field("field1", DataType::Int32),
            arc_field("field2", DataType::Utf8),
        ];

        // Should be OK - missing fields will be filled with nulls
        let result = validate_struct_compatibility(&source_fields, &target_fields);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_struct_compatibility_additional_field_in_source() {
        // Source struct: {field1: Int32, field2: String} (extra field2)
        let source_fields = vec![
            arc_field("field1", DataType::Int32),
            arc_field("field2", DataType::Utf8),
        ];

        // Target struct: {field1: Int32}
        let target_fields = vec![arc_field("field1", DataType::Int32)];

        // Should be OK - extra fields in source are ignored
        let result = validate_struct_compatibility(&source_fields, &target_fields);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_struct_compatibility_no_overlap_mismatch_len() {
        let source_fields = vec![
            arc_field("left", DataType::Int32),
            arc_field("right", DataType::Int32),
        ];
        let target_fields = vec![arc_field("alpha", DataType::Int32)];

        let result = validate_struct_compatibility(&source_fields, &target_fields);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert_contains!(error_msg, "no field name overlap");
    }

    #[test]
    fn test_cast_struct_parent_nulls_retained() {
        let a_array = Arc::new(Int32Array::from(vec![Some(1), Some(2)])) as ArrayRef;
        let fields = vec![arc_field("a", DataType::Int32)];
        let nulls = Some(NullBuffer::from(vec![true, false]));
        let source_struct = StructArray::new(fields.clone().into(), vec![a_array], nulls);
        let source_col = Arc::new(source_struct) as ArrayRef;

        let target_field = struct_field("s", vec![field("a", DataType::Int64)]);

        let result =
            cast_column(&source_col, target_field.data_type(), &DEFAULT_CAST_OPTIONS)
                .unwrap();
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_array.null_count(), 1);
        assert!(struct_array.is_valid(0));
        assert!(struct_array.is_null(1));

        let a_result = get_column_as!(&struct_array, "a", Int64Array);
        assert_eq!(a_result.value(0), 1);
        assert_eq!(a_result.value(1), 2);
    }

    #[test]
    fn test_validate_struct_compatibility_nullable_to_non_nullable() {
        // Source struct: {field1: Int32 nullable}
        let source_fields = vec![arc_field("field1", DataType::Int32)];

        // Target struct: {field1: Int32 non-nullable}
        let target_fields = vec![Arc::new(non_null_field("field1", DataType::Int32))];

        let result = validate_struct_compatibility(&source_fields, &target_fields);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("field1"));
        assert!(error_msg.contains("non-nullable"));
    }

    #[test]
    fn test_validate_struct_compatibility_non_nullable_to_nullable() {
        // Source struct: {field1: Int32 non-nullable}
        let source_fields = vec![Arc::new(non_null_field("field1", DataType::Int32))];

        // Target struct: {field1: Int32 nullable}
        let target_fields = vec![arc_field("field1", DataType::Int32)];

        let result = validate_struct_compatibility(&source_fields, &target_fields);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_struct_compatibility_nested_nullable_to_non_nullable() {
        // Source struct: {field1: {nested: Int32 nullable}}
        let source_fields = vec![Arc::new(non_null_field(
            "field1",
            struct_type(vec![field("nested", DataType::Int32)]),
        ))];

        // Target struct: {field1: {nested: Int32 non-nullable}}
        let target_fields = vec![Arc::new(non_null_field(
            "field1",
            struct_type(vec![non_null_field("nested", DataType::Int32)]),
        ))];

        let result = validate_struct_compatibility(&source_fields, &target_fields);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("nested"));
        assert!(error_msg.contains("non-nullable"));
    }

    #[test]
    fn test_validate_struct_compatibility_by_name() {
        // Source struct: {field1: Int32, field2: String}
        let source_fields = vec![
            arc_field("field1", DataType::Int32),
            arc_field("field2", DataType::Utf8),
        ];

        // Target struct: {field2: String, field1: Int64}
        let target_fields = vec![
            arc_field("field2", DataType::Utf8),
            arc_field("field1", DataType::Int64),
        ];

        let result = validate_struct_compatibility(&source_fields, &target_fields);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_struct_compatibility_by_name_with_type_mismatch() {
        // Source struct: {field1: Binary}
        let source_fields = vec![arc_field("field1", DataType::Binary)];

        // Target struct: {field1: Int32} (incompatible type)
        let target_fields = vec![arc_field("field1", DataType::Int32)];

        let result = validate_struct_compatibility(&source_fields, &target_fields);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert_contains!(
            error_msg,
            "Cannot cast struct field 'field1' from type Binary to type Int32"
        );
    }

    #[test]
    fn test_validate_struct_compatibility_no_overlap_equal_len() {
        let source_fields = vec![
            arc_field("left", DataType::Int32),
            arc_field("right", DataType::Utf8),
        ];

        let target_fields = vec![
            arc_field("alpha", DataType::Int32),
            arc_field("beta", DataType::Utf8),
        ];

        let result = validate_struct_compatibility(&source_fields, &target_fields);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert_contains!(error_msg, "no field name overlap");
    }

    #[test]
    fn test_validate_struct_compatibility_mixed_name_overlap() {
        // Source struct: {a: Int32, b: String, extra: Boolean}
        let source_fields = vec![
            arc_field("a", DataType::Int32),
            arc_field("b", DataType::Utf8),
            arc_field("extra", DataType::Boolean),
        ];

        // Target struct: {b: String, a: Int64, c: Float32}
        // Name overlap with a and b, missing c (nullable)
        let target_fields = vec![
            arc_field("b", DataType::Utf8),
            arc_field("a", DataType::Int64),
            arc_field("c", DataType::Float32),
        ];

        let result = validate_struct_compatibility(&source_fields, &target_fields);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_struct_compatibility_by_name_missing_required_field() {
        // Source struct: {field1: Int32} (missing field2)
        let source_fields = vec![arc_field("field1", DataType::Int32)];

        // Target struct: {field1: Int32, field2: Int32 non-nullable}
        let target_fields = vec![
            arc_field("field1", DataType::Int32),
            Arc::new(non_null_field("field2", DataType::Int32)),
        ];

        let result = validate_struct_compatibility(&source_fields, &target_fields);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert_contains!(
            error_msg,
            "Cannot cast struct: target field 'field2' is non-nullable but missing from source. Cannot fill with NULL."
        );
    }

    #[test]
    fn test_validate_struct_compatibility_partial_name_overlap_with_count_mismatch() {
        // Source struct: {a: Int32} (only one field)
        let source_fields = vec![arc_field("a", DataType::Int32)];

        // Target struct: {a: Int32, b: String} (two fields, but 'a' overlaps)
        let target_fields = vec![
            arc_field("a", DataType::Int32),
            arc_field("b", DataType::Utf8),
        ];

        // This should succeed - partial overlap means by-name mapping
        // and missing field 'b' is nullable
        let result = validate_struct_compatibility(&source_fields, &target_fields);
        assert!(result.is_ok());
    }

    #[test]
    fn test_cast_nested_struct_with_extra_and_missing_fields() {
        // Source inner struct has fields a, b, extra
        let a = Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef;
        let b = Arc::new(Int32Array::from(vec![Some(2), Some(3)])) as ArrayRef;
        let extra = Arc::new(Int32Array::from(vec![Some(9), Some(10)])) as ArrayRef;

        let inner = StructArray::from(vec![
            (arc_field("a", DataType::Int32), a),
            (arc_field("b", DataType::Int32), b),
            (arc_field("extra", DataType::Int32), extra),
        ]);

        let source_struct = StructArray::from(vec![(
            arc_struct_field(
                "inner",
                vec![
                    field("a", DataType::Int32),
                    field("b", DataType::Int32),
                    field("extra", DataType::Int32),
                ],
            ),
            Arc::new(inner) as ArrayRef,
        )]);
        let source_col = Arc::new(source_struct) as ArrayRef;

        // Target inner struct reorders fields, adds "missing", and drops "extra"
        let target_field = struct_field(
            "outer",
            vec![struct_field(
                "inner",
                vec![
                    field("b", DataType::Int64),
                    field("a", DataType::Int32),
                    field("missing", DataType::Int32),
                ],
            )],
        );

        let result =
            cast_column(&source_col, target_field.data_type(), &DEFAULT_CAST_OPTIONS)
                .unwrap();
        let outer = result.as_any().downcast_ref::<StructArray>().unwrap();
        let inner = get_column_as!(&outer, "inner", StructArray);
        assert_eq!(inner.fields().len(), 3);

        let b = get_column_as!(inner, "b", Int64Array);
        assert_eq!(b.value(0), 2);
        assert_eq!(b.value(1), 3);
        assert!(!b.is_null(0));
        assert!(!b.is_null(1));

        let a = get_column_as!(inner, "a", Int32Array);
        assert_eq!(a.value(0), 1);
        assert!(a.is_null(1));

        let missing = get_column_as!(inner, "missing", Int32Array);
        assert!(missing.is_null(0));
        assert!(missing.is_null(1));
    }

    #[test]
    fn test_cast_null_struct_field_to_nested_struct() {
        let null_inner = Arc::new(NullArray::new(2)) as ArrayRef;
        let source_struct = StructArray::from(vec![(
            arc_field("inner", DataType::Null),
            Arc::clone(&null_inner),
        )]);
        let source_col = Arc::new(source_struct) as ArrayRef;

        let target_field = struct_field(
            "outer",
            vec![struct_field("inner", vec![field("a", DataType::Int32)])],
        );

        let result =
            cast_column(&source_col, target_field.data_type(), &DEFAULT_CAST_OPTIONS)
                .unwrap();
        let outer = result.as_any().downcast_ref::<StructArray>().unwrap();
        let inner = get_column_as!(&outer, "inner", StructArray);
        assert_eq!(inner.len(), 2);
        assert!(inner.is_null(0));
        assert!(inner.is_null(1));

        let inner_a = get_column_as!(inner, "a", Int32Array);
        assert!(inner_a.is_null(0));
        assert!(inner_a.is_null(1));
    }

    #[test]
    fn test_cast_struct_with_array_and_map_fields() {
        // Array field with second row null
        let arr_array = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            None,
        ])) as ArrayRef;

        // Map field with second row null
        let string_builder = StringBuilder::new();
        let int_builder = Int32Builder::new();
        let mut map_builder = MapBuilder::new(None, string_builder, int_builder);
        map_builder.keys().append_value("a");
        map_builder.values().append_value(1);
        map_builder.append(true).unwrap();
        map_builder.append(false).unwrap();
        let map_array = Arc::new(map_builder.finish()) as ArrayRef;

        let source_struct = StructArray::from(vec![
            (
                arc_field(
                    "arr",
                    DataType::List(Arc::new(field("item", DataType::Int32))),
                ),
                arr_array,
            ),
            (
                arc_field(
                    "map",
                    DataType::Map(
                        Arc::new(non_null_field(
                            "entries",
                            struct_type(vec![
                                non_null_field("keys", DataType::Utf8),
                                field("values", DataType::Int32),
                            ]),
                        )),
                        false,
                    ),
                ),
                map_array,
            ),
        ]);
        let source_col = Arc::new(source_struct) as ArrayRef;

        let target_field = struct_field(
            "s",
            vec![
                field(
                    "arr",
                    DataType::List(Arc::new(field("item", DataType::Int32))),
                ),
                field(
                    "map",
                    DataType::Map(
                        Arc::new(non_null_field(
                            "entries",
                            struct_type(vec![
                                non_null_field("keys", DataType::Utf8),
                                field("values", DataType::Int32),
                            ]),
                        )),
                        false,
                    ),
                ),
            ],
        );

        let result =
            cast_column(&source_col, target_field.data_type(), &DEFAULT_CAST_OPTIONS)
                .unwrap();
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();

        let arr = get_column_as!(&struct_array, "arr", ListArray);
        assert!(!arr.is_null(0));
        assert!(arr.is_null(1));
        let arr0 = arr.value(0);
        let values = arr0.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), 2);

        let map = get_column_as!(&struct_array, "map", MapArray);
        assert!(!map.is_null(0));
        assert!(map.is_null(1));
        let map0 = map.value(0);
        let entries = map0.as_any().downcast_ref::<StructArray>().unwrap();
        let keys = get_column_as!(entries, "keys", StringArray);
        let vals = get_column_as!(entries, "values", Int32Array);
        assert_eq!(keys.value(0), "a");
        assert_eq!(vals.value(0), 1);
    }

    #[test]
    fn test_cast_struct_field_order_differs() {
        let a = Arc::new(Int32Array::from(vec![Some(1), Some(2)])) as ArrayRef;
        let b = Arc::new(Int32Array::from(vec![Some(3), None])) as ArrayRef;

        let source_struct = StructArray::from(vec![
            (arc_field("a", DataType::Int32), a),
            (arc_field("b", DataType::Int32), b),
        ]);
        let source_col = Arc::new(source_struct) as ArrayRef;

        let target_field = struct_field(
            "s",
            vec![field("b", DataType::Int64), field("a", DataType::Int32)],
        );

        let result =
            cast_column(&source_col, target_field.data_type(), &DEFAULT_CAST_OPTIONS)
                .unwrap();
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();

        let b_col = get_column_as!(&struct_array, "b", Int64Array);
        assert_eq!(b_col.value(0), 3);
        assert!(b_col.is_null(1));

        let a_col = get_column_as!(&struct_array, "a", Int32Array);
        assert_eq!(a_col.value(0), 1);
        assert_eq!(a_col.value(1), 2);
    }

    #[test]
    fn test_cast_struct_no_overlap_rejected() {
        let first = Arc::new(Int32Array::from(vec![Some(10), Some(20)])) as ArrayRef;
        let second =
            Arc::new(StringArray::from(vec![Some("alpha"), Some("beta")])) as ArrayRef;

        let source_struct = StructArray::from(vec![
            (arc_field("left", DataType::Int32), first),
            (arc_field("right", DataType::Utf8), second),
        ]);
        let source_col = Arc::new(source_struct) as ArrayRef;

        let target_field = struct_field(
            "s",
            vec![field("a", DataType::Int64), field("b", DataType::Utf8)],
        );

        let result =
            cast_column(&source_col, target_field.data_type(), &DEFAULT_CAST_OPTIONS);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert_contains!(error_msg, "no field name overlap");
    }

    #[test]
    fn test_cast_struct_missing_non_nullable_field_fails() {
        // Source has only field 'a'
        let a = Arc::new(Int32Array::from(vec![Some(1), Some(2)])) as ArrayRef;
        let source_struct = StructArray::from(vec![(arc_field("a", DataType::Int32), a)]);
        let source_col = Arc::new(source_struct) as ArrayRef;

        // Target has fields 'a' (nullable) and 'b' (non-nullable)
        let target_field = struct_field(
            "s",
            vec![
                field("a", DataType::Int32),
                non_null_field("b", DataType::Int32),
            ],
        );

        // Should fail because 'b' is non-nullable but missing from source
        let result =
            cast_column(&source_col, target_field.data_type(), &DEFAULT_CAST_OPTIONS);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("target field 'b' is non-nullable but missing from source"),
            "Unexpected error: {err}"
        );
    }

    #[test]
    fn test_cast_struct_missing_nullable_field_succeeds() {
        // Source has only field 'a'
        let a = Arc::new(Int32Array::from(vec![Some(1), Some(2)])) as ArrayRef;
        let source_struct = StructArray::from(vec![(arc_field("a", DataType::Int32), a)]);
        let source_col = Arc::new(source_struct) as ArrayRef;

        // Target has fields 'a' and 'b' (both nullable)
        let target_field = struct_field(
            "s",
            vec![field("a", DataType::Int32), field("b", DataType::Int32)],
        );

        // Should succeed - 'b' is nullable so can be filled with NULL
        let result =
            cast_column(&source_col, target_field.data_type(), &DEFAULT_CAST_OPTIONS)
                .unwrap();
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();

        let a_col = get_column_as!(&struct_array, "a", Int32Array);
        assert_eq!(a_col.value(0), 1);
        assert_eq!(a_col.value(1), 2);

        let b_col = get_column_as!(&struct_array, "b", Int32Array);
        assert!(b_col.is_null(0));
        assert!(b_col.is_null(1));
    }

    fn map_type(key_type: DataType, value_type: DataType) -> DataType {
        map_type_with_entry_names(key_type, value_type, "keys", "values", false)
    }

    fn map_type_with_entry_names(
        key_type: DataType,
        value_type: DataType,
        key_name: &str,
        value_name: &str,
        sorted: bool,
    ) -> DataType {
        DataType::Map(
            Arc::new(non_null_field(
                "entries",
                struct_type(vec![
                    non_null_field(key_name, key_type),
                    field(value_name, value_type),
                ]),
            )),
            sorted,
        )
    }

    fn struct_map_array() -> ArrayRef {
        struct_map_array_with_sorted(false)
    }

    fn struct_map_array_with_sorted(sorted: bool) -> ArrayRef {
        struct_map_array_with_key_fields(sorted, false)
    }

    fn struct_map_array_with_key_fields(sorted: bool, include_tenant: bool) -> ArrayRef {
        let mut key_fields = vec![(
            arc_field("id", DataType::Int32),
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
        )];
        if include_tenant {
            key_fields.push((
                arc_field("tenant", DataType::Utf8),
                Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
            ));
        }
        let keys = StructArray::from(key_fields);
        let values = StructArray::from(vec![
            (
                arc_field("amount", DataType::Int32),
                Arc::new(Int32Array::from(vec![10])) as ArrayRef,
            ),
            (
                arc_field("ignored", DataType::Utf8),
                Arc::new(StringArray::from(vec!["x"])) as ArrayRef,
            ),
        ]);
        let entries = StructArray::new(
            vec![
                Arc::new(non_null_field("keys", keys.data_type().clone())),
                arc_field("values", values.data_type().clone()),
            ]
            .into(),
            vec![Arc::new(keys), Arc::new(values)],
            None,
        );
        Arc::new(MapArray::new(
            Arc::new(non_null_field("entries", entries.data_type().clone())),
            OffsetBuffer::new(vec![0, 1, 1].into()),
            entries,
            Some(NullBuffer::from(vec![true, false])),
            sorted,
        ))
    }

    fn nested_struct_map_array(
        key_name: &str,
        value_name: &str,
        sorted: bool,
    ) -> ArrayRef {
        let key_nested = StructArray::from(vec![(
            arc_field("id", DataType::Int32),
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
        )]);
        let keys = StructArray::from(vec![(
            arc_field("nested", key_nested.data_type().clone()),
            Arc::new(key_nested) as ArrayRef,
        )]);
        let value_nested = StructArray::from(vec![(
            arc_field("amount", DataType::Int32),
            Arc::new(Int32Array::from(vec![10])) as ArrayRef,
        )]);
        let values = StructArray::from(vec![(
            arc_field("nested", value_nested.data_type().clone()),
            Arc::new(value_nested) as ArrayRef,
        )]);
        let entries = StructArray::new(
            vec![
                Arc::new(non_null_field(key_name, keys.data_type().clone())),
                arc_field(value_name, values.data_type().clone()),
            ]
            .into(),
            vec![Arc::new(keys), Arc::new(values)],
            None,
        );
        Arc::new(MapArray::new(
            Arc::new(non_null_field("entries", entries.data_type().clone())),
            OffsetBuffer::new(vec![0, 1].into()),
            entries,
            None,
            sorted,
        ))
    }

    #[test]
    fn test_safe_cast_to_non_nullable_struct_field_returns_error() {
        let source_col: ArrayRef = Arc::new(StructArray::new(
            vec![Arc::new(non_null_field("value", DataType::Utf8))].into(),
            vec![Arc::new(StringArray::from(vec!["not-an-int"]))],
            None,
        ));
        let target_type = struct_type(vec![non_null_field("value", DataType::Int32)]);
        let cast_options = CastOptions {
            safe: true,
            ..DEFAULT_CAST_OPTIONS
        };

        let error = cast_column(&source_col, &target_type, &cast_options)
            .unwrap_err()
            .to_string();
        assert_contains!(
            error,
            "Found unmasked nulls for non-nullable StructArray field \"value\""
        );
    }

    #[test]
    fn test_safe_cast_to_non_nullable_map_value_returns_error() {
        let entries = StructArray::new(
            vec![
                Arc::new(non_null_field("keys", DataType::Utf8)),
                Arc::new(non_null_field("values", DataType::Utf8)),
            ]
            .into(),
            vec![
                Arc::new(StringArray::from(vec!["key"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["not-an-int"])) as ArrayRef,
            ],
            None,
        );
        let source_col: ArrayRef = Arc::new(MapArray::new(
            Arc::new(non_null_field("entries", entries.data_type().clone())),
            OffsetBuffer::new(vec![0, 1].into()),
            entries,
            None,
            false,
        ));
        let target_type = DataType::Map(
            Arc::new(non_null_field(
                "entries",
                struct_type(vec![
                    non_null_field("keys", DataType::Utf8),
                    non_null_field("values", DataType::Int32),
                ]),
            )),
            false,
        );
        let cast_options = CastOptions {
            safe: true,
            ..DEFAULT_CAST_OPTIONS
        };

        let error = cast_column(&source_col, &target_type, &cast_options)
            .unwrap_err()
            .to_string();
        assert_contains!(
            error,
            "Found unmasked nulls for non-nullable StructArray field \"values\""
        );
    }

    #[test]
    fn test_cast_primitive_map_uses_arrow_cast() {
        let mut builder = MapBuilder::new(None, Int64Builder::new(), Int64Builder::new());
        builder.keys().append_value(1);
        builder.values().append_value(10);
        builder.append(true).unwrap();

        let source_col: ArrayRef = Arc::new(builder.finish());
        let target_type = map_type(DataType::Int32, DataType::Int32);

        assert!(!requires_nested_struct_cast(
            source_col.data_type(),
            &target_type
        ));
        assert!(
            validate_data_type_compatibility(
                "map_col",
                source_col.data_type(),
                &target_type
            )
            .is_ok()
        );

        let result =
            cast_column(&source_col, &target_type, &DEFAULT_CAST_OPTIONS).unwrap();
        assert_eq!(result.data_type(), &target_type);
    }

    #[test]
    fn test_cast_map_int32_keys_to_int64() {
        let mut builder =
            MapBuilder::new(None, Int32Builder::new(), StringBuilder::new());
        builder.keys().append_value(1);
        builder.keys().append_value(2);
        builder.values().append_value("a");
        builder.values().append_value("b");
        builder.append(true).unwrap();

        let source_col: ArrayRef = Arc::new(builder.finish());
        let target_type = map_type(DataType::Int64, DataType::Utf8);

        assert!(
            validate_data_type_compatibility(
                "map_col",
                source_col.data_type(),
                &target_type
            )
            .is_ok()
        );

        let result =
            cast_column(&source_col, &target_type, &DEFAULT_CAST_OPTIONS).unwrap();
        assert_eq!(result.data_type(), &target_type);
        let map = result.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map.value_offsets(), &[0, 2]);

        let keys = map.keys().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(keys.values(), &[1, 2]);
        let values = map.values().as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(values.value(0), "a");
        assert_eq!(values.value(1), "b");
    }

    #[test]
    fn test_cast_map_uint32_keys_to_int64() {
        let entries = StructArray::new(
            vec![
                Arc::new(non_null_field("keys", DataType::UInt32)),
                arc_field("values", DataType::Utf8),
            ]
            .into(),
            vec![
                Arc::new(UInt32Array::from(vec![u32::MAX])) as ArrayRef,
                Arc::new(StringArray::from(vec!["value"])) as ArrayRef,
            ],
            None,
        );
        let source_col: ArrayRef = Arc::new(MapArray::new(
            Arc::new(non_null_field("entries", entries.data_type().clone())),
            OffsetBuffer::new(vec![0, 1].into()),
            entries,
            None,
            false,
        ));
        let target_type = map_type(DataType::Int64, DataType::Utf8);

        assert!(
            validate_data_type_compatibility(
                "map_col",
                source_col.data_type(),
                &target_type
            )
            .is_ok()
        );

        let result =
            cast_column(&source_col, &target_type, &DEFAULT_CAST_OPTIONS).unwrap();
        let keys = result
            .as_map()
            .keys()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(keys.value(0), i64::from(u32::MAX));
    }

    #[test]
    fn test_injective_unsigned_to_signed_map_key_casts() {
        for (source_type, target_type) in [
            (DataType::UInt8, DataType::Int16),
            (DataType::UInt8, DataType::Int32),
            (DataType::UInt8, DataType::Int64),
            (DataType::UInt16, DataType::Int32),
            (DataType::UInt16, DataType::Int64),
            (DataType::UInt32, DataType::Int64),
        ] {
            assert!(is_injective_map_key_cast(&source_type, &target_type));
        }
        for (source_type, target_type) in [
            (DataType::UInt8, DataType::Int8),
            (DataType::UInt16, DataType::Int16),
            (DataType::UInt32, DataType::Int32),
        ] {
            assert!(!is_injective_map_key_cast(&source_type, &target_type));
        }
    }

    #[test]
    fn test_map_entry_names_match_positionally_and_adapt_nested_structs() {
        let source_col = nested_struct_map_array("source_keys", "source_values", false);
        let target_type = map_type_with_entry_names(
            struct_type(vec![field(
                "nested",
                struct_type(vec![
                    field("id", DataType::Int64),
                    field("label", DataType::Utf8),
                ]),
            )]),
            struct_type(vec![field(
                "nested",
                struct_type(vec![
                    field("amount", DataType::Int64),
                    field("currency", DataType::Utf8),
                ]),
            )]),
            "key",
            "value",
            false,
        );

        assert!(
            validate_data_type_compatibility(
                "map_col",
                source_col.data_type(),
                &target_type
            )
            .is_ok()
        );
        let result =
            cast_column(&source_col, &target_type, &DEFAULT_CAST_OPTIONS).unwrap();
        let map = result.as_any().downcast_ref::<MapArray>().unwrap();
        let (key_field, value_field) = map.entries_fields();
        assert_eq!(key_field.name(), "key");
        assert_eq!(value_field.name(), "value");
        let keys = map.keys().as_struct();
        let key_nested = keys.column_by_name("nested").unwrap().as_struct();
        assert_eq!(get_column_as!(key_nested, "id", Int64Array).value(0), 1);
        assert!(get_column_as!(key_nested, "label", StringArray).is_null(0));
        let values = map.values().as_struct();
        let value_nested = values.column_by_name("nested").unwrap().as_struct();
        assert_eq!(
            get_column_as!(value_nested, "amount", Int64Array).value(0),
            10
        );
        assert!(get_column_as!(value_nested, "currency", StringArray).is_null(0));
    }

    #[test]
    fn test_unsorted_map_key_struct_field_removal_rejected_by_planner_and_runtime() {
        let source_col = struct_map_array_with_key_fields(false, true);
        let target_type = map_type(
            struct_type(vec![field("id", DataType::Int32)]),
            struct_type(vec![
                field("amount", DataType::Int32),
                field("ignored", DataType::Utf8),
            ]),
        );
        assert_map_planning_runtime_error(
            &source_col,
            &target_type,
            "Cannot remove field 'tenant' from a Map key Struct",
        );
    }

    #[test]
    fn test_unsorted_map_non_injective_key_cast_rejected() {
        let source_col = struct_map_array();
        let target_type = map_type(
            struct_type(vec![field("id", DataType::Float64)]),
            struct_type(vec![
                field("amount", DataType::Int32),
                field("ignored", DataType::Utf8),
            ]),
        );
        assert_map_planning_runtime_error(
            &source_col,
            &target_type,
            "Cannot safely evolve Map key type",
        );
    }

    #[test]
    fn test_unsorted_map_key_struct_no_overlap_rejected_by_planner_and_runtime() {
        let keys = StructArray::new_empty_fields(1, Some(NullBuffer::from(vec![true])));
        let values = StructArray::from(vec![(
            arc_field("amount", DataType::Int32),
            Arc::new(Int32Array::from(vec![10])) as ArrayRef,
        )]);
        let entries = StructArray::new(
            vec![
                Arc::new(non_null_field("keys", keys.data_type().clone())),
                arc_field("values", values.data_type().clone()),
            ]
            .into(),
            vec![Arc::new(keys), Arc::new(values)],
            None,
        );
        let source_col: ArrayRef = Arc::new(MapArray::new(
            Arc::new(non_null_field("entries", entries.data_type().clone())),
            OffsetBuffer::new(vec![0, 1].into()),
            entries,
            None,
            false,
        ));
        let target_type = map_type(
            struct_type(vec![field("new_id", DataType::Int32)]),
            struct_type(vec![field("amount", DataType::Int32)]),
        );
        assert_map_planning_runtime_error(
            &source_col,
            &target_type,
            "Cannot cast Map key Struct because there is no field name overlap",
        );
    }

    #[test]
    fn test_sorted_map_key_struct_schema_evolution_rejected() {
        let source_col = struct_map_array_with_sorted(true);
        let target_type = map_type_with_entry_names(
            struct_type(vec![
                field("id", DataType::Int32),
                field("label", DataType::Utf8),
            ]),
            struct_type(vec![
                field("amount", DataType::Int32),
                field("ignored", DataType::Utf8),
            ]),
            "keys",
            "values",
            true,
        );
        assert_map_planning_runtime_error(
            &source_col,
            &target_type,
            "Cannot evolve key type of a sorted Map",
        );
    }

    #[test]
    fn test_sorted_map_value_struct_schema_evolution_preserves_sorted() {
        let source_col = struct_map_array_with_sorted(true);
        let target_type = map_type_with_entry_names(
            struct_type(vec![field("id", DataType::Int32)]),
            struct_type(vec![
                field("amount", DataType::Int64),
                field("currency", DataType::Utf8),
            ]),
            "keys",
            "values",
            true,
        );
        assert!(
            validate_data_type_compatibility(
                "map_col",
                source_col.data_type(),
                &target_type
            )
            .is_ok()
        );
        let result =
            cast_column(&source_col, &target_type, &DEFAULT_CAST_OPTIONS).unwrap();
        assert_eq!(result.data_type(), &target_type);
        let map = result.as_any().downcast_ref::<MapArray>().unwrap();
        let values = map.values().as_struct();
        assert_eq!(get_column_as!(values, "amount", Int64Array).value(0), 10);
        assert!(get_column_as!(values, "currency", StringArray).is_null(0));
    }

    #[test]
    fn test_sorted_map_value_struct_schema_evolution_to_unsorted_rejected() {
        let source_col = struct_map_array_with_sorted(true);
        let target_type = map_type_with_entry_names(
            struct_type(vec![
                field("id", DataType::Int32),
                field("label", DataType::Utf8),
            ]),
            struct_type(vec![
                field("amount", DataType::Int64),
                field("currency", DataType::Utf8),
            ]),
            "keys",
            "values",
            false,
        );

        assert_map_planning_runtime_error(
            &source_col,
            &target_type,
            "Cannot change Map sorted flag",
        );
    }

    #[test]
    fn test_null_map_parent_hides_invalid_nested_value_cast() {
        let values = StructArray::from(vec![(
            arc_field("amount", DataType::Utf8),
            Arc::new(StringArray::from(vec!["bad", "2"])) as ArrayRef,
        )]);
        let keys = StringArray::from(vec!["hidden", "visible"]);
        let entries = StructArray::new(
            vec![
                Arc::new(non_null_field("keys", DataType::Utf8)),
                arc_field("values", values.data_type().clone()),
            ]
            .into(),
            vec![Arc::new(keys), Arc::new(values)],
            None,
        );
        let source_col: ArrayRef = Arc::new(MapArray::new(
            Arc::new(non_null_field("entries", entries.data_type().clone())),
            OffsetBuffer::new(vec![0, 1, 2].into()),
            entries,
            Some(NullBuffer::from(vec![false, true])),
            false,
        ));
        let target_type = map_type(
            DataType::Utf8,
            struct_type(vec![field("amount", DataType::Int32)]),
        );

        let result =
            cast_column(&source_col, &target_type, &DEFAULT_CAST_OPTIONS).unwrap();
        let map = result.as_any().downcast_ref::<MapArray>().unwrap();
        assert!(map.is_null(0));
        assert_eq!(map.value_offsets(), &[0, 0, 1]);
        let visible_entries = map.value(1);
        let visible_values = visible_entries
            .column_by_name("values")
            .unwrap()
            .as_struct();
        assert_eq!(
            get_column_as!(visible_values, "amount", Int32Array).value(0),
            2
        );
    }

    #[test]
    fn test_sliced_map_ignores_unreachable_invalid_nested_value() {
        let values = StructArray::from(vec![(
            arc_field("amount", DataType::Utf8),
            Arc::new(StringArray::from(vec!["bad", "2"])) as ArrayRef,
        )]);
        let entries = StructArray::new(
            vec![
                Arc::new(non_null_field("keys", DataType::Utf8)),
                arc_field("values", values.data_type().clone()),
            ]
            .into(),
            vec![
                Arc::new(StringArray::from(vec!["unreachable", "visible"])),
                Arc::new(values),
            ],
            None,
        );
        let source_col: ArrayRef = Arc::new(
            MapArray::new(
                Arc::new(non_null_field("entries", entries.data_type().clone())),
                OffsetBuffer::new(vec![0, 1, 2].into()),
                entries,
                None,
                false,
            )
            .slice(1, 1),
        );
        let target_type = map_type(
            DataType::Utf8,
            struct_type(vec![field("amount", DataType::Int32)]),
        );

        let result =
            cast_column(&source_col, &target_type, &DEFAULT_CAST_OPTIONS).unwrap();
        let map = result.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map.value_offsets(), &[0, 1]);
        let values = map.values().as_struct();
        assert_eq!(get_column_as!(values, "amount", Int32Array).value(0), 2);
    }

    #[test]
    fn test_cast_map_struct_key_and_value() {
        let source_col = struct_map_array();
        let target_type = map_type(
            struct_type(vec![
                field("id", DataType::Int64),
                field("label", DataType::Utf8),
            ]),
            struct_type(vec![
                field("amount", DataType::Int64),
                field("currency", DataType::Utf8),
            ]),
        );

        assert!(requires_nested_struct_cast(
            source_col.data_type(),
            &target_type
        ));
        assert!(
            validate_data_type_compatibility(
                "map_col",
                source_col.data_type(),
                &target_type
            )
            .is_ok()
        );

        let result =
            cast_column(&source_col, &target_type, &DEFAULT_CAST_OPTIONS).unwrap();
        let map = result.as_any().downcast_ref::<MapArray>().unwrap();
        assert!(map.is_valid(0));
        assert!(map.is_null(1));

        let keys = map.keys().as_any().downcast_ref::<StructArray>().unwrap();
        let key_ids = get_column_as!(keys, "id", Int64Array);
        assert_eq!(key_ids.values(), &[1]);
        let key_labels = get_column_as!(keys, "label", StringArray);
        assert!(key_labels.is_null(0));

        let values = map.values().as_any().downcast_ref::<StructArray>().unwrap();
        let amounts = get_column_as!(values, "amount", Int64Array);
        assert_eq!(amounts.values(), &[10]);
        let currencies = get_column_as!(values, "currency", StringArray);
        assert!(currencies.is_null(0));
        assert!(values.column_by_name("ignored").is_none());
    }

    #[test]
    fn test_cast_all_null_map_with_struct_value() {
        let source_type = map_type(
            DataType::Utf8,
            struct_type(vec![field("amount", DataType::Int32)]),
        );
        let target_type = map_type(
            DataType::Utf8,
            struct_type(vec![
                field("amount", DataType::Int64),
                field("currency", DataType::Utf8),
            ]),
        );
        let source_col = new_null_array(&source_type, 2);

        let result =
            cast_column(&source_col, &target_type, &DEFAULT_CAST_OPTIONS).unwrap();
        let map = result.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map.null_count(), 2);
        assert!(map.entries().is_empty());
        assert_eq!(map.data_type(), &target_type);
    }

    #[test]
    fn test_map_entry_invariants_rejected_during_validation() {
        let source_col = struct_map_array();
        let DataType::Map(target_entries, sorted) = map_type(
            struct_type(vec![field("id", DataType::Int32)]),
            struct_type(vec![field("amount", DataType::Int32)]),
        ) else {
            unreachable!()
        };
        let nullable_entries = DataType::Map(
            Arc::new(target_entries.as_ref().clone().with_nullable(true)),
            sorted,
        );
        let error = validate_data_type_compatibility(
            "map_col",
            source_col.data_type(),
            &nullable_entries,
        )
        .unwrap_err()
        .to_string();
        assert_contains!(error, "Map entries field must be non-nullable");

        let Struct(fields) = target_entries.data_type() else {
            unreachable!()
        };
        let nullable_key_entries = Arc::new(Field::new(
            "entries",
            Struct(
                vec![
                    Arc::new(fields[0].as_ref().clone().with_nullable(true)),
                    Arc::clone(&fields[1]),
                ]
                .into(),
            ),
            false,
        ));
        let error = validate_data_type_compatibility(
            "map_col",
            source_col.data_type(),
            &DataType::Map(nullable_key_entries, sorted),
        )
        .unwrap_err()
        .to_string();
        assert_contains!(error, "Map key field must be non-nullable");

        let sorted_target = DataType::Map(target_entries, !sorted);
        assert_map_planning_runtime_error(
            &source_col,
            &sorted_target,
            "Cannot change Map sorted flag",
        );
    }

    #[test]
    fn test_map_sorted_flag_validation_error_includes_field_name() {
        let source_col = struct_map_array();
        let DataType::Map(target_entries, sorted) = source_col.data_type() else {
            unreachable!()
        };
        let target_type = DataType::Map(Arc::clone(target_entries), !sorted);

        let error = validate_data_type_compatibility(
            "map_col",
            source_col.data_type(),
            &target_type,
        )
        .unwrap_err()
        .to_string();

        assert_contains!(
            error,
            "Cannot change Map sorted flag for field 'map_col' during schema adaptation"
        );
    }

    fn assert_map_planning_runtime_error(
        source: &ArrayRef,
        target: &DataType,
        expected: &str,
    ) {
        let planning_error =
            validate_data_type_compatibility("map_col", source.data_type(), target)
                .unwrap_err()
                .to_string();
        assert_contains!(planning_error, expected);

        let runtime_error = cast_column(source, target, &DEFAULT_CAST_OPTIONS)
            .unwrap_err()
            .to_string();
        assert_contains!(runtime_error, expected);
    }

    #[test]
    fn test_map_struct_planner_runtime_parity_on_invalid_evolution() {
        let source_col = struct_map_array();
        let target_type = map_type(
            struct_type(vec![field("id", DataType::Int32)]),
            struct_type(vec![
                field("amount", DataType::Int32),
                non_null_field("currency", DataType::Utf8),
            ]),
        );
        assert_map_planning_runtime_error(
            &source_col,
            &target_type,
            "target field 'currency' is non-nullable",
        );

        let incompatible_target = map_type(
            struct_type(vec![field("id", DataType::Int32)]),
            struct_type(vec![field(
                "amount",
                struct_type(vec![field("value", DataType::Utf8)]),
            )]),
        );
        assert_map_planning_runtime_error(
            &source_col,
            &incompatible_target,
            "Cannot cast struct field 'amount'",
        );
    }

    #[test]
    fn test_validate_dictionary_value_evolution() {
        let source_inner = struct_type(vec![field("a", DataType::Int32)]);
        let target_inner = struct_type(vec![
            field("a", DataType::Int32),
            field("b", DataType::Utf8),
        ]);
        let source =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(source_inner));
        let target =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(target_inner));
        assert!(validate_data_type_compatibility("col", &source, &target).is_ok());
    }

    #[test]
    fn test_cast_dictionary_struct_value() {
        // Build a Dictionary<Int32, Struct{a: Int32}> and cast to
        // Dictionary<Int32, Struct{a: Int64, b: Utf8}> (field added, type widened).
        let struct_arr = StructArray::from(vec![(
            arc_field("a", DataType::Int32),
            Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef,
        )]);
        // keys: [0, null, 1] mapping into the 2-element struct values array.
        let keys = Int32Array::from(vec![Some(0), None, Some(1)]);
        let source_dict = DictionaryArray::<Int32Type>::new(keys, Arc::new(struct_arr));
        let source_col: ArrayRef = Arc::new(source_dict);

        let target_type = DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(struct_type(vec![
                field("a", DataType::Int64),
                field("b", DataType::Utf8),
            ])),
        );

        let result =
            cast_column(&source_col, &target_type, &DEFAULT_CAST_OPTIONS).unwrap();
        let result_dict = result
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();

        assert!(result_dict.is_valid(0));
        assert!(result_dict.is_null(1));
        assert!(result_dict.is_valid(2));

        let struct_values = result_dict
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let a_col = get_column_as!(&struct_values, "a", Int64Array);
        assert_eq!(a_col.values(), &[10, 20]);
        let b_col = get_column_as!(&struct_values, "b", StringArray);
        assert!(b_col.iter().all(|v| v.is_none()));
    }

    #[test]
    fn test_cast_list_view_struct() {
        // Build a ListView<Struct{a: Int32}> and cast to
        // ListView<Struct{a: Int64, b: Utf8}>.
        let struct_arr = StructArray::from(vec![(
            arc_field("a", DataType::Int32),
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
        )]);

        let source_field =
            arc_field("item", struct_type(vec![field("a", DataType::Int32)]));
        let target_field = arc_field(
            "item",
            struct_type(vec![
                field("a", DataType::Int64),
                field("b", DataType::Utf8),
            ]),
        );

        // Two list-view entries: [0..2] and [2..3]
        let list_view = ListViewArray::new(
            source_field,
            ScalarBuffer::from(vec![0i32, 2]),
            ScalarBuffer::from(vec![2i32, 1]),
            Arc::new(struct_arr),
            None,
        );
        let source_col: ArrayRef = Arc::new(list_view);

        let target_type = DataType::ListView(target_field);

        let result =
            cast_column(&source_col, &target_type, &DEFAULT_CAST_OPTIONS).unwrap();
        let result_lv = result.as_any().downcast_ref::<ListViewArray>().unwrap();
        assert_eq!(result_lv.len(), 2);

        let struct_values = result_lv
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let a_col = get_column_as!(&struct_values, "a", Int64Array);
        assert_eq!(a_col.values(), &[1, 2, 3]);
        let b_col = get_column_as!(&struct_values, "b", StringArray);
        assert!(b_col.iter().all(|v| v.is_none()));
    }

    fn list_struct_values(values: Vec<&str>) -> ArrayRef {
        Arc::new(StructArray::from(vec![(
            arc_field("value", DataType::Utf8),
            Arc::new(StringArray::from(values)) as ArrayRef,
        )]))
    }

    fn list_struct_fields() -> (FieldRef, FieldRef) {
        (
            arc_field("item", struct_type(vec![field("value", DataType::Utf8)])),
            arc_field("item", struct_type(vec![field("value", DataType::Int32)])),
        )
    }

    fn assert_visible_list_value(list: &ArrayRef, row: usize, expected: i32) {
        let values = list.as_list::<i32>().value(row);
        let values = values.as_struct();
        assert_eq!(
            get_column_as!(values, "value", Int32Array).value(0),
            expected
        );
    }

    #[test]
    fn test_sliced_list_ignores_unreachable_invalid_nested_value() {
        let (source_field, target_field) = list_struct_fields();
        let source_col: ArrayRef = Arc::new(
            ListArray::new(
                source_field,
                OffsetBuffer::new(vec![0, 1, 2].into()),
                list_struct_values(vec!["bad", "2"]),
                None,
            )
            .slice(1, 1),
        );
        let target_type = DataType::List(target_field);

        let result =
            cast_column(&source_col, &target_type, &DEFAULT_CAST_OPTIONS).unwrap();

        assert_eq!(result.data_type(), &target_type);
        assert_visible_list_value(&result, 0, 2);
    }

    #[test]
    fn test_null_list_parent_hides_invalid_nested_value() {
        let (source_field, target_field) = list_struct_fields();
        let source_col: ArrayRef = Arc::new(ListArray::new(
            source_field,
            OffsetBuffer::new(vec![0, 1, 2].into()),
            list_struct_values(vec!["bad", "2"]),
            Some(NullBuffer::from(vec![false, true])),
        ));
        let target_type = DataType::List(target_field);

        let result =
            cast_column(&source_col, &target_type, &DEFAULT_CAST_OPTIONS).unwrap();

        assert_eq!(result.data_type(), &target_type);
        assert!(result.is_null(0));
        assert_visible_list_value(&result, 1, 2);
    }

    #[test]
    fn test_sliced_large_list_ignores_unreachable_invalid_nested_value() {
        let (source_field, target_field) = list_struct_fields();
        let source_col: ArrayRef = Arc::new(
            GenericListArray::<i64>::new(
                source_field,
                OffsetBuffer::new(vec![0, 1, 2].into()),
                list_struct_values(vec!["bad", "2"]),
                None,
            )
            .slice(1, 1),
        );
        let target_type = DataType::LargeList(target_field);

        let result =
            cast_column(&source_col, &target_type, &DEFAULT_CAST_OPTIONS).unwrap();
        let values = result.as_list::<i64>().value(0);

        assert_eq!(result.data_type(), &target_type);
        assert_eq!(
            get_column_as!(values.as_struct(), "value", Int32Array).value(0),
            2
        );
    }

    #[test]
    fn test_list_view_ignores_unreachable_invalid_nested_values() {
        let (source_field, target_field) = list_struct_fields();
        let source_col: ArrayRef = Arc::new(ListViewArray::new(
            source_field,
            ScalarBuffer::from(vec![0i32, 1, 2]),
            ScalarBuffer::from(vec![1i32, 1, 1]),
            list_struct_values(vec!["bad", "2", "bad"]),
            Some(NullBuffer::from(vec![false, true, true])),
        ));
        let source_col = source_col.slice(0, 2);
        let target_type = DataType::ListView(target_field);

        let result =
            cast_column(&source_col, &target_type, &DEFAULT_CAST_OPTIONS).unwrap();
        let result = result.as_list_view::<i32>();
        let values = result.value(1);

        assert_eq!(result.data_type(), &target_type);
        assert!(result.is_null(0));
        assert_eq!(result.value_sizes(), &[0, 1]);
        assert_eq!(
            get_column_as!(values.as_struct(), "value", Int32Array).value(0),
            2
        );
    }

    #[test]
    fn test_all_null_list_view_ignores_invalid_backing_values() {
        let (source_field, target_field) = list_struct_fields();
        let source_col: ArrayRef = Arc::new(ListViewArray::new(
            source_field,
            ScalarBuffer::from(vec![0i32, 1]),
            ScalarBuffer::from(vec![1i32, 1]),
            list_struct_values(vec!["bad", "also_bad"]),
            Some(NullBuffer::from(vec![false, false])),
        ));
        let target_type = DataType::ListView(target_field);

        let result =
            cast_column(&source_col, &target_type, &DEFAULT_CAST_OPTIONS).unwrap();
        let result = result.as_list_view::<i32>();

        assert_eq!(result.data_type(), &target_type);
        assert_eq!(result.null_count(), 2);
        assert_eq!(result.value_offsets(), &[0, 0]);
        assert_eq!(result.value_sizes(), &[0, 0]);
        assert!(result.values().is_empty());
    }

    #[test]
    fn test_list_view_compacts_overlapping_out_of_order_ranges() {
        let (source_field, target_field) = list_struct_fields();
        let source_col: ArrayRef = Arc::new(ListViewArray::new(
            source_field,
            ScalarBuffer::from(vec![2i32, 0, 2]),
            ScalarBuffer::from(vec![2i32, 1, 1]),
            list_struct_values(vec!["1", "bad", "2", "3", "bad"]),
            None,
        ));
        let target_type = DataType::ListView(target_field);

        let result =
            cast_column(&source_col, &target_type, &DEFAULT_CAST_OPTIONS).unwrap();
        let result = result.as_list_view::<i32>();
        let first = result.value(0);
        let second = result.value(1);

        assert_eq!(result.value_offsets(), &[1, 0, 1]);
        assert_eq!(result.value_sizes(), &[2, 1, 1]);
        assert_eq!(
            get_column_as!(first.as_struct(), "value", Int32Array).values(),
            &[2, 3]
        );
        assert_eq!(
            get_column_as!(second.as_struct(), "value", Int32Array).values(),
            &[1]
        );
    }

    #[test]
    fn test_nested_sparse_list_view_compacts_backing_once() {
        let (source_inner_field, target_inner_field) = list_struct_fields();
        let inner = ListViewArray::new(
            source_inner_field,
            ScalarBuffer::from(vec![0i32, 1, 2, 3, 5]),
            ScalarBuffer::from(vec![1i32, 1, 1, 2, 1]),
            list_struct_values(vec!["bad", "1", "bad", "2", "3", "bad"]),
            None,
        );
        let source_outer_field = arc_field("item", inner.data_type().clone());
        let target_outer_field =
            arc_field("item", DataType::ListView(Arc::clone(&target_inner_field)));
        let source_col: ArrayRef = Arc::new(ListViewArray::new(
            source_outer_field,
            ScalarBuffer::from(vec![1i32, 3]),
            ScalarBuffer::from(vec![1i32, 1]),
            Arc::new(inner),
            None,
        ));
        let target_type = DataType::ListView(target_outer_field);

        let result =
            cast_column(&source_col, &target_type, &DEFAULT_CAST_OPTIONS).unwrap();
        let outer = result.as_list_view::<i32>();
        let inner = outer.values().as_list_view::<i32>();

        assert_eq!(outer.value_offsets(), &[0, 1]);
        assert_eq!(outer.value_sizes(), &[1, 1]);
        assert_eq!(inner.len(), 2);
        assert_eq!(inner.value_offsets(), &[0, 1]);
        assert_eq!(inner.value_sizes(), &[1, 2]);
        assert_eq!(inner.values().len(), 3);

        let first = outer.value(0);
        let first = first.as_list_view::<i32>().value(0);
        assert_eq!(
            get_column_as!(first.as_struct(), "value", Int32Array).values(),
            &[1]
        );
        let second = outer.value(1);
        let second = second.as_list_view::<i32>().value(0);
        assert_eq!(
            get_column_as!(second.as_struct(), "value", Int32Array).values(),
            &[2, 3]
        );
    }

    #[test]
    fn test_sliced_large_list_view_ignores_unreachable_invalid_nested_value() {
        let (source_field, target_field) = list_struct_fields();
        let source_col: ArrayRef = Arc::new(
            GenericListViewArray::<i64>::new(
                source_field,
                ScalarBuffer::from(vec![0i64, 1]),
                ScalarBuffer::from(vec![1i64, 1]),
                list_struct_values(vec!["bad", "2"]),
                None,
            )
            .slice(1, 1),
        );
        let target_type = DataType::LargeListView(target_field);

        let result =
            cast_column(&source_col, &target_type, &DEFAULT_CAST_OPTIONS).unwrap();
        let result = result.as_list_view::<i64>();
        let values = result.value(0);

        assert_eq!(result.data_type(), &target_type);
        assert_eq!(result.value_offsets(), &[0]);
        assert_eq!(result.value_sizes(), &[1]);
        assert_eq!(
            get_column_as!(values.as_struct(), "value", Int32Array).value(0),
            2
        );
    }

    #[test]
    fn test_large_list_view_visible_invalid_nested_value_returns_error() {
        let (source_field, target_field) = list_struct_fields();
        let source_col: ArrayRef = Arc::new(GenericListViewArray::<i64>::new(
            source_field,
            ScalarBuffer::from(vec![0i64]),
            ScalarBuffer::from(vec![1i64]),
            list_struct_values(vec!["bad"]),
            None,
        ));
        let target_type = DataType::LargeListView(target_field);

        assert!(cast_column(&source_col, &target_type, &DEFAULT_CAST_OPTIONS).is_err());
    }

    fn fixed_size_list_struct_field(fields: Vec<(&str, DataType)>) -> FieldRef {
        arc_field(
            "item",
            struct_type(
                fields
                    .into_iter()
                    .map(|(name, data_type)| field(name, data_type))
                    .collect(),
            ),
        )
    }

    fn create_fixed_size_list_test_fields(
        source_struct_fields: Vec<(&str, DataType)>,
        target_struct_fields: Vec<(&str, DataType)>,
    ) -> (FieldRef, FieldRef) {
        (
            fixed_size_list_struct_field(source_struct_fields),
            fixed_size_list_struct_field(target_struct_fields),
        )
    }

    fn fixed_size_list_struct_values(
        array: &ArrayRef,
    ) -> (&FixedSizeListArray, &StructArray) {
        let list = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        let values = list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        (list, values)
    }

    #[test]
    fn test_cast_fixed_size_list_struct() {
        let struct_arr = StructArray::from(vec![(
            arc_field("a", DataType::Int32),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
        )]);

        let (source_field, target_field) = create_fixed_size_list_test_fields(
            vec![("a", DataType::Int32)],
            vec![("a", DataType::Int64), ("b", DataType::Utf8)],
        );
        let source_col: ArrayRef = Arc::new(FixedSizeListArray::new(
            source_field,
            2,
            Arc::new(struct_arr),
            Some(NullBuffer::from(vec![true, false])),
        ));
        let target_type = DataType::FixedSizeList(target_field, 2);

        let result =
            cast_column(&source_col, &target_type, &DEFAULT_CAST_OPTIONS).unwrap();
        let (result_list, struct_values) = fixed_size_list_struct_values(&result);
        assert_eq!(result_list.len(), 2);
        assert!(result_list.is_valid(0));
        assert!(result_list.is_null(1));
        let a_col = get_column_as!(&struct_values, "a", Int64Array);
        assert_eq!(a_col.values(), &[1, 2, 3, 4]);
        let b_col = get_column_as!(&struct_values, "b", StringArray);
        assert!(b_col.iter().all(|v| v.is_none()));
    }

    #[test]
    fn test_validate_fixed_size_list_struct_compatibility() {
        let (source_field, target_field) = create_fixed_size_list_test_fields(
            vec![("a", DataType::Int32)],
            vec![("a", DataType::Int64), ("b", DataType::Utf8)],
        );
        let source = DataType::FixedSizeList(source_field, 2);
        let target = DataType::FixedSizeList(target_field, 2);

        assert!(requires_nested_struct_cast(&source, &target));
        assert!(validate_data_type_compatibility("col", &source, &target).is_ok());
    }

    #[test]
    fn test_validate_fixed_size_list_struct_missing_non_nullable_field_rejected() {
        let (source_field, _) = create_fixed_size_list_test_fields(
            vec![("a", DataType::Int32)],
            vec![("a", DataType::Int64), ("b", DataType::Utf8)],
        );
        let source = DataType::FixedSizeList(source_field, 2);
        let target = DataType::FixedSizeList(
            arc_field(
                "item",
                struct_type(vec![
                    field("a", DataType::Int32),
                    non_null_field("b", DataType::Utf8),
                ]),
            ),
            2,
        );

        let error = validate_data_type_compatibility("col", &source, &target)
            .unwrap_err()
            .to_string();
        assert_contains!(
            error,
            "target field 'b' is non-nullable but missing from source"
        );
    }

    #[test]
    fn test_fixed_size_list_struct_size_mismatch_rejected() {
        let source_field = fixed_size_list_struct_field(vec![("a", DataType::Int32)]);
        let target_field = Arc::clone(&source_field);
        let source_type = DataType::FixedSizeList(Arc::clone(&source_field), 2);
        let target_type = DataType::FixedSizeList(target_field, 3);

        let validation_error =
            validate_data_type_compatibility("col", &source_type, &target_type)
                .unwrap_err()
                .to_string();
        assert_contains!(validation_error, "Cannot cast struct field 'col'");

        let struct_arr = StructArray::from(vec![(
            arc_field("a", DataType::Int32),
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
        )]);
        let source_col: ArrayRef = Arc::new(FixedSizeListArray::new(
            source_field,
            2,
            Arc::new(struct_arr),
            None,
        ));

        let runtime_error = cast_column(&source_col, &target_type, &DEFAULT_CAST_OPTIONS)
            .unwrap_err()
            .to_string();
        assert_contains!(
            runtime_error,
            "cannot cast fixed-size-list to fixed-size-list with different size"
        );
    }

    #[test]
    fn test_cast_fixed_size_list_struct_all_null() {
        let (source_field, target_field) = create_fixed_size_list_test_fields(
            vec![("a", DataType::Int32)],
            vec![("a", DataType::Int64), ("b", DataType::Utf8)],
        );
        let source_col: ArrayRef =
            Arc::new(FixedSizeListArray::new_null(source_field, 2, 2));
        let target_type = DataType::FixedSizeList(target_field, 2);

        let result =
            cast_column(&source_col, &target_type, &DEFAULT_CAST_OPTIONS).unwrap();
        let (result_list, struct_values) = fixed_size_list_struct_values(&result);
        assert_eq!(result_list.null_count(), 2);
        let a_col = get_column_as!(&struct_values, "a", Int64Array);
        let b_col = get_column_as!(&struct_values, "b", StringArray);
        assert!(a_col.iter().all(|v| v.is_none()));
        assert!(b_col.iter().all(|v| v.is_none()));
    }

    #[test]
    fn test_fixed_size_list_struct_planner_runtime_parity_on_incompatible_type() {
        let source_field =
            arc_field("item", struct_type(vec![field("a", DataType::Binary)]));
        let target_field =
            arc_field("item", struct_type(vec![field("a", DataType::Int32)]));
        let source_type = DataType::FixedSizeList(Arc::clone(&source_field), 2);
        let target_type = DataType::FixedSizeList(target_field, 2);
        let validation_error =
            validate_data_type_compatibility("col", &source_type, &target_type)
                .unwrap_err()
                .to_string();
        assert_contains!(validation_error, "Cannot cast struct field 'a'");

        let struct_arr = StructArray::from(vec![(
            arc_field("a", DataType::Binary),
            Arc::new(BinaryArray::from(vec![
                Some(b"x".as_ref()),
                Some(b"y".as_ref()),
            ])) as ArrayRef,
        )]);
        let source_col: ArrayRef = Arc::new(FixedSizeListArray::new(
            source_field,
            2,
            Arc::new(struct_arr),
            None,
        ));

        let runtime_error = cast_column(&source_col, &target_type, &DEFAULT_CAST_OPTIONS)
            .unwrap_err()
            .to_string();
        assert_contains!(runtime_error, "Cannot cast struct field 'a'");
    }

    #[test]
    fn test_cast_fixed_size_list_struct_missing_non_nullable_field_runtime_rejected() {
        let source_field =
            arc_field("item", struct_type(vec![field("a", DataType::Int32)]));
        let target_field = arc_field(
            "item",
            struct_type(vec![
                field("a", DataType::Int32),
                non_null_field("b", DataType::Utf8),
            ]),
        );
        let source_col: ArrayRef =
            Arc::new(FixedSizeListArray::new_null(source_field, 2, 1));
        let target_type = DataType::FixedSizeList(target_field, 2);

        let error = cast_column(&source_col, &target_type, &DEFAULT_CAST_OPTIONS)
            .unwrap_err()
            .to_string();
        assert_contains!(
            error,
            "target field 'b' is non-nullable but missing from source"
        );
    }

    #[test]
    fn test_cast_fixed_size_list_returns_error_for_non_nullable_child() {
        let source_field = Arc::new(Field::new("item", DataType::Int32, true));
        let target_field = Arc::new(Field::new("item", DataType::Int32, false));
        let source_col: ArrayRef = Arc::new(FixedSizeListArray::new(
            source_field,
            2,
            Arc::new(Int32Array::from(vec![None, Some(1)])),
            None,
        ));
        let target_type = DataType::FixedSizeList(target_field, 2);

        let error = cast_column(&source_col, &target_type, &DEFAULT_CAST_OPTIONS)
            .unwrap_err()
            .to_string();
        assert_contains!(error, "Found unmasked nulls for non-nullable");
    }

    #[test]
    fn test_cast_sliced_fixed_size_list_struct_ignores_hidden_child_values() {
        let source_field =
            arc_field("item", struct_type(vec![field("a", DataType::Utf8)]));
        let target_field =
            arc_field("item", struct_type(vec![field("a", DataType::Int32)]));
        let struct_arr = StructArray::from(vec![(
            arc_field("a", DataType::Utf8),
            Arc::new(StringArray::from(vec![
                "0", "0", "not_int", "also_bad", "1", "2",
            ])) as ArrayRef,
        )]);
        let source_col: ArrayRef = Arc::new(
            FixedSizeListArray::new(
                source_field,
                2,
                Arc::new(struct_arr),
                Some(NullBuffer::from(vec![true, false, true])),
            )
            .slice(1, 2),
        );
        let target_type = DataType::FixedSizeList(target_field, 2);

        let result =
            cast_column(&source_col, &target_type, &DEFAULT_CAST_OPTIONS).unwrap();
        let (result_list, struct_values) = fixed_size_list_struct_values(&result);
        assert!(result_list.is_null(0));
        assert!(result_list.is_valid(1));
        let a_col = get_column_as!(&struct_values, "a", Int32Array);
        assert!(a_col.is_null(0));
        assert!(a_col.is_null(1));
        assert_eq!(a_col.value(2), 1);
        assert_eq!(a_col.value(3), 2);
    }

    #[test]
    fn test_requires_nested_struct_cast() {
        let s1 = struct_type(vec![field("a", DataType::Int32)]);
        let s2 = struct_type(vec![field("a", DataType::Int64)]);

        assert!(requires_nested_struct_cast(&s1, &s2));
        assert!(requires_nested_struct_cast(
            &DataType::List(arc_field("item", s1.clone())),
            &DataType::List(arc_field("item", s2.clone())),
        ));
        assert!(requires_nested_struct_cast(
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(s1.clone())),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(s2.clone())),
        ));
        assert!(requires_nested_struct_cast(
            &map_type(s1.clone(), DataType::Utf8),
            &map_type(s2.clone(), DataType::Utf8),
        ));
        assert!(requires_nested_struct_cast(
            &map_type(DataType::Utf8, map_type(DataType::Utf8, s1.clone()),),
            &map_type(DataType::Utf8, map_type(DataType::Utf8, s2.clone())),
        ));
        assert!(requires_nested_struct_cast(
            &DataType::ListView(arc_field("item", s1.clone())),
            &DataType::ListView(arc_field("item", s2.clone())),
        ));
        assert!(requires_nested_struct_cast(
            &DataType::FixedSizeList(arc_field("item", s1), 2),
            &DataType::FixedSizeList(arc_field("item", s2), 2),
        ));

        // Non-struct types should return false.
        assert!(!requires_nested_struct_cast(
            &DataType::Int32,
            &DataType::Int64
        ));
        assert!(!requires_nested_struct_cast(
            &DataType::List(arc_field("item", DataType::Int32)),
            &DataType::List(arc_field("item", DataType::Int64)),
        ));
        assert!(!requires_nested_struct_cast(
            &map_type(DataType::Int64, DataType::Int64),
            &map_type(DataType::Int32, DataType::Int32),
        ));
        assert!(!requires_nested_struct_cast(
            &map_type(DataType::Utf8, map_type(DataType::Utf8, DataType::Int64),),
            &map_type(DataType::Utf8, map_type(DataType::Utf8, DataType::Int32),),
        ));
        assert!(!requires_nested_struct_cast(
            &DataType::FixedSizeList(arc_field("item", DataType::Int32), 2),
            &DataType::FixedSizeList(arc_field("item", DataType::Int64), 2),
        ));
    }
}

/// Adapts a `RecordBatch` to conform to `target_schema`, verifying that each target field
/// type contains the incoming column data type (as verified by [`arrow::datatypes::DataType::contains`])
/// and transforms the metadata/types of differing columns to match `target_schema`
/// without copying primitive buffer data.
///
/// If `batch` has an incompatible column count or incompatible column data types,
/// an error is returned.
pub fn adapt_batch_to_schema(
    batch: RecordBatch,
    target_schema: &SchemaRef,
) -> Result<RecordBatch> {
    if Arc::ptr_eq(batch.schema_ref(), target_schema)
        || batch.schema().as_ref() == target_schema.as_ref()
    {
        return Ok(batch);
    }

    if batch.num_columns() != target_schema.fields().len() {
        return _plan_err!(
            "Batch schema does not conform to expected schema (column count mismatch). Expected: {target_schema}, got: {}",
            batch.schema()
        );
    }

    let mut columns = Vec::with_capacity(batch.num_columns());
    let mut needs_column_adaptation = false;
    let cast_options = CastOptions::default();

    for (target_field, col) in target_schema.fields().iter().zip(batch.columns()) {
        if target_field.data_type() != col.data_type() {
            // If data types differ, verify that target_field's data type contains
            // the column's data type (e.g. stricter nested struct / list field nullability).
            if !target_field.data_type().contains(col.data_type()) {
                return _plan_err!(
                    "Batch column '{}' with type {} cannot be adapted to expected type {}",
                    target_field.name(),
                    col.data_type(),
                    target_field.data_type()
                );
            }
            needs_column_adaptation = true;
            let adapted_col = cast_column(col, target_field.data_type(), &cast_options)?;
            columns.push(adapted_col);
        } else {
            columns.push(Arc::clone(col));
        }
    }

    if needs_column_adaptation {
        Ok(RecordBatch::try_new(Arc::clone(target_schema), columns)?)
    } else {
        // Schema differs only in top-level metadata or field nullability, while
        // column data types match exactly. Replace the schema on the batch.
        Ok(RecordBatch::try_new(
            Arc::clone(target_schema),
            batch.columns().to_vec(),
        )?)
    }
}

#[cfg(test)]
mod adapt_schema_tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{Field, Fields, Schema};

    #[test]
    fn test_adapt_batch_to_schema_identical() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]));

        let a = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let b = Arc::new(StringArray::from(vec![Some("x"), None, Some("z")])) as ArrayRef;
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![a, b])?;

        let adapted = adapt_batch_to_schema(batch.clone(), &schema)?;
        assert_eq!(adapted, batch);
        Ok(())
    }

    #[test]
    fn test_adapt_batch_to_schema_stricter_nested_struct() -> Result<()> {
        // Declared table schema: {a: Struct({x: Int32 (nullable), y: Utf8 (nullable)})}
        let declared_inner_fields = Fields::from(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("y", DataType::Utf8, true),
        ]);
        let declared_schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            Struct(declared_inner_fields),
            false,
        )]));

        // Runtime batch schema: {a: Struct({x: Int32 (NON-nullable), y: Utf8 (NON-nullable)})}
        let runtime_inner_fields = Fields::from(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Utf8, false),
        ]);
        let runtime_schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            Struct(runtime_inner_fields.clone()),
            false,
        )]));

        let x = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let y = Arc::new(StringArray::from(vec!["x", "y", "z"])) as ArrayRef;
        let struct_array =
            Arc::new(StructArray::new(runtime_inner_fields, vec![x, y], None))
                as ArrayRef;
        let batch = RecordBatch::try_new(runtime_schema, vec![struct_array])?;

        let adapted = adapt_batch_to_schema(batch, &declared_schema)?;
        assert_eq!(adapted.schema().as_ref(), declared_schema.as_ref());
        assert_eq!(adapted.num_rows(), 3);

        // Verify nested fields now have the declared nullability
        let Struct(fields) = adapted.column(0).data_type() else {
            panic!("expected struct");
        };
        assert!(fields[0].is_nullable());
        assert!(fields[1].is_nullable());
        Ok(())
    }

    #[test]
    fn test_adapt_batch_to_schema_top_level_nullability_only() -> Result<()> {
        // Declared schema has nullable column 'a', runtime batch has non-nullable 'a'
        let declared_schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let runtime_schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));

        let a = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let batch = RecordBatch::try_new(runtime_schema, vec![a])?;

        let adapted = adapt_batch_to_schema(batch, &declared_schema)?;
        assert_eq!(adapted.schema().as_ref(), declared_schema.as_ref());
        assert!(adapted.schema().field(0).is_nullable());
        Ok(())
    }

    #[test]
    fn test_adapt_batch_to_schema_null_into_non_nullable_rejected() {
        // Declared schema is non-nullable, but runtime batch is nullable
        let declared_schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let runtime_schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));

        let a = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef;
        let batch = RecordBatch::try_new(runtime_schema, vec![a]).unwrap();

        // Must reject because nullable is not contained by non-nullable
        let result = adapt_batch_to_schema(batch, &declared_schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_adapt_batch_to_schema_incompatible_type_rejected() {
        let declared_schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let runtime_schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));

        let a = Arc::new(StringArray::from(vec!["1", "2"])) as ArrayRef;
        let batch = RecordBatch::try_new(runtime_schema, vec![a]).unwrap();

        let result = adapt_batch_to_schema(batch, &declared_schema);
        assert!(result.is_err());
    }

    fn test_two_field_union(nullable: bool) -> UnionFields {
        UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("value", DataType::Int32, nullable),
                Field::new("str", DataType::Utf8, nullable),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_adapt_batch_to_schema_stricter_sparse_union() -> Result<()> {
        use arrow::array::UnionArray;
        use arrow::buffer::ScalarBuffer;
        use arrow::datatypes::UnionMode;

        let target_union_fields = test_two_field_union(true);
        let declared_schema = Arc::new(Schema::new(vec![Field::new(
            "u",
            DataType::Union(target_union_fields, UnionMode::Sparse),
            false,
        )]));

        let source_union_fields = test_two_field_union(false);
        let runtime_schema = Arc::new(Schema::new(vec![Field::new(
            "u",
            DataType::Union(source_union_fields.clone(), UnionMode::Sparse),
            false,
        )]));

        let int_array: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30]));
        let str_array: ArrayRef =
            Arc::new(StringArray::from(vec!["hello", "world", "!"]));
        let type_ids = [0, 0, 1].into_iter().collect::<ScalarBuffer<i8>>();
        let source_union = UnionArray::try_new(
            source_union_fields,
            type_ids,
            None,
            vec![int_array, str_array],
        )?;
        let batch = RecordBatch::try_new(runtime_schema, vec![Arc::new(source_union)])?;

        let adapted = adapt_batch_to_schema(batch, &declared_schema)?;
        assert_eq!(adapted.schema().as_ref(), declared_schema.as_ref());

        let adapted_union = adapted
            .column(0)
            .as_any()
            .downcast_ref::<UnionArray>()
            .unwrap();
        let DataType::Union(fields, mode) = adapted_union.data_type() else {
            panic!("expected union");
        };
        assert_eq!(*mode, UnionMode::Sparse);
        assert!(fields.iter().all(|(_, f)| f.is_nullable()));

        Ok(())
    }

    #[test]
    fn test_adapt_batch_to_schema_stricter_dense_union() -> Result<()> {
        use arrow::array::UnionArray;
        use arrow::buffer::ScalarBuffer;
        use arrow::datatypes::UnionMode;

        let target_union_fields = test_two_field_union(true);
        let declared_schema = Arc::new(Schema::new(vec![Field::new(
            "u",
            DataType::Union(target_union_fields, UnionMode::Dense),
            false,
        )]));

        let source_union_fields = test_two_field_union(false);
        let runtime_schema = Arc::new(Schema::new(vec![Field::new(
            "u",
            DataType::Union(source_union_fields.clone(), UnionMode::Dense),
            false,
        )]));

        let int_array: ArrayRef = Arc::new(Int32Array::from(vec![10, 30]));
        let str_array: ArrayRef = Arc::new(StringArray::from(vec!["hello"]));
        let type_ids = [0, 1, 0].into_iter().collect::<ScalarBuffer<i8>>();
        let offsets = [0, 0, 1].into_iter().collect::<ScalarBuffer<i32>>();
        let source_union = UnionArray::try_new(
            source_union_fields,
            type_ids,
            Some(offsets),
            vec![int_array, str_array],
        )?;
        let batch = RecordBatch::try_new(runtime_schema, vec![Arc::new(source_union)])?;

        let adapted = adapt_batch_to_schema(batch, &declared_schema)?;
        assert_eq!(adapted.schema().as_ref(), declared_schema.as_ref());

        let adapted_union = adapted
            .column(0)
            .as_any()
            .downcast_ref::<UnionArray>()
            .unwrap();
        let DataType::Union(fields, mode) = adapted_union.data_type() else {
            panic!("expected union");
        };
        assert_eq!(*mode, UnionMode::Dense);
        assert!(fields.iter().all(|(_, f)| f.is_nullable()));

        Ok(())
    }

    #[test]
    fn test_adapt_batch_to_schema_union_reordered_and_non_contiguous_type_ids()
    -> Result<()> {
        use arrow::array::UnionArray;
        use arrow::buffer::ScalarBuffer;
        use arrow::datatypes::UnionMode;

        let target_union_fields = UnionFields::try_new(
            vec![3, 1],
            vec![
                Field::new("str", DataType::Utf8, true),
                Field::new("int", DataType::Int32, true),
            ],
        )?;
        let declared_schema = Arc::new(Schema::new(vec![Field::new(
            "u",
            DataType::Union(target_union_fields, UnionMode::Dense),
            false,
        )]));

        let source_union_fields = UnionFields::try_new(
            vec![1, 3],
            vec![
                Field::new("int", DataType::Int32, false),
                Field::new("str", DataType::Utf8, false),
            ],
        )?;
        let source_schema = Arc::new(Schema::new(vec![Field::new(
            "u",
            DataType::Union(source_union_fields.clone(), UnionMode::Dense),
            false,
        )]));

        let int_array: ArrayRef = Arc::new(Int32Array::from(vec![10, 30]));
        let str_array: ArrayRef = Arc::new(StringArray::from(vec!["b"]));
        let type_ids = [1, 3, 1].into_iter().collect::<ScalarBuffer<i8>>();
        let offsets = [0, 0, 1].into_iter().collect::<ScalarBuffer<i32>>();
        let source_union = UnionArray::try_new(
            source_union_fields,
            type_ids.clone(),
            Some(offsets.clone()),
            vec![int_array, str_array],
        )?;

        let source_batch =
            RecordBatch::try_new(source_schema, vec![Arc::new(source_union)])?;

        let adapted = adapt_batch_to_schema(source_batch, &declared_schema)?;
        assert_eq!(adapted.schema().as_ref(), declared_schema.as_ref());
        let adapted_union = adapted
            .column(0)
            .as_any()
            .downcast_ref::<UnionArray>()
            .unwrap();
        assert_eq!(
            adapted_union.data_type(),
            declared_schema.field(0).data_type()
        );
        assert_eq!(adapted_union.type_ids(), &type_ids);
        assert_eq!(adapted_union.offsets(), Some(&offsets));

        // Child 1 is int, Child 3 is str (accessed by type ID)
        let int_child = adapted_union
            .child(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let str_child = adapted_union
            .child(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        // Row 0: type_id 1 -> int value 10
        assert_eq!(adapted_union.type_id(0), 1);
        assert_eq!(int_child.value(adapted_union.value_offset(0)), 10);

        // Row 1: type_id 3 -> str value "b"
        assert_eq!(adapted_union.type_id(1), 3);
        assert_eq!(str_child.value(adapted_union.value_offset(1)), "b");

        // Row 2: type_id 1 -> int value 30
        assert_eq!(adapted_union.type_id(2), 1);
        assert_eq!(int_child.value(adapted_union.value_offset(2)), 30);

        Ok(())
    }

    #[test]
    fn test_adapt_batch_to_schema_union_nested_struct() -> Result<()> {
        use arrow::array::UnionArray;
        use arrow::buffer::ScalarBuffer;
        use arrow::datatypes::{UnionFields, UnionMode};

        let target_struct_fields = vec![Field::new("x", DataType::Int32, true)];
        let target_union_fields = UnionFields::try_new(
            vec![0],
            vec![Field::new("s", Struct(target_struct_fields.into()), true)],
        )?;
        let declared_schema = Arc::new(Schema::new(vec![Field::new(
            "u",
            DataType::Union(target_union_fields, UnionMode::Dense),
            false,
        )]));

        let source_struct_fields = vec![Field::new("x", DataType::Int32, false)];
        let source_union_fields = UnionFields::try_new(
            vec![0],
            vec![Field::new("s", Struct(source_struct_fields.into()), false)],
        )?;
        let source_schema = Arc::new(Schema::new(vec![Field::new(
            "u",
            DataType::Union(source_union_fields.clone(), UnionMode::Dense),
            false,
        )]));

        let struct_child: ArrayRef = Arc::new(StructArray::new(
            vec![Field::new("x", DataType::Int32, false)].into(),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
            None,
        ));
        let type_ids = [0, 0].into_iter().collect::<ScalarBuffer<i8>>();
        let offsets = [0, 1].into_iter().collect::<ScalarBuffer<i32>>();
        let source_union = UnionArray::try_new(
            source_union_fields,
            type_ids.clone(),
            Some(offsets.clone()),
            vec![struct_child],
        )?;

        let source_batch =
            RecordBatch::try_new(source_schema, vec![Arc::new(source_union)])?;

        let adapted = adapt_batch_to_schema(source_batch, &declared_schema)?;
        assert_eq!(adapted.schema().as_ref(), declared_schema.as_ref());
        let adapted_union = adapted
            .column(0)
            .as_any()
            .downcast_ref::<UnionArray>()
            .unwrap();
        let adapted_child = adapted_union.child(0);
        let struct_arr = adapted_child
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(struct_arr.fields()[0].is_nullable());
        Ok(())
    }

    #[test]
    fn test_adapt_batch_to_schema_union_incompatible_mode_rejected() {
        use arrow::array::UnionArray;
        use arrow::buffer::ScalarBuffer;
        use arrow::datatypes::UnionMode;

        let declared_schema = Arc::new(Schema::new(vec![Field::new(
            "u",
            DataType::Union(test_two_field_union(true), UnionMode::Dense),
            false,
        )]));
        let source_schema = Arc::new(Schema::new(vec![Field::new(
            "u",
            DataType::Union(test_two_field_union(false), UnionMode::Sparse),
            false,
        )]));

        let int_array: ArrayRef = Arc::new(Int32Array::from(vec![10, 20]));
        let str_array: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
        let type_ids = [0, 0].into_iter().collect::<ScalarBuffer<i8>>();
        let source_union = UnionArray::try_new(
            test_two_field_union(false),
            type_ids,
            None,
            vec![int_array, str_array],
        )
        .unwrap();

        let source_batch =
            RecordBatch::try_new(source_schema, vec![Arc::new(source_union)]).unwrap();

        let res = adapt_batch_to_schema(source_batch, &declared_schema);
        assert!(res.is_err());
    }

    #[test]
    fn test_adapt_batch_to_schema_union_field_set_mismatch_rejected() {
        use arrow::array::UnionArray;
        use arrow::buffer::ScalarBuffer;
        use arrow::datatypes::{UnionFields, UnionMode};

        // Target has type ID [0]
        let target_union_fields = UnionFields::try_new(
            vec![0],
            vec![Field::new("value", DataType::Int32, true)],
        )
        .unwrap();
        let declared_schema = Arc::new(Schema::new(vec![Field::new(
            "u",
            DataType::Union(target_union_fields, UnionMode::Sparse),
            false,
        )]));

        // Source has type IDs [0, 1] (where ID 0 is compatible)
        let source_union_fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("value", DataType::Int32, false),
                Field::new("extra", DataType::Utf8, false),
            ],
        )
        .unwrap();
        let source_schema = Arc::new(Schema::new(vec![Field::new(
            "u",
            DataType::Union(source_union_fields.clone(), UnionMode::Sparse),
            false,
        )]));

        let int_array: ArrayRef = Arc::new(Int32Array::from(vec![10, 20]));
        let str_array: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
        let type_ids = [0, 1].into_iter().collect::<ScalarBuffer<i8>>();
        let source_union = UnionArray::try_new(
            source_union_fields,
            type_ids,
            None,
            vec![int_array, str_array],
        )
        .unwrap();

        let source_batch =
            RecordBatch::try_new(source_schema, vec![Arc::new(source_union)]).unwrap();

        let res = adapt_batch_to_schema(source_batch, &declared_schema);
        assert!(res.is_err());
        let err = res.unwrap_err().to_string();
        assert!(
            err.contains("different field sets")
                || err.contains("cannot be adapted to expected type"),
            "unexpected error message: {err}"
        );
    }

    #[test]
    fn test_validate_data_type_compatibility_union() {
        use arrow::datatypes::{UnionFields, UnionMode};

        let target_type = DataType::Union(test_two_field_union(true), UnionMode::Dense);

        // Compatible: exact same type IDs in different order with stricter nullability
        let reordered_source_fields = UnionFields::try_new(
            vec![1, 0],
            vec![
                Field::new("str", DataType::Utf8, false),
                Field::new("value", DataType::Int32, false),
            ],
        )
        .unwrap();
        let source_type = DataType::Union(reordered_source_fields, UnionMode::Dense);
        assert!(
            validate_data_type_compatibility("u", &source_type, &target_type).is_ok()
        );

        // Incompatible: mismatched mode
        let sparse_source_type =
            DataType::Union(test_two_field_union(false), UnionMode::Sparse);
        assert!(
            validate_data_type_compatibility("u", &sparse_source_type, &target_type)
                .is_err()
        );

        // Incompatible: field-set mismatch (extra source ID 2)
        let extra_id_source = DataType::Union(
            UnionFields::try_new(
                vec![0, 1, 2],
                vec![
                    Field::new("value", DataType::Int32, false),
                    Field::new("str", DataType::Utf8, false),
                    Field::new("extra", DataType::Int32, false),
                ],
            )
            .unwrap(),
            UnionMode::Dense,
        );
        assert!(
            validate_data_type_compatibility("u", &extra_id_source, &target_type)
                .is_err()
        );

        // Incompatible: field-set mismatch (missing source ID 1)
        let missing_id_source = DataType::Union(
            UnionFields::try_new(
                vec![0],
                vec![Field::new("value", DataType::Int32, false)],
            )
            .unwrap(),
            UnionMode::Dense,
        );
        assert!(
            validate_data_type_compatibility("u", &missing_id_source, &target_type)
                .is_err()
        );
    }
}
