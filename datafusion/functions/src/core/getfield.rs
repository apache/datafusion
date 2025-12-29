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

use std::any::Any;
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Capacities, GenericListArray, Int64Array, MutableArrayData,
    OffsetSizeTrait, Scalar, make_array, make_comparator,
};
use arrow::buffer::NullBuffer;
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, FieldRef};

use datafusion_common::cast::{
    as_large_list_array, as_list_array, as_map_array, as_struct_array,
};
use datafusion_common::{
    Result, ScalarValue, exec_err, internal_err, plan_datafusion_err,
};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::ExprSimplifyResult;
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;

/// Format a field access argument for display.
/// - String literals: show as the string value (for struct field names)
/// - Integer literals: show as the numeric value (for array indices)
/// - Other: use schema_name()
fn format_field_access_arg(expr: &Expr) -> String {
    match expr {
        Expr::Literal(sv, _) => {
            // For integers, show as bare number (array index)
            if sv.data_type().is_integer() {
                return sv.to_string();
            }
            // For strings, show the string value
            if let Some(s) = sv.try_as_str().flatten() {
                return s.to_string();
            }
            sv.to_string()
        }
        other => other.schema_name().to_string(),
    }
}

#[user_doc(
    doc_section(label = "Other Functions"),
    description = r#"Returns a field within a map or a struct with the given key.
    Supports nested field access by providing multiple field names.
    Note: most users invoke `get_field` indirectly via field access
    syntax such as `my_struct_col['field_name']` which results in a call to
    `get_field(my_struct_col, 'field_name')`.
    Nested access like `my_struct['a']['b']` is optimized to a single call:
    `get_field(my_struct, 'a', 'b')`."#,
    syntax_example = "get_field(expression, field_name[, field_name2, ...])",
    sql_example = r#"```sql
> -- Access a field from a struct column
> create table test( struct_col) as values
    ({name: 'Alice', age: 30}),
    ({name: 'Bob', age: 25});
> select struct_col from test;
+-----------------------------+
| struct_col                  |
+-----------------------------+
| {name: Alice, age: 30}      |
| {name: Bob, age: 25}        |
+-----------------------------+
> select struct_col['name'] as name from test;
+-------+
| name  |
+-------+
| Alice |
| Bob   |
+-------+

> -- Nested field access with multiple arguments
> create table test(struct_col) as values
    ({outer: {inner_val: 42}});
> select struct_col['outer']['inner_val'] as result from test;
+--------+
| result |
+--------+
| 42     |
+--------+
```"#,
    argument(
        name = "expression",
        description = "The map or struct to retrieve a field from."
    ),
    argument(
        name = "field_name",
        description = "The field name(s) to access, in order for nested access. Must evaluate to strings."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct GetFieldFunc {
    signature: Signature,
}

impl Default for GetFieldFunc {
    fn default() -> Self {
        Self::new()
    }
}

/// Process a map array by finding matching keys and extracting corresponding values.
///
/// This function handles both simple (scalar) and nested key types by using
/// appropriate comparison strategies.
fn process_map_array(
    array: &dyn Array,
    key_array: Arc<dyn Array>,
) -> Result<ColumnarValue> {
    let map_array = as_map_array(array)?;
    let keys = if key_array.data_type().is_nested() {
        let comparator = make_comparator(
            map_array.keys().as_ref(),
            key_array.as_ref(),
            SortOptions::default(),
        )?;
        let len = map_array.keys().len().min(key_array.len());
        let values = (0..len).map(|i| comparator(i, i).is_eq()).collect();
        let nulls = NullBuffer::union(map_array.keys().nulls(), key_array.nulls());
        BooleanArray::new(values, nulls)
    } else {
        let be_compared = Scalar::new(key_array);
        arrow::compute::kernels::cmp::eq(&be_compared, map_array.keys())?
    };

    let original_data = map_array.entries().column(1).to_data();
    let capacity = Capacities::Array(original_data.len());
    let mut mutable =
        MutableArrayData::with_capacities(vec![&original_data], true, capacity);

    for entry in 0..map_array.len() {
        let start = map_array.value_offsets()[entry] as usize;
        let end = map_array.value_offsets()[entry + 1] as usize;

        let maybe_matched = keys
            .slice(start, end - start)
            .iter()
            .enumerate()
            .find(|(_, t)| t.unwrap());

        if maybe_matched.is_none() {
            mutable.extend_nulls(1);
            continue;
        }
        let (match_offset, _) = maybe_matched.unwrap();
        mutable.extend(0, start + match_offset, start + match_offset + 1);
    }

    let data = mutable.freeze();
    let data = make_array(data);
    Ok(ColumnarValue::Array(data))
}

/// Process a map array with a nested key type by iterating through entries
/// and using a comparator for key matching.
///
/// This specialized version is used when the key type is nested (e.g., struct, list).
fn process_map_with_nested_key(
    array: &dyn Array,
    key_array: &dyn Array,
) -> Result<ColumnarValue> {
    let map_array = as_map_array(array)?;

    let comparator =
        make_comparator(map_array.keys().as_ref(), key_array, SortOptions::default())?;

    let original_data = map_array.entries().column(1).to_data();
    let capacity = Capacities::Array(original_data.len());
    let mut mutable =
        MutableArrayData::with_capacities(vec![&original_data], true, capacity);

    for entry in 0..map_array.len() {
        let start = map_array.value_offsets()[entry] as usize;
        let end = map_array.value_offsets()[entry + 1] as usize;

        let mut found_match = false;
        for i in start..end {
            if comparator(i, 0).is_eq() {
                mutable.extend(0, i, i + 1);
                found_match = true;
                break;
            }
        }

        if !found_match {
            mutable.extend_nulls(1);
        }
    }

    let data = mutable.freeze();
    let data = make_array(data);
    Ok(ColumnarValue::Array(data))
}

/// Extract a single field from a struct or map array.
///
/// # Field Name Requirements
/// The field name must be a **scalar** value known at planning time. Dynamic field
/// names (array of field names) are not supported because:
/// - Struct field access requires compile-time type inference (different fields may have different types)
/// - The return type would be indeterminate if the field name varied per row
///
/// This is in contrast to array element access, where all elements share the same type,
/// allowing dynamic indices.
///
/// # Supported Types
/// - **Struct**: Field name must be a UTF-8 string
/// - **Map**: Key can be any scalar type matching the map's key type
fn extract_single_field(base: ColumnarValue, name: ScalarValue) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&[base])?;
    let array = Arc::clone(&arrays[0]);

    let string_value = name.try_as_str().flatten().map(|s| s.to_string());

    match (array.data_type(), name, string_value) {
        (DataType::Map(_, _), ScalarValue::List(arr), _) => {
            let key_array: Arc<dyn Array> = arr;
            process_map_array(&array, key_array)
        }
        (DataType::Map(_, _), ScalarValue::Struct(arr), _) => {
            process_map_array(&array, arr as Arc<dyn Array>)
        }
        (DataType::Map(_, _), other, _) => {
            let data_type = other.data_type();
            if data_type.is_nested() {
                process_map_with_nested_key(&array, &other.to_array()?)
            } else {
                process_map_array(&array, other.to_array()?)
            }
        }
        (DataType::Struct(_), _, Some(k)) => {
            let as_struct_array = as_struct_array(&array)?;
            match as_struct_array.column_by_name(&k) {
                None => exec_err!("Field {k} not found in struct"),
                Some(col) => Ok(ColumnarValue::Array(Arc::clone(col))),
            }
        }
        (DataType::Struct(_), name, _) => exec_err!(
            "get_field is only possible on struct with utf8 indexes. \
                         Received with {name:?} index"
        ),
        (DataType::Null, _, _) => Ok(ColumnarValue::Scalar(ScalarValue::Null)),
        (dt, name, _) => exec_err!(
            "get_field is only possible on maps or structs. Received {dt} with {name:?} index"
        ),
    }
}

/// Extract element at 1-indexed position from array.
///
/// # Index Semantics
/// - 1-indexed: index `1` returns the first element
/// - Negative indices count from end: `-1` is the last element
/// - Out-of-bounds access returns null
///
/// # Input Combinations and Output Cardinality
///
/// | Base       | Index      | Output      | Description                                    |
/// |------------|------------|-------------|------------------------------------------------|
/// | Array(N)   | Scalar     | Array(N)    | Same index applied to each list row            |
/// | Scalar     | Array(N)   | Array(N)    | Different indices into the same list           |
/// | Array(N)   | Array(N)   | Array(N)    | Zipped: row i uses base[i] and index[i]        |
/// | Array(N)   | Array(M)   | Error       | Length mismatch (N ≠ M)                        |
/// | Scalar     | Scalar     | Scalar      | Single element extraction                      |
///
/// # Performance
/// Scalar cases (Array/Scalar, Scalar/Array, Scalar/Scalar) are handled directly
/// without array expansion for efficiency, avoiding unnecessary allocations.
fn extract_array_element_columnar(
    base: ColumnarValue,
    index: ColumnarValue,
) -> Result<ColumnarValue> {
    match (base, index) {
        // Fast path: Array base with scalar index (most common case, e.g., arr[3])
        (ColumnarValue::Array(array), ColumnarValue::Scalar(index_scalar)) => {
            let index = scalar_to_i64(&index_scalar)?;
            extract_array_element_array_scalar(&array, index)
        }
        // Fast path: Scalar base with array indices (e.g., literal_list[idx_column])
        (ColumnarValue::Scalar(list_scalar), ColumnarValue::Array(indexes)) => {
            let indexes = cast_to_int64_array(&indexes)?;
            extract_array_element_scalar_array(&list_scalar, &indexes)
        }
        // Fast path: Both scalar (e.g., literal_list[3])
        (ColumnarValue::Scalar(list_scalar), ColumnarValue::Scalar(index_scalar)) => {
            let index = scalar_to_i64(&index_scalar)?;
            extract_array_element_scalar_scalar(&list_scalar, index)
        }
        // General path: Both arrays (zipped, must have equal lengths)
        (ColumnarValue::Array(array), ColumnarValue::Array(indexes)) => {
            let indexes = cast_to_int64_array(&indexes)?;
            if array.len() != indexes.len() {
                return exec_err!(
                    "Array element access requires equal length arrays, got {} and {}",
                    array.len(),
                    indexes.len()
                );
            }
            extract_array_element_array_array(&array, &indexes)
        }
    }
}

/// Convert a scalar value to i64 for use as an array index.
fn scalar_to_i64(scalar: &ScalarValue) -> Result<Option<i64>> {
    if scalar.is_null() {
        return Ok(None);
    }
    match scalar {
        ScalarValue::Int8(v) => Ok(v.map(|x| x as i64)),
        ScalarValue::Int16(v) => Ok(v.map(|x| x as i64)),
        ScalarValue::Int32(v) => Ok(v.map(|x| x as i64)),
        ScalarValue::Int64(v) => Ok(*v),
        ScalarValue::UInt8(v) => Ok(v.map(|x| x as i64)),
        ScalarValue::UInt16(v) => Ok(v.map(|x| x as i64)),
        ScalarValue::UInt32(v) => Ok(v.map(|x| x as i64)),
        ScalarValue::UInt64(v) => Ok(v.map(|x| x as i64)),
        other => exec_err!(
            "Array index must be an integer type, got {}",
            other.data_type()
        ),
    }
}

/// Cast an integer array to Int64Array for use as array indices.
fn cast_to_int64_array(array: &dyn Array) -> Result<Int64Array> {
    if array.data_type() == &DataType::Int64 {
        // Already Int64, just downcast
        return Ok(datafusion_common::cast::as_int64_array(array)?.clone());
    }

    // Cast to Int64
    let casted = arrow::compute::cast(array, &DataType::Int64).map_err(|e| {
        datafusion_common::DataFusionError::Internal(format!(
            "Failed to cast index array to Int64: {e}"
        ))
    })?;

    Ok(datafusion_common::cast::as_int64_array(&casted)?.clone())
}

/// Extract elements from an array column using a scalar index.
///
/// For each row in the list array, extracts the element at the given index.
/// Returns an array with the same length as the input list array.
fn extract_array_element_array_scalar(
    array: &Arc<dyn Array>,
    index: Option<i64>,
) -> Result<ColumnarValue> {
    // Null index returns all nulls
    let Some(index) = index else {
        let element_type = match array.data_type() {
            DataType::List(f) | DataType::LargeList(f) => f.data_type().clone(),
            DataType::Null => return Ok(ColumnarValue::Scalar(ScalarValue::Null)),
            dt => {
                return exec_err!(
                    "get_field array element access does not support type {dt}"
                );
            }
        };
        return Ok(ColumnarValue::Array(Arc::new(
            arrow::array::new_null_array(&element_type, array.len()),
        )));
    };

    match array.data_type() {
        DataType::List(_) => {
            let list_array = as_list_array(array)?;
            general_array_element_scalar::<i32>(list_array, index)
        }
        DataType::LargeList(_) => {
            let list_array = as_large_list_array(array)?;
            general_array_element_scalar::<i64>(list_array, index)
        }
        DataType::Null => Ok(ColumnarValue::Scalar(ScalarValue::Null)),
        dt => exec_err!("get_field array element access does not support type {dt}"),
    }
}

/// Extract elements from a scalar list using an array of indices.
///
/// The same list is indexed by each value in the indices array.
/// Returns an array with the same length as the indices array.
fn extract_array_element_scalar_array(
    list_scalar: &ScalarValue,
    indexes: &Int64Array,
) -> Result<ColumnarValue> {
    // Convert scalar to single-element list array, then use optimized path
    let list_array = list_scalar.to_array()?;

    match list_array.data_type() {
        DataType::List(_) => {
            let list_array = as_list_array(&list_array)?;
            general_array_element_broadcast_list::<i32>(list_array, indexes)
        }
        DataType::LargeList(_) => {
            let list_array = as_large_list_array(&list_array)?;
            general_array_element_broadcast_list::<i64>(list_array, indexes)
        }
        DataType::Null => Ok(ColumnarValue::Array(Arc::new(
            arrow::array::new_null_array(&DataType::Null, indexes.len()),
        ))),
        dt => exec_err!("get_field array element access does not support type {dt}"),
    }
}

/// Extract a single element from a scalar list using a scalar index.
///
/// Returns a scalar value.
fn extract_array_element_scalar_scalar(
    list_scalar: &ScalarValue,
    index: Option<i64>,
) -> Result<ColumnarValue> {
    // Null index returns null
    let Some(index) = index else {
        let element_type = match list_scalar {
            ScalarValue::List(arr) => arr.value_type(),
            ScalarValue::LargeList(arr) => arr.value_type(),
            ScalarValue::Null => return Ok(ColumnarValue::Scalar(ScalarValue::Null)),
            other => {
                return exec_err!(
                    "get_field array element access does not support type {}",
                    other.data_type()
                );
            }
        };
        return Ok(ColumnarValue::Scalar(ScalarValue::try_from(&element_type)?));
    };

    // Extract the list's values and compute the element
    let (values, len) = match list_scalar {
        ScalarValue::List(arr) => {
            let list = arr
                .as_any()
                .downcast_ref::<arrow::array::ListArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(
                        "Failed to downcast to ListArray".to_string(),
                    )
                })?;
            if list.is_empty() || list.is_null(0) {
                return Ok(ColumnarValue::Scalar(ScalarValue::try_from(
                    list.value_type(),
                )?));
            }
            let values = list.value(0);
            let len = values.len() as i64;
            (values, len)
        }
        ScalarValue::LargeList(arr) => {
            let list = arr
                .as_any()
                .downcast_ref::<arrow::array::LargeListArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(
                        "Failed to downcast to LargeListArray".to_string(),
                    )
                })?;
            if list.is_empty() || list.is_null(0) {
                return Ok(ColumnarValue::Scalar(ScalarValue::try_from(
                    list.value_type(),
                )?));
            }
            let values = list.value(0);
            let len = values.len() as i64;
            (values, len)
        }
        ScalarValue::Null => return Ok(ColumnarValue::Scalar(ScalarValue::Null)),
        other => {
            return exec_err!(
                "get_field array element access does not support type {}",
                other.data_type()
            );
        }
    };

    // Adjust index (1-indexed, negative from end)
    let adjusted = if index < 0 { index + len } else { index - 1 };

    if adjusted >= 0 && adjusted < len {
        let scalar = ScalarValue::try_from_array(&values, adjusted as usize)?;
        Ok(ColumnarValue::Scalar(scalar))
    } else {
        // Out of bounds
        Ok(ColumnarValue::Scalar(ScalarValue::try_from(
            values.data_type(),
        )?))
    }
}

/// Extract elements from array columns with array indices (zipped).
///
/// Row i of the output is extracted from row i of the list array using
/// the index at row i of the indices array. Arrays must have equal length.
fn extract_array_element_array_array(
    array: &Arc<dyn Array>,
    indexes: &Int64Array,
) -> Result<ColumnarValue> {
    match array.data_type() {
        DataType::List(_) => {
            let list_array = as_list_array(array)?;
            general_array_element_array::<i32>(list_array, indexes)
        }
        DataType::LargeList(_) => {
            let list_array = as_large_list_array(array)?;
            general_array_element_array::<i64>(list_array, indexes)
        }
        DataType::Null => Ok(ColumnarValue::Scalar(ScalarValue::Null)),
        dt => exec_err!("get_field array element access does not support type {dt}"),
    }
}

/// Generic array element extraction for List/LargeList types with array indices (Array/Array case).
///
/// Extracts elements by zipping the list array with the index array: row i of the output
/// is the element at `indexes[i]` from `array[i]`. Both arrays must have the same length.
fn general_array_element_array<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    indexes: &Int64Array,
) -> Result<ColumnarValue>
where
    i64: TryInto<O>,
{
    let values = array.values();
    if values.data_type().is_null() {
        return Ok(ColumnarValue::Array(Arc::new(
            arrow::array::NullArray::new(array.len()),
        )));
    }

    let original_data = values.to_data();
    let capacity = Capacities::Array(original_data.len());
    let mut mutable =
        MutableArrayData::with_capacities(vec![&original_data], true, capacity);

    for (row_index, offset_window) in array.offsets().windows(2).enumerate() {
        let start = offset_window[0];
        let end = offset_window[1];
        let len = end - start;

        // Empty array or null index returns null
        if len == O::usize_as(0) || indexes.is_null(row_index) {
            mutable.extend_nulls(1);
            continue;
        }

        let index = indexes.value(row_index);

        // Adjust index: 1-indexed, negative counts from end
        let adjusted_index = if index < 0 {
            let idx: O = match index.try_into() {
                Ok(v) => v,
                Err(_) => {
                    mutable.extend_nulls(1);
                    continue;
                }
            };
            idx + len
        } else {
            let idx: O = match index.try_into() {
                Ok(v) => v,
                Err(_) => {
                    mutable.extend_nulls(1);
                    continue;
                }
            };
            idx - O::usize_as(1)
        };

        if adjusted_index >= O::usize_as(0) && adjusted_index < len {
            let abs_index = start.as_usize() + adjusted_index.as_usize();
            mutable.extend(0, abs_index, abs_index + 1);
        } else {
            // Out of bounds
            mutable.extend_nulls(1);
        }
    }

    let data = mutable.freeze();
    Ok(ColumnarValue::Array(make_array(data)))
}

/// Generic array element extraction with a scalar index (Array/Scalar case).
///
/// Extracts the element at the given index from each row of the list array.
/// This is the most common case (e.g., `arr[3]`) and avoids creating an
/// index array filled with the same value.
fn general_array_element_scalar<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    index: i64,
) -> Result<ColumnarValue>
where
    i64: TryInto<O>,
{
    let values = array.values();
    if values.data_type().is_null() {
        return Ok(ColumnarValue::Array(Arc::new(
            arrow::array::NullArray::new(array.len()),
        )));
    }

    let original_data = values.to_data();
    let capacity = Capacities::Array(array.len());
    let mut mutable =
        MutableArrayData::with_capacities(vec![&original_data], true, capacity);

    for offset_window in array.offsets().windows(2) {
        let start = offset_window[0];
        let end = offset_window[1];
        let len = end - start;

        // Empty array returns null
        if len == O::usize_as(0) {
            mutable.extend_nulls(1);
            continue;
        }

        // Adjust index: 1-indexed, negative counts from end
        let adjusted_index = if index < 0 {
            let idx: O = match index.try_into() {
                Ok(v) => v,
                Err(_) => {
                    mutable.extend_nulls(1);
                    continue;
                }
            };
            idx + len
        } else {
            let idx: O = match index.try_into() {
                Ok(v) => v,
                Err(_) => {
                    mutable.extend_nulls(1);
                    continue;
                }
            };
            idx - O::usize_as(1)
        };

        if adjusted_index >= O::usize_as(0) && adjusted_index < len {
            let abs_index = start.as_usize() + adjusted_index.as_usize();
            mutable.extend(0, abs_index, abs_index + 1);
        } else {
            // Out of bounds
            mutable.extend_nulls(1);
        }
    }

    let data = mutable.freeze();
    Ok(ColumnarValue::Array(make_array(data)))
}

/// Generic array element extraction broadcasting a single list to multiple indices (Scalar/Array case).
///
/// The single list (row 0 of the array) is indexed by each value in the indices array.
/// Returns an array with the same length as the indices array.
fn general_array_element_broadcast_list<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    indexes: &Int64Array,
) -> Result<ColumnarValue>
where
    i64: TryInto<O>,
{
    // The array should have exactly one row (the scalar list)
    if array.is_empty() || array.is_null(0) {
        return Ok(ColumnarValue::Array(Arc::new(
            arrow::array::new_null_array(&array.value_type(), indexes.len()),
        )));
    }

    let values = array.values();
    if values.data_type().is_null() {
        return Ok(ColumnarValue::Array(Arc::new(
            arrow::array::NullArray::new(indexes.len()),
        )));
    }

    // Get the single list's bounds
    let start = array.offsets()[0];
    let end = array.offsets()[1];
    let len = end - start;

    let original_data = values.to_data();
    let capacity = Capacities::Array(indexes.len());
    let mut mutable =
        MutableArrayData::with_capacities(vec![&original_data], true, capacity);

    for i in 0..indexes.len() {
        if indexes.is_null(i) {
            mutable.extend_nulls(1);
            continue;
        }

        let index = indexes.value(i);

        // Empty list returns null
        if len == O::usize_as(0) {
            mutable.extend_nulls(1);
            continue;
        }

        // Adjust index: 1-indexed, negative counts from end
        let adjusted_index = if index < 0 {
            let idx: O = match index.try_into() {
                Ok(v) => v,
                Err(_) => {
                    mutable.extend_nulls(1);
                    continue;
                }
            };
            idx + len
        } else {
            let idx: O = match index.try_into() {
                Ok(v) => v,
                Err(_) => {
                    mutable.extend_nulls(1);
                    continue;
                }
            };
            idx - O::usize_as(1)
        };

        if adjusted_index >= O::usize_as(0) && adjusted_index < len {
            let abs_index = start.as_usize() + adjusted_index.as_usize();
            mutable.extend(0, abs_index, abs_index + 1);
        } else {
            // Out of bounds
            mutable.extend_nulls(1);
        }
    }

    let data = mutable.freeze();
    Ok(ColumnarValue::Array(make_array(data)))
}

impl GetFieldFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

// get_field(struct_array, field_name)
impl ScalarUDFImpl for GetFieldFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "get_field"
    }

    fn display_name(&self, args: &[Expr]) -> Result<String> {
        if args.len() < 2 {
            return exec_err!(
                "get_field requires at least 2 arguments, got {}",
                args.len()
            );
        }

        let base = &args[0];
        let field_names: Vec<String> =
            args[1..].iter().map(format_field_access_arg).collect();

        Ok(format!("{}[{}]", base, field_names.join("][")))
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        if args.len() < 2 {
            return exec_err!(
                "get_field requires at least 2 arguments, got {}",
                args.len()
            );
        }

        let base = &args[0];
        let field_names: Vec<String> =
            args[1..].iter().map(format_field_access_arg).collect();

        Ok(format!(
            "{}[{}]",
            base.schema_name(),
            field_names.join("][")
        ))
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        // Validate minimum 2 arguments: base expression + at least one field name
        if args.scalar_arguments.len() < 2 {
            return exec_err!(
                "get_field requires at least 2 arguments, got {}",
                args.scalar_arguments.len()
            );
        }

        let mut current_field = Arc::clone(&args.arg_fields[0]);

        // Iterate through each field name (starting from index 1)
        for (i, sv) in args.scalar_arguments.iter().enumerate().skip(1) {
            match current_field.data_type() {
                DataType::Map(map_field, _) => {
                    match map_field.data_type() {
                        DataType::Struct(fields) if fields.len() == 2 => {
                            // Arrow's MapArray is essentially a ListArray of structs with two columns. They are
                            // often named "key", and "value", but we don't require any specific naming here;
                            // instead, we assume that the second column is the "value" column both here and in
                            // execution.
                            let value_field = fields
                                .get(1)
                                .expect("fields should have exactly two members");

                            current_field = Arc::new(
                                value_field.as_ref().clone().with_nullable(true),
                            );
                        }
                        _ => {
                            return exec_err!(
                                "Map fields must contain a Struct with exactly 2 fields"
                            );
                        }
                    }
                }
                DataType::Struct(fields) => {
                    let field_name = sv
                        .as_ref()
                        .and_then(|sv| {
                            sv.try_as_str().flatten().filter(|s| !s.is_empty())
                        })
                        .ok_or_else(|| {
                            datafusion_common::DataFusionError::Execution(
                                "Field name must be a non-empty string".to_string(),
                            )
                        })?;

                    let child_field = fields
                        .iter()
                        .find(|f| f.name() == field_name)
                        .ok_or_else(|| {
                            plan_datafusion_err!("Field {field_name} not found in struct")
                        })?;

                    let mut new_field = child_field.as_ref().clone();

                    // If the parent is nullable, then getting the child must be nullable
                    if current_field.is_nullable() {
                        new_field = new_field.with_nullable(true);
                    }
                    current_field = Arc::new(new_field);
                }
                DataType::Null => {
                    return Ok(Field::new(self.name(), DataType::Null, true).into());
                }
                // Array types: List, LargeList, ListView, LargeListView, FixedSizeList
                DataType::List(element_field)
                | DataType::LargeList(element_field)
                | DataType::ListView(element_field)
                | DataType::LargeListView(element_field)
                | DataType::FixedSizeList(element_field, _) => {
                    // Check argument type to determine access mode
                    // Use scalar value type if available, otherwise fall back to field type
                    let arg_type = sv
                        .as_ref()
                        .map(|s| s.data_type())
                        .unwrap_or_else(|| args.arg_fields[i].data_type().clone());

                    if arg_type.is_integer() {
                        // Array element access - return element type (nullable for out-of-bounds)
                        current_field =
                            Arc::new(element_field.as_ref().clone().with_nullable(true));
                    } else {
                        return exec_err!(
                            "Array access at argument {} requires integer index, got {}",
                            i,
                            arg_type
                        );
                    }
                }
                other => {
                    return exec_err!(
                        "Cannot access field at argument {}: type {} is not Struct, Map, List, or Null",
                        i,
                        other
                    );
                }
            }
        }

        Ok(current_field)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() < 2 {
            return exec_err!(
                "get_field requires at least 2 arguments, got {}",
                args.args.len()
            );
        }

        let mut current = args.args[0].clone();

        // Early exit for null base
        if current.data_type().is_null() {
            return Ok(ColumnarValue::Scalar(ScalarValue::Null));
        }

        // Iterate through each field name/index
        for field_arg in args.args.iter().skip(1) {
            // Dispatch based on current type and argument
            let current_type = current.data_type();
            let arg_type = field_arg.data_type();

            current = match &current_type {
                // Array types: dispatch to array element or slice extraction
                //
                // Array indices can be scalar or array (for expressions like arr[i + 1]).
                // Dynamic indices are supported because all array elements share the same
                // type, so the return type is known regardless of which index is used.
                DataType::List(_)
                | DataType::LargeList(_)
                | DataType::ListView(_)
                | DataType::LargeListView(_)
                | DataType::FixedSizeList(_, _) => {
                    if arg_type.is_integer() {
                        extract_array_element_columnar(current, field_arg.clone())?
                    } else {
                        return exec_err!(
                            "Array access requires integer index, got {}",
                            arg_type
                        );
                    }
                }
                // Struct/Map types: use existing field extraction (requires scalar field name)
                //
                // Unlike array element access which supports dynamic indices (because all
                // elements have the same type), struct/map field access requires the field
                // name to be a scalar known at planning time. This is because:
                // 1. Different struct fields may have different types
                // 2. The return type must be determined at planning time for type inference
                // 3. Dynamic field names would make the return type indeterminate
                _ => {
                    let field_name_scalar = match field_arg {
                        ColumnarValue::Scalar(name) => name.clone(),
                        _ => {
                            return exec_err!(
                                "Struct/Map field access requires scalar field name"
                            );
                        }
                    };
                    extract_single_field(current, field_name_scalar)?
                }
            };

            // Early exit if we hit null
            if current.data_type().is_null() {
                return Ok(ColumnarValue::Scalar(ScalarValue::Null));
            }
        }

        Ok(current)
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &dyn datafusion_expr::simplify::SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        // Need at least 2 args (base + field)
        if args.len() < 2 {
            return Ok(ExprSimplifyResult::Original(args));
        }

        // Flatten all nested get_field calls in a single pass
        // Pattern: get_field(get_field(get_field(base, a), b), c) => get_field(base, a, b, c)

        // Collect path arguments from all nested levels
        let mut path_args_stack = Vec::new();
        let mut current_expr = &args[0];

        // Push the outermost path arguments first
        path_args_stack.push(&args[1..]);

        // Walk down the chain of nested get_field calls
        let base_expr = loop {
            if let Expr::ScalarFunction(ScalarFunction {
                func,
                args: inner_args,
            }) = current_expr
                && func
                    .inner()
                    .as_any()
                    .downcast_ref::<GetFieldFunc>()
                    .is_some()
            {
                // Store this level's path arguments (all except the first, which is base/nested call)
                path_args_stack.push(&inner_args[1..]);

                // Move to the next level down
                current_expr = &inner_args[0];
                continue;
            }
            // Not a get_field call, this is the base expression
            break current_expr;
        };

        // If no nested get_field calls were found, return original
        if path_args_stack.len() == args.len() - 1 {
            return Ok(ExprSimplifyResult::Original(args));
        }

        // If we found any nested get_field calls, flatten them
        // Build merged args: [base, ...all_path_args_in_correct_order]
        let mut merged_args = vec![base_expr.clone()];

        // Add path args in reverse order (innermost to outermost)
        // Stack is: [outermost_paths, ..., innermost_paths]
        // We want: [base, innermost_paths, ..., outermost_paths]
        for path_slice in path_args_stack.iter().rev() {
            merged_args.extend_from_slice(path_slice);
        }

        Ok(ExprSimplifyResult::Simplified(Expr::ScalarFunction(
            ScalarFunction::new_udf(
                Arc::new(ScalarUDF::new_from_impl(GetFieldFunc::new())),
                merged_args,
            ),
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() < 2 {
            return exec_err!(
                "get_field requires at least 2 arguments, got {}",
                arg_types.len()
            );
        }
        // Accept types as-is, validation happens in return_field_from_args
        Ok(arg_types.to_vec())
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, StructArray};
    use arrow::datatypes::Fields;

    #[test]
    fn test_get_field_utf8view_key() -> Result<()> {
        // Create a struct array with fields "a" and "b"
        let a_values = Int32Array::from(vec![Some(1), Some(2), Some(3)]);
        let b_values = Int32Array::from(vec![Some(10), Some(20), Some(30)]);

        let fields: Fields = vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]
        .into();

        let struct_array = StructArray::new(
            fields,
            vec![
                Arc::new(a_values) as ArrayRef,
                Arc::new(b_values) as ArrayRef,
            ],
            None,
        );

        let base = ColumnarValue::Array(Arc::new(struct_array));

        // Use Utf8View key to access field "a"
        let key = ScalarValue::Utf8View(Some("a".to_string()));

        let result = extract_single_field(base, key)?;

        let result_array = result.into_array(3)?;
        let expected = Int32Array::from(vec![Some(1), Some(2), Some(3)]);

        assert_eq!(result_array.as_ref(), &expected as &dyn Array);

        Ok(())
    }
}
