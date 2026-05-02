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

use std::collections::VecDeque;
use std::hash::Hash;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayData, ArrayRef, ArrowPrimitiveType, MapArray, OffsetSizeTrait,
    StructArray, cast::AsArray,
};
use arrow::buffer::Buffer;
use arrow::datatypes::{
    DataType, Date32Type, Date64Type, Field, FieldRef, Int8Type, Int16Type, Int32Type,
    Int64Type, SchemaBuilder, ToByteSlice, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};

use datafusion_common::utils::{fixed_size_list_to_arrays, list_to_arrays};
use datafusion_common::{
    HashSet, Result, ScalarValue, exec_err, utils::take_function_args,
};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;

use crate::make_array::make_array;

/// Returns a map created from a key list and a value list
pub fn map(keys: Vec<Expr>, values: Vec<Expr>) -> Expr {
    let keys = make_array(keys);
    let values = make_array(values);
    Expr::ScalarFunction(ScalarFunction::new_udf(map_udf(), vec![keys, values]))
}

create_func!(MapFunc, map_udf);

/// Check if we can evaluate the expr to constant directly.
///
/// # Example
/// ```sql
/// SELECT make_map('type', 'test') from test
/// ```
/// We can evaluate the result of `make_map` directly.
fn can_evaluate_to_const(args: &[ColumnarValue]) -> bool {
    args.iter()
        .all(|arg| matches!(arg, ColumnarValue::Scalar(_)))
}

#[cfg(test)]
fn make_map_batch(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    make_map_batch_with_entries(args, None)
}

/// Like [`make_map_batch`] but takes an optional `entries` field whose
/// `Struct<key, value>` carries metadata propagated from the input element
/// fields. Each field-construction site below uses these when supplied so
/// extension types survive `map(...)` calls.
fn make_map_batch_with_entries(
    args: &[ColumnarValue],
    entries: Option<FieldRef>,
) -> Result<ColumnarValue> {
    let [keys_arg, values_arg] = take_function_args("make_map", args)?;

    let can_evaluate_to_const = can_evaluate_to_const(args);

    let keys = get_first_array_ref(keys_arg)?;
    let key_array = keys.as_ref();

    match keys_arg {
        ColumnarValue::Array(_) => match key_array.data_type() {
            DataType::List(_) => keys
                .as_list::<i32>()
                .iter()
                .flatten()
                .try_for_each(|row| validate_map_keys(row.as_ref()))?,
            DataType::LargeList(_) => keys
                .as_list::<i64>()
                .iter()
                .flatten()
                .try_for_each(|row| validate_map_keys(row.as_ref()))?,
            DataType::FixedSizeList(_, _) => {
                keys.as_fixed_size_list()
                    .iter()
                    .flatten()
                    .try_for_each(|row| validate_map_keys(row.as_ref()))?
            }
            data_type => {
                return exec_err!(
                    "Expected list, large_list or fixed_size_list, got {:?}",
                    data_type
                );
            }
        },
        ColumnarValue::Scalar(_) => {
            validate_map_keys(key_array)?;
        }
    }

    let values = get_first_array_ref(values_arg)?;

    make_map_batch_internal(
        &keys,
        &values,
        can_evaluate_to_const,
        &keys_arg.data_type(),
        entries,
    )
}

fn validate_unique_primitive_keys<T: ArrowPrimitiveType>(array: &dyn Array) -> Result<()>
where
    T::Native: Copy + Eq + Hash + std::fmt::Display,
{
    let primitive_array = array.as_primitive::<T>();
    if primitive_array.null_count() > 0 {
        return exec_err!("map key cannot be null");
    }

    if let Some(value) = find_duplicate_value(
        primitive_array.len(),
        primitive_array.values().iter().copied(),
    ) {
        return exec_err!("map key must be unique, duplicate key found: {}", value);
    }

    Ok(())
}

fn validate_unique_str_keys<'a>(
    null_count: usize,
    len: usize,
    values: impl IntoIterator<Item = &'a str>,
) -> Result<()> {
    if null_count > 0 {
        return exec_err!("map key cannot be null");
    }

    if let Some(value) = find_duplicate_value(len, values) {
        return exec_err!("map key must be unique, duplicate key found: {}", value);
    }

    Ok(())
}

fn validate_unique_binary_keys<'a>(
    null_count: usize,
    len: usize,
    values: impl IntoIterator<Item = &'a [u8]>,
) -> Result<()> {
    if null_count > 0 {
        return exec_err!("map key cannot be null");
    }

    if let Some(value) = find_duplicate_value(len, values) {
        return exec_err!("map key must be unique, duplicate key found: {:?}", value);
    }

    Ok(())
}

fn find_duplicate_value<T, I>(len: usize, values: I) -> Option<T>
where
    T: Copy + Eq + Hash,
    I: IntoIterator<Item = T>,
{
    let mut seen_keys = HashSet::with_capacity(len);
    values.into_iter().find(|value| !seen_keys.insert(*value))
}

fn validate_unique_keys_generic(array: &dyn Array) -> Result<()> {
    let mut seen_keys = HashSet::with_capacity(array.len());

    for i in 0..array.len() {
        let key = ScalarValue::try_from_array(array, i)?;

        // Validation 1: Map keys cannot be null
        if key.is_null() {
            return exec_err!("map key cannot be null");
        }

        // Validation 2: Map keys must be unique
        if seen_keys.contains(&key) {
            return exec_err!("map key must be unique, duplicate key found: {}", key);
        }
        seen_keys.insert(key);
    }
    Ok(())
}

/// Validates that map keys are non-null and unique.
fn validate_map_keys(array: &dyn Array) -> Result<()> {
    match array.data_type() {
        DataType::Int8 => validate_unique_primitive_keys::<Int8Type>(array),
        DataType::Int16 => validate_unique_primitive_keys::<Int16Type>(array),
        DataType::Int32 => validate_unique_primitive_keys::<Int32Type>(array),
        DataType::Int64 => validate_unique_primitive_keys::<Int64Type>(array),
        DataType::UInt8 => validate_unique_primitive_keys::<UInt8Type>(array),
        DataType::UInt16 => validate_unique_primitive_keys::<UInt16Type>(array),
        DataType::UInt32 => validate_unique_primitive_keys::<UInt32Type>(array),
        DataType::UInt64 => validate_unique_primitive_keys::<UInt64Type>(array),
        DataType::Date32 => validate_unique_primitive_keys::<Date32Type>(array),
        DataType::Date64 => validate_unique_primitive_keys::<Date64Type>(array),
        DataType::Utf8 => {
            let arr = array.as_string::<i32>();
            validate_unique_str_keys(arr.null_count(), arr.len(), arr.iter().flatten())
        }
        DataType::LargeUtf8 => {
            let arr = array.as_string::<i64>();
            validate_unique_str_keys(arr.null_count(), arr.len(), arr.iter().flatten())
        }
        DataType::Utf8View => {
            let arr = array.as_string_view();
            validate_unique_str_keys(arr.null_count(), arr.len(), arr.iter().flatten())
        }
        DataType::Binary => {
            let arr = array.as_binary::<i32>();
            validate_unique_binary_keys(arr.null_count(), arr.len(), arr.iter().flatten())
        }
        DataType::LargeBinary => {
            let arr = array.as_binary::<i64>();
            validate_unique_binary_keys(arr.null_count(), arr.len(), arr.iter().flatten())
        }
        DataType::BinaryView => {
            let arr = array.as_binary_view();
            validate_unique_binary_keys(arr.null_count(), arr.len(), arr.iter().flatten())
        }
        _ => validate_unique_keys_generic(array),
    }
}

fn get_first_array_ref(columnar_value: &ColumnarValue) -> Result<ArrayRef> {
    match columnar_value {
        ColumnarValue::Scalar(value) => match value {
            ScalarValue::List(array) => Ok(array.value(0)),
            ScalarValue::LargeList(array) => Ok(array.value(0)),
            ScalarValue::FixedSizeList(array) => Ok(array.value(0)),
            _ => exec_err!("Expected array, got {}", value),
        },
        ColumnarValue::Array(array) => Ok(array.to_owned()),
    }
}

fn make_map_batch_internal(
    keys: &ArrayRef,
    values: &ArrayRef,
    can_evaluate_to_const: bool,
    data_type: &DataType,
    entries: Option<FieldRef>,
) -> Result<ColumnarValue> {
    if keys.len() != values.len() {
        return exec_err!("map requires key and value lists to have the same length");
    }

    // Use the array path (make_map_array_internal) in these cases:
    // 1. Not const evaluation (!can_evaluate_to_const) - allows scalar elimination optimization
    // 2. NULL maps present (keys.null_count() > 0) - fast path doesn't handle NULL list elements
    if !can_evaluate_to_const || keys.null_count() > 0 {
        return match data_type {
            DataType::LargeList(..) => {
                make_map_array_internal::<i64>(keys, values, entries)
            }
            DataType::List(..) => make_map_array_internal::<i32>(keys, values, entries),
            DataType::FixedSizeList(..) => {
                // FixedSizeList doesn't use OffsetSizeTrait, so handle it separately
                make_map_array_from_fixed_size_list(keys, values, entries)
            }
            _ => exec_err!(
                "Expected List, LargeList, or FixedSizeList, got {:?}",
                data_type
            ),
        };
    }

    let (key_field, value_field) = key_value_fields_from_entries(
        entries.as_deref(),
        keys.data_type(),
        values.data_type(),
    );
    let key_field = Arc::new(key_field);
    let value_field = Arc::new(value_field);
    let mut entry_struct_buffer: VecDeque<(Arc<Field>, ArrayRef)> = VecDeque::new();
    let mut entry_offsets_buffer = VecDeque::new();
    entry_offsets_buffer.push_back(0);

    entry_struct_buffer.push_back((Arc::clone(&key_field), Arc::clone(keys)));
    entry_struct_buffer.push_back((Arc::clone(&value_field), Arc::clone(values)));
    entry_offsets_buffer.push_back(keys.len() as u32);

    let entry_struct: Vec<(Arc<Field>, ArrayRef)> = entry_struct_buffer.into();
    let entry_struct = StructArray::from(entry_struct);

    let entries_field = entries.unwrap_or_else(|| {
        Arc::new(Field::new(
            "entries",
            entry_struct.data_type().clone(),
            false,
        ))
    });
    let map_data_type = DataType::Map(entries_field, false);

    let entry_offsets: Vec<u32> = entry_offsets_buffer.into();
    let entry_offsets_buffer = Buffer::from(entry_offsets.to_byte_slice());

    let map_data = ArrayData::builder(map_data_type)
        .len(entry_offsets.len() - 1)
        .add_buffer(entry_offsets_buffer)
        .add_child_data(entry_struct.to_data())
        .build()?;
    let map_array = Arc::new(MapArray::from(map_data));

    Ok(if can_evaluate_to_const {
        ColumnarValue::Scalar(ScalarValue::try_from_array(map_array.as_ref(), 0)?)
    } else {
        ColumnarValue::Array(map_array)
    })
}

#[user_doc(
    doc_section(label = "Map Functions"),
    description = "Returns an Arrow map with the specified key-value pairs.\n\n\
    The `make_map` function creates a map from two lists: one for keys and one for values. Each key must be unique and non-null.",
    syntax_example = "map(key, value)\nmap(key: value)\nmake_map(['key1', 'key2'], ['value1', 'value2'])",
    sql_example = r#"
```sql
-- Using map function
SELECT MAP('type', 'test');
----
{type: test}

SELECT MAP(['POST', 'HEAD', 'PATCH'], [41, 33, null]);
----
{POST: 41, HEAD: 33, PATCH: NULL}

SELECT MAP([[1,2], [3,4]], ['a', 'b']);
----
{[1, 2]: a, [3, 4]: b}

SELECT MAP { 'a': 1, 'b': 2 };
----
{a: 1, b: 2}

-- Using make_map function
SELECT MAKE_MAP(['POST', 'HEAD'], [41, 33]);
----
{POST: 41, HEAD: 33}

SELECT MAKE_MAP(['key1', 'key2'], ['value1', null]);
----
{key1: value1, key2: }
```"#,
    argument(
        name = "key",
        description = "For `map`: Expression to be used for key. Can be a constant, column, function, or any combination of arithmetic or string operators.\n\
                        For `make_map`: The list of keys to be used in the map. Each key must be unique and non-null."
    ),
    argument(
        name = "value",
        description = "For `map`: Expression to be used for value. Can be a constant, column, function, or any combination of arithmetic or string operators.\n\
                        For `make_map`: The list of values to be mapped to the corresponding keys."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MapFunc {
    signature: Signature,
}

impl Default for MapFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl MapFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MapFunc {
    fn name(&self) -> &str {
        "map"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [keys_arg, values_arg] = take_function_args(self.name(), arg_types)?;
        let mut builder = SchemaBuilder::new();
        builder.push(Field::new(
            "key",
            get_element_type(keys_arg)?.clone(),
            false,
        ));
        builder.push(Field::new(
            "value",
            get_element_type(values_arg)?.clone(),
            true,
        ));
        let fields = builder.finish().fields;
        Ok(DataType::Map(
            Arc::new(Field::new("entries", DataType::Struct(fields), false)),
            false,
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [keys_arg, values_arg] = take_function_args(self.name(), args.arg_fields)?;

        let key_inner = get_element_field(keys_arg)?;
        let value_inner = get_element_field(values_arg)?;

        let mut key_field = Field::new("key", key_inner.data_type().clone(), false);
        let key_meta = key_inner.metadata();
        if !key_meta.is_empty() {
            key_field = key_field.with_metadata(key_meta.clone());
        }
        let mut value_field = Field::new("value", value_inner.data_type().clone(), true);
        let value_meta = value_inner.metadata();
        if !value_meta.is_empty() {
            value_field = value_field.with_metadata(value_meta.clone());
        }

        let mut builder = SchemaBuilder::new();
        builder.push(key_field);
        builder.push(value_field);
        let fields = builder.finish().fields;
        let entries = Arc::new(Field::new("entries", DataType::Struct(fields), false));
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Map(entries, false),
            false,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let entries = match args.return_field.data_type() {
            DataType::Map(entries, _) => Some(Arc::clone(entries)),
            _ => None,
        };
        make_map_batch_with_entries(&args.args, entries)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn get_element_type(data_type: &DataType) -> Result<&DataType> {
    match data_type {
        DataType::List(element) => Ok(element.data_type()),
        DataType::LargeList(element) => Ok(element.data_type()),
        DataType::FixedSizeList(element, _) => Ok(element.data_type()),
        _ => exec_err!(
            "Expected list, large_list or fixed_size_list, got {:?}",
            data_type
        ),
    }
}

/// Like [`get_element_type`] but returns the full inner [`Field`] so callers
/// can read its metadata (used to propagate Arrow extension types onto the
/// resulting map's `key` / `value` fields).
fn get_element_field(field: &FieldRef) -> Result<&FieldRef> {
    match field.data_type() {
        DataType::List(inner)
        | DataType::LargeList(inner)
        | DataType::FixedSizeList(inner, _) => Ok(inner),
        dt => exec_err!("Expected list, large_list or fixed_size_list, got {:?}", dt),
    }
}

/// Extract `(key, value)` member fields from a map's `entries` struct field.
///
/// Falls back to bare key/value fields built from the supplied data types if
/// `entries` is not provided, the struct shape is unexpected, or member
/// data types disagree (which can happen on the array path that flattens
/// nested lists).
fn key_value_fields_from_entries(
    entries: Option<&Field>,
    key_data_type: &DataType,
    value_data_type: &DataType,
) -> (Field, Field) {
    if let Some(entries) = entries
        && let DataType::Struct(fields) = entries.data_type()
        && fields.len() == 2
        && fields[0].data_type() == key_data_type
        && fields[1].data_type() == value_data_type
    {
        return (fields[0].as_ref().clone(), fields[1].as_ref().clone());
    }
    (
        Field::new("key", key_data_type.clone(), false),
        Field::new("value", value_data_type.clone(), true),
    )
}

/// Helper function to create MapArray from array of values to support arrays for Map scalar function
///
/// ``` text
/// Format of input KEYS and VALUES column
///         keys                        values
/// +---------------------+       +---------------------+
/// | +-----------------+ |       | +-----------------+ |
/// | | [k11, k12, k13] | |       | | [v11, v12, v13] | |
/// | +-----------------+ |       | +-----------------+ |
/// |                     |       |                     |
/// | +-----------------+ |       | +-----------------+ |
/// | | [k21, k22, k23] | |       | | [v21, v22, v23] | |
/// | +-----------------+ |       | +-----------------+ |
/// |                     |       |                     |
/// | +-----------------+ |       | +-----------------+ |
/// | |[k31, k32, k33]  | |       | |[v31, v32, v33]  | |
/// | +-----------------+ |       | +-----------------+ |
/// +---------------------+       +---------------------+
/// ```
/// Flattened keys and values array to user create `StructArray`,
/// which serves as inner child for `MapArray`
///
/// ``` text
/// Flattened           Flattened
/// Keys                Values
/// +-----------+      +-----------+
/// | +-------+ |      | +-------+ |
/// | |  k11  | |      | |  v11  | |
/// | +-------+ |      | +-------+ |
/// | +-------+ |      | +-------+ |
/// | |  k12  | |      | |  v12  | |
/// | +-------+ |      | +-------+ |
/// | +-------+ |      | +-------+ |
/// | |  k13  | |      | |  v13  | |
/// | +-------+ |      | +-------+ |
/// | +-------+ |      | +-------+ |
/// | |  k21  | |      | |  v21  | |
/// | +-------+ |      | +-------+ |
/// | +-------+ |      | +-------+ |
/// | |  k22  | |      | |  v22  | |
/// | +-------+ |      | +-------+ |
/// | +-------+ |      | +-------+ |
/// | |  k23  | |      | |  v23  | |
/// | +-------+ |      | +-------+ |
/// | +-------+ |      | +-------+ |
/// | |  k31  | |      | |  v31  | |
/// | +-------+ |      | +-------+ |
/// | +-------+ |      | +-------+ |
/// | |  k32  | |      | |  v32  | |
/// | +-------+ |      | +-------+ |
/// | +-------+ |      | +-------+ |
/// | |  k33  | |      | |  v33  | |
/// | +-------+ |      | +-------+ |
/// +-----------+      +-----------+
/// ```text
fn make_map_array_internal<O: OffsetSizeTrait>(
    keys: &ArrayRef,
    values: &ArrayRef,
    entries: Option<FieldRef>,
) -> Result<ColumnarValue> {
    // Save original data types and array length before list_to_arrays transforms them
    let keys_data_type = keys.data_type().clone();
    let values_data_type = values.data_type().clone();
    let original_len = keys.len(); // This is the number of rows in the input

    // Save the nulls bitmap from the original keys array (before list_to_arrays)
    // This tells us which MAP values are NULL (not which keys within maps are null)
    let nulls_bitmap = keys.nulls().cloned();

    let keys = list_to_arrays::<O>(keys);
    let values = list_to_arrays_skipping_null_rows::<O>(values, nulls_bitmap.as_ref());

    build_map_array(
        &keys,
        &values,
        &keys_data_type,
        &values_data_type,
        original_len,
        nulls_bitmap,
        entries,
    )
}

/// Helper function specifically for FixedSizeList inputs
/// Similar to make_map_array_internal but uses fixed_size_list_to_arrays instead of list_to_arrays
fn make_map_array_from_fixed_size_list(
    keys: &ArrayRef,
    values: &ArrayRef,
    entries: Option<FieldRef>,
) -> Result<ColumnarValue> {
    // Save original data types and array length
    let keys_data_type = keys.data_type().clone();
    let values_data_type = values.data_type().clone();
    let original_len = keys.len();

    // Save the nulls bitmap from the original keys array
    let nulls_bitmap = keys.nulls().cloned();

    let keys = fixed_size_list_to_arrays(keys);
    let values =
        fixed_size_list_to_arrays_skipping_null_rows(values, nulls_bitmap.as_ref());

    build_map_array(
        &keys,
        &values,
        &keys_data_type,
        &values_data_type,
        original_len,
        nulls_bitmap,
        entries,
    )
}
fn list_to_arrays_skipping_null_rows<O: OffsetSizeTrait>(
    array: &ArrayRef,
    null_rows: Option<&arrow::buffer::NullBuffer>,
) -> Vec<ArrayRef> {
    array
        .as_list::<O>()
        .iter()
        .enumerate()
        .filter_map(|(i, row)| {
            if null_rows.is_some_and(|nulls| nulls.is_null(i)) {
                None
            } else {
                row
            }
        })
        .collect()
}

fn fixed_size_list_to_arrays_skipping_null_rows(
    array: &ArrayRef,
    null_rows: Option<&arrow::buffer::NullBuffer>,
) -> Vec<ArrayRef> {
    array
        .as_fixed_size_list()
        .iter()
        .enumerate()
        .filter_map(|(i, row)| {
            if null_rows.is_some_and(|nulls| nulls.is_null(i)) {
                None
            } else {
                row
            }
        })
        .collect()
}

/// Common logic to build a MapArray from decomposed list arrays
fn build_map_array(
    keys: &[ArrayRef],
    values: &[ArrayRef],
    keys_data_type: &DataType,
    values_data_type: &DataType,
    original_len: usize,
    nulls_bitmap: Option<arrow::buffer::NullBuffer>,
    entries: Option<FieldRef>,
) -> Result<ColumnarValue> {
    if keys.len() != values.len() {
        return exec_err!("map requires key and value lists to have the same length");
    }

    let mut key_array_vec = vec![];
    let mut value_array_vec = vec![];
    for (k, v) in keys.iter().zip(values.iter()) {
        key_array_vec.push(k.as_ref());
        value_array_vec.push(v.as_ref());
    }

    // Build offset buffer that accounts for NULL maps
    // For each row, if it's NULL, the offset stays the same (empty range)
    // If it's not NULL, the offset advances by the number of entries in that map
    // NOTE: MapArray always requires i32 offsets, regardless of input list type
    let mut running_offset = 0i32;
    let mut offset_buffer = vec![running_offset];
    let mut non_null_idx = 0;
    for i in 0..original_len {
        let is_null = nulls_bitmap.as_ref().is_some_and(|nulls| nulls.is_null(i));
        if !is_null {
            let entry_count = keys[non_null_idx].len();
            // Validate that we won't overflow i32 when converting from potentially i64 offsets
            let entry_count_i32 = i32::try_from(entry_count).map_err(|_| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Map offset overflow: entry count {entry_count} at index {i} exceeds i32::MAX",
                ))
            })?;
            running_offset =
                running_offset.checked_add(entry_count_i32).ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution(format!(
                    "Map offset overflow: cumulative offset exceeds i32::MAX at index {i}",
                ))
                })?;
            non_null_idx += 1;
        }
        offset_buffer.push(running_offset);
    }

    // concatenate all the arrays
    // If key_array_vec is empty, it means all maps were NULL (list elements were NULL).
    // In this case, we need to create empty arrays with the correct data type.
    let (flattened_keys, flattened_values) = if key_array_vec.is_empty() {
        // All maps are NULL - create empty arrays
        // We need to infer the data type from the original keys/values arrays
        let key_type = get_element_type(keys_data_type)?;
        let value_type = get_element_type(values_data_type)?;

        (
            arrow::array::new_empty_array(key_type),
            arrow::array::new_empty_array(value_type),
        )
    } else {
        let flattened_keys = arrow::compute::concat(key_array_vec.as_ref())?;
        if flattened_keys.null_count() > 0 {
            return exec_err!("keys cannot be null");
        }
        let flattened_values = arrow::compute::concat(value_array_vec.as_ref())?;
        (flattened_keys, flattened_values)
    };

    let (key_field, value_field) = key_value_fields_from_entries(
        entries.as_deref(),
        flattened_keys.data_type(),
        flattened_values.data_type(),
    );
    let fields = vec![Arc::new(key_field), Arc::new(value_field)];

    let struct_data = ArrayData::builder(DataType::Struct(fields.into()))
        .len(flattened_keys.len())
        .add_child_data(flattened_keys.to_data())
        .add_child_data(flattened_values.to_data())
        .build()?;

    let entries_field = entries.unwrap_or_else(|| {
        Arc::new(Field::new(
            "entries",
            struct_data.data_type().clone(),
            false,
        ))
    });
    let mut map_data_builder = ArrayData::builder(DataType::Map(entries_field, false))
        .len(original_len) // Use the original number of rows, not the filtered count
        .add_child_data(struct_data)
        .add_buffer(Buffer::from_slice_ref(offset_buffer.as_slice()));

    // Add the nulls bitmap if present (to preserve NULL map values)
    if let Some(nulls) = nulls_bitmap {
        map_data_builder = map_data_builder.nulls(Some(nulls));
    }

    let map_data = map_data_builder.build()?;
    Ok(ColumnarValue::Array(Arc::new(MapArray::from(map_data))))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression test for #21982: `map(...)` must propagate the input list
    /// elements' metadata onto the map's `key` and `value` fields.
    #[test]
    fn map_preserves_key_value_field_metadata() -> Result<()> {
        use datafusion_expr::ReturnFieldArgs;

        let key_inner: FieldRef =
            Arc::new(Field::new("e", DataType::Utf8, false).with_metadata(
                std::collections::HashMap::from([(
                    "ARROW:extension:name".to_string(),
                    "arrow.uuid".to_string(),
                )]),
            ));
        let value_inner: FieldRef =
            Arc::new(Field::new("e", DataType::Int64, true).with_metadata(
                std::collections::HashMap::from([(
                    "ARROW:extension:name".to_string(),
                    "arrow.json".to_string(),
                )]),
            ));
        let keys: FieldRef = Arc::new(Field::new(
            "k",
            DataType::List(Arc::clone(&key_inner)),
            true,
        ));
        let values: FieldRef = Arc::new(Field::new(
            "v",
            DataType::List(Arc::clone(&value_inner)),
            true,
        ));
        let arg_fields = vec![keys, values];
        let scalar_args: Vec<Option<&ScalarValue>> = vec![None, None];
        let rf = MapFunc::new().return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &scalar_args,
        })?;
        let DataType::Map(entries, _) = rf.data_type() else {
            panic!("expected Map");
        };
        let DataType::Struct(fields) = entries.data_type() else {
            panic!("expected entries struct");
        };
        assert_eq!(
            fields[0].metadata().get("ARROW:extension:name"),
            Some(&"arrow.uuid".to_string())
        );
        assert_eq!(
            fields[1].metadata().get("ARROW:extension:name"),
            Some(&"arrow.json".to_string())
        );
        Ok(())
    }

    #[test]
    fn test_make_map_with_null_maps() {
        // Test that NULL map values (entire map is NULL) are correctly handled
        // This test directly calls make_map_batch with a List containing NULL elements
        //
        // Background: On main branch, the code would fail with "map key cannot be null"
        // because it couldn't distinguish between:
        // - NULL map (entire map is NULL) - should be allowed
        // - null key within a map - should be rejected

        // Build keys array: [['a'], NULL, ['b']]
        // The middle NULL represents an entire NULL map, not a null key
        let mut key_builder =
            arrow::array::ListBuilder::new(arrow::array::StringBuilder::new());

        // First map: ['a']
        key_builder.values().append_value("a");
        key_builder.append(true);

        // Second map: NULL (entire map is NULL)
        key_builder.append(false);

        // Third map: ['b']
        key_builder.values().append_value("b");
        key_builder.append(true);

        let keys_array = Arc::new(key_builder.finish());

        // Build values array: [[1], [2], [3]]
        let mut value_builder =
            arrow::array::ListBuilder::new(arrow::array::Int32Builder::new());

        value_builder.values().append_value(1);
        value_builder.append(true);

        value_builder.values().append_value(2);
        value_builder.append(true);

        value_builder.values().append_value(3);
        value_builder.append(true);

        let values_array = Arc::new(value_builder.finish());

        // Call make_map_batch - should succeed
        let result = make_map_batch(&[
            ColumnarValue::Array(keys_array),
            ColumnarValue::Array(values_array),
        ]);

        assert!(result.is_ok(), "Should handle NULL maps correctly");

        // Verify the result
        let map_array = match result.unwrap() {
            ColumnarValue::Array(arr) => arr,
            _ => panic!("Expected Array result"),
        };

        assert_eq!(map_array.len(), 3, "Should have 3 maps");
        assert!(!map_array.is_null(0), "First map should not be NULL");
        assert!(map_array.is_null(1), "Second map should be NULL");
        assert!(!map_array.is_null(2), "Third map should not be NULL");
    }

    #[test]
    fn test_make_map_with_null_key_within_map_should_fail() {
        // Test that null keys WITHIN a map are properly rejected
        // This ensures the fix doesn't accidentally allow invalid null keys

        // Build keys array: [['a', NULL, 'b']]
        // The NULL here is a null key within the map, which is invalid
        let mut key_builder =
            arrow::array::ListBuilder::new(arrow::array::StringBuilder::new());

        key_builder.values().append_value("a");
        key_builder.values().append_null(); // Invalid: null key
        key_builder.values().append_value("b");
        key_builder.append(true);

        let keys_array = Arc::new(key_builder.finish());

        // Build values array: [[1, 2, 3]]
        let mut value_builder =
            arrow::array::ListBuilder::new(arrow::array::Int32Builder::new());

        value_builder.values().append_value(1);
        value_builder.values().append_value(2);
        value_builder.values().append_value(3);
        value_builder.append(true);

        let values_array = Arc::new(value_builder.finish());

        // Call make_map_batch - should fail
        let result = make_map_batch(&[
            ColumnarValue::Array(keys_array),
            ColumnarValue::Array(values_array),
        ]);

        assert!(result.is_err(), "Should reject null keys within maps");

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("cannot be null"),
            "Error should mention null keys, got: {err_msg}"
        );
    }

    #[test]
    fn test_make_map_with_large_list() {
        // Test that LargeList inputs work correctly with i32 offset conversion
        // This verifies the fix for the offset buffer type mismatch issue

        // Build keys array as LargeList: [['a', 'b'], ['c']]
        let mut key_builder =
            arrow::array::LargeListBuilder::new(arrow::array::StringBuilder::new());

        // First map: ['a', 'b']
        key_builder.values().append_value("a");
        key_builder.values().append_value("b");
        key_builder.append(true);

        // Second map: ['c']
        key_builder.values().append_value("c");
        key_builder.append(true);

        let keys_array = Arc::new(key_builder.finish());

        // Build values array as LargeList: [[1, 2], [3]]
        let mut value_builder =
            arrow::array::LargeListBuilder::new(arrow::array::Int32Builder::new());

        value_builder.values().append_value(1);
        value_builder.values().append_value(2);
        value_builder.append(true);

        value_builder.values().append_value(3);
        value_builder.append(true);

        let values_array = Arc::new(value_builder.finish());

        // Call make_map_batch - should succeed
        let result = make_map_batch(&[
            ColumnarValue::Array(keys_array),
            ColumnarValue::Array(values_array),
        ]);

        assert!(
            result.is_ok(),
            "Should handle LargeList inputs correctly: {:?}",
            result.err()
        );

        // Verify the result
        let map_array = match result.unwrap() {
            ColumnarValue::Array(arr) => arr,
            _ => panic!("Expected Array result"),
        };

        assert_eq!(map_array.len(), 2, "Should have 2 maps");
        assert!(!map_array.is_null(0), "First map should not be NULL");
        assert!(!map_array.is_null(1), "Second map should not be NULL");
    }

    #[test]
    fn test_make_map_with_fixed_size_list() {
        // Test that FixedSizeList inputs work correctly
        // This verifies the fix for FixedSizeList support in the data type check

        use arrow::array::FixedSizeListBuilder;

        // Build keys array as FixedSizeList(2): [['a', 'b'], NULL, ['c', 'd']]
        let key_values_builder = arrow::array::StringBuilder::new();
        let mut key_builder = FixedSizeListBuilder::new(key_values_builder, 2);

        // First map: ['a', 'b']
        key_builder.values().append_value("a");
        key_builder.values().append_value("b");
        key_builder.append(true);

        // Second map: NULL (entire map is NULL)
        key_builder.values().append_null();
        key_builder.values().append_null();
        key_builder.append(false);

        // Second map: ['c', 'd']
        key_builder.values().append_value("c");
        key_builder.values().append_value("d");
        key_builder.append(true);

        let keys_array = Arc::new(key_builder.finish());

        // Build values array as FixedSizeList(2): [[1, 2], [99, 100], [3, 4]]
        // The middle row should be ignored because the corresponding key row is NULL.
        let value_values_builder = arrow::array::Int32Builder::new();
        let mut value_builder = FixedSizeListBuilder::new(value_values_builder, 2);

        value_builder.values().append_value(1);
        value_builder.values().append_value(2);
        value_builder.append(true);

        value_builder.values().append_value(99);
        value_builder.values().append_value(100);
        value_builder.append(true);

        value_builder.values().append_value(3);
        value_builder.values().append_value(4);
        value_builder.append(true);

        let values_array = Arc::new(value_builder.finish());

        // Call make_map_batch - should succeed
        let result = make_map_batch(&[
            ColumnarValue::Array(keys_array),
            ColumnarValue::Array(values_array),
        ]);

        assert!(
            result.is_ok(),
            "Should handle FixedSizeList inputs correctly: {:?}",
            result.err()
        );

        // Verify the result
        let map_array = match result.unwrap() {
            ColumnarValue::Array(arr) => arr,
            _ => panic!("Expected Array result"),
        };

        assert_eq!(map_array.len(), 3, "Should have 3 maps");
        assert!(!map_array.is_null(0), "First map should not be NULL");
        assert!(map_array.is_null(1), "Second map should be NULL");
        assert!(!map_array.is_null(2), "Third map should not be NULL");
    }
}
