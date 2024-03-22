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

//! implementation kernels for array functions

use arrow::array::{
    Array, ArrayRef, BooleanArray, Capacities, GenericListArray, Int64Array,
    LargeListArray, ListArray, MutableArrayData, OffsetSizeTrait, UInt64Array,
};
use arrow::compute;
use arrow::datatypes::{DataType, Field, UInt64Type};
use arrow_array::new_null_array;
use arrow_buffer::{ArrowNativeType, BooleanBufferBuilder, NullBuffer, OffsetBuffer};
use arrow_schema::FieldRef;
use arrow_schema::SortOptions;

use datafusion_common::cast::{
    as_generic_list_array, as_int64_array, as_large_list_array, as_list_array,
    as_null_array, as_string_array,
};
use datafusion_common::{
    exec_err, internal_datafusion_err, DataFusionError, Result, ScalarValue,
};

use crate::utils::downcast_arg;
use std::any::type_name;
use std::sync::Arc;

/// Returns the length of each array dimension
fn compute_array_dims(arr: Option<ArrayRef>) -> Result<Option<Vec<Option<u64>>>> {
    let mut value = match arr {
        Some(arr) => arr,
        None => return Ok(None),
    };
    if value.is_empty() {
        return Ok(None);
    }
    let mut res = vec![Some(value.len() as u64)];

    loop {
        match value.data_type() {
            DataType::List(..) => {
                value = downcast_arg!(value, ListArray).value(0);
                res.push(Some(value.len() as u64));
            }
            _ => return Ok(Some(res)),
        }
    }
}

fn generic_list_cardinality<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
) -> Result<ArrayRef> {
    let result = array
        .iter()
        .map(|arr| match compute_array_dims(arr)? {
            Some(vector) => Ok(Some(vector.iter().map(|x| x.unwrap()).product::<u64>())),
            None => Ok(None),
        })
        .collect::<Result<UInt64Array>>()?;
    Ok(Arc::new(result) as ArrayRef)
}

/// Cardinality SQL function
pub fn cardinality(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("cardinality expects one argument");
    }

    match &args[0].data_type() {
        DataType::List(_) => {
            let list_array = as_list_array(&args[0])?;
            generic_list_cardinality::<i32>(list_array)
        }
        DataType::LargeList(_) => {
            let list_array = as_large_list_array(&args[0])?;
            generic_list_cardinality::<i64>(list_array)
        }
        other => {
            exec_err!("cardinality does not support type '{:?}'", other)
        }
    }
}

/// Array_dims SQL function
pub fn array_dims(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("array_dims needs one argument");
    }

    let data = match args[0].data_type() {
        DataType::List(_) => {
            let array = as_list_array(&args[0])?;
            array
                .iter()
                .map(compute_array_dims)
                .collect::<Result<Vec<_>>>()?
        }
        DataType::LargeList(_) => {
            let array = as_large_list_array(&args[0])?;
            array
                .iter()
                .map(compute_array_dims)
                .collect::<Result<Vec<_>>>()?
        }
        array_type => {
            return exec_err!("array_dims does not support type '{array_type:?}'");
        }
    };

    let result = ListArray::from_iter_primitive::<UInt64Type, _, _>(data);

    Ok(Arc::new(result) as ArrayRef)
}

/// Array_ndims SQL function
pub fn array_ndims(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("array_ndims needs one argument");
    }

    fn general_list_ndims<O: OffsetSizeTrait>(
        array: &GenericListArray<O>,
    ) -> Result<ArrayRef> {
        let mut data = Vec::new();
        let ndims = datafusion_common::utils::list_ndims(array.data_type());

        for arr in array.iter() {
            if arr.is_some() {
                data.push(Some(ndims))
            } else {
                data.push(None)
            }
        }

        Ok(Arc::new(UInt64Array::from(data)) as ArrayRef)
    }
    match args[0].data_type() {
        DataType::List(_) => {
            let array = as_list_array(&args[0])?;
            general_list_ndims::<i32>(array)
        }
        DataType::LargeList(_) => {
            let array = as_large_list_array(&args[0])?;
            general_list_ndims::<i64>(array)
        }
        array_type => exec_err!("array_ndims does not support type {array_type:?}"),
    }
}

/// Array_empty SQL function
pub fn array_empty(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("array_empty expects one argument");
    }

    if as_null_array(&args[0]).is_ok() {
        // Make sure to return Boolean type.
        return Ok(Arc::new(BooleanArray::new_null(args[0].len())));
    }
    let array_type = args[0].data_type();

    match array_type {
        DataType::List(_) => general_array_empty::<i32>(&args[0]),
        DataType::LargeList(_) => general_array_empty::<i64>(&args[0]),
        _ => exec_err!("array_empty does not support type '{array_type:?}'."),
    }
}

fn general_array_empty<O: OffsetSizeTrait>(array: &ArrayRef) -> Result<ArrayRef> {
    let array = as_generic_list_array::<O>(array)?;
    let builder = array
        .iter()
        .map(|arr| arr.map(|arr| arr.len() == arr.null_count()))
        .collect::<BooleanArray>();
    Ok(Arc::new(builder))
}

/// Returns the length of a concrete array dimension
fn compute_array_length(
    arr: Option<ArrayRef>,
    dimension: Option<i64>,
) -> Result<Option<u64>> {
    let mut current_dimension: i64 = 1;
    let mut value = match arr {
        Some(arr) => arr,
        None => return Ok(None),
    };
    let dimension = match dimension {
        Some(value) => {
            if value < 1 {
                return Ok(None);
            }

            value
        }
        None => return Ok(None),
    };

    loop {
        if current_dimension == dimension {
            return Ok(Some(value.len() as u64));
        }

        match value.data_type() {
            DataType::List(..) => {
                value = downcast_arg!(value, ListArray).value(0);
                current_dimension += 1;
            }
            DataType::LargeList(..) => {
                value = downcast_arg!(value, LargeListArray).value(0);
                current_dimension += 1;
            }
            _ => return Ok(None),
        }
    }
}

/// Dispatch array length computation based on the offset type.
fn general_array_length<O: OffsetSizeTrait>(array: &[ArrayRef]) -> Result<ArrayRef> {
    let list_array = as_generic_list_array::<O>(&array[0])?;
    let dimension = if array.len() == 2 {
        as_int64_array(&array[1])?.clone()
    } else {
        Int64Array::from_value(1, list_array.len())
    };

    let result = list_array
        .iter()
        .zip(dimension.iter())
        .map(|(arr, dim)| compute_array_length(arr, dim))
        .collect::<Result<UInt64Array>>()?;

    Ok(Arc::new(result) as ArrayRef)
}

/// Array_repeat SQL function
pub fn array_repeat(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_repeat expects two arguments");
    }

    let element = &args[0];
    let count_array = as_int64_array(&args[1])?;

    match element.data_type() {
        DataType::List(_) => {
            let list_array = as_list_array(element)?;
            general_list_repeat::<i32>(list_array, count_array)
        }
        DataType::LargeList(_) => {
            let list_array = as_large_list_array(element)?;
            general_list_repeat::<i64>(list_array, count_array)
        }
        _ => general_repeat::<i32>(element, count_array),
    }
}

/// For each element of `array[i]` repeat `count_array[i]` times.
///
/// Assumption for the input:
///     1. `count[i] >= 0`
///     2. `array.len() == count_array.len()`
///
/// For example,
/// ```text
/// array_repeat(
///     [1, 2, 3], [2, 0, 1] => [[1, 1], [], [3]]
/// )
/// ```
fn general_repeat<O: OffsetSizeTrait>(
    array: &ArrayRef,
    count_array: &Int64Array,
) -> Result<ArrayRef> {
    let data_type = array.data_type();
    let mut new_values = vec![];

    let count_vec = count_array
        .values()
        .to_vec()
        .iter()
        .map(|x| *x as usize)
        .collect::<Vec<_>>();

    for (row_index, &count) in count_vec.iter().enumerate() {
        let repeated_array = if array.is_null(row_index) {
            new_null_array(data_type, count)
        } else {
            let original_data = array.to_data();
            let capacity = Capacities::Array(count);
            let mut mutable =
                MutableArrayData::with_capacities(vec![&original_data], false, capacity);

            for _ in 0..count {
                mutable.extend(0, row_index, row_index + 1);
            }

            let data = mutable.freeze();
            arrow_array::make_array(data)
        };
        new_values.push(repeated_array);
    }

    let new_values: Vec<_> = new_values.iter().map(|a| a.as_ref()).collect();
    let values = compute::concat(&new_values)?;

    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::new(Field::new("item", data_type.to_owned(), true)),
        OffsetBuffer::from_lengths(count_vec),
        values,
        None,
    )?))
}

/// Handle List version of `general_repeat`
///
/// For each element of `list_array[i]` repeat `count_array[i]` times.
///
/// For example,
/// ```text
/// array_repeat(
///     [[1, 2, 3], [4, 5], [6]], [2, 0, 1] => [[[1, 2, 3], [1, 2, 3]], [], [[6]]]
/// )
/// ```
fn general_list_repeat<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
    count_array: &Int64Array,
) -> Result<ArrayRef> {
    let data_type = list_array.data_type();
    let value_type = list_array.value_type();
    let mut new_values = vec![];

    let count_vec = count_array
        .values()
        .to_vec()
        .iter()
        .map(|x| *x as usize)
        .collect::<Vec<_>>();

    for (list_array_row, &count) in list_array.iter().zip(count_vec.iter()) {
        let list_arr = match list_array_row {
            Some(list_array_row) => {
                let original_data = list_array_row.to_data();
                let capacity = Capacities::Array(original_data.len() * count);
                let mut mutable = MutableArrayData::with_capacities(
                    vec![&original_data],
                    false,
                    capacity,
                );

                for _ in 0..count {
                    mutable.extend(0, 0, original_data.len());
                }

                let data = mutable.freeze();
                let repeated_array = arrow_array::make_array(data);

                let list_arr = GenericListArray::<O>::try_new(
                    Arc::new(Field::new("item", value_type.clone(), true)),
                    OffsetBuffer::<O>::from_lengths(vec![original_data.len(); count]),
                    repeated_array,
                    None,
                )?;
                Arc::new(list_arr) as ArrayRef
            }
            None => new_null_array(data_type, count),
        };
        new_values.push(list_arr);
    }

    let lengths = new_values.iter().map(|a| a.len()).collect::<Vec<_>>();
    let new_values: Vec<_> = new_values.iter().map(|a| a.as_ref()).collect();
    let values = compute::concat(&new_values)?;

    Ok(Arc::new(ListArray::try_new(
        Arc::new(Field::new("item", data_type.to_owned(), true)),
        OffsetBuffer::<i32>::from_lengths(lengths),
        values,
        None,
    )?))
}

/// Array_length SQL function
pub fn array_length(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 && args.len() != 2 {
        return exec_err!("array_length expects one or two arguments");
    }

    match &args[0].data_type() {
        DataType::List(_) => general_array_length::<i32>(args),
        DataType::LargeList(_) => general_array_length::<i64>(args),
        array_type => exec_err!("array_length does not support type '{array_type:?}'"),
    }
}

/// array_resize SQL function
pub fn array_resize(arg: &[ArrayRef]) -> Result<ArrayRef> {
    if arg.len() < 2 || arg.len() > 3 {
        return exec_err!("array_resize needs two or three arguments");
    }

    let new_len = as_int64_array(&arg[1])?;
    let new_element = if arg.len() == 3 {
        Some(arg[2].clone())
    } else {
        None
    };

    match &arg[0].data_type() {
        DataType::List(field) => {
            let array = as_list_array(&arg[0])?;
            general_list_resize::<i32>(array, new_len, field, new_element)
        }
        DataType::LargeList(field) => {
            let array = as_large_list_array(&arg[0])?;
            general_list_resize::<i64>(array, new_len, field, new_element)
        }
        array_type => exec_err!("array_resize does not support type '{array_type:?}'."),
    }
}

/// array_resize keep the original array and append the default element to the end
fn general_list_resize<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    count_array: &Int64Array,
    field: &FieldRef,
    default_element: Option<ArrayRef>,
) -> Result<ArrayRef>
where
    O: TryInto<i64>,
{
    let data_type = array.value_type();

    let values = array.values();
    let original_data = values.to_data();

    // create default element array
    let default_element = if let Some(default_element) = default_element {
        default_element
    } else {
        let null_scalar = ScalarValue::try_from(&data_type)?;
        null_scalar.to_array_of_size(original_data.len())?
    };
    let default_value_data = default_element.to_data();

    // create a mutable array to store the original data
    let capacity = Capacities::Array(original_data.len() + default_value_data.len());
    let mut offsets = vec![O::usize_as(0)];
    let mut mutable = MutableArrayData::with_capacities(
        vec![&original_data, &default_value_data],
        false,
        capacity,
    );

    for (row_index, offset_window) in array.offsets().windows(2).enumerate() {
        let count = count_array.value(row_index).to_usize().ok_or_else(|| {
            internal_datafusion_err!("array_resize: failed to convert size to usize")
        })?;
        let count = O::usize_as(count);
        let start = offset_window[0];
        if start + count > offset_window[1] {
            let extra_count =
                (start + count - offset_window[1]).try_into().map_err(|_| {
                    internal_datafusion_err!(
                        "array_resize: failed to convert size to i64"
                    )
                })?;
            let end = offset_window[1];
            mutable.extend(0, (start).to_usize().unwrap(), (end).to_usize().unwrap());
            // append default element
            for _ in 0..extra_count {
                mutable.extend(1, row_index, row_index + 1);
            }
        } else {
            let end = start + count;
            mutable.extend(0, (start).to_usize().unwrap(), (end).to_usize().unwrap());
        };
        offsets.push(offsets[row_index] + count);
    }

    let data = mutable.freeze();
    Ok(Arc::new(GenericListArray::<O>::try_new(
        field.clone(),
        OffsetBuffer::<O>::new(offsets.into()),
        arrow_array::make_array(data),
        None,
    )?))
}

/// Array_sort SQL function
pub fn array_sort(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.is_empty() || args.len() > 3 {
        return exec_err!("array_sort expects one to three arguments");
    }

    let sort_option = match args.len() {
        1 => None,
        2 => {
            let sort = as_string_array(&args[1])?.value(0);
            Some(SortOptions {
                descending: order_desc(sort)?,
                nulls_first: true,
            })
        }
        3 => {
            let sort = as_string_array(&args[1])?.value(0);
            let nulls_first = as_string_array(&args[2])?.value(0);
            Some(SortOptions {
                descending: order_desc(sort)?,
                nulls_first: order_nulls_first(nulls_first)?,
            })
        }
        _ => return exec_err!("array_sort expects 1 to 3 arguments"),
    };

    let list_array = as_list_array(&args[0])?;
    let row_count = list_array.len();

    let mut array_lengths = vec![];
    let mut arrays = vec![];
    let mut valid = BooleanBufferBuilder::new(row_count);
    for i in 0..row_count {
        if list_array.is_null(i) {
            array_lengths.push(0);
            valid.append(false);
        } else {
            let arr_ref = list_array.value(i);
            let arr_ref = arr_ref.as_ref();

            let sorted_array = compute::sort(arr_ref, sort_option)?;
            array_lengths.push(sorted_array.len());
            arrays.push(sorted_array);
            valid.append(true);
        }
    }

    // Assume all arrays have the same data type
    let data_type = list_array.value_type();
    let buffer = valid.finish();

    let elements = arrays
        .iter()
        .map(|a| a.as_ref())
        .collect::<Vec<&dyn Array>>();

    let list_arr = ListArray::new(
        Arc::new(Field::new("item", data_type, true)),
        OffsetBuffer::from_lengths(array_lengths),
        Arc::new(compute::concat(elements.as_slice())?),
        Some(NullBuffer::new(buffer)),
    );
    Ok(Arc::new(list_arr))
}

fn order_desc(modifier: &str) -> Result<bool> {
    match modifier.to_uppercase().as_str() {
        "DESC" => Ok(true),
        "ASC" => Ok(false),
        _ => exec_err!("the second parameter of array_sort expects DESC or ASC"),
    }
}

fn order_nulls_first(modifier: &str) -> Result<bool> {
    match modifier.to_uppercase().as_str() {
        "NULLS FIRST" => Ok(true),
        "NULLS LAST" => Ok(false),
        _ => exec_err!(
            "the third parameter of array_sort expects NULLS FIRST or NULLS LAST"
        ),
    }
}

// Create new offsets that are euqiavlent to `flatten` the array.
fn get_offsets_for_flatten<O: OffsetSizeTrait>(
    offsets: OffsetBuffer<O>,
    indexes: OffsetBuffer<O>,
) -> OffsetBuffer<O> {
    let buffer = offsets.into_inner();
    let offsets: Vec<O> = indexes
        .iter()
        .map(|i| buffer[i.to_usize().unwrap()])
        .collect();
    OffsetBuffer::new(offsets.into())
}

fn flatten_internal<O: OffsetSizeTrait>(
    list_arr: GenericListArray<O>,
    indexes: Option<OffsetBuffer<O>>,
) -> Result<GenericListArray<O>> {
    let (field, offsets, values, _) = list_arr.clone().into_parts();
    let data_type = field.data_type();

    match data_type {
        // Recursively get the base offsets for flattened array
        DataType::List(_) | DataType::LargeList(_) => {
            let sub_list = as_generic_list_array::<O>(&values)?;
            if let Some(indexes) = indexes {
                let offsets = get_offsets_for_flatten(offsets, indexes);
                flatten_internal::<O>(sub_list.clone(), Some(offsets))
            } else {
                flatten_internal::<O>(sub_list.clone(), Some(offsets))
            }
        }
        // Reach the base level, create a new list array
        _ => {
            if let Some(indexes) = indexes {
                let offsets = get_offsets_for_flatten(offsets, indexes);
                let list_arr = GenericListArray::<O>::new(field, offsets, values, None);
                Ok(list_arr)
            } else {
                Ok(list_arr.clone())
            }
        }
    }
}

/// Flatten SQL function
pub fn flatten(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("flatten expects one argument");
    }

    let array_type = args[0].data_type();
    match array_type {
        DataType::List(_) => {
            let list_arr = as_list_array(&args[0])?;
            let flattened_array = flatten_internal::<i32>(list_arr.clone(), None)?;
            Ok(Arc::new(flattened_array) as ArrayRef)
        }
        DataType::LargeList(_) => {
            let list_arr = as_large_list_array(&args[0])?;
            let flattened_array = flatten_internal::<i64>(list_arr.clone(), None)?;
            Ok(Arc::new(flattened_array) as ArrayRef)
        }
        DataType::Null => Ok(args[0].clone()),
        _ => {
            exec_err!("flatten does not support type '{array_type:?}'")
        }
    }
}

/// array_reverse SQL function
pub fn array_reverse(arg: &[ArrayRef]) -> Result<ArrayRef> {
    if arg.len() != 1 {
        return exec_err!("array_reverse needs one argument");
    }

    match &arg[0].data_type() {
        DataType::List(field) => {
            let array = as_list_array(&arg[0])?;
            general_array_reverse::<i32>(array, field)
        }
        DataType::LargeList(field) => {
            let array = as_large_list_array(&arg[0])?;
            general_array_reverse::<i64>(array, field)
        }
        DataType::Null => Ok(arg[0].clone()),
        array_type => exec_err!("array_reverse does not support type '{array_type:?}'."),
    }
}

fn general_array_reverse<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    field: &FieldRef,
) -> Result<ArrayRef>
where
    O: TryFrom<i64>,
{
    let values = array.values();
    let original_data = values.to_data();
    let capacity = Capacities::Array(original_data.len());
    let mut offsets = vec![O::usize_as(0)];
    let mut nulls = vec![];
    let mut mutable =
        MutableArrayData::with_capacities(vec![&original_data], false, capacity);

    for (row_index, offset_window) in array.offsets().windows(2).enumerate() {
        // skip the null value
        if array.is_null(row_index) {
            nulls.push(false);
            offsets.push(offsets[row_index] + O::one());
            mutable.extend(0, 0, 1);
            continue;
        } else {
            nulls.push(true);
        }

        let start = offset_window[0];
        let end = offset_window[1];

        let mut index = end - O::one();
        let mut cnt = 0;

        while index >= start {
            mutable.extend(0, index.to_usize().unwrap(), index.to_usize().unwrap() + 1);
            index = index - O::one();
            cnt += 1;
        }
        offsets.push(offsets[row_index] + O::usize_as(cnt));
    }

    let data = mutable.freeze();
    Ok(Arc::new(GenericListArray::<O>::try_new(
        field.clone(),
        OffsetBuffer::<O>::new(offsets.into()),
        arrow_array::make_array(data),
        Some(nulls.into()),
    )?))
}
