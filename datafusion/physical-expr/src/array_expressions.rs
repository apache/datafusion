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

//! Array expressions

use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use arrow::array::*;
use arrow::buffer::OffsetBuffer;
use arrow::compute::{self};
use arrow::datatypes::{DataType, Field, UInt64Type};
use arrow::row::{RowConverter, SortField};
use arrow_buffer::NullBuffer;

use arrow_schema::FieldRef;
use datafusion_common::cast::{
    as_generic_list_array, as_int64_array, as_large_list_array, as_list_array,
};
use datafusion_common::utils::array_into_list_array;
use datafusion_common::{exec_err, internal_err, plan_err, Result};
use itertools::Itertools;

/// Computes a BooleanArray indicating equality or inequality between elements in a list array and a specified element array.
///
/// # Arguments
///
/// * `list_array_row` - A reference to a trait object implementing the Arrow `Array` trait. It represents the list array for which the equality or inequality will be compared.
///
/// * `element_array` - A reference to a trait object implementing the Arrow `Array` trait. It represents the array with which each element in the `list_array_row` will be compared.
///
/// * `row_index` - The index of the row in the `element_array` and `list_array` to use for the comparison.
///
/// * `eq` - A boolean flag. If `true`, the function computes equality; if `false`, it computes inequality.
///
/// # Returns
///
/// Returns a `Result<BooleanArray>` representing the comparison results. The result may contain an error if there are issues with the computation.
///
/// # Example
///
/// ```text
/// compare_element_to_list(
///     [1, 2, 3], [1, 2, 3], 0, true => [true, false, false]
///     [1, 2, 3, 3, 2, 1], [1, 2, 3], 1, true => [false, true, false, false, true, false]
///
///     [[1, 2, 3], [2, 3, 4], [3, 4, 5]], [[1, 2, 3], [2, 3, 4], [3, 4, 5]], 0, true => [true, false, false]
///     [[1, 2, 3], [2, 3, 4], [2, 3, 4]], [[1, 2, 3], [2, 3, 4], [3, 4, 5]], 1, false => [true, false, false]
/// )
/// ```
fn compare_element_to_list(
    list_array_row: &dyn Array,
    element_array: &dyn Array,
    row_index: usize,
    eq: bool,
) -> Result<BooleanArray> {
    if list_array_row.data_type() != element_array.data_type() {
        return exec_err!(
            "compare_element_to_list received incompatible types: '{:?}' and '{:?}'.",
            list_array_row.data_type(),
            element_array.data_type()
        );
    }

    let indices = UInt32Array::from(vec![row_index as u32]);
    let element_array_row = arrow::compute::take(element_array, &indices, None)?;

    // Compute all positions in list_row_array (that is itself an
    // array) that are equal to `from_array_row`
    let res = match element_array_row.data_type() {
        // arrow_ord::cmp::eq does not support ListArray, so we need to compare it by loop
        DataType::List(_) => {
            // compare each element of the from array
            let element_array_row_inner = as_list_array(&element_array_row)?.value(0);
            let list_array_row_inner = as_list_array(list_array_row)?;

            list_array_row_inner
                .iter()
                // compare element by element the current row of list_array
                .map(|row| {
                    row.map(|row| {
                        if eq {
                            row.eq(&element_array_row_inner)
                        } else {
                            row.ne(&element_array_row_inner)
                        }
                    })
                })
                .collect::<BooleanArray>()
        }
        DataType::LargeList(_) => {
            // compare each element of the from array
            let element_array_row_inner =
                as_large_list_array(&element_array_row)?.value(0);
            let list_array_row_inner = as_large_list_array(list_array_row)?;

            list_array_row_inner
                .iter()
                // compare element by element the current row of list_array
                .map(|row| {
                    row.map(|row| {
                        if eq {
                            row.eq(&element_array_row_inner)
                        } else {
                            row.ne(&element_array_row_inner)
                        }
                    })
                })
                .collect::<BooleanArray>()
        }
        _ => {
            let element_arr = Scalar::new(element_array_row);
            // use not_distinct so we can compare NULL
            if eq {
                arrow_ord::cmp::not_distinct(&list_array_row, &element_arr)?
            } else {
                arrow_ord::cmp::distinct(&list_array_row, &element_arr)?
            }
        }
    };

    Ok(res)
}

fn check_datatypes(name: &str, args: &[&ArrayRef]) -> Result<()> {
    let data_type = args[0].data_type();
    if !args.iter().all(|arg| {
        arg.data_type().equals_datatype(data_type)
            || arg.data_type().equals_datatype(&DataType::Null)
    }) {
        let types = args.iter().map(|arg| arg.data_type()).collect::<Vec<_>>();
        return plan_err!("{name} received incompatible types: '{types:?}'.");
    }

    Ok(())
}

/// Convert one or more [`ArrayRef`] of the same type into a
/// `ListArray` or 'LargeListArray' depending on the offset size.
///
/// # Example (non nested)
///
/// Calling `array(col1, col2)` where col1 and col2 are non nested
/// would return a single new `ListArray`, where each row was a list
/// of 2 elements:
///
/// ```text
/// ┌─────────┐   ┌─────────┐           ┌──────────────┐
/// │ ┌─────┐ │   │ ┌─────┐ │           │ ┌──────────┐ │
/// │ │  A  │ │   │ │  X  │ │           │ │  [A, X]  │ │
/// │ ├─────┤ │   │ ├─────┤ │           │ ├──────────┤ │
/// │ │NULL │ │   │ │  Y  │ │──────────▶│ │[NULL, Y] │ │
/// │ ├─────┤ │   │ ├─────┤ │           │ ├──────────┤ │
/// │ │  C  │ │   │ │  Z  │ │           │ │  [C, Z]  │ │
/// │ └─────┘ │   │ └─────┘ │           │ └──────────┘ │
/// └─────────┘   └─────────┘           └──────────────┘
///   col1           col2                    output
/// ```
///
/// # Example (nested)
///
/// Calling `array(col1, col2)` where col1 and col2 are lists
/// would return a single new `ListArray`, where each row was a list
/// of the corresponding elements of col1 and col2.
///
/// ``` text
/// ┌──────────────┐   ┌──────────────┐        ┌─────────────────────────────┐
/// │ ┌──────────┐ │   │ ┌──────────┐ │        │ ┌────────────────────────┐  │
/// │ │  [A, X]  │ │   │ │    []    │ │        │ │    [[A, X], []]        │  │
/// │ ├──────────┤ │   │ ├──────────┤ │        │ ├────────────────────────┤  │
/// │ │[NULL, Y] │ │   │ │[Q, R, S] │ │───────▶│ │ [[NULL, Y], [Q, R, S]] │  │
/// │ ├──────────┤ │   │ ├──────────┤ │        │ ├────────────────────────│  │
/// │ │  [C, Z]  │ │   │ │   NULL   │ │        │ │    [[C, Z], NULL]      │  │
/// │ └──────────┘ │   │ └──────────┘ │        │ └────────────────────────┘  │
/// └──────────────┘   └──────────────┘        └─────────────────────────────┘
///      col1               col2                         output
/// ```
fn array_array<O: OffsetSizeTrait>(
    args: &[ArrayRef],
    data_type: DataType,
) -> Result<ArrayRef> {
    // do not accept 0 arguments.
    if args.is_empty() {
        return plan_err!("Array requires at least one argument");
    }

    let mut data = vec![];
    let mut total_len = 0;
    for arg in args {
        let arg_data = if arg.as_any().is::<NullArray>() {
            ArrayData::new_empty(&data_type)
        } else {
            arg.to_data()
        };
        total_len += arg_data.len();
        data.push(arg_data);
    }

    let mut offsets: Vec<O> = Vec::with_capacity(total_len);
    offsets.push(O::usize_as(0));

    let capacity = Capacities::Array(total_len);
    let data_ref = data.iter().collect::<Vec<_>>();
    let mut mutable = MutableArrayData::with_capacities(data_ref, true, capacity);

    let num_rows = args[0].len();
    for row_idx in 0..num_rows {
        for (arr_idx, arg) in args.iter().enumerate() {
            if !arg.as_any().is::<NullArray>()
                && !arg.is_null(row_idx)
                && arg.is_valid(row_idx)
            {
                mutable.extend(arr_idx, row_idx, row_idx + 1);
            } else {
                mutable.extend_nulls(1);
            }
        }
        offsets.push(O::usize_as(mutable.len()));
    }
    let data = mutable.freeze();

    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::new(Field::new("item", data_type, true)),
        OffsetBuffer::new(offsets.into()),
        arrow_array::make_array(data),
        None,
    )?))
}

/// `make_array` SQL function
pub fn make_array(arrays: &[ArrayRef]) -> Result<ArrayRef> {
    let mut data_type = DataType::Null;
    for arg in arrays {
        let arg_data_type = arg.data_type();
        if !arg_data_type.equals_datatype(&DataType::Null) {
            data_type = arg_data_type.clone();
            break;
        }
    }

    match data_type {
        // Either an empty array or all nulls:
        DataType::Null => {
            let array =
                new_null_array(&DataType::Null, arrays.iter().map(|a| a.len()).sum());
            Ok(Arc::new(array_into_list_array(array)))
        }
        DataType::LargeList(..) => array_array::<i64>(arrays, data_type),
        _ => array_array::<i32>(arrays, data_type),
    }
}

fn general_except<OffsetSize: OffsetSizeTrait>(
    l: &GenericListArray<OffsetSize>,
    r: &GenericListArray<OffsetSize>,
    field: &FieldRef,
) -> Result<GenericListArray<OffsetSize>> {
    let converter = RowConverter::new(vec![SortField::new(l.value_type())])?;

    let l_values = l.values().to_owned();
    let r_values = r.values().to_owned();
    let l_values = converter.convert_columns(&[l_values])?;
    let r_values = converter.convert_columns(&[r_values])?;

    let mut offsets = Vec::<OffsetSize>::with_capacity(l.len() + 1);
    offsets.push(OffsetSize::usize_as(0));

    let mut rows = Vec::with_capacity(l_values.num_rows());
    let mut dedup = HashSet::new();

    for (l_w, r_w) in l.offsets().windows(2).zip(r.offsets().windows(2)) {
        let l_slice = l_w[0].as_usize()..l_w[1].as_usize();
        let r_slice = r_w[0].as_usize()..r_w[1].as_usize();
        for i in r_slice {
            let right_row = r_values.row(i);
            dedup.insert(right_row);
        }
        for i in l_slice {
            let left_row = l_values.row(i);
            if dedup.insert(left_row) {
                rows.push(left_row);
            }
        }

        offsets.push(OffsetSize::usize_as(rows.len()));
        dedup.clear();
    }

    if let Some(values) = converter.convert_rows(rows)?.first() {
        Ok(GenericListArray::<OffsetSize>::new(
            field.to_owned(),
            OffsetBuffer::new(offsets.into()),
            values.to_owned(),
            l.nulls().cloned(),
        ))
    } else {
        internal_err!("array_except failed to convert rows")
    }
}

pub fn array_except(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_except needs two arguments");
    }

    let array1 = &args[0];
    let array2 = &args[1];

    match (array1.data_type(), array2.data_type()) {
        (DataType::Null, _) | (_, DataType::Null) => Ok(array1.to_owned()),
        (DataType::List(field), DataType::List(_)) => {
            check_datatypes("array_except", &[array1, array2])?;
            let list1 = array1.as_list::<i32>();
            let list2 = array2.as_list::<i32>();
            let result = general_except::<i32>(list1, list2, field)?;
            Ok(Arc::new(result))
        }
        (DataType::LargeList(field), DataType::LargeList(_)) => {
            check_datatypes("array_except", &[array1, array2])?;
            let list1 = array1.as_list::<i64>();
            let list2 = array2.as_list::<i64>();
            let result = general_except::<i64>(list1, list2, field)?;
            Ok(Arc::new(result))
        }
        (dt1, dt2) => {
            internal_err!("array_except got unexpected types: {dt1:?} and {dt2:?}")
        }
    }
}

/// Array_position SQL function
pub fn array_position(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!("array_position expects two or three arguments");
    }
    match &args[0].data_type() {
        DataType::List(_) => general_position_dispatch::<i32>(args),
        DataType::LargeList(_) => general_position_dispatch::<i64>(args),
        array_type => exec_err!("array_position does not support type '{array_type:?}'."),
    }
}
fn general_position_dispatch<O: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let list_array = as_generic_list_array::<O>(&args[0])?;
    let element_array = &args[1];

    check_datatypes("array_position", &[list_array.values(), element_array])?;

    let arr_from = if args.len() == 3 {
        as_int64_array(&args[2])?
            .values()
            .to_vec()
            .iter()
            .map(|&x| x - 1)
            .collect::<Vec<_>>()
    } else {
        vec![0; list_array.len()]
    };

    // if `start_from` index is out of bounds, return error
    for (arr, &from) in list_array.iter().zip(arr_from.iter()) {
        if let Some(arr) = arr {
            if from < 0 || from as usize >= arr.len() {
                return internal_err!("start_from index out of bounds");
            }
        } else {
            // We will get null if we got null in the array, so we don't need to check
        }
    }

    generic_position::<O>(list_array, element_array, arr_from)
}

fn generic_position<OffsetSize: OffsetSizeTrait>(
    list_array: &GenericListArray<OffsetSize>,
    element_array: &ArrayRef,
    arr_from: Vec<i64>, // 0-indexed
) -> Result<ArrayRef> {
    let mut data = Vec::with_capacity(list_array.len());

    for (row_index, (list_array_row, &from)) in
        list_array.iter().zip(arr_from.iter()).enumerate()
    {
        let from = from as usize;

        if let Some(list_array_row) = list_array_row {
            let eq_array =
                compare_element_to_list(&list_array_row, element_array, row_index, true)?;

            // Collect `true`s in 1-indexed positions
            let index = eq_array
                .iter()
                .skip(from)
                .position(|e| e == Some(true))
                .map(|index| (from + index + 1) as u64);

            data.push(index);
        } else {
            data.push(None);
        }
    }

    Ok(Arc::new(UInt64Array::from(data)))
}

/// Array_positions SQL function
pub fn array_positions(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_positions expects two arguments");
    }

    let element = &args[1];

    match &args[0].data_type() {
        DataType::List(_) => {
            let arr = as_list_array(&args[0])?;
            check_datatypes("array_positions", &[arr.values(), element])?;
            general_positions::<i32>(arr, element)
        }
        DataType::LargeList(_) => {
            let arr = as_large_list_array(&args[0])?;
            check_datatypes("array_positions", &[arr.values(), element])?;
            general_positions::<i64>(arr, element)
        }
        array_type => {
            exec_err!("array_positions does not support type '{array_type:?}'.")
        }
    }
}

fn general_positions<OffsetSize: OffsetSizeTrait>(
    list_array: &GenericListArray<OffsetSize>,
    element_array: &ArrayRef,
) -> Result<ArrayRef> {
    let mut data = Vec::with_capacity(list_array.len());

    for (row_index, list_array_row) in list_array.iter().enumerate() {
        if let Some(list_array_row) = list_array_row {
            let eq_array =
                compare_element_to_list(&list_array_row, element_array, row_index, true)?;

            // Collect `true`s in 1-indexed positions
            let indexes = eq_array
                .iter()
                .positions(|e| e == Some(true))
                .map(|index| Some(index as u64 + 1))
                .collect::<Vec<_>>();

            data.push(Some(indexes));
        } else {
            data.push(None);
        }
    }

    Ok(Arc::new(
        ListArray::from_iter_primitive::<UInt64Type, _, _>(data),
    ))
}

/// For each element of `list_array[i]`, removed up to `arr_n[i]`  occurences
/// of `element_array[i]`.
///
/// The type of each **element** in `list_array` must be the same as the type of
/// `element_array`. This function also handles nested arrays
/// ([`ListArray`] of [`ListArray`]s)
///
/// For example, when called to remove a list array (where each element is a
/// list of int32s, the second argument are int32 arrays, and the
/// third argument is the number of occurrences to remove
///
/// ```text
/// general_remove(
///   [1, 2, 3, 2], 2, 1    ==> [1, 3, 2]   (only the first 2 is removed)
///   [4, 5, 6, 5], 5, 2    ==> [4, 6]  (both 5s are removed)
/// )
/// ```
fn general_remove<OffsetSize: OffsetSizeTrait>(
    list_array: &GenericListArray<OffsetSize>,
    element_array: &ArrayRef,
    arr_n: Vec<i64>,
) -> Result<ArrayRef> {
    let data_type = list_array.value_type();
    let mut new_values = vec![];
    // Build up the offsets for the final output array
    let mut offsets = Vec::<OffsetSize>::with_capacity(arr_n.len() + 1);
    offsets.push(OffsetSize::zero());

    // n is the number of elements to remove in this row
    for (row_index, (list_array_row, n)) in
        list_array.iter().zip(arr_n.iter()).enumerate()
    {
        match list_array_row {
            Some(list_array_row) => {
                let eq_array = compare_element_to_list(
                    &list_array_row,
                    element_array,
                    row_index,
                    false,
                )?;

                // We need to keep at most first n elements as `false`, which represent the elements to remove.
                let eq_array = if eq_array.false_count() < *n as usize {
                    eq_array
                } else {
                    let mut count = 0;
                    eq_array
                        .iter()
                        .map(|e| {
                            // Keep first n `false` elements, and reverse other elements to `true`.
                            if let Some(false) = e {
                                if count < *n {
                                    count += 1;
                                    e
                                } else {
                                    Some(true)
                                }
                            } else {
                                e
                            }
                        })
                        .collect::<BooleanArray>()
                };

                let filtered_array = arrow::compute::filter(&list_array_row, &eq_array)?;
                offsets.push(
                    offsets[row_index] + OffsetSize::usize_as(filtered_array.len()),
                );
                new_values.push(filtered_array);
            }
            None => {
                // Null element results in a null row (no new offsets)
                offsets.push(offsets[row_index]);
            }
        }
    }

    let values = if new_values.is_empty() {
        new_empty_array(&data_type)
    } else {
        let new_values = new_values.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
        arrow::compute::concat(&new_values)?
    };

    Ok(Arc::new(GenericListArray::<OffsetSize>::try_new(
        Arc::new(Field::new("item", data_type, true)),
        OffsetBuffer::new(offsets.into()),
        values,
        list_array.nulls().cloned(),
    )?))
}

fn array_remove_internal(
    array: &ArrayRef,
    element_array: &ArrayRef,
    arr_n: Vec<i64>,
) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::List(_) => {
            let list_array = array.as_list::<i32>();
            general_remove::<i32>(list_array, element_array, arr_n)
        }
        DataType::LargeList(_) => {
            let list_array = array.as_list::<i64>();
            general_remove::<i64>(list_array, element_array, arr_n)
        }
        array_type => {
            exec_err!("array_remove_all does not support type '{array_type:?}'.")
        }
    }
}

pub fn array_remove_all(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_remove_all expects two arguments");
    }

    let arr_n = vec![i64::MAX; args[0].len()];
    array_remove_internal(&args[0], &args[1], arr_n)
}

pub fn array_remove(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_remove expects two arguments");
    }

    let arr_n = vec![1; args[0].len()];
    array_remove_internal(&args[0], &args[1], arr_n)
}

pub fn array_remove_n(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 3 {
        return exec_err!("array_remove_n expects three arguments");
    }

    let arr_n = as_int64_array(&args[2])?.values().to_vec();
    array_remove_internal(&args[0], &args[1], arr_n)
}

/// For each element of `list_array[i]`, replaces up to `arr_n[i]`  occurences
/// of `from_array[i]`, `to_array[i]`.
///
/// The type of each **element** in `list_array` must be the same as the type of
/// `from_array` and `to_array`. This function also handles nested arrays
/// ([`ListArray`] of [`ListArray`]s)
///
/// For example, when called to replace a list array (where each element is a
/// list of int32s, the second and third argument are int32 arrays, and the
/// fourth argument is the number of occurrences to replace
///
/// ```text
/// general_replace(
///   [1, 2, 3, 2], 2, 10, 1    ==> [1, 10, 3, 2]   (only the first 2 is replaced)
///   [4, 5, 6, 5], 5, 20, 2    ==> [4, 20, 6, 20]  (both 5s are replaced)
/// )
/// ```
fn general_replace<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
    from_array: &ArrayRef,
    to_array: &ArrayRef,
    arr_n: Vec<i64>,
) -> Result<ArrayRef> {
    // Build up the offsets for the final output array
    let mut offsets: Vec<O> = vec![O::usize_as(0)];
    let values = list_array.values();
    let original_data = values.to_data();
    let to_data = to_array.to_data();
    let capacity = Capacities::Array(original_data.len());

    // First array is the original array, second array is the element to replace with.
    let mut mutable = MutableArrayData::with_capacities(
        vec![&original_data, &to_data],
        false,
        capacity,
    );

    let mut valid = BooleanBufferBuilder::new(list_array.len());

    for (row_index, offset_window) in list_array.offsets().windows(2).enumerate() {
        if list_array.is_null(row_index) {
            offsets.push(offsets[row_index]);
            valid.append(false);
            continue;
        }

        let start = offset_window[0];
        let end = offset_window[1];

        let list_array_row = list_array.value(row_index);

        // Compute all positions in list_row_array (that is itself an
        // array) that are equal to `from_array_row`
        let eq_array =
            compare_element_to_list(&list_array_row, &from_array, row_index, true)?;

        let original_idx = O::usize_as(0);
        let replace_idx = O::usize_as(1);
        let n = arr_n[row_index];
        let mut counter = 0;

        // All elements are false, no need to replace, just copy original data
        if eq_array.false_count() == eq_array.len() {
            mutable.extend(
                original_idx.to_usize().unwrap(),
                start.to_usize().unwrap(),
                end.to_usize().unwrap(),
            );
            offsets.push(offsets[row_index] + (end - start));
            valid.append(true);
            continue;
        }

        for (i, to_replace) in eq_array.iter().enumerate() {
            let i = O::usize_as(i);
            if let Some(true) = to_replace {
                mutable.extend(replace_idx.to_usize().unwrap(), row_index, row_index + 1);
                counter += 1;
                if counter == n {
                    // copy original data for any matches past n
                    mutable.extend(
                        original_idx.to_usize().unwrap(),
                        (start + i).to_usize().unwrap() + 1,
                        end.to_usize().unwrap(),
                    );
                    break;
                }
            } else {
                // copy original data for false / null matches
                mutable.extend(
                    original_idx.to_usize().unwrap(),
                    (start + i).to_usize().unwrap(),
                    (start + i).to_usize().unwrap() + 1,
                );
            }
        }

        offsets.push(offsets[row_index] + (end - start));
        valid.append(true);
    }

    let data = mutable.freeze();

    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::new(Field::new("item", list_array.value_type(), true)),
        OffsetBuffer::<O>::new(offsets.into()),
        arrow_array::make_array(data),
        Some(NullBuffer::new(valid.finish())),
    )?))
}

pub fn array_replace(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 3 {
        return exec_err!("array_replace expects three arguments");
    }

    // replace at most one occurence for each element
    let arr_n = vec![1; args[0].len()];
    let array = &args[0];
    match array.data_type() {
        DataType::List(_) => {
            let list_array = array.as_list::<i32>();
            general_replace::<i32>(list_array, &args[1], &args[2], arr_n)
        }
        DataType::LargeList(_) => {
            let list_array = array.as_list::<i64>();
            general_replace::<i64>(list_array, &args[1], &args[2], arr_n)
        }
        array_type => exec_err!("array_replace does not support type '{array_type:?}'."),
    }
}

pub fn array_replace_n(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 4 {
        return exec_err!("array_replace_n expects four arguments");
    }

    // replace the specified number of occurences
    let arr_n = as_int64_array(&args[3])?.values().to_vec();
    let array = &args[0];
    match array.data_type() {
        DataType::List(_) => {
            let list_array = array.as_list::<i32>();
            general_replace::<i32>(list_array, &args[1], &args[2], arr_n)
        }
        DataType::LargeList(_) => {
            let list_array = array.as_list::<i64>();
            general_replace::<i64>(list_array, &args[1], &args[2], arr_n)
        }
        array_type => {
            exec_err!("array_replace_n does not support type '{array_type:?}'.")
        }
    }
}

pub fn array_replace_all(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 3 {
        return exec_err!("array_replace_all expects three arguments");
    }

    // replace all occurrences (up to "i64::MAX")
    let arr_n = vec![i64::MAX; args[0].len()];
    let array = &args[0];
    match array.data_type() {
        DataType::List(_) => {
            let list_array = array.as_list::<i32>();
            general_replace::<i32>(list_array, &args[1], &args[2], arr_n)
        }
        DataType::LargeList(_) => {
            let list_array = array.as_list::<i64>();
            general_replace::<i64>(list_array, &args[1], &args[2], arr_n)
        }
        array_type => {
            exec_err!("array_replace_all does not support type '{array_type:?}'.")
        }
    }
}

#[derive(Debug, PartialEq)]
enum SetOp {
    Union,
    Intersect,
}

impl Display for SetOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SetOp::Union => write!(f, "array_union"),
            SetOp::Intersect => write!(f, "array_intersect"),
        }
    }
}

fn generic_set_lists<OffsetSize: OffsetSizeTrait>(
    l: &GenericListArray<OffsetSize>,
    r: &GenericListArray<OffsetSize>,
    field: Arc<Field>,
    set_op: SetOp,
) -> Result<ArrayRef> {
    if matches!(l.value_type(), DataType::Null) {
        let field = Arc::new(Field::new("item", r.value_type(), true));
        return general_array_distinct::<OffsetSize>(r, &field);
    } else if matches!(r.value_type(), DataType::Null) {
        let field = Arc::new(Field::new("item", l.value_type(), true));
        return general_array_distinct::<OffsetSize>(l, &field);
    }

    if l.value_type() != r.value_type() {
        return internal_err!("{set_op:?} is not implemented for '{l:?}' and '{r:?}'");
    }

    let dt = l.value_type();

    let mut offsets = vec![OffsetSize::usize_as(0)];
    let mut new_arrays = vec![];

    let converter = RowConverter::new(vec![SortField::new(dt)])?;
    for (first_arr, second_arr) in l.iter().zip(r.iter()) {
        if let (Some(first_arr), Some(second_arr)) = (first_arr, second_arr) {
            let l_values = converter.convert_columns(&[first_arr])?;
            let r_values = converter.convert_columns(&[second_arr])?;

            let l_iter = l_values.iter().sorted().dedup();
            let values_set: HashSet<_> = l_iter.clone().collect();
            let mut rows = if set_op == SetOp::Union {
                l_iter.collect::<Vec<_>>()
            } else {
                vec![]
            };
            for r_val in r_values.iter().sorted().dedup() {
                match set_op {
                    SetOp::Union => {
                        if !values_set.contains(&r_val) {
                            rows.push(r_val);
                        }
                    }
                    SetOp::Intersect => {
                        if values_set.contains(&r_val) {
                            rows.push(r_val);
                        }
                    }
                }
            }

            let last_offset = match offsets.last().copied() {
                Some(offset) => offset,
                None => return internal_err!("offsets should not be empty"),
            };
            offsets.push(last_offset + OffsetSize::usize_as(rows.len()));
            let arrays = converter.convert_rows(rows)?;
            let array = match arrays.first() {
                Some(array) => array.clone(),
                None => {
                    return internal_err!("{set_op}: failed to get array from rows");
                }
            };
            new_arrays.push(array);
        }
    }

    let offsets = OffsetBuffer::new(offsets.into());
    let new_arrays_ref = new_arrays.iter().map(|v| v.as_ref()).collect::<Vec<_>>();
    let values = compute::concat(&new_arrays_ref)?;
    let arr = GenericListArray::<OffsetSize>::try_new(field, offsets, values, None)?;
    Ok(Arc::new(arr))
}

fn general_set_op(
    array1: &ArrayRef,
    array2: &ArrayRef,
    set_op: SetOp,
) -> Result<ArrayRef> {
    match (array1.data_type(), array2.data_type()) {
        (DataType::Null, DataType::List(field)) => {
            if set_op == SetOp::Intersect {
                return Ok(new_empty_array(&DataType::Null));
            }
            let array = as_list_array(&array2)?;
            general_array_distinct::<i32>(array, field)
        }

        (DataType::List(field), DataType::Null) => {
            if set_op == SetOp::Intersect {
                return make_array(&[]);
            }
            let array = as_list_array(&array1)?;
            general_array_distinct::<i32>(array, field)
        }
        (DataType::Null, DataType::LargeList(field)) => {
            if set_op == SetOp::Intersect {
                return Ok(new_empty_array(&DataType::Null));
            }
            let array = as_large_list_array(&array2)?;
            general_array_distinct::<i64>(array, field)
        }
        (DataType::LargeList(field), DataType::Null) => {
            if set_op == SetOp::Intersect {
                return make_array(&[]);
            }
            let array = as_large_list_array(&array1)?;
            general_array_distinct::<i64>(array, field)
        }
        (DataType::Null, DataType::Null) => Ok(new_empty_array(&DataType::Null)),

        (DataType::List(field), DataType::List(_)) => {
            let array1 = as_list_array(&array1)?;
            let array2 = as_list_array(&array2)?;
            generic_set_lists::<i32>(array1, array2, field.clone(), set_op)
        }
        (DataType::LargeList(field), DataType::LargeList(_)) => {
            let array1 = as_large_list_array(&array1)?;
            let array2 = as_large_list_array(&array2)?;
            generic_set_lists::<i64>(array1, array2, field.clone(), set_op)
        }
        (data_type1, data_type2) => {
            internal_err!(
                "{set_op} does not support types '{data_type1:?}' and '{data_type2:?}'"
            )
        }
    }
}

/// Array_union SQL function
pub fn array_union(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_union needs two arguments");
    }
    let array1 = &args[0];
    let array2 = &args[1];

    general_set_op(array1, array2, SetOp::Union)
}

/// array_intersect SQL function
pub fn array_intersect(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_intersect needs two arguments");
    }

    let array1 = &args[0];
    let array2 = &args[1];

    general_set_op(array1, array2, SetOp::Intersect)
}

pub fn general_array_distinct<OffsetSize: OffsetSizeTrait>(
    array: &GenericListArray<OffsetSize>,
    field: &FieldRef,
) -> Result<ArrayRef> {
    let dt = array.value_type();
    let mut offsets = Vec::with_capacity(array.len());
    offsets.push(OffsetSize::usize_as(0));
    let mut new_arrays = Vec::with_capacity(array.len());
    let converter = RowConverter::new(vec![SortField::new(dt)])?;
    // distinct for each list in ListArray
    for arr in array.iter().flatten() {
        let values = converter.convert_columns(&[arr])?;
        // sort elements in list and remove duplicates
        let rows = values.iter().sorted().dedup().collect::<Vec<_>>();
        let last_offset: OffsetSize = offsets.last().copied().unwrap();
        offsets.push(last_offset + OffsetSize::usize_as(rows.len()));
        let arrays = converter.convert_rows(rows)?;
        let array = match arrays.first() {
            Some(array) => array.clone(),
            None => {
                return internal_err!("array_distinct: failed to get array from rows")
            }
        };
        new_arrays.push(array);
    }
    let offsets = OffsetBuffer::new(offsets.into());
    let new_arrays_ref = new_arrays.iter().map(|v| v.as_ref()).collect::<Vec<_>>();
    let values = compute::concat(&new_arrays_ref)?;
    Ok(Arc::new(GenericListArray::<OffsetSize>::try_new(
        field.clone(),
        offsets,
        values,
        None,
    )?))
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
