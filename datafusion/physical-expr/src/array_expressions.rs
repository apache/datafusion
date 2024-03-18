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

use std::sync::Arc;

use arrow::array::*;
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use arrow_buffer::NullBuffer;

use datafusion_common::cast::{as_int64_array, as_large_list_array, as_list_array};
use datafusion_common::utils::array_into_list_array;
use datafusion_common::{exec_err, plan_err, Result};

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
