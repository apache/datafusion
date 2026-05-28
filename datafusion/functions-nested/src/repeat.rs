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

//! [`ScalarUDFImpl`] definitions for array_repeat function.

use crate::utils::make_scalar_function;
use arrow::array::{
    Array, ArrayRef, BooleanBufferBuilder, GenericListArray, Int64Array, OffsetSizeTrait,
    UInt64Array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute;
use arrow::datatypes::DataType;
use arrow::datatypes::{
    DataType::{LargeList, List},
    Field,
};
use datafusion_common::cast::{as_int64_array, as_large_list_array, as_list_array};
use datafusion_common::types::{NativeType, logical_int64};
use datafusion_common::{Result, exec_datafusion_err};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};
use datafusion_macros::user_doc;
use std::mem::size_of;
use std::sync::Arc;

const ARRAY_REPEAT_LENGTH_EXCEEDED: &str =
    "array_repeat: requested length exceeds maximum array size";

make_udf_expr_and_func!(
    ArrayRepeat,
    array_repeat,
    element count, // arg name
    "returns an array containing element `count` times.", // doc
    array_repeat_udf // internal function name
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns an array containing element `count` times.",
    syntax_example = "array_repeat(element, count)",
    sql_example = r#"```sql
> select array_repeat(1, 3);
+---------------------------------+
| array_repeat(Int64(1),Int64(3)) |
+---------------------------------+
| [1, 1, 1]                       |
+---------------------------------+
> select array_repeat([1, 2], 2);
+------------------------------------+
| array_repeat(List([1,2]),Int64(2)) |
+------------------------------------+
| [[1, 2], [1, 2]]                   |
+------------------------------------+
```"#,
    argument(
        name = "element",
        description = "Element expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(
        name = "count",
        description = "Value of how many times to repeat the element."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayRepeat {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayRepeat {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayRepeat {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![
                    Coercion::new_exact(TypeSignatureClass::Any),
                    Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_int64()),
                        vec![TypeSignatureClass::Integer],
                        NativeType::Int64,
                    ),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("list_repeat")],
        }
    }
}

impl ScalarUDFImpl for ArrayRepeat {
    fn name(&self) -> &str {
        "array_repeat"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let element_type = &arg_types[0];
        match element_type {
            LargeList(_) => Ok(LargeList(Arc::new(Field::new_list_field(
                element_type.clone(),
                true,
            )))),
            _ => Ok(List(Arc::new(Field::new_list_field(
                element_type.clone(),
                true,
            )))),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(array_repeat_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn array_repeat_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let element = &args[0];
    let count_array = as_int64_array(&args[1])?;

    match element.data_type() {
        List(_) => {
            let list_array = as_list_array(element)?;
            general_list_repeat::<i32>(list_array, count_array)
        }
        LargeList(_) => {
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
    let total_repeated_values =
        (0..count_array.len()).try_fold(0usize, |total, idx| {
            total
                .checked_add(repeat_count(count_array, idx).unwrap_or_default())
                .ok_or_else(|| {
                    exec_datafusion_err!(
                        "array_repeat: total repeated values overflowed usize"
                    )
                })
        })?;
    ensure_repeated_values_fit::<O>(total_repeated_values)?;
    let (offsets, _) = build_repeat_offsets::<O>(count_array)?;

    let mut take_indices = Vec::with_capacity(total_repeated_values);

    for idx in 0..count_array.len() {
        let Some(count) = repeat_count(count_array, idx) else {
            continue;
        };
        take_indices.extend(std::iter::repeat_n(idx as u64, count));
    }

    // Build the flattened values
    let repeated_values = compute::take(
        array.as_ref(),
        &UInt64Array::from_iter_values(take_indices),
        None,
    )?;

    // Construct final ListArray
    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::new(Field::new_list_field(array.data_type().to_owned(), true)),
        OffsetBuffer::new(offsets.into()),
        repeated_values,
        count_array.nulls().cloned(),
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
    let list_offsets = list_array.value_offsets();
    let (outer_offsets, outer_total) = build_repeat_offsets::<O>(count_array)?;

    // calculate capacities for pre-allocation
    let mut inner_total = 0usize;
    for i in 0..count_array.len() {
        let Some(count) = repeat_count(count_array, i) else {
            continue;
        };
        if count > 0 && list_array.is_valid(i) {
            let len = list_offsets[i + 1].to_usize().unwrap()
                - list_offsets[i].to_usize().unwrap();
            inner_total =
                checked_repeat_len_add(inner_total, checked_repeat_len_mul(len, count)?)?;
            ensure_repeated_values_fit::<O>(inner_total)?;
        }
    }

    // Build inner structures
    let inner_offsets_capacity = checked_offset_slots_capacity::<O>(outer_total)?;
    let mut inner_offsets = Vec::with_capacity(inner_offsets_capacity);
    let mut take_indices = Vec::with_capacity(inner_total);
    let mut inner_nulls = BooleanBufferBuilder::new(outer_total);
    let mut inner_running = 0usize;
    inner_offsets.push(O::zero());

    for row_idx in 0..count_array.len() {
        let Some(count) = repeat_count(count_array, row_idx) else {
            continue;
        };
        let list_is_valid = list_array.is_valid(row_idx);
        let start = list_offsets[row_idx].to_usize().unwrap();
        let end = list_offsets[row_idx + 1].to_usize().unwrap();
        let row_len = end - start;

        for _ in 0..count {
            inner_running = checked_repeat_len_add(inner_running, row_len)?;
            ensure_repeated_values_fit::<O>(inner_running)?;
            let offset = checked_repeat_offset::<O>(inner_running)?;
            inner_offsets.push(offset);
            inner_nulls.append(list_is_valid);
            if list_is_valid {
                take_indices.extend(start as u64..end as u64);
            }
        }
    }

    // Build inner ListArray
    let inner_values = compute::take(
        list_array.values().as_ref(),
        &UInt64Array::from_iter_values(take_indices),
        None,
    )?;
    let inner_list = GenericListArray::<O>::try_new(
        Arc::new(Field::new_list_field(list_array.value_type().clone(), true)),
        OffsetBuffer::new(inner_offsets.into()),
        inner_values,
        Some(NullBuffer::new(inner_nulls.finish())),
    )?;

    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::new(Field::new_list_field(
            list_array.data_type().to_owned(),
            true,
        )),
        OffsetBuffer::new(outer_offsets.into()),
        Arc::new(inner_list),
        count_array.nulls().cloned(),
    )?))
}

fn build_repeat_offsets<O: OffsetSizeTrait>(
    count_array: &Int64Array,
) -> Result<(Vec<O>, usize)> {
    let offsets_capacity = checked_offset_slots_capacity::<O>(count_array.len())?;
    let mut offsets = Vec::with_capacity(offsets_capacity);
    offsets.push(O::zero());
    let mut running_offset = 0usize;

    for idx in 0..count_array.len() {
        let Some(count) = repeat_count(count_array, idx) else {
            offsets.push(*offsets.last().unwrap());
            continue;
        };
        running_offset = checked_repeat_len_add(running_offset, count)?;
        ensure_repeated_values_fit::<O>(running_offset)?;
        let offset = checked_repeat_offset::<O>(running_offset)?;
        offsets.push(offset);
    }

    Ok((offsets, running_offset))
}

fn checked_repeat_len_add(lhs: usize, rhs: usize) -> Result<usize> {
    lhs.checked_add(rhs)
        .ok_or_else(|| exec_datafusion_err!("{}", ARRAY_REPEAT_LENGTH_EXCEEDED))
}

fn checked_repeat_len_mul(lhs: usize, rhs: usize) -> Result<usize> {
    lhs.checked_mul(rhs)
        .ok_or_else(|| exec_datafusion_err!("{}", ARRAY_REPEAT_LENGTH_EXCEEDED))
}

fn ensure_repeated_values_fit<O: OffsetSizeTrait>(len: usize) -> Result<()> {
    ensure_vec_capacity::<u64>(len)?;
    checked_repeat_offset::<O>(len)?;

    Ok(())
}

fn ensure_vec_capacity<T>(len: usize) -> Result<()> {
    if len > max_vec_elements::<T>() {
        return Err(exec_datafusion_err!("{}", ARRAY_REPEAT_LENGTH_EXCEEDED));
    }

    Ok(())
}

fn checked_offset_slots_capacity<O>(len: usize) -> Result<usize> {
    let capacity = checked_repeat_len_add(len, 1)?;
    ensure_vec_capacity::<O>(capacity)?;

    Ok(capacity)
}

fn checked_repeat_offset<O: OffsetSizeTrait>(offset: usize) -> Result<O> {
    O::from_usize(offset).ok_or_else(|| {
        exec_datafusion_err!(
            "array_repeat: offset {offset} exceeds the maximum value for offset type"
        )
    })
}

fn max_vec_elements<T>() -> usize {
    let element_size = size_of::<T>();
    (isize::MAX as usize)
        .checked_div(element_size)
        .unwrap_or(usize::MAX)
}

/// Helper function to get count from count_array at given index.
/// Returns `None` for NULL values and `Some(0)` for non-positive counts.
#[inline]
fn repeat_count(count_array: &Int64Array, idx: usize) -> Option<usize> {
    if count_array.is_null(idx) {
        None
    } else {
        let c = count_array.value(idx);
        Some(if c > 0 { c as usize } else { 0 })
    }
}

#[cfg(test)]
mod tests {
    use super::{array_repeat_inner, general_list_repeat, general_repeat};
    use arrow::array::{Array, ArrayRef, AsArray, Int32Array, Int64Array, ListArray};
    use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow::datatypes::{Field, Int32Type};
    use datafusion_common::Result;
    use std::sync::Arc;

    #[test]
    fn test_array_repeat_null_count_stays_null() -> Result<()> {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let counts = Int64Array::new(
            ScalarBuffer::from(vec![2, 1, 1]),
            Some(NullBuffer::from(vec![true, false, true])),
        );

        let result = general_repeat::<i32>(&array, &counts)?;
        let expected = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(1)]),
            None,
            Some(vec![Some(3)]),
        ]);

        assert_eq!(result.as_list::<i32>(), &expected);

        Ok(())
    }

    #[test]
    fn test_array_repeat_nested_null_count_stays_null() -> Result<()> {
        let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3), Some(4)]),
            Some(vec![Some(5)]),
        ]);
        let counts = Int64Array::new(
            ScalarBuffer::from(vec![2, 1, 1]),
            Some(NullBuffer::from(vec![true, false, true])),
        );

        let result = general_list_repeat::<i32>(&list_array, &counts)?;
        let repeated_values = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(5)]),
        ]);
        let expected = ListArray::new(
            Arc::new(Field::new_list_field(
                repeated_values.data_type().clone(),
                true,
            )),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 3])),
            Arc::new(repeated_values),
            Some(NullBuffer::from(vec![true, false, true])),
        );

        assert_eq!(result.as_list::<i32>(), &expected);

        Ok(())
    }

    #[test]
    fn scalar_count_exceeding_max_array_size_returns_error() {
        let element: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let count: ArrayRef = Arc::new(Int64Array::from(vec![i64::MAX]));

        let err = array_repeat_inner(&[element, count]).unwrap_err();
        assert!(
            err.to_string().starts_with(
                "Execution error: array_repeat: requested length exceeds maximum array size"
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn scalar_count_exceeding_list_offset_limit_returns_error() {
        let element: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let count: ArrayRef = Arc::new(Int64Array::from(vec![i32::MAX as i64 + 1]));

        let err = array_repeat_inner(&[element, count]).unwrap_err();
        assert!(
            err.to_string().starts_with(
                "Execution error: array_repeat: offset 2147483648 exceeds the maximum value for offset type"
            ),
            "unexpected error: {err}"
        );
    }
}
