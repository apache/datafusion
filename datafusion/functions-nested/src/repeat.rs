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
    Array, ArrayRef, BooleanBufferBuilder, GenericListArray, OffsetSizeTrait, UInt64Array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute;
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow::datatypes::{
    DataType::{LargeList, List},
    Field,
};
use datafusion_common::cast::{as_large_list_array, as_list_array, as_uint64_array};
use datafusion_common::{Result, exec_err, utils::take_function_args};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;
use std::sync::Arc;

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
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![String::from("list_repeat")],
        }
    }
}

impl ScalarUDFImpl for ArrayRepeat {
    fn as_any(&self) -> &dyn Any {
        self
    }

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

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        make_scalar_function(array_repeat_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [first_type, second_type] = take_function_args(self.name(), arg_types)?;

        // Coerce the second argument to Int64/UInt64 if it's a numeric type
        let second = match second_type {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                DataType::Int64
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                DataType::UInt64
            }
            _ => return exec_err!("count must be an integer type"),
        };

        Ok(vec![first_type.clone(), second])
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn array_repeat_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let element = &args[0];
    let count_array = &args[1];

    let count_array = match count_array.data_type() {
        DataType::Int64 => &cast(count_array, &DataType::UInt64)?,
        DataType::UInt64 => count_array,
        _ => return exec_err!("count must be an integer type"),
    };

    let count_array = as_uint64_array(count_array)?;

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
    count_array: &UInt64Array,
) -> Result<ArrayRef> {
    // Build offsets and take_indices
    let total_repeated_values: usize =
        count_array.values().iter().map(|&c| c as usize).sum();
    let mut take_indices = Vec::with_capacity(total_repeated_values);
    let mut offsets = Vec::with_capacity(count_array.len() + 1);
    offsets.push(O::zero());
    let mut running_offset = 0usize;

    for (idx, &count) in count_array.values().iter().enumerate() {
        let count = count as usize;
        running_offset += count;
        offsets.push(O::from_usize(running_offset).unwrap());
        take_indices.extend(std::iter::repeat_n(idx as u64, count))
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
    count_array: &UInt64Array,
) -> Result<ArrayRef> {
    let counts = count_array.values();
    let list_offsets = list_array.value_offsets();

    // calculate capacities for pre-allocation
    let outer_total = counts.iter().map(|&c| c as usize).sum();
    let inner_total = counts
        .iter()
        .enumerate()
        .filter(|&(i, _)| !list_array.is_null(i))
        .map(|(i, &c)| {
            let len = list_offsets[i + 1].to_usize().unwrap()
                - list_offsets[i].to_usize().unwrap();
            len * (c as usize)
        })
        .sum();

    // Build inner structures
    let mut inner_offsets = Vec::with_capacity(outer_total + 1);
    let mut take_indices = Vec::with_capacity(inner_total);
    let mut inner_nulls = BooleanBufferBuilder::new(outer_total);
    let mut inner_running = 0usize;
    inner_offsets.push(O::zero());

    for (row_idx, &count) in counts.iter().enumerate() {
        let is_valid = !list_array.is_null(row_idx);
        let start = list_offsets[row_idx].to_usize().unwrap();
        let end = list_offsets[row_idx + 1].to_usize().unwrap();
        let row_len = end - start;

        for _ in 0..count {
            inner_running += row_len;
            inner_offsets.push(O::from_usize(inner_running).unwrap());
            inner_nulls.append(is_valid);
            if is_valid {
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

    // Build outer ListArray
    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::new(Field::new_list_field(
            list_array.data_type().to_owned(),
            true,
        )),
        OffsetBuffer::<O>::from_lengths(counts.iter().map(|&c| c as usize)),
        Arc::new(inner_list),
        None,
    )?))
}
