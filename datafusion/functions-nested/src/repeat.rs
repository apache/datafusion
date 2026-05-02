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
    Field, FieldRef,
};
use datafusion_common::cast::{as_int64_array, as_large_list_array, as_list_array};
use datafusion_common::types::{NativeType, logical_int64};
use datafusion_common::utils::nullable_list_item_field_from;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, Volatility,
};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};
use datafusion_macros::user_doc;
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

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let element = &args.arg_fields[0];
        let inner = nullable_list_item_field_from(element);
        let data_type = match element.data_type() {
            LargeList(_) => LargeList(inner),
            _ => List(inner),
        };
        Ok(Arc::new(Field::new(self.name(), data_type, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let inner_field = match args.return_field.data_type() {
            List(field) | LargeList(field) | DataType::FixedSizeList(field, _) => {
                Some(Arc::clone(field))
            }
            _ => None,
        };
        make_scalar_function(move |arrays: &[ArrayRef]| {
            array_repeat_inner_with_field(arrays, inner_field.clone())
        })(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn array_repeat_inner_with_field(
    args: &[ArrayRef],
    inner_field: Option<FieldRef>,
) -> Result<ArrayRef> {
    let element = &args[0];
    let count_array = as_int64_array(&args[1])?;

    match element.data_type() {
        List(_) => {
            let list_array = as_list_array(element)?;
            general_list_repeat::<i32>(list_array, count_array, inner_field)
        }
        LargeList(_) => {
            let list_array = as_large_list_array(element)?;
            general_list_repeat::<i64>(list_array, count_array, inner_field)
        }
        _ => general_repeat::<i32>(element, count_array, inner_field),
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
    inner_field: Option<FieldRef>,
) -> Result<ArrayRef> {
    let total_repeated_values: usize = (0..count_array.len())
        .map(|i| get_count_with_validity(count_array, i))
        .sum();

    let mut take_indices = Vec::with_capacity(total_repeated_values);
    let mut offsets = Vec::with_capacity(count_array.len() + 1);
    offsets.push(O::zero());
    let mut running_offset = 0usize;

    for idx in 0..count_array.len() {
        let count = get_count_with_validity(count_array, idx);
        running_offset = running_offset.checked_add(count).ok_or_else(|| {
            DataFusionError::Execution(
                "array_repeat: running_offset overflowed usize".to_string(),
            )
        })?;
        let offset = O::from_usize(running_offset).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "array_repeat: offset {running_offset} exceeds the maximum value for offset type"
            ))
        })?;
        offsets.push(offset);
        take_indices.extend(std::iter::repeat_n(idx as u64, count));
    }

    // Build the flattened values
    let repeated_values = compute::take(
        array.as_ref(),
        &UInt64Array::from_iter_values(take_indices),
        None,
    )?;

    // Construct final ListArray
    let field = inner_field.unwrap_or_else(|| {
        Arc::new(Field::new_list_field(array.data_type().to_owned(), true))
    });
    Ok(Arc::new(GenericListArray::<O>::try_new(
        field,
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
    inner_field: Option<FieldRef>,
) -> Result<ArrayRef> {
    let list_offsets = list_array.value_offsets();

    // calculate capacities for pre-allocation
    let mut outer_total = 0usize;
    let mut inner_total = 0usize;
    for i in 0..count_array.len() {
        let count = get_count_with_validity(count_array, i);
        if count > 0 {
            outer_total += count;
            if list_array.is_valid(i) {
                let len = list_offsets[i + 1].to_usize().unwrap()
                    - list_offsets[i].to_usize().unwrap();
                inner_total += len * count;
            }
        }
    }

    // Build inner structures
    let mut inner_offsets = Vec::with_capacity(outer_total + 1);
    let mut take_indices = Vec::with_capacity(inner_total);
    let mut inner_nulls = BooleanBufferBuilder::new(outer_total);
    let mut inner_running = 0usize;
    inner_offsets.push(O::zero());

    for row_idx in 0..count_array.len() {
        let count = get_count_with_validity(count_array, row_idx);
        let list_is_valid = list_array.is_valid(row_idx);
        let start = list_offsets[row_idx].to_usize().unwrap();
        let end = list_offsets[row_idx + 1].to_usize().unwrap();
        let row_len = end - start;

        for _ in 0..count {
            inner_running = inner_running.checked_add(row_len).ok_or_else(|| {
                DataFusionError::Execution(
                    "array_repeat: inner offset overflowed usize".to_string(),
                )
            })?;
            let offset = O::from_usize(inner_running).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "array_repeat: offset {inner_running} exceeds the maximum value for offset type"
                ))
            })?;
            inner_offsets.push(offset);
            inner_nulls.append(list_is_valid);
            if list_is_valid {
                take_indices.extend(start as u64..end as u64);
            }
        }
    }

    // Build inner ListArray. Reuse the input list's element field directly so
    // its metadata (e.g. Arrow extension type) is preserved.
    let element_field = match list_array.data_type() {
        List(f) | LargeList(f) => Arc::clone(f),
        _ => Arc::new(Field::new_list_field(list_array.value_type().clone(), true)),
    };
    let inner_values = compute::take(
        list_array.values().as_ref(),
        &UInt64Array::from_iter_values(take_indices),
        None,
    )?;
    let inner_list = GenericListArray::<O>::try_new(
        element_field,
        OffsetBuffer::new(inner_offsets.into()),
        inner_values,
        Some(NullBuffer::new(inner_nulls.finish())),
    )?;

    // Build outer ListArray. Use the planning-time inner field if supplied so
    // metadata flows through.
    let outer_inner_field = inner_field.unwrap_or_else(|| {
        Arc::new(Field::new_list_field(
            list_array.data_type().to_owned(),
            true,
        ))
    });
    Ok(Arc::new(GenericListArray::<O>::try_new(
        outer_inner_field,
        OffsetBuffer::<O>::from_lengths(
            count_array
                .iter()
                .map(|c| c.map(|v| if v > 0 { v as usize } else { 0 }).unwrap_or(0)),
        ),
        Arc::new(inner_list),
        count_array.nulls().cloned(),
    )?))
}

/// Helper function to get count from count_array at given index
/// Return 0 for null values or non-positive count.
#[inline]
fn get_count_with_validity(count_array: &Int64Array, idx: usize) -> usize {
    if count_array.is_null(idx) {
        0
    } else {
        let c = count_array.value(idx);
        if c > 0 { c as usize } else { 0 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_expr::ReturnFieldArgs;
    use std::collections::HashMap;

    /// Regression test for #21982: `array_repeat` must propagate the input
    /// field's metadata onto the resulting list's inner field.
    #[test]
    fn array_repeat_preserves_inner_field_metadata() -> Result<()> {
        let metadata = HashMap::from([(
            "ARROW:extension:name".to_string(),
            "arrow.uuid".to_string(),
        )]);
        let element: FieldRef =
            Arc::new(Field::new("v", DataType::Int64, true).with_metadata(metadata));
        let count: FieldRef = Arc::new(Field::new("n", DataType::Int64, true));
        let scalar_args: Vec<Option<&datafusion_common::ScalarValue>> = vec![None, None];
        let arg_fields = vec![Arc::clone(&element), Arc::clone(&count)];

        let return_field =
            ArrayRepeat::default().return_field_from_args(ReturnFieldArgs {
                arg_fields: &arg_fields,
                scalar_arguments: &scalar_args,
            })?;
        let List(inner) = return_field.data_type() else {
            panic!("expected List return type");
        };
        assert_eq!(
            inner.metadata().get("ARROW:extension:name"),
            Some(&"arrow.uuid".to_string())
        );
        Ok(())
    }
}
