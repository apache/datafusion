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

//! [`ScalarUDFImpl`] definitions for array_compact function.

use crate::utils::make_scalar_function;
use arrow::array::{
    Array, ArrayRef, Capacities, GenericListArray, MutableArrayData, OffsetSizeTrait,
    make_array, new_empty_array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::DataType::{LargeList, List, Null};
use arrow::datatypes::{DataType, FieldRef};
use datafusion_common::cast::{as_large_list_array, as_list_array};
use datafusion_common::utils::{list_values, offset_span};
use datafusion_common::{Result, exec_err, utils::take_function_args};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;
use std::sync::Arc;

make_udf_expr_and_func!(
    ArrayCompact,
    array_compact,
    array,
    "removes null values from the array.",
    array_compact_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Removes null values from the array.",
    syntax_example = "array_compact(array)",
    sql_example = r#"```sql
> select array_compact([1, NULL, 2, NULL, 3]) arr;
+-----------+
| arr       |
+-----------+
| [1, 2, 3] |
+-----------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayCompact {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayCompact {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayCompact {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
            aliases: vec!["list_compact".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayCompact {
    fn name(&self) -> &str {
        "array_compact"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(array_compact_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// array_compact SQL function
fn array_compact_inner(arg: &[ArrayRef]) -> Result<ArrayRef> {
    let [input_array] = take_function_args("array_compact", arg)?;

    match &input_array.data_type() {
        List(field) => {
            let array = as_list_array(input_array)?;
            compact_list::<i32>(array, field)
        }
        LargeList(field) => {
            let array = as_large_list_array(input_array)?;
            compact_list::<i64>(array, field)
        }
        Null => Ok(Arc::clone(input_array)),
        array_type => exec_err!("array_compact does not support type '{array_type}'."),
    }
}

/// Remove null elements from each row of a list array.
fn compact_list<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
    field: &FieldRef,
) -> Result<ArrayRef> {
    let list_offsets = list_array.offsets();
    let (first_offset, visible_len) = offset_span(list_offsets);
    if visible_len == 0 || list_array.null_count() == list_array.len() {
        // No row has values to keep. Return an empty child rather than a clone
        // of the input, which would keep the input's child values alive.
        return Ok(Arc::new(GenericListArray::<O>::new(
            Arc::clone(field),
            OffsetBuffer::new_zeroed(list_array.len()),
            new_empty_array(field.data_type()),
            list_array.nulls().cloned(),
        )));
    }
    // Restrict the child to the visible values before computing logical nulls,
    // which can be expensive.
    let values = list_values(list_array)?;
    // Use logical nulls so element types without a validity buffer
    // (e.g. NullArray) are still treated as null.
    let Some(values_nulls) = values.logical_nulls() else {
        // Fast path: no validity buffer, no nulls to remove
        return Ok(Arc::new(list_array.clone()));
    };
    if values_nulls.null_count() == 0 {
        // Fast path: validity buffer present but no nulls set
        return Ok(Arc::new(list_array.clone()));
    }

    let list_nulls = list_array.nulls();
    let original_data = values.to_data();
    let capacity = visible_len - values_nulls.null_count();
    let mut offsets = Vec::<O>::with_capacity(list_array.len() + 1);
    offsets.push(O::zero());
    let mut mutable = MutableArrayData::with_capacities(
        vec![&original_data],
        false,
        Capacities::Array(capacity),
    );

    for (row_index, window) in list_offsets.windows(2).enumerate() {
        if list_nulls.is_some_and(|n| n.is_null(row_index)) {
            offsets.push(offsets[row_index]);
            continue;
        }

        let start = window[0].as_usize() - first_offset;
        let end = window[1].as_usize() - first_offset;
        let row_null_count = values_nulls.slice(start, end - start).null_count();
        let kept = (end - start) - row_null_count;

        // Batch consecutive non-null elements into single extend() calls
        // to reduce per-element overhead. For [1, 2, NULL, 3, 4] this
        // produces 2 extend calls (0..2, 3..5) instead of 4 individual ones.
        let mut batch_start: Option<usize> = None;
        for i in start..end {
            if values_nulls.is_null(i) {
                // Null breaks the current batch — flush it
                if let Some(bs) = batch_start {
                    mutable.try_extend(0, bs, i)?;
                    batch_start = None;
                }
            } else if batch_start.is_none() {
                batch_start = Some(i);
            }
        }
        // Flush any remaining batch after the loop
        if let Some(bs) = batch_start {
            mutable.try_extend(0, bs, end)?;
        }

        offsets.push(offsets[row_index] + O::usize_as(kept));
    }

    let new_values = make_array(mutable.freeze());
    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::clone(field),
        OffsetBuffer::new(offsets.into()),
        new_values,
        list_nulls.cloned(),
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{DictionaryArray, Int32Array, ListArray, RunArray};
    use arrow::datatypes::{Field, Int32Type};

    #[test]
    fn test_sliced_capacity() -> Result<()> {
        crate::utils::tests::check_sliced_list_behavior(|input| {
            array_compact_inner(std::slice::from_ref(input))
        })
    }

    #[test]
    fn test_compact_sliced_logical_nulls() -> Result<()> {
        let primitive =
            Int32Array::from(vec![None, Some(10), Some(20), None, Some(30), None]);
        let dictionary = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0, 1, 2, 3, 4, 5]),
            Arc::new(primitive.clone()),
        )?;
        let runs = RunArray::<Int32Type>::try_new(
            &Int32Array::from(vec![1, 2, 3, 4, 5, 6]),
            &primitive,
        )?;
        let expected = ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(10), Some(20)]),
            Some(vec![Some(30)]),
        ]);
        for values in [
            Arc::new(primitive) as ArrayRef,
            Arc::new(dictionary),
            Arc::new(runs),
        ] {
            let field = Arc::new(Field::new_list_field(values.data_type().clone(), true));
            let input = ListArray::new(
                Arc::clone(&field),
                OffsetBuffer::from_lengths([1, 2, 2, 1]),
                values,
                None,
            );
            for data_type in [input.data_type().clone(), LargeList(field)] {
                let input = arrow::compute::cast(&input, &data_type)?;
                let result = array_compact_inner(&[input.slice(1, 2)])?;
                assert_eq!(result.data_type(), &data_type);
                let decoded = arrow::compute::cast(&result, expected.data_type())?;
                assert_eq!(decoded.as_ref(), &expected);

                // Nulls outside the selected row must not prevent the zero-copy fast path.
                let sliced = input.slice(1, 1);
                let result = array_compact_inner(std::slice::from_ref(&sliced))?;
                assert!(result.to_data().ptr_eq(&sliced.to_data()));
            }
        }
        Ok(())
    }
}
