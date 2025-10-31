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

//! [`ScalarUDFImpl`] definitions for array_reverse function.

use crate::utils::make_scalar_function;
use arrow::array::{
    Array, ArrayRef, Capacities, FixedSizeListArray, GenericListArray,
    GenericListViewArray, MutableArrayData, OffsetSizeTrait, UInt32Array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::compute::take;
use arrow::datatypes::DataType::{
    FixedSizeList, LargeList, LargeListView, List, ListView, Null,
};
use arrow::datatypes::{DataType, FieldRef};
use datafusion_common::cast::{
    as_fixed_size_list_array, as_large_list_array, as_large_list_view_array,
    as_list_array, as_list_view_array,
};
use datafusion_common::{exec_err, utils::take_function_args, Result};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;
use std::sync::Arc;

make_udf_expr_and_func!(
    ArrayReverse,
    array_reverse,
    array,
    "reverses the order of elements in the array.",
    array_reverse_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns the array with the order of the elements reversed.",
    syntax_example = "array_reverse(array)",
    sql_example = r#"```sql
> select array_reverse([1, 2, 3, 4]);
+------------------------------------------------------------+
| array_reverse(List([1, 2, 3, 4]))                          |
+------------------------------------------------------------+
| [4, 3, 2, 1]                                               |
+------------------------------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayReverse {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayReverse {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayReverse {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
            aliases: vec!["list_reverse".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayReverse {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_reverse"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        make_scalar_function(array_reverse_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// array_reverse SQL function
pub fn array_reverse_inner(arg: &[ArrayRef]) -> Result<ArrayRef> {
    let [input_array] = take_function_args("array_reverse", arg)?;

    match &input_array.data_type() {
        List(field) => {
            let array = as_list_array(input_array)?;
            general_array_reverse::<i32>(array, field)
        }
        LargeList(field) => {
            let array = as_large_list_array(input_array)?;
            general_array_reverse::<i64>(array, field)
        }
        FixedSizeList(field, _) => {
            let array = as_fixed_size_list_array(input_array)?;
            fixed_size_array_reverse(array, field)
        }
        Null => Ok(Arc::clone(input_array)),
        ListView(field) => {
            let array = as_list_view_array(input_array)?;
            list_view_reverse::<i32>(array, field)
        }
        LargeListView(field) => {
            let array = as_large_list_view_array(input_array)?;
            list_view_reverse::<i64>(array, field)
        }
        array_type => exec_err!("array_reverse does not support type '{array_type}'."),
    }
}

fn general_array_reverse<O: OffsetSizeTrait + TryFrom<i64>>(
    array: &GenericListArray<O>,
    field: &FieldRef,
) -> Result<ArrayRef> {
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
        Arc::clone(field),
        OffsetBuffer::<O>::new(offsets.into()),
        arrow::array::make_array(data),
        Some(nulls.into()),
    )?))
}

fn list_view_reverse<O: OffsetSizeTrait + TryFrom<i64>>(
    array: &GenericListViewArray<O>,
    field: &FieldRef,
) -> Result<ArrayRef> {
    let (_, offsets, sizes, values, nulls) = array.clone().into_parts();

    // Construct indices, sizes and offsets for the reversed array by iterating over
    // the list view array in the logical order, and reversing the order of the elements.
    // We end up with a list view array where the elements are in order,
    // even if the original array had elements out of order.
    let mut indices: Vec<O> = Vec::with_capacity(values.len());
    let mut new_sizes = Vec::with_capacity(sizes.len());
    let mut new_offsets: Vec<O> = Vec::with_capacity(offsets.len());
    let mut new_nulls =
        Vec::with_capacity(nulls.clone().map(|nulls| nulls.len()).unwrap_or(0));
    new_offsets.push(O::zero());
    let has_nulls = nulls.is_some();
    for (i, offset) in offsets.iter().enumerate().take(offsets.len()) {
        // If this array is null, we set the new array to null with size 0 and continue
        if let Some(ref nulls) = nulls {
            if nulls.is_null(i) {
                new_nulls.push(false); // null
                new_sizes.push(O::zero());
                new_offsets.push(new_offsets[i]);
                continue;
            } else {
                new_nulls.push(true); // valid
            }
        }

        // Each array is located at [offset, offset + size), so we collect indices in the reverse order
        let array_start = offset.as_usize();
        let array_end = array_start + sizes[i].as_usize();
        for idx in (array_start..array_end).rev() {
            indices.push(O::usize_as(idx));
        }
        new_sizes.push(sizes[i]);
        if i < sizes.len() - 1 {
            new_offsets.push(new_offsets[i] + sizes[i]);
        }
    }

    // Materialize values from underlying array with take
    let indices_array: ArrayRef = if O::IS_LARGE {
        Arc::new(arrow::array::UInt64Array::from(
            indices
                .iter()
                .map(|i| i.as_usize() as u64)
                .collect::<Vec<_>>(),
        ))
    } else {
        Arc::new(UInt32Array::from(
            indices
                .iter()
                .map(|i| i.as_usize() as u32)
                .collect::<Vec<_>>(),
        ))
    };
    let values_reversed = take(&values, &indices_array, None)?;

    Ok(Arc::new(GenericListViewArray::<O>::try_new(
        Arc::clone(field),
        ScalarBuffer::from(new_offsets),
        ScalarBuffer::from(new_sizes),
        values_reversed,
        has_nulls.then_some(NullBuffer::from(new_nulls)),
    )?))
}

fn fixed_size_array_reverse(
    array: &FixedSizeListArray,
    field: &FieldRef,
) -> Result<ArrayRef> {
    let values = array.values();
    let original_data = values.to_data();
    let capacity = Capacities::Array(original_data.len());
    let mut nulls = vec![];
    let mut mutable =
        MutableArrayData::with_capacities(vec![&original_data], false, capacity);
    let value_length = array.value_length() as usize;

    for row_index in 0..array.len() {
        // skip the null value
        if array.is_null(row_index) {
            nulls.push(false);
            mutable.extend(0, 0, value_length);
            continue;
        } else {
            nulls.push(true);
        }
        let start = row_index * value_length;
        let end = start + value_length;
        for idx in (start..end).rev() {
            mutable.extend(0, idx, idx + 1);
        }
    }

    let data = mutable.freeze();
    Ok(Arc::new(FixedSizeListArray::try_new(
        Arc::clone(field),
        array.value_length(),
        arrow::array::make_array(data),
        Some(nulls.into()),
    )?))
}

#[cfg(test)]
mod tests {
    use crate::reverse::list_view_reverse;
    use arrow::{
        array::{AsArray, Int32Array, LargeListViewArray, ListViewArray},
        buffer::{NullBuffer, ScalarBuffer},
        datatypes::{DataType, Field, Int32Type},
    };
    use std::sync::Arc;

    #[test]
    fn test_reverse_list_view_and_large_list_view() {
        // ListView
        let list_view = ListViewArray::new(
            Arc::new(Field::new("a", DataType::Int32, false)),
            ScalarBuffer::from(vec![0, 1, 6, 6]),
            ScalarBuffer::from(vec![1, 5, 0, 3]),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9])),
            Some(NullBuffer::from(vec![true, true, false, true])),
        );
        let result = list_view_reverse(
            &list_view,
            &Arc::new(Field::new("test", DataType::Int32, true)),
        )
        .unwrap();
        let list_view_reversed: Vec<_> = result
            .as_list_view::<i32>()
            .iter()
            .map(|x| x.map(|x| x.as_primitive::<Int32Type>().values().to_vec()))
            .collect();

        // LargeListView
        let large_list_view = LargeListViewArray::new(
            Arc::new(Field::new("a", DataType::Int32, false)),
            ScalarBuffer::from(vec![0, 1, 6, 6]),
            ScalarBuffer::from(vec![1, 5, 0, 3]),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9])),
            Some(NullBuffer::from(vec![true, true, false, true])),
        );
        let result = list_view_reverse(
            &large_list_view,
            &Arc::new(Field::new("test", DataType::Int32, true)),
        )
        .unwrap();
        let large_list_view_reversed: Vec<_> = result
            .as_list_view::<i64>()
            .iter()
            .map(|x| x.map(|x| x.as_primitive::<Int32Type>().values().to_vec()))
            .collect();

        // Check results
        let expected = vec![
            Some(vec![1]),
            Some(vec![6, 5, 4, 3, 2]),
            None,
            Some(vec![9, 8, 7]),
        ];
        assert_eq!(expected, list_view_reversed);
        assert_eq!(expected, large_list_view_reversed);
    }

    #[test]
    fn test_reverse_list_view_out_of_order() {
        let list_view = ListViewArray::new(
            Arc::new(Field::new("a", DataType::Int32, false)),
            ScalarBuffer::from(vec![6, 1, 6, 0]), // out of order
            ScalarBuffer::from(vec![3, 5, 0, 1]),
            Arc::new(Int32Array::from(vec![
                1, // fourth array: offset 0, size 1
                2, 3, 4, 5, 6, // second array: offset 1, size 5
                // third array null but size 0
                7, 8, 9, // first array: offset 6, size 3
            ])),
            Some(NullBuffer::from(vec![true, true, false, true])),
        );
        let list_view_reversed: Vec<_> = list_view_reverse(
            &list_view,
            &Arc::new(Field::new("test", DataType::Int32, true)),
        )
        .unwrap()
        .as_list_view::<i32>()
        .iter()
        .map(|x| x.map(|x| x.as_primitive::<Int32Type>().values().to_vec()))
        .collect();
        let expected = vec![
            Some(vec![9, 8, 7]),
            Some(vec![6, 5, 4, 3, 2]),
            None,
            Some(vec![1]),
        ];
        assert_eq!(expected, list_view_reversed);
    }

    #[test]
    fn test_reverse_list_view_with_nulls() {
        let list_view = ListViewArray::new(
            Arc::new(Field::new("a", DataType::Int32, false)),
            ScalarBuffer::from(vec![16, 1, 6, 0]), // out of order
            ScalarBuffer::from(vec![3, 5, 10, 1]),
            Arc::new(Int32Array::from(vec![
                1, // fourth array: offset 0, size 1
                2, 3, 4, 5, 6, // second array: offset 1, size 5
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, // third array: offset 6, size 10
                7, 8, 9, // first array: offset 6, size 3
            ])),
            Some(NullBuffer::from(vec![true, true, false, true])),
        );
        let list_view_reversed: Vec<_> = list_view_reverse(
            &list_view,
            &Arc::new(Field::new("test", DataType::Int32, true)),
        )
        .unwrap()
        .as_list_view::<i32>()
        .iter()
        .map(|x| x.map(|x| x.as_primitive::<Int32Type>().values().to_vec()))
        .collect();
        let expected = vec![
            Some(vec![9, 8, 7]),
            Some(vec![6, 5, 4, 3, 2]),
            None,
            Some(vec![1]),
        ];
        assert_eq!(expected, list_view_reversed);
    }
}
