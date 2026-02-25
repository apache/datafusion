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
    Array, ArrayRef, FixedSizeListArray, GenericListArray, GenericListViewArray,
    OffsetSizeTrait, UInt32Array, UInt64Array,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::compute::take;
use arrow::datatypes::DataType::{
    FixedSizeList, LargeList, LargeListView, List, ListView, Null,
};
use arrow::datatypes::{DataType, FieldRef};
use datafusion_common::cast::{
    as_fixed_size_list_array, as_large_list_array, as_large_list_view_array,
    as_list_array, as_list_view_array,
};
use datafusion_common::{Result, exec_err, utils::take_function_args};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;
use itertools::Itertools;
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

fn general_array_reverse<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    field: &FieldRef,
) -> Result<ArrayRef> {
    let values = array.values();
    let mut offsets = vec![O::usize_as(0)];
    let mut indices: Vec<O> = Vec::with_capacity(values.len());

    for (row_index, (&start, &end)) in array.offsets().iter().tuple_windows().enumerate()
    {
        // skip the null value
        if array.is_null(row_index) {
            offsets.push(offsets[row_index]);
            continue;
        }

        let mut index = end - O::one();
        while index >= start {
            indices.push(index);
            index = index - O::one();
        }
        let size = end - start;
        offsets.push(offsets[row_index] + size);
    }

    // Materialize values from underlying array with take
    let indices_array: ArrayRef = if O::IS_LARGE {
        Arc::new(UInt64Array::from(
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
    let values = take(&values, &indices_array, None)?;
    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::clone(field),
        OffsetBuffer::<O>::new(offsets.into()),
        values,
        array.nulls().cloned(),
    )?))
}

/// Reverses a list view array.
///
/// Construct indices, sizes and offsets for the reversed array by iterating over
/// the list view array in the logical order, and reversing the order of the elements.
/// We end up with a list view array where the elements are in order,
/// even if the original array had elements out of order.
fn list_view_reverse<O: OffsetSizeTrait>(
    array: &GenericListViewArray<O>,
    field: &FieldRef,
) -> Result<ArrayRef> {
    let offsets = array.offsets();
    let values = array.values();
    let sizes = array.sizes();

    let mut new_offsets: Vec<O> = Vec::with_capacity(offsets.len());
    let mut indices: Vec<O> = Vec::with_capacity(values.len());
    let mut new_sizes = Vec::with_capacity(sizes.len());

    let mut current_offset = O::zero();
    for (row_index, offset) in offsets.iter().enumerate() {
        new_offsets.push(current_offset);

        // If this array is null, we set its size to 0 and continue
        if array.is_null(row_index) {
            new_sizes.push(O::zero());
            continue;
        }
        let size = sizes[row_index];
        new_sizes.push(size);

        // Each array is located at [offset, offset + size), collect indices in the reverse order
        let array_start = *offset;
        let array_end = array_start + size;
        let mut idx = array_end - O::one();
        while idx >= array_start {
            indices.push(idx);
            idx = idx - O::one();
        }

        current_offset += size;
    }

    // Materialize values from underlying array with take
    let indices_array: ArrayRef = if O::IS_LARGE {
        Arc::new(UInt64Array::from(
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
    let values = take(&values, &indices_array, None)?;
    Ok(Arc::new(GenericListViewArray::<O>::try_new(
        Arc::clone(field),
        ScalarBuffer::from(new_offsets),
        ScalarBuffer::from(new_sizes),
        values,
        array.nulls().cloned(),
    )?))
}

fn fixed_size_array_reverse(
    array: &FixedSizeListArray,
    field: &FieldRef,
) -> Result<ArrayRef> {
    let values: &Arc<dyn Array> = array.values();

    // Since each fixed size list in the physical array is the same size and we keep the order
    // of the fixed size lists, we can reverse the indices for each fixed size list.
    let mut indices: Vec<u64> = (0..values.len() as u64).collect();
    for chunk in indices.chunks_mut(array.value_length() as usize) {
        chunk.reverse();
    }

    // Materialize values from underlying array with take
    let indices_array: ArrayRef = Arc::new(UInt64Array::from(indices));
    let values = take(&values, &indices_array, None)?;

    Ok(Arc::new(FixedSizeListArray::try_new(
        Arc::clone(field),
        array.value_length(),
        values,
        array.nulls().cloned(),
    )?))
}

#[cfg(test)]
mod tests {
    use crate::reverse::{fixed_size_array_reverse, list_view_reverse};
    use arrow::{
        array::{
            AsArray, FixedSizeListArray, GenericListViewArray, Int32Array,
            LargeListViewArray, ListViewArray, OffsetSizeTrait,
        },
        buffer::{NullBuffer, ScalarBuffer},
        datatypes::{DataType, Field, Int32Type},
    };
    use datafusion_common::Result;
    use std::sync::Arc;

    fn list_view_values<O: OffsetSizeTrait>(
        array: &GenericListViewArray<O>,
    ) -> Vec<Option<Vec<i32>>> {
        array
            .iter()
            .map(|x| x.map(|x| x.as_primitive::<Int32Type>().values().to_vec()))
            .collect()
    }

    fn fixed_size_list_values(array: &FixedSizeListArray) -> Vec<Option<Vec<i32>>> {
        array
            .iter()
            .map(|x| x.map(|x| x.as_primitive::<Int32Type>().values().to_vec()))
            .collect()
    }

    #[test]
    fn test_reverse_list_view() -> Result<()> {
        let field = Arc::new(Field::new("a", DataType::Int32, false));
        let offsets = ScalarBuffer::from(vec![0, 1, 6, 6]);
        let sizes = ScalarBuffer::from(vec![1, 5, 0, 3]);
        let values = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]));
        let nulls = Some(NullBuffer::from(vec![true, true, false, true]));
        let list_view = ListViewArray::new(field, offsets, sizes, values, nulls);
        let result = list_view_reverse(
            &list_view,
            &Arc::new(Field::new("test", DataType::Int32, true)),
        )?;
        let reversed = list_view_values(result.as_list_view::<i32>());
        let expected = vec![
            Some(vec![1]),
            Some(vec![6, 5, 4, 3, 2]),
            None,
            Some(vec![9, 8, 7]),
        ];
        assert_eq!(expected, reversed);
        Ok(())
    }

    #[test]
    fn test_reverse_large_list_view() -> Result<()> {
        let field = Arc::new(Field::new("a", DataType::Int32, false));
        let offsets = ScalarBuffer::from(vec![0, 1, 6, 6]);
        let sizes = ScalarBuffer::from(vec![1, 5, 0, 3]);
        let values = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]));
        let nulls = Some(NullBuffer::from(vec![true, true, false, true]));
        let list_view = LargeListViewArray::new(field, offsets, sizes, values, nulls);
        let result = list_view_reverse(
            &list_view,
            &Arc::new(Field::new("test", DataType::Int32, true)),
        )?;
        let reversed = list_view_values(result.as_list_view::<i64>());
        let expected = vec![
            Some(vec![1]),
            Some(vec![6, 5, 4, 3, 2]),
            None,
            Some(vec![9, 8, 7]),
        ];
        assert_eq!(expected, reversed);
        Ok(())
    }

    #[test]
    fn test_reverse_list_view_out_of_order() -> Result<()> {
        let field = Arc::new(Field::new("a", DataType::Int32, false));
        let offsets = ScalarBuffer::from(vec![6, 1, 6, 0]); // out of order
        let sizes = ScalarBuffer::from(vec![3, 5, 0, 1]);
        let values = Arc::new(Int32Array::from(vec![
            1, // fourth array: offset 0, size 1
            2, 3, 4, 5, 6, // second array: offset 1, size 5
            // third array: offset 6, size 0 (and null)
            7, 8, 9, // first array: offset 6, size 3
        ]));
        let nulls = Some(NullBuffer::from(vec![true, true, false, true]));
        let list_view = ListViewArray::new(field, offsets, sizes, values, nulls);
        let result = list_view_reverse(
            &list_view,
            &Arc::new(Field::new("test", DataType::Int32, true)),
        )?;
        let reversed = list_view_values(result.as_list_view::<i32>());
        let expected = vec![
            Some(vec![9, 8, 7]),
            Some(vec![6, 5, 4, 3, 2]),
            None,
            Some(vec![1]),
        ];
        assert_eq!(expected, reversed);
        Ok(())
    }

    #[test]
    fn test_reverse_list_view_with_nulls() -> Result<()> {
        let field = Arc::new(Field::new("a", DataType::Int32, false));
        let offsets = ScalarBuffer::from(vec![16, 1, 6, 0]); // out of order
        let sizes = ScalarBuffer::from(vec![3, 5, 10, 1]);
        let values = Arc::new(Int32Array::from(vec![
            1, // fourth array: offset 0, size 1
            2, 3, 4, 5, 6, // second array: offset 1, size 5
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, // third array: offset 6, size 10
            7, 8, 9, // first array: offset 6, size 3
        ]));
        let nulls = Some(NullBuffer::from(vec![true, true, false, true]));
        let list_view = ListViewArray::new(field, offsets, sizes, values, nulls);
        let result = list_view_reverse(
            &list_view,
            &Arc::new(Field::new("test", DataType::Int32, true)),
        )?;
        let reversed = list_view_values(result.as_list_view::<i32>());
        let expected = vec![
            Some(vec![9, 8, 7]),
            Some(vec![6, 5, 4, 3, 2]),
            None,
            Some(vec![1]),
        ];
        assert_eq!(expected, reversed);
        Ok(())
    }

    #[test]
    fn test_reverse_list_view_empty() -> Result<()> {
        let field = Arc::new(Field::new("a", DataType::Int32, false));
        let offsets = ScalarBuffer::from(vec![]);
        let sizes = ScalarBuffer::from(vec![]);
        let empty_array: Vec<i32> = vec![];
        let values = Arc::new(Int32Array::from(empty_array));
        let nulls = None;
        let list_view = ListViewArray::new(field, offsets, sizes, values, nulls);
        let result = list_view_reverse(
            &list_view,
            &Arc::new(Field::new("test", DataType::Int32, true)),
        )?;
        let reversed = list_view_values(result.as_list_view::<i32>());
        let expected: Vec<Option<Vec<i32>>> = vec![];
        assert_eq!(expected, reversed);
        Ok(())
    }

    #[test]
    fn test_reverse_list_view_all_nulls() -> Result<()> {
        let field = Arc::new(Field::new("a", DataType::Int32, false));
        let offsets = ScalarBuffer::from(vec![0, 1, 2, 3]);
        let sizes = ScalarBuffer::from(vec![0, 1, 1, 1]);
        let values = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
        let nulls = Some(NullBuffer::from(vec![false, false, false, false]));
        let list_view = ListViewArray::new(field, offsets, sizes, values, nulls);
        let result = list_view_reverse(
            &list_view,
            &Arc::new(Field::new("test", DataType::Int32, true)),
        )?;
        let reversed = list_view_values(result.as_list_view::<i32>());
        let expected: Vec<Option<Vec<i32>>> = vec![None, None, None, None];
        assert_eq!(expected, reversed);
        Ok(())
    }

    #[test]
    fn test_reverse_fixed_size_list() -> Result<()> {
        let field = Arc::new(Field::new("a", DataType::Int32, false));
        let values = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]));
        let result = fixed_size_array_reverse(
            &FixedSizeListArray::new(
                field,
                3,
                values,
                Some(NullBuffer::from(vec![true, false, true])),
            ),
            &Arc::new(Field::new("test", DataType::Int32, true)),
        )?;
        let reversed = fixed_size_list_values(result.as_fixed_size_list());
        let expected = vec![Some(vec![3, 2, 1]), None, Some(vec![9, 8, 7])];
        assert_eq!(expected, reversed);
        Ok(())
    }

    #[test]
    fn test_reverse_fixed_size_list_empty() -> Result<()> {
        let field = Arc::new(Field::new("a", DataType::Int32, false));
        let empty_array: Vec<i32> = vec![];
        let values = Arc::new(Int32Array::from(empty_array));
        let nulls = None;
        let fixed_size_list = FixedSizeListArray::new(field, 3, values, nulls);
        let result = fixed_size_array_reverse(
            &fixed_size_list,
            &Arc::new(Field::new("test", DataType::Int32, true)),
        )?;
        let reversed = fixed_size_list_values(result.as_fixed_size_list());
        let expected: Vec<Option<Vec<i32>>> = vec![];
        assert_eq!(expected, reversed);
        Ok(())
    }
}
