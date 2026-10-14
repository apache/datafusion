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

//! [`ScalarUDFImpl`] definitions for cardinality function.

use crate::utils::make_scalar_function;
use arrow::array::{Array, ArrayRef, GenericListArray, OffsetSizeTrait, UInt64Array};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{
    DataType,
    DataType::{
        FixedSizeList, LargeList, LargeListView, List, ListView, Map, Null, UInt64,
    },
};
use datafusion_common::Result;
use datafusion_common::cast::{
    as_fixed_size_list_array, as_large_list_array, as_large_list_view_array,
    as_list_array, as_list_view_array, as_map_array,
};
use datafusion_common::exec_err;
use datafusion_common::utils::{ListCoercion, take_function_args};
use datafusion_expr::{
    ArrayFunctionArgument, ArrayFunctionSignature, ColumnarValue, Documentation,
    ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion_macros::user_doc;
use std::sync::Arc;

make_udf_expr_and_func!(
    Cardinality,
    cardinality,
    array,
    "returns the total number of elements in the array or map.",
    cardinality_udf
);

impl Cardinality {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
                        arguments: vec![ArrayFunctionArgument::Array],
                        array_coercion: Some(ListCoercion::FixedSizedListToList),
                    }),
                    TypeSignature::ArraySignature(ArrayFunctionSignature::MapArray),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns the total number of elements in the array.",
    syntax_example = "cardinality(array)",
    sql_example = r#"```sql
> select cardinality([[1, 2, 3, 4], [5, 6, 7, 8]]);
+--------------------------------------+
| cardinality(List([1,2,3,4,5,6,7,8])) |
+--------------------------------------+
| 8                                    |
+--------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Cardinality {
    signature: Signature,
}

impl Default for Cardinality {
    fn default() -> Self {
        Self::new()
    }
}
impl ScalarUDFImpl for Cardinality {
    fn name(&self) -> &str {
        "cardinality"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(UInt64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(cardinality_inner)(&args.args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn cardinality_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = take_function_args("cardinality", args)?;
    match array.data_type() {
        Null => Ok(Arc::new(UInt64Array::new_null(array.len()))),
        List(_) => {
            let list_array = as_list_array(array)?;
            generic_list_cardinality::<i32>(list_array)
        }
        LargeList(_) => {
            let list_array = as_large_list_array(array)?;
            generic_list_cardinality::<i64>(list_array)
        }
        Map(_, _) => {
            let map_array = as_map_array(array)?;
            Ok(cardinality_from_offsets(
                map_array.offsets(),
                map_array.nulls(),
            ))
        }
        arg_type => {
            exec_err!("cardinality does not support type {arg_type}")
        }
    }
}

fn cardinality_from_offsets<O: OffsetSizeTrait>(
    offsets: &OffsetBuffer<O>,
    nulls: Option<&NullBuffer>,
) -> ArrayRef {
    let values = offsets.lengths().map(|len| len as u64).collect::<Vec<_>>();
    Arc::new(UInt64Array::new(values.into(), nulls.cloned()))
}

fn generic_list_cardinality<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
) -> Result<ArrayRef> {
    // Nested lists require recursive counting; for all other lists, we can
    // compute the cardinality from offsets, which is much faster.
    if !array.values().data_type().is_list() {
        return Ok(cardinality_from_offsets(array.offsets(), array.nulls()));
    }

    let result = array
        .iter()
        .map(|arr| match arr {
            Some(arr) => value_cardinality(&arr).map(Some),
            None => Ok(None),
        })
        .collect::<Result<UInt64Array>>()?;
    Ok(Arc::new(result) as ArrayRef)
}

fn value_cardinality(array: &ArrayRef) -> Result<u64> {
    match array.data_type() {
        List(_) => {
            let list = as_list_array(&array)?;
            sum_list_cardinality(list.iter())
        }
        LargeList(_) => {
            let list = as_large_list_array(&array)?;
            sum_list_cardinality(list.iter())
        }
        ListView(_) => {
            let list = as_list_view_array(&array)?;
            sum_list_cardinality(list.iter())
        }
        LargeListView(_) => {
            let list = as_large_list_view_array(&array)?;
            sum_list_cardinality(list.iter())
        }
        FixedSizeList(..) => {
            let list = as_fixed_size_list_array(&array)?;
            sum_list_cardinality(list.iter())
        }
        _ => Ok(array.len() as u64),
    }
}

fn sum_list_cardinality<I>(mut iter: I) -> Result<u64>
where
    I: Iterator<Item = Option<ArrayRef>>,
{
    iter.try_fold(0u64, |total, arr| {
        let value_count = match arr {
            Some(arr) => value_cardinality(&arr)?,
            None => 0,
        };
        total.checked_add(value_count).ok_or_else(|| {
            datafusion_common::exec_datafusion_err!("cardinality overflowed u64")
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, MapArray, StructArray};
    use arrow::datatypes::Field;

    fn check_slices(array: &dyn Array) -> Result<()> {
        let expected = UInt64Array::from(vec![Some(1), Some(2), Some(0), None]);
        // Slices retain nonzero offsets into the values and validity buffers.
        for (offset, len) in [(0, 4), (1, 3), (2, 0)] {
            let result = cardinality_inner(&[array.slice(offset, len)])?;
            assert_eq!(result.as_ref(), &expected.slice(offset, len));
        }
        Ok(())
    }

    #[test]
    fn cardinality_flat_list_offsets() -> Result<()> {
        fn check<O: OffsetSizeTrait>() -> Result<()> {
            let values = Arc::new(Int32Array::from(vec![
                Some(1),
                None,
                Some(3),
                Some(4),
                Some(5),
            ]));
            let array = GenericListArray::<O>::new(
                Arc::new(Field::new_list_field(DataType::Int32, true)),
                OffsetBuffer::from_lengths([1, 2, 0, 2]),
                values,
                Some(NullBuffer::from(vec![true, true, true, false])),
            );
            check_slices(&array)
        }
        check::<i32>()?;
        check::<i64>()
    }

    #[test]
    fn cardinality_map_offsets() -> Result<()> {
        let entries = StructArray::from(vec![
            (
                Arc::new(Field::new("key", DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("value", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![
                    Some(1),
                    None,
                    Some(3),
                    Some(4),
                    Some(5),
                ])) as ArrayRef,
            ),
        ]);
        let array = MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::from_lengths([1, 2, 0, 2]),
            entries,
            Some(NullBuffer::from(vec![true, true, true, false])),
            false,
        );
        check_slices(&array)
    }
}
