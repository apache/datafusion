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

//! implementation of make_array function

use arrow::array::{new_null_array, Array, ArrayData, ArrayRef, Capacities, GenericListArray, MutableArrayData, NullArray, OffsetSizeTrait, new_empty_array, make_array};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::DataType::{FixedSizeList, LargeList, List, Utf8};
use arrow::datatypes::{DataType, Field, FieldRef};
use itertools::Itertools;
use datafusion_common::utils::array_into_list_array;
use datafusion_common::{plan_err, DataFusionError, Result, internal_err, exec_err};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use arrow::compute;
use arrow::row::{RowConverter, SortField};
use datafusion_common::cast::{as_large_list_array, as_list_array};
use crate::make_array::make_array_inner;

// Create static instances of ScalarUDFs for each function
make_udf_function!(
    ArrayUnion,
    array_union,
    xx,    // arg name
    "yyy", // The name of the function to create the ScalarUDF
    union_udf
);
make_udf_function!(
    ArrayIntersect,
    array_intersect,
    xx,    // arg name
    "yyy", // The name of the function to create the ScalarUDF
    intersect_udf
);
make_udf_function!(
    ArrayDistinct,
    array_distinct,
    xx,    // arg name
    "yyy", // The name of the function to create the ScalarUDF
    distinct_udf
);

#[derive(Debug)]
pub(super) struct ArrayUnion {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayUnion {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: vec![],
        }
    }
}

impl ScalarUDFImpl for ArrayUnion {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_union"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;
        Ok(match arg_types[0] {
            List(_) | LargeList(_) | FixedSizeList(_, _) => Utf8,
            _ => {
                return plan_err!("The array_to_string function can only accept List/LargeList/FixedSizeList.");
            }
        })
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 2 {
            return exec_err!("array_union needs two arguments");
        }
        let args = ColumnarValue::values_to_arrays(args)?;
        let array1 = &args[0];
        let array2 = &args[1];

        general_set_op(array1, array2, SetOp::Union)
            .map(ColumnarValue::Array)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

#[derive(Debug)]
pub(super) struct ArrayIntersect {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayIntersect {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: vec![],
        }
    }
}

impl ScalarUDFImpl for ArrayIntersect {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_union"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;
        Ok(match arg_types[0] {
            List(_) | LargeList(_) | FixedSizeList(_, _) => Utf8,
            _ => {
                return plan_err!("The array_to_string function can only accept List/LargeList/FixedSizeList.");
            }
        })
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
            if args.len() != 2 {
                return exec_err!("array_intersect needs two arguments");
            }
            let args = ColumnarValue::values_to_arrays(args)?;

            let array1 = &args[0];
            let array2 = &args[1];

            general_set_op(array1, array2, SetOp::Intersect)
                .map(ColumnarValue::Array)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}



#[derive(Debug)]
pub(super) struct ArrayDistinct {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayDistinct {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: vec![],
        }
    }
}

impl ScalarUDFImpl for ArrayDistinct {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_distinct"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;
        Ok(match arg_types[0] {
            List(_) | LargeList(_) | FixedSizeList(_, _) => Utf8,
            _ => {
                return plan_err!("The array_to_string function can only accept List/LargeList/FixedSizeList.");
            }
        })
    }

    /// array_distinct SQL function
    /// example: from list [1, 3, 2, 3, 1, 2, 4] to [1, 2, 3, 4]
    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
            if args.len() != 1 {
                return exec_err!("array_distinct needs one argument");
            }
        // handle null
        if args[0].data_type() == DataType::Null {
            return Ok(args[0].clone());
        }
        let args = ColumnarValue::values_to_arrays(args)?;


        // handle for list & largelist
        match args[0].data_type() {
            DataType::List(field) => {
                let array = as_list_array(&args[0])?;
                general_array_distinct(array, field)
            }
            DataType::LargeList(field) => {
                let array = as_large_list_array(&args[0])?;
                general_array_distinct(array, field)
            }
            array_type => exec_err!("array_distinct does not support type '{array_type:?}'"),
        }.map(ColumnarValue::Array)

    }

    fn aliases(&self) -> &[String] {
        &self.aliases
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
                return make_array_inner(&[]);
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
                return make_array_inner(&[]);
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



fn general_array_distinct<OffsetSize: OffsetSizeTrait>(
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
