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

//! [`ScalarUDFImpl`] definitions for array_resize function.

use crate::utils::make_scalar_function;
use arrow::array::{
    new_null_array, Array, ArrayRef, Capacities, GenericListArray, Int64Array,
    MutableArrayData, NullBufferBuilder, OffsetSizeTrait,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::ArrowNativeType;
use arrow::datatypes::DataType;
use arrow::datatypes::{
    DataType::{FixedSizeList, LargeList, List},
    FieldRef,
};
use datafusion_common::cast::{as_int64_array, as_large_list_array, as_list_array};
use datafusion_common::{exec_err, internal_datafusion_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;
use std::sync::Arc;

make_udf_expr_and_func!(
    ArrayResize,
    array_resize,
    array size value,
    "returns an array with the specified size filled with the given value.",
    array_resize_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Resizes the list to contain size elements. Initializes new elements with value or empty if value is not set.",
    syntax_example = "array_resize(array, size, value)",
    sql_example = r#"```sql
> select array_resize([1, 2, 3], 5, 0);
+-------------------------------------+
| array_resize(List([1,2,3],5,0))     |
+-------------------------------------+
| [1, 2, 3, 0, 0]                     |
+-------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(name = "size", description = "New size of given array."),
    argument(
        name = "value",
        description = "Defines new elements' value or empty if value is not set."
    )
)]
#[derive(Debug)]
pub struct ArrayResize {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayResize {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayResize {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: vec!["list_resize".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayResize {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_resize"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            List(field) | FixedSizeList(field, _) => Ok(List(Arc::clone(field))),
            LargeList(field) => Ok(LargeList(Arc::clone(field))),
            _ => exec_err!(
                "Not reachable, data_type should be List, LargeList or FixedSizeList"
            ),
        }
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        _number_rows: usize,
    ) -> Result<ColumnarValue> {
        make_scalar_function(array_resize_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// array_resize SQL function
pub(crate) fn array_resize_inner(arg: &[ArrayRef]) -> Result<ArrayRef> {
    if arg.len() < 2 || arg.len() > 3 {
        return exec_err!("array_resize needs two or three arguments");
    }

    let array = &arg[0];

    // Checks if entire array is null
    if array.null_count() == array.len() {
        let return_type = match array.data_type() {
            List(field) => List(Arc::clone(field)),
            LargeList(field) => LargeList(Arc::clone(field)),
            _ => {
                return exec_err!(
                    "array_resize does not support type '{:?}'.",
                    array.data_type()
                )
            }
        };
        return Ok(new_null_array(&return_type, array.len()));
    }

    let new_len = as_int64_array(&arg[1])?;
    let new_element = if arg.len() == 3 {
        Some(Arc::clone(&arg[2]))
    } else {
        None
    };

    match &arg[0].data_type() {
        List(field) => {
            let array = as_list_array(&arg[0])?;
            general_list_resize::<i32>(array, new_len, field, new_element)
        }
        LargeList(field) => {
            let array = as_large_list_array(&arg[0])?;
            general_list_resize::<i64>(array, new_len, field, new_element)
        }
        array_type => exec_err!("array_resize does not support type '{array_type:?}'."),
    }
}

/// array_resize keep the original array and append the default element to the end
fn general_list_resize<O: OffsetSizeTrait + TryInto<i64>>(
    array: &GenericListArray<O>,
    count_array: &Int64Array,
    field: &FieldRef,
    default_element: Option<ArrayRef>,
) -> Result<ArrayRef> {
    let data_type = array.value_type();

    let values = array.values();
    let original_data = values.to_data();

    // create default element array
    let default_element = if let Some(default_element) = default_element {
        default_element
    } else {
        let null_scalar = ScalarValue::try_from(&data_type)?;
        null_scalar.to_array_of_size(original_data.len())?
    };
    let default_value_data = default_element.to_data();

    // create a mutable array to store the original data
    let capacity = Capacities::Array(original_data.len() + default_value_data.len());
    let mut offsets = vec![O::usize_as(0)];
    let mut mutable = MutableArrayData::with_capacities(
        vec![&original_data, &default_value_data],
        false,
        capacity,
    );

    let mut null_builder = NullBufferBuilder::new(array.len());

    for (row_index, offset_window) in array.offsets().windows(2).enumerate() {
        if array.is_null(row_index) {
            null_builder.append_null();
            offsets.push(offsets[row_index]);
            continue;
        }
        null_builder.append_non_null();

        let count = count_array.value(row_index).to_usize().ok_or_else(|| {
            internal_datafusion_err!("array_resize: failed to convert size to usize")
        })?;
        let count = O::usize_as(count);
        let start = offset_window[0];
        if start + count > offset_window[1] {
            let extra_count =
                (start + count - offset_window[1]).try_into().map_err(|_| {
                    internal_datafusion_err!(
                        "array_resize: failed to convert size to i64"
                    )
                })?;
            let end = offset_window[1];
            mutable.extend(0, (start).to_usize().unwrap(), (end).to_usize().unwrap());
            // append default element
            for _ in 0..extra_count {
                mutable.extend(1, row_index, row_index + 1);
            }
        } else {
            let end = start + count;
            mutable.extend(0, (start).to_usize().unwrap(), (end).to_usize().unwrap());
        };
        offsets.push(offsets[row_index] + count);
    }

    let data = mutable.freeze();

    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::clone(field),
        OffsetBuffer::<O>::new(offsets.into()),
        arrow::array::make_array(data),
        null_builder.finish(),
    )?))
}
