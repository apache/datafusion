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

//! [`ScalarUDFImpl`] definitions for `make_array` function.

use std::sync::Arc;
use std::vec;

use crate::utils::make_scalar_function;
use arrow::array::{
    Array, ArrayData, ArrayRef, Capacities, GenericListArray, MutableArrayData,
    NullArray, OffsetSizeTrait, new_null_array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use datafusion_common::utils::SingleRowListArrayBuilder;
use datafusion_common::{Result, plan_err};
use datafusion_expr::binary::{
    try_type_union_resolution_with_struct, type_union_resolution,
};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;
use itertools::Itertools as _;

make_udf_expr_and_func!(
    MakeArray,
    make_array,
    "Returns an Arrow array using the specified input expressions.",
    make_array_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns an array using the specified input expressions.",
    syntax_example = "make_array(expression1[, ..., expression_n])",
    sql_example = r#"```sql
> select make_array(1, 2, 3, 4, 5);
+----------------------------------------------------------+
| make_array(Int64(1),Int64(2),Int64(3),Int64(4),Int64(5)) |
+----------------------------------------------------------+
| [1, 2, 3, 4, 5]                                          |
+----------------------------------------------------------+
```"#,
    argument(
        name = "expression_n",
        description = "Expression to include in the output array. Can be a constant, column, or function, and any combination of arithmetic or string operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MakeArray {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for MakeArray {
    fn default() -> Self {
        Self::new()
    }
}

impl MakeArray {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![String::from("make_list")],
        }
    }
}

impl ScalarUDFImpl for MakeArray {
    fn name(&self) -> &str {
        "make_array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let element_type = if arg_types.is_empty() {
            DataType::Null
        } else {
            // At this point, all the type in array should be coerced to the same one.
            arg_types[0].to_owned()
        };

        Ok(DataType::new_list(element_type, true))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(make_array_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() {
            Ok(vec![])
        } else {
            coerce_types_inner(arg_types, self.name())
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// `make_array_inner` is the implementation of the `make_array` function.
/// Constructs an array using the input `data` as `ArrayRef`.
/// Returns a reference-counted `Array` instance result.
pub(crate) fn make_array_inner(arrays: &[ArrayRef]) -> Result<ArrayRef> {
    // Zero arguments are the only case that should build a scalar empty list.
    if arrays.is_empty() {
        let array = new_null_array(&DataType::Null, 0);
        Ok(Arc::new(
            SingleRowListArrayBuilder::new(array).build_list_array(),
        ))
    } else {
        // All-null inputs still need to flow through `array_array()` so rows
        // are built per input row instead of collapsing to one value.
        let data_type = arrays
            .iter()
            .find_map(|arg| {
                let arg_type = arg.data_type();
                (!arg_type.is_null()).then_some(arg_type)
            })
            .unwrap_or(&DataType::Null);

        array_array::<i32>(arrays, data_type.clone(), Field::LIST_FIELD_DEFAULT_NAME)
    }
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
pub fn array_array<O: OffsetSizeTrait>(
    args: &[ArrayRef],
    data_type: DataType,
    field_name: &str,
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
        Arc::new(Field::new(field_name, data_type, true)),
        OffsetBuffer::new(offsets.into()),
        arrow::array::make_array(data),
        None,
    )?))
}

pub fn coerce_types_inner(arg_types: &[DataType], name: &str) -> Result<Vec<DataType>> {
    if let Ok(unified) = try_type_union_resolution_with_struct(arg_types) {
        return Ok(unified);
    }

    if let Some(unified) = type_union_resolution(arg_types) {
        Ok(vec![unified; arg_types.len()])
    } else {
        plan_err!(
            "Failed to unify argument types of {}: [{}]",
            name,
            arg_types.iter().join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::ListArray;

    #[test]
    fn make_array_inner_all_null_arrays_preserves_row_count_and_width() {
        let inputs = vec![
            Arc::new(NullArray::new(3)) as ArrayRef,
            Arc::new(NullArray::new(3)) as ArrayRef,
        ];

        let result = make_array_inner(&inputs).unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(list.len(), 3);
        assert_eq!(list.value_type(), DataType::Null);
        assert_eq!(list.values().len(), 6);

        for row in 0..list.len() {
            assert_eq!(list.value_length(row), 2);
            let values = list.value(row);
            assert_eq!(values.len(), 2);
            assert_eq!(values.logical_null_count(), 2);
        }
    }
}
