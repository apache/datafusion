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

use std::any::Any;
use std::sync::{Arc, OnceLock};
use std::vec;

use arrow::array::{ArrayData, Capacities, MutableArrayData};
use arrow_array::{
    new_null_array, Array, ArrayRef, GenericListArray, NullArray, OffsetSizeTrait,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::DataType::{LargeList, List, Null};
use arrow_schema::{DataType, Field};
use datafusion_common::{exec_err, internal_err};
use datafusion_common::{plan_err, utils::array_into_list_array_nullable, Result};
use datafusion_expr::binary::type_union_resolution;
use datafusion_expr::scalar_doc_sections::DOC_SECTION_ARRAY;
use datafusion_expr::TypeSignature;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use itertools::Itertools;

use crate::utils::make_scalar_function;

make_udf_expr_and_func!(
    MakeArray,
    make_array,
    "Returns an Arrow array using the specified input expressions.",
    make_array_udf
);

#[derive(Debug)]
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
            signature: Signature::one_of(
                vec![TypeSignature::UserDefined, TypeSignature::Any(0)],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("make_list")],
        }
    }
}

impl ScalarUDFImpl for MakeArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "make_array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types.len() {
            0 => Ok(empty_array_type()),
            _ => {
                // At this point, all the type in array should be coerced to the same one
                Ok(List(Arc::new(Field::new(
                    "item",
                    arg_types[0].to_owned(),
                    true,
                ))))
            }
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(make_array_inner)(args)
    }

    fn invoke_no_args(&self, _number_rows: usize) -> Result<ColumnarValue> {
        make_scalar_function(make_array_inner)(&[])
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if let Some(new_type) = type_union_resolution(arg_types) {
            // TODO: Move the logic to type_union_resolution if this applies to other functions as well
            // Handle struct where we only change the data type but preserve the field name and nullability.
            // Since field name is the key of the struct, so it shouldn't be updated to the common column name like "c0" or "c1"
            let is_struct_and_has_same_key = are_all_struct_and_have_same_key(arg_types)?;
            if is_struct_and_has_same_key {
                let data_types: Vec<_> = if let DataType::Struct(fields) = &arg_types[0] {
                    fields.iter().map(|f| f.data_type().to_owned()).collect()
                } else {
                    return internal_err!("Struct type is checked is the previous function, so this should be unreachable");
                };

                let mut final_struct_types = vec![];
                for s in arg_types {
                    let mut new_fields = vec![];
                    if let DataType::Struct(fields) = s {
                        for (i, f) in fields.iter().enumerate() {
                            let field = Arc::unwrap_or_clone(Arc::clone(f))
                                .with_data_type(data_types[i].to_owned());
                            new_fields.push(Arc::new(field));
                        }
                    }
                    final_struct_types.push(DataType::Struct(new_fields.into()))
                }
                return Ok(final_struct_types);
            }

            if let DataType::FixedSizeList(field, _) = new_type {
                Ok(vec![DataType::List(field); arg_types.len()])
            } else if new_type.is_null() {
                Ok(vec![DataType::Int64; arg_types.len()])
            } else {
                Ok(vec![new_type; arg_types.len()])
            }
        } else {
            plan_err!(
                "Fail to find the valid type between {:?} for {}",
                arg_types,
                self.name()
            )
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_make_array_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_make_array_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_ARRAY)
            .with_description(
                "Returns an array using the specified input expressions.",
            )
            .with_syntax_example("make_array(expression1[, ..., expression_n])")
            .with_sql_example(
                r#"```sql
> select make_array(1, 2, 3, 4, 5);
+----------------------------------------------------------+
| make_array(Int64(1),Int64(2),Int64(3),Int64(4),Int64(5)) |
+----------------------------------------------------------+
| [1, 2, 3, 4, 5]                                          |
+----------------------------------------------------------+
```"#,
            )
            .with_argument(
                "expression_n",
                "Expression to include in the output array. Can be a constant, column, or function, and any combination of arithmetic or string operators.",
            )
            .build()
            .unwrap()
    })
}

fn are_all_struct_and_have_same_key(data_types: &[DataType]) -> Result<bool> {
    let mut keys_string: Option<String> = None;
    for data_type in data_types {
        if let DataType::Struct(fields) = data_type {
            let keys = fields.iter().map(|f| f.name().to_owned()).join(",");
            if let Some(ref k) = keys_string {
                if *k != keys {
                    return exec_err!("Expect same keys for struct type but got mismatched pair {} and {}", *k, keys);
                }
            } else {
                keys_string = Some(keys);
            }
        } else {
            return Ok(false);
        }
    }

    Ok(true)
}

// Empty array is a special case that is useful for many other array functions
pub(super) fn empty_array_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Int64, true)))
}

/// `make_array_inner` is the implementation of the `make_array` function.
/// Constructs an array using the input `data` as `ArrayRef`.
/// Returns a reference-counted `Array` instance result.
pub(crate) fn make_array_inner(arrays: &[ArrayRef]) -> Result<ArrayRef> {
    let mut data_type = Null;
    for arg in arrays {
        let arg_data_type = arg.data_type();
        if !arg_data_type.equals_datatype(&Null) {
            data_type = arg_data_type.clone();
            break;
        }
    }

    match data_type {
        // Either an empty array or all nulls:
        Null => {
            let length = arrays.iter().map(|a| a.len()).sum();
            // By default Int64
            let array = new_null_array(&DataType::Int64, length);
            Ok(Arc::new(array_into_list_array_nullable(array)))
        }
        LargeList(..) => array_array::<i64>(arrays, data_type),
        _ => array_array::<i32>(arrays, data_type),
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
fn array_array<O: OffsetSizeTrait>(
    args: &[ArrayRef],
    data_type: DataType,
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
        Arc::new(Field::new("item", data_type, true)),
        OffsetBuffer::new(offsets.into()),
        arrow_array::make_array(data),
        None,
    )?))
}
