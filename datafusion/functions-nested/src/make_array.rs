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

use std::{any::Any, sync::Arc};

use arrow::array::{ArrayData, Capacities, MutableArrayData};
use arrow_array::{
    new_null_array, Array, ArrayRef, GenericListArray, NullArray, OffsetSizeTrait,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::DataType::{LargeList, List, Null};
use arrow_schema::{DataType, Field};
use datafusion_common::internal_err;
use datafusion_common::{plan_err, utils::array_into_list_array_nullable, Result};
use datafusion_expr::type_coercion::binary::comparison_coercion;
use datafusion_expr::TypeSignature;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

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
                let mut expr_type = DataType::Null;
                for arg_type in arg_types {
                    if !arg_type.equals_datatype(&DataType::Null) {
                        expr_type = arg_type.clone();
                        break;
                    }
                }

                if expr_type.is_null() {
                    expr_type = DataType::Int64;
                }

                Ok(List(Arc::new(Field::new("item", expr_type, true))))
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
        let new_type = arg_types.iter().skip(1).try_fold(
            arg_types.first().unwrap().clone(),
            |acc, x| {
                // The coerced types found by `comparison_coercion` are not guaranteed to be
                // coercible for the arguments. `comparison_coercion` returns more loose
                // types that can be coerced to both `acc` and `x` for comparison purpose.
                // See `maybe_data_types` for the actual coercion.
                let coerced_type = comparison_coercion(&acc, x);
                if let Some(coerced_type) = coerced_type {
                    Ok(coerced_type)
                } else {
                    internal_err!("Coercion from {acc:?} to {x:?} failed.")
                }
            },
        )?;
        Ok(vec![new_type; arg_types.len()])
    }
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
