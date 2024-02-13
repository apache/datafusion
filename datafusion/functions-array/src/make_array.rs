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

use arrow::array::{
    new_null_array, Array, ArrayData, ArrayRef, Capacities, GenericListArray,
    MutableArrayData, NullArray, OffsetSizeTrait,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use datafusion_common::utils::array_into_list_array;
use datafusion_common::{plan_err, DataFusionError, Result};
use datafusion_expr::{
    ColumnarValue, Expr, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

// Create static instances of ScalarUDFs for each function
make_udf_function!(
    MakeArray,
    make_array,
    array,
    "returns an Arrow array using the specified input expressions.",
    udf
);

/// Create a new array from a list of expressions
pub fn array(args: Vec<Expr>) -> Expr {
    udf().call(args)
}

#[derive(Debug)]
pub(super) struct MakeArray {
    signature: Signature,
    aliases: Vec<String>,
}

impl MakeArray {
    pub fn new() -> Self {
        Self {
            signature:                 // 0 or more arguments of arbitrary type
            Signature::one_of(vec![TypeSignature::VariadicEqual, TypeSignature::Any(0)],
                Volatility::Immutable),
            aliases: vec![
            "make_list".to_string(),
            ],
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
        use DataType::*;
        match arg_types.len() {
            0 => Ok(List(Arc::new(Field::new("item", Null, true)))),
            _ => {
                let mut expr_type = Null;
                for input_expr_type in arg_types {
                    if !input_expr_type.equals_datatype(&Null) {
                        expr_type = input_expr_type.clone();
                        break;
                    }
                }

                Ok(List(Arc::new(Field::new("item", expr_type, true))))
            }
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_array_inner(&ColumnarValue::values_to_arrays(args)?)
            .map(ColumnarValue::Array)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// `make_array` SQL function
pub(crate) fn make_array_inner(arrays: &[ArrayRef]) -> Result<ArrayRef> {
    let mut data_type = DataType::Null;
    for arg in arrays {
        let arg_data_type = arg.data_type();
        if !arg_data_type.equals_datatype(&DataType::Null) {
            data_type = arg_data_type.clone();
            break;
        }
    }

    match data_type {
        // Either an empty array or all nulls:
        DataType::Null => {
            let array =
                new_null_array(&DataType::Null, arrays.iter().map(|a| a.len()).sum());
            Ok(Arc::new(array_into_list_array(array)))
        }
        DataType::LargeList(..) => array_array::<i64>(arrays, data_type),
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
        arrow::array::make_array(data),
        None,
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::ListArray;
    use arrow::datatypes::Int64Type;
    use datafusion_common::cast::as_list_array;

    /// Only test internal functions, array-related sql functions will be tested in sqllogictest `array.slt`
    #[test]
    fn test_align_array_dimensions() {
        let array1d_1 =
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                Some(vec![Some(1), Some(2), Some(3)]),
                Some(vec![Some(4), Some(5)]),
            ]));
        let array1d_2 =
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                Some(vec![Some(6), Some(7), Some(8)]),
            ]));

        let array2d_1 = Arc::new(array_into_list_array(array1d_1.clone())) as ArrayRef;
        let array2d_2 = Arc::new(array_into_list_array(array1d_2.clone())) as ArrayRef;

        let res = align_array_dimensions::<i32>(vec![
            array1d_1.to_owned(),
            array2d_2.to_owned(),
        ])
        .unwrap();

        let expected = as_list_array(&array2d_1).unwrap();
        let expected_dim = datafusion_common::utils::list_ndims(array2d_1.data_type());
        assert_ne!(as_list_array(&res[0]).unwrap(), expected);
        assert_eq!(
            datafusion_common::utils::list_ndims(res[0].data_type()),
            expected_dim
        );

        let array3d_1 = Arc::new(array_into_list_array(array2d_1)) as ArrayRef;
        let array3d_2 = array_into_list_array(array2d_2.to_owned());
        let res =
            align_array_dimensions::<i32>(vec![array1d_1, Arc::new(array3d_2.clone())])
                .unwrap();

        let expected = as_list_array(&array3d_1).unwrap();
        let expected_dim = datafusion_common::utils::list_ndims(array3d_1.data_type());
        assert_ne!(as_list_array(&res[0]).unwrap(), expected);
        assert_eq!(
            datafusion_common::utils::list_ndims(res[0].data_type()),
            expected_dim
        );
    }

    fn align_array_dimensions<O: OffsetSizeTrait>(
        args: Vec<ArrayRef>,
    ) -> Result<Vec<ArrayRef>> {
        let args_ndim = args
            .iter()
            .map(|arg| datafusion_common::utils::list_ndims(arg.data_type()))
            .collect::<Vec<_>>();
        let max_ndim = args_ndim.iter().max().unwrap_or(&0);

        // Align the dimensions of the arrays
        let aligned_args: Result<Vec<ArrayRef>> = args
            .into_iter()
            .zip(args_ndim.iter())
            .map(|(array, ndim)| {
                if ndim < max_ndim {
                    let mut aligned_array = array.clone();
                    for _ in 0..(max_ndim - ndim) {
                        let data_type = aligned_array.data_type().to_owned();
                        let array_lengths = vec![1; aligned_array.len()];
                        let offsets = OffsetBuffer::<O>::from_lengths(array_lengths);

                        aligned_array = Arc::new(GenericListArray::<O>::try_new(
                            Arc::new(Field::new("item", data_type, true)),
                            offsets,
                            aligned_array,
                            None,
                        )?)
                    }
                    Ok(aligned_array)
                } else {
                    Ok(array.clone())
                }
            })
            .collect();

        aligned_args
    }
}
