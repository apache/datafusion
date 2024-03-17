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

//! array function utils

use std::sync::Arc;

use arrow::{array::ArrayRef, datatypes::DataType};
use arrow_array::{GenericListArray, OffsetSizeTrait};
use arrow_buffer::OffsetBuffer;
use arrow_schema::Field;
use datafusion_common::{plan_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionImplementation};

pub(crate) fn check_datatypes(name: &str, args: &[&ArrayRef]) -> Result<()> {
    let data_type = args[0].data_type();
    if !args.iter().all(|arg| {
        arg.data_type().equals_datatype(data_type)
            || arg.data_type().equals_datatype(&DataType::Null)
    }) {
        let types = args.iter().map(|arg| arg.data_type()).collect::<Vec<_>>();
        return plan_err!("{name} received incompatible types: '{types:?}'.");
    }

    Ok(())
}

/// array function wrapper that differentiates between scalar (length 1) and array.
pub(crate) fn make_scalar_function<F>(inner: F) -> ScalarFunctionImplementation
where
    F: Fn(&[ArrayRef]) -> Result<ArrayRef> + Sync + Send + 'static,
{
    Arc::new(move |args: &[ColumnarValue]| {
        // first, identify if any of the arguments is an Array. If yes, store its `len`,
        // as any scalar will need to be converted to an array of len `len`.
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let is_scalar = len.is_none();

        let args = ColumnarValue::values_to_arrays(args)?;

        let result = (inner)(&args);

        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    })
}

pub(crate) fn align_array_dimensions<O: OffsetSizeTrait>(
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Int64Type;
    use arrow_array::ListArray;
    use datafusion_common::{cast::as_list_array, utils::array_into_list_array};

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
}
