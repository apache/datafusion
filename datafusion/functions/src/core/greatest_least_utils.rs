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

use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::compute::kernels::zip::zip;
use arrow::datatypes::DataType;
use datafusion_common::{assert_or_internal_err, plan_err, Result, ScalarValue};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::type_coercion::binary::type_union_resolution;
use std::sync::Arc;

pub(super) trait GreatestLeastOperator {
    const NAME: &'static str;

    fn keep_scalar<'a>(
        lhs: &'a ScalarValue,
        rhs: &'a ScalarValue,
    ) -> Result<&'a ScalarValue>;

    /// Return array with true for values that we should keep from the lhs array
    fn get_indexes_to_keep(lhs: &dyn Array, rhs: &dyn Array) -> Result<BooleanArray>;
}

fn keep_array<Op: GreatestLeastOperator>(
    lhs: &dyn Array,
    rhs: &dyn Array,
) -> Result<ArrayRef> {
    // True for values that we should keep from the left array
    let keep_lhs = Op::get_indexes_to_keep(lhs, rhs)?;

    let result = zip(&keep_lhs, &lhs, &rhs)?;

    Ok(result)
}

pub(super) fn execute_conditional<Op: GreatestLeastOperator>(
    args: &[ColumnarValue],
) -> Result<ColumnarValue> {
    assert_or_internal_err!(
        !args.is_empty(),
        "{} was called with no arguments. It requires at least 1.",
        Op::NAME
    );

    // Some engines (e.g. SQL Server) allow greatest/least with single arg, it's a noop
    if args.len() == 1 {
        return Ok(args[0].clone());
    }

    // Split to scalars and arrays for later optimization
    let (scalars, arrays): (Vec<_>, Vec<_>) = args.iter().partition(|x| match x {
        ColumnarValue::Scalar(_) => true,
        ColumnarValue::Array(_) => false,
    });

    let mut arrays_iter = arrays.iter().map(|x| match x {
        ColumnarValue::Array(a) => a,
        _ => unreachable!(),
    });

    let first_array = arrays_iter.next();

    let mut result: ArrayRef;

    // Optimization: merge all scalars into one to avoid recomputing (constant folding)
    if !scalars.is_empty() {
        let mut scalars_iter = scalars.iter().map(|x| match x {
            ColumnarValue::Scalar(s) => s,
            _ => unreachable!(),
        });

        // We have at least one scalar
        let mut result_scalar = scalars_iter.next().unwrap();

        for scalar in scalars_iter {
            result_scalar = Op::keep_scalar(result_scalar, scalar)?;
        }

        // If we only have scalars, return the one that we should keep (largest/least)
        if arrays.is_empty() {
            return Ok(ColumnarValue::Scalar(result_scalar.clone()));
        }

        // We have at least one array
        let first_array = first_array.unwrap();

        // Start with the result value
        result = keep_array::<Op>(
            first_array,
            &result_scalar.to_array_of_size(first_array.len())?,
        )?;
    } else {
        // If we only have arrays, start with the first array
        // (We must have at least one array)
        result = Arc::clone(first_array.unwrap());
    }

    for array in arrays_iter {
        result = keep_array::<Op>(array, &result)?;
    }

    Ok(ColumnarValue::Array(result))
}

pub(super) fn find_coerced_type<Op: GreatestLeastOperator>(
    data_types: &[DataType],
) -> Result<DataType> {
    if data_types.is_empty() {
        plan_err!(
            "{} was called without any arguments. It requires at least 1.",
            Op::NAME
        )
    } else if let Some(coerced_type) = type_union_resolution(data_types) {
        Ok(coerced_type)
    } else {
        plan_err!("Cannot find a common type for arguments")
    }
}
