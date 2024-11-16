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

use std::any::Any;

use arrow::array::{make_comparator, Array, ArrayRef, BooleanArray};
use arrow::compute::kernels::cmp;
use arrow::compute::kernels::zip::zip;
use arrow::compute::SortOptions;
use arrow::datatypes::DataType;
use arrow_buffer::BooleanBuffer;
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use datafusion_expr::binary::type_union_resolution;

const SORT_OPTIONS: SortOptions = SortOptions {
    // We want greatest first
    descending: false,

    // NULL will be less than any other value
    nulls_first: true,
};

#[derive(Debug)]
pub struct GreatestFunc {
    signature: Signature,
}

impl Default for GreatestFunc {
    fn default() -> Self {
        GreatestFunc::new()
    }
}

impl GreatestFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}


/// Return boolean array where `arr[i] = lhs[i] >= rhs[i]` for all i, where `arr` is the result array
/// Nulls are always considered smaller than any other value
fn get_larger(lhs: &dyn Array, rhs: &dyn Array) -> Result<BooleanArray> {
    // Fast path:
    // If both arrays are not nested, have the same length and no nulls, we can use the faster vectorised kernel
    // - If both arrays are not nested: Nested types, such as lists, are not supported as the null semantics are not well-defined.
    // - both array does not have any nulls: cmp::gt_eq will return null if any of the input is null while we want to return false in that case
    if !lhs.data_type().is_nested() && lhs.null_count() == 0 && rhs.null_count() == 0 {
        return cmp::gt_eq(&lhs, &rhs).map_err(|e| e.into());
    }

    let cmp = make_comparator(lhs, rhs, SORT_OPTIONS)?;

    let len = lhs.len().min(rhs.len());

    let values = BooleanBuffer::collect_bool(len, |i| cmp(i, i).is_ge());

    // No nulls as we only want to keep the values that are larger, its either true or false
    Ok(BooleanArray::new(values, None))
}

/// Return array where the largest value at each index is kept
fn keep_larger(lhs: ArrayRef, rhs: ArrayRef) -> Result<ArrayRef> {
    // True for values that we should keep from the left array
    let keep_lhs = get_larger(lhs.as_ref(), rhs.as_ref())?;

    let larger = zip(&keep_lhs, &lhs, &rhs)?;

    Ok(larger)
}

fn keep_larger_scalar<'a>(lhs: &'a ScalarValue, rhs: &'a ScalarValue) -> Result<&'a ScalarValue> {
    if !lhs.data_type().is_nested() {
        return if lhs >= rhs {
            Ok(lhs)
        } else {
            Ok(rhs)
        };
    }

    // If complex type we can't compare directly as we want null values to be smaller
    let cmp = make_comparator(
        lhs.to_array()?.as_ref(),
        rhs.to_array()?.as_ref(),
        SORT_OPTIONS,
    )?;

    if cmp(0, 0).is_ge() {
        Ok(lhs)
    } else {
        Ok(rhs)
    }
}

fn find_coerced_type(data_types: &[DataType]) -> Result<DataType> {
    if let Some(coerced_type) = type_union_resolution(data_types) {
        Ok(coerced_type)
    } else {
        plan_err!("Cannot find a common type for arguments")
    }
}

impl ScalarUDFImpl for GreatestFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "greatest"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        find_coerced_type(arg_types)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        // do not accept less than 2 arguments.
        if args.len() < 2 {
            return exec_err!(
                "greatest was called with {} arguments. It requires at least 2.",
                args.len()
            );
        }

        // Split to scalars and arrays for later optimization
        let (scalars, arrays): (Vec<_>, Vec<_>) = args.iter().partition(|x| match x {
            ColumnarValue::Scalar(_) => true,
            ColumnarValue::Array(_) => false,
        });

        let mut arrays_iter = arrays
            .iter()
            .map(|x| match x {
                ColumnarValue::Array(a) => a,
                _ => unreachable!(),
            });

        let first_array = arrays_iter.next();

        let mut largest: ArrayRef;

        // Optimization: merge all scalars into one to avoid recomputing
        if !scalars.is_empty() {
            let mut scalars_iter = scalars
                .iter()
                .map(|x| match x {
                    ColumnarValue::Scalar(s) => s,
                    _ => unreachable!(),
                });

            // We have at least one scalar
            let mut largest_scalar = scalars_iter.next().unwrap();

            for scalar in scalars_iter {
                largest_scalar = keep_larger_scalar(largest_scalar, scalar)?;
            }

            // If we only have scalars, return the largest one
            if arrays.is_empty() {
                return Ok(ColumnarValue::Scalar(largest_scalar.clone()));
            }

            // We have at least one array
            let first_array = first_array.unwrap();

            // Start with the largest value
            largest = keep_larger(
                first_array.clone(),
                largest_scalar.to_array_of_size(first_array.len())?
            )?;
        } else {
            // If we only have arrays, start with the first array
            // (We must have at least one array)
            largest = first_array.unwrap().clone();
        }

        for array in arrays_iter {
            largest = keep_larger(array.clone(), largest)?;
        }

        Ok(ColumnarValue::Array(largest))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() < 2 {
            return exec_err!(
                "greatest was called with {} arguments. It requires at least 2.",
                arg_types.len()
            );
        }

        let coerced_type = find_coerced_type(arg_types)?;

        Ok(vec![coerced_type; arg_types.len()])
    }
}

#[cfg(test)]
mod test {
    use crate::core;
    use arrow::datatypes::DataType;
    use datafusion_expr::ScalarUDFImpl;

    #[test]
    fn test_greatest_return_types_without_common_supertype_in_arg_type() {
        let greatest = core::greatest::GreatestFunc::new();
        let return_type = greatest
            .return_type(&[DataType::Decimal128(10, 3), DataType::Decimal128(10, 4)])
            .unwrap();
        assert_eq!(return_type, DataType::Decimal128(11, 4));
    }
}
