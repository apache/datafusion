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

use arrow::array::{make_comparator, Array, ArrayRef, BooleanArray};
use arrow::compute::kernels::cmp;
use arrow::compute::kernels::zip::zip;
use arrow::compute::SortOptions;
use arrow::datatypes::DataType;
use arrow_buffer::BooleanBuffer;
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_doc::Documentation;
use datafusion_expr::binary::type_union_resolution;
use datafusion_expr::scalar_doc_sections::DOC_SECTION_CONDITIONAL;
use datafusion_expr::ColumnarValue;
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::{Arc, OnceLock};

const SORT_OPTIONS: SortOptions = SortOptions {
    // Decreasing here as we will use lower than or equal to find the smallest value
    descending: false,

    // NULL will be greater than any other value
    nulls_first: false,
};

#[derive(Debug)]
pub struct LeastFunc {
    signature: Signature,
}

impl Default for LeastFunc {
    fn default() -> Self {
        LeastFunc::new()
    }
}

impl LeastFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

fn get_logical_null_count(arr: &dyn Array) -> usize {
    arr.logical_nulls()
        .map(|n| n.null_count())
        .unwrap_or_default()
}

/// Return boolean array where `arr[i] = lhs[i] <= rhs[i]` for all i, where `arr` is the result array
/// Nulls are always considered larger than any other value
fn get_smallest(lhs: &dyn Array, rhs: &dyn Array) -> Result<BooleanArray> {
    // Fast path:
    // If both arrays are not nested, have the same length and no nulls, we can use the faster vectorised kernel
    // - If both arrays are not nested: Nested types, such as lists, are not supported as the null semantics are not well-defined.
    // - both array does not have any nulls: cmp::lt_eq will return null if any of the input is null while we want to return false in that case
    if !lhs.data_type().is_nested()
        && get_logical_null_count(lhs) == 0
        && get_logical_null_count(rhs) == 0
    {
        return cmp::lt_eq(&lhs, &rhs).map_err(|e| e.into());
    }

    let cmp = make_comparator(lhs, rhs, SORT_OPTIONS)?;

    if lhs.len() != rhs.len() {
        return exec_err!("All arrays should have the same length for least comparison");
    }

    let values = BooleanBuffer::collect_bool(lhs.len(), |i| cmp(i, i).is_le());

    // No nulls as we only want to keep the values that are smaller, its either true or false
    Ok(BooleanArray::new(values, None))
}

/// Return array where the smallest value at each index is kept
fn keep_smallest(lhs: ArrayRef, rhs: ArrayRef) -> Result<ArrayRef> {
    // True for values that we should keep from the left array
    let keep_lhs = get_smallest(lhs.as_ref(), rhs.as_ref())?;

    let smaller = zip(&keep_lhs, &lhs, &rhs)?;

    Ok(smaller)
}

fn keep_smaller_scalar<'a>(
    lhs: &'a ScalarValue,
    rhs: &'a ScalarValue,
) -> Result<&'a ScalarValue> {
    // Manual checking for nulls as:
    // 1. If we're going to use <=, in Rust None is smaller than Some(T), which we don't want
    // 2. And we can't use make_comparator as it has no natural order (Arrow error)
    if lhs.is_null() {
        return Ok(rhs);
    }

    if rhs.is_null() {
        return Ok(lhs);
    }

    if !lhs.data_type().is_nested() {
        return if lhs <= rhs { Ok(lhs) } else { Ok(rhs) };
    }

    // Not using <= as in Rust None is smaller than Some(T)

    // If complex type we can't compare directly as we want null values to be larger
    let cmp = make_comparator(
        lhs.to_array()?.as_ref(),
        rhs.to_array()?.as_ref(),
        SORT_OPTIONS,
    )?;

    if cmp(0, 0).is_le() {
        Ok(lhs)
    } else {
        Ok(rhs)
    }
}

fn find_coerced_type(data_types: &[DataType]) -> Result<DataType> {
    if data_types.is_empty() {
        plan_err!("least was called without any arguments. It requires at least 1.")
    } else if let Some(coerced_type) = type_union_resolution(data_types) {
        Ok(coerced_type)
    } else {
        plan_err!("Cannot find a common type for arguments")
    }
}

impl ScalarUDFImpl for LeastFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "least"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.is_empty() {
            return exec_err!(
                "least was called with no arguments. It requires at least 1."
            );
        }

        // Some engines (e.g. SQL Server) allow least with single arg, it's a noop
        if args.len() == 1 {
            return Ok(args[0].clone());
        }

        // Split to scalars and arrays for later optimization (constant folding)
        let (scalars, arrays): (Vec<_>, Vec<_>) = args.iter().partition(|x| match x {
            ColumnarValue::Scalar(_) => true,
            ColumnarValue::Array(_) => false,
        });

        let mut arrays_iter = arrays.iter().map(|x| match x {
            ColumnarValue::Array(a) => a,
            _ => unreachable!(),
        });

        let first_array = arrays_iter.next();

        let mut smallest: ArrayRef;

        // Optimization: merge all scalars into one to avoid recomputing (constant folding)
        if !scalars.is_empty() {
            let mut scalars_iter = scalars.iter().map(|x| match x {
                ColumnarValue::Scalar(s) => s,
                _ => unreachable!(),
            });

            // We have at least one scalar
            let mut smallest_scalar = scalars_iter.next().unwrap();

            for scalar in scalars_iter {
                smallest_scalar = keep_smaller_scalar(smallest_scalar, scalar)?;
            }

            // If we only have scalars, return the smaller one
            if arrays.is_empty() {
                return Ok(ColumnarValue::Scalar(smallest_scalar.clone()));
            }

            // We have at least one array
            let first_array = first_array.unwrap();

            // Start with the smaller value
            smallest = keep_smallest(
                Arc::clone(first_array),
                smallest_scalar.to_array_of_size(first_array.len())?,
            )?;
        } else {
            // If we only have arrays, start with the first array
            // (We must have at least one array)
            smallest = Arc::clone(first_array.unwrap());
        }

        for array in arrays_iter {
            smallest = keep_smallest(Arc::clone(array), smallest)?;
        }

        Ok(ColumnarValue::Array(smallest))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let coerced_type = find_coerced_type(arg_types)?;

        Ok(vec![coerced_type; arg_types.len()])
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_smallest_doc())
    }
}
static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_smallest_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder(
            DOC_SECTION_CONDITIONAL,
            "Returns the smallest value in a list of expressions. Returns _null_ if all expressions are _null_.",
            "least(expression1[, ..., expression_n])")
            .with_sql_example(r#"```sql
> select least(4, 7, 5);
+---------------------------+
| least(4,7,5)              |
+---------------------------+
| 4                         |
+---------------------------+
```"#,
            )
            .with_argument(
                "expression1, expression_n",
                "Expressions to compare and return the smallest value.. Can be a constant, column, or function, and any combination of arithmetic operators. Pass as many expression arguments as necessary."
            )
            .build()
    })
}

#[cfg(test)]
mod test {
    use crate::core::least::LeastFunc;
    use arrow::datatypes::DataType;
    use datafusion_expr::ScalarUDFImpl;

    #[test]
    fn test_least_return_types_without_common_supertype_in_arg_type() {
        let least = LeastFunc::new();
        let return_type = least
            .coerce_types(&[DataType::Decimal128(10, 3), DataType::Decimal128(10, 4)])
            .unwrap();
        assert_eq!(
            return_type,
            vec![DataType::Decimal128(11, 4), DataType::Decimal128(11, 4)]
        );
    }
}
