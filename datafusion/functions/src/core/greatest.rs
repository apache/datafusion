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

use crate::core::greatest_least_utils::GreatestLeastOperator;
use arrow::array::{Array, BooleanArray, make_comparator};
use arrow::buffer::BooleanBuffer;
use arrow::compute::SortOptions;
use arrow::compute::kernels::cmp;
use arrow::datatypes::DataType;
use datafusion_common::types::NativeType;
use datafusion_common::{Result, ScalarValue, assert_eq_or_internal_err};
use datafusion_doc::Documentation;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;

const SORT_OPTIONS: SortOptions = SortOptions {
    // We want greatest first
    descending: false,

    // NULL will be less than any other value
    nulls_first: true,
};

#[user_doc(
    doc_section(label = "Conditional Functions"),
    description = "Returns the greatest value in a list of expressions. Returns _null_ if all expressions are _null_.",
    syntax_example = "greatest(expression1[, ..., expression_n])",
    sql_example = r#"```sql
> select greatest(4, 7, 5);
+---------------------------+
| greatest(4,7,5)           |
+---------------------------+
| 7                         |
+---------------------------+
```"#,
    argument(
        name = "expression1, expression_n",
        description = "Expressions to compare and return the greatest value.. Can be a constant, column, or function, and any combination of arithmetic operators. Pass as many expression arguments as necessary."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
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

impl GreatestLeastOperator for GreatestFunc {
    const NAME: &'static str = "greatest";

    fn keep_scalar<'a>(
        lhs: &'a ScalarValue,
        rhs: &'a ScalarValue,
    ) -> Result<&'a ScalarValue> {
        if !lhs.data_type().is_nested() {
            return if lhs >= rhs { Ok(lhs) } else { Ok(rhs) };
        }

        // If complex type we can't compare directly as we want null values to be smaller
        let cmp = make_comparator(
            lhs.to_array()?.as_ref(),
            rhs.to_array()?.as_ref(),
            SORT_OPTIONS,
        )?;

        if cmp(0, 0).is_ge() { Ok(lhs) } else { Ok(rhs) }
    }

    /// Return boolean array where `arr[i] = lhs[i] >= rhs[i]` for all i, where `arr` is the result array
    /// Nulls are always considered smaller than any other value
    fn get_indexes_to_keep(lhs: &dyn Array, rhs: &dyn Array) -> Result<BooleanArray> {
        assert_eq_or_internal_err!(
            lhs.len(),
            rhs.len(),
            "All arrays should have the same length for greatest comparison"
        );

        if let Some(values) = vectorized_indexes_to_keep(lhs, rhs) {
            return Ok(BooleanArray::new(values, None));
        }

        let cmp = make_comparator(lhs, rhs, SORT_OPTIONS)?;

        let values = BooleanBuffer::collect_bool(lhs.len(), |i| cmp(i, i).is_ge());

        // No nulls as we only want to keep the values that are larger, its either true or false
        Ok(BooleanArray::new(values, None))
    }
}

/// Computes `lhs[i] >= rhs[i]` with nulls ordered before every other value,
/// using the vectorized comparison kernel rather than a per-row comparator.
///
/// Returns `None` when the inputs are not a good fit for the kernel and the
/// caller should fall back to [`make_comparator`]:
/// - nested types, such as lists, for which the kernel's null semantics are not
///   well-defined, and
/// - nullable floating point values, because `cmp::gt_eq` compares them with
///   IEEE semantics (every comparison against `NaN` is false) while the
///   comparator uses the total order in which `NaN` is the largest value.
fn vectorized_indexes_to_keep(lhs: &dyn Array, rhs: &dyn Array) -> Option<BooleanBuffer> {
    if lhs.data_type().is_nested() {
        return None;
    }

    let (values, both_valid) = cmp::gt_eq(&lhs, &rhs).ok()?.into_parts();

    // The kernel's validity is the intersection of both inputs', so an absent or
    // fully set buffer means there is nothing to fix up: the values are already
    // the answer.
    let Some(both_valid) = both_valid.filter(|nulls| nulls.null_count() > 0) else {
        return Some(values);
    };

    // `NativeType` looks through the encodings that wrap a value type, so this also
    // catches dictionary and run-end encoded floats.
    if NativeType::from(lhs.data_type()).is_float() {
        return None;
    }

    // `cmp::gt_eq` yields null wherever either side is null, but a null must compare
    // as smaller than any other value rather than propagate. So keep the left value
    // where both sides are valid and it compares greater or equal, and wherever the
    // right side is null (which includes both sides being null, where the values are
    // equivalent and the left one is kept).
    let both_valid_and_ge = &values & both_valid.inner();
    Some(match rhs.logical_nulls() {
        // `!rhs_valid` is all zeroes when the right side has no nulls, so the OR
        // would be a no-op.
        Some(rhs_nulls) if rhs_nulls.null_count() > 0 => {
            &both_valid_and_ge | &!rhs_nulls.inner()
        }
        _ => both_valid_and_ge,
    })
}

impl ScalarUDFImpl for GreatestFunc {
    fn name(&self) -> &str {
        "greatest"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        super::greatest_least_utils::execute_conditional::<Self>(&args.args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let coerced_type =
            super::greatest_least_utils::find_coerced_type::<Self>(arg_types)?;

        Ok(vec![coerced_type; arg_types.len()])
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
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
            .coerce_types(&[DataType::Decimal128(10, 3), DataType::Decimal128(10, 4)])
            .unwrap();
        assert_eq!(
            return_type,
            vec![DataType::Decimal128(11, 4), DataType::Decimal128(11, 4)]
        );
    }
}
