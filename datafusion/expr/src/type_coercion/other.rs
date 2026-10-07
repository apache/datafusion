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

use arrow::datatypes::DataType;

use super::binary::{comparison_coercion, type_union_coercion};

/// Fold `coerce_fn` over `types`, starting from `initial_type`.
fn fold_coerce(
    initial_type: &DataType,
    types: &[DataType],
    coerce_fn: fn(&DataType, &DataType) -> Option<DataType>,
) -> Option<DataType> {
    types
        .iter()
        .try_fold(initial_type.clone(), |left_type, right_type| {
            coerce_fn(&left_type, right_type)
        })
}

/// Attempts to coerce the types of `list_types` to be comparable with the
/// `expr_type` for IN list predicates.
/// Returns the common data type for `expr_type` and `list_types`.
///
/// Uses comparison coercion because `x IN (a, b)` is semantically equivalent
/// to `x = a OR x = b`.
///
/// # Deprecated
///
/// This no longer matches how the planner coerces `IN`: `=` reads a
/// timezone-naive timestamp in `datafusion.execution.time_zone` when it is
/// compared against a timezone-aware one, and this function cannot, because it
/// has nowhere to take that setting from. Fold
/// [`comparison_coercion_with_session_timezone`] over the values instead:
///
/// ```
/// # use arrow::datatypes::DataType;
/// # use datafusion_expr::type_coercion::binary::comparison_coercion_with_session_timezone;
/// # let expr_type = DataType::Int32;
/// # let list_types = [DataType::Int64];
/// # let session_time_zone = Some("+08:00");
/// let coerced = list_types.iter().try_fold(expr_type, |left, right| {
///     comparison_coercion_with_session_timezone(&left, right, session_time_zone)
/// });
/// # assert_eq!(coerced, Some(DataType::Int64));
/// ```
///
/// [`comparison_coercion_with_session_timezone`]: super::binary::comparison_coercion_with_session_timezone
#[deprecated(
    since = "56.0.0",
    note = "Fold `comparison_coercion_with_session_timezone` over the values instead, so `IN` coerces exactly as `=` does"
)]
pub fn get_coerce_type_for_list(
    expr_type: &DataType,
    list_types: &[DataType],
) -> Option<DataType> {
    fold_coerce(expr_type, list_types, comparison_coercion)
}

/// Find a common coerceable type for `CASE expr WHEN val1 WHEN val2 ...`
/// conditions. Returns the common type for `case_type` and all `when_types`.
///
/// Uses comparison coercion because `CASE expr WHEN val` is semantically
/// equivalent to `expr = val`.
///
/// # Deprecated
///
/// Deprecated for the same reason as [`get_coerce_type_for_list`]: it cannot
/// apply the `datafusion.execution.time_zone` rule that `=` applies. Fold
/// [`comparison_coercion_with_session_timezone`] over `when_types` instead.
///
/// [`comparison_coercion_with_session_timezone`]: super::binary::comparison_coercion_with_session_timezone
#[deprecated(
    since = "56.0.0",
    note = "Fold `comparison_coercion_with_session_timezone` over the values instead, so `CASE expr WHEN` coerces exactly as `=` does"
)]
pub fn get_coerce_type_for_case_when(
    when_types: &[DataType],
    case_type: &DataType,
) -> Option<DataType> {
    fold_coerce(case_type, when_types, comparison_coercion)
}

/// Find a common coerceable type for CASE THEN/ELSE result expressions.
/// Returns the common data type for `then_types` and `else_type`.
///
/// Uses type union coercion because the result branches must be brought to a
/// common type (like UNION), not compared.
pub fn get_coerce_type_for_case_expression(
    then_types: &[DataType],
    else_type: Option<&DataType>,
) -> Option<DataType> {
    let (initial_type, remaining) = match else_type {
        None => then_types.split_first()?,
        Some(data_type) => (data_type, then_types),
    };
    fold_coerce(initial_type, remaining, type_union_coercion)
}
