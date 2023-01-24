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

use super::binary::comparison_coercion;

/// Attempts to coerce the types of `list_types` to be comparable with the
/// `expr_type`.
/// Returns the common data type for `expr_type` and `list_types`
pub fn get_coerce_type_for_list(
    expr_type: &DataType,
    list_types: &[DataType],
) -> Option<DataType> {
    list_types
        .iter()
        .fold(Some(expr_type.clone()), |left, right_type| match left {
            None => None,
            Some(left_type) => comparison_coercion(&left_type, right_type),
        })
}

/// Find a common coerceable type for all `then_types` as well
/// and the `else_type`, if specified.
/// Returns the common data type for `then_types` and `else_type`
pub fn get_coerce_type_for_case_when(
    then_types: &[DataType],
    else_type: Option<&DataType>,
) -> Option<DataType> {
    let else_type = match else_type {
        None => then_types[0].clone(),
        Some(data_type) => data_type.clone(),
    };
    then_types
        .iter()
        .fold(Some(else_type), |left, right_type| match left {
            // failed to find a valid coercion in a previous iteration
            None => None,
            // TODO: now just use the `equal` coercion rule for case when. If find the issue, and
            // refactor again.
            Some(left_type) => comparison_coercion(&left_type, right_type),
        })
}
