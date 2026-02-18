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

use super::binary::type_union_coercion;

/// Find a common coerceable type for all `when_or_then_types` as well
/// and the `case_or_else_type`, if specified.
/// Returns the common data type for `when_or_then_types` and `case_or_else_type`
pub fn get_coerce_type_for_case_expression(
    when_or_then_types: &[DataType],
    case_or_else_type: Option<&DataType>,
) -> Option<DataType> {
    let case_or_else_type = match case_or_else_type {
        None => when_or_then_types[0].clone(),
        Some(data_type) => data_type.clone(),
    };
    when_or_then_types
        .iter()
        .try_fold(case_or_else_type, |left_type, right_type| {
            type_union_coercion(&left_type, right_type)
        })
}
