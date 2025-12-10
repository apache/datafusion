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

// TODO: https://github.com/apache/spark/tree/master/common/utils/src/main/resources/error

use arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, exec_datafusion_err, internal_datafusion_err};

pub fn invalid_arg_count_exec_err(
    function_name: &str,
    required_range: (i32, i32),
    provided: usize,
) -> DataFusionError {
    let (min_required, max_required) = required_range;
    let required = if min_required == max_required {
        format!(
            "{min_required} argument{}",
            if min_required == 1 { "" } else { "s" }
        )
    } else {
        format!("{min_required} to {max_required} arguments")
    };
    exec_datafusion_err!(
        "Spark `{function_name}` function requires {required}, got {provided}"
    )
}

pub fn unsupported_data_type_exec_err(
    function_name: &str,
    required: &str,
    provided: &DataType,
) -> DataFusionError {
    exec_datafusion_err!(
        "Unsupported Data Type: Spark `{function_name}` function expects {required}, got {provided}"
    )
}

pub fn unsupported_data_types_exec_err(
    function_name: &str,
    required: &str,
    provided: &[DataType],
) -> DataFusionError {
    exec_datafusion_err!(
        "Unsupported Data Type: Spark `{function_name}` function expects {required}, got {}",
        provided
            .iter()
            .map(|dt| format!("{dt}"))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

pub fn generic_exec_err(function_name: &str, message: &str) -> DataFusionError {
    exec_datafusion_err!("Spark `{function_name}` function: {message}")
}

pub fn generic_internal_err(function_name: &str, message: &str) -> DataFusionError {
    internal_datafusion_err!("Spark `{function_name}` function: {message}")
}
