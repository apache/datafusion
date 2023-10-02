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

//! Functions for parsing arbitrary passed strings to valid file_option settings
use crate::{DataFusionError, Result};

/// Converts a String option to a bool, or returns an error if not a valid bool string.
pub(crate) fn parse_boolean_string(option: &str, value: String) -> Result<bool> {
    match value.to_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(DataFusionError::Configuration(format!(
            "Unsupported value {value} for option {option}! \
            Valid values are true or false!"
        ))),
    }
}
