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

//! Utility functions for complex field access

use arrow::datatypes::{DataType, Field};

use crate::error::{DataFusionError, Result};
use crate::scalar::ScalarValue;

/// Returns the field access indexed by `key` from a [`DataType::List`]
/// # Error
/// Errors if
/// * the `data_type` is not a Struct or,
/// * there is no field key is not of the required index type
pub fn get_indexed_field(data_type: &DataType, key: &ScalarValue) -> Result<Field> {
    match (data_type, key) {
        (DataType::List(lt), ScalarValue::Int64(Some(i))) => {
            if *i < 0 {
                Err(DataFusionError::Plan(format!(
                    "List based indexed access requires a positive int, was {0}",
                    i
                )))
            } else {
                Ok(Field::new(&i.to_string(), lt.data_type().clone(), false))
            }
        }
        (DataType::List(_), _) => Err(DataFusionError::Plan(
            "Only ints are valid as an indexed field in a list".to_string(),
        )),
        _ => Err(DataFusionError::Plan(
            "The expression to get an indexed field is only valid for `List` types"
                .to_string(),
        )),
    }
}
