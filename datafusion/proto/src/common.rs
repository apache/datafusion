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

use datafusion_common::DataFusionError;

pub fn csv_delimiter_to_string(b: u8) -> Result<String, DataFusionError> {
    let b = &[b];
    let b = std::str::from_utf8(b)
        .map_err(|_| DataFusionError::Internal("Invalid CSV delimiter".to_owned()))?;
    Ok(b.to_owned())
}

pub fn str_to_byte(s: &String) -> Result<u8, DataFusionError> {
    if s.len() != 1 {
        return Err(DataFusionError::Internal(
            "Invalid CSV delimiter".to_owned(),
        ));
    }
    Ok(s.as_bytes()[0])
}

pub(crate) fn proto_error<S: Into<String>>(message: S) -> DataFusionError {
    DataFusionError::Internal(message.into())
}
