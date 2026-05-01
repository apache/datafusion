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

//! `From`/`TryFrom` conversions between `datafusion-proto-common`'s
//! prost-generated types and `datafusion-common` types.
//!
//! `datafusion-proto-common` only owns conversions for foreign-foreign pairs
//! (Arrow types ↔ proto types). Conversions involving `datafusion-common`
//! types live here so `datafusion-proto-common` can stay free of any
//! `datafusion-*` deps.

use crate::DataFusionError;

pub mod from_proto;
pub mod to_proto;

/// Build a `DataFusionError::Plan` from a string.
pub fn proto_error<S: Into<String>>(message: S) -> DataFusionError {
    DataFusionError::Plan(message.into())
}

impl From<datafusion_proto_common::from_proto::Error> for DataFusionError {
    fn from(e: datafusion_proto_common::from_proto::Error) -> Self {
        match e {
            datafusion_proto_common::from_proto::Error::Arrow(arrow_err) => {
                DataFusionError::ArrowError(
                    Box::new(arrow_err),
                    Some("from_proto".to_owned()),
                )
            }
            other => DataFusionError::Plan(other.to_string()),
        }
    }
}

impl From<datafusion_proto_common::to_proto::Error> for DataFusionError {
    fn from(e: datafusion_proto_common::to_proto::Error) -> Self {
        DataFusionError::Plan(e.to_string())
    }
}
