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

//! Conversions between `datafusion-proto-common` types and the `JsonSink`
//! type. Enabled by the `proto` feature.

use datafusion_common::proto::proto_error;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::file_sink_config::FileSink;
use datafusion_proto_common::protobuf;

use crate::file_format::JsonSink;

impl TryFrom<&protobuf::JsonSink> for JsonSink {
    type Error = DataFusionError;

    fn try_from(value: &protobuf::JsonSink) -> Result<Self, Self::Error> {
        let config = value
            .config
            .as_ref()
            .ok_or_else(|| proto_error("Missing required field config in JsonSink"))?
            .try_into()?;
        let writer_options = value
            .writer_options
            .as_ref()
            .ok_or_else(|| {
                proto_error("Missing required field writer_options in JsonSink")
            })?
            .try_into()?;
        Ok(Self::new(config, writer_options))
    }
}

impl TryFrom<&JsonSink> for protobuf::JsonSink {
    type Error = DataFusionError;

    fn try_from(value: &JsonSink) -> Result<Self, Self::Error> {
        Ok(Self {
            config: Some(value.config().try_into()?),
            writer_options: Some(value.writer_options().try_into()?),
        })
    }
}

impl From<&crate::file_format::JsonFormatFactory> for protobuf::JsonOptions {
    fn from(factory: &crate::file_format::JsonFormatFactory) -> Self {
        let Some(options) = &factory.options else {
            return protobuf::JsonOptions::default();
        };
        protobuf::JsonOptions {
            compression: options.compression as i32,
            schema_infer_max_rec: options.schema_infer_max_rec.map(|v| v as u64),
            compression_level: options.compression_level,
            newline_delimited: Some(options.newline_delimited),
        }
    }
}
