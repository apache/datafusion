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

//! Conversions between `datafusion-proto-common` types and the `CsvSink`
//! type. Enabled by the `proto` feature.

use datafusion_common::proto::proto_error;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::file_sink_config::FileSink;
use datafusion_proto_common::protobuf;

use crate::file_format::CsvSink;

impl TryFrom<&protobuf::CsvSink> for CsvSink {
    type Error = DataFusionError;

    fn try_from(value: &protobuf::CsvSink) -> Result<Self, Self::Error> {
        let config = value
            .config
            .as_ref()
            .ok_or_else(|| proto_error("Missing required field config in CsvSink"))?
            .try_into()?;
        let writer_options = value
            .writer_options
            .as_ref()
            .ok_or_else(|| {
                proto_error("Missing required field writer_options in CsvSink")
            })?
            .try_into()?;
        Ok(Self::new(config, writer_options))
    }
}

impl TryFrom<&CsvSink> for protobuf::CsvSink {
    type Error = DataFusionError;

    fn try_from(value: &CsvSink) -> Result<Self, Self::Error> {
        Ok(Self {
            config: Some(value.config().try_into()?),
            writer_options: Some(value.writer_options().try_into()?),
        })
    }
}

impl From<&crate::file_format::CsvFormatFactory> for protobuf::CsvOptions {
    fn from(factory: &crate::file_format::CsvFormatFactory) -> Self {
        let Some(options) = &factory.options else {
            return protobuf::CsvOptions::default();
        };
        protobuf::CsvOptions {
            has_header: options.has_header.map_or(vec![], |v| vec![v as u8]),
            delimiter: vec![options.delimiter],
            quote: vec![options.quote],
            terminator: options.terminator.map_or(vec![], |v| vec![v]),
            escape: options.escape.map_or(vec![], |v| vec![v]),
            double_quote: options.double_quote.map_or(vec![], |v| vec![v as u8]),
            compression: options.compression as i32,
            schema_infer_max_rec: options.schema_infer_max_rec.map(|v| v as u64),
            date_format: options.date_format.clone().unwrap_or_default(),
            datetime_format: options.datetime_format.clone().unwrap_or_default(),
            timestamp_format: options.timestamp_format.clone().unwrap_or_default(),
            timestamp_tz_format: options.timestamp_tz_format.clone().unwrap_or_default(),
            time_format: options.time_format.clone().unwrap_or_default(),
            null_value: options.null_value.clone().unwrap_or_default(),
            null_regex: options.null_regex.clone().unwrap_or_default(),
            comment: options.comment.map_or(vec![], |v| vec![v]),
            newlines_in_values: options
                .newlines_in_values
                .map_or(vec![], |v| vec![v as u8]),
            truncated_rows: options.truncated_rows.map_or(vec![], |v| vec![v as u8]),
            compression_level: options.compression_level,
            quote_style: options.quote_style as i32,
            ignore_leading_whitespace: options
                .ignore_leading_whitespace
                .map_or(vec![], |v| vec![v as u8]),
            ignore_trailing_whitespace: options
                .ignore_trailing_whitespace
                .map_or(vec![], |v| vec![v as u8]),
        }
    }
}
