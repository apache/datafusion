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

//! Options related to how csv files should be written

use crate::config::CsvOptions;
use crate::error::{DataFusionError, Result};
use crate::parsers::CompressionTypeVariant;

use arrow::csv::WriterBuilder;

/// Options for writing CSV files
#[derive(Clone, Debug)]
pub struct CsvWriterOptions {
    /// Struct from the arrow crate which contains all csv writing related settings
    pub writer_options: WriterBuilder,
    /// Compression to apply after ArrowWriter serializes RecordBatches.
    /// This compression is applied by DataFusion not the ArrowWriter itself.
    pub compression: CompressionTypeVariant,
}

impl CsvWriterOptions {
    pub fn new(
        writer_options: WriterBuilder,
        compression: CompressionTypeVariant,
    ) -> Self {
        Self {
            writer_options,
            compression,
        }
    }
}

impl TryFrom<&CsvOptions> for CsvWriterOptions {
    type Error = DataFusionError;

    fn try_from(value: &CsvOptions) -> Result<Self> {
        let mut builder = WriterBuilder::default()
            .with_header(value.has_header.unwrap_or(true))
            .with_quote(value.quote)
            .with_delimiter(value.delimiter);

        if let Some(v) = &value.date_format {
            builder = builder.with_date_format(v.into())
        }
        if let Some(v) = &value.datetime_format {
            builder = builder.with_datetime_format(v.into())
        }
        if let Some(v) = &value.timestamp_format {
            builder = builder.with_timestamp_format(v.into())
        }
        if let Some(v) = &value.timestamp_tz_format {
            builder = builder.with_timestamp_tz_format(v.into())
        }
        if let Some(v) = &value.time_format {
            builder = builder.with_time_format(v.into())
        }
        if let Some(v) = &value.null_value {
            builder = builder.with_null(v.into())
        }
        if let Some(v) = &value.escape {
            builder = builder.with_escape(*v)
        }
        if let Some(v) = &value.double_quote {
            builder = builder.with_double_quote(*v)
        }
        Ok(CsvWriterOptions {
            writer_options: builder,
            compression: value.compression,
        })
    }
}
