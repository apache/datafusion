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

use std::str::FromStr;

use arrow::csv::WriterBuilder;

use crate::{
    config::ConfigOptions,
    error::{DataFusionError, Result},
    parsers::CompressionTypeVariant,
};

use super::StatementOptions;

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

impl TryFrom<(&ConfigOptions, &StatementOptions)> for CsvWriterOptions {
    type Error = DataFusionError;

    fn try_from(value: (&ConfigOptions, &StatementOptions)) -> Result<Self> {
        let _configs = value.0;
        let statement_options = value.1;
        let mut builder = WriterBuilder::default();
        let mut compression = CompressionTypeVariant::UNCOMPRESSED;
        for (option, value) in &statement_options.options {
            builder = match option.to_lowercase().as_str(){
                "header" => {
                    let has_header = value.parse()
                        .map_err(|_| DataFusionError::Configuration(format!("Unable to parse {value} as bool as required for {option}!")))?;
                    builder.with_header(has_header)
                },
                "date_format" => builder.with_date_format(value.to_owned()),
                "datetime_format" => builder.with_datetime_format(value.to_owned()),
                "timestamp_format" => builder.with_timestamp_format(value.to_owned()),
                "time_format" => builder.with_time_format(value.to_owned()),
                "rfc3339" => builder, // No-op
                "null_value" => builder.with_null(value.to_owned()),
                "compression" => {
                    compression = CompressionTypeVariant::from_str(value.replace('\'', "").as_str())?;
                    builder
                },
                "delimiter" => {
                    // Ignore string literal single quotes passed from sql parsing
                    let value = value.replace('\'', "");
                    let chars: Vec<char> = value.chars().collect();
                    if chars.len()>1{
                        return Err(DataFusionError::Configuration(format!(
                            "CSV Delimiter Option must be a single char, got: {}", value
                        )))
                    }
                    builder.with_delimiter(chars[0].try_into().map_err(|_| {
                        DataFusionError::Internal(
                            "Unable to convert CSV delimiter into u8".into(),
                        )
                    })?)
            },
                "quote" | "escape" => {
                    // These two attributes are only available when reading csv files.
                    // To avoid error
                    builder
                },
                _ => return Err(DataFusionError::Configuration(format!("Found unsupported option {option} with value {value} for CSV format!")))
            }
        }
        Ok(CsvWriterOptions {
            writer_options: builder,
            compression,
        })
    }
}
