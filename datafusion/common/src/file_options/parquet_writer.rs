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

//! Options related to how parquet files should be written

use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};

use crate::{
    config::ConfigOptions,
    file_options::parse_utils::{
        parse_compression_string, parse_encoding_string, parse_statistics_string,
        parse_version_string,
    },
    DataFusionError, Result,
};

use super::StatementOptions;

/// Options for writing parquet files
#[derive(Clone, Debug)]
pub struct ParquetWriterOptions {
    pub writer_options: WriterProperties,
}

impl ParquetWriterOptions {
    pub fn new(writer_options: WriterProperties) -> Self {
        Self { writer_options }
    }
}

impl ParquetWriterOptions {
    pub fn writer_options(&self) -> &WriterProperties {
        &self.writer_options
    }
}

/// Constructs a default Parquet WriterPropertiesBuilder using
/// Session level ConfigOptions to initialize settings
pub fn default_builder(options: &ConfigOptions) -> Result<WriterPropertiesBuilder> {
    let parquet_session_options = &options.execution.parquet;
    let mut builder = WriterProperties::builder()
        .set_data_page_size_limit(parquet_session_options.data_pagesize_limit)
        .set_write_batch_size(parquet_session_options.write_batch_size)
        .set_writer_version(parse_version_string(
            &parquet_session_options.writer_version,
        )?)
        .set_dictionary_page_size_limit(
            parquet_session_options.dictionary_page_size_limit,
        )
        .set_max_row_group_size(parquet_session_options.max_row_group_size)
        .set_created_by(parquet_session_options.created_by.clone())
        .set_column_index_truncate_length(
            parquet_session_options.column_index_truncate_length,
        )
        .set_data_page_row_count_limit(parquet_session_options.data_page_row_count_limit)
        .set_bloom_filter_enabled(parquet_session_options.bloom_filter_enabled);

    builder = match &parquet_session_options.encoding {
        Some(encoding) => builder.set_encoding(parse_encoding_string(encoding)?),
        None => builder,
    };

    builder = match &parquet_session_options.dictionary_enabled {
        Some(enabled) => builder.set_dictionary_enabled(*enabled),
        None => builder,
    };

    builder = match &parquet_session_options.compression {
        Some(compression) => {
            builder.set_compression(parse_compression_string(compression)?)
        }
        None => builder,
    };

    builder = match &parquet_session_options.statistics_enabled {
        Some(statistics) => {
            builder.set_statistics_enabled(parse_statistics_string(statistics)?)
        }
        None => builder,
    };

    builder = match &parquet_session_options.max_statistics_size {
        Some(size) => builder.set_max_statistics_size(*size),
        None => builder,
    };

    builder = match &parquet_session_options.bloom_filter_fpp {
        Some(fpp) => builder.set_bloom_filter_fpp(*fpp),
        None => builder,
    };

    builder = match &parquet_session_options.bloom_filter_ndv {
        Some(ndv) => builder.set_bloom_filter_ndv(*ndv),
        None => builder,
    };

    Ok(builder)
}

impl TryFrom<(&ConfigOptions, &StatementOptions)> for ParquetWriterOptions {
    type Error = DataFusionError;

    fn try_from(
        configs_and_statement_options: (&ConfigOptions, &StatementOptions),
    ) -> Result<Self> {
        let configs = configs_and_statement_options.0;
        let statement_options = configs_and_statement_options.1;
        let mut builder = default_builder(configs)?;
        for (option, value) in &statement_options.options {
            builder = match option.to_lowercase().as_str(){
                "max_row_group_size" => builder
                    .set_max_row_group_size(value.parse()
                    .map_err(|_| DataFusionError::Configuration(format!("Unable to parse {value} as u64 as required for {option}!")))?),
                "data_pagesize_limit" => builder
                    .set_data_page_size_limit(value.parse()
                    .map_err(|_| DataFusionError::Configuration(format!("Unable to parse {value} as usize as required for {option}!")))?),
                "write_batch_size" => builder
                    .set_write_batch_size(value.parse()
                    .map_err(|_| DataFusionError::Configuration(format!("Unable to parse {value} as usize as required for {option}!")))?),
                "writer_version" => builder
                    .set_writer_version(parse_version_string(value)?),
                "dictionary_page_size_limit" => builder
                    .set_dictionary_page_size_limit(value.parse()
                    .map_err(|_| DataFusionError::Configuration(format!("Unable to parse {value} as usize as required for {option}!")))?),
                "created_by" => builder
                    .set_created_by(value.to_owned()),
                "column_index_truncate_length" => builder
                    .set_column_index_truncate_length(Some(value.parse()
                    .map_err(|_| DataFusionError::Configuration(format!("Unable to parse {value} as usize as required for {option}!")))?)),
                "data_page_row_count_limit" => builder
                    .set_data_page_row_count_limit(value.parse()
                    .map_err(|_| DataFusionError::Configuration(format!("Unable to parse {value} as usize as required for {option}!")))?),
                "bloom_filter_enabled" => builder
                    .set_bloom_filter_enabled(value.parse()
                    .map_err(|_| DataFusionError::Configuration(format!("Unable to parse {value} as bool as required for {option}!")))?),
                "encoding" => builder
                    .set_encoding(parse_encoding_string(value)?),
                "dictionary_enabled" => builder
                    .set_dictionary_enabled(value.parse()
                    .map_err(|_| DataFusionError::Configuration(format!("Unable to parse {value} as bool as required for {option}!")))?),
                "compression" => builder
                    .set_compression(parse_compression_string(value)?),
                "statistics_enabled" => builder
                    .set_statistics_enabled(parse_statistics_string(value)?),
                "max_statistics_size" => builder
                    .set_max_statistics_size(value.parse()
                    .map_err(|_| DataFusionError::Configuration(format!("Unable to parse {value} as usize as required for {option}!")))?),
                "bloom_filter_fpp" => builder
                    .set_bloom_filter_fpp(value.parse()
                    .map_err(|_| DataFusionError::Configuration(format!("Unable to parse {value} as f64 as required for {option}!")))?),
                "bloom_filter_ndv" => builder
                    .set_bloom_filter_ndv(value.parse()
                    .map_err(|_| DataFusionError::Configuration(format!("Unable to parse {value} as u64 as required for {option}!")))?),
                _ => return Err(DataFusionError::Configuration(format!("Found unsupported option {option} with value {value} for Parquet format!")))
            }
        }
        Ok(ParquetWriterOptions {
            writer_options: builder.build(),
        })
    }
}
