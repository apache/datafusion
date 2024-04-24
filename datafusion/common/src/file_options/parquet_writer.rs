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

use crate::{
    config::{ParquetOptions, TableParquetOptions},
    DataFusionError, Result,
};

use parquet::{
    basic::{BrotliLevel, GzipLevel, ZstdLevel},
    file::{
        metadata::KeyValue,
        properties::{EnabledStatistics, WriterProperties, WriterVersion},
    },
    schema::types::ColumnPath,
};

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

impl TryFrom<&TableParquetOptions> for ParquetWriterOptions {
    type Error = DataFusionError;

    fn try_from(parquet_options: &TableParquetOptions) -> Result<Self> {
        let ParquetOptions {
            data_pagesize_limit,
            write_batch_size,
            writer_version,
            dictionary_page_size_limit,
            max_row_group_size,
            created_by,
            column_index_truncate_length,
            data_page_row_count_limit,
            bloom_filter_enabled,
            encoding,
            dictionary_enabled,
            compression,
            statistics_enabled,
            max_statistics_size,
            bloom_filter_fpp,
            bloom_filter_ndv,
            //  below is not part of ParquetWriterOptions
            enable_page_index: _,
            pruning: _,
            skip_metadata: _,
            metadata_size_hint: _,
            pushdown_filters: _,
            reorder_filters: _,
            allow_single_file_parallelism: _,
            maximum_parallel_row_group_writers: _,
            maximum_buffered_record_batches_per_stream: _,
        } = &parquet_options.global;

        let key_value_metadata = if !parquet_options.key_value_metadata.is_empty() {
            Some(
                parquet_options
                    .key_value_metadata
                    .clone()
                    .drain()
                    .map(|(key, value)| KeyValue { key, value })
                    .collect::<Vec<_>>(),
            )
        } else {
            None
        };

        let mut builder = WriterProperties::builder()
            .set_data_page_size_limit(*data_pagesize_limit)
            .set_write_batch_size(*write_batch_size)
            .set_writer_version(parse_version_string(writer_version.as_str())?)
            .set_dictionary_page_size_limit(*dictionary_page_size_limit)
            .set_max_row_group_size(*max_row_group_size)
            .set_created_by(created_by.clone())
            .set_column_index_truncate_length(*column_index_truncate_length)
            .set_data_page_row_count_limit(*data_page_row_count_limit)
            .set_bloom_filter_enabled(*bloom_filter_enabled)
            .set_key_value_metadata(key_value_metadata);

        if let Some(encoding) = &encoding {
            builder = builder.set_encoding(parse_encoding_string(encoding)?);
        }

        if let Some(enabled) = dictionary_enabled {
            builder = builder.set_dictionary_enabled(*enabled);
        }

        if let Some(compression) = &compression {
            builder = builder.set_compression(parse_compression_string(compression)?);
        }

        if let Some(statistics) = &statistics_enabled {
            builder =
                builder.set_statistics_enabled(parse_statistics_string(statistics)?);
        }

        if let Some(size) = max_statistics_size {
            builder = builder.set_max_statistics_size(*size);
        }

        if let Some(fpp) = bloom_filter_fpp {
            builder = builder.set_bloom_filter_fpp(*fpp);
        }

        if let Some(ndv) = bloom_filter_ndv {
            builder = builder.set_bloom_filter_ndv(*ndv);
        }

        for (column, options) in &parquet_options.column_specific_options {
            let path = ColumnPath::new(column.split('.').map(|s| s.to_owned()).collect());

            if let Some(bloom_filter_enabled) = options.bloom_filter_enabled {
                builder = builder
                    .set_column_bloom_filter_enabled(path.clone(), bloom_filter_enabled);
            }

            if let Some(encoding) = &options.encoding {
                let parsed_encoding = parse_encoding_string(encoding)?;
                builder = builder.set_column_encoding(path.clone(), parsed_encoding);
            }

            if let Some(dictionary_enabled) = options.dictionary_enabled {
                builder = builder
                    .set_column_dictionary_enabled(path.clone(), dictionary_enabled);
            }

            if let Some(compression) = &options.compression {
                let parsed_compression = parse_compression_string(compression)?;
                builder =
                    builder.set_column_compression(path.clone(), parsed_compression);
            }

            if let Some(statistics_enabled) = &options.statistics_enabled {
                let parsed_value = parse_statistics_string(statistics_enabled)?;
                builder =
                    builder.set_column_statistics_enabled(path.clone(), parsed_value);
            }

            if let Some(bloom_filter_fpp) = options.bloom_filter_fpp {
                builder =
                    builder.set_column_bloom_filter_fpp(path.clone(), bloom_filter_fpp);
            }

            if let Some(bloom_filter_ndv) = options.bloom_filter_ndv {
                builder =
                    builder.set_column_bloom_filter_ndv(path.clone(), bloom_filter_ndv);
            }

            if let Some(max_statistics_size) = options.max_statistics_size {
                builder =
                    builder.set_column_max_statistics_size(path, max_statistics_size);
            }
        }

        // ParquetWriterOptions will have defaults for the remaining fields (e.g. key_value_metadata & sorting_columns)
        Ok(ParquetWriterOptions {
            writer_options: builder.build(),
        })
    }
}

/// Parses datafusion.execution.parquet.encoding String to a parquet::basic::Encoding
pub(crate) fn parse_encoding_string(
    str_setting: &str,
) -> Result<parquet::basic::Encoding> {
    let str_setting_lower: &str = &str_setting.to_lowercase();
    match str_setting_lower {
        "plain" => Ok(parquet::basic::Encoding::PLAIN),
        "plain_dictionary" => Ok(parquet::basic::Encoding::PLAIN_DICTIONARY),
        "rle" => Ok(parquet::basic::Encoding::RLE),
        #[allow(deprecated)]
        "bit_packed" => Ok(parquet::basic::Encoding::BIT_PACKED),
        "delta_binary_packed" => Ok(parquet::basic::Encoding::DELTA_BINARY_PACKED),
        "delta_length_byte_array" => {
            Ok(parquet::basic::Encoding::DELTA_LENGTH_BYTE_ARRAY)
        }
        "delta_byte_array" => Ok(parquet::basic::Encoding::DELTA_BYTE_ARRAY),
        "rle_dictionary" => Ok(parquet::basic::Encoding::RLE_DICTIONARY),
        "byte_stream_split" => Ok(parquet::basic::Encoding::BYTE_STREAM_SPLIT),
        _ => Err(DataFusionError::Configuration(format!(
            "Unknown or unsupported parquet encoding: \
        {str_setting}. Valid values are: plain, plain_dictionary, rle, \
        bit_packed, delta_binary_packed, delta_length_byte_array, \
        delta_byte_array, rle_dictionary, and byte_stream_split."
        ))),
    }
}

/// Splits compression string into compression codec and optional compression_level
/// I.e. gzip(2) -> gzip, 2
fn split_compression_string(str_setting: &str) -> Result<(String, Option<u32>)> {
    // ignore string literal chars passed from sqlparser i.e. remove single quotes
    let str_setting = str_setting.replace('\'', "");
    let split_setting = str_setting.split_once('(');

    match split_setting {
        Some((codec, rh)) => {
            let level = &rh[..rh.len() - 1].parse::<u32>().map_err(|_| {
                DataFusionError::Configuration(format!(
                    "Could not parse compression string. \
                    Got codec: {} and unknown level from {}",
                    codec, str_setting
                ))
            })?;
            Ok((codec.to_owned(), Some(*level)))
        }
        None => Ok((str_setting.to_owned(), None)),
    }
}

/// Helper to ensure compression codecs which don't support levels
/// don't have one set. E.g. snappy(2) is invalid.
fn check_level_is_none(codec: &str, level: &Option<u32>) -> Result<()> {
    if level.is_some() {
        return Err(DataFusionError::Configuration(format!(
            "Compression {codec} does not support specifying a level"
        )));
    }
    Ok(())
}

/// Helper to ensure compression codecs which require a level
/// do have one set. E.g. zstd is invalid, zstd(3) is valid
fn require_level(codec: &str, level: Option<u32>) -> Result<u32> {
    level.ok_or(DataFusionError::Configuration(format!(
        "{codec} compression requires specifying a level such as {codec}(4)"
    )))
}

/// Parses datafusion.execution.parquet.compression String to a parquet::basic::Compression
pub fn parse_compression_string(
    str_setting: &str,
) -> Result<parquet::basic::Compression> {
    let str_setting_lower: &str = &str_setting.to_lowercase();
    let (codec, level) = split_compression_string(str_setting_lower)?;
    let codec = codec.as_str();
    match codec {
        "uncompressed" => {
            check_level_is_none(codec, &level)?;
            Ok(parquet::basic::Compression::UNCOMPRESSED)
        }
        "snappy" => {
            check_level_is_none(codec, &level)?;
            Ok(parquet::basic::Compression::SNAPPY)
        }
        "gzip" => {
            let level = require_level(codec, level)?;
            Ok(parquet::basic::Compression::GZIP(GzipLevel::try_new(
                level,
            )?))
        }
        "lzo" => {
            check_level_is_none(codec, &level)?;
            Ok(parquet::basic::Compression::LZO)
        }
        "brotli" => {
            let level = require_level(codec, level)?;
            Ok(parquet::basic::Compression::BROTLI(BrotliLevel::try_new(
                level,
            )?))
        }
        "lz4" => {
            check_level_is_none(codec, &level)?;
            Ok(parquet::basic::Compression::LZ4)
        }
        "zstd" => {
            let level = require_level(codec, level)?;
            Ok(parquet::basic::Compression::ZSTD(ZstdLevel::try_new(
                level as i32,
            )?))
        }
        "lz4_raw" => {
            check_level_is_none(codec, &level)?;
            Ok(parquet::basic::Compression::LZ4_RAW)
        }
        _ => Err(DataFusionError::Configuration(format!(
            "Unknown or unsupported parquet compression: \
        {str_setting}. Valid values are: uncompressed, snappy, gzip(level), \
        lzo, brotli(level), lz4, zstd(level), and lz4_raw."
        ))),
    }
}

pub(crate) fn parse_version_string(str_setting: &str) -> Result<WriterVersion> {
    let str_setting_lower: &str = &str_setting.to_lowercase();
    match str_setting_lower {
        "1.0" => Ok(WriterVersion::PARQUET_1_0),
        "2.0" => Ok(WriterVersion::PARQUET_2_0),
        _ => Err(DataFusionError::Configuration(format!(
            "Unknown or unsupported parquet writer version {str_setting} \
            valid options are 1.0 and 2.0"
        ))),
    }
}

pub(crate) fn parse_statistics_string(str_setting: &str) -> Result<EnabledStatistics> {
    let str_setting_lower: &str = &str_setting.to_lowercase();
    match str_setting_lower {
        "none" => Ok(EnabledStatistics::None),
        "chunk" => Ok(EnabledStatistics::Chunk),
        "page" => Ok(EnabledStatistics::Page),
        _ => Err(DataFusionError::Configuration(format!(
            "Unknown or unsupported parquet statistics setting {str_setting} \
            valid options are none, page, and chunk"
        ))),
    }
}
