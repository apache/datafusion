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

use parquet::{
    basic::{BrotliLevel, GzipLevel, ZstdLevel},
    file::properties::{EnabledStatistics, WriterVersion},
    schema::types::ColumnPath,
};

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

/// Parses datafusion.execution.parquet.encoding String to a parquet::basic::Encoding
pub(crate) fn parse_encoding_string(
    str_setting: &str,
) -> Result<parquet::basic::Encoding> {
    let str_setting_lower: &str = &str_setting.to_lowercase();
    match str_setting_lower {
        "plain" => Ok(parquet::basic::Encoding::PLAIN),
        "plain_dictionary" => Ok(parquet::basic::Encoding::PLAIN_DICTIONARY),
        "rle" => Ok(parquet::basic::Encoding::RLE),
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
        /// bit_packed, delta_binary_packed, delta_length_byte_array, \
        /// delta_byte_array, rle_dictionary, and byte_stream_split."
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
pub(crate) fn parse_compression_string(
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
            valid options are '1.0' and '2.0'"
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
            valid options are 'none', 'page', and 'chunk'"
        ))),
    }
}

/// Parses a string which contains tuples which determine column level parquet settings
/// e.g. '(col1 snappy, col2 zstd(5), col3.nested.nested2 zstd(10))'
pub(crate) fn parse_column_level_option_tuples(
    col_options: &str,
) -> Result<Vec<(ColumnPath, String)>> {
    println!("{}", col_options);

    let col_options = col_options.replace('\'', "").trim().to_owned();

    if !(col_options.chars().nth(0) == Some('(')
        && col_options.chars().nth_back(0) == Some(')'))
    {
        return Err(DataFusionError::Configuration(format!(
            "Unable to parse column level options specified as {col_options}"
        )));
    }

    println!("clipped {}", &col_options[1..col_options.len() - 1]);

    col_options[1..col_options.len() - 1]
        .split(',')
        .map(|s| {
            s.trim()
                .split_once(' ')
                .map(|(s1, s2)| {
                    (
                        ColumnPath::new(
                            s1.split('.')
                                .map(|sn| sn.to_owned())
                                .collect::<Vec<String>>(),
                        ),
                        s2.to_owned(),
                    )
                })
                .ok_or(DataFusionError::Configuration(format!(
                    "Unable to parse column level options specified as {col_options}"
                )))
        })
        .collect()
}
