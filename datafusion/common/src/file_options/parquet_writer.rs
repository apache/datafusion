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

use std::sync::Arc;

use crate::{
    _internal_datafusion_err, DataFusionError, Result,
    config::{ParquetOptions, TableParquetOptions},
};

use arrow::datatypes::Schema;
use parquet::arrow::encode_arrow_schema;
use parquet::{
    arrow::ARROW_SCHEMA_META_KEY,
    basic::{BrotliLevel, GzipLevel, ZstdLevel},
    file::{
        metadata::KeyValue,
        properties::{
            DEFAULT_STATISTICS_ENABLED, EnabledStatistics, WriterProperties,
            WriterPropertiesBuilder, WriterVersion,
        },
    },
    schema::types::ColumnPath,
};

/// Options for writing parquet files
#[derive(Clone, Debug)]
pub struct ParquetWriterOptions {
    /// parquet-rs writer properties
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

impl TableParquetOptions {
    /// Add the arrow schema to the parquet kv_metadata.
    /// If already exists, then overwrites.
    pub fn arrow_schema(&mut self, schema: &Arc<Schema>) {
        self.key_value_metadata.insert(
            ARROW_SCHEMA_META_KEY.into(),
            Some(encode_arrow_schema(schema)),
        );
    }
}

impl TryFrom<&TableParquetOptions> for ParquetWriterOptions {
    type Error = DataFusionError;

    fn try_from(parquet_table_options: &TableParquetOptions) -> Result<Self> {
        // ParquetWriterOptions will have defaults for the remaining fields (e.g. sorting_columns)
        Ok(ParquetWriterOptions {
            writer_options: WriterPropertiesBuilder::try_from(parquet_table_options)?
                .build(),
        })
    }
}

impl TryFrom<&TableParquetOptions> for WriterPropertiesBuilder {
    type Error = DataFusionError;

    /// Convert the session's [`TableParquetOptions`] into a single write action's [`WriterPropertiesBuilder`].
    ///
    /// The returned [`WriterPropertiesBuilder`] includes customizations applicable per column.
    /// Note that any encryption options are ignored as building the `FileEncryptionProperties`
    /// might require other inputs besides the [`TableParquetOptions`].
    fn try_from(table_parquet_options: &TableParquetOptions) -> Result<Self> {
        // Table options include kv_metadata and col-specific options
        let TableParquetOptions {
            global,
            column_specific_options,
            key_value_metadata,
            crypto: _,
        } = table_parquet_options;

        let mut builder = global.into_writer_properties_builder()?;

        // check that the arrow schema is present in the kv_metadata, if configured to do so
        if !global.skip_arrow_metadata
            && !key_value_metadata.contains_key(ARROW_SCHEMA_META_KEY)
        {
            return Err(_internal_datafusion_err!(
                "arrow schema was not added to the kv_metadata, even though it is required by configuration settings"
            ));
        }

        // add kv_meta, if any
        if !key_value_metadata.is_empty() {
            builder = builder.set_key_value_metadata(Some(
                key_value_metadata
                    .to_owned()
                    .drain()
                    .map(|(key, value)| KeyValue { key, value })
                    .collect(),
            ));
        }

        // Apply column-specific options:
        for (column, options) in column_specific_options {
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
        }

        Ok(builder)
    }
}

impl ParquetOptions {
    /// Convert the global session options, [`ParquetOptions`], into a single write action's [`WriterPropertiesBuilder`].
    ///
    /// The returned [`WriterPropertiesBuilder`] can then be further modified with additional options
    /// applied per column; a customization which is not applicable for [`ParquetOptions`].
    ///
    /// Note that this method does not include the key_value_metadata from [`TableParquetOptions`].
    pub fn into_writer_properties_builder(&self) -> Result<WriterPropertiesBuilder> {
        let ParquetOptions {
            data_pagesize_limit,
            write_batch_size,
            writer_version,
            compression,
            dictionary_enabled,
            dictionary_page_size_limit,
            statistics_enabled,
            max_row_group_size,
            created_by,
            column_index_truncate_length,
            statistics_truncate_length,
            data_page_row_count_limit,
            encoding,
            bloom_filter_on_write,
            bloom_filter_fpp,
            bloom_filter_ndv,

            // not in WriterProperties
            enable_page_index: _,
            pruning: _,
            skip_metadata: _,
            metadata_size_hint: _,
            pushdown_filters: _,
            reorder_filters: _,
            allow_single_file_parallelism: _,
            maximum_parallel_row_group_writers: _,
            maximum_buffered_record_batches_per_stream: _,
            bloom_filter_on_read: _, // reads not used for writer props
            schema_force_view_types: _,
            binary_as_string: _, // not used for writer props
            coerce_int96: _,     // not used for writer props
            skip_arrow_metadata: _,
            max_predicate_cache_size: _,
        } = self;

        let mut builder = WriterProperties::builder()
            .set_data_page_size_limit(*data_pagesize_limit)
            .set_write_batch_size(*write_batch_size)
            .set_writer_version(parse_version_string(writer_version.as_str())?)
            .set_dictionary_page_size_limit(*dictionary_page_size_limit)
            .set_statistics_enabled(
                statistics_enabled
                    .as_ref()
                    .and_then(|s| parse_statistics_string(s).ok())
                    .unwrap_or(DEFAULT_STATISTICS_ENABLED),
            )
            .set_max_row_group_size(*max_row_group_size)
            .set_created_by(created_by.clone())
            .set_column_index_truncate_length(*column_index_truncate_length)
            .set_statistics_truncate_length(*statistics_truncate_length)
            .set_data_page_row_count_limit(*data_page_row_count_limit)
            .set_bloom_filter_enabled(*bloom_filter_on_write);

        if let Some(bloom_filter_fpp) = bloom_filter_fpp {
            builder = builder.set_bloom_filter_fpp(*bloom_filter_fpp);
        };
        if let Some(bloom_filter_ndv) = bloom_filter_ndv {
            builder = builder.set_bloom_filter_ndv(*bloom_filter_ndv);
        };
        if let Some(dictionary_enabled) = dictionary_enabled {
            builder = builder.set_dictionary_enabled(*dictionary_enabled);
        };

        // We do not have access to default ColumnProperties set in Arrow.
        // Therefore, only overwrite if these settings exist.
        if let Some(compression) = compression {
            builder = builder.set_compression(parse_compression_string(compression)?);
        }
        if let Some(encoding) = encoding {
            builder = builder.set_encoding(parse_encoding_string(encoding)?);
        }

        Ok(builder)
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
        #[expect(deprecated)]
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
                    Got codec: {codec} and unknown level from {str_setting}"
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

#[cfg(feature = "parquet")]
#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "parquet_encryption")]
    use crate::config::ConfigFileEncryptionProperties;
    use crate::config::{ParquetColumnOptions, ParquetEncryptionOptions, ParquetOptions};
    use parquet::basic::Compression;
    use parquet::file::properties::{
        BloomFilterProperties, DEFAULT_BLOOM_FILTER_FPP, DEFAULT_BLOOM_FILTER_NDV,
        EnabledStatistics,
    };
    use std::collections::HashMap;

    const COL_NAME: &str = "configured";

    /// Take the column defaults provided in [`ParquetOptions`], and generate a non-default col config.
    fn column_options_with_non_defaults(
        src_col_defaults: &ParquetOptions,
    ) -> ParquetColumnOptions {
        ParquetColumnOptions {
            compression: Some("zstd(22)".into()),
            dictionary_enabled: src_col_defaults.dictionary_enabled.map(|v| !v),
            statistics_enabled: Some("none".into()),
            encoding: Some("RLE".into()),
            bloom_filter_enabled: Some(true),
            bloom_filter_fpp: Some(0.72),
            bloom_filter_ndv: Some(72),
        }
    }

    fn parquet_options_with_non_defaults() -> ParquetOptions {
        let defaults = ParquetOptions::default();
        let writer_version = if defaults.writer_version.eq("1.0") {
            "2.0"
        } else {
            "1.0"
        };

        ParquetOptions {
            data_pagesize_limit: 42,
            write_batch_size: 42,
            writer_version: writer_version.into(),
            compression: Some("zstd(22)".into()),
            dictionary_enabled: Some(!defaults.dictionary_enabled.unwrap_or(false)),
            dictionary_page_size_limit: 42,
            statistics_enabled: Some("chunk".into()),
            max_row_group_size: 42,
            created_by: "wordy".into(),
            column_index_truncate_length: Some(42),
            statistics_truncate_length: Some(42),
            data_page_row_count_limit: 42,
            encoding: Some("BYTE_STREAM_SPLIT".into()),
            bloom_filter_on_write: !defaults.bloom_filter_on_write,
            bloom_filter_fpp: Some(0.42),
            bloom_filter_ndv: Some(42),

            // not in WriterProperties, but itemizing here to not skip newly added props
            enable_page_index: defaults.enable_page_index,
            pruning: defaults.pruning,
            skip_metadata: defaults.skip_metadata,
            metadata_size_hint: defaults.metadata_size_hint,
            pushdown_filters: defaults.pushdown_filters,
            reorder_filters: defaults.reorder_filters,
            allow_single_file_parallelism: defaults.allow_single_file_parallelism,
            maximum_parallel_row_group_writers: defaults
                .maximum_parallel_row_group_writers,
            maximum_buffered_record_batches_per_stream: defaults
                .maximum_buffered_record_batches_per_stream,
            bloom_filter_on_read: defaults.bloom_filter_on_read,
            schema_force_view_types: defaults.schema_force_view_types,
            binary_as_string: defaults.binary_as_string,
            skip_arrow_metadata: defaults.skip_arrow_metadata,
            coerce_int96: None,
            max_predicate_cache_size: defaults.max_predicate_cache_size,
        }
    }

    fn extract_column_options(
        props: &WriterProperties,
        col: ColumnPath,
    ) -> ParquetColumnOptions {
        let bloom_filter_default_props = props.bloom_filter_properties(&col);

        ParquetColumnOptions {
            bloom_filter_enabled: Some(bloom_filter_default_props.is_some()),
            encoding: props.encoding(&col).map(|s| s.to_string()),
            dictionary_enabled: Some(props.dictionary_enabled(&col)),
            compression: match props.compression(&col) {
                Compression::ZSTD(lvl) => {
                    Some(format!("zstd({})", lvl.compression_level()))
                }
                _ => None,
            },
            statistics_enabled: Some(
                match props.statistics_enabled(&col) {
                    EnabledStatistics::None => "none",
                    EnabledStatistics::Chunk => "chunk",
                    EnabledStatistics::Page => "page",
                }
                .into(),
            ),
            bloom_filter_fpp: bloom_filter_default_props.map(|p| p.fpp),
            bloom_filter_ndv: bloom_filter_default_props.map(|p| p.ndv),
        }
    }

    /// For testing only, take a single write's props and convert back into the session config.
    /// (use identity to confirm correct.)
    fn session_config_from_writer_props(props: &WriterProperties) -> TableParquetOptions {
        let default_col = ColumnPath::from("col doesn't have specific config");
        let default_col_props = extract_column_options(props, default_col);

        let configured_col = ColumnPath::from(COL_NAME);
        let configured_col_props = extract_column_options(props, configured_col);

        let key_value_metadata = props
            .key_value_metadata()
            .map(|pairs| {
                HashMap::from_iter(
                    pairs
                        .iter()
                        .cloned()
                        .map(|KeyValue { key, value }| (key, value)),
                )
            })
            .unwrap_or_default();

        let global_options_defaults = ParquetOptions::default();

        let column_specific_options = if configured_col_props.eq(&default_col_props) {
            HashMap::default()
        } else {
            HashMap::from([(COL_NAME.into(), configured_col_props)])
        };

        #[cfg(feature = "parquet_encryption")]
        let fep = props
            .file_encryption_properties()
            .map(ConfigFileEncryptionProperties::from);

        #[cfg(not(feature = "parquet_encryption"))]
        let fep = None;

        TableParquetOptions {
            global: ParquetOptions {
                // global options
                data_pagesize_limit: props.dictionary_page_size_limit(),
                write_batch_size: props.write_batch_size(),
                writer_version: format!("{}.0", props.writer_version().as_num()),
                dictionary_page_size_limit: props.dictionary_page_size_limit(),
                max_row_group_size: props.max_row_group_size(),
                created_by: props.created_by().to_string(),
                column_index_truncate_length: props.column_index_truncate_length(),
                statistics_truncate_length: props.statistics_truncate_length(),
                data_page_row_count_limit: props.data_page_row_count_limit(),

                // global options which set the default column props
                encoding: default_col_props.encoding,
                compression: default_col_props.compression,
                dictionary_enabled: default_col_props.dictionary_enabled,
                statistics_enabled: default_col_props.statistics_enabled,
                bloom_filter_on_write: default_col_props
                    .bloom_filter_enabled
                    .unwrap_or_default(),
                bloom_filter_fpp: default_col_props.bloom_filter_fpp,
                bloom_filter_ndv: default_col_props.bloom_filter_ndv,

                // not in WriterProperties
                enable_page_index: global_options_defaults.enable_page_index,
                pruning: global_options_defaults.pruning,
                skip_metadata: global_options_defaults.skip_metadata,
                metadata_size_hint: global_options_defaults.metadata_size_hint,
                pushdown_filters: global_options_defaults.pushdown_filters,
                reorder_filters: global_options_defaults.reorder_filters,
                allow_single_file_parallelism: global_options_defaults
                    .allow_single_file_parallelism,
                maximum_parallel_row_group_writers: global_options_defaults
                    .maximum_parallel_row_group_writers,
                maximum_buffered_record_batches_per_stream: global_options_defaults
                    .maximum_buffered_record_batches_per_stream,
                bloom_filter_on_read: global_options_defaults.bloom_filter_on_read,
                max_predicate_cache_size: global_options_defaults
                    .max_predicate_cache_size,
                schema_force_view_types: global_options_defaults.schema_force_view_types,
                binary_as_string: global_options_defaults.binary_as_string,
                skip_arrow_metadata: global_options_defaults.skip_arrow_metadata,
                coerce_int96: None,
            },
            column_specific_options,
            key_value_metadata,
            crypto: ParquetEncryptionOptions {
                file_encryption: fep,
                file_decryption: None,
                factory_id: None,
                factory_options: Default::default(),
            },
        }
    }

    #[test]
    fn table_parquet_opts_to_writer_props_skip_arrow_metadata() {
        // TableParquetOptions, all props set to default
        let mut table_parquet_opts = TableParquetOptions::default();
        assert!(
            !table_parquet_opts.global.skip_arrow_metadata,
            "default false, to not skip the arrow schema requirement"
        );

        // see errors without the schema added, using default settings
        let should_error = WriterPropertiesBuilder::try_from(&table_parquet_opts);
        assert!(
            should_error.is_err(),
            "should error without the required arrow schema in kv_metadata",
        );

        // succeeds if we permit skipping the arrow schema
        table_parquet_opts = table_parquet_opts.with_skip_arrow_metadata(true);
        let should_succeed = WriterPropertiesBuilder::try_from(&table_parquet_opts);
        assert!(
            should_succeed.is_ok(),
            "should work with the arrow schema skipped by config",
        );

        // Set the arrow schema back to required
        table_parquet_opts = table_parquet_opts.with_skip_arrow_metadata(false);
        // add the arrow schema to the kv_meta
        table_parquet_opts.arrow_schema(&Arc::new(Schema::empty()));
        let should_succeed = WriterPropertiesBuilder::try_from(&table_parquet_opts);
        assert!(
            should_succeed.is_ok(),
            "should work with the arrow schema included in TableParquetOptions",
        );
    }

    #[test]
    fn table_parquet_opts_to_writer_props() {
        // ParquetOptions, all props set to non-default
        let parquet_options = parquet_options_with_non_defaults();

        // TableParquetOptions, using ParquetOptions for global settings
        let key = ARROW_SCHEMA_META_KEY.to_string();
        let value = Some("bar".into());
        let table_parquet_opts = TableParquetOptions {
            global: parquet_options.clone(),
            column_specific_options: [(
                COL_NAME.into(),
                column_options_with_non_defaults(&parquet_options),
            )]
            .into(),
            key_value_metadata: [(key, value)].into(),
            crypto: Default::default(),
        };

        let writer_props = WriterPropertiesBuilder::try_from(&table_parquet_opts)
            .unwrap()
            .build();
        assert_eq!(
            table_parquet_opts,
            session_config_from_writer_props(&writer_props),
            "the writer_props should have the same configuration as the session's TableParquetOptions",
        );
    }

    /// Ensure that the configuration defaults for writing parquet files are
    /// consistent with the options in arrow-rs
    #[test]
    fn test_defaults_match() {
        // ensure the global settings are the same
        let mut default_table_writer_opts = TableParquetOptions::default();
        let default_parquet_opts = ParquetOptions::default();
        assert_eq!(
            default_table_writer_opts.global, default_parquet_opts,
            "should have matching defaults for TableParquetOptions.global and ParquetOptions",
        );

        // selectively skip the arrow_schema metadata, since the WriterProperties default has an empty kv_meta (no arrow schema)
        default_table_writer_opts =
            default_table_writer_opts.with_skip_arrow_metadata(true);

        // WriterProperties::default, a.k.a. using extern parquet's defaults
        let default_writer_props = WriterProperties::new();

        // WriterProperties::try_from(TableParquetOptions::default), a.k.a. using datafusion's defaults
        let from_datafusion_defaults =
            WriterPropertiesBuilder::try_from(&default_table_writer_opts)
                .unwrap()
                .build();

        // Expected: how the defaults should not match
        assert_ne!(
            default_writer_props.created_by(),
            from_datafusion_defaults.created_by(),
            "should have different created_by sources",
        );
        assert!(
            default_writer_props
                .created_by()
                .starts_with("parquet-rs version"),
            "should indicate that writer_props defaults came from the extern parquet crate",
        );
        assert!(
            default_table_writer_opts
                .global
                .created_by
                .starts_with("datafusion version"),
            "should indicate that table_parquet_opts defaults came from datafusion",
        );

        // Expected: the datafusion default compression is different from arrow-rs's parquet
        assert_eq!(
            default_writer_props.compression(&"default".into()),
            Compression::UNCOMPRESSED,
            "extern parquet's default is None"
        );
        assert!(
            matches!(
                from_datafusion_defaults.compression(&"default".into()),
                Compression::ZSTD(_)
            ),
            "datafusion's default is zstd"
        );

        // Expected: the remaining should match
        let same_created_by = default_table_writer_opts.global.created_by.clone();
        let mut from_extern_parquet =
            session_config_from_writer_props(&default_writer_props);
        from_extern_parquet.global.created_by = same_created_by;
        from_extern_parquet.global.compression = Some("zstd(3)".into());
        from_extern_parquet.global.skip_arrow_metadata = true;

        assert_eq!(
            default_table_writer_opts, from_extern_parquet,
            "the default writer_props should have the same configuration as the session's default TableParquetOptions",
        );
    }

    #[test]
    fn test_bloom_filter_defaults() {
        // the TableParquetOptions::default, with only the bloom filter turned on
        let mut default_table_writer_opts = TableParquetOptions::default();
        default_table_writer_opts.global.bloom_filter_on_write = true;
        default_table_writer_opts.arrow_schema(&Arc::new(Schema::empty())); // add the required arrow schema
        let from_datafusion_defaults =
            WriterPropertiesBuilder::try_from(&default_table_writer_opts)
                .unwrap()
                .build();

        // the WriterProperties::default, with only the bloom filter turned on
        let default_writer_props = WriterProperties::builder()
            .set_bloom_filter_enabled(true)
            .build();

        assert_eq!(
            default_writer_props.bloom_filter_properties(&"default".into()),
            from_datafusion_defaults.bloom_filter_properties(&"default".into()),
            "parquet and datafusion props, should have the same bloom filter props",
        );
        assert_eq!(
            default_writer_props.bloom_filter_properties(&"default".into()),
            Some(&BloomFilterProperties::default()),
            "should use the default bloom filter props"
        );
    }

    #[test]
    fn test_bloom_filter_set_fpp_only() {
        // the TableParquetOptions::default, with only fpp set
        let mut default_table_writer_opts = TableParquetOptions::default();
        default_table_writer_opts.global.bloom_filter_on_write = true;
        default_table_writer_opts.global.bloom_filter_fpp = Some(0.42);
        default_table_writer_opts.arrow_schema(&Arc::new(Schema::empty())); // add the required arrow schema
        let from_datafusion_defaults =
            WriterPropertiesBuilder::try_from(&default_table_writer_opts)
                .unwrap()
                .build();

        // the WriterProperties::default, with only fpp set
        let default_writer_props = WriterProperties::builder()
            .set_bloom_filter_enabled(true)
            .set_bloom_filter_fpp(0.42)
            .build();

        assert_eq!(
            default_writer_props.bloom_filter_properties(&"default".into()),
            from_datafusion_defaults.bloom_filter_properties(&"default".into()),
            "parquet and datafusion props, should have the same bloom filter props",
        );
        assert_eq!(
            default_writer_props.bloom_filter_properties(&"default".into()),
            Some(&BloomFilterProperties {
                fpp: 0.42,
                ndv: DEFAULT_BLOOM_FILTER_NDV
            }),
            "should have only the fpp set, and the ndv at default",
        );
    }

    #[test]
    fn test_bloom_filter_set_ndv_only() {
        // the TableParquetOptions::default, with only ndv set
        let mut default_table_writer_opts = TableParquetOptions::default();
        default_table_writer_opts.global.bloom_filter_on_write = true;
        default_table_writer_opts.global.bloom_filter_ndv = Some(42);
        default_table_writer_opts.arrow_schema(&Arc::new(Schema::empty())); // add the required arrow schema
        let from_datafusion_defaults =
            WriterPropertiesBuilder::try_from(&default_table_writer_opts)
                .unwrap()
                .build();

        // the WriterProperties::default, with only ndv set
        let default_writer_props = WriterProperties::builder()
            .set_bloom_filter_enabled(true)
            .set_bloom_filter_ndv(42)
            .build();

        assert_eq!(
            default_writer_props.bloom_filter_properties(&"default".into()),
            from_datafusion_defaults.bloom_filter_properties(&"default".into()),
            "parquet and datafusion props, should have the same bloom filter props",
        );
        assert_eq!(
            default_writer_props.bloom_filter_properties(&"default".into()),
            Some(&BloomFilterProperties {
                fpp: DEFAULT_BLOOM_FILTER_FPP,
                ndv: 42
            }),
            "should have only the ndv set, and the fpp at default",
        );
    }
}
