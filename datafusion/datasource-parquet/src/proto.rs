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

//! Conversions between `datafusion-proto-common` types and the `ParquetSink`
//! type. Enabled by the `proto` feature.

use datafusion_common::proto::proto_error;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::file_sink_config::FileSink;
use datafusion_proto_common::protobuf;

use crate::file_format::ParquetSink;

impl TryFrom<&protobuf::ParquetSink> for ParquetSink {
    type Error = DataFusionError;

    fn try_from(value: &protobuf::ParquetSink) -> Result<Self, Self::Error> {
        let config = value
            .config
            .as_ref()
            .ok_or_else(|| proto_error("Missing required field config in ParquetSink"))?
            .try_into()?;
        let parquet_options = value
            .parquet_options
            .as_ref()
            .ok_or_else(|| {
                proto_error("Missing required field parquet_options in ParquetSink")
            })?
            .try_into()?;
        Ok(Self::new(config, parquet_options))
    }
}

impl TryFrom<&ParquetSink> for protobuf::ParquetSink {
    type Error = DataFusionError;

    fn try_from(value: &ParquetSink) -> Result<Self, Self::Error> {
        Ok(Self {
            config: Some(value.config().try_into()?),
            parquet_options: Some(value.parquet_options().try_into()?),
        })
    }
}

impl From<&crate::file_format::ParquetFormatFactory> for protobuf::TableParquetOptions {
    fn from(factory: &crate::file_format::ParquetFormatFactory) -> Self {
        use datafusion_proto_common::protobuf::{
            CdcOptions as CdcOptionsProto,
            ParquetColumnOptions as ParquetColumnOptionsProto,
            ParquetColumnSpecificOptions, ParquetOptions as ParquetOptionsProto,
            parquet_column_options, parquet_options,
        };
        let Some(global_options) = factory.options.as_ref().cloned() else {
            return protobuf::TableParquetOptions::default();
        };
        let column_specific_options = global_options.column_specific_options;
        protobuf::TableParquetOptions {
            global: Some(ParquetOptionsProto {
                enable_page_index: global_options.global.enable_page_index,
                pruning: global_options.global.pruning,
                skip_metadata: global_options.global.skip_metadata,
                metadata_size_hint_opt: global_options.global.metadata_size_hint.map(|size| {
                    parquet_options::MetadataSizeHintOpt::MetadataSizeHint(size as u64)
                }),
                pushdown_filters: global_options.global.pushdown_filters,
                reorder_filters: global_options.global.reorder_filters,
                force_filter_selections: global_options.global.force_filter_selections,
                data_pagesize_limit: global_options.global.data_pagesize_limit as u64,
                write_batch_size: global_options.global.write_batch_size as u64,
                writer_version: global_options.global.writer_version.to_string(),
                compression_opt: global_options.global.compression.map(parquet_options::CompressionOpt::Compression),
                dictionary_enabled_opt: global_options.global.dictionary_enabled.map(parquet_options::DictionaryEnabledOpt::DictionaryEnabled),
                dictionary_page_size_limit: global_options.global.dictionary_page_size_limit as u64,
                statistics_enabled_opt: global_options.global.statistics_enabled.map(parquet_options::StatisticsEnabledOpt::StatisticsEnabled),
                max_row_group_size: global_options.global.max_row_group_size as u64,
                created_by: global_options.global.created_by.clone(),
                column_index_truncate_length_opt: global_options.global.column_index_truncate_length.map(|length| {
                    parquet_options::ColumnIndexTruncateLengthOpt::ColumnIndexTruncateLength(length as u64)
                }),
                statistics_truncate_length_opt: global_options.global.statistics_truncate_length.map(|length| {
                    parquet_options::StatisticsTruncateLengthOpt::StatisticsTruncateLength(length as u64)
                }),
                data_page_row_count_limit: global_options.global.data_page_row_count_limit as u64,
                encoding_opt: global_options.global.encoding.map(parquet_options::EncodingOpt::Encoding),
                bloom_filter_on_read: global_options.global.bloom_filter_on_read,
                bloom_filter_on_write: global_options.global.bloom_filter_on_write,
                bloom_filter_fpp_opt: global_options.global.bloom_filter_fpp.map(parquet_options::BloomFilterFppOpt::BloomFilterFpp),
                bloom_filter_ndv_opt: global_options.global.bloom_filter_ndv.map(parquet_options::BloomFilterNdvOpt::BloomFilterNdv),
                allow_single_file_parallelism: global_options.global.allow_single_file_parallelism,
                maximum_parallel_row_group_writers: global_options.global.maximum_parallel_row_group_writers as u64,
                maximum_buffered_record_batches_per_stream: global_options.global.maximum_buffered_record_batches_per_stream as u64,
                schema_force_view_types: global_options.global.schema_force_view_types,
                binary_as_string: global_options.global.binary_as_string,
                skip_arrow_metadata: global_options.global.skip_arrow_metadata,
                coerce_int96_opt: global_options.global.coerce_int96.map(parquet_options::CoerceInt96Opt::CoerceInt96),
                max_predicate_cache_size_opt: global_options.global.max_predicate_cache_size.map(|size| {
                    parquet_options::MaxPredicateCacheSizeOpt::MaxPredicateCacheSize(size as u64)
                }),
                content_defined_chunking: global_options.global.use_content_defined_chunking.as_ref().map(|cdc| {
                    CdcOptionsProto {
                        min_chunk_size: cdc.min_chunk_size as u64,
                        max_chunk_size: cdc.max_chunk_size as u64,
                        norm_level: cdc.norm_level,
                    }
                }),
            }),
            column_specific_options: column_specific_options.into_iter().map(|(column_name, options)| {
                ParquetColumnSpecificOptions {
                    column_name,
                    options: Some(ParquetColumnOptionsProto {
                        bloom_filter_enabled_opt: options.bloom_filter_enabled.map(parquet_column_options::BloomFilterEnabledOpt::BloomFilterEnabled),
                        encoding_opt: options.encoding.map(parquet_column_options::EncodingOpt::Encoding),
                        dictionary_enabled_opt: options.dictionary_enabled.map(parquet_column_options::DictionaryEnabledOpt::DictionaryEnabled),
                        compression_opt: options.compression.map(parquet_column_options::CompressionOpt::Compression),
                        statistics_enabled_opt: options.statistics_enabled.map(parquet_column_options::StatisticsEnabledOpt::StatisticsEnabled),
                        bloom_filter_fpp_opt: options.bloom_filter_fpp.map(parquet_column_options::BloomFilterFppOpt::BloomFilterFpp),
                        bloom_filter_ndv_opt: options.bloom_filter_ndv.map(parquet_column_options::BloomFilterNdvOpt::BloomFilterNdv),
                    })
                }
            }).collect(),
            key_value_metadata: global_options.key_value_metadata
                .iter()
                .filter_map(|(key, value)| {
                    value.as_ref().map(|v| (key.clone(), v.clone()))
                })
                .collect(),
        }
    }
}
