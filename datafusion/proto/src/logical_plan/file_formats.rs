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

use std::sync::Arc;

use datafusion::{
    config::{
        CsvOptions, JsonOptions, ParquetColumnOptions, ParquetOptions,
        TableParquetOptions,
    },
    datasource::file_format::{
        arrow::ArrowFormatFactory, csv::CsvFormatFactory, json::JsonFormatFactory,
        parquet::ParquetFormatFactory, FileFormatFactory,
    },
    prelude::SessionContext,
};
use datafusion_common::{
    exec_err, not_impl_err, parsers::CompressionTypeVariant, DataFusionError,
    TableReference,
};
use prost::Message;

use crate::protobuf::{
    parquet_column_options, parquet_options, CsvOptions as CsvOptionsProto,
    JsonOptions as JsonOptionsProto, ParquetColumnOptions as ParquetColumnOptionsProto,
    ParquetColumnSpecificOptions, ParquetOptions as ParquetOptionsProto,
    TableParquetOptions as TableParquetOptionsProto,
};

use super::LogicalExtensionCodec;

#[derive(Debug)]
pub struct CsvLogicalExtensionCodec;

impl CsvOptionsProto {
    fn from_factory(factory: &CsvFormatFactory) -> Self {
        if let Some(options) = &factory.options {
            CsvOptionsProto {
                has_header: options.has_header.map_or(vec![], |v| vec![v as u8]),
                delimiter: vec![options.delimiter],
                quote: vec![options.quote],
                terminator: options.terminator.map_or(vec![], |v| vec![v]),
                escape: options.escape.map_or(vec![], |v| vec![v]),
                double_quote: options.double_quote.map_or(vec![], |v| vec![v as u8]),
                compression: options.compression as i32,
                schema_infer_max_rec: options.schema_infer_max_rec as u64,
                date_format: options.date_format.clone().unwrap_or_default(),
                datetime_format: options.datetime_format.clone().unwrap_or_default(),
                timestamp_format: options.timestamp_format.clone().unwrap_or_default(),
                timestamp_tz_format: options
                    .timestamp_tz_format
                    .clone()
                    .unwrap_or_default(),
                time_format: options.time_format.clone().unwrap_or_default(),
                null_value: options.null_value.clone().unwrap_or_default(),
                comment: options.comment.map_or(vec![], |v| vec![v]),
                newlines_in_values: options
                    .newlines_in_values
                    .map_or(vec![], |v| vec![v as u8]),
            }
        } else {
            CsvOptionsProto::default()
        }
    }
}

impl From<&CsvOptionsProto> for CsvOptions {
    fn from(proto: &CsvOptionsProto) -> Self {
        CsvOptions {
            has_header: if !proto.has_header.is_empty() {
                Some(proto.has_header[0] != 0)
            } else {
                None
            },
            delimiter: proto.delimiter.first().copied().unwrap_or(b','),
            quote: proto.quote.first().copied().unwrap_or(b'"'),
            terminator: if !proto.terminator.is_empty() {
                Some(proto.terminator[0])
            } else {
                None
            },
            escape: if !proto.escape.is_empty() {
                Some(proto.escape[0])
            } else {
                None
            },
            double_quote: if !proto.double_quote.is_empty() {
                Some(proto.double_quote[0] != 0)
            } else {
                None
            },
            compression: match proto.compression {
                0 => CompressionTypeVariant::GZIP,
                1 => CompressionTypeVariant::BZIP2,
                2 => CompressionTypeVariant::XZ,
                3 => CompressionTypeVariant::ZSTD,
                _ => CompressionTypeVariant::UNCOMPRESSED,
            },
            schema_infer_max_rec: proto.schema_infer_max_rec as usize,
            date_format: if proto.date_format.is_empty() {
                None
            } else {
                Some(proto.date_format.clone())
            },
            datetime_format: if proto.datetime_format.is_empty() {
                None
            } else {
                Some(proto.datetime_format.clone())
            },
            timestamp_format: if proto.timestamp_format.is_empty() {
                None
            } else {
                Some(proto.timestamp_format.clone())
            },
            timestamp_tz_format: if proto.timestamp_tz_format.is_empty() {
                None
            } else {
                Some(proto.timestamp_tz_format.clone())
            },
            time_format: if proto.time_format.is_empty() {
                None
            } else {
                Some(proto.time_format.clone())
            },
            null_value: if proto.null_value.is_empty() {
                None
            } else {
                Some(proto.null_value.clone())
            },
            comment: if !proto.comment.is_empty() {
                Some(proto.comment[0])
            } else {
                None
            },
            newlines_in_values: if proto.newlines_in_values.is_empty() {
                None
            } else {
                Some(proto.newlines_in_values[0] != 0)
            },
        }
    }
}

// TODO! This is a placeholder for now and needs to be implemented for real.
impl LogicalExtensionCodec for CsvLogicalExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[datafusion_expr::LogicalPlan],
        _ctx: &SessionContext,
    ) -> datafusion_common::Result<datafusion_expr::Extension> {
        not_impl_err!("Method not implemented")
    }

    fn try_encode(
        &self,
        _node: &datafusion_expr::Extension,
        _buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        not_impl_err!("Method not implemented")
    }

    fn try_decode_table_provider(
        &self,
        _buf: &[u8],
        _table_ref: &TableReference,
        _schema: arrow::datatypes::SchemaRef,
        _ctx: &SessionContext,
    ) -> datafusion_common::Result<Arc<dyn datafusion::datasource::TableProvider>> {
        not_impl_err!("Method not implemented")
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        _node: Arc<dyn datafusion::datasource::TableProvider>,
        _buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        not_impl_err!("Method not implemented")
    }

    fn try_decode_file_format(
        &self,
        buf: &[u8],
        _ctx: &SessionContext,
    ) -> datafusion_common::Result<Arc<dyn FileFormatFactory>> {
        let proto = CsvOptionsProto::decode(buf).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to decode CsvOptionsProto: {:?}",
                e
            ))
        })?;
        let options: CsvOptions = (&proto).into();
        Ok(Arc::new(CsvFormatFactory {
            options: Some(options),
        }))
    }

    fn try_encode_file_format(
        &self,
        buf: &mut Vec<u8>,
        node: Arc<dyn FileFormatFactory>,
    ) -> datafusion_common::Result<()> {
        let options =
            if let Some(csv_factory) = node.as_any().downcast_ref::<CsvFormatFactory>() {
                csv_factory.options.clone().unwrap_or_default()
            } else {
                return exec_err!("{}", "Unsupported FileFormatFactory type".to_string());
            };

        let proto = CsvOptionsProto::from_factory(&CsvFormatFactory {
            options: Some(options),
        });

        proto.encode(buf).map_err(|e| {
            DataFusionError::Execution(format!("Failed to encode CsvOptions: {:?}", e))
        })?;

        Ok(())
    }
}

impl JsonOptionsProto {
    fn from_factory(factory: &JsonFormatFactory) -> Self {
        if let Some(options) = &factory.options {
            JsonOptionsProto {
                compression: options.compression as i32,
                schema_infer_max_rec: options.schema_infer_max_rec as u64,
            }
        } else {
            JsonOptionsProto::default()
        }
    }
}

impl From<&JsonOptionsProto> for JsonOptions {
    fn from(proto: &JsonOptionsProto) -> Self {
        JsonOptions {
            compression: match proto.compression {
                0 => CompressionTypeVariant::GZIP,
                1 => CompressionTypeVariant::BZIP2,
                2 => CompressionTypeVariant::XZ,
                3 => CompressionTypeVariant::ZSTD,
                _ => CompressionTypeVariant::UNCOMPRESSED,
            },
            schema_infer_max_rec: proto.schema_infer_max_rec as usize,
        }
    }
}

#[derive(Debug)]
pub struct JsonLogicalExtensionCodec;

// TODO! This is a placeholder for now and needs to be implemented for real.
impl LogicalExtensionCodec for JsonLogicalExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[datafusion_expr::LogicalPlan],
        _ctx: &SessionContext,
    ) -> datafusion_common::Result<datafusion_expr::Extension> {
        not_impl_err!("Method not implemented")
    }

    fn try_encode(
        &self,
        _node: &datafusion_expr::Extension,
        _buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        not_impl_err!("Method not implemented")
    }

    fn try_decode_table_provider(
        &self,
        _buf: &[u8],
        _table_ref: &TableReference,
        _schema: arrow::datatypes::SchemaRef,
        _ctx: &SessionContext,
    ) -> datafusion_common::Result<Arc<dyn datafusion::datasource::TableProvider>> {
        not_impl_err!("Method not implemented")
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        _node: Arc<dyn datafusion::datasource::TableProvider>,
        _buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        not_impl_err!("Method not implemented")
    }

    fn try_decode_file_format(
        &self,
        buf: &[u8],
        _ctx: &SessionContext,
    ) -> datafusion_common::Result<Arc<dyn FileFormatFactory>> {
        let proto = JsonOptionsProto::decode(buf).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to decode JsonOptionsProto: {:?}",
                e
            ))
        })?;
        let options: JsonOptions = (&proto).into();
        Ok(Arc::new(JsonFormatFactory {
            options: Some(options),
        }))
    }

    fn try_encode_file_format(
        &self,
        buf: &mut Vec<u8>,
        node: Arc<dyn FileFormatFactory>,
    ) -> datafusion_common::Result<()> {
        let options = if let Some(json_factory) =
            node.as_any().downcast_ref::<JsonFormatFactory>()
        {
            json_factory.options.clone().unwrap_or_default()
        } else {
            return Err(DataFusionError::Execution(
                "Unsupported FileFormatFactory type".to_string(),
            ));
        };

        let proto = JsonOptionsProto::from_factory(&JsonFormatFactory {
            options: Some(options),
        });

        proto.encode(buf).map_err(|e| {
            DataFusionError::Execution(format!("Failed to encode JsonOptions: {:?}", e))
        })?;

        Ok(())
    }
}

impl TableParquetOptionsProto {
    fn from_factory(factory: &ParquetFormatFactory) -> Self {
        let global_options = if let Some(ref options) = factory.options {
            options.clone()
        } else {
            return TableParquetOptionsProto::default();
        };

        let column_specific_options = global_options.column_specific_options;
        TableParquetOptionsProto {
            global: Some(ParquetOptionsProto {
                enable_page_index: global_options.global.enable_page_index,
                pruning: global_options.global.pruning,
                skip_metadata: global_options.global.skip_metadata,
                metadata_size_hint_opt: global_options.global.metadata_size_hint.map(|size| {
                    parquet_options::MetadataSizeHintOpt::MetadataSizeHint(size as u64)
                }),
                pushdown_filters: global_options.global.pushdown_filters,
                reorder_filters: global_options.global.reorder_filters,
                data_pagesize_limit: global_options.global.data_pagesize_limit as u64,
                write_batch_size: global_options.global.write_batch_size as u64,
                writer_version: global_options.global.writer_version.clone(),
                compression_opt: global_options.global.compression.map(|compression| {
                    parquet_options::CompressionOpt::Compression(compression)
                }),
                dictionary_enabled_opt: global_options.global.dictionary_enabled.map(|enabled| {
                    parquet_options::DictionaryEnabledOpt::DictionaryEnabled(enabled)
                }),
                dictionary_page_size_limit: global_options.global.dictionary_page_size_limit as u64,
                statistics_enabled_opt: global_options.global.statistics_enabled.map(|enabled| {
                    parquet_options::StatisticsEnabledOpt::StatisticsEnabled(enabled)
                }),
                max_statistics_size_opt: global_options.global.max_statistics_size.map(|size| {
                    parquet_options::MaxStatisticsSizeOpt::MaxStatisticsSize(size as u64)
                }),
                max_row_group_size: global_options.global.max_row_group_size as u64,
                created_by: global_options.global.created_by.clone(),
                column_index_truncate_length_opt: global_options.global.column_index_truncate_length.map(|length| {
                    parquet_options::ColumnIndexTruncateLengthOpt::ColumnIndexTruncateLength(length as u64)
                }),
                data_page_row_count_limit: global_options.global.data_page_row_count_limit as u64,
                encoding_opt: global_options.global.encoding.map(|encoding| {
                    parquet_options::EncodingOpt::Encoding(encoding)
                }),
                bloom_filter_on_read: global_options.global.bloom_filter_on_read,
                bloom_filter_on_write: global_options.global.bloom_filter_on_write,
                bloom_filter_fpp_opt: global_options.global.bloom_filter_fpp.map(|fpp| {
                    parquet_options::BloomFilterFppOpt::BloomFilterFpp(fpp)
                }),
                bloom_filter_ndv_opt: global_options.global.bloom_filter_ndv.map(|ndv| {
                    parquet_options::BloomFilterNdvOpt::BloomFilterNdv(ndv)
                }),
                allow_single_file_parallelism: global_options.global.allow_single_file_parallelism,
                maximum_parallel_row_group_writers: global_options.global.maximum_parallel_row_group_writers as u64,
                maximum_buffered_record_batches_per_stream: global_options.global.maximum_buffered_record_batches_per_stream as u64,
                schema_force_view_types: global_options.global.schema_force_view_types,
                binary_as_string: global_options.global.binary_as_string,
            }),
            column_specific_options: column_specific_options.into_iter().map(|(column_name, options)| {
                ParquetColumnSpecificOptions {
                    column_name,
                    options: Some(ParquetColumnOptionsProto {
                        bloom_filter_enabled_opt: options.bloom_filter_enabled.map(|enabled| {
                            parquet_column_options::BloomFilterEnabledOpt::BloomFilterEnabled(enabled)
                        }),
                        encoding_opt: options.encoding.map(|encoding| {
                            parquet_column_options::EncodingOpt::Encoding(encoding)
                        }),
                        dictionary_enabled_opt: options.dictionary_enabled.map(|enabled| {
                            parquet_column_options::DictionaryEnabledOpt::DictionaryEnabled(enabled)
                        }),
                        compression_opt: options.compression.map(|compression| {
                            parquet_column_options::CompressionOpt::Compression(compression)
                        }),
                        statistics_enabled_opt: options.statistics_enabled.map(|enabled| {
                            parquet_column_options::StatisticsEnabledOpt::StatisticsEnabled(enabled)
                        }),
                        bloom_filter_fpp_opt: options.bloom_filter_fpp.map(|fpp| {
                            parquet_column_options::BloomFilterFppOpt::BloomFilterFpp(fpp)
                        }),
                        bloom_filter_ndv_opt: options.bloom_filter_ndv.map(|ndv| {
                            parquet_column_options::BloomFilterNdvOpt::BloomFilterNdv(ndv)
                        }),
                        max_statistics_size_opt: options.max_statistics_size.map(|size| {
                            parquet_column_options::MaxStatisticsSizeOpt::MaxStatisticsSize(size as u32)
                        }),
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

impl From<&ParquetOptionsProto> for ParquetOptions {
    fn from(proto: &ParquetOptionsProto) -> Self {
        ParquetOptions {
            enable_page_index: proto.enable_page_index,
            pruning: proto.pruning,
            skip_metadata: proto.skip_metadata,
            metadata_size_hint: proto.metadata_size_hint_opt.as_ref().map(|opt| match opt {
                parquet_options::MetadataSizeHintOpt::MetadataSizeHint(size) => *size as usize,
            }),
            pushdown_filters: proto.pushdown_filters,
            reorder_filters: proto.reorder_filters,
            data_pagesize_limit: proto.data_pagesize_limit as usize,
            write_batch_size: proto.write_batch_size as usize,
            writer_version: proto.writer_version.clone(),
            compression: proto.compression_opt.as_ref().map(|opt| match opt {
                parquet_options::CompressionOpt::Compression(compression) => compression.clone(),
            }),
            dictionary_enabled: proto.dictionary_enabled_opt.as_ref().map(|opt| match opt {
                parquet_options::DictionaryEnabledOpt::DictionaryEnabled(enabled) => *enabled,
            }),
            dictionary_page_size_limit: proto.dictionary_page_size_limit as usize,
            statistics_enabled: proto.statistics_enabled_opt.as_ref().map(|opt| match opt {
                parquet_options::StatisticsEnabledOpt::StatisticsEnabled(statistics) => statistics.clone(),
            }),
            max_statistics_size: proto.max_statistics_size_opt.as_ref().map(|opt| match opt {
                parquet_options::MaxStatisticsSizeOpt::MaxStatisticsSize(size) => *size as usize,
            }),
            max_row_group_size: proto.max_row_group_size as usize,
            created_by: proto.created_by.clone(),
            column_index_truncate_length: proto.column_index_truncate_length_opt.as_ref().map(|opt| match opt {
                parquet_options::ColumnIndexTruncateLengthOpt::ColumnIndexTruncateLength(length) => *length as usize,
            }),
            data_page_row_count_limit: proto.data_page_row_count_limit as usize,
            encoding: proto.encoding_opt.as_ref().map(|opt| match opt {
                parquet_options::EncodingOpt::Encoding(encoding) => encoding.clone(),
            }),
            bloom_filter_on_read: proto.bloom_filter_on_read,
            bloom_filter_on_write: proto.bloom_filter_on_write,
            bloom_filter_fpp: proto.bloom_filter_fpp_opt.as_ref().map(|opt| match opt {
                parquet_options::BloomFilterFppOpt::BloomFilterFpp(fpp) => *fpp,
            }),
            bloom_filter_ndv: proto.bloom_filter_ndv_opt.as_ref().map(|opt| match opt {
                parquet_options::BloomFilterNdvOpt::BloomFilterNdv(ndv) => *ndv,
            }),
            allow_single_file_parallelism: proto.allow_single_file_parallelism,
            maximum_parallel_row_group_writers: proto.maximum_parallel_row_group_writers as usize,
            maximum_buffered_record_batches_per_stream: proto.maximum_buffered_record_batches_per_stream as usize,
            schema_force_view_types: proto.schema_force_view_types,
            binary_as_string: proto.binary_as_string,
        }
    }
}

impl From<ParquetColumnOptionsProto> for ParquetColumnOptions {
    fn from(proto: ParquetColumnOptionsProto) -> Self {
        ParquetColumnOptions {
            bloom_filter_enabled: proto.bloom_filter_enabled_opt.map(
                |parquet_column_options::BloomFilterEnabledOpt::BloomFilterEnabled(v)| v,
            ),
            encoding: proto
                .encoding_opt
                .map(|parquet_column_options::EncodingOpt::Encoding(v)| v),
            dictionary_enabled: proto.dictionary_enabled_opt.map(
                |parquet_column_options::DictionaryEnabledOpt::DictionaryEnabled(v)| v,
            ),
            compression: proto
                .compression_opt
                .map(|parquet_column_options::CompressionOpt::Compression(v)| v),
            statistics_enabled: proto.statistics_enabled_opt.map(
                |parquet_column_options::StatisticsEnabledOpt::StatisticsEnabled(v)| v,
            ),
            bloom_filter_fpp: proto
                .bloom_filter_fpp_opt
                .map(|parquet_column_options::BloomFilterFppOpt::BloomFilterFpp(v)| v),
            bloom_filter_ndv: proto
                .bloom_filter_ndv_opt
                .map(|parquet_column_options::BloomFilterNdvOpt::BloomFilterNdv(v)| v),
            max_statistics_size: proto.max_statistics_size_opt.map(
                |parquet_column_options::MaxStatisticsSizeOpt::MaxStatisticsSize(v)| {
                    v as usize
                },
            ),
        }
    }
}

impl From<&TableParquetOptionsProto> for TableParquetOptions {
    fn from(proto: &TableParquetOptionsProto) -> Self {
        TableParquetOptions {
            global: proto
                .global
                .as_ref()
                .map(ParquetOptions::from)
                .unwrap_or_default(),
            column_specific_options: proto
                .column_specific_options
                .iter()
                .map(|parquet_column_options| {
                    (
                        parquet_column_options.column_name.clone(),
                        ParquetColumnOptions::from(
                            parquet_column_options.options.clone().unwrap_or_default(),
                        ),
                    )
                })
                .collect(),
            key_value_metadata: proto
                .key_value_metadata
                .iter()
                .map(|(k, v)| (k.clone(), Some(v.clone())))
                .collect(),
        }
    }
}

#[derive(Debug)]
pub struct ParquetLogicalExtensionCodec;

// TODO! This is a placeholder for now and needs to be implemented for real.
impl LogicalExtensionCodec for ParquetLogicalExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[datafusion_expr::LogicalPlan],
        _ctx: &SessionContext,
    ) -> datafusion_common::Result<datafusion_expr::Extension> {
        not_impl_err!("Method not implemented")
    }

    fn try_encode(
        &self,
        _node: &datafusion_expr::Extension,
        _buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        not_impl_err!("Method not implemented")
    }

    fn try_decode_table_provider(
        &self,
        _buf: &[u8],
        _table_ref: &TableReference,
        _schema: arrow::datatypes::SchemaRef,
        _ctx: &SessionContext,
    ) -> datafusion_common::Result<Arc<dyn datafusion::datasource::TableProvider>> {
        not_impl_err!("Method not implemented")
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        _node: Arc<dyn datafusion::datasource::TableProvider>,
        _buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        not_impl_err!("Method not implemented")
    }

    fn try_decode_file_format(
        &self,
        buf: &[u8],
        _ctx: &SessionContext,
    ) -> datafusion_common::Result<Arc<dyn FileFormatFactory>> {
        let proto = TableParquetOptionsProto::decode(buf).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to decode TableParquetOptionsProto: {:?}",
                e
            ))
        })?;
        let options: TableParquetOptions = (&proto).into();
        Ok(Arc::new(ParquetFormatFactory {
            options: Some(options),
        }))
    }

    fn try_encode_file_format(
        &self,
        buf: &mut Vec<u8>,
        node: Arc<dyn FileFormatFactory>,
    ) -> datafusion_common::Result<()> {
        let options = if let Some(parquet_factory) =
            node.as_any().downcast_ref::<ParquetFormatFactory>()
        {
            parquet_factory.options.clone().unwrap_or_default()
        } else {
            return Err(DataFusionError::Execution(
                "Unsupported FileFormatFactory type".to_string(),
            ));
        };

        let proto = TableParquetOptionsProto::from_factory(&ParquetFormatFactory {
            options: Some(options),
        });

        proto.encode(buf).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to encode TableParquetOptionsProto: {:?}",
                e
            ))
        })?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct ArrowLogicalExtensionCodec;

// TODO! This is a placeholder for now and needs to be implemented for real.
impl LogicalExtensionCodec for ArrowLogicalExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[datafusion_expr::LogicalPlan],
        _ctx: &SessionContext,
    ) -> datafusion_common::Result<datafusion_expr::Extension> {
        not_impl_err!("Method not implemented")
    }

    fn try_encode(
        &self,
        _node: &datafusion_expr::Extension,
        _buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        not_impl_err!("Method not implemented")
    }

    fn try_decode_table_provider(
        &self,
        _buf: &[u8],
        _table_ref: &TableReference,
        _schema: arrow::datatypes::SchemaRef,
        _ctx: &SessionContext,
    ) -> datafusion_common::Result<Arc<dyn datafusion::datasource::TableProvider>> {
        not_impl_err!("Method not implemented")
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        _node: Arc<dyn datafusion::datasource::TableProvider>,
        _buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        not_impl_err!("Method not implemented")
    }

    fn try_decode_file_format(
        &self,
        __buf: &[u8],
        __ctx: &SessionContext,
    ) -> datafusion_common::Result<Arc<dyn FileFormatFactory>> {
        Ok(Arc::new(ArrowFormatFactory::new()))
    }

    fn try_encode_file_format(
        &self,
        __buf: &mut Vec<u8>,
        __node: Arc<dyn FileFormatFactory>,
    ) -> datafusion_common::Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct AvroLogicalExtensionCodec;

// TODO! This is a placeholder for now and needs to be implemented for real.
impl LogicalExtensionCodec for AvroLogicalExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[datafusion_expr::LogicalPlan],
        _ctx: &SessionContext,
    ) -> datafusion_common::Result<datafusion_expr::Extension> {
        not_impl_err!("Method not implemented")
    }

    fn try_encode(
        &self,
        _node: &datafusion_expr::Extension,
        _buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        not_impl_err!("Method not implemented")
    }

    fn try_decode_table_provider(
        &self,
        _buf: &[u8],
        _table_ref: &TableReference,
        _schema: arrow::datatypes::SchemaRef,
        _cts: &SessionContext,
    ) -> datafusion_common::Result<Arc<dyn datafusion::datasource::TableProvider>> {
        not_impl_err!("Method not implemented")
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        _node: Arc<dyn datafusion::datasource::TableProvider>,
        _buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        not_impl_err!("Method not implemented")
    }

    fn try_decode_file_format(
        &self,
        __buf: &[u8],
        __ctx: &SessionContext,
    ) -> datafusion_common::Result<Arc<dyn FileFormatFactory>> {
        Ok(Arc::new(ArrowFormatFactory::new()))
    }

    fn try_encode_file_format(
        &self,
        __buf: &mut Vec<u8>,
        __node: Arc<dyn FileFormatFactory>,
    ) -> datafusion_common::Result<()> {
        Ok(())
    }
}
