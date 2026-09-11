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

//! Conversions from the protobuf messages in this crate to their
//! `datafusion-common` counterparts.
//!
//! The DataFusion side of these conversions lives *below* this crate in the
//! dependency graph, so it cannot host the impls itself. They live here
//! instead, on the local proto type — the same arrangement
//! `datafusion-proto-common` uses for `ScalarValue` and `Statistics`.

use std::sync::Arc;

use datafusion_common::config::{
    CsvOptions, JsonOptions, MaxRowGroupBytes, ParquetCdcOptions, ParquetColumnOptions,
    ParquetOptions, TableParquetOptions,
};
use datafusion_common::display::{PlanType, StringifiedPlan};
use datafusion_common::parsers::{CompressionTypeVariant, CsvQuoteStyle};
use datafusion_common::utils::usize_from_wire;
use datafusion_common::{
    JoinConstraint, JoinType, NullEquality, RecursionUnnestOption, TableReference,
    UnnestOptions,
};
use datafusion_proto_common::FromProtoError as Error;

use crate::protobuf::{
    self, AnalyzedLogicalPlanType, CsvOptions as CsvOptionsProto,
    CsvQuoteStyle as CsvQuoteStyleProto, JsonOptions as JsonOptionsProto,
    OptimizedLogicalPlanType, OptimizedPhysicalPlanType,
    ParquetCdcOptions as ParquetCdcOptionsProto,
    ParquetColumnOptions as ParquetColumnOptionsProto,
    ParquetOptions as ParquetOptionsProto,
    TableParquetOptions as TableParquetOptionsProto, parquet_column_options,
    parquet_options,
    plan_type::PlanTypeEnum::{
        AnalyzedLogicalPlan, FinalAnalyzedLogicalPlan, FinalLogicalPlan,
        FinalPhysicalPlan, FinalPhysicalPlanWithSchema, FinalPhysicalPlanWithStats,
        InitialLogicalPlan, InitialPhysicalPlan, InitialPhysicalPlanWithSchema,
        InitialPhysicalPlanWithStats, OptimizedLogicalPlan, OptimizedPhysicalPlan,
        PhysicalPlanError,
    },
};

impl From<&protobuf::UnnestOptions> for UnnestOptions {
    fn from(opts: &protobuf::UnnestOptions) -> Self {
        use datafusion_common::NullHandling;
        use protobuf::unnest_options::NullHandling as ProtoNullHandling;
        let null_handling = match ProtoNullHandling::try_from(opts.null_handling) {
            Ok(ProtoNullHandling::Preserve) => NullHandling::Preserve,
            Ok(ProtoNullHandling::Drop) => NullHandling::Drop,
            Ok(ProtoNullHandling::PreserveAndExpandEmpty) => {
                NullHandling::PreserveAndExpandEmpty
            }
            // Unknown enum values fall back to the default (Preserve), which
            // matches DataFusion's historical behavior.
            Err(_) => NullHandling::Preserve,
        };
        Self {
            null_handling,
            recursions: opts
                .recursions
                .iter()
                .map(|r| RecursionUnnestOption {
                    input_column: r.input_column.as_ref().unwrap().into(),
                    output_column: r.output_column.as_ref().unwrap().into(),
                    depth: r.depth as usize,
                })
                .collect::<Vec<_>>(),
        }
    }
}

impl TryFrom<protobuf::TableReference> for TableReference {
    type Error = Error;

    fn try_from(value: protobuf::TableReference) -> Result<Self, Self::Error> {
        use protobuf::table_reference::TableReferenceEnum;
        let table_reference_enum = value
            .table_reference_enum
            .ok_or_else(|| Error::required("table_reference_enum"))?;

        match table_reference_enum {
            TableReferenceEnum::Bare(protobuf::BareTableReference { table }) => {
                Ok(TableReference::bare(table))
            }
            TableReferenceEnum::Partial(protobuf::PartialTableReference {
                schema,
                table,
            }) => Ok(TableReference::partial(schema, table)),
            TableReferenceEnum::Full(protobuf::FullTableReference {
                catalog,
                schema,
                table,
            }) => Ok(TableReference::full(catalog, schema, table)),
        }
    }
}

impl From<&protobuf::StringifiedPlan> for StringifiedPlan {
    fn from(stringified_plan: &protobuf::StringifiedPlan) -> Self {
        Self {
            plan_type: match stringified_plan
                .plan_type
                .as_ref()
                .and_then(|pt| pt.plan_type_enum.as_ref())
                .unwrap_or_else(|| {
                    panic!(
                        "Cannot create protobuf::StringifiedPlan from {stringified_plan:?}"
                    )
                }) {
                InitialLogicalPlan(_) => PlanType::InitialLogicalPlan,
                AnalyzedLogicalPlan(AnalyzedLogicalPlanType { analyzer_name }) => {
                    PlanType::AnalyzedLogicalPlan {
                        analyzer_name:analyzer_name.clone()
                    }
                }
                FinalAnalyzedLogicalPlan(_) => PlanType::FinalAnalyzedLogicalPlan,
                OptimizedLogicalPlan(OptimizedLogicalPlanType { optimizer_name }) => {
                    PlanType::OptimizedLogicalPlan {
                        optimizer_name: optimizer_name.clone(),
                    }
                }
                FinalLogicalPlan(_) => PlanType::FinalLogicalPlan,
                InitialPhysicalPlan(_) => PlanType::InitialPhysicalPlan,
                InitialPhysicalPlanWithStats(_) => PlanType::InitialPhysicalPlanWithStats,
                InitialPhysicalPlanWithSchema(_) => PlanType::InitialPhysicalPlanWithSchema,
                OptimizedPhysicalPlan(OptimizedPhysicalPlanType { optimizer_name }) => {
                    PlanType::OptimizedPhysicalPlan {
                        optimizer_name: optimizer_name.clone(),
                    }
                }
                FinalPhysicalPlan(_) => PlanType::FinalPhysicalPlan,
                FinalPhysicalPlanWithStats(_) => PlanType::FinalPhysicalPlanWithStats,
                FinalPhysicalPlanWithSchema(_) => PlanType::FinalPhysicalPlanWithSchema,
                PhysicalPlanError(_) => PlanType::PhysicalPlanError,
            },
            plan: Arc::new(stringified_plan.plan.clone()),
        }
    }
}

impl From<protobuf::JoinType> for JoinType {
    fn from(t: protobuf::JoinType) -> Self {
        match t {
            protobuf::JoinType::Inner => JoinType::Inner,
            protobuf::JoinType::Left => JoinType::Left,
            protobuf::JoinType::Right => JoinType::Right,
            protobuf::JoinType::Full => JoinType::Full,
            protobuf::JoinType::Leftsemi => JoinType::LeftSemi,
            protobuf::JoinType::Rightsemi => JoinType::RightSemi,
            protobuf::JoinType::Leftanti => JoinType::LeftAnti,
            protobuf::JoinType::Rightanti => JoinType::RightAnti,
            protobuf::JoinType::Leftmark => JoinType::LeftMark,
            protobuf::JoinType::Rightmark => JoinType::RightMark,
        }
    }
}

impl From<protobuf::JoinConstraint> for JoinConstraint {
    fn from(t: protobuf::JoinConstraint) -> Self {
        match t {
            protobuf::JoinConstraint::On => JoinConstraint::On,
            protobuf::JoinConstraint::Using => JoinConstraint::Using,
        }
    }
}

impl From<protobuf::NullEquality> for NullEquality {
    fn from(t: protobuf::NullEquality) -> Self {
        match t {
            protobuf::NullEquality::NullEqualsNothing => NullEquality::NullEqualsNothing,
            protobuf::NullEquality::NullEqualsNull => NullEquality::NullEqualsNull,
        }
    }
}

impl TryFrom<&CsvOptionsProto> for CsvOptions {
    type Error = datafusion_common::DataFusionError;

    fn try_from(proto: &CsvOptionsProto) -> datafusion_common::Result<Self, Self::Error> {
        Ok(CsvOptions {
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
            schema_infer_max_rec: proto
                .schema_infer_max_rec
                .map(|value| usize_from_wire(value, "CsvOptions", "schema_infer_max_rec"))
                .transpose()?,
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
            null_regex: if proto.null_regex.is_empty() {
                None
            } else {
                Some(proto.null_regex.clone())
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
            truncated_rows: if proto.truncated_rows.is_empty() {
                None
            } else {
                Some(proto.truncated_rows[0] != 0)
            },
            compression_level: proto.compression_level,
            quote_style: match CsvQuoteStyleProto::try_from(proto.quote_style) {
                Ok(CsvQuoteStyleProto::Always) => CsvQuoteStyle::Always,
                Ok(CsvQuoteStyleProto::NonNumeric) => CsvQuoteStyle::NonNumeric,
                Ok(CsvQuoteStyleProto::Never) => CsvQuoteStyle::Never,
                Ok(CsvQuoteStyleProto::Necessary) => CsvQuoteStyle::Necessary,
                _ => CsvQuoteStyle::Necessary,
            },
            ignore_leading_whitespace: if proto.ignore_leading_whitespace.is_empty() {
                None
            } else {
                Some(proto.ignore_leading_whitespace[0] != 0)
            },
            ignore_trailing_whitespace: if proto.ignore_trailing_whitespace.is_empty() {
                None
            } else {
                Some(proto.ignore_trailing_whitespace[0] != 0)
            },
        })
    }
}

impl TryFrom<&JsonOptionsProto> for JsonOptions {
    type Error = datafusion_common::DataFusionError;

    fn try_from(
        proto: &JsonOptionsProto,
    ) -> datafusion_common::Result<Self, Self::Error> {
        Ok(JsonOptions {
            compression: match proto.compression {
                0 => CompressionTypeVariant::GZIP,
                1 => CompressionTypeVariant::BZIP2,
                2 => CompressionTypeVariant::XZ,
                3 => CompressionTypeVariant::ZSTD,
                _ => CompressionTypeVariant::UNCOMPRESSED,
            },
            schema_infer_max_rec: proto
                .schema_infer_max_rec
                .map(|value| {
                    usize_from_wire(value, "JsonOptions", "schema_infer_max_rec")
                })
                .transpose()?,
            compression_level: proto.compression_level,
            newline_delimited: proto.newline_delimited.unwrap_or(true),
        })
    }
}

impl TryFrom<ParquetCdcOptionsProto> for ParquetCdcOptions {
    type Error = datafusion_common::DataFusionError;

    fn try_from(
        value: ParquetCdcOptionsProto,
    ) -> datafusion_common::Result<Self, Self::Error> {
        let to_usize =
            |value: u64, field: &str| usize_from_wire(value, "ParquetCdcOptions", field);
        Ok(ParquetCdcOptions {
            enabled: value.enabled,
            min_chunk_size: to_usize(value.min_chunk_size, "min_chunk_size")?,
            max_chunk_size: to_usize(value.max_chunk_size, "max_chunk_size")?,
            norm_level: value.norm_level,
        })
    }
}

impl TryFrom<&ParquetOptionsProto> for ParquetOptions {
    type Error = datafusion_common::DataFusionError;

    fn try_from(
        proto: &ParquetOptionsProto,
    ) -> datafusion_common::Result<Self, Self::Error> {
        let writer_version = match proto.writer_version.as_str() {
            // Proto3 decodes an omitted string field as the empty string. The
            // schema documents writer_version's logical default as "1.0", so
            // preserve that default when the field is absent on the wire.
            "" => ParquetOptions::default().writer_version,
            version => version.parse()?,
        };
        let to_usize =
            |value: u64, field: &str| usize_from_wire(value, "ParquetOptions", field);

        Ok(ParquetOptions {
            enable_page_index: proto.enable_page_index,
            pruning: proto.pruning,
            skip_metadata: proto.skip_metadata,
            metadata_size_hint: proto
                .metadata_size_hint_opt
                .as_ref()
                .map(|opt| match opt {
                    parquet_options::MetadataSizeHintOpt::MetadataSizeHint(size) => {
                        to_usize(*size, "metadata_size_hint")
                    }
                })
                .transpose()?,
            pushdown_filters: proto.pushdown_filters,
            reorder_filters: proto.reorder_filters,
            force_filter_selections: proto.force_filter_selections,
            data_pagesize_limit: to_usize(
                proto.data_pagesize_limit,
                "data_pagesize_limit",
            )?,
            write_batch_size: to_usize(proto.write_batch_size, "write_batch_size")?,
            writer_version,
            compression: proto.compression_opt.as_ref().map(|opt| match opt {
                parquet_options::CompressionOpt::Compression(compression) => {
                    compression.clone()
                }
            }),
            dictionary_enabled: proto.dictionary_enabled_opt.as_ref().map(|opt| {
                match opt {
                    parquet_options::DictionaryEnabledOpt::DictionaryEnabled(
                        enabled,
                    ) => *enabled,
                }
            }),
            dictionary_page_size_limit: to_usize(
                proto.dictionary_page_size_limit,
                "dictionary_page_size_limit",
            )?,
            statistics_enabled: proto
                .statistics_enabled_opt
                .as_ref()
                .map(|opt| match opt {
                    parquet_options::StatisticsEnabledOpt::StatisticsEnabled(
                        statistics,
                    ) => statistics.parse(),
                })
                .transpose()?,
            max_row_group_size: to_usize(proto.max_row_group_size, "max_row_group_size")?,
            max_in_list_size: to_usize(proto.max_in_list_size, "max_in_list_size")?,
            created_by: proto.created_by.clone(),
            column_index_truncate_length: proto
                .column_index_truncate_length_opt
                .as_ref()
                .map(|opt| match opt {
                    parquet_options::ColumnIndexTruncateLengthOpt::ColumnIndexTruncateLength(length) => {
                        to_usize(*length, "column_index_truncate_length")
                    }
                })
                .transpose()?,
            statistics_truncate_length: proto
                .statistics_truncate_length_opt
                .as_ref()
                .map(|opt| match opt {
                    parquet_options::StatisticsTruncateLengthOpt::StatisticsTruncateLength(length) => {
                        to_usize(*length, "statistics_truncate_length")
                    }
                })
                .transpose()?,
            data_page_row_count_limit: to_usize(
                proto.data_page_row_count_limit,
                "data_page_row_count_limit",
            )?,
            encoding: proto.encoding_opt.as_ref().map(|opt| match opt {
                parquet_options::EncodingOpt::Encoding(encoding) => {
                    encoding.clone()
                }
            }),
            bloom_filter_on_read: proto.bloom_filter_on_read,
            bloom_filter_on_write: proto.bloom_filter_on_write,
            bloom_filter_fpp: proto
                .bloom_filter_fpp_opt
                .as_ref()
                .map(|opt| match opt {
                    parquet_options::BloomFilterFppOpt::BloomFilterFpp(fpp) => *fpp,
                }),
            bloom_filter_ndv: proto
                .bloom_filter_ndv_opt
                .as_ref()
                .map(|opt| match opt {
                    parquet_options::BloomFilterNdvOpt::BloomFilterNdv(ndv) => *ndv,
                }),
            allow_single_file_parallelism: proto.allow_single_file_parallelism,
            maximum_parallel_row_group_writers: to_usize(
                proto.maximum_parallel_row_group_writers,
                "maximum_parallel_row_group_writers",
            )?,
            maximum_buffered_record_batches_per_stream: to_usize(
                proto.maximum_buffered_record_batches_per_stream,
                "maximum_buffered_record_batches_per_stream",
            )?,
            schema_force_view_types: proto.schema_force_view_types,
            binary_as_string: proto.binary_as_string,
            skip_arrow_metadata: proto.skip_arrow_metadata,
            coerce_int96: proto.coerce_int96_opt.as_ref().map(|opt| match opt {
                parquet_options::CoerceInt96Opt::CoerceInt96(coerce_int96) => {
                    coerce_int96.clone()
                }
            }),
            coerce_int96_tz: proto
                .coerce_int96_tz_opt
                .as_ref()
                .map(|opt| match opt {
                    parquet_options::CoerceInt96TzOpt::CoerceInt96Tz(tz) => {
                        tz.clone()
                    }
                }),
            max_predicate_cache_size: proto
                .max_predicate_cache_size_opt
                .as_ref()
                .map(|opt| match opt {
                    parquet_options::MaxPredicateCacheSizeOpt::MaxPredicateCacheSize(
                        size,
                    ) => to_usize(*size, "max_predicate_cache_size"),
                })
                .transpose()?,
            max_row_group_bytes: proto
                .max_row_group_bytes_opt
                .as_ref()
                .map(|opt| match opt {
                    parquet_options::MaxRowGroupBytesOpt::MaxRowGroupBytes(size) => {
                        MaxRowGroupBytes::try_new(to_usize(*size, "max_row_group_bytes")?)
                    }
                })
                .transpose()?,
            content_defined_chunking: proto
                .content_defined_chunking
                .map(ParquetCdcOptions::try_from)
                .transpose()?
                .unwrap_or_default(),
        })
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
        }
    }
}

impl TryFrom<&TableParquetOptionsProto> for TableParquetOptions {
    type Error = datafusion_common::DataFusionError;

    fn try_from(
        proto: &TableParquetOptionsProto,
    ) -> datafusion_common::Result<Self, Self::Error> {
        Ok(TableParquetOptions {
            global: proto
                .global
                .as_ref()
                .map(ParquetOptions::try_from)
                .transpose()?
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
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_parquet_statistics() {
        let proto = ParquetOptionsProto {
            statistics_enabled_opt: Some(
                parquet_options::StatisticsEnabledOpt::StatisticsEnabled(
                    "invalid".to_string(),
                ),
            ),
            ..Default::default()
        };

        let err = ParquetOptions::try_from(&proto).unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid parquet statistics setting: invalid")
        );
    }
}
