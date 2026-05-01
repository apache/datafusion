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

use std::collections::HashMap;

use crate::error::_plan_datafusion_err;
use crate::parsers::CsvQuoteStyle;
use crate::{
    Column, ColumnStatistics, Constraint, Constraints, DFSchema, DataFusionError,
    JoinSide, ScalarValue, Statistics,
    config::{
        CsvOptions, JsonOptions, ParquetColumnOptions, ParquetOptions,
        TableParquetOptions,
    },
    file_options::{csv_writer::CsvWriterOptions, json_writer::JsonWriterOptions},
    parsers::CompressionTypeVariant,
    stats::Precision,
};
use arrow::array::{ArrayRef, RecordBatch};
use arrow::csv::WriterBuilder;
use arrow::datatypes::{
    DataType, IntervalDayTimeType, IntervalMonthDayNanoType, TimeUnit, UnionMode,
};
use arrow::ipc::writer::{
    CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions,
};
use datafusion_proto_common::protobuf_common as protobuf;
use datafusion_proto_common::protobuf_common::scalar_value::Value;

#[derive(Debug)]
pub enum Error {
    General(String),

    InvalidScalarValue(ScalarValue),

    InvalidScalarType(DataType),

    InvalidTimeUnit(TimeUnit),

    NotImplemented(String),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::General(desc) => write!(f, "General error: {desc}"),
            Self::InvalidScalarValue(value) => {
                write!(f, "{value:?} is invalid as a DataFusion scalar value")
            }
            Self::InvalidScalarType(data_type) => {
                write!(f, "{data_type} is invalid as a DataFusion scalar type")
            }
            Self::InvalidTimeUnit(time_unit) => {
                write!(
                    f,
                    "Only TimeUnit::Microsecond and TimeUnit::Nanosecond are valid time units, found: {time_unit:?}"
                )
            }
            Self::NotImplemented(s) => {
                write!(f, "Not implemented: {s}")
            }
        }
    }
}

impl From<Error> for DataFusionError {
    fn from(e: Error) -> Self {
        _plan_datafusion_err!("{}", e)
    }
}

impl From<datafusion_proto_common::to_proto::Error> for Error {
    fn from(e: datafusion_proto_common::to_proto::Error) -> Self {
        match e {
            datafusion_proto_common::to_proto::Error::General(s) => Error::General(s),
            datafusion_proto_common::to_proto::Error::InvalidTimeUnit(t) => {
                Error::InvalidTimeUnit(t)
            }
            datafusion_proto_common::to_proto::Error::NotImplemented(s) => {
                Error::NotImplemented(s)
            }
        }
    }
}

impl From<Column> for protobuf::Column {
    fn from(c: Column) -> Self {
        Self {
            relation: c.relation.map(|relation| protobuf::ColumnRelation {
                relation: relation.to_string(),
            }),
            name: c.name,
        }
    }
}

impl From<&Column> for protobuf::Column {
    fn from(c: &Column) -> Self {
        c.clone().into()
    }
}

impl TryFrom<&DFSchema> for protobuf::DfSchema {
    type Error = Error;

    fn try_from(s: &DFSchema) -> Result<Self, Self::Error> {
        let columns = s
            .iter()
            .map(|(qualifier, field)| {
                Ok(protobuf::DfField {
                    field: Some(field.as_ref().try_into()?),
                    qualifier: qualifier.map(|r| protobuf::ColumnRelation {
                        relation: r.to_string(),
                    }),
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(Self {
            columns,
            metadata: s.metadata().clone(),
        })
    }
}

impl TryFrom<&ScalarValue> for protobuf::ScalarValue {
    type Error = Error;

    fn try_from(val: &ScalarValue) -> Result<Self, Self::Error> {
        let data_type = val.data_type();
        match val {
            ScalarValue::Boolean(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| Value::BoolValue(*s))
            }
            ScalarValue::Float16(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::Float32Value((*s).into())
                })
            }
            ScalarValue::Float32(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| Value::Float32Value(*s))
            }
            ScalarValue::Float64(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| Value::Float64Value(*s))
            }
            ScalarValue::Int8(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::Int8Value(*s as i32)
                })
            }
            ScalarValue::Int16(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::Int16Value(*s as i32)
                })
            }
            ScalarValue::Int32(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| Value::Int32Value(*s))
            }
            ScalarValue::Int64(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| Value::Int64Value(*s))
            }
            ScalarValue::UInt8(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::Uint8Value(*s as u32)
                })
            }
            ScalarValue::UInt16(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::Uint16Value(*s as u32)
                })
            }
            ScalarValue::UInt32(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| Value::Uint32Value(*s))
            }
            ScalarValue::UInt64(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| Value::Uint64Value(*s))
            }
            ScalarValue::Utf8(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::Utf8Value(s.to_owned())
                })
            }
            ScalarValue::LargeUtf8(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::LargeUtf8Value(s.to_owned())
                })
            }
            ScalarValue::Utf8View(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::Utf8ViewValue(s.to_owned())
                })
            }
            ScalarValue::List(arr) => {
                encode_scalar_nested_value(arr.to_owned() as ArrayRef, val)
            }
            ScalarValue::LargeList(arr) => {
                encode_scalar_nested_value(arr.to_owned() as ArrayRef, val)
            }
            ScalarValue::FixedSizeList(arr) => {
                encode_scalar_nested_value(arr.to_owned() as ArrayRef, val)
            }
            ScalarValue::ListView(arr) => {
                encode_scalar_nested_value(arr.to_owned() as ArrayRef, val)
            }
            ScalarValue::LargeListView(arr) => {
                encode_scalar_nested_value(arr.to_owned() as ArrayRef, val)
            }
            ScalarValue::Struct(arr) => {
                encode_scalar_nested_value(arr.to_owned() as ArrayRef, val)
            }
            ScalarValue::Map(arr) => {
                encode_scalar_nested_value(arr.to_owned() as ArrayRef, val)
            }
            ScalarValue::Date32(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| Value::Date32Value(*s))
            }
            ScalarValue::TimestampMicrosecond(val, tz) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::TimestampValue(protobuf::ScalarTimestampValue {
                        timezone: tz.as_deref().unwrap_or("").to_string(),
                        value: Some(
                            protobuf::scalar_timestamp_value::Value::TimeMicrosecondValue(
                                *s,
                            ),
                        ),
                    })
                })
            }
            ScalarValue::TimestampNanosecond(val, tz) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::TimestampValue(protobuf::ScalarTimestampValue {
                        timezone: tz.as_deref().unwrap_or("").to_string(),
                        value: Some(
                            protobuf::scalar_timestamp_value::Value::TimeNanosecondValue(
                                *s,
                            ),
                        ),
                    })
                })
            }
            ScalarValue::Decimal32(val, p, s) => match *val {
                Some(v) => {
                    let array = v.to_be_bytes();
                    let vec_val: Vec<u8> = array.to_vec();
                    Ok(protobuf::ScalarValue {
                        value: Some(Value::Decimal32Value(protobuf::Decimal32 {
                            value: vec_val,
                            p: *p as i64,
                            s: *s as i64,
                        })),
                    })
                }
                None => Ok(protobuf::ScalarValue {
                    value: Some(Value::NullValue((&data_type).try_into()?)),
                }),
            },
            ScalarValue::Decimal64(val, p, s) => match *val {
                Some(v) => {
                    let array = v.to_be_bytes();
                    let vec_val: Vec<u8> = array.to_vec();
                    Ok(protobuf::ScalarValue {
                        value: Some(Value::Decimal64Value(protobuf::Decimal64 {
                            value: vec_val,
                            p: *p as i64,
                            s: *s as i64,
                        })),
                    })
                }
                None => Ok(protobuf::ScalarValue {
                    value: Some(Value::NullValue((&data_type).try_into()?)),
                }),
            },
            ScalarValue::Decimal128(val, p, s) => match *val {
                Some(v) => {
                    let array = v.to_be_bytes();
                    let vec_val: Vec<u8> = array.to_vec();
                    Ok(protobuf::ScalarValue {
                        value: Some(Value::Decimal128Value(protobuf::Decimal128 {
                            value: vec_val,
                            p: *p as i64,
                            s: *s as i64,
                        })),
                    })
                }
                None => Ok(protobuf::ScalarValue {
                    value: Some(Value::NullValue((&data_type).try_into()?)),
                }),
            },
            ScalarValue::Decimal256(val, p, s) => match *val {
                Some(v) => {
                    let array = v.to_be_bytes();
                    let vec_val: Vec<u8> = array.to_vec();
                    Ok(protobuf::ScalarValue {
                        value: Some(Value::Decimal256Value(protobuf::Decimal256 {
                            value: vec_val,
                            p: *p as i64,
                            s: *s as i64,
                        })),
                    })
                }
                None => Ok(protobuf::ScalarValue {
                    value: Some(Value::NullValue((&data_type).try_into()?)),
                }),
            },
            ScalarValue::Date64(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| Value::Date64Value(*s))
            }
            ScalarValue::TimestampSecond(val, tz) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::TimestampValue(protobuf::ScalarTimestampValue {
                        timezone: tz.as_deref().unwrap_or("").to_string(),
                        value: Some(
                            protobuf::scalar_timestamp_value::Value::TimeSecondValue(*s),
                        ),
                    })
                })
            }
            ScalarValue::TimestampMillisecond(val, tz) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::TimestampValue(protobuf::ScalarTimestampValue {
                        timezone: tz.as_deref().unwrap_or("").to_string(),
                        value: Some(
                            protobuf::scalar_timestamp_value::Value::TimeMillisecondValue(
                                *s,
                            ),
                        ),
                    })
                })
            }
            ScalarValue::IntervalYearMonth(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::IntervalYearmonthValue(*s)
                })
            }
            ScalarValue::Null => Ok(protobuf::ScalarValue {
                value: Some(Value::NullValue((&data_type).try_into()?)),
            }),

            ScalarValue::Binary(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::BinaryValue(s.to_owned())
                })
            }
            ScalarValue::BinaryView(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::BinaryViewValue(s.to_owned())
                })
            }
            ScalarValue::LargeBinary(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::LargeBinaryValue(s.to_owned())
                })
            }
            ScalarValue::FixedSizeBinary(length, val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::FixedSizeBinaryValue(protobuf::ScalarFixedSizeBinary {
                        values: s.to_owned(),
                        length: *length,
                    })
                })
            }

            ScalarValue::Time32Second(v) => {
                create_proto_scalar(v.as_ref(), &data_type, |v| {
                    Value::Time32Value(protobuf::ScalarTime32Value {
                        value: Some(
                            protobuf::scalar_time32_value::Value::Time32SecondValue(*v),
                        ),
                    })
                })
            }

            ScalarValue::Time32Millisecond(v) => {
                create_proto_scalar(v.as_ref(), &data_type, |v| {
                    Value::Time32Value(protobuf::ScalarTime32Value {
                        value: Some(
                            protobuf::scalar_time32_value::Value::Time32MillisecondValue(
                                *v,
                            ),
                        ),
                    })
                })
            }

            ScalarValue::Time64Microsecond(v) => {
                create_proto_scalar(v.as_ref(), &data_type, |v| {
                    Value::Time64Value(protobuf::ScalarTime64Value {
                        value: Some(
                            protobuf::scalar_time64_value::Value::Time64MicrosecondValue(
                                *v,
                            ),
                        ),
                    })
                })
            }

            ScalarValue::Time64Nanosecond(v) => {
                create_proto_scalar(v.as_ref(), &data_type, |v| {
                    Value::Time64Value(protobuf::ScalarTime64Value {
                        value: Some(
                            protobuf::scalar_time64_value::Value::Time64NanosecondValue(
                                *v,
                            ),
                        ),
                    })
                })
            }

            ScalarValue::IntervalDayTime(val) => {
                let value = if let Some(v) = val {
                    let (days, milliseconds) = IntervalDayTimeType::to_parts(*v);
                    Value::IntervalDaytimeValue(protobuf::IntervalDayTimeValue {
                        days,
                        milliseconds,
                    })
                } else {
                    Value::NullValue((&data_type).try_into()?)
                };

                Ok(protobuf::ScalarValue { value: Some(value) })
            }

            ScalarValue::IntervalMonthDayNano(v) => {
                let value = if let Some(v) = v {
                    let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(*v);
                    Value::IntervalMonthDayNano(protobuf::IntervalMonthDayNanoValue {
                        months,
                        days,
                        nanos,
                    })
                } else {
                    Value::NullValue((&data_type).try_into()?)
                };

                Ok(protobuf::ScalarValue { value: Some(value) })
            }

            ScalarValue::DurationSecond(v) => {
                let value = match v {
                    Some(v) => Value::DurationSecondValue(*v),
                    None => Value::NullValue((&data_type).try_into()?),
                };
                Ok(protobuf::ScalarValue { value: Some(value) })
            }
            ScalarValue::DurationMillisecond(v) => {
                let value = match v {
                    Some(v) => Value::DurationMillisecondValue(*v),
                    None => Value::NullValue((&data_type).try_into()?),
                };
                Ok(protobuf::ScalarValue { value: Some(value) })
            }
            ScalarValue::DurationMicrosecond(v) => {
                let value = match v {
                    Some(v) => Value::DurationMicrosecondValue(*v),
                    None => Value::NullValue((&data_type).try_into()?),
                };
                Ok(protobuf::ScalarValue { value: Some(value) })
            }
            ScalarValue::DurationNanosecond(v) => {
                let value = match v {
                    Some(v) => Value::DurationNanosecondValue(*v),
                    None => Value::NullValue((&data_type).try_into()?),
                };
                Ok(protobuf::ScalarValue { value: Some(value) })
            }

            ScalarValue::Union(val, df_fields, mode) => {
                let mut fields =
                    Vec::<protobuf::UnionField>::with_capacity(df_fields.len());
                for (id, field) in df_fields.iter() {
                    let field_id = id as i32;
                    let field = Some(field.as_ref().try_into()?);
                    let field = protobuf::UnionField { field_id, field };
                    fields.push(field);
                }
                let mode = match mode {
                    UnionMode::Sparse => 0,
                    UnionMode::Dense => 1,
                };
                let value = match val {
                    None => None,
                    Some((_id, v)) => Some(Box::new(v.as_ref().try_into()?)),
                };
                let val = protobuf::UnionValue {
                    value_id: val.as_ref().map(|(id, _v)| *id as i32).unwrap_or(0),
                    value,
                    fields,
                    mode,
                };
                let val = Value::UnionValue(Box::new(val));
                let val = protobuf::ScalarValue { value: Some(val) };
                Ok(val)
            }

            ScalarValue::Dictionary(index_type, val) => {
                let value: protobuf::ScalarValue = val.as_ref().try_into()?;
                Ok(protobuf::ScalarValue {
                    value: Some(Value::DictionaryValue(Box::new(
                        protobuf::ScalarDictionaryValue {
                            index_type: Some(index_type.as_ref().try_into()?),
                            value: Some(Box::new(value)),
                        },
                    ))),
                })
            }

            ScalarValue::RunEndEncoded(run_ends_field, values_field, val) => {
                Ok(protobuf::ScalarValue {
                    value: Some(Value::RunEndEncodedValue(Box::new(
                        protobuf::ScalarRunEndEncodedValue {
                            run_ends_field: Some(run_ends_field.as_ref().try_into()?),
                            values_field: Some(values_field.as_ref().try_into()?),
                            value: Some(Box::new(val.as_ref().try_into()?)),
                        },
                    ))),
                })
            }
        }
    }
}

impl From<Constraints> for protobuf::Constraints {
    fn from(value: Constraints) -> Self {
        let constraints = value.into_iter().map(|item| item.into()).collect();
        protobuf::Constraints { constraints }
    }
}

impl From<Constraint> for protobuf::Constraint {
    fn from(value: Constraint) -> Self {
        let res = match value {
            Constraint::PrimaryKey(indices) => {
                let indices = indices.into_iter().map(|item| item as u64).collect();
                protobuf::constraint::ConstraintMode::PrimaryKey(
                    protobuf::PrimaryKeyConstraint { indices },
                )
            }
            Constraint::Unique(indices) => {
                let indices = indices.into_iter().map(|item| item as u64).collect();
                protobuf::constraint::ConstraintMode::PrimaryKey(
                    protobuf::PrimaryKeyConstraint { indices },
                )
            }
        };
        protobuf::Constraint {
            constraint_mode: Some(res),
        }
    }
}

impl From<&Precision<usize>> for protobuf::Precision {
    fn from(s: &Precision<usize>) -> protobuf::Precision {
        match s {
            Precision::Exact(val) => protobuf::Precision {
                precision_info: protobuf::PrecisionInfo::Exact.into(),
                val: Some(datafusion_proto_common::protobuf_common::ScalarValue {
                    value: Some(Value::Uint64Value(*val as u64)),
                }),
            },
            Precision::Inexact(val) => protobuf::Precision {
                precision_info: protobuf::PrecisionInfo::Inexact.into(),
                val: Some(datafusion_proto_common::protobuf_common::ScalarValue {
                    value: Some(Value::Uint64Value(*val as u64)),
                }),
            },
            Precision::Absent => protobuf::Precision {
                precision_info: protobuf::PrecisionInfo::Absent.into(),
                val: Some(datafusion_proto_common::protobuf_common::ScalarValue {
                    value: None,
                }),
            },
        }
    }
}

impl From<&Precision<ScalarValue>> for protobuf::Precision {
    fn from(s: &Precision<ScalarValue>) -> protobuf::Precision {
        match s {
            Precision::Exact(val) => protobuf::Precision {
                precision_info: protobuf::PrecisionInfo::Exact.into(),
                val: val.try_into().ok(),
            },
            Precision::Inexact(val) => protobuf::Precision {
                precision_info: protobuf::PrecisionInfo::Inexact.into(),
                val: val.try_into().ok(),
            },
            Precision::Absent => protobuf::Precision {
                precision_info: protobuf::PrecisionInfo::Absent.into(),
                val: Some(datafusion_proto_common::protobuf_common::ScalarValue {
                    value: None,
                }),
            },
        }
    }
}

impl From<&Statistics> for protobuf::Statistics {
    fn from(s: &Statistics) -> protobuf::Statistics {
        let column_stats = s.column_statistics.iter().map(|s| s.into()).collect();
        protobuf::Statistics {
            num_rows: Some(protobuf::Precision::from(&s.num_rows)),
            total_byte_size: Some(protobuf::Precision::from(&s.total_byte_size)),
            column_stats,
        }
    }
}

impl From<&ColumnStatistics> for protobuf::ColumnStats {
    fn from(s: &ColumnStatistics) -> protobuf::ColumnStats {
        protobuf::ColumnStats {
            min_value: Some(protobuf::Precision::from(&s.min_value)),
            max_value: Some(protobuf::Precision::from(&s.max_value)),
            sum_value: Some(protobuf::Precision::from(&s.sum_value)),
            null_count: Some(protobuf::Precision::from(&s.null_count)),
            distinct_count: Some(protobuf::Precision::from(&s.distinct_count)),
            byte_size: Some(protobuf::Precision::from(&s.byte_size)),
        }
    }
}

impl From<JoinSide> for protobuf::JoinSide {
    fn from(t: JoinSide) -> Self {
        match t {
            JoinSide::Left => protobuf::JoinSide::LeftSide,
            JoinSide::Right => protobuf::JoinSide::RightSide,
            JoinSide::None => protobuf::JoinSide::None,
        }
    }
}

impl From<&CompressionTypeVariant> for protobuf::CompressionTypeVariant {
    fn from(value: &CompressionTypeVariant) -> Self {
        match value {
            CompressionTypeVariant::GZIP => Self::Gzip,
            CompressionTypeVariant::BZIP2 => Self::Bzip2,
            CompressionTypeVariant::XZ => Self::Xz,
            CompressionTypeVariant::ZSTD => Self::Zstd,
            CompressionTypeVariant::UNCOMPRESSED => Self::Uncompressed,
        }
    }
}

impl From<CsvQuoteStyle> for protobuf::CsvQuoteStyle {
    fn from(value: CsvQuoteStyle) -> Self {
        match value {
            CsvQuoteStyle::Necessary => Self::Necessary,
            CsvQuoteStyle::Always => Self::Always,
            CsvQuoteStyle::NonNumeric => Self::NonNumeric,
            CsvQuoteStyle::Never => Self::Never,
        }
    }
}

impl TryFrom<&CsvWriterOptions> for protobuf::CsvWriterOptions {
    type Error = DataFusionError;

    fn try_from(opts: &CsvWriterOptions) -> crate::Result<Self, Self::Error> {
        Ok(csv_writer_options_to_proto(
            &opts.writer_options,
            &opts.compression,
        ))
    }
}

impl TryFrom<&JsonWriterOptions> for protobuf::JsonWriterOptions {
    type Error = DataFusionError;

    fn try_from(opts: &JsonWriterOptions) -> crate::Result<Self, Self::Error> {
        let compression: protobuf::CompressionTypeVariant = opts.compression.into();
        Ok(protobuf::JsonWriterOptions {
            compression: compression.into(),
        })
    }
}

impl TryFrom<&ParquetOptions> for protobuf::ParquetOptions {
    type Error = DataFusionError;

    fn try_from(value: &ParquetOptions) -> crate::Result<Self, Self::Error> {
        Ok(protobuf::ParquetOptions {
            enable_page_index: value.enable_page_index,
            pruning: value.pruning,
            skip_metadata: value.skip_metadata,
            metadata_size_hint_opt: value.metadata_size_hint.map(|v| protobuf::parquet_options::MetadataSizeHintOpt::MetadataSizeHint(v as u64)),
            pushdown_filters: value.pushdown_filters,
            reorder_filters: value.reorder_filters,
            force_filter_selections: value.force_filter_selections,
            data_pagesize_limit: value.data_pagesize_limit as u64,
            write_batch_size: value.write_batch_size as u64,
            writer_version: value.writer_version.to_string(),
            compression_opt: value.compression.clone().map(protobuf::parquet_options::CompressionOpt::Compression),
            dictionary_enabled_opt: value.dictionary_enabled.map(protobuf::parquet_options::DictionaryEnabledOpt::DictionaryEnabled),
            dictionary_page_size_limit: value.dictionary_page_size_limit as u64,
            statistics_enabled_opt: value.statistics_enabled.clone().map(protobuf::parquet_options::StatisticsEnabledOpt::StatisticsEnabled),
            max_row_group_size: value.max_row_group_size as u64,
            created_by: value.created_by.clone(),
            column_index_truncate_length_opt: value.column_index_truncate_length.map(|v| protobuf::parquet_options::ColumnIndexTruncateLengthOpt::ColumnIndexTruncateLength(v as u64)),
            statistics_truncate_length_opt: value.statistics_truncate_length.map(|v| protobuf::parquet_options::StatisticsTruncateLengthOpt::StatisticsTruncateLength(v as u64)),
            data_page_row_count_limit: value.data_page_row_count_limit as u64,
            encoding_opt: value.encoding.clone().map(protobuf::parquet_options::EncodingOpt::Encoding),
            bloom_filter_on_read: value.bloom_filter_on_read,
            bloom_filter_on_write: value.bloom_filter_on_write,
            bloom_filter_fpp_opt: value.bloom_filter_fpp.map(protobuf::parquet_options::BloomFilterFppOpt::BloomFilterFpp),
            bloom_filter_ndv_opt: value.bloom_filter_ndv.map(protobuf::parquet_options::BloomFilterNdvOpt::BloomFilterNdv),
            allow_single_file_parallelism: value.allow_single_file_parallelism,
            maximum_parallel_row_group_writers: value.maximum_parallel_row_group_writers as u64,
            maximum_buffered_record_batches_per_stream: value.maximum_buffered_record_batches_per_stream as u64,
            schema_force_view_types: value.schema_force_view_types,
            binary_as_string: value.binary_as_string,
            skip_arrow_metadata: value.skip_arrow_metadata,
            coerce_int96_opt: value.coerce_int96.clone().map(protobuf::parquet_options::CoerceInt96Opt::CoerceInt96),
            max_predicate_cache_size_opt: value.max_predicate_cache_size.map(|v| protobuf::parquet_options::MaxPredicateCacheSizeOpt::MaxPredicateCacheSize(v as u64)),
            content_defined_chunking: value.use_content_defined_chunking.as_ref().map(|cdc|
                protobuf::CdcOptions {
                    min_chunk_size: cdc.min_chunk_size as u64,
                    max_chunk_size: cdc.max_chunk_size as u64,
                    norm_level: cdc.norm_level,
                }
            ),
        })
    }
}

impl TryFrom<&ParquetColumnOptions> for protobuf::ParquetColumnOptions {
    type Error = DataFusionError;

    fn try_from(value: &ParquetColumnOptions) -> crate::Result<Self, Self::Error> {
        Ok(protobuf::ParquetColumnOptions {
            compression_opt: value
                .compression
                .clone()
                .map(protobuf::parquet_column_options::CompressionOpt::Compression),
            dictionary_enabled_opt: value
                .dictionary_enabled
                .map(protobuf::parquet_column_options::DictionaryEnabledOpt::DictionaryEnabled),
            statistics_enabled_opt: value
                .statistics_enabled
                .clone()
                .map(protobuf::parquet_column_options::StatisticsEnabledOpt::StatisticsEnabled),
            encoding_opt: value
                .encoding
                .clone()
                .map(protobuf::parquet_column_options::EncodingOpt::Encoding),
            bloom_filter_enabled_opt: value
                .bloom_filter_enabled
                .map(protobuf::parquet_column_options::BloomFilterEnabledOpt::BloomFilterEnabled),
            bloom_filter_fpp_opt: value
                .bloom_filter_fpp
                .map(protobuf::parquet_column_options::BloomFilterFppOpt::BloomFilterFpp),
            bloom_filter_ndv_opt: value
                .bloom_filter_ndv
                .map(protobuf::parquet_column_options::BloomFilterNdvOpt::BloomFilterNdv),
        })
    }
}

impl TryFrom<&TableParquetOptions> for protobuf::TableParquetOptions {
    type Error = DataFusionError;
    fn try_from(value: &TableParquetOptions) -> crate::Result<Self, Self::Error> {
        let column_specific_options = value
            .column_specific_options
            .iter()
            .map(|(k, v)| {
                Ok(protobuf::ParquetColumnSpecificOptions {
                    column_name: k.into(),
                    options: Some(v.try_into()?),
                })
            })
            .collect::<crate::Result<Vec<_>>>()?;
        let key_value_metadata = value
            .key_value_metadata
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|v| (k.clone(), v.clone())))
            .collect::<HashMap<String, String>>();

        let global: protobuf::ParquetOptions = (&value.global).try_into()?;

        Ok(protobuf::TableParquetOptions {
            global: Some(global),
            column_specific_options,
            key_value_metadata,
        })
    }
}

impl TryFrom<&CsvOptions> for protobuf::CsvOptions {
    type Error = DataFusionError; // Define or use an appropriate error type

    fn try_from(opts: &CsvOptions) -> crate::Result<Self, Self::Error> {
        let compression: protobuf::CompressionTypeVariant = opts.compression.into();
        let quote_style: protobuf::CsvQuoteStyle = opts.quote_style.into();
        Ok(protobuf::CsvOptions {
            has_header: opts.has_header.map_or_else(Vec::new, |h| vec![h as u8]),
            delimiter: vec![opts.delimiter],
            quote: vec![opts.quote],
            terminator: opts.terminator.map_or_else(Vec::new, |e| vec![e]),
            escape: opts.escape.map_or_else(Vec::new, |e| vec![e]),
            double_quote: opts.double_quote.map_or_else(Vec::new, |h| vec![h as u8]),
            newlines_in_values: opts
                .newlines_in_values
                .map_or_else(Vec::new, |h| vec![h as u8]),
            compression: compression.into(),
            schema_infer_max_rec: opts.schema_infer_max_rec.map(|h| h as u64),
            date_format: opts.date_format.clone().unwrap_or_default(),
            datetime_format: opts.datetime_format.clone().unwrap_or_default(),
            timestamp_format: opts.timestamp_format.clone().unwrap_or_default(),
            timestamp_tz_format: opts.timestamp_tz_format.clone().unwrap_or_default(),
            time_format: opts.time_format.clone().unwrap_or_default(),
            null_value: opts.null_value.clone().unwrap_or_default(),
            null_regex: opts.null_regex.clone().unwrap_or_default(),
            comment: opts.comment.map_or_else(Vec::new, |h| vec![h]),
            truncated_rows: opts.truncated_rows.map_or_else(Vec::new, |h| vec![h as u8]),
            compression_level: opts.compression_level,
            quote_style: quote_style.into(),
            ignore_leading_whitespace: opts
                .ignore_leading_whitespace
                .map_or_else(Vec::new, |h| vec![h as u8]),
            ignore_trailing_whitespace: opts
                .ignore_trailing_whitespace
                .map_or_else(Vec::new, |h| vec![h as u8]),
        })
    }
}

impl TryFrom<&JsonOptions> for protobuf::JsonOptions {
    type Error = DataFusionError;

    fn try_from(opts: &JsonOptions) -> crate::Result<Self, Self::Error> {
        let compression: protobuf::CompressionTypeVariant = opts.compression.into();
        Ok(protobuf::JsonOptions {
            compression: compression.into(),
            schema_infer_max_rec: opts.schema_infer_max_rec.map(|h| h as u64),
            compression_level: opts.compression_level,
            newline_delimited: Some(opts.newline_delimited),
        })
    }
}

// Conversion impls for `package datafusion_common` enum types whose source
// types live in `datafusion-common`.

impl From<crate::JoinType> for protobuf::JoinType {
    fn from(t: crate::JoinType) -> Self {
        use crate::JoinType;
        match t {
            JoinType::Inner => Self::Inner,
            JoinType::Left => Self::Left,
            JoinType::Right => Self::Right,
            JoinType::Full => Self::Full,
            JoinType::LeftSemi => Self::Leftsemi,
            JoinType::RightSemi => Self::Rightsemi,
            JoinType::LeftAnti => Self::Leftanti,
            JoinType::RightAnti => Self::Rightanti,
            JoinType::LeftMark => Self::Leftmark,
            JoinType::RightMark => Self::Rightmark,
        }
    }
}

impl From<crate::JoinConstraint> for protobuf::JoinConstraint {
    fn from(t: crate::JoinConstraint) -> Self {
        use crate::JoinConstraint;
        match t {
            JoinConstraint::On => Self::On,
            JoinConstraint::Using => Self::Using,
        }
    }
}

impl From<crate::NullEquality> for protobuf::NullEquality {
    fn from(t: crate::NullEquality) -> Self {
        use crate::NullEquality;
        match t {
            NullEquality::NullEqualsNothing => Self::NullEqualsNothing,
            NullEquality::NullEqualsNull => Self::NullEqualsNull,
        }
    }
}

// Conversion impls for `package datafusion` types.

impl From<&crate::UnnestOptions>
    for datafusion_proto_common::generated::datafusion::UnnestOptions
{
    fn from(opts: &crate::UnnestOptions) -> Self {
        Self {
            preserve_nulls: opts.preserve_nulls,
            recursions: opts
                .recursions
                .iter()
                .map(|r| datafusion_proto_common::generated::datafusion::RecursionUnnestOption {
                    input_column: Some((&r.input_column).into()),
                    output_column: Some((&r.output_column).into()),
                    depth: r.depth as u32,
                })
                .collect(),
        }
    }
}

impl From<crate::TableReference>
    for datafusion_proto_common::generated::datafusion::TableReference
{
    fn from(t: crate::TableReference) -> Self {
        use datafusion_proto_common::generated::datafusion::table_reference::TableReferenceEnum;
        use datafusion_proto_common::generated::datafusion::{
            BareTableReference, FullTableReference, PartialTableReference,
            TableReference as ProtoTableReference,
        };
        let table_reference_enum = match t {
            crate::TableReference::Bare { table } => {
                TableReferenceEnum::Bare(BareTableReference {
                    table: table.to_string(),
                })
            }
            crate::TableReference::Partial { schema, table } => {
                TableReferenceEnum::Partial(PartialTableReference {
                    schema: schema.to_string(),
                    table: table.to_string(),
                })
            }
            crate::TableReference::Full {
                catalog,
                schema,
                table,
            } => TableReferenceEnum::Full(FullTableReference {
                catalog: catalog.to_string(),
                schema: schema.to_string(),
                table: table.to_string(),
            }),
        };
        ProtoTableReference {
            table_reference_enum: Some(table_reference_enum),
        }
    }
}

impl From<&crate::display::StringifiedPlan>
    for datafusion_proto_common::generated::datafusion::StringifiedPlan
{
    fn from(stringified_plan: &crate::display::StringifiedPlan) -> Self {
        use crate::display::PlanType as DfPlanType;
        use datafusion_proto_common::EmptyMessage;
        use datafusion_proto_common::generated::datafusion::plan_type::PlanTypeEnum;
        use datafusion_proto_common::generated::datafusion::{
            AnalyzedLogicalPlanType, OptimizedLogicalPlanType, OptimizedPhysicalPlanType,
            PlanType,
        };
        Self {
            plan_type: Some(PlanType {
                plan_type_enum: Some(match &stringified_plan.plan_type {
                    DfPlanType::InitialLogicalPlan => {
                        PlanTypeEnum::InitialLogicalPlan(EmptyMessage {})
                    }
                    DfPlanType::AnalyzedLogicalPlan { analyzer_name } => {
                        PlanTypeEnum::AnalyzedLogicalPlan(AnalyzedLogicalPlanType {
                            analyzer_name: analyzer_name.clone(),
                        })
                    }
                    DfPlanType::FinalAnalyzedLogicalPlan => {
                        PlanTypeEnum::FinalAnalyzedLogicalPlan(EmptyMessage {})
                    }
                    DfPlanType::OptimizedLogicalPlan { optimizer_name } => {
                        PlanTypeEnum::OptimizedLogicalPlan(OptimizedLogicalPlanType {
                            optimizer_name: optimizer_name.clone(),
                        })
                    }
                    DfPlanType::FinalLogicalPlan => {
                        PlanTypeEnum::FinalLogicalPlan(EmptyMessage {})
                    }
                    DfPlanType::InitialPhysicalPlan => {
                        PlanTypeEnum::InitialPhysicalPlan(EmptyMessage {})
                    }
                    DfPlanType::InitialPhysicalPlanWithStats => {
                        PlanTypeEnum::InitialPhysicalPlanWithStats(EmptyMessage {})
                    }
                    DfPlanType::InitialPhysicalPlanWithSchema => {
                        PlanTypeEnum::InitialPhysicalPlanWithSchema(EmptyMessage {})
                    }
                    DfPlanType::OptimizedPhysicalPlan { optimizer_name } => {
                        PlanTypeEnum::OptimizedPhysicalPlan(OptimizedPhysicalPlanType {
                            optimizer_name: optimizer_name.clone(),
                        })
                    }
                    DfPlanType::FinalPhysicalPlan => {
                        PlanTypeEnum::FinalPhysicalPlan(EmptyMessage {})
                    }
                    DfPlanType::FinalPhysicalPlanWithStats => {
                        PlanTypeEnum::FinalPhysicalPlanWithStats(EmptyMessage {})
                    }
                    DfPlanType::FinalPhysicalPlanWithSchema => {
                        PlanTypeEnum::FinalPhysicalPlanWithSchema(EmptyMessage {})
                    }
                    DfPlanType::PhysicalPlanError => {
                        PlanTypeEnum::PhysicalPlanError(EmptyMessage {})
                    }
                }),
            }),
            plan: stringified_plan.plan.to_string(),
        }
    }
}

/// Creates a scalar protobuf value from an optional value (T), and
/// encoding None as the appropriate datatype
fn create_proto_scalar<I, T: FnOnce(&I) -> Value>(
    v: Option<&I>,
    null_arrow_type: &DataType,
    constructor: T,
) -> Result<protobuf::ScalarValue, Error> {
    let value = v
        .map(constructor)
        .unwrap_or(Value::NullValue(null_arrow_type.try_into()?));

    Ok(protobuf::ScalarValue { value: Some(value) })
}

// Nested ScalarValue types (List / FixedSizeList / LargeList / ListView / LargeListView / Struct / Map)
// are serialized using Arrow IPC messages as a single column RecordBatch
fn encode_scalar_nested_value(
    arr: ArrayRef,
    val: &ScalarValue,
) -> Result<protobuf::ScalarValue, Error> {
    let batch = RecordBatch::try_from_iter(vec![("field_name", arr)]).map_err(|e| {
        Error::General(format!(
            "Error creating temporary batch while encoding nested ScalarValue: {e}"
        ))
    })?;

    let ipc_gen = IpcDataGenerator {};
    let mut dict_tracker = DictionaryTracker::new(false);
    let write_options = IpcWriteOptions::default();
    // The IPC writer requires pre-allocated dictionary IDs (normally assigned when
    // serializing the schema). Populate `dict_tracker` by encoding the schema first.
    ipc_gen.schema_to_bytes_with_dictionary_tracker(
        batch.schema().as_ref(),
        &mut dict_tracker,
        &write_options,
    );
    let mut compression_context = CompressionContext::default();
    let (encoded_dictionaries, encoded_message) = ipc_gen
        .encode(
            &batch,
            &mut dict_tracker,
            &write_options,
            &mut compression_context,
        )
        .map_err(|e| {
            Error::General(format!("Error encoding nested ScalarValue as IPC: {e}"))
        })?;

    let schema: protobuf::Schema = batch.schema().try_into()?;

    let scalar_list_value = protobuf::ScalarNestedValue {
        ipc_message: encoded_message.ipc_message,
        arrow_data: encoded_message.arrow_data,
        dictionaries: encoded_dictionaries
            .into_iter()
            .map(|data| protobuf::scalar_nested_value::Dictionary {
                ipc_message: data.ipc_message,
                arrow_data: data.arrow_data,
            })
            .collect(),
        schema: Some(schema),
    };

    match val {
        ScalarValue::List(_) => Ok(protobuf::ScalarValue {
            value: Some(Value::ListValue(scalar_list_value)),
        }),
        ScalarValue::LargeList(_) => Ok(protobuf::ScalarValue {
            value: Some(Value::LargeListValue(scalar_list_value)),
        }),
        ScalarValue::FixedSizeList(_) => Ok(protobuf::ScalarValue {
            value: Some(Value::FixedSizeListValue(scalar_list_value)),
        }),
        ScalarValue::ListView(_) => Ok(protobuf::ScalarValue {
            value: Some(Value::ListViewValue(scalar_list_value)),
        }),
        ScalarValue::LargeListView(_) => Ok(protobuf::ScalarValue {
            value: Some(Value::LargeListViewValue(scalar_list_value)),
        }),
        ScalarValue::Struct(_) => Ok(protobuf::ScalarValue {
            value: Some(Value::StructValue(scalar_list_value)),
        }),
        ScalarValue::Map(_) => Ok(protobuf::ScalarValue {
            value: Some(Value::MapValue(scalar_list_value)),
        }),
        _ => unreachable!(),
    }
}

pub(crate) fn csv_writer_options_to_proto(
    csv_options: &WriterBuilder,
    compression: &CompressionTypeVariant,
) -> protobuf::CsvWriterOptions {
    let compression: protobuf::CompressionTypeVariant = compression.into();
    let quote_style: protobuf::CsvQuoteStyle = csv_options.quote_style().into();
    protobuf::CsvWriterOptions {
        compression: compression.into(),
        delimiter: (csv_options.delimiter() as char).to_string(),
        has_header: csv_options.header(),
        date_format: csv_options.date_format().unwrap_or("").to_owned(),
        datetime_format: csv_options.datetime_format().unwrap_or("").to_owned(),
        timestamp_format: csv_options.timestamp_format().unwrap_or("").to_owned(),
        time_format: csv_options.time_format().unwrap_or("").to_owned(),
        null_value: csv_options.null().to_owned(),
        quote: (csv_options.quote() as char).to_string(),
        escape: (csv_options.escape() as char).to_string(),
        double_quote: csv_options.double_quote(),
        quote_style: quote_style.into(),
        ignore_leading_whitespace: csv_options.ignore_leading_whitespace(),
        ignore_trailing_whitespace: csv_options.ignore_trailing_whitespace(),
    }
}
