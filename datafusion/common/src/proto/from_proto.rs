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
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray};
use arrow::buffer::Buffer;
use arrow::csv::{QuoteStyle, WriterBuilder};
use arrow::datatypes::{
    DataType, Field, IntervalDayTimeType, IntervalMonthDayNanoType, Schema, UnionFields,
    UnionMode, i256,
};
use arrow::ipc::{
    convert::fb_to_schema,
    reader::{read_dictionary, read_record_batch},
    root_as_message,
    writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions},
};
use datafusion_proto_common::from_proto::{
    Error, FromOptionalField, parse_proto_fields_to_fields,
};
use datafusion_proto_common::protobuf_common as protobuf;

use super::proto_error;
use crate::{
    Column, ColumnStatistics, Constraint, Constraints, DFSchema, DataFusionError,
    JoinSide, ScalarValue, Statistics, TableReference,
    config::{
        CdcOptions, CsvOptions, JsonOptions, ParquetColumnOptions, ParquetOptions,
        TableParquetOptions,
    },
    file_options::{csv_writer::CsvWriterOptions, json_writer::JsonWriterOptions},
    parsers::CompressionTypeVariant,
    stats::Precision,
};

impl From<protobuf::ColumnRelation> for TableReference {
    fn from(rel: protobuf::ColumnRelation) -> Self {
        Self::parse_str_normalized(rel.relation.as_str(), true)
    }
}

impl From<protobuf::Column> for Column {
    fn from(c: protobuf::Column) -> Self {
        let protobuf::Column { relation, name } = c;

        Self::new(relation, name)
    }
}

impl From<&protobuf::Column> for Column {
    fn from(c: &protobuf::Column) -> Self {
        c.clone().into()
    }
}

impl TryFrom<&protobuf::DfSchema> for DFSchema {
    type Error = Error;

    fn try_from(df_schema: &protobuf::DfSchema) -> crate::Result<Self, Self::Error> {
        let df_fields = df_schema.columns.clone();
        let qualifiers_and_fields: Vec<(Option<TableReference>, Arc<Field>)> = df_fields
            .iter()
            .map(|df_field| {
                let field: Field = df_field.field.as_ref().required("field")?;
                Ok((
                    df_field.qualifier.as_ref().map(|q| q.clone().into()),
                    Arc::new(field),
                ))
            })
            .collect::<crate::Result<Vec<_>, Error>>()?;

        DFSchema::new_with_metadata(qualifiers_and_fields, df_schema.metadata.clone())
            .map_err(|e: DataFusionError| Error::General(format!("{e}")))
    }
}

impl TryFrom<&protobuf::ScalarValue> for ScalarValue {
    type Error = Error;

    fn try_from(scalar: &protobuf::ScalarValue) -> crate::Result<Self, Self::Error> {
        use protobuf::scalar_value::Value;

        let value = scalar
            .value
            .as_ref()
            .ok_or_else(|| Error::required("value"))?;

        Ok(match value {
            Value::BoolValue(v) => Self::Boolean(Some(*v)),
            Value::Utf8Value(v) => Self::Utf8(Some(v.to_owned())),
            Value::Utf8ViewValue(v) => Self::Utf8View(Some(v.to_owned())),
            Value::LargeUtf8Value(v) => Self::LargeUtf8(Some(v.to_owned())),
            Value::Int8Value(v) => Self::Int8(Some(*v as i8)),
            Value::Int16Value(v) => Self::Int16(Some(*v as i16)),
            Value::Int32Value(v) => Self::Int32(Some(*v)),
            Value::Int64Value(v) => Self::Int64(Some(*v)),
            Value::Uint8Value(v) => Self::UInt8(Some(*v as u8)),
            Value::Uint16Value(v) => Self::UInt16(Some(*v as u16)),
            Value::Uint32Value(v) => Self::UInt32(Some(*v)),
            Value::Uint64Value(v) => Self::UInt64(Some(*v)),
            Value::Float32Value(v) => Self::Float32(Some(*v)),
            Value::Float64Value(v) => Self::Float64(Some(*v)),
            Value::Date32Value(v) => Self::Date32(Some(*v)),
            // Nested ScalarValue types are serialized using arrow IPC format
            Value::ListValue(v)
            | Value::FixedSizeListValue(v)
            | Value::LargeListValue(v)
            | Value::ListViewValue(v)
            | Value::LargeListViewValue(v)
            | Value::StructValue(v)
            | Value::MapValue(v) => {
                let protobuf::ScalarNestedValue {
                    ipc_message,
                    arrow_data,
                    dictionaries,
                    schema,
                } = &v;

                let schema: Schema = if let Some(schema_ref) = schema {
                    schema_ref.try_into()?
                } else {
                    return Err(Error::General(
                        "Invalid schema while deserializing nested ScalarValue"
                            .to_string(),
                    ));
                };

                // IPC dictionary batch IDs are assigned when encoding the schema, but our protobuf
                // `Schema` doesn't preserve those IDs. Reconstruct them deterministically by
                // round-tripping the schema through IPC.
                let schema: Schema = {
                    let ipc_gen = IpcDataGenerator {};
                    let write_options = IpcWriteOptions::default();
                    let mut dict_tracker = DictionaryTracker::new(false);
                    let encoded_schema = ipc_gen.schema_to_bytes_with_dictionary_tracker(
                        &schema,
                        &mut dict_tracker,
                        &write_options,
                    );
                    let message =
                        root_as_message(encoded_schema.ipc_message.as_slice()).map_err(
                            |e| {
                                Error::General(format!(
                                    "Error IPC schema message while deserializing nested ScalarValue: {e}"
                                ))
                            },
                        )?;
                    let ipc_schema = message.header_as_schema().ok_or_else(|| {
                        Error::General(
                            "Unexpected message type deserializing nested ScalarValue schema"
                                .to_string(),
                        )
                    })?;
                    fb_to_schema(ipc_schema)
                };

                let message = root_as_message(ipc_message.as_slice()).map_err(|e| {
                    Error::General(format!(
                        "Error IPC message while deserializing nested ScalarValue: {e}"
                    ))
                })?;
                let buffer = Buffer::from(arrow_data.as_slice());

                let ipc_batch = message.header_as_record_batch().ok_or_else(|| {
                    Error::General(
                        "Unexpected message type deserializing nested ScalarValue"
                            .to_string(),
                    )
                })?;

                let mut dict_by_id: HashMap<i64, ArrayRef> = HashMap::new();
                for protobuf::scalar_nested_value::Dictionary {
                    ipc_message,
                    arrow_data,
                } in dictionaries
                {
                    let message = root_as_message(ipc_message.as_slice()).map_err(|e| {
                        Error::General(format!(
                            "Error IPC message while deserializing nested ScalarValue dictionary message: {e}"
                        ))
                    })?;
                    let buffer = Buffer::from(arrow_data.as_slice());

                    let dict_batch = message.header_as_dictionary_batch().ok_or_else(|| {
                        Error::General(
                            "Unexpected message type deserializing nested ScalarValue dictionary message"
                                .to_string(),
                        )
                    })?;
                    read_dictionary(
                        &buffer,
                        dict_batch,
                        &schema,
                        &mut dict_by_id,
                        &message.version(),
                    )
                    .map_err(|e| {
                        Error::General(format!(
                            "Decoding nested ScalarValue dictionary: {e}"
                        ))
                    })?;
                }

                let record_batch = read_record_batch(
                    &buffer,
                    ipc_batch,
                    Arc::new(schema),
                    &dict_by_id,
                    None,
                    &message.version(),
                )
                .map_err(|e| {
                    Error::General(format!("Decoding nested ScalarValue value: {e}"))
                })?;
                let arr = record_batch.column(0);
                match value {
                    Value::ListValue(_) => {
                        Self::List(arr.as_list::<i32>().to_owned().into())
                    }
                    Value::LargeListValue(_) => {
                        Self::LargeList(arr.as_list::<i64>().to_owned().into())
                    }
                    Value::FixedSizeListValue(_) => {
                        Self::FixedSizeList(arr.as_fixed_size_list().to_owned().into())
                    }
                    Value::ListViewValue(_) => {
                        Self::ListView(arr.as_list_view::<i32>().to_owned().into())
                    }
                    Value::LargeListViewValue(_) => {
                        Self::LargeListView(arr.as_list_view::<i64>().to_owned().into())
                    }
                    Value::StructValue(_) => {
                        Self::Struct(arr.as_struct().to_owned().into())
                    }
                    Value::MapValue(_) => Self::Map(arr.as_map().to_owned().into()),
                    _ => unreachable!(),
                }
            }
            Value::NullValue(v) => {
                let null_type: DataType = v.try_into()?;
                null_type
                    .try_into()
                    .map_err(|e: DataFusionError| Error::General(format!("{e}")))?
            }
            Value::Decimal32Value(val) => {
                let array = vec_to_array(val.value.clone());
                Self::Decimal32(Some(i32::from_be_bytes(array)), val.p as u8, val.s as i8)
            }
            Value::Decimal64Value(val) => {
                let array = vec_to_array(val.value.clone());
                Self::Decimal64(Some(i64::from_be_bytes(array)), val.p as u8, val.s as i8)
            }
            Value::Decimal128Value(val) => {
                let array = vec_to_array(val.value.clone());
                Self::Decimal128(
                    Some(i128::from_be_bytes(array)),
                    val.p as u8,
                    val.s as i8,
                )
            }
            Value::Decimal256Value(val) => {
                let array = vec_to_array(val.value.clone());
                Self::Decimal256(
                    Some(i256::from_be_bytes(array)),
                    val.p as u8,
                    val.s as i8,
                )
            }
            Value::Date64Value(v) => Self::Date64(Some(*v)),
            Value::Time32Value(v) => {
                let time_value =
                    v.value.as_ref().ok_or_else(|| Error::required("value"))?;
                match time_value {
                    protobuf::scalar_time32_value::Value::Time32SecondValue(t) => {
                        Self::Time32Second(Some(*t))
                    }
                    protobuf::scalar_time32_value::Value::Time32MillisecondValue(t) => {
                        Self::Time32Millisecond(Some(*t))
                    }
                }
            }
            Value::Time64Value(v) => {
                let time_value =
                    v.value.as_ref().ok_or_else(|| Error::required("value"))?;
                match time_value {
                    protobuf::scalar_time64_value::Value::Time64MicrosecondValue(t) => {
                        Self::Time64Microsecond(Some(*t))
                    }
                    protobuf::scalar_time64_value::Value::Time64NanosecondValue(t) => {
                        Self::Time64Nanosecond(Some(*t))
                    }
                }
            }
            Value::IntervalYearmonthValue(v) => Self::IntervalYearMonth(Some(*v)),
            Value::DurationSecondValue(v) => Self::DurationSecond(Some(*v)),
            Value::DurationMillisecondValue(v) => Self::DurationMillisecond(Some(*v)),
            Value::DurationMicrosecondValue(v) => Self::DurationMicrosecond(Some(*v)),
            Value::DurationNanosecondValue(v) => Self::DurationNanosecond(Some(*v)),
            Value::TimestampValue(v) => {
                let timezone = if v.timezone.is_empty() {
                    None
                } else {
                    Some(v.timezone.as_str().into())
                };

                let ts_value =
                    v.value.as_ref().ok_or_else(|| Error::required("value"))?;

                match ts_value {
                    protobuf::scalar_timestamp_value::Value::TimeMicrosecondValue(t) => {
                        Self::TimestampMicrosecond(Some(*t), timezone)
                    }
                    protobuf::scalar_timestamp_value::Value::TimeNanosecondValue(t) => {
                        Self::TimestampNanosecond(Some(*t), timezone)
                    }
                    protobuf::scalar_timestamp_value::Value::TimeSecondValue(t) => {
                        Self::TimestampSecond(Some(*t), timezone)
                    }
                    protobuf::scalar_timestamp_value::Value::TimeMillisecondValue(t) => {
                        Self::TimestampMillisecond(Some(*t), timezone)
                    }
                }
            }
            Value::DictionaryValue(v) => {
                let index_type: DataType = v
                    .index_type
                    .as_ref()
                    .ok_or_else(|| Error::required("index_type"))?
                    .try_into()?;

                let value: Self = v
                    .value
                    .as_ref()
                    .ok_or_else(|| Error::required("value"))?
                    .as_ref()
                    .try_into()?;

                Self::Dictionary(Box::new(index_type), Box::new(value))
            }
            Value::RunEndEncodedValue(v) => {
                let run_ends_field: Field = v
                    .run_ends_field
                    .as_ref()
                    .ok_or_else(|| Error::required("run_ends_field"))?
                    .try_into()?;

                let values_field: Field = v
                    .values_field
                    .as_ref()
                    .ok_or_else(|| Error::required("values_field"))?
                    .try_into()?;

                let value: Self = v
                    .value
                    .as_ref()
                    .ok_or_else(|| Error::required("value"))?
                    .as_ref()
                    .try_into()?;

                Self::RunEndEncoded(
                    run_ends_field.into(),
                    values_field.into(),
                    Box::new(value),
                )
            }
            Value::BinaryValue(v) => Self::Binary(Some(v.clone())),
            Value::BinaryViewValue(v) => Self::BinaryView(Some(v.clone())),
            Value::LargeBinaryValue(v) => Self::LargeBinary(Some(v.clone())),
            Value::IntervalDaytimeValue(v) => Self::IntervalDayTime(Some(
                IntervalDayTimeType::make_value(v.days, v.milliseconds),
            )),
            Value::IntervalMonthDayNano(v) => Self::IntervalMonthDayNano(Some(
                IntervalMonthDayNanoType::make_value(v.months, v.days, v.nanos),
            )),
            Value::UnionValue(val) => {
                let mode = match val.mode {
                    0 => UnionMode::Sparse,
                    1 => UnionMode::Dense,
                    id => Err(Error::unknown("UnionMode", id))?,
                };
                let ids = val
                    .fields
                    .iter()
                    .map(|f| f.field_id as i8)
                    .collect::<Vec<_>>();
                let fields = val
                    .fields
                    .iter()
                    .map(|f| f.field.clone())
                    .collect::<Option<Vec<_>>>();
                let fields = fields.ok_or_else(|| Error::required("UnionField"))?;
                let fields = parse_proto_fields_to_fields(&fields)?;
                let union_fields = UnionFields::try_new(ids, fields).map_err(|e| {
                    Error::General(format!("Deserializing Union ScalarValue: {e}"))
                })?;
                let v_id = val.value_id as i8;
                let val = match &val.value {
                    None => None,
                    Some(val) => {
                        let val: ScalarValue = val
                            .as_ref()
                            .try_into()
                            .map_err(|_| Error::General("Invalid Scalar".to_string()))?;
                        Some((v_id, Box::new(val)))
                    }
                };
                Self::Union(val, union_fields, mode)
            }
            Value::FixedSizeBinaryValue(v) => {
                Self::FixedSizeBinary(v.length, Some(v.clone().values))
            }
        })
    }
}

impl From<protobuf::Constraints> for Constraints {
    fn from(constraints: protobuf::Constraints) -> Self {
        Constraints::new_unverified(
            constraints
                .constraints
                .into_iter()
                .map(|item| item.into())
                .collect(),
        )
    }
}

impl From<protobuf::Constraint> for Constraint {
    fn from(value: protobuf::Constraint) -> Self {
        match value.constraint_mode.unwrap() {
            protobuf::constraint::ConstraintMode::PrimaryKey(elem) => {
                Constraint::PrimaryKey(
                    elem.indices.into_iter().map(|item| item as usize).collect(),
                )
            }
            protobuf::constraint::ConstraintMode::Unique(elem) => Constraint::Unique(
                elem.indices.into_iter().map(|item| item as usize).collect(),
            ),
        }
    }
}

impl From<&protobuf::ColumnStats> for ColumnStatistics {
    fn from(cs: &protobuf::ColumnStats) -> ColumnStatistics {
        ColumnStatistics {
            null_count: if let Some(nc) = &cs.null_count {
                nc.clone().into()
            } else {
                Precision::Absent
            },
            max_value: if let Some(max) = &cs.max_value {
                max.clone().into()
            } else {
                Precision::Absent
            },
            min_value: if let Some(min) = &cs.min_value {
                min.clone().into()
            } else {
                Precision::Absent
            },
            sum_value: if let Some(sum) = &cs.sum_value {
                sum.clone().into()
            } else {
                Precision::Absent
            },
            distinct_count: if let Some(dc) = &cs.distinct_count {
                dc.clone().into()
            } else {
                Precision::Absent
            },
            byte_size: if let Some(sbs) = &cs.byte_size {
                sbs.clone().into()
            } else {
                Precision::Absent
            },
        }
    }
}

impl From<protobuf::Precision> for Precision<usize> {
    fn from(s: protobuf::Precision) -> Self {
        let Ok(precision_type) = s.precision_info.try_into() else {
            return Precision::Absent;
        };
        match precision_type {
            protobuf::PrecisionInfo::Exact => {
                if let Some(val) = s.val {
                    if let Ok(ScalarValue::UInt64(Some(val))) =
                        ScalarValue::try_from(&val)
                    {
                        Precision::Exact(val as usize)
                    } else {
                        Precision::Absent
                    }
                } else {
                    Precision::Absent
                }
            }
            protobuf::PrecisionInfo::Inexact => {
                if let Some(val) = s.val {
                    if let Ok(ScalarValue::UInt64(Some(val))) =
                        ScalarValue::try_from(&val)
                    {
                        Precision::Inexact(val as usize)
                    } else {
                        Precision::Absent
                    }
                } else {
                    Precision::Absent
                }
            }
            protobuf::PrecisionInfo::Absent => Precision::Absent,
        }
    }
}

impl From<protobuf::Precision> for Precision<ScalarValue> {
    fn from(s: protobuf::Precision) -> Self {
        let Ok(precision_type) = s.precision_info.try_into() else {
            return Precision::Absent;
        };
        match precision_type {
            protobuf::PrecisionInfo::Exact => {
                if let Some(val) = s.val {
                    if let Ok(val) = ScalarValue::try_from(&val) {
                        Precision::Exact(val)
                    } else {
                        Precision::Absent
                    }
                } else {
                    Precision::Absent
                }
            }
            protobuf::PrecisionInfo::Inexact => {
                if let Some(val) = s.val {
                    if let Ok(val) = ScalarValue::try_from(&val) {
                        Precision::Inexact(val)
                    } else {
                        Precision::Absent
                    }
                } else {
                    Precision::Absent
                }
            }
            protobuf::PrecisionInfo::Absent => Precision::Absent,
        }
    }
}

impl From<protobuf::JoinSide> for JoinSide {
    fn from(t: protobuf::JoinSide) -> Self {
        match t {
            protobuf::JoinSide::LeftSide => JoinSide::Left,
            protobuf::JoinSide::RightSide => JoinSide::Right,
            protobuf::JoinSide::None => JoinSide::None,
        }
    }
}

impl From<&protobuf::Constraint> for Constraint {
    fn from(value: &protobuf::Constraint) -> Self {
        match &value.constraint_mode {
            Some(protobuf::constraint::ConstraintMode::PrimaryKey(elem)) => {
                Constraint::PrimaryKey(
                    elem.indices.iter().map(|&item| item as usize).collect(),
                )
            }
            Some(protobuf::constraint::ConstraintMode::Unique(elem)) => {
                Constraint::Unique(
                    elem.indices.iter().map(|&item| item as usize).collect(),
                )
            }
            None => panic!("constraint_mode not set"),
        }
    }
}

impl TryFrom<&protobuf::Constraints> for Constraints {
    type Error = DataFusionError;

    fn try_from(constraints: &protobuf::Constraints) -> crate::Result<Self, Self::Error> {
        Ok(Constraints::new_unverified(
            constraints
                .constraints
                .iter()
                .map(|item| item.into())
                .collect(),
        ))
    }
}

impl TryFrom<&protobuf::Statistics> for Statistics {
    type Error = DataFusionError;

    fn try_from(s: &protobuf::Statistics) -> crate::Result<Self, Self::Error> {
        // Keep it sync with Statistics::to_proto
        Ok(Statistics {
            num_rows: if let Some(nr) = &s.num_rows {
                nr.clone().into()
            } else {
                Precision::Absent
            },
            total_byte_size: if let Some(tbs) = &s.total_byte_size {
                tbs.clone().into()
            } else {
                Precision::Absent
            },
            // No column statistic (None) is encoded with empty array
            column_statistics: s.column_stats.iter().map(|s| s.into()).collect(),
        })
    }
}

impl From<protobuf::CompressionTypeVariant> for CompressionTypeVariant {
    fn from(value: protobuf::CompressionTypeVariant) -> Self {
        match value {
            protobuf::CompressionTypeVariant::Gzip => Self::GZIP,
            protobuf::CompressionTypeVariant::Bzip2 => Self::BZIP2,
            protobuf::CompressionTypeVariant::Xz => Self::XZ,
            protobuf::CompressionTypeVariant::Zstd => Self::ZSTD,
            protobuf::CompressionTypeVariant::Uncompressed => Self::UNCOMPRESSED,
        }
    }
}

impl From<CompressionTypeVariant> for protobuf::CompressionTypeVariant {
    fn from(value: CompressionTypeVariant) -> Self {
        match value {
            CompressionTypeVariant::GZIP => Self::Gzip,
            CompressionTypeVariant::BZIP2 => Self::Bzip2,
            CompressionTypeVariant::XZ => Self::Xz,
            CompressionTypeVariant::ZSTD => Self::Zstd,
            CompressionTypeVariant::UNCOMPRESSED => Self::Uncompressed,
        }
    }
}

impl From<protobuf::CsvQuoteStyle> for crate::parsers::CsvQuoteStyle {
    fn from(value: protobuf::CsvQuoteStyle) -> Self {
        match value {
            protobuf::CsvQuoteStyle::Necessary => Self::Necessary,
            protobuf::CsvQuoteStyle::Always => Self::Always,
            protobuf::CsvQuoteStyle::NonNumeric => Self::NonNumeric,
            protobuf::CsvQuoteStyle::Never => Self::Never,
        }
    }
}

impl TryFrom<&protobuf::CsvWriterOptions> for CsvWriterOptions {
    type Error = DataFusionError;

    fn try_from(opts: &protobuf::CsvWriterOptions) -> crate::Result<Self, Self::Error> {
        let write_options = csv_writer_options_from_proto(opts)?;
        let compression: CompressionTypeVariant = opts.compression().into();
        Ok(CsvWriterOptions::new(write_options, compression))
    }
}

impl TryFrom<&protobuf::JsonWriterOptions> for JsonWriterOptions {
    type Error = DataFusionError;

    fn try_from(opts: &protobuf::JsonWriterOptions) -> crate::Result<Self, Self::Error> {
        let compression: CompressionTypeVariant = opts.compression().into();
        Ok(JsonWriterOptions::new(compression))
    }
}

impl TryFrom<&protobuf::CsvOptions> for CsvOptions {
    type Error = DataFusionError;

    fn try_from(proto_opts: &protobuf::CsvOptions) -> crate::Result<Self, Self::Error> {
        Ok(CsvOptions {
            has_header: proto_opts.has_header.first().map(|h| *h != 0),
            delimiter: proto_opts.delimiter.first().copied().unwrap_or(b','),
            quote: proto_opts.quote.first().copied().unwrap_or(b'"'),
            terminator: proto_opts.terminator.first().copied(),
            escape: proto_opts.escape.first().copied(),
            double_quote: proto_opts.double_quote.first().map(|h| *h != 0),
            newlines_in_values: proto_opts.newlines_in_values.first().map(|h| *h != 0),
            compression: proto_opts.compression().into(),
            compression_level: proto_opts.compression_level,
            schema_infer_max_rec: proto_opts.schema_infer_max_rec.map(|h| h as usize),
            date_format: (!proto_opts.date_format.is_empty())
                .then(|| proto_opts.date_format.clone()),
            datetime_format: (!proto_opts.datetime_format.is_empty())
                .then(|| proto_opts.datetime_format.clone()),
            timestamp_format: (!proto_opts.timestamp_format.is_empty())
                .then(|| proto_opts.timestamp_format.clone()),
            timestamp_tz_format: (!proto_opts.timestamp_tz_format.is_empty())
                .then(|| proto_opts.timestamp_tz_format.clone()),
            time_format: (!proto_opts.time_format.is_empty())
                .then(|| proto_opts.time_format.clone()),
            null_value: (!proto_opts.null_value.is_empty())
                .then(|| proto_opts.null_value.clone()),
            null_regex: (!proto_opts.null_regex.is_empty())
                .then(|| proto_opts.null_regex.clone()),
            comment: proto_opts.comment.first().copied(),
            truncated_rows: proto_opts.truncated_rows.first().map(|h| *h != 0),
            quote_style: proto_opts.quote_style().into(),
            ignore_leading_whitespace: proto_opts
                .ignore_leading_whitespace
                .first()
                .map(|h| *h != 0),
            ignore_trailing_whitespace: proto_opts
                .ignore_trailing_whitespace
                .first()
                .map(|h| *h != 0),
        })
    }
}

impl TryFrom<&protobuf::ParquetOptions> for ParquetOptions {
    type Error = DataFusionError;

    fn try_from(value: &protobuf::ParquetOptions) -> crate::Result<Self, Self::Error> {
        Ok(ParquetOptions {
            enable_page_index: value.enable_page_index,
            pruning: value.pruning,
            skip_metadata: value.skip_metadata,
            metadata_size_hint: value
                .metadata_size_hint_opt
                .map(|opt| match opt {
                    protobuf::parquet_options::MetadataSizeHintOpt::MetadataSizeHint(v) => Some(v as usize),
                })
                .unwrap_or(None),
            pushdown_filters: value.pushdown_filters,
            reorder_filters: value.reorder_filters,
            force_filter_selections: value.force_filter_selections,
            data_pagesize_limit: value.data_pagesize_limit as usize,
            write_batch_size: value.write_batch_size as usize,
            writer_version: value.writer_version.parse().map_err(|e| {
                DataFusionError::Internal(format!("Failed to parse writer_version: {e}"))
            })?,
            compression: value.compression_opt.clone().map(|opt| match opt {
                protobuf::parquet_options::CompressionOpt::Compression(v) => Some(v),
            }).unwrap_or(None),
            dictionary_enabled: value.dictionary_enabled_opt.as_ref().map(|protobuf::parquet_options::DictionaryEnabledOpt::DictionaryEnabled(v)| *v),
            // Continuing from where we left off in the TryFrom implementation
            dictionary_page_size_limit: value.dictionary_page_size_limit as usize,
            statistics_enabled: value
                .statistics_enabled_opt.clone()
                .map(|opt| match opt {
                    protobuf::parquet_options::StatisticsEnabledOpt::StatisticsEnabled(v) => Some(v),
                })
                .unwrap_or(None),
            max_row_group_size: value.max_row_group_size as usize,
            created_by: value.created_by.clone(),
            column_index_truncate_length: value
                .column_index_truncate_length_opt.as_ref()
                .map(|opt| match opt {
                    protobuf::parquet_options::ColumnIndexTruncateLengthOpt::ColumnIndexTruncateLength(v) => Some(*v as usize),
                })
                .unwrap_or(None),
            statistics_truncate_length: value
                .statistics_truncate_length_opt.as_ref()
                .map(|opt| match opt {
                    protobuf::parquet_options::StatisticsTruncateLengthOpt::StatisticsTruncateLength(v) => Some(*v as usize),
                })
                .unwrap_or(None),
            data_page_row_count_limit: value.data_page_row_count_limit as usize,
            encoding: value
                .encoding_opt.clone()
                .map(|opt| match opt {
                    protobuf::parquet_options::EncodingOpt::Encoding(v) => Some(v),
                })
                .unwrap_or(None),
            bloom_filter_on_read: value.bloom_filter_on_read,
            bloom_filter_on_write: value.bloom_filter_on_write,
            bloom_filter_fpp: value.clone()
                .bloom_filter_fpp_opt
                .map(|opt| match opt {
                    protobuf::parquet_options::BloomFilterFppOpt::BloomFilterFpp(v) => Some(v),
                })
                .unwrap_or(None),
            bloom_filter_ndv: value.clone()
                .bloom_filter_ndv_opt
                .map(|opt| match opt {
                    protobuf::parquet_options::BloomFilterNdvOpt::BloomFilterNdv(v) => Some(v),
                })
                .unwrap_or(None),
            allow_single_file_parallelism: value.allow_single_file_parallelism,
            maximum_parallel_row_group_writers: value.maximum_parallel_row_group_writers as usize,
            maximum_buffered_record_batches_per_stream: value.maximum_buffered_record_batches_per_stream as usize,
            schema_force_view_types: value.schema_force_view_types,
            binary_as_string: value.binary_as_string,
            coerce_int96: value.coerce_int96_opt.clone().map(|opt| match opt {
                protobuf::parquet_options::CoerceInt96Opt::CoerceInt96(v) => Some(v),
            }).unwrap_or(None),
            skip_arrow_metadata: value.skip_arrow_metadata,
            max_predicate_cache_size: value.max_predicate_cache_size_opt.map(|opt| match opt {
                protobuf::parquet_options::MaxPredicateCacheSizeOpt::MaxPredicateCacheSize(v) => Some(v as usize),
            }).unwrap_or(None),
            use_content_defined_chunking: value.content_defined_chunking.map(|cdc| {
                let defaults = CdcOptions::default();
                CdcOptions {
                    // proto3 uses 0 as the wire default for uint64; a zero chunk size is
                    // invalid, so treat it as "field not set" and fall back to the default.
                    min_chunk_size: if cdc.min_chunk_size != 0 { cdc.min_chunk_size as usize } else { defaults.min_chunk_size },
                    max_chunk_size: if cdc.max_chunk_size != 0 { cdc.max_chunk_size as usize } else { defaults.max_chunk_size },
                    // norm_level = 0 is a valid value (and the default), so pass it through directly.
                    norm_level: cdc.norm_level,
                }
            }),
        })
    }
}

impl TryFrom<&protobuf::ParquetColumnOptions> for ParquetColumnOptions {
    type Error = DataFusionError;
    fn try_from(
        value: &protobuf::ParquetColumnOptions,
    ) -> crate::Result<Self, Self::Error> {
        Ok(ParquetColumnOptions {
            compression: value.compression_opt.clone().map(|opt| match opt {
                protobuf::parquet_column_options::CompressionOpt::Compression(v) => Some(v),
            }).unwrap_or(None),
            dictionary_enabled: value.dictionary_enabled_opt.as_ref().map(|protobuf::parquet_column_options::DictionaryEnabledOpt::DictionaryEnabled(v)| *v),
            statistics_enabled: value
                .statistics_enabled_opt.clone()
                .map(|opt| match opt {
                    protobuf::parquet_column_options::StatisticsEnabledOpt::StatisticsEnabled(v) => Some(v),
                })
                .unwrap_or(None),
            encoding: value
                .encoding_opt.clone()
                .map(|opt| match opt {
                    protobuf::parquet_column_options::EncodingOpt::Encoding(v) => Some(v),
                })
                .unwrap_or(None),
            bloom_filter_enabled: value.bloom_filter_enabled_opt.map(|opt| match opt {
                protobuf::parquet_column_options::BloomFilterEnabledOpt::BloomFilterEnabled(v) => Some(v),
            })
                .unwrap_or(None),
            bloom_filter_fpp: value
                .bloom_filter_fpp_opt
                .map(|opt| match opt {
                    protobuf::parquet_column_options::BloomFilterFppOpt::BloomFilterFpp(v) => Some(v),
                })
                .unwrap_or(None),
            bloom_filter_ndv: value
                .bloom_filter_ndv_opt
                .map(|opt| match opt {
                    protobuf::parquet_column_options::BloomFilterNdvOpt::BloomFilterNdv(v) => Some(v),
                })
                .unwrap_or(None),
        })
    }
}

impl TryFrom<&protobuf::TableParquetOptions> for TableParquetOptions {
    type Error = DataFusionError;
    fn try_from(
        value: &protobuf::TableParquetOptions,
    ) -> crate::Result<Self, Self::Error> {
        let mut column_specific_options: HashMap<String, ParquetColumnOptions> =
            HashMap::new();
        for protobuf::ParquetColumnSpecificOptions {
            column_name,
            options: maybe_options,
        } in &value.column_specific_options
        {
            if let Some(options) = maybe_options {
                column_specific_options.insert(column_name.clone(), options.try_into()?);
            }
        }
        let opts = TableParquetOptions {
            global: value
                .global
                .as_ref()
                .map(|v| v.try_into())
                .unwrap()
                .unwrap(),
            column_specific_options,
            key_value_metadata: value
                .key_value_metadata
                .iter()
                .map(|(k, v)| (k.clone(), Some(v.clone())))
                .collect(),
            ..Default::default()
        };
        Ok(opts)
    }
}

impl TryFrom<&protobuf::JsonOptions> for JsonOptions {
    type Error = DataFusionError;

    fn try_from(proto_opts: &protobuf::JsonOptions) -> crate::Result<Self, Self::Error> {
        let compression: protobuf::CompressionTypeVariant = proto_opts.compression();
        Ok(JsonOptions {
            compression: compression.into(),
            compression_level: proto_opts.compression_level,
            schema_infer_max_rec: proto_opts.schema_infer_max_rec.map(|h| h as usize),
            newline_delimited: proto_opts.newline_delimited.unwrap_or(true),
        })
    }
}

// Conversion impls for `package datafusion_common` enum types whose target
// types live in `datafusion-common`. These are foreign-foreign in
// datafusion-proto, but here both sides — proto and Rust — sit on at least
// one local side, so the orphan rule is satisfied.

impl From<protobuf::JoinType> for crate::JoinType {
    fn from(t: protobuf::JoinType) -> Self {
        use crate::JoinType;
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

impl From<protobuf::JoinConstraint> for crate::JoinConstraint {
    fn from(t: protobuf::JoinConstraint) -> Self {
        use crate::JoinConstraint;
        match t {
            protobuf::JoinConstraint::On => JoinConstraint::On,
            protobuf::JoinConstraint::Using => JoinConstraint::Using,
        }
    }
}

impl From<protobuf::NullEquality> for crate::NullEquality {
    fn from(t: protobuf::NullEquality) -> Self {
        use crate::NullEquality;
        match t {
            protobuf::NullEquality::NullEqualsNothing => NullEquality::NullEqualsNothing,
            protobuf::NullEquality::NullEqualsNull => NullEquality::NullEqualsNull,
        }
    }
}

// Conversion impls for `package datafusion` types whose targets live in
// `datafusion-common`. These reference proto types under their full path
// because the local `protobuf` alias only covers the common package.

impl From<&datafusion_proto_common::generated::datafusion::UnnestOptions>
    for crate::UnnestOptions
{
    fn from(
        opts: &datafusion_proto_common::generated::datafusion::UnnestOptions,
    ) -> Self {
        Self {
            preserve_nulls: opts.preserve_nulls,
            recursions: opts
                .recursions
                .iter()
                .map(|r| crate::RecursionUnnestOption {
                    input_column: r.input_column.as_ref().unwrap().into(),
                    output_column: r.output_column.as_ref().unwrap().into(),
                    depth: r.depth as usize,
                })
                .collect::<Vec<_>>(),
        }
    }
}

impl TryFrom<datafusion_proto_common::generated::datafusion::TableReference>
    for TableReference
{
    type Error = Error;

    fn try_from(
        value: datafusion_proto_common::generated::datafusion::TableReference,
    ) -> crate::Result<Self, Self::Error> {
        use datafusion_proto_common::generated::datafusion::table_reference::TableReferenceEnum;
        let table_reference_enum = value
            .table_reference_enum
            .ok_or_else(|| Error::required("table_reference_enum"))?;

        match table_reference_enum {
            TableReferenceEnum::Bare(
                datafusion_proto_common::generated::datafusion::BareTableReference {
                    table,
                },
            ) => Ok(TableReference::bare(table)),
            TableReferenceEnum::Partial(
                datafusion_proto_common::generated::datafusion::PartialTableReference {
                    schema,
                    table,
                },
            ) => Ok(TableReference::partial(schema, table)),
            TableReferenceEnum::Full(
                datafusion_proto_common::generated::datafusion::FullTableReference {
                    catalog,
                    schema,
                    table,
                },
            ) => Ok(TableReference::full(catalog, schema, table)),
        }
    }
}

impl From<&datafusion_proto_common::generated::datafusion::StringifiedPlan>
    for crate::display::StringifiedPlan
{
    fn from(
        stringified_plan: &datafusion_proto_common::generated::datafusion::StringifiedPlan,
    ) -> Self {
        use crate::display::PlanType;
        use datafusion_proto_common::generated::datafusion::plan_type::PlanTypeEnum::{
            AnalyzedLogicalPlan, FinalAnalyzedLogicalPlan, FinalLogicalPlan,
            FinalPhysicalPlan, FinalPhysicalPlanWithSchema, FinalPhysicalPlanWithStats,
            InitialLogicalPlan, InitialPhysicalPlan, InitialPhysicalPlanWithSchema,
            InitialPhysicalPlanWithStats, OptimizedLogicalPlan, OptimizedPhysicalPlan,
            PhysicalPlanError,
        };
        use datafusion_proto_common::generated::datafusion::{
            AnalyzedLogicalPlanType, OptimizedLogicalPlanType, OptimizedPhysicalPlanType,
        };
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
                        analyzer_name: analyzer_name.clone(),
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

// panic here because no better way to convert from Vec to Array
fn vec_to_array<T, const N: usize>(v: Vec<T>) -> [T; N] {
    v.try_into().unwrap_or_else(|v: Vec<T>| {
        panic!("Expected a Vec of length {} but it was {}", N, v.len())
    })
}

pub(crate) fn csv_writer_options_from_proto(
    writer_options: &protobuf::CsvWriterOptions,
) -> crate::Result<WriterBuilder> {
    let mut builder = WriterBuilder::new();
    if !writer_options.delimiter.is_empty() {
        if let Some(delimiter) = writer_options.delimiter.chars().next() {
            if delimiter.is_ascii() {
                builder = builder.with_delimiter(delimiter as u8);
            } else {
                return Err(proto_error("CSV Delimiter is not ASCII"));
            }
        } else {
            return Err(proto_error("Error parsing CSV Delimiter"));
        }
    }
    if !writer_options.quote.is_empty() {
        if let Some(quote) = writer_options.quote.chars().next() {
            if quote.is_ascii() {
                builder = builder.with_quote(quote as u8);
            } else {
                return Err(proto_error("CSV Quote is not ASCII"));
            }
        } else {
            return Err(proto_error("Error parsing CSV Quote"));
        }
    }
    if !writer_options.escape.is_empty() {
        if let Some(escape) = writer_options.escape.chars().next() {
            if escape.is_ascii() {
                builder = builder.with_escape(escape as u8);
            } else {
                return Err(proto_error("CSV Escape is not ASCII"));
            }
        } else {
            return Err(proto_error("Error parsing CSV Escape"));
        }
    }
    let quote_style = match protobuf::CsvQuoteStyle::try_from(writer_options.quote_style)
    {
        Ok(protobuf::CsvQuoteStyle::Always) => QuoteStyle::Always,
        Ok(protobuf::CsvQuoteStyle::NonNumeric) => QuoteStyle::NonNumeric,
        Ok(protobuf::CsvQuoteStyle::Never) => QuoteStyle::Never,
        Ok(protobuf::CsvQuoteStyle::Necessary) => QuoteStyle::Necessary,
        _ => Err(proto_error(
            "Unknown quote style, must be one of: 'Always', 'NonNumeric', 'Never', 'Necessary'",
        ))?,
    };
    Ok(builder
        .with_header(writer_options.has_header)
        .with_date_format(writer_options.date_format.clone())
        .with_datetime_format(writer_options.datetime_format.clone())
        .with_timestamp_format(writer_options.timestamp_format.clone())
        .with_time_format(writer_options.time_format.clone())
        .with_null(writer_options.null_value.clone())
        .with_double_quote(writer_options.double_quote)
        .with_quote_style(quote_style)
        .with_ignore_leading_whitespace(writer_options.ignore_leading_whitespace)
        .with_ignore_trailing_whitespace(writer_options.ignore_trailing_whitespace))
}

#[cfg(test)]
mod tests {
    use crate::config::{CdcOptions, ParquetOptions, TableParquetOptions};

    fn parquet_options_proto_round_trip(opts: ParquetOptions) -> ParquetOptions {
        let proto: datafusion_proto_common::protobuf_common::ParquetOptions =
            (&opts).try_into().expect("to_proto");
        ParquetOptions::try_from(&proto).expect("from_proto")
    }

    fn table_parquet_options_proto_round_trip(
        opts: TableParquetOptions,
    ) -> TableParquetOptions {
        let proto: datafusion_proto_common::protobuf_common::TableParquetOptions =
            (&opts).try_into().expect("to_proto");
        TableParquetOptions::try_from(&proto).expect("from_proto")
    }

    #[test]
    fn test_parquet_options_cdc_disabled_round_trip() {
        let opts = ParquetOptions::default();
        assert!(opts.use_content_defined_chunking.is_none());
        let recovered = parquet_options_proto_round_trip(opts.clone());
        assert_eq!(opts, recovered);
    }

    #[test]
    fn test_parquet_options_cdc_enabled_round_trip() {
        let opts = ParquetOptions {
            use_content_defined_chunking: Some(CdcOptions {
                min_chunk_size: 128 * 1024,
                max_chunk_size: 512 * 1024,
                norm_level: 2,
            }),
            ..ParquetOptions::default()
        };
        let recovered = parquet_options_proto_round_trip(opts.clone());
        let cdc = recovered.use_content_defined_chunking.unwrap();
        assert_eq!(cdc.min_chunk_size, 128 * 1024);
        assert_eq!(cdc.max_chunk_size, 512 * 1024);
        assert_eq!(cdc.norm_level, 2);
    }

    #[test]
    fn test_parquet_options_cdc_negative_norm_level_round_trip() {
        let opts = ParquetOptions {
            use_content_defined_chunking: Some(CdcOptions {
                norm_level: -3,
                ..CdcOptions::default()
            }),
            ..ParquetOptions::default()
        };
        let recovered = parquet_options_proto_round_trip(opts);
        assert_eq!(
            recovered.use_content_defined_chunking.unwrap().norm_level,
            -3
        );
    }

    #[test]
    fn test_table_parquet_options_cdc_round_trip() {
        let mut opts = TableParquetOptions::default();
        opts.global.use_content_defined_chunking = Some(CdcOptions {
            min_chunk_size: 64 * 1024,
            max_chunk_size: 2 * 1024 * 1024,
            norm_level: -1,
        });

        let recovered = table_parquet_options_proto_round_trip(opts.clone());
        let cdc = recovered.global.use_content_defined_chunking.unwrap();
        assert_eq!(cdc.min_chunk_size, 64 * 1024);
        assert_eq!(cdc.max_chunk_size, 2 * 1024 * 1024);
        assert_eq!(cdc.norm_level, -1);
    }

    #[test]
    fn test_table_parquet_options_cdc_disabled_round_trip() {
        let opts = TableParquetOptions::default();
        assert!(opts.global.use_content_defined_chunking.is_none());
        let recovered = table_parquet_options_proto_round_trip(opts.clone());
        assert!(recovered.global.use_content_defined_chunking.is_none());
    }
}
