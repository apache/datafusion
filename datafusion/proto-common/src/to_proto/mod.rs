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

//! Conversions from Arrow types to protobuf wire types.
//!
//! Conversions from `datafusion-common` types live in
//! `datafusion-common::proto::to_proto` (under the `proto` feature).

use std::sync::Arc;

use crate::protobuf_common as protobuf;
use crate::protobuf_common::{EmptyMessage, arrow_type::ArrowTypeEnum};
use arrow::csv::QuoteStyle;
use arrow::datatypes::{
    DataType, Field, IntervalUnit, Schema, SchemaRef, TimeUnit, UnionMode,
};

#[derive(Debug)]
pub enum Error {
    General(String),

    InvalidTimeUnit(TimeUnit),

    NotImplemented(String),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::General(desc) => write!(f, "General error: {desc}"),
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

impl TryFrom<&Field> for protobuf::Field {
    type Error = Error;

    fn try_from(field: &Field) -> Result<Self, Self::Error> {
        let arrow_type = field.data_type().try_into()?;
        Ok(Self {
            name: field.name().to_owned(),
            arrow_type: Some(Box::new(arrow_type)),
            nullable: field.is_nullable(),
            children: Vec::new(),
            metadata: field.metadata().clone(),
        })
    }
}

impl TryFrom<&DataType> for protobuf::ArrowType {
    type Error = Error;

    fn try_from(val: &DataType) -> Result<Self, Self::Error> {
        let arrow_type_enum: ArrowTypeEnum = val.try_into()?;
        Ok(Self {
            arrow_type_enum: Some(arrow_type_enum),
        })
    }
}

impl TryFrom<&DataType> for protobuf::arrow_type::ArrowTypeEnum {
    type Error = Error;

    fn try_from(val: &DataType) -> Result<Self, Self::Error> {
        let res = match val {
            DataType::Null => Self::None(EmptyMessage {}),
            DataType::Boolean => Self::Bool(EmptyMessage {}),
            DataType::Int8 => Self::Int8(EmptyMessage {}),
            DataType::Int16 => Self::Int16(EmptyMessage {}),
            DataType::Int32 => Self::Int32(EmptyMessage {}),
            DataType::Int64 => Self::Int64(EmptyMessage {}),
            DataType::UInt8 => Self::Uint8(EmptyMessage {}),
            DataType::UInt16 => Self::Uint16(EmptyMessage {}),
            DataType::UInt32 => Self::Uint32(EmptyMessage {}),
            DataType::UInt64 => Self::Uint64(EmptyMessage {}),
            DataType::Float16 => Self::Float16(EmptyMessage {}),
            DataType::Float32 => Self::Float32(EmptyMessage {}),
            DataType::Float64 => Self::Float64(EmptyMessage {}),
            DataType::Timestamp(time_unit, timezone) => {
                Self::Timestamp(protobuf::Timestamp {
                    time_unit: protobuf::TimeUnit::from(time_unit) as i32,
                    timezone: timezone.as_deref().unwrap_or("").to_string(),
                })
            }
            DataType::Date32 => Self::Date32(EmptyMessage {}),
            DataType::Date64 => Self::Date64(EmptyMessage {}),
            DataType::Time32(time_unit) => {
                Self::Time32(protobuf::TimeUnit::from(time_unit) as i32)
            }
            DataType::Time64(time_unit) => {
                Self::Time64(protobuf::TimeUnit::from(time_unit) as i32)
            }
            DataType::Duration(time_unit) => {
                Self::Duration(protobuf::TimeUnit::from(time_unit) as i32)
            }
            DataType::Interval(interval_unit) => {
                Self::Interval(protobuf::IntervalUnit::from(interval_unit) as i32)
            }
            DataType::Binary => Self::Binary(EmptyMessage {}),
            DataType::BinaryView => Self::BinaryView(EmptyMessage {}),
            DataType::FixedSizeBinary(size) => Self::FixedSizeBinary(*size),
            DataType::LargeBinary => Self::LargeBinary(EmptyMessage {}),
            DataType::Utf8 => Self::Utf8(EmptyMessage {}),
            DataType::Utf8View => Self::Utf8View(EmptyMessage {}),
            DataType::LargeUtf8 => Self::LargeUtf8(EmptyMessage {}),
            DataType::List(item_type) => Self::List(Box::new(protobuf::List {
                field_type: Some(Box::new(item_type.as_ref().try_into()?)),
            })),
            DataType::FixedSizeList(item_type, size) => {
                Self::FixedSizeList(Box::new(protobuf::FixedSizeList {
                    field_type: Some(Box::new(item_type.as_ref().try_into()?)),
                    list_size: *size,
                }))
            }
            DataType::LargeList(item_type) => Self::LargeList(Box::new(protobuf::List {
                field_type: Some(Box::new(item_type.as_ref().try_into()?)),
            })),
            DataType::ListView(item_type) => Self::ListView(Box::new(protobuf::List {
                field_type: Some(Box::new(item_type.as_ref().try_into()?)),
            })),
            DataType::LargeListView(item_type) => {
                Self::LargeListView(Box::new(protobuf::List {
                    field_type: Some(Box::new(item_type.as_ref().try_into()?)),
                }))
            }
            DataType::Struct(struct_fields) => Self::Struct(protobuf::Struct {
                sub_field_types: convert_arc_fields_to_proto_fields(struct_fields)?,
            }),
            DataType::Union(fields, union_mode) => {
                let union_mode = match union_mode {
                    UnionMode::Sparse => protobuf::UnionMode::Sparse,
                    UnionMode::Dense => protobuf::UnionMode::Dense,
                };
                let (type_ids, fields): (Vec<_>, Vec<_>) = fields
                    .iter()
                    .map(|(type_id, field)| (type_id as i32, field))
                    .unzip();
                Self::Union(protobuf::Union {
                    union_types: convert_arc_fields_to_proto_fields(
                        fields.iter().copied(),
                    )?,
                    union_mode: union_mode.into(),
                    type_ids,
                })
            }
            DataType::Dictionary(key_type, value_type) => {
                Self::Dictionary(Box::new(protobuf::Dictionary {
                    key: Some(Box::new(key_type.as_ref().try_into()?)),
                    value: Some(Box::new(value_type.as_ref().try_into()?)),
                }))
            }
            DataType::Decimal32(precision, scale) => {
                Self::Decimal32(protobuf::Decimal32Type {
                    precision: *precision as u32,
                    scale: *scale as i32,
                })
            }
            DataType::Decimal64(precision, scale) => {
                Self::Decimal64(protobuf::Decimal64Type {
                    precision: *precision as u32,
                    scale: *scale as i32,
                })
            }
            DataType::Decimal128(precision, scale) => {
                Self::Decimal128(protobuf::Decimal128Type {
                    precision: *precision as u32,
                    scale: *scale as i32,
                })
            }
            DataType::Decimal256(precision, scale) => {
                Self::Decimal256(protobuf::Decimal256Type {
                    precision: *precision as u32,
                    scale: *scale as i32,
                })
            }
            DataType::Map(field, keys_sorted) => Self::Map(Box::new(protobuf::Map {
                field_type: Some(Box::new(field.as_ref().try_into()?)),
                keys_sorted: *keys_sorted,
            })),
            DataType::RunEndEncoded(run_end_field, value_field) => {
                Self::RunEndEncoded(Box::new(protobuf::RunEndEncoded {
                    run_ends_field: Some(Box::new(run_end_field.as_ref().try_into()?)),
                    values_field: Some(Box::new(value_field.as_ref().try_into()?)),
                }))
            }
        };
        Ok(res)
    }
}

impl TryFrom<&Schema> for protobuf::Schema {
    type Error = Error;

    fn try_from(schema: &Schema) -> Result<Self, Self::Error> {
        Ok(Self {
            columns: convert_arc_fields_to_proto_fields(schema.fields())?,
            metadata: schema.metadata.clone(),
        })
    }
}

impl TryFrom<SchemaRef> for protobuf::Schema {
    type Error = Error;

    fn try_from(schema: SchemaRef) -> Result<Self, Self::Error> {
        Ok(Self {
            columns: convert_arc_fields_to_proto_fields(schema.fields())?,
            metadata: schema.metadata.clone(),
        })
    }
}

impl From<&TimeUnit> for protobuf::TimeUnit {
    fn from(value: &TimeUnit) -> Self {
        match value {
            TimeUnit::Second => protobuf::TimeUnit::Second,
            TimeUnit::Millisecond => protobuf::TimeUnit::Millisecond,
            TimeUnit::Microsecond => protobuf::TimeUnit::Microsecond,
            TimeUnit::Nanosecond => protobuf::TimeUnit::Nanosecond,
        }
    }
}

impl From<QuoteStyle> for protobuf::CsvQuoteStyle {
    fn from(value: QuoteStyle) -> Self {
        match value {
            QuoteStyle::Necessary => Self::Necessary,
            QuoteStyle::Always => Self::Always,
            QuoteStyle::NonNumeric => Self::NonNumeric,
            QuoteStyle::Never => Self::Never,
            _ => Self::Necessary,
        }
    }
}

impl From<&IntervalUnit> for protobuf::IntervalUnit {
    fn from(interval_unit: &IntervalUnit) -> Self {
        match interval_unit {
            IntervalUnit::YearMonth => protobuf::IntervalUnit::YearMonth,
            IntervalUnit::DayTime => protobuf::IntervalUnit::DayTime,
            IntervalUnit::MonthDayNano => protobuf::IntervalUnit::MonthDayNano,
        }
    }
}

fn convert_arc_fields_to_proto_fields<'a, I>(
    fields: I,
) -> Result<Vec<protobuf::Field>, Error>
where
    I: IntoIterator<Item = &'a Arc<Field>>,
{
    fields
        .into_iter()
        .map(|field| field.as_ref().try_into())
        .collect::<Result<Vec<_>, Error>>()
}
