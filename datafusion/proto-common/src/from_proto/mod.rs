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

//! Conversions from protobuf wire types to Arrow types.
//!
//! Conversions to `datafusion-common` types live in
//! `datafusion-common::proto::from_proto` (under the `proto` feature).

use std::sync::Arc;

use crate::protobuf_common as protobuf;
use arrow::datatypes::{
    DataType, Field, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode,
};

#[derive(Debug)]
pub enum Error {
    General(String),

    /// An Arrow error encountered while decoding a wire message — surfaces
    /// through the proto-common boundary so callers can downcast or
    /// inspect the underlying `arrow::error::ArrowError`.
    Arrow(arrow::error::ArrowError),

    MissingRequiredField(String),

    AtLeastOneValue(String),

    UnknownEnumVariant {
        name: String,
        value: i32,
    },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::General(desc) => write!(f, "General error: {desc}"),

            Self::Arrow(e) => write!(f, "Arrow error: {e}"),

            Self::MissingRequiredField(name) => {
                write!(f, "Missing required field {name}")
            }
            Self::AtLeastOneValue(name) => {
                write!(f, "Must have at least one {name}, found 0")
            }
            Self::UnknownEnumVariant { name, value } => {
                write!(f, "Unknown i32 value for {name} enum: {value}")
            }
        }
    }
}

impl std::error::Error for Error {}

impl From<arrow::error::ArrowError> for Error {
    fn from(e: arrow::error::ArrowError) -> Self {
        Error::Arrow(e)
    }
}

impl Error {
    pub fn required(field: impl Into<String>) -> Error {
        Error::MissingRequiredField(field.into())
    }

    pub fn unknown(name: impl Into<String>, value: i32) -> Error {
        Error::UnknownEnumVariant {
            name: name.into(),
            value,
        }
    }
}

/// An extension trait that adds the methods `optional` and `required` to any
/// Option containing a type implementing `TryInto<U, Error = Error>`
pub trait FromOptionalField<T> {
    /// Converts an optional protobuf field to an option of a different type
    ///
    /// Returns None if the option is None, otherwise calls [`TryInto::try_into`]
    /// on the contained data, returning any error encountered
    fn optional(self) -> Result<Option<T>, Error>;

    /// Converts an optional protobuf field to a different type, returning an error if None
    ///
    /// Returns `Error::MissingRequiredField` if None, otherwise calls [`TryInto::try_into`]
    /// on the contained data, returning any error encountered
    fn required(self, field: impl Into<String>) -> Result<T, Error>;
}

impl<T, U> FromOptionalField<U> for Option<T>
where
    T: TryInto<U, Error = Error>,
{
    fn optional(self) -> Result<Option<U>, Error> {
        self.map(|t| t.try_into()).transpose()
    }

    fn required(self, field: impl Into<String>) -> Result<U, Error> {
        match self {
            None => Err(Error::required(field)),
            Some(t) => t.try_into(),
        }
    }
}

impl TryFrom<&protobuf::ArrowType> for DataType {
    type Error = Error;

    fn try_from(arrow_type: &protobuf::ArrowType) -> Result<Self, Self::Error> {
        arrow_type
            .arrow_type_enum
            .as_ref()
            .required("arrow_type_enum")
    }
}

impl TryFrom<&protobuf::arrow_type::ArrowTypeEnum> for DataType {
    type Error = Error;
    fn try_from(
        arrow_type_enum: &protobuf::arrow_type::ArrowTypeEnum,
    ) -> Result<Self, Self::Error> {
        use protobuf::arrow_type;
        Ok(match arrow_type_enum {
            arrow_type::ArrowTypeEnum::None(_) => DataType::Null,
            arrow_type::ArrowTypeEnum::Bool(_) => DataType::Boolean,
            arrow_type::ArrowTypeEnum::Uint8(_) => DataType::UInt8,
            arrow_type::ArrowTypeEnum::Int8(_) => DataType::Int8,
            arrow_type::ArrowTypeEnum::Uint16(_) => DataType::UInt16,
            arrow_type::ArrowTypeEnum::Int16(_) => DataType::Int16,
            arrow_type::ArrowTypeEnum::Uint32(_) => DataType::UInt32,
            arrow_type::ArrowTypeEnum::Int32(_) => DataType::Int32,
            arrow_type::ArrowTypeEnum::Uint64(_) => DataType::UInt64,
            arrow_type::ArrowTypeEnum::Int64(_) => DataType::Int64,
            arrow_type::ArrowTypeEnum::Float16(_) => DataType::Float16,
            arrow_type::ArrowTypeEnum::Float32(_) => DataType::Float32,
            arrow_type::ArrowTypeEnum::Float64(_) => DataType::Float64,
            arrow_type::ArrowTypeEnum::Utf8(_) => DataType::Utf8,
            arrow_type::ArrowTypeEnum::Utf8View(_) => DataType::Utf8View,
            arrow_type::ArrowTypeEnum::LargeUtf8(_) => DataType::LargeUtf8,
            arrow_type::ArrowTypeEnum::Binary(_) => DataType::Binary,
            arrow_type::ArrowTypeEnum::BinaryView(_) => DataType::BinaryView,
            arrow_type::ArrowTypeEnum::FixedSizeBinary(size) => {
                DataType::FixedSizeBinary(*size)
            }
            arrow_type::ArrowTypeEnum::LargeBinary(_) => DataType::LargeBinary,
            arrow_type::ArrowTypeEnum::Date32(_) => DataType::Date32,
            arrow_type::ArrowTypeEnum::Date64(_) => DataType::Date64,
            arrow_type::ArrowTypeEnum::Duration(time_unit) => {
                DataType::Duration(parse_i32_to_time_unit(time_unit)?)
            }
            arrow_type::ArrowTypeEnum::Timestamp(protobuf::Timestamp {
                time_unit,
                timezone,
            }) => DataType::Timestamp(
                parse_i32_to_time_unit(time_unit)?,
                match timezone.len() {
                    0 => None,
                    _ => Some(timezone.as_str().into()),
                },
            ),
            arrow_type::ArrowTypeEnum::Time32(time_unit) => {
                DataType::Time32(parse_i32_to_time_unit(time_unit)?)
            }
            arrow_type::ArrowTypeEnum::Time64(time_unit) => {
                DataType::Time64(parse_i32_to_time_unit(time_unit)?)
            }
            arrow_type::ArrowTypeEnum::Interval(interval_unit) => {
                DataType::Interval(parse_i32_to_interval_unit(interval_unit)?)
            }
            arrow_type::ArrowTypeEnum::Decimal32(protobuf::Decimal32Type {
                precision,
                scale,
            }) => DataType::Decimal32(*precision as u8, *scale as i8),
            arrow_type::ArrowTypeEnum::Decimal64(protobuf::Decimal64Type {
                precision,
                scale,
            }) => DataType::Decimal64(*precision as u8, *scale as i8),
            arrow_type::ArrowTypeEnum::Decimal128(protobuf::Decimal128Type {
                precision,
                scale,
            }) => DataType::Decimal128(*precision as u8, *scale as i8),
            arrow_type::ArrowTypeEnum::Decimal256(protobuf::Decimal256Type {
                precision,
                scale,
            }) => DataType::Decimal256(*precision as u8, *scale as i8),
            arrow_type::ArrowTypeEnum::List(list) => {
                let list_type =
                    list.as_ref().field_type.as_deref().required("field_type")?;
                DataType::List(Arc::new(list_type))
            }
            arrow_type::ArrowTypeEnum::LargeList(list) => {
                let list_type =
                    list.as_ref().field_type.as_deref().required("field_type")?;
                DataType::LargeList(Arc::new(list_type))
            }
            arrow_type::ArrowTypeEnum::FixedSizeList(list) => {
                let list_type =
                    list.as_ref().field_type.as_deref().required("field_type")?;
                let list_size = list.list_size;
                DataType::FixedSizeList(Arc::new(list_type), list_size)
            }
            arrow_type::ArrowTypeEnum::ListView(list) => {
                let list_type =
                    list.as_ref().field_type.as_deref().required("field_type")?;
                DataType::ListView(Arc::new(list_type))
            }
            arrow_type::ArrowTypeEnum::LargeListView(list) => {
                let list_type =
                    list.as_ref().field_type.as_deref().required("field_type")?;
                DataType::LargeListView(Arc::new(list_type))
            }
            arrow_type::ArrowTypeEnum::Struct(strct) => DataType::Struct(
                parse_proto_fields_to_fields(&strct.sub_field_types)?.into(),
            ),
            arrow_type::ArrowTypeEnum::Union(union) => {
                let union_mode = protobuf::UnionMode::try_from(union.union_mode)
                    .map_err(|_| Error::unknown("UnionMode", union.union_mode))?;
                let union_mode = match union_mode {
                    protobuf::UnionMode::Dense => UnionMode::Dense,
                    protobuf::UnionMode::Sparse => UnionMode::Sparse,
                };
                let union_fields = parse_proto_fields_to_fields(&union.union_types)?;

                // Default to index based type ids if not explicitly provided
                let union_fields = if union.type_ids.is_empty() {
                    UnionFields::from_fields(union_fields)
                } else {
                    let type_ids = union.type_ids.iter().map(|i| *i as i8);
                    UnionFields::try_new(type_ids, union_fields).map_err(Error::Arrow)?
                };
                DataType::Union(union_fields, union_mode)
            }
            arrow_type::ArrowTypeEnum::Dictionary(dict) => {
                let key_datatype = dict.as_ref().key.as_deref().required("key")?;
                let value_datatype = dict.as_ref().value.as_deref().required("value")?;
                DataType::Dictionary(Box::new(key_datatype), Box::new(value_datatype))
            }
            arrow_type::ArrowTypeEnum::Map(map) => {
                let field: Field =
                    map.as_ref().field_type.as_deref().required("field_type")?;
                let keys_sorted = map.keys_sorted;
                DataType::Map(Arc::new(field), keys_sorted)
            }
            arrow_type::ArrowTypeEnum::RunEndEncoded(run_end_encoded) => {
                let run_ends_field: Field = run_end_encoded
                    .as_ref()
                    .run_ends_field
                    .as_deref()
                    .required("run_ends_field")?;
                let value_field: Field = run_end_encoded
                    .as_ref()
                    .values_field
                    .as_deref()
                    .required("values_field")?;
                DataType::RunEndEncoded(run_ends_field.into(), value_field.into())
            }
        })
    }
}

impl TryFrom<&protobuf::Field> for Field {
    type Error = Error;
    fn try_from(field: &protobuf::Field) -> Result<Self, Self::Error> {
        let datatype = field.arrow_type.as_deref().required("arrow_type")?;
        let field = Self::new(field.name.as_str(), datatype, field.nullable)
            .with_metadata(field.metadata.clone());
        Ok(field)
    }
}

impl TryFrom<&protobuf::Schema> for Schema {
    type Error = Error;

    fn try_from(schema: &protobuf::Schema) -> Result<Self, Self::Error> {
        let fields = schema
            .columns
            .iter()
            .map(Field::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self::new_with_metadata(fields, schema.metadata.clone()))
    }
}

impl From<protobuf::TimeUnit> for TimeUnit {
    fn from(time_unit: protobuf::TimeUnit) -> Self {
        match time_unit {
            protobuf::TimeUnit::Second => TimeUnit::Second,
            protobuf::TimeUnit::Millisecond => TimeUnit::Millisecond,
            protobuf::TimeUnit::Microsecond => TimeUnit::Microsecond,
            protobuf::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
        }
    }
}

impl From<protobuf::IntervalUnit> for IntervalUnit {
    fn from(interval_unit: protobuf::IntervalUnit) -> Self {
        match interval_unit {
            protobuf::IntervalUnit::YearMonth => IntervalUnit::YearMonth,
            protobuf::IntervalUnit::DayTime => IntervalUnit::DayTime,
            protobuf::IntervalUnit::MonthDayNano => IntervalUnit::MonthDayNano,
        }
    }
}

pub fn parse_i32_to_time_unit(value: &i32) -> Result<TimeUnit, Error> {
    protobuf::TimeUnit::try_from(*value)
        .map(|t| t.into())
        .map_err(|_| Error::unknown("TimeUnit", *value))
}

pub fn parse_i32_to_interval_unit(value: &i32) -> Result<IntervalUnit, Error> {
    protobuf::IntervalUnit::try_from(*value)
        .map(|i| i.into())
        .map_err(|_| Error::unknown("IntervalUnit", *value))
}

pub fn parse_proto_fields_to_fields<'a, I>(
    fields: I,
) -> std::result::Result<Vec<Field>, Error>
where
    I: IntoIterator<Item = &'a protobuf::Field>,
{
    fields
        .into_iter()
        .map(Field::try_from)
        .collect::<Result<_, _>>()
}
