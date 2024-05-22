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

use crate::protobuf_common::arrow_type::ArrowTypeEnum;
use crate::protobuf_common::EmptyMessage;
use crate::{protobuf_common as protobuf, protobuf_common};
use arrow::datatypes::{
    DataType, Field, IntervalUnit, Schema, SchemaRef, TimeUnit, UnionMode,
};
use datafusion_common::{
    plan_datafusion_err, Column, DFSchema, DFSchemaRef, DataFusionError, ScalarValue,
};
use std::sync::Arc;

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
                write!(f, "{data_type:?} is invalid as a DataFusion scalar type")
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
            dict_id: field.dict_id().unwrap_or(0),
            dict_ordered: field.dict_is_ordered().unwrap_or(false),
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
            DataType::FixedSizeBinary(size) => Self::FixedSizeBinary(*size),
            DataType::LargeBinary => Self::LargeBinary(EmptyMessage {}),
            DataType::Utf8 => Self::Utf8(EmptyMessage {}),
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
            DataType::Struct(struct_fields) => Self::Struct(protobuf::Struct {
                sub_field_types: convert_arc_fields_to_proto_fields(struct_fields)?,
            }),
            DataType::Union(fields, union_mode) => {
                let union_mode = match union_mode {
                    UnionMode::Sparse => protobuf::UnionMode::Sparse,
                    UnionMode::Dense => protobuf::UnionMode::Dense,
                };
                Self::Union(protobuf::Union {
                    union_types: convert_arc_fields_to_proto_fields(fields.iter().map(|(_, item)|item))?,
                    union_mode: union_mode.into(),
                    type_ids: fields.iter().map(|(x, _)| x as i32).collect(),
                })
            }
            DataType::Dictionary(key_type, value_type) => {
                Self::Dictionary(Box::new(protobuf::Dictionary {
                    key: Some(Box::new(key_type.as_ref().try_into()?)),
                    value: Some(Box::new(value_type.as_ref().try_into()?)),
                }))
            }
            DataType::Decimal128(precision, scale) => Self::Decimal(protobuf::Decimal {
                precision: *precision as u32,
                scale: *scale as i32,
            }),
            DataType::Decimal256(_, _) => {
                return Err(Error::General("Proto serialization error: The Decimal256 data type is not yet supported".to_owned()))
            }
            DataType::Map(field, sorted) => {
                Self::Map(Box::new(
                    protobuf::Map {
                        field_type: Some(Box::new(field.as_ref().try_into()?)),
                        keys_sorted: *sorted,
                    }
                ))
            }
            DataType::RunEndEncoded(_, _) => {
                return Err(Error::General(
                    "Proto serialization error: The RunEndEncoded data type is not yet supported".to_owned()
                ))
            }
            DataType::Utf8View | DataType::BinaryView | DataType::ListView(_) | DataType::LargeListView(_) => {
                return Err(Error::General(format!("Proto serialization error: {val} not yet supported")))
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

impl TryFrom<&DFSchema> for protobuf::DfSchema {
    type Error = Error;

    fn try_from(s: &DFSchema) -> Result<Self, Self::Error> {
        let columns = s
            .iter()
            .map(|(qualifier, field)| {
                Ok(protobuf::DfField {
                    field: Some(field.as_ref().try_into()?),
                    qualifier: qualifier.map(|r| protobuf_common::ColumnRelation {
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

impl TryFrom<&DFSchemaRef> for protobuf::DfSchema {
    type Error = Error;

    fn try_from(s: &DFSchemaRef) -> Result<Self, Self::Error> {
        s.as_ref().try_into()
    }
}

impl From<&TimeUnit> for protobuf::TimeUnit {
    fn from(val: &TimeUnit) -> Self {
        match val {
            TimeUnit::Second => protobuf::TimeUnit::Second,
            TimeUnit::Millisecond => protobuf::TimeUnit::Millisecond,
            TimeUnit::Microsecond => protobuf::TimeUnit::Microsecond,
            TimeUnit::Nanosecond => protobuf::TimeUnit::Nanosecond,
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

/// Converts a vector of `Arc<arrow::Field>`s to `protobuf::Field`s
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

impl From<Error> for DataFusionError {
    fn from(e: Error) -> Self {
        plan_datafusion_err!("{}", e)
    }
}
