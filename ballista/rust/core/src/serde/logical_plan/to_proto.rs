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

//! Serde code to convert Arrow schemas and DataFusion logical plans to Ballista protocol
//! buffer format, allowing DataFusion logical plans to be serialized and transmitted between
//! processes.

use super::super::proto_error;
use crate::serde::{byte_to_string, protobuf, BallistaError};
use datafusion::arrow::datatypes::{
    DataType, Field, IntervalUnit, Schema, SchemaRef, TimeUnit,
};
use datafusion::datasource::file_format::avro::AvroFormat;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::TableProvider;

use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingTable;
use datafusion::logical_plan::plan::{
    Aggregate, EmptyRelation, Filter, Join, Projection, Sort, Window,
};
use datafusion::logical_plan::{
    exprlist_to_fields,
    window_frames::{WindowFrame, WindowFrameBound, WindowFrameUnits},
    Column, CreateExternalTable, CrossJoin, Expr, JoinConstraint, JoinType, Limit,
    LogicalPlan, Repartition, TableScan, Values,
};
use datafusion::physical_plan::aggregates::AggregateFunction;
use datafusion::physical_plan::functions::BuiltinScalarFunction;
use datafusion::physical_plan::window_functions::{
    BuiltInWindowFunction, WindowFunction,
};
use datafusion::physical_plan::{ColumnStatistics, Statistics};
use protobuf::listing_table_scan_node::FileFormatType;
use protobuf::{
    arrow_type, logical_expr_node::ExprType, scalar_type, DateUnit, PrimitiveScalarType,
    ScalarListValue, ScalarType,
};
use std::{
    boxed,
    convert::{TryFrom, TryInto},
};

impl protobuf::IntervalUnit {
    pub fn from_arrow_interval_unit(interval_unit: &IntervalUnit) -> Self {
        match interval_unit {
            IntervalUnit::YearMonth => protobuf::IntervalUnit::YearMonth,
            IntervalUnit::DayTime => protobuf::IntervalUnit::DayTime,
        }
    }

    pub fn from_i32_to_arrow(
        interval_unit_i32: i32,
    ) -> Result<IntervalUnit, BallistaError> {
        let pb_interval_unit = protobuf::IntervalUnit::from_i32(interval_unit_i32);
        match pb_interval_unit {
            Some(interval_unit) => Ok(match interval_unit {
                protobuf::IntervalUnit::YearMonth => IntervalUnit::YearMonth,
                protobuf::IntervalUnit::DayTime => IntervalUnit::DayTime,
            }),
            None => Err(proto_error(
                "Error converting i32 to DateUnit: Passed invalid variant",
            )),
        }
    }
}
/* Arrow changed dates to no longer have date unit

impl protobuf::DateUnit {
    pub fn from_arrow_date_unit(val: &DateUnit) -> Self {
        match val {
            DateUnit::Day => protobuf::DateUnit::Day,
            DateUnit::Millisecond => protobuf::DateUnit::DateMillisecond,
        }
    }
    pub fn from_i32_to_arrow(date_unit_i32: i32) -> Result<DateUnit, BallistaError> {
        let pb_date_unit = protobuf::DateUnit::from_i32(date_unit_i32);
        use datafusion::DateUnit;
        match pb_date_unit {
            Some(date_unit) => Ok(match date_unit {
                protobuf::DateUnit::Day => DateUnit::Day,
                protobuf::DateUnit::DateMillisecond => DateUnit::Millisecond,
            }),
            None => Err(proto_error("Error converting i32 to DateUnit: Passed invalid variant")),
        }
    }

}*/

impl protobuf::TimeUnit {
    pub fn from_arrow_time_unit(val: &TimeUnit) -> Self {
        match val {
            TimeUnit::Second => protobuf::TimeUnit::Second,
            TimeUnit::Millisecond => protobuf::TimeUnit::TimeMillisecond,
            TimeUnit::Microsecond => protobuf::TimeUnit::Microsecond,
            TimeUnit::Nanosecond => protobuf::TimeUnit::Nanosecond,
        }
    }
    pub fn from_i32_to_arrow(time_unit_i32: i32) -> Result<TimeUnit, BallistaError> {
        let pb_time_unit = protobuf::TimeUnit::from_i32(time_unit_i32);
        match pb_time_unit {
            Some(time_unit) => Ok(match time_unit {
                protobuf::TimeUnit::Second => TimeUnit::Second,
                protobuf::TimeUnit::TimeMillisecond => TimeUnit::Millisecond,
                protobuf::TimeUnit::Microsecond => TimeUnit::Microsecond,
                protobuf::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
            }),
            None => Err(proto_error(
                "Error converting i32 to TimeUnit: Passed invalid variant",
            )),
        }
    }
}

impl From<&Field> for protobuf::Field {
    fn from(field: &Field) -> Self {
        protobuf::Field {
            name: field.name().to_owned(),
            arrow_type: Some(Box::new(field.data_type().into())),
            nullable: field.is_nullable(),
            children: Vec::new(),
        }
    }
}

impl From<&DataType> for protobuf::ArrowType {
    fn from(val: &DataType) -> protobuf::ArrowType {
        protobuf::ArrowType {
            arrow_type_enum: Some(val.into()),
        }
    }
}

impl TryInto<DataType> for &protobuf::ArrowType {
    type Error = BallistaError;
    fn try_into(self) -> Result<DataType, Self::Error> {
        let pb_arrow_type = self.arrow_type_enum.as_ref().ok_or_else(|| {
            proto_error(
                "Protobuf deserialization error: ArrowType missing required field 'data_type'",
            )
        })?;
        Ok(match pb_arrow_type {
            protobuf::arrow_type::ArrowTypeEnum::None(_) => DataType::Null,
            protobuf::arrow_type::ArrowTypeEnum::Bool(_) => DataType::Boolean,
            protobuf::arrow_type::ArrowTypeEnum::Uint8(_) => DataType::UInt8,
            protobuf::arrow_type::ArrowTypeEnum::Int8(_) => DataType::Int8,
            protobuf::arrow_type::ArrowTypeEnum::Uint16(_) => DataType::UInt16,
            protobuf::arrow_type::ArrowTypeEnum::Int16(_) => DataType::Int16,
            protobuf::arrow_type::ArrowTypeEnum::Uint32(_) => DataType::UInt32,
            protobuf::arrow_type::ArrowTypeEnum::Int32(_) => DataType::Int32,
            protobuf::arrow_type::ArrowTypeEnum::Uint64(_) => DataType::UInt64,
            protobuf::arrow_type::ArrowTypeEnum::Int64(_) => DataType::Int64,
            protobuf::arrow_type::ArrowTypeEnum::Float16(_) => DataType::Float16,
            protobuf::arrow_type::ArrowTypeEnum::Float32(_) => DataType::Float32,
            protobuf::arrow_type::ArrowTypeEnum::Float64(_) => DataType::Float64,
            protobuf::arrow_type::ArrowTypeEnum::Utf8(_) => DataType::Utf8,
            protobuf::arrow_type::ArrowTypeEnum::LargeUtf8(_) => DataType::LargeUtf8,
            protobuf::arrow_type::ArrowTypeEnum::Binary(_) => DataType::Binary,
            protobuf::arrow_type::ArrowTypeEnum::FixedSizeBinary(size) => {
                DataType::FixedSizeBinary(*size)
            }
            protobuf::arrow_type::ArrowTypeEnum::LargeBinary(_) => DataType::LargeBinary,
            protobuf::arrow_type::ArrowTypeEnum::Date32(_) => DataType::Date32,
            protobuf::arrow_type::ArrowTypeEnum::Date64(_) => DataType::Date64,
            protobuf::arrow_type::ArrowTypeEnum::Duration(time_unit_i32) => {
                DataType::Duration(protobuf::TimeUnit::from_i32_to_arrow(*time_unit_i32)?)
            }
            protobuf::arrow_type::ArrowTypeEnum::Timestamp(timestamp) => {
                DataType::Timestamp(
                    protobuf::TimeUnit::from_i32_to_arrow(timestamp.time_unit)?,
                    match timestamp.timezone.is_empty() {
                        true => None,
                        false => Some(timestamp.timezone.to_owned()),
                    },
                )
            }
            protobuf::arrow_type::ArrowTypeEnum::Time32(time_unit_i32) => {
                DataType::Time32(protobuf::TimeUnit::from_i32_to_arrow(*time_unit_i32)?)
            }
            protobuf::arrow_type::ArrowTypeEnum::Time64(time_unit_i32) => {
                DataType::Time64(protobuf::TimeUnit::from_i32_to_arrow(*time_unit_i32)?)
            }
            protobuf::arrow_type::ArrowTypeEnum::Interval(interval_unit_i32) => {
                DataType::Interval(protobuf::IntervalUnit::from_i32_to_arrow(
                    *interval_unit_i32,
                )?)
            }
            protobuf::arrow_type::ArrowTypeEnum::Decimal(protobuf::Decimal {
                whole,
                fractional,
            }) => DataType::Decimal(*whole as usize, *fractional as usize),
            protobuf::arrow_type::ArrowTypeEnum::List(boxed_list) => {
                let field_ref = boxed_list
                    .field_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: List message was missing required field 'field_type'"))?
                    .as_ref();
                DataType::List(Box::new(field_ref.try_into()?))
            }
            protobuf::arrow_type::ArrowTypeEnum::LargeList(boxed_list) => {
                let field_ref = boxed_list
                    .field_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: List message was missing required field 'field_type'"))?
                    .as_ref();
                DataType::LargeList(Box::new(field_ref.try_into()?))
            }
            protobuf::arrow_type::ArrowTypeEnum::FixedSizeList(boxed_list) => {
                let fsl_ref = boxed_list.as_ref();
                let pb_fieldtype = fsl_ref
                    .field_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: FixedSizeList message was missing required field 'field_type'"))?;
                DataType::FixedSizeList(
                    Box::new(pb_fieldtype.as_ref().try_into()?),
                    fsl_ref.list_size,
                )
            }
            protobuf::arrow_type::ArrowTypeEnum::Struct(struct_type) => {
                let fields = struct_type
                    .sub_field_types
                    .iter()
                    .map(|field| field.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                DataType::Struct(fields)
            }
            protobuf::arrow_type::ArrowTypeEnum::Union(union) => {
                let union_types = union
                    .union_types
                    .iter()
                    .map(|field| field.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                DataType::Union(union_types)
            }
            protobuf::arrow_type::ArrowTypeEnum::Dictionary(boxed_dict) => {
                let dict_ref = boxed_dict.as_ref();
                let pb_key = dict_ref
                    .key
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: Dictionary message was missing required field 'key'"))?;
                let pb_value = dict_ref
                    .value
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: Dictionary message was missing required field 'value'"))?;
                DataType::Dictionary(
                    Box::new(pb_key.as_ref().try_into()?),
                    Box::new(pb_value.as_ref().try_into()?),
                )
            }
        })
    }
}

impl TryInto<DataType> for &Box<protobuf::List> {
    type Error = BallistaError;
    fn try_into(self) -> Result<DataType, Self::Error> {
        let list_ref = self.as_ref();
        match &list_ref.field_type {
            Some(pb_field) => {
                let pb_field_ref = pb_field.as_ref();
                let arrow_field: Field = pb_field_ref.try_into()?;
                Ok(DataType::List(Box::new(arrow_field)))
            }
            None => Err(proto_error(
                "List message missing required field 'field_type'",
            )),
        }
    }
}

impl From<&DataType> for protobuf::arrow_type::ArrowTypeEnum {
    fn from(val: &DataType) -> protobuf::arrow_type::ArrowTypeEnum {
        use protobuf::arrow_type::ArrowTypeEnum;
        use protobuf::ArrowType;
        use protobuf::EmptyMessage;
        match val {
            DataType::Null => ArrowTypeEnum::None(EmptyMessage {}),
            DataType::Boolean => ArrowTypeEnum::Bool(EmptyMessage {}),
            DataType::Int8 => ArrowTypeEnum::Int8(EmptyMessage {}),
            DataType::Int16 => ArrowTypeEnum::Int16(EmptyMessage {}),
            DataType::Int32 => ArrowTypeEnum::Int32(EmptyMessage {}),
            DataType::Int64 => ArrowTypeEnum::Int64(EmptyMessage {}),
            DataType::UInt8 => ArrowTypeEnum::Uint8(EmptyMessage {}),
            DataType::UInt16 => ArrowTypeEnum::Uint16(EmptyMessage {}),
            DataType::UInt32 => ArrowTypeEnum::Uint32(EmptyMessage {}),
            DataType::UInt64 => ArrowTypeEnum::Uint64(EmptyMessage {}),
            DataType::Float16 => ArrowTypeEnum::Float16(EmptyMessage {}),
            DataType::Float32 => ArrowTypeEnum::Float32(EmptyMessage {}),
            DataType::Float64 => ArrowTypeEnum::Float64(EmptyMessage {}),
            DataType::Timestamp(time_unit, timezone) => {
                ArrowTypeEnum::Timestamp(protobuf::Timestamp {
                    time_unit: protobuf::TimeUnit::from_arrow_time_unit(time_unit) as i32,
                    timezone: timezone.to_owned().unwrap_or_else(String::new),
                })
            }
            DataType::Date32 => ArrowTypeEnum::Date32(EmptyMessage {}),
            DataType::Date64 => ArrowTypeEnum::Date64(EmptyMessage {}),
            DataType::Time32(time_unit) => ArrowTypeEnum::Time32(
                protobuf::TimeUnit::from_arrow_time_unit(time_unit) as i32,
            ),
            DataType::Time64(time_unit) => ArrowTypeEnum::Time64(
                protobuf::TimeUnit::from_arrow_time_unit(time_unit) as i32,
            ),
            DataType::Duration(time_unit) => ArrowTypeEnum::Duration(
                protobuf::TimeUnit::from_arrow_time_unit(time_unit) as i32,
            ),
            DataType::Interval(interval_unit) => ArrowTypeEnum::Interval(
                protobuf::IntervalUnit::from_arrow_interval_unit(interval_unit) as i32,
            ),
            DataType::Binary => ArrowTypeEnum::Binary(EmptyMessage {}),
            DataType::FixedSizeBinary(size) => ArrowTypeEnum::FixedSizeBinary(*size),
            DataType::LargeBinary => ArrowTypeEnum::LargeBinary(EmptyMessage {}),
            DataType::Utf8 => ArrowTypeEnum::Utf8(EmptyMessage {}),
            DataType::LargeUtf8 => ArrowTypeEnum::LargeUtf8(EmptyMessage {}),
            DataType::List(item_type) => ArrowTypeEnum::List(Box::new(protobuf::List {
                field_type: Some(Box::new(item_type.as_ref().into())),
            })),
            DataType::FixedSizeList(item_type, size) => {
                ArrowTypeEnum::FixedSizeList(Box::new(protobuf::FixedSizeList {
                    field_type: Some(Box::new(item_type.as_ref().into())),
                    list_size: *size,
                }))
            }
            DataType::LargeList(item_type) => {
                ArrowTypeEnum::LargeList(Box::new(protobuf::List {
                    field_type: Some(Box::new(item_type.as_ref().into())),
                }))
            }
            DataType::Struct(struct_fields) => ArrowTypeEnum::Struct(protobuf::Struct {
                sub_field_types: struct_fields
                    .iter()
                    .map(|field| field.into())
                    .collect::<Vec<_>>(),
            }),
            DataType::Union(union_types) => ArrowTypeEnum::Union(protobuf::Union {
                union_types: union_types
                    .iter()
                    .map(|field| field.into())
                    .collect::<Vec<_>>(),
            }),
            DataType::Dictionary(key_type, value_type) => {
                ArrowTypeEnum::Dictionary(Box::new(protobuf::Dictionary {
                    key: Some(Box::new(key_type.as_ref().into())),
                    value: Some(Box::new(value_type.as_ref().into())),
                }))
            }
            DataType::Decimal(whole, fractional) => {
                ArrowTypeEnum::Decimal(protobuf::Decimal {
                    whole: *whole as u64,
                    fractional: *fractional as u64,
                })
            }
            DataType::Map(_, _) => {
                unimplemented!("Ballista does not yet support Map data type")
            }
        }
    }
}

//Does not check if list subtypes are valid
fn is_valid_scalar_type_no_list_check(datatype: &DataType) -> bool {
    match datatype {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::LargeUtf8
        | DataType::Utf8
        | DataType::Date32 => true,
        DataType::Time64(time_unit) => {
            matches!(time_unit, TimeUnit::Microsecond | TimeUnit::Nanosecond)
        }

        DataType::List(_) => true,
        _ => false,
    }
}

impl TryFrom<&DataType> for protobuf::scalar_type::Datatype {
    type Error = BallistaError;
    fn try_from(val: &DataType) -> Result<Self, Self::Error> {
        use protobuf::{List, PrimitiveScalarType};
        let scalar_value = match val {
            DataType::Boolean => scalar_type::Datatype::Scalar(PrimitiveScalarType::Bool as i32),
            DataType::Int8 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Int8 as i32),
            DataType::Int16 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Int16 as i32),
            DataType::Int32 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Int32 as i32),
            DataType::Int64 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Int64 as i32),
            DataType::UInt8 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Uint8 as i32),
            DataType::UInt16 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Uint16 as i32),
            DataType::UInt32 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Uint32 as i32),
            DataType::UInt64 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Uint64 as i32),
            DataType::Float32 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Float32 as i32),
            DataType::Float64 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Float64 as i32),
            DataType::Date32 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Date32 as i32),
            DataType::Time64(time_unit) => match time_unit {
                TimeUnit::Microsecond => scalar_type::Datatype::Scalar(PrimitiveScalarType::TimeMicrosecond as i32),
                TimeUnit::Nanosecond => scalar_type::Datatype::Scalar(PrimitiveScalarType::TimeNanosecond as i32),
                _ => {
                    return Err(proto_error(format!(
                        "Found invalid time unit for scalar value, only TimeUnit::Microsecond and TimeUnit::Nanosecond are valid time units: {:?}",
                        time_unit
                    )))
                }
            },
            DataType::Utf8 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Utf8 as i32),
            DataType::LargeUtf8 => scalar_type::Datatype::Scalar(PrimitiveScalarType::LargeUtf8 as i32),
            DataType::List(field_type) => {
                let mut field_names: Vec<String> = Vec::new();
                let mut curr_field = field_type.as_ref();
                field_names.push(curr_field.name().to_owned());
                //For each nested field check nested datatype, since datafusion scalars only support recursive lists with a leaf scalar type
                // any other compound types are errors.

                while let DataType::List(nested_field_type) = curr_field.data_type() {
                    curr_field = nested_field_type.as_ref();
                    field_names.push(curr_field.name().to_owned());
                    if !is_valid_scalar_type_no_list_check(curr_field.data_type()) {
                        return Err(proto_error(format!("{:?} is an invalid scalar type", curr_field)));
                    }
                }
                let deepest_datatype = curr_field.data_type();
                if !is_valid_scalar_type_no_list_check(deepest_datatype) {
                    return Err(proto_error(format!("The list nested type {:?} is an invalid scalar type", curr_field)));
                }
                let pb_deepest_type: PrimitiveScalarType = match deepest_datatype {
                    DataType::Boolean => PrimitiveScalarType::Bool,
                    DataType::Int8 => PrimitiveScalarType::Int8,
                    DataType::Int16 => PrimitiveScalarType::Int16,
                    DataType::Int32 => PrimitiveScalarType::Int32,
                    DataType::Int64 => PrimitiveScalarType::Int64,
                    DataType::UInt8 => PrimitiveScalarType::Uint8,
                    DataType::UInt16 => PrimitiveScalarType::Uint16,
                    DataType::UInt32 => PrimitiveScalarType::Uint32,
                    DataType::UInt64 => PrimitiveScalarType::Uint64,
                    DataType::Float32 => PrimitiveScalarType::Float32,
                    DataType::Float64 => PrimitiveScalarType::Float64,
                    DataType::Date32 => PrimitiveScalarType::Date32,
                    DataType::Time64(time_unit) => match time_unit {
                        TimeUnit::Microsecond => PrimitiveScalarType::TimeMicrosecond,
                        TimeUnit::Nanosecond => PrimitiveScalarType::TimeNanosecond,
                        _ => {
                            return Err(proto_error(format!(
                                "Found invalid time unit for scalar value, only TimeUnit::Microsecond and TimeUnit::Nanosecond are valid time units: {:?}",
                                time_unit
                            )))
                        }
                    },

                    DataType::Utf8 => PrimitiveScalarType::Utf8,
                    DataType::LargeUtf8 => PrimitiveScalarType::LargeUtf8,
                    _ => {
                        return Err(proto_error(format!(
                            "Error converting to Datatype to scalar type, {:?} is invalid as a datafusion scalar.",
                            val
                        )))
                    }
                };
                protobuf::scalar_type::Datatype::List(protobuf::ScalarListType {
                    field_names,
                    deepest_type: pb_deepest_type as i32,
                })
            }
            DataType::Null
            | DataType::Float16
            | DataType::Timestamp(_, _)
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::Binary
            | DataType::FixedSizeBinary(_)
            | DataType::LargeBinary
            | DataType::FixedSizeList(_, _)
            | DataType::LargeList(_)
            | DataType::Struct(_)
            | DataType::Union(_)
            | DataType::Dictionary(_, _)
            | DataType::Map(_, _)
            | DataType::Decimal(_, _) => {
                return Err(proto_error(format!(
                    "Error converting to Datatype to scalar type, {:?} is invalid as a datafusion scalar.",
                    val
                )))
            }
        };
        Ok(scalar_value)
    }
}

impl TryFrom<&datafusion::scalar::ScalarValue> for protobuf::ScalarValue {
    type Error = BallistaError;
    fn try_from(
        val: &datafusion::scalar::ScalarValue,
    ) -> Result<protobuf::ScalarValue, Self::Error> {
        use datafusion::scalar;
        use protobuf::scalar_value::Value;
        use protobuf::PrimitiveScalarType;
        let scalar_val = match val {
            scalar::ScalarValue::Boolean(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Bool, |s| Value::BoolValue(*s))
            }
            scalar::ScalarValue::Float32(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Float32, |s| {
                    Value::Float32Value(*s)
                })
            }
            scalar::ScalarValue::Float64(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Float64, |s| {
                    Value::Float64Value(*s)
                })
            }
            scalar::ScalarValue::Int8(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Int8, |s| {
                    Value::Int8Value(*s as i32)
                })
            }
            scalar::ScalarValue::Int16(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Int16, |s| {
                    Value::Int16Value(*s as i32)
                })
            }
            scalar::ScalarValue::Int32(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Int32, |s| Value::Int32Value(*s))
            }
            scalar::ScalarValue::Int64(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Int64, |s| Value::Int64Value(*s))
            }
            scalar::ScalarValue::UInt8(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Uint8, |s| {
                    Value::Uint8Value(*s as u32)
                })
            }
            scalar::ScalarValue::UInt16(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Uint16, |s| {
                    Value::Uint16Value(*s as u32)
                })
            }
            scalar::ScalarValue::UInt32(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Uint32, |s| Value::Uint32Value(*s))
            }
            scalar::ScalarValue::UInt64(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Uint64, |s| Value::Uint64Value(*s))
            }
            scalar::ScalarValue::Utf8(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Utf8, |s| {
                    Value::Utf8Value(s.to_owned())
                })
            }
            scalar::ScalarValue::LargeUtf8(val) => {
                create_proto_scalar(val, PrimitiveScalarType::LargeUtf8, |s| {
                    Value::LargeUtf8Value(s.to_owned())
                })
            }
            scalar::ScalarValue::List(value, datatype) => {
                println!("Current datatype of list: {:?}", datatype);
                match value {
                    Some(values) => {
                        if values.is_empty() {
                            protobuf::ScalarValue {
                                value: Some(protobuf::scalar_value::Value::ListValue(
                                    protobuf::ScalarListValue {
                                        datatype: Some(datatype.as_ref().try_into()?),
                                        values: Vec::new(),
                                    },
                                )),
                            }
                        } else {
                            let scalar_type = match datatype.as_ref() {
                                DataType::List(field) => field.as_ref().data_type(),
                                _ => todo!("Proper error handling"),
                            };
                            println!("Current scalar type for list: {:?}", scalar_type);
                            let type_checked_values: Vec<protobuf::ScalarValue> = values
                                .iter()
                                .map(|scalar| match (scalar, scalar_type) {
                                    (scalar::ScalarValue::List(_, list_type), DataType::List(field)) => {
                                        if let DataType::List(list_field) = list_type.as_ref() {
                                            let scalar_datatype = field.data_type();
                                            let list_datatype = list_field.data_type();
                                            if std::mem::discriminant(list_datatype) != std::mem::discriminant(scalar_datatype) {
                                                return Err(proto_error(format!(
                                                    "Protobuf serialization error: Lists with inconsistent typing {:?} and {:?} found within list",
                                                    list_datatype, scalar_datatype
                                                )));
                                            }
                                            scalar.try_into()
                                        } else {
                                            Err(proto_error(format!(
                                                "Protobuf serialization error, {:?} was inconsistent with designated type {:?}",
                                                scalar, datatype
                                            )))
                                        }
                                    }
                                    (scalar::ScalarValue::Boolean(_), DataType::Boolean) => scalar.try_into(),
                                    (scalar::ScalarValue::Float32(_), DataType::Float32) => scalar.try_into(),
                                    (scalar::ScalarValue::Float64(_), DataType::Float64) => scalar.try_into(),
                                    (scalar::ScalarValue::Int8(_), DataType::Int8) => scalar.try_into(),
                                    (scalar::ScalarValue::Int16(_), DataType::Int16) => scalar.try_into(),
                                    (scalar::ScalarValue::Int32(_), DataType::Int32) => scalar.try_into(),
                                    (scalar::ScalarValue::Int64(_), DataType::Int64) => scalar.try_into(),
                                    (scalar::ScalarValue::UInt8(_), DataType::UInt8) => scalar.try_into(),
                                    (scalar::ScalarValue::UInt16(_), DataType::UInt16) => scalar.try_into(),
                                    (scalar::ScalarValue::UInt32(_), DataType::UInt32) => scalar.try_into(),
                                    (scalar::ScalarValue::UInt64(_), DataType::UInt64) => scalar.try_into(),
                                    (scalar::ScalarValue::Utf8(_), DataType::Utf8) => scalar.try_into(),
                                    (scalar::ScalarValue::LargeUtf8(_), DataType::LargeUtf8) => scalar.try_into(),
                                    _ => Err(proto_error(format!(
                                        "Protobuf serialization error, {:?} was inconsistent with designated type {:?}",
                                        scalar, datatype
                                    ))),
                                })
                                .collect::<Result<Vec<_>, _>>()?;
                            protobuf::ScalarValue {
                                value: Some(protobuf::scalar_value::Value::ListValue(
                                    protobuf::ScalarListValue {
                                        datatype: Some(datatype.as_ref().try_into()?),
                                        values: type_checked_values,
                                    },
                                )),
                            }
                        }
                    }
                    None => protobuf::ScalarValue {
                        value: Some(protobuf::scalar_value::Value::NullListValue(
                            datatype.as_ref().try_into()?,
                        )),
                    },
                }
            }
            datafusion::scalar::ScalarValue::Date32(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Date32, |s| Value::Date32Value(*s))
            }
            datafusion::scalar::ScalarValue::TimestampMicrosecond(val, _) => {
                create_proto_scalar(val, PrimitiveScalarType::TimeMicrosecond, |s| {
                    Value::TimeMicrosecondValue(*s)
                })
            }
            datafusion::scalar::ScalarValue::TimestampNanosecond(val, _) => {
                create_proto_scalar(val, PrimitiveScalarType::TimeNanosecond, |s| {
                    Value::TimeNanosecondValue(*s)
                })
            }
            _ => {
                return Err(proto_error(format!(
                    "Error converting to Datatype to scalar type, {:?} is invalid as a datafusion scalar.",
                    val
                )))
            }
        };
        Ok(scalar_val)
    }
}

impl TryInto<protobuf::LogicalPlanNode> for &LogicalPlan {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::LogicalPlanNode, Self::Error> {
        use protobuf::logical_plan_node::LogicalPlanType;
        match self {
            LogicalPlan::Values(Values { values, .. }) => {
                let n_cols = if values.is_empty() {
                    0
                } else {
                    values[0].len()
                } as u64;
                let values_list = values
                    .iter()
                    .flatten()
                    .map(|v| v.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Values(
                        protobuf::ValuesNode {
                            n_cols,
                            values_list,
                        },
                    )),
                })
            }
            LogicalPlan::TableScan(TableScan {
                table_name,
                source,
                filters,
                projection,
                ..
            }) => {
                let schema = source.schema();
                let source = source.as_any();

                let projection = match projection {
                    None => None,
                    Some(columns) => {
                        let column_names = columns
                            .iter()
                            .map(|i| schema.field(*i).name().to_owned())
                            .collect();
                        Some(protobuf::ProjectionColumns {
                            columns: column_names,
                        })
                    }
                };
                let schema: protobuf::Schema = schema.as_ref().into();

                let filters: Vec<protobuf::LogicalExprNode> = filters
                    .iter()
                    .map(|filter| filter.try_into())
                    .collect::<Result<Vec<_>, _>>()?;

                if let Some(listing_table) = source.downcast_ref::<ListingTable>() {
                    let any = listing_table.options().format.as_any();
                    let file_format_type = if let Some(parquet) =
                        any.downcast_ref::<ParquetFormat>()
                    {
                        FileFormatType::Parquet(protobuf::ParquetFormat {
                            enable_pruning: parquet.enable_pruning(),
                        })
                    } else if let Some(csv) = any.downcast_ref::<CsvFormat>() {
                        FileFormatType::Csv(protobuf::CsvFormat {
                            delimiter: byte_to_string(csv.delimiter())?,
                            has_header: csv.has_header(),
                        })
                    } else if any.is::<AvroFormat>() {
                        FileFormatType::Avro(protobuf::AvroFormat {})
                    } else {
                        return Err(proto_error(format!(
                            "Error converting file format, {:?} is invalid as a datafusion foramt.",
                            listing_table.options().format
                        )));
                    };
                    Ok(protobuf::LogicalPlanNode {
                        logical_plan_type: Some(LogicalPlanType::ListingScan(
                            protobuf::ListingTableScanNode {
                                file_format_type: Some(file_format_type),
                                table_name: table_name.to_owned(),
                                collect_stat: listing_table.options().collect_stat,
                                file_extension: listing_table
                                    .options()
                                    .file_extension
                                    .clone(),
                                table_partition_cols: listing_table
                                    .options()
                                    .table_partition_cols
                                    .clone(),
                                path: listing_table.table_path().to_owned(),
                                schema: Some(schema),
                                projection,
                                filters,
                                target_partitions: listing_table
                                    .options()
                                    .target_partitions
                                    as u32,
                            },
                        )),
                    })
                } else {
                    Err(BallistaError::General(format!(
                        "logical plan to_proto unsupported table provider {:?}",
                        source
                    )))
                }
            }
            LogicalPlan::Projection(Projection {
                expr, input, alias, ..
            }) => Ok(protobuf::LogicalPlanNode {
                logical_plan_type: Some(LogicalPlanType::Projection(Box::new(
                    protobuf::ProjectionNode {
                        input: Some(Box::new(input.as_ref().try_into()?)),
                        expr: expr.iter().map(|expr| expr.try_into()).collect::<Result<
                            Vec<_>,
                            BallistaError,
                        >>(
                        )?,
                        optional_alias: alias
                            .clone()
                            .map(protobuf::projection_node::OptionalAlias::Alias),
                    },
                ))),
            }),
            LogicalPlan::Filter(Filter { predicate, input }) => {
                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Selection(Box::new(
                        protobuf::SelectionNode {
                            input: Some(Box::new(input)),
                            expr: Some(predicate.try_into()?),
                        },
                    ))),
                })
            }
            LogicalPlan::Window(Window {
                input, window_expr, ..
            }) => {
                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Window(Box::new(
                        protobuf::WindowNode {
                            input: Some(Box::new(input)),
                            window_expr: window_expr
                                .iter()
                                .map(|expr| expr.try_into())
                                .collect::<Result<Vec<_>, _>>()?,
                        },
                    ))),
                })
            }
            LogicalPlan::Aggregate(Aggregate {
                group_expr,
                aggr_expr,
                input,
                ..
            }) => {
                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Aggregate(Box::new(
                        protobuf::AggregateNode {
                            input: Some(Box::new(input)),
                            group_expr: group_expr
                                .iter()
                                .map(|expr| expr.try_into())
                                .collect::<Result<Vec<_>, _>>()?,
                            aggr_expr: aggr_expr
                                .iter()
                                .map(|expr| expr.try_into())
                                .collect::<Result<Vec<_>, _>>()?,
                        },
                    ))),
                })
            }
            LogicalPlan::Join(Join {
                left,
                right,
                on,
                join_type,
                join_constraint,
                null_equals_null,
                ..
            }) => {
                let left: protobuf::LogicalPlanNode = left.as_ref().try_into()?;
                let right: protobuf::LogicalPlanNode = right.as_ref().try_into()?;
                let (left_join_column, right_join_column) =
                    on.iter().map(|(l, r)| (l.into(), r.into())).unzip();
                let join_type: protobuf::JoinType = join_type.to_owned().into();
                let join_constraint: protobuf::JoinConstraint =
                    join_constraint.to_owned().into();
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Join(Box::new(
                        protobuf::JoinNode {
                            left: Some(Box::new(left)),
                            right: Some(Box::new(right)),
                            join_type: join_type.into(),
                            join_constraint: join_constraint.into(),
                            left_join_column,
                            right_join_column,
                            null_equals_null: *null_equals_null,
                        },
                    ))),
                })
            }
            LogicalPlan::Limit(Limit { input, n }) => {
                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Limit(Box::new(
                        protobuf::LimitNode {
                            input: Some(Box::new(input)),
                            limit: *n as u32,
                        },
                    ))),
                })
            }
            LogicalPlan::Sort(Sort { input, expr }) => {
                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;
                let selection_expr: Vec<protobuf::LogicalExprNode> = expr
                    .iter()
                    .map(|expr| expr.try_into())
                    .collect::<Result<Vec<_>, BallistaError>>()?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Sort(Box::new(
                        protobuf::SortNode {
                            input: Some(Box::new(input)),
                            expr: selection_expr,
                        },
                    ))),
                })
            }
            LogicalPlan::Repartition(Repartition {
                input,
                partitioning_scheme,
            }) => {
                use datafusion::logical_plan::Partitioning;
                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;

                //Assumed common usize field was batch size
                //Used u64 to avoid any nastyness involving large values, most data clusters are probably uniformly 64 bits any ways
                use protobuf::repartition_node::PartitionMethod;

                let pb_partition_method = match partitioning_scheme {
                    Partitioning::Hash(exprs, partition_count) => {
                        PartitionMethod::Hash(protobuf::HashRepartition {
                            hash_expr: exprs
                                .iter()
                                .map(|expr| expr.try_into())
                                .collect::<Result<Vec<_>, BallistaError>>()?,
                            partition_count: *partition_count as u64,
                        })
                    }
                    Partitioning::RoundRobinBatch(batch_size) => {
                        PartitionMethod::RoundRobin(*batch_size as u64)
                    }
                };

                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Repartition(Box::new(
                        protobuf::RepartitionNode {
                            input: Some(Box::new(input)),
                            partition_method: Some(pb_partition_method),
                        },
                    ))),
                })
            }
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row, ..
            }) => Ok(protobuf::LogicalPlanNode {
                logical_plan_type: Some(LogicalPlanType::EmptyRelation(
                    protobuf::EmptyRelationNode {
                        produce_one_row: *produce_one_row,
                    },
                )),
            }),
            LogicalPlan::CreateExternalTable(CreateExternalTable {
                name,
                location,
                file_type,
                has_header,
                schema: df_schema,
            }) => {
                use datafusion::sql::parser::FileType;

                let pb_file_type: protobuf::FileType = match file_type {
                    FileType::NdJson => protobuf::FileType::NdJson,
                    FileType::Parquet => protobuf::FileType::Parquet,
                    FileType::CSV => protobuf::FileType::Csv,
                    FileType::Avro => protobuf::FileType::Avro,
                };

                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::CreateExternalTable(
                        protobuf::CreateExternalTableNode {
                            name: name.clone(),
                            location: location.clone(),
                            file_type: pb_file_type as i32,
                            has_header: *has_header,
                            schema: Some(df_schema.into()),
                        },
                    )),
                })
            }
            LogicalPlan::Analyze(a) => {
                let input: protobuf::LogicalPlanNode = a.input.as_ref().try_into()?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Analyze(Box::new(
                        protobuf::AnalyzeNode {
                            input: Some(Box::new(input)),
                            verbose: a.verbose,
                        },
                    ))),
                })
            }
            LogicalPlan::Explain(a) => {
                let input: protobuf::LogicalPlanNode = a.plan.as_ref().try_into()?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Explain(Box::new(
                        protobuf::ExplainNode {
                            input: Some(Box::new(input)),
                            verbose: a.verbose,
                        },
                    ))),
                })
            }
            LogicalPlan::Extension { .. } => unimplemented!(),
            LogicalPlan::Union(_) => unimplemented!(),
            LogicalPlan::CrossJoin(CrossJoin { left, right, .. }) => {
                let left: protobuf::LogicalPlanNode = left.as_ref().try_into()?;
                let right: protobuf::LogicalPlanNode = right.as_ref().try_into()?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::CrossJoin(Box::new(
                        protobuf::CrossJoinNode {
                            left: Some(Box::new(left)),
                            right: Some(Box::new(right)),
                        },
                    ))),
                })
            }
            LogicalPlan::CreateMemoryTable(_) => Err(proto_error(
                "Error converting CreateMemoryTable. Not yet supported in Ballista",
            )),
            LogicalPlan::DropTable(_) => Err(proto_error(
                "Error converting DropTable. Not yet supported in Ballista",
            )),
        }
    }
}

fn create_proto_scalar<I, T: FnOnce(&I) -> protobuf::scalar_value::Value>(
    v: &Option<I>,
    null_arrow_type: protobuf::PrimitiveScalarType,
    constructor: T,
) -> protobuf::ScalarValue {
    protobuf::ScalarValue {
        value: Some(v.as_ref().map(constructor).unwrap_or(
            protobuf::scalar_value::Value::NullValue(null_arrow_type as i32),
        )),
    }
}

impl TryInto<protobuf::LogicalExprNode> for &Expr {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::LogicalExprNode, Self::Error> {
        use datafusion::scalar::ScalarValue;
        use protobuf::scalar_value::Value;
        match self {
            Expr::Column(c) => {
                let expr = protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::Column(c.into())),
                };
                Ok(expr)
            }
            Expr::Alias(expr, alias) => {
                let alias = Box::new(protobuf::AliasNode {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                    alias: alias.to_owned(),
                });
                let expr = protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::Alias(alias)),
                };
                Ok(expr)
            }
            Expr::Literal(value) => {
                let pb_value: protobuf::ScalarValue = value.try_into()?;
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::Literal(pb_value)),
                })
            }
            Expr::BinaryExpr { left, op, right } => {
                let binary_expr = Box::new(protobuf::BinaryExprNode {
                    l: Some(Box::new(left.as_ref().try_into()?)),
                    r: Some(Box::new(right.as_ref().try_into()?)),
                    op: format!("{:?}", op),
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::BinaryExpr(binary_expr)),
                })
            }
            Expr::WindowFunction {
                ref fun,
                ref args,
                ref partition_by,
                ref order_by,
                ref window_frame,
            } => {
                let window_function = match fun {
                    WindowFunction::AggregateFunction(fun) => {
                        protobuf::window_expr_node::WindowFunction::AggrFunction(
                            protobuf::AggregateFunction::from(fun).into(),
                        )
                    }
                    WindowFunction::BuiltInWindowFunction(fun) => {
                        protobuf::window_expr_node::WindowFunction::BuiltInFunction(
                            protobuf::BuiltInWindowFunction::from(fun).into(),
                        )
                    }
                };
                let arg_expr: Option<Box<protobuf::LogicalExprNode>> = if !args.is_empty()
                {
                    let arg = &args[0];
                    Some(Box::new(arg.try_into()?))
                } else {
                    None
                };
                let partition_by = partition_by
                    .iter()
                    .map(|e| e.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                let order_by = order_by
                    .iter()
                    .map(|e| e.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                let window_frame = window_frame.map(|window_frame| {
                    protobuf::window_expr_node::WindowFrame::Frame(window_frame.into())
                });
                let window_expr = Box::new(protobuf::WindowExprNode {
                    expr: arg_expr,
                    window_function: Some(window_function),
                    partition_by,
                    order_by,
                    window_frame,
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::WindowExpr(window_expr)),
                })
            }
            Expr::AggregateFunction {
                ref fun, ref args, ..
            } => {
                let aggr_function = match fun {
                    AggregateFunction::ApproxDistinct => {
                        protobuf::AggregateFunction::ApproxDistinct
                    }
                    AggregateFunction::ArrayAgg => protobuf::AggregateFunction::ArrayAgg,
                    AggregateFunction::Min => protobuf::AggregateFunction::Min,
                    AggregateFunction::Max => protobuf::AggregateFunction::Max,
                    AggregateFunction::Sum => protobuf::AggregateFunction::Sum,
                    AggregateFunction::Avg => protobuf::AggregateFunction::Avg,
                    AggregateFunction::Count => protobuf::AggregateFunction::Count,
                };

                let arg = &args[0];
                let aggregate_expr = Box::new(protobuf::AggregateExprNode {
                    aggr_function: aggr_function.into(),
                    expr: Some(Box::new(arg.try_into()?)),
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::AggregateExpr(aggregate_expr)),
                })
            }
            Expr::ScalarVariable(_) => unimplemented!(),
            Expr::ScalarFunction { ref fun, ref args } => {
                let fun: protobuf::ScalarFunction = fun.try_into()?;
                let args: Vec<protobuf::LogicalExprNode> = args
                    .iter()
                    .map(|e| e.try_into())
                    .collect::<Result<Vec<protobuf::LogicalExprNode>, BallistaError>>()?;
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(
                        protobuf::logical_expr_node::ExprType::ScalarFunction(
                            protobuf::ScalarFunctionNode {
                                fun: fun.into(),
                                args,
                            },
                        ),
                    ),
                })
            }
            Expr::ScalarUDF { .. } => unimplemented!(),
            Expr::AggregateUDF { .. } => unimplemented!(),
            Expr::Not(expr) => {
                let expr = Box::new(protobuf::Not {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::NotExpr(expr)),
                })
            }
            Expr::IsNull(expr) => {
                let expr = Box::new(protobuf::IsNull {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::IsNullExpr(expr)),
                })
            }
            Expr::IsNotNull(expr) => {
                let expr = Box::new(protobuf::IsNotNull {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::IsNotNullExpr(expr)),
                })
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let expr = Box::new(protobuf::BetweenNode {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                    negated: *negated,
                    low: Some(Box::new(low.as_ref().try_into()?)),
                    high: Some(Box::new(high.as_ref().try_into()?)),
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::Between(expr)),
                })
            }
            Expr::Case {
                expr,
                when_then_expr,
                else_expr,
            } => {
                let when_then_expr = when_then_expr
                    .iter()
                    .map(|(w, t)| {
                        Ok(protobuf::WhenThen {
                            when_expr: Some(w.as_ref().try_into()?),
                            then_expr: Some(t.as_ref().try_into()?),
                        })
                    })
                    .collect::<Result<Vec<protobuf::WhenThen>, BallistaError>>()?;
                let expr = Box::new(protobuf::CaseNode {
                    expr: match expr {
                        Some(e) => Some(Box::new(e.as_ref().try_into()?)),
                        None => None,
                    },
                    when_then_expr,
                    else_expr: match else_expr {
                        Some(e) => Some(Box::new(e.as_ref().try_into()?)),
                        None => None,
                    },
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::Case(expr)),
                })
            }
            Expr::Cast { expr, data_type } => {
                let expr = Box::new(protobuf::CastNode {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                    arrow_type: Some(data_type.into()),
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::Cast(expr)),
                })
            }
            Expr::Sort {
                expr,
                asc,
                nulls_first,
            } => {
                let expr = Box::new(protobuf::SortExprNode {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                    asc: *asc,
                    nulls_first: *nulls_first,
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::Sort(expr)),
                })
            }
            Expr::Negative(expr) => {
                let expr = Box::new(protobuf::NegativeNode {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(protobuf::logical_expr_node::ExprType::Negative(
                        expr,
                    )),
                })
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let expr = Box::new(protobuf::InListNode {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                    list: list.iter().map(|expr| expr.try_into()).collect::<Result<
                        Vec<_>,
                        BallistaError,
                    >>(
                    )?,
                    negated: *negated,
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(protobuf::logical_expr_node::ExprType::InList(expr)),
                })
            }
            Expr::Wildcard => Ok(protobuf::LogicalExprNode {
                expr_type: Some(protobuf::logical_expr_node::ExprType::Wildcard(true)),
            }),
            _ => unimplemented!(),
        }
    }
}

impl From<Column> for protobuf::Column {
    fn from(c: Column) -> protobuf::Column {
        protobuf::Column {
            relation: c
                .relation
                .map(|relation| protobuf::ColumnRelation { relation }),
            name: c.name,
        }
    }
}

impl From<&Column> for protobuf::Column {
    fn from(c: &Column) -> protobuf::Column {
        c.clone().into()
    }
}

#[allow(clippy::from_over_into)]
impl Into<protobuf::Schema> for &Schema {
    fn into(self) -> protobuf::Schema {
        protobuf::Schema {
            columns: self
                .fields()
                .iter()
                .map(protobuf::Field::from)
                .collect::<Vec<_>>(),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<protobuf::Schema> for SchemaRef {
    fn into(self) -> protobuf::Schema {
        protobuf::Schema {
            columns: self
                .fields()
                .iter()
                .map(protobuf::Field::from)
                .collect::<Vec<_>>(),
        }
    }
}

impl From<&datafusion::logical_plan::DFField> for protobuf::DfField {
    fn from(f: &datafusion::logical_plan::DFField) -> protobuf::DfField {
        protobuf::DfField {
            field: Some(f.field().into()),
            qualifier: f.qualifier().map(|r| protobuf::ColumnRelation {
                relation: r.to_string(),
            }),
        }
    }
}

impl From<&datafusion::logical_plan::DFSchemaRef> for protobuf::DfSchema {
    fn from(s: &datafusion::logical_plan::DFSchemaRef) -> protobuf::DfSchema {
        let columns = s.fields().iter().map(|f| f.into()).collect::<Vec<_>>();
        protobuf::DfSchema { columns }
    }
}

impl From<&AggregateFunction> for protobuf::AggregateFunction {
    fn from(value: &AggregateFunction) -> Self {
        match value {
            AggregateFunction::Min => Self::Min,
            AggregateFunction::Max => Self::Max,
            AggregateFunction::Sum => Self::Sum,
            AggregateFunction::Avg => Self::Avg,
            AggregateFunction::Count => Self::Count,
            AggregateFunction::ApproxDistinct => Self::ApproxDistinct,
            AggregateFunction::ArrayAgg => Self::ArrayAgg,
        }
    }
}

impl From<&BuiltInWindowFunction> for protobuf::BuiltInWindowFunction {
    fn from(value: &BuiltInWindowFunction) -> Self {
        match value {
            BuiltInWindowFunction::FirstValue => Self::FirstValue,
            BuiltInWindowFunction::LastValue => Self::LastValue,
            BuiltInWindowFunction::NthValue => Self::NthValue,
            BuiltInWindowFunction::Ntile => Self::Ntile,
            BuiltInWindowFunction::CumeDist => Self::CumeDist,
            BuiltInWindowFunction::PercentRank => Self::PercentRank,
            BuiltInWindowFunction::RowNumber => Self::RowNumber,
            BuiltInWindowFunction::Rank => Self::Rank,
            BuiltInWindowFunction::Lag => Self::Lag,
            BuiltInWindowFunction::Lead => Self::Lead,
            BuiltInWindowFunction::DenseRank => Self::DenseRank,
        }
    }
}

impl From<WindowFrameUnits> for protobuf::WindowFrameUnits {
    fn from(units: WindowFrameUnits) -> Self {
        match units {
            WindowFrameUnits::Rows => protobuf::WindowFrameUnits::Rows,
            WindowFrameUnits::Range => protobuf::WindowFrameUnits::Range,
            WindowFrameUnits::Groups => protobuf::WindowFrameUnits::Groups,
        }
    }
}

impl From<WindowFrameBound> for protobuf::WindowFrameBound {
    fn from(bound: WindowFrameBound) -> Self {
        match bound {
            WindowFrameBound::CurrentRow => protobuf::WindowFrameBound {
                window_frame_bound_type: protobuf::WindowFrameBoundType::CurrentRow
                    .into(),
                bound_value: None,
            },
            WindowFrameBound::Preceding(v) => protobuf::WindowFrameBound {
                window_frame_bound_type: protobuf::WindowFrameBoundType::Preceding.into(),
                bound_value: v.map(protobuf::window_frame_bound::BoundValue::Value),
            },
            WindowFrameBound::Following(v) => protobuf::WindowFrameBound {
                window_frame_bound_type: protobuf::WindowFrameBoundType::Following.into(),
                bound_value: v.map(protobuf::window_frame_bound::BoundValue::Value),
            },
        }
    }
}

impl From<WindowFrame> for protobuf::WindowFrame {
    fn from(window: WindowFrame) -> Self {
        protobuf::WindowFrame {
            window_frame_units: protobuf::WindowFrameUnits::from(window.units).into(),
            start_bound: Some(window.start_bound.into()),
            end_bound: Some(protobuf::window_frame::EndBound::Bound(
                window.end_bound.into(),
            )),
        }
    }
}

impl TryFrom<&DataType> for protobuf::ScalarType {
    type Error = BallistaError;
    fn try_from(value: &DataType) -> Result<Self, Self::Error> {
        let datatype = protobuf::scalar_type::Datatype::try_from(value)?;
        Ok(protobuf::ScalarType {
            datatype: Some(datatype),
        })
    }
}

impl TryInto<protobuf::ScalarFunction> for &BuiltinScalarFunction {
    type Error = BallistaError;
    fn try_into(self) -> Result<protobuf::ScalarFunction, Self::Error> {
        match self {
            BuiltinScalarFunction::Sqrt => Ok(protobuf::ScalarFunction::Sqrt),
            BuiltinScalarFunction::Sin => Ok(protobuf::ScalarFunction::Sin),
            BuiltinScalarFunction::Cos => Ok(protobuf::ScalarFunction::Cos),
            BuiltinScalarFunction::Tan => Ok(protobuf::ScalarFunction::Tan),
            BuiltinScalarFunction::Asin => Ok(protobuf::ScalarFunction::Asin),
            BuiltinScalarFunction::Acos => Ok(protobuf::ScalarFunction::Acos),
            BuiltinScalarFunction::Atan => Ok(protobuf::ScalarFunction::Atan),
            BuiltinScalarFunction::Exp => Ok(protobuf::ScalarFunction::Exp),
            BuiltinScalarFunction::Log => Ok(protobuf::ScalarFunction::Log),
            BuiltinScalarFunction::Ln => Ok(protobuf::ScalarFunction::Ln),
            BuiltinScalarFunction::Log10 => Ok(protobuf::ScalarFunction::Log10),
            BuiltinScalarFunction::Floor => Ok(protobuf::ScalarFunction::Floor),
            BuiltinScalarFunction::Ceil => Ok(protobuf::ScalarFunction::Ceil),
            BuiltinScalarFunction::Round => Ok(protobuf::ScalarFunction::Round),
            BuiltinScalarFunction::Trunc => Ok(protobuf::ScalarFunction::Trunc),
            BuiltinScalarFunction::Abs => Ok(protobuf::ScalarFunction::Abs),
            BuiltinScalarFunction::OctetLength => {
                Ok(protobuf::ScalarFunction::Octetlength)
            }
            BuiltinScalarFunction::Concat => Ok(protobuf::ScalarFunction::Concat),
            BuiltinScalarFunction::Lower => Ok(protobuf::ScalarFunction::Lower),
            BuiltinScalarFunction::Upper => Ok(protobuf::ScalarFunction::Upper),
            BuiltinScalarFunction::Trim => Ok(protobuf::ScalarFunction::Trim),
            BuiltinScalarFunction::Ltrim => Ok(protobuf::ScalarFunction::Ltrim),
            BuiltinScalarFunction::Rtrim => Ok(protobuf::ScalarFunction::Rtrim),
            BuiltinScalarFunction::ToTimestamp => {
                Ok(protobuf::ScalarFunction::Totimestamp)
            }
            BuiltinScalarFunction::Array => Ok(protobuf::ScalarFunction::Array),
            BuiltinScalarFunction::NullIf => Ok(protobuf::ScalarFunction::Nullif),
            BuiltinScalarFunction::DatePart => Ok(protobuf::ScalarFunction::Datepart),
            BuiltinScalarFunction::DateTrunc => Ok(protobuf::ScalarFunction::Datetrunc),
            BuiltinScalarFunction::MD5 => Ok(protobuf::ScalarFunction::Md5),
            BuiltinScalarFunction::SHA224 => Ok(protobuf::ScalarFunction::Sha224),
            BuiltinScalarFunction::SHA256 => Ok(protobuf::ScalarFunction::Sha256),
            BuiltinScalarFunction::SHA384 => Ok(protobuf::ScalarFunction::Sha384),
            BuiltinScalarFunction::SHA512 => Ok(protobuf::ScalarFunction::Sha512),
            BuiltinScalarFunction::Digest => Ok(protobuf::ScalarFunction::Digest),
            BuiltinScalarFunction::ToTimestampMillis => {
                Ok(protobuf::ScalarFunction::Totimestampmillis)
            }
            _ => Err(BallistaError::General(format!(
                "logical_plan::to_proto() unsupported scalar function {:?}",
                self
            ))),
        }
    }
}
