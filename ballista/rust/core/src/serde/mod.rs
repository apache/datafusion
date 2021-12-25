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

//! This crate contains code generated from the Ballista Protocol Buffer Definition as well
//! as convenience code for interacting with the generated code.

use std::{convert::TryInto, io::Cursor};

use datafusion::logical_plan::{JoinConstraint, JoinType, Operator};
use datafusion::physical_plan::aggregates::AggregateFunction;
use datafusion::physical_plan::window_functions::BuiltInWindowFunction;

use crate::{error::BallistaError, serde::scheduler::Action as BallistaAction};

use prost::Message;

// include the generated protobuf source as a submodule
#[allow(clippy::all)]
pub mod protobuf {
    include!(concat!(env!("OUT_DIR"), "/ballista.protobuf.rs"));
}

pub mod logical_plan;
pub mod physical_plan;
pub mod scheduler;

pub fn decode_protobuf(bytes: &[u8]) -> Result<BallistaAction, BallistaError> {
    let mut buf = Cursor::new(bytes);

    protobuf::Action::decode(&mut buf)
        .map_err(|e| BallistaError::Internal(format!("{:?}", e)))
        .and_then(|node| node.try_into())
}

pub(crate) fn proto_error<S: Into<String>>(message: S) -> BallistaError {
    BallistaError::General(message.into())
}

#[macro_export]
macro_rules! convert_required {
    ($PB:expr) => {{
        if let Some(field) = $PB.as_ref() {
            field.try_into()
        } else {
            Err(proto_error("Missing required field in protobuf"))
        }
    }};
}

#[macro_export]
macro_rules! into_required {
    ($PB:expr) => {{
        if let Some(field) = $PB.as_ref() {
            Ok(field.into())
        } else {
            Err(proto_error("Missing required field in protobuf"))
        }
    }};
}

#[macro_export]
macro_rules! convert_box_required {
    ($PB:expr) => {{
        if let Some(field) = $PB.as_ref() {
            field.as_ref().try_into()
        } else {
            Err(proto_error("Missing required field in protobuf"))
        }
    }};
}

pub(crate) fn from_proto_binary_op(op: &str) -> Result<Operator, BallistaError> {
    match op {
        "And" => Ok(Operator::And),
        "Or" => Ok(Operator::Or),
        "Eq" => Ok(Operator::Eq),
        "NotEq" => Ok(Operator::NotEq),
        "LtEq" => Ok(Operator::LtEq),
        "Lt" => Ok(Operator::Lt),
        "Gt" => Ok(Operator::Gt),
        "GtEq" => Ok(Operator::GtEq),
        "Plus" => Ok(Operator::Plus),
        "Minus" => Ok(Operator::Minus),
        "Multiply" => Ok(Operator::Multiply),
        "Divide" => Ok(Operator::Divide),
        "Modulo" => Ok(Operator::Modulo),
        "Like" => Ok(Operator::Like),
        "NotLike" => Ok(Operator::NotLike),
        other => Err(proto_error(format!(
            "Unsupported binary operator '{:?}'",
            other
        ))),
    }
}

impl From<protobuf::AggregateFunction> for AggregateFunction {
    fn from(agg_fun: protobuf::AggregateFunction) -> AggregateFunction {
        match agg_fun {
            protobuf::AggregateFunction::Min => AggregateFunction::Min,
            protobuf::AggregateFunction::Max => AggregateFunction::Max,
            protobuf::AggregateFunction::Sum => AggregateFunction::Sum,
            protobuf::AggregateFunction::Avg => AggregateFunction::Avg,
            protobuf::AggregateFunction::Count => AggregateFunction::Count,
            protobuf::AggregateFunction::ApproxDistinct => {
                AggregateFunction::ApproxDistinct
            }
            protobuf::AggregateFunction::ArrayAgg => AggregateFunction::ArrayAgg,
        }
    }
}

impl From<protobuf::BuiltInWindowFunction> for BuiltInWindowFunction {
    fn from(built_in_function: protobuf::BuiltInWindowFunction) -> Self {
        match built_in_function {
            protobuf::BuiltInWindowFunction::RowNumber => {
                BuiltInWindowFunction::RowNumber
            }
            protobuf::BuiltInWindowFunction::Rank => BuiltInWindowFunction::Rank,
            protobuf::BuiltInWindowFunction::PercentRank => {
                BuiltInWindowFunction::PercentRank
            }
            protobuf::BuiltInWindowFunction::DenseRank => {
                BuiltInWindowFunction::DenseRank
            }
            protobuf::BuiltInWindowFunction::Lag => BuiltInWindowFunction::Lag,
            protobuf::BuiltInWindowFunction::Lead => BuiltInWindowFunction::Lead,
            protobuf::BuiltInWindowFunction::FirstValue => {
                BuiltInWindowFunction::FirstValue
            }
            protobuf::BuiltInWindowFunction::CumeDist => BuiltInWindowFunction::CumeDist,
            protobuf::BuiltInWindowFunction::Ntile => BuiltInWindowFunction::Ntile,
            protobuf::BuiltInWindowFunction::NthValue => BuiltInWindowFunction::NthValue,
            protobuf::BuiltInWindowFunction::LastValue => {
                BuiltInWindowFunction::LastValue
            }
        }
    }
}

impl TryInto<datafusion::arrow::datatypes::DataType>
    for &protobuf::arrow_type::ArrowTypeEnum
{
    type Error = BallistaError;
    fn try_into(self) -> Result<datafusion::arrow::datatypes::DataType, Self::Error> {
        use datafusion::arrow::datatypes::DataType;
        use protobuf::arrow_type;
        Ok(match self {
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
            arrow_type::ArrowTypeEnum::LargeUtf8(_) => DataType::LargeUtf8,
            arrow_type::ArrowTypeEnum::Binary(_) => DataType::Binary,
            arrow_type::ArrowTypeEnum::FixedSizeBinary(size) => {
                DataType::FixedSizeBinary(*size)
            }
            arrow_type::ArrowTypeEnum::LargeBinary(_) => DataType::LargeBinary,
            arrow_type::ArrowTypeEnum::Date32(_) => DataType::Date32,
            arrow_type::ArrowTypeEnum::Date64(_) => DataType::Date64,
            arrow_type::ArrowTypeEnum::Duration(time_unit) => {
                DataType::Duration(protobuf::TimeUnit::from_i32_to_arrow(*time_unit)?)
            }
            arrow_type::ArrowTypeEnum::Timestamp(protobuf::Timestamp {
                time_unit,
                timezone,
            }) => DataType::Timestamp(
                protobuf::TimeUnit::from_i32_to_arrow(*time_unit)?,
                match timezone.len() {
                    0 => None,
                    _ => Some(timezone.to_owned()),
                },
            ),
            arrow_type::ArrowTypeEnum::Time32(time_unit) => {
                DataType::Time32(protobuf::TimeUnit::from_i32_to_arrow(*time_unit)?)
            }
            arrow_type::ArrowTypeEnum::Time64(time_unit) => {
                DataType::Time64(protobuf::TimeUnit::from_i32_to_arrow(*time_unit)?)
            }
            arrow_type::ArrowTypeEnum::Interval(interval_unit) => DataType::Interval(
                protobuf::IntervalUnit::from_i32_to_arrow(*interval_unit)?,
            ),
            arrow_type::ArrowTypeEnum::Decimal(protobuf::Decimal {
                whole,
                fractional,
            }) => DataType::Decimal(*whole as usize, *fractional as usize),
            arrow_type::ArrowTypeEnum::List(list) => {
                let list_type: &protobuf::Field = list
                    .as_ref()
                    .field_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: List message missing required field 'field_type'"))?
                    .as_ref();
                DataType::List(Box::new(list_type.try_into()?))
            }
            arrow_type::ArrowTypeEnum::LargeList(list) => {
                let list_type: &protobuf::Field = list
                    .as_ref()
                    .field_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: List message missing required field 'field_type'"))?
                    .as_ref();
                DataType::LargeList(Box::new(list_type.try_into()?))
            }
            arrow_type::ArrowTypeEnum::FixedSizeList(list) => {
                let list_type: &protobuf::Field = list
                    .as_ref()
                    .field_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: List message missing required field 'field_type'"))?
                    .as_ref();
                let list_size = list.list_size;
                DataType::FixedSizeList(Box::new(list_type.try_into()?), list_size)
            }
            arrow_type::ArrowTypeEnum::Struct(strct) => DataType::Struct(
                strct
                    .sub_field_types
                    .iter()
                    .map(|field| field.try_into())
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            arrow_type::ArrowTypeEnum::Union(union) => DataType::Union(
                union
                    .union_types
                    .iter()
                    .map(|field| field.try_into())
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            arrow_type::ArrowTypeEnum::Dictionary(dict) => {
                let pb_key_datatype = dict
                    .as_ref()
                    .key
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: Dictionary message missing required field 'key'"))?;
                let pb_value_datatype = dict
                    .as_ref()
                    .value
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: Dictionary message missing required field 'key'"))?;
                let key_datatype: DataType = pb_key_datatype.as_ref().try_into()?;
                let value_datatype: DataType = pb_value_datatype.as_ref().try_into()?;
                DataType::Dictionary(Box::new(key_datatype), Box::new(value_datatype))
            }
        })
    }
}

#[allow(clippy::from_over_into)]
impl Into<datafusion::arrow::datatypes::DataType> for protobuf::PrimitiveScalarType {
    fn into(self) -> datafusion::arrow::datatypes::DataType {
        use datafusion::arrow::datatypes::{DataType, TimeUnit};
        match self {
            protobuf::PrimitiveScalarType::Bool => DataType::Boolean,
            protobuf::PrimitiveScalarType::Uint8 => DataType::UInt8,
            protobuf::PrimitiveScalarType::Int8 => DataType::Int8,
            protobuf::PrimitiveScalarType::Uint16 => DataType::UInt16,
            protobuf::PrimitiveScalarType::Int16 => DataType::Int16,
            protobuf::PrimitiveScalarType::Uint32 => DataType::UInt32,
            protobuf::PrimitiveScalarType::Int32 => DataType::Int32,
            protobuf::PrimitiveScalarType::Uint64 => DataType::UInt64,
            protobuf::PrimitiveScalarType::Int64 => DataType::Int64,
            protobuf::PrimitiveScalarType::Float32 => DataType::Float32,
            protobuf::PrimitiveScalarType::Float64 => DataType::Float64,
            protobuf::PrimitiveScalarType::Utf8 => DataType::Utf8,
            protobuf::PrimitiveScalarType::LargeUtf8 => DataType::LargeUtf8,
            protobuf::PrimitiveScalarType::Date32 => DataType::Date32,
            protobuf::PrimitiveScalarType::TimeMicrosecond => {
                DataType::Time64(TimeUnit::Microsecond)
            }
            protobuf::PrimitiveScalarType::TimeNanosecond => {
                DataType::Time64(TimeUnit::Nanosecond)
            }
            protobuf::PrimitiveScalarType::Null => DataType::Null,
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
            protobuf::JoinType::Semi => JoinType::Semi,
            protobuf::JoinType::Anti => JoinType::Anti,
        }
    }
}

impl From<JoinType> for protobuf::JoinType {
    fn from(t: JoinType) -> Self {
        match t {
            JoinType::Inner => protobuf::JoinType::Inner,
            JoinType::Left => protobuf::JoinType::Left,
            JoinType::Right => protobuf::JoinType::Right,
            JoinType::Full => protobuf::JoinType::Full,
            JoinType::Semi => protobuf::JoinType::Semi,
            JoinType::Anti => protobuf::JoinType::Anti,
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

impl From<JoinConstraint> for protobuf::JoinConstraint {
    fn from(t: JoinConstraint) -> Self {
        match t {
            JoinConstraint::On => protobuf::JoinConstraint::On,
            JoinConstraint::Using => protobuf::JoinConstraint::Using,
        }
    }
}

fn byte_to_string(b: u8) -> Result<String, BallistaError> {
    let b = &[b];
    let b = std::str::from_utf8(b)
        .map_err(|_| BallistaError::General("Invalid CSV delimiter".to_owned()))?;
    Ok(b.to_owned())
}

fn str_to_byte(s: &str) -> Result<u8, BallistaError> {
    if s.len() != 1 {
        return Err(BallistaError::General("Invalid CSV delimiter".to_owned()));
    }
    Ok(s.as_bytes()[0])
}
