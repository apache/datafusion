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

//! This module contains TokomakDataType and the conversion functions TokomakDatType<->DataType

use datafusion::arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion::error::DataFusionError;
use egg::*;
use std::convert::TryFrom;
use std::str::FromStr;
///Maximum allowable precision for decimal 128 datatype
pub const MAX_PRECISION_FOR_DECIMAL128: usize = 38;
///Maximum allowable scale for decimal 128 datatype
pub const MAX_SCALE_FOR_DECIMAL128: usize = 38;

///Represents DataType in a Cast expression. Type definitions should map 1-1 with [datafusion::arrow::data_types::DataType]
#[allow(missing_docs)]
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Debug)]
pub enum TokomakDataType {
    Date32,
    Date64,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Utf8,
    LargeUtf8,
    Timestamp(TimeUnit, Option<String>),
    Time32(TimeUnit),
    Time64(TimeUnit),
    Interval(IntervalUnit),
    ///Precision and scale
    Decimal128(usize, usize),
    Duration(TimeUnit),
}

impl Language for TokomakDataType {
    fn matches(&self, other: &Self) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }

    fn children(&self) -> &[Id] {
        &[]
    }

    fn children_mut(&mut self) -> &mut [Id] {
        &mut []
    }
}

fn matches_type_with_parameter_pack(prefix: &str, op: &str) -> bool {
    let ends_with = op.ends_with('>');
    if !ends_with {
        return false;
    }
    //If the prefix is not long enougth to contain the op and the two extra bytes required
    if prefix.len() + 2 >= op.len() {
        return false;
    }
    let mut byte_op_iter = op.as_bytes().iter();
    for prefix_byte in prefix.as_bytes().iter() {
        //Since op is longer than prefix this is okay
        let op_byte = byte_op_iter.next().unwrap();
        if *op_byte != *prefix_byte {
            return false;
        }
    }
    *byte_op_iter.next().unwrap_or(&b't') == b'<'
}

fn extract_parameter_pack<'a>(prefix: &str, op: &'a str) -> &'a str {
    let prefix_len = prefix.len();
    &op[prefix_len + 1..op.len() - 1]
}

fn parse_time_unit(time_unit: &str) -> Result<TimeUnit, DataFusionError> {
    Ok(match time_unit{
        "s" => TimeUnit::Second,
        "ms" => TimeUnit::Millisecond,
        "us" => TimeUnit::Microsecond,
        "ns" => TimeUnit::Nanosecond,
        _=> return Err(DataFusionError::Internal(format!("{} is not a valid time unit, valid units are s, ms, us, and ns which refere to Second, Millisecond, Microsecond, and Nanosecond respectively", time_unit))),
    })
}

impl FromStr for TokomakDataType {
    type Err = DataFusionError;

    fn from_str(op: &str) -> Result<Self, Self::Err> {
        let dt = match op {
            "date32" => TokomakDataType::Date32,
            "date64" => TokomakDataType::Date64,
            "bool" => TokomakDataType::Boolean,
            "int8" => TokomakDataType::Int8,
            "int16" => TokomakDataType::Int16,
            "int32" => TokomakDataType::Int32,
            "int64" => TokomakDataType::Int64,
            "uint8" => TokomakDataType::UInt8,
            "uint16" => TokomakDataType::UInt16,
            "uint32" => TokomakDataType::UInt32,
            "uint64" => TokomakDataType::UInt64,
            "float32" => TokomakDataType::Float32,
            "float64" => TokomakDataType::Float64,
            "utf8" => TokomakDataType::Utf8,
            "largeutf8" => TokomakDataType::LargeUtf8,
            _ if matches_type_with_parameter_pack("timestamp", op) => {
                let param_pack = extract_parameter_pack("timestamp", op);
                let mut param_iter = param_pack.split(',');
                let time_unit = param_iter.next().ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Could not extract datatype parameter pack from {}",
                        op
                    ))
                })?;
                let tz = param_iter.next();
                let time_unit = parse_time_unit(time_unit)?;
                let tz = tz.map(String::from);
                TokomakDataType::Timestamp(time_unit, tz)
            }
            _ if matches_type_with_parameter_pack("interval", op) => {
                let interval_unit_str = extract_parameter_pack("interval", op);
                let interval_unit=match interval_unit_str{
                    "ym"=>IntervalUnit::YearMonth,
                    "dt"=>IntervalUnit::DayTime,
                    "mdn" => IntervalUnit::MonthDayNano,
                    _=> return Err(DataFusionError::Internal(format!("{} is not a valid interval unit. Valid interval units are ym for YearMonth and dt for DayTime", interval_unit_str))),
                };
                TokomakDataType::Interval(interval_unit)
            }
            _ if matches_type_with_parameter_pack("decimal", op) => {
                let param_pack = extract_parameter_pack("decimal", op);
                let mut param_iter = param_pack.split(',');
                let precision_str = param_iter.next().ok_or_else(|| {
                    DataFusionError::Internal(String::from(
                        "Could not extract decimal precision from parameter pack",
                    ))
                })?;
                let scale_str = param_iter.next().ok_or_else(|| {
                    DataFusionError::Internal(String::from(
                        "Could not extract decimal scale from parameter pack",
                    ))
                })?;
                let precision: usize = precision_str.parse().map_err(|e| {
                    DataFusionError::Internal(format!(
                        "Could not parse {} as decimal precision: {}",
                        precision_str, e
                    ))
                })?;
                let scale: usize = scale_str.parse().map_err(|e| {
                    DataFusionError::Internal(format!(
                        "Could not parse {} as decimal scale: {}",
                        precision_str, e
                    ))
                })?;
                if precision > MAX_PRECISION_FOR_DECIMAL128 {
                    return Err(DataFusionError::Internal(format!("The precision {} exceeded the maximum allowed precision of {} for decimal128", precision, MAX_PRECISION_FOR_DECIMAL128)));
                }
                if scale > precision {
                    return Err(DataFusionError::Internal(format!(
                        "The scale, {}, was larger than the precision {}",
                        scale, precision
                    )));
                }
                TokomakDataType::Decimal128(precision, scale)
            }

            _ if matches_type_with_parameter_pack("time32", op) => {
                let param_pack = extract_parameter_pack("time32", op);
                let time_unit = parse_time_unit(param_pack)?;
                TokomakDataType::Time32(time_unit)
            }
            _ if matches_type_with_parameter_pack("time64", op) => {
                let param_pack = extract_parameter_pack("time64", op);
                let time_unit = parse_time_unit(param_pack)?;
                TokomakDataType::Time32(time_unit)
            }
            _ if matches_type_with_parameter_pack("duration", op) => {
                let param_pack = extract_parameter_pack("duration", op);
                let time_unit = parse_time_unit(param_pack)?;
                TokomakDataType::Duration(time_unit)
            }
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "The string {} is not a valid tokomak datatype",
                    op
                )))
            }
        };
        Ok(dt)
    }
}

impl From<TokomakDataType> for DataType {
    fn from(v: TokomakDataType) -> Self {
        match v {
            TokomakDataType::Date32 => DataType::Date32,
            TokomakDataType::Date64 => DataType::Date64,
            TokomakDataType::Boolean => DataType::Boolean,
            TokomakDataType::Int8 => DataType::Int8,
            TokomakDataType::Int16 => DataType::Int16,
            TokomakDataType::Int32 => DataType::Int32,
            TokomakDataType::Int64 => DataType::Int64,
            TokomakDataType::UInt8 => DataType::UInt8,
            TokomakDataType::UInt16 => DataType::UInt16,
            TokomakDataType::UInt32 => DataType::UInt32,
            TokomakDataType::UInt64 => DataType::UInt64,
            TokomakDataType::Float32 => DataType::Float32,
            TokomakDataType::Float64 => DataType::Float64,
            TokomakDataType::Utf8 => DataType::Utf8,
            TokomakDataType::LargeUtf8 => DataType::LargeUtf8,
            TokomakDataType::Timestamp(unit, tz) => DataType::Timestamp(unit, tz),
            TokomakDataType::Interval(unit) => DataType::Interval(unit),
            TokomakDataType::Time32(unit) => DataType::Time32(unit),
            TokomakDataType::Time64(unit) => DataType::Time32(unit),
            TokomakDataType::Decimal128(precision, scale) => {
                DataType::Decimal(precision, scale)
            }
            TokomakDataType::Duration(unit) => DataType::Duration(unit),
        }
    }
}

impl TryFrom<DataType> for TokomakDataType {
    type Error = DataFusionError;
    fn try_from(val: DataType) -> Result<Self, Self::Error> {
        Ok(match val {
            DataType::Date32 => TokomakDataType::Date32,
            DataType::Date64 => TokomakDataType::Date64,
            DataType::Boolean => TokomakDataType::Boolean,
            DataType::Int8 => TokomakDataType::Int8,
            DataType::Int16 => TokomakDataType::Int16,
            DataType::Int32 => TokomakDataType::Int32,
            DataType::Int64 => TokomakDataType::Int64,
            DataType::UInt8 => TokomakDataType::UInt8,
            DataType::UInt16 => TokomakDataType::UInt16,
            DataType::UInt32 => TokomakDataType::UInt32,
            DataType::UInt64 => TokomakDataType::UInt64,
            DataType::Float32 => TokomakDataType::Float32,
            DataType::Float64 => TokomakDataType::Float64,
            DataType::Utf8 => TokomakDataType::Utf8,
            DataType::LargeUtf8 => TokomakDataType::LargeUtf8,
            DataType::Timestamp(unit, tz) => TokomakDataType::Timestamp(unit, tz),
            DataType::Interval(unit) => TokomakDataType::Interval(unit),
            DataType::Decimal(precision, scale) => {
                TokomakDataType::Decimal128(precision, scale)
            }
            DataType::Time32(unit) => TokomakDataType::Time32(unit),
            DataType::Time64(unit) => TokomakDataType::Time64(unit),
            DataType::Duration(unit) => TokomakDataType::Duration(unit),
            //TODO: Do more complex types need supprt? Only used in Cast and TryCast Expr. Can those cast more complex types?
            dt @ (DataType::Float16
            | DataType::Null
            | DataType::Binary
            | DataType::FixedSizeBinary(_)
            | DataType::LargeBinary
            | DataType::List(_)
            | DataType::FixedSizeList(_, _)
            | DataType::LargeList(_)
            | DataType::Struct(_)
            | DataType::Union(_, _)
            | DataType::Dictionary(_, _)
            | DataType::Map(_, _)) => {
                return Err(DataFusionError::Internal(format!(
                    "The data type {} is invalid as a tokomak datatype",
                    dt
                )))
            }
        })
    }
}

impl std::fmt::Display for TokomakDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let disp_tunit = |tunit: &TimeUnit| -> &'static str {
            match tunit {
                TimeUnit::Second => "s",
                TimeUnit::Millisecond => "ms",
                TimeUnit::Microsecond => "us",
                TimeUnit::Nanosecond => "ns",
            }
        };

        let simple_fmt = match self {
            TokomakDataType::Date32 => "date32",
            TokomakDataType::Date64 => "date64",
            TokomakDataType::Boolean => "bool",
            TokomakDataType::Int8 => "int8",
            TokomakDataType::Int16 => "int16",
            TokomakDataType::Int32 => "int32",
            TokomakDataType::Int64 => "int64",
            TokomakDataType::UInt8 => "uint8",
            TokomakDataType::UInt16 => "uint16",
            TokomakDataType::UInt32 => "uint32",
            TokomakDataType::UInt64 => "uint64",
            TokomakDataType::Float32 => "float32",
            TokomakDataType::Float64 => "float64",
            TokomakDataType::Utf8 => "utf8",
            TokomakDataType::LargeUtf8 => "largeutf8",
            TokomakDataType::Timestamp(unit, None) => {
                return write!(f, "timestamp<{}>", disp_tunit(unit))
            }
            TokomakDataType::Timestamp(unit, Some(tz)) => {
                return write!(f, "timestamp<{},{}>", disp_tunit(unit), tz)
            }
            TokomakDataType::Time32(unit) => {
                return write!(f, "time32<{}>", disp_tunit(unit))
            }
            TokomakDataType::Time64(unit) => {
                return write!(f, "time64<{}>", disp_tunit(unit))
            }
            TokomakDataType::Interval(unit) => {
                return write!(
                    f,
                    "interval<{}>",
                    match unit {
                        IntervalUnit::DayTime => "dt",
                        IntervalUnit::YearMonth => "ym",
                        IntervalUnit::MonthDayNano => "mdn",
                    }
                )
            }
            TokomakDataType::Decimal128(precision, scale) => {
                return write!(f, "decimal<{},{}>", precision, scale)
            }
            TokomakDataType::Duration(unit) => {
                return write!(f, "duration<{}>", disp_tunit(unit))
            }
        };
        write!(f, "{}", simple_fmt)
    }
}
