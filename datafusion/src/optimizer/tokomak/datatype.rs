use crate::error::DataFusionError;
use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use egg::*;
use std::convert::{TryFrom};
use std::fmt::Display;
use std::str::FromStr;

define_language! {
    #[derive(Copy)]
    pub enum TokomakDataType {
        "date32" = Date32,
        "date64" = Date64,
        "bool" = Boolean,
        "int8" = Int8,
        "int16" =Int16,
        "int32" =Int32,
        "int64" =Int64,
        "uint8" =UInt8,
        "uint16" =UInt16,
        "uint32" =UInt32,
        "uint64" =UInt64,
        "float16" =Float16,
        "float32" =Float32,
        "float64" =Float64,
        "utf8"=Utf8,
        "largeutf8"=LargeUtf8,
        "time(s)"=TimestampSecond,
        "time(ms)"=TimestampMillisecond,
        "time(us)"=TimestampMicrosecond,
        "time(ns)"=TimestampNanosecond,
        "interval(yearmonth)"=IntervalYearMonth,
        "interval(daytime)"=IntervalDayTime,
    }
}

impl Display for TokomakDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}", self))
    }
}

impl Into<DataType> for &TokomakDataType {
    fn into(self) -> DataType {
        let v = *self;
        v.into()
    }
}

impl Into<DataType> for TokomakDataType {
    fn into(self) -> DataType {
        match self {
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
            TokomakDataType::Float16 => DataType::Float16,
            TokomakDataType::Float32 => DataType::Float32,
            TokomakDataType::Float64 => DataType::Float64,
            TokomakDataType::Utf8 => DataType::Utf8,
            TokomakDataType::LargeUtf8 => DataType::LargeUtf8,
            TokomakDataType::TimestampSecond => {
                DataType::Timestamp(TimeUnit::Second, None)
            }
            TokomakDataType::TimestampMillisecond => {
                DataType::Timestamp(TimeUnit::Millisecond, None)
            }
            TokomakDataType::TimestampMicrosecond => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            TokomakDataType::TimestampNanosecond => {
                DataType::Timestamp(TimeUnit::Nanosecond, None)
            }
            TokomakDataType::IntervalYearMonth => {
                DataType::Interval(IntervalUnit::YearMonth)
            }
            TokomakDataType::IntervalDayTime => DataType::Interval(IntervalUnit::DayTime),
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
            DataType::Float16 => TokomakDataType::Float16,
            DataType::Float32 => TokomakDataType::Float32,
            DataType::Float64 => TokomakDataType::Float64,
            DataType::Utf8 => TokomakDataType::Utf8,
            DataType::LargeUtf8 => TokomakDataType::LargeUtf8,
            DataType::Timestamp(TimeUnit::Second, None) => {
                TokomakDataType::TimestampSecond
            }
            DataType::Timestamp(TimeUnit::Millisecond, None) => {
                TokomakDataType::TimestampMillisecond
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                TokomakDataType::TimestampMicrosecond
            }
            DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                TokomakDataType::TimestampNanosecond
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                TokomakDataType::IntervalYearMonth
            }
            DataType::Interval(IntervalUnit::DayTime) => TokomakDataType::IntervalDayTime,
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "The data type {} is invalid as a tokomak datatype",
                    val
                )))
            }
        })
    }
}

impl FromStr for TokomakDataType {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "date32" => Ok(TokomakDataType::Date32),
            "date64" => Ok(TokomakDataType::Date64),
            "bool" => Ok(TokomakDataType::Boolean),
            "int8" => Ok(TokomakDataType::Int8),
            "int16" => Ok(TokomakDataType::Int16),
            "int32" => Ok(TokomakDataType::Int32),
            "int64" => Ok(TokomakDataType::Int64),
            "uint8" => Ok(TokomakDataType::UInt8),
            "uint16" => Ok(TokomakDataType::UInt16),
            "uint32" => Ok(TokomakDataType::UInt32),
            "uint64" => Ok(TokomakDataType::UInt64),
            "float16" => Ok(TokomakDataType::Float16),
            "float32" => Ok(TokomakDataType::Float32),
            "float64" => Ok(TokomakDataType::Float64),
            "utf8" => Ok(TokomakDataType::Utf8),
            "largeutf8" => Ok(TokomakDataType::LargeUtf8),
            "time(s)" => Ok(TokomakDataType::TimestampSecond),
            "time(ms)" => Ok(TokomakDataType::TimestampMillisecond),
            "time(us)" => Ok(TokomakDataType::TimestampMicrosecond),
            "time(ns)" => Ok(TokomakDataType::TimestampNanosecond),
            "interval(yearmonth)" => Ok(TokomakDataType::IntervalYearMonth),
            "interval(daytime)" => Ok(TokomakDataType::IntervalDayTime),
            _ => Err(DataFusionError::Internal(
                "Parsing string as TokomakDataType failed".to_string(),
            )),
        }
    }
}
