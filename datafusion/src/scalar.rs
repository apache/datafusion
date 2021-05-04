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

//! This module provides ScalarValue, an enum that can be used for storage of single elements

use std::{convert::TryFrom, fmt, iter::repeat, sync::Arc};

use arrow::datatypes::{DataType, Field, IntervalUnit, TimeUnit};
use arrow::{
    array::*,
    datatypes::{ArrowNativeType, Float32Type, TimestampNanosecondType},
};
use arrow::{
    array::{
        ArrayRef, Int16Builder, Int32Builder, Int64Builder, Int8Builder, ListBuilder,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
    },
    datatypes::{
        TimestampMicrosecondType, TimestampMillisecondType, TimestampSecondType,
    },
};

use crate::error::{DataFusionError, Result};

/// Represents a dynamically typed, nullable single value.
/// This is the single-valued counter-part of arrow’s `Array`.
#[derive(Clone, PartialEq)]
pub enum ScalarValue {
    /// true or false value
    Boolean(Option<bool>),
    /// 32bit float
    Float32(Option<f32>),
    /// 64bit float
    Float64(Option<f64>),
    /// signed 8bit int
    Int8(Option<i8>),
    /// signed 16bit int
    Int16(Option<i16>),
    /// signed 32bit int
    Int32(Option<i32>),
    /// signed 64bit int
    Int64(Option<i64>),
    /// unsigned 8bit int
    UInt8(Option<u8>),
    /// unsigned 16bit int
    UInt16(Option<u16>),
    /// unsigned 32bit int
    UInt32(Option<u32>),
    /// unsigned 64bit int
    UInt64(Option<u64>),
    /// utf-8 encoded string.
    Utf8(Option<String>),
    /// utf-8 encoded string representing a LargeString's arrow type.
    LargeUtf8(Option<String>),
    /// binary
    Binary(Option<Vec<u8>>),
    /// large binary
    LargeBinary(Option<Vec<u8>>),
    /// list of nested ScalarValue
    List(Option<Vec<ScalarValue>>, DataType),
    /// Date stored as a signed 32bit int
    Date32(Option<i32>),
    /// Date stored as a signed 64bit int
    Date64(Option<i64>),
    /// Timestamp Second
    TimestampSecond(Option<i64>),
    /// Timestamp Milliseconds
    TimestampMillisecond(Option<i64>),
    /// Timestamp Microseconds
    TimestampMicrosecond(Option<i64>),
    /// Timestamp Nanoseconds
    TimestampNanosecond(Option<i64>),
    /// Interval with YearMonth unit
    IntervalYearMonth(Option<i32>),
    /// Interval with DayTime unit
    IntervalDayTime(Option<i64>),
}

macro_rules! typed_cast {
    ($array:expr, $index:expr, $ARRAYTYPE:ident, $SCALAR:ident) => {{
        let array = $array.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        ScalarValue::$SCALAR(match array.is_null($index) {
            true => None,
            false => Some(array.value($index).into()),
        })
    }};
}

macro_rules! build_list {
    ($VALUE_BUILDER_TY:ident, $SCALAR_TY:ident, $VALUES:expr, $SIZE:expr) => {{
        match $VALUES {
            // the return on the macro is necessary, to short-circuit and return ArrayRef
            None => {
                return new_null_array(
                    &DataType::List(Box::new(Field::new(
                        "item",
                        DataType::$SCALAR_TY,
                        true,
                    ))),
                    $SIZE,
                )
            }
            Some(values) => {
                let mut builder = ListBuilder::new($VALUE_BUILDER_TY::new(values.len()));

                for _ in 0..$SIZE {
                    for scalar_value in values {
                        match scalar_value {
                            ScalarValue::$SCALAR_TY(Some(v)) => {
                                builder.values().append_value(v.clone()).unwrap()
                            }
                            ScalarValue::$SCALAR_TY(None) => {
                                builder.values().append_null().unwrap();
                            }
                            _ => panic!("Incompatible ScalarValue for list"),
                        };
                    }
                    builder.append(true).unwrap();
                }

                builder.finish()
            }
        }
    }};
}

impl ScalarValue {
    /// Getter for the `DataType` of the value
    pub fn get_datatype(&self) -> DataType {
        match self {
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::UInt8(_) => DataType::UInt8,
            ScalarValue::UInt16(_) => DataType::UInt16,
            ScalarValue::UInt32(_) => DataType::UInt32,
            ScalarValue::UInt64(_) => DataType::UInt64,
            ScalarValue::Int8(_) => DataType::Int8,
            ScalarValue::Int16(_) => DataType::Int16,
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::TimestampSecond(_) => {
                DataType::Timestamp(TimeUnit::Second, None)
            }
            ScalarValue::TimestampMillisecond(_) => {
                DataType::Timestamp(TimeUnit::Millisecond, None)
            }
            ScalarValue::TimestampMicrosecond(_) => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            ScalarValue::TimestampNanosecond(_) => {
                DataType::Timestamp(TimeUnit::Nanosecond, None)
            }
            ScalarValue::Float32(_) => DataType::Float32,
            ScalarValue::Float64(_) => DataType::Float64,
            ScalarValue::Utf8(_) => DataType::Utf8,
            ScalarValue::LargeUtf8(_) => DataType::LargeUtf8,
            ScalarValue::Binary(_) => DataType::Binary,
            ScalarValue::LargeBinary(_) => DataType::LargeBinary,
            ScalarValue::List(_, data_type) => {
                DataType::List(Box::new(Field::new("item", data_type.clone(), true)))
            }
            ScalarValue::Date32(_) => DataType::Date32,
            ScalarValue::Date64(_) => DataType::Date64,
            ScalarValue::IntervalYearMonth(_) => {
                DataType::Interval(IntervalUnit::YearMonth)
            }
            ScalarValue::IntervalDayTime(_) => DataType::Interval(IntervalUnit::DayTime),
        }
    }

    /// Calculate arithmetic negation for a scalar value
    pub fn arithmetic_negate(&self) -> Self {
        match self {
            ScalarValue::Boolean(None)
            | ScalarValue::Int8(None)
            | ScalarValue::Int16(None)
            | ScalarValue::Int32(None)
            | ScalarValue::Int64(None)
            | ScalarValue::Float32(None) => self.clone(),
            ScalarValue::Float64(Some(v)) => ScalarValue::Float64(Some(-v)),
            ScalarValue::Float32(Some(v)) => ScalarValue::Float32(Some(-v)),
            ScalarValue::Int8(Some(v)) => ScalarValue::Int8(Some(-v)),
            ScalarValue::Int16(Some(v)) => ScalarValue::Int16(Some(-v)),
            ScalarValue::Int32(Some(v)) => ScalarValue::Int32(Some(-v)),
            ScalarValue::Int64(Some(v)) => ScalarValue::Int64(Some(-v)),
            _ => panic!("Cannot run arithmetic negate on scalar value: {:?}", self),
        }
    }

    /// whether this value is null or not.
    pub fn is_null(&self) -> bool {
        matches!(
            *self,
            ScalarValue::Boolean(None)
                | ScalarValue::UInt8(None)
                | ScalarValue::UInt16(None)
                | ScalarValue::UInt32(None)
                | ScalarValue::UInt64(None)
                | ScalarValue::Int8(None)
                | ScalarValue::Int16(None)
                | ScalarValue::Int32(None)
                | ScalarValue::Int64(None)
                | ScalarValue::Float32(None)
                | ScalarValue::Float64(None)
                | ScalarValue::Utf8(None)
                | ScalarValue::LargeUtf8(None)
                | ScalarValue::List(None, _)
                | ScalarValue::TimestampMillisecond(None)
                | ScalarValue::TimestampMicrosecond(None)
                | ScalarValue::TimestampNanosecond(None)
        )
    }

    /// Converts a scalar value into an 1-row array.
    pub fn to_array(&self) -> ArrayRef {
        self.to_array_of_size(1)
    }

    /// Converts a scalar value into an array of `size` rows.
    pub fn to_array_of_size(&self, size: usize) -> ArrayRef {
        match self {
            ScalarValue::Boolean(e) => {
                Arc::new(BooleanArray::from(vec![*e; size])) as ArrayRef
            }
            ScalarValue::Float64(e) => match e {
                Some(value) => Arc::new(Float64Array::from_value(*value, size)),
                None => new_null_array(&DataType::Float64, size),
            },
            ScalarValue::Float32(e) => match e {
                Some(value) => Arc::new(Float32Array::from_value(*value, size)),
                None => new_null_array(&DataType::Float32, size),
            },
            ScalarValue::Int8(e) => match e {
                Some(value) => Arc::new(Int8Array::from_value(*value, size)),
                None => new_null_array(&DataType::Int8, size),
            },
            ScalarValue::Int16(e) => match e {
                Some(value) => Arc::new(Int16Array::from_value(*value, size)),
                None => new_null_array(&DataType::Int16, size),
            },
            ScalarValue::Int32(e) => match e {
                Some(value) => Arc::new(Int32Array::from_value(*value, size)),
                None => new_null_array(&DataType::Int32, size),
            },
            ScalarValue::Int64(e) => match e {
                Some(value) => Arc::new(Int64Array::from_value(*value, size)),
                None => new_null_array(&DataType::Int64, size),
            },
            ScalarValue::UInt8(e) => match e {
                Some(value) => Arc::new(UInt8Array::from_value(*value, size)),
                None => new_null_array(&DataType::UInt8, size),
            },
            ScalarValue::UInt16(e) => match e {
                Some(value) => Arc::new(UInt16Array::from_value(*value, size)),
                None => new_null_array(&DataType::UInt16, size),
            },
            ScalarValue::UInt32(e) => match e {
                Some(value) => Arc::new(UInt32Array::from_value(*value, size)),
                None => new_null_array(&DataType::UInt32, size),
            },
            ScalarValue::UInt64(e) => match e {
                Some(value) => Arc::new(UInt64Array::from_value(*value, size)),
                None => new_null_array(&DataType::UInt64, size),
            },
            ScalarValue::TimestampSecond(e) => match e {
                Some(value) => Arc::new(TimestampSecondArray::from_iter_values(
                    repeat(*value).take(size),
                )),
                None => {
                    new_null_array(&DataType::Timestamp(TimeUnit::Second, None), size)
                }
            },
            ScalarValue::TimestampMillisecond(e) => match e {
                Some(value) => Arc::new(TimestampMillisecondArray::from_iter_values(
                    repeat(*value).take(size),
                )),
                None => new_null_array(
                    &DataType::Timestamp(TimeUnit::Millisecond, None),
                    size,
                ),
            },
            ScalarValue::TimestampMicrosecond(e) => match e {
                Some(value) => {
                    Arc::new(TimestampMicrosecondArray::from_value(*value, size))
                }
                None => new_null_array(
                    &DataType::Timestamp(TimeUnit::Microsecond, None),
                    size,
                ),
            },
            ScalarValue::TimestampNanosecond(e) => match e {
                Some(value) => {
                    Arc::new(TimestampNanosecondArray::from_value(*value, size))
                }
                None => {
                    new_null_array(&DataType::Timestamp(TimeUnit::Nanosecond, None), size)
                }
            },
            ScalarValue::Utf8(e) => match e {
                Some(value) => {
                    Arc::new(StringArray::from_iter_values(repeat(value).take(size)))
                }
                None => new_null_array(&DataType::Utf8, size),
            },
            ScalarValue::LargeUtf8(e) => match e {
                Some(value) => {
                    Arc::new(LargeStringArray::from_iter_values(repeat(value).take(size)))
                }
                None => new_null_array(&DataType::LargeUtf8, size),
            },
            ScalarValue::Binary(e) => match e {
                Some(value) => Arc::new(
                    repeat(Some(value.as_slice()))
                        .take(size)
                        .collect::<BinaryArray>(),
                ),
                None => {
                    Arc::new(repeat(None::<&str>).take(size).collect::<BinaryArray>())
                }
            },
            ScalarValue::LargeBinary(e) => match e {
                Some(value) => Arc::new(
                    repeat(Some(value.as_slice()))
                        .take(size)
                        .collect::<LargeBinaryArray>(),
                ),
                None => Arc::new(
                    repeat(None::<&str>)
                        .take(size)
                        .collect::<LargeBinaryArray>(),
                ),
            },
            ScalarValue::List(values, data_type) => Arc::new(match data_type {
                DataType::Boolean => build_list!(BooleanBuilder, Boolean, values, size),
                DataType::Int8 => build_list!(Int8Builder, Int8, values, size),
                DataType::Int16 => build_list!(Int16Builder, Int16, values, size),
                DataType::Int32 => build_list!(Int32Builder, Int32, values, size),
                DataType::Int64 => build_list!(Int64Builder, Int64, values, size),
                DataType::UInt8 => build_list!(UInt8Builder, UInt8, values, size),
                DataType::UInt16 => build_list!(UInt16Builder, UInt16, values, size),
                DataType::UInt32 => build_list!(UInt32Builder, UInt32, values, size),
                DataType::UInt64 => build_list!(UInt64Builder, UInt64, values, size),
                DataType::Utf8 => build_list!(StringBuilder, Utf8, values, size),
                DataType::Float32 => build_list!(Float32Builder, Float32, values, size),
                DataType::Float64 => build_list!(Float64Builder, Float64, values, size),
                DataType::LargeUtf8 => {
                    build_list!(LargeStringBuilder, LargeUtf8, values, size)
                }
                _ => panic!("Unexpected DataType for list"),
            }),
            ScalarValue::Date32(e) => match e {
                Some(value) => Arc::new(Date32Array::from_value(*value, size)),
                None => new_null_array(&DataType::Date32, size),
            },
            ScalarValue::Date64(e) => match e {
                Some(value) => Arc::new(Date64Array::from_value(*value, size)),
                None => new_null_array(&DataType::Date64, size),
            },
            ScalarValue::IntervalDayTime(e) => match e {
                Some(value) => Arc::new(IntervalDayTimeArray::from_value(*value, size)),
                None => new_null_array(&DataType::Interval(IntervalUnit::DayTime), size),
            },
            ScalarValue::IntervalYearMonth(e) => match e {
                Some(value) => Arc::new(IntervalYearMonthArray::from_value(*value, size)),
                None => {
                    new_null_array(&DataType::Interval(IntervalUnit::YearMonth), size)
                }
            },
        }
    }

    /// Converts a value in `array` at `index` into a ScalarValue
    pub fn try_from_array(array: &ArrayRef, index: usize) -> Result<Self> {
        Ok(match array.data_type() {
            DataType::Boolean => typed_cast!(array, index, BooleanArray, Boolean),
            DataType::Float64 => typed_cast!(array, index, Float64Array, Float64),
            DataType::Float32 => typed_cast!(array, index, Float32Array, Float32),
            DataType::UInt64 => typed_cast!(array, index, UInt64Array, UInt64),
            DataType::UInt32 => typed_cast!(array, index, UInt32Array, UInt32),
            DataType::UInt16 => typed_cast!(array, index, UInt16Array, UInt16),
            DataType::UInt8 => typed_cast!(array, index, UInt8Array, UInt8),
            DataType::Int64 => typed_cast!(array, index, Int64Array, Int64),
            DataType::Int32 => typed_cast!(array, index, Int32Array, Int32),
            DataType::Int16 => typed_cast!(array, index, Int16Array, Int16),
            DataType::Int8 => typed_cast!(array, index, Int8Array, Int8),
            DataType::Utf8 => typed_cast!(array, index, StringArray, Utf8),
            DataType::LargeUtf8 => typed_cast!(array, index, LargeStringArray, LargeUtf8),
            DataType::List(nested_type) => {
                let list_array =
                    array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                        DataFusionError::Internal(
                            "Failed to downcast ListArray".to_string(),
                        )
                    })?;
                let value = match list_array.is_null(index) {
                    true => None,
                    false => {
                        let nested_array = list_array.value(index);
                        let scalar_vec = (0..nested_array.len())
                            .map(|i| ScalarValue::try_from_array(&nested_array, i))
                            .collect::<Result<Vec<_>>>()?;
                        Some(scalar_vec)
                    }
                };
                ScalarValue::List(value, nested_type.data_type().clone())
            }
            DataType::Date32 => {
                typed_cast!(array, index, Date32Array, Date32)
            }
            DataType::Date64 => {
                typed_cast!(array, index, Date64Array, Date64)
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                typed_cast!(array, index, TimestampSecondArray, TimestampSecond)
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                typed_cast!(
                    array,
                    index,
                    TimestampMillisecondArray,
                    TimestampMillisecond
                )
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                typed_cast!(
                    array,
                    index,
                    TimestampMicrosecondArray,
                    TimestampMicrosecond
                )
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                typed_cast!(array, index, TimestampNanosecondArray, TimestampNanosecond)
            }
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Can't create a scalar of array of type \"{:?}\"",
                    other
                )))
            }
        })
    }
}

impl From<f64> for ScalarValue {
    fn from(value: f64) -> Self {
        ScalarValue::Float64(Some(value))
    }
}

impl From<f32> for ScalarValue {
    fn from(value: f32) -> Self {
        ScalarValue::Float32(Some(value))
    }
}

impl From<i8> for ScalarValue {
    fn from(value: i8) -> Self {
        ScalarValue::Int8(Some(value))
    }
}

impl From<i16> for ScalarValue {
    fn from(value: i16) -> Self {
        ScalarValue::Int16(Some(value))
    }
}

impl From<i32> for ScalarValue {
    fn from(value: i32) -> Self {
        ScalarValue::Int32(Some(value))
    }
}

impl From<i64> for ScalarValue {
    fn from(value: i64) -> Self {
        ScalarValue::Int64(Some(value))
    }
}

impl From<bool> for ScalarValue {
    fn from(value: bool) -> Self {
        ScalarValue::Boolean(Some(value))
    }
}

impl From<u8> for ScalarValue {
    fn from(value: u8) -> Self {
        ScalarValue::UInt8(Some(value))
    }
}

impl From<u16> for ScalarValue {
    fn from(value: u16) -> Self {
        ScalarValue::UInt16(Some(value))
    }
}

impl From<u32> for ScalarValue {
    fn from(value: u32) -> Self {
        ScalarValue::UInt32(Some(value))
    }
}

impl From<u64> for ScalarValue {
    fn from(value: u64) -> Self {
        ScalarValue::UInt64(Some(value))
    }
}

macro_rules! impl_try_from {
    ($SCALAR:ident, $NATIVE:ident) => {
        impl TryFrom<ScalarValue> for $NATIVE {
            type Error = DataFusionError;

            fn try_from(value: ScalarValue) -> Result<Self> {
                match value {
                    ScalarValue::$SCALAR(Some(inner_value)) => Ok(inner_value),
                    _ => Err(DataFusionError::Internal(format!(
                        "Cannot convert {:?} to {}",
                        value,
                        std::any::type_name::<Self>()
                    ))),
                }
            }
        }
    };
}

impl_try_from!(Int8, i8);
impl_try_from!(Int16, i16);

// special implementation for i32 because of Date32
impl TryFrom<ScalarValue> for i32 {
    type Error = DataFusionError;

    fn try_from(value: ScalarValue) -> Result<Self> {
        match value {
            ScalarValue::Int32(Some(inner_value))
            | ScalarValue::Date32(Some(inner_value)) => Ok(inner_value),
            _ => Err(DataFusionError::Internal(format!(
                "Cannot convert {:?} to {}",
                value,
                std::any::type_name::<Self>()
            ))),
        }
    }
}

// special implementation for i64 because of TimeNanosecond
impl TryFrom<ScalarValue> for i64 {
    type Error = DataFusionError;

    fn try_from(value: ScalarValue) -> Result<Self> {
        match value {
            ScalarValue::Int64(Some(inner_value))
            | ScalarValue::TimestampNanosecond(Some(inner_value)) => Ok(inner_value),
            _ => Err(DataFusionError::Internal(format!(
                "Cannot convert {:?} to {}",
                value,
                std::any::type_name::<Self>()
            ))),
        }
    }
}

impl_try_from!(UInt8, u8);
impl_try_from!(UInt16, u16);
impl_try_from!(UInt32, u32);
impl_try_from!(UInt64, u64);
impl_try_from!(Float32, f32);
impl_try_from!(Float64, f64);
impl_try_from!(Boolean, bool);

impl TryFrom<&DataType> for ScalarValue {
    type Error = DataFusionError;

    fn try_from(datatype: &DataType) -> Result<Self> {
        Ok(match datatype {
            DataType::Boolean => ScalarValue::Boolean(None),
            DataType::Float64 => ScalarValue::Float64(None),
            DataType::Float32 => ScalarValue::Float32(None),
            DataType::Int8 => ScalarValue::Int8(None),
            DataType::Int16 => ScalarValue::Int16(None),
            DataType::Int32 => ScalarValue::Int32(None),
            DataType::Int64 => ScalarValue::Int64(None),
            DataType::UInt8 => ScalarValue::UInt8(None),
            DataType::UInt16 => ScalarValue::UInt16(None),
            DataType::UInt32 => ScalarValue::UInt32(None),
            DataType::UInt64 => ScalarValue::UInt64(None),
            DataType::Utf8 => ScalarValue::Utf8(None),
            DataType::LargeUtf8 => ScalarValue::LargeUtf8(None),
            DataType::Timestamp(TimeUnit::Second, _) => {
                ScalarValue::TimestampSecond(None)
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                ScalarValue::TimestampMillisecond(None)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                ScalarValue::TimestampMicrosecond(None)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                ScalarValue::TimestampNanosecond(None)
            }
            DataType::List(ref nested_type) => {
                ScalarValue::List(None, nested_type.data_type().clone())
            }
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Can't create a scalar of type \"{:?}\"",
                    datatype
                )))
            }
        })
    }
}

macro_rules! format_option {
    ($F:expr, $EXPR:expr) => {{
        match $EXPR {
            Some(e) => write!($F, "{}", e),
            None => write!($F, "NULL"),
        }
    }};
}

impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ScalarValue::Boolean(e) => format_option!(f, e)?,
            ScalarValue::Float32(e) => format_option!(f, e)?,
            ScalarValue::Float64(e) => format_option!(f, e)?,
            ScalarValue::Int8(e) => format_option!(f, e)?,
            ScalarValue::Int16(e) => format_option!(f, e)?,
            ScalarValue::Int32(e) => format_option!(f, e)?,
            ScalarValue::Int64(e) => format_option!(f, e)?,
            ScalarValue::UInt8(e) => format_option!(f, e)?,
            ScalarValue::UInt16(e) => format_option!(f, e)?,
            ScalarValue::UInt32(e) => format_option!(f, e)?,
            ScalarValue::UInt64(e) => format_option!(f, e)?,
            ScalarValue::TimestampSecond(e) => format_option!(f, e)?,
            ScalarValue::TimestampMillisecond(e) => format_option!(f, e)?,
            ScalarValue::TimestampMicrosecond(e) => format_option!(f, e)?,
            ScalarValue::TimestampNanosecond(e) => format_option!(f, e)?,
            ScalarValue::Utf8(e) => format_option!(f, e)?,
            ScalarValue::LargeUtf8(e) => format_option!(f, e)?,
            ScalarValue::Binary(e) => match e {
                Some(l) => write!(
                    f,
                    "{}",
                    l.iter()
                        .map(|v| format!("{}", v))
                        .collect::<Vec<_>>()
                        .join(",")
                )?,
                None => write!(f, "NULL")?,
            },
            ScalarValue::LargeBinary(e) => match e {
                Some(l) => write!(
                    f,
                    "{}",
                    l.iter()
                        .map(|v| format!("{}", v))
                        .collect::<Vec<_>>()
                        .join(",")
                )?,
                None => write!(f, "NULL")?,
            },
            ScalarValue::List(e, _) => match e {
                Some(l) => write!(
                    f,
                    "{}",
                    l.iter()
                        .map(|v| format!("{}", v))
                        .collect::<Vec<_>>()
                        .join(",")
                )?,
                None => write!(f, "NULL")?,
            },
            ScalarValue::Date32(e) => format_option!(f, e)?,
            ScalarValue::Date64(e) => format_option!(f, e)?,
            ScalarValue::IntervalDayTime(e) => format_option!(f, e)?,
            ScalarValue::IntervalYearMonth(e) => format_option!(f, e)?,
        };
        Ok(())
    }
}

impl fmt::Debug for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScalarValue::Boolean(_) => write!(f, "Boolean({})", self),
            ScalarValue::Float32(_) => write!(f, "Float32({})", self),
            ScalarValue::Float64(_) => write!(f, "Float64({})", self),
            ScalarValue::Int8(_) => write!(f, "Int8({})", self),
            ScalarValue::Int16(_) => write!(f, "Int16({})", self),
            ScalarValue::Int32(_) => write!(f, "Int32({})", self),
            ScalarValue::Int64(_) => write!(f, "Int64({})", self),
            ScalarValue::UInt8(_) => write!(f, "UInt8({})", self),
            ScalarValue::UInt16(_) => write!(f, "UInt16({})", self),
            ScalarValue::UInt32(_) => write!(f, "UInt32({})", self),
            ScalarValue::UInt64(_) => write!(f, "UInt64({})", self),
            ScalarValue::TimestampSecond(_) => write!(f, "TimestampSecond({})", self),
            ScalarValue::TimestampMillisecond(_) => {
                write!(f, "TimestampMillisecond({})", self)
            }
            ScalarValue::TimestampMicrosecond(_) => {
                write!(f, "TimestampMicrosecond({})", self)
            }
            ScalarValue::TimestampNanosecond(_) => {
                write!(f, "TimestampNanosecond({})", self)
            }
            ScalarValue::Utf8(None) => write!(f, "Utf8({})", self),
            ScalarValue::Utf8(Some(_)) => write!(f, "Utf8(\"{}\")", self),
            ScalarValue::LargeUtf8(None) => write!(f, "LargeUtf8({})", self),
            ScalarValue::LargeUtf8(Some(_)) => write!(f, "LargeUtf8(\"{}\")", self),
            ScalarValue::Binary(None) => write!(f, "Binary({})", self),
            ScalarValue::Binary(Some(_)) => write!(f, "Binary(\"{}\")", self),
            ScalarValue::LargeBinary(None) => write!(f, "LargeBinary({})", self),
            ScalarValue::LargeBinary(Some(_)) => write!(f, "LargeBinary(\"{}\")", self),
            ScalarValue::List(_, _) => write!(f, "List([{}])", self),
            ScalarValue::Date32(_) => write!(f, "Date32(\"{}\")", self),
            ScalarValue::Date64(_) => write!(f, "Date64(\"{}\")", self),
            ScalarValue::IntervalDayTime(_) => {
                write!(f, "IntervalDayTime(\"{}\")", self)
            }
            ScalarValue::IntervalYearMonth(_) => {
                write!(f, "IntervalYearMonth(\"{}\")", self)
            }
        }
    }
}

/// Trait used to map a NativeTime to a ScalarType.
pub trait ScalarType<T: ArrowNativeType> {
    /// returns a scalar from an optional T
    fn scalar(r: Option<T>) -> ScalarValue;
}

impl ScalarType<f32> for Float32Type {
    fn scalar(r: Option<f32>) -> ScalarValue {
        ScalarValue::Float32(r)
    }
}

impl ScalarType<i64> for TimestampSecondType {
    fn scalar(r: Option<i64>) -> ScalarValue {
        ScalarValue::TimestampSecond(r)
    }
}

impl ScalarType<i64> for TimestampMillisecondType {
    fn scalar(r: Option<i64>) -> ScalarValue {
        ScalarValue::TimestampMillisecond(r)
    }
}

impl ScalarType<i64> for TimestampMicrosecondType {
    fn scalar(r: Option<i64>) -> ScalarValue {
        ScalarValue::TimestampMicrosecond(r)
    }
}

impl ScalarType<i64> for TimestampNanosecondType {
    fn scalar(r: Option<i64>) -> ScalarValue {
        ScalarValue::TimestampNanosecond(r)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_list_null_to_array() {
        let list_array_ref = ScalarValue::List(None, DataType::UInt64).to_array();
        let list_array = list_array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        assert!(list_array.is_null(0));
        assert_eq!(list_array.len(), 1);
        assert_eq!(list_array.values().len(), 0);
    }

    #[test]
    fn scalar_list_to_array() {
        let list_array_ref = ScalarValue::List(
            Some(vec![
                ScalarValue::UInt64(Some(100)),
                ScalarValue::UInt64(None),
                ScalarValue::UInt64(Some(101)),
            ]),
            DataType::UInt64,
        )
        .to_array();

        let list_array = list_array_ref.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list_array.len(), 1);
        assert_eq!(list_array.values().len(), 3);

        let prim_array_ref = list_array.value(0);
        let prim_array = prim_array_ref
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(prim_array.len(), 3);
        assert_eq!(prim_array.value(0), 100);
        assert!(prim_array.is_null(1));
        assert_eq!(prim_array.value(2), 101);
    }
}
