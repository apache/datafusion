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

use arrow::{
    array::*,
    bitmap::MutableBitmap,
    buffer::MutableBuffer,
    datatypes::{DataType, IntervalUnit, TimeUnit},
    error::{ArrowError, Result as ArrowResult},
    types::days_ms,
};

type StringArray = Utf8Array<i32>;
type LargeStringArray = Utf8Array<i64>;
type SmallBinaryArray = BinaryArray<i32>;
type LargeBinaryArray = BinaryArray<i64>;

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
    // 1st argument are the inner values (e.g. Int64Array)
    // 2st argument is the Lists' datatype (i.e. it includes `Field`)
    // to downcast inner values, use ListArray::<i32>::get_child()
    List(Option<Arc<dyn Array>>, DataType),
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
    IntervalDayTime(Option<days_ms>),
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

macro_rules! dyn_to_array {
    ($self:expr, $value:expr, $size:expr, $ty:ty) => {{
        Arc::new(PrimitiveArray::<$ty>::from_data(
            $self.get_datatype(),
            MutableBuffer::<$ty>::from_trusted_len_iter(repeat(*$value).take($size))
                .into(),
            None,
        ))
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
            ScalarValue::List(_, data_type) => data_type.clone(),
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

    /// Converts an iterator of references [`ScalarValue`] into an [`ArrayRef`]
    /// corresponding to those values. For example,
    ///
    /// Returns an error if the iterator is empty or if the
    /// [`ScalarValue`]s are not all the same type
    ///
    /// Example
    /// ```
    /// use datafusion::scalar::ScalarValue;
    /// use arrow::array::{ArrayRef, BooleanArray};
    ///
    /// let scalars = vec![
    ///   ScalarValue::Boolean(Some(true)),
    ///   ScalarValue::Boolean(None),
    ///   ScalarValue::Boolean(Some(false)),
    /// ];
    ///
    /// // Build an Array from the list of ScalarValues
    /// let array = ScalarValue::iter_to_array(scalars.into_iter())
    ///   .unwrap();
    ///
    /// let expected: ArrayRef = std::sync::Arc::new(
    ///   BooleanArray::from(vec![
    ///     Some(true),
    ///     None,
    ///     Some(false)
    ///   ]
    /// ));
    ///
    /// assert_eq!(&array, &expected);
    /// ```
    pub fn iter_to_array(
        scalars: impl IntoIterator<Item = ScalarValue>,
    ) -> Result<ArrayRef> {
        let mut scalars = scalars.into_iter().peekable();

        // figure out the type based on the first element
        let data_type = match scalars.peek() {
            None => {
                return Err(DataFusionError::Internal(
                    "Empty iterator passed to ScalarValue::iter_to_array".to_string(),
                ))
            }
            Some(sv) => sv.get_datatype(),
        };

        /// Creates an array of $ARRAY_TY by unpacking values of
        /// SCALAR_TY for primitive types
        macro_rules! build_array_primitive {
            ($TY:ty, $SCALAR_TY:ident, $DT:ident) => {{
                {
                    Arc::new(scalars
                        .map(|sv| {
                            if let ScalarValue::$SCALAR_TY(v) = sv {
                                Ok(v)
                            } else {
                                Err(DataFusionError::Internal(format!(
                                    "Inconsistent types in ScalarValue::iter_to_array. \
                                     Expected {:?}, got {:?}",
                                    data_type, sv
                                )))
                            }
                        })
                        .collect::<Result<PrimitiveArray<$TY>>>()?.to($DT)
                        ) as ArrayRef
                }
            }};
        }

        /// Creates an array of $ARRAY_TY by unpacking values of
        /// SCALAR_TY for "string-like" types.
        macro_rules! build_array_string {
            ($ARRAY_TY:ident, $SCALAR_TY:ident) => {{
                {
                    let array = scalars
                        .map(|sv| {
                            if let ScalarValue::$SCALAR_TY(v) = sv {
                                Ok(v)
                            } else {
                                Err(DataFusionError::Internal(format!(
                                    "Inconsistent types in ScalarValue::iter_to_array. \
                                     Expected {:?}, got {:?}",
                                    data_type, sv
                                )))
                            }
                        })
                        .collect::<Result<$ARRAY_TY>>()?;
                    Arc::new(array)
                }
            }};
        }

        use DataType::*;
        let array: ArrayRef = match &data_type {
            DataType::Boolean => Arc::new(
                scalars
                    .map(|sv| {
                        if let ScalarValue::Boolean(v) = sv {
                            Ok(v)
                        } else {
                            Err(DataFusionError::Internal(format!(
                                "Inconsistent types in ScalarValue::iter_to_array. \
                                 Expected {:?}, got {:?}",
                                data_type, sv
                            )))
                        }
                    })
                    .collect::<Result<BooleanArray>>()?,
            ),
            Float32 => {
                build_array_primitive!(f32, Float32, Float32)
            }
            Float64 => {
                build_array_primitive!(f64, Float64, Float64)
            }
            Int8 => build_array_primitive!(i8, Int8, Int8),
            Int16 => build_array_primitive!(i16, Int16, Int16),
            Int32 => build_array_primitive!(i32, Int32, Int32),
            Int64 => build_array_primitive!(i64, Int64, Int64),
            UInt8 => build_array_primitive!(u8, UInt8, UInt8),
            UInt16 => build_array_primitive!(u16, UInt16, UInt16),
            UInt32 => build_array_primitive!(u32, UInt32, UInt32),
            UInt64 => build_array_primitive!(u64, UInt64, UInt64),
            Utf8 => build_array_string!(StringArray, Utf8),
            LargeUtf8 => build_array_string!(LargeStringArray, LargeUtf8),
            Binary => build_array_string!(SmallBinaryArray, Binary),
            LargeBinary => build_array_string!(LargeBinaryArray, LargeBinary),
            Date32 => build_array_primitive!(i32, Date32, Date32),
            Date64 => build_array_primitive!(i64, Date64, Date64),
            Timestamp(TimeUnit::Second, None) => {
                build_array_primitive!(i64, TimestampSecond, data_type)
            }
            Timestamp(TimeUnit::Millisecond, None) => {
                build_array_primitive!(i64, TimestampMillisecond, data_type)
            }
            Timestamp(TimeUnit::Microsecond, None) => {
                build_array_primitive!(i64, TimestampMicrosecond, data_type)
            }
            Timestamp(TimeUnit::Nanosecond, None) => {
                build_array_primitive!(i64, TimestampNanosecond, data_type)
            }
            Interval(IntervalUnit::DayTime) => {
                build_array_primitive!(days_ms, IntervalDayTime, data_type)
            }
            Interval(IntervalUnit::YearMonth) => {
                build_array_primitive!(i32, IntervalYearMonth, data_type)
            }
            List(_) => {
                let iter = scalars
                    .map(|sv| {
                        if let ScalarValue::List(v, _) = sv {
                            Ok(v)
                        } else {
                            Err(ArrowError::from_external_error(
                                DataFusionError::Internal(format!(
                                    "Inconsistent types in ScalarValue::iter_to_array. \
                                 Expected {:?}, got {:?}",
                                    data_type, sv
                                )),
                            ))
                        }
                    })
                    .collect::<ArrowResult<Vec<_>>>()?;
                let mut offsets = MutableBuffer::<i32>::with_capacity(1 + iter.len());
                offsets.push(0);
                let mut validity = MutableBitmap::with_capacity(iter.len());
                let mut values = Vec::with_capacity(iter.len());
                iter.iter().fold(0i32, |mut length, x| {
                    if let Some(array) = x {
                        length += array.len() as i32;
                        values.push(array.as_ref());
                        validity.push(true)
                    } else {
                        validity.push(false)
                    };
                    offsets.push(length);
                    length
                });
                let values = arrow::compute::concat::concatenate(&values)?;
                Arc::new(ListArray::from_data(
                    data_type,
                    offsets.into(),
                    values.into(),
                    validity.into(),
                ))
            }
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "Unsupported creation of {:?} array from ScalarValue {:?}",
                    data_type,
                    scalars.peek()
                )))
            }
        };

        Ok(array)
    }

    /// Converts a scalar value into an array of `size` rows.
    pub fn to_array_of_size(&self, size: usize) -> ArrayRef {
        match self {
            ScalarValue::Boolean(e) => {
                Arc::new(BooleanArray::from(vec![*e; size])) as ArrayRef
            }
            ScalarValue::Float64(e) => match e {
                Some(value) => dyn_to_array!(self, value, size, f64),
                None => new_null_array(self.get_datatype(), size).into(),
            },
            ScalarValue::Float32(e) => match e {
                Some(value) => dyn_to_array!(self, value, size, f32),
                None => new_null_array(self.get_datatype(), size).into(),
            },
            ScalarValue::Int8(e) => match e {
                Some(value) => dyn_to_array!(self, value, size, i8),
                None => new_null_array(self.get_datatype(), size).into(),
            },
            ScalarValue::Int16(e) => match e {
                Some(value) => dyn_to_array!(self, value, size, i16),
                None => new_null_array(self.get_datatype(), size).into(),
            },
            ScalarValue::Int32(e)
            | ScalarValue::Date32(e)
            | ScalarValue::IntervalYearMonth(e) => match e {
                Some(value) => dyn_to_array!(self, value, size, i32),
                None => new_null_array(self.get_datatype(), size).into(),
            },
            ScalarValue::Int64(e)
            | ScalarValue::Date64(e)
            | ScalarValue::TimestampSecond(e)
            | ScalarValue::TimestampMillisecond(e)
            | ScalarValue::TimestampMicrosecond(e)
            | ScalarValue::TimestampNanosecond(e) => match e {
                Some(value) => dyn_to_array!(self, value, size, i64),
                None => new_null_array(self.get_datatype(), size).into(),
            },
            ScalarValue::UInt8(e) => match e {
                Some(value) => dyn_to_array!(self, value, size, u8),
                None => new_null_array(self.get_datatype(), size).into(),
            },
            ScalarValue::UInt16(e) => match e {
                Some(value) => dyn_to_array!(self, value, size, u16),
                None => new_null_array(self.get_datatype(), size).into(),
            },
            ScalarValue::UInt32(e) => match e {
                Some(value) => dyn_to_array!(self, value, size, u32),
                None => new_null_array(self.get_datatype(), size).into(),
            },
            ScalarValue::UInt64(e) => match e {
                Some(value) => dyn_to_array!(self, value, size, u64),
                None => new_null_array(self.get_datatype(), size).into(),
            },
            ScalarValue::Utf8(e) => match e {
                Some(value) => Arc::new(Utf8Array::<i32>::from_trusted_len_values_iter(
                    repeat(&value).take(size),
                )),
                None => new_null_array(self.get_datatype(), size).into(),
            },
            ScalarValue::LargeUtf8(e) => match e {
                Some(value) => Arc::new(Utf8Array::<i64>::from_trusted_len_values_iter(
                    repeat(&value).take(size),
                )),
                None => new_null_array(self.get_datatype(), size).into(),
            },
            ScalarValue::Binary(e) => match e {
                Some(value) => Arc::new(
                    repeat(Some(value.as_slice()))
                        .take(size)
                        .collect::<BinaryArray<i32>>(),
                ),
                None => new_null_array(self.get_datatype(), size).into(),
            },
            ScalarValue::LargeBinary(e) => match e {
                Some(value) => Arc::new(
                    repeat(Some(value.as_slice()))
                        .take(size)
                        .collect::<BinaryArray<i64>>(),
                ),
                None => new_null_array(self.get_datatype(), size).into(),
            },
            ScalarValue::List(values, data_type) => {
                if let Some(values) = values {
                    let length = values.len();
                    let refs = std::iter::repeat(values.as_ref())
                        .take(size)
                        .collect::<Vec<_>>();
                    let values =
                        arrow::compute::concat::concatenate(&refs).unwrap().into();
                    let offsets: arrow::buffer::Buffer<i32> =
                        (0..=size).map(|i| (i * length) as i32).collect();
                    Arc::new(ListArray::from_data(
                        data_type.clone(),
                        offsets,
                        values,
                        None,
                    ))
                } else {
                    new_null_array(self.get_datatype(), size).into()
                }
            }
            ScalarValue::IntervalDayTime(e) => match e {
                Some(value) => {
                    Arc::new(PrimitiveArray::<days_ms>::from_trusted_len_values_iter(
                        std::iter::repeat(*value).take(size),
                    ))
                }
                None => new_null_array(self.get_datatype(), size).into(),
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
            DataType::List(_) => {
                let list_array = array
                    .as_any()
                    .downcast_ref::<ListArray<i32>>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "Failed to downcast ListArray".to_string(),
                        )
                    })?;
                let is_valid = list_array.is_valid(index);
                let value = if is_valid {
                    Some(list_array.value(index).into())
                } else {
                    None
                };
                ScalarValue::List(value, array.data_type().clone())
            }
            DataType::Date32 => {
                typed_cast!(array, index, Int32Array, Date32)
            }
            DataType::Date64 => {
                typed_cast!(array, index, Int64Array, Date64)
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                typed_cast!(array, index, Int64Array, TimestampSecond)
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                typed_cast!(array, index, Int64Array, TimestampMillisecond)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                typed_cast!(array, index, Int64Array, TimestampMicrosecond)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                typed_cast!(array, index, Int64Array, TimestampNanosecond)
            }
            DataType::Dictionary(index_type, _) => match **index_type {
                DataType::Int8 => Self::try_from_dict_array::<i8>(array, index)?,
                DataType::Int16 => Self::try_from_dict_array::<i16>(array, index)?,
                DataType::Int32 => Self::try_from_dict_array::<i32>(array, index)?,
                DataType::Int64 => Self::try_from_dict_array::<i64>(array, index)?,
                DataType::UInt8 => Self::try_from_dict_array::<u8>(array, index)?,
                DataType::UInt16 => Self::try_from_dict_array::<u16>(array, index)?,
                DataType::UInt32 => Self::try_from_dict_array::<u32>(array, index)?,
                DataType::UInt64 => Self::try_from_dict_array::<u64>(array, index)?,
                _ => {
                    return Err(DataFusionError::Internal(format!(
                    "Index type not supported while creating scalar from dictionary: {}",
                    array.data_type(),
                )))
                }
            },
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Can't create a scalar from array of type \"{:?}\"",
                    other
                )))
            }
        })
    }

    fn try_from_dict_array<K: DictionaryKey>(
        array: &ArrayRef,
        index: usize,
    ) -> Result<Self> {
        let dict_array = array.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();

        // look up the index in the values dictionary
        let keys_col = dict_array.keys();
        let values_index = keys_col.value(index).to_usize().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Can not convert index to usize in dictionary of type creating group by value {:?}",
                keys_col.data_type()
            ))
        })?;
        Self::try_from_array(dict_array.values(), values_index)
    }
}

macro_rules! impl_scalar {
    ($ty:ty, $scalar:tt) => {
        impl From<$ty> for ScalarValue {
            fn from(value: $ty) -> Self {
                ScalarValue::$scalar(Some(value))
            }
        }

        impl From<Option<$ty>> for ScalarValue {
            fn from(value: Option<$ty>) -> Self {
                ScalarValue::$scalar(value)
            }
        }
    };
}

impl_scalar!(f64, Float64);
impl_scalar!(f32, Float32);
impl_scalar!(i8, Int8);
impl_scalar!(i16, Int16);
impl_scalar!(i32, Int32);
impl_scalar!(i64, Int64);
impl_scalar!(bool, Boolean);
impl_scalar!(u8, UInt8);
impl_scalar!(u16, UInt16);
impl_scalar!(u32, UInt32);
impl_scalar!(u64, UInt64);

impl From<&str> for ScalarValue {
    fn from(value: &str) -> Self {
        ScalarValue::Utf8(Some(value.to_string()))
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
            | ScalarValue::TimestampNanosecond(Some(inner_value))
            | ScalarValue::TimestampMicrosecond(Some(inner_value))
            | ScalarValue::TimestampMillisecond(Some(inner_value))
            | ScalarValue::TimestampSecond(Some(inner_value)) => Ok(inner_value),
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
            DataType::List(_) => ScalarValue::List(None, datatype.clone()),
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
            ScalarValue::List(e, _) => {
                if let Some(e) = e {
                    write!(f, "{}", e)?
                } else {
                    write!(f, "NULL")?
                }
            }
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

#[cfg(test)]
mod tests {
    use arrow::datatypes::Field;

    use super::*;

    #[test]
    fn scalar_value_to_array_u64() {
        let value = ScalarValue::UInt64(Some(13u64));
        let array = value.to_array();
        let array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(array.len(), 1);
        assert!(!array.is_null(0));
        assert_eq!(array.value(0), 13);

        let value = ScalarValue::UInt64(None);
        let array = value.to_array();
        let array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(array.len(), 1);
        assert!(array.is_null(0));
    }

    #[test]
    fn scalar_value_to_array_u32() {
        let value = ScalarValue::UInt32(Some(13u32));
        let array = value.to_array();
        let array = array.as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(array.len(), 1);
        assert!(!array.is_null(0));
        assert_eq!(array.value(0), 13);

        let value = ScalarValue::UInt32(None);
        let array = value.to_array();
        let array = array.as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(array.len(), 1);
        assert!(array.is_null(0));
    }

    #[test]
    fn scalar_list_null_to_array() {
        let list_array_ref = ScalarValue::List(
            None,
            DataType::List(Box::new(Field::new("", DataType::UInt64, true))),
        )
        .to_array();
        let list_array = list_array_ref
            .as_any()
            .downcast_ref::<ListArray<i32>>()
            .unwrap();

        assert!(list_array.is_null(0));
        assert_eq!(list_array.len(), 1);
        assert_eq!(list_array.values().len(), 0);
    }

    /// Creates array directly and via ScalarValue and ensures they are the same
    macro_rules! check_scalar_iter {
        ($SCALAR_T:ident, $ARRAYTYPE:ident, $INPUT:expr) => {{
            let scalars: Vec<_> =
                $INPUT.iter().map(|v| ScalarValue::$SCALAR_T(*v)).collect();

            let array = ScalarValue::iter_to_array(scalars.into_iter()).unwrap();

            let expected: ArrayRef = Arc::new($ARRAYTYPE::from($INPUT));

            assert_eq!(&array, &expected);
        }};
    }

    /// Creates array directly and via ScalarValue and ensures they
    /// are the same, for string  arrays
    macro_rules! check_scalar_iter_string {
        ($SCALAR_T:ident, $ARRAYTYPE:ident, $INPUT:expr) => {{
            let scalars: Vec<_> = $INPUT
                .iter()
                .map(|v| ScalarValue::$SCALAR_T(v.map(|v| v.to_string())))
                .collect();

            let array = ScalarValue::iter_to_array(scalars.into_iter()).unwrap();

            let expected: ArrayRef = Arc::new($ARRAYTYPE::from($INPUT));

            assert_eq!(&array, &expected);
        }};
    }

    /// Creates array directly and via ScalarValue and ensures they
    /// are the same, for binary arrays
    macro_rules! check_scalar_iter_binary {
        ($SCALAR_T:ident, $ARRAYTYPE:ident, $INPUT:expr) => {{
            let scalars: Vec<_> = $INPUT
                .iter()
                .map(|v| ScalarValue::$SCALAR_T(v.map(|v| v.to_vec())))
                .collect();

            let array = ScalarValue::iter_to_array(scalars.into_iter()).unwrap();

            let expected: $ARRAYTYPE =
                $INPUT.iter().map(|v| v.map(|v| v.to_vec())).collect();

            let expected: ArrayRef = Arc::new(expected);

            assert_eq!(&array, &expected);
        }};
    }

    #[test]
    fn scalar_iter_to_array_boolean() {
        check_scalar_iter!(Boolean, BooleanArray, vec![Some(true), None, Some(false)]);
        check_scalar_iter!(Float32, Float32Array, vec![Some(1.9), None, Some(-2.1)]);
        check_scalar_iter!(Float64, Float64Array, vec![Some(1.9), None, Some(-2.1)]);

        check_scalar_iter!(Int8, Int8Array, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(Int16, Int16Array, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(Int32, Int32Array, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(Int64, Int64Array, vec![Some(1), None, Some(3)]);

        check_scalar_iter!(UInt8, UInt8Array, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(UInt16, UInt16Array, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(UInt32, UInt32Array, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(UInt64, UInt64Array, vec![Some(1), None, Some(3)]);

        check_scalar_iter_string!(
            Utf8,
            StringArray,
            vec![Some("foo"), None, Some("bar")]
        );
        check_scalar_iter_string!(
            LargeUtf8,
            LargeStringArray,
            vec![Some("foo"), None, Some("bar")]
        );
        check_scalar_iter_binary!(
            Binary,
            SmallBinaryArray,
            vec![Some(b"foo"), None, Some(b"bar")]
        );
        check_scalar_iter_binary!(
            LargeBinary,
            LargeBinaryArray,
            vec![Some(b"foo"), None, Some(b"bar")]
        );
    }

    #[test]
    fn scalar_iter_to_array_empty() {
        let scalars = vec![] as Vec<ScalarValue>;

        let result = ScalarValue::iter_to_array(scalars.into_iter()).unwrap_err();
        assert!(
            result
                .to_string()
                .contains("Empty iterator passed to ScalarValue::iter_to_array"),
            "{}",
            result
        );
    }

    #[test]
    fn scalar_iter_to_array_mismatched_types() {
        use ScalarValue::*;
        // If the scalar values are not all the correct type, error here
        let scalars: Vec<ScalarValue> = vec![Boolean(Some(true)), Int32(Some(5))];

        let result = ScalarValue::iter_to_array(scalars.into_iter()).unwrap_err();
        assert!(result.to_string().contains("Inconsistent types in ScalarValue::iter_to_array. Expected Boolean, got Int32(5)"),
                "{}", result);
    }
}
