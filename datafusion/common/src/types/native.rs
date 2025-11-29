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

use super::{
    LogicalField, LogicalFieldRef, LogicalFields, LogicalType, LogicalUnionFields,
    TypeSignature,
};
use crate::error::{_internal_err, Result};
use arrow::compute::can_cast_types;
use arrow::datatypes::{
    DECIMAL32_MAX_PRECISION, DECIMAL64_MAX_PRECISION, DECIMAL128_MAX_PRECISION, DataType,
    Field, FieldRef, Fields, IntervalUnit, TimeUnit, UnionFields,
};
use std::{fmt::Display, sync::Arc};

/// Representation of a type that DataFusion can handle natively. It is a subset
/// of the physical variants in Arrow's native [`DataType`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum NativeType {
    /// Null type
    Null,
    /// A boolean type representing the values `true` and `false`.
    Boolean,
    /// A signed 8-bit integer.
    Int8,
    /// A signed 16-bit integer.
    Int16,
    /// A signed 32-bit integer.
    Int32,
    /// A signed 64-bit integer.
    Int64,
    /// An unsigned 8-bit integer.
    UInt8,
    /// An unsigned 16-bit integer.
    UInt16,
    /// An unsigned 32-bit integer.
    UInt32,
    /// An unsigned 64-bit integer.
    UInt64,
    /// A 16-bit floating point number.
    Float16,
    /// A 32-bit floating point number.
    Float32,
    /// A 64-bit floating point number.
    Float64,
    /// A timestamp with an optional timezone.
    ///
    /// Time is measured as a Unix epoch, counting the seconds from
    /// 00:00:00.000 on 1 January 1970, excluding leap seconds,
    /// as a signed 64-bit integer.
    ///
    /// The time zone is a string indicating the name of a time zone, one of:
    ///
    /// * As used in the Olson time zone database (the "tz database" or
    ///   "tzdata"), such as "America/New_York"
    /// * An absolute time zone offset of the form +XX:XX or -XX:XX, such as +07:30
    ///
    /// Timestamps with a non-empty timezone
    /// ------------------------------------
    ///
    /// If a Timestamp column has a non-empty timezone value, its epoch is
    /// 1970-01-01 00:00:00 (January 1st 1970, midnight) in the *UTC* timezone
    /// (the Unix epoch), regardless of the Timestamp's own timezone.
    ///
    /// Therefore, timestamp values with a non-empty timezone correspond to
    /// physical points in time together with some additional information about
    /// how the data was obtained and/or how to display it (the timezone).
    ///
    ///   For example, the timestamp value 0 with the timezone string "Europe/Paris"
    ///   corresponds to "January 1st 1970, 00h00" in the UTC timezone, but the
    ///   application may prefer to display it as "January 1st 1970, 01h00" in
    ///   the Europe/Paris timezone (which is the same physical point in time).
    ///
    /// One consequence is that timestamp values with a non-empty timezone
    /// can be compared and ordered directly, since they all share the same
    /// well-known point of reference (the Unix epoch).
    ///
    /// Timestamps with an unset / empty timezone
    /// -----------------------------------------
    ///
    /// If a Timestamp column has no timezone value, its epoch is
    /// 1970-01-01 00:00:00 (January 1st 1970, midnight) in an *unknown* timezone.
    ///
    /// Therefore, timestamp values without a timezone cannot be meaningfully
    /// interpreted as physical points in time, but only as calendar / clock
    /// indications ("wall clock time") in an unspecified timezone.
    ///
    ///   For example, the timestamp value 0 with an empty timezone string
    ///   corresponds to "January 1st 1970, 00h00" in an unknown timezone: there
    ///   is not enough information to interpret it as a well-defined physical
    ///   point in time.
    ///
    /// One consequence is that timestamp values without a timezone cannot
    /// be reliably compared or ordered, since they may have different points of
    /// reference.  In particular, it is *not* possible to interpret an unset
    /// or empty timezone as the same as "UTC".
    ///
    /// Conversion between timezones
    /// ----------------------------
    ///
    /// If a Timestamp column has a non-empty timezone, changing the timezone
    /// to a different non-empty value is a metadata-only operation:
    /// the timestamp values need not change as their point of reference remains
    /// the same (the Unix epoch).
    ///
    /// However, if a Timestamp column has no timezone value, changing it to a
    /// non-empty value requires to think about the desired semantics.
    /// One possibility is to assume that the original timestamp values are
    /// relative to the epoch of the timezone being set; timestamp values should
    /// then adjusted to the Unix epoch (for example, changing the timezone from
    /// empty to "Europe/Paris" would require converting the timestamp values
    /// from "Europe/Paris" to "UTC", which seems counter-intuitive but is
    /// nevertheless correct).
    ///
    /// ```
    /// # use arrow::datatypes::{DataType, TimeUnit};
    /// DataType::Timestamp(TimeUnit::Second, None);
    /// DataType::Timestamp(TimeUnit::Second, Some("literal".into()));
    /// DataType::Timestamp(TimeUnit::Second, Some("string".to_string().into()));
    /// ```
    Timestamp(TimeUnit, Option<Arc<str>>),
    /// A signed date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in days.
    Date,
    /// A signed time representing the elapsed time since midnight in the unit of `TimeUnit`.
    Time(TimeUnit),
    /// Measure of elapsed time in either seconds, milliseconds, microseconds or nanoseconds.
    Duration(TimeUnit),
    /// A "calendar" interval which models types that don't necessarily
    /// have a precise duration without the context of a base timestamp (e.g.
    /// days can differ in length during day light savings time transitions).
    Interval(IntervalUnit),
    /// Opaque binary data of variable length.
    Binary,
    /// Opaque binary data of fixed size.
    /// Enum parameter specifies the number of bytes per value.
    FixedSizeBinary(i32),
    /// A variable-length string in Unicode with UTF-8 encoding.
    String,
    /// A list of some logical data type with variable length.
    List(LogicalFieldRef),
    /// A list of some logical data type with fixed length.
    FixedSizeList(LogicalFieldRef, i32),
    /// A nested type that contains a number of sub-fields.
    Struct(LogicalFields),
    /// A nested type that can represent slots of differing types.
    Union(LogicalUnionFields),
    /// Decimal value with precision and scale
    ///
    /// * precision is the total number of digits
    /// * scale is the number of digits past the decimal
    ///
    /// For example the number 123.45 has precision 5 and scale 2.
    ///
    /// In certain situations, scale could be negative number. For
    /// negative scale, it is the number of padding 0 to the right
    /// of the digits.
    ///
    /// For example the number 12300 could be treated as a decimal
    /// has precision 3 and scale -2.
    Decimal(u8, i8),
    /// A Map is a type that an association between a key and a value.
    ///
    /// The key and value types are not constrained, but keys should be
    /// hashable and unique.
    ///
    /// In a field with Map type, key type and the second the value type. The names of the
    /// child fields may be respectively "entries", "key", and "value", but this is
    /// not enforced.
    Map(LogicalFieldRef),
}

impl Display for NativeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}") // TODO: nicer formatting
    }
}

impl LogicalType for NativeType {
    fn native(&self) -> &NativeType {
        self
    }

    fn signature(&self) -> TypeSignature<'_> {
        TypeSignature::Native(self)
    }

    /// Returns the default casted type for the given arrow type
    ///
    /// For types like String or Date, multiple arrow types mapped to the same logical type
    /// If the given arrow type is one of them, we return the same type
    /// Otherwise, we define the default casted type for the given arrow type
    fn default_cast_for(&self, origin: &DataType) -> Result<DataType> {
        use DataType::*;

        fn default_field_cast(to: &LogicalField, from: &Field) -> Result<FieldRef> {
            Ok(Arc::new(Field::new(
                to.name.clone(),
                to.logical_type.default_cast_for(from.data_type())?,
                to.nullable,
            )))
        }

        Ok(match (self, origin) {
            (Self::Null, _) => Null,
            (Self::Boolean, _) => Boolean,
            (Self::Int8, _) => Int8,
            (Self::Int16, _) => Int16,
            (Self::Int32, _) => Int32,
            (Self::Int64, _) => Int64,
            (Self::UInt8, _) => UInt8,
            (Self::UInt16, _) => UInt16,
            (Self::UInt32, _) => UInt32,
            (Self::UInt64, _) => UInt64,
            (Self::Float16, _) => Float16,
            (Self::Float32, _) => Float32,
            (Self::Float64, _) => Float64,
            (Self::Decimal(p, s), _) if *p <= DECIMAL32_MAX_PRECISION => {
                Decimal32(*p, *s)
            }
            (Self::Decimal(p, s), _) if *p <= DECIMAL64_MAX_PRECISION => {
                Decimal64(*p, *s)
            }
            (Self::Decimal(p, s), _) if *p <= DECIMAL128_MAX_PRECISION => {
                Decimal128(*p, *s)
            }
            (Self::Decimal(p, s), _) => Decimal256(*p, *s),
            (Self::Timestamp(tu, tz), _) => Timestamp(*tu, tz.clone()),
            // If given type is Date, return the same type
            (Self::Date, Date32 | Date64) => origin.to_owned(),
            (Self::Date, _) => Date32,
            (Self::Time(tu), _) => match tu {
                TimeUnit::Second | TimeUnit::Millisecond => Time32(*tu),
                TimeUnit::Microsecond | TimeUnit::Nanosecond => Time64(*tu),
            },
            (Self::Duration(tu), _) => Duration(*tu),
            (Self::Interval(iu), _) => Interval(*iu),
            (Self::Binary, LargeUtf8) => LargeBinary,
            (Self::Binary, Utf8View) => BinaryView,
            // We don't cast to another kind of binary type if the origin one is already a binary type
            (Self::Binary, Binary | LargeBinary | BinaryView) => origin.to_owned(),
            (Self::Binary, data_type) if can_cast_types(data_type, &BinaryView) => {
                BinaryView
            }
            (Self::Binary, data_type) if can_cast_types(data_type, &LargeBinary) => {
                LargeBinary
            }
            (Self::Binary, data_type) if can_cast_types(data_type, &Binary) => Binary,
            (Self::FixedSizeBinary(size), _) => FixedSizeBinary(*size),
            (Self::String, LargeBinary) => LargeUtf8,
            (Self::String, BinaryView) => Utf8View,
            // We don't cast to another kind of string type if the origin one is already a string type
            (Self::String, Utf8 | LargeUtf8 | Utf8View) => origin.to_owned(),
            (Self::String, data_type) if can_cast_types(data_type, &Utf8View) => Utf8View,
            (Self::String, data_type) if can_cast_types(data_type, &LargeUtf8) => {
                LargeUtf8
            }
            (Self::String, data_type) if can_cast_types(data_type, &Utf8) => Utf8,
            (Self::List(to_field), List(from_field) | FixedSizeList(from_field, _)) => {
                List(default_field_cast(to_field, from_field)?)
            }
            (Self::List(to_field), LargeList(from_field)) => {
                LargeList(default_field_cast(to_field, from_field)?)
            }
            (Self::List(to_field), ListView(from_field)) => {
                ListView(default_field_cast(to_field, from_field)?)
            }
            (Self::List(to_field), LargeListView(from_field)) => {
                LargeListView(default_field_cast(to_field, from_field)?)
            }
            // List array where each element is a len 1 list of the origin type
            (Self::List(field), _) => List(Arc::new(Field::new(
                field.name.clone(),
                field.logical_type.default_cast_for(origin)?,
                field.nullable,
            ))),
            (
                Self::FixedSizeList(to_field, to_size),
                FixedSizeList(from_field, from_size),
            ) if from_size == to_size => {
                FixedSizeList(default_field_cast(to_field, from_field)?, *to_size)
            }
            (
                Self::FixedSizeList(to_field, size),
                List(from_field)
                | LargeList(from_field)
                | ListView(from_field)
                | LargeListView(from_field),
            ) => FixedSizeList(default_field_cast(to_field, from_field)?, *size),
            // FixedSizeList array where each element is a len 1 list of the origin type
            (Self::FixedSizeList(field, size), _) => FixedSizeList(
                Arc::new(Field::new(
                    field.name.clone(),
                    field.logical_type.default_cast_for(origin)?,
                    field.nullable,
                )),
                *size,
            ),
            // From https://github.com/apache/arrow-rs/blob/56525efbd5f37b89d1b56aa51709cab9f81bc89e/arrow-cast/src/cast/mod.rs#L189-L196
            (Self::Struct(to_fields), Struct(from_fields))
                if from_fields.len() == to_fields.len() =>
            {
                Struct(
                    from_fields
                        .iter()
                        .zip(to_fields.iter())
                        .map(|(from, to)| default_field_cast(to, from))
                        .collect::<Result<Fields>>()?,
                )
            }
            (Self::Struct(to_fields), Null) => Struct(
                to_fields
                    .iter()
                    .map(|field| {
                        Ok(Arc::new(Field::new(
                            field.name.clone(),
                            field.logical_type.default_cast_for(&Null)?,
                            field.nullable,
                        )))
                    })
                    .collect::<Result<Fields>>()?,
            ),
            (Self::Map(to_field), Map(from_field, sorted)) => {
                Map(default_field_cast(to_field, from_field)?, *sorted)
            }
            (Self::Map(field), Null) => Map(
                Arc::new(Field::new(
                    field.name.clone(),
                    field.logical_type.default_cast_for(&Null)?,
                    field.nullable,
                )),
                false,
            ),
            (Self::Union(to_fields), Union(from_fields, mode))
                if from_fields.len() == to_fields.len() =>
            {
                Union(
                    from_fields
                        .iter()
                        .zip(to_fields.iter())
                        .map(|((_, from), (i, to))| {
                            Ok((*i, default_field_cast(to, from)?))
                        })
                        .collect::<Result<UnionFields>>()?,
                    *mode,
                )
            }
            _ => {
                return _internal_err!(
                    "Unavailable default cast for native type {} from physical type {}",
                    self,
                    origin
                );
            }
        })
    }
}

// The following From<DataType>, From<Field>, ... implementations are temporary
// mapping solutions to provide backwards compatibility while transitioning from
// the purely physical system to a logical / physical system.

impl From<&DataType> for NativeType {
    fn from(value: &DataType) -> Self {
        value.clone().into()
    }
}

impl From<DataType> for NativeType {
    fn from(value: DataType) -> Self {
        use NativeType::*;
        match value {
            DataType::Null => Null,
            DataType::Boolean => Boolean,
            DataType::Int8 => Int8,
            DataType::Int16 => Int16,
            DataType::Int32 => Int32,
            DataType::Int64 => Int64,
            DataType::UInt8 => UInt8,
            DataType::UInt16 => UInt16,
            DataType::UInt32 => UInt32,
            DataType::UInt64 => UInt64,
            DataType::Float16 => Float16,
            DataType::Float32 => Float32,
            DataType::Float64 => Float64,
            DataType::Timestamp(tu, tz) => Timestamp(tu, tz),
            DataType::Date32 | DataType::Date64 => Date,
            DataType::Time32(tu) | DataType::Time64(tu) => Time(tu),
            DataType::Duration(tu) => Duration(tu),
            DataType::Interval(iu) => Interval(iu),
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => Binary,
            DataType::FixedSizeBinary(size) => FixedSizeBinary(size),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => String,
            DataType::List(field)
            | DataType::ListView(field)
            | DataType::LargeList(field)
            | DataType::LargeListView(field) => List(Arc::new(field.as_ref().into())),
            DataType::FixedSizeList(field, size) => {
                FixedSizeList(Arc::new(field.as_ref().into()), size)
            }
            DataType::Struct(fields) => Struct(LogicalFields::from(&fields)),
            DataType::Union(union_fields, _) => {
                Union(LogicalUnionFields::from(&union_fields))
            }
            DataType::Decimal32(p, s)
            | DataType::Decimal64(p, s)
            | DataType::Decimal128(p, s)
            | DataType::Decimal256(p, s) => Decimal(p, s),
            DataType::Map(field, _) => Map(Arc::new(field.as_ref().into())),
            DataType::Dictionary(_, data_type) => data_type.as_ref().clone().into(),
            DataType::RunEndEncoded(_, field) => field.data_type().clone().into(),
        }
    }
}

impl NativeType {
    #[inline]
    pub fn is_numeric(&self) -> bool {
        self.is_integer() || self.is_float() || self.is_decimal()
    }

    #[inline]
    pub fn is_integer(&self) -> bool {
        use NativeType::*;
        matches!(
            self,
            UInt8 | UInt16 | UInt32 | UInt64 | Int8 | Int16 | Int32 | Int64
        )
    }

    #[inline]
    pub fn is_timestamp(&self) -> bool {
        matches!(self, NativeType::Timestamp(_, _))
    }

    #[inline]
    pub fn is_date(&self) -> bool {
        matches!(self, NativeType::Date)
    }

    #[inline]
    pub fn is_time(&self) -> bool {
        matches!(self, NativeType::Time(_))
    }

    #[inline]
    pub fn is_interval(&self) -> bool {
        matches!(self, NativeType::Interval(_))
    }

    #[inline]
    pub fn is_duration(&self) -> bool {
        matches!(self, NativeType::Duration(_))
    }

    #[inline]
    pub fn is_binary(&self) -> bool {
        matches!(self, NativeType::Binary | NativeType::FixedSizeBinary(_))
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, NativeType::Null)
    }

    #[inline]
    pub fn is_decimal(&self) -> bool {
        matches!(self, Self::Decimal(_, _))
    }

    #[inline]
    pub fn is_float(&self) -> bool {
        matches!(self, Self::Float16 | Self::Float32 | Self::Float64)
    }
}
