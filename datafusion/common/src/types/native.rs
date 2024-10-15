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

use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, IntervalUnit, TimeUnit, UnionFields};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NativeField {
    name: String,
    native_type: NativeType,
    nullable: bool,
}

pub type NativeFieldRef = Arc<NativeField>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NativeFields(Arc<[NativeFieldRef]>);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NativeUnionFields(Arc<[(i8, NativeFieldRef)]>);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum NativeType {
    /// Null type
    Null,
    /// A boolean datatype representing the values `true` and `false`.
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
    /// # use arrow_schema::{DataType, TimeUnit};
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
    Utf8,
    /// A list of some logical data type with variable length.
    List(NativeFieldRef),
    /// A list of some logical data type with fixed length.
    FixedSizeList(NativeFieldRef, i32),
    /// A nested datatype that contains a number of sub-fields.
    Struct(NativeFields),
    /// A nested datatype that can represent slots of differing types.
    Union(NativeUnionFields),
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
    /// In a field with Map type, the field has a child Struct field, which then
    /// has two children: key type and the second the value type. The names of the
    /// child fields may be respectively "entries", "key", and "value", but this is
    /// not enforced.
    Map(NativeFieldRef),
}

// The following From<DataType>, From<Field>, ... implementations are temporary
// mapping solutions to provide backwards compatibility while transitioning from
// the purely physical system to a logical / physical system.

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
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Utf8,
            DataType::List(field)
            | DataType::ListView(field)
            | DataType::LargeList(field)
            | DataType::LargeListView(field) => List(Arc::new(field.as_ref().into())),
            DataType::FixedSizeList(field, size) => {
                FixedSizeList(Arc::new(field.as_ref().into()), size)
            }
            DataType::Struct(fields) => Struct(NativeFields::from(&fields)),
            DataType::Union(union_fields, _) => {
                Union(NativeUnionFields::from(&union_fields))
            }
            DataType::Dictionary(_, data_type) => data_type.as_ref().clone().into(),
            DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => Decimal(p, s),
            DataType::Map(field, _) => Map(Arc::new(field.as_ref().into())),
            DataType::RunEndEncoded(_, field) => field.data_type().clone().into(),
        }
    }
}

impl From<&Field> for NativeField {
    fn from(value: &Field) -> Self {
        Self {
            name: value.name().clone(),
            native_type: value.data_type().clone().into(),
            nullable: value.is_nullable(),
        }
    }
}

impl From<&Fields> for NativeFields {
    fn from(value: &Fields) -> Self {
        value
            .iter()
            .map(|field| Arc::new(NativeField::from(field.as_ref())))
            .collect()
    }
}

impl FromIterator<NativeFieldRef> for NativeFields {
    fn from_iter<T: IntoIterator<Item = NativeFieldRef>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl From<&UnionFields> for NativeUnionFields {
    fn from(value: &UnionFields) -> Self {
        value
            .iter()
            .map(|(i, field)| (i, Arc::new(NativeField::from(field.as_ref()))))
            .collect()
    }
}

impl FromIterator<(i8, NativeFieldRef)> for NativeUnionFields {
    fn from_iter<T: IntoIterator<Item = (i8, NativeFieldRef)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}
