mod from_scalar_value;
mod logical_date;
mod logical_decimal;
mod logical_duration;
mod logical_fixed_size_binary;
mod logical_fixed_size_list;
mod logical_interval;
mod logical_list;
mod logical_map;
mod logical_struct;
mod logical_time;
mod logical_timestamp;
mod logical_union;

use crate::types::{
    logical_binary, logical_boolean, logical_date, logical_float16, logical_float32,
    logical_float64, logical_int16, logical_int32, logical_int64, logical_int8,
    logical_null, logical_string, logical_uint16, logical_uint32, logical_uint64,
    logical_uint8, LogicalField, LogicalType, LogicalTypeRef, NativeType,
};
use crate::ScalarValue;
use arrow_array::Array;
use arrow_schema::DataType;
use bigdecimal::num_traits::FromBytes;
use bigdecimal::ToPrimitive;
use half::f16;
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};

pub use logical_date::LogicalDate;
pub use logical_decimal::LogicalDecimal;
pub use logical_duration::LogicalDuration;
pub use logical_fixed_size_binary::LogicalFixedSizeBinary;
pub use logical_fixed_size_list::LogicalFixedSizeList;
pub use logical_interval::LogicalInterval;
pub use logical_list::LogicalList;
pub use logical_map::LogicalMap;
pub use logical_struct::LogicalStruct;
pub use logical_time::LogicalTime;
pub use logical_timestamp::LogicalTimestamp;
pub use logical_timestamp::LogicalTimestampValue;
pub use logical_union::LogicalUnion;

/// Representation of a logical scalar. Contrary to a physical [`ScalarValue`], a logical scalar
/// is *not* coupled to a physical [`DataType`].
///
/// Most variants of this enum allow storing values for the variants in [`NativeType`]. Furthermore,
/// [`LogicalScalar::Extension`] allows storing logical values for extension types.
#[derive(Clone, Debug)]
pub enum LogicalScalar {
    /// A null value
    Null,
    /// Stores a scalar for [`NativeType::Boolean`].
    Boolean(bool),
    /// Stores a scalar for [`NativeType::Int8`].
    Int8(i8),
    /// Stores a scalar for [`NativeType::Int16`].
    Int16(i16),
    /// Stores a scalar for [`NativeType::Int32`].
    Int32(i32),
    /// Stores a scalar for [`NativeType::Int64`].
    Int64(i64),
    /// Stores a scalar for [`NativeType::UInt8`].
    UInt8(u8),
    /// Stores a scalar for [`NativeType::UInt16`].
    UInt16(u16),
    /// Stores a scalar for [`NativeType::UInt32`].
    UInt32(u32),
    /// Stores a scalar for [`NativeType::UInt64`].
    UInt64(u64),
    /// Stores a scalar for [`NativeType::Float16`].
    Float16(f16),
    /// Stores a scalar for [`NativeType::Float32`].
    Float32(f32),
    /// Stores a scalar for [`NativeType::Float64`].
    Float64(f64),
    /// Stores a scalar for [`NativeType::Timestamp`].
    Timestamp(LogicalTimestamp),
    /// Stores a scalar for [NativeType::Date].
    Date(LogicalDate),
    /// Stores a scalar for [`NativeType::Time`].
    Time(LogicalTime),
    /// Stores a scalar for [`NativeType::Duration`].
    Duration(LogicalDuration),
    /// Stores a scalar for [`NativeType::Interval`].
    Interval(LogicalInterval),
    /// Stores a scalar for [`NativeType::Binary`].
    Binary(Vec<u8>),
    /// Stores a scalar for [`NativeType::FixedSizeBinary`].
    FixedSizeBinary(LogicalFixedSizeBinary),
    /// Stores a scalar for [`NativeType::String`].
    String(String),
    /// Stores a scalar for [`NativeType::List`].
    List(LogicalList),
    /// Stores a scalar for [`NativeType::FixedSizeList`].
    FixedSizeList(LogicalFixedSizeList),
    /// Stores a scalar for [`NativeType::Struct`].
    Struct(LogicalStruct),
    /// Stores a scalar for [`NativeType::Union`].
    Union(LogicalUnion),
    /// Stores a scalar for [`NativeType::Decimal`].
    Decimal(LogicalDecimal),
    /// Stores a scalar for [`NativeType::Map`].
    Map(LogicalMap),
    // TODO logical-types: Values for ExtensionTypes
}

impl LogicalScalar {
    /// Returns the logical type of this value.
    pub fn logical_type(&self) -> LogicalTypeRef {
        match self {
            LogicalScalar::Null => logical_null(),
            LogicalScalar::Boolean(_) => logical_boolean(),
            LogicalScalar::Int8(_) => logical_int8(),
            LogicalScalar::Int16(_) => logical_int16(),
            LogicalScalar::Int32(_) => logical_int32(),
            LogicalScalar::Int64(_) => logical_int64(),
            LogicalScalar::UInt8(_) => logical_uint8(),
            LogicalScalar::UInt16(_) => logical_uint16(),
            LogicalScalar::UInt32(_) => logical_uint32(),
            LogicalScalar::UInt64(_) => logical_uint64(),
            LogicalScalar::Float16(_) => logical_float16(),
            LogicalScalar::Float32(_) => logical_float32(),
            LogicalScalar::Float64(_) => logical_float64(),
            LogicalScalar::Timestamp(timestamp) => timestamp.logical_type(),
            LogicalScalar::Date(_) => logical_date(),
            LogicalScalar::Time(time) => time.logical_type(),
            LogicalScalar::Duration(duration) => duration.logical_type(),
            LogicalScalar::Interval(i) => i.logical_type(),
            LogicalScalar::Binary(_) => logical_binary(),
            LogicalScalar::FixedSizeBinary(value) => value.logical_type(),
            LogicalScalar::String(_) => logical_string(),
            LogicalScalar::List(list) => list.logical_type(),
            LogicalScalar::FixedSizeList(list) => list.logical_type(),
            LogicalScalar::Struct(value) => value.logical_type(),
            LogicalScalar::Union(value) => value.logical_type(),
            LogicalScalar::Decimal(value) => value.logical_type(),
            LogicalScalar::Map(value) => value.logical_type(),
        }
    }

    /// Checks whether this logical scalar is an instance of the given `logical_type`.
    pub fn has_type(&self, logical_type: &dyn LogicalType) -> bool {
        self.logical_type().as_ref().signature() == logical_type.signature()
    }

    /// Checks whether this logical scalar can be stored in `field`.
    ///
    /// This is true if:
    /// - The logical type is equal to the logical type of `field` *or*
    /// - The `field` is nullable and this scalar is [LogicalScalar::Null]
    pub fn is_compatible_with_field(&self, field: &LogicalField) -> bool {
        self.has_type(field.logical_type.as_ref()) || field.nullable && self.is_null()
    }

    /// Returns whether this scalar is [`LogicalScalar::Null`].
    pub fn is_null(&self) -> bool {
        *self == LogicalScalar::Null
    }
}

// manual implementation of `PartialEq`
impl PartialEq for LogicalScalar {
    fn eq(&self, other: &Self) -> bool {
        use LogicalScalar::*;
        // This purposely doesn't have a catch-all "(_, _)" so that
        // any newly added enum variant will require editing this list
        // or else face a compile error
        match (self, other) {
            (Decimal(v1), Decimal(v2)) => v1.eq(v2),
            (Decimal(_), _) => false,
            (Boolean(v1), Boolean(v2)) => v1.eq(v2),
            (Boolean(_), _) => false,
            (Float32(v1), Float32(v2)) => v1.to_bits() == v2.to_bits(),
            (Float16(v1), Float16(v2)) => v1.to_bits() == v2.to_bits(),
            (Float32(_), _) => false,
            (Float16(_), _) => false,
            (Float64(v1), Float64(v2)) => v1.to_bits() == v2.to_bits(),
            (Float64(_), _) => false,
            (Int8(v1), Int8(v2)) => v1.eq(v2),
            (Int8(_), _) => false,
            (Int16(v1), Int16(v2)) => v1.eq(v2),
            (Int16(_), _) => false,
            (Int32(v1), Int32(v2)) => v1.eq(v2),
            (Int32(_), _) => false,
            (Int64(v1), Int64(v2)) => v1.eq(v2),
            (Int64(_), _) => false,
            (UInt8(v1), UInt8(v2)) => v1.eq(v2),
            (UInt8(_), _) => false,
            (UInt16(v1), UInt16(v2)) => v1.eq(v2),
            (UInt16(_), _) => false,
            (UInt32(v1), UInt32(v2)) => v1.eq(v2),
            (UInt32(_), _) => false,
            (UInt64(v1), UInt64(v2)) => v1.eq(v2),
            (UInt64(_), _) => false,
            (String(v1), String(v2)) => v1.eq(v2),
            (String(_), _) => false,
            (Binary(v1), Binary(v2)) => v1.eq(v2),
            (Binary(_), _) => false,
            (FixedSizeBinary(v1), FixedSizeBinary(v2)) => v1.eq(v2),
            (FixedSizeBinary(_), _) => false,
            (FixedSizeList(v1), FixedSizeList(v2)) => v1.eq(v2),
            (FixedSizeList(_), _) => false,
            (List(v1), List(v2)) => v1.eq(v2),
            (List(_), _) => false,
            (Struct(v1), Struct(v2)) => v1.eq(v2),
            (Struct(_), _) => false,
            (Date(v1), Date(v2)) => v1.eq(v2),
            (Date(_), _) => false,
            (Time(v1), Time(v2)) => v1.eq(v2),
            (Time(_), _) => false,
            (Timestamp(v1), Timestamp(v2)) => v1.eq(v2),
            (Timestamp(_), _) => false,
            (Duration(v1), Duration(v2)) => v1.eq(v2),
            (Duration(_), _) => false,
            (Interval(v1), Interval(v2)) => v1.eq(v2),
            (Interval(_), _) => false,
            (Union(v1), Union(v2)) => v1.eq(v2),
            (Union(_), _) => false,
            (Map(v1), Map(v2)) => v1.eq(v2),
            (Map(_), _) => false,
            (Null, Null) => true,
            (Null, _) => false,
        }
    }
}

impl Eq for LogicalScalar {}

// manual implementation of `PartialOrd`
impl PartialOrd for LogicalScalar {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use LogicalScalar::*;
        // This purposely doesn't have a catch-all "(_, _)" so that
        // any newly added enum variant will require editing this list
        // or else face a compile error
        match (self, other) {
            (Decimal(v1), Decimal(v2)) => v1.partial_cmp(v2),
            (Decimal(_), _) => None,
            (Boolean(v1), Boolean(v2)) => v1.partial_cmp(v2),
            (Boolean(_), _) => None,
            (Float32(v1), Float32(v2)) => v1.partial_cmp(v2),
            (Float16(v1), Float16(v2)) => v1.partial_cmp(v2),
            (Float32(_), _) => None,
            (Float16(_), _) => None,
            (Float64(v1), Float64(v2)) => v1.partial_cmp(v2),
            (Float64(_), _) => None,
            (Int8(v1), Int8(v2)) => v1.partial_cmp(v2),
            (Int8(_), _) => None,
            (Int16(v1), Int16(v2)) => v1.partial_cmp(v2),
            (Int16(_), _) => None,
            (Int32(v1), Int32(v2)) => v1.partial_cmp(v2),
            (Int32(_), _) => None,
            (Int64(v1), Int64(v2)) => v1.partial_cmp(v2),
            (Int64(_), _) => None,
            (UInt8(v1), UInt8(v2)) => v1.partial_cmp(v2),
            (UInt8(_), _) => None,
            (UInt16(v1), UInt16(v2)) => v1.partial_cmp(v2),
            (UInt16(_), _) => None,
            (UInt32(v1), UInt32(v2)) => v1.partial_cmp(v2),
            (UInt32(_), _) => None,
            (UInt64(v1), UInt64(v2)) => v1.partial_cmp(v2),
            (UInt64(_), _) => None,
            (String(v1), String(v2)) => v1.partial_cmp(v2),
            (String(_), _) => None,
            (Binary(v1), Binary(v2)) => v1.partial_cmp(v2),
            (Binary(_), _) => None,
            (FixedSizeBinary(v1), FixedSizeBinary(v2)) => v1.partial_cmp(v2),
            (FixedSizeBinary(_), _) => None,
            // ScalarValue::List / ScalarValue::FixedSizeList / ScalarValue::LargeList are ensure to have length 1
            (List(v1), List(v2)) => v1.partial_cmp(v2),
            (List(_), _) => None,
            (FixedSizeList(v1), FixedSizeList(v2)) => v1.partial_cmp(v2),
            (FixedSizeList(_), _) => None,
            (Struct(v1), Struct(v2)) => v1.partial_cmp(v2),
            (Struct(_), _) => None,
            (Date(v1), Date(v2)) => v1.partial_cmp(v2),
            (Date(_), _) => None,
            (Time(v1), Time(v2)) => v1.partial_cmp(v2),
            (Time(_), _) => None,
            (Timestamp(v1), Timestamp(v2)) => v1.partial_cmp(v2),
            (Timestamp(_), _) => None,
            (Interval(v1), Interval(v2)) => v1.partial_cmp(v2),
            (Interval(_), _) => None,
            (Duration(v1), Duration(v2)) => v1.partial_cmp(v2),
            (Duration(_), _) => None,
            (Union(v1), Union(v2)) => v1.partial_cmp(v2),
            (Union(_), _) => None,
            (Map(v1), Map(v2)) => v1.partial_cmp(v2),
            (Map(_), _) => None,
            (Null, Null) => Some(Ordering::Equal),
            (Null, _) => None,
        }
    }
}

// manual implementation of `Hash`
//
// # Panics
//
// Panics if there is an error when creating hash values for rows
impl Hash for LogicalScalar {
    fn hash<H: Hasher>(&self, state: &mut H) {
        use LogicalScalar::*;
        match self {
            Decimal(v) => v.hash(state),
            Boolean(v) => v.hash(state),
            Float16(v) => v.to_bits().hash(state),
            Float32(v) => {
                state.write(&<f32>::from_ne_bytes(v.to_ne_bytes()).to_ne_bytes())
            }
            Float64(v) => {
                state.write(&<f64>::from_ne_bytes(v.to_ne_bytes()).to_ne_bytes())
            }
            Int8(v) => v.hash(state),
            Int16(v) => v.hash(state),
            Int32(v) => v.hash(state),
            Int64(v) => v.hash(state),
            UInt8(v) => v.hash(state),
            UInt16(v) => v.hash(state),
            UInt32(v) => v.hash(state),
            UInt64(v) => v.hash(state),
            String(v) => v.hash(state),
            Binary(v) => v.hash(state),
            FixedSizeBinary(v) => v.hash(state),
            List(v) => v.hash(state),
            FixedSizeList(v) => v.hash(state),
            Struct(v) => v.hash(state),
            Date(v) => v.hash(state),
            Time(v) => v.hash(state),
            Timestamp(v) => v.hash(state),
            Duration(v) => v.hash(state),
            Interval(v) => v.hash(state),
            Union(v) => v.hash(state),
            Map(v) => v.hash(state),
            // stable hash for Null value
            Null => 1.hash(state),
        }
    }
}

impl Display for LogicalScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
