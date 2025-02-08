use crate::error::_internal_err;
use crate::scalar::{
    fixed_size_list_to_logical_scalar, list_to_logical_scalar, map_to_logical_scalar,
    struct_to_logical_scalar, union_to_logical_scalar,
};
use crate::types::{
    logical_binary, logical_boolean, logical_date, logical_decimal, logical_duration,
    logical_fixed_size_binary, logical_fixed_size_list, logical_float16, logical_float32,
    logical_float64, logical_int16, logical_int32, logical_int64, logical_int8,
    logical_interval, logical_list, logical_map, logical_null, logical_string,
    logical_struct, logical_time, logical_timestamp, logical_uint16, logical_uint32,
    logical_uint64, logical_uint8, logical_union, LogicalFieldRef, LogicalFields,
    LogicalTypeRef, LogicalUnionFields,
};
use crate::{DataFusionError, Result, ScalarValue};
use arrow::datatypes::{IntervalDayTime, IntervalMonthDayNano};
use arrow_array::temporal_conversions::{
    time32ms_to_time, time32s_to_time, time64ns_to_time, time64us_to_time,
};
use arrow_array::Array;
use arrow_schema::{IntervalUnit, TimeUnit};
use bigdecimal::num_bigint::BigInt;
use bigdecimal::{BigDecimal, ToPrimitive};
use chrono::NaiveTime;
use half::f16;
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use bigdecimal::num_traits::FromBytes;

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
    Timestamp(TimeUnit, Option<Arc<str>>, i64),
    /// Stores a scalar for [NativeType::Date].
    Date(i32),
    /// Stores a scalar for [`NativeType::Time`].
    Time(LogicalTime),
    /// Stores a scalar for [`NativeType::Duration`].
    Duration(TimeUnit, i64),
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
}

impl LogicalScalar {
    /// Returns the [`LogicalTypeRef`] for [`self`].
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
            LogicalScalar::Timestamp(time_unit, time_zone, _) => {
                logical_timestamp(time_unit.clone(), time_zone.clone())
            }
            LogicalScalar::Date(_) => logical_date(),
            LogicalScalar::Time(time) => logical_time(time.time_unit()),
            LogicalScalar::Duration(time_unit, _) => logical_duration(time_unit.clone()),
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

    /// Returns whether this scalar is [`LogicalScalar::Null`].
    pub fn is_null(&self) -> bool {
        *self == LogicalScalar::Null
    }
}

/// Stores a scalar for [`NativeType::Time`].
///
/// This struct is used to ensure the integer size (`i32` or `i64`) for different [`TimeUnit`]s.
#[derive(Debug, Clone, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub enum LogicalTime {
    /// Time in seconds.
    /// Time in seconds as `i32`.
    Second(i32),
    /// Time in milliseconds as `i32`.
    Millisecond(i32),
    /// Time in microseconds as `i64`.
    Microsecond(i64),
    /// Time in nanoseconds as `i64`.
    Nanosecond(i64),
}

impl LogicalTime {
    /// Returns the [`TimeUnit`].
    pub fn time_unit(&self) -> TimeUnit {
        match self {
            LogicalTime::Second(_) => TimeUnit::Second,
            LogicalTime::Millisecond(_) => TimeUnit::Millisecond,
            LogicalTime::Microsecond(_) => TimeUnit::Microsecond,
            LogicalTime::Nanosecond(_) => TimeUnit::Nanosecond,
        }
    }

    /// Returns a [NaiveTime] representing the time of [self].
    pub fn value_as_time(&self) -> Option<NaiveTime> {
        match self {
            LogicalTime::Second(v) => time32s_to_time(*v),
            LogicalTime::Millisecond(v) => time32ms_to_time(*v),
            LogicalTime::Microsecond(v) => time64us_to_time(*v),
            LogicalTime::Nanosecond(v) => time64ns_to_time(*v),
        }
    }
}

/// Stores a scalar for [`NativeType::Interval`].
///
/// This struct is used to provide type-safe access to different [`IntervalUnit`].
#[derive(Debug, Clone, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub enum LogicalInterval {
    /// Stores the value for a [`IntervalUnit::YearMonth`].
    YearMonth(i32),
    /// Stores the values for a [`IntervalUnit::DayTime`].
    DayTime(IntervalDayTime),
    /// Stores the value for a [`IntervalUnit::MonthDayNano`].
    MonthDayNano(IntervalMonthDayNano),
}

impl LogicalInterval {
    /// Returns the [`LogicalTypeRef`] for [`self`].
    pub fn logical_type(&self) -> LogicalTypeRef {
        logical_interval(self.interval_unit())
    }

    /// Returns the corresponding [`IntervalUnit`] for [`self`].
    pub fn interval_unit(&self) -> IntervalUnit {
        match self {
            LogicalInterval::YearMonth(_) => IntervalUnit::YearMonth,
            LogicalInterval::DayTime(_) => IntervalUnit::DayTime,
            LogicalInterval::MonthDayNano(_) => IntervalUnit::MonthDayNano,
        }
    }
}

/// TODO @tobixdev
#[derive(Debug, Clone, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct LogicalFixedSizeBinary {
    /// The value of the fixed size binary scalar
    value: Vec<u8>,
}

impl LogicalFixedSizeBinary {
    /// TODO @tobixdev
    pub fn try_new(value: Vec<u8>) -> Result<Self> {
        if value.len().to_i32().is_none() {
            return _internal_err!("Value too big for fixed-size binary.");
        }
        Ok(Self { value })
    }

    /// TODO @tobixdev
    pub fn len(&self) -> i32 {
        self.value.len() as i32
    }

    /// TODO @tobixdev
    pub fn value(&self) -> &[u8] {
        self.value.as_ref()
    }

    /// TODO @tobixdev
    pub fn into_value(self) -> Vec<u8> {
        self.value
    }

    /// TODO @tobixdev
    pub fn logical_type(&self) -> LogicalTypeRef {
        // LogicalFixedSizeBinary::new guarantees that the cast succeeds
        logical_fixed_size_binary(self.len())
    }
}

/// TODO @tobixdev
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct LogicalList {
    /// The logical type and nullability of all list elements.
    element_field: LogicalFieldRef,
    /// The list of contained logical scalars.
    values: Vec<LogicalScalar>,
}

impl LogicalList {
    /// Creates a new [`LogicalList`].
    ///
    /// The logical types of the elements in [`value`] must be equal to [`element_type`]. If
    /// [`nullable`] is true, [`LogicalValue::Null`] is also allowed.
    ///
    /// # Errors
    ///
    /// Returns an error if the elements of [`value`] do not match [`element_type`].
    pub fn try_new(
        element_field: LogicalFieldRef,
        value: Vec<LogicalScalar>,
    ) -> Result<Self> {
        for element in &value {
            let has_matching_type =
                &element.logical_type() == &element_field.logical_type;
            let is_allowed_null =
                element_field.nullable && element.logical_type().native().is_null();

            if !has_matching_type && !is_allowed_null {
                return _internal_err!(
                    "Incompatible element type creating logical list."
                );
            }
        }

        Ok(Self {
            element_field,
            values: value,
        })
    }

    /// Returns the logical type of this list.
    pub fn logical_type(&self) -> LogicalTypeRef {
        logical_list(self.element_field.clone())
    }

    /// Returns the length of this list.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// TODO logical-types
    pub fn values(&self) -> &[LogicalScalar] {
        self.values.as_slice()
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct LogicalFixedSizeList {
    /// The inner list with a fixed size.
    inner: LogicalList,
}

impl LogicalFixedSizeList {
    /// Tries to create a new [`LogicalFixedSizeList`].
    ///
    /// # Errors
    ///
    /// An error is returned if [`LogicalList::try_new`] fails, or if the resulting list is too big.
    pub fn try_new(
        element_field: LogicalFieldRef,
        value: Vec<LogicalScalar>,
    ) -> Result<Self> {
        let inner = LogicalList::try_new(element_field, value)?;

        if inner.len().to_i32().is_none() {
            return _internal_err!("List too big for fixed-size list.");
        }

        Ok(Self { inner })
    }

    /// Returns the [`LogicalTypeRef`] for [`self`].
    pub fn logical_type(&self) -> LogicalTypeRef {
        // LogicalFixedSizeList::new guarantees that the cast succeeds
        logical_fixed_size_list(self.inner.element_field.clone(), self.inner.len() as i32)
    }

    /// TODO logical-types
    pub fn values(&self) -> &[LogicalScalar] {
        self.inner.values()
    }
}

/// A [LogicalList] may be turned into a [LogicalFixedSizeList].
///
/// # Errors
///
/// Returns an error if the [LogicalList] is too big.
impl TryFrom<LogicalList> for LogicalFixedSizeList {
    type Error = DataFusionError;

    fn try_from(value: LogicalList) -> std::result::Result<Self, Self::Error> {
        LogicalFixedSizeList::try_new(value.element_field, value.values)
    }
}

/// TODO @tobixdev
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct LogicalStruct {
    fields: Arc<[LogicalFieldRef]>,
    values: Arc<[LogicalScalar]>,
}

impl LogicalStruct {
    /// Returns the [`LogicalTypeRef`] for [`self`].
    pub fn logical_type(&self) -> LogicalTypeRef {
        logical_struct(LogicalFields::from_iter(self.fields.iter().cloned()))
    }
}

/// TODO @tobixdev
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct LogicalUnion {
    fields: LogicalUnionFields,
    type_id: i8,
    value: Box<LogicalScalar>,
}

impl LogicalUnion {
    /// Returns the [`LogicalTypeRef`] for [`self`].
    pub fn logical_type(&self) -> LogicalTypeRef {
        logical_union(self.fields.clone())
    }
}

/// TODO @tobixdev
#[derive(Debug, Clone, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct LogicalDecimal {
    /// The actual value of the logical decimal
    value: BigDecimal,
}

impl LogicalDecimal {
    /// Creates a new [`LogicalDecimal`] from the given value.
    ///
    /// # Errors
    ///
    /// This function returns an error if [`value`] violates one of the following:
    /// - The digits of the decimal must be representable in 256 bits.
    /// - The scale must be representable with an `i8`.
    pub fn try_new(value: BigDecimal) -> Result<Self> {
        const MAX_PHYSICAL_DECIMAL_BYTES: usize = 256 / 8;
        if value.digits().to_ne_bytes().len() > MAX_PHYSICAL_DECIMAL_BYTES {
            return _internal_err!("Too many bytes for logical decimal");
        }

        if value.fractional_digit_count().to_i8().is_none() {
            return _internal_err!("Scale not supported for logical decimal");
        }

        Ok(Self { value })
    }

    /// Returns the value of this logical decimal.
    pub fn value(&self) -> &BigDecimal {
        &self.value
    }

    /// Returns the [`LogicalTypeRef`] for [`self`].
    pub fn logical_type(&self) -> LogicalTypeRef {
        // LogicalDecimal::new guarantees that the casts succeed
        logical_decimal(
            self.value.digits() as u8,
            self.value.fractional_digit_count() as i8,
        )
    }
}

/// TODO @tobixdev
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct LogicalMap {
    value_type: LogicalFieldRef,
    // TODO @tobixdev
}

impl LogicalMap {
    /// Returns the [`LogicalTypeRef`] for [`self`].
    pub fn logical_type(&self) -> LogicalTypeRef {
        logical_map(self.value_type.clone())
    }
}

/// TODO @tobixdev
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct LogicalExtension {
    // TODO @tobixdev
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
            (Timestamp(tu1, tz1, v1), Timestamp(tu2, tz2, v2)) => {
                tu1.eq(tu2) && tz1.eq(tz2) && v1.eq(v2)
            }
            (Timestamp(_, _, _), _) => false,
            (Duration(tu1, v1), Duration(tu2, v2)) => tu1.eq(tu2) && v1.eq(v2),
            (Duration(_, _), _) => false,
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
            (Timestamp(tu1, _, v1), Timestamp(tu2, _, v2)) => {
                if tu1 == tu2 {
                    v1.partial_cmp(v2)
                } else {
                    None
                }
            }
            (Timestamp(_, _, _), _) => None,
            (Interval(v1), Interval(v2)) => v1.partial_cmp(v2),
            (Interval(_), _) => None,
            (Duration(tu1, v1), Duration(tu2, v2)) => {
                if tu1 == tu2 {
                    v1.partial_cmp(v2)
                } else {
                    None
                }
            }
            (Duration(_, _), _) => None,
            (Union(v1), Union(v2)) => v1.partial_cmp(v2),
            (Union(_), _) => None,
            (Map(v1), Map(v2)) => v1.partial_cmp(v2),
            (Map(_), _) => None,
            (Null, Null) => Some(Ordering::Equal),
            (Null, _) => None,
        }
    }
}

impl PartialOrd for LogicalList {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        todo!("logical-types")
    }
}

impl PartialOrd for LogicalFixedSizeList {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        todo!("logical-types")
    }
}

impl PartialOrd for LogicalMap {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        todo!("logical-types")
    }
}

impl PartialOrd for LogicalStruct {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        todo!("logical-types")
    }
}

impl PartialOrd for LogicalUnion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        todo!("logical-types")
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
            Timestamp(_, _, v) => v.hash(state),
            Duration(_, v) => v.hash(state),
            Interval(v) => v.hash(state),
            Union(v) => v.hash(state),
            Map(v) => v.hash(state),
            // stable hash for Null value
            Null => 1.hash(state),
        }
    }
}

impl Hash for LogicalStruct {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.values.iter().for_each(|v| v.hash(state))
    }
}

impl Display for LogicalScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

/// Converts this physical [`ScalarValue`] into a logical [`LogicalScalar`].
impl From<ScalarValue> for LogicalScalar {
    fn from(value: ScalarValue) -> Self {
        match value {
            ScalarValue::Null => LogicalScalar::Null,
            ScalarValue::Boolean(None) => LogicalScalar::Null,
            ScalarValue::Boolean(Some(val)) => LogicalScalar::Boolean(val),
            ScalarValue::Float16(None) => LogicalScalar::Null,
            ScalarValue::Float16(Some(v)) => LogicalScalar::Float16(v),
            ScalarValue::Float32(None) => LogicalScalar::Null,
            ScalarValue::Float32(Some(v)) => LogicalScalar::Float32(v),
            ScalarValue::Float64(None) => LogicalScalar::Null,
            ScalarValue::Float64(Some(v)) => LogicalScalar::Float64(v),
            ScalarValue::Decimal128(None, _, _) => LogicalScalar::Null,
            ScalarValue::Decimal128(Some(v), p, s) => LogicalScalar::Decimal(
                LogicalDecimal::try_new(BigDecimal::new(BigInt::from(v), s as i64))
                    .expect("Value cannot be too big"),
            ),
            ScalarValue::Decimal256(None, _, _) => LogicalScalar::Null,
            ScalarValue::Decimal256(Some(v), p, s) => LogicalScalar::Decimal(
                LogicalDecimal::try_new(BigDecimal::new(
                    BigInt::from_ne_bytes(v.to_le_bytes().as_ref()),
                    s as i64,
                ))
                .expect("Value cannot be too big"),
            ),
            ScalarValue::Int8(None) => LogicalScalar::Null,
            ScalarValue::Int8(Some(v)) => LogicalScalar::Int8(v),
            ScalarValue::Int16(None) => LogicalScalar::Null,
            ScalarValue::Int16(Some(v)) => LogicalScalar::Int16(v),
            ScalarValue::Int32(None) => LogicalScalar::Null,
            ScalarValue::Int32(Some(v)) => LogicalScalar::Int32(v),
            ScalarValue::Int64(None) => LogicalScalar::Null,
            ScalarValue::Int64(Some(v)) => LogicalScalar::Int64(v),
            ScalarValue::UInt8(None) => LogicalScalar::Null,
            ScalarValue::UInt8(Some(v)) => LogicalScalar::UInt8(v),
            ScalarValue::UInt16(None) => LogicalScalar::Null,
            ScalarValue::UInt16(Some(v)) => LogicalScalar::UInt16(v),
            ScalarValue::UInt32(None) => LogicalScalar::Null,
            ScalarValue::UInt32(Some(v)) => LogicalScalar::UInt32(v),
            ScalarValue::UInt64(None) => LogicalScalar::Null,
            ScalarValue::UInt64(Some(v)) => LogicalScalar::UInt64(v),
            ScalarValue::Utf8(None) => LogicalScalar::Null,
            ScalarValue::Utf8(Some(v)) => LogicalScalar::String(v),
            ScalarValue::Utf8View(None) => LogicalScalar::Null,
            ScalarValue::Utf8View(Some(v)) => LogicalScalar::String(v),
            ScalarValue::LargeUtf8(None) => LogicalScalar::Null,
            ScalarValue::LargeUtf8(Some(v)) => LogicalScalar::String(v),
            ScalarValue::Binary(None) => LogicalScalar::Null,
            ScalarValue::Binary(Some(v)) => LogicalScalar::Binary(v),
            ScalarValue::BinaryView(None) => LogicalScalar::Null,
            ScalarValue::BinaryView(Some(v)) => LogicalScalar::Binary(v),
            ScalarValue::FixedSizeBinary(_, None) => LogicalScalar::Null,
            ScalarValue::FixedSizeBinary(_, Some(v)) => LogicalScalar::FixedSizeBinary(
                LogicalFixedSizeBinary::try_new(v).expect("Vec cannot be too big"),
            ),
            ScalarValue::LargeBinary(None) => LogicalScalar::Null,
            ScalarValue::LargeBinary(Some(v)) => LogicalScalar::Binary(v),
            ScalarValue::FixedSizeList(v) => fixed_size_list_to_logical_scalar(v),
            ScalarValue::List(v) => {
                list_to_logical_scalar(v.value_type(), v.is_nullable(), v.values())
            }
            ScalarValue::LargeList(v) => {
                list_to_logical_scalar(v.value_type(), v.is_nullable(), v.values())
            }
            ScalarValue::Struct(v) => struct_to_logical_scalar(v),
            ScalarValue::Map(v) => map_to_logical_scalar(v),
            ScalarValue::Date32(None) => LogicalScalar::Null,
            ScalarValue::Date32(Some(v)) => LogicalScalar::Date(v),
            ScalarValue::Date64(None) => LogicalScalar::Null,
            ScalarValue::Date64(Some(v)) => LogicalScalar::Timestamp(TimeUnit::Millisecond, None, v),
            ScalarValue::Time32Second(None) => LogicalScalar::Null,
            ScalarValue::Time32Second(Some(v)) => {
                LogicalScalar::Time(LogicalTime::Second(v))
            }
            ScalarValue::Time32Millisecond(None) => LogicalScalar::Null,
            ScalarValue::Time32Millisecond(Some(v)) => {
                LogicalScalar::Time(LogicalTime::Millisecond(v))
            }
            ScalarValue::Time64Microsecond(None) => LogicalScalar::Null,
            ScalarValue::Time64Microsecond(Some(v)) => {
                LogicalScalar::Time(LogicalTime::Microsecond(v))
            }
            ScalarValue::Time64Nanosecond(None) => LogicalScalar::Null,
            ScalarValue::Time64Nanosecond(Some(v)) => {
                LogicalScalar::Time(LogicalTime::Nanosecond(v))
            }
            ScalarValue::TimestampSecond(None, _) => LogicalScalar::Null,
            ScalarValue::TimestampSecond(Some(v), tz) => {
                LogicalScalar::Timestamp(TimeUnit::Second, tz, v)
            }
            ScalarValue::TimestampMillisecond(None, _) => LogicalScalar::Null,
            ScalarValue::TimestampMillisecond(Some(v), tz) => {
                LogicalScalar::Timestamp(TimeUnit::Millisecond, tz, v)
            }
            ScalarValue::TimestampMicrosecond(None, _) => LogicalScalar::Null,
            ScalarValue::TimestampMicrosecond(Some(v), tz) => {
                LogicalScalar::Timestamp(TimeUnit::Microsecond, tz, v)
            }
            ScalarValue::TimestampNanosecond(None, _) => LogicalScalar::Null,
            ScalarValue::TimestampNanosecond(Some(v), tz) => {
                LogicalScalar::Timestamp(TimeUnit::Nanosecond, tz, v)
            }
            ScalarValue::IntervalYearMonth(None) => LogicalScalar::Null,
            ScalarValue::IntervalYearMonth(Some(v)) => {
                LogicalScalar::Interval(LogicalInterval::YearMonth(v))
            }
            ScalarValue::IntervalDayTime(None) => LogicalScalar::Null,
            ScalarValue::IntervalDayTime(Some(v)) => {
                LogicalScalar::Interval(LogicalInterval::DayTime(v))
            }
            ScalarValue::IntervalMonthDayNano(None) => LogicalScalar::Null,
            ScalarValue::IntervalMonthDayNano(Some(v)) => {
                LogicalScalar::Interval(LogicalInterval::MonthDayNano(v))
            }
            ScalarValue::DurationSecond(None) => LogicalScalar::Null,
            ScalarValue::DurationSecond(Some(v)) => {
                LogicalScalar::Duration(TimeUnit::Second, v)
            }
            ScalarValue::DurationMillisecond(None) => LogicalScalar::Null,
            ScalarValue::DurationMillisecond(Some(v)) => {
                LogicalScalar::Duration(TimeUnit::Millisecond, v)
            }
            ScalarValue::DurationMicrosecond(None) => LogicalScalar::Null,
            ScalarValue::DurationMicrosecond(Some(v)) => {
                LogicalScalar::Duration(TimeUnit::Microsecond, v)
            }
            ScalarValue::DurationNanosecond(None) => LogicalScalar::Null,
            ScalarValue::DurationNanosecond(Some(v)) => {
                LogicalScalar::Duration(TimeUnit::Nanosecond, v)
            }
            ScalarValue::Union(v, f, _) => union_to_logical_scalar(v, f),
            ScalarValue::Dictionary(_, v) => (*v).into(),
        }
    }
}
