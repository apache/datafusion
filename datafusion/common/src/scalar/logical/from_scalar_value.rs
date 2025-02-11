use crate::scalar::logical::logical_duration::LogicalDuration;
use crate::scalar::logical::logical_timestamp::LogicalTimestamp;
use crate::scalar::{
    LogicalDate, LogicalDecimal, LogicalFixedSizeBinary, LogicalFixedSizeList,
    LogicalInterval, LogicalList, LogicalMap, LogicalScalar, LogicalStruct, LogicalTime,
    LogicalUnion,
};
use crate::types::{LogicalField, LogicalFields, LogicalUnionFields, NativeType};
use crate::{HashMap, ScalarValue};
use arrow_array::types::{Decimal128Type, Decimal256Type, DecimalType};
use arrow_array::{Array, FixedSizeListArray, MapArray, StructArray};
use arrow_schema::{DataType, TimeUnit, UnionFields};
use bigdecimal::BigDecimal;
use std::str::FromStr;
use std::sync::Arc;

/// Converts a physical [`ScalarValue`] into a logical [`LogicalScalar`].
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
                LogicalDecimal::try_new(
                    BigDecimal::from_str(&Decimal128Type::format_decimal(v, p, s))
                        .expect("Value must be parsable"),
                )
                .expect("Value cannot be too big"),
            ),
            ScalarValue::Decimal256(None, _, _) => LogicalScalar::Null,
            ScalarValue::Decimal256(Some(v), p, s) => LogicalScalar::Decimal(
                LogicalDecimal::try_new(
                    BigDecimal::from_str(&Decimal256Type::format_decimal(v, p, s))
                        .expect("Value must be parsable"),
                )
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
            ScalarValue::Date32(Some(v)) => LogicalScalar::Date(LogicalDate::new(v)),
            ScalarValue::Date64(None) => LogicalScalar::Null,
            ScalarValue::Date64(Some(v)) => LogicalScalar::Timestamp(
                LogicalTimestamp::new(TimeUnit::Millisecond, None, v),
            ),
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
                LogicalScalar::Timestamp(LogicalTimestamp::new(TimeUnit::Second, tz, v))
            }
            ScalarValue::TimestampMillisecond(None, _) => LogicalScalar::Null,
            ScalarValue::TimestampMillisecond(Some(v), tz) => LogicalScalar::Timestamp(
                LogicalTimestamp::new(TimeUnit::Millisecond, tz, v),
            ),
            ScalarValue::TimestampMicrosecond(None, _) => LogicalScalar::Null,
            ScalarValue::TimestampMicrosecond(Some(v), tz) => LogicalScalar::Timestamp(
                LogicalTimestamp::new(TimeUnit::Microsecond, tz, v),
            ),
            ScalarValue::TimestampNanosecond(None, _) => LogicalScalar::Null,
            ScalarValue::TimestampNanosecond(Some(v), tz) => LogicalScalar::Timestamp(
                LogicalTimestamp::new(TimeUnit::Nanosecond, tz, v),
            ),
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
                LogicalScalar::Duration(LogicalDuration::new(TimeUnit::Second, v))
            }
            ScalarValue::DurationMillisecond(None) => LogicalScalar::Null,
            ScalarValue::DurationMillisecond(Some(v)) => {
                LogicalScalar::Duration(LogicalDuration::new(TimeUnit::Millisecond, v))
            }
            ScalarValue::DurationMicrosecond(None) => LogicalScalar::Null,
            ScalarValue::DurationMicrosecond(Some(v)) => {
                LogicalScalar::Duration(LogicalDuration::new(TimeUnit::Microsecond, v))
            }
            ScalarValue::DurationNanosecond(None) => LogicalScalar::Null,
            ScalarValue::DurationNanosecond(Some(v)) => {
                LogicalScalar::Duration(LogicalDuration::new(TimeUnit::Nanosecond, v))
            }
            ScalarValue::Union(None, _, _) => LogicalScalar::Null,
            ScalarValue::Union(Some((type_id, value)), f, _) => {
                union_to_logical_scalar(f, type_id, value)
            }
            ScalarValue::Dictionary(_, v) => (*v).into(),
        }
    }
}

/// Creates a [`LogicalList`] based on a [`ScalarValue::List`] or [`ScalarValue::LargeList`].
///
/// We use multiple parameters (instead of a single [`ListArray`]) to handle [`DataType::List`] and
/// [`DataType::LargeList`] in a single function.
fn list_to_logical_scalar(
    value_type: DataType,
    nullable: bool,
    values: &dyn Array,
) -> LogicalScalar {
    let element_type = Arc::new(LogicalField {
        name: String::from("values"),
        logical_type: Arc::new(NativeType::from(value_type)),
        nullable,
    });

    // TODO logical-values: Can we avoid returning a result?
    let logical_values = (0..values.len())
        .map(|i| ScalarValue::try_from_array(values, i))
        .map(|v| v.map(LogicalScalar::from))
        .collect::<crate::Result<Vec<_>>>()
        .expect("Extracting a ScalarValue from a ListArray should not fail");

    let values = LogicalList::try_new(element_type, logical_values)
        .expect("Types must have been ensured by ScalarValue");
    LogicalScalar::List(values)
}

/// Creates a [LogicalFixedSizeList] from a [FixedSizeListArray]  and wraps it in a `LogicalScalar`.
fn fixed_size_list_to_logical_scalar(list: Arc<FixedSizeListArray>) -> LogicalScalar {
    let list =
        list_to_logical_scalar(list.value_type(), list.is_nullable(), list.values());
    let LogicalScalar::List(list) = list else {
        unreachable!()
    };
    LogicalScalar::FixedSizeList(
        LogicalFixedSizeList::try_from(list).expect("FixedSizeList cannot be too big"),
    )
}

/// Creates a [LogicalStruct] from a [StructArray] and wraps it in a `LogicalScalar`.
fn struct_to_logical_scalar(value: Arc<StructArray>) -> LogicalScalar {
    let fields = value
        .fields()
        .iter()
        .map(|f| Arc::new(LogicalField::from(f.as_ref())))
        .collect::<LogicalFields>();

    let mut values = HashMap::new();
    for f in fields.iter() {
        let value_array = value
            .column_by_name(f.name())
            .expect("Fields extracted from value");
        let value = ScalarValue::try_from_array(value_array, 0)
            .expect("Extracting a scalar must succeed");
        values.insert(f.name().to_string(), LogicalScalar::from(value));
    }

    let logical_struct = LogicalStruct::try_new(fields, values)
        .expect("Fields extracted from StructArray");
    LogicalScalar::Struct(logical_struct)
}

fn map_to_logical_scalar(value: Arc<MapArray>) -> LogicalScalar {
    let keys = value.keys();
    let values = value.values();
    assert_eq!(keys.len(), values.len(), "Illegal map array");

    let mut map = HashMap::new();
    for i in 0..keys.len() {
        let key = ScalarValue::try_from_array(keys, i)
            .expect("Should be able to create a ScalarValue");
        let value = ScalarValue::try_from_array(values, i)
            .expect("Should be able to create a ScalarValue");
        map.insert(LogicalScalar::from(key), LogicalScalar::from(value));
    }

    LogicalScalar::Map(
        LogicalMap::try_new(map).expect("Physical map should also not have null as key"),
    )
}

fn union_to_logical_scalar(
    union_fields: UnionFields,
    type_id: i8,
    value: Box<ScalarValue>,
) -> LogicalScalar {
    let union_fields = union_fields
        .iter()
        .map(|(tid, f)| (tid, Arc::new(LogicalField::from(f.as_ref()))))
        .collect::<LogicalUnionFields>();
    let value = LogicalScalar::from(*value);
    let union = LogicalUnion::try_new(union_fields, type_id, value)
        .expect("Fields extracted from union");
    LogicalScalar::Union(union)
}
