use arrow::datatypes::DataType;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::sync::Arc;

use crate::scalar::ScalarStructBuilder;
use crate::ScalarValue;

use arrow::array::*;
use arrow::datatypes::*;

impl Serialize for ScalarValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ScalarValue::Null => {
                let mut st = serializer.serialize_struct("ScalarValue", 1)?;
                st.serialize_field("type", "Null")?;
                st.end()
            }
            ScalarValue::Boolean(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "Boolean")?;
                st.serialize_field("value", v)?;
                st.end()
            }
            ScalarValue::Float16(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "Float16")?;
                st.serialize_field("value", &v.map(|f| f.to_f32()))?;
                st.end()
            }
            ScalarValue::Float32(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "Float32")?;
                st.serialize_field("value", v)?;
                st.end()
            }
            ScalarValue::Float64(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "Float64")?;
                st.serialize_field("value", v)?;
                st.end()
            }
            ScalarValue::Decimal128(v, p, s) => {
                let mut st = serializer.serialize_struct("ScalarValue", 4)?;
                st.serialize_field("type", "Decimal128")?;
                st.serialize_field("value", &v.map(|x| x.to_string()))?;
                st.serialize_field("precision", p)?;
                st.serialize_field("scale", s)?;
                st.end()
            }
            ScalarValue::Decimal256(v, p, s) => {
                let mut st = serializer.serialize_struct("ScalarValue", 4)?;
                st.serialize_field("type", "Decimal256")?;
                st.serialize_field("value", &v.map(|x| x.to_string()))?;
                st.serialize_field("precision", p)?;
                st.serialize_field("scale", s)?;
                st.end()
            }
            ScalarValue::Int8(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "Int8")?;
                st.serialize_field("value", v)?;
                st.end()
            }
            ScalarValue::Int16(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "Int16")?;
                st.serialize_field("value", v)?;
                st.end()
            }
            ScalarValue::Int32(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "Int32")?;
                st.serialize_field("value", v)?;
                st.end()
            }
            ScalarValue::Int64(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "Int64")?;
                st.serialize_field("value", v)?;
                st.end()
            }
            ScalarValue::UInt8(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "UInt8")?;
                st.serialize_field("value", v)?;
                st.end()
            }
            ScalarValue::UInt16(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "UInt16")?;
                st.serialize_field("value", v)?;
                st.end()
            }
            ScalarValue::UInt32(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "UInt32")?;
                st.serialize_field("value", v)?;
                st.end()
            }
            ScalarValue::UInt64(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "UInt64")?;
                st.serialize_field("value", v)?;
                st.end()
            }
            ScalarValue::Utf8(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "Utf8")?;
                st.serialize_field("value", v)?;
                st.end()
            }
            ScalarValue::LargeUtf8(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "LargeUtf8")?;
                st.serialize_field("value", v)?;
                st.end()
            }
            ScalarValue::Binary(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "Binary")?;
                st.serialize_field("value", &v.as_ref().map(|b| base64::encode(b)))?;
                st.end()
            }
            ScalarValue::LargeBinary(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "LargeBinary")?;
                st.serialize_field("value", &v.as_ref().map(|b| base64::encode(b)))?;
                st.end()
            }
            ScalarValue::FixedSizeBinary(size, v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 3)?;
                st.serialize_field("type", "FixedSizeBinary")?;
                st.serialize_field("size", size)?;
                st.serialize_field("value", &v.as_ref().map(|b| base64::encode(b)))?;
                st.end()
            }
            ScalarValue::List(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 3)?;
                st.serialize_field("type", "List")?;
                if let arr = v.as_ref() {
                    let list_data_type = arr.data_type();
                    println!("arr len {}", arr.len());
                    if let DataType::List(field) = list_data_type {
                        st.serialize_field("child_type", &field.data_type().to_string())?;
                        let values = arr.value(0);
                        let nested_values = if arr.is_null(0) {
                            vec![None]
                        } else {
                            let ret = (0..values.len())
                                .map(|i| {
                                    ScalarValue::try_from_array(&values, i)
                                        .map(Some)
                                        .unwrap()
                                })
                                .collect::<Vec<Option<ScalarValue>>>();
                            ret
                        };
                        st.serialize_field("value", &nested_values)?;
                    } else {
                        return Err(serde::ser::Error::custom("Invalid List data type"));
                    }
                } else {
                    st.serialize_field("child_type", &DataType::Null.to_string())?;
                    st.serialize_field(
                        "value",
                        &Option::<Vec<Option<ScalarValue>>>::None,
                    )?;
                }
                st.end()
            }
            ScalarValue::Date32(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "Date32")?;
                st.serialize_field("value", v)?;
                st.end()
            }
            ScalarValue::Date64(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "Date64")?;
                st.serialize_field("value", v)?;
                st.end()
            }
            ScalarValue::Time32Second(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "Time32Second")?;
                st.serialize_field("value", v)?;
                st.end()
            }
            ScalarValue::Time32Millisecond(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "Time32Millisecond")?;
                st.serialize_field("value", v)?;
                st.end()
            }
            ScalarValue::Time64Microsecond(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "Time64Microsecond")?;
                st.serialize_field("value", v)?;
                st.end()
            }
            ScalarValue::Time64Nanosecond(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "Time64Nanosecond")?;
                st.serialize_field("value", v)?;
                st.end()
            }
            ScalarValue::TimestampSecond(v, tz)
            | ScalarValue::TimestampMillisecond(v, tz)
            | ScalarValue::TimestampMicrosecond(v, tz)
            | ScalarValue::TimestampNanosecond(v, tz) => {
                let mut st = serializer.serialize_struct("ScalarValue", 4)?;
                st.serialize_field("type", "Timestamp")?;
                st.serialize_field("value", v)?;
                st.serialize_field("timezone", &tz.as_ref().map(|s| s.to_string()))?;
                st.serialize_field(
                    "unit",
                    match self {
                        ScalarValue::TimestampSecond(_, _) => "Second",
                        ScalarValue::TimestampMillisecond(_, _) => "Millisecond",
                        ScalarValue::TimestampMicrosecond(_, _) => "Microsecond",
                        ScalarValue::TimestampNanosecond(_, _) => "Nanosecond",
                        _ => unreachable!(),
                    },
                )?;
                st.end()
            }
            ScalarValue::IntervalYearMonth(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "IntervalYearMonth")?;
                st.serialize_field("value", v)?;
                st.end()
            }
            ScalarValue::IntervalDayTime(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "IntervalDayTime")?;
                st.serialize_field("value", &v.map(|x| (x.days, x.milliseconds)))?;
                st.end()
            }
            ScalarValue::IntervalMonthDayNano(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "IntervalMonthDayNano")?;
                st.serialize_field(
                    "value",
                    &v.map(|x| (x.months, x.days, x.nanoseconds)),
                )?;
                st.end()
            }
            ScalarValue::DurationSecond(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "DurationSecond")?;
                st.serialize_field("value", v)?;
                st.end()
            }
            ScalarValue::DurationMillisecond(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "DurationMillisecond")?;
                st.serialize_field("value", v)?;
                st.end()
            }
            ScalarValue::DurationMicrosecond(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "DurationMicrosecond")?;
                st.serialize_field("value", v)?;
                st.end()
            }
            ScalarValue::DurationNanosecond(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "DurationNanosecond")?;
                st.serialize_field("value", v)?;
                st.end()
            }
            ScalarValue::Union(v, fields, mode) => {
                let mut st = serializer.serialize_struct("ScalarValue", 4)?;
                st.serialize_field("type", "Union")?;
                st.serialize_field("value", v)?;
                st.serialize_field(
                    "fields",
                    &fields
                        .iter()
                        .map(|(i, f)| {
                            (i, f.name().to_string(), f.data_type().to_string())
                        })
                        .collect::<Vec<_>>(),
                )?;
                st.serialize_field(
                    "mode",
                    match mode {
                        UnionMode::Sparse => "Sparse",
                        UnionMode::Dense => "Dense",
                    },
                )?;
                st.end()
            }
            ScalarValue::Dictionary(key_type, value) => {
                let mut st = serializer.serialize_struct("ScalarValue", 3)?;
                st.serialize_field("type", "Dictionary")?;
                st.serialize_field("key_type", &key_type.to_string())?;
                st.serialize_field("value", value)?;
                st.end()
            }
            ScalarValue::Utf8View(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "Utf8View")?;
                st.serialize_field("value", v)?;
                st.end()
            }
            ScalarValue::BinaryView(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 2)?;
                st.serialize_field("type", "BinaryView")?;
                st.serialize_field("value", &v.as_ref().map(|b| base64::encode(b)))?;
                st.end()
            }
            ScalarValue::LargeList(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 3)?;
                st.serialize_field("type", "LargeList")?;
                if let arr = v.as_ref() {
                    let list_data_type = arr.data_type();
                    if let DataType::LargeList(field) = list_data_type {
                        st.serialize_field("child_type", &field.data_type().to_string())?;
                        let values = arr.value(0);
                        let nested_values = if arr.is_null(0) {
                            vec![None]
                        } else {
                            let ret = (0..values.len())
                                .map(|i| {
                                    ScalarValue::try_from_array(&values, i)
                                        .map(Some)
                                        .unwrap()
                                })
                                .collect::<Vec<Option<ScalarValue>>>();
                            ret
                        };
                        st.serialize_field("value", &nested_values)?;
                    } else {
                        return Err(serde::ser::Error::custom(
                            "Invalid LargeList data type",
                        ));
                    }
                } else {
                    st.serialize_field("child_type", &DataType::Null.to_string())?;
                    st.serialize_field(
                        "value",
                        &Option::<Vec<Option<ScalarValue>>>::None,
                    )?;
                }
                st.end()
            }
            ScalarValue::FixedSizeList(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 4)?;
                st.serialize_field("type", "FixedSizeList")?;
                if let arr = v.as_ref() {
                    let list_data_type = arr.data_type();
                    if let DataType::FixedSizeList(field, size) = list_data_type {
                        st.serialize_field("child_type", &field.data_type().to_string())?;
                        st.serialize_field("size", size)?;
                        let values = arr.value(0);
                        let nested_values = if arr.is_null(0) {
                            vec![None]
                        } else {
                            let ret = (0..values.len())
                                .map(|i| {
                                    ScalarValue::try_from_array(&values, i)
                                        .map(Some)
                                        .unwrap()
                                })
                                .collect::<Vec<Option<ScalarValue>>>();
                            ret
                        };
                        st.serialize_field("value", &nested_values)?;
                    } else {
                        return Err(serde::ser::Error::custom(
                            "Invalid FixedSizeList data type",
                        ));
                    }
                } else {
                    st.serialize_field("child_type", &DataType::Null.to_string())?;
                    st.serialize_field("size", &0)?;
                    st.serialize_field(
                        "value",
                        &Option::<Vec<Option<ScalarValue>>>::None,
                    )?;
                }
                st.end()
            }
            ScalarValue::Struct(v) => {
                let mut st = serializer.serialize_struct("ScalarValue", 3)?;
                st.serialize_field("type", "Struct")?;
                if let struct_arr = v.as_ref() {
                    let fields: Vec<(String, String)> = struct_arr
                        .fields()
                        .iter()
                        .map(|f| (f.name().to_string(), f.data_type().to_string()))
                        .collect();
                    st.serialize_field("fields", &fields)?;

                    let values: Vec<Option<ScalarValue>> = struct_arr
                        .columns()
                        .iter()
                        .enumerate()
                        .map(|(i, field)| {
                            if struct_arr.is_null(0) {
                                Ok(None)
                            } else {
                                ScalarValue::try_from_array(field, 0)
                                    .map(Some)
                                    .map_err(serde::ser::Error::custom)
                            }
                        })
                        .collect::<Result<_, S::Error>>()?;
                    st.serialize_field("value", &values)?;
                } else {
                    st.serialize_field("fields", &Vec::<(String, String)>::new())?;
                    st.serialize_field(
                        "value",
                        &Option::<Vec<Option<ScalarValue>>>::None,
                    )?;
                }
                st.end()
            }
        }
    }
}

use std::str::FromStr;

impl<'de> Deserialize<'de> for ScalarValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(tag = "type")]
        enum ScalarValueHelper {
            Null,
            Boolean {
                value: Option<bool>,
            },
            Float16 {
                value: Option<f32>,
            },
            Float32 {
                value: Option<f32>,
            },
            Float64 {
                value: Option<f64>,
            },
            Decimal128 {
                value: Option<String>,
                precision: u8,
                scale: i8,
            },
            Decimal256 {
                value: Option<String>,
                precision: u8,
                scale: i8,
            },
            Int8 {
                value: Option<i8>,
            },
            Int16 {
                value: Option<i16>,
            },
            Int32 {
                value: Option<i32>,
            },
            Int64 {
                value: Option<i64>,
            },
            UInt8 {
                value: Option<u8>,
            },
            UInt16 {
                value: Option<u16>,
            },
            UInt32 {
                value: Option<u32>,
            },
            UInt64 {
                value: Option<u64>,
            },
            Utf8 {
                value: Option<String>,
            },
            LargeUtf8 {
                value: Option<String>,
            },
            Binary {
                value: Option<String>,
            },
            LargeBinary {
                value: Option<String>,
            },
            FixedSizeBinary {
                size: i32,
                value: Option<String>,
            },
            List {
                child_type: String,
                value: Option<Vec<Option<ScalarValue>>>,
            },
            LargeList {
                child_type: String,
                value: Option<Vec<Option<ScalarValue>>>,
            },
            FixedSizeList {
                child_type: String,
                size: usize,
                value: Option<Vec<Option<ScalarValue>>>,
            },
            Date32 {
                value: Option<i32>,
            },
            Date64 {
                value: Option<i64>,
            },
            Time32Second {
                value: Option<i32>,
            },
            Time32Millisecond {
                value: Option<i32>,
            },
            Time64Microsecond {
                value: Option<i64>,
            },
            Time64Nanosecond {
                value: Option<i64>,
            },
            Timestamp {
                value: Option<i64>,
                timezone: Option<String>,
                unit: String,
            },
            IntervalYearMonth {
                value: Option<i32>,
            },
            IntervalDayTime {
                value: Option<(i32, i32)>,
            },
            IntervalMonthDayNano {
                value: Option<(i32, i32, i64)>,
            },
            DurationSecond {
                value: Option<i64>,
            },
            DurationMillisecond {
                value: Option<i64>,
            },
            DurationMicrosecond {
                value: Option<i64>,
            },
            DurationNanosecond {
                value: Option<i64>,
            },
            Union {
                value: Option<(i8, Box<ScalarValue>)>,
                fields: Vec<(i8, String, String)>,
                mode: String,
            },
            Dictionary {
                key_type: String,
                value: Box<ScalarValue>,
            },
            Utf8View {
                value: Option<String>,
            },
            BinaryView {
                value: Option<String>,
            },
            Struct {
                fields: Vec<(String, String)>,
                value: Option<Vec<Option<ScalarValue>>>,
            },
        }

        let helper = ScalarValueHelper::deserialize(deserializer)?;

        Ok(match helper {
            ScalarValueHelper::Null => ScalarValue::Null,
            ScalarValueHelper::Boolean { value } => ScalarValue::Boolean(value),
            ScalarValueHelper::Float16 { value } => {
                ScalarValue::Float16(value.map(half::f16::from_f32))
            }
            ScalarValueHelper::Float32 { value } => ScalarValue::Float32(value),
            ScalarValueHelper::Float64 { value } => ScalarValue::Float64(value),
            ScalarValueHelper::Decimal128 {
                value,
                precision,
                scale,
            } => ScalarValue::Decimal128(
                value.map(|s| s.parse().unwrap()), //TODO: fix me
                precision,
                scale,
            ),
            ScalarValueHelper::Decimal256 {
                value,
                precision,
                scale,
            } => ScalarValue::Decimal256(
                value.map(|s| s.parse().unwrap()),
                precision,
                scale,
            ),
            ScalarValueHelper::Int8 { value } => ScalarValue::Int8(value),
            ScalarValueHelper::Int16 { value } => ScalarValue::Int16(value),
            ScalarValueHelper::Int32 { value } => ScalarValue::Int32(value),
            ScalarValueHelper::Int64 { value } => ScalarValue::Int64(value),
            ScalarValueHelper::UInt8 { value } => ScalarValue::UInt8(value),
            ScalarValueHelper::UInt16 { value } => ScalarValue::UInt16(value),
            ScalarValueHelper::UInt32 { value } => ScalarValue::UInt32(value),
            ScalarValueHelper::UInt64 { value } => ScalarValue::UInt64(value),
            ScalarValueHelper::Utf8 { value } => ScalarValue::Utf8(value),
            ScalarValueHelper::LargeUtf8 { value } => ScalarValue::LargeUtf8(value),
            ScalarValueHelper::Binary { value } => {
                ScalarValue::Binary(value.map(|s| base64::decode(s).unwrap()))
            }
            ScalarValueHelper::LargeBinary { value } => {
                ScalarValue::LargeBinary(value.map(|s| base64::decode(s).unwrap()))
            }
            ScalarValueHelper::FixedSizeBinary { size, value } => {
                ScalarValue::FixedSizeBinary(
                    size,
                    value.map(|s| base64::decode(s).unwrap()),
                )
            }
            ScalarValueHelper::List { child_type, value } => {
                let field = Arc::new(Field::new(
                    "item",
                    DataType::from_str(&child_type).map_err(serde::de::Error::custom)?,
                    true,
                ));
                let values: Vec<Option<ScalarValue>> = value.unwrap_or_default();
                let scalar_values: Vec<ScalarValue> = values
                    .into_iter()
                    .map(|v| v.unwrap_or(ScalarValue::Null))
                    .collect();
                let data = ScalarValue::new_list(&scalar_values, &field.data_type());
                ScalarValue::List(data)
            }
            ScalarValueHelper::LargeList { child_type, value } => {
                let field = Arc::new(Field::new(
                    "item",
                    DataType::from_str(&child_type).map_err(serde::de::Error::custom)?,
                    true,
                ));
                let values: Vec<Option<ScalarValue>> = value.unwrap_or_default();
                let scalar_values: Vec<ScalarValue> = values
                    .into_iter()
                    .map(|v| v.unwrap_or(ScalarValue::Null))
                    .collect();
                ScalarValue::LargeList(ScalarValue::new_large_list(
                    &scalar_values,
                    &field.data_type(),
                ))
            }
            ScalarValueHelper::FixedSizeList {
                child_type,
                size,
                value,
            } => {
                let field = Arc::new(Field::new(
                    "item",
                    DataType::from_str(&child_type).map_err(serde::de::Error::custom)?,
                    true,
                ));
                let values: Vec<Option<ScalarValue>> = value.unwrap_or_default();
                let scalar_values: Vec<ScalarValue> = values
                    .into_iter()
                    .map(|v| v.unwrap_or(ScalarValue::Null))
                    .collect();
                let data_type =
                    DataType::FixedSizeList(field, scalar_values.len() as i32);
                let value_data =
                    ScalarValue::new_list(&scalar_values, &data_type).to_data();
                let list_array = FixedSizeListArray::from(value_data);
                ScalarValue::FixedSizeList(Arc::new(list_array))
            }
            ScalarValueHelper::Date32 { value } => ScalarValue::Date32(value),
            ScalarValueHelper::Date64 { value } => ScalarValue::Date64(value),
            ScalarValueHelper::Time32Second { value } => ScalarValue::Time32Second(value),
            ScalarValueHelper::Time32Millisecond { value } => {
                ScalarValue::Time32Millisecond(value)
            }
            ScalarValueHelper::Time64Microsecond { value } => {
                ScalarValue::Time64Microsecond(value)
            }
            ScalarValueHelper::Time64Nanosecond { value } => {
                ScalarValue::Time64Nanosecond(value)
            }
            ScalarValueHelper::Timestamp {
                value,
                timezone,
                unit,
            } => match unit.as_str() {
                "Second" => ScalarValue::TimestampSecond(value, timezone.map(Arc::from)),
                "Millisecond" => {
                    ScalarValue::TimestampMillisecond(value, timezone.map(Arc::from))
                }
                "Microsecond" => {
                    ScalarValue::TimestampMicrosecond(value, timezone.map(Arc::from))
                }
                "Nanosecond" => {
                    ScalarValue::TimestampNanosecond(value, timezone.map(Arc::from))
                }
                _ => return Err(serde::de::Error::custom("Invalid timestamp unit")),
            },
            ScalarValueHelper::IntervalYearMonth { value } => {
                ScalarValue::IntervalYearMonth(value)
            }
            ScalarValueHelper::IntervalDayTime { value } => ScalarValue::IntervalDayTime(
                value.map(|(days, millis)| IntervalDayTime::new(days, millis)),
            ),
            ScalarValueHelper::IntervalMonthDayNano { value } => {
                ScalarValue::IntervalMonthDayNano(value.map(|(months, days, nanos)| {
                    IntervalMonthDayNano::new(months, days, nanos)
                }))
            }
            ScalarValueHelper::DurationSecond { value } => {
                ScalarValue::DurationSecond(value)
            }
            ScalarValueHelper::DurationMillisecond { value } => {
                ScalarValue::DurationMillisecond(value)
            }
            ScalarValueHelper::DurationMicrosecond { value } => {
                ScalarValue::DurationMicrosecond(value)
            }
            ScalarValueHelper::DurationNanosecond { value } => {
                ScalarValue::DurationNanosecond(value)
            }
            ScalarValueHelper::Union {
                value,
                fields,
                mode,
            } => {
                let union_fields = fields
                    .into_iter()
                    .map(|(i, name, type_str)| {
                        (
                            i,
                            Arc::new(Field::new(
                                name,
                                DataType::from_str(&type_str).unwrap(),
                                true,
                            )),
                        )
                    })
                    .collect();
                let union_mode = match mode.as_str() {
                    "Sparse" => UnionMode::Sparse,
                    "Dense" => UnionMode::Dense,
                    _ => return Err(serde::de::Error::custom("Invalid union mode")),
                };
                ScalarValue::Union(value, union_fields, union_mode)
            }
            ScalarValueHelper::Dictionary { key_type, value } => ScalarValue::Dictionary(
                Box::new(DataType::from_str(&key_type).unwrap()),
                value,
            ),
            ScalarValueHelper::Utf8View { value } => ScalarValue::Utf8View(value),
            ScalarValueHelper::BinaryView { value } => {
                ScalarValue::BinaryView(value.map(|s| base64::decode(s).unwrap()))
            }
            ScalarValueHelper::Struct { fields, value } => {
                let struct_fields: Vec<Field> = fields
                    .into_iter()
                    .map(|(name, type_str)| {
                        Field::new(name, DataType::from_str(&type_str).unwrap(), true)
                    })
                    .collect();
                let values: Vec<Option<ScalarValue>> = value.unwrap_or_default();
                let scalar_values: Vec<ScalarValue> = values
                    .into_iter()
                    .map(|v| v.unwrap_or(ScalarValue::Null))
                    .collect();
                let mut builder = ScalarStructBuilder::new();
                for (field, value) in struct_fields.clone().into_iter().zip(scalar_values)
                {
                    builder = builder.with_scalar(field, value);
                }
                let struct_array = builder.build().unwrap();

                ScalarValue::Struct(Arc::new(StructArray::new(
                    Fields::from(struct_fields),
                    vec![struct_array.to_array().unwrap()],
                    None,
                )))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;
    use std::sync::Arc;

    fn test_serde_roundtrip(scalar: ScalarValue) {
        let serialized = serde_json::to_string(&scalar).unwrap();
        let deserialized: ScalarValue = serde_json::from_str(&serialized).unwrap();
        assert_eq!(scalar, deserialized);
    }

    #[test]
    fn test_large_utf8() {
        test_serde_roundtrip(ScalarValue::LargeUtf8(Some("hello".to_string())));
        test_serde_roundtrip(ScalarValue::LargeUtf8(None));
    }

    #[test]
    fn test_binary() {
        test_serde_roundtrip(ScalarValue::Binary(Some(vec![1, 2, 3])));
        test_serde_roundtrip(ScalarValue::Binary(None));
    }

    #[test]
    fn test_large_binary() {
        test_serde_roundtrip(ScalarValue::LargeBinary(Some(vec![1, 2, 3])));
        test_serde_roundtrip(ScalarValue::LargeBinary(None));
    }

    #[test]
    fn test_fixed_size_binary() {
        test_serde_roundtrip(ScalarValue::FixedSizeBinary(3, Some(vec![1, 2, 3])));
        test_serde_roundtrip(ScalarValue::FixedSizeBinary(3, None));
    }

    #[test]
    fn test_list() {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(1),
            Some(2),
            Some(3),
        ])]);
        test_serde_roundtrip(ScalarValue::List(Arc::new(list)));
    }

    #[test]
    fn test_large_list() {
        let list =
            LargeListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
                Some(1),
                Some(2),
                Some(3),
            ])]);
        test_serde_roundtrip(ScalarValue::LargeList(Arc::new(list)));
    }

    // #[test]
    // fn test_fixed_size_list() {
    //     let list = FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
    //         vec![Some(vec![Some(1), Some(2), Some(3)])],
    //         3,
    //     );
    //     test_serde_roundtrip(ScalarValue::FixedSizeList(Arc::new(list)));
    // }

    #[test]
    fn test_date32() {
        test_serde_roundtrip(ScalarValue::Date32(Some(1000)));
        test_serde_roundtrip(ScalarValue::Date32(None));
    }

    #[test]
    fn test_date64() {
        test_serde_roundtrip(ScalarValue::Date64(Some(86400000)));
        test_serde_roundtrip(ScalarValue::Date64(None));
    }

    #[test]
    fn test_time32_second() {
        test_serde_roundtrip(ScalarValue::Time32Second(Some(3600)));
        test_serde_roundtrip(ScalarValue::Time32Second(None));
    }

    #[test]
    fn test_time32_millisecond() {
        test_serde_roundtrip(ScalarValue::Time32Millisecond(Some(3600000)));
        test_serde_roundtrip(ScalarValue::Time32Millisecond(None));
    }

    #[test]
    fn test_time64_microsecond() {
        test_serde_roundtrip(ScalarValue::Time64Microsecond(Some(3600000000)));
        test_serde_roundtrip(ScalarValue::Time64Microsecond(None));
    }

    #[test]
    fn test_time64_nanosecond() {
        test_serde_roundtrip(ScalarValue::Time64Nanosecond(Some(3600000000000)));
        test_serde_roundtrip(ScalarValue::Time64Nanosecond(None));
    }

    #[test]
    fn test_timestamp() {
        test_serde_roundtrip(ScalarValue::TimestampSecond(
            Some(1625097600),
            Some(Arc::from("UTC")),
        ));
        test_serde_roundtrip(ScalarValue::TimestampMillisecond(
            Some(1625097600000),
            None,
        ));
        test_serde_roundtrip(ScalarValue::TimestampMicrosecond(
            Some(1625097600000000),
            Some(Arc::from("UTC")),
        ));
        test_serde_roundtrip(ScalarValue::TimestampNanosecond(
            Some(1625097600000000000),
            None,
        ));
    }

    #[test]
    fn test_interval_year_month() {
        test_serde_roundtrip(ScalarValue::IntervalYearMonth(Some(14)));
        test_serde_roundtrip(ScalarValue::IntervalYearMonth(None));
    }

    #[test]
    fn test_interval_day_time() {
        test_serde_roundtrip(ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(
            5, 43200000,
        ))));
        test_serde_roundtrip(ScalarValue::IntervalDayTime(None));
    }

    #[test]
    fn test_interval_month_day_nano() {
        test_serde_roundtrip(ScalarValue::IntervalMonthDayNano(Some(
            IntervalMonthDayNano::new(1, 15, 1000000000),
        )));
        test_serde_roundtrip(ScalarValue::IntervalMonthDayNano(None));
    }

    #[test]
    fn test_duration() {
        test_serde_roundtrip(ScalarValue::DurationSecond(Some(3600)));
        test_serde_roundtrip(ScalarValue::DurationMillisecond(Some(3600000)));
        test_serde_roundtrip(ScalarValue::DurationMicrosecond(Some(3600000000)));
        test_serde_roundtrip(ScalarValue::DurationNanosecond(Some(3600000000000)));
    }

    // #[test]
    // fn test_union() {
    //     let fields = vec![
    //         (0, Arc::new(Field::new("f1", DataType::Int32, true))),
    //         (1, Arc::new(Field::new("f2", DataType::Utf8, true))),
    //     ];
    //     test_serde_roundtrip(ScalarValue::Union(
    //         Some((0, Box::new(ScalarValue::Int32(Some(42))))),
    //         fields.clone(),
    //         UnionMode::Sparse,
    //     ));
    //     test_serde_roundtrip(ScalarValue::Union(None, fields, UnionMode::Dense));
    // }

    #[test]
    fn test_dictionary() {
        test_serde_roundtrip(ScalarValue::Dictionary(
            Box::new(DataType::Int8),
            Box::new(ScalarValue::Utf8(Some("hello".to_string()))),
        ));
    }

    #[test]
    fn test_utf8_view() {
        test_serde_roundtrip(ScalarValue::Utf8View(Some("hello".to_string())));
        test_serde_roundtrip(ScalarValue::Utf8View(None));
    }

    #[test]
    fn test_binary_view() {
        test_serde_roundtrip(ScalarValue::BinaryView(Some(vec![1, 2, 3])));
        test_serde_roundtrip(ScalarValue::BinaryView(None));
    }

    /* #[test]
    fn test_struct() {
        let fields = vec![
            Field::new("f1", DataType::Int32, true),
            Field::new("f2", DataType::Utf8, true),
        ];
        let values = vec![
            Some(ScalarValue::Int32(Some(42))),
            Some(ScalarValue::Utf8(Some("hello".to_string()))),
        ];
        let struct_array = StructArray::from(values);
        test_serde_roundtrip(ScalarValue::Struct(Arc::new(struct_array)));
    } */
}
