use datafusion::{error::DataFusionError, scalar::ScalarValue};
use datafusion::arrow::datatypes::{DataType, Field};
use ordered_float::OrderedFloat;
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum TokomakScalar {
    /// true or false value
    Boolean(Option<bool>),
    /// 32bit float
    Float32(Option<OrderedFloat<f32>>),
    /// 64bit float
    Float64(Option<OrderedFloat<f64>>),
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
    /// list of nested ScalarValue (boxed to reduce size_of(ScalarValue))
    #[allow(clippy::box_vec)]
    List(Option<Box<Vec<TokomakScalar>>>, Box<DataType>),
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
    /// Scalar Struct
    #[allow(clippy::box_vec)]
    Struct(Option<Box<Vec<TokomakScalar>>>, Box<Vec<Field>>)
}

//This is mostly for convience's sake when testing the optimizer so this implmentation is shoddy
impl std::str::FromStr for TokomakScalar {
    type Err = DataFusionError;
    fn from_str(s: &str) -> Result<TokomakScalar, DataFusionError> {
        let value = if let Ok(val) = s.parse() {
            TokomakScalar::Boolean(Some(val))
        } else if let Ok(val) = s.parse() {
            TokomakScalar::UInt8(Some(val))
        } else if let Ok(val) = s.parse() {
            TokomakScalar::Int8(Some(val))
        } else if let Ok(val) = s.parse() {
            TokomakScalar::UInt16(Some(val))
        } else if let Ok(val) = s.parse() {
            TokomakScalar::Int16(Some(val))
        } else if let Ok(val) = s.parse() {
            TokomakScalar::UInt32(Some(val))
        } else if let Ok(val) = s.parse() {
            TokomakScalar::Int32(Some(val))
        } else if let Ok(val) = s.parse() {
            TokomakScalar::UInt64(Some(val))
        } else if let Ok(val) = s.parse() {
            TokomakScalar::Int64(Some(val))
        } else if let Ok(val) = s.parse() {
            TokomakScalar::Float32(Some(val))
        } else if let Ok(val) = s.parse() {
            TokomakScalar::Float64(Some(val))
        } else if let Ok(val) = s.parse() {
            TokomakScalar::Boolean(Some(val))
        } else {
            let first_char = match s.chars().nth(0) {
                Some(c) => c,
                _ => return Err(DataFusionError::Internal(String::new())),
            };
            if (first_char == '?'
                || first_char.is_numeric()
                || first_char.is_ascii_graphic())
                && (first_char.is_ascii_punctuation()
                    && !(first_char == '\'' || first_char == '"'))
            {
                //println!("Could not parse: {}",first_char.is_ascii_punctuation() && !(first_char == '\'' || first_char == '"'));
                return Err(DataFusionError::Internal(String::new()));
            }
            let mut str_in = s;
            if first_char == '"' || first_char == '\'' {
                str_in = match &str_in[1..].strip_suffix(first_char) {
                    Some(v) => v,
                    None => return Err(DataFusionError::Internal(String::new())),
                };
            } else {
                return Err(DataFusionError::Internal(String::new()));
            }
            TokomakScalar::Utf8(Some(str_in.to_string()))
        };
        Ok(value)
    }
}

impl From<ScalarValue> for TokomakScalar {
    fn from(val: ScalarValue) -> Self {
        match val {
            ScalarValue::Boolean(v) => TokomakScalar::Boolean(v),
            ScalarValue::Float32(v) => TokomakScalar::Float32(v.map(OrderedFloat::from)),
            ScalarValue::Float64(v) => TokomakScalar::Float64(v.map(OrderedFloat::from)),
            ScalarValue::Int8(v) => TokomakScalar::Int8(v),
            ScalarValue::Int16(v) => TokomakScalar::Int16(v),
            ScalarValue::Int32(v) => TokomakScalar::Int32(v),
            ScalarValue::Int64(v) => TokomakScalar::Int64(v),
            ScalarValue::UInt8(v) => TokomakScalar::UInt8(v),
            ScalarValue::UInt16(v) => TokomakScalar::UInt16(v),
            ScalarValue::UInt32(v) => TokomakScalar::UInt32(v),
            ScalarValue::UInt64(v) => TokomakScalar::UInt64(v),
            ScalarValue::Utf8(v) => TokomakScalar::Utf8(v),
            ScalarValue::LargeUtf8(v) => TokomakScalar::LargeUtf8(v),
            ScalarValue::Binary(v) => TokomakScalar::Binary(v),
            ScalarValue::LargeBinary(v) => TokomakScalar::LargeBinary(v),
            ScalarValue::List(v, d) => TokomakScalar::List(
                v.map(|list| {
                    Box::new(list.into_iter().map(|item| item.into()).collect())
                }),
                d,
            ),
            ScalarValue::Date32(v) => TokomakScalar::Date32(v),
            ScalarValue::Date64(v) => TokomakScalar::Date64(v),
            ScalarValue::TimestampSecond(v) => TokomakScalar::TimestampSecond(v),
            ScalarValue::TimestampMillisecond(v) => {
                TokomakScalar::TimestampMillisecond(v)
            }
            ScalarValue::TimestampMicrosecond(v) => {
                TokomakScalar::TimestampMicrosecond(v)
            }
            ScalarValue::TimestampNanosecond(v) => TokomakScalar::TimestampNanosecond(v),
            ScalarValue::IntervalYearMonth(v) => TokomakScalar::IntervalYearMonth(v),
            ScalarValue::IntervalDayTime(v) => TokomakScalar::IntervalDayTime(v),
            ScalarValue::Struct(fields, datatypes) => {
                let fields = fields.map(|f|Box::new( f.into_iter().map(TokomakScalar::from).collect()));
                TokomakScalar::Struct(fields, datatypes)
            },
        }
    }
}
impl From<&TokomakScalar> for ScalarValue{
    fn from(val: &TokomakScalar) -> Self { 
        val.clone().into()
    }
}

impl From<TokomakScalar> for ScalarValue {
    fn from(val: TokomakScalar) -> Self {
        match val {
            TokomakScalar::Boolean(v) => ScalarValue::Boolean(v),
            TokomakScalar::Float32(v) => ScalarValue::Float32(v.map(|f| f.0)),
            TokomakScalar::Float64(v) => ScalarValue::Float64(v.map(|f| f.0)),
            TokomakScalar::Int8(v) => ScalarValue::Int8(v),
            TokomakScalar::Int16(v) => ScalarValue::Int16(v),
            TokomakScalar::Int32(v) => ScalarValue::Int32(v),
            TokomakScalar::Int64(v) => ScalarValue::Int64(v),
            TokomakScalar::UInt8(v) => ScalarValue::UInt8(v),
            TokomakScalar::UInt16(v) => ScalarValue::UInt16(v),
            TokomakScalar::UInt32(v) => ScalarValue::UInt32(v),
            TokomakScalar::UInt64(v) => ScalarValue::UInt64(v),
            TokomakScalar::Utf8(v) => ScalarValue::Utf8(v),
            TokomakScalar::LargeUtf8(v) => ScalarValue::LargeUtf8(v),
            TokomakScalar::Binary(v) => ScalarValue::Binary(v),
            TokomakScalar::LargeBinary(v) => ScalarValue::LargeBinary(v),
            TokomakScalar::List(v, d) => ScalarValue::List(
                v.map(|list| {
                    Box::new(list.into_iter().map(|item| item.into()).collect())
                }),
                d,
            ),
            TokomakScalar::Date32(v) => ScalarValue::Date32(v),
            TokomakScalar::Date64(v) => ScalarValue::Date64(v),
            TokomakScalar::TimestampSecond(v) => ScalarValue::TimestampSecond(v),
            TokomakScalar::TimestampMillisecond(v) => {
                ScalarValue::TimestampMillisecond(v)
            }
            TokomakScalar::TimestampMicrosecond(v) => {
                ScalarValue::TimestampMicrosecond(v)
            }
            TokomakScalar::TimestampNanosecond(v) => ScalarValue::TimestampNanosecond(v),
            TokomakScalar::IntervalYearMonth(v) => ScalarValue::IntervalYearMonth(v),
            TokomakScalar::IntervalDayTime(v) => ScalarValue::IntervalDayTime(v),
            TokomakScalar::Struct(fields, datatypes) => {
                let fields = fields.map(|f| Box::new(f.into_iter().map(ScalarValue::from).collect()));
                ScalarValue::Struct(fields, datatypes)
            },
        }
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

impl std::fmt::Display for TokomakScalar {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            TokomakScalar::Boolean(e) => format_option!(f, e)?,
            TokomakScalar::Float32(e) => format_option!(f, e)?,
            TokomakScalar::Float64(e) => format_option!(f, e)?,
            TokomakScalar::Int8(e) => format_option!(f, e)?,
            TokomakScalar::Int16(e) => format_option!(f, e)?,
            TokomakScalar::Int32(e) => format_option!(f, e)?,
            TokomakScalar::Int64(e) => format_option!(f, e)?,
            TokomakScalar::UInt8(e) => format_option!(f, e)?,
            TokomakScalar::UInt16(e) => format_option!(f, e)?,
            TokomakScalar::UInt32(e) => format_option!(f, e)?,
            TokomakScalar::UInt64(e) => format_option!(f, e)?,
            TokomakScalar::TimestampSecond(e) => format_option!(f, e)?,
            TokomakScalar::TimestampMillisecond(e) => format_option!(f, e)?,
            TokomakScalar::TimestampMicrosecond(e) => format_option!(f, e)?,
            TokomakScalar::TimestampNanosecond(e) => format_option!(f, e)?,
            TokomakScalar::Utf8(e) => format_option!(f, e)?,
            TokomakScalar::LargeUtf8(e) => format_option!(f, e)?,
            TokomakScalar::Binary(e) => match e {
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
            TokomakScalar::LargeBinary(e) => match e {
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
            TokomakScalar::List(e, _) => match e {
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
            TokomakScalar::Date32(e) => format_option!(f, e)?,
            TokomakScalar::Date64(e) => format_option!(f, e)?,
            TokomakScalar::IntervalDayTime(e) => format_option!(f, e)?,
            TokomakScalar::IntervalYearMonth(e) => format_option!(f, e)?,
            TokomakScalar::Struct(e, fields) => match e {
                Some(l) => write!(
                    f,
                    "{{{}}}",
                    l.iter()
                        .zip(fields.iter())
                        .map(|(value, field)| format!("{}:{}", field.name(), value))
                        .collect::<Vec<_>>()
                        .join(",")
                )?,
                None => write!(f, "NULL")?,
            },
        };

        Ok(())
    }
}
