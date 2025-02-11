use crate::types::NativeType;
use arrow_array::temporal_conversions::{
    time32ms_to_time, time32s_to_time, time64ns_to_time, time64us_to_time,
};
use arrow_schema::TimeUnit;
use chrono::NaiveTime;

/// Stores a scalar for [`NativeType::Time`].
///
/// This struct is used to ensure the integer size (`i32` or `i64`) for different [`TimeUnit`]s.
#[derive(Debug, Clone, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub enum LogicalTime {
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
