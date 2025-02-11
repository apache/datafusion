use crate::types::{logical_duration, LogicalTypeRef};
use arrow_schema::TimeUnit;
use chrono::Duration;
use std::fmt::{Display, Formatter};

/// TODO logical-types
#[derive(Debug, Clone, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct LogicalDuration {
    time_unit: TimeUnit,
    value: i64,
}

impl LogicalDuration {
    pub fn new(time_unit: TimeUnit, value: i64) -> LogicalDuration {
        LogicalDuration { time_unit, value }
    }

    /// Returns the logical type of this value.
    pub fn logical_type(&self) -> LogicalTypeRef {
        logical_duration(self.time_unit)
    }

    /// Returns the value as [Duration].
    pub fn value(&self) -> Duration {
        match self.time_unit {
            TimeUnit::Second => Duration::seconds(self.value),
            TimeUnit::Millisecond => Duration::milliseconds(self.value),
            TimeUnit::Microsecond => Duration::microseconds(self.value),
            TimeUnit::Nanosecond => Duration::nanoseconds(self.value),
        }
    }
}

impl Display for LogicalDuration {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value())
    }
}
