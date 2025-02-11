use crate::types::{logical_duration, LogicalTypeRef};
use arrow_schema::TimeUnit;

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

    /// Returns the logical type of this duration.
    pub fn logical_type(&self) -> LogicalTypeRef {
        logical_duration(self.time_unit)
    }
}
