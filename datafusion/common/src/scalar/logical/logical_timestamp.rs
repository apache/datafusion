use crate::types::{logical_timestamp, LogicalTypeRef};
use arrow_schema::TimeUnit;
use std::sync::Arc;

/// TODO logical-types
#[derive(Debug, Clone, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct LogicalTimestamp {
    time_unit: TimeUnit,
    time_zone: Option<Arc<str>>,
    value: i64,
}

impl LogicalTimestamp {
    /// Creates a new [LogicalTimestamp].
    pub fn new(
        time_unit: TimeUnit,
        time_zone: Option<Arc<str>>,
        value: i64,
    ) -> LogicalTimestamp {
        LogicalTimestamp {
            time_unit,
            time_zone,
            value,
        }
    }

    /// Returns the logical type of this timestamp.
    pub fn logical_type(&self) -> LogicalTypeRef {
        logical_timestamp(self.time_unit, self.time_zone.clone())
    }
}
