use crate::types::{logical_date, LogicalTypeRef};
use crate::{Result, _internal_datafusion_err};
use arrow_array::temporal_conversions::date32_to_datetime;
use chrono::NaiveDate;

/// TODO logical-types
#[derive(Debug, Clone, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct LogicalDate {
    value: i32,
}

impl LogicalDate {
    /// Creates a new [LogicalDate].
    pub fn new(value: i32) -> LogicalDate {
        LogicalDate { value }
    }

    /// Returns the logical type of this value.
    pub fn logical_type(&self) -> LogicalTypeRef {
        logical_date()
    }

    /// Returns the value of this [LogicalDate] as [NaiveDate].
    pub fn value(&self) -> Result<NaiveDate> {
        date32_to_datetime(self.value)
            .ok_or(_internal_datafusion_err!(
                "Unable to convert LogicalDate to NaiveDate"
            ))
            .map(|d| d.date())
    }
}
