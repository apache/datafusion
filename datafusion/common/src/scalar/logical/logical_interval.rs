use arrow::datatypes::{IntervalDayTime, IntervalMonthDayNano};
use arrow_schema::IntervalUnit;
use crate::types::{logical_interval, LogicalTypeRef, NativeType};

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
