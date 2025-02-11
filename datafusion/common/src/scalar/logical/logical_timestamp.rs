// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::types::{logical_timestamp, LogicalTypeRef};
use crate::{Result, _internal_datafusion_err};
use arrow_array::temporal_conversions::{as_datetime, as_datetime_with_timezone};
use arrow_array::timezone::Tz;
use arrow_array::types::{
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use arrow_schema::TimeUnit;
use chrono::{DateTime, NaiveDateTime};
use std::fmt::{Display, Formatter};
use std::str::FromStr;
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

    /// Returns the logical type of this value.
    pub fn logical_type(&self) -> LogicalTypeRef {
        logical_timestamp(self.time_unit, self.time_zone.clone())
    }

    /// Returns the [TimeUnit] of this value.
    pub fn time_unit(&self) -> TimeUnit {
        self.time_unit
    }

    /// Returns the value of this timestamp as [DateTime] or [NaiveDateTime] depending on whether
    /// there is a time zone.
    pub fn value(&self) -> Result<LogicalTimestampValue> {
        if let Some(tz) = &self.time_zone {
            LogicalTimestampValue::try_with_tz(self.time_unit, tz, self.value)
        } else {
            LogicalTimestampValue::try_without_tz(self.time_unit, self.value)
        }
    }
}

/// Represents the value of a [LogicalTimestamp] as [DateTime] or [NaiveDateTime].
#[derive(Debug, Clone, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub enum LogicalTimestampValue {
    WithTimezone(DateTime<Tz>),
    WithoutTimezone(NaiveDateTime),
}

impl LogicalTimestampValue {
    /// Tries to create a [LogicalTimestampValue] with time zone `tz`.
    pub fn try_with_tz(
        time_unit: TimeUnit,
        tz: &str,
        value: i64,
    ) -> Result<LogicalTimestampValue> {
        let tz = Tz::from_str(tz)?;
        let date_time = match time_unit {
            TimeUnit::Second => {
                as_datetime_with_timezone::<TimestampSecondType>(value, tz)
            }
            TimeUnit::Millisecond => {
                as_datetime_with_timezone::<TimestampMillisecondType>(value, tz)
            }
            TimeUnit::Microsecond => {
                as_datetime_with_timezone::<TimestampMicrosecondType>(value, tz)
            }
            TimeUnit::Nanosecond => {
                as_datetime_with_timezone::<TimestampNanosecondType>(value, tz)
            }
        }
        .ok_or(_internal_datafusion_err!(
            "Unable to convert {value:?} to DateTime"
        ))?;
        Ok(LogicalTimestampValue::WithTimezone(date_time))
    }

    /// Tries to create a [LogicalTimestampValue] without a time zone.
    pub fn try_without_tz(
        time_unit: TimeUnit,
        value: i64,
    ) -> Result<LogicalTimestampValue> {
        let result = match time_unit {
            TimeUnit::Second => as_datetime::<TimestampSecondType>(value),
            TimeUnit::Millisecond => as_datetime::<TimestampMillisecondType>(value),
            TimeUnit::Microsecond => as_datetime::<TimestampMicrosecondType>(value),
            TimeUnit::Nanosecond => as_datetime::<TimestampNanosecondType>(value),
        }
        .ok_or(_internal_datafusion_err!(
            "Unable to convert {value:?} to DateTime"
        ))?;
        Ok(LogicalTimestampValue::WithoutTimezone(result))
    }
}

impl Display for LogicalTimestamp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value().map_err(|_| std::fmt::Error)?)
    }
}

impl Display for LogicalTimestampValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LogicalTimestampValue::WithTimezone(v) => write!(f, "{}", v),
            LogicalTimestampValue::WithoutTimezone(v) => write!(f, "{}", v),
        }
    }
}
