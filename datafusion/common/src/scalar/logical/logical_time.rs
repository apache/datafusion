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

use crate::types::{logical_time, LogicalTypeRef};
use crate::{Result, _internal_datafusion_err};
use arrow_array::temporal_conversions::{
    time32ms_to_time, time32s_to_time, time64ns_to_time, time64us_to_time,
};
use arrow_schema::TimeUnit;
use chrono::NaiveTime;
use std::fmt::{Display, Formatter};

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
    /// Returns the logical type of this value.
    pub fn logical_type(&self) -> LogicalTypeRef {
        logical_time(self.time_unit())
    }

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
    pub fn value(&self) -> Result<NaiveTime> {
        match self {
            LogicalTime::Second(v) => time32s_to_time(*v),
            LogicalTime::Millisecond(v) => time32ms_to_time(*v),
            LogicalTime::Microsecond(v) => time64us_to_time(*v),
            LogicalTime::Nanosecond(v) => time64ns_to_time(*v),
        }
        .ok_or(_internal_datafusion_err!("Cannot create NaiveTime."))
    }
}

impl Display for LogicalTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value().map_err(|_| std::fmt::Error)?)
    }
}
