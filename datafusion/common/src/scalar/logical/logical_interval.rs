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

use crate::types::{logical_interval, LogicalTypeRef};
use arrow::datatypes::{IntervalDayTime, IntervalMonthDayNano};
use arrow_schema::IntervalUnit;
use std::fmt::{Display, Formatter};

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
    /// Returns the logical type of this value.
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

impl Display for LogicalInterval {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LogicalInterval::YearMonth(v) => write!(f, "{:?}", v),
            LogicalInterval::DayTime(v) => write!(f, "{:?}", v),
            LogicalInterval::MonthDayNano(v) => write!(f, "{:?}", v),
        }
    }
}
