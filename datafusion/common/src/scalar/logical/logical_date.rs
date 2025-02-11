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

use crate::types::{logical_date, LogicalTypeRef};
use crate::{Result, _internal_datafusion_err};
use arrow_array::temporal_conversions::date32_to_datetime;
use chrono::NaiveDate;
use std::fmt::{Display, Formatter};

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

impl Display for LogicalDate {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value().map_err(|_| std::fmt::Error)?)
    }
}
