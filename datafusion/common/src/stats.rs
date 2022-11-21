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

//! This module provides data structures to represent statistics

use crate::ScalarValue;

/// Statistics for a relation
/// Fields are optional and can be inexact because the sources
/// sometimes provide approximate estimates for performance reasons
/// and the transformations output are not always predictable.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Statistics {
    /// The number of table rows
    pub num_rows: Option<usize>,
    /// total bytes of the table rows
    pub total_byte_size: Option<usize>,
    /// Statistics on a column level
    pub column_statistics: Option<Vec<ColumnStatistics>>,
    /// If true, any field that is `Some(..)` is the actual value in the data provided by the operator (it is not
    /// an estimate). Any or all other fields might still be None, in which case no information is known.
    /// if false, any field that is `Some(..)` may contain an inexact estimate and may not be the actual value.
    pub is_exact: bool,
}

/// Statistics for a column within a relation
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ColumnStatistics {
    /// Number of null values on column
    pub null_count: Option<usize>,
    /// Maximum value of column
    pub max_value: Option<ScalarValue>,
    /// Minimum value of column
    pub min_value: Option<ScalarValue>,
    /// Number of distinct values
    pub distinct_count: Option<usize>,
}
