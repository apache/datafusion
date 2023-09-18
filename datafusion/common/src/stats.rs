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

use arrow::datatypes::{DataType, SchemaRef};
use std::fmt::Display;

use arrow::datatypes::DataType;

use crate::ScalarValue;

/// Statistics for a relation
/// Fields are optional and can be inexact because the sources
/// sometimes provide approximate estimates for performance reasons
/// and the transformations output are not always predictable.
#[derive(Debug, Clone, PartialEq, Eq)]
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

impl Statistics {
    /// Returns a [`Statistics`] instance corresponding to the given schema by assigning infinite
    /// bounds to each column in the schema. This is useful when the input statistics are not
    /// known to give an opportunity to the current executor to shrink the bounds of some columns.
    pub fn new_with_unbounded_columns(schema: SchemaRef) -> Self {
        Self {
            num_rows: None,
            total_byte_size: None,
            column_statistics: Some(
                schema
                    .fields()
                    .iter()
                    .map(|field| {
                        let inf = ScalarValue::try_from(field.data_type()).ok();
                        ColumnStatistics {
                            null_count: None,
                            max_value: inf.clone(),
                            min_value: inf,
                            distinct_count: None,
                        }
                    })
                    .collect(),
            ),
            is_exact: false,
        }
    }
}

impl Display for Statistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.num_rows.is_none() && self.total_byte_size.is_none() && !self.is_exact {
            return Ok(());
        }

        let rows = self
            .num_rows
            .map_or_else(|| "None".to_string(), |v| v.to_string());
        let bytes = self
            .total_byte_size
            .map_or_else(|| "None".to_string(), |v| v.to_string());

        write!(f, "rows={}, bytes={}, exact={}", rows, bytes, self.is_exact)?;

        Ok(())
    }
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

impl ColumnStatistics {
    /// Column contains a single non null value (e.g constant).
    pub fn is_singleton(&self) -> bool {
        match (&self.min_value, &self.max_value) {
            // Min and max values are the same and not infinity.
            (Some(min), Some(max)) => !min.is_null() && !max.is_null() && (min == max),
            (_, _) => false,
        }
    }

    /// Returns the [`ColumnStatistics`] corresponding to the given datatype by assigning infinite bounds.
    pub fn new_with_unbounded_column(dt: &DataType) -> ColumnStatistics {
        let null = ScalarValue::try_from(dt.clone()).ok();
        ColumnStatistics {
            null_count: None,
            max_value: null.clone(),
            min_value: null,
            distinct_count: None,
        }
    }
}
