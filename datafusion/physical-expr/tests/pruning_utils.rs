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

use std::collections::HashSet;

use arrow::array::{ArrayRef, BooleanArray};
use datafusion_common::pruning::PruningStatistics;
use datafusion_common::{Column, HashMap, ScalarValue};

pub struct DummyStats {
    num_containers: usize,
}

impl DummyStats {
    pub fn new(num_containers: usize) -> Self {
        Self { num_containers }
    }
}

impl PruningStatistics for DummyStats {
    fn min_values(&self, _: &Column) -> Option<ArrayRef> {
        None
    }
    fn max_values(&self, _: &Column) -> Option<ArrayRef> {
        None
    }
    fn num_containers(&self) -> usize {
        self.num_containers
    }
    fn null_counts(&self, _: &Column) -> Option<ArrayRef> {
        None
    }
    fn row_counts(&self, _: &Column) -> Option<ArrayRef> {
        None
    }
    fn contained(&self, _: &Column, _: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        None
    }
}

/// Builder for a single column's pruning statistics.
#[derive(Default)]
pub struct ColumnPruningStatistics {
    pub column: String,
    pub mins: Option<ArrayRef>,
    pub maxs: Option<ArrayRef>,
    pub null_counts: Option<ArrayRef>,
    pub row_counts: Option<ArrayRef>,
}

impl ColumnPruningStatistics {
    pub fn new(column: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            ..Default::default()
        }
    }

    pub fn with_range(mut self, mins: Option<ArrayRef>, maxs: Option<ArrayRef>) -> Self {
        self.mins = mins;
        self.maxs = maxs;
        self
    }

    pub fn with_nulls(
        mut self,
        null_counts: Option<ArrayRef>,
        row_counts: Option<ArrayRef>,
    ) -> Self {
        self.null_counts = null_counts;
        self.row_counts = row_counts;
        self
    }
}

#[derive(Clone)]
pub struct MultiColumnPruningStatistics {
    pub mins: HashMap<String, ArrayRef>,
    pub maxs: HashMap<String, ArrayRef>,
    pub null_counts: HashMap<String, ArrayRef>,
    pub row_counts: HashMap<String, ArrayRef>,
    pub num_containers: usize,
}

impl MultiColumnPruningStatistics {
    pub fn new(num_containers: usize) -> Self {
        Self {
            mins: HashMap::new(),
            maxs: HashMap::new(),
            null_counts: HashMap::new(),
            row_counts: HashMap::new(),
            num_containers,
        }
    }

    /// Insert a pre-built column statistics entry.
    pub fn with_column(mut self, column_stats: ColumnPruningStatistics) -> Self {
        self.validate_len(&column_stats.mins, "mins");
        self.validate_len(&column_stats.maxs, "maxs");
        self.validate_len(&column_stats.null_counts, "null_counts");
        self.validate_len(&column_stats.row_counts, "row_counts");

        if let Some(mins) = column_stats.mins {
            self.mins.insert(column_stats.column.clone(), mins);
        }
        if let Some(maxs) = column_stats.maxs {
            self.maxs.insert(column_stats.column.clone(), maxs);
        }
        if let Some(null_counts) = column_stats.null_counts {
            self.null_counts
                .insert(column_stats.column.clone(), null_counts);
        }
        if let Some(row_counts) = column_stats.row_counts {
            self.row_counts
                .insert(column_stats.column.clone(), row_counts);
        }

        self
    }

    fn validate_len(&self, arr: &Option<ArrayRef>, label: &str) {
        if let Some(arr) = arr {
            assert_eq!(
                arr.len(),
                self.num_containers,
                "{label} length must match num_containers"
            );
        }
    }
}

impl PruningStatistics for MultiColumnPruningStatistics {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.mins.get(&column.name).cloned()
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.maxs.get(&column.name).cloned()
    }

    fn num_containers(&self) -> usize {
        self.num_containers
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        self.null_counts.get(&column.name).cloned()
    }

    fn row_counts(&self, column: &Column) -> Option<ArrayRef> {
        self.row_counts.get(&column.name).cloned()
    }

    fn contained(&self, _: &Column, _: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        unimplemented!(
            "Sets support for stats propagation pruning is not implemented yet"
        )
    }
}

pub fn single_column_stats(
    column: impl Into<String>,
    min_values: Option<ArrayRef>,
    max_values: Option<ArrayRef>,
    null_counts: Option<ArrayRef>,
    row_counts: Option<ArrayRef>,
) -> MultiColumnPruningStatistics {
    let num_containers = min_values
        .as_ref()
        .or(max_values.as_ref())
        .or(null_counts.as_ref())
        .or(row_counts.as_ref())
        .map(|a| a.len())
        .unwrap_or(0);

    MultiColumnPruningStatistics::new(num_containers).with_column(
        ColumnPruningStatistics::new(column)
            .with_range(min_values, max_values)
            .with_nulls(null_counts, row_counts),
    )
}
