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
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray};
use datafusion_common::pruning::PruningStatistics;
use datafusion_common::{Column, ScalarValue};

pub struct DummyStats;

impl PruningStatistics for DummyStats {
    fn min_values(&self, _: &Column) -> Option<ArrayRef> {
        None
    }
    fn max_values(&self, _: &Column) -> Option<ArrayRef> {
        None
    }
    fn num_containers(&self) -> usize {
        0
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

/// Simple `PruningStatistics` implementation backed by literal arrays.
/// Useful for testing expression pruning semantics without plumbing
/// actual file/row group metadata.
pub struct MockPruningStatistics {
    column: String,
    min_values: ArrayRef,
    max_values: ArrayRef,
    null_counts: ArrayRef,
    row_counts: Option<ArrayRef>,
    num_containers: usize,
    value_sets: Option<Vec<Option<HashSet<ScalarValue>>>>,
}

impl MockPruningStatistics {
    pub fn new(
        column: impl Into<String>,
        min_values: ArrayRef,
        max_values: ArrayRef,
        null_counts: ArrayRef,
        row_counts: Option<ArrayRef>,
    ) -> Self {
        Self::new_with_sets(
            column,
            min_values,
            max_values,
            null_counts,
            row_counts,
            None,
        )
    }

    pub fn new_with_sets(
        column: impl Into<String>,
        min_values: ArrayRef,
        max_values: ArrayRef,
        null_counts: ArrayRef,
        row_counts: Option<ArrayRef>,
        value_sets: Option<Vec<Option<HashSet<ScalarValue>>>>,
    ) -> Self {
        let num_containers = min_values.len();
        if let Some(value_sets) = value_sets.as_ref() {
            assert_eq!(
                value_sets.len(),
                num_containers,
                "value sets must match container count"
            );
        }
        Self {
            column: column.into(),
            min_values,
            max_values,
            null_counts,
            row_counts,
            num_containers,
            value_sets,
        }
    }

    /// Convenience constructor for uniform literal stats.
    pub fn from_scalar(
        column: impl Into<String>,
        value: ScalarValue,
        num_containers: usize,
        rows_per_container: u64,
    ) -> Self {
        let mins = ScalarValue::iter_to_array(
            std::iter::repeat(value.clone()).take(num_containers),
        )
        .expect("scalar to array");
        let maxs = ScalarValue::iter_to_array(
            std::iter::repeat(value.clone()).take(num_containers),
        )
        .expect("scalar to array");
        let null_counts = ScalarValue::iter_to_array(
            std::iter::repeat(ScalarValue::UInt64(Some(0))).take(num_containers),
        )
        .expect("zeros");
        let row_counts = ScalarValue::iter_to_array(
            std::iter::repeat(ScalarValue::UInt64(Some(rows_per_container)))
                .take(num_containers),
        )
        .expect("rows");

        Self::new(column, mins, maxs, null_counts, Some(row_counts))
    }
}

impl PruningStatistics for MockPruningStatistics {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        (column.name == self.column).then(|| Arc::clone(&self.min_values))
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        (column.name == self.column).then(|| Arc::clone(&self.max_values))
    }

    fn num_containers(&self) -> usize {
        self.num_containers
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        (column.name == self.column).then(|| Arc::clone(&self.null_counts))
    }

    fn row_counts(&self, column: &Column) -> Option<ArrayRef> {
        (column.name == self.column)
            .then(|| self.row_counts.as_ref().map(Arc::clone))
            .flatten()
    }

    fn value_sets(&self, column: &Column) -> Option<Vec<Option<HashSet<ScalarValue>>>> {
        if column.name == self.column {
            self.value_sets.clone()
        } else {
            None
        }
    }

    fn contained(&self, _: &Column, _: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        None
    }
}
