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

//! Defines the cross join plan for loading the left side of the cross join
//! and producing batches in parallel for the right partitions

use datafusion_common::stats::Precision;
use datafusion_common::{ColumnStatistics, ScalarValue, Statistics};
use std::mem;

/// Represents statistics data grouped by partition.
///
/// This structure maintains a collection of statistics, one for each partition
/// of a distributed dataset, allowing access to statistics by partition index.
#[derive(Debug, Clone)]
pub struct PartitionedStatistics {
    inner: Vec<Statistics>,
}

impl PartitionedStatistics {
    /// Creates a new PartitionedStatistics instance from a vector of Statistics.
    pub fn new(statistics: Vec<Statistics>) -> Self {
        Self { inner: statistics }
    }

    /// Returns the number of partitions.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if there are no partitions.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns the statistics for the specified partition.
    ///
    /// # Panics
    ///
    /// Panics if `partition_idx` is out of bounds.
    pub fn statistics(&self, partition_idx: usize) -> &Statistics {
        &self.inner[partition_idx]
    }

    /// Returns the statistics for the specified partition, or None if the index is out of bounds.
    pub fn get_statistics(&self, partition_idx: usize) -> Option<&Statistics> {
        self.inner.get(partition_idx)
    }

    /// Returns an iterator over the statistics.
    pub fn iter(&self) -> impl Iterator<Item = &Statistics> {
        self.inner.iter()
    }
}

/// Generic function to compute statistics across multiple items that have statistics
pub fn compute_summary_statistics<T, I>(
    items: I,
    column_count: usize,
    stats_extractor: impl Fn(&T) -> Option<&Statistics>,
) -> Statistics
where
    I: IntoIterator<Item = T>,
{
    let mut col_stats_set = vec![ColumnStatistics::default(); column_count];
    let mut num_rows = Precision::<usize>::Absent;
    let mut total_byte_size = Precision::<usize>::Absent;

    for (idx, item) in items.into_iter().enumerate() {
        if let Some(item_stats) = stats_extractor(&item) {
            if idx == 0 {
                // First item, set values directly
                num_rows = item_stats.num_rows;
                total_byte_size = item_stats.total_byte_size;
                for (index, column_stats) in
                    item_stats.column_statistics.iter().enumerate()
                {
                    col_stats_set[index].null_count = column_stats.null_count;
                    col_stats_set[index].max_value = column_stats.max_value.clone();
                    col_stats_set[index].min_value = column_stats.min_value.clone();
                    col_stats_set[index].sum_value = column_stats.sum_value.clone();
                }
                continue;
            }

            // Accumulate statistics for subsequent items
            num_rows = add_row_stats(item_stats.num_rows, num_rows);
            total_byte_size = add_row_stats(item_stats.total_byte_size, total_byte_size);

            for (item_col_stats, col_stats) in item_stats
                .column_statistics
                .iter()
                .zip(col_stats_set.iter_mut())
            {
                col_stats.null_count =
                    add_row_stats(item_col_stats.null_count, col_stats.null_count);
                set_max_if_greater(&item_col_stats.max_value, &mut col_stats.max_value);
                set_min_if_lesser(&item_col_stats.min_value, &mut col_stats.min_value);
                col_stats.sum_value = item_col_stats.sum_value.add(&col_stats.sum_value);
            }
        }
    }

    Statistics {
        num_rows,
        total_byte_size,
        column_statistics: col_stats_set,
    }
}

/// If the given value is numerically greater than the original maximum value,
/// return the new maximum value with appropriate exactness information.
pub fn set_max_if_greater(
    max_nominee: &Precision<ScalarValue>,
    max_value: &mut Precision<ScalarValue>,
) {
    match (&max_value, max_nominee) {
        (Precision::Exact(val1), Precision::Exact(val2)) if val1 < val2 => {
            *max_value = max_nominee.clone();
        }
        (Precision::Exact(val1), Precision::Inexact(val2))
        | (Precision::Inexact(val1), Precision::Inexact(val2))
        | (Precision::Inexact(val1), Precision::Exact(val2))
            if val1 < val2 =>
        {
            *max_value = max_nominee.clone().to_inexact();
        }
        (Precision::Exact(_), Precision::Absent) => {
            let exact_max = mem::take(max_value);
            *max_value = exact_max.to_inexact();
        }
        (Precision::Absent, Precision::Exact(_)) => {
            *max_value = max_nominee.clone().to_inexact();
        }
        (Precision::Absent, Precision::Inexact(_)) => {
            *max_value = max_nominee.clone();
        }
        _ => {}
    }
}

/// If the given value is numerically lesser than the original minimum value,
/// return the new minimum value with appropriate exactness information.
pub fn set_min_if_lesser(
    min_nominee: &Precision<ScalarValue>,
    min_value: &mut Precision<ScalarValue>,
) {
    match (&min_value, min_nominee) {
        (Precision::Exact(val1), Precision::Exact(val2)) if val1 > val2 => {
            *min_value = min_nominee.clone();
        }
        (Precision::Exact(val1), Precision::Inexact(val2))
        | (Precision::Inexact(val1), Precision::Inexact(val2))
        | (Precision::Inexact(val1), Precision::Exact(val2))
            if val1 > val2 =>
        {
            *min_value = min_nominee.clone().to_inexact();
        }
        (Precision::Exact(_), Precision::Absent) => {
            let exact_min = mem::take(min_value);
            *min_value = exact_min.to_inexact();
        }
        (Precision::Absent, Precision::Exact(_)) => {
            *min_value = min_nominee.clone().to_inexact();
        }
        (Precision::Absent, Precision::Inexact(_)) => {
            *min_value = min_nominee.clone();
        }
        _ => {}
    }
}

pub fn add_row_stats(
    file_num_rows: Precision<usize>,
    num_rows: Precision<usize>,
) -> Precision<usize> {
    match (file_num_rows, &num_rows) {
        (Precision::Absent, _) => num_rows.to_inexact(),
        (lhs, Precision::Absent) => lhs.to_inexact(),
        (lhs, rhs) => lhs.add(rhs),
    }
}
