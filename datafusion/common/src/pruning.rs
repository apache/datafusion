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

use crate::stats::Precision;
use arrow::array::UInt64Array;
use arrow::datatypes::FieldRef;
use arrow::{
    array::{ArrayRef, BooleanArray},
    datatypes::{Schema, SchemaRef},
};

use crate::Column;
use crate::{ScalarValue, Statistics};

/// A source of runtime statistical information to [`PruningPredicate`]s.
///
/// # Supported Information
///
/// 1. Minimum and maximum values for columns
///
/// 2. Null counts and row counts for columns
///
/// 3. Whether the values in a column are contained in a set of literals
///
/// # Vectorized Interface
///
/// Information for containers / files are returned as Arrow [`ArrayRef`], so
/// the evaluation happens once on a single `RecordBatch`, which amortizes the
/// overhead of evaluating the predicate. This is important when pruning 1000s
/// of containers which often happens in analytic systems that have 1000s of
/// potential files to consider.
///
/// For example, for the following three files with a single column `a`:
/// ```text
/// file1: column a: min=5, max=10
/// file2: column a: No stats
/// file2: column a: min=20, max=30
/// ```
///
/// PruningStatistics would return:
///
/// ```text
/// min_values("a") -> Some([5, Null, 20])
/// max_values("a") -> Some([10, Null, 30])
/// min_values("X") -> None
/// ```
///
/// [`PruningPredicate`]: https://docs.rs/datafusion/latest/datafusion/physical_optimizer/pruning/struct.PruningPredicate.html
pub trait PruningStatistics {
    /// Return the minimum values for the named column, if known.
    ///
    /// If the minimum value for a particular container is not known, the
    /// returned array should have `null` in that row. If the minimum value is
    /// not known for any row, return `None`.
    ///
    /// Note: the returned array must contain [`Self::num_containers`] rows
    fn min_values(&self, column: &Column) -> Option<ArrayRef>;

    /// Return the maximum values for the named column, if known.
    ///
    /// See [`Self::min_values`] for when to return `None` and null values.
    ///
    /// Note: the returned array must contain [`Self::num_containers`] rows
    fn max_values(&self, column: &Column) -> Option<ArrayRef>;

    /// Return the number of containers (e.g. Row Groups) being pruned with
    /// these statistics.
    ///
    /// This value corresponds to the size of the [`ArrayRef`] returned by
    /// [`Self::min_values`], [`Self::max_values`], [`Self::null_counts`],
    /// and [`Self::row_counts`].
    fn num_containers(&self) -> usize;

    /// Return the number of null values for the named column as an
    /// [`UInt64Array`]
    ///
    /// See [`Self::min_values`] for when to return `None` and null values.
    ///
    /// Note: the returned array must contain [`Self::num_containers`] rows
    ///
    /// [`UInt64Array`]: arrow::array::UInt64Array
    fn null_counts(&self, column: &Column) -> Option<ArrayRef>;

    /// Return the number of rows for the named column in each container
    /// as an [`UInt64Array`].
    ///
    /// See [`Self::min_values`] for when to return `None` and null values.
    ///
    /// Note: the returned array must contain [`Self::num_containers`] rows
    ///
    /// [`UInt64Array`]: arrow::array::UInt64Array
    fn row_counts(&self, column: &Column) -> Option<ArrayRef>;

    /// Returns [`BooleanArray`] where each row represents information known
    /// about specific literal `values` in a column.
    ///
    /// For example, Parquet Bloom Filters implement this API to communicate
    /// that `values` are known not to be present in a Row Group.
    ///
    /// The returned array has one row for each container, with the following
    /// meanings:
    /// * `true` if the values in `column`  ONLY contain values from `values`
    /// * `false` if the values in `column` are NOT ANY of `values`
    /// * `null` if the neither of the above holds or is unknown.
    ///
    /// If these statistics can not determine column membership for any
    /// container, return `None` (the default).
    ///
    /// Note: the returned array must contain [`Self::num_containers`] rows
    fn contained(
        &self,
        column: &Column,
        values: &HashSet<ScalarValue>,
    ) -> Option<BooleanArray>;
}

/// Prune files based on their partition values.
/// This is used both at planning time and execution time to prune
/// files based on their partition values.
/// This feeds into [`CompositePruningStatistics`] to allow pruning
/// with filters that depend both on partition columns and data columns
/// (e.g. `WHERE partition_col = data_col`).
pub struct PartitionPruningStatistics {
    /// Values for each column for each container.
    /// The outer vectors represent the columns while the inner
    /// vectors represent the containers.
    /// The order must match the order of the partition columns in
    /// [`PartitionPruningStatistics::partition_schema`].
    partition_values: Vec<Vec<ScalarValue>>,
    /// The number of containers.
    /// Stored since the partition values are column-major and if
    /// there are no columns we wouldn't know the number of containers.
    num_containers: usize,
    /// The schema of the partition columns.
    /// This must **not** be the schema of the entire file or table:
    /// it must only be the schema of the partition columns,
    /// in the same order as the values in [`PartitionPruningStatistics::partition_values`].
    partition_schema: SchemaRef,
}

impl PartitionPruningStatistics {
    /// Create a new instance of [`PartitionPruningStatistics`].
    ///
    /// Args:
    /// * `partition_values`: A vector of vectors of [`ScalarValue`]s.
    ///   The outer vector represents the containers while the inner
    ///   vector represents the partition values for each column.
    ///   Note that this is the **opposite** of the order of the
    ///   partition columns in [`PartitionPruningStatistics::partition_schema`].
    /// * `partition_schema`: The schema of the partition columns.
    ///   This must **not** be the schema of the entire file or table:
    ///   instead it must only be the schema of the partition columns,
    ///   in the same order as the values in `partition_values`.
    pub fn new(
        partition_values: Vec<Vec<ScalarValue>>,
        partition_fields: Vec<FieldRef>,
    ) -> Self {
        let num_containers = partition_values.len();
        let partition_schema = Arc::new(Schema::new(partition_fields));
        let mut partition_valeus_by_column =
            vec![vec![]; partition_schema.fields().len()];
        for partition_value in partition_values.iter() {
            for (i, value) in partition_value.iter().enumerate() {
                partition_valeus_by_column[i].push(value.clone());
            }
        }
        Self {
            partition_values: partition_valeus_by_column,
            num_containers,
            partition_schema,
        }
    }
}

impl PruningStatistics for PartitionPruningStatistics {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let index = self.partition_schema.index_of(column.name()).ok()?;
        let partition_values = self.partition_values.get(index)?;
        let mut values = Vec::with_capacity(self.partition_values.len());
        for partition_value in partition_values {
            match partition_value {
                ScalarValue::Null => values.push(ScalarValue::Null),
                _ => values.push(partition_value.clone()),
            }
        }
        match ScalarValue::iter_to_array(values) {
            Ok(array) => Some(array),
            Err(_) => {
                log::warn!(
                    "Failed to convert min values to array for column {}",
                    column.name()
                );
                None
            }
        }
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.min_values(column)
    }

    fn num_containers(&self) -> usize {
        self.num_containers
    }

    fn null_counts(&self, _column: &Column) -> Option<ArrayRef> {
        None
    }

    fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        None
    }

    fn contained(
        &self,
        column: &Column,
        values: &HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        let index = self.partition_schema.index_of(column.name()).ok()?;
        let partition_values = self.partition_values.get(index)?;
        let mut contained = Vec::with_capacity(self.partition_values.len());
        for partition_value in partition_values {
            let contained_value = if values.contains(partition_value) {
                Some(true)
            } else {
                Some(false)
            };
            contained.push(contained_value);
        }
        let array = BooleanArray::from(contained);
        Some(array)
    }
}

/// Prune a set of containers represented by their statistics.
/// Each [`Statistics`] represents a container (e.g. a file or a partition of files).
pub struct PrunableStatistics {
    /// Statistics for each container.
    statistics: Vec<Arc<Statistics>>,
    /// The schema of the file these statistics are for.
    schema: SchemaRef,
}

impl PrunableStatistics {
    /// Create a new instance of [`PrunableStatistics`].
    /// Each [`Statistics`] represents a container (e.g. a file or a partition of files).
    /// The `schema` is the schema of the data in the containers and should apply to all files.
    pub fn new(statistics: Vec<Arc<Statistics>>, schema: SchemaRef) -> Self {
        Self { statistics, schema }
    }
}

impl PruningStatistics for PrunableStatistics {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let index = self.schema.index_of(column.name()).ok()?;
        let mut values = Vec::with_capacity(self.statistics.len());
        for stats in &self.statistics {
            let stat = stats.column_statistics.get(index)?;
            match &stat.min_value {
                Precision::Exact(min) => {
                    values.push(min.clone());
                }
                _ => values.push(ScalarValue::Null),
            }
        }
        match ScalarValue::iter_to_array(values) {
            Ok(array) => Some(array),
            Err(_) => {
                log::warn!(
                    "Failed to convert min values to array for column {}",
                    column.name()
                );
                None
            }
        }
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let index = self.schema.index_of(column.name()).ok()?;
        let mut values = Vec::with_capacity(self.statistics.len());
        for stats in &self.statistics {
            let stat = stats.column_statistics.get(index)?;
            match &stat.max_value {
                Precision::Exact(max) => {
                    values.push(max.clone());
                }
                _ => values.push(ScalarValue::Null),
            }
        }
        match ScalarValue::iter_to_array(values) {
            Ok(array) => Some(array),
            Err(_) => {
                log::warn!(
                    "Failed to convert max values to array for column {}",
                    column.name()
                );
                None
            }
        }
    }

    fn num_containers(&self) -> usize {
        self.statistics.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let index = self.schema.index_of(column.name()).ok()?;
        let mut values = Vec::with_capacity(self.statistics.len());
        let mut has_null_count = false;
        for stats in &self.statistics {
            let stat = stats.column_statistics.get(index)?;
            match &stat.null_count {
                Precision::Exact(null_count) => match u64::try_from(*null_count) {
                    Ok(null_count) => {
                        has_null_count = true;
                        values.push(Some(null_count));
                    }
                    Err(_) => {
                        values.push(None);
                    }
                },
                _ => values.push(None),
            }
        }
        if has_null_count {
            Some(Arc::new(UInt64Array::from(values)))
        } else {
            None
        }
    }

    fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        let mut values = Vec::with_capacity(self.statistics.len());
        let mut has_row_count = false;
        for stats in &self.statistics {
            match &stats.num_rows {
                Precision::Exact(row_count) => match u64::try_from(*row_count) {
                    Ok(row_count) => {
                        has_row_count = true;
                        values.push(Some(row_count));
                    }
                    Err(_) => {
                        values.push(None);
                    }
                },
                _ => values.push(None),
            }
        }
        if has_row_count {
            Some(Arc::new(UInt64Array::from(values)))
        } else {
            None
        }
    }

    fn contained(
        &self,
        _column: &Column,
        _values: &HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        None
    }
}

/// Combine multiple [`PruningStatistics`] into a single
/// [`CompositePruningStatistics`].
/// This can be used to combine statistics from different sources,
/// for example partition values and file statistics.
/// This allows pruning with filters that depend on multiple sources of statistics,
/// such as `WHERE partition_col = data_col`.
pub struct CompositePruningStatistics {
    pub statistics: Vec<Box<dyn PruningStatistics>>,
}

impl CompositePruningStatistics {
    /// Create a new instance of [`CompositePruningStatistics`] from
    /// a vector of [`PruningStatistics`].
    pub fn new(statistics: Vec<Box<dyn PruningStatistics>>) -> Self {
        assert!(!statistics.is_empty());
        Self { statistics }
    }
}

impl PruningStatistics for CompositePruningStatistics {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        for stats in &self.statistics {
            if let Some(array) = stats.min_values(column) {
                return Some(array);
            }
        }
        None
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        for stats in &self.statistics {
            if let Some(array) = stats.max_values(column) {
                return Some(array);
            }
        }
        None
    }

    fn num_containers(&self) -> usize {
        self.statistics[0].num_containers()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        for stats in &self.statistics {
            if let Some(array) = stats.null_counts(column) {
                return Some(array);
            }
        }
        None
    }

    fn row_counts(&self, column: &Column) -> Option<ArrayRef> {
        for stats in &self.statistics {
            if let Some(array) = stats.row_counts(column) {
                return Some(array);
            }
        }
        None
    }

    fn contained(
        &self,
        column: &Column,
        values: &HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        for stats in &self.statistics {
            if let Some(array) = stats.contained(column, values) {
                return Some(array);
            }
        }
        None
    }
}
