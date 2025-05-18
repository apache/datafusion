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

use arrow::array::{ArrayRef, BooleanArray};
use std::collections::HashSet;

use crate::Column;
use crate::ScalarValue;

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
