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

use arrow::array::UInt64Array;
use arrow::array::{ArrayRef, BooleanArray};
use arrow::datatypes::{FieldRef, Schema, SchemaRef};
use std::collections::HashSet;
use std::sync::Arc;

use crate::stats::Precision;
use crate::{Column, Statistics};
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
    ///   partition columns in `PartitionPruningStatistics::partition_schema`.
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
        for partition_value in partition_values {
            for (i, value) in partition_value.into_iter().enumerate() {
                partition_valeus_by_column[i].push(value);
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
        match ScalarValue::iter_to_array(partition_values.iter().cloned()) {
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
    /// These are taken as a reference since they may be rather large / expensive to clone
    /// and we often won't return all of them as ArrayRefs (we only return the columns the predicate requests).
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
        if self.statistics.iter().any(|s| {
            s.column_statistics
                .get(index)
                .is_some_and(|stat| stat.min_value.is_exact().unwrap_or(false))
        }) {
            match ScalarValue::iter_to_array(self.statistics.iter().map(|s| {
                s.column_statistics
                    .get(index)
                    .and_then(|stat| {
                        if let Precision::Exact(min) = &stat.min_value {
                            Some(min.clone())
                        } else {
                            None
                        }
                    })
                    .unwrap_or(ScalarValue::Null)
            })) {
                Ok(array) => Some(array),
                Err(_) => {
                    log::warn!(
                        "Failed to convert min values to array for column {}",
                        column.name()
                    );
                    None
                }
            }
        } else {
            None
        }
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let index = self.schema.index_of(column.name()).ok()?;
        if self.statistics.iter().any(|s| {
            s.column_statistics
                .get(index)
                .is_some_and(|stat| stat.max_value.is_exact().unwrap_or(false))
        }) {
            match ScalarValue::iter_to_array(self.statistics.iter().map(|s| {
                s.column_statistics
                    .get(index)
                    .and_then(|stat| {
                        if let Precision::Exact(max) = &stat.max_value {
                            Some(max.clone())
                        } else {
                            None
                        }
                    })
                    .unwrap_or(ScalarValue::Null)
            })) {
                Ok(array) => Some(array),
                Err(_) => {
                    log::warn!(
                        "Failed to convert max values to array for column {}",
                        column.name()
                    );
                    None
                }
            }
        } else {
            None
        }
    }

    fn num_containers(&self) -> usize {
        self.statistics.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let index = self.schema.index_of(column.name()).ok()?;
        if self.statistics.iter().any(|s| {
            s.column_statistics
                .get(index)
                .is_some_and(|stat| stat.null_count.is_exact().unwrap_or(false))
        }) {
            Some(Arc::new(
                self.statistics
                    .iter()
                    .map(|s| {
                        s.column_statistics.get(index).and_then(|stat| {
                            if let Precision::Exact(null_count) = &stat.null_count {
                                u64::try_from(*null_count).ok()
                            } else {
                                None
                            }
                        })
                    })
                    .collect::<UInt64Array>(),
            ))
        } else {
            None
        }
    }

    fn row_counts(&self, column: &Column) -> Option<ArrayRef> {
        // If the column does not exist in the schema, return None
        if self.schema.index_of(column.name()).is_err() {
            return None;
        }
        if self
            .statistics
            .iter()
            .any(|s| s.num_rows.is_exact().unwrap_or(false))
        {
            Some(Arc::new(
                self.statistics
                    .iter()
                    .map(|s| {
                        if let Precision::Exact(row_count) = &s.num_rows {
                            u64::try_from(*row_count).ok()
                        } else {
                            None
                        }
                    })
                    .collect::<UInt64Array>(),
            ))
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
/// This is done by iterating over the statistics and returning the first
/// one that has information for the requested column.
/// If multiple statistics have information for the same column,
/// the first one is returned without any regard for completeness or accuracy.
/// That is: if the first statistics has information for a column, even if it is incomplete,
/// that is returned even if a later statistics has more complete information.
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


#[cfg(test)]
mod tests {
    use crate::{cast::{as_int32_array, as_uint64_array}, ColumnStatistics};

    use super::*;
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    #[test]
    fn test_partition_pruning_statistics() {
        let partition_values = vec![
            vec![ScalarValue::Int32(Some(1)), ScalarValue::Int32(Some(2))],
            vec![ScalarValue::Int32(Some(3)), ScalarValue::Int32(Some(4))],
        ];
        let partition_fields = vec![
            Arc::new(Field::new("a", DataType::Int32, false)),
            Arc::new(Field::new("b", DataType::Int32, false)),
        ];
        let partition_stats = PartitionPruningStatistics::new(partition_values, partition_fields);

        let column_a = Column::new_unqualified("a");
        let column_b = Column::new_unqualified("b");

        // Partition values don't know anything about nulls or row counts
        assert!(partition_stats.null_counts(&column_a).is_none());
        assert!(partition_stats.row_counts(&column_a).is_none());
        assert!(partition_stats.null_counts(&column_b).is_none());
        assert!(partition_stats.row_counts(&column_b).is_none());

        // Min/max values are the same as the partition values
        let min_values_a = as_int32_array(&partition_stats.min_values(&column_a).unwrap()).unwrap().into_iter().collect::<Vec<_>>();
        let expected_values_a = vec![Some(1), Some(3)];
        assert_eq!(min_values_a, expected_values_a);
        let max_values_a = as_int32_array(&partition_stats.max_values(&column_a).unwrap()).unwrap().into_iter().collect::<Vec<_>>();
        let expected_values_a = vec![Some(1), Some(3)];
        assert_eq!(max_values_a, expected_values_a);

        let min_values_b = as_int32_array(&partition_stats.min_values(&column_b).unwrap()).unwrap().into_iter().collect::<Vec<_>>();
        let expected_values_b = vec![Some(2), Some(4)];
        assert_eq!(min_values_b, expected_values_b);
        let max_values_b = as_int32_array(&partition_stats.max_values(&column_b).unwrap()).unwrap().into_iter().collect::<Vec<_>>();
        let expected_values_b = vec![Some(2), Some(4)];
        assert_eq!(max_values_b, expected_values_b);

        // Contained values are only true for the partition values
        let values = HashSet::from([ScalarValue::Int32(Some(1))]);
        let contained_a = partition_stats.contained(&column_a, &values).unwrap();
        let expected_contained_a = BooleanArray::from(vec![true, false]);
        assert_eq!(contained_a, expected_contained_a);
        let contained_b = partition_stats.contained(&column_b, &values).unwrap();
        let expected_contained_b = BooleanArray::from(vec![false, false]);
        assert_eq!(contained_b, expected_contained_b);

        // The number of containers is the length of the partition values
        assert_eq!(partition_stats.num_containers(), 2);
    }

    #[test]
    fn test_partition_pruning_statistics_empty() {
        let partition_values = vec![];
        let partition_fields = vec![
            Arc::new(Field::new("a", DataType::Int32, false)),
            Arc::new(Field::new("b", DataType::Int32, false)),
        ];
        let partition_stats = PartitionPruningStatistics::new(partition_values, partition_fields);

        let column_a = Column::new_unqualified("a");
        let column_b = Column::new_unqualified("b");

        // Partition values don't know anything about nulls or row counts
        assert!(partition_stats.null_counts(&column_a).is_none());
        assert!(partition_stats.row_counts(&column_a).is_none());
        assert!(partition_stats.null_counts(&column_b).is_none());
        assert!(partition_stats.row_counts(&column_b).is_none());

        // Min/max values are all missing
        assert!(partition_stats.min_values(&column_a).is_none());
        assert!(partition_stats.max_values(&column_a).is_none());
        assert!(partition_stats.min_values(&column_b).is_none());
        assert!(partition_stats.max_values(&column_b).is_none());

        // Contained values are all empty
        let values = HashSet::from([ScalarValue::Int32(Some(1))]);
        let contained_a = partition_stats.contained(&column_a, &values);
        let expected_contained_a = BooleanArray::from(Vec::<Option<bool>>::new());
        assert_eq!(contained_a, Some(expected_contained_a));
    }

    #[test]
    fn test_statistics_pruning_statistics() {
        let statistics = vec![
            Arc::new(
                Statistics::default()
                    .add_column_statistics(
                        ColumnStatistics::new_unknown()
                            .with_min_value(Precision::Exact(ScalarValue::Int32(Some(0))))
                            .with_max_value(Precision::Exact(ScalarValue::Int32(Some(100))))
                            .with_null_count(Precision::Exact(0))
                    )
                    .add_column_statistics(
                        ColumnStatistics::new_unknown()
                            .with_min_value(Precision::Exact(ScalarValue::Int32(Some(100))))
                            .with_max_value(Precision::Exact(ScalarValue::Int32(Some(200))))
                            .with_null_count(Precision::Exact(5))
                    )
                    .with_num_rows(Precision::Exact(100))
            ),
            Arc::new(
                Statistics::default()
                    .add_column_statistics(
                        ColumnStatistics::new_unknown()
                            .with_min_value(Precision::Exact(ScalarValue::Int32(Some(50))))
                            .with_max_value(Precision::Exact(ScalarValue::Int32(Some(300))))
                            .with_null_count(Precision::Exact(10))
                    )
                    .add_column_statistics(
                        ColumnStatistics::new_unknown()
                            .with_min_value(Precision::Exact(ScalarValue::Int32(Some(200))))
                            .with_max_value(Precision::Exact(ScalarValue::Int32(Some(400))))
                            .with_null_count(Precision::Exact(0))
                    )
                    .with_num_rows(Precision::Exact(200))
            ),
        ];

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));
        let pruning_stats = PrunableStatistics::new(statistics, schema);

        let column_a = Column::new_unqualified("a");
        let column_b = Column::new_unqualified("b");

        // Min/max values are the same as the statistics
        let min_values_a = as_int32_array(&pruning_stats.min_values(&column_a).unwrap()).unwrap().into_iter().collect::<Vec<_>>();
        let expected_values_a = vec![Some(0), Some(50)];
        assert_eq!(min_values_a, expected_values_a);
        let max_values_a = as_int32_array(&pruning_stats.max_values(&column_a).unwrap()).unwrap().into_iter().collect::<Vec<_>>();
        let expected_values_a = vec![Some(100), Some(300)];
        assert_eq!(max_values_a, expected_values_a);
        let min_values_b = as_int32_array(&pruning_stats.min_values(&column_b).unwrap()).unwrap().into_iter().collect::<Vec<_>>();
        let expected_values_b = vec![Some(100), Some(200)];
        assert_eq!(min_values_b, expected_values_b);
        let max_values_b = as_int32_array(&pruning_stats.max_values(&column_b).unwrap()).unwrap().into_iter().collect::<Vec<_>>();
        let expected_values_b = vec![Some(200), Some(400)];
        assert_eq!(max_values_b, expected_values_b);

        // Null counts are the same as the statistics
        let null_counts_a = as_uint64_array(&pruning_stats.null_counts(&column_a).unwrap()).unwrap().into_iter().collect::<Vec<_>>();
        let expected_null_counts_a = vec![Some(0), Some(10)];
        assert_eq!(null_counts_a, expected_null_counts_a);
        let null_counts_b = as_uint64_array(&pruning_stats.null_counts(&column_b).unwrap()).unwrap().into_iter().collect::<Vec<_>>();
        let expected_null_counts_b = vec![Some(5), Some(0)];
        assert_eq!(null_counts_b, expected_null_counts_b);
        
        // Row counts are the same as the statistics
        let row_counts_a = as_uint64_array(&pruning_stats.row_counts(&column_a).unwrap()).unwrap().into_iter().collect::<Vec<_>>();
        let expected_row_counts_a = vec![Some(100), Some(200)];
        assert_eq!(row_counts_a, expected_row_counts_a);
        let row_counts_b = as_uint64_array(&pruning_stats.row_counts(&column_b).unwrap()).unwrap().into_iter().collect::<Vec<_>>();
        let expected_row_counts_b = vec![Some(100), Some(200)];
        assert_eq!(row_counts_b, expected_row_counts_b);

        // Contained values are all null/missing (we can't know this just from statistics)
        let values = HashSet::from([ScalarValue::Int32(Some(0))]);
        assert!(pruning_stats.contained(&column_a, &values).is_none());
        assert!(pruning_stats.contained(&column_b, &values).is_none());

        // The number of containers is the length of the statistics
        assert_eq!(pruning_stats.num_containers(), 2);

        // Test with a column that has no statistics
        let column_c = Column::new_unqualified("c");
        assert!(pruning_stats.min_values(&column_c).is_none());
        assert!(pruning_stats.max_values(&column_c).is_none());
        assert!(pruning_stats.null_counts(&column_c).is_none());
        // Since row counts uses the first column that has row counts we get them back even
        // if this columns does not have them set.
        // This is debatable, personally I think `row_count` should not take a `Column` as an argument
        // at all since all columns should have the same number of rows.
        // But for now we just document the current behavior in this test.
        let row_counts_c = as_uint64_array(&pruning_stats.row_counts(&column_c).unwrap()).unwrap().into_iter().collect::<Vec<_>>();
        let expected_row_counts_c = vec![Some(100), Some(200)];
        assert_eq!(row_counts_c, expected_row_counts_c);
        assert!(pruning_stats.contained(&column_c, &values).is_none());

        // Test with a column that doesn't exist
        let column_d = Column::new_unqualified("d");
        assert!(pruning_stats.min_values(&column_d).is_none());
        assert!(pruning_stats.max_values(&column_d).is_none());
        assert!(pruning_stats.null_counts(&column_d).is_none());
        assert!(pruning_stats.row_counts(&column_d).is_none());
        assert!(pruning_stats.contained(&column_d, &values).is_none());
    }

    #[test]
    fn test_statistics_pruning_statistics_empty() {
        let statistics = vec![];
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));
        let pruning_stats = PrunableStatistics::new(statistics, schema);

        let column_a = Column::new_unqualified("a");
        let column_b = Column::new_unqualified("b");

        // Min/max values are all missing
        assert!(pruning_stats.min_values(&column_a).is_none());
        assert!(pruning_stats.max_values(&column_a).is_none());
        assert!(pruning_stats.min_values(&column_b).is_none());
        assert!(pruning_stats.max_values(&column_b).is_none());

        // Null counts are all missing
        assert!(pruning_stats.null_counts(&column_a).is_none());
        assert!(pruning_stats.null_counts(&column_b).is_none());

        // Row counts are all missing
        assert!(pruning_stats.row_counts(&column_a).is_none());
        assert!(pruning_stats.row_counts(&column_b).is_none());

        // Contained values are all empty
        let values = HashSet::from([ScalarValue::Int32(Some(1))]);
        assert!(pruning_stats.contained(&column_a, &values).is_none());
    }
}