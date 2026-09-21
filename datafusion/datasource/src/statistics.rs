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

//! Use statistics to optimize physical planning.
//!
//! Currently, this module houses code to sort file groups if they are non-overlapping with
//! respect to the required sort order. See [`MinMaxStatistics`]

use std::sync::Arc;

use crate::PartitionedFile;
use crate::file_groups::FileGroup;

use arrow::array::RecordBatch;
use arrow::compute::SortColumn;
use arrow::datatypes::SchemaRef;
use arrow::row::{Row, Rows};
use datafusion_common::stats::NdvFallback;
use datafusion_common::{
    DataFusionError, Result, ScalarValue, plan_datafusion_err, plan_err,
};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_plan::Statistics;

/// A normalized representation of file min/max statistics that allows for efficient sorting & comparison.
/// The min/max values are ordered by [`Self::sort_order`].
/// Furthermore, any columns that are reversed in the sort order have their min/max values swapped.
pub(crate) struct MinMaxStatistics {
    min_by_sort_order: Rows,
    max_by_sort_order: Rows,
    sort_order: LexOrdering,
}

impl MinMaxStatistics {
    /// Sort order used to sort the statistics
    #[expect(unused)]
    pub fn sort_order(&self) -> &LexOrdering {
        &self.sort_order
    }

    /// Min value at index
    #[expect(unused)]
    pub fn min(&'_ self, idx: usize) -> Row<'_> {
        self.min_by_sort_order.row(idx)
    }

    /// Max value at index
    pub fn max(&'_ self, idx: usize) -> Row<'_> {
        self.max_by_sort_order.row(idx)
    }

    pub fn new_from_files<'a>(
        projected_sort_order: &LexOrdering, // Sort order with respect to projected schema
        projected_schema: &SchemaRef,       // Projected schema
        projection: Option<&[usize]>, // Indices of projection in full table schema (None = all columns)
        files: impl IntoIterator<Item = &'a PartitionedFile>,
    ) -> Result<Self> {
        let Some(statistics_and_partition_values) = files
            .into_iter()
            .map(|file| {
                file.statistics
                    .as_ref()
                    .zip(Some(file.partition_values.as_slice()))
            })
            .collect::<Option<Vec<_>>>()
        else {
            return plan_err!("Parquet file missing statistics");
        };

        // Helper function to get min/max statistics for a given column of projected_schema
        let get_min_max = |i: usize| -> Result<(Vec<ScalarValue>, Vec<ScalarValue>)> {
            Ok(statistics_and_partition_values
                .iter()
                .map(|(s, pv)| {
                    if i < s.column_statistics.len() {
                        s.column_statistics[i]
                            .min_value
                            .get_value()
                            .cloned()
                            .zip(s.column_statistics[i].max_value.get_value().cloned())
                            .ok_or_else(|| plan_datafusion_err!("statistics not found"))
                    } else if let Some(partition_value) =
                        pv.get(i - s.column_statistics.len())
                    {
                        Ok((partition_value.clone(), partition_value.clone()))
                    } else {
                        Err(plan_datafusion_err!(
                            "statistics not found for partition, expected at most {}",
                            s.column_statistics.len()
                        ))
                    }
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .unzip())
        };

        let Some(sort_columns) =
            sort_columns_from_physical_sort_exprs(projected_sort_order)
        else {
            return plan_err!("sort expression must be on column");
        };

        // Project the schema & sort order down to just the relevant columns
        let min_max_schema = Arc::new(
            projected_schema
                .project(&(sort_columns.iter().map(|c| c.index()).collect::<Vec<_>>()))?,
        );

        let min_max_sort_order = projected_sort_order
            .iter()
            .zip(sort_columns.iter())
            .enumerate()
            .map(|(idx, (sort_expr, col))| {
                let expr = Arc::new(Column::new(col.name(), idx));
                PhysicalSortExpr::new(expr, sort_expr.options)
            });
        // Safe to `unwrap` as we know that sort columns are non-empty:
        let min_max_sort_order = LexOrdering::new(min_max_sort_order).unwrap();

        let (min_values, max_values): (Vec<_>, Vec<_>) = sort_columns
            .iter()
            .map(|c| {
                // Reverse the projection to get the index of the column in the full statistics
                // The file statistics contains _every_ column , but the sort column's index()
                // refers to the index in projected_schema
                let i = projection
                    .map(|p| p[c.index()])
                    .unwrap_or_else(|| c.index());

                let (min, max) = get_min_max(i).map_err(|e| {
                    e.context(format!("get min/max for column: '{}'", c.name()))
                })?;
                Ok((
                    ScalarValue::iter_to_array(min)?,
                    ScalarValue::iter_to_array(max)?,
                ))
            })
            .collect::<Result<Vec<_>>>()
            .map_err(|e| e.context("collect min/max values"))?
            .into_iter()
            .unzip();

        let min_batch = RecordBatch::try_new(Arc::clone(&min_max_schema), min_values)
            .map_err(|e| {
                DataFusionError::ArrowError(
                    Box::new(e),
                    Some("\ncreate min batch".to_string()),
                )
            })?;
        let max_batch = RecordBatch::try_new(Arc::clone(&min_max_schema), max_values)
            .map_err(|e| {
                DataFusionError::ArrowError(
                    Box::new(e),
                    Some("\ncreate max batch".to_string()),
                )
            })?;

        Self::new(&min_max_sort_order, &min_max_schema, min_batch, max_batch)
    }

    #[expect(clippy::needless_pass_by_value)]
    pub fn new(
        sort_order: &LexOrdering,
        schema: &SchemaRef,
        min_values: RecordBatch,
        max_values: RecordBatch,
    ) -> Result<Self> {
        use arrow::row::*;

        let sort_fields = sort_order
            .iter()
            .map(|expr| {
                expr.expr
                    .data_type(schema)
                    .map(|data_type| SortField::new_with_options(data_type, expr.options))
            })
            .collect::<Result<Vec<_>>>()
            .map_err(|e| e.context("create sort fields"))?;
        let converter = RowConverter::new(sort_fields)?;

        let Some(sort_columns) = sort_columns_from_physical_sort_exprs(sort_order) else {
            return plan_err!("sort expression must be on column");
        };

        // swap min/max if they're reversed in the ordering
        let (new_min_cols, new_max_cols): (Vec<_>, Vec<_>) = sort_order
            .iter()
            .zip(sort_columns.iter().copied())
            .map(|(sort_expr, column)| {
                let maxes = max_values.column_by_name(column.name());
                let mins = min_values.column_by_name(column.name());
                let opt_value = if sort_expr.options.descending {
                    maxes.zip(mins)
                } else {
                    mins.zip(maxes)
                };
                opt_value.ok_or_else(|| {
                    plan_datafusion_err!(
                        "missing column in MinMaxStatistics::new: '{}'",
                        column.name()
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .unzip();

        let [min, max] = [new_min_cols, new_max_cols].map(|cols| {
            let values = RecordBatch::try_new(
                min_values.schema(),
                cols.into_iter().cloned().collect(),
            )?;
            let sorting_columns = sort_order
                .iter()
                .zip(sort_columns.iter().copied())
                .map(|(sort_expr, column)| {
                    let schema = values.schema();
                    let idx = schema.index_of(column.name())?;

                    Ok(SortColumn {
                        values: Arc::clone(values.column(idx)),
                        options: Some(sort_expr.options),
                    })
                })
                .collect::<Result<Vec<_>>>()
                .map_err(|e| e.context("create sorting columns"))?;
            converter
                .convert_columns(
                    &sorting_columns
                        .into_iter()
                        .map(|c| c.values)
                        .collect::<Vec<_>>(),
                )
                .map_err(|e| {
                    DataFusionError::ArrowError(
                        Box::new(e),
                        Some("convert columns".to_string()),
                    )
                })
        });

        Ok(Self {
            min_by_sort_order: min.map_err(|e| e.context("build min rows"))?,
            max_by_sort_order: max.map_err(|e| e.context("build max rows"))?,
            sort_order: sort_order.clone(),
        })
    }

    /// Return a sorted list of the min statistics together with the original indices
    pub fn min_values_sorted(&self) -> Vec<(usize, Row<'_>)> {
        let mut sort: Vec<_> = self.min_by_sort_order.iter().enumerate().collect();
        sort.sort_unstable_by_key(|(_, row)| *row);
        sort
    }

    /// Check if the min/max statistics are in order and non-overlapping
    /// (or touching at boundaries)
    pub fn is_sorted(&self) -> bool {
        self.max_by_sort_order
            .iter()
            .zip(self.min_by_sort_order.iter().skip(1))
            .all(|(max, next_min)| max <= next_min)
    }
}

fn sort_columns_from_physical_sort_exprs(
    sort_order: &LexOrdering,
) -> Option<Vec<&Column>> {
    sort_order
        .iter()
        .map(|expr| expr.expr.downcast_ref::<Column>())
        .collect()
}

/// Computes the summary statistics for a group of files(`FileGroup` level's statistics).
///
/// This function combines statistics from all files in the file group to create
/// summary statistics. It handles the following aspects:
/// - Merges row counts and byte sizes across files
/// - Computes column-level statistics like min/max values
/// - Maintains appropriate precision information (exact, inexact, absent)
///
/// # Parameters
/// * `file_group` - The group of files to process
/// * `file_schema` - Schema of the files
/// * `collect_stats` - Whether to collect statistics (if false, returns original file group)
///
/// # Returns
/// A new file group with summary statistics attached
#[expect(clippy::needless_pass_by_value)]
pub fn compute_file_group_statistics(
    file_group: FileGroup,
    file_schema: SchemaRef,
    collect_stats: bool,
) -> Result<FileGroup> {
    if !collect_stats {
        return Ok(file_group);
    }

    let file_group_stats = file_group.iter().filter_map(|file| {
        let stats = file.statistics.as_ref()?;
        Some(stats.as_ref())
    });
    let statistics = Statistics::try_merge_iter_with_ndv_fallback(
        file_group_stats,
        &file_schema,
        NdvFallback::Max,
    )?;

    Ok(file_group.with_statistics(Arc::new(statistics)))
}

/// Computes statistics for all files across multiple file groups.
///
/// This function:
/// 1. Computes statistics for each individual file group
/// 2. Summary statistics across all file groups
/// 3. Optionally marks statistics as inexact
///
/// # Parameters
/// * `file_groups` - Vector of file groups to process
/// * `table_schema` - Schema of the table
/// * `collect_stats` - Whether to collect statistics
/// * `inexact_stats` - Whether to mark the resulting statistics as inexact
///
/// # Returns
/// A tuple containing:
/// * The processed file groups with their individual statistics attached
/// * The summary statistics across all file groups, aka all files summary statistics
#[expect(clippy::needless_pass_by_value)]
pub fn compute_all_files_statistics(
    file_groups: Vec<FileGroup>,
    table_schema: SchemaRef,
    collect_stats: bool,
    inexact_stats: bool,
) -> Result<(Vec<FileGroup>, Statistics)> {
    let file_groups_with_stats = file_groups
        .into_iter()
        .map(|file_group| {
            compute_file_group_statistics(
                file_group,
                Arc::clone(&table_schema),
                collect_stats,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    // Then summary statistics across all file groups
    let file_groups_statistics = file_groups_with_stats
        .iter()
        .filter_map(|file_group| file_group.file_statistics(None));

    let mut statistics = Statistics::try_merge_iter_with_ndv_fallback(
        file_groups_statistics,
        &table_schema,
        NdvFallback::Max,
    )?;

    if inexact_stats {
        statistics = statistics.to_inexact()
    }

    Ok((file_groups_with_stats, statistics))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PartitionedFile;
    use crate::file_groups::FileGroup;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::stats::Precision;
    use datafusion_physical_plan::ColumnStatistics;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]))
    }

    fn utf8_file_stats(ndv: usize, min: &str, max: &str) -> Statistics {
        Statistics {
            num_rows: Precision::Exact(1),
            total_byte_size: Precision::Exact(16),
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Exact(0),
                max_value: Precision::Exact(ScalarValue::Utf8(Some(max.to_string()))),
                min_value: Precision::Exact(ScalarValue::Utf8(Some(min.to_string()))),
                sum_value: Precision::Absent,
                distinct_count: Precision::Exact(ndv),
                byte_size: Precision::Exact(16),
            }],
        }
    }

    fn file_with_stats(path: &str, stats: Statistics) -> PartitionedFile {
        PartitionedFile::new(path, 1).with_statistics(Arc::new(stats))
    }

    #[test]
    fn test_compute_file_group_statistics_uses_max_ndv_fallback() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Utf8, true)]));
        let file_group = FileGroup::new(vec![
            file_with_stats("f1.parquet", utf8_file_stats(5, "a", "x")),
            file_with_stats("f2.parquet", utf8_file_stats(8, "b", "z")),
        ]);

        let file_group =
            compute_file_group_statistics(file_group, Arc::clone(&schema), true)?;
        let stats = file_group.file_statistics(None).unwrap();

        assert_eq!(
            stats.column_statistics[0].distinct_count,
            Precision::Inexact(8)
        );
        assert_eq!(
            stats.column_statistics[0].min_value,
            Precision::Exact(ScalarValue::Utf8(Some("a".to_string())))
        );
        assert_eq!(
            stats.column_statistics[0].max_value,
            Precision::Exact(ScalarValue::Utf8(Some("z".to_string())))
        );

        Ok(())
    }

    #[test]
    fn test_compute_all_files_statistics_uses_max_ndv_fallback() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Utf8, true)]));
        let file_groups = vec![
            FileGroup::new(vec![
                file_with_stats("f1.parquet", utf8_file_stats(5, "a", "x")),
                file_with_stats("f2.parquet", utf8_file_stats(8, "b", "z")),
            ]),
            FileGroup::new(vec![
                file_with_stats("f3.parquet", utf8_file_stats(3, "c", "w")),
                file_with_stats("f4.parquet", utf8_file_stats(6, "d", "y")),
            ]),
        ];

        let (file_groups, stats) =
            compute_all_files_statistics(file_groups, schema, true, false)?;

        assert_eq!(
            file_groups[0]
                .file_statistics(None)
                .unwrap()
                .column_statistics[0]
                .distinct_count,
            Precision::Inexact(8)
        );
        assert_eq!(
            file_groups[1]
                .file_statistics(None)
                .unwrap()
                .column_statistics[0]
                .distinct_count,
            Precision::Inexact(6)
        );
        assert_eq!(
            stats.column_statistics[0].distinct_count,
            Precision::Inexact(8)
        );

        Ok(())
    }

    #[test]
    fn min_max_statistics_missing_column_stats_returns_error() {
        let schema = test_schema();
        let sort_order =
            [PhysicalSortExpr::new_default(Arc::new(Column::new("a", 0)))].into();
        let files = [
            file_with_stats("f1.parquet", Statistics::default()),
            file_with_stats("f2.parquet", Statistics::default()),
        ];

        let Err(err) =
            MinMaxStatistics::new_from_files(&sort_order, &schema, None, files.iter())
        else {
            panic!("expected missing statistics error")
        };

        assert!(
            err.to_string()
                .contains("statistics not found for partition"),
            "unexpected error: {err:?}"
        );
    }
}
