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
use datafusion_common::stats::Precision;
use datafusion_common::{
    DataFusionError, Result, ScalarValue, plan_datafusion_err, plan_err,
};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_plan::{ColumnStatistics, Statistics};

use futures::{Stream, StreamExt};

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
                    } else {
                        let partition_value = &pv[i - s.column_statistics.len()];
                        Ok((partition_value.clone(), partition_value.clone()))
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
        sort.sort_unstable_by(|(_, a), (_, b)| a.cmp(b));
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
        .map(|expr| expr.expr.as_any().downcast_ref::<Column>())
        .collect()
}

fn seed_summary_statistics(summary_statistics: &mut Statistics, file_stats: &Statistics) {
    summary_statistics.num_rows = file_stats.num_rows;
    summary_statistics.total_byte_size = file_stats.total_byte_size;

    for (summary_col_stats, file_col_stats) in summary_statistics
        .column_statistics
        .iter_mut()
        .zip(file_stats.column_statistics.iter())
    {
        summary_col_stats.null_count = file_col_stats.null_count;
        summary_col_stats.max_value = file_col_stats.max_value.clone();
        summary_col_stats.min_value = file_col_stats.min_value.clone();
        summary_col_stats.sum_value = file_col_stats.sum_value.cast_to_sum_type();
        summary_col_stats.byte_size = file_col_stats.byte_size;
    }
}

fn merge_summary_statistics(
    summary_statistics: &mut Statistics,
    file_stats: &Statistics,
) {
    summary_statistics.num_rows = summary_statistics.num_rows.add(&file_stats.num_rows);
    summary_statistics.total_byte_size = summary_statistics
        .total_byte_size
        .add(&file_stats.total_byte_size);

    for (summary_col_stats, file_col_stats) in summary_statistics
        .column_statistics
        .iter_mut()
        .zip(file_stats.column_statistics.iter())
    {
        let ColumnStatistics {
            null_count: file_nc,
            max_value: file_max,
            min_value: file_min,
            sum_value: file_sum,
            distinct_count: _,
            byte_size: file_sbs,
        } = file_col_stats;

        summary_col_stats.null_count = summary_col_stats.null_count.add(file_nc);
        summary_col_stats.max_value = summary_col_stats.max_value.max(file_max);
        summary_col_stats.min_value = summary_col_stats.min_value.min(file_min);
        summary_col_stats.sum_value = summary_col_stats.sum_value.add_for_sum(file_sum);
        summary_col_stats.byte_size = summary_col_stats.byte_size.add(file_sbs);
    }
}

fn seed_first_file_statistics(
    limit_num_rows: &mut Precision<usize>,
    summary_statistics: &mut Statistics,
    file_stats: &Statistics,
    collect_stats: bool,
) {
    *limit_num_rows = file_stats.num_rows;

    if collect_stats {
        seed_summary_statistics(summary_statistics, file_stats);
    }
}

fn merge_file_statistics(
    limit_num_rows: &mut Precision<usize>,
    summary_statistics: &mut Statistics,
    file_stats: &Statistics,
    collect_stats: bool,
) {
    *limit_num_rows = limit_num_rows.add(&file_stats.num_rows);

    if collect_stats {
        merge_summary_statistics(summary_statistics, file_stats);
    }
}

/// Get all files as well as the file level summary statistics (no statistic for partition columns).
/// If the optional `limit` is provided, includes only sufficient files. Needed to read up to
/// `limit` number of rows. `collect_stats` is passed down from the configuration parameter on
/// `ListingTable`. If it is false we only construct bare statistics and skip a potentially expensive
///  call to `multiunzip` for constructing file level summary statistics.
#[deprecated(
    since = "47.0.0",
    note = "Please use `get_files_with_limit` and  `compute_all_files_statistics` instead"
)]
#[cfg_attr(not(test), expect(unused))]
pub async fn get_statistics_with_limit(
    all_files: impl Stream<Item = Result<(PartitionedFile, Arc<Statistics>)>>,
    file_schema: SchemaRef,
    limit: Option<usize>,
    collect_stats: bool,
) -> Result<(FileGroup, Statistics)> {
    let mut result_files = FileGroup::default();
    // These statistics can be calculated as long as at least one file provides
    // useful information. If none of the files provides any information, then
    // they will end up having `Precision::Absent` values. Throughout calculations,
    // missing values will be imputed as:
    // - zero for summations, and
    // - neutral element for extreme points.
    let size = file_schema.fields().len();
    let mut summary_statistics = Statistics {
        num_rows: Precision::Absent,
        total_byte_size: Precision::Absent,
        column_statistics: vec![ColumnStatistics::default(); size],
    };
    // Keep limit pruning separate from the returned summary so `collect_stats=false`
    // can still stop early using known file row counts.
    let mut limit_num_rows = Precision::<usize>::Absent;

    // Fusing the stream allows us to call next safely even once it is finished.
    let mut all_files = Box::pin(all_files.fuse());

    if let Some(first_file) = all_files.next().await {
        let (mut file, file_stats) = first_file?;
        file.statistics = Some(Arc::clone(&file_stats));
        result_files.push(file);

        seed_first_file_statistics(
            &mut limit_num_rows,
            &mut summary_statistics,
            &file_stats,
            collect_stats,
        );

        // If the number of rows exceeds the limit, we can stop processing
        // files. This only applies when we know the number of rows. It also
        // currently ignores tables that have no statistics regarding the
        // number of rows.
        let conservative_num_rows = match limit_num_rows {
            Precision::Exact(nr) => nr,
            _ => usize::MIN,
        };
        if conservative_num_rows <= limit.unwrap_or(usize::MAX) {
            while let Some(current) = all_files.next().await {
                let (mut file, file_stats) = current?;
                file.statistics = Some(Arc::clone(&file_stats));
                result_files.push(file);
                merge_file_statistics(
                    &mut limit_num_rows,
                    &mut summary_statistics,
                    &file_stats,
                    collect_stats,
                );

                // If the number of rows exceeds the limit, we can stop processing
                // files. This only applies when we know the number of rows. It also
                // currently ignores tables that have no statistics regarding the
                // number of rows.
                if limit_num_rows.get_value().unwrap_or(&usize::MIN)
                    > &limit.unwrap_or(usize::MAX)
                {
                    break;
                }
            }
        }
    };

    let mut statistics = summary_statistics;
    if all_files.next().await.is_some() {
        // If we still have files in the stream, it means that the limit kicked
        // in, and the statistic could have been different had we processed the
        // files in a different order.
        statistics = statistics.to_inexact()
    }

    Ok((result_files, statistics))
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
    let statistics = Statistics::try_merge_iter(file_group_stats, &file_schema)?;

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

    let mut statistics =
        Statistics::try_merge_iter(file_groups_statistics, &table_schema)?;

    if inexact_stats {
        statistics = statistics.to_inexact()
    }

    Ok((file_groups_with_stats, statistics))
}

#[deprecated(since = "47.0.0", note = "Use Statistics::add")]
pub fn add_row_stats(
    file_num_rows: Precision<usize>,
    num_rows: Precision<usize>,
) -> Precision<usize> {
    file_num_rows.add(&num_rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PartitionedFile;
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::stream;

    fn file_stats(sum: u32) -> Statistics {
        Statistics {
            num_rows: Precision::Exact(1),
            total_byte_size: Precision::Exact(4),
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Exact(0),
                max_value: Precision::Exact(ScalarValue::UInt32(Some(sum))),
                min_value: Precision::Exact(ScalarValue::UInt32(Some(sum))),
                sum_value: Precision::Exact(ScalarValue::UInt32(Some(sum))),
                distinct_count: Precision::Exact(1),
                byte_size: Precision::Exact(4),
            }],
        }
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]))
    }

    fn make_file_stats(
        num_rows: usize,
        total_byte_size: usize,
        col_stats: ColumnStatistics,
    ) -> Arc<Statistics> {
        Arc::new(Statistics {
            num_rows: Precision::Exact(num_rows),
            total_byte_size: Precision::Exact(total_byte_size),
            column_statistics: vec![col_stats],
        })
    }

    fn rich_col_stats(
        null_count: usize,
        min: i64,
        max: i64,
        sum: i64,
        byte_size: usize,
    ) -> ColumnStatistics {
        ColumnStatistics {
            null_count: Precision::Exact(null_count),
            max_value: Precision::Exact(ScalarValue::Int64(Some(max))),
            min_value: Precision::Exact(ScalarValue::Int64(Some(min))),
            distinct_count: Precision::Absent,
            sum_value: Precision::Exact(ScalarValue::Int64(Some(sum))),
            byte_size: Precision::Exact(byte_size),
        }
    }

    #[tokio::test]
    #[expect(deprecated)]
    async fn test_get_statistics_with_limit_casts_first_file_sum_to_sum_type()
    -> Result<()> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("c1", DataType::UInt32, true)]));

        let files = stream::iter(vec![Ok((
            PartitionedFile::new("f1.parquet", 1),
            Arc::new(file_stats(100)),
        ))]);

        let (_group, stats) =
            get_statistics_with_limit(files, schema, None, true).await?;

        assert_eq!(
            stats.column_statistics[0].sum_value,
            Precision::Exact(ScalarValue::UInt64(Some(100)))
        );

        Ok(())
    }

    #[tokio::test]
    #[expect(deprecated)]
    async fn test_get_statistics_with_limit_merges_sum_with_unsigned_widening()
    -> Result<()> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("c1", DataType::UInt32, true)]));

        let files = stream::iter(vec![
            Ok((
                PartitionedFile::new("f1.parquet", 1),
                Arc::new(file_stats(100)),
            )),
            Ok((
                PartitionedFile::new("f2.parquet", 1),
                Arc::new(file_stats(200)),
            )),
        ]);

        let (_group, stats) =
            get_statistics_with_limit(files, schema, None, true).await?;

        assert_eq!(
            stats.column_statistics[0].sum_value,
            Precision::Exact(ScalarValue::UInt64(Some(300)))
        );

        Ok(())
    }

    #[tokio::test]
    #[expect(deprecated)]
    async fn get_statistics_with_limit_collect_stats_false_returns_bare_statistics() {
        let all_files = stream::iter(vec![
            Ok((
                PartitionedFile::new("first.parquet", 10),
                make_file_stats(0, 0, rich_col_stats(1, 1, 9, 15, 64)),
            )),
            Ok((
                PartitionedFile::new("second.parquet", 20),
                make_file_stats(10, 100, rich_col_stats(2, 10, 99, 300, 128)),
            )),
        ]);

        let (_files, statistics) =
            get_statistics_with_limit(all_files, test_schema(), None, false)
                .await
                .unwrap();

        assert_eq!(statistics.num_rows, Precision::Absent);
        assert_eq!(statistics.total_byte_size, Precision::Absent);
        assert_eq!(statistics.column_statistics.len(), 1);
        assert_eq!(
            statistics.column_statistics[0].null_count,
            Precision::Absent
        );
        assert_eq!(statistics.column_statistics[0].max_value, Precision::Absent);
        assert_eq!(statistics.column_statistics[0].min_value, Precision::Absent);
        assert_eq!(statistics.column_statistics[0].sum_value, Precision::Absent);
        assert_eq!(statistics.column_statistics[0].byte_size, Precision::Absent);
    }

    #[tokio::test]
    #[expect(deprecated)]
    async fn get_statistics_with_limit_collect_stats_false_uses_row_counts_for_limit() {
        let all_files = stream::iter(vec![
            Ok((
                PartitionedFile::new("first.parquet", 10),
                make_file_stats(3, 30, rich_col_stats(1, 1, 9, 15, 64)),
            )),
            Ok((
                PartitionedFile::new("second.parquet", 20),
                make_file_stats(3, 30, rich_col_stats(2, 10, 99, 300, 128)),
            )),
            Ok((
                PartitionedFile::new("third.parquet", 30),
                make_file_stats(3, 30, rich_col_stats(0, 100, 199, 450, 256)),
            )),
        ]);

        let (files, statistics) =
            get_statistics_with_limit(all_files, test_schema(), Some(4), false)
                .await
                .unwrap();

        assert_eq!(files.len(), 2);
        assert_eq!(statistics.num_rows, Precision::Absent);
        assert_eq!(statistics.total_byte_size, Precision::Absent);
    }

    #[tokio::test]
    #[expect(deprecated)]
    async fn get_statistics_with_limit_collect_stats_true_aggregates_statistics() {
        let all_files = stream::iter(vec![
            Ok((
                PartitionedFile::new("first.parquet", 10),
                make_file_stats(5, 50, rich_col_stats(1, 1, 9, 15, 64)),
            )),
            Ok((
                PartitionedFile::new("second.parquet", 20),
                make_file_stats(10, 100, rich_col_stats(2, 10, 99, 300, 128)),
            )),
        ]);

        let (_files, statistics) =
            get_statistics_with_limit(all_files, test_schema(), None, true)
                .await
                .unwrap();

        assert_eq!(statistics.num_rows, Precision::Exact(15));
        assert_eq!(statistics.total_byte_size, Precision::Exact(150));
        assert_eq!(
            statistics.column_statistics[0].null_count,
            Precision::Exact(3)
        );
        assert_eq!(
            statistics.column_statistics[0].min_value,
            Precision::Exact(ScalarValue::Int64(Some(1)))
        );
        assert_eq!(
            statistics.column_statistics[0].max_value,
            Precision::Exact(ScalarValue::Int64(Some(99)))
        );
        assert_eq!(
            statistics.column_statistics[0].sum_value,
            Precision::Exact(ScalarValue::Int64(Some(315)))
        );
        assert_eq!(
            statistics.column_statistics[0].byte_size,
            Precision::Exact(192)
        );
    }

    #[tokio::test]
    #[expect(deprecated)]
    async fn get_statistics_with_limit_collect_stats_true_limit_marks_inexact() {
        let all_files = stream::iter(vec![
            Ok((
                PartitionedFile::new("first.parquet", 10),
                make_file_stats(5, 50, rich_col_stats(0, 1, 5, 15, 64)),
            )),
            Ok((
                PartitionedFile::new("second.parquet", 20),
                make_file_stats(5, 50, rich_col_stats(1, 6, 10, 40, 64)),
            )),
            Ok((
                PartitionedFile::new("third.parquet", 20),
                make_file_stats(5, 50, rich_col_stats(2, 11, 15, 65, 64)),
            )),
        ]);

        let (files, statistics) =
            get_statistics_with_limit(all_files, test_schema(), Some(8), true)
                .await
                .unwrap();

        assert_eq!(files.len(), 2);
        assert_eq!(statistics.num_rows, Precision::Inexact(10));
        assert_eq!(statistics.total_byte_size, Precision::Inexact(100));
        assert_eq!(
            statistics.column_statistics[0].min_value,
            Precision::Inexact(ScalarValue::Int64(Some(1)))
        );
        assert_eq!(
            statistics.column_statistics[0].max_value,
            Precision::Inexact(ScalarValue::Int64(Some(10)))
        );
        assert_eq!(
            statistics.column_statistics[0].sum_value,
            Precision::Inexact(ScalarValue::Int64(Some(55)))
        );
        assert_eq!(
            statistics.column_statistics[0].byte_size,
            Precision::Inexact(128)
        );
    }
}
