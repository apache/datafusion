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

use futures::{Stream, StreamExt};
use std::sync::Arc;

use crate::file_groups::FileGroup;
use crate::PartitionedFile;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::{
    compute::SortColumn,
    row::{Row, Rows},
};
use datafusion_common::stats::Precision;
use datafusion_common::{plan_datafusion_err, plan_err, DataFusionError, Result};
use datafusion_physical_expr::{expressions::Column, PhysicalSortExpr};
use datafusion_physical_expr_common::sort_expr::LexOrdering;
pub(crate) use datafusion_physical_plan::statistics::{
    add_row_stats, compute_summary_statistics, set_max_if_greater, set_min_if_lesser,
};
use datafusion_physical_plan::{ColumnStatistics, Statistics};

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
    #[allow(unused)]
    pub fn sort_order(&self) -> &LexOrdering {
        &self.sort_order
    }

    /// Min value at index
    #[allow(unused)]
    pub fn min(&self, idx: usize) -> Row {
        self.min_by_sort_order.row(idx)
    }

    /// Max value at index
    pub fn max(&self, idx: usize) -> Row {
        self.max_by_sort_order.row(idx)
    }

    pub fn new_from_files<'a>(
        projected_sort_order: &LexOrdering, // Sort order with respect to projected schema
        projected_schema: &SchemaRef,       // Projected schema
        projection: Option<&[usize]>, // Indices of projection in full table schema (None = all columns)
        files: impl IntoIterator<Item = &'a PartitionedFile>,
    ) -> Result<Self> {
        use datafusion_common::ScalarValue;

        let statistics_and_partition_values = files
            .into_iter()
            .map(|file| {
                file.statistics
                    .as_ref()
                    .zip(Some(file.partition_values.as_slice()))
            })
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                DataFusionError::Plan("Parquet file missing statistics".to_string())
            })?;

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
                            .ok_or_else(|| {
                                DataFusionError::Plan("statistics not found".to_string())
                            })
                    } else {
                        let partition_value = &pv[i - s.column_statistics.len()];
                        Ok((partition_value.clone(), partition_value.clone()))
                    }
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .unzip())
        };

        let sort_columns = sort_columns_from_physical_sort_exprs(projected_sort_order)
            .ok_or(DataFusionError::Plan(
                "sort expression must be on column".to_string(),
            ))?;

        // Project the schema & sort order down to just the relevant columns
        let min_max_schema = Arc::new(
            projected_schema
                .project(&(sort_columns.iter().map(|c| c.index()).collect::<Vec<_>>()))?,
        );
        let min_max_sort_order = LexOrdering::from(
            sort_columns
                .iter()
                .zip(projected_sort_order.iter())
                .enumerate()
                .map(|(i, (col, sort))| PhysicalSortExpr {
                    expr: Arc::new(Column::new(col.name(), i)),
                    options: sort.options,
                })
                .collect::<Vec<_>>(),
        );

        let (min_values, max_values): (Vec<_>, Vec<_>) = sort_columns
            .iter()
            .map(|c| {
                // Reverse the projection to get the index of the column in the full statistics
                // The file statistics contains _every_ column , but the sort column's index()
                // refers to the index in projected_schema
                let i = projection.map(|p| p[c.index()]).unwrap_or(c.index());

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

        Self::new(
            &min_max_sort_order,
            &min_max_schema,
            RecordBatch::try_new(Arc::clone(&min_max_schema), min_values).map_err(
                |e| {
                    DataFusionError::ArrowError(e, Some("\ncreate min batch".to_string()))
                },
            )?,
            RecordBatch::try_new(Arc::clone(&min_max_schema), max_values).map_err(
                |e| {
                    DataFusionError::ArrowError(e, Some("\ncreate max batch".to_string()))
                },
            )?,
        )
    }

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

        let sort_columns = sort_columns_from_physical_sort_exprs(sort_order).ok_or(
            DataFusionError::Plan("sort expression must be on column".to_string()),
        )?;

        // swap min/max if they're reversed in the ordering
        let (new_min_cols, new_max_cols): (Vec<_>, Vec<_>) = sort_order
            .iter()
            .zip(sort_columns.iter().copied())
            .map(|(sort_expr, column)| {
                if sort_expr.options.descending {
                    max_values
                        .column_by_name(column.name())
                        .zip(min_values.column_by_name(column.name()))
                } else {
                    min_values
                        .column_by_name(column.name())
                        .zip(max_values.column_by_name(column.name()))
                }
                .ok_or_else(|| {
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
                    let field = schema.field(idx);

                    // check that sort columns are non-nullable
                    if field.is_nullable() {
                        return plan_err!("cannot sort by nullable column");
                    }

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
                    DataFusionError::ArrowError(e, Some("convert columns".to_string()))
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
    pub fn is_sorted(&self) -> bool {
        self.max_by_sort_order
            .iter()
            .zip(self.min_by_sort_order.iter().skip(1))
            .all(|(max, next_min)| max < next_min)
    }
}

fn sort_columns_from_physical_sort_exprs(
    sort_order: &LexOrdering,
) -> Option<Vec<&Column>> {
    sort_order
        .iter()
        .map(|expr| expr.expr.as_any().downcast_ref::<Column>())
        .collect::<Option<Vec<_>>>()
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
#[allow(unused)]
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
    let mut col_stats_set = vec![ColumnStatistics::default(); size];
    let mut num_rows = Precision::<usize>::Absent;
    let mut total_byte_size = Precision::<usize>::Absent;

    // Fusing the stream allows us to call next safely even once it is finished.
    let mut all_files = Box::pin(all_files.fuse());

    if let Some(first_file) = all_files.next().await {
        let (mut file, file_stats) = first_file?;
        file.statistics = Some(Arc::clone(&file_stats));
        result_files.push(file);

        // First file, we set them directly from the file statistics.
        num_rows = file_stats.num_rows;
        total_byte_size = file_stats.total_byte_size;
        for (index, file_column) in
            file_stats.column_statistics.clone().into_iter().enumerate()
        {
            col_stats_set[index].null_count = file_column.null_count;
            col_stats_set[index].max_value = file_column.max_value;
            col_stats_set[index].min_value = file_column.min_value;
            col_stats_set[index].sum_value = file_column.sum_value;
        }

        // If the number of rows exceeds the limit, we can stop processing
        // files. This only applies when we know the number of rows. It also
        // currently ignores tables that have no statistics regarding the
        // number of rows.
        let conservative_num_rows = match num_rows {
            Precision::Exact(nr) => nr,
            _ => usize::MIN,
        };
        if conservative_num_rows <= limit.unwrap_or(usize::MAX) {
            while let Some(current) = all_files.next().await {
                let (mut file, file_stats) = current?;
                file.statistics = Some(Arc::clone(&file_stats));
                result_files.push(file);
                if !collect_stats {
                    continue;
                }

                // We accumulate the number of rows, total byte size and null
                // counts across all the files in question. If any file does not
                // provide any information or provides an inexact value, we demote
                // the statistic precision to inexact.
                num_rows = add_row_stats(file_stats.num_rows, num_rows);

                total_byte_size =
                    add_row_stats(file_stats.total_byte_size, total_byte_size);

                for (file_col_stats, col_stats) in file_stats
                    .column_statistics
                    .iter()
                    .zip(col_stats_set.iter_mut())
                {
                    let ColumnStatistics {
                        null_count: file_nc,
                        max_value: file_max,
                        min_value: file_min,
                        sum_value: file_sum,
                        distinct_count: _,
                    } = file_col_stats;

                    col_stats.null_count = add_row_stats(*file_nc, col_stats.null_count);
                    set_max_if_greater(file_max, &mut col_stats.max_value);
                    set_min_if_lesser(file_min, &mut col_stats.min_value);
                    col_stats.sum_value = file_sum.add(&col_stats.sum_value);
                }

                // If the number of rows exceeds the limit, we can stop processing
                // files. This only applies when we know the number of rows. It also
                // currently ignores tables that have no statistics regarding the
                // number of rows.
                if num_rows.get_value().unwrap_or(&usize::MIN)
                    > &limit.unwrap_or(usize::MAX)
                {
                    break;
                }
            }
        }
    };

    let mut statistics = Statistics {
        num_rows,
        total_byte_size,
        column_statistics: col_stats_set,
    };
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
pub fn compute_file_group_statistics(
    file_group: FileGroup,
    file_schema: SchemaRef,
    collect_stats: bool,
) -> Result<FileGroup> {
    if !collect_stats {
        return Ok(file_group);
    }

    let statistics = compute_summary_statistics(
        file_group.iter(),
        file_schema.fields().len(),
        |file| file.statistics.as_ref().map(|stats| stats.as_ref()),
    );

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
/// * `file_schema` - Schema of the files
/// * `collect_stats` - Whether to collect statistics
/// * `inexact_stats` - Whether to mark the resulting statistics as inexact
///
/// # Returns
/// A tuple containing:
/// * The processed file groups with their individual statistics attached
/// * The summary statistics across all file groups, aka all files summary statistics
pub fn compute_all_files_statistics(
    file_groups: Vec<FileGroup>,
    file_schema: SchemaRef,
    collect_stats: bool,
    inexact_stats: bool,
) -> Result<(Vec<FileGroup>, Statistics)> {
    let mut file_groups_with_stats = Vec::with_capacity(file_groups.len());

    // First compute statistics for each file group
    for file_group in file_groups {
        file_groups_with_stats.push(compute_file_group_statistics(
            file_group,
            Arc::clone(&file_schema),
            collect_stats,
        )?);
    }

    // Then summary statistics across all file groups
    let mut statistics = compute_summary_statistics(
        &file_groups_with_stats,
        file_schema.fields().len(),
        |file_group| file_group.statistics_ref(),
    );

    if inexact_stats {
        statistics = statistics.to_inexact()
    }

    Ok((file_groups_with_stats, statistics))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::ScalarValue;
    use std::sync::Arc;

    #[test]
    fn test_compute_summary_statistics_basic() {
        // Create a schema with two columns
        let schema = Arc::new(Schema::new(vec![
            Field::new("col1", DataType::Int32, false),
            Field::new("col2", DataType::Int32, false),
        ]));

        // Create items with statistics
        let stats1 = Statistics {
            num_rows: Precision::Exact(10),
            total_byte_size: Precision::Exact(100),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(1),
                    max_value: Precision::Exact(ScalarValue::Int32(Some(100))),
                    min_value: Precision::Exact(ScalarValue::Int32(Some(1))),
                    sum_value: Precision::Exact(ScalarValue::Int32(Some(500))),
                    distinct_count: Precision::Absent,
                },
                ColumnStatistics {
                    null_count: Precision::Exact(2),
                    max_value: Precision::Exact(ScalarValue::Int32(Some(200))),
                    min_value: Precision::Exact(ScalarValue::Int32(Some(10))),
                    sum_value: Precision::Exact(ScalarValue::Int32(Some(1000))),
                    distinct_count: Precision::Absent,
                },
            ],
        };

        let stats2 = Statistics {
            num_rows: Precision::Exact(15),
            total_byte_size: Precision::Exact(150),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(2),
                    max_value: Precision::Exact(ScalarValue::Int32(Some(120))),
                    min_value: Precision::Exact(ScalarValue::Int32(Some(-10))),
                    sum_value: Precision::Exact(ScalarValue::Int32(Some(600))),
                    distinct_count: Precision::Absent,
                },
                ColumnStatistics {
                    null_count: Precision::Exact(3),
                    max_value: Precision::Exact(ScalarValue::Int32(Some(180))),
                    min_value: Precision::Exact(ScalarValue::Int32(Some(5))),
                    sum_value: Precision::Exact(ScalarValue::Int32(Some(1200))),
                    distinct_count: Precision::Absent,
                },
            ],
        };

        let items = vec![Arc::new(stats1), Arc::new(stats2)];

        // Call compute_summary_statistics
        let summary_stats =
            compute_summary_statistics(items, schema.fields().len(), |item| {
                Some(item.as_ref())
            });

        // Verify the results
        assert_eq!(summary_stats.num_rows, Precision::Exact(25)); // 10 + 15
        assert_eq!(summary_stats.total_byte_size, Precision::Exact(250)); // 100 + 150

        // Verify column statistics
        let col1_stats = &summary_stats.column_statistics[0];
        assert_eq!(col1_stats.null_count, Precision::Exact(3)); // 1 + 2
        assert_eq!(
            col1_stats.max_value,
            Precision::Exact(ScalarValue::Int32(Some(120)))
        );
        assert_eq!(
            col1_stats.min_value,
            Precision::Exact(ScalarValue::Int32(Some(-10)))
        );
        assert_eq!(
            col1_stats.sum_value,
            Precision::Exact(ScalarValue::Int32(Some(1100)))
        ); // 500 + 600

        let col2_stats = &summary_stats.column_statistics[1];
        assert_eq!(col2_stats.null_count, Precision::Exact(5)); // 2 + 3
        assert_eq!(
            col2_stats.max_value,
            Precision::Exact(ScalarValue::Int32(Some(200)))
        );
        assert_eq!(
            col2_stats.min_value,
            Precision::Exact(ScalarValue::Int32(Some(5)))
        );
        assert_eq!(
            col2_stats.sum_value,
            Precision::Exact(ScalarValue::Int32(Some(2200)))
        ); // 1000 + 1200
    }

    #[test]
    fn test_compute_summary_statistics_mixed_precision() {
        // Create a schema with one column
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));

        // Create items with different precision levels
        let stats1 = Statistics {
            num_rows: Precision::Exact(10),
            total_byte_size: Precision::Inexact(100),
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Exact(1),
                max_value: Precision::Exact(ScalarValue::Int32(Some(100))),
                min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                sum_value: Precision::Exact(ScalarValue::Int32(Some(500))),
                distinct_count: Precision::Absent,
            }],
        };

        let stats2 = Statistics {
            num_rows: Precision::Inexact(15),
            total_byte_size: Precision::Exact(150),
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Inexact(2),
                max_value: Precision::Inexact(ScalarValue::Int32(Some(120))),
                min_value: Precision::Exact(ScalarValue::Int32(Some(-10))),
                sum_value: Precision::Absent,
                distinct_count: Precision::Absent,
            }],
        };

        let items = vec![Arc::new(stats1), Arc::new(stats2)];

        let summary_stats =
            compute_summary_statistics(items, schema.fields().len(), |item| {
                Some(item.as_ref())
            });

        assert_eq!(summary_stats.num_rows, Precision::Inexact(25));
        assert_eq!(summary_stats.total_byte_size, Precision::Inexact(250));

        let col_stats = &summary_stats.column_statistics[0];
        assert_eq!(col_stats.null_count, Precision::Inexact(3));
        assert_eq!(
            col_stats.max_value,
            Precision::Inexact(ScalarValue::Int32(Some(120)))
        );
        assert_eq!(
            col_stats.min_value,
            Precision::Inexact(ScalarValue::Int32(Some(-10)))
        );
        assert!(matches!(col_stats.sum_value, Precision::Absent));
    }

    #[test]
    fn test_compute_summary_statistics_empty() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));

        // Empty collection
        let items: Vec<Arc<Statistics>> = vec![];

        let summary_stats =
            compute_summary_statistics(items, schema.fields().len(), |item| {
                Some(item.as_ref())
            });

        // Verify default values for empty collection
        assert_eq!(summary_stats.num_rows, Precision::Absent);
        assert_eq!(summary_stats.total_byte_size, Precision::Absent);
        assert_eq!(summary_stats.column_statistics.len(), 1);
        assert_eq!(
            summary_stats.column_statistics[0].null_count,
            Precision::Absent
        );
    }
}
