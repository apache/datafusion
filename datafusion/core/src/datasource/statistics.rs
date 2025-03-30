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

use crate::arrow::datatypes::SchemaRef;
use crate::error::Result;
use crate::physical_plan::{ColumnStatistics, Statistics};
use datafusion_common::stats::Precision;
use datafusion_common::ScalarValue;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::PartitionedFile;
use futures::{Stream, StreamExt};
use std::mem;
use std::sync::Arc;

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

/// Generic function to compute statistics across multiple items that have statistics
fn compute_summary_statistics<T, I>(
    items: I,
    file_schema: &SchemaRef,
    stats_extractor: impl Fn(&T) -> Option<&Statistics>,
) -> Statistics
where
    I: IntoIterator<Item = T>,
{
    let size = file_schema.fields().len();
    let mut col_stats_set = vec![ColumnStatistics::default(); size];
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

    let statistics =
        compute_summary_statistics(file_group.iter(), &file_schema, |file| {
            file.statistics.as_ref().map(|stats| stats.as_ref())
        });

    Ok(file_group.with_statistics(statistics))
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
    let mut statistics =
        compute_summary_statistics(&file_groups_with_stats, &file_schema, |file_group| {
            file_group.statistics()
        });

    if inexact_stats {
        statistics = statistics.to_inexact()
    }

    Ok((file_groups_with_stats, statistics))
}

pub(crate) fn add_row_stats(
    file_num_rows: Precision<usize>,
    num_rows: Precision<usize>,
) -> Precision<usize> {
    match (file_num_rows, &num_rows) {
        (Precision::Absent, _) => num_rows.to_inexact(),
        (lhs, Precision::Absent) => lhs.to_inexact(),
        (lhs, rhs) => lhs.add(rhs),
    }
}

/// If the given value is numerically greater than the original maximum value,
/// return the new maximum value with appropriate exactness information.
fn set_max_if_greater(
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
fn set_min_if_lesser(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::datatypes::{DataType, Field, Schema};
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
            compute_summary_statistics(items, &schema, |item| Some(item.as_ref()));

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
            compute_summary_statistics(items, &schema, |item| Some(item.as_ref()));

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
            compute_summary_statistics(items, &schema, |item| Some(item.as_ref()));

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
