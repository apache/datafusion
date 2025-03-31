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

use std::mem;
use std::sync::Arc;

use futures::{Stream, StreamExt};

use crate::arrow::datatypes::SchemaRef;
use crate::error::Result;
use crate::physical_plan::{ColumnStatistics, Statistics};
use datafusion_common::stats::Precision;
use datafusion_common::ScalarValue;
use datafusion_datasource::file_groups::FileGroup;

use super::listing::PartitionedFile;

/// Get all files as well as the file level summary statistics (no statistic for partition columns).
/// If the optional `limit` is provided, includes only sufficient files. Needed to read up to
/// `limit` number of rows. `collect_stats` is passed down from the configuration parameter on
/// `ListingTable`. If it is false we only construct bare statistics and skip a potentially expensive
///  call to `multiunzip` for constructing file level summary statistics.
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
        file.statistics = Some(file_stats.as_ref().clone());
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
                file.statistics = Some(file_stats.as_ref().clone());
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

fn add_row_stats(
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
