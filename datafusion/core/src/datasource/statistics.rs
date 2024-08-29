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

use datafusion_common::stats::Precision;
use datafusion_common::ScalarValue;

use crate::arrow::datatypes::SchemaRef;
use crate::error::Result;
use crate::physical_plan::{ColumnStatistics, Statistics};

#[cfg(feature = "parquet")]
use crate::{
    arrow::datatypes::Schema,
    functions_aggregate::min_max::{MaxAccumulator, MinAccumulator},
    physical_plan::Accumulator,
};

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
) -> Result<(Vec<PartitionedFile>, Statistics)> {
    let mut result_files = vec![];
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
                        distinct_count: _,
                    } = file_col_stats;

                    col_stats.null_count = add_row_stats(*file_nc, col_stats.null_count);
                    set_max_if_greater(file_max, &mut col_stats.max_value);
                    set_min_if_lesser(file_min, &mut col_stats.min_value)
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

// only adding this cfg b/c this is the only feature it's used with currently
#[cfg(feature = "parquet")]
pub(crate) fn create_max_min_accs(
    schema: &Schema,
) -> (Vec<Option<MaxAccumulator>>, Vec<Option<MinAccumulator>>) {
    let max_values: Vec<Option<MaxAccumulator>> = schema
        .fields()
        .iter()
        .map(|field| {
            MaxAccumulator::try_new(min_max_aggregate_data_type(field.data_type())).ok()
        })
        .collect();
    let min_values: Vec<Option<MinAccumulator>> = schema
        .fields()
        .iter()
        .map(|field| {
            MinAccumulator::try_new(min_max_aggregate_data_type(field.data_type())).ok()
        })
        .collect();
    (max_values, min_values)
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

// only adding this cfg b/c this is the only feature it's used with currently
#[cfg(feature = "parquet")]
pub(crate) fn get_col_stats(
    schema: &Schema,
    null_counts: Vec<Precision<usize>>,
    max_values: &mut [Option<MaxAccumulator>],
    min_values: &mut [Option<MinAccumulator>],
) -> Vec<ColumnStatistics> {
    (0..schema.fields().len())
        .map(|i| {
            let max_value = match max_values.get_mut(i).unwrap() {
                Some(max_value) => max_value.evaluate().ok(),
                None => None,
            };
            let min_value = match min_values.get_mut(i).unwrap() {
                Some(min_value) => min_value.evaluate().ok(),
                None => None,
            };
            ColumnStatistics {
                null_count: null_counts[i],
                max_value: max_value.map(Precision::Exact).unwrap_or(Precision::Absent),
                min_value: min_value.map(Precision::Exact).unwrap_or(Precision::Absent),
                distinct_count: Precision::Absent,
            }
        })
        .collect()
}

// Min/max aggregation can take Dictionary encode input but always produces unpacked
// (aka non Dictionary) output. We need to adjust the output data type to reflect this.
// The reason min/max aggregate produces unpacked output because there is only one
// min/max value per group; there is no needs to keep them Dictionary encode
//
// only adding this cfg b/c this is the only feature it's used with currently
#[cfg(feature = "parquet")]
fn min_max_aggregate_data_type(
    input_type: &arrow_schema::DataType,
) -> &arrow_schema::DataType {
    if let arrow_schema::DataType::Dictionary(_, value_type) = input_type {
        value_type.as_ref()
    } else {
        input_type
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
