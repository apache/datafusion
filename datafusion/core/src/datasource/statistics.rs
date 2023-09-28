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

use crate::arrow::datatypes::{Schema, SchemaRef};
use crate::error::Result;
use crate::physical_plan::expressions::{MaxAccumulator, MinAccumulator};
use crate::physical_plan::{Accumulator, ColumnStatistics, Statistics};
use datafusion_common::stats::Sharpness;
use futures::Stream;
use futures::StreamExt;

use super::listing::PartitionedFile;

/// Get all files as well as the file level summary statistics (no statistic for partition columns).
/// If the optional `limit` is provided, includes only sufficient files.
/// Needed to read up to `limit` number of rows.
pub async fn get_statistics_with_limit(
    all_files: impl Stream<Item = Result<(PartitionedFile, Statistics)>>,
    file_schema: SchemaRef,
    limit: Option<usize>,
) -> Result<(Vec<PartitionedFile>, Statistics)> {
    let mut result_files = vec![];

    let mut null_counts = vec![Sharpness::Exact(0 as usize); file_schema.fields().len()];
    let mut has_statistics = false;
    let (mut max_values, mut min_values) = create_max_min_accs(&file_schema);

    // The number of rows and the total byte size can be calculated as long as
    // at least one file has them. If none of the files provide them, then they
    // will be omitted from the statistics. The missing values will be counted
    // as zero.
    let mut num_rows = Sharpness::Absent;
    let mut total_byte_size = Sharpness::Absent;

    // fusing the stream allows us to call next safely even once it is finished
    let mut all_files = Box::pin(all_files.fuse());
    while let Some(res) = all_files.next().await {
        let (file, file_stats) = res?;
        result_files.push(file);
        num_rows = if let Some(exactness) = num_rows.is_exact() {
            if exactness {
                if file_stats.num_rows == Sharpness::Absent {
                    Sharpness::Exact(0 as usize)
                        .add(&Sharpness::Exact(num_rows.get_value().unwrap()))
                } else {
                    file_stats
                        .num_rows
                        .add(&Sharpness::Exact(num_rows.get_value().unwrap()))
                }
            } else {
                if file_stats.num_rows == Sharpness::Absent {
                    Sharpness::Exact(0 as usize)
                        .add(&Sharpness::Inexact(num_rows.get_value().unwrap()))
                } else {
                    file_stats
                        .num_rows
                        .add(&Sharpness::Inexact(num_rows.get_value().unwrap()))
                }
            }
        } else {
            file_stats.num_rows
        };
        total_byte_size = if let Some(exactness) = total_byte_size.is_exact() {
            if exactness {
                if file_stats.total_byte_size == Sharpness::Absent {
                    Sharpness::Exact(0 as usize)
                        .add(&Sharpness::Exact(total_byte_size.get_value().unwrap()))
                } else {
                    file_stats
                        .total_byte_size
                        .add(&Sharpness::Exact(total_byte_size.get_value().unwrap()))
                }
            } else {
                if file_stats.total_byte_size == Sharpness::Absent {
                    Sharpness::Exact(0 as usize)
                        .add(&Sharpness::Inexact(total_byte_size.get_value().unwrap()))
                } else {
                    file_stats
                        .total_byte_size
                        .add(&Sharpness::Inexact(total_byte_size.get_value().unwrap()))
                }
            }
        } else {
            file_stats.total_byte_size
        };
        if !file_stats.column_statistics.is_empty() {
            has_statistics = true;
            for (i, cs) in file_stats.column_statistics.iter().enumerate() {
                null_counts[i] = if cs.null_count == Sharpness::Absent {
                    null_counts[i].clone()
                } else {
                    null_counts[i].add(&cs.null_count)
                };

                if let Some(max_value) = &mut max_values[i] {
                    if let Some(file_max) = cs.max_value.get_value() {
                        match max_value.update_batch(&[file_max.to_array()]) {
                            Ok(_) => {}
                            Err(_) => {
                                max_values[i] = None;
                            }
                        }
                    } else {
                        max_values[i] = None;
                    }
                }

                if let Some(min_value) = &mut min_values[i] {
                    if let Some(file_min) = cs.min_value.get_value() {
                        match min_value.update_batch(&[file_min.to_array()]) {
                            Ok(_) => {}
                            Err(_) => {
                                min_values[i] = None;
                            }
                        }
                    } else {
                        min_values[i] = None;
                    }
                }
            }
        }
        // If the number of rows exceeds the limit, we can stop processing
        // files. This only applies when we know the number of rows. It also
        // currently ignores tables that have no statistics regarding the
        // number of rows.
        if num_rows.get_value().unwrap_or(usize::MIN) > limit.unwrap_or(usize::MAX) {
            break;
        }
    }

    let column_stats = if has_statistics {
        get_col_stats(&file_schema, null_counts, &mut max_values, &mut min_values)
    } else {
        Statistics::new_with_unbounded_columns(&file_schema).column_statistics
    };

    let statistics = if all_files.next().await.is_some() {
        // if we still have files in the stream, it means that the limit kicked
        // in and that the statistic could have been different if we processed
        // the files in a different order.
        Statistics {
            num_rows,
            total_byte_size,
            column_statistics: column_stats,
        }
        .make_inexact()
    } else {
        Statistics {
            num_rows,
            total_byte_size,
            column_statistics: column_stats,
        }
    };

    Ok((result_files, statistics))
}

pub(crate) fn create_max_min_accs(
    schema: &Schema,
) -> (Vec<Option<MaxAccumulator>>, Vec<Option<MinAccumulator>>) {
    let max_values: Vec<Option<MaxAccumulator>> = schema
        .fields()
        .iter()
        .map(|field| MaxAccumulator::try_new(field.data_type()).ok())
        .collect::<Vec<_>>();
    let min_values: Vec<Option<MinAccumulator>> = schema
        .fields()
        .iter()
        .map(|field| MinAccumulator::try_new(field.data_type()).ok())
        .collect::<Vec<_>>();
    (max_values, min_values)
}

pub(crate) fn get_col_stats(
    schema: &Schema,
    null_counts: Vec<Sharpness<usize>>,
    max_values: &mut [Option<MaxAccumulator>],
    min_values: &mut [Option<MinAccumulator>],
) -> Vec<ColumnStatistics> {
    (0..schema.fields().len())
        .map(|i| {
            let max_value = match &max_values[i] {
                Some(max_value) => max_value.evaluate().ok(),
                None => None,
            };
            let min_value = match &min_values[i] {
                Some(min_value) => min_value.evaluate().ok(),
                None => None,
            };
            ColumnStatistics {
                null_count: null_counts[i].clone(),
                max_value: max_value
                    .map(|val| Sharpness::Exact(val))
                    .unwrap_or(Sharpness::Absent),
                min_value: min_value
                    .map(|val| Sharpness::Exact(val))
                    .unwrap_or(Sharpness::Absent),
                distinct_count: Sharpness::Absent,
            }
        })
        .collect()
}
