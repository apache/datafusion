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
use arrow_array::Scalar;
use datafusion_common::stats::Sharpness;
use datafusion_common::ScalarValue;
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
    let mut null_counts = vec![Sharpness::Exact(0_usize); file_schema.fields().len()];
    let (mut max_values, mut min_values): (
        Option<Vec<Sharpness<ScalarValue>>>,
        Option<Vec<Sharpness<ScalarValue>>>,
    ) = (None, None);

    // The number of rows and the total byte size can be calculated as long as
    // at least one file has them. If none of the files provide them, then they
    // will be omitted from the statistics. The missing values will be counted
    // as zero.
    let mut num_rows = Sharpness::Exact(0);
    let mut total_byte_size = Sharpness::Exact(0);

    // fusing the stream allows us to call next safely even once it is finished
    let mut all_files = Box::pin(all_files.fuse());
    while let Some(res) = all_files.next().await {
        let (file, file_stats) = res?;
        result_files.push(file);
        num_rows = match (file_stats.num_rows, num_rows.clone()) {
            (Sharpness::Absent, rhs) => rhs.to_inexact(),
            (lhs, Sharpness::Absent) => lhs.to_inexact(),
            (lhs, rhs) => lhs.add(&rhs),
        };
        total_byte_size = match (file_stats.total_byte_size, total_byte_size.clone()) {
            (Sharpness::Absent, rhs) => rhs.to_inexact(),
            (lhs, Sharpness::Absent) => lhs.to_inexact(),
            (lhs, rhs) => lhs.add(&rhs),
        };

        for (i, cs) in file_stats.column_statistics.iter().enumerate() {
            null_counts[i] = if cs.null_count == Sharpness::Absent {
                // Downcast to inexact
                null_counts[i].clone().to_inexact()
            } else {
                null_counts[i].add(&cs.null_count)
            };
        }

        if let Some(some_max_values) = &mut max_values {
            for (i, cs) in file_stats.column_statistics.iter().enumerate() {
                set_max_if_greater(&mut some_max_values[i], &cs.max_value);
            }
        } else {
            let mut new_col_stats_max = vec![];
            for cs in file_stats.column_statistics.iter() {
                new_col_stats_max.push(cs.max_value.clone());
            }
            for _ in 0..file_schema.fields().len() - file_stats.column_statistics.len() {
                new_col_stats_max.push(Sharpness::Absent)
            }
            max_values = Some(new_col_stats_max);
        };

        if let Some(some_min_values) = &mut min_values {
            for (i, cs) in file_stats.column_statistics.iter().enumerate() {
                set_min_if_lesser(&mut some_min_values[i], &cs.min_value);
            }
        } else {
            let mut new_col_stats_min = vec![];
            for cs in file_stats.column_statistics.iter() {
                new_col_stats_min.push(cs.min_value.clone());
            }
            for _ in 0..file_schema.fields().len() - file_stats.column_statistics.len() {
                new_col_stats_min.push(Sharpness::Absent)
            }
            min_values = Some(new_col_stats_min);
        };

        // If the number of rows exceeds the limit, we can stop processing
        // files. This only applies when we know the number of rows. It also
        // currently ignores tables that have no statistics regarding the
        // number of rows.
        if num_rows.get_value().unwrap_or(usize::MIN) > limit.unwrap_or(usize::MAX) {
            break;
        }
    }

    let column_stats =
        get_col_stats_vec(&file_schema, null_counts, max_values, min_values);

    let statistics = if all_files.next().await.is_some() {
        // if we still have files in the stream, it means that the limit kicked
        // in and the statistic could have been different if we have
        // processed the files in a different order.
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

/// It is the [`Sharpness::Exact`] version of `ColumnStatistics::new_with_unbounded_column` function.
pub(crate) fn create_max_min_vec(schema: &Schema) -> Vec<Sharpness<ScalarValue>> {
    schema
        .fields()
        .iter()
        .map(|field| {
            let dt = ScalarValue::try_from(field.data_type());
            if let Ok(dt) = dt {
                Sharpness::Exact(dt)
            } else {
                Sharpness::Absent
            }
        })
        .collect::<Vec<_>>()
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

pub(crate) fn get_col_stats_vec(
    schema: &Schema,
    null_counts: Vec<Sharpness<usize>>,
    max_values: Option<Vec<Sharpness<ScalarValue>>>,
    min_values: Option<Vec<Sharpness<ScalarValue>>>,
) -> Vec<ColumnStatistics> {
    (0..schema.fields().len())
        .map(|i| {
            let max_value = if let Some(some_max_values) = &max_values {
                some_max_values[i].clone()
            } else {
                match ScalarValue::try_from(schema.fields[i].data_type()) {
                    Ok(dt) => Sharpness::Inexact(dt),
                    Err(_) => Sharpness::Absent,
                }
            };
            let min_value = if let Some(some_min_values) = &min_values {
                some_min_values[i].clone()
            } else {
                match ScalarValue::try_from(schema.fields[i].data_type()) {
                    Ok(dt) => Sharpness::Inexact(dt),
                    Err(_) => Sharpness::Absent,
                }
            };
            ColumnStatistics {
                null_count: null_counts[i].clone(),
                max_value,
                min_value,
                distinct_count: Sharpness::Absent,
            }
        })
        .collect()
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
                max_value: max_value.map(Sharpness::Exact).unwrap_or(Sharpness::Absent),
                min_value: min_value.map(Sharpness::Exact).unwrap_or(Sharpness::Absent),
                distinct_count: Sharpness::Absent,
            }
        })
        .collect()
}

/// If the given value is numerically greater than the original value,
/// it set the new max value with the exactness information.
fn set_max_if_greater(
    max_values: &mut Sharpness<ScalarValue>,
    max_value: &Sharpness<ScalarValue>,
) {
    if let Sharpness::Exact(max) = &max_value {
        match max_values.clone() {
            Sharpness::Exact(val) => {
                if val < *max {
                    *max_values = Sharpness::Exact(max.clone())
                }
            }
            Sharpness::Inexact(val) => {
                *max_values = max_values.clone().to_inexact();
                if val < *max {
                    *max_values = Sharpness::Inexact(max.clone())
                }
            }
            Sharpness::Absent => *max_values = max_values.clone().to_inexact(),
        }
    } else if let Sharpness::Inexact(max) = &max_value {
        if let Some(val) = max_values.get_value() {
            if val < *max {
                *max_values = Sharpness::Inexact(max.clone())
            }
        }
    }
}

/// If the given value is numerically lesser than the original value,
/// it set the new min value with the exactness information.
fn set_min_if_lesser(
    max_values: &mut Sharpness<ScalarValue>,
    max_value: &Sharpness<ScalarValue>,
) {
    if let Sharpness::Exact(max) = &max_value {
        match max_values.clone() {
            Sharpness::Exact(val) => {
                if val < *max {
                    *max_values = Sharpness::Exact(max.clone())
                }
            }
            Sharpness::Inexact(val) => {
                *max_values = max_values.clone().to_inexact();
                if val < *max {
                    *max_values = Sharpness::Inexact(max.clone())
                }
            }
            Sharpness::Absent => *max_values = max_values.clone().to_inexact(),
        }
    } else if let Sharpness::Inexact(max) = &max_value {
        if let Some(val) = max_values.get_value() {
            if val < *max {
                *max_values = Sharpness::Inexact(max.clone())
            }
        }
    }
}
