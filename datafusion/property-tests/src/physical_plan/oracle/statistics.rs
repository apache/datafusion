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

use std::cmp::Ordering;
use std::collections::HashSet;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Schema};
use datafusion_common::stats::Precision;
use datafusion_common::{ColumnStatistics, Result, ScalarValue, Statistics};

/// Computes the exact statistics of `batches`, which must all have `schema`.
///
/// For every column this computes the null count, the minimum and maximum
/// non-null values, the number of distinct non-null values, and for integer
/// and decimal columns the sum of the non-null values, in the type of SQL
/// `SUM` (see [`ColumnStatistics::sum_value`]). The minimum, maximum and sum
/// are `Absent` when a column has no non-null values, and the sum is also
/// `Absent` when it overflows. The minimum and maximum of a nested column,
/// such as a list or a struct, are not computed, as file formats do not
/// report them. The sum of a floating point column is not
/// computed, because its exact value depends on the order in which the values
/// are added. The byte sizes and the total byte size are not computed and are
/// `Absent`, because their exact values depend on implementation details such
/// as buffer sizes.
pub fn exact_statistics(schema: &Schema, batches: &[RecordBatch]) -> Result<Statistics> {
    let num_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    let column_statistics = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(column, field)| column_statistics(batches, column, field.data_type()))
        .collect::<Result<Vec<_>>>()?;
    Ok(Statistics {
        num_rows: Precision::Exact(num_rows),
        total_byte_size: Precision::Absent,
        column_statistics,
    })
}

#[allow(clippy::allow_attributes, clippy::mutable_key_type)] // ScalarValue has interior mutability but is intentionally used as hash key
fn column_statistics(
    batches: &[RecordBatch],
    column: usize,
    data_type: &DataType,
) -> Result<ColumnStatistics> {
    let mut null_count = 0;
    let mut min: Option<ScalarValue> = None;
    let mut max: Option<ScalarValue> = None;
    let summed = data_type.is_integer() || data_type.is_decimal();
    let bounded = !data_type.is_nested();
    let mut sum: Option<Precision<ScalarValue>> = None;
    let mut distinct = HashSet::new();

    for batch in batches {
        let array = batch.column(column);
        // Logical nulls include values that are null without a null buffer,
        // such as every value of a `NullArray`
        let nulls = array.logical_nulls();
        for row in 0..array.len() {
            if nulls.as_ref().is_some_and(|nulls| nulls.is_null(row)) {
                null_count += 1;
                continue;
            }
            let value = ScalarValue::try_from_array(array, row)?;
            if bounded
                && min
                    .as_ref()
                    .is_none_or(|m| value.partial_cmp(m) == Some(Ordering::Less))
            {
                min = Some(value.clone());
            }
            if bounded
                && max
                    .as_ref()
                    .is_none_or(|m| value.partial_cmp(m) == Some(Ordering::Greater))
            {
                max = Some(value.clone());
            }
            if summed {
                // Added as `Statistics::try_merge_iter` adds sums: widened to the
                // type of SQL `SUM`, and `Absent` once it overflows
                let value = Precision::Exact(value.clone());
                sum = Some(match sum {
                    None => value.cast_to_sum_type(),
                    Some(sum) => sum.add_for_sum(&value),
                });
            }
            distinct.insert(value);
        }
    }

    Ok(ColumnStatistics::new_unknown()
        .with_null_count(Precision::Exact(null_count))
        .with_min_value(min.map_or(Precision::Absent, Precision::Exact))
        .with_max_value(max.map_or(Precision::Absent, Precision::Exact))
        .with_sum_value(sum.unwrap_or(Precision::Absent))
        .with_distinct_count(Precision::Exact(distinct.len())))
}
