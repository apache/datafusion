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

//! Defines common code used in execution plans

use std::fs;
use std::fs::metadata;
use std::sync::Arc;

use super::SendableRecordBatchStream;
use crate::expressions::Column;
use crate::projection::{ProjectionExec, ProjectionExpr};
use crate::stream::RecordBatchReceiverStream;
use crate::{ColumnStatistics, ExecutionPlan, Statistics};

use arrow::array::Array;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::stats::Precision;
use datafusion_common::{Result, internal_err, plan_err};
use datafusion_execution::memory_pool::MemoryReservation;

use futures::{StreamExt, TryStreamExt};
use parking_lot::Mutex;

/// [`MemoryReservation`] used across query execution streams
pub(crate) type SharedMemoryReservation = Arc<Mutex<MemoryReservation>>;

/// Create a vector of record batches from a stream
pub async fn collect(stream: SendableRecordBatchStream) -> Result<Vec<RecordBatch>> {
    stream.try_collect::<Vec<_>>().await
}

/// Recursively builds a list of files in a directory with a given extension
pub fn build_checked_file_list(dir: &str, ext: &str) -> Result<Vec<String>> {
    let mut filenames: Vec<String> = Vec::new();
    build_file_list_recurse(dir, &mut filenames, ext)?;
    if filenames.is_empty() {
        return plan_err!("No files found at {dir} with file extension {ext}");
    }
    Ok(filenames)
}

/// Recursively builds a list of files in a directory with a given extension
pub fn build_file_list(dir: &str, ext: &str) -> Result<Vec<String>> {
    let mut filenames: Vec<String> = Vec::new();
    build_file_list_recurse(dir, &mut filenames, ext)?;
    Ok(filenames)
}

/// Recursively build a list of files in a directory with a given extension with an accumulator list
fn build_file_list_recurse(
    dir: &str,
    filenames: &mut Vec<String>,
    ext: &str,
) -> Result<()> {
    let metadata = metadata(dir)?;
    if metadata.is_file() {
        if dir.ends_with(ext) {
            filenames.push(dir.to_string());
        }
    } else {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(path_name) = path.to_str() {
                if path.is_dir() {
                    build_file_list_recurse(path_name, filenames, ext)?;
                } else if path_name.ends_with(ext) {
                    filenames.push(path_name.to_string());
                }
            } else {
                return plan_err!("Invalid path");
            }
        }
    }
    Ok(())
}

/// Align `input`'s physical plan schema with `expected_schema` when only field names differ.
///
/// This helper is intended for operators that combine independently planned children but
/// expose a single declared output schema. It returns `input` unchanged when schemas already
/// match exactly. Otherwise, it validates that projection can safely produce the expected
/// schema, then wraps `input` in a [`ProjectionExec`] that keeps columns in their existing
/// positional order and aliases them to `expected_schema`'s field names.
///
/// [`ProjectionExec`] can rename fields but preserves column data types, nullability, field
/// metadata, and schema metadata from the input expressions. Therefore, this helper rejects
/// mismatches in those attributes rather than returning a plan whose schema still differs
/// from `expected_schema`.
pub fn project_plan_to_schema(
    input: Arc<dyn ExecutionPlan>,
    expected_schema: &SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    let input_schema = input.schema();
    if input_schema.as_ref() == expected_schema.as_ref() {
        return Ok(input);
    }

    if input_schema.fields().len() != expected_schema.fields().len() {
        return internal_err!(
            "Cannot project plan to expected schema: expected {} column(s), got {}",
            expected_schema.fields().len(),
            input_schema.fields().len()
        );
    }

    if input_schema.metadata() != expected_schema.metadata() {
        return internal_err!(
            "Cannot project plan to expected schema: schema metadata differ"
        );
    }

    if let Some((i, (input_field, expected_field))) = input_schema
        .fields()
        .iter()
        .zip(expected_schema.fields().iter())
        .enumerate()
        .find(|(_, (input_field, expected_field))| {
            input_field.data_type() != expected_field.data_type()
                || input_field.is_nullable() != expected_field.is_nullable()
                || input_field.metadata() != expected_field.metadata()
        })
    {
        return internal_err!(
            "Cannot project plan column {i} ('{}') to expected output field '{}': \
             fields differ beyond name (input field: {:?}, expected field: {:?})",
            input_field.name(),
            expected_field.name(),
            input_field,
            expected_field
        );
    }

    let projection_exprs = expected_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, expected_field)| ProjectionExpr {
            expr: Arc::new(Column::new(input_schema.field(i).name(), i)),
            alias: expected_field.name().clone(),
        })
        .collect::<Vec<_>>();

    let projection = ProjectionExec::try_new(projection_exprs, input)?;
    debug_assert_eq!(projection.schema().as_ref(), expected_schema.as_ref());
    Ok(Arc::new(projection))
}

/// If running in a tokio context spawns the execution of `stream` to a separate task
/// allowing it to execute in parallel with an intermediate buffer of size `buffer`
pub fn spawn_buffered(
    mut input: SendableRecordBatchStream,
    buffer: usize,
) -> SendableRecordBatchStream {
    // Use tokio only if running from a multi-thread tokio context
    match tokio::runtime::Handle::try_current() {
        Ok(handle)
            if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread =>
        {
            let mut builder = RecordBatchReceiverStream::builder(input.schema(), buffer);

            let sender = builder.tx();

            builder.spawn(async move {
                while let Some(item) = input.next().await {
                    if sender.send(item).await.is_err() {
                        // Receiver dropped when query is shutdown early (e.g., limit) or error,
                        // no need to return propagate the send error.
                        return Ok(());
                    }
                }

                Ok(())
            });

            builder.build()
        }
        _ => input,
    }
}

/// Computes the statistics for an in-memory RecordBatch
///
/// Only computes statistics that are in arrows metadata (num rows, byte size and nulls)
/// and does not apply any kernel on the actual data.
pub fn compute_record_batch_statistics(
    batches: &[Vec<RecordBatch>],
    schema: &Schema,
    projection: Option<Vec<usize>>,
) -> Statistics {
    let nb_rows = batches.iter().flatten().map(RecordBatch::num_rows).sum();

    let projection = match projection {
        Some(p) => p,
        None => (0..schema.fields().len()).collect(),
    };

    let total_byte_size = batches
        .iter()
        .flatten()
        .map(|b| {
            projection
                .iter()
                .map(|index| b.column(*index).get_array_memory_size())
                .sum::<usize>()
        })
        .sum();

    let mut null_counts = vec![0; projection.len()];

    for partition in batches.iter() {
        for batch in partition {
            for (stat_index, col_index) in projection.iter().enumerate() {
                null_counts[stat_index] += batch
                    .column(*col_index)
                    .logical_nulls()
                    .map(|nulls| nulls.null_count())
                    .unwrap_or_default();
            }
        }
    }
    let column_statistics = null_counts
        .into_iter()
        .map(|null_count| {
            let mut s = ColumnStatistics::new_unknown();
            s.null_count = Precision::Exact(null_count);
            s
        })
        .collect();

    Statistics {
        num_rows: Precision::Exact(nb_rows),
        total_byte_size: Precision::Exact(total_byte_size),
        column_statistics,
    }
}

/// Checks if the given projection is valid for the given schema.
pub fn can_project(schema: &SchemaRef, projection: Option<&[usize]>) -> Result<()> {
    match projection {
        Some(columns) => {
            if columns
                .iter()
                .max()
                .is_some_and(|&i| i >= schema.fields().len())
            {
                Err(arrow::error::ArrowError::SchemaError(format!(
                    "project index {} out of bounds, max field {}",
                    columns.iter().max().unwrap(),
                    schema.fields().len()
                ))
                .into())
            } else {
                Ok(())
            }
        }
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::{
        array::{Float32Array, Float64Array, UInt64Array},
        datatypes::{DataType, Field},
    };

    #[test]
    fn test_compute_record_batch_statistics_empty() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("f32", DataType::Float32, false),
            Field::new("f64", DataType::Float64, false),
        ]));
        let stats = compute_record_batch_statistics(&[], &schema, Some(vec![0, 1]));

        assert_eq!(stats.num_rows, Precision::Exact(0));
        assert_eq!(stats.total_byte_size, Precision::Exact(0));
        Ok(())
    }

    #[test]
    fn test_compute_record_batch_statistics() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("f32", DataType::Float32, false),
            Field::new("f64", DataType::Float64, false),
            Field::new("u64", DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Float32Array::from(vec![1., 2., 3.])),
                Arc::new(Float64Array::from(vec![9., 8., 7.])),
                Arc::new(UInt64Array::from(vec![4, 5, 6])),
            ],
        )?;

        // Just select f32,f64
        let select_projection = Some(vec![0, 1]);
        let byte_size = batch
            .project(&select_projection.clone().unwrap())
            .unwrap()
            .get_array_memory_size();

        let actual =
            compute_record_batch_statistics(&[vec![batch]], &schema, select_projection);

        let expected = Statistics {
            num_rows: Precision::Exact(3),
            total_byte_size: Precision::Exact(byte_size),
            column_statistics: vec![
                ColumnStatistics {
                    distinct_count: Precision::Absent,
                    max_value: Precision::Absent,
                    min_value: Precision::Absent,
                    sum_value: Precision::Absent,
                    null_count: Precision::Exact(0),
                    byte_size: Precision::Absent,
                },
                ColumnStatistics {
                    distinct_count: Precision::Absent,
                    max_value: Precision::Absent,
                    min_value: Precision::Absent,
                    sum_value: Precision::Absent,
                    null_count: Precision::Exact(0),
                    byte_size: Precision::Absent,
                },
            ],
        };

        assert_eq!(actual, expected);
        Ok(())
    }

    #[test]
    fn test_compute_record_batch_statistics_null() -> Result<()> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("u64", DataType::UInt64, true)]));
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt64Array::from(vec![Some(1), None, None]))],
        )?;
        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt64Array::from(vec![Some(1), Some(2), None]))],
        )?;
        let byte_size = batch1.get_array_memory_size() + batch2.get_array_memory_size();
        let actual =
            compute_record_batch_statistics(&[vec![batch1], vec![batch2]], &schema, None);

        let expected = Statistics {
            num_rows: Precision::Exact(6),
            total_byte_size: Precision::Exact(byte_size),
            column_statistics: vec![ColumnStatistics {
                distinct_count: Precision::Absent,
                max_value: Precision::Absent,
                min_value: Precision::Absent,
                sum_value: Precision::Absent,
                null_count: Precision::Exact(3),
                byte_size: Precision::Absent,
            }],
        };

        assert_eq!(actual, expected);
        Ok(())
    }
}
