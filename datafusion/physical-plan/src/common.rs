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
use std::fs::{metadata, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::SendableRecordBatchStream;
use crate::stream::RecordBatchReceiverStream;
use crate::{ColumnStatistics, Statistics};

use arrow::array::Array;
use arrow::datatypes::Schema;
use arrow::ipc::writer::{FileWriter, IpcWriteOptions};
use arrow::record_batch::RecordBatch;
use datafusion_common::stats::Precision;
use datafusion_common::{plan_err, DataFusionError, Result};
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

/// If running in a tokio context spawns the execution of `stream` to a separate task
/// allowing it to execute in parallel with an intermediate buffer of size `buffer`
pub(crate) fn spawn_buffered(
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

/// Write in Arrow IPC format.
pub struct IPCWriter {
    /// Path
    pub path: PathBuf,
    /// Inner writer
    pub writer: FileWriter<File>,
    /// Batches written
    pub num_batches: usize,
    /// Rows written
    pub num_rows: usize,
    /// Bytes written
    pub num_bytes: usize,
}

impl IPCWriter {
    /// Create new writer
    pub fn new(path: &Path, schema: &Schema) -> Result<Self> {
        let file = File::create(path).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to create partition file at {path:?}: {e:?}"
            ))
        })?;
        Ok(Self {
            num_batches: 0,
            num_rows: 0,
            num_bytes: 0,
            path: path.into(),
            writer: FileWriter::try_new(file, schema)?,
        })
    }

    /// Create new writer with IPC write options
    pub fn new_with_options(
        path: &Path,
        schema: &Schema,
        write_options: IpcWriteOptions,
    ) -> Result<Self> {
        let file = File::create(path).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to create partition file at {path:?}: {e:?}"
            ))
        })?;
        Ok(Self {
            num_batches: 0,
            num_rows: 0,
            num_bytes: 0,
            path: path.into(),
            writer: FileWriter::try_new_with_options(file, schema, write_options)?,
        })
    }
    /// Write one single batch
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write(batch)?;
        self.num_batches += 1;
        self.num_rows += batch.num_rows();
        let num_bytes: usize = batch.get_array_memory_size();
        self.num_bytes += num_bytes;
        Ok(())
    }

    /// Finish the writer
    pub fn finish(&mut self) -> Result<()> {
        self.writer.finish().map_err(Into::into)
    }

    /// Path write to
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Checks if the given projection is valid for the given schema.
pub fn can_project(
    schema: &arrow::datatypes::SchemaRef,
    projection: Option<&Vec<usize>>,
) -> Result<()> {
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
                },
                ColumnStatistics {
                    distinct_count: Precision::Absent,
                    max_value: Precision::Absent,
                    min_value: Precision::Absent,
                    sum_value: Precision::Absent,
                    null_count: Precision::Exact(0),
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
            }],
        };

        assert_eq!(actual, expected);
        Ok(())
    }
}
