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

//! Defines the spilling functions

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::FileReader;
use arrow::record_batch::RecordBatch;
use log::debug;
use tokio::sync::mpsc::Sender;

use datafusion_common::{exec_datafusion_err, Result};
use datafusion_execution::disk_manager::RefCountedTempFile;
use datafusion_execution::memory_pool::human_readable_size;
use datafusion_execution::SendableRecordBatchStream;

use crate::common::IPCWriter;
use crate::stream::RecordBatchReceiverStream;

/// Read spilled batches from the disk
///
/// `path` - temp file
/// `schema` - batches schema, should be the same across batches
/// `buffer` - internal buffer of capacity batches
pub(crate) fn read_spill_as_stream(
    path: RefCountedTempFile,
    schema: SchemaRef,
    buffer: usize,
) -> Result<SendableRecordBatchStream> {
    let mut builder = RecordBatchReceiverStream::builder(schema, buffer);
    let sender = builder.tx();

    builder.spawn_blocking(move || read_spill(sender, path.path()));

    Ok(builder.build())
}

/// Spills in-memory `batches` to disk.
///
/// Returns total number of the rows spilled to disk.
pub(crate) fn spill_record_batches(
    batches: Vec<RecordBatch>,
    path: PathBuf,
    schema: SchemaRef,
) -> Result<usize> {
    let mut writer = IPCWriter::new(path.as_ref(), schema.as_ref())?;
    for batch in batches {
        writer.write(&batch)?;
    }
    writer.finish()?;
    debug!(
        "Spilled {} batches of total {} rows to disk, memory released {}",
        writer.num_batches,
        writer.num_rows,
        human_readable_size(writer.num_bytes),
    );
    Ok(writer.num_rows)
}

fn read_spill(sender: Sender<Result<RecordBatch>>, path: &Path) -> Result<()> {
    let file = BufReader::new(File::open(path)?);
    let reader = FileReader::try_new(file, None)?;
    for batch in reader {
        sender
            .blocking_send(batch.map_err(Into::into))
            .map_err(|e| exec_datafusion_err!("{e}"))?;
    }
    Ok(())
}

/// Spill the `RecordBatch` to disk as smaller batches
/// split by `batch_size_rows`
pub fn spill_record_batch_by_size(
    batch: &RecordBatch,
    path: PathBuf,
    schema: SchemaRef,
    batch_size_rows: usize,
) -> Result<()> {
    let mut offset = 0;
    let total_rows = batch.num_rows();
    let mut writer = IPCWriter::new(&path, schema.as_ref())?;

    while offset < total_rows {
        let length = std::cmp::min(total_rows - offset, batch_size_rows);
        let batch = batch.slice(offset, length);
        offset += batch.num_rows();
        writer.write(&batch)?;
    }
    writer.finish()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::spill::{spill_record_batch_by_size, spill_record_batches};
    use crate::test::build_table_i32;
    use datafusion_common::Result;
    use datafusion_execution::disk_manager::DiskManagerConfig;
    use datafusion_execution::DiskManager;
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;

    #[test]
    fn test_batch_spill_and_read() -> Result<()> {
        let batch1 = build_table_i32(
            ("a2", &vec![0, 1, 2]),
            ("b2", &vec![3, 4, 5]),
            ("c2", &vec![4, 5, 6]),
        );

        let batch2 = build_table_i32(
            ("a2", &vec![10, 11, 12]),
            ("b2", &vec![13, 14, 15]),
            ("c2", &vec![14, 15, 16]),
        );

        let disk_manager = DiskManager::try_new(DiskManagerConfig::NewOs)?;

        let spill_file = disk_manager.create_tmp_file("Test Spill")?;
        let schema = batch1.schema();
        let num_rows = batch1.num_rows() + batch2.num_rows();
        let cnt = spill_record_batches(
            vec![batch1, batch2],
            spill_file.path().into(),
            Arc::clone(&schema),
        );
        assert_eq!(cnt.unwrap(), num_rows);

        let file = BufReader::new(File::open(spill_file.path())?);
        let reader = arrow::ipc::reader::FileReader::try_new(file, None)?;

        assert_eq!(reader.num_batches(), 2);
        assert_eq!(reader.schema(), schema);

        Ok(())
    }

    #[test]
    fn test_batch_spill_by_size() -> Result<()> {
        let batch1 = build_table_i32(
            ("a2", &vec![0, 1, 2, 3]),
            ("b2", &vec![3, 4, 5, 6]),
            ("c2", &vec![4, 5, 6, 7]),
        );

        let disk_manager = DiskManager::try_new(DiskManagerConfig::NewOs)?;

        let spill_file = disk_manager.create_tmp_file("Test Spill")?;
        let schema = batch1.schema();
        spill_record_batch_by_size(
            &batch1,
            spill_file.path().into(),
            Arc::clone(&schema),
            1,
        )?;

        let file = BufReader::new(File::open(spill_file.path())?);
        let reader = arrow::ipc::reader::FileReader::try_new(file, None)?;

        assert_eq!(reader.num_batches(), 4);
        assert_eq!(reader.schema(), schema);

        Ok(())
    }
}
