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
pub fn read_spill_as_stream(
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
pub fn spill_record_batches(
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
