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

use arrow::error::Result;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use sha2::{Digest, Sha256};
use std::io::Cursor;

pub fn record_batch_to_buffer(batch: &RecordBatch) -> Result<Vec<u8>> {
    let schema = batch.schema();
    let mut buffer = Vec::new();
    {
        let mut cursor = Cursor::new(&mut buffer);
        let mut writer = StreamWriter::try_new(&mut cursor, &schema)?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buffer)
}

pub fn buffer_to_record_batch(buffer: &[u8]) -> Result<RecordBatch> {
    let cursor = Cursor::new(buffer);
    let mut reader = StreamReader::try_new(cursor, None)?;

    // Assuming the buffer contains only one RecordBatch
    let batch = reader.next().unwrap()?;
    Ok(batch)
}

pub fn buffer_to_hash(buffer: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(buffer);
    let result = hasher.finalize();
    format!("{:x}", result)
}
