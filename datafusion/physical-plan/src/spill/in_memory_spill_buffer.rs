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

use crate::memory::MemoryStream;
use crate::spill::spill_manager::GetSlicedSize;
use arrow::array::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::SendableRecordBatchStream;
use std::sync::Arc;

#[derive(Debug)]
pub struct InMemorySpillBuffer {
    batches: Vec<RecordBatch>,
    total_bytes: usize,
}

impl InMemorySpillBuffer {
    pub fn from_batch(batch: &RecordBatch) -> Result<Self> {
        Ok(Self {
            batches: vec![batch.clone()],
            total_bytes: batch.get_sliced_size()?,
        })
    }

    pub fn from_batches(batches: &[RecordBatch]) -> Result<Self> {
        let mut total_bytes = 0;
        let mut owned = Vec::with_capacity(batches.len());
        for b in batches {
            total_bytes += b.get_sliced_size()?;
            owned.push(b.clone());
        }
        Ok(Self {
            batches: owned,
            total_bytes,
        })
    }

    pub fn as_stream(
        self: Arc<Self>,
        schema: Arc<arrow_schema::Schema>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = MemoryStream::try_new(self.batches.clone(), schema, None)?;
        Ok(Box::pin(stream))
    }

    pub fn size(&self) -> usize {
        self.total_bytes
    }
}
