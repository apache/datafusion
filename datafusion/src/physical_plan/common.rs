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
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use futures::channel::mpsc;
use futures::{SinkExt, Stream, StreamExt, TryStreamExt};
use tokio::task::JoinHandle;

use crate::arrow::error::ArrowError;
use crate::error::{DataFusionError, Result};
use crate::physical_plan::ExecutionPlan;

use super::{RecordBatchStream, SendableRecordBatchStream};

/// Stream of record batches
pub struct SizedRecordBatchStream {
    schema: SchemaRef,
    batches: Vec<Arc<RecordBatch>>,
    index: usize,
}

impl SizedRecordBatchStream {
    /// Create a new RecordBatchIterator
    pub fn new(schema: SchemaRef, batches: Vec<Arc<RecordBatch>>) -> Self {
        SizedRecordBatchStream {
            schema,
            index: 0,
            batches,
        }
    }
}

impl Stream for SizedRecordBatchStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(if self.index < self.batches.len() {
            self.index += 1;
            Some(Ok(self.batches[self.index - 1].as_ref().clone()))
        } else {
            None
        })
    }
}

impl RecordBatchStream for SizedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Create a vector of record batches from a stream
pub async fn collect(stream: SendableRecordBatchStream) -> Result<Vec<RecordBatch>> {
    stream
        .try_collect::<Vec<_>>()
        .await
        .map_err(DataFusionError::from)
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
                return Err(DataFusionError::Plan("Invalid path".to_string()));
            }
        }
    }
    Ok(())
}

/// Spawns a task to the tokio threadpool and writes its outputs to the provided mpsc sender
pub(crate) fn spawn_execution(
    input: Arc<dyn ExecutionPlan>,
    mut output: mpsc::Sender<ArrowResult<RecordBatch>>,
    partition: usize,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut stream = match input.execute(partition).await {
            Err(e) => {
                // If send fails, plan being torn
                // down, no place to send the error
                let arrow_error = ArrowError::ExternalError(Box::new(e));
                output.send(Err(arrow_error)).await.ok();
                return;
            }
            Ok(stream) => stream,
        };

        while let Some(item) = stream.next().await {
            // If send fails, plan being torn down,
            // there is no place to send the error
            output.send(item).await.ok();
        }
    })
}
