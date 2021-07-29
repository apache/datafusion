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

use super::{RecordBatchStream, SendableRecordBatchStream};
use crate::error::{DataFusionError, Result};
use crate::physical_plan::ExecutionPlan;
use arrow::compute::concat;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use futures::channel::mpsc;
use futures::{SinkExt, Stream, StreamExt, TryStreamExt};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::task::JoinHandle;

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

/// Combine a slice of record batches into one, or returns None if the slice itself
/// is empty; all the record batches inside the slice must be of the same schema.
pub(crate) fn combine_batches(
    batches: &[RecordBatch],
    schema: SchemaRef,
) -> ArrowResult<Option<RecordBatch>> {
    if batches.is_empty() {
        Ok(None)
    } else {
        let columns = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, _)| {
                concat(
                    &batches
                        .iter()
                        .map(|batch| batch.column(i).as_ref())
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<ArrowResult<Vec<_>>>()?;
        Ok(Some(RecordBatch::try_new(schema.clone(), columns)?))
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Float32Array, Float64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    #[test]
    fn test_combine_batches_empty() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("f32", DataType::Float32, false),
            Field::new("f64", DataType::Float64, false),
        ]));
        let result = combine_batches(&[], schema)?;
        assert!(result.is_none());
        Ok(())
    }

    #[test]
    fn test_combine_batches() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("f32", DataType::Float32, false),
            Field::new("f64", DataType::Float64, false),
        ]));

        let batch_count = 1000;
        let batch_size = 10;
        let batches = (0..batch_count)
            .map(|i| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Float32Array::from(vec![i as f32; batch_size])),
                        Arc::new(Float64Array::from(vec![i as f64; batch_size])),
                    ],
                )
                .unwrap()
            })
            .collect::<Vec<_>>();

        let result = combine_batches(&batches, schema)?;
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(batch_count * batch_size, result.num_rows());
        Ok(())
    }
}
