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

//! `ExchangeSourceExec`: the consumer leaf of a streaming in-memory exchange.
//! For output partition `d` it takes the receivers `(from_stage_id, t, d)` for
//! every producer task `t`, merges them, IPC-decodes each frame back into
//! `RecordBatch`es, and streams them out. The streaming replacement for
//! `ShuffleReaderExec`.

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::stream::{self, StreamExt};
use tokio::sync::mpsc::Receiver;

use super::{Frame, InMemoryExchange};

/// Consumer leaf for a shuffle edge. For output partition `d` it merges the
/// `T` channels `(from_stage_id, t, d)` (one per producer task) into a single
/// stream, decoding each IPC frame back into `RecordBatch`es. The merged
/// stream ends only when all `T` producers have closed their senders.
pub struct ExchangeSourceExec {
    from_stage_id: usize,
    schema: SchemaRef,
    num_producer_tasks: usize,
    output_partition_count: usize,
    exchange: Arc<InMemoryExchange>,
    properties: Arc<PlanProperties>,
}

impl ExchangeSourceExec {
    pub fn try_new(
        from_stage_id: usize,
        schema: SchemaRef,
        num_producer_tasks: usize,
        output_partition_count: usize,
        exchange: Arc<InMemoryExchange>,
    ) -> Result<Self> {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(output_partition_count),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            from_stage_id,
            schema,
            num_producer_tasks,
            output_partition_count,
            exchange,
            properties,
        })
    }

    #[expect(clippy::wrong_self_convention)]
    pub fn from_stage_id(&self) -> usize {
        self.from_stage_id
    }

    pub fn num_producer_tasks(&self) -> usize {
        self.num_producer_tasks
    }

    pub fn output_partition_count(&self) -> usize {
        self.output_partition_count
    }
}

impl fmt::Debug for ExchangeSourceExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ExchangeSourceExec")
            .field("from_stage_id", &self.from_stage_id)
            .field("num_producer_tasks", &self.num_producer_tasks)
            .field("output_partition_count", &self.output_partition_count)
            .finish()
    }
}

impl DisplayAs for ExchangeSourceExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "ExchangeSourceExec: from_stage={}, num_producer_tasks={}, output_partition_count={}",
                    self.from_stage_id,
                    self.num_producer_tasks,
                    self.output_partition_count
                )
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "from_stage={}", self.from_stage_id)
            }
        }
    }
}

impl ExecutionPlan for ExchangeSourceExec {
    fn name(&self) -> &'static str {
        "ExchangeSourceExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Leaf node: nothing to swap.
        Ok(self)
    }

    /// Merges every producer task's channel for output partition `partition`,
    /// decoding IPC frames back into record batches as they arrive.
    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Take one receiver per producer task, up front — a missing/taken
        // receiver is a wiring bug and fails here.
        let mut receivers = Vec::with_capacity(self.num_producer_tasks);
        for t in 0..self.num_producer_tasks {
            receivers.push(self.exchange.take_receiver(
                self.from_stage_id,
                t,
                partition,
            )?);
        }

        // Turn each receiver into a stream of frames via `unfold` (no
        // `tokio-stream` dep), then `select_all`-merge them: the merged stream
        // is exhausted only once every producer has closed its sender.
        let frame_streams = receivers
            .into_iter()
            .map(|rx| receiver_to_stream(rx).boxed());
        let merged = stream::select_all(frame_streams);

        // Decode each frame (one frame -> one or more batches) and flatten.
        let batch_stream = merged.flat_map(|frame| {
            let decoded: Vec<Result<RecordBatch>> = match decode_frame(&frame) {
                Ok(batches) => batches.into_iter().map(Ok).collect(),
                Err(e) => vec![Err(e)],
            };
            stream::iter(decoded)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            batch_stream,
        )))
    }
}

/// Turns a bounded mpsc `Receiver` into a `Stream<Item = Frame>` that ends
/// when the channel is closed.
fn receiver_to_stream(rx: Receiver<Frame>) -> impl futures::Stream<Item = Frame> {
    stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|f| (f, rx)) })
}

/// IPC-decodes one frame (produced by the sink's `StreamWriter`) back into its
/// record batches.
fn decode_frame(frame: &[u8]) -> Result<Vec<RecordBatch>> {
    let reader = StreamReader::try_new(std::io::Cursor::new(frame), None)?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    Ok(batches)
}
