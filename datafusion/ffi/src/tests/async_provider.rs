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

//! This is an example of an async table provider that will call functions on
//! the tokio runtime of the library providing the function. Since we cannot
//! share a tokio runtime across the ffi boundary and the producer and consumer
//! may have different runtimes, we need to store a reference to the runtime
//! and enter it during streaming calls. The entering of the runtime will
//! occur by the datafusion_ffi crate during the streaming calls. This code
//! serves as an integration test of this feature. If we do not correctly
//! access the runtime, then you will get a panic when trying to do operations
//! such as spawning a tokio task.

use std::{any::Any, fmt::Debug, sync::Arc};

use crate::table_provider::FFI_TableProvider;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    error::{DataFusionError, Result},
    execution::RecordBatchStream,
    physical_expr::EquivalenceProperties,
    physical_plan::{ExecutionPlan, Partitioning},
    prelude::Expr,
};
use futures::Stream;
use tokio::{
    runtime::Handle,
    sync::{broadcast, mpsc},
};

use super::create_record_batch;

#[derive(Debug)]
pub struct AsyncTableProvider {
    batch_request: mpsc::Sender<bool>,
    shutdown: mpsc::Sender<bool>,
    batch_receiver: broadcast::Receiver<Option<RecordBatch>>,
    _join_handle: Option<std::thread::JoinHandle<()>>,
}

fn async_table_provider_thread(
    mut shutdown: mpsc::Receiver<bool>,
    mut batch_request: mpsc::Receiver<bool>,
    batch_sender: broadcast::Sender<Option<RecordBatch>>,
    tokio_rt: mpsc::Sender<Handle>,
) {
    let runtime = Arc::new(
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("Unable to create tokio runtime"),
    );
    let _runtime_guard = runtime.enter();
    tokio_rt
        .blocking_send(runtime.handle().clone())
        .expect("Unable to send tokio runtime back to main thread");

    runtime.block_on(async move {
        let mut num_received = 0;
        while let Some(true) = batch_request.recv().await {
            let record_batch = match num_received {
                0 => Some(create_record_batch(1, 5)),
                1 => Some(create_record_batch(6, 1)),
                2 => Some(create_record_batch(7, 5)),
                _ => None,
            };
            num_received += 1;

            if batch_sender.send(record_batch).is_err() {
                break;
            }
        }
    });

    let _ = shutdown.blocking_recv();
}

pub fn start_async_provider() -> (AsyncTableProvider, Handle) {
    let (batch_request_tx, batch_request_rx) = mpsc::channel(10);
    let (record_batch_tx, record_batch_rx) = broadcast::channel(10);
    let (tokio_rt_tx, mut tokio_rt_rx) = mpsc::channel(10);
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    // It is important that we are not using tokio to spawn here. We want this
    // other thread to create it's own runtime, which is similar to a model used
    // in datafusion-python and probably other places. This will let us test that
    // we do correctly enter the runtime of the foreign provider.
    let join_handle = Some(std::thread::spawn(move || {
        async_table_provider_thread(
            shutdown_rx,
            batch_request_rx,
            record_batch_tx,
            tokio_rt_tx,
        )
    }));

    let tokio_rt = tokio_rt_rx
        .blocking_recv()
        .expect("Unable to receive tokio runtime from spawned thread");

    let table_provider = AsyncTableProvider {
        shutdown: shutdown_tx,
        batch_request: batch_request_tx,
        batch_receiver: record_batch_rx,
        _join_handle: join_handle,
    };

    (table_provider, tokio_rt)
}

#[async_trait]
impl TableProvider for AsyncTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        super::create_test_schema()
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(AsyncTestExecutionPlan::new(
            self.batch_request.clone(),
            self.batch_receiver.resubscribe(),
        )))
    }
}

impl Drop for AsyncTableProvider {
    fn drop(&mut self) {
        self.shutdown
            .blocking_send(false)
            .expect("Unable to call shutdown on spawned thread.")
    }
}

#[derive(Debug)]
struct AsyncTestExecutionPlan {
    properties: datafusion::physical_plan::PlanProperties,
    batch_request: mpsc::Sender<bool>,
    batch_receiver: broadcast::Receiver<Option<RecordBatch>>,
}

impl AsyncTestExecutionPlan {
    pub fn new(
        batch_request: mpsc::Sender<bool>,
        batch_receiver: broadcast::Receiver<Option<RecordBatch>>,
    ) -> Self {
        Self {
            properties: datafusion::physical_plan::PlanProperties::new(
                EquivalenceProperties::new(super::create_test_schema()),
                Partitioning::UnknownPartitioning(3),
                datafusion::physical_plan::execution_plan::EmissionType::Incremental,
                datafusion::physical_plan::execution_plan::Boundedness::Bounded,
            ),
            batch_request,
            batch_receiver,
        }
    }
}

impl ExecutionPlan for AsyncTestExecutionPlan {
    fn name(&self) -> &str {
        "async test execution plan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::default()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream> {
        Ok(Box::pin(AsyncTestRecordBatchStream {
            batch_request: self.batch_request.clone(),
            batch_receiver: self.batch_receiver.resubscribe(),
        }))
    }
}

impl datafusion::physical_plan::DisplayAs for AsyncTestExecutionPlan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        _f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        // Do nothing, just a test
        Ok(())
    }
}

struct AsyncTestRecordBatchStream {
    batch_request: mpsc::Sender<bool>,
    batch_receiver: broadcast::Receiver<Option<RecordBatch>>,
}

impl RecordBatchStream for AsyncTestRecordBatchStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        super::create_test_schema()
    }
}

impl Stream for AsyncTestRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut this = self.as_mut();

        #[allow(clippy::disallowed_methods)]
        tokio::spawn(async move {
            // Nothing to do. We just need to simulate an async
            // task running
        });

        if let Err(e) = this.batch_request.try_send(true) {
            return std::task::Poll::Ready(Some(Err(DataFusionError::Execution(
                format!("Unable to send batch request, {}", e),
            ))));
        }

        match this.batch_receiver.blocking_recv() {
            Ok(batch) => match batch {
                Some(batch) => std::task::Poll::Ready(Some(Ok(batch))),
                None => std::task::Poll::Ready(None),
            },
            Err(e) => std::task::Poll::Ready(Some(Err(DataFusionError::Execution(
                format!("Unable receive record batch: {}", e),
            )))),
        }
    }
}

pub(crate) fn create_async_table_provider() -> FFI_TableProvider {
    let (table_provider, tokio_rt) = start_async_provider();
    FFI_TableProvider::new(Arc::new(table_provider), true, Some(tokio_rt))
}
