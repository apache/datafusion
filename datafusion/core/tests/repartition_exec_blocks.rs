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

use arrow::array::UInt32Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::from_slice::FromSlice;
use datafusion_common::{Result, Statistics};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{PhysicalExpr, PhysicalSortExpr};
use futures::{Stream, StreamExt};
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// A mock execution plan that simply returns the provided data source characteristic
#[derive(Debug, Clone)]
pub struct MyUnboundedExec {
    batch_produce: Option<usize>,
    schema: Arc<Schema>,
    /// Ref-counting helper to check if the plan and the produced stream are still in memory.
    refs: Arc<()>,
}
impl MyUnboundedExec {
    pub fn new(batch_produce: Option<usize>, schema: Schema) -> Self {
        Self {
            batch_produce,
            schema: Arc::new(schema),
            refs: Default::default(),
        }
    }
}
impl ExecutionPlan for MyUnboundedExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(UnboundedStream {
            batch_produce: self.batch_produce,
            count: 0,
            schema: Arc::clone(&self.schema),
            _refs: Arc::clone(&self.refs),
        }))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "UnboundableExec: unbounded={}",
                    self.batch_produce.is_none(),
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

#[derive(Debug)]
pub struct UnboundedStream {
    batch_produce: Option<usize>,
    count: usize,
    /// Schema mocked by this stream.
    schema: SchemaRef,

    /// Ref-counting helper to check if the stream are still in memory.
    _refs: Arc<()>,
}

impl Stream for UnboundedStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(val) = self.batch_produce {
            if val <= self.count {
                println!("Stream Finished");
                return Poll::Ready(None);
            }
        }
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![Arc::new(UInt32Array::from_slice([1]))],
        )?;
        self.count += 1;
        Poll::Ready(Some(Ok(batch)))
    }
}

impl RecordBatchStream for UnboundedStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// See <https://github.com/apache/arrow-datafusion/issues/5278>
#[tokio::test]
async fn unbounded_repartition_sa() -> Result<()> {
    let config = SessionConfig::new();
    let ctx = SessionContext::with_config(config);
    let task = ctx.task_ctx();
    let schema = Schema::new(vec![Field::new("a2", DataType::UInt32, false)]);
    let input = Arc::new(MyUnboundedExec::new(None, schema.clone()));
    let on: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Column::new("a2", 0))];
    let plan = Arc::new(RepartitionExec::try_new(input, Partitioning::Hash(on, 3))?);
    let plan = Arc::new(CoalescePartitionsExec::new(plan.clone()));
    let mut stream = plan.execute(0, task)?;

    // Note: `tokio::time::timeout` does NOT help here because in the mentioned issue, the whole runtime is blocked by a
    // CPU-spinning thread. Using a multithread runtime with multiple threads is NOT a solution since this would not
    // trigger the bug (the bug is not specific to a single-thread RT though, it's just the only way to trigger it reliably).
    let batch = stream
        .next()
        .await
        .expect("not terminated")
        .expect("no error in stream");
    assert_eq!(batch.schema().as_ref(), &schema);
    Ok(())
}
