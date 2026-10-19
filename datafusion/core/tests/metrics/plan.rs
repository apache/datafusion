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

//! Shared fixture: real streaming, filter and projection operators.

use std::sync::Arc;

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::{Result, ScalarValue};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::Operator;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::streaming::{PartitionStream, StreamingTableExec};
use futures::StreamExt;
use tokio::sync::Notify;

#[derive(Debug)]
struct InputPartition {
    schema: SchemaRef,
    batch: RecordBatch,
    gate: Option<Arc<Notify>>,
}

impl PartitionStream for InputPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        let first = self.batch.clone();
        let second = self.batch.clone();
        let gate = self.gate.clone();
        let stream = futures::stream::once(async move { Ok(first) }).chain(
            futures::stream::once(async move {
                if let Some(gate) = gate {
                    gate.notified().await;
                }
                Ok(second)
            }),
        );
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))
    }
}

pub type SharedPlan = (Vec<Arc<dyn ExecutionPlan>>, Arc<Notify>);

/// Partition 0 emits one batch, then waits while other partitions finish.
/// Returns nodes in root-to-leaf order for per-node reporting.
pub fn shared_plan(partitions: usize) -> Result<SharedPlan> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from_iter_values(0..32))],
    )?;
    let gate = Arc::new(Notify::new());
    let inputs = (0..partitions)
        .map(|partition| {
            Arc::new(InputPartition {
                schema: Arc::clone(&schema),
                batch: batch.clone(),
                gate: (partition == 0).then(|| Arc::clone(&gate)),
            }) as Arc<dyn PartitionStream>
        })
        .collect();
    // The limit enables StreamingTableExec's own baseline metrics without
    // truncating this finite input.
    let source: Arc<dyn ExecutionPlan> = Arc::new(StreamingTableExec::try_new(
        schema,
        inputs,
        None,
        [],
        false,
        Some(usize::MAX),
    )?);
    let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("a", 0));
    let predicate = Arc::new(BinaryExpr::new(
        Arc::clone(&column),
        Operator::Lt,
        Arc::new(Literal::new(ScalarValue::Int32(Some(16)))),
    ));
    let filter: Arc<dyn ExecutionPlan> = Arc::new(
        FilterExec::try_new(predicate, Arc::clone(&source))?.with_batch_size(16)?,
    );
    let expression: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        column,
        Operator::Plus,
        Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
    ));
    let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
        vec![(expression, "b".to_string())],
        Arc::clone(&filter),
    )?);
    Ok((vec![projection, filter, source], gate))
}
