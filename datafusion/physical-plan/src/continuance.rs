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

//! Defines the continuance query plan

use std::any::Any;
use std::sync::Arc;

// use crate::error::{DataFusionError, Result};
// use crate::physical_plan::{
//     DisplayFormatType, Distribution, ExecutionPlan, Partitioning,
// };
use arrow::datatypes::SchemaRef;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::Partitioning;

use crate::{DisplayAs, DisplayFormatType, ExecutionPlan};

use super::expressions::PhysicalSortExpr;
use super::stream::RecordBatchReceiverStream;
use super::{
    metrics::{ExecutionPlanMetricsSet, MetricsSet},
    SendableRecordBatchStream, Statistics,
};
use datafusion_common::{DataFusionError, Result};

// use crate::exe::context::TaskContext;

/// A temporary "working table" operation wehre the input data will be
/// taken from the named handle during the execution and will be re-published
/// as is (kind of like a mirror).
///
/// Most notably used in the implementation of recursive queries where the
/// underlying relation does not exist yet but the data will come as the previous
/// term is evaluated.
#[derive(Debug)]
pub struct ContinuanceExec {
    /// Name of the relation handler
    name: String,
    /// The schema of the stream
    schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl ContinuanceExec {
    /// Create a new execution plan for a continuance stream. The given relation
    /// handler must exist in the task context before calling [`execute`] on this
    /// plan.
    pub fn new(name: String, schema: SchemaRef) -> Self {
        Self {
            name,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for ContinuanceExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                // TODO: add more details
                write!(f, "ContinuanceExec: name={}", self.name)
            }
        }
    }
}

impl ExecutionPlan for ContinuanceExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ContinuanceExec::new(
            self.name.clone(),
            self.schema.clone(),
        )))
    }

    /// This plan does not come with any special streams, but rather we use
    /// the existing [`RecordBatchReceiverStream`] to receive the data from
    /// the registered handle.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Continuance streams must be the plan base.
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "ContinuanceExec got an invalid partition {} (expected 0)",
                partition
            )));
        }

        // let stream = Box::pin(CombinedRecordBatchStream::new(
        //     self.schema(),
        //     input_stream_vec,
        // ));
        // return Ok(Box::pin(ObservedStream::new(stream, baseline_metrics)));

        // The relation handler must be already registered by the
        // parent op.
        let receiver = context.pop_relation_handler(self.name.clone())?;
        // TODO: this looks wrong.
        Ok(RecordBatchReceiverStream::builder(self.schema.clone(), 1).build())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

#[cfg(test)]
mod tests {}
