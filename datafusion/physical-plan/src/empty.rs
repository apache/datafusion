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

//! EmptyRelation execution plan

use std::any::Any;
use std::sync::Arc;

use super::expressions::PhysicalSortExpr;
use super::{common, DisplayAs, SendableRecordBatchStream, Statistics};
use crate::{memory::MemoryStream, DisplayFormatType, ExecutionPlan, Partitioning};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_execution::TaskContext;

use log::trace;

/// Execution plan for empty relation (produces no rows)
#[derive(Debug)]
pub struct EmptyExec {
    /// The schema for the produced row
    schema: SchemaRef,
    /// Number of partitions
    partitions: usize,
}

impl EmptyExec {
    /// Create a new EmptyExec
    pub fn new(schema: SchemaRef) -> Self {
        EmptyExec {
            schema,
            partitions: 1,
        }
    }

    /// Create a new EmptyExec with specified partition number
    pub fn with_partitions(mut self, partitions: usize) -> Self {
        self.partitions = partitions;
        self
    }

    fn data(&self) -> Result<Vec<RecordBatch>> {
        Ok(vec![])
    }
}

impl DisplayAs for EmptyExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "EmptyExec")
            }
        }
    }
}

impl ExecutionPlan for EmptyExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partitions)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(EmptyExec::new(self.schema.clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!("Start EmptyExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());

        if partition >= self.partitions {
            return internal_err!(
                "EmptyExec invalid partition {} (expected less than {})",
                partition,
                self.partitions
            );
        }

        Ok(Box::pin(MemoryStream::try_new(
            self.data()?,
            self.schema.clone(),
            None,
        )?))
    }

    fn statistics(&self) -> Result<Statistics> {
        let batch = self
            .data()
            .expect("Create empty RecordBatch should not fail");
        Ok(common::compute_record_batch_statistics(
            &[batch],
            &self.schema,
            None,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::with_new_children_if_necessary;
    use crate::{common, test};

    #[tokio::test]
    async fn empty() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = test::aggr_test_schema();

        let empty = EmptyExec::new(schema.clone());
        assert_eq!(empty.schema(), schema);

        // we should have no results
        let iter = empty.execute(0, task_ctx)?;
        let batches = common::collect(iter).await?;
        assert!(batches.is_empty());

        Ok(())
    }

    #[test]
    fn with_new_children() -> Result<()> {
        let schema = test::aggr_test_schema();
        let empty = Arc::new(EmptyExec::new(schema.clone()));

        let empty2 = with_new_children_if_necessary(empty.clone(), vec![])?.into();
        assert_eq!(empty.schema(), empty2.schema());

        let too_many_kids = vec![empty2];
        assert!(
            with_new_children_if_necessary(empty, too_many_kids).is_err(),
            "expected error when providing list of kids"
        );
        Ok(())
    }

    #[tokio::test]
    async fn invalid_execute() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = test::aggr_test_schema();
        let empty = EmptyExec::new(schema);

        // ask for the wrong partition
        assert!(empty.execute(1, task_ctx.clone()).is_err());
        assert!(empty.execute(20, task_ctx).is_err());
        Ok(())
    }
}
