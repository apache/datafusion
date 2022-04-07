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

use crate::error::{DataFusionError, Result};
use crate::physical_plan::{
    memory::MemoryStream, DisplayFormatType, Distribution, ExecutionPlan, Partitioning,
};
use arrow::array::NullArray;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use super::expressions::PhysicalSortExpr;
use super::{common, SendableRecordBatchStream, Statistics};

use crate::execution::context::TaskContext;
use async_trait::async_trait;

/// Execution plan for empty relation (produces no rows)
#[derive(Debug)]
pub struct EmptyExec {
    /// Specifies whether this exec produces a row or not
    produce_one_row: bool,
    /// The schema for the produced row
    schema: SchemaRef,
}

impl EmptyExec {
    /// Create a new EmptyExec
    pub fn new(produce_one_row: bool, schema: SchemaRef) -> Self {
        EmptyExec {
            produce_one_row,
            schema,
        }
    }

    /// Specifies whether this exec produces a row or not
    pub fn produce_one_row(&self) -> bool {
        self.produce_one_row
    }

    fn data(&self) -> Result<Vec<RecordBatch>> {
        let batch = if self.produce_one_row {
            vec![RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "placeholder",
                    DataType::Null,
                    true,
                )])),
                vec![Arc::new(NullArray::new(1))],
            )?]
        } else {
            vec![]
        };

        Ok(batch)
    }
}

#[async_trait]
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

    fn required_child_distribution(&self) -> Distribution {
        Distribution::UnspecifiedDistribution
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(EmptyExec::new(
            self.produce_one_row,
            self.schema.clone(),
        )))
    }

    async fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // GlobalLimitExec has a single output partition
        if 0 != partition {
            return Err(DataFusionError::Internal(format!(
                "EmptyExec invalid partition {} (expected 0)",
                partition
            )));
        }

        Ok(Box::pin(MemoryStream::try_new(
            self.data()?,
            self.schema.clone(),
            None,
        )?))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "EmptyExec: produce_one_row={}", self.produce_one_row)
            }
        }
    }

    fn statistics(&self) -> Statistics {
        let batch = self
            .data()
            .expect("Create empty RecordBatch should not fail");
        common::compute_record_batch_statistics(&[batch], &self.schema, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::with_new_children_if_necessary;
    use crate::prelude::SessionContext;
    use crate::{physical_plan::common, test_util};

    #[tokio::test]
    async fn empty() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let schema = test_util::aggr_test_schema();

        let empty = EmptyExec::new(false, schema.clone());
        assert_eq!(empty.schema(), schema);

        // we should have no results
        let iter = empty.execute(0, task_ctx).await?;
        let batches = common::collect(iter).await?;
        assert!(batches.is_empty());

        Ok(())
    }

    #[test]
    fn with_new_children() -> Result<()> {
        let schema = test_util::aggr_test_schema();
        let empty = Arc::new(EmptyExec::new(false, schema.clone()));
        let empty_with_row = Arc::new(EmptyExec::new(true, schema));

        let empty2 = with_new_children_if_necessary(empty.clone(), vec![])?;
        assert_eq!(empty.schema(), empty2.schema());

        let empty_with_row_2 =
            with_new_children_if_necessary(empty_with_row.clone(), vec![])?;
        assert_eq!(empty_with_row.schema(), empty_with_row_2.schema());

        let too_many_kids = vec![empty2];
        assert!(
            with_new_children_if_necessary(empty, too_many_kids).is_err(),
            "expected error when providing list of kids"
        );
        Ok(())
    }

    #[tokio::test]
    async fn invalid_execute() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let schema = test_util::aggr_test_schema();
        let empty = EmptyExec::new(false, schema);

        // ask for the wrong partition
        assert!(empty.execute(1, task_ctx.clone()).await.is_err());
        assert!(empty.execute(20, task_ctx).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn produce_one_row() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let schema = test_util::aggr_test_schema();
        let empty = EmptyExec::new(true, schema);

        let iter = empty.execute(0, task_ctx).await?;
        let batches = common::collect(iter).await?;

        // should have one item
        assert_eq!(batches.len(), 1);

        Ok(())
    }
}
