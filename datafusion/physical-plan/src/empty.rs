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

use arrow::array::{ArrayRef, NullArray};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_execution::TaskContext;

use log::trace;

/// Execution plan for empty relation (produces no rows)
#[derive(Debug)]
pub struct EmptyExec {
    /// Specifies whether this exec produces a row or not
    produce_one_row: bool,
    /// The schema for the produced row
    schema: SchemaRef,
    /// Number of partitions
    partitions: usize,
}

impl EmptyExec {
    /// Create a new EmptyExec
    pub fn new(produce_one_row: bool, schema: SchemaRef) -> Self {
        EmptyExec {
            produce_one_row,
            schema,
            partitions: 1,
        }
    }

    /// Create a new EmptyExec with specified partition number
    pub fn with_partitions(mut self, partitions: usize) -> Self {
        self.partitions = partitions;
        self
    }

    /// Specifies whether this exec produces a row or not
    pub fn produce_one_row(&self) -> bool {
        self.produce_one_row
    }

    fn data(&self) -> Result<Vec<RecordBatch>> {
        let batch = if self.produce_one_row {
            let n_field = self.schema.fields.len();
            // hack for https://github.com/apache/arrow-datafusion/pull/3242
            let n_field = if n_field == 0 { 1 } else { n_field };
            vec![RecordBatch::try_new(
                Arc::new(Schema::new(
                    (0..n_field)
                        .map(|i| {
                            Field::new(format!("placeholder_{i}"), DataType::Null, true)
                        })
                        .collect::<Fields>(),
                )),
                (0..n_field)
                    .map(|_i| {
                        let ret: ArrayRef = Arc::new(NullArray::new(1));
                        ret
                    })
                    .collect(),
            )?]
        } else {
            vec![]
        };

        Ok(batch)
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
                write!(f, "EmptyExec: produce_one_row={}", self.produce_one_row)
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
        Ok(Arc::new(EmptyExec::new(
            self.produce_one_row,
            self.schema.clone(),
        )))
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

        let empty = EmptyExec::new(false, schema.clone());
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
        let empty = Arc::new(EmptyExec::new(false, schema.clone()));
        let empty_with_row = Arc::new(EmptyExec::new(true, schema));

        let empty2 = with_new_children_if_necessary(empty.clone(), vec![])?.into();
        assert_eq!(empty.schema(), empty2.schema());

        let empty_with_row_2 =
            with_new_children_if_necessary(empty_with_row.clone(), vec![])?.into();
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
        let task_ctx = Arc::new(TaskContext::default());
        let schema = test::aggr_test_schema();
        let empty = EmptyExec::new(false, schema);

        // ask for the wrong partition
        assert!(empty.execute(1, task_ctx.clone()).is_err());
        assert!(empty.execute(20, task_ctx).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn produce_one_row() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = test::aggr_test_schema();
        let empty = EmptyExec::new(true, schema);

        let iter = empty.execute(0, task_ctx)?;
        let batches = common::collect(iter).await?;

        // should have one item
        assert_eq!(batches.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn produce_one_row_multiple_partition() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = test::aggr_test_schema();
        let partitions = 3;
        let empty = EmptyExec::new(true, schema).with_partitions(partitions);

        for n in 0..partitions {
            let iter = empty.execute(n, task_ctx.clone())?;
            let batches = common::collect(iter).await?;

            // should have one item
            assert_eq!(batches.len(), 1);
        }

        Ok(())
    }
}
