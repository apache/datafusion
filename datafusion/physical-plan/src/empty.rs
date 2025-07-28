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

//! EmptyRelation with produce_one_row=false execution plan

use std::any::Any;
use std::sync::Arc;

use crate::memory::MemoryStream;
use crate::{
    execution_plan::{Boundedness, EmissionType},
    DisplayFormatType, ExecutionPlan, Partitioning,
};
use crate::{DisplayAs, PlanProperties, SendableRecordBatchStream, Statistics};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{internal_err, Result};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::EquivalenceProperties;

use crate::execution_plan::SchedulingType;
use datafusion_common::stats::Precision;
use log::trace;

/// Execution plan for empty relation with produce_one_row=false
#[derive(Debug, Clone)]
pub struct EmptyExec {
    /// The schema for the produced row
    schema: SchemaRef,
    /// Number of partitions
    partitions: usize,
    cache: PlanProperties,
}

impl EmptyExec {
    /// Create a new EmptyExec
    pub fn new(schema: SchemaRef) -> Self {
        let cache = Self::compute_properties(Arc::clone(&schema), 1);
        EmptyExec {
            schema,
            partitions: 1,
            cache,
        }
    }

    /// Create a new EmptyExec with specified partition number
    pub fn with_partitions(mut self, partitions: usize) -> Self {
        self.partitions = partitions;
        // Changing partitions may invalidate output partitioning, so update it:
        let output_partitioning = Self::output_partitioning_helper(self.partitions);
        self.cache = self.cache.with_partitioning(output_partitioning);
        self
    }

    fn data(&self) -> Result<Vec<RecordBatch>> {
        Ok(vec![])
    }

    fn output_partitioning_helper(n_partitions: usize) -> Partitioning {
        Partitioning::UnknownPartitioning(n_partitions)
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(schema: SchemaRef, n_partitions: usize) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Self::output_partitioning_helper(n_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
        .with_scheduling_type(SchedulingType::Cooperative)
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
            DisplayFormatType::TreeRender => {
                // TODO: collect info
                write!(f, "")
            }
        }
    }
}

impl ExecutionPlan for EmptyExec {
    fn name(&self) -> &'static str {
        "EmptyExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
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
            Arc::clone(&self.schema),
            None,
        )?))
    }

    fn statistics(&self) -> Result<Statistics> {
        self.partition_statistics(None)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        if let Some(partition) = partition {
            if partition >= self.partitions {
                return internal_err!(
                    "EmptyExec invalid partition {} (expected less than {})",
                    partition,
                    self.partitions
                );
            }
        }
        // Build explicit stats: exact zero rows and bytes, with unknown columns
        let mut stats = Statistics::default()
            .with_num_rows(Precision::Exact(0))
            .with_total_byte_size(Precision::Exact(0));

        // Add unknown column stats for each field in schema
        for _field in self.schema.fields() {
            stats = stats.add_column_statistics(
                datafusion_common::stats::ColumnStatistics::new_unknown(),
            );
        }

        Ok(stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::with_new_children_if_necessary;
    use crate::{common, test};
    use arrow_schema::Schema;

    #[tokio::test]
    async fn empty() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = test::aggr_test_schema();

        let empty = EmptyExec::new(Arc::clone(&schema));
        assert_eq!(empty.schema(), schema);

        // We should have no results
        let iter = empty.execute(0, task_ctx)?;
        let batches = common::collect(iter).await?;
        assert!(batches.is_empty());

        Ok(())
    }

    #[test]
    fn with_new_children() -> Result<()> {
        let schema = test::aggr_test_schema();
        let empty = Arc::new(EmptyExec::new(Arc::clone(&schema)));

        let empty2 = with_new_children_if_necessary(
            Arc::clone(&empty) as Arc<dyn ExecutionPlan>,
            vec![],
        )?;
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
        assert!(empty.execute(1, Arc::clone(&task_ctx)).is_err());
        assert!(empty.execute(20, task_ctx).is_err());
        Ok(())
    }

    #[test]
    fn empty_partition_statistics_explicit_zero() -> Result<()> {
        let schema: Arc<Schema> = Arc::new(Schema::empty());
        let exec1: EmptyExec = EmptyExec::new(schema.clone());
        // default partition = 1

        // global stats
        let stats_all_1: Statistics = exec1.partition_statistics(None)?;
        assert_eq!(stats_all_1.num_rows, Precision::Exact(0));
        assert_eq!(stats_all_1.total_byte_size, Precision::Exact(0));
        assert_eq!(stats_all_1.column_statistics.len(), schema.fields().len());

        // partition 0
        let stats0_1: Statistics = exec1.partition_statistics(Some(0))?;
        assert_eq!(stats0_1.num_rows, Precision::Exact(0));
        assert_eq!(stats0_1.total_byte_size, Precision::Exact(0));
        assert_eq!(stats0_1.column_statistics.len(), schema.fields().len());

        // invalid partition for default
        assert!(exec1.partition_statistics(Some(1)).is_err());

        // Now with 2 partitions
        let exec2: EmptyExec = EmptyExec::new(schema.clone()).with_partitions(2);

        // valid partitions 0 and 1
        for part in 0..2 {
            let stats = exec2.partition_statistics(Some(part))?;
            assert_eq!(stats.num_rows, Precision::Exact(0));
            assert_eq!(stats.total_byte_size, Precision::Exact(0));
            assert_eq!(stats.column_statistics.len(), schema.fields().len());
        }

        // invalid partition 2
        assert!(exec2.partition_statistics(Some(2)).is_err());
        Ok(())
    }
}
