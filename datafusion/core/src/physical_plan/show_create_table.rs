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

//! Defines the SHOW CREATE TABLE operator

use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use crate::datasource::TableProvider;
use crate::{
    error::{DataFusionError, Result},
    physical_plan::{
        common::SizedRecordBatchStream, DisplayFormatType, ExecutionPlan, Partitioning,
        Statistics,
    },
};
use arrow::{array::StringArray, datatypes::SchemaRef, record_batch::RecordBatch};
use log::debug;

use super::{expressions::PhysicalSortExpr, SendableRecordBatchStream};
use crate::execution::context::TaskContext;
use crate::physical_plan::metrics::{ExecutionPlanMetricsSet, MemTrackingMetrics};

/// `SHOW CREATE TABLE` execution plan operator.
/// This operator looks the table or view up in the
/// catalog and returns its name and create statement.
#[derive(Clone)]
pub struct ShowCreateTableExec {
    /// The name of the table or view
    pub table_name: String,
    /// The table provider
    pub provider: Arc<dyn TableProvider>,
    /// The output schema for RecordBatches of this exec node
    pub schema: SchemaRef,
}

impl Debug for ShowCreateTableExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "ShowCreateTableExec: {:?}", self.table_name)
    }
}

impl ShowCreateTableExec {
    /// Create a new ShowCreateTableExec
    pub fn new(
        table_name: String,
        provider: Arc<dyn TableProvider>,
        schema: SchemaRef,
    ) -> Self {
        ShowCreateTableExec {
            table_name,
            provider,
            schema,
        }
    }
}

impl ExecutionPlan for ShowCreateTableExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn relies_on_input_order(&self) -> bool {
        false
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
        debug!("Start ShowCreateTableExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());
        if 0 != partition {
            return Err(DataFusionError::Internal(format!(
                "ShowCreateTableExec invalid partition {}",
                partition
            )));
        }

        let name_arr = StringArray::from(vec![Some(self.table_name.as_str())]);
        let stmt = self.provider.create_statement();
        let statement_arr = StringArray::from(vec![stmt.as_deref()]);

        let record_batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![Arc::new(name_arr), Arc::new(statement_arr)],
        )?;

        let metrics = ExecutionPlanMetricsSet::new();
        let tracking_metrics = MemTrackingMetrics::new(&metrics, partition);

        debug!(
            "Before returning SizedRecordBatch in ShowCreateTableExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());

        Ok(Box::pin(SizedRecordBatchStream::new(
            self.schema.clone(),
            vec![Arc::new(record_batch)],
            tracking_metrics,
        )))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "ShowCreateTableExec: {}", self.table_name)
            }
        }
    }

    fn statistics(&self) -> Statistics {
        // Statistics inn SHOW CREATE plan are not relevant
        Statistics::default()
    }
}

#[cfg(test)]
mod tests {
    use crate::prelude::SessionContext;
    use crate::{assert_batches_eq, execution::context::SessionConfig};

    use super::*;

    #[tokio::test]
    async fn show_create_view() -> Result<()> {
        let session_ctx = SessionContext::with_config(
            SessionConfig::new().with_information_schema(true),
        );
        let table_sql = "CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)";
        session_ctx.sql(table_sql).await?.collect().await?;
        let view_sql = "CREATE VIEW xyz AS SELECT * FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let result_sql = "SHOW CREATE TABLE xyz";
        let results = session_ctx.sql(result_sql).await?.collect().await?;
        assert_eq!(results[0].num_rows(), 1);

        let expected = vec![
            "+------+--------------------------------------+",
            "| name | statement                            |",
            "+------+--------------------------------------+",
            "| xyz  | CREATE VIEW xyz AS SELECT * FROM abc |",
            "+------+--------------------------------------+",
        ];

        assert_batches_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn show_create_view_in_catalog() -> Result<()> {
        let session_ctx = SessionContext::with_config(
            SessionConfig::new().with_information_schema(true),
        );
        let table_sql = "CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)";
        session_ctx.sql(table_sql).await?.collect().await?;
        let db_sql = "CREATE SCHEMA test";
        session_ctx.sql(db_sql).await?.collect().await?;
        let view_sql = "CREATE VIEW test.xyz AS SELECT * FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let result_sql = "SHOW CREATE TABLE test.xyz";
        let results = session_ctx.sql(result_sql).await?.collect().await?;
        assert_eq!(results[0].num_rows(), 1);

        let expected = vec![
            "+----------+-------------------------------------------+",
            "| name     | statement                                 |",
            "+----------+-------------------------------------------+",
            "| test.xyz | CREATE VIEW test.xyz AS SELECT * FROM abc |",
            "+----------+-------------------------------------------+",
        ];

        assert_batches_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn explain_show_create_view() -> Result<()> {
        let session_ctx = SessionContext::with_config(
            SessionConfig::new().with_information_schema(true),
        );
        let table_sql = "CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)";
        session_ctx.sql(table_sql).await?.collect().await?;

        let view_sql = "CREATE VIEW xyz AS SELECT * FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let result_sql = "EXPLAIN SHOW CREATE TABLE xyz";
        let results = session_ctx.sql(result_sql).await?.collect().await?;

        let expected = vec![
            "+---------------+--------------------------+",
            "| plan_type     | plan                     |",
            "+---------------+--------------------------+",
            "| logical_plan  | ShowCreateTable: \"xyz\"   |",
            "| physical_plan | ShowCreateTableExec: xyz |",
            "|               |                          |",
            "+---------------+--------------------------+",
        ];

        assert_batches_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn show_create_table() -> Result<()> {
        let session_ctx = SessionContext::with_config(
            SessionConfig::new().with_information_schema(true),
        );
        let table_sql = "CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)";
        session_ctx.sql(table_sql).await?.collect().await?;

        let result_sql = "SHOW CREATE TABLE abc";
        let results = session_ctx.sql(result_sql).await?.collect().await?;

        let expected = vec![
            "+------+-----------+",
            "| name | statement |",
            "+------+-----------+",
            "| abc  |           |",
            "+------+-----------+",
        ];

        assert_batches_eq!(expected, &results);

        Ok(())
    }
}
