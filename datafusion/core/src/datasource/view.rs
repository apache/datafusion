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

//! View data source which uses a LogicalPlan as it's input.

use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;

use crate::{
    error::Result,
    execution::context::SessionContext,
    logical_plan::{Expr, LogicalPlan},
    physical_plan::ExecutionPlan,
};

use crate::datasource::{TableProvider, TableType};

/// An implementation of `TableProvider` that uses another logical plan.
pub struct ViewTable {
    /// To create ExecutionPlan
    context: SessionContext,
    /// LogicalPlan of the view
    logical_plan: LogicalPlan,
    /// File fields + partition columns
    table_schema: SchemaRef,
}

impl ViewTable {
    /// Create new view that is executed at query runtime.
    /// Takes a `LogicalPlan` as input.
    pub fn try_new(context: SessionContext, logical_plan: LogicalPlan) -> Result<Self> {
        let table_schema = logical_plan.schema().as_ref().to_owned().into();

        let view = Self {
            context,
            logical_plan,
            table_schema,
        };

        Ok(view)
    }
}

#[async_trait]
impl TableProvider for ViewTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.context.create_physical_plan(&self.logical_plan).await
    }
}

#[cfg(test)]
mod tests {
    use crate::{assert_batches_eq, execution::context::SessionConfig};

    use super::*;

    #[tokio::test]
    async fn query_view() -> Result<()> {
        let session_ctx = SessionContext::with_config(
            SessionConfig::new().with_information_schema(true),
        );

        session_ctx
            .sql("CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)")
            .await?
            .collect()
            .await?;

        let view_sql = "CREATE VIEW xyz AS SELECT * FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let results = session_ctx.sql("SELECT * FROM information_schema.tables WHERE table_type='VIEW' AND table_name = 'xyz'").await?.collect().await?;
        assert_eq!(results[0].num_rows(), 1);

        let results = session_ctx
            .sql("SELECT * FROM xyz")
            .await?
            .collect()
            .await?;

        let expected = vec![
            "+---------+---------+---------+",
            "| column1 | column2 | column3 |",
            "+---------+---------+---------+",
            "| 1       | 2       | 3       |",
            "| 4       | 5       | 6       |",
            "+---------+---------+---------+",
        ];

        assert_batches_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn query_view_with_projection() -> Result<()> {
        let session_ctx = SessionContext::with_config(
            SessionConfig::new().with_information_schema(true),
        );

        session_ctx
            .sql("CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)")
            .await?
            .collect()
            .await?;

        let view_sql = "CREATE VIEW xyz AS SELECT column1, column2 FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let results = session_ctx.sql("SELECT * FROM information_schema.tables WHERE table_type='VIEW' AND table_name = 'xyz'").await?.collect().await?;
        assert_eq!(results[0].num_rows(), 1);

        let results = session_ctx
            .sql("SELECT column1 FROM xyz")
            .await?
            .collect()
            .await?;

        let expected = vec![
            "+---------+",
            "| column1 |",
            "+---------+",
            "| 1       |",
            "| 4       |",
            "+---------+",
        ];

        assert_batches_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn query_view_with_filter() -> Result<()> {
        let session_ctx = SessionContext::with_config(
            SessionConfig::new().with_information_schema(true),
        );

        session_ctx
            .sql("CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)")
            .await?
            .collect()
            .await?;

        let view_sql = "CREATE VIEW xyz AS SELECT column1, column2 FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let results = session_ctx.sql("SELECT * FROM information_schema.tables WHERE table_type='VIEW' AND table_name = 'xyz'").await?.collect().await?;
        assert_eq!(results[0].num_rows(), 1);

        let results = session_ctx
            .sql("SELECT column1 FROM xyz WHERE column2 = 5")
            .await?
            .collect()
            .await?;

        let expected = vec![
            "+---------+",
            "| column1 |",
            "+---------+",
            "| 4       |",
            "+---------+",
        ];

        assert_batches_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn query_join_views() -> Result<()> {
        let session_ctx = SessionContext::with_config(
            SessionConfig::new().with_information_schema(true),
        );

        session_ctx
            .sql("CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)")
            .await?
            .collect()
            .await?;

        let view_sql = "CREATE VIEW xyz AS SELECT column1, column2 FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let view_sql = "CREATE VIEW lmn AS SELECT column1, column3 FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let results = session_ctx.sql("SELECT * FROM information_schema.tables WHERE table_type='VIEW' AND (table_name = 'xyz' OR table_name = 'lmn')").await?.collect().await?;
        assert_eq!(results[0].num_rows(), 2);

        let results = session_ctx
            .sql("SELECT * FROM xyz JOIN lmn USING (column1) ORDER BY column2")
            .await?
            .collect()
            .await?;

        let expected = vec![
            "+---------+---------+---------+",
            "| column2 | column1 | column3 |",
            "+---------+---------+---------+",
            "| 2       | 1       | 3       |",
            "| 5       | 4       | 6       |",
            "+---------+---------+---------+",
        ];

        assert_batches_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn create_view_plan() -> Result<()> {
        let session_ctx = SessionContext::with_config(
            SessionConfig::new().with_information_schema(true),
        );

        session_ctx
            .sql("CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)")
            .await?
            .collect()
            .await?;

        let view_sql = "CREATE VIEW xyz AS SELECT * FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let results = session_ctx
            .sql("EXPLAIN CREATE VIEW xyz AS SELECT * FROM abc")
            .await?
            .collect()
            .await?;

        let expected = vec![
            "+---------------+--------------------------------------------------------+",
            "| plan_type     | plan                                                   |",
            "+---------------+--------------------------------------------------------+",
            "| logical_plan  | CreateView: \"xyz\"                                      |",
            "|               |   Projection: #abc.column1, #abc.column2, #abc.column3 |",
            "|               |     TableScan: abc projection=Some([0, 1, 2])          |",
            "| physical_plan | EmptyExec: produce_one_row=false                       |",
            "|               |                                                        |",
            "+---------------+--------------------------------------------------------+",
        ];

        assert_batches_eq!(expected, &results);

        let results = session_ctx
            .sql("EXPLAIN CREATE VIEW xyz AS SELECT * FROM abc WHERE column2 = 5")
            .await?
            .collect()
            .await?;

        let expected = vec![
            "+---------------+--------------------------------------------------------+",
            "| plan_type     | plan                                                   |",
            "+---------------+--------------------------------------------------------+",
            "| logical_plan  | CreateView: \"xyz\"                                      |",
            "|               |   Projection: #abc.column1, #abc.column2, #abc.column3 |",
            "|               |     Filter: #abc.column2 = Int64(5)                    |",
            "|               |       TableScan: abc projection=Some([0, 1, 2])        |",
            "| physical_plan | EmptyExec: produce_one_row=false                       |",
            "|               |                                                        |",
            "+---------------+--------------------------------------------------------+",
        ];

        assert_batches_eq!(expected, &results);

        let results = session_ctx
            .sql("EXPLAIN CREATE VIEW xyz AS SELECT column1, column2 FROM abc WHERE column2 = 5")
            .await?
            .collect()
            .await?;

        let expected = vec![
            "+---------------+----------------------------------------------+",
            "| plan_type     | plan                                         |",
            "+---------------+----------------------------------------------+",
            "| logical_plan  | CreateView: \"xyz\"                            |",
            "|               |   Projection: #abc.column1, #abc.column2     |",
            "|               |     Filter: #abc.column2 = Int64(5)          |",
            "|               |       TableScan: abc projection=Some([0, 1]) |",
            "| physical_plan | EmptyExec: produce_one_row=false             |",
            "|               |                                              |",
            "+---------------+----------------------------------------------+",
        ];

        assert_batches_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn create_or_replace_view() -> Result<()> {
        let session_ctx = SessionContext::with_config(
            SessionConfig::new().with_information_schema(true),
        );

        session_ctx
            .sql("CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)")
            .await?
            .collect()
            .await?;

        let view_sql = "CREATE VIEW xyz AS SELECT * FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let view_sql = "CREATE OR REPLACE VIEW xyz AS SELECT column1 FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let results = session_ctx.sql("SELECT * FROM information_schema.tables WHERE table_type='VIEW' AND table_name = 'xyz'").await?.collect().await?;
        assert_eq!(results[0].num_rows(), 1);

        let results = session_ctx
            .sql("SELECT * FROM xyz")
            .await?
            .collect()
            .await?;

        let expected = vec![
            "+---------+",
            "| column1 |",
            "+---------+",
            "| 1       |",
            "| 4       |",
            "+---------+",
        ];

        assert_batches_eq!(expected, &results);

        Ok(())
    }
}
