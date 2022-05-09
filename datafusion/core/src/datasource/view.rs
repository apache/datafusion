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

//! The table implementation.

use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;

use crate::{
    error::Result,
    execution::context::SessionContext,
    logical_plan::{Expr, LogicalPlan},
    physical_plan::ExecutionPlan,
};

use crate::datasource::{
    datasource::TableProviderFilterPushDown, TableProvider, TableType,
};

/// An implementation of `TableProvider` that uses the object store
/// or file system listing capability to get the list of files.
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
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.context.create_physical_plan(&self.logical_plan).await
    }

    fn supports_filter_pushdown(
        &self,
        filter: &Expr,
    ) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Unsupported)
    }
}

#[cfg(test)]
mod tests {
    use crate::assert_batches_eq;

    use super::*;

    #[tokio::test]
    async fn query_view() -> Result<()> {
        let session_ctx = SessionContext::new();

        session_ctx
            .sql("CREATE TABLE abc AS VALUES (1,2,3)")
            .await?
            .collect()
            .await?;

        let view_sql = "CREATE VIEW xyz AS SELECT * FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let results = session_ctx
            .sql("SELECT * FROM abc")
            .await?
            .collect()
            .await?;

        let expected = vec![
            "+---------+---------+---------+",
            "| column1 | column2 | column3 |",
            "+---------+---------+---------+",
            "| 1       | 2       | 3       |",
            "+---------+---------+---------+",
        ];

        assert_batches_eq!(expected, &results);

        Ok(())
    }
}
