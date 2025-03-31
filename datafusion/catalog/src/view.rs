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

use std::{any::Any, borrow::Cow, sync::Arc};

use crate::Session;
use crate::TableProvider;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion_common::error::Result;
use datafusion_common::Column;
use datafusion_expr::TableType;
use datafusion_expr::{Expr, LogicalPlan};
use datafusion_expr::{LogicalPlanBuilder, TableProviderFilterPushDown};
use datafusion_physical_plan::ExecutionPlan;

/// An implementation of `TableProvider` that uses another logical plan.
#[derive(Debug)]
pub struct ViewTable {
    /// LogicalPlan of the view
    logical_plan: LogicalPlan,
    /// File fields + partition columns
    table_schema: SchemaRef,
    /// SQL used to create the view, if available
    definition: Option<String>,
}

impl ViewTable {
    /// Create new view that is executed at query runtime.
    ///
    /// Takes a `LogicalPlan` and optionally the SQL text of the `CREATE`
    /// statement.
    ///
    /// Notes: the `LogicalPlan` is not validated or type coerced. If this is
    /// needed it should be done after calling this function.
    pub fn new(logical_plan: LogicalPlan, definition: Option<String>) -> Self {
        let table_schema = logical_plan.schema().as_ref().to_owned().into();
        Self {
            logical_plan,
            table_schema,
            definition,
        }
    }

    #[deprecated(
        since = "47.0.0",
        note = "Use `ViewTable::new` instead and apply TypeCoercion to the logical plan if needed"
    )]
    pub fn try_new(
        logical_plan: LogicalPlan,
        definition: Option<String>,
    ) -> Result<Self> {
        Ok(Self::new(logical_plan, definition))
    }

    /// Get definition ref
    pub fn definition(&self) -> Option<&String> {
        self.definition.as_ref()
    }

    /// Get logical_plan ref
    pub fn logical_plan(&self) -> &LogicalPlan {
        &self.logical_plan
    }
}

#[async_trait]
impl TableProvider for ViewTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_logical_plan(&self) -> Option<Cow<LogicalPlan>> {
        Some(Cow::Borrowed(&self.logical_plan))
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.definition.as_deref()
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // A filter is added on the View when given
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filter = filters.iter().cloned().reduce(|acc, new| acc.and(new));
        let plan = self.logical_plan().clone();
        let mut plan = LogicalPlanBuilder::from(plan);

        if let Some(filter) = filter {
            plan = plan.filter(filter)?;
        }

        let mut plan = if let Some(projection) = projection {
            // avoiding adding a redundant projection (e.g. SELECT * FROM view)
            let current_projection =
                (0..plan.schema().fields().len()).collect::<Vec<usize>>();
            if projection == &current_projection {
                plan
            } else {
                let fields: Vec<Expr> = projection
                    .iter()
                    .map(|i| {
                        Expr::Column(Column::from(
                            self.logical_plan.schema().qualified_field(*i),
                        ))
                    })
                    .collect();
                plan.project(fields)?
            }
        } else {
            plan
        };

        if let Some(limit) = limit {
            plan = plan.limit(0, Some(limit))?;
        }

        state.create_physical_plan(&plan.build()?).await
    }
}
