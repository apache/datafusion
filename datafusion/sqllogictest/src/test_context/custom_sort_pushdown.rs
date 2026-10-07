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

use std::fmt::{self, Formatter};
use std::sync::Arc;

use arrow::array::record_batch;
use arrow::datatypes::{self as arrow_schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{col, negative};
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::test::TestMemoryExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use datafusion::prelude::SessionContext;

/// Register a fixed custom operator that negates its input column.
pub(super) fn register_custom_sort_pushdown(ctx: &SessionContext) -> Result<()> {
    let batch = record_batch!(("a", Int32, [2, 1, 3]))?;
    let schema = batch.schema();
    let input = TestMemoryExec::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)?;
    let projection = ProjectionExec::try_new(
        [(negative(col("a", &schema)?, &schema)?, "a".to_string())],
        input,
    )?;
    ctx.register_table(
        "custom_negated_projection",
        Arc::new(CustomProjection(projection)),
    )?;
    Ok(())
}

/// Negates values while preserving row positions. Keeping a distinct plan type
/// exercises the generic custom-plan sort pushdown path.
#[derive(Debug, Clone)]
struct CustomProjection(ProjectionExec);

#[async_trait]
impl TableProvider for CustomProjection {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _: &dyn Session,
        projection: Option<&[usize]>,
        _: &[Expr],
        _: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan: Arc<dyn ExecutionPlan> = Arc::new(self.clone());
        if let Some(projection) = projection {
            let expressions = ProjectionExprs::from_indices(projection, &self.0.schema());
            Ok(Arc::new(ProjectionExec::try_new(
                expressions.iter().cloned(),
                plan,
            )?))
        } else {
            Ok(plan)
        }
    }
}

impl DisplayAs for CustomProjection {
    fn fmt_as(&self, display_type: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "Custom")?;
        self.0.fmt_as(display_type, f)
    }
}

impl ExecutionPlan for CustomProjection {
    fn name(&self) -> &str {
        "CustomProjectionExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.0.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.0.children()
    }

    // Row positions are preserved; projected values have the opposite ordering.
    fn maintains_input_order(&self) -> Vec<bool> {
        self.0.maintains_input_order()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self(ProjectionExec::try_new(
            self.0.expr().to_vec(),
            Arc::clone(&children[0]),
        )?)))
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.0.apply_expressions(f)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.0.execute(partition, context)
    }
}
