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
use datafusion::catalog::{Session, TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DFSchema, Result, ScalarValue, plan_err};
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::test::TestMemoryExec;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    ReplaceChildrenOptions,
};
use datafusion::prelude::SessionContext;

/// Expose custom operators through SQL without changing the optimizer rules.
pub(super) fn register_custom_sort_pushdown(ctx: &SessionContext) {
    ctx.register_udtf("custom_projection", Arc::new(CustomProjectionFunction));
}

#[derive(Debug)]
struct CustomProjectionFunction;

impl TableFunctionImpl for CustomProjectionFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let first = record_batch!(("a", Int32, [2, 1]), ("b", Int32, [30, 10]))?;
        let second = record_batch!(("a", Int32, [3]), ("b", Int32, [20]))?;
        let schema = first.schema();
        let source = TestMemoryExec::try_new_exec(
            &[vec![first], vec![second]],
            Arc::clone(&schema),
            None,
        )?;
        let df_schema = DFSchema::try_from(schema)?;
        let state = args
            .session()
            .as_any()
            .downcast_ref::<SessionState>()
            .expect("sqllogictests use SessionState");
        // Parse the expressions here so they execute inside the custom plan,
        // rather than in SQL's built-in ProjectionExec.
        let expressions = args
            .exprs()
            .iter()
            .map(|arg| {
                let Expr::Literal(ScalarValue::Utf8(Some(sql)), _) = arg else {
                    return plan_err!("custom_projection expects SQL expression strings");
                };
                let expr = state.create_logical_expr(sql, &df_schema)?;
                let name = expr.schema_name().to_string();
                Ok((state.create_physical_expr(expr, &df_schema)?, name))
            })
            .collect::<Result<Vec<_>>>()?;
        let custom = CustomProjection(ProjectionExec::try_new(expressions, source)?);
        Ok(Arc::new(CustomTable(Arc::new(custom))))
    }
}

#[derive(Debug)]
struct CustomTable(Arc<dyn ExecutionPlan>);

#[async_trait]
impl TableProvider for CustomTable {
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
        if let Some(projection) = projection {
            let schema = self.schema();
            let expressions = projection.iter().map(|&index| {
                let name = schema.field(index).name();
                (
                    Arc::new(Column::new(name, index)) as Arc<dyn PhysicalExpr>,
                    name.clone(),
                )
            });
            Ok(Arc::new(ProjectionExec::try_new(
                expressions,
                Arc::clone(&self.0),
            )?))
        } else {
            Ok(Arc::clone(&self.0))
        }
    }
}

/// A projection with the default custom-plan sort pushdown behavior. Row order
/// is preserved, while column positions and values can change.
#[derive(Debug)]
struct CustomProjection(ProjectionExec);

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

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self(ProjectionExec::try_new(
            self.0.expr().to_vec(),
            Arc::clone(&children[0]),
        )?)))
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
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
