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
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::test::TestMemoryExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SortOrderPushdownResult,
};
use datafusion::prelude::SessionContext;

pub(super) fn register_exact_sort_source(ctx: &SessionContext) -> Result<()> {
    let partitions = (0..4)
        .map(|i| {
            Ok(vec![record_batch!(
                ("group_id", Int32, [0, 0, 1, 1]),
                ("value", Int32, [i, i + 4, i + 8, i + 12])
            )?])
        })
        .collect::<Result<Vec<_>>>()?;
    let input = TestMemoryExec::try_new(&partitions, partitions[0][0].schema(), None)?;
    ctx.register_table("exact_sort_source", Arc::new(ExactSortSource(input)))?;
    Ok(())
}

fn ordering() -> LexOrdering {
    [
        PhysicalSortExpr::new_default(Arc::new(Column::new("group_id", 0))).nulls_last(),
        PhysicalSortExpr::new_default(Arc::new(Column::new("value", 1))).nulls_last(),
    ]
    .into()
}

/// Accepts exact sort pushdown without supporting fetch pushdown. Its rows are
/// already sorted within each partition; the hook makes that ordering explicit.
#[derive(Debug, Clone)]
struct ExactSortSource(TestMemoryExec);

#[async_trait]
impl TableProvider for ExactSortSource {
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
        assert!(projection.is_none_or(|indices| indices == [0, 1]));
        Ok(Arc::new(self.clone()))
    }
}

impl DisplayAs for ExactSortSource {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "ExactSortSource")
    }
}

impl ExecutionPlan for ExactSortSource {
    fn name(&self) -> &str {
        "ExactSortSource"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.0.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert!(children.is_empty());
        Ok(self)
    }

    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.0.execute(partition, context)
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        if order != ordering().as_ref() {
            return Ok(SortOrderPushdownResult::Unsupported);
        }
        Ok(SortOrderPushdownResult::Exact {
            inner: Arc::new(Self(
                self.0.clone().try_with_sort_information(vec![ordering()])?,
            )),
        })
    }
}
