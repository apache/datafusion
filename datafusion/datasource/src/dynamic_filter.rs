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

use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use datafusion_common::{
    Result, internal_datafusion_err,
    tree_node::{Transformed, TreeNode},
};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::DynamicFilterPhysicalExpr;

/// A partition-aware wrapper around [`DynamicFilterPhysicalExpr`].
///
/// This expression evaluates the dynamic filter using partition-specific
/// expressions when available.
#[derive(Debug, Hash, PartialEq, Eq)]
pub(crate) struct PartitionedDynamicFilterExpr {
    filter: Arc<DynamicFilterPhysicalExpr>,
    partition: usize,
}

impl PartitionedDynamicFilterExpr {
    pub(crate) fn new(filter: Arc<DynamicFilterPhysicalExpr>, partition: usize) -> Self {
        Self { filter, partition }
    }
}

impl std::fmt::Display for PartitionedDynamicFilterExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.filter)
    }
}

impl PhysicalExpr for PartitionedDynamicFilterExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.filter.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let updated = self.filter.clone().with_new_children(children)?;
        let any_expr: Arc<dyn Any + Send + Sync> = updated;
        let filter =
            Arc::downcast::<DynamicFilterPhysicalExpr>(any_expr).map_err(|_| {
                internal_datafusion_err!(
                    "expected DynamicFilterPhysicalExpr after remapping children"
                )
            })?;
        Ok(Arc::new(Self {
            filter,
            partition: self.partition,
        }))
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.filter
            .current_for_partition(self.partition)?
            .data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.filter
            .current_for_partition(self.partition)?
            .nullable(input_schema)
    }

    fn evaluate(
        &self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> Result<datafusion_expr::ColumnarValue> {
        let current = self.filter.current_for_partition(self.partition)?;
        #[cfg(test)]
        {
            let schema = batch.schema();
            self.nullable(&schema)?;
            self.data_type(&schema)?;
        };
        current.evaluate(batch)
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.filter.fmt_sql(f)
    }

    fn snapshot(&self) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        Ok(Some(self.filter.current_for_partition(self.partition)?))
    }

    fn snapshot_generation(&self) -> u64 {
        self.filter.snapshot_generation()
    }
}

pub(crate) fn wrap_partitioned_dynamic_filters(
    expr: Arc<dyn PhysicalExpr>,
    partition: usize,
) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
    // Fast path: dynamic filter is the root expression.
    if expr.as_any().is::<DynamicFilterPhysicalExpr>() {
        let any_expr: Arc<dyn Any + Send + Sync> = expr.clone();
        if let Ok(dynamic_filter) = Arc::downcast::<DynamicFilterPhysicalExpr>(any_expr) {
            let wrapped: Arc<dyn PhysicalExpr> =
                Arc::new(PartitionedDynamicFilterExpr::new(dynamic_filter, partition));
            return Ok(Transformed::yes(wrapped));
        }
    }

    expr.transform_up(|expr| {
        if expr.as_any().is::<DynamicFilterPhysicalExpr>() {
            let any_expr: Arc<dyn Any + Send + Sync> = expr.clone();
            if let Ok(dynamic_filter) =
                Arc::downcast::<DynamicFilterPhysicalExpr>(any_expr)
            {
                let wrapped: Arc<dyn PhysicalExpr> = Arc::new(
                    PartitionedDynamicFilterExpr::new(dynamic_filter, partition),
                );
                return Ok(Transformed::yes(wrapped));
            }
        }
        Ok(Transformed::no(expr))
    })
}
