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

use std::{any::Any, hash::Hash, sync::Arc};

use datafusion_common::{
    tree_node::{Transformed, TransformedResult, TreeNode},
    Result,
};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::{expressions::lit, utils::conjunction, PhysicalExpr};

/// A source of dynamic runtime filters.
///
/// During query execution, operators implementing this trait can provide
/// filter expressions that other operators can use to dynamically prune data.
///
/// See `TopKDynamicFilterSource` in datafusion/physical-plan/src/topk/mod.rs for examples.
pub trait DynamicFilterSource: Send + Sync + std::fmt::Debug + 'static {
    /// Take a snapshot of the current state of filtering, returning a non-dynamic PhysicalExpr.
    /// This is used to e.g. serialize dynamic filters across the wire or to pass them into systems
    /// that won't use the `PhysicalExpr` API (e.g. matching on the concrete types of the expressions like `PruningPredicate` does).
    /// For example, it is expected that this returns a relatively simple expression such as `col1 > 5` for a TopK operator or
    /// `col2 IN (1, 2, ... N)` for a HashJoin operator.
    fn snapshot_current_filters(&self) -> Result<Vec<Arc<dyn PhysicalExpr>>>;

    /// Transform this dynamic filter source into a PhysicalExpr.
    /// Unlike the `snapshot_current_filters` method, this method is expected to return a
    /// PhysicalExpr that maintains a reference to the dynamic filter source and updates
    /// itself for every batch it is evaluated on.
    /// Often this will be a [`DynamicFilterPhysicalExpr`] that wraps the dynamic filter source,
    /// but it can also be a different expression that is more efficient to evaluate.
    /// This is used for systems that can evalaute a PhysicalExpr directly and want to optimize
    /// dynamic filter pushdown / effectiveness.
    /// For example, this may return:
    /// - A reference to a TopK's heap to filter out elements that won't replace the heap.
    /// - A bloom filter tied to a HashJoin to filter out elements that definitely won't match.
    fn as_dynamic_physical_expr(&self) -> Arc<dyn PhysicalExpr>;
}

#[derive(Debug)]
pub struct DynamicFilterPhysicalExpr {
    /// The children of this expression.
    /// In particular, it is important that if the dynamic expression will reference any columns
    /// those columns be marked as children of this expression so that the expression can be properly
    /// bound to the schema.
    children: Vec<Arc<dyn PhysicalExpr>>,
    /// Remapped children, if `PhysicalExpr::with_new_children` was called.
    /// This is used to ensure that the children of the expression are always the same
    /// as the children of the dynamic filter source.
    remapped_children: Option<Vec<Arc<dyn PhysicalExpr>>>,
    /// The source of dynamic filters.
    inner: Arc<dyn DynamicFilterSource>,
}

impl std::fmt::Display for DynamicFilterPhysicalExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DynamicFilterPhysicalExpr")
    }
}

// Manually derive PartialEq and Hash to work around https://github.com/rust-lang/rust/issues/78808
impl PartialEq for DynamicFilterPhysicalExpr {
    fn eq(&self, other: &Self) -> bool {
        self.current().eq(&other.current())
    }
}

impl Eq for DynamicFilterPhysicalExpr {}

impl Hash for DynamicFilterPhysicalExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.current().hash(state)
    }
}

impl DynamicFilterPhysicalExpr {
    pub fn new(
        children: Vec<Arc<dyn PhysicalExpr>>,
        inner: Arc<dyn DynamicFilterSource>,
    ) -> Self {
        Self {
            children,
            remapped_children: None,
            inner,
        }
    }

    pub fn snapshot_current_filters(&self) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
        self.inner.snapshot_current_filters()
    }

    pub fn current(&self) -> Arc<dyn PhysicalExpr> {
        let current = if let Ok(current) = self.inner.snapshot_current_filters() {
            conjunction(current)
        } else {
            lit(false)
        };
        if let Some(remapped_children) = &self.remapped_children {
            // Remap children to the current children
            // of the expression.
            current
                .transform_up(|expr| {
                    // Check if this is any of our original children
                    if let Some(pos) = self
                        .children
                        .iter()
                        .position(|c| c.as_ref() == expr.as_ref())
                    {
                        // If so, remap it to the current children
                        // of the expression.
                        let new_child = remapped_children[pos].clone();
                        Ok(Transformed::yes(new_child))
                    } else {
                        // Otherwise, just return the expression
                        Ok(Transformed::no(expr))
                    }
                })
                .data()
                .expect("transformation is infallible")
        } else {
            current
        }
    }
}

impl PhysicalExpr for DynamicFilterPhysicalExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.remapped_children
            .as_ref()
            .unwrap_or(&self.children)
            .iter()
            .collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(DynamicFilterPhysicalExpr {
            children: self.children.clone(),
            remapped_children: Some(children),
            inner: self.inner.clone(),
        }))
    }

    fn data_type(
        &self,
        input_schema: &arrow::datatypes::Schema,
    ) -> Result<arrow::datatypes::DataType> {
        self.current().data_type(input_schema)
    }

    fn nullable(&self, input_schema: &arrow::datatypes::Schema) -> Result<bool> {
        self.current().nullable(input_schema)
    }

    fn evaluate(
        &self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> Result<ColumnarValue> {
        let current = self.current();
        current.evaluate(batch)
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Ok(inner) = self.inner.snapshot_current_filters() {
            conjunction(inner).fmt_sql(f)
        } else {
            write!(f, "dynamic_filter_expr()") // What do we want to do here?
        }
    }
}
