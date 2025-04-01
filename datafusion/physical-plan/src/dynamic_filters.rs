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

use std::{
    any::Any,
    fmt::Display,
    hash::Hash,
    sync::{Arc, RwLock},
};

use datafusion_common::{
    tree_node::{Transformed, TransformedResult, TreeNode},
    Result,
};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::{utils::conjunction, PhysicalExpr};
use datafusion_physical_expr_common::physical_expr::{DynEq, DynHash};

/// A source of dynamic runtime filters.
///
/// During query execution, operators implementing this trait can provide
/// filter expressions that other operators can use to dynamically prune data.
///
/// See `TopKDynamicFilterSource` in datafusion/physical-plan/src/topk/mod.rs for examples.
pub trait DynamicFilterSource:
    Send + Sync + std::fmt::Debug + DynEq + DynHash + Display + 'static
{
    /// Take a snapshot of the current state of filtering, returning a non-dynamic PhysicalExpr.
    /// This is used to e.g. serialize dynamic filters across the wire or to pass them into systems
    /// that won't use the `PhysicalExpr` API (e.g. matching on the concrete types of the expressions like `PruningPredicate` does).
    /// For example, it is expected that this returns a relatively simple expression such as `col1 > 5` for a TopK operator or
    /// `col2 IN (1, 2, ... N)` for a HashJoin operator.
    fn snapshot_current_filters(&self) -> Result<Vec<Arc<dyn PhysicalExpr>>>;

    fn as_any(&self) -> &dyn Any;
}

impl PartialEq for dyn DynamicFilterSource {
    fn eq(&self, other: &Self) -> bool {
        self.dyn_eq(other.as_any())
    }
}

impl Eq for dyn DynamicFilterSource {}

/// A wrapper around a [`DynamicFilterSource`] that allows it to be used as a physical expression.
/// This will call [`DynamicFilterSource::snapshot_current_filters`] to get the current filters for each call to
/// [`PhysicalExpr::evaluate`], [`PhysicalExpr::data_type`], and [`PhysicalExpr::nullable`].
/// It also implements [`PhysicalExpr::snapshot`] by forwarding the call to [`DynamicFilterSource::snapshot_current_filters`].
#[derive(Debug)]
pub struct DynamicFilterPhysicalExpr {
    /// The original children of this PhysicalExpr, if any.
    /// This is necessary because the dynamic filter may be initialized with a placeholder (e.g. `lit(true)`)
    /// and later remapped to the actual expressions that are being filtered.
    /// But we need to know the children (e.g. columns referenced in the expression) ahead of time to evaluate the expression correctly.
    children: Vec<Arc<dyn PhysicalExpr>>,
    /// If any of the children were remapped / modified (e.g. to adjust for projections) we need to keep track of the new children
    /// so that when we update `current()` in subsequent iterations we can re-apply the replacements.
    remapped_children: Option<Vec<Arc<dyn PhysicalExpr>>>,
    /// The source of dynamic filters.
    inner: Arc<dyn DynamicFilterSource>,
    /// For testing purposes track the data type and nullability to make sure they don't change.
    /// If they do, there's a bug in the implementation.
    /// But this can have overhead in production, so it's only included in our tests.
    data_type: Arc<RwLock<Option<arrow::datatypes::DataType>>>,
    nullable: Arc<RwLock<Option<bool>>>,
}

impl Hash for DynamicFilterPhysicalExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.dyn_hash(state);
        self.children.dyn_hash(state);
        self.remapped_children.dyn_hash(state);
    }
}

impl PartialEq for DynamicFilterPhysicalExpr {
    fn eq(&self, other: &Self) -> bool {
        self.inner.dyn_eq(other.inner.as_any())
            && self.children == other.children
            && self.remapped_children == other.remapped_children
    }
}

impl Eq for DynamicFilterPhysicalExpr {}

impl Display for DynamicFilterPhysicalExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DynamicFilterPhysicalExpr [ {} ]", self.inner)
    }
}

impl DynamicFilterPhysicalExpr {
    pub fn new(
        children: Vec<Arc<dyn PhysicalExpr>>,
        inner: Arc<dyn DynamicFilterSource>,
    ) -> Self {
        Self {
            children,
            remapped_children: None, // Initially no remapped children
            inner,
            data_type: Arc::new(RwLock::new(None)),
            nullable: Arc::new(RwLock::new(None)),
        }
    }

    fn current(&self) -> Result<Arc<dyn PhysicalExpr>> {
        let current = conjunction(self.inner.snapshot_current_filters()?);
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
                        let new_child = Arc::clone(&remapped_children[pos]);
                        Ok(Transformed::yes(new_child))
                    } else {
                        // Otherwise, just return the expression
                        Ok(Transformed::no(expr))
                    }
                })
                .data()
        } else {
            Ok(current)
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
        Ok(Arc::new(Self {
            children: self.children.clone(),
            remapped_children: Some(children),
            inner: Arc::clone(&self.inner),
            data_type: Arc::clone(&self.data_type),
            nullable: Arc::clone(&self.nullable),
        }))
    }

    fn data_type(
        &self,
        input_schema: &arrow::datatypes::Schema,
    ) -> Result<arrow::datatypes::DataType> {
        let res = self.current()?.data_type(input_schema)?;
        #[cfg(test)]
        {
            use datafusion_common::internal_err;
            // Check if the data type has changed.
            let mut data_type_lock = self
                .data_type
                .write()
                .expect("Failed to acquire write lock for data_type");
            if let Some(existing) = &*data_type_lock {
                if existing != &res {
                    // If the data type has changed, we have a bug.
                    return internal_err!(
                        "DynamicFilterPhysicalExpr data type has changed unexpectedly. \
                        Expected: {existing:?}, Actual: {res:?}"
                    );
                }
            } else {
                *data_type_lock = Some(res.clone());
            }
        }
        Ok(res)
    }

    fn nullable(&self, input_schema: &arrow::datatypes::Schema) -> Result<bool> {
        let res = self.current()?.nullable(input_schema)?;
        #[cfg(test)]
        {
            use datafusion_common::internal_err;
            // Check if the nullability has changed.
            let mut nullable_lock = self
                .nullable
                .write()
                .expect("Failed to acquire write lock for nullable");
            if let Some(existing) = *nullable_lock {
                if existing != res {
                    // If the nullability has changed, we have a bug.
                    return internal_err!(
                        "DynamicFilterPhysicalExpr nullability has changed unexpectedly. \
                        Expected: {existing}, Actual: {res}"
                    );
                }
            } else {
                *nullable_lock = Some(res);
            }
        }
        Ok(res)
    }

    fn evaluate(
        &self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> Result<ColumnarValue> {
        let current = self.current()?;
        #[cfg(test)]
        {
            // Ensure that we are not evaluating after the expression has changed.
            let schema = batch.schema();
            self.nullable(&schema)?;
            self.data_type(&schema)?;
        };
        current.evaluate(batch)
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Ok(inner) = self.inner.snapshot_current_filters() {
            conjunction(inner).fmt_sql(f)
        } else {
            write!(f, "dynamic_filter_expr()") // What do we want to do here?
        }
    }

    fn snapshot(&self) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        // Return the current expression as a snapshot.
        Ok(Some(self.current()?))
    }
}

#[cfg(test)]
mod test {
    use arrow::array::RecordBatch;
    use datafusion_common::ScalarValue;
    use datafusion_physical_expr::expressions::lit;

    use super::*;

    #[test]
    fn test_dynamic_filter_physical_expr_misbehaves_data_type_nullable() {
        #[derive(Debug)]
        struct MockDynamicFilterSource {
            current_expr: Arc<RwLock<Arc<dyn PhysicalExpr>>>,
        }

        impl Hash for MockDynamicFilterSource {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                // Hash the current expression to ensure uniqueness
                self.current_expr.read().unwrap().dyn_hash(state);
            }
        }

        impl DynEq for MockDynamicFilterSource {
            fn dyn_eq(&self, other: &dyn Any) -> bool {
                if let Some(other) = other.downcast_ref::<MockDynamicFilterSource>() {
                    self.current_expr
                        .read()
                        .unwrap()
                        .eq(&other.current_expr.read().unwrap())
                } else {
                    false
                }
            }
        }

        impl DynamicFilterSource for MockDynamicFilterSource {
            fn snapshot_current_filters(&self) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
                let expr = self.current_expr.read().unwrap().clone();
                Ok(vec![expr])
            }

            fn as_any(&self) -> &dyn Any {
                self
            }
        }

        impl Display for MockDynamicFilterSource {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "MockDynamicFilterSource [ current_expr: {:?} ]",
                    self.current_expr.read().unwrap()
                )
            }
        }

        let source = Arc::new(MockDynamicFilterSource {
            current_expr: Arc::new(RwLock::new(lit(42) as Arc<dyn PhysicalExpr>)),
        });
        let dynamic_filter = DynamicFilterPhysicalExpr::new(
            vec![],
            Arc::clone(&source) as Arc<dyn DynamicFilterSource>,
        );

        // First call to data_type and nullable should set the initial values.
        let initial_data_type = dynamic_filter
            .data_type(&arrow::datatypes::Schema::empty())
            .unwrap();
        let initial_nullable = dynamic_filter
            .nullable(&arrow::datatypes::Schema::empty())
            .unwrap();

        // Call again and expect no change.
        let second_data_type = dynamic_filter
            .data_type(&arrow::datatypes::Schema::empty())
            .unwrap();
        let second_nullable = dynamic_filter
            .nullable(&arrow::datatypes::Schema::empty())
            .unwrap();
        assert_eq!(
            initial_data_type, second_data_type,
            "Data type should not change on second call."
        );
        assert_eq!(
            initial_nullable, second_nullable,
            "Nullability should not change on second call."
        );

        // Now change the current expression to something else.
        {
            let mut current = source.current_expr.write().unwrap();
            *current = lit(ScalarValue::Utf8(None)) as Arc<dyn PhysicalExpr>;
        }
        // Check that we error if we call data_type, nullable or evaluate after changing the expression.
        assert!(
            dynamic_filter
                .data_type(&arrow::datatypes::Schema::empty())
                .is_err(),
            "Expected err when data_type is called after changing the expression."
        );
        assert!(
            dynamic_filter
                .nullable(&arrow::datatypes::Schema::empty())
                .is_err(),
            "Expected err when nullable is called after changing the expression."
        );
        let batch = RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::empty()));
        assert!(
            dynamic_filter.evaluate(&batch).is_err(),
            "Expected err when evaluate is called after changing the expression."
        );
    }
}
