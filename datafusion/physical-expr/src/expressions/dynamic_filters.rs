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

use crate::PhysicalExpr;
use arrow::datatypes::{DataType, Schema};
use datafusion_common::{
    tree_node::{Transformed, TransformedResult, TreeNode},
    Result,
};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr_common::physical_expr::{DynEq, DynHash};

/// A dynamic [`PhysicalExpr`] that can be updated by anyone with a reference to it.
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
    inner: Arc<RwLock<Arc<dyn PhysicalExpr>>>,
    /// For testing purposes track the data type and nullability to make sure they don't change.
    /// If they do, there's a bug in the implementation.
    /// But this can have overhead in production, so it's only included in our tests.
    data_type: Arc<RwLock<Option<DataType>>>,
    nullable: Arc<RwLock<Option<bool>>>,
}

impl Hash for DynamicFilterPhysicalExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let inner = self.current().expect("Failed to get current expression");
        inner.dyn_hash(state);
        self.children.dyn_hash(state);
        self.remapped_children.dyn_hash(state);
    }
}

impl PartialEq for DynamicFilterPhysicalExpr {
    fn eq(&self, other: &Self) -> bool {
        let inner = self.current().expect("Failed to get current expression");
        let our_children = self.remapped_children.as_ref().unwrap_or(&self.children);
        let other_children = other.remapped_children.as_ref().unwrap_or(&other.children);
        let other = other.current().expect("Failed to get current expression");
        inner.dyn_eq(other.as_any()) && our_children == other_children
    }
}

impl Eq for DynamicFilterPhysicalExpr {}

impl Display for DynamicFilterPhysicalExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.current().expect("Failed to get current expression");
        write!(f, "DynamicFilterPhysicalExpr [ {} ]", inner)
    }
}

impl DynamicFilterPhysicalExpr {
    /// Create a new [`DynamicFilterPhysicalExpr`]
    /// from an initial expression and a list of children.
    /// The list of children is provided separately because
    /// the initial expression may not have the same children.
    /// For example, if the initial expression is just `true`
    /// it will not reference any columns, but we may know that
    /// we are going to replace this expression with a real one
    /// that does reference certain columns.
    /// In this case you **must** pass in the columns that will be
    /// used in the final expression as children to this function
    /// since DataFusion is generally not compatible with dynamic
    /// *children* in expressions.
    ///
    /// To determine the children you can:
    ///
    /// - Use [`collect_columns`] to collect the columns from the expression.
    /// - Use existing information, such as the sort columns in a `SortExec`.
    ///
    /// Generally the important bit is that the *leaf children that reference columns
    /// do not change* since those will be used to determine what columns need to read or projected
    /// when evaluating the expression.
    ///
    /// [`collect_columns`]: crate::utils::collect_columns
    #[allow(dead_code)] // Only used in tests for now
    pub fn new(
        children: Vec<Arc<dyn PhysicalExpr>>,
        inner: Arc<dyn PhysicalExpr>,
    ) -> Self {
        Self {
            children,
            remapped_children: None, // Initially no remapped children
            inner: Arc::new(RwLock::new(inner)),
            data_type: Arc::new(RwLock::new(None)),
            nullable: Arc::new(RwLock::new(None)),
        }
    }

    /// Get the current expression.
    /// This will return the current expression with any children
    /// remapped to match calls to [`PhysicalExpr::with_new_children`].
    pub fn current(&self) -> Result<Arc<dyn PhysicalExpr>> {
        let current = self
            .inner
            .read()
            .map_err(|_| {
                datafusion_common::DataFusionError::Execution(
                    "Failed to acquire read lock for inner".to_string(),
                )
            })?
            .clone();
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

    /// Update the current expression.
    /// Any children of this expression must be a subset of the original children
    /// passed to the constructor.
    /// This should be called e.g.:
    /// - When we've computed the probe side's hash table in a HashJoinExec
    /// - After every batch is processed if we update the TopK heap in a SortExec using a TopK approach.
    pub fn update(&self, new_expr: Arc<dyn PhysicalExpr>) -> Result<()> {
        let mut current = self.inner.write().map_err(|_| {
            datafusion_common::DataFusionError::Execution(
                "Failed to acquire write lock for inner".to_string(),
            )
        })?;
        *current = new_expr;
        Ok(())
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

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
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

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
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
        let inner = self.current().map_err(|_| std::fmt::Error)?;
        inner.fmt_sql(f)
    }

    fn snapshot(&self) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        // Return the current expression as a snapshot.
        Ok(Some(self.current()?))
    }
}

#[cfg(test)]
mod test {
    use crate::{
        expressions::{col, lit, BinaryExpr},
        utils::reassign_predicate_columns,
    };
    use arrow::{
        array::RecordBatch,
        datatypes::{DataType, Field, Schema},
    };
    use datafusion_common::ScalarValue;

    use super::*;

    #[test]
    fn test_remap_children() {
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let file_schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Int32, false),
            Field::new("a", DataType::Int32, false),
        ]));
        let expr = Arc::new(BinaryExpr::new(
            col("a", &table_schema).unwrap(),
            datafusion_expr::Operator::Gt,
            lit(42) as Arc<dyn PhysicalExpr>,
        ));
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![col("a", &table_schema).unwrap()],
            expr as Arc<dyn PhysicalExpr>,
        ));
        // Take an initial snapshot
        let snap = dynamic_filter.snapshot().unwrap().unwrap();
        insta::assert_snapshot!(format!("{snap:?}"), @r#"BinaryExpr { left: Column { name: "a", index: 0 }, op: Gt, right: Literal { value: Int32(42) }, fail_on_overflow: false }"#);
        // Remap the children to the file schema
        let dynamic_filter =
            reassign_predicate_columns(dynamic_filter, &file_schema, false).unwrap();
        // Take a snapshot after remapping, the children in the snapshot should be remapped to the file schema
        let snap = dynamic_filter.snapshot().unwrap().unwrap();
        insta::assert_snapshot!(format!("{snap:?}"), @r#"BinaryExpr { left: Column { name: "a", index: 1 }, op: Gt, right: Literal { value: Int32(42) }, fail_on_overflow: false }"#);
    }

    #[test]
    fn test_snapshot() {
        let expr = lit(42) as Arc<dyn PhysicalExpr>;
        let dynamic_filter = DynamicFilterPhysicalExpr::new(vec![], Arc::clone(&expr));

        // Take a snapshot of the current expression
        let snapshot = dynamic_filter.snapshot().unwrap();
        assert_eq!(snapshot, Some(expr));

        // Update the current expression
        let new_expr = lit(100) as Arc<dyn PhysicalExpr>;
        dynamic_filter.update(Arc::clone(&new_expr)).unwrap();
        // Take another snapshot
        let snapshot = dynamic_filter.snapshot().unwrap();
        assert_eq!(snapshot, Some(new_expr));
    }

    #[test]
    fn test_dynamic_filter_physical_expr_misbehaves_data_type_nullable() {
        let dynamic_filter =
            DynamicFilterPhysicalExpr::new(vec![], lit(42) as Arc<dyn PhysicalExpr>);

        // First call to data_type and nullable should set the initial values.
        let initial_data_type = dynamic_filter.data_type(&Schema::empty()).unwrap();
        let initial_nullable = dynamic_filter.nullable(&Schema::empty()).unwrap();

        // Call again and expect no change.
        let second_data_type = dynamic_filter.data_type(&Schema::empty()).unwrap();
        let second_nullable = dynamic_filter.nullable(&Schema::empty()).unwrap();
        assert_eq!(
            initial_data_type, second_data_type,
            "Data type should not change on second call."
        );
        assert_eq!(
            initial_nullable, second_nullable,
            "Nullability should not change on second call."
        );

        // Now change the current expression to something else.
        dynamic_filter
            .update(lit(ScalarValue::Utf8(None)) as Arc<dyn PhysicalExpr>)
            .expect("Failed to update expression");
        // Check that we error if we call data_type, nullable or evaluate after changing the expression.
        assert!(
            dynamic_filter.data_type(&Schema::empty()).is_err(),
            "Expected err when data_type is called after changing the expression."
        );
        assert!(
            dynamic_filter.nullable(&Schema::empty()).is_err(),
            "Expected err when nullable is called after changing the expression."
        );
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        assert!(
            dynamic_filter.evaluate(&batch).is_err(),
            "Expected err when evaluate is called after changing the expression."
        );
    }
}
