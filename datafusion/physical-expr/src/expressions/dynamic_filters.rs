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
use arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion_common::{
    tree_node::{Transformed, TransformedResult, TreeNode},
    Result,
};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr_common::physical_expr::{DynEq, DynHash, PhysicalExprRef};

use super::Column;

/// A dynamic [`PhysicalExpr`] that can be updated by anyone with a reference to it.
#[derive(Debug)]
pub struct DynamicFilterPhysicalExpr {
    /// The source of dynamic filters.
    inner: PhysicalExprRef,

    /// For testing purposes track the data type and nullability to make sure they don't change.
    /// If they do, there's a bug in the implementation.
    /// But this can have overhead in production, so it's only included in our tests.
    data_type: Arc<RwLock<Option<DataType>>>,
    nullable: Arc<RwLock<Option<bool>>>,
}

impl Hash for DynamicFilterPhysicalExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        todo!("")
    }
}

impl PartialEq for DynamicFilterPhysicalExpr {
    fn eq(&self, other: &Self) -> bool {
        todo!("")
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
        // children: Vec<Arc<dyn PhysicalExpr>>,
        inner: Arc<dyn PhysicalExpr>,
    ) -> Self {
        Self {
            inner,
            data_type: Arc::new(RwLock::new(None)),
            nullable: Arc::new(RwLock::new(None)),
        }
    }

    // udpate schema
    // pub fn with_schema(&self, schema: SchemaRef) -> Self {
    //     Self {
    //         remapped_schema: Some(schema),
    //         inner: Arc::clone(&self.inner),
    //         data_type: Arc::clone(&self.data_type),
    //         nullable: Arc::clone(&self.nullable),
    //     }
    // }

    // get the source filter
    pub fn current(&self) -> Result<Arc<dyn PhysicalExpr>> {
        let inner = Arc::clone(&self.inner);

        // let inner = self
        //     .inner
        //     .read()
        //     .map_err(|_| {
        //         datafusion_common::DataFusionError::Execution(
        //             "Failed to acquire read lock for inner".to_string(),
        //         )
        //     })?
        //     .clone();
        Ok(inner)
    }

    // update source filter
    // create a new one
    pub fn update(&mut self, filter: PhysicalExprRef) {
        self.inner = filter;
        // let mut w = self.inner.write().unwrap();
        // *w = filter;
    }
}

impl PhysicalExpr for DynamicFilterPhysicalExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        todo!("")
    }

    // update source filter
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        debug_assert_eq!(children.len(), 1);
        let inner = children.swap_remove(0);
        Ok(Arc::new(Self::new(inner)))
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

    // snapshot with given schema based on the source filter.
    // only evalute is expected to be called after this output. no schema or source filter are updated for the snapshot.
    fn snapshot(
        &self,
        remapped_schema: Option<SchemaRef>,
    ) -> Result<Option<PhysicalExprRef>> {
        if let Some(remapped_schema) = remapped_schema {
            let pred = self.current()?;
            let new_pred = pred
                .transform_up(|expr| {
                    if let Some(col) = expr.as_any().downcast_ref::<Column>() {
                        let index = match remapped_schema.index_of(col.name()) {
                            Ok(idx) => idx,
                            Err(_) => {
                                return Err(datafusion_common::DataFusionError::Plan(
                                    format!("Column {} not found in schema", col.name()),
                                ))
                            }
                        };
                        return Ok(Transformed::yes(Arc::new(Column::new(
                            col.name(),
                            index,
                        ))));
                    } else {
                        // If the expression is not a column, just return it
                        return Ok(Transformed::no(expr));
                    }
                })
                .data()?;

            Ok(Some(new_pred))
        } else {
            Ok(Some(self.current()?))
        }
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
    use datafusion_physical_expr_common::physical_expr::PhysicalExprRef;

    use super::*;

    #[test]
    fn test_remap_children() {
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let expr = Arc::new(BinaryExpr::new(
            col("a", &table_schema).unwrap(),
            datafusion_expr::Operator::Eq,
            lit(42) as Arc<dyn PhysicalExpr>,
        ));
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            // vec![col("a", &table_schema).unwrap()],
            expr as Arc<dyn PhysicalExpr>,
        ));
        // Simulate two `ParquetSource` files with different filter schemas
        // Both of these should hit the same inner `PhysicalExpr` even after `update()` is called
        // and be able to remap children independently.
        let filter_schema_1 = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let filter_schema_2 = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Int32, false),
            Field::new("a", DataType::Int32, false),
        ]));
        // Each ParquetExec calls `with_new_children` on the DynamicFilterPhysicalExpr
        // and remaps the children to the file schema.
        let snap_1 = dynamic_filter
            .snapshot(Some(Arc::clone(&filter_schema_1)))
            .unwrap()
            .unwrap();
        insta::assert_snapshot!(format!("{snap_1:?}"), @r#"BinaryExpr { left: Column { name: "a", index: 0 }, op: Eq, right: Literal { value: Int32(42) }, fail_on_overflow: false }"#);
        let snap_2 = dynamic_filter
            .snapshot(Some(Arc::clone(&filter_schema_2)))
            .unwrap()
            .unwrap();
        insta::assert_snapshot!(format!("{snap_2:?}"), @r#"BinaryExpr { left: Column { name: "a", index: 1 }, op: Eq, right: Literal { value: Int32(42) }, fail_on_overflow: false }"#);
        // Both filters allow evaluating the same expression
        let batch_1 = RecordBatch::try_new(
            Arc::clone(&filter_schema_1),
            vec![
                // a
                ScalarValue::Int32(Some(42)).to_array_of_size(1).unwrap(),
                // b
                ScalarValue::Int32(Some(43)).to_array_of_size(1).unwrap(),
            ],
        )
        .unwrap();
        let batch_2 = RecordBatch::try_new(
            Arc::clone(&filter_schema_2),
            vec![
                // b
                ScalarValue::Int32(Some(43)).to_array_of_size(1).unwrap(),
                // a
                ScalarValue::Int32(Some(42)).to_array_of_size(1).unwrap(),
            ],
        )
        .unwrap();
        // Evaluate the expression on both batches
        let result_1 = snap_1.evaluate(&batch_1).unwrap();
        let result_2 = snap_2.evaluate(&batch_2).unwrap();
        // Check that the results are the same
        let ColumnarValue::Array(arr_1) = result_1 else {
            panic!("Expected ColumnarValue::Array");
        };
        let ColumnarValue::Array(arr_2) = result_2 else {
            panic!("Expected ColumnarValue::Array");
        };
        assert!(arr_1.eq(&arr_2));
        let expected = ScalarValue::Boolean(Some(true))
            .to_array_of_size(1)
            .unwrap();
        assert!(arr_1.eq(&expected));
        // Now lets update the expression
        // Note that we update the *original* expression and that should be reflected in both the derived expressions
        let new_expr = Arc::new(BinaryExpr::new(
            col("a", &table_schema).unwrap(),
            datafusion_expr::Operator::Gt,
            lit(43) as Arc<dyn PhysicalExpr>,
        )) as PhysicalExprRef;

        let dynamic_filter = dynamic_filter
            .with_new_children(vec![new_expr])
            .expect("Failed to update children");
        // dynamic_filter.update(new_expr);

        let snap_1 = dynamic_filter
            .snapshot(Some(Arc::clone(&filter_schema_1)))
            .unwrap()
            .unwrap();
        let snap_2 = dynamic_filter
            .snapshot(Some(Arc::clone(&filter_schema_2)))
            .unwrap()
            .unwrap();

        // Now we should be able to evaluate the new expression on both batches
        let result_1 = snap_1.evaluate(&batch_1).unwrap();
        let result_2 = snap_2.evaluate(&batch_2).unwrap();

        // Check that the results are the same
        let ColumnarValue::Array(arr_1) = result_1 else {
            panic!("Expected ColumnarValue::Array");
        };
        let ColumnarValue::Array(arr_2) = result_2 else {
            panic!("Expected ColumnarValue::Array");
        };
        assert!(arr_1.eq(&arr_2));
        let expected = ScalarValue::Boolean(Some(false))
            .to_array_of_size(1)
            .unwrap();
        assert!(arr_1.eq(&expected));
    }

    #[test]
    fn test_snapshot() {
        let expr = lit(42) as Arc<dyn PhysicalExpr>;
        let dynamic_filter = DynamicFilterPhysicalExpr::new(Arc::clone(&expr));

        // Take a snapshot of the current expression
        let snapshot = dynamic_filter.snapshot(None).unwrap().unwrap();
        assert_eq!(&snapshot, &expr);

        // Update the current expression
        let new_expr = lit(100) as Arc<dyn PhysicalExpr>;
        let df = Arc::new(dynamic_filter) as PhysicalExprRef;
        let df = df
            .with_new_children(vec![new_expr.clone()])
            .expect("Failed to update expression");
        // dynamic_filter.update(Arc::clone(&new_expr));
        // Take another snapshot
        let snapshot = df.snapshot(None).unwrap().unwrap();
        assert_eq!(&snapshot, &new_expr);
    }

    #[test]
    fn test_dynamic_filter_physical_expr_misbehaves_data_type_nullable() {
        let mut dynamic_filter = DynamicFilterPhysicalExpr::new(lit(42));

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
        dynamic_filter.update(lit(ScalarValue::Utf8(None)));
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

        let snap = dynamic_filter.snapshot(None).unwrap().unwrap();
        // this is changed to ok, but makes sense
        assert!(snap.evaluate(&batch).is_ok(),);
    }
}
