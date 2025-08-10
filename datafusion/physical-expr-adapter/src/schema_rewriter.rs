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

//! Physical expression schema rewriting utilities with struct support

use std::sync::Arc;

use arrow::compute::can_cast_types;
use arrow::datatypes::{DataType, FieldRef, Schema};
use datafusion_common::{
    exec_err,
    tree_node::{Transformed, TransformedResult, TreeNode},
    Result, ScalarValue,
};
use datafusion_functions::core::getfield::GetFieldFunc;
use datafusion_physical_expr::{
    expressions::{self, CastExpr, Column},
    ScalarFunctionExpr,
};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

/// Builder for rewriting physical expressions to match different schemas.
///
/// # Example
///
/// ```rust
/// use datafusion_physical_expr_adapter::PhysicalExprSchemaRewriter;
/// use arrow::datatypes::Schema;
/// use std::sync::Arc;
///
/// # fn example(
/// #     predicate: std::sync::Arc<dyn datafusion_physical_expr_common::physical_expr::PhysicalExpr>,
/// #     physical_file_schema: &Schema,
/// #     logical_file_schema: &Schema,
/// # ) -> datafusion_common::Result<()> {
/// let rewriter = PhysicalExprSchemaRewriter::new(&physical_file_schema, &logical_file_schema);
/// let adapted_predicate = rewriter.rewrite(predicate)?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct PhysicalExprSchemaRewriter<'a> {
    physical_file_schema: &'a Schema,
    logical_file_schema: &'a Schema,
    partition_fields: Vec<(FieldRef, ScalarValue)>,
}

impl<'a> PhysicalExprSchemaRewriter<'a> {
    /// Create a new instance of the physical expression rewriter.
    ///
    /// This rewriter adapts expressions to match the physical schema of the file being scanned,
    /// handling type mismatches and missing columns by filling them with null values.
    pub fn new(physical_file_schema: &'a Schema, logical_file_schema: &'a Schema) -> Self {
        Self {
            physical_file_schema,
            logical_file_schema,
            partition_fields: Vec::new(),
        }
    }

    /// Add partition columns with their values
    pub fn with_partition_columns(
        mut self,
        partition_fields: Vec<FieldRef>,
        partition_values: Vec<ScalarValue>,
    ) -> Self {
        self.partition_fields = partition_fields
            .into_iter()
            .zip(partition_values)
            .collect();
        self
    }

    /// Rewrite the given physical expression to match the target schema
    ///
    /// This method applies the following transformations:
    /// 1. Replaces partition column references with literal values
    /// 2. Handles missing columns by inserting null literals
    /// 3. Casts columns when logical and physical schemas have different types
    /// 4. Recursively rebuilds struct expressions with proper field mapping
    pub fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        expr.transform(|expr| self.rewrite_expr(expr)).data()
    }
}

impl<'a> PhysicalExprSchemaRewriter<'a> {
    fn rewrite_expr(
        &self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        if let Some(transformed) = self.try_rewrite_struct_field_access(&expr)? {
            return Ok(Transformed::yes(transformed));
        }

        if let Some(column) = expr.as_any().downcast_ref::<Column>() {
            return self.rewrite_column(Arc::clone(&expr), column);
        }

        Ok(Transformed::no(expr))
    }

    fn try_rewrite_struct_field_access(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        let get_field_expr = match expr.as_any().downcast_ref::<ScalarFunctionExpr>() {
            Some(expr) => expr,
            None => return Ok(None),
        };

        if get_field_expr.name() != "get_field" {
            return Ok(None);
        }

        if get_field_expr
            .fun()
            .inner()
            .as_any()
            .downcast_ref::<GetFieldFunc>()
            .is_none()
        {
            return Ok(None);
        }

        let source_expr = match get_field_expr.args().first() {
            Some(expr) => expr,
            None => return Ok(None),
        };

        let field_name_expr = match get_field_expr.args().get(1) {
            Some(expr) => expr,
            None => return Ok(None),
        };

        let lit = match field_name_expr
            .as_any()
            .downcast_ref::<expressions::Literal>()
        {
            Some(lit) => lit,
            None => return Ok(None),
        };

        let field_name = match lit.value() {
            ScalarValue::Utf8(Some(name))
            | ScalarValue::LargeUtf8(Some(name))
            | ScalarValue::Utf8View(Some(name)) => name,
            _ => return Ok(None),
        };

        let column = match source_expr.as_any().downcast_ref::<Column>() {
            Some(column) => column,
            None => return Ok(None),
        };

        let physical_field =
            match self.physical_file_schema.field_with_name(column.name()) {
                Ok(field) => field,
                Err(_) => return Ok(None),
            };

        let physical_struct_fields = match physical_field.data_type() {
            DataType::Struct(fields) => fields,
            _ => return Ok(None),
        };

        if physical_struct_fields
            .iter()
            .any(|f| f.name() == field_name)
        {
            return Ok(None);
        }

        let logical_field = match self.logical_file_schema.field_with_name(column.name())
        {
            Ok(field) => field,
            Err(_) => return Ok(None),
        };

        let logical_struct_fields = match logical_field.data_type() {
            DataType::Struct(fields) => fields,
            _ => return Ok(None),
        };

        let logical_struct_field = match logical_struct_fields
            .iter()
            .find(|f| f.name() == field_name)
        {
            Some(field) => field,
            None => return Ok(None),
        };

        let null_value = ScalarValue::Null.cast_to(logical_struct_field.data_type())?;
        Ok(Some(expressions::lit(null_value)))
    }

    fn rewrite_column(
        &self,
        expr: Arc<dyn PhysicalExpr>,
        column: &Column,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        // Get the logical field for this column if it exists in the logical schema
        let logical_field = match self.logical_file_schema.field_with_name(column.name())
        {
            Ok(field) => field,
            Err(e) => {
                // If the column is a partition field, we can use the partition value
                if let Some(partition_value) = self.get_partition_value(column.name()) {
                    return Ok(Transformed::yes(expressions::lit(partition_value)));
                }
                // This can be hit if a custom rewrite injected a reference to a column that doesn't exist in the logical schema.
                // For example, a pre-computed column that is kept only in the physical schema.
                // If the column exists in the physical schema, we can still use it.
                if let Ok(physical_field) =
                    self.physical_file_schema.field_with_name(column.name())
                {
                    // If the column exists in the physical schema, we can use it in place of the logical column.
                    // This is nice to users because if they do a rewrite that results in something like `phyiscal_int32_col = 123u64`
                    // we'll at least handle the casts for them.
                    physical_field
                } else {
                    // A completely unknown column that doesn't exist in either schema!
                    // This should probably never be hit unless something upstream broke, but nontheless it's better
                    // for us to return a handleable error than to panic / do something unexpected.
                    return Err(e.into());
                }
            }
        };

        // Check if the column exists in the physical schema
        let physical_column_index =
            match self.physical_file_schema.index_of(column.name()) {
                Ok(index) => index,
                Err(_) => {
                    if !logical_field.is_nullable() {
                        return exec_err!(
                        "Non-nullable column '{}' is missing from the physical schema",
                        column.name()
                    );
                    }
                    // If the column is missing from the physical schema fill it in with nulls as `SchemaAdapter` would do.
                    // TODO: do we need to sync this with what the `SchemaAdapter` actually does?
                    // While the default implementation fills in nulls in theory a custom `SchemaAdapter` could do something else!
                    // See https://github.com/apache/datafusion/issues/16527
                    let null_value =
                        ScalarValue::Null.cast_to(logical_field.data_type())?;
                    return Ok(Transformed::yes(expressions::lit(null_value)));
                }
            };
        let physical_field = self.physical_file_schema.field(physical_column_index);

        let column = match (
            column.index() == physical_column_index,
            logical_field.data_type() == physical_field.data_type(),
        ) {
            // If the column index matches and the data types match, we can use the column as is
            (true, true) => return Ok(Transformed::no(expr)),
            // If the indexes or data types do not match, we need to create a new column expression
            (true, _) => column.clone(),
            (false, _) => {
                Column::new_with_schema(logical_field.name(), self.physical_file_schema)?
            }
        };

        if logical_field.data_type() == physical_field.data_type() {
            // If the data types match, we can use the column as is
            return Ok(Transformed::yes(Arc::new(column)));
        }

        // We need to cast the column to the logical data type
        // TODO: add optimization to move the cast from the column to literal expressions in the case of `col = 123`
        // since that's much cheaper to evalaute.
        // See https://github.com/apache/datafusion/issues/15780#issuecomment-2824716928
        let is_compatible =
            can_cast_types(physical_field.data_type(), logical_field.data_type());
        if !is_compatible {
            return exec_err!(
                "Cannot cast column '{}' from '{}' (physical data type) to '{}' (logical data type)",
                column.name(),
                physical_field.data_type(),
                logical_field.data_type()
            );
        }

        let cast_expr = Arc::new(CastExpr::new(
            Arc::new(column),
            logical_field.data_type().clone(),
            None,
        ));

        Ok(Transformed::yes(cast_expr))
    }

    fn get_partition_value(&self, column_name: &str) -> Option<ScalarValue> {
        self.partition_fields
            .iter()
            .find(|(field, _)| field.name() == column_name)
            .map(|(_, value)| value.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{RecordBatch, RecordBatchOptions};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::{assert_contains, record_batch, Result, ScalarValue};
    use datafusion_physical_expr::expressions::{col, lit, BinaryExpr, CastExpr, Column, Literal};
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
    use datafusion_expr::Operator;
    use itertools::Itertools;
    use std::sync::Arc;

    fn create_test_schema() -> (Schema, Schema) {
        let physical_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]);

        let logical_schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false), // Different type
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Float64, true), // Missing from physical
        ]);

        (physical_schema, logical_schema)
    }

    #[test]
    fn test_rewrite_column_with_type_cast() {
        let (physical_schema, logical_schema) = create_test_schema();

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema);
        let column_expr = Arc::new(Column::new("a", 0));

        let result = rewriter.rewrite(column_expr).unwrap();

        let expected = Arc::new(CastExpr::new(
            Arc::new(Column::new("a", 0)),
            DataType::Int64,
            None,
        )) as Arc<dyn PhysicalExpr>;

        assert_eq!(result.to_string(), expected.to_string());
    }

    #[test]
    fn test_rewrite_struct_column_incompatible() {
        let physical_schema = Schema::new(vec![Field::new(
            "data",
            DataType::Struct(vec![Field::new("field1", DataType::Binary, true)].into()),
            true,
        )]);

        let logical_schema = Schema::new(vec![Field::new(
            "data",
            DataType::Struct(vec![Field::new("field1", DataType::Int32, true)].into()),
            true,
        )]);

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema);
        let column_expr = Arc::new(Column::new("data", 0));

        let result = rewriter.rewrite(column_expr);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert_contains!(error_msg, "Cannot cast column 'data'");
    }

    #[test]
    fn test_rewrite_struct_compatible_cast() {
        let physical_schema = Schema::new(vec![Field::new(
            "data",
            DataType::Struct(
                vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("name", DataType::Utf8, true),
                ]
                .into(),
            ),
            false,
        )]);

        let logical_schema = Schema::new(vec![Field::new(
            "data",
            DataType::Struct(
                vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("name", DataType::Utf8View, true),
                ]
                .into(),
            ),
            false,
        )]);

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema);
        let column_expr = Arc::new(Column::new("data", 0));

        let result = rewriter.rewrite(column_expr).unwrap();

        let expected = Arc::new(CastExpr::new(
            Arc::new(Column::new("data", 0)),
            DataType::Struct(
                vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("name", DataType::Utf8View, true),
                ]
                .into(),
            ),
            None,
        )) as Arc<dyn PhysicalExpr>;

        assert_eq!(result.to_string(), expected.to_string());
    }

    #[test]
    fn test_rewrite_missing_column_non_nullable_error() {
        let physical_schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let logical_schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false), // Missing and non-nullable
        ]);

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema);
        let column_expr = Arc::new(Column::new("b", 1));

        let result = rewriter.rewrite(column_expr);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert_contains!(error_msg, "Non-nullable column 'b' is missing");
    }


    #[test]
    fn test_rewrite_missing_column_nullable() {
        let physical_schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let logical_schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true), // Missing but nullable
        ]);

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema);
        let column_expr = Arc::new(Column::new("b", 1));

        let result = rewriter.rewrite(column_expr).unwrap();

        let expected =
            Arc::new(Literal::new(ScalarValue::Utf8(None))) as Arc<dyn PhysicalExpr>;

        assert_eq!(result.to_string(), expected.to_string());
    }

    #[test]
    fn test_rewrite_partition_column() {
        let physical_schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        // Partition column is NOT in logical schema - it will be handled by partition logic
        let logical_schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let partition_fields =
            vec![Arc::new(Field::new("part_col", DataType::Utf8, false))];
        let partition_values =
            vec![ScalarValue::Utf8(Some("partition_value".to_string()))];

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema)
            .with_partition_columns(partition_fields, partition_values.clone());

        let column_expr = Arc::new(Column::new("part_col", 1));

        let result = rewriter.rewrite(column_expr).unwrap();

        let expected =
            Arc::new(Literal::new(partition_values[0].clone())) as Arc<dyn PhysicalExpr>;

        assert_eq!(result.to_string(), expected.to_string());
    }

    #[test]
    fn test_rewrite_mulit_column_expr_with_type_cast() {
        let (physical_schema, logical_schema) = create_test_schema();
        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema);

        // Create a complex expression: (a + 5) OR (c > 0.0) that tests the recursive case of the rewriter
        let column_a = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let column_c = Arc::new(Column::new("c", 2)) as Arc<dyn PhysicalExpr>;
        let expr = BinaryExpr::new(
            Arc::clone(&column_a),
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Int64(Some(5)))),
        );
        let expr = BinaryExpr::new(
            Arc::new(expr),
            Operator::Or,
            Arc::new(BinaryExpr::new(
                Arc::clone(&column_c),
                Operator::Gt,
                Arc::new(Literal::new(ScalarValue::Float64(Some(0.0)))),
            )),
        );

        let result = rewriter.rewrite(Arc::new(expr)).unwrap();

        let expected = BinaryExpr::new(
            Arc::new(CastExpr::new(
                Arc::new(Column::new("a", 0)),
                DataType::Int64,
                None,
            )),
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Int64(Some(5)))),
        );
        let expected = Arc::new(BinaryExpr::new(
            Arc::new(expected),
            Operator::Or,
            Arc::new(BinaryExpr::new(
                lit(ScalarValue::Float64(None)), // c is missing, so it becomes null
                Operator::Gt,
                Arc::new(Literal::new(ScalarValue::Float64(Some(0.0)))),
            )),
        )) as Arc<dyn PhysicalExpr>;

        assert_eq!(
            result.to_string(),
            expected.to_string(),
            "The rewritten expression did not match the expected output"
        );
    }

    #[test]
    fn test_rewrite_no_change_needed() {
        let (physical_schema, logical_schema) = create_test_schema();
        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema);
        
        // Column "b" exists in both schemas with the same type (Utf8)
        let column_expr = Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>;

        let result = rewriter.rewrite(Arc::clone(&column_expr)).unwrap();

        // The expression should remain unchanged since the column exists in both schemas
        // with the same type. We check the actual content rather than pointer equality
        // since the new API might create new instances.
        assert_eq!(result.to_string(), column_expr.to_string());
        
        // Verify it's still a Column expression
        assert!(result.as_any().downcast_ref::<Column>().is_some());
    }

    /// Helper function to project expressions onto a RecordBatch
    fn batch_project(
        expr: Vec<Arc<dyn PhysicalExpr>>,
        batch: &RecordBatch,
        schema: SchemaRef,
    ) -> Result<RecordBatch> {
        let arrays = expr
            .iter()
            .map(|expr| {
                expr.evaluate(batch)
                    .and_then(|v| v.into_array(batch.num_rows()))
            })
            .collect::<Result<Vec<_>>>()?;

        if arrays.is_empty() {
            let options =
                RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
            RecordBatch::try_new_with_options(Arc::clone(&schema), arrays, &options)
                .map_err(Into::into)
        } else {
            RecordBatch::try_new(Arc::clone(&schema), arrays).map_err(Into::into)
        }
    }

    /// Example showing how we can use the `PhysicalExprSchemaRewriter` to adapt RecordBatches during a scan
    /// to apply projections, type conversions and handling of missing columns all at once.
    #[test]
    fn test_adapt_batches() {
        let physical_batch = record_batch!(
            ("a", Int32, vec![Some(1), None, Some(3)]),
            ("extra", Utf8, vec![Some("x"), Some("y"), None])
        )
        .unwrap();

        let physical_schema = physical_batch.schema();

        let logical_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true), // Different type
            Field::new("b", DataType::Utf8, true),  // Missing from physical
        ]));

        let projection = vec![
            col("b", &logical_schema).unwrap(),
            col("a", &logical_schema).unwrap(),
        ];

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema);

        let adapted_projection = projection
            .into_iter()
            .map(|expr| rewriter.rewrite(expr).unwrap())
            .collect_vec();

        let adapted_schema = Arc::new(Schema::new(
            adapted_projection
                .iter()
                .map(|expr| expr.return_field(&physical_schema).unwrap())
                .collect_vec(),
        ));

        let res = batch_project(
            adapted_projection,
            &physical_batch,
            Arc::clone(&adapted_schema),
        )
        .unwrap();

        assert_eq!(res.num_columns(), 2);
        assert_eq!(res.column(0).data_type(), &DataType::Utf8);
        assert_eq!(res.column(1).data_type(), &DataType::Int64);
        assert_eq!(
            res.column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap()
                .iter()
                .collect_vec(),
            vec![None, None, None]
        );
        assert_eq!(
            res.column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .iter()
                .collect_vec(),
            vec![Some(1), None, Some(3)]
        );
    }
}
