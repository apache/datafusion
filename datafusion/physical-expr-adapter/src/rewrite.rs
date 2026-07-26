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

//! Rewrite expressions in preparation for files being scanned, such as scan-metadata scalar UDFs.
//!
//! Functions like [`file_row_index()`] and [`input_file_name()`] are placeholders
//! whose value is only known during a file scan. The helpers here replace those
//! UDFs with ordinary physical expressions bound to the current file: a column
//! reference into a source-provided row-index column, or a per-file literal, etc.
//!
//! [`file_row_index()`]: datafusion_functions::core::file_row_index::FileRowIndexFunc
//! [`input_file_name()`]: datafusion_functions::core::input_file_name::InputFileNameFunc

use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use datafusion_common::{
    Result, ScalarValue,
    tree_node::{Transformed, TreeNode, TreeNodeRecursion},
};
use datafusion_expr::ScalarUDFImpl;
use datafusion_functions::core::file_row_index::FileRowIndexFunc;
use datafusion_functions::core::input_file_name::InputFileNameFunc;
use datafusion_physical_expr::ScalarFunctionExpr;
use datafusion_physical_expr::expressions::{CastExpr, Column, Literal};
use datafusion_physical_expr::projection::{ProjectionExpr, ProjectionExprs};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

/// Return true if a [`PhysicalExpr`] references scalar UDF `T`.
///
/// This matches the concrete [`ScalarUDFImpl`] type rather than the function
/// name, so unrelated UDFs with the same name are not treated as matches.
pub fn expr_references_scalar_udf<T: ScalarUDFImpl>(
    expr: &Arc<dyn PhysicalExpr>,
) -> bool {
    let mut found = false;

    expr.apply(|node| {
        if ScalarFunctionExpr::try_downcast_func::<T>(node.as_ref()).is_some() {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("Infallible traversal of PhysicalExpr tree failed");

    found
}

/// Rewrite occurrences of scalar UDF `T` in a [`PhysicalExpr`] using
/// `replacement`.
///
/// The rewrite matches the concrete [`ScalarUDFImpl`] type rather than the
/// function name. `replacement` is called with each matching
/// [`ScalarFunctionExpr`] after its children have been rewritten.
fn rewrite_scalar_udf<T, F>(
    expr: Arc<dyn PhysicalExpr>,
    mut replacement: F,
) -> Result<Arc<dyn PhysicalExpr>>
where
    T: ScalarUDFImpl,
    F: FnMut(&ScalarFunctionExpr) -> Result<Arc<dyn PhysicalExpr>>,
{
    expr.transform_up(|node| {
        if let Some(scalar_fn) = ScalarFunctionExpr::try_downcast_func::<T>(node.as_ref())
        {
            Ok(Transformed::yes(replacement(scalar_fn)?))
        } else {
            Ok(Transformed::no(node))
        }
    })
    .map(|transformed| transformed.data)
}

/// Rewrite [`file_row_index()`][FileRowIndexFunc] in a [`PhysicalExpr`] to
/// read from a source-provided row-index column.
///
/// `row_index_idx` is the index of `row_index_name` in the schema that the
/// rewritten expression will be evaluated against. The rewrite uses ordinary
/// physical expressions: a [`Column`] that reads the source row-index values
/// wrapped in a [`CastExpr`] that exposes the public `file_row_index: Int64`
/// return field without source-specific extension metadata.
pub fn rewrite_file_row_index_expr(
    expr: Arc<dyn PhysicalExpr>,
    row_index_name: &str,
    row_index_idx: usize,
) -> Result<Arc<dyn PhysicalExpr>> {
    rewrite_scalar_udf::<FileRowIndexFunc, _>(expr, |_| {
        let source = Arc::new(Column::new(row_index_name, row_index_idx));
        let target_field = Arc::new(Field::new("file_row_index", DataType::Int64, true));
        Ok(Arc::new(CastExpr::new_with_target_field(
            source,
            target_field,
            None,
        )))
    })
}

/// Rewrite [`file_row_index()`][FileRowIndexFunc] in pushed [`ProjectionExprs`]
/// to read from a source-provided row-index column.
///
///
/// For example if `row_index_column` is `__datafusion_row_idx` this function rewrites all
/// instances of [`file_row_index()`][FileRowIndexFunc] to
/// `__datafusion_row_index` [`Column`] references.
///
/// `base_projection` is the current projection already pushed into a source.
/// The row-index source column is appended to that base projection if it is not
/// already present. `projection` is rewritten to read from the projected
/// row-index column and then merged on top of the extended base projection.
pub fn rewrite_file_row_index_projection(
    base_projection: &ProjectionExprs,
    projection: &ProjectionExprs,
    row_index_col: &Column,
) -> Result<ProjectionExprs> {
    let mut base_exprs = base_projection.as_ref().to_vec();
    let row_index_projection_idx =
        base_projection.projected_column_position(row_index_col);

    // If the column doesn't exist in the projection yet
    if row_index_projection_idx.is_none() {
        base_exprs.push(ProjectionExpr {
            expr: Arc::new(row_index_col.clone()),
            alias: row_index_col.name().to_owned(),
        });
    }

    let rewritten_projection = projection.clone().try_map_exprs(|expr| {
        rewrite_file_row_index_expr(
            expr,
            row_index_col.name(),
            row_index_projection_idx.unwrap_or(base_exprs.len() - 1),
        )
    })?;

    ProjectionExprs::new(base_exprs).try_merge(&rewritten_projection)
}

/// Rewrite [`input_file_name()`][InputFileNameFunc] in pushed
/// [`ProjectionExprs`] to a per-file [`Literal`] holding `file_name`.
///
/// If the projection contains no [`input_file_name()`][InputFileNameFunc] UDF it
/// is returned unchanged, without allocating the literal or rebuilding the
/// projection tree (the common case for queries that don't use the function).
pub fn rewrite_input_file_name_in_projection(
    projection: ProjectionExprs,
    file_name: &str,
) -> Result<ProjectionExprs> {
    if !projection
        .iter()
        .any(|p| expr_references_scalar_udf::<InputFileNameFunc>(&p.expr))
    {
        return Ok(projection);
    }

    let file_name_lit =
        Arc::new(Literal::new(ScalarValue::Utf8(Some(file_name.to_string()))))
            as Arc<dyn PhysicalExpr>;

    projection.try_map_exprs(|expr| {
        rewrite_scalar_udf::<InputFileNameFunc, _>(expr, |_| {
            Ok(Arc::clone(&file_name_lit))
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::Schema;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{Operator, ScalarUDF};
    use datafusion_physical_expr::expressions;
    use std::collections::HashMap;

    fn file_row_index_expr() -> Arc<dyn PhysicalExpr> {
        Arc::new(ScalarFunctionExpr::new(
            "file_row_index",
            Arc::new(ScalarUDF::from(FileRowIndexFunc::new())),
            vec![],
            Arc::new(Field::new("file_row_index", DataType::Int64, true)),
            Arc::new(ConfigOptions::default()),
        ))
    }

    fn input_file_name_expr() -> Arc<dyn PhysicalExpr> {
        Arc::new(ScalarFunctionExpr::new(
            "input_file_name",
            Arc::new(ScalarUDF::from(InputFileNameFunc::new())),
            vec![],
            Arc::new(Field::new("input_file_name", DataType::Utf8, true)),
            Arc::new(ConfigOptions::default()),
        ))
    }

    #[test]
    fn test_rewrite_scalar_udf_replaces_nested_typed_udf() -> Result<()> {
        let expr = Arc::new(expressions::BinaryExpr::new(
            file_row_index_expr(),
            Operator::Plus,
            expressions::lit(ScalarValue::Int64(Some(1))),
        )) as Arc<dyn PhysicalExpr>;

        let rewritten = rewrite_scalar_udf::<FileRowIndexFunc, _>(expr, |_| {
            Ok(expressions::lit(ScalarValue::Int64(Some(7))))
        })?;

        let binary = rewritten
            .downcast_ref::<expressions::BinaryExpr>()
            .expect("rewritten expression should remain binary");
        assert_eq!(binary.op(), &Operator::Plus);

        let left = binary
            .left()
            .downcast_ref::<Literal>()
            .expect("left side should be rewritten to a literal");
        assert_eq!(left.value(), &ScalarValue::Int64(Some(7)));

        let right = binary
            .right()
            .downcast_ref::<Literal>()
            .expect("right side should remain the original literal");
        assert_eq!(right.value(), &ScalarValue::Int64(Some(1)));
        Ok(())
    }

    #[test]
    fn test_rewrite_input_file_name_in_projection() -> Result<()> {
        let file_name = "part=west/data.parquet";
        let projection = ProjectionExprs::new([
            ProjectionExpr::new(input_file_name_expr(), "file_name"),
            ProjectionExpr::new(
                Arc::new(expressions::BinaryExpr::new(
                    input_file_name_expr(),
                    Operator::Eq,
                    expressions::lit(ScalarValue::Utf8(Some(file_name.to_string()))),
                )),
                "matches_file",
            ),
        ]);

        let rewritten = rewrite_input_file_name_in_projection(projection, file_name)?;
        let rewritten = rewritten.as_ref();
        assert_eq!(rewritten[0].alias, "file_name");
        assert_eq!(rewritten[1].alias, "matches_file");

        let file_name_lit = rewritten[0]
            .expr
            .downcast_ref::<Literal>()
            .expect("input_file_name should rewrite to a literal");
        assert_eq!(
            file_name_lit.value(),
            &ScalarValue::Utf8(Some(file_name.to_string()))
        );

        let binary = rewritten[1]
            .expr
            .downcast_ref::<expressions::BinaryExpr>()
            .expect("nested expression should remain binary");
        assert_eq!(binary.op(), &Operator::Eq);

        let left = binary
            .left()
            .downcast_ref::<Literal>()
            .expect("nested input_file_name should rewrite to a literal");
        assert_eq!(
            left.value(),
            &ScalarValue::Utf8(Some(file_name.to_string()))
        );

        let right = binary
            .right()
            .downcast_ref::<Literal>()
            .expect("comparison literal should remain unchanged");
        assert_eq!(
            right.value(),
            &ScalarValue::Utf8(Some(file_name.to_string()))
        );
        Ok(())
    }

    #[test]
    fn test_rewrite_file_row_index_expr_to_source_column() -> Result<()> {
        let expr = rewrite_file_row_index_expr(
            file_row_index_expr(),
            "__datafusion_file_row_index",
            2,
        )?;

        let cast_expr = expr
            .downcast_ref::<CastExpr>()
            .expect("file row index expression should be a cast");
        assert_eq!(cast_expr.cast_type(), &DataType::Int64);
        let target_field = cast_expr.target_field();
        assert_eq!(target_field.name(), "file_row_index");
        assert_eq!(target_field.data_type(), &DataType::Int64);
        assert!(target_field.is_nullable());
        assert!(target_field.metadata().is_empty());

        let source = cast_expr
            .expr()
            .downcast_ref::<Column>()
            .expect("source column");
        assert_eq!(source.name(), "__datafusion_file_row_index");
        assert_eq!(source.index(), 2);

        let input_schema = Schema::new(vec![
            Field::new("value", DataType::Int64, true),
            Field::new("__datafusion_file_row_index", DataType::Int64, false)
                .with_metadata(HashMap::from([(
                    "source".to_string(),
                    "virtual".to_string(),
                )])),
        ]);
        let return_field = expr.return_field(&input_schema)?;
        assert_eq!(return_field.name(), "file_row_index");
        assert_eq!(return_field.data_type(), &DataType::Int64);
        assert!(return_field.is_nullable());
        assert!(return_field.metadata().is_empty());
        Ok(())
    }
}
