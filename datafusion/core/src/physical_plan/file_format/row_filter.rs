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

use arrow::array::{Array, BooleanArray};
use arrow::compute::prep_null_mask_filter;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::RecordBatch;
use datafusion_common::{Column, Result, ScalarValue, ToDFSchema};
use datafusion_expr::expr_rewriter::{ExprRewritable, ExprRewriter, RewriteRecursion};

use datafusion_expr::{Expr, Operator};
use datafusion_physical_expr::execution_props::ExecutionProps;
use datafusion_physical_expr::{create_physical_expr, PhysicalExpr};
use parquet::arrow::arrow_reader::{ArrowPredicate, RowFilter};
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetMetaData;
use std::sync::Arc;

/// A predicate which can be passed to `ParquetRecordBatchStream` to perform row-level
/// filtering during parquet decoding.
#[derive(Debug)]
pub(crate) struct DatafusionArrowPredicate {
    physical_expr: Arc<dyn PhysicalExpr>,
    projection: ProjectionMask,
}

impl DatafusionArrowPredicate {
    pub fn try_new(
        candidate: FilterCandidate,
        schema: &Schema,
        metadata: &ParquetMetaData,
    ) -> Result<Self> {
        let props = ExecutionProps::default();

        let schema = schema.project(&candidate.projection)?;
        let df_schema = schema.clone().to_dfschema()?;

        let physical_expr =
            create_physical_expr(&candidate.expr, &df_schema, &schema, &props)?;

        Ok(Self {
            physical_expr,
            projection: ProjectionMask::roots(
                metadata.file_metadata().schema_descr(),
                candidate.projection,
            ),
        })
    }
}

impl ArrowPredicate for DatafusionArrowPredicate {
    fn projection(&self) -> &ProjectionMask {
        &self.projection
    }

    fn evaluate(&mut self, batch: RecordBatch) -> ArrowResult<BooleanArray> {
        match self
            .physical_expr
            .evaluate(&batch)
            .map(|v| v.into_array(batch.num_rows()))
        {
            Ok(array) => {
                if let Some(mask) = array.as_any().downcast_ref::<BooleanArray>() {
                    let mask = match mask.null_count() {
                        0 => BooleanArray::from(mask.data().clone()),
                        _ => prep_null_mask_filter(mask),
                    };

                    Ok(mask)
                } else {
                    Err(ArrowError::ComputeError(
                        "Unexpected result of predicate evaluation, expected BooleanArray".to_owned(),
                    ))
                }
            }
            Err(e) => Err(ArrowError::ComputeError(format!(
                "Error evaluating filter predicate: {:?}",
                e
            ))),
        }
    }
}

/// A candidate expression for creating a `RowFilter` contains the
/// expression as well as data to estimate the cost of evaluating
/// the resulting expression.
pub(crate) struct FilterCandidate {
    expr: Expr,
    required_bytes: usize,
    can_use_index: bool,
    projection: Vec<usize>,
}

/// Helper to build a `FilterCandidate`. This will do several things
/// 1. Determine the columns required to evaluate the expression
/// 2. Calculate data required to estimate the cost of evaluating the filter
/// 3. Rewrite column expressions in the predicate which reference columns not in the particular file schema.
///    This is relevant in the case where we have determined the table schema by merging all individual file schemas
///    and any given file may or may not contain all columns in the merged schema. If a particular column is not present
///    we replace the column expression with a literal expression that produces a null value.
struct FilterCandidateBuilder<'a> {
    expr: Expr,
    file_schema: &'a Schema,
    table_schema: &'a Schema,
    required_column_indices: Vec<usize>,
    non_primitive_columns: bool,
    projected_columns: bool,
}

impl<'a> FilterCandidateBuilder<'a> {
    pub fn new(expr: Expr, file_schema: &'a Schema, table_schema: &'a Schema) -> Self {
        Self {
            expr,
            file_schema,
            table_schema,
            required_column_indices: vec![],
            non_primitive_columns: false,
            projected_columns: false,
        }
    }

    pub fn build(
        mut self,
        metadata: &ParquetMetaData,
    ) -> Result<Option<FilterCandidate>> {
        let expr = self.expr.clone();
        let expr = expr.rewrite(&mut self)?;

        if self.non_primitive_columns || self.projected_columns {
            Ok(None)
        } else {
            let required_bytes =
                size_of_columns(&self.required_column_indices, metadata)?;
            let can_use_index = columns_sorted(&self.required_column_indices, metadata)?;

            Ok(Some(FilterCandidate {
                expr,
                required_bytes,
                can_use_index,
                projection: self.required_column_indices,
            }))
        }
    }
}

impl<'a> ExprRewriter for FilterCandidateBuilder<'a> {
    fn pre_visit(&mut self, expr: &Expr) -> Result<RewriteRecursion> {
        if let Expr::Column(column) = expr {
            if let Ok(idx) = self.file_schema.index_of(&column.name) {
                self.required_column_indices.push(idx);

                if !is_primitive_field(self.file_schema.field(idx)) {
                    self.non_primitive_columns = true;
                }
            } else if self.table_schema.index_of(&column.name).is_err() {
                // If the column does not exist in the (un-projected) table schema then
                // it must be a projected column.
                self.projected_columns = true;
            }
        }
        Ok(RewriteRecursion::Continue)
    }

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        if let Expr::Column(Column { name, .. }) = &expr {
            if self.file_schema.field_with_name(name).is_err() {
                return Ok(Expr::Literal(ScalarValue::Null));
            }
        }

        Ok(expr)
    }
}

/// Calculate the total compressed size of all `Column's required for
/// predicate `Expr`. This should represent the total amount of file IO
/// required to evaluate the predicate.
fn size_of_columns(columns: &[usize], metadata: &ParquetMetaData) -> Result<usize> {
    let mut total_size = 0;
    let row_groups = metadata.row_groups();
    for idx in columns {
        for rg in row_groups.iter() {
            total_size += rg.column(*idx).compressed_size() as usize;
        }
    }

    Ok(total_size)
}

/// For a given set of `Column`s required for predicate `Expr` determine whether all
/// columns are sorted. Sorted columns may be queried more efficiently in the presence of
/// a PageIndex.
fn columns_sorted(_columns: &[usize], _metadata: &ParquetMetaData) -> Result<bool> {
    // TODO How do we know this?
    Ok(false)
}

/// Build a [`RowFilter`] from the given predicate `Expr`
pub fn build_row_filter(
    expr: Expr,
    file_schema: &Schema,
    table_schema: &Schema,
    metadata: &ParquetMetaData,
    reorder_predicates: bool,
) -> Result<Option<RowFilter>> {
    let predicates = disjoin_filters(expr);

    let mut candidates: Vec<FilterCandidate> = predicates
        .into_iter()
        .flat_map(|expr| {
            if let Ok(candidate) =
                FilterCandidateBuilder::new(expr, file_schema, table_schema)
                    .build(metadata)
            {
                candidate
            } else {
                None
            }
        })
        .collect();

    if candidates.is_empty() {
        Ok(None)
    } else if reorder_predicates {
        candidates.sort_by_key(|c| c.required_bytes);

        let (indexed_candidates, other_candidates): (Vec<_>, Vec<_>) =
            candidates.into_iter().partition(|c| c.can_use_index);

        let mut filters: Vec<Box<dyn ArrowPredicate>> = vec![];

        for candidate in indexed_candidates {
            let filter =
                DatafusionArrowPredicate::try_new(candidate, file_schema, metadata)?;

            filters.push(Box::new(filter));
        }

        for candidate in other_candidates {
            let filter =
                DatafusionArrowPredicate::try_new(candidate, file_schema, metadata)?;

            filters.push(Box::new(filter));
        }

        Ok(Some(RowFilter::new(filters)))
    } else {
        let mut filters: Vec<Box<dyn ArrowPredicate>> = vec![];
        for candidate in candidates {
            let filter =
                DatafusionArrowPredicate::try_new(candidate, file_schema, metadata)?;

            filters.push(Box::new(filter));
        }

        Ok(Some(RowFilter::new(filters)))
    }
}

fn is_primitive_field(field: &Field) -> bool {
    !matches!(
        field.data_type(),
        DataType::List(_)
            | DataType::FixedSizeList(_, _)
            | DataType::LargeList(_)
            | DataType::Struct(_)
            | DataType::Union(_, _, _)
            | DataType::Map(_, _)
    )
}

/// Take combined filter (multiple boolean expressions ANDed together)
/// and break down into distinct filters. This should be the inverse of
/// `datafusion_expr::expr_fn::combine_filters`
fn disjoin_filters(combined_expr: Expr) -> Vec<Expr> {
    match combined_expr {
        Expr::BinaryExpr {
            left,
            op: Operator::And,
            right,
        } => {
            let mut exprs = disjoin_filters(*left);
            exprs.extend(disjoin_filters(*right));
            exprs
        }
        expr => {
            vec![expr]
        }
    }
}

#[cfg(test)]
mod test {
    use crate::physical_plan::file_format::row_filter::disjoin_filters;
    use arrow::datatypes::{DataType, Field, Schema};

    use datafusion_expr::{and, col, lit, Expr};

    fn assert_predicates(actual: Vec<Expr>, expected: Vec<Expr>) {
        assert_eq!(
            actual.len(),
            expected.len(),
            "Predicates are not equal, found {} predicates but expected {}",
            actual.len(),
            expected.len()
        );

        for expr in expected.into_iter() {
            assert!(
                actual.contains(&expr),
                "Predicates are not equal, predicate {:?} not found in {:?}",
                expr,
                actual
            );
        }
    }

    #[test]
    fn test_disjoin() {
        let _schema = Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Utf8, true),
        ]);

        let expr = col("a").eq(lit("s"));
        let actual = disjoin_filters(expr);

        assert_predicates(actual, vec![col("a").eq(lit("s"))]);
    }

    #[test]
    fn test_disjoin_complex() {
        let _schema = Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Utf8, true),
        ]);

        let expr = and(col("a"), col("b"));
        let actual = disjoin_filters(expr);

        assert_predicates(actual, vec![col("a"), col("b")]);

        let expr = col("a").and(col("b")).or(col("c"));
        let actual = disjoin_filters(expr.clone());

        assert_predicates(actual, vec![expr]);
    }
}
