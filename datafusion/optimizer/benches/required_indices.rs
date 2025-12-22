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

//! Microbench comparing old vs new required index collection paths.

use std::hint::black_box;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchemaRef, Result, TableReference, ToDFSchema};
use datafusion_expr::expr::Exists;
use datafusion_expr::logical_plan::{LogicalPlan, LogicalPlanBuilder, Subquery};
use datafusion_expr::{Expr, col, lit};

fn collect_old(plan: &LogicalPlan, schema: &DFSchemaRef) -> Result<Vec<usize>> {
    let mut indices = Vec::new();
    plan.apply_expressions(|expr| {
        let mut cols: std::collections::HashSet<&Column> = expr.column_refs();
        outer_columns(expr, &mut cols);
        for col in cols.iter() {
            if let Some(idx) = schema.maybe_index_of_column(col) {
                indices.push(idx);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    indices.sort_unstable();
    indices.dedup();
    Ok(indices)
}

fn collect_new(plan: &LogicalPlan, schema: &DFSchemaRef) -> Result<Vec<usize>> {
    let mut indices = Vec::new();
    plan.apply_expressions(|expr| {
        collect_expr_indices(&mut indices, schema, expr);
        Ok(TreeNodeRecursion::Continue)
    })?;
    indices.sort_unstable();
    indices.dedup();
    Ok(indices)
}

fn bench_required_indices(c: &mut Criterion) {
    let (wide_plan, wide_schema) = wide_projection_plan(200).unwrap();
    let (outer_plan, outer_schema) = outer_ref_plan().unwrap();

    let mut group = c.benchmark_group("required_indices");

    group.bench_function("new_wide", |b| {
        b.iter(|| black_box(collect_new(&wide_plan, &wide_schema).unwrap()))
    });
    group.bench_function("old_wide", |b| {
        b.iter(|| black_box(collect_old(&wide_plan, &wide_schema).unwrap()))
    });
    group.bench_function("new_outer_ref", |b| {
        b.iter(|| black_box(collect_new(&outer_plan, &outer_schema).unwrap()))
    });
    group.bench_function("old_outer_ref", |b| {
        b.iter(|| black_box(collect_old(&outer_plan, &outer_schema).unwrap()))
    });

    group.finish();
}

fn wide_projection_plan(num_exprs: usize) -> Result<(LogicalPlan, DFSchemaRef)> {
    let fields: Vec<Field> = (0..num_exprs)
        .map(|i| Field::new(format!("c{i}"), DataType::Int32, false))
        .collect();
    let schema = Schema::new(fields);
    let df_schema = Arc::new(schema.to_dfschema()?);

    let exprs: Vec<Expr> = (0..num_exprs)
        .map(|i| col(format!("c{i}")) + lit(i as i32))
        .collect();

    let base = LogicalPlan::EmptyRelation(datafusion_expr::EmptyRelation {
        produce_one_row: true,
        schema: Arc::clone(&df_schema),
    });

    let plan = LogicalPlanBuilder::from(base).project(exprs)?.build()?;

    Ok((plan, df_schema))
}

fn outer_ref_plan() -> Result<(LogicalPlan, DFSchemaRef)> {
    let fields: Vec<Field> = (0..10)
        .map(|i| Field::new(format!("c{i}"), DataType::Int32, false))
        .collect();
    let schema = Schema::new(fields);
    let df_schema = Arc::new(schema.to_dfschema()?);

    let outer_col = Column::new(None::<TableReference>, "c0");

    let subquery_input = LogicalPlan::EmptyRelation(datafusion_expr::EmptyRelation {
        produce_one_row: true,
        schema: Arc::clone(&df_schema),
    });
    let subquery_plan = LogicalPlanBuilder::from(subquery_input)
        .filter(Expr::Column(outer_col.clone()).eq(lit(1)))?
        .build()?;

    let exists_expr = Expr::Exists(Exists {
        subquery: Subquery {
            subquery: Arc::new(subquery_plan),
            outer_ref_columns: vec![Expr::Column(outer_col.clone())],
            spans: Default::default(),
        },
        negated: false,
    });

    let base = LogicalPlan::EmptyRelation(datafusion_expr::EmptyRelation {
        produce_one_row: true,
        schema: Arc::clone(&df_schema),
    });
    let plan = LogicalPlanBuilder::from(base)
        .project(vec![Expr::Column(outer_col.clone())])?
        .filter(exists_expr)?
        .build()?;

    Ok((plan, df_schema))
}

fn collect_expr_indices(indices: &mut Vec<usize>, schema: &DFSchemaRef, expr: &Expr) {
    expr.apply(|expr| {
        match expr {
            Expr::Column(col) | Expr::OuterReferenceColumn(_, col) => {
                push_column_index(indices, schema, col);
            }
            Expr::ScalarSubquery(subquery) => {
                collect_outer_ref_exprs(indices, schema, &subquery.outer_ref_columns);
            }
            Expr::Exists(exists) => {
                collect_outer_ref_exprs(
                    indices,
                    schema,
                    &exists.subquery.outer_ref_columns,
                );
            }
            Expr::InSubquery(insubquery) => {
                collect_outer_ref_exprs(
                    indices,
                    schema,
                    &insubquery.subquery.outer_ref_columns,
                );
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("traversal should not fail");
}

fn collect_outer_ref_exprs(
    indices: &mut Vec<usize>,
    schema: &DFSchemaRef,
    exprs: &[Expr],
) {
    exprs.iter().for_each(|outer_expr| {
        outer_expr
            .apply(|expr| {
                match expr {
                    Expr::Column(col) | Expr::OuterReferenceColumn(_, col) => {
                        push_column_index(indices, schema, col);
                    }
                    Expr::ScalarSubquery(subquery) => collect_outer_ref_exprs(
                        indices,
                        schema,
                        &subquery.outer_ref_columns,
                    ),
                    Expr::Exists(exists) => collect_outer_ref_exprs(
                        indices,
                        schema,
                        &exists.subquery.outer_ref_columns,
                    ),
                    Expr::InSubquery(insubquery) => collect_outer_ref_exprs(
                        indices,
                        schema,
                        &insubquery.subquery.outer_ref_columns,
                    ),
                    _ => {}
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .expect("outer reference traversal should not fail");
    });
}

fn push_column_index(indices: &mut Vec<usize>, schema: &DFSchemaRef, col: &Column) {
    if let Some(idx) = schema.maybe_index_of_column(col) {
        indices.push(idx);
    }
}

fn outer_columns<'a>(
    expr: &'a Expr,
    columns: &mut std::collections::HashSet<&'a Column>,
) {
    expr.apply(|expr| {
        match expr {
            Expr::OuterReferenceColumn(_, col) => {
                columns.insert(col);
            }
            Expr::ScalarSubquery(subquery) => {
                outer_columns_helper_multi(&subquery.outer_ref_columns, columns);
            }
            Expr::Exists(exists) => {
                outer_columns_helper_multi(&exists.subquery.outer_ref_columns, columns);
            }
            Expr::InSubquery(insubquery) => {
                outer_columns_helper_multi(
                    &insubquery.subquery.outer_ref_columns,
                    columns,
                );
            }
            _ => {}
        };
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
}

fn outer_columns_helper_multi<'a, 'b>(
    exprs: impl IntoIterator<Item = &'a Expr>,
    columns: &'b mut std::collections::HashSet<&'a Column>,
) {
    exprs.into_iter().for_each(|e| outer_columns(e, columns));
}

criterion_group!(benches, bench_required_indices);
criterion_main!(benches);
