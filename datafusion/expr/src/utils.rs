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

//! Expression utilities

use crate::expr_visitor::{ExprVisitable, ExpressionVisitor, Recursion};
use crate::{Expr, LogicalPlan};
use datafusion_common::{Column, DFField, DFSchema, DataFusionError, Result};
use std::collections::HashSet;

/// Recursively walk a list of expression trees, collecting the unique set of columns
/// referenced in the expression
pub fn exprlist_to_columns(expr: &[Expr], accum: &mut HashSet<Column>) -> Result<()> {
    for e in expr {
        expr_to_columns(e, accum)?;
    }
    Ok(())
}

/// Recursively walk an expression tree, collecting the unique set of column names
/// referenced in the expression
struct ColumnNameVisitor<'a> {
    accum: &'a mut HashSet<Column>,
}

impl ExpressionVisitor for ColumnNameVisitor<'_> {
    fn pre_visit(self, expr: &Expr) -> Result<Recursion<Self>> {
        match expr {
            Expr::Column(qc) => {
                self.accum.insert(qc.clone());
            }
            Expr::ScalarVariable(_, var_names) => {
                self.accum.insert(Column::from_name(var_names.join(".")));
            }
            Expr::Alias(_, _)
            | Expr::Literal(_)
            | Expr::BinaryExpr { .. }
            | Expr::Not(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::Negative(_)
            | Expr::Between { .. }
            | Expr::Case { .. }
            | Expr::Cast { .. }
            | Expr::TryCast { .. }
            | Expr::Sort { .. }
            | Expr::ScalarFunction { .. }
            | Expr::ScalarUDF { .. }
            | Expr::WindowFunction { .. }
            | Expr::AggregateFunction { .. }
            | Expr::GroupingSet(_)
            | Expr::AggregateUDF { .. }
            | Expr::InList { .. }
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::ScalarSubquery(_)
            | Expr::Wildcard
            | Expr::QualifiedWildcard { .. }
            | Expr::GetIndexedField { .. } => {}
        }
        Ok(Recursion::Continue(self))
    }
}

/// Recursively walk an expression tree, collecting the unique set of columns
/// referenced in the expression
pub fn expr_to_columns(expr: &Expr, accum: &mut HashSet<Column>) -> Result<()> {
    expr.accept(ColumnNameVisitor { accum })?;
    Ok(())
}

/// Resolves an `Expr::Wildcard` to a collection of `Expr::Column`'s.
pub fn expand_wildcard(schema: &DFSchema, plan: &LogicalPlan) -> Result<Vec<Expr>> {
    let using_columns = plan.using_columns()?;
    let columns_to_skip = using_columns
        .into_iter()
        // For each USING JOIN condition, only expand to one column in projection
        .flat_map(|cols| {
            let mut cols = cols.into_iter().collect::<Vec<_>>();
            // sort join columns to make sure we consistently keep the same
            // qualified column
            cols.sort();
            cols.into_iter().skip(1)
        })
        .collect::<HashSet<_>>();

    if columns_to_skip.is_empty() {
        Ok(schema
            .fields()
            .iter()
            .map(|f| Expr::Column(f.qualified_column()))
            .collect::<Vec<Expr>>())
    } else {
        Ok(schema
            .fields()
            .iter()
            .filter_map(|f| {
                let col = f.qualified_column();
                if !columns_to_skip.contains(&col) {
                    Some(Expr::Column(col))
                } else {
                    None
                }
            })
            .collect::<Vec<Expr>>())
    }
}

/// Resolves an `Expr::Wildcard` to a collection of qualified `Expr::Column`'s.
pub fn expand_qualified_wildcard(
    qualifier: &str,
    schema: &DFSchema,
    plan: &LogicalPlan,
) -> Result<Vec<Expr>> {
    let qualified_fields: Vec<DFField> = schema
        .fields_with_qualified(qualifier)
        .into_iter()
        .cloned()
        .collect();
    if qualified_fields.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "Invalid qualifier {}",
            qualifier
        )));
    }
    let qualifier_schema =
        DFSchema::new_with_metadata(qualified_fields, schema.metadata().clone())?;
    expand_wildcard(&qualifier_schema, plan)
}
