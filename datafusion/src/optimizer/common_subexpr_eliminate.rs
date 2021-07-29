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

//! Eliminate common sub-expression.

use std::collections::{HashMap, HashSet};

use crate::error::Result;
use crate::execution::context::ExecutionProps;
use crate::logical_plan::{Expr, ExpressionVisitor, LogicalPlan, Recursion};
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::utils::{self};

pub struct CommonSubexprEliminate {}

impl OptimizerRule for CommonSubexprEliminate {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        execution_props: &ExecutionProps,
    ) -> Result<LogicalPlan> {
        todo!()
    }

    fn name(&self) -> &str {
        "common_sub_expression_eliminate"
    }
}

impl CommonSubexprEliminate {}

fn optimize(plan: &LogicalPlan, execution_props: &ExecutionProps) -> Result<LogicalPlan> {
    let mut expr_set = ExprSet::new();
    let mut addr_map = ExprAddrToId::new();
    let mut affected: Vec<()> = vec![];

    match plan {
        LogicalPlan::Projection {
            expr,
            input,
            schema,
        } => {
            for e in expr {
                expr_to_identifier(e, &mut expr_set, &mut addr_map)?;
            }
        }
        LogicalPlan::Filter { predicate, input } => {
            expr_to_identifier(predicate, &mut expr_set, &mut addr_map)?;
        }
        LogicalPlan::Window {
            input,
            window_expr,
            schema,
        } => {
            for e in window_expr {
                expr_to_identifier(e, &mut expr_set, &mut addr_map)?;
            }
        }
        LogicalPlan::Aggregate {
            input,
            group_expr,
            aggr_expr,
            schema,
        } => {
            for e in group_expr {
                expr_to_identifier(e, &mut expr_set, &mut addr_map)?;
            }

            for e in aggr_expr {
                expr_to_identifier(e, &mut expr_set, &mut addr_map)?;
            }
        }
        LogicalPlan::Sort { expr, input } => {}
        LogicalPlan::Join { .. }
        | LogicalPlan::CrossJoin { .. }
        | LogicalPlan::Repartition { .. }
        | LogicalPlan::Union { .. }
        | LogicalPlan::TableScan { .. }
        | LogicalPlan::EmptyRelation { .. }
        | LogicalPlan::Limit { .. }
        | LogicalPlan::CreateExternalTable { .. }
        | LogicalPlan::Explain { .. }
        | LogicalPlan::Extension { .. } => {}
    }
    todo!()
}

/// Go through an expression tree and generate identity.
struct ExprIdentifierVisitor<'a> {
    visit_stack: Vec<Item>,
    expr_set: &'a mut ExprSet,
    addr_map: &'a mut ExprAddrToId,
}

// Helper struct & func

/// A map from expression's identifier to tuple including
/// - the expression itself (cloned)
/// - a hash set contains all addresses with the same identifier.
/// - counter
/// - A alternative plan.
pub type ExprSet = HashMap<String, (Expr, HashSet<*const Expr>, usize, Option<()>)>;

pub type ExprAddrToId = HashMap<*const Expr, String>;

enum Item {
    EnterMark,
    ExprItem(String),
}

impl<'a> ExprIdentifierVisitor<'a> {
    fn desc_expr(expr: &Expr) -> String {
        let mut desc = String::new();
        match expr {
            Expr::Column(column) => {
                desc.push_str("Column-");
                desc.push_str(&column.flat_name());
            }
            Expr::ScalarVariable(var_names) => {
                // accum.insert(Column::from_name(var_names.join(".")));
                desc.push_str("ScalarVariable-");
                desc.push_str(&var_names.join("."));
            }
            Expr::Alias(_, alias) => {
                desc.push_str("Alias-");
                desc.push_str(alias);
            }
            Expr::Literal(value) => {
                desc.push_str("Literal");
                desc.push_str(&value.to_string());
            }
            Expr::BinaryExpr { op, .. } => {
                desc.push_str("BinaryExpr-");
                desc.push_str(&op.to_string());
            }
            Expr::Not(_) => {}
            Expr::IsNotNull(_) => {}
            Expr::IsNull(_) => {}
            Expr::Negative(_) => {}
            Expr::Between { .. } => {}
            Expr::Case { .. } => {}
            Expr::Cast { .. } => {}
            Expr::TryCast { .. } => {}
            Expr::Sort { .. } => {}
            Expr::ScalarFunction { .. } => {}
            Expr::ScalarUDF { .. } => {}
            Expr::WindowFunction { .. } => {}
            Expr::AggregateFunction { fun, distinct, .. } => {
                desc.push_str("AggregateFunction-");
                desc.push_str(&fun.to_string());
                desc.push_str(&distinct.to_string());
            }
            Expr::AggregateUDF { .. } => {}
            Expr::InList { .. } => {}
            Expr::Wildcard => {}
        }

        desc
    }

    fn pop_enter_mark(&mut self) -> String {
        let mut desc = String::new();

        while let Some(item) = self.visit_stack.pop() {
            match item {
                Item::EnterMark => return desc,
                Item::ExprItem(s) => {
                    desc.push_str(&s);
                }
            }
        }

        desc
    }
}

impl ExpressionVisitor for ExprIdentifierVisitor<'_> {
    fn pre_visit(mut self, _expr: &Expr) -> Result<Recursion<Self>> {
        self.visit_stack.push(Item::EnterMark);
        Ok(Recursion::Continue(self))
    }

    fn post_visit(mut self, expr: &Expr) -> Result<Self> {
        let sub_expr_desc = self.pop_enter_mark();
        let mut desc = Self::desc_expr(expr);
        desc.push_str(&sub_expr_desc);

        self.visit_stack.push(Item::ExprItem(desc.clone()));
        self.expr_set
            .entry(desc.clone())
            .or_insert_with(|| (expr.clone(), HashSet::new(), 0, None))
            .2 += 1;
        self.addr_map.insert(expr as *const Expr, desc);
        Ok(self)
    }
}

/// Go through an expression tree and generate identity.
pub fn expr_to_identifier(
    expr: &Expr,
    expr_set: &mut ExprSet,
    addr_map: &mut ExprAddrToId,
) -> Result<()> {
    expr.accept(ExprIdentifierVisitor {
        visit_stack: vec![],
        expr_set,
        addr_map,
    })?;

    Ok(())
}
