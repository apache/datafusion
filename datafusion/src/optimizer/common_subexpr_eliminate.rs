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
use std::sync::Arc;

use arrow::datatypes::DataType;

use crate::error::Result;
use crate::execution::context::ExecutionProps;
use crate::logical_plan::{
    col, DFField, DFSchema, Expr, ExprRewriter, ExpressionVisitor, LogicalPlan,
    Recursion, RewriteRecursion,
};
use crate::optimizer::optimizer::OptimizerRule;

pub struct CommonSubexprEliminate {}

impl OptimizerRule for CommonSubexprEliminate {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        execution_props: &ExecutionProps,
    ) -> Result<LogicalPlan> {
        optimize(plan, execution_props)
    }

    fn name(&self) -> &str {
        "common_sub_expression_eliminate"
    }
}

impl CommonSubexprEliminate {}

fn optimize(plan: &LogicalPlan, execution_props: &ExecutionProps) -> Result<LogicalPlan> {
    let mut expr_set = ExprSet::new();
    let mut addr_map = ExprAddrToId::new();
    let mut affected_id = HashSet::new();

    match plan {
        LogicalPlan::Projection {
            expr,
            input,
            schema,
        } => {
            let mut arrays = vec![];
            for e in expr {
                let data_type = e.get_type(input.schema())?;
                let mut id_array = vec![];
                expr_to_identifier(
                    e,
                    &mut expr_set,
                    &mut addr_map,
                    &mut id_array,
                    data_type,
                )?;
                arrays.push(id_array);
            }

            return optimize(input, execution_props);
        }
        LogicalPlan::Filter { predicate, input } => {
            let data_type = predicate.get_type(input.schema())?;
            let mut id_array = vec![];
            expr_to_identifier(
                predicate,
                &mut expr_set,
                &mut addr_map,
                &mut id_array,
                data_type,
            )?;

            return optimize(input, execution_props);
        }
        LogicalPlan::Window {
            input,
            window_expr,
            schema,
        } => {
            let mut arrays = vec![];
            for e in window_expr {
                let data_type = e.get_type(input.schema())?;
                let mut id_array = vec![];
                expr_to_identifier(
                    e,
                    &mut expr_set,
                    &mut addr_map,
                    &mut id_array,
                    data_type,
                )?;
                arrays.push(id_array);
            }

            return optimize(input, execution_props);
        }
        LogicalPlan::Aggregate {
            input,
            group_expr,
            aggr_expr,
            schema,
        } => {
            // collect id
            let mut group_arrays = vec![];
            for e in group_expr {
                let data_type = e.get_type(input.schema())?;
                let mut id_array = vec![];
                expr_to_identifier(
                    e,
                    &mut expr_set,
                    &mut addr_map,
                    &mut id_array,
                    data_type,
                )?;
                group_arrays.push(id_array);
            }
            let mut aggr_arrays = vec![];
            for e in aggr_expr {
                let data_type = e.get_type(input.schema())?;
                let mut id_array = vec![];
                expr_to_identifier(
                    e,
                    &mut expr_set,
                    &mut addr_map,
                    &mut id_array,
                    data_type,
                )?;
                aggr_arrays.push(id_array);
            }

            // rewrite
            let new_group_expr = group_expr
                .into_iter()
                .cloned()
                .zip(group_arrays.into_iter())
                .map(|(expr, id_array)| {
                    replace_common_expr(expr, &id_array, &mut expr_set, &mut affected_id)
                })
                .collect::<Result<Vec<_>>>()?;
            let new_aggr_expr = aggr_expr
                .into_iter()
                .cloned()
                .zip(aggr_arrays.into_iter())
                .map(|(expr, id_array)| {
                    replace_common_expr(expr, &id_array, &mut expr_set, &mut affected_id)
                })
                .collect::<Result<Vec<_>>>()?;

            let mut new_input = optimize(input, execution_props)?;
            if !affected_id.is_empty() {
                new_input =
                    build_project_plan(new_input, affected_id, &expr_set, schema)?;
            }

            return Ok(LogicalPlan::Aggregate {
                input: Arc::new(new_input),
                group_expr: new_group_expr,
                aggr_expr: new_aggr_expr,
                schema: schema.clone(),
            });
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
    Ok(plan.clone())
}

fn build_project_plan(
    input: LogicalPlan,
    affected_id: HashSet<Identifier>,
    expr_set: &ExprSet,
    schema: &DFSchema,
) -> Result<LogicalPlan> {
    let mut project_exprs = vec![];
    let mut fields = vec![];

    for id in affected_id {
        let (expr, _, _, _, data_type) = expr_set.get(&id).unwrap();
        // todo: check `nullable`
        fields.push(DFField::new(None, &id, data_type.clone(), true));
        project_exprs.push(expr.clone());
    }

    Ok(LogicalPlan::Projection {
        expr: project_exprs,
        input: Arc::new(input),
        schema: Arc::new(DFSchema::new(fields)?),
    })
}

// Helper struct & func

/// A map from expression's identifier to tuple including
/// - the expression itself (cloned)
/// - a hash set contains all addresses with the same identifier.
/// - counter
/// - A alternative plan.
pub type ExprSet =
    HashMap<Identifier, (Expr, HashSet<*const Expr>, usize, Option<()>, DataType)>;

pub type ExprAddrToId = HashMap<*const Expr, Identifier>;

/// Identifier type. Current implementation use descript of a expression (type String) as
/// Identifier.
///
/// A Identifier should (ideally) be "hashable", "accumulatable", "Eq", "collisionless"
/// (as low probability as possible).
///
/// Since a identifier is likely to be copied many times, it is better that a identifier
/// is small or "copy". otherwise some kinds of reference count is needed.
pub type Identifier = String;

/// Go through an expression tree and generate identifier.
/// `Expr` without sub-expr (column, literal etc.) will not have identifier
/// because they should not be recognized as common sub-expr.
struct ExprIdentifierVisitor<'a> {
    // param
    expr_set: &'a mut ExprSet,
    addr_map: &'a mut ExprAddrToId,
    /// series number (usize) and identifier.
    id_array: &'a mut Vec<(usize, Identifier)>,
    data_type: DataType,

    // inner states
    visit_stack: Vec<Item>,
    /// increased in pre_visit, start from 0.
    node_count: usize,
    /// increased in post_visit, start from 1.
    post_visit_number: usize,
}

enum Item {
    /// `usize` is the monotone increasing series number assigned in pre_visit().
    /// Starts from 0. Is used to index ithe dentifier array `id_array` in post_visit().
    EnterMark(usize),
    /// Accumulated identifier of sub expression.
    ExprItem(Identifier),
}

impl ExprIdentifierVisitor<'_> {
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

    fn pop_enter_mark(&mut self) -> (usize, Identifier) {
        let mut desc = String::new();

        while let Some(item) = self.visit_stack.pop() {
            match item {
                Item::EnterMark(idx) => {
                    return (idx, desc);
                }
                Item::ExprItem(s) => {
                    desc.push_str(&s);
                }
            }
        }

        (0, desc)
    }
}

impl ExpressionVisitor for ExprIdentifierVisitor<'_> {
    fn pre_visit(mut self, _expr: &Expr) -> Result<Recursion<Self>> {
        self.visit_stack.push(Item::EnterMark(self.node_count));
        self.node_count += 1;
        // put placeholder
        self.id_array.push((0, "".to_string()));
        Ok(Recursion::Continue(self))
    }

    fn post_visit(mut self, expr: &Expr) -> Result<Self> {
        self.post_visit_number += 1;

        let (idx, sub_expr_desc) = self.pop_enter_mark();
        // skip exprs should not be recognize.
        if matches!(
            expr,
            Expr::Literal(..)
                | Expr::Column(..)
                | Expr::ScalarVariable(..)
                | Expr::Wildcard
        ) {
            self.id_array[idx].0 = self.post_visit_number;
            let desc = Self::desc_expr(expr);
            self.visit_stack.push(Item::ExprItem(desc));
            return Ok(self);
        }
        let mut desc = Self::desc_expr(expr);
        desc.push_str(&sub_expr_desc);

        self.id_array[idx] = (self.post_visit_number, desc.clone());
        self.visit_stack.push(Item::ExprItem(desc.clone()));
        let data_type = self.data_type.clone();
        self.expr_set
            .entry(desc.clone())
            .or_insert_with(|| (expr.clone(), HashSet::new(), 0, None, data_type))
            .2 += 1;
        self.addr_map.insert(expr as *const Expr, desc);
        Ok(self)
    }
}

/// Go through an expression tree and generate identity.
fn expr_to_identifier(
    expr: &Expr,
    expr_set: &mut ExprSet,
    addr_map: &mut ExprAddrToId,
    id_array: &mut Vec<(usize, Identifier)>,
    data_type: DataType,
) -> Result<()> {
    expr.accept(ExprIdentifierVisitor {
        expr_set,
        addr_map,
        id_array,
        data_type,
        visit_stack: vec![],
        node_count: 0,
        post_visit_number: 0,
    })?;

    Ok(())
}

struct CommonSubexprRewriter<'a> {
    expr_set: &'a mut ExprSet,
    id_array: &'a Vec<(usize, Identifier)>,
    affected_id: &'a mut HashSet<Identifier>,

    max_series_number: usize,
    curr_index: usize,
}

impl CommonSubexprRewriter<'_> {}

impl ExprRewriter for CommonSubexprRewriter<'_> {
    fn pre_visit(&mut self, _: &Expr) -> Result<RewriteRecursion> {
        if self.curr_index >= self.id_array.len()
            || self.max_series_number > self.id_array[self.curr_index].0
        {
            return Ok(RewriteRecursion::Stop);
        }

        let curr_id = &self.id_array[self.curr_index].1;
        // skip `Expr`s without identifier (empty identifier).
        if curr_id.is_empty() {
            self.curr_index += 1;
            return Ok(RewriteRecursion::Continue);
        }
        let (stored_expr, somewhat_set, counter, another_thing, _) =
            self.expr_set.get(curr_id).unwrap();
        if *counter > 1 {
            self.affected_id.insert(curr_id.clone());
            Ok(RewriteRecursion::Mutate)
        } else {
            self.curr_index += 1;
            Ok(RewriteRecursion::Continue)
        }
    }

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        if self.curr_index >= self.id_array.len() {
            return Ok(expr);
        }

        let (series_number, id) = &self.id_array[self.curr_index];
        if *series_number < self.max_series_number || id.is_empty() {
            return Ok(expr);
        }
        self.max_series_number = *series_number;

        // step index, skip all sub-node (which has smaller series number).
        self.curr_index += 1;
        while self.curr_index < self.id_array.len()
            && *series_number > self.id_array[self.curr_index].0
        {
            self.curr_index += 1;
        }

        Ok(col(id))
    }
}

fn replace_common_expr(
    expr: Expr,
    id_array: &Vec<(usize, Identifier)>,
    expr_set: &mut ExprSet,
    affected_id: &mut HashSet<Identifier>,
) -> Result<Expr> {
    expr.rewrite(&mut CommonSubexprRewriter {
        expr_set,
        id_array,
        affected_id,
        max_series_number: 0,
        curr_index: 0,
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::logical_plan::{binary_expr, col, lit, sum, LogicalPlanBuilder, Operator};
    use crate::test::*;

    #[test]
    #[ignore]
    fn dev_driver_tpch_q1_simplified() -> Result<()> {
        // SQL:
        //  select
        //      sum(a * (1 - b)),
        //      sum(a * (1 - b) * (1 + c))
        //  from T

        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![],
                vec![
                    sum(binary_expr(
                        col("a"),
                        Operator::Multiply,
                        binary_expr(lit(1), Operator::Minus, col("b")),
                    )),
                    sum(binary_expr(
                        binary_expr(
                            col("a"),
                            Operator::Multiply,
                            binary_expr(lit(1), Operator::Minus, col("b")),
                        ),
                        Operator::Multiply,
                        binary_expr(lit(1), Operator::Plus, col("c")),
                    )),
                ],
            )?
            .build()?;

        let optimizer = CommonSubexprEliminate {};
        let new_plan = optimizer.optimize(&plan, &ExecutionProps::new()).unwrap();

        Ok(())
    }
}
