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

use crate::logical_plan::producer::SubstraitProducer;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchemaRef, not_impl_err};
use datafusion::logical_expr::expr::{Exists, InSubquery, SetComparison};
use datafusion::logical_expr::{Case, Expr, LogicalPlan};
use substrait::proto::Expression;
use substrait::proto::expression::if_then::IfClause;
use substrait::proto::expression::{IfThen, RexType};

pub fn from_case(
    producer: &mut impl SubstraitProducer,
    case: &Case,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<Expression> {
    let Case {
        expr,
        when_then_expr,
        else_expr,
    } = case;

    // Substrait's `IfThen` has no notion of a base expression: every `IfClause`
    // is a standalone boolean condition. A `CASE <base> WHEN <value> THEN ...`
    // is therefore emitted as `IfClause`s over `<base> = <value>`, the same
    // desugaring `from_between` applies to `BETWEEN`. DataFusion matches a base
    // expression with `=` semantics, so this preserves the plan's meaning,
    // including a `NULL` `<value>` never matching.
    //
    // The base is written once per WHEN, which a volatile base would then
    // evaluate once per arm. `CaseExpr` evaluates it once and compares every
    // WHEN against that one value, so such a plan has no faithful `IfThen`
    // encoding and is rejected instead.
    if let Some(base) = expr
        && is_volatile_including_subqueries(base)?
    {
        return not_impl_err!(
            "Substrait does not support a volatile CASE base expression: {base}"
        );
    }

    let mut ifs: Vec<IfClause> = Vec::with_capacity(when_then_expr.len());
    for (when, then) in when_then_expr {
        let condition = match expr {
            Some(base) => {
                let eq = Expr::eq(*base.clone(), *when.clone());
                producer.handle_expr(&eq, schema)?
            }
            None => producer.handle_expr(when, schema)?,
        };
        ifs.push(IfClause {
            r#if: Some(condition),
            then: Some(producer.handle_expr(then, schema)?),
        });
    }

    // Parse outer `else`
    let r#else: Option<Box<Expression>> = match else_expr {
        Some(e) => Some(Box::new(producer.handle_expr(e, schema)?)),
        None => None,
    };

    Ok(Expression {
        rex_type: Some(RexType::IfThen(Box::new(IfThen { ifs, r#else }))),
    })
}

/// Whether evaluating `expr` twice can give two different values.
///
/// [`Expr::is_volatile`] walks the expression tree, where a subquery is a leaf,
/// so it reports `(SELECT random())` as not volatile. The plan inside one has to
/// be walked as well, or a base holding it would be duplicated by the
/// desugaring above.
fn is_volatile_including_subqueries(expr: &Expr) -> datafusion::common::Result<bool> {
    expr.exists(|expr| match expr {
        Expr::ScalarSubquery(subquery)
        | Expr::Exists(Exists { subquery, .. })
        | Expr::InSubquery(InSubquery { subquery, .. })
        | Expr::SetComparison(SetComparison { subquery, .. }) => {
            plan_is_volatile(&subquery.subquery)
        }
        expr => Ok(expr.is_volatile_node()),
    })
}

/// Whether any expression in `plan`, or in a plan nested in one of them, is
/// volatile.
fn plan_is_volatile(plan: &LogicalPlan) -> datafusion::common::Result<bool> {
    plan.exists(|plan| {
        let mut volatile = false;
        plan.apply_expressions(|expr| {
            volatile = is_volatile_including_subqueries(expr)?;
            Ok(if volatile {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })?;
        Ok(volatile)
    })
}
