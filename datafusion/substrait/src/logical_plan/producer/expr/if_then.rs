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
use datafusion::common::{DFSchemaRef, ScalarValue, not_impl_err};
use datafusion::logical_expr::expr::{Exists, InSubquery, SetComparison};
use datafusion::logical_expr::{Case, Expr, ExprSchemable, LogicalPlan};
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

    // A NULL base answers from ELSE without any WHEN being evaluated:
    // `CaseExpr::case_when_with_expr` fills those rows in and drops them from
    // the batch before it evaluates the first WHEN. The desugaring below would
    // evaluate them, because `<base> = <when>` evaluates both of its operands,
    // so a WHEN that errors or has a side effect would reach rows the plan
    // never ran it on. Emitting that skip as a leading clause restores it.
    //
    // It is only needed when a WHEN operand can do something on those rows.
    // Reading a literal or a column cannot fail and has no side effect, so the
    // common `CASE <base> WHEN <literal> ...` keeps the encoding it had.
    let when_operand_is_inert = |(when, _): &(Box<Expr>, Box<Expr>)| {
        matches!(when.as_ref(), Expr::Literal(..) | Expr::Column(_))
    };
    let null_base_guard = match expr {
        Some(base)
            if !when_then_expr.iter().all(when_operand_is_inert)
                && base.nullable(schema.as_ref())? =>
        {
            Some(base)
        }
        _ => None,
    };

    let mut ifs: Vec<IfClause> =
        Vec::with_capacity(when_then_expr.len() + usize::from(null_base_guard.is_some()));

    if let Some(base) = null_base_guard {
        let condition = producer.handle_expr(&base.clone().is_null(), schema)?;
        // The value a NULL base yields: ELSE, or a NULL of the result type when
        // the CASE has none.
        let then = match else_expr {
            Some(e) => producer.handle_expr(e, schema)?,
            None => {
                let result_type = match when_then_expr.first() {
                    Some((_, then)) => then.get_type(schema.as_ref())?,
                    None => {
                        return not_impl_err!("CASE with no WHEN clause");
                    }
                };
                let null = Expr::Literal(ScalarValue::try_from(&result_type)?, None);
                producer.handle_expr(&null, schema)?
            }
        };
        ifs.push(IfClause {
            r#if: Some(condition),
            then: Some(then),
        });
    }
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
