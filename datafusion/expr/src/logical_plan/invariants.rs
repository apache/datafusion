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

use datafusion_common::{
    assert_or_internal_err, plan_err,
    tree_node::{TreeNode, TreeNodeRecursion},
    DFSchemaRef, Result,
};

use crate::{
    expr::{Exists, InSubquery},
    expr_rewriter::strip_outer_reference,
    utils::{collect_subquery_cols, split_conjunction},
    Aggregate, Expr, Filter, Join, JoinType, LogicalPlan, Window,
};

use super::Extension;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum InvariantLevel {
    /// Invariants that are always true in DataFusion `LogicalPlan`s
    /// such as the number of expected children and no duplicated output fields
    Always,
    /// Invariants that must hold true for the plan to be "executable"
    /// such as the type and number of function arguments are correct and
    /// that wildcards have been expanded
    ///
    /// To ensure a LogicalPlan satisfies the `Executable` invariants, run the
    /// `Analyzer`
    Executable,
}

/// Apply the [`InvariantLevel::Always`] check at the current plan node only.
///
/// This does not recurs to any child nodes.
pub fn assert_always_invariants_at_current_node(plan: &LogicalPlan) -> Result<()> {
    // Refer to <https://datafusion.apache.org/contributor-guide/specification/invariants.html#relation-name-tuples-in-logical-fields-and-logical-columns-are-unique>
    assert_unique_field_names(plan)?;

    Ok(())
}

/// Visit the plan nodes, and confirm the [`InvariantLevel::Executable`]
/// as well as the less stringent [`InvariantLevel::Always`] checks.
pub fn assert_executable_invariants(plan: &LogicalPlan) -> Result<()> {
    // Always invariants
    assert_always_invariants_at_current_node(plan)?;
    assert_valid_extension_nodes(plan, InvariantLevel::Always)?;

    // Executable invariants
    assert_valid_extension_nodes(plan, InvariantLevel::Executable)?;
    assert_valid_semantic_plan(plan)?;
    Ok(())
}

/// Asserts that the query plan, and subplan, extension nodes have valid invariants.
///
/// Refer to [`UserDefinedLogicalNode::check_invariants`](super::UserDefinedLogicalNode)
/// for more details of user-provided extension node invariants.
fn assert_valid_extension_nodes(plan: &LogicalPlan, check: InvariantLevel) -> Result<()> {
    plan.apply_with_subqueries(|plan: &LogicalPlan| {
        if let LogicalPlan::Extension(Extension { node }) = plan {
            node.check_invariants(check)?;
        }
        plan.apply_expressions(|expr| {
            // recursively look for subqueries
            expr.apply(|expr| {
                match expr {
                    Expr::Exists(Exists { subquery, .. })
                    | Expr::InSubquery(InSubquery { subquery, .. })
                    | Expr::ScalarSubquery(subquery) => {
                        assert_valid_extension_nodes(&subquery.subquery, check)?;
                    }
                    _ => {}
                };
                Ok(TreeNodeRecursion::Continue)
            })
        })
    })
    .map(|_| ())
}

/// Returns an error if plan, and subplans, do not have unique fields.
///
/// This invariant is subject to change.
/// refer: <https://github.com/apache/datafusion/issues/13525#issuecomment-2494046463>
fn assert_unique_field_names(plan: &LogicalPlan) -> Result<()> {
    plan.schema().check_names()
}

/// Returns an error if the plan is not semantically valid.
fn assert_valid_semantic_plan(plan: &LogicalPlan) -> Result<()> {
    assert_subqueries_are_valid(plan)?;

    Ok(())
}

/// Returns an error if the plan does not have the expected schema.
/// Ignores metadata and nullability.
pub fn assert_expected_schema(schema: &DFSchemaRef, plan: &LogicalPlan) -> Result<()> {
    let compatible = plan.schema().logically_equivalent_names_and_types(schema);

    assert_or_internal_err!(
        compatible,
        "Failed due to a difference in schemas: original schema: {:?}, new schema: {:?}",
        schema,
        plan.schema()
    );
    Ok(())
}

/// Asserts that the subqueries are structured properly with valid node placement.
///
/// Refer to [`check_subquery_expr`] for more details of the internal invariants.
fn assert_subqueries_are_valid(plan: &LogicalPlan) -> Result<()> {
    plan.apply_with_subqueries(|plan: &LogicalPlan| {
        plan.apply_expressions(|expr| {
            // recursively look for subqueries
            expr.apply(|expr| {
                match expr {
                    Expr::Exists(Exists { subquery, .. })
                    | Expr::InSubquery(InSubquery { subquery, .. })
                    | Expr::ScalarSubquery(subquery) => {
                        check_subquery_expr(plan, &subquery.subquery, expr)?;
                    }
                    _ => {}
                };
                Ok(TreeNodeRecursion::Continue)
            })
        })
    })
    .map(|_| ())
}

/// Do necessary check on subquery expressions and fail the invalid plan
/// 1) Check whether the outer plan is in the allowed outer plans list to use subquery expressions,
///    the allowed while list: [Projection, Filter, Window, Aggregate, Join].
/// 2) Check whether the inner plan is in the allowed inner plans list to use correlated(outer) expressions.
/// 3) Check and validate unsupported cases to use the correlated(outer) expressions inside the subquery(inner) plans/inner expressions.
///    For example, we do not want to support to use correlated expressions as the Join conditions in the subquery plan when the Join
///    is a Full Out Join
pub fn check_subquery_expr(
    outer_plan: &LogicalPlan,
    inner_plan: &LogicalPlan,
    expr: &Expr,
) -> Result<()> {
    assert_subqueries_are_valid(inner_plan)?;
    if let Expr::ScalarSubquery(subquery) = expr {
        // Scalar subquery should only return one column
        if subquery.subquery.schema().fields().len() > 1 {
            return plan_err!(
                "Scalar subquery should only return one column, but found {}: {}",
                subquery.subquery.schema().fields().len(),
                subquery.subquery.schema().field_names().join(", ")
            );
        }
        // Correlated scalar subquery must be aggregated to return at most one row
        if !subquery.outer_ref_columns.is_empty() {
            match strip_inner_query(inner_plan) {
                LogicalPlan::Aggregate(agg) => {
                    check_aggregation_in_scalar_subquery(inner_plan, agg)
                }
                LogicalPlan::Filter(Filter { input, .. })
                    if matches!(input.as_ref(), LogicalPlan::Aggregate(_)) =>
                {
                    if let LogicalPlan::Aggregate(agg) = input.as_ref() {
                        check_aggregation_in_scalar_subquery(inner_plan, agg)
                    } else {
                        Ok(())
                    }
                }
                _ => {
                    if inner_plan
                        .max_rows()
                        .filter(|max_row| *max_row <= 1)
                        .is_some()
                    {
                        Ok(())
                    } else {
                        plan_err!(
                            "Correlated scalar subquery must be aggregated to return at most one row"
                        )
                    }
                }
            }?;
            match outer_plan {
                LogicalPlan::Projection(_)
                | LogicalPlan::Filter(_) => Ok(()),
                LogicalPlan::Aggregate(Aggregate { group_expr, aggr_expr, .. }) => {
                    if group_expr.contains(expr) && !aggr_expr.contains(expr) {
                        // TODO revisit this validation logic
                        plan_err!(
                            "Correlated scalar subquery in the GROUP BY clause must also be in the aggregate expressions"
                        )
                    } else {
                        Ok(())
                    }
                }
                _ => plan_err!(
                    "Correlated scalar subquery can only be used in Projection, Filter, Aggregate plan nodes"
                )
            }?;
        }
        check_correlations_in_subquery(inner_plan)
    } else {
        if let Expr::InSubquery(subquery) = expr {
            // InSubquery should only return one column
            if subquery.subquery.subquery.schema().fields().len() > 1 {
                return plan_err!(
                    "InSubquery should only return one column, but found {}: {}",
                    subquery.subquery.subquery.schema().fields().len(),
                    subquery.subquery.subquery.schema().field_names().join(", ")
                );
            }
        }
        match outer_plan {
            LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::TableScan(_)
            | LogicalPlan::Window(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Join(_) => Ok(()),
            _ => plan_err!(
                "In/Exist subquery can only be used in \
                Projection, Filter, TableScan, Window functions, Aggregate and Join plan nodes, \
                but was used in [{}]",
                outer_plan.display()
            ),
        }?;
        check_correlations_in_subquery(inner_plan)
    }
}

// Recursively check the unsupported outer references in the sub query plan.
fn check_correlations_in_subquery(inner_plan: &LogicalPlan) -> Result<()> {
    check_inner_plan(inner_plan)
}

// Recursively check the unsupported outer references in the sub query plan.
#[cfg_attr(feature = "recursive_protection", recursive::recursive)]
fn check_inner_plan(inner_plan: &LogicalPlan) -> Result<()> {
    // We want to support as many operators as possible inside the correlated subquery
    match inner_plan {
        LogicalPlan::Aggregate(_) => {
            inner_plan.apply_children(|plan| {
                check_inner_plan(plan)?;
                Ok(TreeNodeRecursion::Continue)
            })?;
            Ok(())
        }
        LogicalPlan::Filter(Filter { input, .. }) => check_inner_plan(input),
        LogicalPlan::Window(window) => {
            check_mixed_out_refer_in_window(window)?;
            inner_plan.apply_children(|plan| {
                check_inner_plan(plan)?;
                Ok(TreeNodeRecursion::Continue)
            })?;
            Ok(())
        }
        LogicalPlan::Projection(_)
        | LogicalPlan::Distinct(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Union(_)
        | LogicalPlan::TableScan(_)
        | LogicalPlan::EmptyRelation(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::Subquery(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Unnest(_) => {
            inner_plan.apply_children(|plan| {
                check_inner_plan(plan)?;
                Ok(TreeNodeRecursion::Continue)
            })?;
            Ok(())
        }
        LogicalPlan::Join(Join {
            left,
            right,
            join_type,
            ..
        }) => match join_type {
            JoinType::Inner => {
                inner_plan.apply_children(|plan| {
                    check_inner_plan(plan)?;
                    Ok(TreeNodeRecursion::Continue)
                })?;
                Ok(())
            }
            JoinType::Left
            | JoinType::LeftSemi
            | JoinType::LeftAnti
            | JoinType::LeftMark => {
                check_inner_plan(left)?;
                check_no_outer_references(right)
            }
            JoinType::Right
            | JoinType::RightSemi
            | JoinType::RightAnti
            | JoinType::RightMark => {
                check_no_outer_references(left)?;
                check_inner_plan(right)
            }
            JoinType::Full => {
                inner_plan.apply_children(|plan| {
                    check_no_outer_references(plan)?;
                    Ok(TreeNodeRecursion::Continue)
                })?;
                Ok(())
            }
        },
        LogicalPlan::Extension(_) => Ok(()),
        plan => check_no_outer_references(plan),
    }
}

fn check_no_outer_references(inner_plan: &LogicalPlan) -> Result<()> {
    if inner_plan.contains_outer_reference() {
        plan_err!(
            "Accessing outer reference columns is not allowed in the plan: {}",
            inner_plan.display()
        )
    } else {
        Ok(())
    }
}

fn check_aggregation_in_scalar_subquery(
    inner_plan: &LogicalPlan,
    agg: &Aggregate,
) -> Result<()> {
    if agg.aggr_expr.is_empty() {
        return plan_err!(
            "Correlated scalar subquery must be aggregated to return at most one row"
        );
    }
    if !agg.group_expr.is_empty() {
        let correlated_exprs = get_correlated_expressions(inner_plan)?;
        let inner_subquery_cols =
            collect_subquery_cols(&correlated_exprs, agg.input.schema())?;
        let mut group_columns = agg
            .group_expr
            .iter()
            .map(|group| Ok(group.column_refs().into_iter().cloned().collect::<Vec<_>>()))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten();

        if !group_columns.all(|group| inner_subquery_cols.contains(&group)) {
            // Group BY columns must be a subset of columns in the correlated expressions
            return plan_err!(
                "A GROUP BY clause in a scalar correlated subquery cannot contain non-correlated columns"
            );
        }
    }
    Ok(())
}

fn strip_inner_query(inner_plan: &LogicalPlan) -> &LogicalPlan {
    match inner_plan {
        LogicalPlan::Projection(projection) => {
            strip_inner_query(projection.input.as_ref())
        }
        LogicalPlan::SubqueryAlias(alias) => strip_inner_query(alias.input.as_ref()),
        other => other,
    }
}

fn get_correlated_expressions(inner_plan: &LogicalPlan) -> Result<Vec<Expr>> {
    let mut exprs = vec![];
    inner_plan.apply_with_subqueries(|plan| {
        if let LogicalPlan::Filter(Filter { predicate, .. }) = plan {
            let (correlated, _): (Vec<_>, Vec<_>) = split_conjunction(predicate)
                .into_iter()
                .partition(|e| e.contains_outer());

            for expr in correlated {
                exprs.push(strip_outer_reference(expr.clone()));
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(exprs)
}

/// Check whether the window expressions contain a mixture of out reference columns and inner columns
fn check_mixed_out_refer_in_window(window: &Window) -> Result<()> {
    let mixed = window
        .window_expr
        .iter()
        .any(|win_expr| win_expr.contains_outer() && win_expr.any_column_refs());
    if mixed {
        plan_err!(
            "Window expressions should not contain a mixed of outer references and inner columns"
        )
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::cmp::Ordering;
    use std::sync::Arc;

    use crate::{Extension, UserDefinedLogicalNodeCore};
    use datafusion_common::{DFSchema, DFSchemaRef};

    use super::*;

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct MockUserDefinedLogicalPlan {
        empty_schema: DFSchemaRef,
    }

    impl PartialOrd for MockUserDefinedLogicalPlan {
        fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
            None
        }
    }

    impl UserDefinedLogicalNodeCore for MockUserDefinedLogicalPlan {
        fn name(&self) -> &str {
            "MockUserDefinedLogicalPlan"
        }

        fn inputs(&self) -> Vec<&LogicalPlan> {
            vec![]
        }

        fn schema(&self) -> &DFSchemaRef {
            &self.empty_schema
        }

        fn expressions(&self) -> Vec<Expr> {
            vec![]
        }

        fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "MockUserDefinedLogicalPlan")
        }

        fn with_exprs_and_inputs(
            &self,
            _exprs: Vec<Expr>,
            _inputs: Vec<LogicalPlan>,
        ) -> Result<Self> {
            Ok(Self {
                empty_schema: Arc::clone(&self.empty_schema),
            })
        }

        fn supports_limit_pushdown(&self) -> bool {
            false // Disallow limit push-down by default
        }
    }

    #[test]
    fn wont_fail_extension_plan() {
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(MockUserDefinedLogicalPlan {
                empty_schema: DFSchemaRef::new(DFSchema::empty()),
            }),
        });

        check_inner_plan(&plan).unwrap();
    }
}
