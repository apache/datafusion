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

//! [`OptimizeProjections`] identifies and eliminates unused columns

mod required_indices;

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};
use std::collections::HashSet;
use std::sync::Arc;

use datafusion_common::{
    assert_eq_or_internal_err, get_required_group_by_exprs_indices,
    internal_datafusion_err, internal_err, Column, DFSchema, HashMap, JoinType, Result,
};
use datafusion_expr::expr::Alias;
use datafusion_expr::{
    logical_plan::LogicalPlan, Aggregate, Distinct, EmptyRelation, Expr, Projection,
    TableScan, Unnest, Window,
};

use crate::optimize_projections::required_indices::RequiredIndices;
use crate::utils::NamePreserver;
use datafusion_common::tree_node::{
    Transformed, TreeNode, TreeNodeContainer, TreeNodeRecursion,
};

/// Optimizer rule to prune unnecessary columns from intermediate schemas
/// inside the [`LogicalPlan`]. This rule:
/// - Removes unnecessary columns that do not appear at the output and/or are
///   not used during any computation step.
/// - Adds projections to decrease table column size before operators that
///   benefit from a smaller memory footprint at its input.
/// - Removes unnecessary [`LogicalPlan::Projection`]s from the [`LogicalPlan`].
///
/// `OptimizeProjections` is an optimizer rule that identifies and eliminates
/// columns from a logical plan that are not used by downstream operations.
/// This can improve query performance and reduce unnecessary data processing.
///
/// The rule analyzes the input logical plan, determines the necessary column
/// indices, and then removes any unnecessary columns. It also removes any
/// unnecessary projections from the plan tree.
///
/// ## Schema, Field Properties, and Metadata Handling
///
/// The `OptimizeProjections` rule preserves schema and field metadata in most optimization scenarios:
///
/// **Schema-level metadata preservation by plan type**:
/// - **Window and Aggregate plans**: Schema metadata is preserved
/// - **Projection plans**: Schema metadata is preserved per [`projection_schema`](datafusion_expr::logical_plan::projection_schema).
/// - **Other logical plans**: Schema metadata is preserved unless [`LogicalPlan::recompute_schema`]
///   is called on plan types that drop metadata
///
/// **Field-level properties and metadata**: Individual field properties are preserved when fields
/// are retained in the optimized plan, determined by [`exprlist_to_fields`](datafusion_expr::utils::exprlist_to_fields)
/// and [`ExprSchemable::to_field`](datafusion_expr::expr_schema::ExprSchemable::to_field).
///
/// **Field precedence**: When the same field appears multiple times, the optimizer
/// maintains one occurrence and removes duplicates (refer to `RequiredIndices::compact()`),
/// preserving the properties and metadata of that occurrence.
#[derive(Default, Debug)]
pub struct OptimizeProjections {}

impl OptimizeProjections {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for OptimizeProjections {
    fn name(&self) -> &str {
        "optimize_projections"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // All output fields are necessary:
        let indices = RequiredIndices::new_for_all_exprs(&plan);
        optimize_projections(plan, config, indices)
    }
}

/// Removes unnecessary columns (e.g. columns that do not appear in the output
/// schema and/or are not used during any computation step such as expression
/// evaluation) from the logical plan and its inputs.
///
/// # Parameters
///
/// - `plan`: A reference to the input `LogicalPlan` to optimize.
/// - `config`: A reference to the optimizer configuration.
/// - `indices`: A slice of column indices that represent the necessary column
///   indices for downstream (parent) plan nodes.
///
/// # Returns
///
/// A `Result` object with the following semantics:
///
/// - `Ok(Some(LogicalPlan))`: An optimized `LogicalPlan` without unnecessary
///   columns.
/// - `Ok(None)`: Signal that the given logical plan did not require any change.
/// - `Err(error)`: An error occurred during the optimization process.
#[cfg_attr(feature = "recursive_protection", recursive::recursive)]
fn optimize_projections(
    plan: LogicalPlan,
    config: &dyn OptimizerConfig,
    indices: RequiredIndices,
) -> Result<Transformed<LogicalPlan>> {
    // Recursively rewrite any nodes that may be able to avoid computation given
    // their parents' required indices.
    match plan {
        LogicalPlan::Projection(proj) => {
            return merge_consecutive_projections(proj)?.transform_data(|proj| {
                rewrite_projection_given_requirements(proj, config, &indices)
            })
        }
        LogicalPlan::Aggregate(aggregate) => {
            // Split parent requirements to GROUP BY and aggregate sections:
            let n_group_exprs = aggregate.group_expr_len()?;
            // Offset aggregate indices so that they point to valid indices at
            // `aggregate.aggr_expr`:
            let (group_by_reqs, aggregate_reqs) = indices.split_off(n_group_exprs);

            // Get absolutely necessary GROUP BY fields:
            let group_by_expr_existing = aggregate
                .group_expr
                .iter()
                .map(|group_by_expr| group_by_expr.schema_name().to_string())
                .collect::<Vec<_>>();

            let new_group_bys = if let Some(simplest_groupby_indices) =
                get_required_group_by_exprs_indices(
                    aggregate.input.schema(),
                    &group_by_expr_existing,
                ) {
                // Some of the fields in the GROUP BY may be required by the
                // parent even if these fields are unnecessary in terms of
                // functional dependency.
                group_by_reqs
                    .append(&simplest_groupby_indices)
                    .get_at_indices(&aggregate.group_expr)
            } else {
                aggregate.group_expr
            };

            // Only use the absolutely necessary aggregate expressions required
            // by the parent:
            let new_aggr_expr = aggregate_reqs.get_at_indices(&aggregate.aggr_expr);

            if new_group_bys.is_empty() && new_aggr_expr.is_empty() {
                // Global aggregation with no aggregate functions always produces 1 row and no columns.
                return Ok(Transformed::yes(LogicalPlan::EmptyRelation(
                    EmptyRelation {
                        produce_one_row: true,
                        schema: Arc::new(DFSchema::empty()),
                    },
                )));
            }

            let all_exprs_iter = new_group_bys.iter().chain(new_aggr_expr.iter());
            let schema = aggregate.input.schema();
            let necessary_indices =
                RequiredIndices::new().with_exprs(schema, all_exprs_iter);
            let necessary_exprs = necessary_indices.get_required_exprs(schema);

            return optimize_projections(
                Arc::unwrap_or_clone(aggregate.input),
                config,
                necessary_indices,
            )?
            .transform_data(|aggregate_input| {
                // Simplify the input of the aggregation by adding a projection so
                // that its input only contains absolutely necessary columns for
                // the aggregate expressions. Note that necessary_indices refer to
                // fields in `aggregate.input.schema()`.
                add_projection_on_top_if_helpful(aggregate_input, necessary_exprs)
            })?
            .map_data(|aggregate_input| {
                // Create a new aggregate plan with the updated input and only the
                // absolutely necessary fields:
                Aggregate::try_new(
                    Arc::new(aggregate_input),
                    new_group_bys,
                    new_aggr_expr,
                )
                .map(LogicalPlan::Aggregate)
            });
        }
        LogicalPlan::Window(window) => {
            let input_schema = Arc::clone(window.input.schema());
            // Split parent requirements to child and window expression sections:
            let n_input_fields = input_schema.fields().len();
            // Offset window expression indices so that they point to valid
            // indices at `window.window_expr`:
            let (child_reqs, window_reqs) = indices.split_off(n_input_fields);

            // Only use window expressions that are absolutely necessary according
            // to parent requirements:
            let new_window_expr = window_reqs.get_at_indices(&window.window_expr);

            // Get all the required column indices at the input, either by the
            // parent or window expression requirements.
            let required_indices = child_reqs.with_exprs(&input_schema, &new_window_expr);

            return optimize_projections(
                Arc::unwrap_or_clone(window.input),
                config,
                required_indices.clone(),
            )?
            .transform_data(|window_child| {
                if new_window_expr.is_empty() {
                    // When no window expression is necessary, use the input directly:
                    Ok(Transformed::no(window_child))
                } else {
                    // Calculate required expressions at the input of the window.
                    // Please note that we use `input_schema`, because `required_indices`
                    // refers to that schema
                    let required_exprs =
                        required_indices.get_required_exprs(&input_schema);
                    let window_child =
                        add_projection_on_top_if_helpful(window_child, required_exprs)?
                            .data;
                    Window::try_new(new_window_expr, Arc::new(window_child))
                        .map(LogicalPlan::Window)
                        .map(Transformed::yes)
                }
            });
        }
        LogicalPlan::TableScan(table_scan) => {
            let TableScan {
                table_name,
                source,
                projection,
                filters,
                fetch,
                projected_schema: _,
            } = table_scan;

            // Get indices referred to in the original (schema with all fields)
            // given projected indices.
            let projection = match &projection {
                Some(projection) => indices.into_mapped_indices(|idx| projection[idx]),
                None => indices.into_inner(),
            };
            return TableScan::try_new(
                table_name,
                source,
                Some(projection),
                filters,
                fetch,
            )
            .map(LogicalPlan::TableScan)
            .map(Transformed::yes);
        }
        // Other node types are handled below
        _ => {}
    };

    // For other plan node types, calculate indices for columns they use and
    // try to rewrite their children
    let mut child_required_indices: Vec<RequiredIndices> = match &plan {
        LogicalPlan::Sort(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Repartition(_)
        | LogicalPlan::Union(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Distinct(Distinct::On(_)) => {
            // Pass index requirements from the parent as well as column indices
            // that appear in this plan's expressions to its child. All these
            // operators benefit from "small" inputs, so the projection_beneficial
            // flag is `true`.
            plan.inputs()
                .into_iter()
                .map(|input| {
                    indices
                        .clone()
                        .with_projection_beneficial()
                        .with_plan_exprs(&plan, input.schema())
                })
                .collect::<Result<_>>()?
        }
        LogicalPlan::Limit(_) => {
            // Pass index requirements from the parent as well as column indices
            // that appear in this plan's expressions to its child. These operators
            // do not benefit from "small" inputs, so the projection_beneficial
            // flag is `false`.
            plan.inputs()
                .into_iter()
                .map(|input| indices.clone().with_plan_exprs(&plan, input.schema()))
                .collect::<Result<_>>()?
        }
        LogicalPlan::Copy(_)
        | LogicalPlan::Ddl(_)
        | LogicalPlan::Dml(_)
        | LogicalPlan::Explain(_)
        | LogicalPlan::Analyze(_)
        | LogicalPlan::Subquery(_)
        | LogicalPlan::Statement(_)
        | LogicalPlan::Distinct(Distinct::All(_)) => {
            // These plans require all their fields, and their children should
            // be treated as final plans -- otherwise, we may have schema a
            // mismatch.
            // TODO: For some subquery variants (e.g. a subquery arising from an
            //       EXISTS expression), we may not need to require all indices.
            plan.inputs()
                .into_iter()
                .map(RequiredIndices::new_for_all_exprs)
                .collect()
        }
        LogicalPlan::Extension(extension) => {
            let Some(necessary_children_indices) =
                extension.node.necessary_children_exprs(indices.indices())
            else {
                // Requirements from parent cannot be routed down to user defined logical plan safely
                return Ok(Transformed::no(plan));
            };
            let children = extension.node.inputs();
            assert_eq_or_internal_err!(
                children.len(),
                necessary_children_indices.len(),
                "Inconsistent length between children and necessary children indices. \
                Make sure `.necessary_children_exprs` implementation of the \
                `UserDefinedLogicalNode` is consistent with actual children length \
                for the node."
            );
            children
                .into_iter()
                .zip(necessary_children_indices)
                .map(|(child, necessary_indices)| {
                    RequiredIndices::new_from_indices(necessary_indices)
                        .with_plan_exprs(&plan, child.schema())
                })
                .collect::<Result<Vec<_>>>()?
        }
        LogicalPlan::EmptyRelation(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::DescribeTable(_) => {
            // These operators have no inputs, so stop the optimization process.
            return Ok(Transformed::no(plan));
        }
        LogicalPlan::RecursiveQuery(recursive) => {
            // Only allow subqueries that reference the current CTE; nested subqueries are not yet
            // supported for projection pushdown for simplicity.
            // TODO: be able to do projection pushdown on recursive CTEs with subqueries
            if plan_contains_other_subqueries(
                recursive.static_term.as_ref(),
                &recursive.name,
            ) || plan_contains_other_subqueries(
                recursive.recursive_term.as_ref(),
                &recursive.name,
            ) {
                return Ok(Transformed::no(plan));
            }

            plan.inputs()
                .into_iter()
                .map(|input| {
                    indices
                        .clone()
                        .with_projection_beneficial()
                        .with_plan_exprs(&plan, input.schema())
                })
                .collect::<Result<Vec<_>>>()?
        }
        LogicalPlan::Join(join) => {
            let left_len = join.left.schema().fields().len();
            let (left_req_indices, right_req_indices) =
                split_join_requirements(left_len, indices, &join.join_type);
            let left_indices =
                left_req_indices.with_plan_exprs(&plan, join.left.schema())?;
            let right_indices =
                right_req_indices.with_plan_exprs(&plan, join.right.schema())?;
            // Joins benefit from "small" input tables (lower memory usage).
            // Therefore, each child benefits from projection:
            vec![
                left_indices.with_projection_beneficial(),
                right_indices.with_projection_beneficial(),
            ]
        }
        // these nodes are explicitly rewritten in the match statement above
        LogicalPlan::Projection(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::Window(_)
        | LogicalPlan::TableScan(_) => {
            return internal_err!(
                "OptimizeProjection: should have handled in the match statement above"
            );
        }
        LogicalPlan::Unnest(Unnest {
            input,
            dependency_indices,
            ..
        }) => {
            // at least provide the indices for the exec-columns as a starting point
            let required_indices =
                RequiredIndices::new().with_plan_exprs(&plan, input.schema())?;

            // Add additional required indices from the parent
            let mut additional_necessary_child_indices = Vec::new();
            indices.indices().iter().for_each(|idx| {
                if let Some(index) = dependency_indices.get(*idx) {
                    additional_necessary_child_indices.push(*index);
                }
            });
            vec![required_indices.append(&additional_necessary_child_indices)]
        }
    };

    // Required indices are currently ordered (child0, child1, ...)
    // but the loop pops off the last element, so we need to reverse the order
    child_required_indices.reverse();
    assert_eq_or_internal_err!(
        child_required_indices.len(),
        plan.inputs().len(),
        "OptimizeProjection: child_required_indices length mismatch with plan inputs"
    );

    // Rewrite children of the plan
    let transformed_plan = plan.map_children(|child| {
        let required_indices = child_required_indices.pop().ok_or_else(|| {
            internal_datafusion_err!(
                "Unexpected number of required_indices in OptimizeProjections rule"
            )
        })?;

        let projection_beneficial = required_indices.projection_beneficial();
        let project_exprs = required_indices.get_required_exprs(child.schema());

        optimize_projections(child, config, required_indices)?.transform_data(
            |new_input| {
                if projection_beneficial {
                    add_projection_on_top_if_helpful(new_input, project_exprs)
                } else {
                    Ok(Transformed::no(new_input))
                }
            },
        )
    })?;

    // If any of the children are transformed, we need to potentially update the plan's schema
    if transformed_plan.transformed {
        transformed_plan.map_data(|plan| plan.recompute_schema())
    } else {
        Ok(transformed_plan)
    }
}

/// Merges consecutive projections.
///
/// Given a projection `proj`, this function attempts to merge it with a previous
/// projection if it exists and if merging is beneficial. Merging is considered
/// beneficial when expressions in the current projection are non-trivial and
/// appear more than once in its input fields. This can act as a caching mechanism
/// for non-trivial computations.
///
/// ## Metadata Handling During Projection Merging
///
/// **Alias metadata preservation**: When merging projections, alias metadata from both
/// the current and previous projections is carefully preserved. The presence of metadata
/// precludes alias trimming.
///
/// **Schema, Fields, and metadata**: If a projection is rewritten, the schema and metadata
/// are preserved. Individual field properties and metadata flows through expression rewriting
/// and are preserved when fields are referenced in the merged projection.
/// Refer to [`projection_schema`](datafusion_expr::logical_plan::projection_schema)
/// for more details.
///
/// # Parameters
///
/// * `proj` - A reference to the `Projection` to be merged.
///
/// # Returns
///
/// A `Result` object with the following semantics:
///
/// - `Ok(Some(Projection))`: Merge was beneficial and successful. Contains the
///   merged projection.
/// - `Ok(None)`: Signals that merge is not beneficial (and has not taken place).
/// - `Err(error)`: An error occurred during the function call.
fn merge_consecutive_projections(proj: Projection) -> Result<Transformed<Projection>> {
    let Projection {
        expr,
        input,
        schema,
        ..
    } = proj;
    let LogicalPlan::Projection(prev_projection) = input.as_ref() else {
        return Projection::try_new_with_schema(expr, input, schema).map(Transformed::no);
    };

    // A fast path: if the previous projection is same as the current projection
    // we can directly remove the current projection and return child projection.
    if prev_projection.expr == expr {
        return Projection::try_new_with_schema(
            expr,
            Arc::clone(&prev_projection.input),
            schema,
        )
        .map(Transformed::yes);
    }

    // Count usages (referrals) of each projection expression in its input fields:
    let mut column_referral_map = HashMap::<&Column, usize>::new();
    expr.iter()
        .for_each(|expr| expr.add_column_ref_counts(&mut column_referral_map));

    // If an expression is non-trivial and appears more than once, do not merge
    // them as consecutive projections will benefit from a compute-once approach.
    // For details, see: https://github.com/apache/datafusion/issues/8296
    if column_referral_map.into_iter().any(|(col, usage)| {
        usage > 1
            && !is_expr_trivial(
                &prev_projection.expr
                    [prev_projection.schema.index_of_column(col).unwrap()],
            )
    }) {
        // no change
        return Projection::try_new_with_schema(expr, input, schema).map(Transformed::no);
    }

    let LogicalPlan::Projection(prev_projection) = Arc::unwrap_or_clone(input) else {
        // We know it is a `LogicalPlan::Projection` from check above
        unreachable!();
    };

    // Try to rewrite the expressions in the current projection using the
    // previous projection as input:
    let name_preserver = NamePreserver::new_for_projection();
    let mut original_names = vec![];
    let new_exprs = expr.map_elements(|expr| {
        original_names.push(name_preserver.save(&expr));

        // do not rewrite top level Aliases (rewriter will remove all aliases within exprs)
        match expr {
            Expr::Alias(Alias {
                expr,
                relation,
                name,
                metadata,
            }) => rewrite_expr(*expr, &prev_projection).map(|result| {
                result.update_data(|expr| {
                    Expr::Alias(Alias::new(expr, relation, name).with_metadata(metadata))
                })
            }),
            e => rewrite_expr(e, &prev_projection),
        }
    })?;

    // if the expressions could be rewritten, create a new projection with the
    // new expressions
    if new_exprs.transformed {
        // Add any needed aliases back to the expressions
        let new_exprs = new_exprs
            .data
            .into_iter()
            .zip(original_names)
            .map(|(expr, original_name)| original_name.restore(expr))
            .collect::<Vec<_>>();
        Projection::try_new(new_exprs, prev_projection.input).map(Transformed::yes)
    } else {
        // not rewritten, so put the projection back together
        let input = Arc::new(LogicalPlan::Projection(prev_projection));
        Projection::try_new_with_schema(new_exprs.data, input, schema)
            .map(Transformed::no)
    }
}

// Check whether `expr` is trivial; i.e. it doesn't imply any computation.
fn is_expr_trivial(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(_) | Expr::Literal(_, _))
}

/// Rewrites a projection expression using the projection before it (i.e. its input)
/// This is a subroutine to the `merge_consecutive_projections` function.
///
/// # Parameters
///
/// * `expr` - A reference to the expression to rewrite.
/// * `input` - A reference to the input of the projection expression (itself
///   a projection).
///
/// # Returns
///
/// A `Result` object with the following semantics:
///
/// - `Ok(Some(Expr))`: Rewrite was successful. Contains the rewritten result.
/// - `Ok(None)`: Signals that `expr` can not be rewritten.
/// - `Err(error)`: An error occurred during the function call.
///
/// # Notes
/// This rewrite also removes any unnecessary layers of aliasing. "Unnecessary" is
/// defined as not contributing new information, such as metadata.
///
/// Without trimming, we can end up with unnecessary indirections inside expressions
/// during projection merges.
///
/// Consider:
///
/// ```text
/// Projection(a1 + b1 as sum1)
/// --Projection(a as a1, b as b1)
/// ----Source(a, b)
/// ```
///
/// After merge, we want to produce:
///
/// ```text
/// Projection(a + b as sum1)
/// --Source(a, b)
/// ```
///
/// Without trimming, we would end up with:
///
/// ```text
/// Projection((a as a1 + b as b1) as sum1)
/// --Source(a, b)
/// ```
fn rewrite_expr(expr: Expr, input: &Projection) -> Result<Transformed<Expr>> {
    expr.transform_up(|expr| {
        match expr {
            //  remove any intermediate aliases if they do not carry metadata
            Expr::Alias(alias) => {
                match alias
                    .metadata
                    .as_ref()
                    .map(|h| h.is_empty())
                    .unwrap_or(true)
                {
                    true => Ok(Transformed::yes(*alias.expr)),
                    false => Ok(Transformed::no(Expr::Alias(alias))),
                }
            }
            Expr::Column(col) => {
                // Find index of column:
                let idx = input.schema.index_of_column(&col)?;
                // get the corresponding unaliased input expression
                //
                // For example:
                // * the input projection is [`a + b` as c, `d + e` as f]
                // * the current column is an expression "f"
                //
                // return the expression `d + e` (not `d + e` as f)
                let input_expr = input.expr[idx].clone().unalias_nested().data;
                Ok(Transformed::yes(input_expr))
            }
            // Unsupported type for consecutive projection merge analysis.
            _ => Ok(Transformed::no(expr)),
        }
    })
}

/// Accumulates outer-referenced columns by the
/// given expression, `expr`.
///
/// # Parameters
///
/// * `expr` - The expression to analyze for outer-referenced columns.
/// * `columns` - A mutable reference to a `HashSet<Column>` where detected
///   columns are collected.
fn outer_columns<'a>(expr: &'a Expr, columns: &mut HashSet<&'a Column>) {
    // inspect_expr_pre doesn't handle subquery references, so find them explicitly
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
    // unwrap: closure above never returns Err, so can not be Err here
    .unwrap();
}

/// A recursive subroutine that accumulates outer-referenced columns by the
/// given expressions (`exprs`).
///
/// # Parameters
///
/// * `exprs` - The expressions to analyze for outer-referenced columns.
/// * `columns` - A mutable reference to a `HashSet<Column>` where detected
///   columns are collected.
fn outer_columns_helper_multi<'a, 'b>(
    exprs: impl IntoIterator<Item = &'a Expr>,
    columns: &'b mut HashSet<&'a Column>,
) {
    exprs.into_iter().for_each(|e| outer_columns(e, columns));
}

/// Splits requirement indices for a join into left and right children based on
/// the join type.
///
/// This function takes the length of the left child, a slice of requirement
/// indices, and the type of join (e.g. `INNER`, `LEFT`, `RIGHT`) as arguments.
/// Depending on the join type, it divides the requirement indices into those
/// that apply to the left child and those that apply to the right child.
///
/// - For `INNER`, `LEFT`, `RIGHT`, `FULL`, `LEFTMARK`, and `RIGHTMARK` joins,
///   the requirements are split between left and right children. The right
///   child indices are adjusted to point to valid positions within the right
///   child by subtracting the length of the left child.
///
/// - For `LEFT ANTI`, `LEFT SEMI`, `RIGHT SEMI` and `RIGHT ANTI` joins, all
///   requirements are re-routed to either the left child or the right child
///   directly, depending on the join type.
///
/// # Parameters
///
/// * `left_len` - The length of the left child.
/// * `indices` - A slice of requirement indices.
/// * `join_type` - The type of join (e.g. `INNER`, `LEFT`, `RIGHT`).
///
/// # Returns
///
/// A tuple containing two vectors of `usize` indices: The first vector represents
/// the requirements for the left child, and the second vector represents the
/// requirements for the right child. The indices are appropriately split and
/// adjusted based on the join type.
fn split_join_requirements(
    left_len: usize,
    indices: RequiredIndices,
    join_type: &JoinType,
) -> (RequiredIndices, RequiredIndices) {
    match join_type {
        // In these cases requirements are split between left/right children:
        JoinType::Inner
        | JoinType::Left
        | JoinType::Right
        | JoinType::Full
        | JoinType::LeftMark
        | JoinType::RightMark => {
            // Decrease right side indices by `left_len` so that they point to valid
            // positions within the right child:
            indices.split_off(left_len)
        }
        // All requirements can be re-routed to left child directly.
        JoinType::LeftAnti | JoinType::LeftSemi => (indices, RequiredIndices::new()),
        // All requirements can be re-routed to right side directly.
        // No need to change index, join schema is right child schema.
        JoinType::RightSemi | JoinType::RightAnti => (RequiredIndices::new(), indices),
    }
}

/// Adds a projection on top of a logical plan if doing so reduces the number
/// of columns for the parent operator.
///
/// This function takes a `LogicalPlan` and a list of projection expressions.
/// If the projection is beneficial (it reduces the number of columns in the
/// plan) a new `LogicalPlan` with the projection is created and returned, along
/// with a `true` flag. If the projection doesn't reduce the number of columns,
/// the original plan is returned with a `false` flag.
///
/// # Parameters
///
/// * `plan` - The input `LogicalPlan` to potentially add a projection to.
/// * `project_exprs` - A list of expressions for the projection.
///
/// # Returns
///
/// A `Transformed` indicating if a projection was added
fn add_projection_on_top_if_helpful(
    plan: LogicalPlan,
    project_exprs: Vec<Expr>,
) -> Result<Transformed<LogicalPlan>> {
    // Make sure projection decreases the number of columns, otherwise it is unnecessary.
    if project_exprs.len() >= plan.schema().fields().len() {
        Ok(Transformed::no(plan))
    } else {
        Projection::try_new(project_exprs, Arc::new(plan))
            .map(LogicalPlan::Projection)
            .map(Transformed::yes)
    }
}

/// Rewrite the given projection according to the fields required by its
/// ancestors.
///
/// # Parameters
///
/// * `proj` - A reference to the original projection to rewrite.
/// * `config` - A reference to the optimizer configuration.
/// * `indices` - A slice of indices representing the columns required by the
///   ancestors of the given projection.
///
/// # Returns
///
/// A `Result` object with the following semantics:
///
/// - `Ok(Some(LogicalPlan))`: Contains the rewritten projection
/// - `Ok(None)`: No rewrite necessary.
/// - `Err(error)`: An error occurred during the function call.
fn rewrite_projection_given_requirements(
    proj: Projection,
    config: &dyn OptimizerConfig,
    indices: &RequiredIndices,
) -> Result<Transformed<LogicalPlan>> {
    let Projection { expr, input, .. } = proj;

    let exprs_used = indices.get_at_indices(&expr);

    let required_indices =
        RequiredIndices::new().with_exprs(input.schema(), exprs_used.iter());

    // rewrite the children projection, and if they are changed rewrite the
    // projection down
    optimize_projections(Arc::unwrap_or_clone(input), config, required_indices)?
        .transform_data(|input| {
            if is_projection_unnecessary(&input, &exprs_used)? {
                Ok(Transformed::yes(input))
            } else {
                Projection::try_new(exprs_used, Arc::new(input))
                    .map(LogicalPlan::Projection)
                    .map(Transformed::yes)
            }
        })
}

/// Projection is unnecessary, when
/// - input schema of the projection, output schema of the projection are same, and
/// - all projection expressions are either Column or Literal
pub fn is_projection_unnecessary(
    input: &LogicalPlan,
    proj_exprs: &[Expr],
) -> Result<bool> {
    // First check if the number of expressions is equal to the number of fields in the input schema.
    if proj_exprs.len() != input.schema().fields().len() {
        return Ok(false);
    }
    Ok(input.schema().iter().zip(proj_exprs.iter()).all(
        |((field_relation, field_name), expr)| {
            // Check if the expression is a column and if it matches the field name
            if let Expr::Column(col) = expr {
                col.relation.as_ref() == field_relation && col.name.eq(field_name.name())
            } else {
                false
            }
        },
    ))
}

/// Returns true if the plan subtree contains any subqueries that are not the
/// CTE reference itself. This treats any non-CTE [`LogicalPlan::SubqueryAlias`]
/// node (including aliased relations) as a blocker, along with expression-level
/// subqueries like scalar, EXISTS, or IN. These cases prevent projection
/// pushdown for now because we cannot safely reason about their column usage.
fn plan_contains_other_subqueries(plan: &LogicalPlan, cte_name: &str) -> bool {
    if let LogicalPlan::SubqueryAlias(alias) = plan {
        if alias.alias.table() != cte_name
            && !subquery_alias_targets_recursive_cte(alias.input.as_ref(), cte_name)
        {
            return true;
        }
    }

    let mut found = false;
    plan.apply_expressions(|expr| {
        if expr_contains_subquery(expr) {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })
    .expect("expression traversal never fails");
    if found {
        return true;
    }

    plan.inputs()
        .into_iter()
        .any(|child| plan_contains_other_subqueries(child, cte_name))
}

fn expr_contains_subquery(expr: &Expr) -> bool {
    expr.exists(|e| match e {
        Expr::ScalarSubquery(_) | Expr::Exists(_) | Expr::InSubquery(_) => Ok(true),
        _ => Ok(false),
    })
    // Safe unwrap since we are doing a simple boolean check
    .unwrap()
}

fn subquery_alias_targets_recursive_cte(plan: &LogicalPlan, cte_name: &str) -> bool {
    match plan {
        LogicalPlan::TableScan(scan) => scan.table_name.table() == cte_name,
        LogicalPlan::SubqueryAlias(alias) => {
            subquery_alias_targets_recursive_cte(alias.input.as_ref(), cte_name)
        }
        _ => {
            let inputs = plan.inputs();
            if inputs.len() == 1 {
                subquery_alias_targets_recursive_cte(inputs[0], cte_name)
            } else {
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::collections::HashMap;
    use std::fmt::Formatter;
    use std::ops::Add;
    use std::sync::Arc;
    use std::vec;

    use crate::optimize_projections::OptimizeProjections;
    use crate::optimizer::Optimizer;
    use crate::test::{
        assert_fields_eq, scan_empty, test_table_scan, test_table_scan_fields,
        test_table_scan_with_name,
    };
    use crate::{OptimizerContext, OptimizerRule};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{
        Column, DFSchema, DFSchemaRef, JoinType, Result, TableReference,
    };
    use datafusion_expr::ExprFunctionExt;
    use datafusion_expr::{
        binary_expr, build_join_schema,
        builder::table_scan_with_filters,
        col,
        expr::{self, Cast},
        lit,
        logical_plan::{builder::LogicalPlanBuilder, table_scan},
        not, try_cast, when, BinaryExpr, Expr, Extension, Like, LogicalPlan, Operator,
        Projection, UserDefinedLogicalNodeCore, WindowFunctionDefinition,
    };
    use insta::assert_snapshot;

    use crate::assert_optimized_plan_eq_snapshot;
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_aggregate::expr_fn::{count, max, min};
    use datafusion_functions_aggregate::min_max::max_udaf;

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![Arc::new(OptimizeProjections::new())];
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                $plan,
                @ $expected,
            )
        }};
    }

    #[derive(Debug, Hash, PartialEq, Eq)]
    struct NoOpUserDefined {
        exprs: Vec<Expr>,
        schema: DFSchemaRef,
        input: Arc<LogicalPlan>,
    }

    impl NoOpUserDefined {
        fn new(schema: DFSchemaRef, input: Arc<LogicalPlan>) -> Self {
            Self {
                exprs: vec![],
                schema,
                input,
            }
        }

        fn with_exprs(mut self, exprs: Vec<Expr>) -> Self {
            self.exprs = exprs;
            self
        }
    }

    // Manual implementation needed because of `schema` field. Comparison excludes this field.
    impl PartialOrd for NoOpUserDefined {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            match self.exprs.partial_cmp(&other.exprs) {
                Some(Ordering::Equal) => self.input.partial_cmp(&other.input),
                cmp => cmp,
            }
            // TODO (https://github.com/apache/datafusion/issues/17477) avoid recomparing all fields
            .filter(|cmp| *cmp != Ordering::Equal || self == other)
        }
    }

    impl UserDefinedLogicalNodeCore for NoOpUserDefined {
        fn name(&self) -> &str {
            "NoOpUserDefined"
        }

        fn inputs(&self) -> Vec<&LogicalPlan> {
            vec![&self.input]
        }

        fn schema(&self) -> &DFSchemaRef {
            &self.schema
        }

        fn expressions(&self) -> Vec<Expr> {
            self.exprs.clone()
        }

        fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "NoOpUserDefined")
        }

        fn with_exprs_and_inputs(
            &self,
            exprs: Vec<Expr>,
            mut inputs: Vec<LogicalPlan>,
        ) -> Result<Self> {
            Ok(Self {
                exprs,
                input: Arc::new(inputs.swap_remove(0)),
                schema: Arc::clone(&self.schema),
            })
        }

        fn necessary_children_exprs(
            &self,
            output_columns: &[usize],
        ) -> Option<Vec<Vec<usize>>> {
            // Since schema is same. Output columns requires their corresponding version in the input columns.
            Some(vec![output_columns.to_vec()])
        }

        fn supports_limit_pushdown(&self) -> bool {
            false // Disallow limit push-down by default
        }
    }

    #[derive(Debug, Hash, PartialEq, Eq)]
    struct UserDefinedCrossJoin {
        exprs: Vec<Expr>,
        schema: DFSchemaRef,
        left_child: Arc<LogicalPlan>,
        right_child: Arc<LogicalPlan>,
    }

    impl UserDefinedCrossJoin {
        fn new(left_child: Arc<LogicalPlan>, right_child: Arc<LogicalPlan>) -> Self {
            let left_schema = left_child.schema();
            let right_schema = right_child.schema();
            let schema = Arc::new(
                build_join_schema(left_schema, right_schema, &JoinType::Inner).unwrap(),
            );
            Self {
                exprs: vec![],
                schema,
                left_child,
                right_child,
            }
        }
    }

    // Manual implementation needed because of `schema` field. Comparison excludes this field.
    impl PartialOrd for UserDefinedCrossJoin {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            match self.exprs.partial_cmp(&other.exprs) {
                Some(Ordering::Equal) => {
                    match self.left_child.partial_cmp(&other.left_child) {
                        Some(Ordering::Equal) => {
                            self.right_child.partial_cmp(&other.right_child)
                        }
                        cmp => cmp,
                    }
                }
                cmp => cmp,
            }
            // TODO (https://github.com/apache/datafusion/issues/17477) avoid recomparing all fields
            .filter(|cmp| *cmp != Ordering::Equal || self == other)
        }
    }

    impl UserDefinedLogicalNodeCore for UserDefinedCrossJoin {
        fn name(&self) -> &str {
            "UserDefinedCrossJoin"
        }

        fn inputs(&self) -> Vec<&LogicalPlan> {
            vec![&self.left_child, &self.right_child]
        }

        fn schema(&self) -> &DFSchemaRef {
            &self.schema
        }

        fn expressions(&self) -> Vec<Expr> {
            self.exprs.clone()
        }

        fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "UserDefinedCrossJoin")
        }

        fn with_exprs_and_inputs(
            &self,
            exprs: Vec<Expr>,
            mut inputs: Vec<LogicalPlan>,
        ) -> Result<Self> {
            assert_eq!(inputs.len(), 2);
            Ok(Self {
                exprs,
                left_child: Arc::new(inputs.remove(0)),
                right_child: Arc::new(inputs.remove(0)),
                schema: Arc::clone(&self.schema),
            })
        }

        fn necessary_children_exprs(
            &self,
            output_columns: &[usize],
        ) -> Option<Vec<Vec<usize>>> {
            let left_child_len = self.left_child.schema().fields().len();
            let mut left_reqs = vec![];
            let mut right_reqs = vec![];
            for &out_idx in output_columns {
                if out_idx < left_child_len {
                    left_reqs.push(out_idx);
                } else {
                    // Output indices further than the left_child_len
                    // comes from right children
                    right_reqs.push(out_idx - left_child_len)
                }
            }
            Some(vec![left_reqs, right_reqs])
        }

        fn supports_limit_pushdown(&self) -> bool {
            false // Disallow limit push-down by default
        }
    }

    #[test]
    fn merge_two_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .project(vec![binary_expr(lit(1), Operator::Plus, col("a"))])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: Int32(1) + test.a
          TableScan: test projection=[a]
        "
        )
    }

    #[test]
    fn merge_three_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .project(vec![col("a")])?
            .project(vec![binary_expr(lit(1), Operator::Plus, col("a"))])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: Int32(1) + test.a
          TableScan: test projection=[a]
        "
        )
    }

    #[test]
    fn merge_alias() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .project(vec![col("a").alias("alias")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a AS alias
          TableScan: test projection=[a]
        "
        )
    }

    #[test]
    fn merge_nested_alias() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("alias1").alias("alias2")])?
            .project(vec![col("alias2").alias("alias")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a AS alias
          TableScan: test projection=[a]
        "
        )
    }

    #[test]
    fn test_nested_count() -> Result<()> {
        let schema = Schema::new(vec![Field::new("foo", DataType::Int32, false)]);

        let groups: Vec<Expr> = vec![];

        let plan = table_scan(TableReference::none(), &schema, None)
            .unwrap()
            .aggregate(groups.clone(), vec![count(lit(1))])
            .unwrap()
            .aggregate(groups, vec![count(lit(1))])
            .unwrap()
            .build()
            .unwrap();

        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[]], aggr=[[count(Int32(1))]]
          EmptyRelation: rows=1
        "
        )
    }

    #[test]
    fn test_neg_push_down() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![-col("a")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: (- test.a)
          TableScan: test projection=[a]
        "
        )
    }

    #[test]
    fn test_is_null() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").is_null()])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a IS NULL
          TableScan: test projection=[a]
        "
        )
    }

    #[test]
    fn test_is_not_null() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").is_not_null()])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a IS NOT NULL
          TableScan: test projection=[a]
        "
        )
    }

    #[test]
    fn test_is_true() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").is_true()])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a IS TRUE
          TableScan: test projection=[a]
        "
        )
    }

    #[test]
    fn test_is_not_true() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").is_not_true()])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a IS NOT TRUE
          TableScan: test projection=[a]
        "
        )
    }

    #[test]
    fn test_is_false() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").is_false()])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a IS FALSE
          TableScan: test projection=[a]
        "
        )
    }

    #[test]
    fn test_is_not_false() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").is_not_false()])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a IS NOT FALSE
          TableScan: test projection=[a]
        "
        )
    }

    #[test]
    fn test_is_unknown() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").is_unknown()])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a IS UNKNOWN
          TableScan: test projection=[a]
        "
        )
    }

    #[test]
    fn test_is_not_unknown() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").is_not_unknown()])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a IS NOT UNKNOWN
          TableScan: test projection=[a]
        "
        )
    }

    #[test]
    fn test_not() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![not(col("a"))])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: NOT test.a
          TableScan: test projection=[a]
        "
        )
    }

    #[test]
    fn test_try_cast() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![try_cast(col("a"), DataType::Float64)])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: TRY_CAST(test.a AS Float64)
          TableScan: test projection=[a]
        "
        )
    }

    #[test]
    fn test_similar_to() -> Result<()> {
        let table_scan = test_table_scan()?;
        let expr = Box::new(col("a"));
        let pattern = Box::new(lit("[0-9]"));
        let similar_to_expr =
            Expr::SimilarTo(Like::new(false, expr, pattern, None, false));
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![similar_to_expr])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r#"
        Projection: test.a SIMILAR TO Utf8("[0-9]")
          TableScan: test projection=[a]
        "#
        )
    }

    #[test]
    fn test_between() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").between(lit(1), lit(3))])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a BETWEEN Int32(1) AND Int32(3)
          TableScan: test projection=[a]
        "
        )
    }

    // Test Case expression
    #[test]
    fn test_case_merged() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), lit(0).alias("d")])?
            .project(vec![
                col("a"),
                when(col("a").eq(lit(1)), lit(10))
                    .otherwise(col("d"))?
                    .alias("d"),
            ])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a, CASE WHEN test.a = Int32(1) THEN Int32(10) ELSE Int32(0) END AS d
          TableScan: test projection=[a]
        "
        )
    }

    // Test outer projection isn't discarded despite the same schema as inner
    // https://github.com/apache/datafusion/issues/8942
    #[test]
    fn test_derived_column() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").add(lit(1)).alias("a"), lit(0).alias("d")])?
            .project(vec![
                col("a"),
                when(col("a").eq(lit(1)), lit(10))
                    .otherwise(col("d"))?
                    .alias("d"),
            ])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: a, CASE WHEN a = Int32(1) THEN Int32(10) ELSE d END AS d
          Projection: test.a + Int32(1) AS a, Int32(0) AS d
            TableScan: test projection=[a]
        "
        )
    }

    // Since only column `a` is referred at the output. Scan should only contain projection=[a].
    // User defined node should be able to propagate necessary expressions by its parent to its child.
    #[test]
    fn test_user_defined_logical_plan_node() -> Result<()> {
        let table_scan = test_table_scan()?;
        let custom_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(NoOpUserDefined::new(
                Arc::clone(table_scan.schema()),
                Arc::new(table_scan.clone()),
            )),
        });
        let plan = LogicalPlanBuilder::from(custom_plan)
            .project(vec![col("a"), lit(0).alias("d")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a, Int32(0) AS d
          NoOpUserDefined
            TableScan: test projection=[a]
        "
        )
    }

    // Only column `a` is referred at the output. However, User defined node itself uses column `b`
    // during its operation. Hence, scan should contain projection=[a, b].
    // User defined node should be able to propagate necessary expressions by its parent, as well as its own
    // required expressions.
    #[test]
    fn test_user_defined_logical_plan_node2() -> Result<()> {
        let table_scan = test_table_scan()?;
        let exprs = vec![Expr::Column(Column::from_qualified_name("b"))];
        let custom_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(
                NoOpUserDefined::new(
                    Arc::clone(table_scan.schema()),
                    Arc::new(table_scan.clone()),
                )
                .with_exprs(exprs),
            ),
        });
        let plan = LogicalPlanBuilder::from(custom_plan)
            .project(vec![col("a"), lit(0).alias("d")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a, Int32(0) AS d
          NoOpUserDefined
            TableScan: test projection=[a, b]
        "
        )
    }

    // Only column `a` is referred at the output. However, User defined node itself uses expression `b+c`
    // during its operation. Hence, scan should contain projection=[a, b, c].
    // User defined node should be able to propagate necessary expressions by its parent, as well as its own
    // required expressions. Expressions doesn't have to be just column. Requirements from complex expressions
    // should be propagated also.
    #[test]
    fn test_user_defined_logical_plan_node3() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left_expr = Expr::Column(Column::from_qualified_name("b"));
        let right_expr = Expr::Column(Column::from_qualified_name("c"));
        let binary_expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left_expr),
            Operator::Plus,
            Box::new(right_expr),
        ));
        let exprs = vec![binary_expr];
        let custom_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(
                NoOpUserDefined::new(
                    Arc::clone(table_scan.schema()),
                    Arc::new(table_scan.clone()),
                )
                .with_exprs(exprs),
            ),
        });
        let plan = LogicalPlanBuilder::from(custom_plan)
            .project(vec![col("a"), lit(0).alias("d")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a, Int32(0) AS d
          NoOpUserDefined
            TableScan: test projection=[a, b, c]
        "
        )
    }

    // Columns `l.a`, `l.c`, `r.a` is referred at the output.
    // User defined node should be able to propagate necessary expressions by its parent, to its children.
    // Even if it has multiple children.
    // left child should have `projection=[a, c]`, and right side should have `projection=[a]`.
    #[test]
    fn test_user_defined_logical_plan_node4() -> Result<()> {
        let left_table = test_table_scan_with_name("l")?;
        let right_table = test_table_scan_with_name("r")?;
        let custom_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(UserDefinedCrossJoin::new(
                Arc::new(left_table),
                Arc::new(right_table),
            )),
        });
        let plan = LogicalPlanBuilder::from(custom_plan)
            .project(vec![col("l.a"), col("l.c"), col("r.a"), lit(0).alias("d")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: l.a, l.c, r.a, Int32(0) AS d
          UserDefinedCrossJoin
            TableScan: l projection=[a, c]
            TableScan: r projection=[a]
        "
        )
    }

    #[test]
    fn aggregate_no_group_by() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![max(col("b"))])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[]], aggr=[[max(test.b)]]
          TableScan: test projection=[b]
        "
        )
    }

    #[test]
    fn aggregate_group_by() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("c")], vec![max(col("b"))])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[test.c]], aggr=[[max(test.b)]]
          TableScan: test projection=[b, c]
        "
        )
    }

    #[test]
    fn aggregate_group_by_with_table_alias() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .alias("a")?
            .aggregate(vec![col("c")], vec![max(col("b"))])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[a.c]], aggr=[[max(a.b)]]
          SubqueryAlias: a
            TableScan: test projection=[b, c]
        "
        )
    }

    #[test]
    fn aggregate_no_group_by_with_filter() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("c").gt(lit(1)))?
            .aggregate(Vec::<Expr>::new(), vec![max(col("b"))])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[]], aggr=[[max(test.b)]]
          Projection: test.b
            Filter: test.c > Int32(1)
              TableScan: test projection=[b, c]
        "
        )
    }

    #[test]
    fn aggregate_with_periods() -> Result<()> {
        let schema = Schema::new(vec![Field::new("tag.one", DataType::Utf8, false)]);

        // Build a plan that looks as follows (note "tag.one" is a column named
        // "tag.one", not a column named "one" in a table named "tag"):
        //
        // Projection: tag.one
        //   Aggregate: groupBy=[], aggr=[max("tag.one") AS "tag.one"]
        //    TableScan
        let plan = table_scan(Some("m4"), &schema, None)?
            .aggregate(
                Vec::<Expr>::new(),
                vec![max(col(Column::new_unqualified("tag.one"))).alias("tag.one")],
            )?
            .project([col(Column::new_unqualified("tag.one"))])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[]], aggr=[[max(m4.tag.one) AS tag.one]]
          TableScan: m4 projection=[tag.one]
        "
        )
    }

    #[test]
    fn redundant_project() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .project(vec![col("a"), col("c"), col("b")])?
            .build()?;
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a, test.c, test.b
          TableScan: test projection=[a, b, c]
        "
        )
    }

    #[test]
    fn reorder_scan() -> Result<()> {
        let schema = Schema::new(test_table_scan_fields());

        let plan = table_scan(Some("test"), &schema, Some(vec![1, 0, 2]))?.build()?;
        assert_optimized_plan_equal!(
            plan,
            @"TableScan: test projection=[b, a, c]"
        )
    }

    #[test]
    fn reorder_scan_projection() -> Result<()> {
        let schema = Schema::new(test_table_scan_fields());

        let plan = table_scan(Some("test"), &schema, Some(vec![1, 0, 2]))?
            .project(vec![col("a"), col("b")])?
            .build()?;
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a, test.b
          TableScan: test projection=[b, a]
        "
        )
    }

    #[test]
    fn reorder_projection() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("c"), col("b"), col("a")])?
            .build()?;
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.c, test.b, test.a
          TableScan: test projection=[a, b, c]
        "
        )
    }

    #[test]
    fn noncontinuous_redundant_projection() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("c"), col("b"), col("a")])?
            .filter(col("c").gt(lit(1)))?
            .project(vec![col("c"), col("a"), col("b")])?
            .filter(col("b").gt(lit(1)))?
            .filter(col("a").gt(lit(1)))?
            .project(vec![col("a"), col("c"), col("b")])?
            .build()?;
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a, test.c, test.b
          Filter: test.a > Int32(1)
            Filter: test.b > Int32(1)
              Projection: test.c, test.a, test.b
                Filter: test.c > Int32(1)
                  Projection: test.c, test.b, test.a
                    TableScan: test projection=[a, b, c]
        "
        )
    }

    #[test]
    fn join_schema_trim_full_join_column_projection() -> Result<()> {
        let table_scan = test_table_scan()?;

        let schema = Schema::new(vec![Field::new("c1", DataType::UInt32, false)]);
        let table2_scan = scan_empty(Some("test2"), &schema, None)?.build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .join(table2_scan, JoinType::Left, (vec!["a"], vec!["c1"]), None)?
            .project(vec![col("a"), col("b"), col("c1")])?
            .build()?;

        let optimized_plan = optimize(plan)?;

        // make sure projections are pushed down to both table scans
        assert_snapshot!(
            optimized_plan.clone(),
            @r"
        Left Join: test.a = test2.c1
          TableScan: test projection=[a, b]
          TableScan: test2 projection=[c1]
        "
        );

        // make sure schema for join node include both join columns
        let optimized_join = optimized_plan;
        assert_eq!(
            **optimized_join.schema(),
            DFSchema::new_with_metadata(
                vec![
                    (
                        Some("test".into()),
                        Arc::new(Field::new("a", DataType::UInt32, false))
                    ),
                    (
                        Some("test".into()),
                        Arc::new(Field::new("b", DataType::UInt32, false))
                    ),
                    (
                        Some("test2".into()),
                        Arc::new(Field::new("c1", DataType::UInt32, true))
                    ),
                ],
                HashMap::new()
            )?,
        );

        Ok(())
    }

    #[test]
    fn join_schema_trim_partial_join_column_projection() -> Result<()> {
        // test join column push down without explicit column projections

        let table_scan = test_table_scan()?;

        let schema = Schema::new(vec![Field::new("c1", DataType::UInt32, false)]);
        let table2_scan = scan_empty(Some("test2"), &schema, None)?.build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .join(table2_scan, JoinType::Left, (vec!["a"], vec!["c1"]), None)?
            // projecting joined column `a` should push the right side column `c1` projection as
            // well into test2 table even though `c1` is not referenced in projection.
            .project(vec![col("a"), col("b")])?
            .build()?;

        let optimized_plan = optimize(plan)?;

        // make sure projections are pushed down to both table scans
        assert_snapshot!(
            optimized_plan.clone(),
            @r"
        Projection: test.a, test.b
          Left Join: test.a = test2.c1
            TableScan: test projection=[a, b]
            TableScan: test2 projection=[c1]
        "
        );

        // make sure schema for join node include both join columns
        let optimized_join = optimized_plan.inputs()[0];
        assert_eq!(
            **optimized_join.schema(),
            DFSchema::new_with_metadata(
                vec![
                    (
                        Some("test".into()),
                        Arc::new(Field::new("a", DataType::UInt32, false))
                    ),
                    (
                        Some("test".into()),
                        Arc::new(Field::new("b", DataType::UInt32, false))
                    ),
                    (
                        Some("test2".into()),
                        Arc::new(Field::new("c1", DataType::UInt32, true))
                    ),
                ],
                HashMap::new()
            )?,
        );

        Ok(())
    }

    #[test]
    fn join_schema_trim_using_join() -> Result<()> {
        // shared join columns from using join should be pushed to both sides

        let table_scan = test_table_scan()?;

        let schema = Schema::new(vec![Field::new("a", DataType::UInt32, false)]);
        let table2_scan = scan_empty(Some("test2"), &schema, None)?.build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .join_using(table2_scan, JoinType::Left, vec!["a".into()])?
            .project(vec![col("a"), col("b")])?
            .build()?;

        let optimized_plan = optimize(plan)?;

        // make sure projections are pushed down to table scan
        assert_snapshot!(
            optimized_plan.clone(),
            @r"
        Projection: test.a, test.b
          Left Join: Using test.a = test2.a
            TableScan: test projection=[a, b]
            TableScan: test2 projection=[a]
        "
        );

        // make sure schema for join node include both join columns
        let optimized_join = optimized_plan.inputs()[0];
        assert_eq!(
            **optimized_join.schema(),
            DFSchema::new_with_metadata(
                vec![
                    (
                        Some("test".into()),
                        Arc::new(Field::new("a", DataType::UInt32, false))
                    ),
                    (
                        Some("test".into()),
                        Arc::new(Field::new("b", DataType::UInt32, false))
                    ),
                    (
                        Some("test2".into()),
                        Arc::new(Field::new("a", DataType::UInt32, true))
                    ),
                ],
                HashMap::new()
            )?,
        );

        Ok(())
    }

    #[test]
    fn cast() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![Expr::Cast(Cast::new(
                Box::new(col("c")),
                DataType::Float64,
            ))])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: CAST(test.c AS Float64)
          TableScan: test projection=[c]
        "
        )
    }

    #[test]
    fn table_scan_projected_schema() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .project(vec![col("a"), col("b")])?
            .build()?;

        assert_eq!(3, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);
        assert_fields_eq(&plan, vec!["a", "b"]);

        assert_optimized_plan_equal!(
            plan,
            @"TableScan: test projection=[a, b]"
        )
    }

    #[test]
    fn table_scan_projected_schema_non_qualified_relation() -> Result<()> {
        let table_scan = test_table_scan()?;
        let input_schema = table_scan.schema();
        assert_eq!(3, input_schema.fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        // Build the LogicalPlan directly (don't use PlanBuilder), so
        // that the Column references are unqualified (e.g. their
        // relation is `None`). PlanBuilder resolves the expressions
        let expr = vec![col("test.a"), col("test.b")];
        let plan =
            LogicalPlan::Projection(Projection::try_new(expr, Arc::new(table_scan))?);

        assert_fields_eq(&plan, vec!["a", "b"]);

        assert_optimized_plan_equal!(
            plan,
            @"TableScan: test projection=[a, b]"
        )
    }

    #[test]
    fn table_limit() -> Result<()> {
        let table_scan = test_table_scan()?;
        assert_eq!(3, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("c"), col("a")])?
            .limit(0, Some(5))?
            .build()?;

        assert_fields_eq(&plan, vec!["c", "a"]);

        assert_optimized_plan_equal!(
            plan,
            @r"
        Limit: skip=0, fetch=5
          Projection: test.c, test.a
            TableScan: test projection=[a, c]
        "
        )
    }

    #[test]
    fn table_scan_without_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan).build()?;
        // should expand projection to all columns without projection
        assert_optimized_plan_equal!(
            plan,
            @"TableScan: test projection=[a, b, c]"
        )
    }

    #[test]
    fn table_scan_with_literal_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![lit(1_i64), lit(2_i64)])?
            .build()?;
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: Int64(1), Int64(2)
          TableScan: test projection=[]
        "
        )
    }

    /// tests that it removes unused columns in projections
    #[test]
    fn table_unused_column() -> Result<()> {
        let table_scan = test_table_scan()?;
        assert_eq!(3, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        // we never use "b" in the first projection => remove it
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("c"), col("a"), col("b")])?
            .filter(col("c").gt(lit(1)))?
            .aggregate(vec![col("c")], vec![max(col("a"))])?
            .build()?;

        assert_fields_eq(&plan, vec!["c", "max(test.a)"]);

        let plan = optimize(plan).expect("failed to optimize plan");
        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[test.c]], aggr=[[max(test.a)]]
          Filter: test.c > Int32(1)
            Projection: test.c, test.a
              TableScan: test projection=[a, c]
        "
        )
    }

    /// tests that it removes un-needed projections
    #[test]
    fn table_unused_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        assert_eq!(3, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        // there is no need for the first projection
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("b")])?
            .project(vec![lit(1).alias("a")])?
            .build()?;

        assert_fields_eq(&plan, vec!["a"]);

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: Int32(1) AS a
          TableScan: test projection=[]
        "
        )
    }

    #[test]
    fn table_full_filter_pushdown() -> Result<()> {
        let schema = Schema::new(test_table_scan_fields());

        let table_scan = table_scan_with_filters(
            Some("test"),
            &schema,
            None,
            vec![col("b").eq(lit(1))],
        )?
        .build()?;
        assert_eq!(3, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        // there is no need for the first projection
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("b")])?
            .project(vec![lit(1).alias("a")])?
            .build()?;

        assert_fields_eq(&plan, vec!["a"]);

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: Int32(1) AS a
          TableScan: test projection=[], full_filters=[b = Int32(1)]
        "
        )
    }

    /// tests that optimizing twice yields same plan
    #[test]
    fn test_double_optimization() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("b")])?
            .project(vec![lit(1).alias("a")])?
            .build()?;

        let optimized_plan1 = optimize(plan).expect("failed to optimize plan");
        let optimized_plan2 =
            optimize(optimized_plan1.clone()).expect("failed to optimize plan");

        let formatted_plan1 = format!("{optimized_plan1:?}");
        let formatted_plan2 = format!("{optimized_plan2:?}");
        assert_eq!(formatted_plan1, formatted_plan2);
        Ok(())
    }

    /// tests that it removes an aggregate is never used downstream
    #[test]
    fn table_unused_aggregate() -> Result<()> {
        let table_scan = test_table_scan()?;
        assert_eq!(3, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        // we never use "min(b)" => remove it
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a"), col("c")], vec![max(col("b")), min(col("b"))])?
            .filter(col("c").gt(lit(1)))?
            .project(vec![col("c"), col("a"), col("max(test.b)")])?
            .build()?;

        assert_fields_eq(&plan, vec!["c", "a", "max(test.b)"]);

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.c, test.a, max(test.b)
          Filter: test.c > Int32(1)
            Aggregate: groupBy=[[test.a, test.c]], aggr=[[max(test.b)]]
              TableScan: test projection=[a, b, c]
        "
        )
    }

    #[test]
    fn aggregate_filter_pushdown() -> Result<()> {
        let table_scan = test_table_scan()?;
        let aggr_with_filter = count_udaf()
            .call(vec![col("b")])
            .filter(col("c").gt(lit(42)))
            .build()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![count(col("b")), aggr_with_filter.alias("count2")],
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[test.a]], aggr=[[count(test.b), count(test.b) FILTER (WHERE test.c > Int32(42)) AS count2]]
          TableScan: test projection=[a, b, c]
        "
        )
    }

    #[test]
    fn pushdown_through_distinct() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .distinct()?
            .project(vec![col("a")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a
          Distinct:
            TableScan: test projection=[a, b]
        "
        )
    }

    #[test]
    fn test_window() -> Result<()> {
        let table_scan = test_table_scan()?;

        let max1 = Expr::from(expr::WindowFunction::new(
            WindowFunctionDefinition::AggregateUDF(max_udaf()),
            vec![col("test.a")],
        ))
        .partition_by(vec![col("test.b")])
        .build()
        .unwrap();

        let max2 = Expr::from(expr::WindowFunction::new(
            WindowFunctionDefinition::AggregateUDF(max_udaf()),
            vec![col("test.b")],
        ));
        let col1 = col(max1.schema_name().to_string());
        let col2 = col(max2.schema_name().to_string());

        let plan = LogicalPlanBuilder::from(table_scan)
            .window(vec![max1])?
            .window(vec![max2])?
            .project(vec![col1, col2])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: max(test.a) PARTITION BY [test.b] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, max(test.b) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          WindowAggr: windowExpr=[[max(test.b) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]
            Projection: test.b, max(test.a) PARTITION BY [test.b] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
              WindowAggr: windowExpr=[[max(test.a) PARTITION BY [test.b] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]
                TableScan: test projection=[a, b]
        "
        )
    }

    fn observe(_plan: &LogicalPlan, _rule: &dyn OptimizerRule) {}

    fn optimize(plan: LogicalPlan) -> Result<LogicalPlan> {
        let optimizer = Optimizer::with_rules(vec![Arc::new(OptimizeProjections::new())]);
        let optimized_plan =
            optimizer.optimize(plan, &OptimizerContext::new(), observe)?;
        Ok(optimized_plan)
    }
}
