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

//! [`DecorrelateLateralJoin`] decorrelates logical plans produced by lateral joins.

use std::sync::Arc;

use crate::decorrelate::{PullUpCorrelatedExpr, UN_MATCHED_ROW_INDICATOR};
use crate::optimizer::ApplyOrder;
use crate::utils::evaluates_to_null;
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_expr::expr::{WindowFunction, WindowFunctionDefinition};
use datafusion_expr::{Expr, Join, expr};

use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion_common::{Column, DFSchema, Result, ScalarValue, TableReference};
use datafusion_expr::logical_plan::{JoinType, Subquery};
use datafusion_expr::utils::{conjunction, find_join_exprs, split_conjunction};
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder, SubqueryAlias};

/// Optimizer rule for rewriting lateral joins to joins
#[derive(Default, Debug)]
pub struct DecorrelateLateralJoin {}

impl DecorrelateLateralJoin {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self::default()
    }
}

impl OptimizerRule for DecorrelateLateralJoin {
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // Find cross joins with outer column references on the right side (i.e., the apply operator).
        let LogicalPlan::Join(join) = plan else {
            return Ok(Transformed::no(plan));
        };

        rewrite_internal(join, config)
    }

    fn name(&self) -> &str {
        "decorrelate_lateral_join"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

// Build the decorrelated join based on the original lateral join query.
// Supports INNER and LEFT lateral joins.
fn rewrite_internal(
    join: Join,
    config: &dyn OptimizerConfig,
) -> Result<Transformed<LogicalPlan>> {
    if !matches!(join.join_type, JoinType::Inner | JoinType::Left) {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    }
    let original_join_type = join.join_type;

    // The right side is wrapped in a Subquery node when it contains outer
    // references. Quickly skip joins that don't have this structure.
    let Some((subquery, alias)) = extract_lateral_subquery(join.right.as_ref()) else {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    };

    // If the subquery has no outer references, there is nothing to decorrelate.
    // A LATERAL with no outer references is just a cross join.
    let has_outer_refs = matches!(
        subquery.subquery.apply_with_subqueries(|p| {
            if p.contains_outer_reference() {
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })?,
        TreeNodeRecursion::Stop
    );
    if !has_outer_refs {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    }

    let subquery_plan = subquery.subquery.as_ref();
    let original_join_filter = join.filter.clone();

    // DISTINCT must be applied after the correlated filter for each outer row.
    // Pulling a filter on a non-grouped column above DISTINCT adds that column
    // to the grouping keys and can expose duplicates. Handle the simple shape
    // here by joining before the DISTINCT aggregate and grouping by a unique
    // outer-row id as well as the original DISTINCT keys.
    let row_number_udwf = config
        .function_registry()
        .and_then(|registry| registry.udwf("row_number").ok());
    if let Some(plan) =
        rewrite_correlated_distinct(&join, subquery_plan, row_number_udwf)?
    {
        return Ok(Transformed::new(plan, true, TreeNodeRecursion::Jump));
    }

    // Walk the subquery plan bottom-up, extracting correlated filter
    // predicates into join conditions and converting ungrouped aggregates
    // into group-by aggregates keyed on the correlation columns.
    let mut pull_up = PullUpCorrelatedExpr::new().with_need_handle_count_bug(true);
    let rewritten_subquery = subquery_plan.clone().rewrite(&mut pull_up).data()?;
    if !pull_up.can_pull_up {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    }

    // TODO: support HAVING in lateral subqueries.
    // <https://github.com/apache/datafusion/issues/21198>
    if pull_up.pull_up_having_expr.is_some() {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    }

    // The correlation predicates (extracted from the subquery's WHERE) become
    // the rewritten join's ON clause. See below for discussion of how the
    // user's original ON clause is handled.
    let correlation_filter = conjunction(pull_up.join_filters);

    // Look up each aggregate's default value on empty input (e.g., COUNT → 0,
    // SUM → NULL). This must happen before wrapping in SubqueryAlias, because
    // the map is keyed by LogicalPlan and wrapping changes the plan.
    let collected_count_expr_map = pull_up
        .collected_count_expr_map
        .get(&rewritten_subquery)
        .cloned();

    // Re-wrap in SubqueryAlias if the original had one, preserving the alias name.
    // The SubqueryAlias re-qualifies all columns with the alias, so we must also
    // rewrite column references in both the correlation and ON-clause filters.
    let (right_plan, correlation_filter, original_join_filter) =
        if let Some(ref alias) = alias {
            let inner_schema = Arc::clone(rewritten_subquery.schema());
            let right = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                Arc::new(rewritten_subquery),
                alias.clone(),
            )?);
            let corr = correlation_filter
                .map(|f| requalify_filter(f, &inner_schema, alias))
                .transpose()?;
            let on = original_join_filter
                .map(|f| requalify_filter(f, &inner_schema, alias))
                .transpose()?;
            (right, corr, on)
        } else {
            (rewritten_subquery, correlation_filter, original_join_filter)
        };

    // For LEFT lateral joins, verify that all column references in the
    // correlation filter are resolvable within the join's left and right
    // schemas. If the lateral subquery references columns from an outer scope,
    // the extracted filter will contain unresolvable columns and we must skip
    // decorrelation.
    //
    // INNER lateral joins do not need this check: later optimizer passes
    // (filter pushdown, join reordering) can restructure the plan to resolve
    // cross-scope references. LEFT joins cannot be freely reordered.
    if original_join_type == JoinType::Left
        && let Some(ref filter) = correlation_filter
    {
        let left_schema = join.left.schema();
        let right_schema = right_plan.schema();
        let has_outer_scope_refs = filter
            .column_refs()
            .iter()
            .any(|col| !left_schema.has_column(col) && !right_schema.has_column(col));
        if has_outer_scope_refs {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        }
    }

    // Use a left join when the user wrote LEFT LATERAL or when a scalar
    // aggregation was pulled up (preserves outer rows with no matches).
    let join_type =
        if original_join_type == JoinType::Left || pull_up.pulled_up_scalar_agg {
            JoinType::Left
        } else {
            JoinType::Inner
        };

    // The correlation predicates (extracted from the subquery's WHERE) are
    // turned into the rewritten join's ON clause. There are three cases that
    // determine how the user's original ON clause is handled:
    //
    // - INNER lateral: user ON clause becomes a post-join filter. This restores
    //   inner-join semantics if the join is upgraded to LEFT for count-bug
    //   handling.
    //
    // - LEFT lateral with grouped (or no) agg: user ON clause is merged into
    //   the rewritten ON clause, alongside the correlation predicates. LEFT
    //   join semantics correctly preserve unmatched rows with NULLs.
    //
    // - LEFT lateral with an ungrouped aggregate (which decorrelation converts
    //   to a group-by keyed on the correlation columns): user ON clause cannot
    //   be placed in the join condition (it would conflict with count-bug
    //   compensation) or as a post-join filter (that would remove
    //   left-preserved rows). Instead, a projection is added after count-bug
    //   compensation that replaces each right-side column with NULL when the ON
    //   condition is not satisfied:
    //
    //      CASE WHEN (on_cond) IS NOT TRUE THEN NULL ELSE <col> END
    //
    //   This simulates LEFT JOIN semantics for the user's ON clause without
    //   interfering with count-bug compensation.
    let (join_filter, post_join_filter, on_condition_for_projection) =
        if original_join_type == JoinType::Left {
            if pull_up.pulled_up_scalar_agg {
                (correlation_filter, None, original_join_filter)
            } else {
                let combined = conjunction(
                    correlation_filter.into_iter().chain(original_join_filter),
                );
                (combined, None, None)
            }
        } else {
            (correlation_filter, original_join_filter, None)
        };

    let left_field_count = join.left.schema().fields().len();
    let new_plan = LogicalPlanBuilder::from(join.left)
        .join_on(right_plan, join_type, join_filter)?
        .build()?;

    // Handle the count bug: in the rewritten left join, unmatched outer
    // rows get NULLs for all right-side columns. But some aggregates
    // have non-NULL defaults on empty input (e.g., COUNT returns 0, not
    // NULL). Add a projection that wraps those columns:
    //   CASE WHEN __always_true IS NULL THEN <default> ELSE <column> END
    let new_plan = if let Some(expr_map) = collected_count_expr_map {
        let join_schema = new_plan.schema();
        let alias_qualifier = alias.as_ref();
        let mut proj_exprs: Vec<Expr> = vec![];

        for (i, (qualifier, field)) in join_schema.iter().enumerate() {
            let col = Expr::Column(Column::new(qualifier.cloned(), field.name()));

            // Only compensate right-side (subquery) fields. Left-side fields
            // may share a name with an aggregate alias but must not be wrapped.
            let name = field.name();
            if i >= left_field_count
                && let Some(default_value) = expr_map.get(name.as_str())
                && !evaluates_to_null(default_value.clone(), default_value.column_refs())?
            {
                // Column whose aggregate doesn't naturally return NULL
                // on empty input (e.g., COUNT returns 0). Wrap it.
                let indicator_col =
                    Column::new(alias_qualifier.cloned(), UN_MATCHED_ROW_INDICATOR);
                let case_expr = Expr::Case(expr::Case {
                    expr: None,
                    when_then_expr: vec![(
                        Box::new(Expr::IsNull(Box::new(Expr::Column(indicator_col)))),
                        Box::new(default_value.clone()),
                    )],
                    else_expr: Some(Box::new(col)),
                });
                proj_exprs.push(Expr::Alias(expr::Alias {
                    expr: Box::new(case_expr),
                    relation: qualifier.cloned(),
                    name: name.to_string(),
                    metadata: None,
                }));
                continue;
            }
            proj_exprs.push(col);
        }

        LogicalPlanBuilder::from(new_plan)
            .project(proj_exprs)?
            .build()?
    } else {
        new_plan
    };

    // For LEFT lateral joins with an ungrouped aggregate, simulate LEFT JOIN
    // semantics for the user's ON clause by adding a projection that replaces
    // right-side columns with NULL when the ON condition is false (see
    // commentary above).
    //
    // Note: the ON condition expression is duplicated per column, so this
    // assumes it is deterministic.
    let new_plan = if let Some(on_cond) = on_condition_for_projection {
        let schema = Arc::clone(new_plan.schema());
        let mut proj_exprs: Vec<Expr> = vec![];

        for (i, (qualifier, field)) in schema.iter().enumerate() {
            let col = Expr::Column(Column::new(qualifier.cloned(), field.name()));

            if i < left_field_count {
                proj_exprs.push(col);
                continue;
            }

            let typed_null =
                Expr::Literal(ScalarValue::try_from(field.data_type())?, None);
            let case_expr = Expr::Case(expr::Case {
                expr: None,
                when_then_expr: vec![(
                    Box::new(Expr::IsNotTrue(Box::new(on_cond.clone()))),
                    Box::new(typed_null),
                )],
                else_expr: Some(Box::new(col)),
            });
            proj_exprs.push(case_expr.alias_qualified(qualifier.cloned(), field.name()));
        }

        LogicalPlanBuilder::from(new_plan)
            .project(proj_exprs)?
            .build()?
    } else {
        new_plan
    };

    // Apply the original ON clause as a post-join filter (INNER lateral only).
    let new_plan = if let Some(on_filter) = post_join_filter {
        LogicalPlanBuilder::from(new_plan)
            .filter(on_filter)?
            .build()?
    } else {
        new_plan
    };

    Ok(Transformed::new(new_plan, true, TreeNodeRecursion::Jump))
}

/// Rewrite a simple `DISTINCT(Projection(Filter(input)))` LATERAL subquery when
/// its correlated filter reads columns that are not part of the DISTINCT key.
/// Keeping a row number from the outer input in the grouping keys preserves
/// duplicate outer rows under SQL bag semantics.
fn rewrite_correlated_distinct(
    join: &Join,
    subquery: &LogicalPlan,
    row_number_udwf: Option<Arc<datafusion_expr::WindowUDF>>,
) -> Result<Option<LogicalPlan>> {
    if join.join_type != JoinType::Inner
        || join.filter.as_ref().is_some_and(|filter| {
            !matches!(filter, Expr::Literal(ScalarValue::Boolean(Some(true)), _))
        })
    {
        return Ok(None);
    }

    let LogicalPlan::Aggregate(aggregate) = subquery else {
        return Ok(None);
    };
    if !aggregate.aggr_expr.is_empty()
        || aggregate
            .group_expr
            .iter()
            .any(|expr| matches!(expr, Expr::GroupingSet(_)))
    {
        return Ok(None);
    }
    if join.right.schema().fields().len() != aggregate.group_expr.len() {
        return Ok(None);
    }
    // SQL expresses SELECT DISTINCT x as Aggregate(Projection(Filter(...)));
    // only direct-column projections are needed for this targeted rewrite.
    let (projection_expr, filter) = match aggregate.input.as_ref() {
        LogicalPlan::Projection(projection) => {
            let LogicalPlan::Filter(filter) = projection.input.as_ref() else {
                return Ok(None);
            };
            (projection.expr.as_slice(), filter)
        }
        LogicalPlan::Filter(filter) => (aggregate.group_expr.as_slice(), filter),
        _ => return Ok(None),
    };
    if projection_expr
        .iter()
        .any(|expr| !matches!(expr, Expr::Column(_)))
    {
        return Ok(None);
    }
    if !filter.predicate.contains_outer() {
        return Ok(None);
    }

    let predicates = split_conjunction(&filter.predicate);
    let (join_filters, local_filters) = find_join_exprs(predicates)?;
    if join_filters.is_empty() {
        return Ok(None);
    }

    // This special case is only needed when a correlated predicate reads an
    // inner column outside the DISTINCT key. If it reads only grouped columns,
    // the existing pull-up is semantics-preserving.
    let distinct_columns = aggregate
        .group_expr
        .iter()
        .filter_map(|expr| match expr {
            Expr::Column(column) => Some(column),
            _ => None,
        })
        .collect::<Vec<_>>();
    let pulls_non_key_column = join_filters.iter().any(|predicate| {
        predicate.column_refs().iter().any(|column| {
            filter.input.schema().has_column(column) && !distinct_columns.contains(column)
        })
    });
    if !pulls_non_key_column {
        return Ok(None);
    }
    let Some(row_number_udwf) = row_number_udwf else {
        return Ok(None);
    };

    let mut inner_group_expr = Vec::with_capacity(aggregate.group_expr.len());
    for group in &aggregate.group_expr {
        let Expr::Column(group_column) = group else {
            return Ok(None);
        };
        let Some((index, _)) = aggregate.input.schema().iter().enumerate().find(
            |(_, (qualifier, field))| {
                Column::new(qualifier.cloned(), field.name()) == *group_column
            },
        ) else {
            return Ok(None);
        };
        let Some(group_expr) = projection_expr.get(index) else {
            return Ok(None);
        };
        inner_group_expr.push(group_expr.clone());
    }

    // Leave unsupported nested scopes and non-deterministic projections to the
    // existing rule. This path only moves a filter across one DISTINCT.
    if filter.input.contains_outer_reference() {
        return Ok(None);
    }

    let mut identity_name = "__df_lateral_row_id".to_string();
    while join
        .left
        .schema()
        .has_column(&Column::new_unqualified(identity_name.clone()))
    {
        identity_name.push('_');
    }
    let row_number = Expr::WindowFunction(Box::new(WindowFunction::new(
        WindowFunctionDefinition::WindowUDF(row_number_udwf),
        vec![],
    )))
    .alias(identity_name.clone());
    let left_with_identity = LogicalPlanBuilder::from((*join.left).clone())
        .window(vec![row_number])?
        .build()?;

    let right_input = if let Some(local_filter) = conjunction(local_filters) {
        LogicalPlanBuilder::from((*filter.input).clone())
            .filter(local_filter)?
            .build()?
    } else {
        (*filter.input).clone()
    };
    let joined = LogicalPlanBuilder::from(left_with_identity)
        .join_on(right_input, JoinType::Inner, conjunction(join_filters))?
        .build()?;

    let left_count = join.left.schema().fields().len();
    let mut group_expr = joined
        .schema()
        .iter()
        .take(left_count + 1)
        .map(|(qualifier, field)| {
            Expr::Column(Column::new(qualifier.cloned(), field.name()))
        })
        .collect::<Vec<_>>();
    group_expr.extend(inner_group_expr);
    let grouped = LogicalPlanBuilder::from(joined)
        .aggregate(group_expr, Vec::<Expr>::new())?
        .build()?;

    // Restore the original lateral join's visible schema, dropping the row
    // identity and exposing only the original DISTINCT output columns.
    let mut projection = Vec::with_capacity(join.schema.fields().len());
    for (index, (target_qualifier, target_field)) in join.schema.iter().enumerate() {
        let source_index = if index < left_count { index } else { index + 1 };
        let Some((source_qualifier, source_field)) =
            grouped.schema().iter().nth(source_index)
        else {
            return Ok(None);
        };
        let column =
            Expr::Column(Column::new(source_qualifier.cloned(), source_field.name()));
        projection
            .push(column.alias_qualified(target_qualifier.cloned(), target_field.name()));
    }
    let projected = LogicalPlanBuilder::from(grouped)
        .project(projection)?
        .build()?;

    Ok(Some(projected))
}

/// Extract the Subquery and optional alias from a lateral join's right side.
fn extract_lateral_subquery(
    plan: &LogicalPlan,
) -> Option<(Subquery, Option<TableReference>)> {
    match plan {
        LogicalPlan::Subquery(sq) => Some((sq.clone(), None)),
        LogicalPlan::SubqueryAlias(SubqueryAlias { input, alias, .. }) => {
            if let LogicalPlan::Subquery(sq) = input.as_ref() {
                Some((sq.clone(), Some(alias.clone())))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Rewrite column references in a join filter expression so that columns
/// belonging to the inner (right) side use the SubqueryAlias qualifier.
///
/// The `PullUpCorrelatedExpr` pass extracts join filters with the inner
/// columns qualified by their original table names (e.g., `t2.t1_id`).
/// When the inner plan is wrapped in a `SubqueryAlias("sub")`, those
/// columns are re-qualified as `sub.t1_id`. This function applies the
/// same requalification to the filter so it matches the aliased schema.
fn requalify_filter(
    filter: Expr,
    inner_schema: &DFSchema,
    alias: &TableReference,
) -> Result<Expr> {
    filter
        .transform(|expr| {
            if let Expr::Column(col) = &expr
                && inner_schema.has_column(col)
            {
                let new_col = Column::new(Some(alias.clone()), col.name.clone());
                return Ok(Transformed::yes(Expr::Column(new_col)));
            }
            Ok(Transformed::no(expr))
        })
        .data()
}
