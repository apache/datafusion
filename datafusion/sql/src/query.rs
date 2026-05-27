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

use std::collections::HashSet;
use std::sync::Arc;

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};

use crate::stack::StackGuard;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{Constraints, DFSchema, DFSchemaRef, Result, not_impl_err};
use datafusion_expr::expr::{Sort, WildcardOptions};
use datafusion_expr::logical_plan::{
    Extension, MaterializedCteProducer, MaterializedCteReader,
};

use datafusion_expr::select_expr::SelectExpr;
use datafusion_expr::{
    CreateMemoryTable, DdlStatement, Distinct, Expr, LogicalPlan, LogicalPlanBuilder,
    Operator,
};
use sqlparser::ast::{
    Expr as SQLExpr, ExprWithAliasAndOrderBy, Ident, LimitClause, Offset, OffsetRows,
    OrderBy, OrderByExpr, OrderByKind, PipeOperator, Query, SelectInto, SetExpr,
    SetOperator, SetQuantifier, TableAlias,
};
use sqlparser::tokenizer::Span;

impl<S: ContextProvider> SqlToRel<'_, S> {
    /// Generate a logical plan from an SQL query/subquery
    pub(crate) fn query_to_plan(
        &self,
        query: Query,
        outer_planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        // Each query has its own planner context, including CTEs that are visible within that query.
        // It also inherits the CTEs from the outer query by cloning the outer planner context.
        let mut query_plan_context = outer_planner_context.clone();
        let planner_context = &mut query_plan_context;

        let Query {
            with,
            body,
            order_by,
            limit_clause,
            fetch,
            locks: _,
            for_clause: _,
            settings: _,
            format_clause: _,
            pipe_operators,
        } = query;

        if fetch.is_some() {
            return not_impl_err!("FETCH clause is not supported yet");
        }

        let has_with = with.is_some();
        if let Some(with) = with {
            self.plan_with_clause(with, planner_context)?;
        }

        let set_expr = *body;
        let plan = match set_expr {
            SetExpr::Select(mut select) => {
                let select_into = select.into.take();
                let plan =
                    self.select_to_plan(*select, order_by.clone(), planner_context)?;
                let plan = self.limit(plan, limit_clause.clone(), planner_context)?;
                // Process the `SELECT INTO` after `LIMIT`.
                self.select_into(plan, select_into)
            }
            other => {
                // The functions called from `set_expr_to_plan()` need more than 128KB
                // stack in debug builds as investigated in:
                // https://github.com/apache/datafusion/pull/13310#discussion_r1836813902
                let plan = {
                    // scope for dropping _guard
                    let _guard = StackGuard::new(256 * 1024);
                    self.set_expr_to_plan(other, planner_context)
                }?;
                let oby_exprs = to_order_by_exprs(order_by)?;
                let order_by_rex = self.order_by_to_sort_expr(
                    oby_exprs,
                    plan.schema(),
                    planner_context,
                    true,
                    None,
                )?;
                let plan = self.order_by(plan, order_by_rex)?;
                self.limit(plan, limit_clause, planner_context)
            }
        }?;

        let plan = self.pipe_operators(plan, pipe_operators, planner_context)?;

        // Apply CTE materialization if this query had a WITH clause
        if has_with {
            self.apply_cte_materialization(plan, planner_context)
        } else {
            Ok(plan)
        }
    }

    /// Apply CTE materialization to the plan.
    ///
    /// For each CTE that should be materialized, this replaces SubqueryAlias
    /// references with MaterializedCteReader nodes and wraps the plan in
    /// MaterializedCteProducer nodes.
    fn apply_cte_materialization(
        &self,
        plan: LogicalPlan,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        // Check if materialized CTEs are enabled
        if !self
            .context_provider
            .options()
            .execution
            .enable_materialized_ctes
        {
            return Ok(plan);
        }

        // Collect CTE names that should be materialized
        let cte_names: Vec<String> = planner_context.cte_names().cloned().collect();
        let mut ctes_to_materialize: Vec<(String, LogicalPlan)> = Vec::new();

        for cte_name in &cte_names {
            // Skip recursive CTEs (they have their own execution mechanism)
            if planner_context.is_recursive_cte(cte_name) {
                continue;
            }

            // Skip CTEs explicitly marked NOT MATERIALIZED
            if planner_context.is_not_materialized_cte(cte_name) {
                continue;
            }

            // Count references in the plan tree
            let ref_count = count_cte_references(&plan, cte_name);
            // Determine if we should materialize:
            // 1. Explicitly marked MATERIALIZED, OR
            // 2. CTEs referenced more than once.
            let should_materialize = planner_context.is_materialized_cte(cte_name)
                || (ref_count > 1 && {
                    let cte_plan = planner_context.get_cte(cte_name);
                    cte_plan.is_some_and(|cte_plan| {
                        should_materialize_multi_reference_cte(
                            cte_plan, cte_name, &plan, ref_count,
                        )
                    })
                });

            if should_materialize
                && ref_count > 0
                && let Some(cte_plan) = planner_context.get_cte(cte_name)
            {
                ctes_to_materialize.push((cte_name.clone(), cte_plan.clone()));
            }
        }

        if ctes_to_materialize.is_empty() {
            return Ok(plan);
        }

        // Sort CTEs by dependency order: CTEs that depend on other CTEs
        // should be processed first (wrapped innermost = executed last)
        ctes_to_materialize.sort_by(|(name_a, _), (name_b, _)| {
            let a_deps_on_b = planner_context
                .get_cte(name_a)
                .is_some_and(|p| plan_references_cte(p, name_b));
            let b_deps_on_a = planner_context
                .get_cte(name_b)
                .is_some_and(|p| plan_references_cte(p, name_a));
            if a_deps_on_b {
                std::cmp::Ordering::Less
            } else if b_deps_on_a {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        });

        // Apply materialization: replace references and wrap plan
        let mut result_plan = plan;
        for (cte_name, cte_plan) in ctes_to_materialize {
            // Replace all SubqueryAlias references to this CTE with readers
            result_plan =
                replace_cte_with_reader(result_plan, &cte_name, cte_plan.schema())?;

            // Wrap the plan in a producer
            let producer = MaterializedCteProducer {
                name: cte_name.clone(),
                cte_plan: Arc::new(cte_plan),
                continuation: Arc::new(result_plan.clone()),
                schema: Arc::clone(result_plan.schema()),
            };
            result_plan = LogicalPlan::Extension(Extension {
                node: Arc::new(producer),
            });
        }

        Ok(result_plan)
    }

    /// Apply pipe operators to a plan
    fn pipe_operators(
        &self,
        mut plan: LogicalPlan,
        pipe_operators: Vec<PipeOperator>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        for pipe_operator in pipe_operators {
            plan = self.pipe_operator(plan, pipe_operator, planner_context)?;
        }
        Ok(plan)
    }

    /// Apply a pipe operator to a plan
    fn pipe_operator(
        &self,
        plan: LogicalPlan,
        pipe_operator: PipeOperator,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        match pipe_operator {
            PipeOperator::Where { expr } => {
                self.plan_selection(Some(expr), plan, planner_context)
            }
            PipeOperator::OrderBy { exprs } => {
                let sort_exprs = self.order_by_to_sort_expr(
                    exprs,
                    plan.schema(),
                    planner_context,
                    true,
                    None,
                )?;
                self.order_by(plan, sort_exprs)
            }
            PipeOperator::Limit { expr, offset } => self.limit(
                plan,
                Some(LimitClause::LimitOffset {
                    limit: Some(expr),
                    offset: offset.map(|offset| Offset {
                        value: offset,
                        rows: OffsetRows::None,
                    }),
                    limit_by: vec![],
                }),
                planner_context,
            ),
            PipeOperator::Select { exprs } => {
                let empty_from = matches!(plan, LogicalPlan::EmptyRelation(_));
                let select_exprs =
                    self.prepare_select_exprs(&plan, exprs, empty_from, planner_context)?;
                self.project(plan, select_exprs, None)
            }
            PipeOperator::Extend { exprs } => {
                let empty_from = matches!(plan, LogicalPlan::EmptyRelation(_));
                let extend_exprs =
                    self.prepare_select_exprs(&plan, exprs, empty_from, planner_context)?;
                let all_exprs =
                    std::iter::once(SelectExpr::Wildcard(WildcardOptions::default()))
                        .chain(extend_exprs)
                        .collect();
                self.project(plan, all_exprs, None)
            }
            PipeOperator::As { alias } => self.apply_table_alias(
                plan,
                TableAlias {
                    name: alias,
                    // Apply to all fields
                    columns: vec![],
                    explicit: true,
                    at: None,
                },
            ),
            PipeOperator::Union {
                set_quantifier,
                queries,
            } => self.pipe_operator_set(
                plan,
                SetOperator::Union,
                set_quantifier,
                queries,
                planner_context,
            ),
            PipeOperator::Intersect {
                set_quantifier,
                queries,
            } => self.pipe_operator_set(
                plan,
                SetOperator::Intersect,
                set_quantifier,
                queries,
                planner_context,
            ),
            PipeOperator::Except {
                set_quantifier,
                queries,
            } => self.pipe_operator_set(
                plan,
                SetOperator::Except,
                set_quantifier,
                queries,
                planner_context,
            ),
            PipeOperator::Aggregate {
                full_table_exprs,
                group_by_expr,
            } => self.pipe_operator_aggregate(
                plan,
                full_table_exprs,
                group_by_expr,
                planner_context,
            ),
            PipeOperator::Join(join) => {
                self.parse_relation_join(plan, join, planner_context)
            }

            x => not_impl_err!("`{x}` pipe operator is not supported yet"),
        }
    }

    /// Handle Union/Intersect/Except pipe operators
    fn pipe_operator_set(
        &self,
        mut plan: LogicalPlan,
        set_operator: SetOperator,
        set_quantifier: SetQuantifier,
        queries: Vec<Query>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        for query in queries {
            let right_plan = self.query_to_plan(query, planner_context)?;
            plan = self.set_operation_to_plan(
                set_operator,
                plan,
                right_plan,
                set_quantifier,
            )?;
        }

        Ok(plan)
    }

    /// Wrap a plan in a limit
    fn limit(
        &self,
        input: LogicalPlan,
        limit_clause: Option<LimitClause>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let Some(limit_clause) = limit_clause else {
            return Ok(input);
        };

        let empty_schema = DFSchema::empty();

        let (skip, fetch, limit_by_exprs) = match limit_clause {
            LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            } => {
                let skip = offset
                    .map(|o| self.sql_to_expr(o.value, &empty_schema, planner_context))
                    .transpose()?;

                let fetch = limit
                    .map(|e| self.sql_to_expr(e, &empty_schema, planner_context))
                    .transpose()?;

                let limit_by_exprs = limit_by
                    .into_iter()
                    .map(|e| self.sql_to_expr(e, &empty_schema, planner_context))
                    .collect::<Result<Vec<_>>>()?;

                (skip, fetch, limit_by_exprs)
            }
            LimitClause::OffsetCommaLimit { offset, limit } => {
                let skip =
                    Some(self.sql_to_expr(offset, &empty_schema, planner_context)?);
                let fetch =
                    Some(self.sql_to_expr(limit, &empty_schema, planner_context)?);
                (skip, fetch, vec![])
            }
        };

        if !limit_by_exprs.is_empty() {
            return not_impl_err!("LIMIT BY clause is not supported yet");
        }

        if skip.is_none() && fetch.is_none() {
            return Ok(input);
        }

        LogicalPlanBuilder::from(input)
            .limit_by_expr(skip, fetch)?
            .build()
    }

    /// Wrap the logical in a sort
    pub(super) fn order_by(
        &self,
        plan: LogicalPlan,
        order_by: Vec<Sort>,
    ) -> Result<LogicalPlan> {
        if order_by.is_empty() {
            return Ok(plan);
        }

        if let LogicalPlan::Distinct(Distinct::On(ref distinct_on)) = plan {
            // In case of `DISTINCT ON` we must capture the sort expressions since during the plan
            // optimization we're effectively doing a `first_value` aggregation according to them.
            let distinct_on = distinct_on.clone().with_sort_expr(order_by)?;
            Ok(LogicalPlan::Distinct(Distinct::On(distinct_on)))
        } else {
            LogicalPlanBuilder::from(plan).sort(order_by)?.build()
        }
    }

    /// Handle AGGREGATE pipe operator
    fn pipe_operator_aggregate(
        &self,
        plan: LogicalPlan,
        full_table_exprs: Vec<ExprWithAliasAndOrderBy>,
        group_by_expr: Vec<ExprWithAliasAndOrderBy>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let plan_schema = plan.schema();
        let process_expr =
            |expr_with_alias_and_order_by: ExprWithAliasAndOrderBy,
             planner_context: &mut PlannerContext| {
                let expr_with_alias = expr_with_alias_and_order_by.expr;
                let sql_expr = expr_with_alias.expr;
                let alias = expr_with_alias.alias;

                let df_expr = self.sql_to_expr(sql_expr, plan_schema, planner_context)?;

                match alias {
                    Some(alias_ident) => df_expr.alias_if_changed(alias_ident.value),
                    None => Ok(df_expr),
                }
            };

        let aggr_exprs: Vec<Expr> = full_table_exprs
            .into_iter()
            .map(|e| process_expr(e, planner_context))
            .collect::<Result<Vec<_>>>()?;

        let group_by_exprs: Vec<Expr> = group_by_expr
            .into_iter()
            .map(|e| process_expr(e, planner_context))
            .collect::<Result<Vec<_>>>()?;

        LogicalPlanBuilder::from(plan)
            .aggregate(group_by_exprs, aggr_exprs)?
            .build()
    }

    /// Wrap the logical plan in a `SelectInto`
    fn select_into(
        &self,
        plan: LogicalPlan,
        select_into: Option<SelectInto>,
    ) -> Result<LogicalPlan> {
        match select_into {
            Some(into) => Ok(LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(
                CreateMemoryTable {
                    name: self.object_name_to_table_reference(into.name)?,
                    constraints: Constraints::default(),
                    input: Arc::new(plan),
                    if_not_exists: false,
                    or_replace: false,
                    temporary: false,
                    column_defaults: vec![],
                },
            ))),
            _ => Ok(plan),
        }
    }
}

/// Decide whether to materialize a CTE referenced more than once.
///
/// Multi-reference CTEs stay materialized by default, but cheap CTEs and CTEs
/// consumed below a top-level limit are left inline. Aggregate/distinct/window
/// CTEs and complex CTEs with many base table references stay materialized.
fn should_materialize_multi_reference_cte(
    cte_plan: &LogicalPlan,
    cte_name: &str,
    continuation_plan: &LogicalPlan,
    ref_count: usize,
) -> bool {
    if ref_count <= 1 || is_cheap_to_inline(cte_plan) {
        return false;
    }

    if ends_in_aggregate_distinct_or_window(cte_plan) {
        if consumers_apply_disjoint_group_key_filters(
            cte_name,
            continuation_plan,
            ref_count,
        ) {
            return false;
        }
        return true;
    }

    let base_table_references = count_base_table_references(cte_plan);
    if base_table_references > 2 && base_table_references * ref_count > 10 {
        return true;
    }

    !contains_limit_on_single_child_path(continuation_plan)
}

fn ends_in_aggregate_distinct_or_window(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Aggregate(_) => true,
        LogicalPlan::Distinct(_) => true,
        LogicalPlan::Window(_) => true,
        _ => {
            let inputs = plan.inputs();
            inputs.len() == 1 && ends_in_aggregate_distinct_or_window(inputs[0])
        }
    }
}

/// Detects Q39-style patterns where each CTE reference is filtered on a different
/// literal value of a group-by key. In this case inlining is better because the
/// optimizer can push the filter through the aggregate, specializing each copy.
fn consumers_apply_disjoint_group_key_filters(
    cte_name: &str,
    continuation_plan: &LogicalPlan,
    ref_count: usize,
) -> bool {
    let per_ref_filters = collect_per_reference_filters(continuation_plan, cte_name);
    if per_ref_filters.len() != ref_count || per_ref_filters.is_empty() {
        return false;
    }

    // Collect all column names that appear in any reference's filters.
    let all_col_names: HashSet<&str> = per_ref_filters
        .iter()
        .flat_map(|filters| filters.iter().map(|(col, _)| col.as_str()))
        .collect();

    // For each column, check if every reference applies an equality filter on it
    // with a distinct literal value per reference.
    for col_name in all_col_names {
        let mut seen_values: HashSet<&str> = HashSet::new();
        let mut all_have_filter = true;
        for filters in &per_ref_filters {
            let mut found = false;
            for (filter_col, filter_val) in filters {
                if filter_col == col_name {
                    seen_values.insert(filter_val.as_str());
                    found = true;
                    break;
                }
            }
            if !found {
                all_have_filter = false;
                break;
            }
        }
        if all_have_filter && seen_values.len() == ref_count {
            return true;
        }
    }

    false
}

/// For each CTE reference in the continuation plan, collect equality filter
/// conditions (column_name, literal_value) that are attributed to that specific
/// reference. Uses column qualifiers to match filters to the correct reference.
fn collect_per_reference_filters(
    plan: &LogicalPlan,
    cte_name: &str,
) -> Vec<Vec<(String, String)>> {
    // Step 1: Find all CTE reference aliases and any filters on the path.
    // A CTE reference is SubqueryAlias(cte_name) wrapped by an outer alias.
    // Example: SubqueryAlias("inv1") → SubqueryAlias("inv") → [CTE body]
    let mut ref_aliases: Vec<String> = Vec::new();
    collect_cte_ref_aliases(plan, cte_name, &mut ref_aliases);

    if ref_aliases.is_empty() {
        return Vec::new();
    }

    // Step 2: Collect all equality filters from the plan (before the join).
    // These are qualified like "inv1.d_moy = 4"
    let mut all_filters: Vec<(Option<String>, String, String)> = Vec::new();
    collect_all_equality_filters(plan, cte_name, &mut all_filters);

    // Step 3: For each reference alias, find the filters that target it.
    let mut results = Vec::new();
    for alias in &ref_aliases {
        let mut ref_filters = Vec::new();
        for (qualifier, col_name, value) in &all_filters {
            if qualifier.as_deref() == Some(alias.as_str()) {
                ref_filters.push((col_name.clone(), value.clone()));
            }
        }
        results.push(ref_filters);
    }

    results
}

/// Find the outer aliases wrapping each CTE reference.
/// For "FROM inv inv1, inv inv2", finds ["inv1", "inv2"]
fn collect_cte_ref_aliases(
    plan: &LogicalPlan,
    cte_name: &str,
    aliases: &mut Vec<String>,
) {
    if let LogicalPlan::SubqueryAlias(outer_alias) = plan
        && outer_alias.alias.table() != cte_name
        && let LogicalPlan::SubqueryAlias(inner) = outer_alias.input.as_ref()
        && inner.alias.table() == cte_name
    {
        aliases.push(outer_alias.alias.table().to_string());
        return;
    }
    for input in plan.inputs() {
        collect_cte_ref_aliases(input, cte_name, aliases);
    }
}

/// Collect equality conditions from Filter nodes, extracting (qualifier, column_name, value).
/// Also handles simple constant arithmetic (like 4+1).
fn collect_all_equality_filters(
    plan: &LogicalPlan,
    cte_name: &str,
    out: &mut Vec<(Option<String>, String, String)>,
) {
    if let LogicalPlan::SubqueryAlias(alias) = plan
        && alias.alias.table() == cte_name
    {
        return;
    }

    if let LogicalPlan::Filter(filter) = plan {
        extract_qualified_equality_conditions(&filter.predicate, out);
    }

    for input in plan.inputs() {
        collect_all_equality_filters(input, cte_name, out);
    }
}

fn extract_qualified_equality_conditions(
    expr: &Expr,
    out: &mut Vec<(Option<String>, String, String)>,
) {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            match (binary.left.as_ref(), binary.right.as_ref()) {
                (Expr::Column(col), rhs) => {
                    if let Some(val) = try_eval_constant(rhs) {
                        out.push((
                            col.relation.as_ref().map(|r| r.table().to_string()),
                            col.name().to_string(),
                            val,
                        ));
                    }
                }
                (lhs, Expr::Column(col)) => {
                    if let Some(val) = try_eval_constant(lhs) {
                        out.push((
                            col.relation.as_ref().map(|r| r.table().to_string()),
                            col.name().to_string(),
                            val,
                        ));
                    }
                }
                _ => {}
            }
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            extract_qualified_equality_conditions(&binary.left, out);
            extract_qualified_equality_conditions(&binary.right, out);
        }
        _ => {}
    }
}

/// Try to evaluate an expression as a constant value (literal or simple arithmetic).
fn try_eval_constant(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(val, _) => Some(val.to_string()),
        Expr::BinaryExpr(binary) => {
            let left = try_eval_constant_i64(&binary.left)?;
            let right = try_eval_constant_i64(&binary.right)?;
            let result = match binary.op {
                Operator::Plus => left.checked_add(right)?,
                Operator::Minus => left.checked_sub(right)?,
                Operator::Multiply => left.checked_mul(right)?,
                _ => return None,
            };
            Some(result.to_string())
        }
        _ => None,
    }
}

fn try_eval_constant_i64(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(val, _) => {
            use datafusion_common::ScalarValue;
            match val {
                ScalarValue::Int8(Some(v)) => Some(*v as i64),
                ScalarValue::Int16(Some(v)) => Some(*v as i64),
                ScalarValue::Int32(Some(v)) => Some(*v as i64),
                ScalarValue::Int64(Some(v)) => Some(*v),
                ScalarValue::UInt8(Some(v)) => Some(*v as i64),
                ScalarValue::UInt16(Some(v)) => Some(*v as i64),
                ScalarValue::UInt32(Some(v)) => Some(*v as i64),
                _ => None,
            }
        }
        _ => None,
    }
}

fn is_cheap_to_inline(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::EmptyRelation(_) => true,
        _ => {
            let inputs = plan.inputs();
            inputs.len() == 1 && is_cheap_to_inline(inputs[0])
        }
    }
}

fn count_base_table_references(plan: &LogicalPlan) -> usize {
    let mut count = 0;
    plan.apply(|node| {
        if let LogicalPlan::TableScan(_) = node {
            count += 1;
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    count
}

fn contains_limit_on_single_child_path(plan: &LogicalPlan) -> bool {
    if matches!(plan, LogicalPlan::Limit(_)) {
        return true;
    }

    let inputs = plan.inputs();
    inputs.len() == 1 && contains_limit_on_single_child_path(inputs[0])
}

/// Check if a plan contains a SubqueryAlias reference to a given CTE name.
fn plan_references_cte(plan: &LogicalPlan, cte_name: &str) -> bool {
    let mut found = false;
    plan.apply(|node| {
        if let LogicalPlan::SubqueryAlias(alias) = node
            && alias.alias.table() == cte_name
        {
            found = true;
            return Ok(TreeNodeRecursion::Jump);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    found
}

/// Count how many times a CTE (by SubqueryAlias name) is referenced in the plan tree.
fn count_cte_references(plan: &LogicalPlan, cte_name: &str) -> usize {
    let mut count = 0;
    plan.apply(|node| {
        if let LogicalPlan::SubqueryAlias(alias) = node
            && alias.alias.table() == cte_name
        {
            count += 1;
            return Ok(TreeNodeRecursion::Jump);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    count
}

/// Replace SubqueryAlias nodes matching a CTE name with a MaterializedCteReader.
fn replace_cte_with_reader(
    plan: LogicalPlan,
    cte_name: &str,
    cte_schema: &DFSchemaRef,
) -> Result<LogicalPlan> {
    plan.transform_down(|node| {
        if let LogicalPlan::SubqueryAlias(ref alias) = node
            && alias.alias.table() == cte_name
        {
            let reader = MaterializedCteReader {
                name: cte_name.to_string(),
                schema: Arc::clone(cte_schema),
            };
            let extension = LogicalPlan::Extension(Extension {
                node: Arc::new(reader),
            });
            return Ok(datafusion_common::tree_node::Transformed::yes(extension));
        }
        Ok(datafusion_common::tree_node::Transformed::no(node))
    })
    .map(|t| t.data)
}

/// Returns the order by expressions from the query.
fn to_order_by_exprs(order_by: Option<OrderBy>) -> Result<Vec<OrderByExpr>> {
    to_order_by_exprs_with_select(order_by, None)
}

/// Returns the order by expressions from the query with the select expressions.
pub(crate) fn to_order_by_exprs_with_select(
    order_by: Option<OrderBy>,
    select_exprs: Option<&Vec<Expr>>,
) -> Result<Vec<OrderByExpr>> {
    let Some(OrderBy { kind, interpolate }) = order_by else {
        // If no order by, return an empty array.
        return Ok(vec![]);
    };
    if let Some(_interpolate) = interpolate {
        return not_impl_err!("ORDER BY INTERPOLATE is not supported");
    }
    match kind {
        OrderByKind::All(order_by_options) => {
            let Some(exprs) = select_exprs else {
                return Ok(vec![]);
            };
            let order_by_exprs = exprs
                .iter()
                .map(|select_expr| match select_expr {
                    Expr::Column(column) => Ok(OrderByExpr {
                        expr: SQLExpr::Identifier(Ident {
                            value: column.name.clone(),
                            quote_style: None,
                            span: Span::empty(),
                        }),
                        options: order_by_options,
                        with_fill: None,
                    }),
                    // TODO: Support other types of expressions
                    _ => not_impl_err!(
                        "ORDER BY ALL is not supported for non-column expressions"
                    ),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(order_by_exprs)
        }
        OrderByKind::Expressions(order_by_exprs) => Ok(order_by_exprs),
    }
}
