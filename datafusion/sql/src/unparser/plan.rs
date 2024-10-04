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

use crate::unparser::utils::unproject_agg_exprs;
use datafusion_common::{
    internal_err, not_impl_err, Column, DataFusionError, Result, TableReference,
};
use datafusion_expr::{
    expr::Alias, Distinct, Expr, JoinConstraint, JoinType, LogicalPlan,
    LogicalPlanBuilder, Projection, SortExpr,
};
use sqlparser::ast::{self, Ident, SetExpr};
use std::sync::Arc;

use super::{
    ast::{
        BuilderError, DerivedRelationBuilder, QueryBuilder, RelationBuilder,
        SelectBuilder, TableRelationBuilder, TableWithJoinsBuilder,
    },
    rewrite::{
        inject_column_aliases_into_subquery, normalize_union_schema,
        rewrite_plan_for_sort_on_non_projected_fields,
        subquery_alias_inner_query_and_columns,
    },
    utils::{
        find_agg_node_within_select, find_window_nodes_within_select,
        unproject_window_exprs,
    },
    Unparser,
};

/// Convert a DataFusion [`LogicalPlan`] to [`ast::Statement`]
///
/// This function is the opposite of [`SqlToRel::sql_statement_to_plan`] and can
/// be used to, among other things, to convert `LogicalPlan`s to SQL strings.
///
/// # Errors
///
/// This function returns an error if the plan cannot be converted to SQL.
///
/// # See Also
///
/// * [`expr_to_sql`] for converting [`Expr`], a single expression to SQL
///
/// # Example
/// ```
/// use arrow::datatypes::{DataType, Field, Schema};
/// use datafusion_expr::{col, logical_plan::table_scan};
/// use datafusion_sql::unparser::plan_to_sql;
/// let schema = Schema::new(vec![
///     Field::new("id", DataType::Utf8, false),
///     Field::new("value", DataType::Utf8, false),
/// ]);
/// // Scan 'table' and select columns 'id' and 'value'
/// let plan = table_scan(Some("table"), &schema, None)
///     .unwrap()
///     .project(vec![col("id"), col("value")])
///     .unwrap()
///     .build()
///     .unwrap();
/// let sql = plan_to_sql(&plan).unwrap(); // convert to AST
/// // use the Display impl to convert to SQL text
/// assert_eq!(sql.to_string(), "SELECT \"table\".id, \"table\".\"value\" FROM \"table\"")
/// ```
///
/// [`SqlToRel::sql_statement_to_plan`]: crate::planner::SqlToRel::sql_statement_to_plan
/// [`expr_to_sql`]: crate::unparser::expr_to_sql
pub fn plan_to_sql(plan: &LogicalPlan) -> Result<ast::Statement> {
    let unparser = Unparser::default();
    unparser.plan_to_sql(plan)
}

impl Unparser<'_> {
    pub fn plan_to_sql(&self, plan: &LogicalPlan) -> Result<ast::Statement> {
        let plan = normalize_union_schema(plan)?;

        match plan {
            LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Window(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Join(_)
            | LogicalPlan::CrossJoin(_)
            | LogicalPlan::Repartition(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::TableScan(_)
            | LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Subquery(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Statement(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::Distinct(_) => self.select_to_sql_statement(&plan),
            LogicalPlan::Dml(_) => self.dml_to_sql(&plan),
            LogicalPlan::Explain(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Extension(_)
            | LogicalPlan::Prepare(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::DescribeTable(_)
            | LogicalPlan::RecursiveQuery(_)
            | LogicalPlan::Unnest(_) => not_impl_err!("Unsupported plan: {plan:?}"),
        }
    }

    fn select_to_sql_statement(&self, plan: &LogicalPlan) -> Result<ast::Statement> {
        let mut query_builder = Some(QueryBuilder::default());

        let body = self.select_to_sql_expr(plan, &mut query_builder)?;

        let query = query_builder.unwrap().body(Box::new(body)).build()?;

        Ok(ast::Statement::Query(Box::new(query)))
    }

    fn select_to_sql_expr(
        &self,
        plan: &LogicalPlan,
        query: &mut Option<QueryBuilder>,
    ) -> Result<SetExpr> {
        let mut select_builder = SelectBuilder::default();
        select_builder.push_from(TableWithJoinsBuilder::default());
        let mut relation_builder = RelationBuilder::default();
        self.select_to_sql_recursively(
            plan,
            query,
            &mut select_builder,
            &mut relation_builder,
        )?;

        // If we were able to construct a full body (i.e. UNION ALL), return it
        if let Some(body) = query.as_mut().and_then(|q| q.take_body()) {
            return Ok(*body);
        }

        // If no projection is set, add a wildcard projection to the select
        // which will be translated to `SELECT *` in the SQL statement
        if !select_builder.already_projected() {
            select_builder.projection(vec![ast::SelectItem::Wildcard(
                ast::WildcardAdditionalOptions::default(),
            )]);
        }

        let mut twj = select_builder.pop_from().unwrap();
        twj.relation(relation_builder);
        select_builder.push_from(twj);

        Ok(SetExpr::Select(Box::new(select_builder.build()?)))
    }

    /// Reconstructs a SELECT SQL statement from a logical plan by unprojecting column expressions
    /// found in a [Projection] node. This requires scanning the plan tree for relevant Aggregate
    /// and Window nodes and matching column expressions to the appropriate agg or window expressions.
    fn reconstruct_select_statement(
        &self,
        plan: &LogicalPlan,
        p: &Projection,
        select: &mut SelectBuilder,
    ) -> Result<()> {
        match (
            find_agg_node_within_select(plan, true),
            find_window_nodes_within_select(plan, None, true),
        ) {
            (Some(agg), window) => {
                let window_option = window.as_deref();
                let items = p
                    .expr
                    .iter()
                    .map(|proj_expr| {
                        let unproj = unproject_agg_exprs(proj_expr, agg, window_option)?;
                        self.select_item_to_sql(&unproj)
                    })
                    .collect::<Result<Vec<_>>>()?;

                select.projection(items);
                select.group_by(ast::GroupByExpr::Expressions(
                    agg.group_expr
                        .iter()
                        .map(|expr| self.expr_to_sql(expr))
                        .collect::<Result<Vec<_>>>()?,
                    vec![],
                ));
            }
            (None, Some(window)) => {
                let items = p
                    .expr
                    .iter()
                    .map(|proj_expr| {
                        let unproj = unproject_window_exprs(proj_expr, &window)?;
                        self.select_item_to_sql(&unproj)
                    })
                    .collect::<Result<Vec<_>>>()?;

                select.projection(items);
            }
            _ => {
                let items = p
                    .expr
                    .iter()
                    .map(|e| self.select_item_to_sql(e))
                    .collect::<Result<Vec<_>>>()?;
                select.projection(items);
            }
        }
        Ok(())
    }

    fn derive(&self, plan: &LogicalPlan, relation: &mut RelationBuilder) -> Result<()> {
        let mut derived_builder = DerivedRelationBuilder::default();
        derived_builder.lateral(false).alias(None).subquery({
            let inner_statement = self.plan_to_sql(plan)?;
            if let ast::Statement::Query(inner_query) = inner_statement {
                inner_query
            } else {
                return internal_err!(
                    "Subquery must be a Query, but found {inner_statement:?}"
                );
            }
        });
        relation.derived(derived_builder);

        Ok(())
    }

    fn select_to_sql_recursively(
        &self,
        plan: &LogicalPlan,
        query: &mut Option<QueryBuilder>,
        select: &mut SelectBuilder,
        relation: &mut RelationBuilder,
    ) -> Result<()> {
        match plan {
            LogicalPlan::TableScan(scan) => {
                if scan.projection.is_some()
                    || !scan.filters.is_empty()
                    || scan.fetch.is_some()
                {
                    let unparsed_table_scan =
                        Self::unparse_table_scan_pushdown(plan, None)?;
                    return self.select_to_sql_recursively(
                        &unparsed_table_scan,
                        query,
                        select,
                        relation,
                    );
                }
                let mut builder = TableRelationBuilder::default();
                let mut table_parts = vec![];
                if let Some(catalog_name) = scan.table_name.catalog() {
                    table_parts
                        .push(self.new_ident_quoted_if_needs(catalog_name.to_string()));
                }
                if let Some(schema_name) = scan.table_name.schema() {
                    table_parts
                        .push(self.new_ident_quoted_if_needs(schema_name.to_string()));
                }
                table_parts.push(
                    self.new_ident_quoted_if_needs(scan.table_name.table().to_string()),
                );
                builder.name(ast::ObjectName(table_parts));
                relation.table(builder);

                Ok(())
            }
            LogicalPlan::Projection(p) => {
                if let Some(new_plan) = rewrite_plan_for_sort_on_non_projected_fields(p) {
                    return self
                        .select_to_sql_recursively(&new_plan, query, select, relation);
                }

                // Projection can be top-level plan for derived table
                if select.already_projected() {
                    return self.derive(plan, relation);
                }
                self.reconstruct_select_statement(plan, p, select)?;
                self.select_to_sql_recursively(p.input.as_ref(), query, select, relation)
            }
            LogicalPlan::Filter(filter) => {
                if let Some(agg) =
                    find_agg_node_within_select(plan, select.already_projected())
                {
                    let unprojected = unproject_agg_exprs(&filter.predicate, agg, None)?;
                    let filter_expr = self.expr_to_sql(&unprojected)?;
                    select.having(Some(filter_expr));
                } else {
                    let filter_expr = self.expr_to_sql(&filter.predicate)?;
                    select.selection(Some(filter_expr));
                }

                self.select_to_sql_recursively(
                    filter.input.as_ref(),
                    query,
                    select,
                    relation,
                )
            }
            LogicalPlan::Limit(limit) => {
                // Limit can be top-level plan for derived table
                if select.already_projected() {
                    return self.derive(plan, relation);
                }
                if let Some(fetch) = limit.fetch {
                    let Some(query) = query.as_mut() else {
                        return internal_err!(
                            "Limit operator only valid in a statement context."
                        );
                    };
                    query.limit(Some(ast::Expr::Value(ast::Value::Number(
                        fetch.to_string(),
                        false,
                    ))));
                }

                self.select_to_sql_recursively(
                    limit.input.as_ref(),
                    query,
                    select,
                    relation,
                )
            }
            LogicalPlan::Sort(sort) => {
                // Sort can be top-level plan for derived table
                if select.already_projected() {
                    return self.derive(plan, relation);
                }
                let Some(query_ref) = query else {
                    return internal_err!(
                        "Sort operator only valid in a statement context."
                    );
                };

                let sort_exprs: &Vec<SortExpr> =
                    // In case of aggregation there could be columns containing aggregation functions we need to unproject
                    match find_agg_node_within_select(plan, select.already_projected()) {
                        Some(agg) => &sort
                            .expr
                            .iter()
                            .map(|sort_expr| {
                                let mut sort_expr = sort_expr.clone();

                                // ORDER BY can't have aliases, this indicates that the column was not properly unparsed, update it
                                if let Expr::Alias(alias) = &sort_expr.expr {
                                    sort_expr.expr = *alias.expr.clone();
                                }

                                // Unproject the sort expression if it is a column from the aggregation
                                if let Expr::Column(c) = &sort_expr.expr {
                                    if c.relation.is_none() && agg.schema.is_column_from_schema(&c) {
                                        sort_expr.expr = unproject_agg_exprs(
                                            &sort_expr.expr,
                                            agg,
                                            None,
                                        )?;
                                    }
                                }

                                Ok::<_, DataFusionError>(sort_expr)
                            })
                            .collect::<Result<Vec<_>>>()?,
                        None => &sort.expr,
                    };

                query_ref.order_by(self.sorts_to_sql(sort_exprs)?);

                self.select_to_sql_recursively(
                    sort.input.as_ref(),
                    query,
                    select,
                    relation,
                )
            }
            LogicalPlan::Aggregate(agg) => {
                // Aggregate nodes are handled simultaneously with Projection nodes
                self.select_to_sql_recursively(
                    agg.input.as_ref(),
                    query,
                    select,
                    relation,
                )
            }
            LogicalPlan::Distinct(distinct) => {
                // Distinct can be top-level plan for derived table
                if select.already_projected() {
                    return self.derive(plan, relation);
                }
                let (select_distinct, input) = match distinct {
                    Distinct::All(input) => (ast::Distinct::Distinct, input.as_ref()),
                    Distinct::On(on) => {
                        let exprs = on
                            .on_expr
                            .iter()
                            .map(|e| self.expr_to_sql(e))
                            .collect::<Result<Vec<_>>>()?;
                        let items = on
                            .select_expr
                            .iter()
                            .map(|e| self.select_item_to_sql(e))
                            .collect::<Result<Vec<_>>>()?;
                        if let Some(sort_expr) = &on.sort_expr {
                            if let Some(query_ref) = query {
                                query_ref.order_by(self.sorts_to_sql(sort_expr)?);
                            } else {
                                return internal_err!(
                                    "Sort operator only valid in a statement context."
                                );
                            }
                        }
                        select.projection(items);
                        (ast::Distinct::On(exprs), on.input.as_ref())
                    }
                };
                select.distinct(Some(select_distinct));
                self.select_to_sql_recursively(input, query, select, relation)
            }
            LogicalPlan::Join(join) => {
                let join_constraint = self.join_constraint_to_sql(
                    join.join_constraint,
                    &join.on,
                    join.filter.as_ref(),
                )?;

                let mut right_relation = RelationBuilder::default();

                self.select_to_sql_recursively(
                    join.left.as_ref(),
                    query,
                    select,
                    relation,
                )?;
                self.select_to_sql_recursively(
                    join.right.as_ref(),
                    query,
                    select,
                    &mut right_relation,
                )?;

                let Ok(Some(relation)) = right_relation.build() else {
                    return internal_err!("Failed to build right relation");
                };

                let ast_join = ast::Join {
                    relation,
                    global: false,
                    join_operator: self
                        .join_operator_to_sql(join.join_type, join_constraint),
                };
                let mut from = select.pop_from().unwrap();
                from.push_join(ast_join);
                select.push_from(from);

                Ok(())
            }
            LogicalPlan::CrossJoin(cross_join) => {
                // Cross joins are the same as unconditional inner joins
                let mut right_relation = RelationBuilder::default();

                self.select_to_sql_recursively(
                    cross_join.left.as_ref(),
                    query,
                    select,
                    relation,
                )?;
                self.select_to_sql_recursively(
                    cross_join.right.as_ref(),
                    query,
                    select,
                    &mut right_relation,
                )?;

                let Ok(Some(relation)) = right_relation.build() else {
                    return internal_err!("Failed to build right relation");
                };

                let ast_join = ast::Join {
                    relation,
                    global: false,
                    join_operator: self.join_operator_to_sql(
                        JoinType::Inner,
                        ast::JoinConstraint::On(ast::Expr::Value(ast::Value::Boolean(
                            true,
                        ))),
                    ),
                };
                let mut from = select.pop_from().unwrap();
                from.push_join(ast_join);
                select.push_from(from);

                Ok(())
            }
            LogicalPlan::SubqueryAlias(plan_alias) => {
                let (plan, mut columns) =
                    subquery_alias_inner_query_and_columns(plan_alias);
                let plan = Self::unparse_table_scan_pushdown(
                    plan,
                    Some(plan_alias.alias.clone()),
                )?;
                if !columns.is_empty()
                    && !self.dialect.supports_column_alias_in_table_alias()
                {
                    // Instead of specifying column aliases as part of the outer table, inject them directly into the inner projection
                    let rewritten_plan =
                        match inject_column_aliases_into_subquery(plan, columns) {
                            Ok(p) => p,
                            Err(e) => {
                                return internal_err!(
                                    "Failed to transform SubqueryAlias plan: {e}"
                                )
                            }
                        };

                    columns = vec![];

                    self.select_to_sql_recursively(
                        &rewritten_plan,
                        query,
                        select,
                        relation,
                    )?;
                } else {
                    self.select_to_sql_recursively(&plan, query, select, relation)?;
                }

                relation.alias(Some(
                    self.new_table_alias(plan_alias.alias.table().to_string(), columns),
                ));

                Ok(())
            }
            LogicalPlan::Union(union) => {
                if union.inputs.len() != 2 {
                    return not_impl_err!(
                        "UNION ALL expected 2 inputs, but found {}",
                        union.inputs.len()
                    );
                }

                let input_exprs: Vec<SetExpr> = union
                    .inputs
                    .iter()
                    .map(|input| self.select_to_sql_expr(input, query))
                    .collect::<Result<Vec<_>>>()?;

                let union_expr = SetExpr::SetOperation {
                    op: ast::SetOperator::Union,
                    set_quantifier: ast::SetQuantifier::All,
                    left: Box::new(input_exprs[0].clone()),
                    right: Box::new(input_exprs[1].clone()),
                };

                let Some(query) = query.as_mut() else {
                    return internal_err!(
                        "UNION ALL operator only valid in a statement context"
                    );
                };
                query.body(Box::new(union_expr));

                Ok(())
            }
            LogicalPlan::Window(window) => {
                // Window nodes are handled simultaneously with Projection nodes
                self.select_to_sql_recursively(
                    window.input.as_ref(),
                    query,
                    select,
                    relation,
                )
            }
            LogicalPlan::EmptyRelation(_) => {
                relation.empty();
                Ok(())
            }
            LogicalPlan::Extension(_) => not_impl_err!("Unsupported operator: {plan:?}"),
            _ => not_impl_err!("Unsupported operator: {plan:?}"),
        }
    }

    fn unparse_table_scan_pushdown(
        plan: &LogicalPlan,
        alias: Option<TableReference>,
    ) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::TableScan(table_scan) => {
                // TODO: support filters for table scan with alias. Remove this check after #12368 issue.
                // see the issue: https://github.com/apache/datafusion/issues/12368
                if alias.is_some() && !table_scan.filters.is_empty() {
                    return not_impl_err!(
                        "Subquery alias is not supported for table scan with pushdown filters"
                    );
                }

                let mut builder = LogicalPlanBuilder::scan(
                    table_scan.table_name.clone(),
                    Arc::clone(&table_scan.source),
                    None,
                )?;
                if let Some(project_vec) = &table_scan.projection {
                    let project_columns = project_vec
                        .iter()
                        .cloned()
                        .map(|i| {
                            let (qualifier, field) =
                                table_scan.projected_schema.qualified_field(i);
                            if alias.is_some() {
                                Column::new(alias.clone(), field.name().clone())
                            } else {
                                Column::new(qualifier.cloned(), field.name().clone())
                            }
                        })
                        .collect::<Vec<_>>();
                    if let Some(alias) = alias {
                        builder = builder.alias(alias)?;
                    }
                    builder = builder.project(project_columns)?;
                }

                let filter_expr = table_scan
                    .filters
                    .iter()
                    .cloned()
                    .reduce(|acc, expr| acc.and(expr));
                if let Some(filter) = filter_expr {
                    builder = builder.filter(filter)?;
                }

                if let Some(fetch) = table_scan.fetch {
                    builder = builder.limit(0, Some(fetch))?;
                }

                builder.build()
            }
            LogicalPlan::SubqueryAlias(subquery_alias) => {
                let new_plan = Self::unparse_table_scan_pushdown(
                    &subquery_alias.input,
                    Some(subquery_alias.alias.clone()),
                )?;
                LogicalPlanBuilder::from(new_plan)
                    .alias(subquery_alias.alias.clone())?
                    .build()
            }
            _ => Ok(plan.clone()),
        }
    }

    fn select_item_to_sql(&self, expr: &Expr) -> Result<ast::SelectItem> {
        match expr {
            Expr::Alias(Alias { expr, name, .. }) => {
                let inner = self.expr_to_sql(expr)?;

                Ok(ast::SelectItem::ExprWithAlias {
                    expr: inner,
                    alias: self.new_ident_quoted_if_needs(name.to_string()),
                })
            }
            _ => {
                let inner = self.expr_to_sql(expr)?;

                Ok(ast::SelectItem::UnnamedExpr(inner))
            }
        }
    }

    fn sorts_to_sql(&self, sort_exprs: &Vec<SortExpr>) -> Result<Vec<ast::OrderByExpr>> {
        sort_exprs
            .iter()
            .map(|sort_expr| self.sort_to_sql(sort_expr))
            .collect::<Result<Vec<_>>>()
    }

    fn join_operator_to_sql(
        &self,
        join_type: JoinType,
        constraint: ast::JoinConstraint,
    ) -> ast::JoinOperator {
        match join_type {
            JoinType::Inner => ast::JoinOperator::Inner(constraint),
            JoinType::Left => ast::JoinOperator::LeftOuter(constraint),
            JoinType::Right => ast::JoinOperator::RightOuter(constraint),
            JoinType::Full => ast::JoinOperator::FullOuter(constraint),
            JoinType::LeftAnti => ast::JoinOperator::LeftAnti(constraint),
            JoinType::LeftSemi => ast::JoinOperator::LeftSemi(constraint),
            JoinType::RightAnti => ast::JoinOperator::RightAnti(constraint),
            JoinType::RightSemi => ast::JoinOperator::RightSemi(constraint),
        }
    }

    /// Convert the components of a USING clause to the USING AST. Returns
    /// 'None' if the conditions are not compatible with a USING expression,
    /// e.g. non-column expressions or non-matching names.
    fn join_using_to_sql(
        &self,
        join_conditions: &[(Expr, Expr)],
    ) -> Option<ast::JoinConstraint> {
        let mut idents = Vec::with_capacity(join_conditions.len());
        for (left, right) in join_conditions {
            match (left, right) {
                (
                    Expr::Column(Column {
                        relation: _,
                        name: left_name,
                    }),
                    Expr::Column(Column {
                        relation: _,
                        name: right_name,
                    }),
                ) if left_name == right_name => {
                    idents.push(self.new_ident_quoted_if_needs(left_name.to_string()));
                }
                // USING is only valid with matching column names; arbitrary expressions
                // are not allowed
                _ => return None,
            }
        }
        Some(ast::JoinConstraint::Using(idents))
    }

    /// Convert a join constraint and associated conditions and filter to a SQL AST node
    fn join_constraint_to_sql(
        &self,
        constraint: JoinConstraint,
        conditions: &[(Expr, Expr)],
        filter: Option<&Expr>,
    ) -> Result<ast::JoinConstraint> {
        match (constraint, conditions, filter) {
            // No constraints
            (JoinConstraint::On | JoinConstraint::Using, [], None) => {
                Ok(ast::JoinConstraint::None)
            }

            (JoinConstraint::Using, conditions, None) => {
                match self.join_using_to_sql(conditions) {
                    Some(using) => Ok(using),
                    // As above, this should not be reachable from parsed SQL,
                    // but a user could create this; we "downgrade" to ON.
                    None => self.join_conditions_to_sql_on(conditions, None),
                }
            }

            // Two cases here:
            // 1. Straightforward ON case, with possible equi-join conditions
            //    and additional filters
            // 2. USING with additional filters; we "downgrade" to ON, because
            //    you can't use USING with arbitrary filters. (This should not
            //    be accessible from parsed SQL, but may have been a
            //    custom-built JOIN by a user.)
            (JoinConstraint::On | JoinConstraint::Using, conditions, filter) => {
                self.join_conditions_to_sql_on(conditions, filter)
            }
        }
    }

    // Convert a list of equi0join conditions and an optional filter to a SQL ON
    // AST node, with the equi-join conditions and the filter merged into a
    // single conditional expression
    fn join_conditions_to_sql_on(
        &self,
        join_conditions: &[(Expr, Expr)],
        filter: Option<&Expr>,
    ) -> Result<ast::JoinConstraint> {
        let mut condition = None;
        // AND the join conditions together to create the overall condition
        for (left, right) in join_conditions {
            // Parse left and right
            let l = self.expr_to_sql(left)?;
            let r = self.expr_to_sql(right)?;
            let e = self.binary_op_to_sql(l, r, ast::BinaryOperator::Eq);
            condition = match condition {
                Some(expr) => Some(self.and_op_to_sql(expr, e)),
                None => Some(e),
            };
        }

        // Then AND the non-equijoin filter condition as well
        condition = match (condition, filter) {
            (Some(expr), Some(filter)) => {
                Some(self.and_op_to_sql(expr, self.expr_to_sql(filter)?))
            }
            (Some(expr), None) => Some(expr),
            (None, Some(filter)) => Some(self.expr_to_sql(filter)?),
            (None, None) => None,
        };

        let constraint = match condition {
            Some(filter) => ast::JoinConstraint::On(filter),
            None => ast::JoinConstraint::None,
        };

        Ok(constraint)
    }

    fn and_op_to_sql(&self, lhs: ast::Expr, rhs: ast::Expr) -> ast::Expr {
        self.binary_op_to_sql(lhs, rhs, ast::BinaryOperator::And)
    }

    fn new_table_alias(&self, alias: String, columns: Vec<Ident>) -> ast::TableAlias {
        ast::TableAlias {
            name: self.new_ident_quoted_if_needs(alias),
            columns,
        }
    }

    fn dml_to_sql(&self, plan: &LogicalPlan) -> Result<ast::Statement> {
        not_impl_err!("Unsupported plan: {plan:?}")
    }
}

impl From<BuilderError> for DataFusionError {
    fn from(e: BuilderError) -> Self {
        DataFusionError::External(Box::new(e))
    }
}
