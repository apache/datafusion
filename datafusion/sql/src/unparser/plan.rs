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

use super::{
    ast::{
        BuilderError, DerivedRelationBuilder, QueryBuilder, RelationBuilder,
        SelectBuilder, TableRelationBuilder, TableWithJoinsBuilder,
    },
    rewrite::{
        inject_column_aliases_into_subquery, normalize_union_schema,
        rewrite_plan_for_sort_on_non_projected_fields,
        subquery_alias_inner_query_and_columns, TableAliasRewriter,
    },
    utils::{
        find_agg_node_within_select, find_unnest_node_within_select,
        find_window_nodes_within_select, try_transform_to_simple_table_scan_with_filters,
        unproject_sort_expr, unproject_unnest_expr, unproject_window_exprs,
    },
    Unparser,
};
use crate::unparser::ast::UnnestRelationBuilder;
use crate::unparser::extension_unparser::{
    UnparseToStatementResult, UnparseWithinStatementResult,
};
use crate::unparser::utils::{find_unnest_node_until_relation, unproject_agg_exprs};
use crate::utils::UNNEST_PLACEHOLDER;
use datafusion_common::{
    internal_err, not_impl_err,
    tree_node::{TransformedResult, TreeNode},
    Column, DataFusionError, Result, ScalarValue, TableReference,
};
use datafusion_expr::expr::OUTER_REFERENCE_COLUMN_PREFIX;
use datafusion_expr::{
    expr::Alias, BinaryExpr, Distinct, Expr, JoinConstraint, JoinType, LogicalPlan,
    LogicalPlanBuilder, Operator, Projection, SortExpr, TableScan, Unnest,
    UserDefinedLogicalNode,
};
use sqlparser::ast::{self, Ident, SetExpr, TableAliasColumnDef};
use std::sync::Arc;

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
            LogicalPlan::Extension(extension) => {
                self.extension_to_statement(extension.node.as_ref())
            }
            LogicalPlan::Explain(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::DescribeTable(_)
            | LogicalPlan::RecursiveQuery(_)
            | LogicalPlan::Unnest(_) => not_impl_err!("Unsupported plan: {plan:?}"),
        }
    }

    /// Try to unparse a [UserDefinedLogicalNode] to a SQL statement.
    /// If multiple unparsers are registered for the same [UserDefinedLogicalNode],
    /// the first unparsing result will be returned.
    fn extension_to_statement(
        &self,
        node: &dyn UserDefinedLogicalNode,
    ) -> Result<ast::Statement> {
        let mut statement = None;
        for unparser in &self.extension_unparsers {
            match unparser.unparse_to_statement(node, self)? {
                UnparseToStatementResult::Modified(stmt) => {
                    statement = Some(stmt);
                    break;
                }
                UnparseToStatementResult::Unmodified => {}
            }
        }
        if let Some(statement) = statement {
            Ok(statement)
        } else {
            not_impl_err!("Unsupported extension node: {node:?}")
        }
    }

    /// Try to unparse a [UserDefinedLogicalNode] to a SQL statement.
    /// If multiple unparsers are registered for the same [UserDefinedLogicalNode],
    /// the first unparser supporting the node will be used.
    fn extension_to_sql(
        &self,
        node: &dyn UserDefinedLogicalNode,
        query: &mut Option<&mut QueryBuilder>,
        select: &mut Option<&mut SelectBuilder>,
        relation: &mut Option<&mut RelationBuilder>,
    ) -> Result<()> {
        for unparser in &self.extension_unparsers {
            match unparser.unparse(node, self, query, select, relation)? {
                UnparseWithinStatementResult::Modified => return Ok(()),
                UnparseWithinStatementResult::Unmodified => {}
            }
        }
        not_impl_err!("Unsupported extension node: {node:?}")
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
        let mut exprs = p.expr.clone();

        // If an Unnest node is found within the select, find and unproject the unnest column
        if let Some(unnest) = find_unnest_node_within_select(plan) {
            exprs = exprs
                .into_iter()
                .map(|e| unproject_unnest_expr(e, unnest))
                .collect::<Result<Vec<_>>>()?;
        };

        match (
            find_agg_node_within_select(plan, true),
            find_window_nodes_within_select(plan, None, true),
        ) {
            (Some(agg), window) => {
                let window_option = window.as_deref();
                let items = exprs
                    .into_iter()
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
                let items = exprs
                    .into_iter()
                    .map(|proj_expr| {
                        let unproj = unproject_window_exprs(proj_expr, &window)?;
                        self.select_item_to_sql(&unproj)
                    })
                    .collect::<Result<Vec<_>>>()?;

                select.projection(items);
            }
            _ => {
                let items = exprs
                    .iter()
                    .map(|e| self.select_item_to_sql(e))
                    .collect::<Result<Vec<_>>>()?;
                select.projection(items);
            }
        }
        Ok(())
    }

    fn derive(
        &self,
        plan: &LogicalPlan,
        relation: &mut RelationBuilder,
        alias: Option<ast::TableAlias>,
        lateral: bool,
    ) -> Result<()> {
        let mut derived_builder = DerivedRelationBuilder::default();
        derived_builder.lateral(lateral).alias(alias).subquery({
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

    fn derive_with_dialect_alias(
        &self,
        alias: &str,
        plan: &LogicalPlan,
        relation: &mut RelationBuilder,
        lateral: bool,
    ) -> Result<()> {
        if self.dialect.requires_derived_table_alias() {
            self.derive(
                plan,
                relation,
                Some(self.new_table_alias(alias.to_string(), vec![])),
                lateral,
            )
        } else {
            self.derive(plan, relation, None, lateral)
        }
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
                if let Some(unparsed_table_scan) = Self::unparse_table_scan_pushdown(
                    plan,
                    None,
                    select.already_projected(),
                )? {
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

                // Projection can be top-level plan for unnest relation
                // The projection generated by the `RecursiveUnnestRewriter` from a UNNEST relation will have
                // only one expression, which is the placeholder column generated by the rewriter.
                let unnest_input_type = if p.expr.len() == 1 {
                    Self::check_unnest_placeholder_with_outer_ref(&p.expr[0])
                } else {
                    None
                };
                if self.dialect.unnest_as_table_factor() && unnest_input_type.is_some() {
                    if let LogicalPlan::Unnest(unnest) = &p.input.as_ref() {
                        return self
                            .unnest_to_table_factor_sql(unnest, query, select, relation);
                    }
                }

                // Projection can be top-level plan for derived table
                if select.already_projected() {
                    return self.derive_with_dialect_alias(
                        "derived_projection",
                        plan,
                        relation,
                        unnest_input_type
                            .filter(|t| matches!(t, UnnestInputType::OuterReference))
                            .is_some(),
                    );
                }
                self.reconstruct_select_statement(plan, p, select)?;
                self.select_to_sql_recursively(p.input.as_ref(), query, select, relation)
            }
            LogicalPlan::Filter(filter) => {
                if let Some(agg) =
                    find_agg_node_within_select(plan, select.already_projected())
                {
                    let unprojected =
                        unproject_agg_exprs(filter.predicate.clone(), agg, None)?;
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
                    return self.derive_with_dialect_alias(
                        "derived_limit",
                        plan,
                        relation,
                        false,
                    );
                }
                if let Some(fetch) = &limit.fetch {
                    let Some(query) = query.as_mut() else {
                        return internal_err!(
                            "Limit operator only valid in a statement context."
                        );
                    };
                    query.limit(Some(self.expr_to_sql(fetch)?));
                }

                if let Some(skip) = &limit.skip {
                    let Some(query) = query.as_mut() else {
                        return internal_err!(
                            "Offset operator only valid in a statement context."
                        );
                    };
                    query.offset(Some(ast::Offset {
                        rows: ast::OffsetRows::None,
                        value: self.expr_to_sql(skip)?,
                    }));
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
                    return self.derive_with_dialect_alias(
                        "derived_sort",
                        plan,
                        relation,
                        false,
                    );
                }
                let Some(query_ref) = query else {
                    return internal_err!(
                        "Sort operator only valid in a statement context."
                    );
                };

                if let Some(fetch) = sort.fetch {
                    query_ref.limit(Some(ast::Expr::Value(ast::Value::Number(
                        fetch.to_string(),
                        false,
                    ))));
                };

                let agg = find_agg_node_within_select(plan, select.already_projected());
                // unproject sort expressions
                let sort_exprs: Vec<SortExpr> = sort
                    .expr
                    .iter()
                    .map(|sort_expr| {
                        unproject_sort_expr(sort_expr, agg, sort.input.as_ref())
                    })
                    .collect::<Result<Vec<_>>>()?;

                query_ref.order_by(self.sorts_to_sql(&sort_exprs)?);

                self.select_to_sql_recursively(
                    sort.input.as_ref(),
                    query,
                    select,
                    relation,
                )
            }
            LogicalPlan::Aggregate(agg) => {
                // Aggregation can be already handled in the projection case
                if !select.already_projected() {
                    // The query returns aggregate and group expressions. If that weren't the case,
                    // the aggregate would have been placed inside a projection, making the check above^ false
                    let exprs: Vec<_> = agg
                        .aggr_expr
                        .iter()
                        .chain(agg.group_expr.iter())
                        .map(|expr| self.select_item_to_sql(expr))
                        .collect::<Result<Vec<_>>>()?;
                    select.projection(exprs);

                    select.group_by(ast::GroupByExpr::Expressions(
                        agg.group_expr
                            .iter()
                            .map(|expr| self.expr_to_sql(expr))
                            .collect::<Result<Vec<_>>>()?,
                        vec![],
                    ));
                }

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
                    return self.derive_with_dialect_alias(
                        "derived_distinct",
                        plan,
                        relation,
                        false,
                    );
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
                let mut table_scan_filters = vec![];

                let left_plan =
                    match try_transform_to_simple_table_scan_with_filters(&join.left)? {
                        Some((plan, filters)) => {
                            table_scan_filters.extend(filters);
                            Arc::new(plan)
                        }
                        None => Arc::clone(&join.left),
                    };

                self.select_to_sql_recursively(
                    left_plan.as_ref(),
                    query,
                    select,
                    relation,
                )?;

                let right_plan =
                    match try_transform_to_simple_table_scan_with_filters(&join.right)? {
                        Some((plan, filters)) => {
                            table_scan_filters.extend(filters);
                            Arc::new(plan)
                        }
                        None => Arc::clone(&join.right),
                    };

                let mut right_relation = RelationBuilder::default();

                self.select_to_sql_recursively(
                    right_plan.as_ref(),
                    query,
                    select,
                    &mut right_relation,
                )?;

                let join_filters = if table_scan_filters.is_empty() {
                    join.filter.clone()
                } else {
                    // Combine `table_scan_filters` into a single filter using `AND`
                    let Some(combined_filters) =
                        table_scan_filters.into_iter().reduce(|acc, filter| {
                            Expr::BinaryExpr(BinaryExpr {
                                left: Box::new(acc),
                                op: Operator::And,
                                right: Box::new(filter),
                            })
                        })
                    else {
                        return internal_err!("Failed to combine TableScan filters");
                    };

                    // Combine `join.filter` with `combined_filters` using `AND`
                    match &join.filter {
                        Some(filter) => Some(Expr::BinaryExpr(BinaryExpr {
                            left: Box::new(filter.clone()),
                            op: Operator::And,
                            right: Box::new(combined_filters),
                        })),
                        None => Some(combined_filters),
                    }
                };

                let join_constraint = self.join_constraint_to_sql(
                    join.join_constraint,
                    &join.on,
                    join_filters.as_ref(),
                )?;

                self.select_to_sql_recursively(
                    right_plan.as_ref(),
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
                        .join_operator_to_sql(join.join_type, join_constraint)?,
                };
                let mut from = select.pop_from().unwrap();
                from.push_join(ast_join);
                select.push_from(from);

                Ok(())
            }
            LogicalPlan::SubqueryAlias(plan_alias) => {
                let (plan, mut columns) =
                    subquery_alias_inner_query_and_columns(plan_alias);
                let unparsed_table_scan = Self::unparse_table_scan_pushdown(
                    plan,
                    Some(plan_alias.alias.clone()),
                    select.already_projected(),
                )?;
                // if the child plan is a TableScan with pushdown operations, we don't need to
                // create an additional subquery for it
                if !select.already_projected() && unparsed_table_scan.is_none() {
                    select.projection(vec![ast::SelectItem::Wildcard(
                        ast::WildcardAdditionalOptions::default(),
                    )]);
                }
                let plan = unparsed_table_scan.unwrap_or_else(|| plan.clone());
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
                // Covers cases where the UNION is a subquery and the projection is at the top level
                if select.already_projected() {
                    return self.derive_with_dialect_alias(
                        "derived_union",
                        plan,
                        relation,
                        false,
                    );
                }

                let input_exprs: Vec<SetExpr> = union
                    .inputs
                    .iter()
                    .map(|input| self.select_to_sql_expr(input, query))
                    .collect::<Result<Vec<_>>>()?;

                if input_exprs.len() < 2 {
                    return internal_err!("UNION operator requires at least 2 inputs");
                }

                // Build the union expression tree bottom-up by reversing the order
                // note that we are also swapping left and right inputs because of the rev
                let union_expr = input_exprs
                    .into_iter()
                    .rev()
                    .reduce(|a, b| SetExpr::SetOperation {
                        op: ast::SetOperator::Union,
                        set_quantifier: ast::SetQuantifier::All,
                        left: Box::new(b),
                        right: Box::new(a),
                    })
                    .unwrap();

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
                // An EmptyRelation could be behind an UNNEST node. If the dialect supports UNNEST as a table factor,
                // a TableRelationBuilder will be created for the UNNEST node first.
                if !relation.has_relation() {
                    relation.empty();
                }
                Ok(())
            }
            LogicalPlan::Extension(extension) => {
                if let Some(query) = query.as_mut() {
                    self.extension_to_sql(
                        extension.node.as_ref(),
                        &mut Some(query),
                        &mut Some(select),
                        &mut Some(relation),
                    )
                } else {
                    self.extension_to_sql(
                        extension.node.as_ref(),
                        &mut None,
                        &mut Some(select),
                        &mut Some(relation),
                    )
                }
            }
            LogicalPlan::Unnest(unnest) => {
                if !unnest.struct_type_columns.is_empty() {
                    return internal_err!(
                        "Struct type columns are not currently supported in UNNEST: {:?}",
                        unnest.struct_type_columns
                    );
                }

                // In the case of UNNEST, the Unnest node is followed by a duplicate Projection node that we should skip.
                // Otherwise, there will be a duplicate SELECT clause.
                // | Projection: table.col1, UNNEST(table.col2)
                // |   Unnest: UNNEST(table.col2)
                // |     Projection: table.col1, table.col2 AS UNNEST(table.col2)
                // |       Filter: table.col3 = Int64(3)
                // |         TableScan: table projection=None
                if let LogicalPlan::Projection(p) = unnest.input.as_ref() {
                    // continue with projection input
                    self.select_to_sql_recursively(&p.input, query, select, relation)
                } else {
                    internal_err!("Unnest input is not a Projection: {unnest:?}")
                }
            }
            LogicalPlan::Subquery(subquery)
                if find_unnest_node_until_relation(subquery.subquery.as_ref())
                    .is_some() =>
            {
                if self.dialect.unnest_as_table_factor() {
                    self.select_to_sql_recursively(
                        subquery.subquery.as_ref(),
                        query,
                        select,
                        relation,
                    )
                } else {
                    self.derive_with_dialect_alias(
                        "derived_unnest",
                        subquery.subquery.as_ref(),
                        relation,
                        true,
                    )
                }
            }
            _ => {
                not_impl_err!("Unsupported operator: {plan:?}")
            }
        }
    }

    /// Try to find the placeholder column name generated by `RecursiveUnnestRewriter`.
    ///
    /// - If the column is a placeholder column match the pattern `Expr::Alias(Expr::Column("__unnest_placeholder(...)"))`,
    ///     it means it is a scalar column, return [UnnestInputType::Scalar].
    /// - If the column is a placeholder column match the pattern `Expr::Alias(Expr::Column("__unnest_placeholder(outer_ref(...)))")`,
    ///     it means it is an outer reference column, return [UnnestInputType::OuterReference].
    /// - If the column is not a placeholder column, return [None].
    ///
    /// `outer_ref` is the display result of [Expr::OuterReferenceColumn]
    fn check_unnest_placeholder_with_outer_ref(expr: &Expr) -> Option<UnnestInputType> {
        if let Expr::Alias(Alias { expr, .. }) = expr {
            if let Expr::Column(Column { name, .. }) = expr.as_ref() {
                if let Some(prefix) = name.strip_prefix(UNNEST_PLACEHOLDER) {
                    if prefix.starts_with(&format!("({}(", OUTER_REFERENCE_COLUMN_PREFIX))
                    {
                        return Some(UnnestInputType::OuterReference);
                    }
                    return Some(UnnestInputType::Scalar);
                }
            }
        }
        None
    }

    fn unnest_to_table_factor_sql(
        &self,
        unnest: &Unnest,
        query: &mut Option<QueryBuilder>,
        select: &mut SelectBuilder,
        relation: &mut RelationBuilder,
    ) -> Result<()> {
        let mut unnest_relation = UnnestRelationBuilder::default();
        let LogicalPlan::Projection(p) = unnest.input.as_ref() else {
            return internal_err!("Unnest input is not a Projection: {unnest:?}");
        };
        let exprs = p
            .expr
            .iter()
            .map(|e| self.expr_to_sql(e))
            .collect::<Result<Vec<_>>>()?;
        unnest_relation.array_exprs(exprs);
        relation.unnest(unnest_relation);
        self.select_to_sql_recursively(p.input.as_ref(), query, select, relation)
    }

    fn is_scan_with_pushdown(scan: &TableScan) -> bool {
        scan.projection.is_some() || !scan.filters.is_empty() || scan.fetch.is_some()
    }

    /// Try to unparse a table scan with pushdown operations into a new subquery plan.
    /// If the table scan is without any pushdown operations, return None.
    fn unparse_table_scan_pushdown(
        plan: &LogicalPlan,
        alias: Option<TableReference>,
        already_projected: bool,
    ) -> Result<Option<LogicalPlan>> {
        match plan {
            LogicalPlan::TableScan(table_scan) => {
                if !Self::is_scan_with_pushdown(table_scan) {
                    return Ok(None);
                }
                let table_schema = table_scan.source.schema();
                let mut filter_alias_rewriter =
                    alias.as_ref().map(|alias_name| TableAliasRewriter {
                        table_schema: &table_schema,
                        alias_name: alias_name.clone(),
                    });

                let mut builder = LogicalPlanBuilder::scan(
                    table_scan.table_name.clone(),
                    Arc::clone(&table_scan.source),
                    None,
                )?;
                // We will rebase the column references to the new alias if it exists.
                // If the projection or filters are empty, we will append alias to the table scan.
                //
                // Example:
                //   select t1.c1 from t1 where t1.c1 > 1 -> select a.c1 from t1 as a where a.c1 > 1
                if let Some(ref alias) = alias {
                    if table_scan.projection.is_some() || !table_scan.filters.is_empty() {
                        builder = builder.alias(alias.clone())?;
                    }
                }

                // Avoid creating a duplicate Projection node, which would result in an additional subquery if a projection already exists.
                // For example, if the `optimize_projection` rule is applied, there will be a Projection node, and duplicate projection
                // information included in the TableScan node.
                if !already_projected {
                    if let Some(project_vec) = &table_scan.projection {
                        if project_vec.is_empty() {
                            builder = builder.project(vec![Expr::Literal(
                                ScalarValue::Int64(Some(1)),
                            )])?;
                        } else {
                            let project_columns = project_vec
                                .iter()
                                .cloned()
                                .map(|i| {
                                    let schema = table_scan.source.schema();
                                    let field = schema.field(i);
                                    if alias.is_some() {
                                        Column::new(alias.clone(), field.name().clone())
                                    } else {
                                        Column::new(
                                            Some(table_scan.table_name.clone()),
                                            field.name().clone(),
                                        )
                                    }
                                })
                                .collect::<Vec<_>>();
                            builder = builder.project(project_columns)?;
                        };
                    }
                }

                let filter_expr: Result<Option<Expr>> = table_scan
                    .filters
                    .iter()
                    .cloned()
                    .map(|expr| {
                        if let Some(ref mut rewriter) = filter_alias_rewriter {
                            expr.rewrite(rewriter).data()
                        } else {
                            Ok(expr)
                        }
                    })
                    .reduce(|acc, expr_result| {
                        acc.and_then(|acc_expr| {
                            expr_result.map(|expr| acc_expr.and(expr))
                        })
                    })
                    .transpose();

                if let Some(filter) = filter_expr? {
                    builder = builder.filter(filter)?;
                }

                if let Some(fetch) = table_scan.fetch {
                    builder = builder.limit(0, Some(fetch))?;
                }

                // If the table scan has an alias but no projection or filters, it means no column references are rebased.
                // So we will append the alias to this subquery.
                // Example:
                //   select * from t1 limit 10 -> (select * from t1 limit 10) as a
                if let Some(alias) = alias {
                    if table_scan.projection.is_none() && table_scan.filters.is_empty() {
                        builder = builder.alias(alias)?;
                    }
                }

                Ok(Some(builder.build()?))
            }
            LogicalPlan::SubqueryAlias(subquery_alias) => {
                Self::unparse_table_scan_pushdown(
                    &subquery_alias.input,
                    Some(subquery_alias.alias.clone()),
                    already_projected,
                )
            }
            // SubqueryAlias could be rewritten to a plan with a projection as the top node by [rewrite::subquery_alias_inner_query_and_columns].
            // The inner table scan could be a scan with pushdown operations.
            LogicalPlan::Projection(projection) => {
                if let Some(plan) = Self::unparse_table_scan_pushdown(
                    &projection.input,
                    alias.clone(),
                    already_projected,
                )? {
                    let exprs = if alias.is_some() {
                        let mut alias_rewriter =
                            alias.as_ref().map(|alias_name| TableAliasRewriter {
                                table_schema: plan.schema().as_arrow(),
                                alias_name: alias_name.clone(),
                            });
                        projection
                            .expr
                            .iter()
                            .cloned()
                            .map(|expr| {
                                if let Some(ref mut rewriter) = alias_rewriter {
                                    expr.rewrite(rewriter).data()
                                } else {
                                    Ok(expr)
                                }
                            })
                            .collect::<Result<Vec<_>>>()?
                    } else {
                        projection.expr.clone()
                    };
                    Ok(Some(
                        LogicalPlanBuilder::from(plan).project(exprs)?.build()?,
                    ))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
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

    fn sorts_to_sql(&self, sort_exprs: &[SortExpr]) -> Result<Vec<ast::OrderByExpr>> {
        sort_exprs
            .iter()
            .map(|sort_expr| self.sort_to_sql(sort_expr))
            .collect::<Result<Vec<_>>>()
    }

    fn join_operator_to_sql(
        &self,
        join_type: JoinType,
        constraint: ast::JoinConstraint,
    ) -> Result<ast::JoinOperator> {
        Ok(match join_type {
            JoinType::Inner => match &constraint {
                ast::JoinConstraint::On(_)
                | ast::JoinConstraint::Using(_)
                | ast::JoinConstraint::Natural => ast::JoinOperator::Inner(constraint),
                ast::JoinConstraint::None => {
                    // Inner joins with no conditions or filters are not valid SQL in most systems,
                    // return a CROSS JOIN instead
                    ast::JoinOperator::CrossJoin
                }
            },
            JoinType::Left => ast::JoinOperator::LeftOuter(constraint),
            JoinType::Right => ast::JoinOperator::RightOuter(constraint),
            JoinType::Full => ast::JoinOperator::FullOuter(constraint),
            JoinType::LeftAnti => ast::JoinOperator::LeftAnti(constraint),
            JoinType::LeftSemi => ast::JoinOperator::LeftSemi(constraint),
            JoinType::RightAnti => ast::JoinOperator::RightAnti(constraint),
            JoinType::RightSemi => ast::JoinOperator::RightSemi(constraint),
            JoinType::LeftMark => unimplemented!("Unparsing of Left Mark join type"),
        })
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
        let columns = columns
            .into_iter()
            .map(|ident| TableAliasColumnDef {
                name: ident,
                data_type: None,
            })
            .collect();
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

/// The type of the input to the UNNEST table factor.
#[derive(Debug)]
enum UnnestInputType {
    /// The input is a column reference. It will be presented like `outer_ref(column_name)`.
    OuterReference,
    /// The input is a scalar value. It will be presented like a scalar array or struct.
    Scalar,
}
