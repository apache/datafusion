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

use datafusion_common::{internal_err, not_impl_err, plan_err, DataFusionError, Result};
use datafusion_expr::{
    expr::Alias, Distinct, Expr, JoinConstraint, JoinType, LogicalPlan, Projection,
};
use sqlparser::ast::{self, SetExpr};

use crate::unparser::utils::unproject_agg_exprs;

use super::{
    ast::{
        BuilderError, DerivedRelationBuilder, QueryBuilder, RelationBuilder,
        SelectBuilder, TableRelationBuilder, TableWithJoinsBuilder,
    },
    utils::{find_agg_node_within_select, unproject_window_exprs, AggVariant},
    Unparser,
};

/// Convert a DataFusion [`LogicalPlan`] to `sqlparser::ast::Statement`
///
/// This function is the opposite of `SqlToRel::sql_statement_to_plan` and can
/// be used to, among other things, convert `LogicalPlan`s to strings.
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
/// let plan = table_scan(Some("table"), &schema, None)
///     .unwrap()
///     .project(vec![col("id"), col("value")])
///     .unwrap()
///     .build()
///     .unwrap();
/// let sql = plan_to_sql(&plan).unwrap();
///
/// assert_eq!(format!("{}", sql), "SELECT \"table\".id, \"table\".\"value\" FROM \"table\"")
/// ```
pub fn plan_to_sql(plan: &LogicalPlan) -> Result<ast::Statement> {
    let unparser = Unparser::default();
    unparser.plan_to_sql(plan)
}

impl Unparser<'_> {
    pub fn plan_to_sql(&self, plan: &LogicalPlan) -> Result<ast::Statement> {
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
            | LogicalPlan::Distinct(_) => self.select_to_sql_statement(plan),
            LogicalPlan::Dml(_) => self.dml_to_sql(plan),
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
    ) -> Result<ast::SetExpr> {
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

        let mut twj = select_builder.pop_from().unwrap();
        twj.relation(relation_builder);
        select_builder.push_from(twj);

        Ok(ast::SetExpr::Select(Box::new(select_builder.build()?)))
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
        match find_agg_node_within_select(plan, None, true) {
            Some(AggVariant::Aggregate(agg)) => {
                let items = p
                    .expr
                    .iter()
                    .map(|proj_expr| {
                        let unproj = unproject_agg_exprs(proj_expr, agg)?;
                        self.select_item_to_sql(&unproj)
                    })
                    .collect::<Result<Vec<_>>>()?;

                select.projection(items);
                select.group_by(ast::GroupByExpr::Expressions(
                    agg.group_expr
                        .iter()
                        .map(|expr| self.expr_to_sql(expr))
                        .collect::<Result<Vec<_>>>()?,
                ));
            }
            Some(AggVariant::Window(window)) => {
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
            None => {
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

    fn projection_to_sql(
        &self,
        plan: &LogicalPlan,
        p: &Projection,
        query: &mut Option<QueryBuilder>,
        select: &mut SelectBuilder,
        relation: &mut RelationBuilder,
    ) -> Result<()> {
        // A second projection implies a derived tablefactor
        if !select.already_projected() {
            self.reconstruct_select_statement(plan, p, select)?;
            self.select_to_sql_recursively(p.input.as_ref(), query, select, relation)
        } else {
            let mut derived_builder = DerivedRelationBuilder::default();
            derived_builder.lateral(false).alias(None).subquery({
                let inner_statment = self.plan_to_sql(plan)?;
                if let ast::Statement::Query(inner_query) = inner_statment {
                    inner_query
                } else {
                    return internal_err!(
                        "Subquery must be a Query, but found {inner_statment:?}"
                    );
                }
            });
            relation.derived(derived_builder);
            Ok(())
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
                self.projection_to_sql(plan, p, query, select, relation)
            }
            LogicalPlan::Filter(filter) => {
                if let Some(AggVariant::Aggregate(agg)) =
                    find_agg_node_within_select(plan, None, select.already_projected())
                {
                    let unprojected = unproject_agg_exprs(&filter.predicate, agg)?;
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
                if let Some(query_ref) = query {
                    query_ref.order_by(self.sort_to_sql(sort.expr.clone())?);
                } else {
                    return internal_err!(
                        "Sort operator only valid in a statement context."
                    );
                }

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
                        match &on.sort_expr {
                            Some(sort_expr) => {
                                if let Some(query_ref) = query {
                                    query_ref
                                        .order_by(self.sort_to_sql(sort_expr.clone())?);
                                } else {
                                    return internal_err!(
                                "Sort operator only valid in a statement context."
                            );
                                }
                            }
                            None => {}
                        }
                        select.projection(items);
                        (ast::Distinct::On(exprs), on.input.as_ref())
                    }
                };
                select.distinct(Some(select_distinct));
                self.select_to_sql_recursively(input, query, select, relation)
            }
            LogicalPlan::Join(join) => {
                match join.join_constraint {
                    JoinConstraint::On => {}
                    JoinConstraint::Using => {
                        return not_impl_err!(
                            "Unsupported join constraint: {:?}",
                            join.join_constraint
                        )
                    }
                }

                // parse filter if exists
                let join_filter = match &join.filter {
                    Some(filter) => Some(self.expr_to_sql(filter)?),
                    None => None,
                };

                // map join.on to `l.a = r.a AND l.b = r.b AND ...`
                let eq_op = ast::BinaryOperator::Eq;
                let join_on = self.join_conditions_to_sql(&join.on, eq_op)?;

                // Merge `join_on` and `join_filter`
                let join_expr = match (join_filter, join_on) {
                    (Some(filter), Some(on)) => Some(self.and_op_to_sql(filter, on)),
                    (Some(filter), None) => Some(filter),
                    (None, Some(on)) => Some(on),
                    (None, None) => None,
                };
                let join_constraint = match join_expr {
                    Some(expr) => ast::JoinConstraint::On(expr),
                    None => ast::JoinConstraint::None,
                };

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

                let ast_join = ast::Join {
                    relation: match right_relation.build()? {
                        Some(relation) => relation,
                        None => return internal_err!("Failed to build right relation"),
                    },
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

                let ast_join = ast::Join {
                    relation: match right_relation.build()? {
                        Some(relation) => relation,
                        None => return internal_err!("Failed to build right relation"),
                    },
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
                // Handle bottom-up to allocate relation
                self.select_to_sql_recursively(
                    plan_alias.input.as_ref(),
                    query,
                    select,
                    relation,
                )?;

                relation.alias(Some(
                    self.new_table_alias(plan_alias.alias.table().to_string()),
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
                    .map(|input| self.select_to_sql_expr(input, &mut None))
                    .collect::<Result<Vec<_>>>()?;

                let union_expr = ast::SetExpr::SetOperation {
                    op: ast::SetOperator::Union,
                    set_quantifier: ast::SetQuantifier::All,
                    left: Box::new(input_exprs[0].clone()),
                    right: Box::new(input_exprs[1].clone()),
                };

                query
                    .as_mut()
                    .expect("to have a query builder")
                    .body(Box::new(union_expr));

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

    fn sort_to_sql(&self, sort_exprs: Vec<Expr>) -> Result<Vec<ast::OrderByExpr>> {
        sort_exprs
            .iter()
            .map(|expr: &Expr| match expr {
                Expr::Sort(sort_expr) => {
                    let col = self.expr_to_sql(&sort_expr.expr)?;

                    let nulls_first = if self.dialect.supports_nulls_first_in_sort() {
                        Some(sort_expr.nulls_first)
                    } else {
                        None
                    };

                    Ok(ast::OrderByExpr {
                        asc: Some(sort_expr.asc),
                        expr: col,
                        nulls_first,
                    })
                }
                _ => plan_err!("Expecting Sort expr"),
            })
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

    fn join_conditions_to_sql(
        &self,
        join_conditions: &Vec<(Expr, Expr)>,
        eq_op: ast::BinaryOperator,
    ) -> Result<Option<ast::Expr>> {
        // Only support AND conjunction for each binary expression in join conditions
        let mut exprs: Vec<ast::Expr> = vec![];
        for (left, right) in join_conditions {
            // Parse left
            let l = self.expr_to_sql(left)?;
            // Parse right
            let r = self.expr_to_sql(right)?;
            // AND with existing expression
            exprs.push(self.binary_op_to_sql(l, r, eq_op.clone()));
        }
        let join_expr: Option<ast::Expr> =
            exprs.into_iter().reduce(|r, l| self.and_op_to_sql(r, l));
        Ok(join_expr)
    }

    fn and_op_to_sql(&self, lhs: ast::Expr, rhs: ast::Expr) -> ast::Expr {
        self.binary_op_to_sql(lhs, rhs, ast::BinaryOperator::And)
    }

    fn new_table_alias(&self, alias: String) -> ast::TableAlias {
        ast::TableAlias {
            name: self.new_ident_quoted_if_needs(alias),
            columns: Vec::new(),
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
