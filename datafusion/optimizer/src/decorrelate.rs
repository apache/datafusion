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

//! [`PullUpCorrelatedExpr`] converts correlated subqueries to `Joins`

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::simplify_expressions::ExprSimplifier;

use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
};
use datafusion_common::{
    Column, DFSchemaRef, HashMap, Result, ScalarValue, assert_or_internal_err, plan_err,
};
use datafusion_expr::expr::Alias;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_expr::utils::{
    collect_subquery_cols, collect_subquery_join_exprs, conjunction, find_join_exprs,
    split_conjunction,
};
use datafusion_expr::{
    BinaryExpr, Cast, EmptyRelation, Expr, ExprSchemable, FetchType, LogicalPlan,
    LogicalPlanBuilder, Operator, expr, lit,
};

/// This struct rewrite the sub query plan by pull up the correlated
/// expressions(contains outer reference columns) from the inner subquery's
/// 'Filter'. It adds the inner reference columns to the 'Projection' or
/// 'Aggregate' of the subquery if they are missing, so that they can be
/// evaluated by the parent operator as the join condition.
#[derive(Debug)]
pub struct PullUpCorrelatedExpr {
    pub join_filters: Vec<Expr>,
    /// mapping from the plan to its holding correlated columns
    pub correlated_subquery_cols_map: HashMap<LogicalPlan, BTreeSet<Column>>,
    pub in_predicate_opt: Option<Expr>,
    /// Is this an Exists(Not Exists) SubQuery. Defaults to **FALSE**
    pub exists_sub_query: bool,
    /// Can the correlated expressions be pulled up. Defaults to **TRUE**
    pub can_pull_up: bool,
    /// Indicates if we encounter any correlated expression that can not be pulled up
    /// above a aggregation without changing the meaning of the query.
    can_pull_over_aggregation: bool,
    /// Do we need to handle [the count bug] during the pull up process.
    ///
    /// The "count bug" was described in [Optimization of Nested SQL
    /// Queries Revisited](https://dl.acm.org/doi/pdf/10.1145/38714.38723). This bug is
    /// not specific to the COUNT function, and it can occur with any aggregate function,
    /// such as SUM, AVG, etc. The anomaly arises because aggregates fail to distinguish
    /// between an empty set and null values when optimizing a correlated query as a join.
    /// Here, we use "the count bug" to refer to all such cases.
    ///
    /// [the count bug]: https://github.com/apache/datafusion/issues/10553
    pub need_handle_count_bug: bool,
    /// mapping from the plan to its expressions' evaluation result on empty batch
    pub collected_count_expr_map: HashMap<LogicalPlan, ExprResultMap>,
    /// pull up having expr, which must be evaluated after the Join
    pub pull_up_having_expr: Option<Expr>,
    /// whether we have converted a scalar aggregation into a group aggregation. When unnesting
    /// lateral joins, we need to produce a left outer join in such cases.
    pub pulled_up_scalar_agg: bool,
    /// A correlated column wrapped in an expression (e.g. `CAST(t2.b AS INT)`)
    /// gets grouped by that entire expression instead of the bare column,
    /// aliased to a generated name, once, in the `Aggregate` this column belongs to. Every
    /// later reference to that column, in a `Projection` above it for
    /// instance, needs to use the same alias instead of the now
    /// unresolvable bare column. This records the mapping the first
    /// time it's made.
    correlated_col_aliases: HashMap<Column, Expr>,
    /// `LIMIT 0` forces the subquery to zero rows unconditionally, regardless
    /// of whether the correlation matched, collapsing it to an `EmptyRelation`.
    /// Join-compensation needs this flag to
    /// distinguish "empty" from its usual "matched" default.
    pub forces_empty_result: bool,
}

impl Default for PullUpCorrelatedExpr {
    fn default() -> Self {
        Self::new()
    }
}

impl PullUpCorrelatedExpr {
    pub fn new() -> Self {
        Self {
            join_filters: vec![],
            correlated_subquery_cols_map: HashMap::new(),
            in_predicate_opt: None,
            exists_sub_query: false,
            can_pull_up: true,
            can_pull_over_aggregation: true,
            need_handle_count_bug: false,
            collected_count_expr_map: HashMap::new(),
            pull_up_having_expr: None,
            pulled_up_scalar_agg: false,
            correlated_col_aliases: HashMap::new(),
            forces_empty_result: false,
        }
    }

    /// Set if we need to handle [the count bug] during the pull up process
    ///
    /// [the count bug]: https://github.com/apache/datafusion/issues/10553
    pub fn with_need_handle_count_bug(mut self, need_handle_count_bug: bool) -> Self {
        self.need_handle_count_bug = need_handle_count_bug;
        self
    }

    /// Set the in_predicate_opt
    pub fn with_in_predicate_opt(mut self, in_predicate_opt: Option<Expr>) -> Self {
        self.in_predicate_opt = in_predicate_opt;
        self
    }

    /// Set if this is an Exists(Not Exists) SubQuery
    pub fn with_exists_sub_query(mut self, exists_sub_query: bool) -> Self {
        self.exists_sub_query = exists_sub_query;
        self
    }
}

/// Used to indicate the unmatched rows from the inner(subquery) table after the left out Join
/// This is used to handle [the Count bug]
///
/// [the Count bug]: https://github.com/apache/datafusion/issues/10553
pub const UN_MATCHED_ROW_INDICATOR: &str = "__always_true";

/// Mapping from expr display name to its evaluation result on empty record
/// batch (for example: 'count(*)' is 'ScalarValue(0)', 'count(*) + 2' is
/// 'ScalarValue(2)')
pub type ExprResultMap = HashMap<String, Expr>;

impl TreeNodeRewriter for PullUpCorrelatedExpr {
    type Node = LogicalPlan;

    fn f_down(&mut self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Filter(_) => Ok(Transformed::no(plan)),
            // Subquery nodes are scope boundaries for correlation. A nested
            // Subquery's outer references belong to a different decorrelation
            // level and must not be pulled up into the current scope.
            LogicalPlan::Subquery(_) => {
                Ok(Transformed::new(plan, false, TreeNodeRecursion::Jump))
            }
            LogicalPlan::Union(_) | LogicalPlan::Sort(_) | LogicalPlan::Extension(_) => {
                let plan_hold_outer = !plan.all_out_ref_exprs().is_empty();
                if plan_hold_outer {
                    // the unsupported case
                    self.can_pull_up = false;
                    Ok(Transformed::new(plan, false, TreeNodeRecursion::Jump))
                } else {
                    Ok(Transformed::no(plan))
                }
            }
            LogicalPlan::Limit(_) => {
                let plan_hold_outer = !plan.all_out_ref_exprs().is_empty();
                match (self.exists_sub_query, plan_hold_outer) {
                    (false, true) => {
                        // the unsupported case
                        self.can_pull_up = false;
                        Ok(Transformed::new(plan, false, TreeNodeRecursion::Jump))
                    }
                    _ => Ok(Transformed::no(plan)),
                }
            }
            _ if plan.contains_outer_reference() => {
                // the unsupported cases, the plan expressions contain out reference columns(like window expressions)
                self.can_pull_up = false;
                Ok(Transformed::new(plan, false, TreeNodeRecursion::Jump))
            }
            _ => Ok(Transformed::no(plan)),
        }
    }

    fn f_up(&mut self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let subquery_schema = plan.schema();
        match &plan {
            LogicalPlan::Filter(plan_filter) => {
                let subquery_filter_exprs = split_conjunction(&plan_filter.predicate);
                self.can_pull_over_aggregation = self.can_pull_over_aggregation
                    && subquery_filter_exprs
                        .iter()
                        .filter(|e| e.contains_outer())
                        .all(|&e| can_pullup_over_aggregation(e));
                let (mut join_filters, subquery_filters) =
                    find_join_exprs(subquery_filter_exprs)?;
                if let Some(in_predicate) = &self.in_predicate_opt {
                    // in_predicate may be already included in the join filters, remove it from the join filters first.
                    join_filters = remove_duplicated_filter(join_filters, in_predicate)?;
                }
                let correlated_subquery_cols =
                    collect_subquery_cols(&join_filters, subquery_schema)?;
                for expr in join_filters {
                    if !self.join_filters.contains(&expr) {
                        self.join_filters.push(expr)
                    }
                }

                let mut expr_result_map_for_count_bug = HashMap::new();
                let pull_up_expr_opt = if let Some(expr_result_map) =
                    self.collected_count_expr_map.get(&*plan_filter.input)
                {
                    if let Some(expr) = conjunction(subquery_filters.clone()) {
                        filter_exprs_evaluation_result_on_empty_batch(
                            &expr,
                            Arc::clone(plan_filter.input.schema()),
                            expr_result_map,
                            &mut expr_result_map_for_count_bug,
                        )?
                    } else {
                        None
                    }
                } else {
                    None
                };

                if self.pull_up_having_expr.is_some()
                    && let Some(expr) = conjunction(subquery_filters.clone())
                {
                    let unqualified_expr = expr
                        .transform_up(|e| {
                            if let Expr::Column(Column { name, .. }) = &e {
                                Ok(Transformed::yes(Expr::Column(
                                    Column::new_unqualified(name),
                                )))
                            } else {
                                Ok(Transformed::no(e))
                            }
                        })
                        .data()?;
                    let combined = match self.pull_up_having_expr.take() {
                        Some(existing) => existing.and(unqualified_expr),
                        None => unqualified_expr,
                    };
                    self.pull_up_having_expr = Some(combined);
                    let new_plan =
                        LogicalPlanBuilder::from((*plan_filter.input).clone()).build()?;
                    let mut carried_correlated_cols = correlated_subquery_cols;
                    if let Some(existing) =
                        self.correlated_subquery_cols_map.get(&*plan_filter.input)
                    {
                        carried_correlated_cols.extend(existing.iter().cloned());
                    }
                    self.correlated_subquery_cols_map
                        .insert(new_plan.clone(), carried_correlated_cols);
                    if !expr_result_map_for_count_bug.is_empty() {
                        self.collected_count_expr_map
                            .insert(new_plan.clone(), expr_result_map_for_count_bug);
                    } else if let Some(input_map) = self
                        .collected_count_expr_map
                        .get(&*plan_filter.input)
                        .cloned()
                    {
                        self.collected_count_expr_map
                            .insert(new_plan.clone(), input_map);
                    }
                    return Ok(Transformed::yes(new_plan));
                }

                match (&pull_up_expr_opt, &self.pull_up_having_expr) {
                    (Some(_), Some(_)) => {
                        // Error path
                        plan_err!("Unsupported Subquery plan")
                    }
                    (Some(_), None) => {
                        self.pull_up_having_expr = pull_up_expr_opt;
                        let new_plan =
                            LogicalPlanBuilder::from((*plan_filter.input).clone())
                                .build()?;
                        self.correlated_subquery_cols_map
                            .insert(new_plan.clone(), correlated_subquery_cols);
                        Ok(Transformed::yes(new_plan))
                    }
                    (None, _) => {
                        // if the subquery still has filter expressions, restore them.
                        let mut plan =
                            LogicalPlanBuilder::from((*plan_filter.input).clone());
                        if let Some(expr) = conjunction(subquery_filters) {
                            plan = plan.filter(expr)?
                        }
                        let new_plan = plan.build()?;
                        self.correlated_subquery_cols_map
                            .insert(new_plan.clone(), correlated_subquery_cols);
                        Ok(Transformed::yes(new_plan))
                    }
                }
            }
            LogicalPlan::Projection(projection)
                if self.in_predicate_opt.is_some() || !self.join_filters.is_empty() =>
            {
                let mut local_correlated_cols = BTreeSet::new();
                collect_local_correlated_cols(
                    &plan,
                    &self.correlated_subquery_cols_map,
                    &mut local_correlated_cols,
                );
                // add missing columns to Projection
                let mut missing_exprs = self.collect_missing_exprs(
                    &projection.expr,
                    &local_correlated_cols,
                    projection.input.schema(),
                )?;

                let mut expr_result_map_for_count_bug = HashMap::new();
                if let Some(expr_result_map) =
                    self.collected_count_expr_map.get(&*projection.input)
                {
                    proj_exprs_evaluation_result_on_empty_batch(
                        &missing_exprs,
                        projection.input.schema(),
                        expr_result_map,
                        &mut expr_result_map_for_count_bug,
                    )?;
                    if !expr_result_map_for_count_bug.is_empty() {
                        // has count bug
                        let un_matched_row = Expr::Column(Column::new_unqualified(
                            UN_MATCHED_ROW_INDICATOR.to_string(),
                        ));
                        // add the unmatched rows indicator to the Projection expressions
                        missing_exprs.push(un_matched_row);
                    }
                }

                let new_plan = LogicalPlanBuilder::from((*projection.input).clone())
                    .project(missing_exprs)?
                    .build()?;
                if !expr_result_map_for_count_bug.is_empty() {
                    self.collected_count_expr_map
                        .insert(new_plan.clone(), expr_result_map_for_count_bug);
                }
                Ok(Transformed::yes(new_plan))
            }
            LogicalPlan::Aggregate(aggregate)
                if self.in_predicate_opt.is_some() || !self.join_filters.is_empty() =>
            {
                // If the aggregation is from a distinct it will not change the result for
                // exists/in subqueries so we can still pull up all predicates.
                let is_distinct = aggregate.aggr_expr.is_empty();
                if !is_distinct {
                    self.can_pull_up = self.can_pull_up && self.can_pull_over_aggregation;
                }
                let mut local_correlated_cols = BTreeSet::new();
                collect_local_correlated_cols(
                    &plan,
                    &self.correlated_subquery_cols_map,
                    &mut local_correlated_cols,
                );
                // add missing columns to Aggregation's group expressions
                let mut missing_exprs = self.collect_missing_exprs(
                    &aggregate.group_expr,
                    &local_correlated_cols,
                    aggregate.input.schema(),
                )?;

                // if the original group expressions are empty, need to handle the Count bug
                let mut expr_result_map_for_count_bug = HashMap::new();
                if self.need_handle_count_bug
                    && aggregate.group_expr.is_empty()
                    && !missing_exprs.is_empty()
                {
                    agg_exprs_evaluation_result_on_empty_batch(
                        &aggregate.aggr_expr,
                        aggregate.input.schema(),
                        &mut expr_result_map_for_count_bug,
                    )?;
                    if !expr_result_map_for_count_bug.is_empty() {
                        // has count bug
                        let un_matched_row = lit(true).alias(UN_MATCHED_ROW_INDICATOR);
                        // add the unmatched rows indicator to the Aggregation's group expressions
                        missing_exprs.push(un_matched_row);
                    }
                }
                if aggregate.group_expr.is_empty() {
                    // TODO: how do we handle the case where we have pulled multiple aggregations? For example,
                    // a group agg with a scalar agg as child.
                    self.pulled_up_scalar_agg = true;
                }
                let new_plan = LogicalPlanBuilder::from((*aggregate.input).clone())
                    .aggregate(missing_exprs, aggregate.aggr_expr.to_vec())?
                    .build()?;
                if !expr_result_map_for_count_bug.is_empty() {
                    self.collected_count_expr_map
                        .insert(new_plan.clone(), expr_result_map_for_count_bug);
                }
                Ok(Transformed::yes(new_plan))
            }
            LogicalPlan::SubqueryAlias(alias) => {
                let mut local_correlated_cols = BTreeSet::new();
                collect_local_correlated_cols(
                    &plan,
                    &self.correlated_subquery_cols_map,
                    &mut local_correlated_cols,
                );
                let mut new_correlated_cols = BTreeSet::new();
                for col in local_correlated_cols.iter() {
                    let requalified =
                        Column::new(Some(alias.alias.clone()), col.name.clone());
                    // A column already folded into a group-by alias (see
                    // `collect_missing_exprs`) needs that mapping carried
                    // forward under its requalified name.
                    if let Some(existing_alias) = self.correlated_col_aliases.get(col) {
                        self.correlated_col_aliases
                            .insert(requalified.clone(), existing_alias.clone());
                    }
                    new_correlated_cols.insert(requalified);
                }

                let new_plan = if alias.input.schema().fields().len()
                    != alias.schema.fields().len()
                {
                    LogicalPlanBuilder::from((*alias.input).clone())
                        .alias(alias.alias.clone())?
                        .build()?
                } else {
                    plan.clone()
                };

                self.correlated_subquery_cols_map
                    .insert(new_plan.clone(), new_correlated_cols);
                if let Some(input_map) = self.collected_count_expr_map.get(&*alias.input)
                {
                    self.collected_count_expr_map
                        .insert(new_plan.clone(), input_map.clone());
                }

                if new_plan != plan {
                    Ok(Transformed::yes(new_plan))
                } else {
                    Ok(Transformed::no(plan))
                }
            }
            LogicalPlan::Limit(limit) => {
                let input_expr_map =
                    self.collected_count_expr_map.get(&*limit.input).cloned();
                // handling the limit clause in the subquery
                let new_plan = match (self.exists_sub_query, self.join_filters.is_empty())
                {
                    // Correlated exist subquery, remove the limit(so that correlated expressions can pull up)
                    (true, false) => Transformed::yes(match limit.get_fetch_type()? {
                        FetchType::Literal(Some(0)) => {
                            self.forces_empty_result = true;
                            LogicalPlan::EmptyRelation(EmptyRelation {
                                produce_one_row: false,
                                schema: Arc::clone(limit.input.schema()),
                            })
                        }
                        _ => LogicalPlanBuilder::from((*limit.input).clone()).build()?,
                    }),
                    _ => Transformed::no(plan),
                };
                if let Some(input_map) = input_expr_map {
                    self.collected_count_expr_map
                        .insert(new_plan.data.clone(), input_map);
                }
                Ok(new_plan)
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}

impl PullUpCorrelatedExpr {
    fn collect_missing_exprs(
        &mut self,
        exprs: &[Expr],
        correlated_subquery_cols: &BTreeSet<Column>,
        subquery_schema: &DFSchemaRef,
    ) -> Result<Vec<Expr>> {
        let mut missing_exprs = vec![];
        for expr in exprs {
            if !missing_exprs.contains(expr) {
                missing_exprs.push(expr.clone())
            }
        }
        // A correlated column compared bare (`t1.a = t2.b`) only ever
        // matches one row per value, so grouping the subquery's own
        // aggregate by that column is enough. But if the join filter
        // wraps it in an expression (`t1.a = CAST(t2.b AS INT)`), two
        // different column values can compare equal after the cast, and
        // grouping by the bare column would compute the aggregate once
        // per underlying value instead of once per outer row it actually
        // joins against. Grouping by the wrapping expression instead
        // keeps those together, matching what the join predicate itself
        // treats as equal.
        let join_filter_exprs =
            collect_subquery_join_exprs(&self.join_filters, subquery_schema)?;
        for col in correlated_subquery_cols.iter() {
            if let Some(existing_alias) = self.correlated_col_aliases.get(col) {
                if !collides_with_existing(&missing_exprs, existing_alias) {
                    missing_exprs.push(existing_alias.clone())
                }
                continue;
            }
            let wrapped = join_filter_exprs
                .iter()
                .find(|e| e.column_refs().len() == 1 && e.column_refs().contains(col));
            let col_expr = match wrapped {
                Some(e) if !matches!(e, Expr::Column(_)) => {
                    let alias_name = format!("__correlated_group_expr_{}", col.name);
                    let aliased = e.clone().alias(alias_name.clone());
                    let reference = Expr::Column(Column::new_unqualified(&alias_name));
                    self.join_filters = self
                        .join_filters
                        .iter()
                        .map(|f| {
                            f.clone()
                                .transform_up(|node| {
                                    if &node == e {
                                        Ok(Transformed::yes(reference.clone()))
                                    } else {
                                        Ok(Transformed::no(node))
                                    }
                                })
                                .data()
                        })
                        .collect::<Result<Vec<_>>>()?;
                    self.correlated_col_aliases.insert(col.clone(), reference);
                    aliased
                }
                _ => Expr::Column(col.clone()),
            };
            if !collides_with_existing(&missing_exprs, &col_expr) {
                missing_exprs.push(col_expr)
            }
        }
        if let Some(pull_up_having) = &self.pull_up_having_expr {
            for col in pull_up_having.column_refs() {
                let col_expr = Expr::Column(col.clone());
                // `col` is unqualified but the projection may already have
                // it as `agg.c`. Check by bare name so that doesn't get
                // added twice.
                let already_present = missing_exprs
                    .iter()
                    .any(|expr| matches!(expr, Expr::Column(c) if c.name == col.name))
                    || collides_with_existing(&missing_exprs, &col_expr);
                if !already_present {
                    missing_exprs.push(col_expr)
                }
            }
        }
        Ok(missing_exprs)
    }
}

/// True if `candidate` collides with an existing entry's schema name,
/// e.g. a bare column already wrapped in an equally-named `CAST`.
/// Colliding means `LogicalPlanBuilder::project` would reject it as a duplicate.
fn collides_with_existing(exprs: &[Expr], candidate: &Expr) -> bool {
    let candidate_name = candidate.schema_name().to_string();
    exprs
        .iter()
        .any(|expr| expr.schema_name().to_string() == candidate_name)
}

fn can_pullup_over_aggregation(expr: &Expr) -> bool {
    if let Expr::BinaryExpr(BinaryExpr {
        left,
        op: Operator::Eq,
        right,
    }) = expr
    {
        match (&**left, &**right) {
            (Expr::Column(_), right) => !right.any_column_refs(),
            (left, Expr::Column(_)) => !left.any_column_refs(),
            (Expr::Cast(Cast { expr, .. }), right)
                if matches!(&**expr, Expr::Column(_)) =>
            {
                !right.any_column_refs()
            }
            (left, Expr::Cast(Cast { expr, .. }))
                if matches!(&**expr, Expr::Column(_)) =>
            {
                !left.any_column_refs()
            }
            (_, _) => false,
        }
    } else {
        false
    }
}

fn collect_local_correlated_cols(
    plan: &LogicalPlan,
    all_cols_map: &HashMap<LogicalPlan, BTreeSet<Column>>,
    local_cols: &mut BTreeSet<Column>,
) {
    for child in plan.inputs() {
        if let Some(cols) = all_cols_map.get(child) {
            local_cols.extend(cols.clone());
        }
        // SubqueryAlias is treated as the leaf node
        if !matches!(child, LogicalPlan::SubqueryAlias(_)) {
            collect_local_correlated_cols(child, all_cols_map, local_cols);
        }
    }
}

fn remove_duplicated_filter(
    filters: Vec<Expr>,
    in_predicate: &Expr,
) -> Result<Vec<Expr>> {
    // We assume below that swapping the order of operands to an operator does
    // not change behavior, which is only true if the operator is commutative.
    assert_or_internal_err!(
        match in_predicate {
            Expr::BinaryExpr(b) => b.op.swap() == Some(b.op),
            _ => true,
        },
        "remove_duplicated_filter: in_predicate must use a commutative operator"
    );

    Ok(filters
        .into_iter()
        .filter(|filter| {
            if filter == in_predicate {
                return false;
            }

            // Treat swapped operand order to a binary operator as equivalent
            !match (filter, in_predicate) {
                (Expr::BinaryExpr(a_expr), Expr::BinaryExpr(b_expr)) => {
                    a_expr.op == b_expr.op
                        && ((a_expr.left == b_expr.left && a_expr.right == b_expr.right)
                            || (a_expr.left == b_expr.right
                                && a_expr.right == b_expr.left))
                }
                _ => false,
            }
        })
        .collect::<Vec<_>>())
}

fn agg_exprs_evaluation_result_on_empty_batch(
    agg_expr: &[Expr],
    schema: &DFSchemaRef,
    expr_result_map_for_count_bug: &mut ExprResultMap,
) -> Result<()> {
    for e in agg_expr.iter() {
        let result_expr = e
            .clone()
            .transform_up(|expr| {
                let new_expr = if let Expr::AggregateFunction(agg) = &expr {
                    let return_type = expr.get_type(schema.as_ref())?;
                    let default_value = agg.func.default_value(&return_type)?;
                    Transformed::yes(Expr::Literal(default_value, None))
                } else {
                    Transformed::no(expr)
                };
                Ok(new_expr)
            })
            .data()?;

        let result_expr = result_expr.unalias();
        let info = SimplifyContext::builder()
            .with_schema(Arc::clone(schema))
            .build();
        let simplifier = ExprSimplifier::new(info);
        let result_expr = simplifier.simplify(result_expr)?;
        expr_result_map_for_count_bug.insert(e.schema_name().to_string(), result_expr);
    }
    Ok(())
}

fn proj_exprs_evaluation_result_on_empty_batch(
    proj_expr: &[Expr],
    schema: &DFSchemaRef,
    input_expr_result_map_for_count_bug: &ExprResultMap,
    expr_result_map_for_count_bug: &mut ExprResultMap,
) -> Result<()> {
    for expr in proj_expr.iter() {
        let result_expr = expr
            .clone()
            .transform_up(|expr| {
                if let Expr::Column(Column { name, .. }) = &expr {
                    if let Some(result_expr) =
                        input_expr_result_map_for_count_bug.get(name)
                    {
                        Ok(Transformed::yes(result_expr.clone()))
                    } else {
                        Ok(Transformed::no(expr))
                    }
                } else {
                    Ok(Transformed::no(expr))
                }
            })
            .data()?;

        if result_expr.ne(expr) {
            let info = SimplifyContext::builder()
                .with_schema(Arc::clone(schema))
                .build();
            let simplifier = ExprSimplifier::new(info);
            let result_expr = simplifier.simplify(result_expr)?;
            let expr_name = match expr {
                Expr::Alias(Alias { name, .. }) => name.to_string(),
                Expr::Column(Column {
                    relation: _,
                    name,
                    spans: _,
                }) => name.to_string(),
                _ => expr.schema_name().to_string(),
            };
            expr_result_map_for_count_bug.insert(expr_name, result_expr);
        }
    }
    Ok(())
}

fn filter_exprs_evaluation_result_on_empty_batch(
    filter_expr: &Expr,
    schema: DFSchemaRef,
    input_expr_result_map_for_count_bug: &ExprResultMap,
    expr_result_map_for_count_bug: &mut ExprResultMap,
) -> Result<Option<Expr>> {
    let result_expr = filter_expr
        .clone()
        .transform_up(|expr| {
            if let Expr::Column(Column { name, .. }) = &expr {
                if let Some(result_expr) = input_expr_result_map_for_count_bug.get(name) {
                    Ok(Transformed::yes(result_expr.clone()))
                } else {
                    Ok(Transformed::no(expr))
                }
            } else {
                Ok(Transformed::no(expr))
            }
        })
        .data()?;

    let pull_up_expr = if result_expr.ne(filter_expr) {
        let info = SimplifyContext::builder().with_schema(schema).build();
        let simplifier = ExprSimplifier::new(info);
        let result_expr = simplifier.simplify(result_expr)?;
        match &result_expr {
            Expr::Literal(ScalarValue::Null, _)
            | Expr::Literal(ScalarValue::Boolean(None), _)
            | Expr::Literal(ScalarValue::Boolean(Some(false)), _) => {
                for (name, exprs) in input_expr_result_map_for_count_bug {
                    expr_result_map_for_count_bug.insert(name.clone(), exprs.clone());
                }
                None
            }
            // evaluate to true on empty batch, need to pull up the expr
            Expr::Literal(ScalarValue::Boolean(Some(true)), _) => {
                for (name, exprs) in input_expr_result_map_for_count_bug {
                    expr_result_map_for_count_bug.insert(name.clone(), exprs.clone());
                }
                Some(filter_expr.clone())
            }
            // can not evaluate statically
            _ => {
                for input_expr in input_expr_result_map_for_count_bug.values() {
                    let new_expr = Expr::Case(expr::Case {
                        expr: None,
                        when_then_expr: vec![(
                            Box::new(result_expr.clone()),
                            Box::new(input_expr.clone()),
                        )],
                        else_expr: Some(Box::new(Expr::Literal(ScalarValue::Null, None))),
                    });
                    let expr_key = new_expr.schema_name().to_string();
                    expr_result_map_for_count_bug.insert(expr_key, new_expr);
                }
                None
            }
        }
    } else {
        for (name, exprs) in input_expr_result_map_for_count_bug {
            expr_result_map_for_count_bug.insert(name.clone(), exprs.clone());
        }
        None
    };
    Ok(pull_up_expr)
}
