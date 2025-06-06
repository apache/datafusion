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
use std::ops::Deref;
use std::sync::Arc;

use crate::simplify_expressions::ExprSimplifier;

use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
};
use datafusion_common::{plan_err, Column, DFSchemaRef, HashMap, Result, ScalarValue};
use datafusion_expr::expr::Alias;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_expr::utils::{
    collect_subquery_cols, conjunction, find_join_exprs, split_conjunction,
};
use datafusion_expr::{
    expr, lit, BinaryExpr, Cast, EmptyRelation, Expr, FetchType, LogicalPlan,
    LogicalPlanBuilder, Operator,
};
use datafusion_physical_expr::execution_props::ExecutionProps;

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
                    join_filters = remove_duplicated_filter(join_filters, in_predicate);
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
                    self.collected_count_expr_map.get(plan_filter.input.deref())
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
                let mut missing_exprs =
                    self.collect_missing_exprs(&projection.expr, &local_correlated_cols)?;

                let mut expr_result_map_for_count_bug = HashMap::new();
                if let Some(expr_result_map) =
                    self.collected_count_expr_map.get(projection.input.deref())
                {
                    proj_exprs_evaluation_result_on_empty_batch(
                        &projection.expr,
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
                    new_correlated_cols
                        .insert(Column::new(Some(alias.alias.clone()), col.name.clone()));
                }
                self.correlated_subquery_cols_map
                    .insert(plan.clone(), new_correlated_cols);
                if let Some(input_map) =
                    self.collected_count_expr_map.get(alias.input.deref())
                {
                    self.collected_count_expr_map
                        .insert(plan.clone(), input_map.clone());
                }
                Ok(Transformed::no(plan))
            }
            LogicalPlan::Limit(limit) => {
                let input_expr_map = self
                    .collected_count_expr_map
                    .get(limit.input.deref())
                    .cloned();
                // handling the limit clause in the subquery
                let new_plan = match (self.exists_sub_query, self.join_filters.is_empty())
                {
                    // Correlated exist subquery, remove the limit(so that correlated expressions can pull up)
                    (true, false) => Transformed::yes(match limit.get_fetch_type()? {
                        FetchType::Literal(Some(0)) => {
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
        &self,
        exprs: &[Expr],
        correlated_subquery_cols: &BTreeSet<Column>,
    ) -> Result<Vec<Expr>> {
        let mut missing_exprs = vec![];
        for expr in exprs {
            if !missing_exprs.contains(expr) {
                missing_exprs.push(expr.clone())
            }
        }
        for col in correlated_subquery_cols.iter() {
            let col_expr = Expr::Column(col.clone());
            if !missing_exprs.contains(&col_expr) {
                missing_exprs.push(col_expr)
            }
        }
        if let Some(pull_up_having) = &self.pull_up_having_expr {
            let filter_apply_columns = pull_up_having.column_refs();
            for col in filter_apply_columns {
                // add to missing_exprs if not already there
                let contains = missing_exprs
                    .iter()
                    .any(|expr| matches!(expr, Expr::Column(c) if c == col));
                if !contains {
                    missing_exprs.push(Expr::Column(col.clone()))
                }
            }
        }
        Ok(missing_exprs)
    }
}

fn can_pullup_over_aggregation(expr: &Expr) -> bool {
    if let Expr::BinaryExpr(BinaryExpr {
        left,
        op: Operator::Eq,
        right,
    }) = expr
    {
        match (left.deref(), right.deref()) {
            (Expr::Column(_), right) => !right.any_column_refs(),
            (left, Expr::Column(_)) => !left.any_column_refs(),
            (Expr::Cast(Cast { expr, .. }), right)
                if matches!(expr.deref(), Expr::Column(_)) =>
            {
                !right.any_column_refs()
            }
            (left, Expr::Cast(Cast { expr, .. }))
                if matches!(expr.deref(), Expr::Column(_)) =>
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

fn remove_duplicated_filter(filters: Vec<Expr>, in_predicate: &Expr) -> Vec<Expr> {
    filters
        .into_iter()
        .filter(|filter| {
            if filter == in_predicate {
                return false;
            }

            // ignore the binary order
            !match (filter, in_predicate) {
                (Expr::BinaryExpr(a_expr), Expr::BinaryExpr(b_expr)) => {
                    (a_expr.op == b_expr.op)
                        && (a_expr.left == b_expr.left && a_expr.right == b_expr.right)
                        || (a_expr.left == b_expr.right && a_expr.right == b_expr.left)
                }
                _ => false,
            }
        })
        .collect::<Vec<_>>()
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
                let new_expr = match expr {
                    Expr::AggregateFunction(expr::AggregateFunction { func, .. }) => {
                        if func.name() == "count" {
                            Transformed::yes(Expr::Literal(
                                ScalarValue::Int64(Some(0)),
                                None,
                            ))
                        } else {
                            Transformed::yes(Expr::Literal(ScalarValue::Null, None))
                        }
                    }
                    _ => Transformed::no(expr),
                };
                Ok(new_expr)
            })
            .data()?;

        let result_expr = result_expr.unalias();
        let props = ExecutionProps::new();
        let info = SimplifyContext::new(&props).with_schema(Arc::clone(schema));
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
            let props = ExecutionProps::new();
            let info = SimplifyContext::new(&props).with_schema(Arc::clone(schema));
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
        let props = ExecutionProps::new();
        let info = SimplifyContext::new(&props).with_schema(schema);
        let simplifier = ExprSimplifier::new(info);
        let result_expr = simplifier.simplify(result_expr)?;
        match &result_expr {
            // evaluate to false or null on empty batch, no need to pull up
            Expr::Literal(ScalarValue::Null, _)
            | Expr::Literal(ScalarValue::Boolean(Some(false)), _) => None,
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
