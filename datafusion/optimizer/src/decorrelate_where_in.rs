use crate::{utils, OptimizerConfig, OptimizerRule};
use datafusion_common::{Column, DataFusionError};
use datafusion_expr::logical_plan::{Filter, JoinType, Subquery};
use datafusion_expr::{combine_filters, Expr, LogicalPlan, LogicalPlanBuilder, Operator};
use std::sync::Arc;
use crate::utils::{get_id, split_conjunction};

#[derive(Default)]
pub struct DecorrelateWhereIn {}

impl DecorrelateWhereIn {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for DecorrelateWhereIn {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &OptimizerConfig,
    ) -> datafusion_common::Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter(Filter { predicate, input }) => {
                let (subqueries, others) = extract_subquery_exprs(predicate);
                let subquery_expr = match subqueries.as_slice() {
                    [it] => it,
                    _ => return Ok(plan.clone()) // TODO: >1 subquery
                };
                let others: Vec<_> = others.iter().map(|it| (*it).clone()).collect();

                // Apply optimizer rule to current input
                let subquery_expr = match subquery_expr {
                    Expr::InSubquery { expr, subquery, negated } => {
                        let expr = Box::new((**expr).clone());
                        let negated = *negated;
                        let subquery = self.optimize(&*subquery.subquery, optimizer_config)?;
                        let subquery = Arc::new(subquery);
                        let subquery = Subquery { subquery };
                        Expr::InSubquery {expr, subquery, negated}
                    },
                    _ => Err(DataFusionError::Plan("Invalid where in subquery!".to_string()))?
                };

                optimize_where_in(plan, &subquery_expr, &input, &others)
            }
            _ => {
                // Apply the optimization to all inputs of the plan
                utils::optimize_children(self, plan, optimizer_config)
            }
        }
    }

    fn name(&self) -> &str {
        "decorrelate_where_in"
    }
}

fn optimize_where_in(
    plan: &LogicalPlan,
    subquery_expr: &Expr,
    input: &LogicalPlan,
    outer_others: &[Expr],
) -> datafusion_common::Result<LogicalPlan> {
    let (in_expr, subquery, negated) = match subquery_expr {
        Expr::InSubquery { expr, subquery, negated } => {
            (expr, subquery, negated)
        },
        _ => Err(DataFusionError::Plan("Invalid where in subquery!".to_string()))?
    };
    if *negated {
        return Ok(plan.clone()) // TODO: no negations yet
    }
    let wtf = format!("{}", subquery.subquery.display_indent());

    let proj = match &*subquery.subquery {
        LogicalPlan::Projection(it) => it,
        _ => return Ok(plan.clone())
    };
    let sub_input = proj.input.clone();
    let proj = match proj.expr.as_slice() {
        [it] => it,
        _ => return Ok(plan.clone()) // in subquery means only 1 expr
    };
    let r_col = match proj {
        Expr::Column(it) => it.name.clone(),
        _ => return Ok(plan.clone()) // only operate on columns for now, not arbitrary expressions
    };
    let proj = Expr::Alias(Box::new(proj.clone()), "__value".to_string());

    // Grab column names to join on
    let l_col = match &**in_expr {
        Expr::Column(it) => it.name.clone(),
        _ => return Ok(plan.clone()) // only operate on columns for now, not arbitrary expressions
    };
    let cols = vec![(l_col, r_col)];

    // Only operate if one column is present and the other closed upon from outside scope
    let r_alias = format!("__sq_{}", get_id());
    let l_col: Vec<_> = cols
        .iter()
        .map(|it| &it.0)
        .map(|it| Column::from(it.as_str()))
        .collect();
    let r_col: Vec<_> = cols
        .iter()
        .map(|it| &it.1)
        .map(|it| Column::from(it.as_str()))
        .collect();
    let group_by: Vec<_> = r_col.iter().map(|it| Expr::Column(it.clone())).collect();
    let aggr_expr: Vec<Expr> = vec![];

    // build right side of join - the thing the subquery was querying
    let right = LogicalPlanBuilder::from((*sub_input).clone());
    let proj: Vec<_> = group_by.clone();
    let right = right
        .aggregate(group_by.clone(), aggr_expr)?
        .project_with_alias(proj, Some(r_alias.clone()))?
        .build()?;

    // qualify the join columns for outside the subquery
    let r_col: Vec<_> = cols
        .iter()
        .map(|it| &it.1)
        .map(|it| {
            Column { relation: Some(r_alias.clone()), name: it.clone() }
        })
        .collect();
    let join_keys = (l_col, r_col);
    let planny = format!("Joining:\n{}\nto:\n{}\non{:?}", right.display_indent(), input.display_indent(), join_keys);

    // join our sub query into the main plan
    let new_plan = LogicalPlanBuilder::from(input.clone())
        .join(&right, JoinType::Inner, join_keys, None)?;
    let new_plan = if let Some(expr) = combine_filters(outer_others) {
        new_plan.filter(expr)? // if the main query had additional expressions, restore them
    } else {
        new_plan
    };

    // restore conditions
    let right = Box::new(Expr::Column(Column {
        relation: Some(r_alias), name: "__value".to_string()
    }));
    let expr = Expr::BinaryExpr {left: in_expr.clone(), op: Operator::Eq, right};
    new_plan.filter(expr)?;

    let new_plan = new_plan.build()?;
    let mcblah = format!("{}", new_plan.display_indent());
    Ok(new_plan)
}

pub fn extract_subquery_exprs(predicate: &Expr) -> (Vec<&Expr>, Vec<&Expr>) {
    let mut filters = vec![];
    split_conjunction(predicate, &mut filters);

    let (subqueries, others): (Vec<_>, Vec<_>) = filters.iter()
        .partition(|expr| {
            match expr {
                Expr::InSubquery { expr: _, subquery: _, negated: _ } => true,
                _ => false,
            }
        });
    (subqueries, others)
}

pub fn extract_subqueries(predicate: &Expr) -> Vec<Subquery> {
    let mut filters = vec![];
    split_conjunction(predicate, &mut filters);

    let subqueries = filters.iter()
        .fold(vec![], |mut acc, expr| {
            match expr {
                Expr::InSubquery { expr: _, subquery, negated: _} => {
                    acc.push(subquery.clone())
                },
                _ => {}
            }
            acc
        });
    return subqueries;
}
