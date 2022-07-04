use std::collections::HashSet;
use crate::{utils, OptimizerConfig, OptimizerRule};
use datafusion_common::{Column, DataFusionError};
use datafusion_expr::logical_plan::{Filter, JoinType, Subquery};
use datafusion_expr::{combine_filters, Expr, LogicalPlan, LogicalPlanBuilder};
use std::sync::Arc;
use log::debug;
use crate::utils::{find_join_exprs, get_id, split_conjunction};

/// Optimizer rule for rewriting subquery filters to joins
#[derive(Default)]
pub struct DecorrelateScalarSubquery {}

impl DecorrelateScalarSubquery {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for DecorrelateScalarSubquery {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &OptimizerConfig,
    ) -> datafusion_common::Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter(Filter { predicate, input }) => {
                // Apply optimizer rule to current input
                let optimized_input = self.optimize(input, optimizer_config)?;

                let (subqueries, others) = extract_subquery_exprs(predicate);
                let subquery_expr = match subqueries.as_slice() {
                    [it] => it,
                    _ => {
                        let optimized_plan = LogicalPlan::Filter(Filter {
                            predicate: predicate.clone(),
                            input: Arc::new(optimized_input),
                        });
                        return Ok(optimized_plan); // TODO: copy >1 subquery from where exists
                    }
                };
                let others: Vec<_> = others.iter().map(|it| (*it).clone()).collect();

                optimize_scalar(plan, &subquery_expr, &optimized_input, &others)
            }
            _ => {
                // Apply the optimization to all inputs of the plan
                utils::optimize_children(self, plan, optimizer_config)
            }
        }
    }

    fn name(&self) -> &str {
        "decorrelate_scalar_subquery"
    }
}

/// Takes a query like:
///
/// ```select id from customers where balance > (select avg(total) from orders)```
///
/// and optimizes it into:
///
/// ```select c.id from customers c
/// inner join (select c_id, avg(total) as val from orders group by c_id) o on o.c_id = c.c_id
/// where c.balance > o.val```
///
/// # Arguments
///
/// * filter_plan - The logical plan for the filter containing the `where exists` clause
/// * subqry - The subquery portion of the `where exists` (select * from orders)
/// * negated - True if the subquery is a `where not exists`
/// * filter_input - The non-subquery portion (from customers)
/// * other_exprs - Any additional parts to the `where` expression (and c.x = y)
fn optimize_scalar(
    filter_plan: &LogicalPlan,
    subqry: &Expr,
    filter_input: &LogicalPlan,
    other_filter_exprs: &[Expr],
) -> datafusion_common::Result<LogicalPlan> {
    let subqueries = extract_subqueries(subqry);
    let subquery = match subqueries.as_slice() {
        [it] => it,
        _ => return Ok(filter_plan.clone())
    };

    let proj = match &*subquery.subquery {
        LogicalPlan::Projection(it) => it,
        _ => return Ok(filter_plan.clone())
    };
    let proj = match proj.expr.as_slice() {
        [it] => it,
        _ => return Ok(filter_plan.clone()) // scalar subquery means only 1 expr
    };
    let proj = Expr::Alias(Box::new(proj.clone()), "__value".to_string());

    // Only operate if there is one input
    let sub_inputs = subquery.subquery.inputs();
    let sub_input = match sub_inputs.as_slice() {
        [it] => it,
        _ => return Ok(filter_plan.clone())
    };

    // Only operate on subqueries that are trying to filter on an expression from an outer query
    let aggr = match sub_input {
        LogicalPlan::Aggregate(a) => a,
        _ => return Ok(filter_plan.clone())
    };
    let filter = match &*aggr.input {
        LogicalPlan::Filter(f) => f,
        _ => return Ok(filter_plan.clone())
    };

    // split into filters
    let mut subqry_filter_exprs = vec![];
    split_conjunction(&filter.predicate, &mut subqry_filter_exprs);

    // get names of fields
    let subqry_fields: HashSet<_> = filter.input.schema().fields().iter()
        .map(|f| f.qualified_name()).collect();
    debug!("{:?}", subqry_fields);

    // Grab column names to join on
    let (col_exprs, other_subqry_exprs) = find_join_exprs(subqry_filter_exprs, &subqry_fields);
    let (subqry_cols, filter_input_cols) = col_exprs;

    // Only operate if one column is present and the other closed upon from outside scope
    let subqry_alias = format!("__sq_{}", get_id());
    let group_by: Vec<_> = subqry_cols.iter().map(|it| Expr::Column(it.clone())).collect();

    // build subqry side of join - the thing the subquery was querying
    let subqry_plan = LogicalPlanBuilder::from((*filter.input).clone());
    let subqry_plan = if let Some(expr) = combine_filters(&other_subqry_exprs) {
        subqry_plan.filter(expr)? // if the subquery had additional expressions, restore them
    } else {
        subqry_plan
    };
    let proj: Vec<_> = group_by.iter().cloned()
        .chain(vec![proj].iter().cloned()).collect();
    let subqry_plan = subqry_plan
        .aggregate(group_by.clone(), aggr.aggr_expr.clone())?
        .project_with_alias(proj, Some(subqry_alias.clone()))?
        .build()?;

    // qualify the join columns for outside the subquery
    let subqry_cols: Vec<_> = subqry_cols.iter().map(|it| {
            Column { relation: Some(subqry_alias.clone()), name: it.name.clone() }
        }).collect();
    let join_keys = (filter_input_cols, subqry_cols);
    println!("Scalar Joining:\n{}\nto:\n{}\non{:?}", subqry_plan.display_indent(), filter_input.display_indent(), join_keys);

    // join our sub query into the main plan
    let new_plan = LogicalPlanBuilder::from(filter_input.clone());
    let new_plan = if join_keys.0.len() > 0 {
        new_plan.join(&subqry_plan, JoinType::Inner, join_keys, None)?
    } else {
        new_plan.cross_join(&subqry_plan)?
    };
    let new_plan = if let Some(expr) = combine_filters(other_filter_exprs) {
        new_plan.filter(expr)? // if the main query had additional expressions, restore them
    } else {
        new_plan
    };

    // restore conditions
    let new_plan = match subqry {
        Expr::BinaryExpr { left, op, right } => {
            match &**right {
                Expr::ScalarSubquery(subquery) => {
                    let right = Box::new(Expr::Column(Column {
                        relation: Some(subqry_alias),
                        name: "__value".to_string(),
                    }));
                    let expr = Expr::BinaryExpr { left: left.clone(), op: op.clone(), right };
                    new_plan.filter(expr)?
                }
                _ => return Err(DataFusionError::Plan("Not a scalar subquery!".to_string()))
            }
        }
        _ => return Err(DataFusionError::Plan("Not a scalar subquery!".to_string()))
    };

    let new_plan = new_plan.build()?;
    println!("{}", new_plan.display_indent());
    Ok(new_plan)
}

pub fn extract_subquery_exprs(predicate: &Expr) -> (Vec<&Expr>, Vec<&Expr>) {
    let mut filters = vec![];
    split_conjunction(predicate, &mut filters);

    let (subqueries, others): (Vec<_>, Vec<_>) = filters.iter()
        .partition(|expr| {
            match expr {
                Expr::BinaryExpr { left, op: _, right } => {
                    let l_query = match &**left {
                        Expr::ScalarSubquery(subquery) => {
                            Some(subquery.clone())
                        }
                        _ => None,
                    };
                    let r_query = match &**right {
                        Expr::ScalarSubquery(subquery) => {
                            Some(subquery.clone())
                        }
                        _ => None,
                    };
                    if l_query.is_some() && r_query.is_some() {
                        return false; // TODO: (subquery A) = (subquery B)
                    }
                    match l_query {
                        Some(_) => return true,
                        _ => {}
                    }
                    match r_query {
                        Some(_) => return true,
                        _ => {}
                    }
                    false
                }
                _ => false
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
                Expr::BinaryExpr { left, op: _, right } => {
                    vec![&**left, &**right].iter().for_each(|it| {
                        match it {
                            Expr::ScalarSubquery(subquery) => {
                                acc.push(subquery.clone());
                            }
                            _ => {}
                        };
                    })
                }
                _ => {}
            }
            acc
        });
    return subqueries;
}
