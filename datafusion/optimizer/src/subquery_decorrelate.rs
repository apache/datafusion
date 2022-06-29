use crate::{utils, OptimizerConfig, OptimizerRule};
use datafusion_common::Column;
use datafusion_expr::logical_plan::{Filter, JoinType, Subquery};
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder, Operator};
use hashbrown::HashSet;
use std::sync::Arc;

/// Optimizer rule for rewriting subquery filters to joins
#[derive(Default)]
pub struct SubqueryDecorrelate {}

impl SubqueryDecorrelate {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for SubqueryDecorrelate {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &OptimizerConfig,
    ) -> datafusion_common::Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter(Filter { predicate, input }) => {
                return match predicate {
                    // TODO: arbitrary expressions
                    Expr::Exists { subquery, negated } => {
                        if *negated {
                            return Ok(plan.clone());
                        }
                        optimize_exists(plan, subquery, input)
                    }
                    _ => Ok(plan.clone()),
                };
            }
            _ => {
                // Apply the optimization to all inputs of the plan
                utils::optimize_children(self, plan, optimizer_config)
            }
        }
    }

    fn name(&self) -> &str {
        "subquery_decorrelate"
    }
}

/// Takes a query like:
///
/// select c.id from customers c where exists (select * from orders o where o.c_id = c.id)
///
/// and optimizes it into:
///
/// select c.id from customers c
/// inner join (select o.c_id from orders o group by o.c_id) o on o.c_id = c.c_id
fn optimize_exists(
    plan: &LogicalPlan,
    subquery: &Subquery,
    input: &Arc<LogicalPlan>,
) -> datafusion_common::Result<LogicalPlan> {
    // Only operate if there is one input
    let sub_inputs = subquery.subquery.inputs();
    if sub_inputs.len() != 1 {
        return Ok(plan.clone());
    }
    let sub_input = if let Some(i) = sub_inputs.get(0) {
        i
    } else {
        return Ok(plan.clone());
    };

    // Only operate on subqueries that are trying to filter on an expression from an outer query
    let filter = if let LogicalPlan::Filter(f) = sub_input {
        f
    } else {
        return Ok(plan.clone());
    };

    // Only operate on a single binary equality expression (for now)
    let (left, op, right) =
        if let Expr::BinaryExpr { left, op, right } = &filter.predicate {
            (left, op, right)
        } else {
            return Ok(plan.clone());
        };
    match op {
        Operator::Eq => {},
        _ => return Ok(plan.clone())
    }

    // collect list of columns
    let lcol = match &**left {
        Expr::Column(col) => col,
        _ => return Ok(plan.clone()),
    };
    let rcol = match &**right {
        Expr::Column(col) => col,
        _ => return Ok(plan.clone()),
    };
    let cols = vec![lcol, rcol];
    let cols: HashSet<_> = cols.iter().map(|c| &c.name).collect();
    let fields: HashSet<_> = sub_input
        .schema()
        .fields()
        .iter()
        .map(|f| f.name())
        .collect();

    // Only operate if one column is present and the other closed upon from outside scope
    let found: Vec<_> = cols.intersection(&fields).map(|it| (*it).clone()).collect();
    let closed_upon: Vec<_> = cols.difference(&fields).map(|it| (*it).clone()).collect();
    if found.len() != 1 || closed_upon.len() != 1 {
        return Ok(plan.clone());
    }
    let found = if let Some(it) = found.get(0) {
        it
    } else {
        return Ok(plan.clone());
    };
    let closed_upon = if let Some(it) = closed_upon.get(0) {
        it
    } else {
        return Ok(plan.clone());
    };

    let c_col = vec![Column::from_qualified_name(closed_upon)];
    let f_col = vec![Column::from_qualified_name(found)];
    let expr = vec![Expr::Column(found.as_str().into())];
    let group_expr = vec![Expr::Column(found.as_str().into())];
    let aggr_expr: Vec<Expr> = vec![];
    let join_keys = (c_col.clone(), f_col.clone());
    let right = LogicalPlanBuilder::from((*filter.input).clone())
        .aggregate(group_expr, aggr_expr)?
        .project(expr)?
        .build()?;
    let new_plan = LogicalPlanBuilder::from((**input).clone())
        .join(&right, JoinType::Inner, join_keys, None)?
        .build()?;
    Ok(new_plan)
}
