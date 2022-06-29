use crate::{utils, OptimizerConfig, OptimizerRule};
use datafusion_common::{Column, DFSchemaRef};
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

    // split into filters
    let mut filters = vec![];
    utils::split_conjunction(&filter.predicate, &mut filters);

    // Grab column names to join on
    let cols = find_join_exprs(filters, sub_input.schema());
    if cols.is_empty() {
        return Ok(plan.clone());
    }

    // Only operate if one column is present and the other closed upon from outside scope
    let l_col: Vec<_> = cols.iter()
        .map(|it| &it.0)
        .map(|it| Column::from_qualified_name(it.as_str()))
        .collect();
    let r_col: Vec<_> = cols.iter()
        .map(|it| &it.1)
        .map(|it| Column::from_qualified_name(it.as_str()))
        .collect();
    let expr: Vec<_> = r_col.iter().map(|it| Expr::Column(it.clone())).collect();
    let aggr_expr: Vec<Expr> = vec![];
    let join_keys = (l_col.clone(), r_col.clone());
    let right = LogicalPlanBuilder::from((*filter.input).clone())
        .aggregate(expr.clone(), aggr_expr)?
        .project(expr)?
        .build()?;
    let new_plan = LogicalPlanBuilder::from((**input).clone())
        .join(&right, JoinType::Inner, join_keys, None)?
        .build()?;
    Ok(new_plan)
}

fn find_join_exprs(filters: Vec<&Expr>, schema: &DFSchemaRef) -> Vec<(String, String)> {
    // only process equals expressions for joins
    let equals: Vec<_> = filters.iter().map(|it| {
        match it {
            Expr::BinaryExpr { left, op, right } => {
                match op {
                    Operator::Eq => Some((*left.clone(), *right.clone())),
                    _ => None,
                }
            }
            _ => None
        }
    }).flatten().collect();

    // only process column expressions for joins
    let cols: Vec<_> = equals.iter().map(|it| {
        let l = match &it.0 {
            Expr::Column(col) => col,
            _ => return None,
        };
        let r = match &it.1 {
            Expr::Column(col) => col,
            _ => return None,
        };
        Some((l.name.clone(), r.name.clone()))
    }).flatten().collect();

    // get names of fields TODO: Must fully qualify these!
    let fields: HashSet<_> = schema
        .fields()
        .iter()
        .map(|f| f.name())
        .collect();

    // Ensure closed-upon fields are always on left, and in-scope on the right
    let sorted: Vec<_> = cols.iter().map(|it| {
        if fields.contains(&it.0) && fields.contains(&it.1) {
            return None; // Need one of each
        }
        if !fields.contains(&it.0) && !fields.contains(&it.1) {
            return None; // Need one of each
        }
        if fields.contains(&it.0) {
            Some((it.1.clone(), it.0.clone()))
        } else {
            Some((it.0.clone(), it.1.clone()))
        }
    }).flatten().collect();

    sorted
}