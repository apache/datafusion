use std::collections::HashSet;
use crate::{utils, OptimizerConfig, OptimizerRule};
use datafusion_common::{Column};
use datafusion_expr::logical_plan::{Filter, JoinType};
use datafusion_expr::{combine_filters, Expr, LogicalPlan, LogicalPlanBuilder};
use std::sync::Arc;
use uuid::Uuid;
use crate::utils::{extract_subquery, extract_subquery_exprs, find_join_exprs};

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
                let (subqueries, others) = extract_subquery_exprs(predicate);
                if subqueries.len() != 1 {
                    return Ok(plan.clone()); // TODO: >1 subquery
                }
                let subquery = match subqueries.get(0) {
                    Some(q) => q,
                    _ => return Ok(plan.clone())
                };
                let others: Vec<_> = others.iter().map(|it| (*it).clone()).collect();

                optimize_scalar(plan, subquery, input, &others)
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

fn optimize_scalar(
    plan: &LogicalPlan,
    subquery_expr: &Expr,
    input: &Arc<LogicalPlan>,
    outer_others: &[Expr],
) -> datafusion_common::Result<LogicalPlan> {
    let subqueries = extract_subquery(subquery_expr);
    if subqueries.len() != 1 {
        return Ok(plan.clone()); // TODO: multiple subqueries in expression
    }
    let subquery = match subqueries.get(0) {
        Some(q) => q,
        _ => return Ok(plan.clone()) // no subquery
    };

    // Only operate if there is one input
    let sub_inputs = subquery.subquery.inputs();
    if sub_inputs.len() != 1 {
        return Ok(plan.clone());
    }
    let sub_input = match sub_inputs.get(0) {
        Some(i) => i,
        _ => return Ok(plan.clone())
    };

    // Only operate on subqueries that are trying to filter on an expression from an outer query
    let filter = match sub_input {
        LogicalPlan::Aggregate(a) => {
            match &*a.input {
                LogicalPlan::Filter(f) => f,
                _ => return Ok(plan.clone())
            }
        },
        _ => return Ok(plan.clone())
    };

    // split into filters
    let mut filters = vec![];
    utils::split_conjunction(&filter.predicate, &mut filters);

    // get names of fields TODO: Must fully qualify these!
    let fields: HashSet<_> = filter.input
        .schema()
        .fields()
        .iter()
        .map(|f| f.name())
        .collect();
    let blah = format!("{:?}", fields);

    // Grab column names to join on
    let (cols, others) = find_join_exprs(filters, &fields);
    if cols.is_empty() {
        return Ok(plan.clone()); // no joins found
    }

    // Only operate if one column is present and the other closed upon from outside scope
    let r_alias = Uuid::new_v4().to_string();
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
    let right = LogicalPlanBuilder::from((*filter.input).clone());
    let right = if let Some(expr) = combine_filters(&others) {
        right.filter(expr)? // if the subquery had additional expressions, restore them
    } else {
        right
    };
    let right = right
        .aggregate(group_by.clone(), aggr_expr)?
        .project(group_by)?
        .alias(r_alias.as_str())?
        .build()?;

    // qualify the join columns for outside the subquery
    // let r_col: Vec<_> = cols
    //     .iter()
    //     .map(|it| &it.1)
    //     .map(|it| {
    //         Column { relation: Some(r_alias.clone()), name: it.clone() }
    //     })
    //     .collect();
    let join_keys = (l_col, r_col);
    let planny = format!("Joining:\n{}\nto:\n{}\non{:?}", right.display_indent(), input.display_indent(), join_keys);

    // join our sub query into the main plan
    let new_plan = LogicalPlanBuilder::from((**input).clone())
        .join(&right, JoinType::Inner, join_keys, None)?;
    let new_plan = if let Some(expr) = combine_filters(outer_others) {
        new_plan.filter(expr)? // if the main query had additional expressions, restore them
    } else {
        new_plan
    };

    // restore conditions
    match subquery_expr {
        Expr::BinaryExpr { left, op: _, right } => {
            match &**right {
                Expr::ScalarSubquery(subquery) => {
                    // TODO: put aggregate projections back
                    // TODO: replace RHS of expr with alias to aggregated field
                    return Ok(plan.clone()) // TODO: the work
                }
                _ => {}
            }
        }
        _ => return Ok(plan.clone()) // TODO: panic or something
    }

    new_plan.build()
}
