use crate::utils::{exprs_to_join_cols, find_join_exprs, split_conjunction};
use crate::{utils, OptimizerConfig, OptimizerRule};
use datafusion_common::{Column, DataFusionError};
use datafusion_expr::logical_plan::{Filter, JoinType, Subquery};
use datafusion_expr::{combine_filters, Expr, LogicalPlan, LogicalPlanBuilder};
use std::collections::HashSet;
use std::sync::Arc;
use itertools::{Either, Itertools};
use log::debug;

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
        optimizer_config: &mut OptimizerConfig,
    ) -> datafusion_common::Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter(Filter { predicate, input }) => {
                // Apply optimizer rule to current input
                let optimized_input = self.optimize(input, optimizer_config)?;

                let (subqueries, other_exprs) = extract_subquery_exprs(predicate);
                let optimized_plan = LogicalPlan::Filter(Filter {
                    predicate: predicate.clone(),
                    input: Arc::new(optimized_input),
                });
                if subqueries.is_empty() {
                    // regular filter, no subquery exists clause here
                    return Ok(optimized_plan);
                }

                // iterate through all exists clauses in predicate, turning each into a join
                let mut cur_input = (**input).clone();
                for subquery in subqueries {
                    let res = optimize_scalar(
                        &subquery,
                        &cur_input,
                        &other_exprs,
                        optimizer_config,
                    )?;
                    if let Some(res) = res {
                        cur_input = res
                    }
                }
                Ok(cur_input)
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
/// * `subqry` - The subquery portion of the `where exists` (select * from orders)
/// * `negated` - True if the subquery is a `where not exists`
/// * `filter_input` - The non-subquery portion (from customers)
/// * `other_filter_exprs` - Any additional parts to the `where` expression (and c.x = y)
/// * `optimizer_config` - Used to generate unique subquery aliases
fn optimize_scalar(
    subqry: &Expr,
    filter_input: &LogicalPlan,
    other_filter_exprs: &[Expr],
    optimizer_config: &mut OptimizerConfig,
) -> datafusion_common::Result<Option<LogicalPlan>> {
    let subqueries = extract_subqueries(subqry);
    let subquery = match subqueries.as_slice() {
        [it] => it,
        _ => return Ok(None), // only one subquery per conjugated expression for now
    };

    // Scalar subqueries should be projecting a single value, grab and alias it
    let proj = match &*subquery.subquery {
        LogicalPlan::Projection(it) => it,
        _ => return Ok(None), // should be projecting something
    };
    let proj = match proj.expr.as_slice() {
        [it] => it,
        _ => return Ok(None), // scalar subquery means only 1 expr
    };
    let proj = Expr::Alias(Box::new(proj.clone()), "__value".to_string());

    // Only operate if there is one input
    let sub_inputs = subquery.subquery.inputs();
    let sub_input = match sub_inputs.as_slice() {
        [it] => it,
        _ => return Ok(None), // shouldn't be a join (>1 input)
    };

    // Scalar subqueries should be aggregating a value
    let aggr = match sub_input {
        LogicalPlan::Aggregate(a) => a,
        _ => return Ok(None),
    };
    let filter = match &*aggr.input {
        LogicalPlan::Filter(f) => f,
        _ => return Ok(None), // Not correlated - TODO: also handle this case
    };

    // split into filters
    let mut subqry_filter_exprs = vec![];
    split_conjunction(&filter.predicate, &mut subqry_filter_exprs);

    // get names of fields
    let subqry_fields: HashSet<_> = filter.input.schema().fields().iter()
        .map(|f| f.qualified_name()).collect();
    debug!("Scalar subquery fields: {:?}", subqry_fields);

    // Grab column names to join on
    let (col_exprs, other_subqry_exprs) =
        find_join_exprs(subqry_filter_exprs, &subqry_fields);
    let (col_exprs, join_filters) = exprs_to_join_cols(&col_exprs, &subqry_fields, false)?;
    if join_filters.is_some() {
        return Ok(None); // non-column join expressions not yet supported
    }
    let (outer_cols, subqry_cols) = col_exprs;

    // Only operate if one column is present and the other closed upon from outside scope
    let subqry_alias = format!("__sq_{}", optimizer_config.next_id());
    let group_by: Vec<_> = subqry_cols.iter()
        .map(|it| Expr::Column(it.clone())).collect();

    // build subquery side of join - the thing the subquery was querying
    let subqry_plan = LogicalPlanBuilder::from((*filter.input).clone());
    let subqry_plan = if let Some(expr) = combine_filters(&other_subqry_exprs) {
        subqry_plan.filter(expr)? // if the subquery had additional expressions, restore them
    } else {
        subqry_plan
    };

    // project the prior projection + any correlated (and now grouped) columns
    let proj: Vec<_> = group_by.iter().cloned()
        .chain(vec![proj].iter().cloned()).collect();
    let subqry_plan = subqry_plan
        .aggregate(group_by.clone(), aggr.aggr_expr.clone())?
        .project_with_alias(proj, Some(subqry_alias.clone()))?
        .build()?;

    // qualify the join columns for outside the subquery
    let subqry_cols: Vec<_> = subqry_cols
        .iter()
        .map(|it| Column {
            relation: Some(subqry_alias.clone()),
            name: it.name.clone(),
        })
        .collect();
    let join_keys = (outer_cols, subqry_cols);

    // join our sub query into the main plan
    let new_plan = LogicalPlanBuilder::from(filter_input.clone());
    let new_plan = if join_keys.0.len() > 0 {
        // inner join if correlated, grouping by the join keys so we don't change row count
        new_plan.join(&subqry_plan, JoinType::Inner, join_keys, None)?
    } else {
        // if not correlated, group down to 1 row and cross join on that (preserving row count)
        new_plan.cross_join(&subqry_plan)?
    };

    // if the main query had additional expressions, restore them
    let new_plan = if let Some(expr) = combine_filters(other_filter_exprs) {
        new_plan.filter(expr)?
    } else {
        new_plan
    };

    // restore conditions
    let new_plan = match subqry {
        Expr::BinaryExpr { left, op, right } => match &**right {
            Expr::ScalarSubquery(_) => {
                let right = Box::new(Expr::Column(Column {
                    relation: Some(subqry_alias),
                    name: "__value".to_string(),
                }));
                let expr = Expr::BinaryExpr {
                    left: left.clone(),
                    op: op.clone(),
                    right,
                };
                new_plan.filter(expr)?
            }
            _ => return Err(DataFusionError::Plan("Not a scalar subquery!".to_string())),
        },
        _ => return Err(DataFusionError::Plan("Not a scalar subquery!".to_string())),
    };

    let new_plan = new_plan.build()?;
    Ok(Some(new_plan))
}

/// Finds expressions that have a scalar subquery in them
///
/// # Arguments
///
/// * `predicate` - A conjunction to split and search
///
/// Returns a tuple of (subquery expressions, remaining expressions)
pub fn extract_subquery_exprs(predicate: &Expr) -> (Vec<Expr>, Vec<Expr>) {
    let mut filters = vec![];
    split_conjunction(predicate, &mut filters); // TODO: disjunctions

    let (subqueries, others): (Vec<_>, Vec<_>) =
        filters.iter().partition_map(|f| {
        match f {
            Expr::BinaryExpr { left, op: _, right } => {
                let l_query = match &**left {
                    Expr::ScalarSubquery(subquery) => Some(subquery.clone()),
                    _ => None,
                };
                let r_query = match &**right {
                    Expr::ScalarSubquery(subquery) => Some(subquery.clone()),
                    _ => None,
                };
                if l_query.is_some() && r_query.is_some() {
                    return Either::Right((*f).clone()); // TODO: (subquery A) = (subquery B)
                }
                match l_query {
                    Some(_) => return Either::Left((*f).clone()),
                    _ => {}
                }
                match r_query {
                    Some(_) => return Either::Left((*f).clone()),
                    _ => {}
                }
                Either::Right((*f).clone())
            }
            _ => Either::Right((*f).clone()),
        }
    });
    (subqueries, others)
}

/// Extract any scalar subquery expressions from a conjunction
pub fn extract_subqueries(predicate: &Expr) -> Vec<Subquery> {
    let mut filters = vec![];
    split_conjunction(predicate, &mut filters); // TODO: disjunction

    let subqueries = filters.iter().fold(vec![], |mut acc, expr| {
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
