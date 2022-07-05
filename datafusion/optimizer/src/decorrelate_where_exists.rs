use std::collections::HashSet;
use crate::{utils, OptimizerConfig, OptimizerRule};
use datafusion_common::{Column};
use datafusion_expr::logical_plan::{Filter, JoinType, Subquery};
use datafusion_expr::{combine_filters, Expr, LogicalPlan, LogicalPlanBuilder};
use itertools::{Either, Itertools};
use std::sync::Arc;
use log::{debug, warn};
use crate::utils::{exprs_to_group_cols, exprs_to_join_cols, find_join_exprs, get_id};

/// Optimizer rule for rewriting subquery filters to joins
#[derive(Default)]
pub struct DecorrelateWhereExists {}

impl DecorrelateWhereExists {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for DecorrelateWhereExists {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &OptimizerConfig,
    ) -> datafusion_common::Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter(Filter { predicate, input: filter_input }) => {
                // Apply optimizer rule to current input
                let optimized_input = self.optimize(filter_input, optimizer_config)?;

                debug!("optimizing exists:\n{}", optimized_input.display_indent());

                let mut filters = vec![];
                utils::split_conjunction(predicate, &mut filters);

                let (subqueries, other_exprs): (Vec<_>, Vec<_>) = filters.iter()
                    .partition_map(|f| {
                        match f {
                            Expr::Exists { subquery, negated } => {
                                Either::Left((subquery.clone(), *negated))
                            }
                            _ => Either::Right((*f).clone())
                        }
                    });
                let optimized_plan = LogicalPlan::Filter(Filter {
                    predicate: predicate.clone(),
                    input: Arc::new(optimized_input),
                });
                if subqueries.is_empty() {
                    // regular filter, no subquery exists clause here
                    return Ok(optimized_plan);
                }

                // iterate through all exists clauses in predicate, turning each into a join
                let mut cur_input = (**filter_input).clone();
                for subquery in subqueries {
                    let (subquery, negated) = subquery;
                    cur_input = optimize_exists(&optimized_plan, &subquery, negated, &cur_input, &other_exprs)?;
                    println!("where optimized:\n{}", cur_input.display_indent());
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
        "decorrelate_where_exists"
    }
}

/// Takes a query like:
///
/// ```select c.id from customers c where exists (select * from orders o where o.c_id = c.id)```
///
/// and optimizes it into:
///
/// ```select c.id from customers c
/// inner join (select o.c_id from orders o group by o.c_id) o on o.c_id = c.c_id```
///
/// # Arguments
///
/// * filter_plan - The logical plan for the filter containing the `where exists` clause
/// * subqry - The subquery portion of the `where exists` (select * from orders)
/// * negated - True if the subquery is a `where not exists`
/// * filter_input - The non-subquery portion (from customers)
/// * other_exprs - Any additional parts to the `where` expression (and c.x = y)
fn optimize_exists(
    filter_plan: &LogicalPlan,
    subqry: &Subquery,
    negated: bool,
    filter_input: &LogicalPlan,
    other_filter_exprs: &[Expr],
) -> datafusion_common::Result<LogicalPlan> {
    // Only operate if there is one input
    let subqry_inputs = subqry.subquery.inputs();
    let subqry_input = match subqry_inputs.as_slice() {
        [it] => it,
        _ => {
            warn!("Filter with multiple inputs during where exists!");
            return Ok(filter_plan.clone()) // where exists is a filter, not a join, so 1 input only
        }
    };

    // Only operate on subqueries that are trying to filter on an expression from an outer query
    let subqry_filter = match subqry_input {
        LogicalPlan::Filter(f) => f,
        _ => return Ok(filter_plan.clone()) // not correlated
    };

    // split into filters
    let mut subqry_filter_exprs = vec![];
    utils::split_conjunction(&subqry_filter.predicate, &mut subqry_filter_exprs);

    // get names of fields
    let subqry_fields: HashSet<_> = subqry_filter.input.schema().fields().iter()
        .map(|it| it.qualified_name()).collect();
    debug!("exists fields {:?}", subqry_fields);

    // Grab column names to join on
    let (col_exprs, other_subqry_exprs) = find_join_exprs(subqry_filter_exprs, &subqry_fields);
    let (group_cols, _) = exprs_to_group_cols(&col_exprs, &subqry_fields)?;
    let (col_exprs, join_filters) = exprs_to_join_cols(&col_exprs, &subqry_fields)?;
    let (subqry_cols, filter_input_cols) = col_exprs;
    if subqry_cols.is_empty() || filter_input_cols.is_empty() {
        return Ok(filter_plan.clone()); // not correlated
    }

    // Only operate if one column is present and the other closed upon from outside scope
    let subqry_alias = format!("__sq_{}", get_id());
    let group_by: Vec<_> = group_cols.iter().map(|it| Expr::Column(it.clone())).collect();
    let aggr_expr: Vec<Expr> = vec![];

    // build subqry side of join - the thing the subquery was querying
    let subqry_plan = LogicalPlanBuilder::from((*subqry_filter.input).clone());
    let subqry_plan = if let Some(expr) = combine_filters(&other_subqry_exprs) {
        subqry_plan.filter(expr)? // if the subquery had additional expressions, restore them
    } else {
        subqry_plan
    };
    debug!("Aggregating\n{}\non\n{:?}", subqry_plan.build()?.display_indent(), group_by);
    let subqry_plan = subqry_plan
        .aggregate(group_by.clone(), aggr_expr)?
        .project(group_by)?
        // .project_with_alias(group_by, Some(subqry_alias.clone()))? // TODO: put back
        .build()?;

    // qualify the join columns for outside the subquery
    // TODO: put back
    // let subqry_cols: Vec<_> = subqry_cols.iter().map(|it| {
    //         Column { relation: Some(subqry_alias.clone()), name: it.name.clone() }
    //     }).collect();
    let join_keys = (filter_input_cols, subqry_cols);
    debug!("Exists Joining:\n{}\nto:\n{}\non{:?}", subqry_plan.display_indent(), filter_input.display_indent(), join_keys);

    // join our sub query into the main plan
    let new_plan = LogicalPlanBuilder::from(filter_input.clone());
    let new_plan = if negated {
        new_plan.join(&subqry_plan, JoinType::Anti, join_keys, join_filters)?
    } else {
        new_plan.join(&subqry_plan, JoinType::Semi, join_keys, join_filters)?
    };
    let new_plan = if let Some(expr) = combine_filters(other_filter_exprs) {
        new_plan.filter(expr)? // if the main query had additional expressions, restore them
    } else {
        new_plan
    };

    let result = new_plan.build()?;
    println!("exists result:\n{}", result.display_indent());
    Ok(result)
}
