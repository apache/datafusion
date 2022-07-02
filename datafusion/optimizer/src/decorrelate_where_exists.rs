use std::collections::HashSet;
use crate::{utils, OptimizerConfig, OptimizerRule};
use datafusion_common::{Column, ScalarValue};
use datafusion_expr::logical_plan::{Filter, JoinType, Subquery};
use datafusion_expr::{combine_filters, Expr, LogicalPlan, LogicalPlanBuilder, Operator};
use itertools::{Either, Itertools};
use std::sync::Arc;
use datafusion_expr::Expr::BinaryExpr;
use crate::utils::{find_join_exprs, get_id};

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
            LogicalPlan::Filter(Filter { predicate, input }) => {
                let mut filters = vec![];
                utils::split_conjunction(predicate, &mut filters);

                let (subqueries, others): (Vec<_>, Vec<_>) = filters.iter()
                    .partition_map(|f| {
                        match f {
                            Expr::Exists { subquery, negated } => {
                                if *negated { // TODO: not exists
                                    Either::Left((subquery.clone(), *negated))
                                } else {
                                    Either::Left((subquery.clone(), *negated))
                                }
                            }
                            _ => Either::Right((*f).clone())
                        }
                    });
                let (subquery, negated) = match subqueries.as_slice() {
                    [it] => it,
                    _ => return Ok(plan.clone()) // TODO: >1 subquery
                };

                optimize_exists(plan, subquery, input, &others, *negated)
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

/*
#orders.o_orderpriority ASC NULLS LAST
    Projection: #orders.o_orderpriority, #COUNT(UInt8(1)) AS order_count
        Aggregate: groupBy=[[#orders.o_orderpriority]], aggr=[[COUNT(UInt8(1))]]
            Filter: EXISTS (                                                         -- plan
                Subquery: Projection: *                                              -- proj
                    Filter: #lineitem.l_orderkey = #orders.o_orderkey                -- filter
                        TableScan: lineitem projection=None                          -- filter.input
            )
                TableScan: orders projection=None                                    -- plan.inputs
             */

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
    outer_others: &[Expr],
    negated: bool,
) -> datafusion_common::Result<LogicalPlan> {
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
        LogicalPlan::Filter(f) => f,
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
    let right = LogicalPlanBuilder::from((*filter.input).clone());
    let right = if let Some(expr) = combine_filters(&others) {
        right.filter(expr)? // if the subquery had additional expressions, restore them
    } else {
        right
    };
    let right = right
        .aggregate(group_by.clone(), aggr_expr)?
        .project_with_alias(group_by, Some(r_alias.clone()))?
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

    // negate or not
    let mut filter = Expr::Literal(ScalarValue::Boolean(Some(true)));
    for col in &join_keys.1 {
        let expr = if negated {
            Expr::IsNull(Box::new(Expr::Column(col.clone())))
        } else {
            Expr::IsNotNull(Box::new(Expr::Column(col.clone())))
        };
        filter = BinaryExpr {
            left: Box::new(filter),
            op: Operator::And,
            right: Box::new(expr)
        }
    }

    // join our sub query into the main plan
    let new_plan = LogicalPlanBuilder::from((**input).clone())
        .join(&right, JoinType::Left, join_keys, None)?
        .filter(filter)?;
    let new_plan = if let Some(expr) = combine_filters(outer_others) {
        new_plan.filter(expr)? // if the main query had additional expressions, restore them
    } else {
        new_plan
    };

    new_plan.build()
}
