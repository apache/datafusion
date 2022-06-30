use crate::{utils, OptimizerConfig, OptimizerRule};
use datafusion_common::Column;
use datafusion_expr::logical_plan::{Filter, JoinType, Subquery};
use datafusion_expr::{combine_filters, Expr, LogicalPlan, LogicalPlanBuilder, Operator};
use hashbrown::HashSet;
use itertools::{Either, Itertools};
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
                let fields: HashSet<_> = plan
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect();
                println!("{:?}", fields);
                match predicate {
                    // TODO: arbitrary expressions
                    Expr::Exists { subquery, negated } => {
                        if *negated {
                            return Ok(plan.clone());
                        }
                        optimize_exists(plan, subquery, input)
                    }
                    _ => Ok(plan.clone()),
                }
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

    // get names of fields TODO: Must fully qualify these!
    let fields: HashSet<_> = sub_input
        .schema()
        .fields()
        .iter()
        .map(|f| f.name())
        .collect();
    println!("{:?}", fields);

    // Grab column names to join on
    let (cols, others) = find_join_exprs(filters, &fields);
    if cols.is_empty() {
        return Ok(plan.clone()); // no joins found
    }

    // Only operate if one column is present and the other closed upon from outside scope
    let l_col: Vec<_> = cols
        .iter()
        .map(|it| &it.0)
        .map(|it| Column::from_qualified_name(it.as_str()))
        .collect();
    let r_col: Vec<_> = cols
        .iter()
        .map(|it| &it.1)
        .map(|it| Column::from_qualified_name(it.as_str()))
        .collect();
    let expr: Vec<_> = r_col.iter().map(|it| Expr::Column(it.clone())).collect();
    let aggr_expr: Vec<Expr> = vec![];
    let join_keys = (l_col, r_col);
    let right = LogicalPlanBuilder::from((*filter.input).clone());
    let right = if let Some(expr) = combine_filters(&others) {
        right.filter(expr)?
    } else {
        right
    };
    let right = right
        .aggregate(expr.clone(), aggr_expr)?
        .project(expr)?
        .build()?;
    println!("Joining:\n{}\nto:\n{}\non:\n{:?}", right.display_indent(), input.display_indent(), join_keys);
    let new_plan = LogicalPlanBuilder::from((**input).clone())
        .join(&right, JoinType::Inner, join_keys, None)?
        .build();
    if let Err(e) = &new_plan {
        println!("wtf");
    }
    // println!("Optimized:\n{}\n\ninto:\n\n{}", plan.display_indent(), new_plan.display_indent());
    new_plan
}

fn find_join_exprs(
    filters: Vec<&Expr>,
    fields: &HashSet<&String>,
) -> (Vec<(String, String)>, Vec<Expr>) {
    let (joins, others): (Vec<_>, Vec<_>) = filters.iter().partition_map(|filter| {
        let (left, op, right) = match filter {
            Expr::BinaryExpr { left, op, right } => (*left.clone(), *op, *right.clone()),
            _ => return Either::Right((*filter).clone()),
        };
        match op {
            Operator::Eq => {}
            _ => return Either::Right((*filter).clone()),
        }
        let left = match left {
            Expr::Column(c) => c,
            _ => return Either::Right((*filter).clone()),
        };
        let right = match right {
            Expr::Column(c) => c,
            _ => return Either::Right((*filter).clone()),
        };
        if fields.contains(&left.name) && fields.contains(&right.name) {
            return Either::Right((*filter).clone()); // Need one of each
        }
        if !fields.contains(&left.name) && !fields.contains(&right.name) {
            return Either::Right((*filter).clone()); // Need one of each
        }

        let sorted = if fields.contains(&left.name) {
            (right.name, left.name)
        } else {
            (left.name, right.name)
        };

        Either::Left(sorted)
    });

    (joins, others)
}
