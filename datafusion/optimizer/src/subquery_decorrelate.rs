use crate::{utils, OptimizerConfig, OptimizerRule};
use datafusion_common::Column;
use datafusion_expr::logical_plan::{Filter, JoinType, Subquery};
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
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
                    // TODO: arbitrary expression trees, Expr::InSubQuery
                    Expr::Exists {
                        subquery,
                        negated: _,
                    } => optimize_exists(plan, subquery, input),
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

    // Only operate on a single binary expression (for now)
    let (left, _op, right) =
        if let Expr::BinaryExpr { left, op, right } = &filter.predicate {
            (left, op, right)
        } else {
            return Ok(plan.clone());
        };

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
        .aggregate(group_expr, aggr_expr)
        .unwrap()
        .project(expr)
        .unwrap()
        .build()
        .unwrap();
    return LogicalPlanBuilder::from((**input).clone())
        .join(&right, JoinType::Inner, join_keys, None)
        .unwrap()
        .build();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use datafusion_expr::{col, in_subquery, logical_plan::LogicalPlanBuilder};
    use std::sync::Arc;

    #[test]
    fn in_subquery_simple() -> datafusion_common::Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(in_subquery(col("c"), test_subquery_with_name("sq")?))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: #test.b [b:UInt32]\
        \n  Semi Join: #test.c = #sq.c [a:UInt32, b:UInt32, c:UInt32]\
        \n    TableScan: test projection=None [a:UInt32, b:UInt32, c:UInt32]\
        \n    Projection: #sq.c [c:UInt32]\
        \n      TableScan: sq projection=None [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    // TODO: deduplicate with subquery_filter_to_join
    fn test_subquery_with_name(
        name: &str,
    ) -> datafusion_common::Result<Arc<LogicalPlan>> {
        let table_scan = test_table_scan_with_name(name)?;
        Ok(Arc::new(
            LogicalPlanBuilder::from(table_scan)
                .project(vec![col("c")])?
                .build()?,
        ))
    }

    // TODO: deduplicate with subquery_filter_to_join
    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let rule = SubqueryDecorrelate::new();
        let optimized_plan = rule
            .optimize(plan, &OptimizerConfig::new())
            .expect("failed to optimize plan");
        let formatted_plan = format!("{}", optimized_plan.display_indent_schema());
        assert_eq!(formatted_plan, expected);
    }
}
