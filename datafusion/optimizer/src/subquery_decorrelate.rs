use std::hash::Hash;
use std::sync::Arc;
use hashbrown::HashSet;
use datafusion_expr::logical_plan::Filter;
use crate::{OptimizerConfig, OptimizerRule, utils};
use datafusion_expr::{Expr, LogicalPlan};

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
                // Apply optimizer rule to current input
                let optimized_input = self.optimize(input, optimizer_config)?;

                for input in plan.inputs() {
                    println!("{}", input.display_indent());
                }

                match predicate {
                    // TODO: arbitrary expression trees, Expr::InSubQuery
                    Expr::Exists { subquery,negated } => {
                        let text = format!("{:?}", subquery);
                        match &*subquery.subquery {
                            LogicalPlan::Projection(proj) => {
                                println!("proj");
                            }
                            _ => return Ok(plan.clone())
                        }
                        for input in subquery.subquery.inputs() {
                            match input {
                                LogicalPlan::Filter(filter) => {
                                    match &filter.predicate {
                                        Expr::BinaryExpr { left, op, right } => {
                                            let lcol = match &**left {
                                                Expr::Column(col) => col,
                                                _ => return Ok(plan.clone())
                                            };
                                            let rcol = match &**right {
                                                Expr::Column(col) => col,
                                                _ => return Ok(plan.clone())
                                            };
                                            let cols = vec![lcol, rcol];
                                            let cols: HashSet<_> = cols.iter().map(|c| &c.name).collect();
                                            let fields: HashSet<_> = input.schema().fields().iter().map(|f| f.name()).collect();

                                            let found: Vec<_> = cols.intersection(&fields).map(|it| (*it).clone()).collect();
                                            let closed_upon: Vec<_> = cols.difference(&fields).map(|it| (*it).clone()).collect();

                                            println!("{:?} {:?}", found, closed_upon);
                                        },
                                        _ => return Ok(plan.clone())
                                    }
                                },
                                _ => return Ok(plan.clone())
                            }
                        }
                    },
                    _ => return Ok(plan.clone())
                }

                return Ok(plan.clone())
            },
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use super::*;
    use crate::test::*;
    use datafusion_expr::{
        col, in_subquery, logical_plan::LogicalPlanBuilder,
    };

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
    fn test_subquery_with_name(name: &str) -> datafusion_common::Result<Arc<LogicalPlan>> {
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
