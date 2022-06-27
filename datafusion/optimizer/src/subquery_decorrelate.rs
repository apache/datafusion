use crate::{OptimizerConfig, OptimizerRule};
use datafusion_expr::LogicalPlan;

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
        _optimizer_config: &OptimizerConfig,
    ) -> datafusion_common::Result<LogicalPlan> {
        println!("{}", plan.display_indent());
        return Ok(plan.clone());
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
