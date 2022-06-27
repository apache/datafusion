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
        optimizer_config: &OptimizerConfig,
    ) -> datafusion_common::Result<LogicalPlan> {
        println!("{}", plan.display_indent());
        return Ok(plan.clone());
    }

    fn name(&self) -> &str {
        "subquery_decorrelate"
    }
}
