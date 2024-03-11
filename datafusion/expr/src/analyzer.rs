use std::sync::Arc;

/// [`AnalyzerRule`]s transform [`LogicalPlan`]s in some way to make
/// the plan valid prior to the rest of the DataFusion optimization process.
///
/// This is different than an [`OptimizerRule`](crate::OptimizerRule)
/// which must preserve the semantics of the `LogicalPlan`, while computing
/// results in a more optimal way.
///
/// For example, an `AnalyzerRule` may resolve [`Expr`]s into more specific
/// forms such as a subquery reference, or do type coercion to ensure the types
/// of operands are correct.
///
/// Use [`SessionState::add_analyzer_rule`] to register additional
/// `AnalyzerRule`s.
///
/// [`SessionState::add_analyzer_rule`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionState.html#method.add_analyzer_rule
use datafusion_common::config::ConfigOptions;
use datafusion_common::Result;

use crate::LogicalPlan;

pub trait AnalyzerRule {
    /// Rewrite `plan`
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan>;

    /// A human readable name for this analyzer rule
    fn name(&self) -> &str;
}

pub type AnalyzerRuleRef = Arc<dyn AnalyzerRule + Send + Sync>;
