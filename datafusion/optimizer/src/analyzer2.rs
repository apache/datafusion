mod count_wildcard_rule;
mod function_rewrite;


// Plan common things about AnalyzerRule, like trait, struct, utils function for all the sub-crate

pub trait AnalyzerRule {
    /// Rewrite `plan`
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan>;

    /// A human readable name for this analyzer rule
    fn name(&self) -> &str;
}