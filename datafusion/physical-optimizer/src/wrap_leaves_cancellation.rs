use crate::PhysicalOptimizerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::Result;
use datafusion_physical_plan::yield_stream::YieldStreamExec;
use datafusion_physical_plan::ExecutionPlan;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// `WrapLeaves` is a `PhysicalOptimizerRule` that traverses a physical plan
/// and wraps every leaf node (i.e., an `ExecutionPlan` with no children)
/// inside a `YieldStreamExec`. This ensures that long-running leaf operators
/// periodically yield back to the executor and participate in cancellation checks.
pub struct WrapLeaves {}

impl WrapLeaves {
    /// Create a new instance of the WrapLeaves rule.
    pub fn new() -> Self {
        Self {}
    }

    /// Recursively walk the plan:
    /// - If `plan.children_any().is_empty()`, it’s a leaf, so wrap it.
    /// - Otherwise, recurse into its children, rebuild the node with
    ///   `with_new_children_any(...)`, and return that.
    fn wrap_recursive(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let children = plan.children();
        if children.is_empty() {
            // Leaf node: wrap it in `YieldStreamExec`
            let wrapped = Arc::new(YieldStreamExec::new(plan.clone()));
            Ok(wrapped)
        } else {
            // Non-leaf: first process all children recursively
            let mut new_children = Vec::with_capacity(children.len());
            for child in children {
                let wrapped_child = self.wrap_recursive(child.clone())?;
                new_children.push(wrapped_child);
            }
            // Rebuild this node with the new children
            let new_plan = plan.with_new_children(new_children)?;
            Ok(new_plan)
        }
    }
}

impl Debug for WrapLeaves {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl PhysicalOptimizerRule for WrapLeaves {
    fn name(&self) -> &str {
        "wrap_leaves"
    }

    /// Apply the rule by calling `wrap_recursive` on the root plan.
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.wrap_recursive(plan)
    }

    /// Wrapping leaves does not change the schema, so this remains true.
    fn schema_check(&self) -> bool {
        true
    }
}
