use crate::PhysicalOptimizerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::Result;
use datafusion_physical_plan::execution_plan::EmissionType;
use datafusion_physical_plan::yield_stream::YieldStreamExec;
use datafusion_physical_plan::ExecutionPlan;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// `WrapLeaves` is a `PhysicalOptimizerRule` that traverses a physical plan
/// and, for every operator whose `emission_type` is `Final`, wraps its direct
/// children inside a `YieldStreamExec`. This ensures that pipeline‐breaking
/// operators (i.e. those with `Final` emission) have a “yield point” immediately
/// upstream, without having to wait until the leaves.
pub struct WrapLeaves {}

impl WrapLeaves {
    /// Create a new instance of the WrapLeaves rule.
    pub fn new() -> Self {
        Self {}
    }

    /// Recursively walk the plan:
    /// - If `plan.children()` is empty, return it unchanged.
    /// - If `plan.properties().emission_type == EmissionType::Final`, wrap each
    ///   direct child in `YieldStreamExec`, then recurse into that wrapper.
    /// - Otherwise, recurse into the children normally (without wrapping).
    #[allow(clippy::only_used_in_recursion)]
    fn wrap_recursive(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let children = plan.children();
        if children.is_empty() {
            // Leaf: no changes
            return Ok(plan);
        }

        // Recurse into children depending on emission_type
        if plan.properties().emission_type == EmissionType::Final {
            // For Final‐emission nodes, wrap each direct child in YieldStreamExec
            let mut new_children = Vec::with_capacity(children.len());
            for child in children {
                // Wrap the immediate child
                let wrapped_child = Arc::new(YieldStreamExec::new(Arc::clone(child)));
                // Then recurse into the wrapped child, in case there are deeper Final nodes
                let rec_wrapped = self.wrap_recursive(wrapped_child)?;
                new_children.push(rec_wrapped);
            }
            // Rebuild this node with its newly wrapped children
            let new_plan = plan.with_new_children(new_children)?;
            Ok(new_plan)
        } else {
            // Non‐Final: just recurse into children normally
            let mut new_children = Vec::with_capacity(children.len());
            for child in children {
                let rec_wrapped = self.wrap_recursive(Arc::clone(child))?;
                new_children.push(rec_wrapped);
            }
            let new_plan = plan.with_new_children(new_children)?;
            Ok(new_plan)
        }
    }
}

impl Default for WrapLeaves {
    fn default() -> Self {
        Self::new()
    }
}

impl Debug for WrapLeaves {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WrapLeaves").finish()
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

    /// Since we only add `YieldStreamExec` wrappers (which preserve schema), schema_check remains true.
    fn schema_check(&self) -> bool {
        true
    }
}
