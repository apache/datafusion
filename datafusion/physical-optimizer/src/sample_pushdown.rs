// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! `SamplePushdown` — push a [`SampleExec`] into the source it sits
//! above.
//!
//! The rule walks the physical plan top-down. For each `SampleExec`
//! it finds, it asks the immediate child via
//! [`ExecutionPlan::try_push_sample`] what to do:
//!
//! * [`SamplePushdownResult::Absorbed`]: the child has incorporated
//!   the sample. Replace the `SampleExec` with the new child.
//! * [`SamplePushdownResult::Passthrough`]: the child is row-preserving
//!   for sampling (filter, projection, coalesce, repartition,
//!   non-fetch sort). Recurse into the child's single input and, if
//!   that recursion succeeds, rebuild the child with the new
//!   grandchild and drop the original `SampleExec`.
//! * [`SamplePushdownResult::Unsupported`]: pushdown stops here.
//!   Today this is a planning error — a generic post-scan filter
//!   exec is a follow-up.
//!
//! Run order: this rule must come *after* the rules that may
//! introduce or rewrite scan nodes (filter pushdown, projection
//! pushdown). It runs before `SanityCheckPlan` so any leftover
//! `SampleExec` produces a clean error.
//!
//! [`SampleExec`]: datafusion_physical_plan::sample::SampleExec
//! [`SamplePushdownResult`]: datafusion_physical_plan::sample_pushdown::SamplePushdownResult

use std::sync::Arc;

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::sample::SampleExec;
use datafusion_physical_plan::sample_pushdown::{SamplePushdownResult, SampleSpec};

use crate::PhysicalOptimizerRule;

/// Optimizer rule that attempts to push every `SampleExec` into the
/// source below it.
#[derive(Default, Debug)]
pub struct SamplePushdown;

impl PhysicalOptimizerRule for SamplePushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|node| {
            let Some(sample) = node.as_ref().downcast_ref::<SampleExec>() else {
                return Ok(Transformed::no(node));
            };
            let spec = sample.spec();
            let child = Arc::clone(sample.input());
            match push_into(child, &spec)? {
                Pushdown::Pushed(new_child) => Ok(Transformed::yes(new_child)),
                Pushdown::Failed(reason) => {
                    datafusion_common::plan_err!(
                        "TABLESAMPLE could not be pushed down: {reason}. \
                         A generic post-scan SampleExec is not yet implemented; \
                         see https://github.com/apache/datafusion/issues/16533"
                    )
                }
            }
        })
        .data()
    }

    fn name(&self) -> &str {
        "SamplePushdown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

enum Pushdown {
    /// Sample fully absorbed; the surrounding `SampleExec` should be
    /// replaced with this node.
    Pushed(Arc<dyn ExecutionPlan>),
    /// Pushdown blocked at some descendant; planning should error.
    Failed(String),
}

fn push_into(node: Arc<dyn ExecutionPlan>, spec: &SampleSpec) -> Result<Pushdown> {
    match Arc::clone(&node).try_push_sample(spec)? {
        SamplePushdownResult::Absorbed { inner } => Ok(Pushdown::Pushed(inner)),
        SamplePushdownResult::Passthrough => {
            // Single-child commute. Multi-child nodes shouldn't return
            // Passthrough — guard defensively.
            let children = node.children();
            if children.len() != 1 {
                return Ok(Pushdown::Failed(format!(
                    "{}: Passthrough returned but node has {} children",
                    node.name(),
                    children.len()
                )));
            }
            let child = Arc::clone(children[0]);
            match push_into(child, spec)? {
                Pushdown::Pushed(new_grandchild) => {
                    let rebuilt = node.with_new_children(vec![new_grandchild])?;
                    Ok(Pushdown::Pushed(rebuilt))
                }
                Pushdown::Failed(reason) => Ok(Pushdown::Failed(reason)),
            }
        }
        SamplePushdownResult::Unsupported { reason } => Ok(Pushdown::Failed(reason)),
    }
}
