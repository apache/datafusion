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

//! Physical [`SampleExec`] node — the lowering target of a logical
//! `Sample` extension. The optimizer rule `SamplePushdown` walks the
//! plan and pushes the sampling into the source (or through transparent
//! intermediate nodes) where possible.
//!
//! Today, executing a `SampleExec` directly returns a `not implemented`
//! error: the rule must successfully push it down for the query to run.
//! When a generic post-scan filter implementation lands, this will
//! become the default fallback for plan shapes pushdown can't handle.

use std::any::Any;
use std::fmt::{self, Formatter};
use std::sync::Arc;

use crate::execution_plan::ExecutionPlanProperties;
use crate::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase,
    FilterPushdownPropagation,
};
use crate::sample_pushdown::{SampleMethod, SampleSpec};
use crate::sort_pushdown::SortOrderPushdownResult;
use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::not_impl_err;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{PhysicalExpr, PhysicalSortExpr};

/// Passthrough physical node carrying TABLESAMPLE intent.
///
/// Output schema, partitioning, and ordering are inherited unchanged
/// from the input — sampling drops rows but nothing else.
#[derive(Debug, Clone)]
pub struct SampleExec {
    input: Arc<dyn ExecutionPlan>,
    method: SampleMethod,
    fraction: f64,
    seed: Option<u64>,
    cache: Arc<PlanProperties>,
}

impl SampleExec {
    /// Construct a [`SampleExec`] over `input`. Caller is expected to
    /// have validated `fraction` upstream (the logical `Sample` node
    /// does this in `try_new`).
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        method: SampleMethod,
        fraction: f64,
        seed: Option<u64>,
    ) -> Self {
        let cache = Arc::new(Self::compute_properties(&input));
        Self {
            input,
            method,
            fraction,
            seed,
            cache,
        }
    }

    /// Convenience: construct from a [`SampleSpec`].
    pub fn from_spec(input: Arc<dyn ExecutionPlan>, spec: &SampleSpec) -> Self {
        Self::new(input, spec.method, spec.fraction, spec.seed)
    }

    /// Sampling method.
    pub fn method(&self) -> SampleMethod {
        self.method
    }
    /// Target fraction in `(0.0, 1.0]`.
    pub fn fraction(&self) -> f64 {
        self.fraction
    }
    /// Optional `REPEATABLE(seed)`.
    pub fn seed(&self) -> Option<u64> {
        self.seed
    }
    /// Input plan.
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
    /// The [`SampleSpec`] this node represents.
    pub fn spec(&self) -> SampleSpec {
        SampleSpec {
            method: self.method,
            fraction: self.fraction,
            seed: self.seed,
        }
    }

    fn compute_properties(input: &Arc<dyn ExecutionPlan>) -> PlanProperties {
        // Sampling preserves equivalence properties (subset of rows
        // satisfies the same equivalences) and partitioning/ordering.
        // Bounded if the input is bounded; otherwise sampling
        // semantics over an unbounded stream aren't well-defined here
        // and pushdown will reject it before we reach execution.
        PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        )
    }
}

impl DisplayAs for SampleExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let method = match self.method {
                    SampleMethod::System => "SYSTEM",
                };
                write!(f, "SampleExec: {method}({:.4}%)", self.fraction * 100.0)?;
                if let Some(seed) = self.seed {
                    write!(f, " REPEATABLE({seed})")?;
                }
                Ok(())
            }
            DisplayFormatType::TreeRender => write!(f, "SampleExec"),
        }
    }
}

impl ExecutionPlan for SampleExec {
    fn name(&self) -> &'static str {
        "SampleExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion_common::DataFusionError::Internal(format!(
                "SampleExec wrong number of children: {}",
                children.len()
            )));
        }
        let mut child = children;
        Ok(Arc::new(SampleExec::new(
            child.swap_remove(0),
            self.method,
            self.fraction,
            self.seed,
        )))
    }

    // ---- Make SampleExec transparent to other pushdown rules so they ----
    // ---- aren't blocked when a Sample sits between them and the source. ----
    //
    // Filter pushdown: `sample(filter(x))` ≡ `filter(sample(x))`, so any
    // filters the parent wants to push past us can pass through.

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(FilterPushdownPropagation::if_all(child_pushdown_result))
    }

    // Sort pushdown: sampling preserves ordering, so a sort requirement
    // can pass through us. We'd never get a `SortExec(SampleExec(..))`
    // shape in practice (sort + sample is unusual), but providing this
    // keeps the rule from giving up unnecessarily.
    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        self.input.try_pushdown_sort(order)?.try_map(|new_input| {
            Ok(Arc::new(SampleExec::new(
                new_input,
                self.method,
                self.fraction,
                self.seed,
            )) as Arc<dyn ExecutionPlan>)
        })
    }

    // Limit pushdown: deliberately NOT supported. `LIMIT(SAMPLE(x))`
    // (sample first, then take first N) is not the same as
    // `SAMPLE(LIMIT(x))` (take first N, then sample). Default impl
    // returns false; we leave it explicit here for documentation.

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // The pushdown rule should have either (a) absorbed this node
        // into the source or (b) emitted a planning error. If we
        // reach `execute()`, neither happened — that's a planner bug
        // until a generic SampleExec filter lands.
        not_impl_err!(
            "SampleExec could not be pushed into the source and a generic \
             post-scan sample filter is not yet implemented; see \
             https://github.com/apache/datafusion/issues/16533"
        )
    }
}

impl SampleExec {
    /// Allow downstream code (mostly the pushdown rule) to treat
    /// `&dyn ExecutionPlan` as `Self` ergonomically.
    pub fn downcast_arc(plan: &Arc<dyn ExecutionPlan>) -> Option<&SampleExec> {
        plan.as_ref().downcast_ref::<SampleExec>()
    }

    /// `as_any` companion for `dyn ExecutionPlan`.
    pub fn as_any_ref(&self) -> &dyn Any {
        self
    }
}
