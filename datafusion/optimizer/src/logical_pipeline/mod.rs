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

//! [`LogicalPlanningPipeline`]: an ordered sequence of named [`Phase`]s that
//! transform a [`LogicalPlan`] before physical planning.
//!
//! Inspired by Spark SQL's `Batch(name, strategy, rules)` model.

mod async_phase;
mod pipeline;
mod sync_phase;

pub use async_phase::{AsyncAnalysisPhase, AsyncOptimizationPhase, AsyncPhase};
pub use pipeline::LogicalPlanningPipeline;
pub use sync_phase::{SyncAnalysisPhase, SyncOptimizationPhase, SyncPhase};

/// Name of the default synchronous analysis phase.
pub const DEFAULT_ANALYSIS_PHASE: &str = "analysis";

/// Name of the default synchronous optimization phase.
pub const DEFAULT_OPTIMIZATION_PHASE: &str = "optimization";

/// How many times a phase's rules are applied.
///
/// # Choosing a strategy
///
/// Use [`Strategy::Once`] when rules are idempotent and a single pass always
/// suffices (e.g. a post-processing cleanup that must run exactly one time).
///
/// Use [`Strategy::FixedPoint`] when rules interact: one rule's output may
/// unlock another rule's pattern. The phase repeats until the plan signature
/// stops changing or the pass limit is hit. This is the correct choice for
/// both the default analysis and optimization phases.
///
/// Note: `FixedPoint { max_passes: Some(1) }` is mechanically equivalent to
/// `Once` (both execute rules exactly one time), but carries different intent.
/// `Once` means "one pass by design"; `FixedPoint { max_passes: Some(1) }`
/// means "convergence loop, hard-capped at one iteration."
#[derive(Clone, Debug)]
pub enum Strategy {
    /// Apply each rule exactly once. No convergence check is performed.
    Once,
    /// Repeat until the plan stops changing or `max_passes` iterations are
    /// reached. Convergence is detected via [`LogicalPlanSignature`] so the
    /// loop exits early as soon as a full pass produces no change.
    ///
    /// `max_passes: None` defers to `config.optimizer.max_passes` at runtime.
    ///
    /// [`LogicalPlanSignature`]: crate::plan_signature::LogicalPlanSignature
    FixedPoint { max_passes: Option<usize> },
}

/// One step in a [`LogicalPlanningPipeline`].
#[derive(Clone, Debug)]
pub enum Phase {
    SyncAnalysis(SyncAnalysisPhase),
    SyncOptimization(SyncOptimizationPhase),
    AsyncAnalysis(AsyncAnalysisPhase),
    AsyncOptimization(AsyncOptimizationPhase),
}

impl Phase {
    pub fn name(&self) -> &str {
        match self {
            Phase::SyncAnalysis(p) => p.name(),
            Phase::SyncOptimization(p) => p.name(),
            Phase::AsyncAnalysis(p) => p.name(),
            Phase::AsyncOptimization(p) => p.name(),
        }
    }

    pub fn is_async(&self) -> bool {
        matches!(self, Phase::AsyncAnalysis(_) | Phase::AsyncOptimization(_))
    }

    pub fn is_enabled(&self) -> bool {
        match self {
            Phase::SyncAnalysis(p) => p.enabled,
            Phase::SyncOptimization(p) => p.enabled,
            Phase::AsyncAnalysis(p) => p.enabled,
            Phase::AsyncOptimization(p) => p.enabled,
        }
    }
}
