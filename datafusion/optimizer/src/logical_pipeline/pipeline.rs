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

use datafusion_common::display::{PlanType, StringifiedPlan, ToStringifiedPlan};
use datafusion_common::{Result, plan_err};
use datafusion_expr::LogicalPlan;

use crate::optimizer::OptimizerConfig;
use crate::{Analyzer, Optimizer};

use super::sync_phase::{SyncAnalysisPhase, SyncOptimizationPhase};
use super::{DEFAULT_ANALYSIS_PHASE, DEFAULT_OPTIMIZATION_PHASE, Phase, Strategy};

/// An ordered sequence of named [`Phase`]s that transform a [`LogicalPlan`].
///
/// The default pipeline mirrors the existing DataFusion behavior:
/// an analysis phase followed by an optimization phase, both sync.
///
/// Users can inject custom phases (including async ones) at any position.
/// When any enabled async phase is present, use
/// [`LogicalPlanningPipeline::apply`] (requires an async context). The sync
/// [`LogicalPlanningPipeline::apply_sync`] returns an error if an enabled
/// async phase is encountered.
#[derive(Clone, Debug)]
pub struct LogicalPlanningPipeline {
    phases: Vec<Phase>,
}

impl Default for LogicalPlanningPipeline {
    fn default() -> Self {
        let mut analysis = SyncAnalysisPhase::new(
            DEFAULT_ANALYSIS_PHASE,
            Strategy::FixedPoint { max_passes: None },
        );
        for rule in Analyzer::new().rules {
            analysis.rules.push(rule);
        }

        let mut optimization = SyncOptimizationPhase::new(
            DEFAULT_OPTIMIZATION_PHASE,
            Strategy::FixedPoint { max_passes: None },
        );
        for rule in Optimizer::new().rules {
            optimization.rules.push(rule);
        }

        Self::new(vec![
            Phase::SyncAnalysis(analysis),
            Phase::SyncOptimization(optimization),
        ])
    }
}

impl LogicalPlanningPipeline {
    pub fn new(phases: Vec<Phase>) -> Self {
        Self { phases }
    }

    pub fn phases(&self) -> &[Phase] {
        &self.phases
    }

    pub fn phase(&self, name: &str) -> Option<&Phase> {
        self.phases.iter().find(|p| p.name() == name)
    }

    pub fn phase_mut(&mut self, name: &str) -> Option<&mut Phase> {
        self.phases.iter_mut().find(|p| p.name() == name)
    }

    /// Append a phase to the end of the pipeline.
    pub fn push(&mut self, phase: Phase) {
        self.phases.push(phase);
    }

    /// Insert `phase` immediately before the phase named `anchor`.
    ///
    /// Returns `false` if `anchor` is not found; the phase is not inserted.
    /// Use the return value to detect misconfiguration.
    #[must_use]
    pub fn insert_before(&mut self, anchor: &str, phase: Phase) -> bool {
        if let Some(pos) = self.phases.iter().position(|p| p.name() == anchor) {
            self.phases.insert(pos, phase);
            true
        } else {
            false
        }
    }

    /// Insert `phase` immediately after the phase named `anchor`.
    ///
    /// Returns `false` if `anchor` is not found; the phase is not inserted.
    /// Use the return value to detect misconfiguration.
    #[must_use]
    pub fn insert_after(&mut self, anchor: &str, phase: Phase) -> bool {
        if let Some(pos) = self.phases.iter().position(|p| p.name() == anchor) {
            self.phases.insert(pos + 1, phase);
            true
        } else {
            false
        }
    }

    /// Returns `true` if any enabled phase is async.
    pub fn has_async(&self) -> bool {
        self.phases.iter().any(|p| p.is_enabled() && p.is_async())
    }

    /// Run all phases synchronously.
    ///
    /// Returns an error if an enabled async phase is encountered. Callers
    /// that may have async phases registered should use
    /// [`LogicalPlanningPipeline::apply`] from an async context instead.
    pub fn apply_sync(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<LogicalPlan> {
        let config_opts = config.options();
        let mut plan = plan;
        for phase in &self.phases {
            if !phase.is_enabled() {
                continue;
            }
            plan = match phase {
                Phase::SyncAnalysis(p) => p.apply(plan, &config_opts)?,
                Phase::SyncOptimization(p) => p.apply(plan, config)?,
                Phase::AsyncAnalysis(p) => {
                    return plan_err!(
                        "pipeline has async analysis phase '{}'; \
                         use create_physical_plan() instead of optimize()",
                        p.name()
                    );
                }
                Phase::AsyncOptimization(p) => {
                    return plan_err!(
                        "pipeline has async optimization phase '{}'; \
                         use create_physical_plan() instead of optimize()",
                        p.name()
                    );
                }
            };
        }
        Ok(plan)
    }

    /// Run all phases, capturing per-rule intermediate plans for EXPLAIN output.
    ///
    /// Returns `(optimized_plan, stringified_plans)` where `stringified_plans`
    /// contains [`PlanType::AnalyzedLogicalPlan`], [`PlanType::FinalAnalyzedLogicalPlan`],
    /// and [`PlanType::OptimizedLogicalPlan`] entries in application order.
    pub fn apply_sync_explained(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<(LogicalPlan, Vec<StringifiedPlan>)> {
        let config_opts = config.options();
        let mut plan = plan;
        let mut stringified: Vec<StringifiedPlan> = Vec::new();
        let mut pending_final_analyzed = false;

        for phase in &self.phases {
            if !phase.is_enabled() {
                continue;
            }
            match phase {
                Phase::SyncAnalysis(p) => {
                    plan =
                        p.apply_observed(plan, &config_opts, &mut |p, rule_name| {
                            stringified.push(p.to_stringified(
                                PlanType::AnalyzedLogicalPlan {
                                    analyzer_name: rule_name.to_string(),
                                },
                            ));
                        })?;
                    pending_final_analyzed = true;
                }
                Phase::SyncOptimization(p) => {
                    if pending_final_analyzed {
                        stringified.push(
                            plan.to_stringified(PlanType::FinalAnalyzedLogicalPlan),
                        );
                        pending_final_analyzed = false;
                    }
                    plan = p.apply_observed(plan, config, &mut |p, rule_name| {
                        stringified.push(p.to_stringified(
                            PlanType::OptimizedLogicalPlan {
                                optimizer_name: rule_name.to_string(),
                            },
                        ));
                    })?;
                }
                Phase::AsyncAnalysis(p) => {
                    return plan_err!(
                        "pipeline has async analysis phase '{}'; \
                         use create_physical_plan() instead of optimize()",
                        p.name()
                    );
                }
                Phase::AsyncOptimization(p) => {
                    return plan_err!(
                        "pipeline has async optimization phase '{}'; \
                         use create_physical_plan() instead of optimize()",
                        p.name()
                    );
                }
            }
        }
        if pending_final_analyzed {
            stringified.push(plan.to_stringified(PlanType::FinalAnalyzedLogicalPlan));
        }
        Ok((plan, stringified))
    }

    /// Run all phases, awaiting async ones.
    pub async fn apply(
        &self,
        plan: LogicalPlan,
        config: &(dyn OptimizerConfig + Sync),
    ) -> Result<LogicalPlan> {
        let config_opts = config.options();
        let mut plan = plan;
        for phase in &self.phases {
            if !phase.is_enabled() {
                continue;
            }
            plan = match phase {
                Phase::SyncAnalysis(p) => p.apply(plan, &config_opts)?,
                Phase::SyncOptimization(p) => p.apply(plan, config)?,
                Phase::AsyncAnalysis(p) => p.apply(plan, &config_opts).await?,
                Phase::AsyncOptimization(p) => p.apply(plan, &config_opts).await?,
            };
        }
        Ok(plan)
    }

    /// Run all phases (including async), capturing per-rule intermediate plans
    /// for EXPLAIN output.
    ///
    /// Returns `(optimized_plan, stringified_plans)` where `stringified_plans`
    /// contains [`PlanType::AnalyzedLogicalPlan`], [`PlanType::FinalAnalyzedLogicalPlan`],
    /// and [`PlanType::OptimizedLogicalPlan`] entries in application order across
    /// both sync and async phases.
    pub async fn apply_explained(
        &self,
        plan: LogicalPlan,
        config: &(dyn OptimizerConfig + Sync),
    ) -> Result<(LogicalPlan, Vec<StringifiedPlan>)> {
        let config_opts = config.options();
        let mut plan = plan;
        let mut stringified: Vec<StringifiedPlan> = Vec::new();
        let mut pending_final_analyzed = false;

        for phase in &self.phases {
            if !phase.is_enabled() {
                continue;
            }
            match phase {
                Phase::SyncAnalysis(p) => {
                    plan =
                        p.apply_observed(plan, &config_opts, &mut |p, rule_name| {
                            stringified.push(p.to_stringified(
                                PlanType::AnalyzedLogicalPlan {
                                    analyzer_name: rule_name.to_string(),
                                },
                            ));
                        })?;
                    pending_final_analyzed = true;
                }
                Phase::AsyncAnalysis(p) => {
                    plan = p
                        .apply_observed(plan, &config_opts, &mut |p, rule_name| {
                            stringified.push(p.to_stringified(
                                PlanType::AnalyzedLogicalPlan {
                                    analyzer_name: rule_name.to_string(),
                                },
                            ));
                        })
                        .await?;
                    pending_final_analyzed = true;
                }
                Phase::SyncOptimization(p) => {
                    if pending_final_analyzed {
                        stringified.push(
                            plan.to_stringified(PlanType::FinalAnalyzedLogicalPlan),
                        );
                        pending_final_analyzed = false;
                    }
                    plan = p.apply_observed(plan, config, &mut |p, rule_name| {
                        stringified.push(p.to_stringified(
                            PlanType::OptimizedLogicalPlan {
                                optimizer_name: rule_name.to_string(),
                            },
                        ));
                    })?;
                }
                Phase::AsyncOptimization(p) => {
                    if pending_final_analyzed {
                        stringified.push(
                            plan.to_stringified(PlanType::FinalAnalyzedLogicalPlan),
                        );
                        pending_final_analyzed = false;
                    }
                    plan = p
                        .apply_observed(plan, &config_opts, &mut |p, rule_name| {
                            stringified.push(p.to_stringified(
                                PlanType::OptimizedLogicalPlan {
                                    optimizer_name: rule_name.to_string(),
                                },
                            ));
                        })
                        .await?;
                }
            }
        }
        if pending_final_analyzed {
            stringified.push(plan.to_stringified(PlanType::FinalAnalyzedLogicalPlan));
        }
        Ok((plan, stringified))
    }
}
