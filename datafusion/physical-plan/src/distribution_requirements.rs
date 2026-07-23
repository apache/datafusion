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

//! Input distribution requirements for physical execution plans.

use std::sync::Arc;

use datafusion_common::{Result, ScalarValue, internal_err, validate_range_split_points};
use datafusion_physical_expr::{
    Distribution, EquivalenceProperties, Partitioning, PartitioningSatisfaction,
    PhysicalExpr, RangePartitioning, physical_exprs_equal,
};

use crate::execution_plan::{ExecutionPlan, ExecutionPlanProperties, InvariantLevel};

/// Distribution requirements for an [`ExecutionPlan`]'s inputs.
///
/// [`InputDistributionRequirements`] describes what distribution an operator
/// requires from each child.
///
/// - [`Self::new`] describes independent per-child requirements.
/// - [`Self::co_partitioned`] additionally requires child partitions with the
///   same index to cover compatible key ranges.
///
/// For a single-input aggregate:
///
/// ```text
/// AggregateExec
///   child 0 requirement: KeyPartitioned(group_exprs)
/// ```
///
/// each input partition can aggregate its own key domain independently.
///
/// For a partitioned join:
///
/// ```text
/// HashJoinExec
///   child 0 requirement: KeyPartitioned(left_keys)
///   child 1 requirement: KeyPartitioned(right_keys)
///
///   partition 0: join(left partition 0, right partition 0)
///   partition 1: join(left partition 1, right partition 1)
///   partition 2: join(left partition 2, right partition 2)
/// ```
///
/// each child must satisfy its own key requirement. In addition, matching
/// partition indexes must be safe to process together.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct InputDistributionRequirements {
    /// Per-child distribution requirements, indexed by child position.
    children: Vec<ChildDistributionRequirement>,
    /// Child indexes that must also have compatible partition layouts.
    co_partitioned: Option<Vec<usize>>,
}

/// Options for checking child distribution satisfaction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChildSatisfactionOptions {
    allow_subset: bool,
}

impl ChildSatisfactionOptions {
    /// Create default satisfaction options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Allow a child partitioning whose key expressions are a subset of the
    /// required key expressions to satisfy the requirement.
    pub fn with_allow_subset(mut self, allow_subset: bool) -> Self {
        self.allow_subset = allow_subset;
        self
    }

    /// Whether subset satisfaction is enabled.
    pub fn allow_subset(&self) -> bool {
        self.allow_subset
    }
}

impl InputDistributionRequirements {
    /// Create independent per-child requirements.
    pub fn new(per_child: Vec<Distribution>) -> Self {
        let children = per_child
            .into_iter()
            .map(|distribution| ChildDistributionRequirement {
                distribution,
                satisfaction: InputDistributionSatisfaction::Default,
            })
            .collect();

        Self {
            children,
            co_partitioned: None,
        }
    }

    /// Create a requirement that all children are co-partitioned.
    ///
    /// Each child must satisfy its own [`Distribution`]. Matching partition
    /// indexes are processed together:
    ///
    /// ```text
    /// left:  Range(left.a ASC,  split_points=[10, 20])
    /// right: Range(right.x ASC, split_points=[10, 20])
    ///
    /// partition 0 from both sides contains keys before 10
    /// partition 1 from both sides contains keys in [10, 20)
    /// partition 2 from both sides contains keys at/after 20
    /// ```
    ///
    /// If the split points differ, partition `i` from one side no longer covers
    /// the same key range as partition `i` from the other side.
    pub fn co_partitioned(per_child: Vec<Distribution>) -> Self {
        debug_assert!(
            per_child.len() >= 2,
            "co-partitioned distribution requirements need at least two children"
        );
        let co_partitioned = (0..per_child.len()).collect();
        let mut result = Self::new(per_child);
        result.co_partitioned = Some(co_partitioned);
        result
    }

    /// Return the per-child distribution requirements.
    pub fn per_child_distributions(
        &self,
    ) -> impl ExactSizeIterator<Item = &Distribution> + '_ {
        self.children.iter().map(|child| &child.distribution)
    }

    /// Return the distribution requirement for a child.
    pub fn child_distribution(&self, child_idx: usize) -> Option<&Distribution> {
        self.children
            .get(child_idx)
            .map(|child| &child.distribution)
    }

    /// Return the per-child distribution requirements.
    ///
    /// WARNING: This intentionally drops any grouped relationship.
    pub fn into_per_child(self) -> Vec<Distribution> {
        self.children
            .into_iter()
            .map(|child| child.distribution)
            .collect()
    }

    /// Returns how a child satisfies its distribution requirement.
    ///
    /// This preserves the requirement set's satisfaction policy.
    pub fn child_satisfaction(
        &self,
        child_idx: usize,
        child: &dyn ExecutionPlan,
        options: ChildSatisfactionOptions,
    ) -> Result<PartitioningSatisfaction> {
        let Some(requirement) = self.children.get(child_idx) else {
            return internal_err!(
                "missing distribution requirement for child {child_idx}"
            );
        };

        Ok(requirement.satisfaction.satisfaction(
            child.output_partitioning(),
            &requirement.distribution,
            child.equivalence_properties(),
            options.allow_subset(),
        ))
    }

    /// Return child indexes whose co-partitioning requirements are
    /// unsatisfied by the provided candidate children.
    ///
    /// Independent per-child requirements are intentionally ignored here, use
    /// [`Self::child_satisfaction`] for those checks. An empty result means all
    /// co-partitioning requirements are satisfied.
    #[doc(hidden)]
    pub fn unsatisfied_co_partitioned_children(
        &self,
        plan_name: &str,
        children: &[&dyn ExecutionPlan],
    ) -> Result<Vec<usize>> {
        self.validate_shape(plan_name, children.len())?;

        let Some(co_partitioned) = &self.co_partitioned else {
            return Ok(vec![]);
        };
        if self.co_partitioning_satisfied(co_partitioned, children) {
            return Ok(vec![]);
        }

        Ok(co_partitioned.clone())
    }

    /// TODO: remove this temporary bridge once [`Partitioning::Range`]
    /// generally satisfies [`Distribution::KeyPartitioned`] through
    /// [`Partitioning::satisfaction`].
    /// <https://github.com/apache/datafusion/issues/23266>.
    ///
    /// Also allow compatible [`Partitioning::Range`] to satisfy
    /// [`Distribution::KeyPartitioned`].
    #[expect(
        deprecated,
        reason = "HashPartitioned is accepted during the KeyPartitioned migration"
    )]
    pub(crate) fn allow_range_satisfaction_for_key_partitioning(mut self) -> Self {
        for child in &mut self.children {
            if matches!(
                child.distribution,
                Distribution::HashPartitioned(_) | Distribution::KeyPartitioned(_)
            ) {
                child.satisfaction =
                    InputDistributionSatisfaction::AllowRangeKeyPartitioning;
            }
        }
        self
    }

    /// Validate the requirements against a plan's children.
    pub(crate) fn check_invariants<P: ExecutionPlan + ?Sized>(
        &self,
        plan: &P,
        check: InvariantLevel,
    ) -> Result<()> {
        let children = plan.children();
        self.validate_shape(plan.name(), children.len())?;

        let children = children
            .into_iter()
            .map(|child| child.as_ref())
            .collect::<Vec<_>>();
        if matches!(check, InvariantLevel::Executable)
            && let Some(co_partitioned) = &self.co_partitioned
            && !self.co_partitioning_satisfied(co_partitioned, &children)
        {
            return internal_err!(
                "{} requires children {:?} to be co-partitioned",
                plan.name(),
                co_partitioned
            );
        }

        Ok(())
    }

    fn validate_shape(&self, plan_name: &str, children_len: usize) -> Result<()> {
        if self.children.len() != children_len {
            return internal_err!(
                "{plan_name}::input_distribution_requirements returned incorrect child count: {} != {}",
                self.children.len(),
                children_len
            );
        }

        if let Some(co_partitioned) = &self.co_partitioned {
            if co_partitioned.len() < 2 {
                return internal_err!(
                    "{plan_name} has invalid co-partitioning requirement: at least two children are required"
                );
            }
            let mut seen = vec![false; self.children.len()];
            for &child in co_partitioned {
                validate_child_index(plan_name, child, self.children.len(), &mut seen)?;
                if matches!(
                    self.children[child].distribution,
                    Distribution::UnspecifiedDistribution
                ) {
                    return internal_err!(
                        "{plan_name} has invalid co-partitioning requirement: child {child} has unspecified distribution"
                    );
                }
            }
        }

        Ok(())
    }

    fn co_partitioning_satisfied(
        &self,
        co_partitioned: &[usize],
        children: &[&dyn ExecutionPlan],
    ) -> bool {
        let first_idx = co_partitioned[0];
        let first_requirement = &self.children[first_idx];
        let first = children[first_idx];
        let first_partitioning = first.output_partitioning();

        if !first_requirement
            .satisfaction
            .satisfaction(
                first_partitioning,
                &first_requirement.distribution,
                first.equivalence_properties(),
                false,
            )
            .is_satisfied()
        {
            return false;
        }

        for &child_idx in co_partitioned.iter().skip(1) {
            let requirement = &self.children[child_idx];
            let child = children[child_idx];
            if !requirement
                .satisfaction
                .satisfaction(
                    child.output_partitioning(),
                    &requirement.distribution,
                    child.equivalence_properties(),
                    false,
                )
                .is_satisfied()
                || !compatible_co_partitioning_layout(
                    first_requirement,
                    first_partitioning,
                    requirement,
                    child.output_partitioning(),
                )
            {
                return false;
            }
        }

        true
    }
}

/// A distribution requirement for a single child.
#[derive(Debug, Clone)]
struct ChildDistributionRequirement {
    distribution: Distribution,
    satisfaction: InputDistributionSatisfaction,
}

/// TODO: remove this temporary bridge once [`Partitioning::Range`]
/// generally satisfies [`Distribution::KeyPartitioned`] through
/// [`Partitioning::satisfaction`].
/// <https://github.com/apache/datafusion/issues/23266>.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum InputDistributionSatisfaction {
    /// Use [`Partitioning::satisfaction`] as-is.
    #[default]
    Default,
    /// Also allow [`Partitioning::Range`] to satisfy
    /// [`Distribution::KeyPartitioned`].
    AllowRangeKeyPartitioning,
}

impl InputDistributionSatisfaction {
    /// Returns how `partitioning` satisfies `requirement`.
    #[expect(
        deprecated,
        reason = "HashPartitioned is accepted during the KeyPartitioned migration"
    )]
    fn satisfaction(
        self,
        partitioning: &Partitioning,
        requirement: &Distribution,
        eq_properties: &EquivalenceProperties,
        allow_subset: bool,
    ) -> PartitioningSatisfaction {
        let satisfaction =
            partitioning.satisfaction(requirement, eq_properties, allow_subset);
        if satisfaction.is_satisfied() {
            return satisfaction;
        }

        if !matches!(self, Self::AllowRangeKeyPartitioning) {
            return PartitioningSatisfaction::NotSatisfied;
        }

        let (Distribution::HashPartitioned(required_exprs)
        | Distribution::KeyPartitioned(required_exprs)) = requirement
        else {
            return PartitioningSatisfaction::NotSatisfied;
        };

        range_satisfies_key_partitioning(
            partitioning,
            required_exprs,
            eq_properties,
            allow_subset,
        )
    }
}

fn validate_child_index(
    plan_name: &str,
    child_idx: usize,
    child_count: usize,
    seen: &mut [bool],
) -> Result<()> {
    if child_idx >= child_count {
        return internal_err!(
            "{plan_name} has invalid distribution requirement: child index {child_idx} out of bounds"
        );
    }
    if seen[child_idx] {
        return internal_err!(
            "{plan_name} has invalid distribution requirement: child {child_idx} appears more than once"
        );
    }
    seen[child_idx] = true;
    Ok(())
}

/// TODO: remove this temporary bridge once [`Partitioning::Range`]
/// generally satisfies [`Distribution::KeyPartitioned`] through
/// [`Partitioning::satisfaction`].
/// <https://github.com/apache/datafusion/issues/23266>.
fn range_satisfies_key_partitioning(
    partitioning: &Partitioning,
    required_exprs: &[Arc<dyn PhysicalExpr>],
    eq_properties: &EquivalenceProperties,
    allow_subset: bool,
) -> PartitioningSatisfaction {
    let Partitioning::Range(range) = partitioning else {
        return PartitioningSatisfaction::NotSatisfied;
    };

    let partition_exprs = range
        .ordering()
        .iter()
        .map(|sort_expr| Arc::clone(&sort_expr.expr))
        .collect::<Vec<_>>();

    if partition_exprs.is_empty() || required_exprs.is_empty() {
        return PartitioningSatisfaction::NotSatisfied;
    }

    let eq_group = eq_properties.eq_group();
    let normalized_partition_exprs = partition_exprs
        .iter()
        .map(|expr| eq_group.normalize_expr(Arc::clone(expr)))
        .collect::<Vec<_>>();
    let normalized_required_exprs = required_exprs
        .iter()
        .map(|expr| eq_group.normalize_expr(Arc::clone(expr)))
        .collect::<Vec<_>>();

    if physical_exprs_equal(&normalized_required_exprs, &normalized_partition_exprs) {
        return PartitioningSatisfaction::Exact;
    }

    if allow_subset
        && normalized_partition_exprs.len() < normalized_required_exprs.len()
        && normalized_partition_exprs.iter().all(|partition_expr| {
            normalized_required_exprs
                .iter()
                .any(|required_expr| partition_expr.eq(required_expr))
        })
    {
        PartitioningSatisfaction::Subset
    } else {
        PartitioningSatisfaction::NotSatisfied
    }
}

fn compatible_co_partitioning_layout(
    first: &ChildDistributionRequirement,
    first_partitioning: &Partitioning,
    other: &ChildDistributionRequirement,
    other_partitioning: &Partitioning,
) -> bool {
    if first_partitioning.partition_count() == 1
        && other_partitioning.partition_count() == 1
    {
        return true;
    }

    if first_partitioning.partition_count() != other_partitioning.partition_count() {
        return false;
    }

    match (first_partitioning, other_partitioning) {
        (Partitioning::Hash(_, _), Partitioning::Hash(_, _)) => true,
        (Partitioning::Range(left), Partitioning::Range(right))
            if first.satisfaction
                == InputDistributionSatisfaction::AllowRangeKeyPartitioning
                && other.satisfaction
                    == InputDistributionSatisfaction::AllowRangeKeyPartitioning =>
        {
            left.split_points() == right.split_points()
                && left.ordering().len() == right.ordering().len()
                && left
                    .ordering()
                    .iter()
                    .zip(right.ordering())
                    .all(|(left, right)| left.options == right.options)
        }
        _ => false,
    }
}
