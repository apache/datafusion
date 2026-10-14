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

//! Deriving the inputs of a [`Case`] by probing the input requirements of the
//! plan the factory builds.

use std::fmt;
use std::sync::Arc;

use datafusion_common::{Result, plan_err};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{
    Distribution, LexOrdering, LexRequirement, OrderingRequirements, PhysicalExpr,
    PhysicalSortExpr,
};
use datafusion_physical_plan::distribution_requirements::ChildSatisfactionOptions;
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use super::{Case, PlanFactory, Profile, ROW_ID_RANGE};
use crate::physical_plan::fixtures::{
    BatchLayout, COPY_SUFFIX, ROW_ID_COLUMN, SourceSpec,
};

/// How many times the harness builds a plan to find inputs that meet its
/// requirements, which can change with the inputs
const MAX_PROBES: usize = 5;

/// How the rows of every input partition are split into batches
const BATCH_LAYOUT: BatchLayout = BatchLayout::Random { max_rows: 16 };

/// Derive the case of `factory` for `profile`: build the plan on inputs laid
/// out by the profile, change the inputs to meet the requirements of the nodes
/// directly above them, and repeat until every requirement is met, at most
/// [`MAX_PROBES`] times.
pub(super) fn derive_case(factory: &PlanFactory, profile: &Profile) -> Result<Case> {
    let mut specs: Vec<SourceSpec> = factory
        .inputs
        .iter()
        .enumerate()
        .map(|(i, base)| {
            let rows = base.num_rows() * profile.row_multiplier;
            let mut spec = base.clone();
            // The columns of the base schema, without the columns the spec
            // adds to it
            let schema = base.schema();
            let columns = schema
                .fields()
                .iter()
                .map(|field| field.name())
                .filter(|name| *name != ROW_ID_COLUMN && !name.ends_with(COPY_SUFFIX));
            for name in columns {
                if let Some(values) = profile.constants {
                    spec = spec.with_constant(name, values);
                }
                if profile.copies {
                    spec = spec.with_copy(name);
                }
            }
            if profile.row_id_ordering {
                spec = spec.with_row_id_ordering();
            }
            spec = spec.with_partition_rows(&profile.partition_rows(rows));
            if profile.hash_partitioning
                && let Some(first) = schema.fields().first()
            {
                let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new(first.name(), 0));
                spec = spec.with_hash_partitioning(
                    vec![key],
                    profile.partition_weights.len().max(1),
                    rows,
                );
            }
            spec.with_batch_layout(BATCH_LAYOUT)
                .with_statistics_precision(profile.statistics)
                .with_seed(profile.seed.wrapping_mul(1000).wrapping_add(i as u64))
                .with_row_ids(i as u64 * ROW_ID_RANGE)
        })
        .collect();
    let partitions = profile.partition_weights.len().max(1);
    let mut probes = 0;
    loop {
        probes += 1;
        let inputs = specs
            .iter()
            .map(SourceSpec::build_arc)
            .collect::<Result<Vec<_>>>()?;
        let plan = factory.create(inputs.clone())?;
        let unmet = unmet_requirements(&plan, &inputs);
        if unmet.is_empty() {
            return Ok(Case {
                profile: profile.clone(),
                inputs: specs,
                plan,
            });
        }
        let description = unmet
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ");
        if unmet.iter().all(|unmet| unmet.input.is_none()) {
            return plan_err!(
                "{description}; the harness only changes the inputs, so the factory \
                 must build the other children so that they meet their requirements"
            );
        }
        if probes == MAX_PROBES {
            return plan_err!(
                "the input requirements were still not met after building the plan \
                 {MAX_PROBES} times: {description}"
            );
        }
        for unmet in unmet {
            if let Some(input) = unmet.input {
                specs[input] = unmet.fix.apply(specs[input].clone(), partitions);
            }
        }
    }
}

/// A change to the spec of an input that meets a requirement
#[derive(Debug, Clone)]
enum Fix {
    /// Sort every partition
    Ordering(LexOrdering),
    /// Hash partition on these expressions, into as many partitions as the
    /// profile has, which is the same for every input, so that children that
    /// must be co-partitioned are
    Hash(Vec<Arc<dyn PhysicalExpr>>),
    /// Put all rows in one partition
    SinglePartition,
}

impl Fix {
    fn apply(&self, spec: SourceSpec, partitions: usize) -> SourceSpec {
        let rows = spec.num_rows();
        match self {
            Fix::Ordering(ordering) => spec.with_ordering(ordering.clone()),
            Fix::Hash(exprs) => {
                spec.with_hash_partitioning(exprs.clone(), partitions, rows)
            }
            Fix::SinglePartition => spec.with_partition_rows(&[rows]),
        }
    }
}

impl fmt::Display for Fix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Fix::Ordering(ordering) => write!(f, "sorted by [{ordering}]"),
            Fix::Hash(exprs) => {
                let exprs: Vec<String> = exprs.iter().map(ToString::to_string).collect();
                write!(f, "hash partitioned on [{}]", exprs.join(", "))
            }
            Fix::SinglePartition => write!(f, "in a single partition"),
        }
    }
}

/// A requirement of a node that its child does not meet
#[derive(Debug)]
struct Unmet {
    node: String,
    child: usize,
    /// The input the child is, if it is one
    input: Option<usize>,
    fix: Fix,
}

impl fmt::Display for Unmet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} requires child {} to be {}",
            self.node, self.child, self.fix
        )?;
        if self.input.is_none() {
            write!(f, ", and the child is not an input")?;
        }
        Ok(())
    }
}

/// Every hard ordering requirement and distribution requirement of a node of
/// `plan` that its child does not meet, down to the inputs. Soft ordering
/// requirements, which a node can work without, are not imposed.
fn unmet_requirements(
    plan: &Arc<dyn ExecutionPlan>,
    inputs: &[Arc<dyn ExecutionPlan>],
) -> Vec<Unmet> {
    let mut unmet = vec![];
    let mut stack = vec![Arc::clone(plan)];
    while let Some(node) = stack.pop() {
        let orderings = node.required_input_ordering();
        let distributions = node.input_distribution_requirements();
        for (c, child) in node.children().into_iter().enumerate() {
            let input = inputs.iter().position(|input| Arc::ptr_eq(input, child));
            let mut require = |fix| {
                unmet.push(Unmet {
                    node: node.name().to_string(),
                    child: c,
                    input,
                    fix,
                })
            };
            if let Some(Some(OrderingRequirements::Hard(alternatives))) = orderings.get(c)
                && !alternatives.iter().any(|requirement| {
                    child
                        .equivalence_properties()
                        .ordering_satisfy_requirement(requirement.clone())
                        .unwrap_or(false)
                })
                && let Some(ordering) = alternatives.first().and_then(to_ordering)
            {
                require(Fix::Ordering(ordering));
            }
            let satisfied = distributions
                .child_satisfaction(c, child.as_ref(), ChildSatisfactionOptions::new())
                .is_ok_and(|satisfaction| satisfaction.is_satisfied());
            if !satisfied
                && let Some(fix) = distributions.child_distribution(c).and_then(fix_for)
            {
                require(fix);
            }
            if input.is_none() {
                stack.push(Arc::clone(child));
            }
        }
    }
    unmet
}

/// The fix that meets `distribution`
#[expect(
    deprecated,
    reason = "HashPartitioned is accepted during the KeyPartitioned migration"
)]
fn fix_for(distribution: &Distribution) -> Option<Fix> {
    match distribution {
        Distribution::UnspecifiedDistribution => None,
        Distribution::SinglePartition => Some(Fix::SinglePartition),
        Distribution::HashPartitioned(exprs) | Distribution::KeyPartitioned(exprs) => {
            Some(Fix::Hash(exprs.clone()))
        }
    }
}

/// The ordering that meets `requirement`, with default sort options where
/// the requirement has none
fn to_ordering(requirement: &LexRequirement) -> Option<LexOrdering> {
    LexOrdering::new(requirement.iter().map(|requirement| {
        PhysicalSortExpr::new(
            Arc::clone(&requirement.expr),
            requirement.options.unwrap_or_default(),
        )
    }))
}
