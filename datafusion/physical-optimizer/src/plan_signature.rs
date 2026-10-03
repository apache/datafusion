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

//! [`PhysicalPlanSignature`]: the physical analog of `LogicalPlanSignature`.
//!
//! The logical optimizer loop terminates on plan identity: after each pass it
//! inserts the plan's signature into a `HashSet` and stops on the first
//! revisit, which also terminates cycles (a plan oscillating between two
//! forms revisits one of them on the third pass). `LogicalPlan` implements
//! `Hash`, so its signature is a plain hash.
//!
//! `ExecutionPlan` implements neither `Hash` nor `Eq`, so identity has to be
//! derived. This module derives it from what physical optimizer rules
//! actually consult: the indented rendering with schemas shown, plus each
//! node's `PlanProperties` (partitioning, orderings, equivalences, emission
//! type, boundedness) appended in the same pre-order. Statistics are
//! deliberately not part of the identity: rendering them could mean
//! recomputing them for every pass, and a rule whose decisions depend on
//! statistics is not safe to drive from this signature (see the tests, which
//! pin both what the signature separates and what it deliberately does not).

use std::fmt::Write;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::num::NonZeroUsize;

use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::displayable;

/// Identity of an [`ExecutionPlan`] for convergence detection, mirroring
/// `LogicalPlanSignature`.
///
/// # Guarantees
///
/// Two plans with different node counts have different signatures. Two plans
/// whose fingerprints (rendering + per-node `PlanProperties`) differ have
/// different signatures, up to hash collision.
///
/// # Caveats
///
/// Two *different* plans can share a signature when they differ only in what
/// a node neither prints nor exposes through `PlanProperties` (statistics are
/// the named instance). A shared signature ends an optimization loop early;
/// it never changes a plan, so the failure mode is a missed optimization, not
/// a wrong result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PhysicalPlanSignature {
    node_count: NonZeroUsize,
    fingerprint_hash: u64,
}

impl PhysicalPlanSignature {
    /// Returns the [`PhysicalPlanSignature`] of the given plan.
    pub fn new(plan: &dyn ExecutionPlan) -> Self {
        let mut hasher = DefaultHasher::new();
        plan_fingerprint(plan).hash(&mut hasher);

        Self {
            node_count: node_count(plan),
            fingerprint_hash: hasher.finish(),
        }
    }
}

fn node_count(plan: &dyn ExecutionPlan) -> NonZeroUsize {
    let mut count = 1;
    for child in plan.children() {
        count += node_count(child.as_ref()).get();
    }
    // SAFETY-free: count starts at 1 and only grows.
    NonZeroUsize::new(count).expect("a plan has at least one node")
}

/// Renders a plan into the string the signature hashes.
///
/// The rendering is what each node chooses to print plus its schema. The
/// properties a rule most often rewrites, partitioning and ordering, are not
/// part of that for every node, and two plans that differ only in them would
/// otherwise compare equal. They are appended per node, in the same pre-order
/// the rendering uses.
///
/// `equivalence_properties` is rendered with `Debug`, not `Display`: the
/// latter prints the equivalence and ordering classes but omits the table
/// constraints, which also factor into whether a requirement is satisfied.
pub fn plan_fingerprint(plan: &dyn ExecutionPlan) -> String {
    let mut out = displayable(plan)
        .set_show_schema(true)
        .indent(true)
        .to_string();
    fn append_properties(plan: &dyn ExecutionPlan, depth: usize, out: &mut String) {
        let props = plan.properties();
        // Writing to a `String` cannot fail, so the result is discarded.
        let _ = write!(
            out,
            "\n{:indent$}props[{}]: partitioning={:?} ordering={:?} \
emission={:?} boundedness={:?} equivalence={:?}",
            "",
            plan.name(),
            props.output_partitioning(),
            props.output_ordering(),
            props.emission_type,
            props.boundedness,
            props.equivalence_properties(),
            indent = depth * 2,
        );
        for child in plan.children() {
            append_properties(child.as_ref(), depth + 1, out);
        }
    }
    append_properties(plan, 0, &mut out);
    out
}
