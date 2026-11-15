// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Helpers for aggregating execution metrics across a physical plan tree.
//!
//! `ExecutionPlan::metrics()` returns metrics for a single plan node only; it
//! does not include metrics from child operators. These helpers recursively walk
//! the plan tree so tests can assert on metrics that may move between operators
//! after optimizer rewrites, such as pushing a `SortExec` below a
//! `ProjectionExec`.

use datafusion_physical_plan::ExecutionPlan;

/// Returns the total number of spill events recorded by `plan` and all of its
/// descendants.
///
/// Missing `spill_count` metrics are treated as zero.
pub fn plan_spill_count(plan: &dyn ExecutionPlan) -> usize {
    let own = plan.metrics().and_then(|m| m.spill_count()).unwrap_or(0);

    own + plan
        .children()
        .into_iter()
        .map(|child| plan_spill_count(child.as_ref()))
        .sum::<usize>()
}

/// Returns the total number of spilled bytes recorded by `plan` and all of its
/// descendants.
///
/// Missing `spilled_bytes` metrics are treated as zero.
pub fn plan_spilled_bytes(plan: &dyn ExecutionPlan) -> usize {
    let own = plan.metrics().and_then(|m| m.spilled_bytes()).unwrap_or(0);

    own + plan
        .children()
        .into_iter()
        .map(|child| plan_spilled_bytes(child.as_ref()))
        .sum::<usize>()
}
