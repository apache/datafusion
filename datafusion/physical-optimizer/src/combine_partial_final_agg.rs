// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! CombinePartialFinalAggregate optimizer rule checks the adjacent Partial and Final AggregateExecs
//! and try to combine them if necessary

use std::sync::Arc;

use datafusion_common::error::Result;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::aggregates::AggregateExec;

use crate::PhysicalOptimizerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{TransformedResult, TreeNode};

/// Combines adjacent partial and final `AggregateExec`s into a single aggregate
/// when their grouping and aggregate expressions match.
///
/// ```txt
/// Before:
/// AggregateExec(mode=Final)          <-- current plan
///   AggregateExec(mode=Partial)
///     DataSourceExec
///
/// After:
/// AggregateExec(mode=Single)         <-- current plan
///   DataSourceExec
/// ```
///
/// This rule should be applied after the `EnsureRequirements` rule (which
/// handles both distribution and sorting enforcement).
///
/// # Background
///
/// The relevant optimization steps, in order, are:
///
/// 1. Initial physical planning: creates a partial/final pair so aggregation can
///    run in parallel. For grouped, partitioned aggregation:
///
///    ```txt
///    AggregateExec(mode=FinalPartitioned)
///      AggregateExec(mode=Partial)
///        DataSourceExec
///    ```
///
/// 2. `EnsureRequirements` rule: inserts a `RepartitionExec` between the stages when
///    needed to satisfy the final aggregate's distribution requirements:
///
///    ```txt
///    AggregateExec(mode=FinalPartitioned)
///      RepartitionExec                 <-- inserted here
///        AggregateExec(mode=Partial)
///          DataSourceExec
///    ```
///
/// 3. This rule: If the input already satisfies these requirements, e.g. because it
///    is partitioned by the grouping keys, no repartition is needed. This rule
///    then combines the adjacent, compatible aggregate stages into one.
#[derive(Default, Debug)]
pub struct CombinePartialFinalAggregate {}

impl CombinePartialFinalAggregate {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for CombinePartialFinalAggregate {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(AggregateExec::try_combine_partial_final)
            .data()
    }

    fn name(&self) -> &str {
        "CombinePartialFinalAggregate"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

// See tests in datafusion/core/tests/physical_optimizer
