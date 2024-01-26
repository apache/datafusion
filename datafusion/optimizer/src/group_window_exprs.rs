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

//! GroupWindowExprs rule groups window expressions according to their ordering requirements
//! such that window expression with same requirements works in same window executor.

use std::cmp::Ordering;

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

use datafusion_common::Result;
use datafusion_expr::utils::{compare_sort_expr, group_window_expr_by_sort_keys};
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder};

/// [`GroupWindowExprs`] groups window expressions according to their requirement
/// window_exprs: vec![
///   SUM(a) OVER(PARTITION BY a, ORDER BY b),
///   COUNT(*) OVER(PARTITION BY a, ORDER BY b),
///   SUM(a) OVER(PARTITION BY a, ORDER BY c),
///   COUNT(*) OVER(PARTITION BY a, ORDER BY c)
/// ]
/// will be received as
#[derive(Default)]
pub struct GroupWindowExprs {}

impl GroupWindowExprs {
    pub fn new() -> Self {
        Self::default()
    }
}

impl OptimizerRule for GroupWindowExprs {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        if let LogicalPlan::Window(window) = plan {
            let window_exprs = window.window_expr.to_vec();
            let input = &window.input;

            let mut groups = group_window_expr_by_sort_keys(window_exprs)?;
            // To align with the behavior of PostgreSQL, we want the sort_keys sorted as same rule as PostgreSQL that first
            // we compare the sort key themselves and if one window's sort keys are a prefix of another
            // put the window with more sort keys first. so more deeply sorted plans gets nested further down as children.
            // The sort_by() implementation here is a stable sort.
            // Note that by this rule if there's an empty over, it'll be at the top level
            groups.sort_by(|(key_a, _), (key_b, _)| {
                for ((first, _), (second, _)) in key_a.iter().zip(key_b.iter()) {
                    let key_ordering = compare_sort_expr(first, second, plan.schema());
                    match key_ordering {
                        Ordering::Less => {
                            return Ordering::Less;
                        }
                        Ordering::Greater => {
                            return Ordering::Greater;
                        }
                        Ordering::Equal => {}
                    }
                }
                key_b.len().cmp(&key_a.len())
            });
            let mut plan = input.as_ref().clone();
            for (_, exprs) in groups {
                let window_exprs = exprs.into_iter().collect::<Vec<_>>();
                // Partition and sorting is done at physical level, see the EnforceDistribution
                // and EnforceSorting rules.
                plan = LogicalPlanBuilder::from(plan)
                    .window(window_exprs)?
                    .build()?;
            }
            return Ok(Some(plan));
        }
        Ok(None)
    }

    fn name(&self) -> &str {
        "group_window_exprs"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }
}
