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

//! A special-case optimizer rule that pushes limit into a grouped aggregation
//! which has no aggregate expressions or sorting requirements

use std::sync::Arc;

use datafusion_physical_plan::aggregates::AggregateExec;
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::Result;

use crate::{OptimizerContext, PhysicalOptimizerRule};
use itertools::Itertools;

/// An optimizer rule that passes a `limit` hint into grouped aggregations which don't require all
/// rows in the group to be processed for correctness. Example queries fitting this description are:
/// - `SELECT distinct l_orderkey FROM lineitem LIMIT 10;`
/// - `SELECT l_orderkey FROM lineitem GROUP BY l_orderkey LIMIT 10;`
#[derive(Debug)]
pub struct LimitedDistinctAggregation {}

impl LimitedDistinctAggregation {
    /// Create a new `LimitedDistinctAggregation`
    pub fn new() -> Self {
        Self {}
    }

    fn transform_agg(
        aggr: &AggregateExec,
        limit: usize,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        // rules for transforming this Aggregate are held in this method
        if !aggr.is_unordered_unfiltered_group_by_distinct() {
            return None;
        }

        // We found what we want: clone, copy the limit down, and return modified node
        let new_aggr = AggregateExec::try_new(
            *aggr.mode(),
            aggr.group_expr().clone(),
            aggr.aggr_expr().to_vec(),
            aggr.filter_expr().to_vec(),
            aggr.input().to_owned(),
            aggr.input_schema(),
        )
        .expect("Unable to copy Aggregate!")
        .with_limit(Some(limit));
        Some(Arc::new(new_aggr))
    }

    /// transform_limit matches an `AggregateExec` as the child of a `LocalLimitExec`
    /// or `GlobalLimitExec` and pushes the limit into the aggregation as a soft limit when
    /// there is a group by, but no sorting, no aggregate expressions, and no filters in the
    /// aggregation
    fn transform_limit(plan: Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        let limit: usize;
        let mut global_fetch: Option<usize> = None;
        let mut global_skip: usize = 0;
        let children: Vec<Arc<dyn ExecutionPlan>>;
        let mut is_global_limit = false;
        if let Some(local_limit) = plan.as_any().downcast_ref::<LocalLimitExec>() {
            limit = local_limit.fetch();
            children = local_limit.children().into_iter().cloned().collect();
        } else if let Some(global_limit) = plan.as_any().downcast_ref::<GlobalLimitExec>()
        {
            global_fetch = global_limit.fetch();
            global_fetch?;
            global_skip = global_limit.skip();
            // the aggregate must read at least fetch+skip number of rows
            limit = global_fetch.unwrap() + global_skip;
            children = global_limit.children().into_iter().cloned().collect();
            is_global_limit = true
        } else {
            return None;
        }
        let child = children.iter().exactly_one().ok()?;
        // ensure there is no output ordering; can this rule be relaxed?
        if plan.output_ordering().is_some() {
            return None;
        }
        // ensure no ordering is required on the input
        if plan.required_input_ordering()[0].is_some() {
            return None;
        }

        // if found_match_aggr is true, match_aggr holds a parent aggregation whose group_by
        // must match that of a child aggregation in order to rewrite the child aggregation
        let mut match_aggr: Arc<dyn ExecutionPlan> = plan;
        let mut found_match_aggr = false;

        let mut rewrite_applicable = true;
        let closure = |plan: Arc<dyn ExecutionPlan>| {
            if !rewrite_applicable {
                return Ok(Transformed::no(plan));
            }
            if let Some(aggr) = plan.as_any().downcast_ref::<AggregateExec>() {
                if found_match_aggr {
                    if let Some(parent_aggr) =
                        match_aggr.as_any().downcast_ref::<AggregateExec>()
                    {
                        if !parent_aggr.group_expr().eq(aggr.group_expr()) {
                            // a partial and final aggregation with different groupings disqualifies
                            // rewriting the child aggregation
                            rewrite_applicable = false;
                            return Ok(Transformed::no(plan));
                        }
                    }
                }
                // either we run into an Aggregate and transform it, or disable the rewrite
                // for subsequent children
                match Self::transform_agg(aggr, limit) {
                    None => {}
                    Some(new_aggr) => {
                        match_aggr = plan;
                        found_match_aggr = true;
                        return Ok(Transformed::yes(new_aggr));
                    }
                }
            }
            rewrite_applicable = false;
            Ok(Transformed::no(plan))
        };
        let child = child.to_owned().transform_down(closure).data().ok()?;
        if is_global_limit {
            return Some(Arc::new(GlobalLimitExec::new(
                child,
                global_skip,
                global_fetch,
            )));
        }
        Some(Arc::new(LocalLimitExec::new(child, limit)))
    }
}

impl Default for LimitedDistinctAggregation {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOptimizerRule for LimitedDistinctAggregation {
    fn optimize_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &OptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let config = context.session_config().options();
        if config.optimizer.enable_distinct_aggregation_soft_limit {
            plan.transform_down(|plan| {
                Ok(
                    if let Some(plan) =
                        LimitedDistinctAggregation::transform_limit(plan.to_owned())
                    {
                        Transformed::yes(plan)
                    } else {
                        Transformed::no(plan)
                    },
                )
            })
            .data()
        } else {
            Ok(plan)
        }
    }

    fn name(&self) -> &str {
        "LimitedDistinctAggregation"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

// See tests in datafusion/core/tests/physical_optimizer
