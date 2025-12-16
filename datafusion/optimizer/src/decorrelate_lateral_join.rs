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

//! [`DecorrelateLateralJoin`] decorrelates logical plans produced by lateral joins.

use std::collections::BTreeSet;

use crate::decorrelate::PullUpCorrelatedExpr;
use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_expr::{Join, lit};

use datafusion_common::Result;
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion_expr::logical_plan::JoinType;
use datafusion_expr::utils::conjunction;
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder};

/// Optimizer rule for rewriting lateral joins to joins
#[derive(Default, Debug)]
pub struct DecorrelateLateralJoin {}

impl DecorrelateLateralJoin {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self::default()
    }
}

impl OptimizerRule for DecorrelateLateralJoin {
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // Find cross joins with outer column references on the right side (i.e., the apply operator).
        let LogicalPlan::Join(join) = plan else {
            return Ok(Transformed::no(plan));
        };

        rewrite_internal(join)
    }

    fn name(&self) -> &str {
        "decorrelate_lateral_join"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

// Build the decorrelated join based on the original lateral join query. For now, we only support cross/inner
// lateral joins.
fn rewrite_internal(join: Join) -> Result<Transformed<LogicalPlan>> {
    if join.join_type != JoinType::Inner {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    }

    match join.right.apply_with_subqueries(|p| {
        // TODO: support outer joins
        if p.contains_outer_reference() {
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })? {
        TreeNodeRecursion::Stop => {}
        TreeNodeRecursion::Continue => {
            // The left side contains outer references, we need to decorrelate it.
            return Ok(Transformed::new(
                LogicalPlan::Join(join),
                false,
                TreeNodeRecursion::Jump,
            ));
        }
        TreeNodeRecursion::Jump => {
            unreachable!("")
        }
    }

    let LogicalPlan::Subquery(subquery) = join.right.as_ref() else {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    };

    if join.join_type != JoinType::Inner {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    }
    let subquery_plan = subquery.subquery.as_ref();
    let mut pull_up = PullUpCorrelatedExpr::new().with_need_handle_count_bug(true);
    let rewritten_subquery = subquery_plan.clone().rewrite(&mut pull_up).data()?;
    if !pull_up.can_pull_up {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    }

    let mut all_correlated_cols = BTreeSet::new();
    pull_up
        .correlated_subquery_cols_map
        .values()
        .for_each(|cols| all_correlated_cols.extend(cols.clone()));
    let join_filter_opt = conjunction(pull_up.join_filters);
    let join_filter = match join_filter_opt {
        Some(join_filter) => join_filter,
        None => lit(true),
    };
    // -- inner join but the right side always has one row, we need to rewrite it to a left join
    // SELECT * FROM t0, LATERAL (SELECT sum(v1) FROM t1 WHERE t0.v0 = t1.v0);
    // -- inner join but the right side number of rows is related to the filter (join) condition, so keep inner join.
    // SELECT * FROM t0, LATERAL (SELECT * FROM t1 WHERE t0.v0 = t1.v0);
    let new_plan = LogicalPlanBuilder::from(join.left)
        .join_on(
            rewritten_subquery,
            if pull_up.pulled_up_scalar_agg {
                JoinType::Left
            } else {
                JoinType::Inner
            },
            Some(join_filter),
        )?
        .build()?;
    // TODO: handle count(*) bug
    Ok(Transformed::new(new_plan, true, TreeNodeRecursion::Jump))
}
