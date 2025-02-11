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

use crate::optimizer::ApplyOrder;
use crate::{decorrelate_predicate_subquery, OptimizerConfig, OptimizerRule};

use datafusion_common::tree_node::{Transformed, TreeNodeRecursion, TreeNodeVisitor};
use datafusion_common::Result;
use datafusion_expr::logical_plan::JoinType;
use datafusion_expr::LogicalPlan;

/// Optimizer rule for rewriting lateral joins to joins
#[derive(Default, Debug)]
pub struct DecorrelateLateralJoin {}

impl DecorrelateLateralJoin {
    #[allow(missing_docs)]
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
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // Find cross joins with outer column references on the right side (i.e., the apply operator).
        let LogicalPlan::Join(join) = &plan else {
            return Ok(Transformed::no(plan));
        };
        if join.join_type != JoinType::Inner {
            return Ok(Transformed::no(plan));
        }
        // TODO: this makes the rule to be quadratic to the number of nodes, in theory, we can build this property
        // bottom-up.
        if !plan_contains_outer_reference(&join.right) {
            return Ok(Transformed::no(plan));
        }
        // The right side contains outer references, we need to decorrelate it.
        let LogicalPlan::Subquery(subquery) = &*join.right else {
            return Ok(Transformed::no(plan));
        };
        let alias = config.alias_generator();
        let Some(new_plan) = decorrelate_predicate_subquery::build_join(
            &join.left,
            subquery.subquery.as_ref(),
            None,
            join.join_type,
            alias.next("__lateral_sq"),
        )?
        else {
            return Ok(Transformed::no(plan));
        };
        Ok(Transformed::new(new_plan, true, TreeNodeRecursion::Jump))
    }

    fn name(&self) -> &str {
        "decorrelate_lateral_join"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

fn plan_contains_outer_reference(plan: &LogicalPlan) -> bool {
    struct Visitor {
        contains: bool,
    }
    impl<'n> TreeNodeVisitor<'n> for Visitor {
        type Node = LogicalPlan;
        fn f_down(&mut self, plan: &'n LogicalPlan) -> Result<TreeNodeRecursion> {
            if plan.contains_outer_reference() {
                self.contains = true;
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        }
    }
    let mut visitor = Visitor { contains: false };
    plan.visit_with_subqueries(&mut visitor).unwrap();
    visitor.contains
}
