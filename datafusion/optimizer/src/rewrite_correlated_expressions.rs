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

use std::collections::HashMap;

use datafusion_common::tree_node::{
    Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
};
use datafusion_common::{Column, DFSchemaRef, Result};
use datafusion_expr::{Expr, LogicalPlan};

// Helper to rewrite correlated expressions within a single LogicalPlan.
#[derive(Debug)]
pub struct CorrelatedExpressionsRewriter {
    base_table: DFSchemaRef,
    correlated_map: HashMap<Column, bool>,
    // To keep track of the number of dependent joins encountered.
    lateral_depth: usize,
    // This flag is used to determine if the rewrite should recursively update
    // for all OuterRefColumn in the plan.
    recursive_rewrite: bool,
}

impl CorrelatedExpressionsRewriter {
    pub fn new(
        base_table: DFSchemaRef,
        correlated_map: HashMap<Column, bool>,
        lateral_depth: usize,
        recursive_rewrite: bool,
    ) -> Self {
        Self {
            base_table,
            correlated_map,
            lateral_depth,
            recursive_rewrite,
        }
    }

    pub fn rewrite_plan(&mut self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let mut new_plan = plan;

        if self.recursive_rewrite {
            // Update OuterRefColumn from left child of lateral to right child.
            if let LogicalPlan::DependentJoin(dependent_join) = new_plan {
                let left_new_transform =
                    self.rewrite_plan(dependent_join.left.as_ref().clone())?;
                self.lateral_depth += 1;
                let right_new_transfrom =
                    self.rewrite_plan(dependent_join.right.as_ref().clone())?;
                self.lateral_depth -= 1;
                new_plan =
                    LogicalPlan::DependentJoin(dependent_join.with_new_left_right(
                        left_new_transform.data,
                        right_new_transfrom.data,
                    ));
            }
        } else {
            let mut inputs = vec![];
            for input in new_plan.inputs().iter() {
                inputs.push(self.rewrite_plan(input.clone().clone())?.data);
            }
            new_plan = new_plan.with_new_inputs(inputs)?;
        }

        new_plan.rewrite(self)
    }
}

impl TreeNodeRewriter for CorrelatedExpressionsRewriter {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let mut new_node = node;
        let mut is_transformed = false;
        let mut new_expr = None;
        new_node.apply_expressions(|expr| {
            match expr {
                // Subquery detected within this subquery.
                // Recursively rewrite it using the [`CorrelatedRecursiveRewriter`].
                Expr::Exists(_exists) => todo!(),
                Expr::InSubquery(_in_subquery) => todo!(),
                Expr::ScalarSubquery(_subquery) => todo!(),
                Expr::OuterReferenceColumn(data_type, column) => {
                    // TODO: check depth
                    // if dpeth <= self.lateral_depth {
                    //     // Indicates local correlations not relevant for the current rewrite.
                    //     return Ok(TreeNodeRecursion::Continue);
                    // }

                    // Correlated column.
                    // Replace with the entry referring to the duplicate eliminated scan.
                    // TODO: check depth again.

                    // TODO: construct new column for outer reference column.
                    is_transformed = true;
                    new_expr = Some(Expr::OuterReferenceColumn(
                        data_type.clone(),
                        column.clone(),
                    ));

                    Ok(TreeNodeRecursion::Continue)
                }
                _ => Ok(TreeNodeRecursion::Continue),
            }
        });

        if let Some(new_expr) = new_expr {
            new_node = new_node.with_new_exprs(vec![new_expr])?;
        }

        if is_transformed {
            Ok(Transformed::yes(new_node))
        } else {
            Ok(Transformed::no(new_node))
        }
    }

    fn f_up(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        Ok(Transformed::no(node))
    }
}
