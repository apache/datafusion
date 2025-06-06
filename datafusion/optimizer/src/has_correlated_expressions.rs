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

use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion_common::Result;
use datafusion_expr::{Expr, LogicalPlan};

#[allow(dead_code)]
#[derive(Debug)]
pub struct HasCorrelateExpressionsVisitor {
    pub has_correlated_expressions: bool,
    pub lateral: bool,

    correlated_columns: Vec<(usize, Expr)>,
    /// Tracks number of nested laterals.
    lateral_depth: usize,
}

impl HasCorrelateExpressionsVisitor {
    pub fn new(
        correlated_columns: &Vec<(usize, Expr)>,
        lateral: bool,
        lateral_depth: usize,
    ) -> Self {
        Self {
            has_correlated_expressions: false,
            lateral,
            correlated_columns: correlated_columns.clone(),
            lateral_depth,
        }
    }
}

impl<'n> TreeNodeVisitor<'n> for HasCorrelateExpressionsVisitor {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &'n Self::Node) -> Result<TreeNodeRecursion> {
        node.apply_expressions(|expr| {
            expr.apply_children(|expr| {
                let mut found_match = false;
                match expr {
                    Expr::OuterReferenceColumn(_, column) => {
                        // Indicates local correlations (all correlation within a child)
                        // for the root.
                        // TODO: add depth in OuterReferenceColumn
                        // if depth < self.lateral_depth
                        // {
                        //     return Ok(TreeNodeRecursion::Continue);
                        // }

                        // TODO: check if expr.depth > 1 + self.lateral_depth

                        // Note: This is added since we only want to
                        // set has_correlated_expression to true when the
                        // OuterReferenceColumn has the same bindings as one of the
                        // correlated_columns from the left hand side.
                        // (correlated_columns is the correlated_columns from left hand side).

                        for correlated_col in &self.correlated_columns {
                            if let Expr::OuterReferenceColumn(_, col) = &correlated_col.1
                            {
                                if *col == *column {
                                    found_match = true;
                                    break;
                                }
                            } else {
                                unreachable!()
                            }
                        }
                    }
                    Expr::Exists(exists) => {
                        found_match = find_match(
                            &self.correlated_columns,
                            &exists.subquery.outer_ref_columns,
                        );
                    }
                    Expr::InSubquery(in_subquery) => {
                        found_match = find_match(
                            &self.correlated_columns,
                            &in_subquery.subquery.outer_ref_columns,
                        )
                    }
                    Expr::ScalarSubquery(subquery) => {
                        found_match = find_match(
                            &self.correlated_columns,
                            &subquery.outer_ref_columns,
                        )
                    }
                    _ => {
                        return Ok(TreeNodeRecursion::Continue);
                    }
                }

                // Correlated column reference.
                self.has_correlated_expressions |= found_match;

                if self.has_correlated_expressions {
                    Ok(TreeNodeRecursion::Stop)
                } else {
                    Ok(TreeNodeRecursion::Continue)
                }
            })
        })
    }

    fn f_up(&mut self, _node: &Self::Node) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Stop)
    }
}

fn find_match(
    correlated_columns: &Vec<(usize, Expr)>,
    outer_ref_cols: &Vec<Expr>,
) -> bool {
    for (_, correlated_col) in correlated_columns {
        if let Expr::OuterReferenceColumn(_, column) = &correlated_col {
            for outer_expr in outer_ref_cols {
                if let Expr::OuterReferenceColumn(_, outer_ref_col) = outer_expr {
                    if *column == *outer_ref_col {
                        return true;
                    }
                } else {
                    unreachable!()
                }
            }
        } else {
            unreachable!()
        }
    }

    false
}
