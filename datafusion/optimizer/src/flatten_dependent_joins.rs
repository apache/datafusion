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
use std::sync::Arc;

use arrow::datatypes::DataType;

use crate::has_correlated_expressions::HasCorrelateExpressionsVisitor;
use crate::rewrite_correlated_expressions::CorrelatedExpressionsRewriter;
use crate::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{
    Transformed, TreeNode, TreeNodeContainer, TreeNodeRecursion, TreeNodeRewriter,
};
use datafusion_common::{Column, DFSchema, DFSchemaRef, DataFusionError, Result};
use datafusion_expr::{DelimGet, Expr, Join, JoinConstraint, JoinType, LogicalPlan};

#[derive(Debug, Clone)]
pub struct FlattenDependentJoinsRewriter {
    pub correlated_columns: Vec<(usize, Expr)>,
    pub has_correlated_expressions: HashMap<LogicalPlan, bool>,

    pub delim_types: Vec<DataType>,
    pub correlated_map: HashMap<Column, bool>,

    pub any_join: bool,
    pub base_table: DFSchemaRef,

    pub is_initial: bool,
}

#[allow(dead_code)]
impl FlattenDependentJoinsRewriter {
    /// Create a new rewriter with the given correlated columns
    fn new(
        correlated_columns: Vec<(usize, Expr)>,
        is_initial: bool,
    ) -> FlattenDependentJoinsRewriter {
        let delim_types = correlated_columns
            .iter()
            .map(|(_, expr)| {
                if let Expr::OuterReferenceColumn(data_type, _) = expr {
                    data_type.clone()
                } else {
                    todo!() // TODO: unreachable
                }
            })
            .collect();

        FlattenDependentJoinsRewriter {
            correlated_columns,
            has_correlated_expressions: HashMap::default(),
            delim_types,
            correlated_map: HashMap::new(),
            any_join: false,
            base_table: Arc::new(DFSchema::empty()),
            is_initial,
        }
    }

    pub fn detect_correlated_expressions(
        &mut self,
        plan: &LogicalPlan,
        lateral: bool,
        lateral_depth: usize,
        parent_is_dependent_join: bool,
    ) -> bool {
        let mut is_lateral_join = false;

        // Check if this entry has correlated expressions.
        if let LogicalPlan::DependentJoin(_) = plan {
            is_lateral_join = true;
        }

        let mut visitor = HasCorrelateExpressionsVisitor::new(
            &self.correlated_columns,
            lateral,
            lateral_depth,
        );
        let _ = plan.visit(&mut visitor);

        let mut has_correlation = visitor.has_correlated_expressions;
        let mut child_idx = 0;

        // Now visit the children of this entry and check if they have correlated expressions.
        let _ = plan.apply_children(|plan| {
            let mut new_lateral_depth = lateral_depth;
            if is_lateral_join && child_idx == 1 {
                new_lateral_depth = lateral_depth + 1;
            }

            // We Or the property with its children such that has_correlation is true if either
            // (1) this plan has a correlated expression or
            // (2) one of its children has a correlated expression
            let condition =
                (parent_is_dependent_join || is_lateral_join) && child_idx == 0;
            if self.detect_correlated_expressions(
                plan,
                lateral,
                new_lateral_depth,
                condition,
            ) {
                has_correlation = true;
            }
            child_idx += 1;

            Ok(TreeNodeRecursion::Stop)
        });

        // TODO: handle CTE

        // Set the entry in the map.
        // TODO: no clone
        self.has_correlated_expressions
            .insert(plan.clone(), has_correlation);

        // TODO: handle materialized or recursive CTE.

        return has_correlation;
    }

    fn push_down_dependent_join(
        &mut self,
        plan: LogicalPlan,
        mut propagate_null_values: bool,
        lateral_depth: usize,
    ) -> Result<Transformed<LogicalPlan>> {
        // TODO: check
        self.push_down_dependent_join_internal(
            plan,
            &mut propagate_null_values,
            lateral_depth,
        )
    }

    fn push_down_dependent_join_internal(
        &mut self,
        plan: LogicalPlan,
        parent_propagate_null_values: &mut bool,
        lateral_depth: usize,
    ) -> Result<Transformed<LogicalPlan>> {
        let mut _is_transformed = false;

        let is_correlated =
            self.has_correlated_expressions.get(&plan).ok_or_else(|| {
                DataFusionError::Internal(
                    "No correlated expressions entry found for plan".to_string(),
                )
            })?;

        let mut exit_projection = false;
        let mut delim_get = None;
        if !is_correlated {
            // We reached a node without correlated expressions.
            // We can eliminate the dependent join now and create a simple cross product.
            // Now create the duplicate eliminated scan for this node.

            // TODO: handle cte.

            // Create corss product with Dlim Join.
            // TODO: construct an empty schema here.
            self.base_table = Arc::new(DFSchema::empty());
            delim_get = Some(DelimGet::try_new(
                self.base_table.clone(),
                &self.delim_types,
            ));
            if let LogicalPlan::Projection(_) = plan {
                // We want the logical projection for positionality.
                exit_projection = true;
            } else {
                let new_plan = self.decorrelate(plan, true, 0)?.data;

                // Construct a cross product join.
                let join_plan = Join::try_new(
                    Arc::new(new_plan),
                    Arc::new(LogicalPlan::DelimGet(delim_get.unwrap())),
                    vec![],
                    None,
                    JoinType::Inner,
                    JoinConstraint::On,
                    false,
                )?;

                return Ok(Transformed::yes(LogicalPlan::Join(join_plan)));
            }
        }

        match plan {
            LogicalPlan::Filter(_) | LogicalPlan::Union(_) => {
                plan.apply_expressions(|expr| {
                    self.any_join |= contains_subquery(expr);
                    Ok(TreeNodeRecursion::Continue)
                });
            }
            _ => {}
        }

        let mut new_plan = plan;
        match new_plan {
            LogicalPlan::Filter(filter) => {
                // First we flatten the dependeint join in the child of the filter.
                let transform = self.push_down_dependent_join(
                    filter.input.as_ref().clone(),
                    *parent_propagate_null_values,
                    lateral_depth,
                )?;
                _is_transformed |= transform.transformed;
                new_plan =
                    LogicalPlan::Filter(filter.with_new_input(Arc::new(transform.data)));

                // Then we replace any correlated expressions with the corresponding entry in the
                // correlated_map.
                let mut rewriter = CorrelatedExpressionsRewriter::new(
                    self.base_table.clone(),
                    self.correlated_map.clone(),
                    lateral_depth,
                    false,
                );
                let mut transform = rewriter.rewrite_plan(new_plan)?;
                transform.transformed |= _is_transformed;
                return Ok(transform);
            }
            LogicalPlan::Projection(ref projection) => {
                // First we flatten the dependent join in the child of the projection.
                projection.expr.apply_elements(|expr| {
                    *parent_propagate_null_values &= propagate_null_values(expr);
                    Ok(TreeNodeRecursion::Continue)
                });

                // If outer immediate children is a dependent join, the projection expressions did
                // contain a subquery expression previously-Which does not propagate null values.
                // We have to account for that.
                let child_is_dependent_join =
                    matches!(*projection.input, LogicalPlan::DependentJoin(_));
                *parent_propagate_null_values &= !child_is_dependent_join;

                // If the node has no correlated expressions, push the cross product with the
                // delim get only below the projection.
                // This will preserve positionality of the columns and prevent errors when
                // reordering of delim gets is enabled.
                if exit_projection {
                    if let Some(delim_get) = delim_get {
                        _is_transformed = true;
                        let transform =
                            self.decorrelate(projection.input.as_ref().clone(), true, 0)?;
                        let join_plan = Join::try_new(
                            Arc::new(transform.data),
                            Arc::new(LogicalPlan::DelimGet(delim_get)),
                            vec![],
                            None,
                            JoinType::Inner,
                            JoinConstraint::On,
                            false,
                        )?;

                        new_plan =
                            new_plan.with_new_inputs(vec![LogicalPlan::Projection(
                                projection.clone().with_new_input(Arc::new(
                                    LogicalPlan::Join(join_plan),
                                )),
                            )])?;
                    } else {
                        unreachable!()
                    }
                } else {
                    let transform = self.push_down_dependent_join_internal(
                        projection.input.as_ref().clone(),
                        parent_propagate_null_values,
                        lateral_depth,
                    )?;
                    _is_transformed |= transform.transformed;

                    new_plan =
                        new_plan.with_new_inputs(vec![LogicalPlan::Projection(
                            projection.clone().with_new_input(Arc::new(transform.data)),
                        )])?;
                }

                // Then we replace any correlated expressions with the corresponding entry in the
                // correlated_map.
                let mut rewriter = CorrelatedExpressionsRewriter::new(
                    self.base_table.clone(),
                    self.correlated_map.clone(),
                    lateral_depth,
                    false,
                );
                let transform = rewriter.rewrite_plan(new_plan)?;
                _is_transformed |= transform.transformed;
                new_plan = transform.data;

                // Now we add all the columns of the delim_get to the projection list.
                let outer_cols = self.collect_outer_cols();
                new_plan = new_plan.with_new_exprs(outer_cols)?;

                let mut transform = Transformed::no(new_plan);
                transform.transformed = _is_transformed;
                return Ok(transform);
            }
            LogicalPlan::TableScan(_table_scan) => {
                // TODO: is is reachable?
                unreachable!()
                // // Get current schema
                // let mut schema = DFSchema::new_with_metadata(
                //     table_scan.projected_schema.fields().to_vec(),
                //     table_scan.projected_schema.metadata().clone(),
                // )?;

                // // Add correlated columns to schema
                // for (_, expr) in self.correlated_columns.iter() {
                //     if let Expr::Column(col) = expr {
                //         schema.add_column(
                //             col.relation.clone(),
                //             col.name.clone(),
                //             col.data_type(),
                //             true,
                //         )?;
                //     }
                // }

                // // Create new table scan with updated schema
                // let new_table_scan =
                //     table_scan.with_projected_schema_ref(Arc::new(schema))?;
                // Ok(Transformed::yes(LogicalPlan::TableScan(new_table_scan)))
            }
            LogicalPlan::Sort(sort) => {
                let transform = self.push_down_dependent_join(
                    sort.input.as_ref().clone(),
                    *parent_propagate_null_values,
                    lateral_depth,
                )?;
                new_plan =
                    LogicalPlan::Sort(sort.with_new_input(Arc::new(transform.data)));
                return Ok(Transformed::yes(new_plan));
            }
            LogicalPlan::Join(join) => {
                // Check the correlated expressions in the children of the join.
                let left_has_correlation = if let Some(res) =
                    self.has_correlated_expressions.get(join.left.as_ref())
                {
                    *res
                } else {
                    unreachable!() // TODO
                };

                let right_has_correlation = if let Some(res) =
                    self.has_correlated_expressions.get(join.right.as_ref())
                {
                    *res
                } else {
                    unreachable!() // TODO
                };

                match join.join_type {
                    JoinType::Inner => {
                        if !right_has_correlation {
                            // Only left has correlation: push into left.
                            let transform_left = self.push_down_dependent_join(
                                join.left.as_ref().clone(),
                                *parent_propagate_null_values,
                                lateral_depth,
                            )?;
                            _is_transformed |= transform_left.transformed;

                            let transform_right = self
                                .decorrelate_independent(join.right.as_ref().clone())?;
                            _is_transformed |= transform_right.transformed;

                            new_plan = LogicalPlan::Join(join.with_new_left_right(
                                Arc::new(transform_left.data),
                                Arc::new(transform_right.data),
                            ));

                            // Remove the correlated columns coming from outside for
                            // current join node.
                            return Ok(Transformed {
                                data: new_plan,
                                transformed: _is_transformed,
                                tnr: TreeNodeRecursion::Continue,
                            });
                        }

                        if !left_has_correlation {
                            // Only rigth has correlation: push into right.
                            let transform_right = self.push_down_dependent_join(
                                join.right.as_ref().clone(),
                                *parent_propagate_null_values,
                                lateral_depth,
                            )?;
                            _is_transformed |= transform_right.transformed;

                            let transform_left =
                                self.decorrelate_independent(join.left.as_ref().clone())?;
                            _is_transformed |= transform_left.transformed;

                            new_plan = LogicalPlan::Join(join.with_new_left_right(
                                Arc::new(transform_left.data),
                                Arc::new(transform_right.data),
                            ));

                            // Remove the correlated columns coming from outside for
                            // current join node.
                            return Ok(Transformed {
                                data: new_plan,
                                transformed: _is_transformed,
                                tnr: TreeNodeRecursion::Continue,
                            });
                        }
                    }
                    JoinType::Left => {
                        // Left outer join.
                        if !right_has_correlation {
                            // Only left has correlation: push into left.
                            let transform_left = self.push_down_dependent_join(
                                join.left.as_ref().clone(),
                                *parent_propagate_null_values,
                                lateral_depth,
                            )?;
                            _is_transformed |= transform_left.transformed;

                            let transfrom_right = self
                                .decorrelate_independent(join.right.as_ref().clone())?;
                            _is_transformed |= transfrom_right.transformed;

                            new_plan = LogicalPlan::Join(join.with_new_left_right(
                                Arc::new(transform_left.data),
                                Arc::new(transfrom_right.data),
                            ));

                            // Remove the correlated columns coming from outside for current join
                            // plan.
                            return Ok(Transformed {
                                data: new_plan,
                                transformed: _is_transformed,
                                tnr: TreeNodeRecursion::Continue,
                            });
                        }
                    }
                    JoinType::Right => {
                        // Right outer join.
                        if !left_has_correlation {
                            // Only right has correlation: push into right.
                            let transform_right = self.push_down_dependent_join(
                                join.right.as_ref().clone(),
                                *parent_propagate_null_values,
                                lateral_depth,
                            )?;
                            _is_transformed |= transform_right.transformed;

                            let transform_left =
                                self.decorrelate_independent(join.left.as_ref().clone())?;
                            _is_transformed |= transform_left.transformed;

                            new_plan = LogicalPlan::Join(join.with_new_left_right(
                                Arc::new(transform_left.data),
                                Arc::new(transform_right.data),
                            ));

                            return Ok(Transformed {
                                data: new_plan,
                                transformed: _is_transformed,
                                tnr: TreeNodeRecursion::Continue,
                            });
                        }
                    }
                    JoinType::LeftMark => {
                        if right_has_correlation {
                            unreachable!()
                        }

                        // Push the child into the LHS.
                        let transform_left = self.push_down_dependent_join(
                            join.left.as_ref().clone(),
                            *parent_propagate_null_values,
                            lateral_depth,
                        )?;
                        _is_transformed |= transform_left.transformed;

                        let transform_right =
                            self.decorrelate_independent(join.right.as_ref().clone())?;
                        _is_transformed |= transform_right.transformed;

                        new_plan = LogicalPlan::Join(join.with_new_left_right(
                            Arc::new(transform_left.data),
                            Arc::new(transform_right.data),
                        ));

                        // Rewrite expressions in the join condition.
                        let mut rewriter = CorrelatedExpressionsRewriter::new(
                            self.base_table.clone(),
                            self.correlated_map.clone(),
                            lateral_depth,
                            false,
                        );
                        let transform = rewriter.rewrite_plan(new_plan)?;
                        _is_transformed |= _is_transformed;
                        return Ok(transform);
                    }
                    _ => unreachable!(
                        "Unsupported join type for flattening correlated subquery"
                    ),
                }

                // Both sides have correlation.
                // Push into both sides.
                let transform_left = self.push_down_dependent_join(
                    join.left.as_ref().clone(),
                    *parent_propagate_null_values,
                    lateral_depth,
                )?;
                _is_transformed |= transform_left.transformed;

                let transform_right = self.push_down_dependent_join(
                    join.right.as_ref().clone(),
                    *parent_propagate_null_values,
                    lateral_depth,
                )?;
                _is_transformed |= transform_right.transformed;

                // TODO
                // // NOTE:
                // // for OUTER JOIN, it matters what the BASE BINDING is after the join
                // // for the LEEF OUTER JOIN, we want the LEFT side to be the base binding after we
                // // push because the RIGHT binding might contain NULL values.
                // match join.join_type {
                //     JoinType::Left => {}
                //     JoinType::Right => {}
                //     _ => {}
                // }

                // Add the correlated columns to the join conditions.

                // Then we replace any correlated expressions with the corresponding entry in the
                // correlated_map.

                todo!()
            }
            _ => todo!(),
        }
    }

    pub fn decorrelate_independent(
        &mut self,
        plan: LogicalPlan,
    ) -> Result<Transformed<LogicalPlan>> {
        let mut flatten = FlattenDependentJoinsRewriter::new(vec![], true);

        flatten.decorrelate(plan, true, 0)
    }

    fn decorrelate(
        &mut self,
        plan: LogicalPlan,
        parent_propagate_null_values: bool,
        lateral_depth: usize,
    ) -> Result<Transformed<LogicalPlan>> {
        let mut is_transformed = false;

        let mut new_plan = plan;
        if let LogicalPlan::DependentJoin(dependent_join) = new_plan.clone() {
            // If we have a parent, we unnest the left side of the dependent join in
            // the parent's context.
            if !self.is_initial {
                // only push the dependent join to the left side, if there is correlation.

                if *self
                    .has_correlated_expressions
                    .get(&new_plan)
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "No correlated expressions entry found for plan".to_string(),
                        )
                    })?
                {
                    let transform = self.push_down_dependent_join(
                        dependent_join.left.as_ref().clone(),
                        parent_propagate_null_values,
                        lateral_depth,
                    )?;
                    is_transformed |= transform.transformed;
                    new_plan = LogicalPlan::DependentJoin(
                        dependent_join.clone().with_new_left(transform.data),
                    );
                } else {
                    // There might be unrelated correlation, so we have to traverse the tree.
                    let transfrom = self
                        .decorrelate_independent(dependent_join.left.as_ref().clone())?;
                    is_transformed |= transfrom.transformed;
                    new_plan = LogicalPlan::DependentJoin(
                        dependent_join.clone().with_new_left(transfrom.data),
                    );
                }

                // // rewrite
                // let lateral_depth = 0;
                // let mut rewriter = CorrelatedExpressionsRewriter::new(
                //     self.base_table.clone(),
                //     self.correlated_columns.clone(),
                //     lateral_depth,
                //     false,
                // );
                // let transform = rewriter.rewrite_plan(new_plan)?;
                // is_transformed = transform.transformed;
                // new_plan = transform.data;
            }

            let lateral_depth = 0;
            let propagate_null_values = false; // TODO: fetch from dependent join
            let mut flatten = FlattenDependentJoinsRewriter::new(
                dependent_join.correlated_columns.clone(),
                self.is_initial,
            );

            // First we check which logical plan have correlated expressions in the first place.
            flatten.detect_correlated_expressions(
                dependent_join.right.as_ref(),
                false, // TODO: fetch from dependent join
                lateral_depth,
                false,
            );

            // Now we push the dependent join down.
            let transform = flatten.push_down_dependent_join(
                dependent_join.right.as_ref().clone(),
                propagate_null_values,
                lateral_depth,
            )?;
            is_transformed |= transform.transformed;
        } else {
            let mut inputs = vec![];
            for input in new_plan.inputs().iter() {
                let transform = self.decorrelate(
                    input.clone().clone(),
                    parent_propagate_null_values,
                    lateral_depth,
                )?;
                is_transformed |= transform.transformed;
                inputs.push(transform.data);
            }
            new_plan = new_plan.with_new_inputs(inputs)?;
        }

        if is_transformed {
            Ok(Transformed::yes(new_plan))
        } else {
            Ok(Transformed::no(new_plan))
        }
    }

    fn collect_outer_cols(&self) -> Vec<Expr> {
        let outer_cols: Vec<Expr> = self
            .correlated_columns
            .iter()
            .map(|(_, expr)| expr.clone())
            .collect();

        outer_cols
    }
}

// If current expr contains any subquery expr this function must not be recursive.
fn contains_subquery(expr: &Expr) -> bool {
    expr.exists(|expr| {
        Ok(matches!(
            expr,
            Expr::ScalarSubquery(_) | Expr::InSubquery(_) | Expr::Exists(_)
        ))
    })
    .expect("Inner is always Ok")
}

fn propagate_null_values(expr: &Expr) -> bool {
    let mut propagate_null_values = true;
    expr.apply_children(|expr| match expr {
        Expr::IsNotNull(_)
        | Expr::IsNull(_)
        | Expr::IsTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsUnknown(_)
        | Expr::IsNotTrue(_)
        | Expr::IsNotFalse(_)
        | Expr::IsNotUnknown(_) => {
            propagate_null_values = false;
            Ok(TreeNodeRecursion::Stop)
        }
        _ => Ok(TreeNodeRecursion::Continue),
    });

    propagate_null_values
}

impl TreeNodeRewriter for FlattenDependentJoinsRewriter {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        Ok(Transformed::no(node))
    }
}

#[derive(Default, Debug)]
pub struct FlattenDependentJoins {}

impl OptimizerRule for FlattenDependentJoins {
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "flatten_dependent_joins"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let mut rewriter = FlattenDependentJoinsRewriter::new(vec![], true);
        rewriter.decorrelate_independent(plan)
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }
}
