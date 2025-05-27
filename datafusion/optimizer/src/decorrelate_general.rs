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

//! [`DependentJoinRewriter`] converts correlated subqueries to `DependentJoin`

use std::ops::Deref;
use std::sync::Arc;

use crate::{ApplyOrder, OptimizerConfig, OptimizerRule};

use arrow::datatypes::DataType;
use datafusion_common::alias::AliasGenerator;
use datafusion_common::tree_node::{
    Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
};
use datafusion_common::{internal_err, Column, HashMap, Result};
use datafusion_expr::{col, lit, Expr, LogicalPlan, LogicalPlanBuilder};

use indexmap::IndexMap;
use itertools::Itertools;

pub struct DependentJoinRewriter {
    // each logical plan traversal will assign it a integer id
    current_id: usize,
    subquery_depth: usize,
    // each newly visted `LogicalPlan` is inserted inside this map for tracking
    nodes: IndexMap<usize, Node>,
    // all the node ids from root to the current node
    // this is mutated duri traversal
    stack: Vec<usize>,
    // track for each column, the nodes/logical plan that reference to its within the tree
    all_outer_ref_columns: IndexMap<Column, Vec<ColumnAccess>>,
    alias_generator: Arc<AliasGenerator>,
}

#[derive(Debug, Hash, PartialEq, PartialOrd, Eq, Clone)]
struct ColumnAccess {
    // node ids from root to the node that is referencing the column
    stack: Vec<usize>,
    // the node referencing the column
    node_id: usize,
    col: Column,
    data_type: DataType,
    subquery_depth: usize,
}

impl DependentJoinRewriter {
    // lowest common ancestor from stack
    // given a tree of
    // n1
    // |
    // n2 filter where outer.column = exists(subquery)
    // ----------------------
    // |                    \
    // |                    n5: subquery
    // |                        |
    // n3 scan table outer   n6 filter outer.column=inner.column
    //                          |
    //                      n7 scan table inner
    // this function is called with 2 args a:[1,2,3] and [1,2,5,6,7]
    // it then returns the id of the dependent join node (2)
    // and the id of the subquery node (5)
    fn dependent_join_and_subquery_node_ids(
        stack_with_table_provider: &[usize],
        stack_with_subquery: &[usize],
    ) -> (usize, usize) {
        let mut lowest_common_ancestor = 0;
        let mut subquery_node_id = 0;

        let min_len = stack_with_table_provider
            .len()
            .min(stack_with_subquery.len());

        for i in 0..min_len {
            let right_id = stack_with_subquery[i];
            let left_id = stack_with_table_provider[i];

            if right_id == left_id {
                // common parent
                lowest_common_ancestor = right_id;
                subquery_node_id = stack_with_subquery[i + 1];
            } else {
                break;
            }
        }

        (lowest_common_ancestor, subquery_node_id)
    }

    // because the column providers are visited after column-accessor
    // (function visit_with_subqueries always visit the subquery before visiting the other children)
    // we can always infer the LCA inside this function, by getting the deepest common parent
    fn conclude_lowest_dependent_join_node_if_any(
        &mut self,
        child_id: usize,
        col: &Column,
    ) {
        if let Some(accesses) = self.all_outer_ref_columns.get(col) {
            for access in accesses.iter() {
                let mut cur_stack = self.stack.clone();

                cur_stack.push(child_id);
                let (dependent_join_node_id, subquery_node_id) =
                    Self::dependent_join_and_subquery_node_ids(&cur_stack, &access.stack);
                let node = self.nodes.get_mut(&dependent_join_node_id).unwrap();
                let accesses = node
                    .columns_accesses_by_subquery_id
                    .entry(subquery_node_id)
                    .or_default();
                accesses.push(ColumnAccess {
                    col: col.clone(),
                    node_id: access.node_id,
                    stack: access.stack.clone(),
                    data_type: access.data_type.clone(),
                    subquery_depth: access.subquery_depth,
                });
            }
        }
    }

    fn mark_outer_column_access(
        &mut self,
        child_id: usize,
        data_type: &DataType,
        col: &Column,
    ) {
        // iter from bottom to top, the goal is to mark the dependent node
        // the current child's access
        self.all_outer_ref_columns
            .entry(col.clone())
            .or_default()
            .push(ColumnAccess {
                stack: self.stack.clone(),
                node_id: child_id,
                col: col.clone(),
                data_type: data_type.clone(),
                subquery_depth: self.subquery_depth,
            });
    }
    fn rewrite_subqueries_into_dependent_joins(
        &mut self,
        plan: LogicalPlan,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.rewrite_with_subqueries(self)
    }
}

impl DependentJoinRewriter {
    fn new(alias_generator: Arc<AliasGenerator>) -> Self {
        DependentJoinRewriter {
            alias_generator,
            current_id: 0,
            nodes: IndexMap::new(),
            stack: vec![],
            all_outer_ref_columns: IndexMap::new(),
            subquery_depth: 0,
        }
    }
}

#[derive(Debug, Clone)]
struct Node {
    plan: LogicalPlan,

    // This field is only meaningful if the node is dependent join node.
    // It tracks which descendent nodes still accessing the outer columns provided by its
    // left child
    // The key of this map is node_id of the children subqueries.
    // The insertion order matters here, and thus we use IndexMap
    columns_accesses_by_subquery_id: IndexMap<usize, Vec<ColumnAccess>>,

    is_dependent_join_node: bool,

    // note that for dependent join nodes, there can be more than 1
    // subquery children at a time, but always 1 outer-column-providing-child
    // which is at the last element
    subquery_type: SubqueryType,
}
#[derive(Debug, Clone)]
enum SubqueryType {
    None,
    In,
    Exists,
    Scalar,
    LateralJoin,
}

impl SubqueryType {
    fn prefix(&self) -> String {
        match self {
            SubqueryType::None => "",
            SubqueryType::In => "__in_sq",
            SubqueryType::Exists => "__exists_sq",
            SubqueryType::Scalar => "__scalar_sq",
            SubqueryType::LateralJoin => "__lateral_sq",
        }
        .to_string()
    }
}
fn unwrap_subquery_input_from_expr(expr: &Expr) -> Arc<LogicalPlan> {
    match expr {
        Expr::ScalarSubquery(sq) => Arc::clone(&sq.subquery),
        Expr::Exists(exists) => Arc::clone(&exists.subquery.subquery),
        Expr::InSubquery(in_sq) => Arc::clone(&in_sq.subquery.subquery),
        _ => unreachable!(),
    }
}

// if current expr contains any subquery expr
// this function must not be recursive
fn contains_subquery(expr: &Expr) -> bool {
    expr.exists(|expr| {
        Ok(matches!(
            expr,
            Expr::ScalarSubquery(_) | Expr::InSubquery(_) | Expr::Exists(_)
        ))
    })
    .expect("Inner is always Ok")
}

/// The rewriting happens up-down, where the parent nodes are downward-visited
/// before its children (subqueries children are visited first).
/// This behavior allow the fact that, at any moment, if we observe a `LogicalPlan`
/// that provides the data for columns, we can assume that all subqueries that reference
/// its data were already visited, and we can conclude the information of the `DependentJoin`
/// needed for the decorrelation:
/// - The subquery expr
/// - The correlated columns on the LHS referenced from the RHS (and its recursing subqueries if any)
///
/// If in the original node there exists multiple subqueries at the same time
/// two nested `DependentJoin` plans are generated (with equal depth).
///
/// For illustration, given this query
/// ```sql
/// SELECT ID FROM T1 WHERE EXISTS(SELECT * FROM T2 WHERE T2.ID=T1.ID) OR EXISTS(SELECT * FROM T2 WHERE T2.VALUE=T1.ID);
/// ```
///
/// The traversal happens in the following sequence
///
/// ```text
///                   ↓1   
///                   ↑12   
///              ┌────────────┐
///              │  FILTER    │<--- DependentJoin rewrite
///              │            │     happens here
///              └────┬────┬──┘
///             ↓2    ↓6   ↓10
///             ↑5    ↑9   ↑11 <---Here we already have enough information
///              │     |     |     of which node is accessing which column
///              │     |     |     provided by "Table Scan t1" node
///              │     |     |
///        ┌─────┘     │     └─────┐
///        │           │           │
///    ┌───▼───┐    ┌──▼───┐   ┌───▼───────┐
///    │SUBQ1  │    │SUBQ2 │   │TABLE SCAN │
///    └──┬────┘    └──┬───┘   │    t1     │
///      ↓3           ↓7       └───────────┘
///      ↑4           ↑8
///   ┌──▼────┐    ┌──▼────┐
///   │SCAN t2│    │SCAN t2│
///   └───────┘    └───────┘
/// ```
impl TreeNodeRewriter for DependentJoinRewriter {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let new_id = self.current_id;
        self.current_id += 1;
        let mut is_dependent_join_node = false;
        let mut subquery_type = SubqueryType::None;
        // for each node, find which column it is accessing, which column it is providing
        // Set of columns current node access
        match &node {
            LogicalPlan::Filter(f) => {
                if contains_subquery(&f.predicate) {
                    is_dependent_join_node = true;
                }

                f.predicate
                    .apply(|expr| {
                        if let Expr::OuterReferenceColumn(data_type, col) = expr {
                            self.mark_outer_column_access(new_id, data_type, col);
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })
                    .expect("traversal is infallible");
            }
            // TODO: maybe there are more logical plan that provides columns
            // aside from TableScan
            LogicalPlan::TableScan(tbl_scan) => {
                tbl_scan.projected_schema.columns().iter().for_each(|col| {
                    self.conclude_lowest_dependent_join_node_if_any(new_id, col);
                });
            }
            // Similar to TableScan, this node may provide column names which
            // is referenced inside some subqueries
            LogicalPlan::SubqueryAlias(alias) => {
                alias.schema.columns().iter().for_each(|col| {
                    self.conclude_lowest_dependent_join_node_if_any(new_id, col);
                });
            }
            // TODO: this is untested
            LogicalPlan::Projection(proj) => {
                for expr in &proj.expr {
                    if contains_subquery(expr) {
                        is_dependent_join_node = true;
                        break;
                    }
                    expr.apply(|expr| {
                        if let Expr::OuterReferenceColumn(data_type, col) = expr {
                            self.mark_outer_column_access(new_id, data_type, col);
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })?;
                }
            }
            LogicalPlan::Subquery(subquery) => {
                let parent = self.stack.last().unwrap();
                let parent_node = self.nodes.get_mut(parent).unwrap();
                // the inserting sequence matter here
                // when a parent has multiple children subquery at the same time
                // we rely on the order in which subquery children are visited
                // to later on find back the corresponding subquery (if some part of them
                // were rewritten in the lower node)
                parent_node
                    .columns_accesses_by_subquery_id
                    .insert(new_id, vec![]);

                if let LogicalPlan::Join(_) = parent_node.plan {
                    subquery_type = SubqueryType::LateralJoin;
                } else {
                    for expr in parent_node.plan.expressions() {
                        expr.exists(|e| {
                            let (found_sq, checking_type) = match e {
                                Expr::ScalarSubquery(sq) => {
                                    if sq == subquery {
                                        (true, SubqueryType::Scalar)
                                    } else {
                                        (false, SubqueryType::None)
                                    }
                                }
                                Expr::Exists(exist) => {
                                    if &exist.subquery == subquery {
                                        (true, SubqueryType::Exists)
                                    } else {
                                        (false, SubqueryType::None)
                                    }
                                }
                                Expr::InSubquery(in_sq) => {
                                    if &in_sq.subquery == subquery {
                                        (true, SubqueryType::In)
                                    } else {
                                        (false, SubqueryType::None)
                                    }
                                }
                                _ => (false, SubqueryType::None),
                            };
                            if found_sq {
                                subquery_type = checking_type;
                            }

                            Ok(found_sq)
                        })?;
                    }
                }
            }
            LogicalPlan::Aggregate(_) => {}
            LogicalPlan::Join(join) => {
                let mut sq_count = if let LogicalPlan::Subquery(_) = &join.left.as_ref() {
                    1
                } else {
                    0
                };
                sq_count += if let LogicalPlan::Subquery(_) = join.right.as_ref() {
                    1
                } else {
                    0
                };
                match sq_count {
                    0 => {}
                    1 => {
                        is_dependent_join_node = true;
                    }
                    _ => {
                        return internal_err!(
                            "plan error: join logical plan has both children with type \
                            Subquery"
                        );
                    }
                };

                if is_dependent_join_node {
                    self.subquery_depth += 1;
                    self.stack.push(new_id);
                    self.nodes.insert(
                        new_id,
                        Node {
                            plan: node.clone(),
                            is_dependent_join_node,
                            columns_accesses_by_subquery_id: IndexMap::new(),
                            subquery_type,
                        },
                    );

                    // we assume that RHS is always a subquery for the join
                    // and because this function assume that subquery side is visited first
                    // during f_down, we have to visit it at this step, else
                    // the function visit_with_subqueries will call f_down for the LHS instead
                    let transformed_subquery = self
                        .rewrite_subqueries_into_dependent_joins(
                            join.right.deref().clone(),
                        )?
                        .data;
                    let transformed_left = self
                        .rewrite_subqueries_into_dependent_joins(
                            join.left.deref().clone(),
                        )?
                        .data;
                    let mut new_join_node = join.clone();
                    new_join_node.right = Arc::new(transformed_subquery);
                    new_join_node.left = Arc::new(transformed_left);
                    return Ok(Transformed::new(
                        LogicalPlan::Join(new_join_node),
                        true,
                        // since we rewrite the children directly in this function,
                        TreeNodeRecursion::Jump,
                    ));
                }
            }
            _ => {
                return internal_err!("impl f_down for node type {:?}", node);
            }
        };

        if is_dependent_join_node {
            self.subquery_depth += 1
        }
        self.stack.push(new_id);
        self.nodes.insert(
            new_id,
            Node {
                plan: node.clone(),
                is_dependent_join_node,
                columns_accesses_by_subquery_id: IndexMap::new(),
                subquery_type,
            },
        );

        Ok(Transformed::no(node))
    }

    /// All rewrite happens inside upward traversal
    /// and only happens if the node is a "dependent join node"
    /// (i.e the node with at least one subquery expr)
    /// When all dependency information are already collected
    fn f_up(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        // if the node in the f_up meet any node in the stack, it means that node itself
        // is a dependent join node,transformation by
        // build a join based on
        let current_node_id = self.stack.pop().unwrap();
        let node_info = self.nodes.get(&current_node_id).unwrap();
        if !node_info.is_dependent_join_node {
            return Ok(Transformed::no(node));
        }
        let current_subquery_depth = self.subquery_depth;
        self.subquery_depth -= 1;

        let cloned_input = (**node.inputs().first().unwrap()).clone();
        let mut current_plan = LogicalPlanBuilder::new(cloned_input);
        let mut subquery_alias_by_offset = HashMap::new();
        let mut subquery_expr_by_offset = HashMap::new();
        for (subquery_offset, (subquery_id, _)) in
            node_info.columns_accesses_by_subquery_id.iter().enumerate()
        {
            let subquery_node = self.nodes.get(subquery_id).unwrap();
            let alias = self
                .alias_generator
                .next(&subquery_node.subquery_type.prefix());
            subquery_alias_by_offset.insert(subquery_offset, alias);
        }

        match &node {
            LogicalPlan::Projection(_) => {
                // TODO: implement me
            }
            LogicalPlan::Join(join) => {
                assert!(node_info.columns_accesses_by_subquery_id.len() == 1);
                let (_, column_accesses) =
                    node_info.columns_accesses_by_subquery_id.first().unwrap();
                let alias = subquery_alias_by_offset.get(&0).unwrap();
                let correlated_columns = column_accesses
                    .iter()
                    .map(|ac| {
                        (
                            ac.subquery_depth,
                            Expr::OuterReferenceColumn(
                                ac.data_type.clone(),
                                ac.col.clone(),
                            ),
                        )
                    })
                    .unique()
                    .collect();

                let subquery_plan = &join.right;
                let sq = if let LogicalPlan::Subquery(sq) = subquery_plan.as_ref() {
                    sq
                } else {
                    return internal_err!(
                        "lateral join must have right join as a subquery"
                    );
                };
                let right = sq.subquery.deref().clone();
                // At the time of implementation lateral join condition is not fully clear yet
                // So a TODO for future tracking
                let lateral_join_condition = if let Some(ref filter) = join.filter {
                    filter.clone()
                } else {
                    lit(true)
                };
                current_plan = current_plan.dependent_join(
                    right,
                    correlated_columns,
                    None,
                    current_subquery_depth,
                    alias.to_string(),
                    Some((join.join_type, lateral_join_condition)),
                )?;
            }
            LogicalPlan::Filter(filter) => {
                // everytime we meet a subquery during traversal, we increment this by 1
                // we can use this offset to lookup the original subquery info
                // in subquery_alias_by_offset
                // the reason why we cannot create a hashmap keyed by Subquery object HashMap<Subquery,String>
                // is that the subquery inside this filter expr may have been rewritten in
                // the lower level
                let mut offset = 0;
                let offset_ref = &mut offset;
                let new_predicate = filter
                    .predicate
                    .clone()
                    .transform(|e| {
                        // replace any subquery expr with subquery_alias.output
                        // column
                        let alias = match e {
                            Expr::InSubquery(_)
                            | Expr::Exists(_)
                            | Expr::ScalarSubquery(_) => {
                                subquery_alias_by_offset.get(offset_ref).unwrap()
                            }
                            _ => return Ok(Transformed::no(e)),
                        };
                        // we are aware that the original subquery can be rewritten
                        // update the latest expr to this map
                        subquery_expr_by_offset.insert(*offset_ref, e);
                        *offset_ref += 1;

                        // TODO: this assume that after decorrelation
                        // the dependent join will provide an extra column with the structure
                        // of "subquery_alias.output"
                        // On later step of decorrelation, it rely on this structure
                        // to again rename the expression after join
                        // for example if the real join type is LeftMark, the correct output
                        // column should be "mark" instead, else after the join
                        // one extra layer of projection is needed to alias "mark" into
                        // "alias.output"
                        Ok(Transformed::yes(col(format!("{alias}.output"))))
                    })?
                    .data;
                // because dependent join may introduce extra columns
                // to evaluate the subquery, the final plan should
                // has another projection to remove these redundant columns
                let post_join_projections: Vec<Expr> = filter
                    .input
                    .schema()
                    .columns()
                    .iter()
                    .map(|c| col(c.clone()))
                    .collect();
                for (subquery_offset, (_, column_accesses)) in
                    node_info.columns_accesses_by_subquery_id.iter().enumerate()
                {
                    let alias = subquery_alias_by_offset.get(&subquery_offset).unwrap();
                    let subquery_expr =
                        subquery_expr_by_offset.get(&subquery_offset).unwrap();

                    let subquery_input = unwrap_subquery_input_from_expr(subquery_expr);

                    let correlated_columns = column_accesses
                        .iter()
                        .map(|ac| {
                            (
                                ac.subquery_depth,
                                Expr::OuterReferenceColumn(
                                    ac.data_type.clone(),
                                    ac.col.clone(),
                                ),
                            )
                        })
                        .unique()
                        .collect();

                    current_plan = current_plan.dependent_join(
                        subquery_input.deref().clone(),
                        correlated_columns,
                        Some(subquery_expr.clone()),
                        current_subquery_depth,
                        alias.clone(),
                        None, // TODO: handle this when we support lateral join rewrite
                    )?;
                }
                current_plan = current_plan
                    .filter(new_predicate.clone())?
                    .project(post_join_projections)?;
            }
            _ => {
                unimplemented!(
                    "implement more dependent join node creation for node {}",
                    node
                )
            }
        }
        Ok(Transformed::yes(current_plan.build()?))
    }
}

#[allow(dead_code)]
#[derive(Debug)]
struct Decorrelation {}

impl OptimizerRule for Decorrelation {
    fn supports_rewrite(&self) -> bool {
        true
    }

    // There will be 2 rewrites going on
    // - Convert all subqueries (maybe including lateral join in the future) to temporary
    // LogicalPlan node called DependentJoin
    // - Decorrelate DependentJoin following top-down approach recursively
    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let mut transformer =
            DependentJoinRewriter::new(Arc::clone(config.alias_generator()));
        let rewrite_result = transformer.rewrite_subqueries_into_dependent_joins(plan)?;
        if rewrite_result.transformed {
            // At this point, we have a logical plan with DependentJoin similar to duckdb
            unimplemented!("implement dependent join decorrelation")
        }
        Ok(rewrite_result)
    }

    fn name(&self) -> &str {
        "decorrelate_subquery"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::DependentJoinRewriter;
    use crate::test::test_table_scan_with_name;
    use arrow::datatypes::DataType as ArrowDataType;
    use datafusion_common::{alias::AliasGenerator, Result, Spans};
    use datafusion_expr::{
        binary_expr, exists, expr_fn::col, in_subquery, lit, out_ref_col,
        scalar_subquery, Expr, JoinType, LogicalPlan, LogicalPlanBuilder, Subquery,
    };
    use datafusion_functions_aggregate::count::count;
    use insta::assert_snapshot;
    use std::sync::Arc;

    macro_rules! assert_dependent_join_rewrite {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let mut index = DependentJoinRewriter::new(Arc::new(AliasGenerator::new()));
            let transformed = index.rewrite_subqueries_into_dependent_joins($plan)?;
            assert!(transformed.transformed);
            let display = transformed.data.display_indent_schema();
            assert_snapshot!(
                display,
                @ $expected,
            )
        }};
    }
    #[test]
    fn rewrite_dependent_join_with_nested_lateral_join() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;

        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;

        let inner_table_lv2 = test_table_scan_with_name("inner_table_lv2")?;
        let scalar_sq_level2 =
            Arc::new(
                LogicalPlanBuilder::from(inner_table_lv2)
                    .filter(
                        col("inner_table_lv2.a")
                            .eq(out_ref_col(ArrowDataType::UInt32, "outer_table.a"))
                            .and(col("inner_table_lv2.b").eq(out_ref_col(
                                ArrowDataType::UInt32,
                                "inner_table_lv1.b",
                            ))),
                    )?
                    .aggregate(Vec::<Expr>::new(), vec![count(col("inner_table_lv2.a"))])?
                    .build()?,
            );
        let sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1.clone())
                .filter(
                    col("inner_table_lv1.c")
                        .eq(out_ref_col(ArrowDataType::UInt32, "outer_table.c"))
                        .and(scalar_subquery(scalar_sq_level2).eq(lit(1))),
                )?
                .aggregate(Vec::<Expr>::new(), vec![count(col("inner_table_lv1.a"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(outer_table.clone())
            .join_on(
                LogicalPlan::Subquery(Subquery {
                    subquery: sq_level1,
                    outer_ref_columns: vec![out_ref_col(
                        ArrowDataType::UInt32,
                        "outer_table.c",
                        // note that subquery lvl2 is referencing outer_table.a, and it is not being listed here
                        // this simulate the limitation of current subquery planning and assert
                        // that the rewriter can fill in this gap
                    )],
                    spans: Spans::new(),
                }),
                JoinType::Inner,
                vec![lit(true)],
            )?
            .build()?;
        assert_dependent_join_rewrite!(plan, @r"
        DependentJoin on [outer_table.a lvl 2, outer_table.c lvl 1] lateral Inner join with Boolean(true) depth 1 [a:UInt32, b:UInt32, c:UInt32]
          TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
          Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv1.a)]] [count(inner_table_lv1.a):Int64]
            Projection: inner_table_lv1.a, inner_table_lv1.b, inner_table_lv1.c [a:UInt32, b:UInt32, c:UInt32]
              Filter: inner_table_lv1.c = outer_ref(outer_table.c) AND __scalar_sq_1.output = Int32(1) [a:UInt32, b:UInt32, c:UInt32, output:Int64]
                DependentJoin on [inner_table_lv1.b lvl 2] with expr (<subquery>) depth 2 [a:UInt32, b:UInt32, c:UInt32, output:Int64]
                  TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
                  Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv2.a)]] [count(inner_table_lv2.a):Int64]
                    Filter: inner_table_lv2.a = outer_ref(outer_table.a) AND inner_table_lv2.b = outer_ref(inner_table_lv1.b) [a:UInt32, b:UInt32, c:UInt32]
                      TableScan: inner_table_lv2 [a:UInt32, b:UInt32, c:UInt32]
        ");
        Ok(())
    }

    #[test]
    fn rewrite_dependent_join_with_lhs_as_a_join() -> Result<()> {
        let outer_left_table = test_table_scan_with_name("outer_right_table")?;
        let outer_right_table = test_table_scan_with_name("outer_left_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;
        let sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1)
                .filter(col("inner_table_lv1.a").eq(binary_expr(
                    out_ref_col(ArrowDataType::UInt32, "outer_left_table.a"),
                    datafusion_expr::Operator::Plus,
                    out_ref_col(ArrowDataType::UInt32, "outer_right_table.a"),
                )))?
                .aggregate(Vec::<Expr>::new(), vec![count(col("inner_table_lv1.a"))])?
                .project(vec![count(col("inner_table_lv1.a")).alias("count_a")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(outer_left_table.clone())
            .join_on(
                outer_right_table,
                JoinType::Left,
                vec![col("outer_left_table.a").eq(col("outer_right_table.a"))],
            )?
            .filter(
                col("outer_table.a")
                    .gt(lit(1))
                    .and(in_subquery(col("outer_table.c"), sq_level1)),
            )?
            .build()?;
        assert_dependent_join_rewrite!(plan, @r"
        Projection: outer_right_table.a, outer_right_table.b, outer_right_table.c, outer_left_table.a, outer_left_table.b, outer_left_table.c [a:UInt32, b:UInt32, c:UInt32, a:UInt32;N, b:UInt32;N, c:UInt32;N]
          Filter: outer_table.a > Int32(1) AND __in_sq_1.output [a:UInt32, b:UInt32, c:UInt32, a:UInt32;N, b:UInt32;N, c:UInt32;N, output:Boolean]
            DependentJoin on [outer_right_table.a lvl 1, outer_left_table.a lvl 1] with expr outer_table.c IN (<subquery>) depth 1 [a:UInt32, b:UInt32, c:UInt32, a:UInt32;N, b:UInt32;N, c:UInt32;N, output:Boolean]
              Left Join:  Filter: outer_left_table.a = outer_right_table.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32;N, b:UInt32;N, c:UInt32;N]
                TableScan: outer_right_table [a:UInt32, b:UInt32, c:UInt32]
                TableScan: outer_left_table [a:UInt32, b:UInt32, c:UInt32]
              Projection: count(inner_table_lv1.a) AS count_a [count_a:Int64]
                Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv1.a)]] [count(inner_table_lv1.a):Int64]
                  Filter: inner_table_lv1.a = outer_ref(outer_left_table.a) + outer_ref(outer_right_table.a) [a:UInt32, b:UInt32, c:UInt32]
                    TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
        ");
        Ok(())
    }
    #[test]
    fn rewrite_dependent_join_in_from_expr() -> Result<()> {
        Ok(())
    }
    #[test]
    fn rewrite_dependent_join_inside_select_expr() -> Result<()> {
        Ok(())
    }

    #[test]
    fn rewrite_dependent_join_two_nested_subqueries() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;

        let inner_table_lv2 = test_table_scan_with_name("inner_table_lv2")?;
        let scalar_sq_level2 =
            Arc::new(
                LogicalPlanBuilder::from(inner_table_lv2)
                    .filter(
                        col("inner_table_lv2.a")
                            .eq(out_ref_col(ArrowDataType::UInt32, "outer_table.a"))
                            .and(col("inner_table_lv2.b").eq(out_ref_col(
                                ArrowDataType::UInt32,
                                "inner_table_lv1.b",
                            ))),
                    )?
                    .aggregate(Vec::<Expr>::new(), vec![count(col("inner_table_lv2.a"))])?
                    .build()?,
            );
        let scalar_sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1.clone())
                .filter(
                    col("inner_table_lv1.c")
                        .eq(out_ref_col(ArrowDataType::UInt32, "outer_table.c"))
                        .and(scalar_subquery(scalar_sq_level2).eq(lit(1))),
                )?
                .aggregate(Vec::<Expr>::new(), vec![count(col("inner_table_lv1.a"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(outer_table.clone())
            .filter(
                col("outer_table.a")
                    .gt(lit(1))
                    .and(scalar_subquery(scalar_sq_level1).eq(col("outer_table.a"))),
            )?
            .build()?;
        assert_dependent_join_rewrite!(plan, @r"
        Projection: outer_table.a, outer_table.b, outer_table.c [a:UInt32, b:UInt32, c:UInt32]
          Filter: outer_table.a > Int32(1) AND __scalar_sq_2.output = outer_table.a [a:UInt32, b:UInt32, c:UInt32, output:Int64]
            DependentJoin on [outer_table.a lvl 2, outer_table.c lvl 1] with expr (<subquery>) depth 1 [a:UInt32, b:UInt32, c:UInt32, output:Int64]
              TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
              Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv1.a)]] [count(inner_table_lv1.a):Int64]
                Projection: inner_table_lv1.a, inner_table_lv1.b, inner_table_lv1.c [a:UInt32, b:UInt32, c:UInt32]
                  Filter: inner_table_lv1.c = outer_ref(outer_table.c) AND __scalar_sq_1.output = Int32(1) [a:UInt32, b:UInt32, c:UInt32, output:Int64]
                    DependentJoin on [inner_table_lv1.b lvl 2] with expr (<subquery>) depth 2 [a:UInt32, b:UInt32, c:UInt32, output:Int64]
                      TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
                      Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv2.a)]] [count(inner_table_lv2.a):Int64]
                        Filter: inner_table_lv2.a = outer_ref(outer_table.a) AND inner_table_lv2.b = outer_ref(inner_table_lv1.b) [a:UInt32, b:UInt32, c:UInt32]
                          TableScan: inner_table_lv2 [a:UInt32, b:UInt32, c:UInt32]
        ");
        Ok(())
    }
    #[test]
    fn rewrite_dependent_join_two_subqueries_at_the_same_level() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;
        let in_sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1.clone())
                .filter(col("inner_table_lv1.c").eq(lit(2)))?
                .project(vec![col("inner_table_lv1.a")])?
                .build()?,
        );
        let exist_sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1)
                .filter(
                    col("inner_table_lv1.a").and(col("inner_table_lv1.b").eq(lit(1))),
                )?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(outer_table.clone())
            .filter(
                col("outer_table.a")
                    .gt(lit(1))
                    .and(exists(exist_sq_level1))
                    .and(in_subquery(col("outer_table.b"), in_sq_level1)),
            )?
            .build()?;
        assert_dependent_join_rewrite!(plan, @r"
Projection: outer_table.a, outer_table.b, outer_table.c [a:UInt32, b:UInt32, c:UInt32]
  Filter: outer_table.a > Int32(1) AND __exists_sq_1.output AND __in_sq_2.output [a:UInt32, b:UInt32, c:UInt32, output:Boolean, output:Boolean]
    DependentJoin on [] with expr outer_table.b IN (<subquery>) depth 1 [a:UInt32, b:UInt32, c:UInt32, output:Boolean, output:Boolean]
      DependentJoin on [] with expr EXISTS (<subquery>) depth 1 [a:UInt32, b:UInt32, c:UInt32, output:Boolean]
        TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
        Filter: inner_table_lv1.a AND inner_table_lv1.b = Int32(1) [a:UInt32, b:UInt32, c:UInt32]
          TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
      Projection: inner_table_lv1.a [a:UInt32]
        Filter: inner_table_lv1.c = Int32(2) [a:UInt32, b:UInt32, c:UInt32]
          TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
        ");
        Ok(())
    }

    #[test]
    fn rewrite_dependent_join_in_subquery_with_count_depth_1() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;
        let sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1)
                .filter(
                    col("inner_table_lv1.a")
                        .eq(out_ref_col(ArrowDataType::UInt32, "outer_table.a"))
                        .and(
                            out_ref_col(ArrowDataType::UInt32, "outer_table.a")
                                .gt(col("inner_table_lv1.c")),
                        )
                        .and(col("inner_table_lv1.b").eq(lit(1)))
                        .and(
                            out_ref_col(ArrowDataType::UInt32, "outer_table.b")
                                .eq(col("inner_table_lv1.b")),
                        ),
                )?
                .aggregate(Vec::<Expr>::new(), vec![count(col("inner_table_lv1.a"))])?
                .project(vec![count(col("inner_table_lv1.a")).alias("count_a")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(outer_table.clone())
            .filter(
                col("outer_table.a")
                    .gt(lit(1))
                    .and(in_subquery(col("outer_table.c"), sq_level1)),
            )?
            .build()?;
        assert_dependent_join_rewrite!(plan, @r"
Projection: outer_table.a, outer_table.b, outer_table.c [a:UInt32, b:UInt32, c:UInt32]
  Filter: outer_table.a > Int32(1) AND __in_sq_1.output [a:UInt32, b:UInt32, c:UInt32, output:Boolean]
    DependentJoin on [outer_table.a lvl 1, outer_table.b lvl 1] with expr outer_table.c IN (<subquery>) depth 1 [a:UInt32, b:UInt32, c:UInt32, output:Boolean]
      TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
      Projection: count(inner_table_lv1.a) AS count_a [count_a:Int64]
        Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv1.a)]] [count(inner_table_lv1.a):Int64]
          Filter: inner_table_lv1.a = outer_ref(outer_table.a) AND outer_ref(outer_table.a) > inner_table_lv1.c AND inner_table_lv1.b = Int32(1) AND outer_ref(outer_table.b) = inner_table_lv1.b [a:UInt32, b:UInt32, c:UInt32]
            TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
        ");
        Ok(())
    }
    #[test]
    fn rewrite_dependent_join_exist_subquery_with_dependent_columns() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;
        let sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1)
                .filter(
                    col("inner_table_lv1.a")
                        .eq(out_ref_col(ArrowDataType::UInt32, "outer_table.a"))
                        .and(
                            out_ref_col(ArrowDataType::UInt32, "outer_table.a")
                                .gt(col("inner_table_lv1.c")),
                        )
                        .and(col("inner_table_lv1.b").eq(lit(1)))
                        .and(
                            out_ref_col(ArrowDataType::UInt32, "outer_table.b")
                                .eq(col("inner_table_lv1.b")),
                        ),
                )?
                .project(vec![out_ref_col(ArrowDataType::UInt32, "outer_table.b")
                    .alias("outer_b_alias")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(outer_table.clone())
            .filter(col("outer_table.a").gt(lit(1)).and(exists(sq_level1)))?
            .build()?;
        assert_dependent_join_rewrite!(plan, @r"
Projection: outer_table.a, outer_table.b, outer_table.c [a:UInt32, b:UInt32, c:UInt32]
  Filter: outer_table.a > Int32(1) AND __exists_sq_1.output [a:UInt32, b:UInt32, c:UInt32, output:Boolean]
    DependentJoin on [outer_table.a lvl 1, outer_table.b lvl 1] with expr EXISTS (<subquery>) depth 1 [a:UInt32, b:UInt32, c:UInt32, output:Boolean]
      TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
      Projection: outer_ref(outer_table.b) AS outer_b_alias [outer_b_alias:UInt32;N]
        Filter: inner_table_lv1.a = outer_ref(outer_table.a) AND outer_ref(outer_table.a) > inner_table_lv1.c AND inner_table_lv1.b = Int32(1) AND outer_ref(outer_table.b) = inner_table_lv1.b [a:UInt32, b:UInt32, c:UInt32]
          TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
        ");
        Ok(())
    }

    #[test]
    fn rewrite_dependent_join_with_exist_subquery_with_no_dependent_columns() -> Result<()>
    {
        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;
        let sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1)
                .filter(col("inner_table_lv1.b").eq(lit(1)))?
                .project(vec![col("inner_table_lv1.b"), col("inner_table_lv1.a")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(outer_table.clone())
            .filter(col("outer_table.a").gt(lit(1)).and(exists(sq_level1)))?
            .build()?;

        assert_dependent_join_rewrite!(plan, @r"
Projection: outer_table.a, outer_table.b, outer_table.c [a:UInt32, b:UInt32, c:UInt32]
  Filter: outer_table.a > Int32(1) AND __exists_sq_1.output [a:UInt32, b:UInt32, c:UInt32, output:Boolean]
    DependentJoin on [] with expr EXISTS (<subquery>) depth 1 [a:UInt32, b:UInt32, c:UInt32, output:Boolean]
      TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
      Projection: inner_table_lv1.b, inner_table_lv1.a [b:UInt32, a:UInt32]
        Filter: inner_table_lv1.b = Int32(1) [a:UInt32, b:UInt32, c:UInt32]
          TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
");

        Ok(())
    }
    #[test]
    fn rewrite_dependent_join_with_in_subquery_no_dependent_column() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;
        let sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1)
                .filter(col("inner_table_lv1.b").eq(lit(1)))?
                .project(vec![col("inner_table_lv1.b")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(outer_table.clone())
            .filter(
                col("outer_table.a")
                    .gt(lit(1))
                    .and(in_subquery(col("outer_table.c"), sq_level1)),
            )?
            .build()?;
        assert_dependent_join_rewrite!(plan, @r"
Projection: outer_table.a, outer_table.b, outer_table.c [a:UInt32, b:UInt32, c:UInt32]
  Filter: outer_table.a > Int32(1) AND __in_sq_1.output [a:UInt32, b:UInt32, c:UInt32, output:Boolean]
    DependentJoin on [] with expr outer_table.c IN (<subquery>) depth 1 [a:UInt32, b:UInt32, c:UInt32, output:Boolean]
      TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
      Projection: inner_table_lv1.b [b:UInt32]
        Filter: inner_table_lv1.b = Int32(1) [a:UInt32, b:UInt32, c:UInt32]
          TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
        ");

        Ok(())
    }
    #[test]
    fn rewrite_dependent_join_with_in_subquery_has_dependent_column() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;
        let sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1)
                .filter(
                    col("inner_table_lv1.a")
                        .eq(out_ref_col(ArrowDataType::UInt32, "outer_table.a"))
                        .and(
                            out_ref_col(ArrowDataType::UInt32, "outer_table.a")
                                .gt(col("inner_table_lv1.c")),
                        )
                        .and(col("inner_table_lv1.b").eq(lit(1)))
                        .and(
                            out_ref_col(ArrowDataType::UInt32, "outer_table.b")
                                .eq(col("inner_table_lv1.b")),
                        ),
                )?
                .project(vec![out_ref_col(ArrowDataType::UInt32, "outer_table.b")
                    .alias("outer_b_alias")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(outer_table.clone())
            .filter(
                col("outer_table.a")
                    .gt(lit(1))
                    .and(in_subquery(col("outer_table.c"), sq_level1)),
            )?
            .build()?;
        assert_dependent_join_rewrite!(plan, @r"
Projection: outer_table.a, outer_table.b, outer_table.c [a:UInt32, b:UInt32, c:UInt32]
  Filter: outer_table.a > Int32(1) AND __in_sq_1.output [a:UInt32, b:UInt32, c:UInt32, output:Boolean]
    DependentJoin on [outer_table.a lvl 1, outer_table.b lvl 1] with expr outer_table.c IN (<subquery>) depth 1 [a:UInt32, b:UInt32, c:UInt32, output:Boolean]
      TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
      Projection: outer_ref(outer_table.b) AS outer_b_alias [outer_b_alias:UInt32;N]
        Filter: inner_table_lv1.a = outer_ref(outer_table.a) AND outer_ref(outer_table.a) > inner_table_lv1.c AND inner_table_lv1.b = Int32(1) AND outer_ref(outer_table.b) = inner_table_lv1.b [a:UInt32, b:UInt32, c:UInt32]
          TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
        ");
        Ok(())
    }

    #[test]
    fn rewrite_dependent_join_reference_outer_column_with_alias_name() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;
        let sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1)
                .filter(
                    col("inner_table_lv1.a")
                        .eq(out_ref_col(ArrowDataType::UInt32, "outer_table_alias.a")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![count(col("inner_table_lv1.a"))])?
                .project(vec![count(col("inner_table_lv1.a")).alias("count_a")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(outer_table.clone())
            .alias("outer_table_alias")?
            .filter(
                col("outer_table.a")
                    .gt(lit(1))
                    .and(in_subquery(col("outer_table.c"), sq_level1)),
            )?
            .build()?;
        assert_dependent_join_rewrite!(plan, @r"
        Projection: outer_table_alias.a, outer_table_alias.b, outer_table_alias.c [a:UInt32, b:UInt32, c:UInt32]
          Filter: outer_table.a > Int32(1) AND __in_sq_1.output [a:UInt32, b:UInt32, c:UInt32, output:Boolean]
            DependentJoin on [outer_table_alias.a lvl 1] with expr outer_table.c IN (<subquery>) depth 1 [a:UInt32, b:UInt32, c:UInt32, output:Boolean]
              SubqueryAlias: outer_table_alias [a:UInt32, b:UInt32, c:UInt32]
                TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
              Projection: count(inner_table_lv1.a) AS count_a [count_a:Int64]
                Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv1.a)]] [count(inner_table_lv1.a):Int64]
                  Filter: inner_table_lv1.a = outer_ref(outer_table_alias.a) [a:UInt32, b:UInt32, c:UInt32]
                    TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
        ");
        Ok(())
    }
}
