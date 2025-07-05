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
use datafusion_common::{
    internal_datafusion_err, internal_err, not_impl_err, Column, HashMap, Result,
};
use datafusion_expr::{
    col, lit, Aggregate, CorrelatedColumnInfo, Expr, Filter, Join, LogicalPlan,
    LogicalPlanBuilder, Projection,
};

use indexmap::map::Entry;
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
    // this function is to rewrite logical plan having arbitrary exprs that contain
    // subquery expr into dependent join logical plan
    fn rewrite_exprs_into_dependent_join_plan(
        exprs: Vec<Vec<&Expr>>,
        dependent_join_node: &Node,
        current_subquery_depth: usize,
        mut current_plan: LogicalPlanBuilder,
        subquery_alias_by_offset: HashMap<usize, String>,
    ) -> Result<(LogicalPlanBuilder, Vec<Vec<Expr>>)> {
        // everytime we meet a subquery during traversal, we increment this by 1
        // we can use this offset to lookup the original subquery info
        // in subquery_alias_by_offset
        // the reason why we cannot create a hashmap keyed by Subquery object HashMap<Subquery,String>
        // is that the subquery inside this filter expr may have been rewritten in
        // the lower level
        let mut offset = 0;
        let offset_ref = &mut offset;
        let mut subquery_expr_by_offset = HashMap::new();
        let mut rewritten_exprs_groups = vec![];
        for expr_group in exprs {
            let rewritten_exprs = expr_group
                .iter()
                .cloned()
                .map(|e| {
                    Ok(e.clone()
                        .transform(|e| {
                            // replace any subquery expr with subquery_alias.output column
                            let alias = match e {
                                Expr::InSubquery(_)
                                | Expr::Exists(_)
                                | Expr::ScalarSubquery(_) => subquery_alias_by_offset
                                    .get(offset_ref)
                                    .ok_or(internal_datafusion_err!(
                                        "subquery alias not found at offset {}",
                                        *offset_ref
                                    )),
                                _ => return Ok(Transformed::no(e)),
                            }?;

                            // We are aware that the original subquery can be rewritten update the
                            // latest expr to this map.
                            subquery_expr_by_offset.insert(*offset_ref, e);
                            *offset_ref += 1;

                            Ok(Transformed::yes(col(format!("{alias}.output"))))
                        })?
                        .data)
                })
                .collect::<Result<Vec<Expr>>>()?;
            rewritten_exprs_groups.push(rewritten_exprs);
        }

        for (subquery_offset, (_, column_accesses)) in dependent_join_node
            .columns_accesses_by_subquery_id
            .iter()
            .enumerate()
        {
            let alias = subquery_alias_by_offset.get(&subquery_offset).ok_or(
                internal_datafusion_err!(
                    "subquery alias not found at offset {subquery_offset}"
                ),
            )?;
            let subquery_expr = subquery_expr_by_offset.get(&subquery_offset).ok_or(
                internal_datafusion_err!(
                    "subquery expr not found at offset {subquery_offset}"
                ),
            )?;

            let subquery_input = unwrap_subquery_input_from_expr(subquery_expr);

            let correlated_columns = column_accesses
                .iter()
                .map(|ac| CorrelatedColumnInfo {
                    col: ac.col.clone(),
                    data_type: ac.data_type.clone(),
                    depth: ac.subquery_depth,
                })
                .unique()
                .collect();

            current_plan = current_plan.dependent_join(
                subquery_input.deref().clone(),
                correlated_columns,
                Some(subquery_expr.clone()),
                current_subquery_depth,
                alias.clone(),
                None,
            )?;
        }
        Ok((current_plan, rewritten_exprs_groups))
    }

    fn rewrite_filter(
        &mut self,
        filter: &Filter,
        dependent_join_node: &Node,
        current_subquery_depth: usize,
        current_plan: LogicalPlanBuilder,
        subquery_alias_by_offset: HashMap<usize, String>,
    ) -> Result<LogicalPlanBuilder> {
        // because dependent join may introduce extra columns
        // to evaluate the subquery, the final plan should
        // have another projection to remove these redundant columns
        let post_join_projections: Vec<Expr> = filter
            .input
            .schema()
            .columns()
            .iter()
            .map(|c| col(c.clone()))
            .collect();
        let (transformed_plan, transformed_exprs) =
            Self::rewrite_exprs_into_dependent_join_plan(
                vec![vec![&filter.predicate]],
                dependent_join_node,
                current_subquery_depth,
                current_plan,
                subquery_alias_by_offset,
            )?;

        let transformed_predicate = transformed_exprs
            .first()
            .ok_or(internal_datafusion_err!(
                "transform predicate does not return 1 element"
            ))?
            .first()
            .ok_or(internal_datafusion_err!(
                "transform predicate does not return 1 element"
            ))?;

        transformed_plan
            .filter(transformed_predicate.clone())?
            .project(post_join_projections)
    }

    fn rewrite_projection(
        &mut self,
        original_proj: &Projection,
        dependent_join_node: &Node,
        current_subquery_depth: usize,
        current_plan: LogicalPlanBuilder,
        subquery_alias_by_offset: HashMap<usize, String>,
    ) -> Result<LogicalPlanBuilder> {
        let (transformed_plan, transformed_exprs) =
            Self::rewrite_exprs_into_dependent_join_plan(
                vec![original_proj.expr.iter().collect::<Vec<&Expr>>()],
                dependent_join_node,
                current_subquery_depth,
                current_plan,
                subquery_alias_by_offset,
            )?;
        let transformed_proj_exprs =
            transformed_exprs.first().ok_or(internal_datafusion_err!(
                "transform projection expr does not return 1 element"
            ))?;
        transformed_plan.project(transformed_proj_exprs.clone())
    }

    fn rewrite_aggregate(
        &mut self,
        aggregate: &Aggregate,
        dependent_join_node: &Node,
        current_subquery_depth: usize,
        current_plan: LogicalPlanBuilder,
        subquery_alias_by_offset: HashMap<usize, String>,
    ) -> Result<LogicalPlanBuilder> {
        // because dependent join may introduce extra columns
        // to evaluate the subquery, the final plan should
        // have another projection to remove these redundant columns
        let post_join_projections: Vec<Expr> = aggregate
            .schema
            .columns()
            .iter()
            .map(|c| col(c.clone()))
            .collect();

        let (transformed_plan, transformed_exprs) =
            Self::rewrite_exprs_into_dependent_join_plan(
                vec![
                    aggregate.group_expr.iter().collect::<Vec<&Expr>>(),
                    aggregate.aggr_expr.iter().collect::<Vec<&Expr>>(),
                ],
                dependent_join_node,
                current_subquery_depth,
                current_plan,
                subquery_alias_by_offset,
            )?;
        let (new_group_exprs, new_aggr_exprs) = match transformed_exprs.as_slice() {
            [first, second] => (first, second),
            _ => {
                return internal_err!(
                "transform group and aggr exprs does not return vector of 2 Vec<Expr>")
            }
        };

        transformed_plan
            .aggregate(new_group_exprs.clone(), new_aggr_exprs.clone())?
            .project(post_join_projections)
    }

    fn rewrite_lateral_join(
        &mut self,
        join: &Join,
        dependent_join_node: &Node,
        current_subquery_depth: usize,
        current_plan: LogicalPlanBuilder,
        subquery_alias_by_offset: HashMap<usize, String>,
    ) -> Result<LogicalPlanBuilder> {
        // this is lateral join
        assert!(dependent_join_node.columns_accesses_by_subquery_id.len() == 1);
        let (_, column_accesses) = dependent_join_node
            .columns_accesses_by_subquery_id
            .first()
            .ok_or(internal_datafusion_err!(
                "a lateral join should always have one child subquery"
            ))?;
        let alias = subquery_alias_by_offset
            .get(&0)
            .ok_or(internal_datafusion_err!(
                "cannot find subquery alias for only-child of lateral join"
            ))?;
        let correlated_columns = column_accesses
            .iter()
            .map(|ac| CorrelatedColumnInfo {
                col: ac.col.clone(),
                data_type: ac.data_type.clone(),
                depth: ac.subquery_depth,
            })
            .unique()
            .collect();

        let sq = if let LogicalPlan::Subquery(sq) = join.right.as_ref() {
            sq
        } else {
            return internal_err!("right side of a lateral join is not a subquery");
        };
        let right = sq.subquery.deref().clone();
        // At the time of implementation lateral join condition is not fully clear yet
        // So a TODO for future tracking
        let lateral_join_condition = if let Some(ref filter) = join.filter {
            filter.clone()
        } else {
            lit(true)
        };
        current_plan.dependent_join(
            right,
            correlated_columns,
            None,
            current_subquery_depth,
            alias.to_string(),
            Some((join.join_type, lateral_join_condition)),
        )
    }

    // TODO: it is sub-optimal that we completely remove all
    // the filters (including the ones that have no subquery attached)
    // from the original join
    // We have to check if after decorrelation, the other optimizers
    // that follows are capable of merging these filters back to the
    // join node or not
    fn rewrite_join(
        &mut self,
        join: &Join,
        dependent_join_node: &Node,
        current_subquery_depth: usize,
        subquery_alias_by_offset: HashMap<usize, String>,
    ) -> Result<LogicalPlanBuilder> {
        let mut new_join = join.clone();
        let filter = if let Some(ref filter) = join.filter {
            filter
        } else {
            return internal_err!(
                "rewriting a correlated join node without any filter condition"
            );
        };

        new_join.filter = None;

        let (transformed_plan, transformed_exprs) =
            Self::rewrite_exprs_into_dependent_join_plan(
                vec![vec![filter]],
                dependent_join_node,
                current_subquery_depth,
                LogicalPlanBuilder::new(LogicalPlan::Join(new_join)),
                subquery_alias_by_offset,
            )?;

        let transformed_predicate = transformed_exprs
            .first()
            .ok_or(internal_datafusion_err!(
                "transform predicate does not return 1 element"
            ))?
            .first()
            .ok_or(internal_datafusion_err!(
                "transform predicate does not return 1 element"
            ))?;

        transformed_plan.filter(transformed_predicate.clone())
    }

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
    ) -> Result<()> {
        if let Some(accesses) = self.all_outer_ref_columns.get(col) {
            for access in accesses.iter() {
                let mut cur_stack = self.stack.clone();

                cur_stack.push(child_id);
                let (dependent_join_node_id, subquery_node_id) =
                    Self::dependent_join_and_subquery_node_ids(&cur_stack, &access.stack);
                let node = self.nodes.get_mut(&dependent_join_node_id).ok_or(
                    internal_datafusion_err!(
                        "dependent join node with id {dependent_join_node_id} not found"
                    ),
                )?;
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
        Ok(())
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

    pub fn rewrite_subqueries_into_dependent_joins(
        &mut self,
        plan: LogicalPlan,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.rewrite_with_subqueries(self)
    }
}

impl DependentJoinRewriter {
    pub fn new(alias_generator: Arc<AliasGenerator>) -> Self {
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
    // a dependent join node with LogicalPlan::Join variation can have subquery children
    // in two scenarios:
    // - it is a lateral join
    // - it is a normal join, but the join conditions contain subquery
    // These two scenarios are mutually exclusive and we need to maintain a flag for this
    is_lateral_join: bool,

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
/// its data were already visited, and we can conclude the information of
/// the `DependentJoin`
/// needed for the decorrelation:
/// - The subquery expr
/// - The correlated columns on the LHS referenced from the RHS
///   (and its recursing subqueries if any)
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
///            ┌──────────────┐
///            │    FILTER    │<--- DependentJoin rewrite
///            │     (1)      │     happens here (step 12)
///            └─┬─────┬────┬─┘     Here we already have enough information
///              │     │    │         of which node is accessing which column
///              │     │    │         provided by "Table Scan t1" node
///              │     │    │         (for example node (6) below )
///              │     │    │
///              │     │    │
///              │     │    │
///        ↓2────┘     ↓6   └────↓10
///        ↑5          ↑11         ↑11
///    ┌───▼───┐    ┌──▼───┐   ┌───▼───────┐
///    │SUBQ1  │    │SUBQ2 │   │TABLE SCAN │
///    └──┬────┘    └──┬───┘   │    t1     │
///       │            │       └───────────┘
///       │            │
///       │            │
///       │            ↓7
///       │            ↑10
///       │        ┌───▼──────┐
///       │        │Filter    │----> mark_outer_column_access(outer_ref)
///       │        │outer_ref │
///       │        │ (6)      │
///       │        └──┬───────┘
///       │           │
///      ↓3           ↓8
///      ↑4           ↑9
///    ┌──▼────┐    ┌──▼────┐
///    │SCAN t2│    │SCAN t2│
///    └───────┘    └───────┘
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
                tbl_scan
                    .projected_schema
                    .columns()
                    .iter()
                    .try_for_each(|col| {
                        self.conclude_lowest_dependent_join_node_if_any(new_id, col)
                    })?;
            }
            // Similar to TableScan, this node may provide column names which
            // is referenced inside some subqueries
            LogicalPlan::SubqueryAlias(alias) => {
                alias.schema.columns().iter().try_for_each(|col| {
                    self.conclude_lowest_dependent_join_node_if_any(new_id, col)
                })?;
            }
            LogicalPlan::Projection(proj) => {
                for expr in &proj.expr {
                    if contains_subquery(expr) {
                        is_dependent_join_node = true;
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
                let parent = self.stack.last().ok_or(internal_datafusion_err!(
                    "subquery node cannot be at the beginning of the query plan"
                ))?;

                let parent_node = self
                    .nodes
                    .get_mut(parent)
                    .ok_or(internal_datafusion_err!("node {parent} not found"))?;
                // the inserting sequence matter here
                // when a parent has multiple children subquery at the same time
                // we rely on the order in which subquery children are visited
                // to later on find back the corresponding subquery (if some part of them
                // were rewritten in the lower node)
                parent_node
                    .columns_accesses_by_subquery_id
                    .insert(new_id, vec![]);

                if parent_node.is_lateral_join {
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
            LogicalPlan::Aggregate(aggregate) => {
                for expr in &aggregate.group_expr {
                    if contains_subquery(expr) {
                        is_dependent_join_node = true;
                    }

                    expr.apply(|expr| {
                        if let Expr::OuterReferenceColumn(data_type, col) = expr {
                            self.mark_outer_column_access(new_id, data_type, col);
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })?;
                }

                for expr in &aggregate.aggr_expr {
                    if contains_subquery(expr) {
                        is_dependent_join_node = true;
                    }

                    expr.apply(|expr| {
                        if let Expr::OuterReferenceColumn(data_type, col) = expr {
                            self.mark_outer_column_access(new_id, data_type, col);
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })?;
                }
            }
            LogicalPlan::Join(join) => {
                if let LogicalPlan::Subquery(_) = &join.left.as_ref() {
                    return internal_err!("left side of a join cannot be a subquery");
                }

                // Handle the case lateral join
                if let LogicalPlan::Subquery(_) = join.right.as_ref() {
                    if let Some(ref filter) = join.filter {
                        if contains_subquery(filter) {
                            return not_impl_err!(
                                "subquery inside lateral join condition is not supported"
                            );
                        }
                    }
                    self.subquery_depth += 1;
                    self.stack.push(new_id);
                    self.nodes.insert(
                        new_id,
                        Node {
                            plan: node.clone(),
                            is_dependent_join_node: true,
                            columns_accesses_by_subquery_id: IndexMap::new(),
                            subquery_type,
                            is_lateral_join: true,
                        },
                    );

                    // we assume that RHS is always a subquery for the lateral join
                    // and because this function assume that subquery side is always
                    // visited first during f_down, we have to explicitly swap the rewrite
                    // order at this step, else the function visit_with_subqueries will
                    // call f_down for the LHS instead
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

                if let Some(filter) = &join.filter {
                    if contains_subquery(filter) {
                        is_dependent_join_node = true;
                    }

                    filter.apply(|expr| {
                        if let Expr::OuterReferenceColumn(data_type, col) = expr {
                            self.mark_outer_column_access(new_id, data_type, col);
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })?;
                }
            }
            LogicalPlan::Sort(sort) => {
                for expr in &sort.expr {
                    if contains_subquery(&expr.expr) {
                        is_dependent_join_node = true;
                    }

                    expr.expr.apply(|expr| {
                        if let Expr::OuterReferenceColumn(data_type, col) = expr {
                            self.mark_outer_column_access(new_id, data_type, col);
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })?;
                }
            }
            _ => {}
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
                is_lateral_join: false,
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
        let current_node_id = self.stack.pop().ok_or(internal_datafusion_err!(
            "stack cannot be empty during upward traversal"
        ))?;
        let node_info = if let Entry::Occupied(e) = self.nodes.entry(current_node_id) {
            let node_info = e.get();
            if !node_info.is_dependent_join_node {
                return Ok(Transformed::no(node));
            }
            e.swap_remove()
        } else {
            unreachable!()
        };

        let current_subquery_depth = self.subquery_depth;
        self.subquery_depth -= 1;

        let cloned_input = (**node.inputs().first().ok_or(internal_datafusion_err!(
            "logical plan {} does not have any input",
            node
        ))?)
        .clone();
        let mut current_plan = LogicalPlanBuilder::new(cloned_input);
        let mut subquery_alias_by_offset = HashMap::new();
        for (subquery_offset, (subquery_id, _)) in
            node_info.columns_accesses_by_subquery_id.iter().enumerate()
        {
            let subquery_node = self
                .nodes
                .get(subquery_id)
                .ok_or(internal_datafusion_err!("node {subquery_id} not found"))?;
            let alias = self
                .alias_generator
                .next(&subquery_node.subquery_type.prefix());
            subquery_alias_by_offset.insert(subquery_offset, alias);
        }

        match &node {
            LogicalPlan::Projection(projection) => {
                current_plan = self.rewrite_projection(
                    projection,
                    &node_info,
                    current_subquery_depth,
                    current_plan,
                    subquery_alias_by_offset,
                )?;
            }
            LogicalPlan::Filter(filter) => {
                current_plan = self.rewrite_filter(
                    filter,
                    &node_info,
                    current_subquery_depth,
                    current_plan,
                    subquery_alias_by_offset,
                )?;
            }
            LogicalPlan::Join(join) => {
                if node_info.is_lateral_join {
                    current_plan = self.rewrite_lateral_join(
                        join,
                        &node_info,
                        current_subquery_depth,
                        current_plan,
                        subquery_alias_by_offset,
                    )?
                } else {
                    // Correlated subquery in join filter.
                    current_plan = self.rewrite_join(
                        join,
                        &node_info,
                        current_subquery_depth,
                        subquery_alias_by_offset,
                    )?;
                };
            }
            LogicalPlan::Aggregate(aggregate) => {
                current_plan = self.rewrite_aggregate(
                    aggregate,
                    &node_info,
                    current_subquery_depth,
                    current_plan,
                    subquery_alias_by_offset,
                )?;
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

/// Optimizer rule for rewriting subqueries to dependent join.
#[allow(dead_code)]
#[derive(Debug)]
pub struct RewriteDependentJoin {}

impl Default for RewriteDependentJoin {
    fn default() -> Self {
        Self::new()
    }
}

impl RewriteDependentJoin {
    pub fn new() -> Self {
        RewriteDependentJoin {}
    }
}

impl OptimizerRule for RewriteDependentJoin {
    fn supports_rewrite(&self) -> bool {
        true
    }

    // Convert all subqueries (maybe including lateral join in the future) to temporary
    // LogicalPlan node called DependentJoin.
    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let mut transformer =
            DependentJoinRewriter::new(Arc::clone(config.alias_generator()));
        let rewrite_result = transformer.rewrite_subqueries_into_dependent_joins(plan)?;
        if rewrite_result.transformed {
            println!("dependent join plan {}", rewrite_result.data);
        }
        Ok(rewrite_result)
    }

    fn name(&self) -> &str {
        "rewrite_dependent_join"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::DependentJoinRewriter;

    use crate::test::{test_table_scan_with_name, test_table_with_columns};
    use arrow::datatypes::{DataType, TimeUnit};
    use datafusion_common::{alias::AliasGenerator, Result, Spans};
    use datafusion_expr::{
        and, binary_expr, exists, expr::InSubquery, expr_fn::col, in_subquery, lit,
        out_ref_col, scalar_subquery, Expr, JoinType, LogicalPlan, LogicalPlanBuilder,
        Operator, SortExpr, Subquery,
    };
    use datafusion_functions_aggregate::{count::count, sum::sum};
    use insta::assert_snapshot;
    use std::sync::Arc;

    macro_rules! assert_dependent_join_rewrite_err {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let mut index = DependentJoinRewriter::new(Arc::new(AliasGenerator::new()));
            let transformed = index.rewrite_subqueries_into_dependent_joins($plan.clone());
            if let Err(err) = transformed{
                assert_snapshot!(
                    err,
                    @ $expected,
                )
            } else{
                panic!("rewriting {} was not returning error",$plan)
            }

        }};
    }

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
    fn uncorrelated_lateral_join() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;

        let lateral_join_rhs = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1.clone())
                .filter(col("inner_table_lv1.c").eq(lit(1)))?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(outer_table.clone())
            .join_on(
                LogicalPlan::Subquery(Subquery {
                    subquery: lateral_join_rhs,
                    outer_ref_columns: vec![],
                    spans: Spans::new(),
                }),
                JoinType::Inner,
                vec![lit(true)],
            )?
            .build()?;

        // Inner Join:  Filter: Boolean(true)
        //   TableScan: outer_table
        //   Subquery:
        //     Filter: inner_table_lv1.c = outer_ref(outer_table.c)
        //       TableScan: inner_table_lv1

        assert_dependent_join_rewrite!(plan, @r"
        DependentJoin on [] lateral Inner join with Boolean(true) depth 1 [a:UInt32, b:UInt32, c:UInt32]
          TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
          Filter: inner_table_lv1.c = Int32(1) [a:UInt32, b:UInt32, c:UInt32]
            TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
        ");
        Ok(())
    }

    #[test]
    fn correlated_lateral_join() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;

        let lateral_join_rhs = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1.clone())
                .filter(
                    col("inner_table_lv1.c")
                        .eq(out_ref_col(DataType::UInt32, "outer_table.c")),
                )?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(outer_table.clone())
            .join_on(
                LogicalPlan::Subquery(Subquery {
                    subquery: lateral_join_rhs,
                    outer_ref_columns: vec![out_ref_col(
                        DataType::UInt32,
                        "outer_table.c",
                    )],
                    spans: Spans::new(),
                }),
                JoinType::Inner,
                vec![lit(true)],
            )?
            .build()?;

        // Inner Join:  Filter: Boolean(true)
        //   TableScan: outer_table
        //   Subquery:
        //     Filter: inner_table_lv1.c = outer_ref(outer_table.c)
        //       TableScan: inner_table_lv1

        assert_dependent_join_rewrite!(plan, @r"
        DependentJoin on [outer_table.c lvl 1] lateral Inner join with Boolean(true) depth 1 [a:UInt32, b:UInt32, c:UInt32]
          TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
          Filter: inner_table_lv1.c = outer_ref(outer_table.c) [a:UInt32, b:UInt32, c:UInt32]
            TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
        ");
        Ok(())
    }

    #[test]
    fn scalar_subquery_nested_inside_a_lateral_join() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;

        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;

        let inner_table_lv2 = test_table_scan_with_name("inner_table_lv2")?;
        let scalar_sq_level2 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv2)
                .filter(
                    col("inner_table_lv2.a")
                        .eq(out_ref_col(DataType::UInt32, "outer_table.a"))
                        .and(
                            col("inner_table_lv2.b")
                                .eq(out_ref_col(DataType::UInt32, "inner_table_lv1.b")),
                        ),
                )?
                .aggregate(Vec::<Expr>::new(), vec![count(col("inner_table_lv2.a"))])?
                .build()?,
        );
        let sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1.clone())
                .filter(
                    col("inner_table_lv1.c")
                        .eq(out_ref_col(DataType::UInt32, "outer_table.c"))
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
                        DataType::UInt32,
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

        // Inner Join:  Filter: Boolean(true)
        //   TableScan: outer_table
        //   Subquery:
        //     Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv1.a)]]
        //       Filter: inner_table_lv1.c = outer_ref(outer_table.c) AND (<subquery>) = Int32(1)
        //         Subquery:
        //           Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv2.a)]]
        //             Filter: inner_table_lv2.a = outer_ref(outer_table.a) AND inner_table_lv2.b = outer_ref(inner_table_lv1.b)
        //               TableScan: inner_table_lv2
        //         TableScan: inner_table_lv1

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
    fn join_logical_plan_with_subquery_in_filter_expr() -> Result<()> {
        let outer_left_table = test_table_scan_with_name("outer_right_table")?;
        let outer_right_table = test_table_scan_with_name("outer_left_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;
        let sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1)
                .filter(col("inner_table_lv1.a").eq(binary_expr(
                    out_ref_col(DataType::UInt32, "outer_left_table.a"),
                    Operator::Plus,
                    out_ref_col(DataType::UInt32, "outer_right_table.a"),
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

        // Filter: outer_table.a > Int32(1) AND outer_table.c IN (<subquery>)
        //   Subquery:
        //     Projection: count(inner_table_lv1.a) AS count_a
        //       Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv1.a)]]
        //         Filter: inner_table_lv1.a = outer_ref(outer_left_table.a) + outer_ref(outer_right_table.a)
        //           TableScan: inner_table_lv1
        //   Left Join:  Filter: outer_left_table.a = outer_right_table.a
        //     TableScan: outer_right_table
        //     TableScan: outer_left_table

        assert_dependent_join_rewrite!(plan, @r"
        Projection: outer_right_table.a, outer_right_table.b, outer_right_table.c, outer_left_table.a, outer_left_table.b, outer_left_table.c [a:UInt32, b:UInt32, c:UInt32, a:UInt32;N, b:UInt32;N, c:UInt32;N]
          Filter: outer_table.a > Int32(1) AND __in_sq_1.output [a:UInt32, b:UInt32, c:UInt32, a:UInt32;N, b:UInt32;N, c:UInt32;N, output:Boolean]
            DependentJoin on [outer_right_table.a lvl 1, outer_left_table.a lvl 1] with expr outer_table.c IN (<subquery>) depth 1 [a:UInt32, b:UInt32, c:UInt32, a:UInt32;N, b:UInt32;N, c:UInt32;N, output:Boolean]
              Left Join(ComparisonJoin):  Filter: outer_left_table.a = outer_right_table.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32;N, b:UInt32;N, c:UInt32;N]
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
    fn subquery_in_from_expr() -> Result<()> {
        Ok(())
    }
    #[test]
    fn nested_subquery_in_projection_expr() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;

        let inner_table_lv2 = test_table_scan_with_name("inner_table_lv2")?;
        let scalar_sq_level2 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv2)
                .filter(
                    col("inner_table_lv2.a")
                        .eq(out_ref_col(DataType::UInt32, "outer_table.a"))
                        .and(
                            col("inner_table_lv2.b")
                                .eq(out_ref_col(DataType::UInt32, "inner_table_lv1.b")),
                        ),
                )?
                .aggregate(Vec::<Expr>::new(), vec![count(col("inner_table_lv2.a"))])?
                .build()?,
        );
        let scalar_sq_level1_a = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1.clone())
                .filter(
                    col("inner_table_lv1.c")
                        .eq(out_ref_col(DataType::UInt32, "outer_table.c"))
                        // scalar_sq_level2 is intentionally shared between both
                        // scalar_sq_level1_a and scalar_sq_level1_b
                        // to check if the framework can uniquely identify the correlated columns
                        .and(scalar_subquery(Arc::clone(&scalar_sq_level2)).eq(lit(1))),
                )?
                .aggregate(Vec::<Expr>::new(), vec![count(col("inner_table_lv1.a"))])?
                .build()?,
        );
        let scalar_sq_level1_b = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1.clone())
                .filter(
                    col("inner_table_lv1.c")
                        .eq(out_ref_col(DataType::UInt32, "outer_table.c"))
                        .and(scalar_subquery(scalar_sq_level2).eq(lit(1))),
                )?
                .aggregate(Vec::<Expr>::new(), vec![count(col("inner_table_lv1.b"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(outer_table.clone())
            .project(vec![
                col("outer_table.a"),
                binary_expr(
                    scalar_subquery(scalar_sq_level1_a),
                    Operator::Plus,
                    scalar_subquery(scalar_sq_level1_b),
                ),
            ])?
            .build()?;

        // Projection: outer_table.a, (<subquery>) + (<subquery>)
        //   Subquery:
        //     Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv1.a)]]
        //       Filter: inner_table_lv1.c = outer_ref(outer_table.c) AND (<subquery>) = Int32(1)
        //         Subquery:
        //           Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv2.a)]]
        //             Filter: inner_table_lv2.a = outer_ref(outer_table.a) AND inner_table_lv2.b = outer_ref(inner_table_lv1.b)
        //               TableScan: inner_table_lv2
        //         TableScan: inner_table_lv1
        //   Subquery:
        //     Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv1.b)]]
        //       Filter: inner_table_lv1.c = outer_ref(outer_table.c) AND (<subquery>) = Int32(1)
        //         Subquery:
        //           Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv2.a)]]
        //             Filter: inner_table_lv2.a = outer_ref(outer_table.a) AND inner_table_lv2.b = outer_ref(inner_table_lv1.b)
        //               TableScan: inner_table_lv2
        //         TableScan: inner_table_lv1
        //   TableScan: outer_table

        assert_dependent_join_rewrite!(plan, @r"
        Projection: outer_table.a, __scalar_sq_3.output + __scalar_sq_4.output [a:UInt32, __scalar_sq_3.output + __scalar_sq_4.output:Int64]
          DependentJoin on [outer_table.a lvl 2, outer_table.c lvl 1] with expr (<subquery>) depth 1 [a:UInt32, b:UInt32, c:UInt32, output:Int64, output:Int64]
            DependentJoin on [inner_table_lv1.b lvl 2, outer_table.a lvl 2, outer_table.c lvl 1] with expr (<subquery>) depth 1 [a:UInt32, b:UInt32, c:UInt32, output:Int64]
              TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
              Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv1.a)]] [count(inner_table_lv1.a):Int64]
                Projection: inner_table_lv1.a, inner_table_lv1.b, inner_table_lv1.c [a:UInt32, b:UInt32, c:UInt32]
                  Filter: inner_table_lv1.c = outer_ref(outer_table.c) AND __scalar_sq_1.output = Int32(1) [a:UInt32, b:UInt32, c:UInt32, output:Int64]
                    DependentJoin on [inner_table_lv1.b lvl 2] with expr (<subquery>) depth 2 [a:UInt32, b:UInt32, c:UInt32, output:Int64]
                      TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
                      Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv2.a)]] [count(inner_table_lv2.a):Int64]
                        Filter: inner_table_lv2.a = outer_ref(outer_table.a) AND inner_table_lv2.b = outer_ref(inner_table_lv1.b) [a:UInt32, b:UInt32, c:UInt32]
                          TableScan: inner_table_lv2 [a:UInt32, b:UInt32, c:UInt32]
            Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv1.b)]] [count(inner_table_lv1.b):Int64]
              Projection: inner_table_lv1.a, inner_table_lv1.b, inner_table_lv1.c [a:UInt32, b:UInt32, c:UInt32]
                Filter: inner_table_lv1.c = outer_ref(outer_table.c) AND __scalar_sq_2.output = Int32(1) [a:UInt32, b:UInt32, c:UInt32, output:Int64]
                  DependentJoin on [inner_table_lv1.b lvl 2] with expr (<subquery>) depth 2 [a:UInt32, b:UInt32, c:UInt32, output:Int64]
                    TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
                    Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv2.a)]] [count(inner_table_lv2.a):Int64]
                      Filter: inner_table_lv2.a = outer_ref(outer_table.a) AND inner_table_lv2.b = outer_ref(inner_table_lv1.b) [a:UInt32, b:UInt32, c:UInt32]
                        TableScan: inner_table_lv2 [a:UInt32, b:UInt32, c:UInt32]
        ");
        Ok(())
    }

    #[test]
    fn nested_subquery_in_filter() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;

        let inner_table_lv2 = test_table_scan_with_name("inner_table_lv2")?;
        let scalar_sq_level2 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv2)
                .filter(
                    col("inner_table_lv2.a")
                        .eq(out_ref_col(DataType::UInt32, "outer_table.a"))
                        .and(
                            col("inner_table_lv2.b")
                                .eq(out_ref_col(DataType::UInt32, "inner_table_lv1.b")),
                        ),
                )?
                .aggregate(Vec::<Expr>::new(), vec![count(col("inner_table_lv2.a"))])?
                .build()?,
        );
        let scalar_sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1.clone())
                .filter(
                    col("inner_table_lv1.c")
                        .eq(out_ref_col(DataType::UInt32, "outer_table.c"))
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

        // Filter: outer_table.a > Int32(1) AND (<subquery>) = outer_table.a
        //   Subquery:
        //     Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv1.a)]]
        //       Filter: inner_table_lv1.c = outer_ref(outer_table.c) AND (<subquery>) = Int32(1)
        //         Subquery:
        //           Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv2.a)]]
        //             Filter: inner_table_lv2.a = outer_ref(outer_table.a) AND inner_table_lv2.b = outer_ref(inner_table_lv1
        // .b)
        //               TableScan: inner_table_lv2
        //         TableScan: inner_table_lv1
        //   TableScan: outer_table

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
    fn two_subqueries_in_the_same_filter_expr() -> Result<()> {
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

        // Filter: outer_table.a > Int32(1) AND EXISTS (<subquery>) AND outer_table.b IN (<subquery>)
        //   Subquery:
        //     Filter: inner_table_lv1.a AND inner_table_lv1.b = Int32(1)
        //       TableScan: inner_table_lv1
        //   Subquery:
        //     Projection: inner_table_lv1.a
        //       Filter: inner_table_lv1.c = Int32(2)
        //         TableScan: inner_table_lv1
        //   TableScan: outer_table

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
    fn in_subquery_with_count_of_1_depth() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;
        let sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1)
                .filter(
                    col("inner_table_lv1.a")
                        .eq(out_ref_col(DataType::UInt32, "outer_table.a"))
                        .and(
                            out_ref_col(DataType::UInt32, "outer_table.a")
                                .gt(col("inner_table_lv1.c")),
                        )
                        .and(col("inner_table_lv1.b").eq(lit(1)))
                        .and(
                            out_ref_col(DataType::UInt32, "outer_table.b")
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

        // Filter: outer_table.a > Int32(1) AND outer_table.c IN (<subquery>)
        //   Subquery:
        //     Projection: count(inner_table_lv1.a) AS count_a
        //       Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv1.a)]]
        //         Filter: inner_table_lv1.a = outer_ref(outer_table.a) AND outer_ref(outer_table.a) > inner_table_lv1.c AND inner_table_lv1.b = Int32(1) AND outer_ref(outer_table.b) = inner_table_lv1.b
        //           TableScan: inner_table_lv1
        //   TableScan: outer_table

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
    fn correlated_exist_subquery() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;
        let sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1)
                .filter(
                    col("inner_table_lv1.a")
                        .eq(out_ref_col(DataType::UInt32, "outer_table.a"))
                        .and(
                            out_ref_col(DataType::UInt32, "outer_table.a")
                                .gt(col("inner_table_lv1.c")),
                        )
                        .and(col("inner_table_lv1.b").eq(lit(1)))
                        .and(
                            out_ref_col(DataType::UInt32, "outer_table.b")
                                .eq(col("inner_table_lv1.b")),
                        ),
                )?
                .project(vec![
                    out_ref_col(DataType::UInt32, "outer_table.b").alias("outer_b_alias")
                ])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(outer_table.clone())
            .filter(col("outer_table.a").gt(lit(1)).and(exists(sq_level1)))?
            .build()?;

        // Filter: outer_table.a > Int32(1) AND EXISTS (<subquery>)
        //   Subquery:
        //     Projection: outer_ref(outer_table.b) AS outer_b_alias
        //       Filter: inner_table_lv1.a = outer_ref(outer_table.a) AND outer_ref(outer_table.a) > inner_table_lv1.c AND in
        // ner_table_lv1.b = Int32(1) AND outer_ref(outer_table.b) = inner_table_lv1.b
        //         TableScan: inner_table_lv1
        //   TableScan: outer_table

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
    fn uncorrelated_exist_subquery() -> Result<()> {
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

        // Filter: outer_table.a > Int32(1) AND EXISTS (<subquery>)
        //   Subquery:
        //     Projection: inner_table_lv1.b, inner_table_lv1.a
        //       Filter: inner_table_lv1.b = Int32(1)
        //         TableScan: inner_table_lv1
        //   TableScan: outer_table

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
    fn uncorrelated_in_subquery() -> Result<()> {
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

        // Filter: outer_table.a > Int32(1) AND outer_table.c IN (<subquery>)
        //   Subquery:
        //     Projection: inner_table_lv1.b
        //       Filter: inner_table_lv1.b = Int32(1)
        //         TableScan: inner_table_lv1
        //   TableScan: outer_table

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
    fn correlated_in_subquery() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;
        let sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1)
                .filter(
                    col("inner_table_lv1.a")
                        .eq(out_ref_col(DataType::UInt32, "outer_table.a"))
                        .and(
                            out_ref_col(DataType::UInt32, "outer_table.a")
                                .gt(col("inner_table_lv1.c")),
                        )
                        .and(col("inner_table_lv1.b").eq(lit(1)))
                        .and(
                            out_ref_col(DataType::UInt32, "outer_table.b")
                                .eq(col("inner_table_lv1.b")),
                        ),
                )?
                .project(vec![
                    out_ref_col(DataType::UInt32, "outer_table.b").alias("outer_b_alias")
                ])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(outer_table.clone())
            .filter(
                col("outer_table.a")
                    .gt(lit(1))
                    .and(in_subquery(col("outer_table.c"), sq_level1)),
            )?
            .build()?;

        // Filter: outer_table.a > Int32(1) AND outer_table.c IN (<subquery>)
        //   Subquery:
        //     Projection: outer_ref(outer_table.b) AS outer_b_alias
        //       Filter: inner_table_lv1.a = outer_ref(outer_table.a) AND outer_ref(outer_table.a) > inner_table_lv1.c AND inner_table_lv1.b = Int32(1) AND outer_ref(outer_table.b) = inner_table_lv1.b
        //         TableScan: inner_table_lv1
        //   TableScan: outer_table

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
    fn correlated_subquery_with_alias() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;
        let sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1)
                .filter(
                    col("inner_table_lv1.a")
                        .eq(out_ref_col(DataType::UInt32, "outer_table_alias.a")),
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

        // Filter: outer_table.a > Int32(1) AND outer_table.c IN (<subquery>)
        //   Subquery:
        //     Projection: count(inner_table_lv1.a) AS count_a
        //       Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv1.a)]]
        //         Filter: inner_table_lv1.a = outer_ref(outer_table_alias.a)
        //           TableScan: inner_table_lv1
        //   SubqueryAlias: outer_table_alias
        //     TableScan: outer_table

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

    // from duckdb test: https://github.com/duckdb/duckdb/blob/main/test/sql/subquery/any_all/test_correlated_any_all.test
    #[test]
    fn test_correlated_any_all_1() -> Result<()> {
        // CREATE TABLE integers(i INTEGER);
        // SELECT i = ANY(
        //     SELECT i
        //     FROM integers
        //     WHERE i = i1.i
        // )
        // FROM integers i1
        // ORDER BY i;

        // Create base table
        let integers = test_table_with_columns("integers", &[("i", DataType::Int32)])?;

        // Build correlated subquery:
        // SELECT i FROM integers WHERE i = i1.i
        let subquery = Arc::new(
            LogicalPlanBuilder::from(integers.clone())
                .filter(col("integers.i").eq(out_ref_col(DataType::Int32, "i1.i")))?
                .project(vec![col("integers.i")])?
                .build()?,
        );

        // Build main query with table alias i1
        let plan = LogicalPlanBuilder::from(integers)
            .alias("i1")? // Alias the table as i1
            .filter(
                // i = ANY(subquery)
                Expr::InSubquery(InSubquery {
                    expr: Box::new(col("i1.i")),
                    subquery: Subquery {
                        subquery,
                        outer_ref_columns: vec![out_ref_col(DataType::Int32, "i1.i")],
                        spans: Spans::new(),
                    },
                    negated: false,
                }),
            )?
            .sort(vec![SortExpr::new(col("i1.i"), false, false)])? // ORDER BY i
            .build()?;

        // original plan:
        // Sort: i1.i DESC NULLS LAST
        //   Filter: i1.i IN (<subquery>)
        //     Subquery:
        //       Projection: integers.i
        //         Filter: integers.i = outer_ref(i1.i)
        //           TableScan: integers
        //     SubqueryAlias: i1
        //       TableScan: integers

        // Verify the rewrite result
        assert_dependent_join_rewrite!(
            plan,
            @r#"
            Sort: i1.i DESC NULLS LAST [i:Int32]
              Projection: i1.i [i:Int32]
                Filter: __in_sq_1.output [i:Int32, output:Boolean]
                  DependentJoin on [i1.i lvl 1] with expr i1.i IN (<subquery>) depth 1 [i:Int32, output:Boolean]
                    SubqueryAlias: i1 [i:Int32]
                      TableScan: integers [i:Int32]
                    Projection: integers.i [i:Int32]
                      Filter: integers.i = outer_ref(i1.i) [i:Int32]
                        TableScan: integers [i:Int32]
        "#
        );

        Ok(())
    }

    // from duckdb: https://github.com/duckdb/duckdb/blob/main/test/sql/subquery/any_all/issue_2999.test
    #[test]
    fn test_any_subquery_with_derived_join() -> Result<()> {
        // SQL equivalent:
        // CREATE TABLE t0 (c0 INT);
        // CREATE TABLE t1 (c0 INT);
        // SELECT 1 = ANY(
        //     SELECT 1
        //     FROM t1
        //     JOIN (
        //         SELECT count(*)
        //         GROUP BY t0.c0
        //     ) AS x(x) ON TRUE
        // )
        // FROM t0;

        // Create base tables
        let t0 = test_table_with_columns("t0", &[("c0", DataType::Int32)])?;
        let t1 = test_table_with_columns("t1", &[("c0", DataType::Int32)])?;

        // Build derived table subquery:
        // SELECT count(*) GROUP BY t0.c0
        let derived_table = Arc::new(
            LogicalPlanBuilder::from(t1.clone())
                .aggregate(
                    vec![out_ref_col(DataType::Int32, "t0.c0")], // GROUP BY t0.c0
                    vec![count(lit(1))],                         // count(*)
                )?
                .build()?,
        );

        // Build the join subquery:
        // SELECT 1 FROM t1 JOIN (derived_table) x(x) ON TRUE
        let join_subquery = Arc::new(
            LogicalPlanBuilder::from(t1)
                .join_on(
                    LogicalPlan::Subquery(Subquery {
                        subquery: derived_table,
                        outer_ref_columns: vec![out_ref_col(DataType::Int32, "t0.c0")],
                        spans: Spans::new(),
                    }),
                    JoinType::Inner,
                    vec![lit(true)], // ON TRUE
                )?
                .project(vec![lit(1)])? // SELECT 1
                .build()?,
        );

        // Build main query
        let plan = LogicalPlanBuilder::from(t0)
            .filter(
                // 1 = ANY(subquery)
                Expr::InSubquery(InSubquery {
                    expr: Box::new(lit(1)),
                    subquery: Subquery {
                        subquery: join_subquery,
                        outer_ref_columns: vec![out_ref_col(DataType::Int32, "t0.c0")],
                        spans: Spans::new(),
                    },
                    negated: false,
                }),
            )?
            .build()?;

        // Filter: Int32(1) IN (<subquery>)
        //   Subquery:
        //     Projection: Int32(1)
        //       Inner Join:  Filter: Boolean(true)
        //         TableScan: t1
        //         Subquery:
        //           Aggregate: groupBy=[[outer_ref(t0.c0)]], aggr=[[count(Int32(1))]]
        //             TableScan: t1
        //   TableScan: t0

        // Verify the rewrite result
        assert_dependent_join_rewrite!(
            plan,
            @r#"
            Projection: t0.c0 [c0:Int32]
              Filter: __in_sq_2.output [c0:Int32, output:Boolean]
                DependentJoin on [t0.c0 lvl 2] with expr Int32(1) IN (<subquery>) depth 1 [c0:Int32, output:Boolean]
                  TableScan: t0 [c0:Int32]
                  Projection: Int32(1) [Int32(1):Int32]
                    DependentJoin on [] lateral Inner join with Boolean(true) depth 2 [c0:Int32]
                      TableScan: t1 [c0:Int32]
                      Aggregate: groupBy=[[outer_ref(t0.c0)]], aggr=[[count(Int32(1))]] [outer_ref(t0.c0):Int32;N, count(Int32(1)):Int64]
                        TableScan: t1 [c0:Int32]
        "#
        );

        Ok(())
    }

    #[test]
    fn test_simple_correlated_agg_subquery() -> Result<()> {
        // CREATE TABLE t(a INT, b INT);
        // SELECT a,
        //     (SELECT SUM(b)
        //      FROM t t2
        //      WHERE t2.a = t1.a) as sum_b
        // FROM t t1;

        // Create base table
        let t = test_table_with_columns(
            "t",
            &[("a", DataType::Int32), ("b", DataType::Int32)],
        )?;

        // Build scalar subquery:
        // SELECT SUM(b) FROM t t2 WHERE t2.a = t1.a
        let scalar_sub = Arc::new(
            LogicalPlanBuilder::from(t.clone())
                .alias("t2")?
                .filter(col("t2.a").eq(out_ref_col(DataType::Int32, "t1.a")))?
                .aggregate(
                    vec![col("t2.b")],      // No GROUP BY
                    vec![sum(col("t2.b"))], // SUM(b)
                )?
                .build()?,
        );

        // Build main query
        let plan = LogicalPlanBuilder::from(t)
            .alias("t1")?
            .project(vec![
                col("t1.a"),                 // a
                scalar_subquery(scalar_sub), // (SELECT SUM(b) ...)
            ])?
            .build()?;

        // Projection: t1.a, (<subquery>)
        //   Subquery:
        //     Aggregate: groupBy=[[t2.b]], aggr=[[sum(t2.b)]]
        //       Filter: t2.a = outer_ref(t1.a)
        //         SubqueryAlias: t2
        //           TableScan: t
        //   SubqueryAlias: t1
        //     TableScan: t

        // Verify the rewrite result
        assert_dependent_join_rewrite!(
            plan,
            @r#"
            Projection: t1.a, __scalar_sq_1.output [a:Int32, output:Int32]
              DependentJoin on [t1.a lvl 1] with expr (<subquery>) depth 1 [a:Int32, b:Int32, output:Int32]
                SubqueryAlias: t1 [a:Int32, b:Int32]
                  TableScan: t [a:Int32, b:Int32]
                Aggregate: groupBy=[[t2.b]], aggr=[[sum(t2.b)]] [b:Int32, sum(t2.b):Int64;N]
                  Filter: t2.a = outer_ref(t1.a) [a:Int32, b:Int32]
                    SubqueryAlias: t2 [a:Int32, b:Int32]
                      TableScan: t [a:Int32, b:Int32]
        "#
        );

        Ok(())
    }

    #[test]
    fn test_simple_subquery_in_agg() -> Result<()> {
        // CREATE TABLE t(a INT, b INT);
        // SELECT a,
        //     SUM(
        //         (SELECT b FROM t t2 WHERE t2.a = t1.a)
        //     ) as sum_scalar
        // FROM t t1
        // GROUP BY a;

        // Create base table
        let t = test_table_with_columns(
            "t",
            &[("a", DataType::Int32), ("b", DataType::Int32)],
        )?;

        // Build inner scalar subquery:
        // SELECT b FROM t t2 WHERE t2.a = t1.a
        let scalar_sub = Arc::new(
            LogicalPlanBuilder::from(t.clone())
                .alias("t2")?
                .filter(col("t2.a").eq(out_ref_col(DataType::Int32, "t1.a")))?
                .project(vec![col("t2.b")])? // SELECT b
                .build()?,
        );

        // Build main query
        let plan = LogicalPlanBuilder::from(t)
            .alias("t1")?
            .aggregate(
                vec![col("t1.a")], // GROUP BY a
                vec![sum(scalar_subquery(scalar_sub)) // SUM((SELECT b ...))
                    .alias("sum_scalar")],
            )?
            .build()?;

        // Aggregate: groupBy=[[t1.a]], aggr=[[sum((<subquery>)) AS sum_scalar]]
        //   Subquery:
        //     Projection: t2.b
        //       Filter: t2.a = outer_ref(t1.a)
        //         SubqueryAlias: t2
        //           TableScan: t
        //   SubqueryAlias: t1
        //     TableScan: t

        // Verify the rewrite result
        assert_dependent_join_rewrite!(
            plan,
            @r#"
            Projection: t1.a, sum_scalar [a:Int32, sum_scalar:Int64;N]
              Aggregate: groupBy=[[t1.a]], aggr=[[sum(__scalar_sq_1.output) AS sum_scalar]] [a:Int32, sum_scalar:Int64;N]
                DependentJoin on [t1.a lvl 1] with expr (<subquery>) depth 1 [a:Int32, b:Int32, output:Int32]
                  SubqueryAlias: t1 [a:Int32, b:Int32]
                    TableScan: t [a:Int32, b:Int32]
                  Projection: t2.b [b:Int32]
                    Filter: t2.a = outer_ref(t1.a) [a:Int32, b:Int32]
                      SubqueryAlias: t2 [a:Int32, b:Int32]
                        TableScan: t [a:Int32, b:Int32]
        "#
        );

        Ok(())
    }

    #[test]
    // https://github.com/duckdb/duckdb/blob/4d7cb701cabd646d8232a9933dd058a089ea7348/test/sql/subquery/any_all/subquery_in.test
    fn correlated_scalar_subquery_returning_more_than_1_row() -> Result<()> {
        //  SELECT (FALSE) IN (TRUE, (SELECT TIME '13:35:07' FROM t1) BETWEEN t0.c0 AND t0.c0) FROM t0;
        let t0 = test_table_with_columns(
            "t0",
            &[
                ("c0", DataType::Time64(TimeUnit::Second)),
                ("c1", DataType::Float64),
            ],
        )?;
        let t1 = test_table_with_columns("t1", &[("c0", DataType::Int32)])?;
        let t1_subquery = Arc::new(
            LogicalPlanBuilder::from(t1)
                .project(vec![lit("13:35:07")])?
                .build()?,
        );
        let plan = LogicalPlanBuilder::from(t0)
            .project(vec![lit(false).in_list(
                vec![
                    lit(true),
                    scalar_subquery(t1_subquery).between(col("t0.c0"), col("t0.c0")),
                ],
                false,
            )])?
            .build()?;
        // Projection: Boolean(false) IN ([Boolean(true), (<subquery>) BETWEEN t0.c0 AND t0.c0])
        //   Subquery:
        //     Projection: Utf8("13:35:07")
        //       TableScan: t1
        //   TableScan: t0
        assert_dependent_join_rewrite!(
            plan,
            @r#"
        Projection: Boolean(false) IN ([Boolean(true), __scalar_sq_1.output BETWEEN t0.c0 AND t0.c0]) [Boolean(false) IN Boolean(true), __scalar_sq_1.output BETWEEN t0.c0 AND t0.c0:Boolean]
          DependentJoin on [] with expr (<subquery>) depth 1 [c0:Time64(Second), c1:Float64, output:Utf8]
            TableScan: t0 [c0:Time64(Second), c1:Float64]
            Projection: Utf8("13:35:07") [Utf8("13:35:07"):Utf8]
              TableScan: t1 [c0:Int32]
        "#
        );

        Ok(())
    }

    #[test]
    fn test_correlated_subquery_in_join_filter() -> Result<()> {
        // Test demonstrates traversal order issue with subquery in JOIN condition
        // Query pattern:
        // SELECT * FROM t1
        // JOIN t2 ON t2.key = t1.key
        //   AND t2.val > (SELECT COUNT(*) FROM t3 WHERE t3.id = t1.id);

        let t1 = test_table_with_columns(
            "t1",
            &[
                ("key", DataType::Int32),
                ("id", DataType::Int32),
                ("val", DataType::Int32),
            ],
        )?;

        let t2 = test_table_with_columns(
            "t2",
            &[("key", DataType::Int32), ("val", DataType::Int32)],
        )?;

        let t3 = test_table_with_columns(
            "t3",
            &[("id", DataType::Int32), ("val", DataType::Int32)],
        )?;

        // Subquery in join condition: SELECT COUNT(*) FROM t3 WHERE t3.id = t1.id
        let scalar_sq = Arc::new(
            LogicalPlanBuilder::from(t3)
                .filter(col("t3.id").eq(out_ref_col(DataType::Int32, "t1.id")))?
                .aggregate(Vec::<Expr>::new(), vec![count(lit(1))])?
                .build()?,
        );

        // Build join condition: t2.key = t1.key AND t2.val > scalar_sq AND EXISTS(exists_sq)
        let join_condition = and(
            col("t2.key").eq(col("t1.key")),
            col("t2.val").gt(scalar_subquery(scalar_sq)),
        );
        let plan = LogicalPlanBuilder::from(t1)
            .join_on(t2, JoinType::Inner, vec![join_condition])?
            .build()?;

        // Inner Join:  Filter: t2.key = t1.key AND t2.val > (<subquery>)
        //   Subquery:
        //     Aggregate: groupBy=[[]], aggr=[[count(Int32(1))]]
        //       Filter: t3.id = outer_ref(t1.id)
        //         TableScan: t3
        //   TableScan: t1
        //   TableScan: t2

        assert_dependent_join_rewrite!(
            plan,
            @r"
        Filter: t2.key = t1.key AND t2.val > __scalar_sq_1.output [key:Int32, id:Int32, val:Int32, key:Int32, val:Int32, output:Int64]
          DependentJoin on [t1.id lvl 1] with expr (<subquery>) depth 1 [key:Int32, id:Int32, val:Int32, key:Int32, val:Int32, output:Int64]
            Cross Join(ComparisonJoin):  [key:Int32, id:Int32, val:Int32, key:Int32, val:Int32]
              TableScan: t1 [key:Int32, id:Int32, val:Int32]
              TableScan: t2 [key:Int32, val:Int32]
            Aggregate: groupBy=[[]], aggr=[[count(Int32(1))]] [count(Int32(1)):Int64]
              Filter: t3.id = outer_ref(t1.id) [id:Int32, val:Int32]
                TableScan: t3 [id:Int32, val:Int32]
        "
        );

        Ok(())
    }

    #[test]
    fn test_correlated_subquery_in_lateral_join_filter() -> Result<()> {
        // Test demonstrates traversal order issue with subquery in JOIN condition
        // Query pattern:
        // SELECT * FROM t1
        // JOIN t2 ON t2.key = t1.key
        //   AND t2.val > (SELECT COUNT(*) FROM t3 WHERE t3.id = t1.id);

        let t1 = test_table_with_columns(
            "t1",
            &[
                ("key", DataType::Int32),
                ("id", DataType::Int32),
                ("val", DataType::Int32),
            ],
        )?;

        let t2 = test_table_with_columns(
            "t2",
            &[("key", DataType::Int32), ("val", DataType::Int32)],
        )?;

        let t3 = test_table_with_columns(
            "t3",
            &[("id", DataType::Int32), ("val", DataType::Int32)],
        )?;

        // Subquery in join condition: SELECT COUNT(*) FROM t3 WHERE t3.id = t1.id
        let scalar_sq = Arc::new(
            LogicalPlanBuilder::from(t3)
                .filter(col("t3.id").eq(out_ref_col(DataType::Int32, "t1.id")))?
                .aggregate(Vec::<Expr>::new(), vec![count(lit(1))])?
                .build()?,
        );

        // Build join condition: t2.key = t1.key AND t2.val > scalar_sq AND EXISTS(exists_sq)
        let join_condition = and(
            col("t2.key").eq(col("t1.key")),
            col("t2.val").gt(scalar_subquery(scalar_sq)),
        );

        let plan = LogicalPlanBuilder::from(t1)
            .join_on(
                LogicalPlan::Subquery(Subquery {
                    subquery: t2.into(),
                    outer_ref_columns: vec![],
                    spans: Spans::new(),
                }),
                JoinType::Inner,
                vec![join_condition],
            )?
            .build()?;

        // Inner Join:  Filter: t2.key = t1.key AND t2.val > (<subquery>)
        //   Subquery:
        //     Aggregate: groupBy=[[]], aggr=[[count(Int32(1))]]
        //       Filter: t3.id = outer_ref(t1.id)
        //         TableScan: t3
        //   TableScan: t1
        //   TableScan: t2
        assert_dependent_join_rewrite_err!(
            plan,
            @"This feature is not implemented: subquery inside lateral join condition is not supported"
        );

        Ok(())
    }

    #[test]
    fn test_multiple_correlated_subqueries_in_join_filter() -> Result<()> {
        // Test demonstrates traversal order issue with subquery in JOIN condition
        // Query pattern:
        // SELECT * FROM t1
        // JOIN t2 ON (t2.key = t1.key
        //   AND t2.val > (SELECT COUNT(*) FROM t3 WHERE t3.id = t1.id))
        // OR exits (
        //  SELECT * FROM T3 WHERE T3.ID = T2.KEY
        // );

        let t1 = test_table_with_columns(
            "t1",
            &[
                ("key", DataType::Int32),
                ("id", DataType::Int32),
                ("val", DataType::Int32),
            ],
        )?;

        let t2 = test_table_with_columns(
            "t2",
            &[("key", DataType::Int32), ("val", DataType::Int32)],
        )?;

        let t3 = test_table_with_columns(
            "t3",
            &[("id", DataType::Int32), ("val", DataType::Int32)],
        )?;

        // Subquery in join condition: SELECT COUNT(*) FROM t3 WHERE t3.id = t1.id
        let scalar_sq = Arc::new(
            LogicalPlanBuilder::from(t3.clone())
                .filter(col("t3.id").eq(out_ref_col(DataType::Int32, "t1.id")))?
                .aggregate(Vec::<Expr>::new(), vec![count(lit(1))])?
                .build()?,
        );
        let exists_sq = Arc::new(
            LogicalPlanBuilder::from(t3)
                .filter(col("t3.id").eq(out_ref_col(DataType::Int32, "t2.key")))?
                .build()?,
        );

        // Build join condition: (t2.key = t1.key AND t2.val > scalar_sq) OR (exists(exists_sq))
        let join_condition = and(
            col("t2.key").eq(col("t1.key")),
            col("t2.val").gt(scalar_subquery(scalar_sq)),
        )
        .or(exists(exists_sq));

        let plan = LogicalPlanBuilder::from(t1)
            .join_on(t2, JoinType::Inner, vec![join_condition])?
            .build()?;
        // Inner Join:  Filter: t2.key = t1.key AND t2.val > (<subquery>) OR EXISTS (<subquery>)
        //   Subquery:
        //     Aggregate: groupBy=[[]], aggr=[[count(Int32(1))]]
        //       Filter: t3.id = outer_ref(t1.id)
        //         TableScan: t3
        //   Subquery:
        //     Filter: t3.id = outer_ref(t2.key)
        //       TableScan: t3
        //   TableScan: t1
        //   TableScan: t2

        assert_dependent_join_rewrite!(
            plan,
            @r"
        Filter: t2.key = t1.key AND t2.val > __scalar_sq_1.output OR __exists_sq_2.output [key:Int32, id:Int32, val:Int32, key:Int32, val:Int32, output:Int64, output:Boolean]
          DependentJoin on [t2.key lvl 1] with expr EXISTS (<subquery>) depth 1 [key:Int32, id:Int32, val:Int32, key:Int32, val:Int32, output:Int64, output:Boolean]
            DependentJoin on [t1.id lvl 1] with expr (<subquery>) depth 1 [key:Int32, id:Int32, val:Int32, key:Int32, val:Int32, output:Int64]
              Cross Join(ComparisonJoin):  [key:Int32, id:Int32, val:Int32, key:Int32, val:Int32]
                TableScan: t1 [key:Int32, id:Int32, val:Int32]
                TableScan: t2 [key:Int32, val:Int32]
              Aggregate: groupBy=[[]], aggr=[[count(Int32(1))]] [count(Int32(1)):Int64]
                Filter: t3.id = outer_ref(t1.id) [id:Int32, val:Int32]
                  TableScan: t3 [id:Int32, val:Int32]
            Filter: t3.id = outer_ref(t2.key) [id:Int32, val:Int32]
              TableScan: t3 [id:Int32, val:Int32]
        "
        );

        Ok(())
    }
}
