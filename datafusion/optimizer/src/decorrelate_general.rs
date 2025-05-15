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

//! [`GeneralPullUpCorrelatedExpr`] converts correlated subqueries to `Joins`

use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use crate::analyzer::type_coercion::TypeCoercionRewriter;
use crate::decorrelate::UN_MATCHED_ROW_INDICATOR;
use crate::{ApplyOrder, OptimizerConfig, OptimizerRule};

use datafusion_common::alias::AliasGenerator;
use datafusion_common::tree_node::{
    Transformed, TreeNode, TreeNodeRecursion, TreeNodeVisitor,
};
use datafusion_common::{internal_err, Column, Result};
use datafusion_expr::expr::{self, Exists};
use datafusion_expr::expr_rewriter::strip_outer_reference;
use datafusion_expr::select_expr::SelectExpr;
use datafusion_expr::utils::{
    conjunction, disjunction, split_conjunction, split_disjunction,
};
use datafusion_expr::{
    binary_expr, col, lit, BinaryExpr, Cast, Expr, JoinType, LogicalPlan,
    LogicalPlanBuilder, Operator as ExprOperator, Subquery,
};
// use datafusion_sql::unparser::Unparser;

use datafusion_sql::unparser::Unparser;
// use datafusion_sql::TableReference;
use indexmap::map::Entry;
use indexmap::{IndexMap, IndexSet};
use itertools::Itertools;
use log::Log;

pub struct DependentJoinTracker {
    root: Option<usize>,
    // each logical plan traversal will assign it a integer id
    current_id: usize,
    // each newly visted operator is inserted inside this map for tracking
    nodes: IndexMap<usize, Operator>,
    // all the node ids from root to the current node
    // this is used during traversal only
    stack: Vec<usize>,
    // track for each column, the nodes/logical plan that reference to its within the tree
    accessed_columns: IndexMap<Column, Vec<ColumnAccess>>,
    alias_generator: Arc<AliasGenerator>,
}

#[derive(Debug, Hash, PartialEq, PartialOrd, Eq, Clone)]
struct ColumnAccess {
    stack: Vec<usize>,
    node_id: usize,
    col: Column,
}
// pub struct GeneralDecorrelation {
//     index: AlgebraIndex,
// }

// data structure to store equivalent columns
// Expr is used to represent either own column or outer referencing columns
#[derive(Clone)]
pub struct UnionFind {
    parent: IndexMap<Expr, Expr>,
    rank: IndexMap<Expr, usize>,
}

impl UnionFind {
    pub fn new() -> Self {
        Self {
            parent: IndexMap::new(),
            rank: IndexMap::new(),
        }
    }

    pub fn find(&mut self, x: Expr) -> Expr {
        let p = self.parent.get(&x).cloned();
        match p {
            None => {
                self.parent.insert(x.clone(), x.clone());
                self.rank.insert(x.clone(), 0);
                x
            }
            Some(parent) => {
                if parent == x {
                    x
                } else {
                    let root = self.find(parent.clone());
                    self.parent.insert(x, root.clone());
                    root
                }
            }
        }
    }

    pub fn union(&mut self, x: Expr, y: Expr) -> bool {
        let root_x = self.find(x.clone());
        let root_y = self.find(y.clone());
        if root_x == root_y {
            return false;
        }

        let rank_x = *self.rank.get(&root_x).unwrap_or(&0);
        let rank_y = *self.rank.get(&root_y).unwrap_or(&0);

        if rank_x < rank_y {
            self.parent.insert(root_x, root_y);
        } else if rank_x > rank_y {
            self.parent.insert(root_y, root_x);
        } else {
            // asign y as children of x
            self.parent.insert(root_y.clone(), root_x.clone());
            *self.rank.entry(root_x).or_insert(0) += 1;
        }

        true
    }
}
// TODO: impl me
#[derive(Clone)]
struct DependentJoin {
    //
    original_expr: LogicalPlan,
    left: Operator,
    right: Operator,
    // TODO: combine into one Expr
    join_conditions: Vec<Expr>,
    // join_type:
}
impl DependentJoin {}

#[derive(Clone)]
struct UnnestingInfo {
    // join: DependentJoin,
    domain: LogicalPlan,
    parent: Option<Unnesting>,
}
#[derive(Clone)]
struct Unnesting {
    info: Arc<UnnestingInfo>, // cclasses: union find data structure of equivalent columns
    equivalences: UnionFind,
    need_handle_count_bug: bool,

    // for each outer exprs on the left, the set of exprs
    // on the right required pulling up for the join condition to happen
    // i.e select * from t1 where t1.col1 = (
    // select count(*) from t2 where t2.col1 > t1.col2 + t2.col2 or t1.col3 = t1.col2 or t1.col4=2 and t1.col3=1)
    // we do this by split the complex expr into conjuctive sets
    // for each of such set, if there exists any or binary operator
    // we substitute the whole binary operator as true and add every expr appearing in the or condition
    // to grouped_by
    // and push every
    pulled_up_columns: Vec<Column>,
    //these predicates are conjunctive
    pulled_up_predicates: Vec<Expr>,
    count_exprs_dectected: IndexSet<Expr>,
    // mapping from outer ref column to new column, if any
    // i.e in some subquery (
    // ... where outer.column_c=inner.column_a
    // )
    // and through union find we have outer.column_c = some_other_expr
    // we can substitute the inner query with inner.column_a=some_other_expr
    replaces: IndexMap<Column, Column>,

    join_conditions: Vec<Expr>,
}
impl Unnesting {
    fn get_replaced_col(&self, col: &Column) -> Column {
        match self.replaces.get(col) {
            Some(col) => col.clone(),
            None => col.clone(),
        }
    }
}

// TODO: looks like this function can be improved to allow more expr pull up
fn can_pull_up(expr: &Expr) -> bool {
    if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr {
        match op {
            ExprOperator::Eq
            | ExprOperator::Gt
            | ExprOperator::Lt
            | ExprOperator::GtEq
            | ExprOperator::LtEq => {}
            _ => return false,
        }
        match (left.deref(), right.deref()) {
            (Expr::Column(_), right) => !right.any_column_refs(),
            (left, Expr::Column(_)) => !left.any_column_refs(),
            (Expr::Cast(Cast { expr, .. }), right)
                if matches!(expr.deref(), Expr::Column(_)) =>
            {
                !right.any_column_refs()
            }
            (left, Expr::Cast(Cast { expr, .. }))
                if matches!(expr.deref(), Expr::Column(_)) =>
            {
                !left.any_column_refs()
            }
            (_, _) => false,
        }
    } else {
        false
    }
}

struct SimpleDecorrelationResult {
    // new: Option<LogicalPlan>,
    // if projection pull up happened, each will be tracked, so that later on general decorrelation
    // can rewrite them (a.k.a outer ref column maybe renamed/substituted some where in the parent already
    // because the decorrelation is top-down)
    pulled_up_projections: IndexSet<Expr>,
    pulled_up_predicates: Vec<Expr>,
}

fn try_transform_subquery_to_join_expr(
    expr: &Expr,
    sq: &Subquery,
    replace_columns: &[Expr],
) -> Result<(bool, Option<Expr>, Option<Expr>)> {
    let mut post_join_predicate = None;

    // this is used for exist query
    let mut join_predicate = None;

    let found_sq = expr.exists(|e| match e {
        Expr::InSubquery(isq) => {
            if replace_columns.len() != 1 {
                return internal_err!(
                    "result of IN subquery should only involve one column"
                );
            }
            if isq.subquery == *sq {
                if isq.negated {
                    join_predicate = Some(binary_expr(
                        *isq.expr.clone(),
                        ExprOperator::NotEq,
                        strip_outer_reference(replace_columns[0].clone()),
                    ));
                    return Ok(true);
                }

                join_predicate = Some(binary_expr(
                    *isq.expr.clone(),
                    ExprOperator::Eq,
                    strip_outer_reference(replace_columns[0].clone()),
                ));
                return Ok(true);
            }
            return Ok(false);
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let (exist, transformed, post_join_expr_from_left) =
                try_transform_subquery_to_join_expr(left.as_ref(), sq, replace_columns)?;
            if !exist {
                let (right_exist, transformed_right, post_join_expr_from_right) =
                    try_transform_subquery_to_join_expr(
                        right.as_ref(),
                        sq,
                        replace_columns,
                    )?;
                if !right_exist {
                    return Ok(false);
                }
                if let Some(transformed_right) = transformed_right {
                    join_predicate =
                        Some(binary_expr(*left.clone(), *op, transformed_right));
                }
                if let Some(transformed_right) = post_join_expr_from_right {
                    post_join_predicate =
                        Some(binary_expr(*left.clone(), *op, transformed_right));
                }

                return Ok(true);
            }
            // TODO: exist query won't have any transformed expr,
            // meaning this query is not supported `where bool_col = exists(subquery)`

            if let Some(transformed) = transformed {
                join_predicate = Some(binary_expr(transformed, *op, *right.clone()));
            }
            if let Some(transformed) = post_join_expr_from_left {
                post_join_predicate = Some(binary_expr(transformed, *op, *right.clone()));
            }
            return Ok(true);
        }
        Expr::Exists(Exists {
            subquery: inner_sq,
            negated,
            ..
        }) => {
            if inner_sq.clone() == *sq {
                let mark_predicate = if *negated { !col("mark") } else { col("mark") };
                post_join_predicate = Some(mark_predicate);
                return Ok(true);
            }
            return Ok(false);
        }
        Expr::ScalarSubquery(ssq) => {
            unimplemented!(
                "we need to store map between scalarsubquery and replaced_expr later on"
            );
            if let LogicalPlan::Subquery(inner_sq) = ssq.subquery.as_ref() {
                if inner_sq.clone() == *sq {
                    return Ok(true);
                }
            }
            return Ok(false);
        }
        _ => Ok(false),
    })?;
    return Ok((found_sq, join_predicate, post_join_predicate));
}

// impl Default for GeneralDecorrelation {
//     fn default() -> Self {
//         return GeneralDecorrelation {
//             index: AlgebraIndex::default(),
//         };
//     }
// }
struct GeneralDecorrelationResult {
    // i.e for aggregation, dependent columns are added to the projection for joining
    added_columns: Vec<Expr>,
    // the reason is, unnesting group by happen at lower nodes,
    // but the filtering (if any) of such expr may happen higher node
    // (because of known count_bug)
    count_expr_map: HashSet<Expr>,
}
impl DependentJoinTracker {
    fn is_linear_operator(&self, plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Limit(_) => true,
            LogicalPlan::TableScan(_) => true,
            LogicalPlan::Projection(_) => true,
            LogicalPlan::Filter(_) => true,
            LogicalPlan::Repartition(_) => true,
            _ => false,
        }
    }
    fn is_linear_path(&self, parent: &Operator, child: &Operator) -> bool {
        if !self.is_linear_operator(&child.plan) {
            return false;
        }

        let mut current_node = child.parent.unwrap();

        loop {
            let child_node = self.nodes.get(&current_node).unwrap();
            if !self.is_linear_operator(&child_node.plan) {
                match child_node.parent {
                    None => {
                        unimplemented!("traversing from descedent to top does not meet expected root")
                    }
                    Some(new_parent) => {
                        if new_parent == parent.id {
                            return true;
                        }
                        return false;
                    }
                }
            }
            match child_node.parent {
                None => return true,
                Some(new_parent) => {
                    current_node = new_parent;
                }
            };
        }
    }
    fn remove_node(&mut self, parent: &mut Operator, node: &mut Operator) {
        let next_children = node.children.first().unwrap();
        let next_children_node = self.nodes.swap_remove(next_children).unwrap();
        // let next_children_node = self.nodes.get_mut(next_children).unwrap();
        *node = next_children_node;
        node.parent = Some(parent.id);
    }
    // decorrelate all descendant(recursively) with simple unnesting
    // returns true if all children were eliminated
    // TODO(impl me)
    fn try_simple_decorrelate_descendent(
        &mut self,
        root_node: &mut Operator,
        child_node: &mut Operator,
        col_access: &ColumnAccess,
        result: &mut SimpleDecorrelationResult,
    ) -> Result<()> {
        // unnest children first
        // println!("decorrelating {} from {}", child, root);

        if !self.is_linear_path(root_node, child_node) {
            // TODO:
            return Ok(());
        }

        // TODO: inplace update
        // let mut child_node = self.nodes.swap_remove(child).unwrap().clone();
        // let mut root_node = self.nodes.swap_remove(root).unwrap();

        match &mut child_node.plan {
            LogicalPlan::Projection(proj) => {
                // TODO: handle the case outer_ref_a + outer_ref_b???
                // if we only see outer_ref_a and decide to move the whole expr
                // outer_ref_b is accidentally pulled up
                let pulled_up_expr: IndexSet<_> = proj
                    .expr
                    .iter()
                    .filter(|proj_expr| {
                        proj_expr
                            .exists(|expr| {
                                // TODO: what if parent has already rewritten outer_ref_col
                                if let Expr::OuterReferenceColumn(_, col) = expr {
                                    root_node.access_tracker.remove(col_access);
                                    return Ok(*col == col_access.col);
                                }
                                Ok(false)
                            })
                            .unwrap()
                    })
                    .cloned()
                    .collect();

                if !pulled_up_expr.is_empty() {
                    for expr in pulled_up_expr.iter() {
                        result.pulled_up_projections.insert(expr.clone());
                    }
                    // all expr of this node is pulled up, fully remove this node from the tree
                    if proj.expr.len() == pulled_up_expr.len() {
                        self.remove_node(root_node, child_node);
                        return Ok(());
                    }

                    let new_proj = proj
                        .expr
                        .iter()
                        .filter(|expr| !pulled_up_expr.contains(*expr))
                        .cloned()
                        .collect();
                    proj.expr = new_proj;
                }
                // TODO: try_decorrelate for each of the child
            }
            LogicalPlan::Filter(filter) => {
                // let accessed_from_child = &child_node.access_tracker;
                let subquery_filter_exprs: Vec<Expr> =
                    split_conjunction(&filter.predicate)
                        .into_iter()
                        .cloned()
                        .collect();

                let (pulled_up, kept): (Vec<_>, Vec<_>) = subquery_filter_exprs
                    .iter()
                    .cloned()
                    .partition(|e| e.contains_outer() && can_pull_up(e));

                // only remove the access tracker if non of the kept expr contains reference to the column
                // i.e some of the remaining expr still reference to the column and not pullable
                let removable = kept.iter().all(|e| {
                    !e.exists(|e| {
                        if let Expr::Column(col) = e {
                            return Ok(*col == col_access.col);
                        }
                        Ok(false)
                    })
                    .unwrap()
                });
                if removable {
                    root_node.access_tracker.swap_remove(col_access);
                }
                result.pulled_up_predicates.extend(pulled_up);
                if kept.is_empty() {
                    self.remove_node(root_node, child_node);
                    return Ok(());
                }
                filter.predicate = conjunction(kept).unwrap();
            }

            // LogicalPlan::Subquery(sq) => {
            //     let descendent_id = child_node.children.get(0).unwrap();
            //     let mut descendent_node = self.nodes.get(descendent_id).unwrap().clone();
            //     self.try_simple_unnest_descendent(
            //         root_node,
            //         &mut descendent_node,
            //         result,
            //     )?;
            //     self.nodes.insert(*descendent_id, descendent_node);
            // }
            _ => {
                // unimplemented!(
                //     "simple unnest is missing for this operator {}",
                //     child_node.plan
                // )
            }
        };

        Ok(())
    }

    fn general_decorrelate(
        &mut self,
        node: &mut Operator,
        unnesting: &mut Unnesting,
        outer_refs_from_parent: &mut IndexSet<ColumnAccess>,
    ) -> Result<()> {
        if node.is_dependent_join_node {
            unimplemented!("recursive unnest not implemented yet")
        }

        match &mut node.plan {
            LogicalPlan::Subquery(sq) => {
                let next_node = node.children.first().unwrap();
                let mut only_child = self.nodes.swap_remove(next_node).unwrap();
                self.general_decorrelate(
                    &mut only_child,
                    unnesting,
                    outer_refs_from_parent,
                )?;
                *node = only_child;
                return Ok(());
            }
            LogicalPlan::Aggregate(agg) => {
                let is_static = agg.group_expr.is_empty(); // TODO: grouping set also needs to check is_static
                let next_node = node.children.first().unwrap();
                let mut only_child = self.nodes.swap_remove(next_node).unwrap();
                // keep this for later projection
                let mut original_expr = agg.aggr_expr.clone();
                original_expr.extend_from_slice(&agg.group_expr);

                self.general_decorrelate(
                    &mut only_child,
                    unnesting,
                    outer_refs_from_parent,
                )?;
                agg.input = Arc::new(only_child.plan.clone());
                self.nodes.insert(*next_node, only_child);

                Self::rewrite_columns(agg.group_expr.iter_mut(), unnesting)?;
                for col in unnesting.pulled_up_columns.iter() {
                    let replaced_col = unnesting.get_replaced_col(col);
                    agg.group_expr.push(Expr::Column(replaced_col.clone()));
                }
                for agg in agg.aggr_expr.iter() {
                    if contains_count_expr(agg) {
                        unnesting.count_exprs_dectected.insert(agg.clone());
                    }
                }

                if is_static {
                    if !unnesting.count_exprs_dectected.is_empty()
                        & unnesting.need_handle_count_bug
                    {
                        let un_matched_row = lit(true).alias(UN_MATCHED_ROW_INDICATOR);
                        agg.group_expr.push(un_matched_row);
                    }
                    // let right = LogicalPlanBuilder::new(node.plan.clone());
                    // the evaluation of
                    // let mut post_join_projection = vec![];

                    let join_condition =
                        unnesting.pulled_up_predicates.iter().filter_map(|e| {
                            let stripped_outer = strip_outer_reference(e.clone());
                            if contains_count_expr(&stripped_outer) {
                                unimplemented!("handle having count(*) predicate pull up")
                                // post_join_predicates.push(stripped_outer);
                                // return None;
                            }
                            return Some(stripped_outer);
                        });

                    let right = LogicalPlanBuilder::new(agg.input.deref().clone())
                        .aggregate(agg.group_expr.clone(), agg.aggr_expr.clone())?
                        .build()?;
                    let mut new_plan =
                        LogicalPlanBuilder::new(unnesting.info.domain.clone())
                            .join_detailed(
                                right,
                                JoinType::Left,
                                (Vec::<Column>::new(), Vec::<Column>::new()),
                                conjunction(join_condition),
                                true,
                            )?;
                    for expr in original_expr.iter_mut() {
                        if contains_count_expr(expr) {
                            let new_expr = Expr::Case(expr::Case {
                                expr: None,
                                when_then_expr: vec![(
                                    Box::new(Expr::IsNull(Box::new(Expr::Column(
                                        Column::new_unqualified(UN_MATCHED_ROW_INDICATOR),
                                    )))),
                                    Box::new(lit(0)),
                                )],
                                else_expr: Some(Box::new(Expr::Column(
                                    Column::new_unqualified(
                                        expr.schema_name().to_string(),
                                    ),
                                ))),
                            });
                            let mut expr_rewrite = TypeCoercionRewriter {
                                schema: new_plan.schema(),
                            };
                            *expr = new_expr.rewrite(&mut expr_rewrite)?.data;
                        }
                    }
                    new_plan = new_plan.project(original_expr)?;

                    node.plan = new_plan.build()?;

                    println!("{}", node.plan);
                    // self.remove_node(parent, node);

                    // 01)Projection: t1.t1_id, CASE WHEN __scalar_sq_1.__always_true IS NULL THEN Int64(0) ELSE __scalar_sq_1.count(*) END AS count(*)
                    // TODO: how domain projection work
                    // left = select distinct domain
                    // right = new group by
                    // if there exists count in the group by, the projection set should be something like
                    //
                    // 01)Projection: t1.t1_id, CASE WHEN __scalar_sq_1.__always_true IS NULL THEN Int64(0) ELSE __scalar_sq_1.count(*) END AS count(*)
                    // 02)--Left Join: t1.t1_int = __scalar_sq_1.t2_int
                } else {
                    unimplemented!("non static aggregation sq decorrelation not implemented, i.e exists sq with count")
                }
            }
            LogicalPlan::Filter(filter) => {
                let conjuctives: Vec<Expr> = split_conjunction(&filter.predicate)
                    .into_iter()
                    .cloned()
                    .collect();
                let mut remained_expr = vec![];
                // TODO: the paper mention there are 2 approaches to remove these dependent predicate
                // - substitute the outer ref columns and push them to the parent node (i.e add them to aggregation node)
                // - perform a join with domain directly here
                // for now we only implement with the approach substituting

                let mut pulled_up_columns = IndexSet::new();
                for expr in conjuctives.iter() {
                    if !expr.contains_outer() {
                        remained_expr.push(expr.clone());
                        continue;
                    }
                    // extract all columns mentioned in this expr
                    // and push them up the dependent join

                    unnesting.pulled_up_predicates.push(expr.clone());
                    expr.clone().map_children(|e| {
                        if let Expr::Column(ref col) = e {
                            pulled_up_columns.insert(col.clone());
                        }
                        Ok(Transformed::no(e))
                    })?;
                }
                filter.predicate = match conjunction(remained_expr) {
                    Some(expr) => expr,
                    None => lit(true),
                };
                unnesting.pulled_up_columns.extend(pulled_up_columns);
                outer_refs_from_parent.retain(|ac| ac.node_id != node.id);
                if !outer_refs_from_parent.is_empty() {
                    let next_node = node.children.first().unwrap();
                    let mut only_child = self.nodes.swap_remove(next_node).unwrap();
                    self.general_decorrelate(
                        &mut only_child,
                        unnesting,
                        outer_refs_from_parent,
                    )?;
                    self.nodes.insert(*next_node, only_child);
                }
                // TODO: add equivalences from select.predicate to info.cclasses
                Self::rewrite_columns(vec![&mut filter.predicate].into_iter(), unnesting);
                return Ok(());
            }
            LogicalPlan::Projection(proj) => {
                let next_node = node.children.first().unwrap();
                let mut only_child = self.nodes.swap_remove(next_node).unwrap();
                // TODO: if the children of this node was added with some extra column (i.e)
                // aggregation + group by dependent_column
                // the projection exprs must also include these new expr
                self.general_decorrelate(
                    &mut only_child,
                    unnesting,
                    outer_refs_from_parent,
                )?;

                self.nodes.insert(*next_node, only_child);
                proj.expr.extend(
                    unnesting
                        .pulled_up_columns
                        .iter()
                        .map(|c| Expr::Column(c.clone())),
                );
                Self::rewrite_columns(proj.expr.iter_mut(), unnesting);
                return Ok(());
            }
            _ => {
                unimplemented!()
            }
        };
        unimplemented!()
        // if unnesting.info.parent.is_some() {
        //     not_impl_err!("impl me")
        //     // TODO
        // }
        // // info = Un
        // let node = self.nodes.get(node_id).unwrap();
        // match node.plan {
        //     LogicalPlan::Aggregate(aggr) => {}
        //     _ => {}
        // }
        // Ok(())
    }
    fn right_owned(&mut self, node: &Operator) -> Operator {
        assert_eq!(2, node.children.len());
        // during the building of the tree, the subquery (right node) is always traversed first
        let node_id = node.children.first().unwrap();
        return self.nodes.swap_remove(node_id).unwrap();
    }
    fn left_owned(&mut self, node: &Operator) -> Operator {
        assert_eq!(2, node.children.len());
        // during the building of the tree, the subquery (right node) is always traversed first
        let node_id = node.children.get(1).unwrap();
        return self.nodes.swap_remove(node_id).unwrap();
    }
    fn root_dependent_join_elimination(&mut self) -> Result<LogicalPlan> {
        let root = self.root.unwrap();
        let node = self.nodes.get(&root).unwrap();
        // TODO: need to store the first dependent join node
        assert!(
            node.is_dependent_join_node,
            "need to handle the case root node is not dependent join node"
        );

        let unnesting_info = UnnestingInfo {
            parent: None,
            domain: node.plan.clone(), // dummy
        };

        let mut outer_refs = node.access_tracker.clone();
        // let unnesting = Unnesting {
        //     info: Arc::new(unnesting),
        //     equivalences: UnionFind::new(),
        //     replaces: IndexMap::new(),
        // };

        self.dependent_join_elimination(node.id, &unnesting_info, &mut IndexSet::new())
    }

    fn column_accesses(&self, node_id: usize) -> Vec<&ColumnAccess> {
        let node = self.nodes.get(&node_id).unwrap();
        node.access_tracker.iter().collect()
    }
    // fn new_dependent_join(&self, node: &Operator) -> DependentJoin {
    //     DependentJoin {
    //         original_expr: node.plan.clone(),
    //         left: self.left(node).clone(),
    //         right: self.right(node).clone(),
    //         join_conditions: vec![],
    //     }
    // }
    fn get_subquery_children(
        &self,
        parent: &Operator,
    ) -> Result<(LogicalPlan, Subquery)> {
        let subquery = parent.children.get(0).unwrap();
        let sq_node = self.nodes.get(subquery).unwrap();
        assert!(sq_node.is_subquery_node);
        let query = sq_node.children.get(0).unwrap();
        let target_node = self.nodes.get(query).unwrap();
        // let op = .clone();
        if let LogicalPlan::Subquery(subquery) = sq_node.plan.clone() {
            return Ok((target_node.plan.clone(), subquery));
        } else {
            internal_err!("")
        }
    }

    fn build_join_from_simple_unnest(
        &self,
        dependent_join_node: &mut Operator,
        ret: SimpleDecorrelationResult,
    ) -> Result<LogicalPlan> {
        let (subquery_children, subquery) =
            self.get_subquery_children(dependent_join_node)?;
        match dependent_join_node.plan {
            LogicalPlan::Filter(ref mut filter) => {
                let exprs = split_conjunction(&filter.predicate);
                let mut join_exprs = vec![];
                let mut kept_predicates = vec![];
                // maybe we also need to collect join columns here
                let pulled_projection: Vec<Expr> = ret
                    .pulled_up_projections
                    .iter()
                    .cloned()
                    .map(strip_outer_reference)
                    .collect();
                let right_exprs: Vec<Expr> = if ret.pulled_up_projections.is_empty() {
                    subquery_children.expressions()
                } else {
                    ret.pulled_up_projections
                        .iter()
                        .cloned()
                        .map(strip_outer_reference)
                        .collect()
                };
                let mut join_type = JoinType::LeftSemi;
                for expr in exprs.into_iter() {
                    // exist query may not have any transformed expr
                    // i.e where exists(suquery) => semi join
                    let (transformed, maybe_transformed_expr, maybe_post_join_expr) =
                        try_transform_subquery_to_join_expr(
                            expr,
                            &subquery,
                            &right_exprs,
                        )?;

                    if let Some(transformed) = maybe_transformed_expr {
                        join_exprs.push(transformed)
                    }
                    if let Some(post_join_expr) = maybe_post_join_expr {
                        if post_join_expr
                            .exists(|e| {
                                if let Expr::Column(col) = e {
                                    return Ok(col.name == "mark");
                                }
                                return Ok(false);
                            })
                            .unwrap()
                        {
                            // only use mark join if required
                            join_type = JoinType::LeftMark
                        }
                        kept_predicates.push(post_join_expr)
                    }
                    if !transformed {
                        kept_predicates.push(expr.clone())
                    }
                }

                let new_predicates = ret
                    .pulled_up_predicates
                    .iter()
                    .map(|e| strip_outer_reference(e.clone()));
                join_exprs.extend(new_predicates);
                // TODO: some predicate is join predicate, some is just filter
                // kept_predicates.extend(new_predicates);
                // filter.predicate = conjunction(kept_predicates).unwrap();
                // left
                let mut builder = LogicalPlanBuilder::new(filter.input.deref().clone());

                builder = if join_exprs.is_empty() {
                    builder.join_on(subquery_children, join_type, vec![lit(true)])?
                } else {
                    builder.join_on(
                        subquery_children,
                        // TODO: join type based on filter condition
                        join_type,
                        join_exprs,
                    )?
                };

                if kept_predicates.len() > 0 {
                    builder = builder.filter(conjunction(kept_predicates).unwrap())?
                }
                builder.build()
            }
            _ => {
                unimplemented!()
            }
        }
    }

    fn build_domain(&self, node: &Operator, left: &Operator) -> Result<LogicalPlan> {
        let unique_outer_refs: Vec<Column> = node
            .access_tracker
            .iter()
            .map(|c| c.col.clone())
            .unique()
            .collect();

        // TODO: handle this correctly.
        // the direct left child of root is not always the table scan node
        // and there are many more table providing logical plan
        let initial_domain = LogicalPlanBuilder::new(left.plan.clone())
            .project(
                unique_outer_refs
                    .iter()
                    .map(|col| SelectExpr::Expression(Expr::Column(col.clone()))),
            )?
            .build()?;
        return Ok(initial_domain);
    }

    fn dependent_join_elimination(
        &mut self,
        node: usize,
        unnesting: &UnnestingInfo,
        outer_refs_from_parent: &mut IndexSet<ColumnAccess>,
    ) -> Result<LogicalPlan> {
        let parent = unnesting.parent.clone();
        let mut root_node = self.nodes.swap_remove(&node).unwrap();
        let simple_unnest_result = self.simple_decorrelation(&mut root_node)?;
        if root_node.access_tracker.is_empty() {
            if parent.is_some() {
                // for each projection of outer column moved up by simple_decorrelation
                // replace them with the expr store inside parent.replaces
                unimplemented!("simple dependent join not implemented for the case of recursive subquery");
                self.general_decorrelate(
                    &mut root_node,
                    &mut parent.unwrap(),
                    outer_refs_from_parent,
                )?;
                return Ok(root_node.plan.clone());
            }
            return self
                .build_join_from_simple_unnest(&mut root_node, simple_unnest_result);
            unimplemented!()
            // return Ok(dependent_join);
        }

        // let mut join = self.new_dependent_join(&root_node);
        let mut left = self.left_owned(&root_node);
        let mut right = self.right_owned(&root_node);
        if parent.is_some() {
            unimplemented!("");
            // i.e exists (where inner.col_a = outer_col.b and other_nested_subquery...)

            let mut outer_ref_from_left = IndexSet::new();
            // let left = join.left.clone();
            for col_from_parent in outer_refs_from_parent.iter() {
                if left
                    .plan
                    .all_out_ref_exprs()
                    .contains(&Expr::Column(col_from_parent.col))
                {
                    outer_ref_from_left.insert(col_from_parent.clone());
                }
            }
            let mut parent_unnesting = parent.clone().unwrap();
            self.general_decorrelate(
                &mut left,
                &mut parent_unnesting,
                &mut outer_ref_from_left,
            )?;
            // join.replace_left(new_left, &parent_unnesting.replaces);

            // TODO: after imple simple_decorrelation, rewrite the projection pushed up column as well
        }
        let domain = match parent {
            None => self.build_domain(&root_node, &left)?,
            Some(info) => {
                unimplemented!()
            }
        };

        let new_unnesting_info = UnnestingInfo {
            parent: parent.clone(),
            domain,
            // join: join.clone(),
            // domain: vec![],     // TODO: populate me
        };
        let mut unnesting = Unnesting {
            info: Arc::new(new_unnesting_info.clone()),
            join_conditions: vec![],
            equivalences: UnionFind {
                parent: IndexMap::new(),
                rank: IndexMap::new(),
            },
            replaces: IndexMap::new(),
            pulled_up_columns: vec![],
            pulled_up_predicates: vec![],
            count_exprs_dectected: IndexSet::new(), // outer_col_ref_map: HashMap::new(),
            need_handle_count_bug: true,            // TODO
        };
        let mut accesses: IndexSet<ColumnAccess> = root_node.access_tracker.clone();
        // .iter()
        // .map(|a| a.col.clone())
        // .collect();
        if parent.is_some() {
            for col_access in outer_refs_from_parent.iter() {
                if right
                    .plan
                    .all_out_ref_exprs()
                    .contains(&Expr::Column(col_access.col.clone()))
                {
                    accesses.insert(col_access.clone());
                }
            }
            // add equivalences from join.condition to unnest.cclasses
        }

        //TODO: add equivalences from join.condition to unnest.cclasses
        self.general_decorrelate(&mut right, &mut unnesting, &mut accesses)?;
        println!("temporary transformed result {:?}", self);
        unimplemented!("implement relacing right node");
        // join.replace_right(new_right, &new_unnesting_info, &unnesting.replaces);
        // for acc in new_unnesting_info.outer_refs{
        //     join.join_conditions.append(other);
        // }
    }
    fn rewrite_columns<'a>(
        exprs: impl Iterator<Item = &'a mut Expr>,
        unnesting: &Unnesting,
    ) -> Result<()> {
        for expr in exprs {
            *expr = expr
                .clone()
                .transform(|e| {
                    match &e {
                        Expr::Column(col) => {
                            if let Some(replaced_by) = unnesting.replaces.get(col) {
                                return Ok(Transformed::yes(Expr::Column(
                                    replaced_by.clone(),
                                )));
                            }
                        }
                        Expr::OuterReferenceColumn(_, col) => {
                            if let Some(replaced_by) = unnesting.replaces.get(col) {
                                // TODO: no sure if we should use column or outer ref column here
                                return Ok(Transformed::yes(Expr::Column(
                                    replaced_by.clone(),
                                )));
                            }
                        }
                        _ => {}
                    };
                    Ok(Transformed::no(e))
                })?
                .data;
        }
        Ok(())
    }
    fn get_node_uncheck(&self, node_id: &usize) -> Operator {
        self.nodes.get(node_id).unwrap().clone()
    }

    fn simple_decorrelation(
        &mut self,
        node: &mut Operator,
    ) -> Result<SimpleDecorrelationResult> {
        let mut result = SimpleDecorrelationResult {
            // new: None,
            pulled_up_projections: IndexSet::new(),
            pulled_up_predicates: vec![],
        };

        // the iteration should happen with the order of bottom up, so any node push up won't
        // affect its children (by accident)
        let accesses_bottom_up = node.access_tracker.clone().sorted_by(|a, b| {
            if a.node_id < b.node_id {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        });

        for col_access in accesses_bottom_up {
            // create two copy because of
            // let mut descendent = self.get_node_uncheck(&col_access.node_id);
            let mut descendent = self.nodes.swap_remove(&col_access.node_id).unwrap();
            self.try_simple_decorrelate_descendent(
                node,
                &mut descendent,
                &col_access,
                &mut result,
            )?;
            // TODO: find a nicer way to do in-place update
            // self.nodes.insert(node_id, parent_node.clone());
            self.nodes.insert(col_access.node_id, descendent);
        }

        Ok(result)
    }
}

fn contains_count_expr(
    expr: &Expr,
    // schema: &DFSchemaRef,
    // expr_result_map_for_count_bug: &mut HashMap<String, Expr>,
) -> bool {
    expr.exists(|e| match e {
        Expr::AggregateFunction(expr::AggregateFunction { func, .. }) => {
            Ok(func.name() == "count")
        }
        _ => Ok(false),
    })
    .unwrap()
}

impl fmt::Debug for DependentJoinTracker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "GeneralDecorrelation Tree:")?;
        if let Some(root_op) = &self.root {
            self.fmt_operator(f, *root_op, 0, false)?;
        } else {
            writeln!(f, "  <empty root>")?;
        }
        Ok(())
    }
}

impl DependentJoinTracker {
    fn fmt_operator(
        &self,
        f: &mut fmt::Formatter<'_>,
        node_id: usize,
        indent: usize,
        is_last: bool,
    ) -> fmt::Result {
        // Find the LogicalPlan corresponding to this Operator
        let op = self.nodes.get(&node_id).unwrap();
        let lp = &op.plan;

        for i in 0..indent {
            if i + 1 == indent {
                if is_last {
                    write!(f, "    ")?; // if last child, no vertical line
                } else {
                    write!(f, "|   ")?; // vertical line continues
                }
            } else {
                write!(f, "|   ")?;
            }
        }
        if indent > 0 {
            write!(f, "|--- ")?; // branch
        }

        let unparsed_sql = match Unparser::default().plan_to_sql(lp) {
            Ok(str) => str.to_string(),
            Err(_) => "".to_string(),
        };
        let (node_color, display_str) = match lp {
            LogicalPlan::Subquery(sq) => (
                "\x1b[32m",
                format!("\x1b[1m{}{}", lp.display(), sq.subquery),
            ),
            _ => ("\x1b[33m", lp.display().to_string()),
        };

        writeln!(f, "{} [{}] {}\x1b[0m", node_color, node_id, display_str)?;
        if !unparsed_sql.is_empty() {
            for i in 0..=indent {
                if i < indent {
                    write!(f, "|   ")?;
                } else if indent > 0 {
                    write!(f, "|    ")?; // Align with LogicalPlan text
                }
            }

            writeln!(f, "{}", unparsed_sql)?;
        }

        for i in 0..=indent {
            if i < indent {
                write!(f, "|   ")?;
            } else if indent > 0 {
                write!(f, "|    ")?; // Align with LogicalPlan text
            }
        }

        let accessed_by_string = op
            .access_tracker
            .iter()
            .map(|c| c.debug())
            .collect::<Vec<_>>()
            .join(", ");
        // Now print the Operator details
        writeln!(f, "accessed_by: {}", accessed_by_string,)?;
        let len = op.children.len();

        // Recursively print children if Operator has children
        for (i, child) in op.children.iter().enumerate() {
            let last = i + 1 == len;

            self.fmt_operator(f, *child, indent + 1, last)?;
        }

        Ok(())
    }

    fn lca_from_stack(a: &[usize], b: &[usize]) -> usize {
        let mut lca = None;

        let min_len = a.len().min(b.len());

        for i in 0..min_len {
            let ai = a[i];
            let bi = b[i];

            if ai == bi {
                lca = Some(ai);
            } else {
                break;
            }
        }

        lca.unwrap()
    }

    // because the column providers are visited after column-accessor
    // (function visit_with_subqueries always visit the subquery before visiting the other children)
    // we can always infer the LCA inside this function, by getting the deepest common parent
    fn conclude_lowest_dependent_join_node(&mut self, child_id: usize, col: &Column) {
        if let Some(accesses) = self.accessed_columns.get(col) {
            for access in accesses.iter() {
                let mut cur_stack = self.stack.clone();
                cur_stack.push(child_id);
                // this is a dependent join node
                let lca_node = Self::lca_from_stack(&cur_stack, &access.stack);
                let node = self.nodes.get_mut(&lca_node).unwrap();
                node.access_tracker.insert(ColumnAccess {
                    col: col.clone(),
                    node_id: access.node_id,
                    stack: access.stack.clone(),
                });
            }
        }
    }

    fn mark_column_access(&mut self, child_id: usize, col: &Column) {
        // iter from bottom to top, the goal is to mark the dependent node
        // the current child's access
        let mut stack = self.stack.clone();
        stack.push(child_id);
        self.accessed_columns
            .entry(col.clone())
            .or_default()
            .push(ColumnAccess {
                stack,
                node_id: child_id,
                col: col.clone(),
            });
    }
    fn build(&mut self, plan: LogicalPlan) -> Result<()> {
        // let mut index = AlgebraIndex::default();
        plan.visit_with_subqueries(self)?;
        Ok(())
    }
    fn create_child_relationship(&mut self, parent: usize, child: usize) {
        let operator = self.nodes.get_mut(&parent).unwrap();
        operator.children.push(child);
    }
}

impl DependentJoinTracker {
    fn new(alias_generator: Arc<AliasGenerator>) -> Self {
        return DependentJoinTracker {
            root: None,
            alias_generator,
            current_id: 0,
            nodes: IndexMap::new(),
            stack: vec![],
            accessed_columns: IndexMap::new(),
        };
    }
}

impl ColumnAccess {
    fn debug(&self) -> String {
        format!("\x1b[31m{} ({})\x1b[0m", self.node_id, self.col)
    }
}
#[derive(Debug, Clone)]
struct Operator {
    id: usize,
    plan: LogicalPlan,
    parent: Option<usize>,

    // This field is only set if the node is dependent join node
    // it track which child still accessing which column of
    // the insertion order is top down
    access_tracker: IndexSet<ColumnAccess>,

    is_dependent_join_node: bool,
    is_subquery_node: bool,
    children: Vec<usize>,
}
impl Operator {
    // fn to_dependent_join(&self) -> DependentJoin {
    //     DependentJoin {
    //         original_expr: self.plan.clone(),
    //         left: self.left(),
    //         right: self.right(),
    //         join_conditions: vec![],
    //     }
    // }
}

fn contains_subquery(expr: &Expr) -> bool {
    expr.exists(|expr| {
        Ok(matches!(
            expr,
            Expr::ScalarSubquery(_) | Expr::InSubquery(_) | Expr::Exists(_)
        ))
    })
    .expect("Inner is always Ok")
}

fn print(a: &Expr) -> Result<()> {
    let unparser = Unparser::default();
    let round_trip_sql = unparser.expr_to_sql(a)?.to_string();
    println!("{}", round_trip_sql);
    Ok(())
}

impl TreeNodeVisitor<'_> for DependentJoinTracker {
    type Node = LogicalPlan;
    fn f_down(&mut self, node: &LogicalPlan) -> Result<TreeNodeRecursion> {
        self.current_id += 1;
        if self.root.is_none() {
            self.root = Some(self.current_id);
        }
        let mut is_subquery_node = false;
        let mut is_dependent_join_node = false;
        // for each node, find which column it is accessing, which column it is providing
        // Set of columns current node access
        match node {
            LogicalPlan::Filter(f) => {
                if contains_subquery(&f.predicate) {
                    is_dependent_join_node = true;
                }
                f.predicate.outer_column_refs().into_iter().for_each(|f| {
                    self.mark_column_access(self.current_id, f);
                });
            }
            LogicalPlan::TableScan(tbl_scan) => {
                tbl_scan.projected_schema.columns().iter().for_each(|col| {
                    self.conclude_lowest_dependent_join_node(self.current_id, &col);
                });
            }
            // TODO
            // 1.handle subquery inside projection
            // 2.projection also provide some new columns
            // 3.if within projection exists multiple subquery, how does this work
            LogicalPlan::Projection(proj) => {
                let mut outer_cols = HashSet::new();
                for expr in &proj.expr {
                    if contains_subquery(expr) {
                        is_dependent_join_node = true;
                        break;
                    }
                    expr.add_outer_column_refs(&mut outer_cols);
                }
                outer_cols.into_iter().for_each(|c| {
                    self.mark_column_access(self.current_id, c);
                });
            }
            LogicalPlan::Subquery(subquery) => {
                is_subquery_node = true;
                // TODO: once we detect the subquery
            }
            LogicalPlan::Aggregate(_) => {}
            _ => {
                return internal_err!("impl scan for node type {:?}", node);
            }
        };

        let parent = if self.stack.is_empty() {
            None
        } else {
            let previous_node = self.stack.last().unwrap().to_owned();
            self.create_child_relationship(previous_node, self.current_id);
            Some(self.stack.last().unwrap().to_owned())
        };

        self.stack.push(self.current_id);
        self.nodes.insert(
            self.current_id,
            Operator {
                id: self.current_id,
                parent,
                plan: node.clone(),
                is_subquery_node,
                is_dependent_join_node,
                children: vec![],
                access_tracker: IndexSet::new(),
            },
        );

        Ok(TreeNodeRecursion::Continue)
    }

    /// Invoked while traversing up the tree after children are visited. Default
    /// implementation continues the recursion.
    fn f_up(&mut self, _node: &Self::Node) -> Result<TreeNodeRecursion> {
        self.stack.pop();
        Ok(TreeNodeRecursion::Continue)
    }
}

impl OptimizerRule for DependentJoinTracker {
    fn supports_rewrite(&self) -> bool {
        true
    }
    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        internal_err!("todo")
    }

    fn name(&self) -> &str {
        "decorrelate_subquery"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion_common::{alias::AliasGenerator, DFSchema, Result};
    use datafusion_expr::{
        exists,
        expr_fn::{self, col, not},
        in_subquery, lit, out_ref_col, scalar_subquery, table_scan, CreateMemoryTable,
        EmptyRelation, Expr, LogicalPlan, LogicalPlanBuilder,
    };
    use datafusion_functions_aggregate::{count::count, sum::sum};
    use regex_syntax::ast::LiteralKind;

    use crate::test::{test_table_scan, test_table_scan_with_name};

    use super::DependentJoinTracker;
    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType as ArrowDataType, Field, Fields, Schema},
    };

    #[test]
    fn complex_1_level_decorrelate_in_subquery_with_count() -> Result<()> {
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

        let input1 = LogicalPlanBuilder::from(outer_table.clone())
            .filter(
                col("outer_table.a")
                    .gt(lit(1))
                    .and(in_subquery(col("outer_table.c"), sq_level1)),
            )?
            .build()?;
        let mut index = DependentJoinTracker::new(Arc::new(AliasGenerator::new()));
        index.build(input1)?;
        println!("{:?}", index);
        let new_plan = index.root_dependent_join_elimination()?;
        let expected = "\
        Filter: outer_table.a > Int32(1) AND inner_table_lv1.mark\
        \n  LeftMark Join:  Filter: inner_table_lv1.a = outer_table.a AND outer_table.a > inner_table_lv1.c AND outer_table.b = inner_table_lv1.b\
        \n    TableScan: outer_table\
        \n    Filter: inner_table_lv1.b = Int32(1)\
        \n      TableScan: inner_table_lv1";
        assert_eq!(expected, format!("{new_plan}"));
        Ok(())
    }
    #[test]
    fn simple_decorrelate_with_exist_subquery_with_dependent_columns() -> Result<()> {
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

        let input1 = LogicalPlanBuilder::from(outer_table.clone())
            .filter(col("outer_table.a").gt(lit(1)).and(exists(sq_level1)))?
            .build()?;
        let mut index = DependentJoinTracker::new(Arc::new(AliasGenerator::new()));
        index.build(input1)?;
        let new_plan = index.root_dependent_join_elimination()?;
        let expected = "\
        Filter: outer_table.a > Int32(1) AND inner_table_lv1.mark\
        \n  LeftMark Join:  Filter: inner_table_lv1.a = outer_table.a AND outer_table.a > inner_table_lv1.c AND outer_table.b = inner_table_lv1.b\
        \n    TableScan: outer_table\
        \n    Filter: inner_table_lv1.b = Int32(1)\
        \n      TableScan: inner_table_lv1";
        assert_eq!(expected, format!("{new_plan}"));
        Ok(())
    }
    #[test]
    fn simple_decorrelate_with_exist_subquery_no_dependent_column() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;
        let sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1)
                .filter(col("inner_table_lv1.b").eq(lit(1)))?
                .project(vec![col("inner_table_lv1.b"), col("inner_table_lv1.a")])?
                .build()?,
        );

        let input1 = LogicalPlanBuilder::from(outer_table.clone())
            .filter(col("outer_table.a").gt(lit(1)).and(exists(sq_level1)))?
            .build()?;
        let mut index = DependentJoinTracker::new(Arc::new(AliasGenerator::new()));
        index.build(input1)?;
        let new_plan = index.root_dependent_join_elimination()?;
        let expected = "\
        Filter: outer_table.a > Int32(1) AND inner_table_lv1.mark\
        \n  LeftMark Join:  Filter: Boolean(true)\
        \n    TableScan: outer_table\
        \n    Projection: inner_table_lv1.b, inner_table_lv1.a\
        \n      Filter: inner_table_lv1.b = Int32(1)\
        \n        TableScan: inner_table_lv1";
        assert_eq!(expected, format!("{new_plan}"));
        Ok(())
    }
    #[test]
    fn simple_decorrelate_with_in_subquery_no_dependent_column() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;
        let sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1)
                .filter(col("inner_table_lv1.b").eq(lit(1)))?
                .project(vec![col("inner_table_lv1.b")])?
                .build()?,
        );

        let input1 = LogicalPlanBuilder::from(outer_table.clone())
            .filter(
                col("outer_table.a")
                    .gt(lit(1))
                    .and(in_subquery(col("outer_table.c"), sq_level1)),
            )?
            .build()?;
        let mut index = DependentJoinTracker::new(Arc::new(AliasGenerator::new()));
        index.build(input1)?;
        let new_plan = index.root_dependent_join_elimination()?;
        let expected = "\
        Filter: outer_table.a > Int32(1)\
        \n  LeftSemi Join:  Filter: outer_table.c = inner_table_lv1.b\
        \n    TableScan: outer_table\
        \n    Projection: inner_table_lv1.b\
        \n      Filter: inner_table_lv1.b = Int32(1)\
        \n        TableScan: inner_table_lv1";
        assert_eq!(expected, format!("{new_plan}"));
        Ok(())
    }
    #[test]
    fn simple_decorrelate_with_in_subquery_has_dependent_column() -> Result<()> {
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

        let input1 = LogicalPlanBuilder::from(outer_table.clone())
            .filter(
                col("outer_table.a")
                    .gt(lit(1))
                    .and(in_subquery(col("outer_table.c"), sq_level1)),
            )?
            .build()?;
        let mut index = DependentJoinTracker::new(Arc::new(AliasGenerator::new()));
        index.build(input1)?;
        let new_plan = index.root_dependent_join_elimination()?;
        let expected = "\
        Filter: outer_table.a > Int32(1)\
        \n  LeftSemi Join:  Filter: outer_table.c = outer_table.b AS outer_b_alias AND inner_table_lv1.a = outer_table.a AND outer_table.a > inner_table_lv1.c AND outer_table.b = inner_table_lv1.b\
        \n    TableScan: outer_table\
        \n    Filter: inner_table_lv1.b = Int32(1)\
        \n      TableScan: inner_table_lv1";
        assert_eq!(expected, format!("{new_plan}"));
        Ok(())
    }
}
