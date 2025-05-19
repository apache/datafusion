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
use datafusion_common::{internal_err, Column, HashMap, Result};
use datafusion_expr::expr::{self, Exists, InSubquery};
use datafusion_expr::expr_rewriter::{normalize_col, strip_outer_reference};
use datafusion_expr::select_expr::SelectExpr;
use datafusion_expr::utils::{
    conjunction, disjunction, split_conjunction, split_disjunction,
};
use datafusion_expr::{
    binary_expr, col, expr_fn, lit, BinaryExpr, Cast, Expr, Filter, JoinType,
    LogicalPlan, LogicalPlanBuilder, Operator as ExprOperator, Subquery,
};
// use datafusion_sql::unparser::Unparser;

use datafusion_sql::unparser::Unparser;
use datafusion_sql::TableReference;
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
    nodes: IndexMap<usize, Node>,
    // all the node ids from root to the current node
    // this is used during traversal only
    stack: Vec<usize>,
    // track for each column, the nodes/logical plan that reference to its within the tree
    accessed_columns: IndexMap<Column, Vec<ColumnAccess>>,
    alias_generator: Arc<AliasGenerator>,
}

#[derive(Debug, Hash, PartialEq, PartialOrd, Eq, Clone)]
struct ColumnAccess {
    // node ids from root to the node that is referencing the column
    stack: Vec<usize>,
    // the node referencing the column
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

#[derive(Clone)]
struct UnnestingInfo {
    // join: DependentJoin,
    domain: LogicalPlan,
    parent: Option<Unnesting>,
}
#[derive(Clone)]
struct Unnesting {
    original_subquery: LogicalPlan,
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

    // need this tracked to later on transform for which original subquery requires which join using which metadata
    count_exprs_detected: IndexSet<Expr>,
    // mapping from outer ref column to new column, if any
    // i.e in some subquery (
    // ... where outer.column_c=inner.column_a
    // )
    // and through union find we have outer.column_c = some_other_expr
    // we can substitute the inner query with inner.column_a=some_other_expr
    replaces: IndexMap<Column, Column>,

    subquery_type: SubqueryType,
    decorrelated_subquery: Option<Subquery>,
}
impl Unnesting {
    fn get_replaced_col(&self, col: &Column) -> Column {
        match self.replaces.get(col) {
            Some(col) => col.clone(),
            None => col.clone(),
        }
    }

    fn rewrite_all_pulled_up_expr(
        &mut self,
        alias_name: &String,
        outer_relations: &[String],
    ) -> Result<()> {
        for expr in self.pulled_up_predicates.iter_mut() {
            *expr = replace_col_base_table(expr.clone(), outer_relations, alias_name)?;
        }
        // let rewritten_projections = self
        //     .pulled_up_columns
        //     .iter()
        //     .map(|e| replace_col_base_table(e.clone(), &outer_relations, alias_name))
        //     .collect::<Result<IndexSet<_>>>()?;
        // self.pulled_up_projections = rewritten_projections;
        Ok(())
    }
}

pub fn replace_col_base_table(
    expr: Expr,
    skip_tables: &[String],
    new_table: &String,
) -> Result<Expr> {
    Ok(expr
        .transform(|expr| {
            if let Expr::Column(c) = &expr {
                if let Some(relation) = &c.relation {
                    if !skip_tables.contains(&relation.table().to_string()) {
                        return Ok(Transformed::yes(Expr::Column(
                            c.with_relation(TableReference::bare(new_table.clone())),
                        )));
                    }
                }
            }
            Ok(Transformed::no(expr))
        })?
        .data)
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

#[derive(Debug, Hash, PartialEq, PartialOrd, Eq, Clone)]
struct PulledUpExpr {
    expr: Expr,
    // multiple expr can be pulled up at a time, and because multiple subquery exists
    // at the same level, we need to track which subquery the pulling up is happening for
    subquery_node_id: usize,
}

struct SimpleDecorrelationResult {
    pulled_up_projections: IndexSet<PulledUpExpr>,
    pulled_up_predicates: Vec<PulledUpExpr>,
}
impl SimpleDecorrelationResult {
    // fn get_decorrelated_subquery_node_ids(&self) -> Vec<usize> {
    //     self.pulled_up_predicates
    //         .iter()
    //         .map(|e| e.subquery_node_id)
    //         .chain(
    //             self.pulled_up_projections
    //                 .iter()
    //                 .map(|e| e.subquery_node_id),
    //         )
    //         .unique()
    //         .collect()
    //     // node_ids.extend(
    //     //     self.pulled_up_projections
    //     //         .iter()
    //     //         .map(|e| e.subquery_node_id),
    //     // );
    //     // node_ids.into_iter().unique().collect()
    // }
    // because we don't track which expr was pullled up for which relation to give alias for
    fn rewrite_all_pulled_up_expr(
        &mut self,
        subquery_node_alias_map: &IndexMap<String, Node>,
        outer_relations: &[String],
    ) -> Result<()> {
        let alias_by_subquery_node_id: IndexMap<usize, &String> = subquery_node_alias_map
            .iter()
            .map(|(alias, node)| (node.id, alias))
            .collect();
        for expr in self.pulled_up_predicates.iter_mut() {
            let alias = alias_by_subquery_node_id
                .get(&expr.subquery_node_id)
                .unwrap();
            expr.expr =
                replace_col_base_table(expr.expr.clone(), &outer_relations, *alias)?;
        }
        let rewritten_projections = self
            .pulled_up_projections
            .iter()
            .map(|expr| {
                let alias = alias_by_subquery_node_id
                    .get(&expr.subquery_node_id)
                    .unwrap();
                Ok(PulledUpExpr {
                    subquery_node_id: expr.subquery_node_id,
                    expr: replace_col_base_table(
                        expr.expr.clone(),
                        &outer_relations,
                        *alias,
                    )?,
                })
            })
            .collect::<Result<IndexSet<_>>>()?;
        self.pulled_up_projections = rewritten_projections;
        Ok(())
    }
}

fn extract_join_metadata_from_subquery(
    expr: &Expr,
    sq: &Subquery,
    subquery_projected_exprs: &[Expr],
    alias: &String,
    outer_relations: &[String],
) -> Result<(bool, Option<Expr>, Option<Expr>)> {
    let mut post_join_predicate = None;

    // this can either be a projection expr or a predicate expr
    let mut transformed_expr = None;

    let found_sq = expr.exists(|e| match e {
        Expr::InSubquery(isq) => {
            if subquery_projected_exprs.len() != 1 {
                return internal_err!(
                    "result of IN subquery should only involve one column"
                );
            }
            if isq.subquery == *sq {
                let expr_with_alias = replace_col_base_table(
                    subquery_projected_exprs[0].clone(),
                    outer_relations,
                    alias,
                )?;
                if isq.negated {
                    transformed_expr = Some(binary_expr(
                        *isq.expr.clone(),
                        ExprOperator::NotEq,
                        strip_outer_reference(expr_with_alias),
                    ));
                    return Ok(true);
                }

                transformed_expr = Some(binary_expr(
                    *isq.expr.clone(),
                    ExprOperator::Eq,
                    strip_outer_reference(expr_with_alias),
                ));
                return Ok(true);
            }
            return Ok(false);
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let (exist, transformed, post_join_expr_from_left) =
                extract_join_metadata_from_subquery(
                    left.as_ref(),
                    sq,
                    subquery_projected_exprs,
                    alias,
                    outer_relations,
                )?;
            if !exist {
                let (right_exist, transformed_right, post_join_expr_from_right) =
                    extract_join_metadata_from_subquery(
                        right.as_ref(),
                        sq,
                        subquery_projected_exprs,
                        alias,
                        outer_relations,
                    )?;
                if !right_exist {
                    return Ok(false);
                }
                if let Some(transformed_right) = transformed_right {
                    transformed_expr =
                        Some(binary_expr(*left.clone(), *op, transformed_right));
                }
                if let Some(transformed_right) = post_join_expr_from_right {
                    post_join_predicate =
                        Some(binary_expr(*left.clone(), *op, transformed_right));
                }

                return Ok(true);
            }
            if let Some(transformed) = transformed {
                transformed_expr = Some(binary_expr(transformed, *op, *right.clone()));
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
            if subquery_projected_exprs.len() != 1 {
                return internal_err!(
                    "result of scalar subquery should only involve one column"
                );
            }
            if let LogicalPlan::Subquery(inner_sq) = ssq.subquery.as_ref() {
                if inner_sq.clone() == *sq {
                    transformed_expr = Some(subquery_projected_exprs[0].clone());
                    return Ok(true);
                }
            }
            return Ok(false);
        }
        _ => Ok(false),
    })?;
    return Ok((found_sq, transformed_expr, post_join_predicate));
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
    fn is_linear_path(&self, parent: &Node, child: &Node) -> bool {
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
    fn remove_node(&mut self, parent: &mut Node, node: &mut Node) {
        let next_children = node.children.first().unwrap();
        let next_children_node = self.nodes.swap_remove(next_children).unwrap();
        // let next_children_node = self.nodes.get_mut(next_children).unwrap();
        *node = next_children_node;
        node.parent = Some(parent.id);
    }

    // decorrelate all descendant with simple unnesting
    // this function will remove corresponding entry in root_node.access_tracker if applicable
    // , so caller can rely on the length of this field to detect if simple decorrelation is enough
    // and the decorrelation can stop using "simple method".
    // It also does the in-place update to
    //
    // TODO: this is not yet recursive, but theoreically nested subqueries
    // can be decorrelated using simple method as long as they are independent
    // with each other
    fn try_simple_decorrelate_descendent(
        &mut self,
        root_node: &mut Node,
        child_node: &mut Node,
        col_access: &ColumnAccess,
        result: &mut SimpleDecorrelationResult,
    ) -> Result<()> {
        if !self.is_linear_path(root_node, child_node) {
            return Ok(());
        }
        // offest 0 (root) is dependent join node, will immediately followed by subquery node
        let subquery_node_id = col_access.stack[1];

        match &mut child_node.plan {
            LogicalPlan::Projection(proj) => {
                // TODO: handle the case select binary_expr(outer_ref_a, outer_ref_b) ???
                // if we only see outer_ref_a and decide to pull up the whole expr here
                // outer_ref_b is accidentally pulled up
                let pulled_up_expr: IndexSet<_> = proj
                    .expr
                    .iter()
                    .filter(|proj_expr| {
                        proj_expr
                            .exists(|expr| {
                                if let Expr::OuterReferenceColumn(_, col) = expr {
                                    root_node.access_tracker.swap_remove(col_access);
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
                        result.pulled_up_projections.insert(PulledUpExpr {
                            expr: expr.clone(),
                            subquery_node_id,
                        });
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
                    // NOTE: if later on we decide to support nested subquery inside this function
                    // (i.e multiple subqueries exist in the stack)
                    // the call to e.contains_outer must be aware of which subquery it is checking for:w
                    .partition(|e| e.contains_outer() && can_pull_up(e));

                // only remove the access tracker if none of the kept expr contains reference to the column
                // i.e some of the remaining expr still reference to the column and not pullable
                // For example where outer.col_a=1 and outer.col_a=(some nested subqueries)
                // in this case outer.col_a=1 is pull up, but the access tracker must remain
                // so later on we can tell "simple approach" is not enough, and continue with
                // the "general approach".
                let can_pull_up = kept.iter().all(|e| {
                    !e.exists(|e| {
                        if let Expr::Column(col) = e {
                            return Ok(*col == col_access.col);
                        }
                        Ok(false)
                    })
                    .unwrap()
                });
                if !can_pull_up {
                    return Ok(());
                }
                root_node.access_tracker.swap_remove(col_access);
                result
                    .pulled_up_predicates
                    .extend(pulled_up.iter().map(|e| PulledUpExpr {
                        expr: e.clone(),
                        subquery_node_id,
                    }));
                if kept.is_empty() {
                    self.remove_node(root_node, child_node);
                    return Ok(());
                }
                filter.predicate = conjunction(kept).unwrap();
            }

            // TODO: nested subqueries can also be linear with each other
            // i.e select expr, (subquery1) where expr = subquery2
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
            unsupported => {
                unimplemented!(
                    "simple unnest is missing for this operator {}",
                    unsupported
                )
            }
        };

        Ok(())
    }

    fn general_decorrelate(
        &mut self,
        node: &mut Node,
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
                unnesting.decorrelated_subquery = Some(sq.clone());
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
                        unnesting.count_exprs_detected.insert(agg.clone());
                    }
                }

                if is_static {
                    if !unnesting.count_exprs_detected.is_empty()
                        & unnesting.need_handle_count_bug
                    {
                        let un_matched_row = lit(true).alias(UN_MATCHED_ROW_INDICATOR);
                        agg.group_expr.push(un_matched_row);
                    }
                    // let right = LogicalPlanBuilder::new(node.plan.clone());
                    // the evaluation of
                    // let mut post_join_projection = vec![];
                    let alias =
                        self.alias_generator.next(&unnesting.subquery_type.prefix());

                    let join_condition =
                        unnesting.pulled_up_predicates.iter().filter_map(|e| {
                            let stripped_outer = strip_outer_reference(e.clone());
                            if contains_count_expr(&stripped_outer) {
                                unimplemented!("handle having count(*) predicate pull up")
                                // post_join_predicates.push(stripped_outer);
                                // return None;
                            }
                            match &stripped_outer {
                                Expr::Column(col) => {
                                    println!("{:?}", col);
                                }
                                _ => {}
                            }
                            Some(stripped_outer)
                        });

                    let right = LogicalPlanBuilder::new(agg.input.deref().clone())
                        .aggregate(agg.group_expr.clone(), agg.aggr_expr.clone())?
                        .alias(alias.clone())?
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

                        // *expr = Expr::Column(create_col_from_scalar_expr(
                        //     expr,
                        //     alias.clone(),
                        // )?);
                    }
                    new_plan = new_plan.project(original_expr)?;

                    node.plan = new_plan.build()?;

                    println!("{}", node.plan);
                    return Ok(());
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
    fn right_owned(&mut self, node: &Node) -> Node {
        assert_eq!(2, node.children.len());
        // during the building of the tree, the subquery (right node) is always traversed first
        let node_id = node.children.first().unwrap();
        return self.nodes.swap_remove(node_id).unwrap();
    }
    fn left_owned(&mut self, node: &Node) -> Node {
        assert_eq!(2, node.children.len());
        // during the building of the tree, the subquery (right node) is always traversed first
        let node_id = node.children.last().unwrap();
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

        self.dependent_join_elimination(node.id, &unnesting_info, &mut IndexSet::new())
    }

    fn column_accesses(&self, node_id: usize) -> Vec<&ColumnAccess> {
        let node = self.nodes.get(&node_id).unwrap();
        node.access_tracker.iter().collect()
    }
    fn get_children_subquery_ids(&self, node: &Node) -> Vec<usize> {
        return node.children[..node.children.len() - 1].to_owned();
    }

    fn get_subquery_info(
        &self,
        parent: &Node,
        // because one dependent join node can have multiple subquery at a time
        sq_offset: usize,
    ) -> Result<(LogicalPlan, Subquery, SubqueryType)> {
        let subquery = parent.children.get(sq_offset).unwrap();
        let sq_node = self.nodes.get(subquery).unwrap();
        assert!(sq_node.is_subquery_node);
        let query = sq_node.children.first().unwrap();
        let target_node = self.nodes.get(query).unwrap();
        // let op = .clone();
        if let LogicalPlan::Subquery(subquery) = sq_node.plan.clone() {
            Ok((target_node.plan.clone(), subquery, sq_node.subquery_type))
        } else {
            internal_err!(
                "object construction error: subquery.plan is not with type Subquery"
            )
        }
    }

    // Rewrite from
    // TopNodeParent
    // |
    // (current_top_node)
    // |-SubqueryNode -----> This was decorelated
    // |    |- (subquery_input_node)
    // |-SubqueryNode2 -----> This is not yet decorrelated
    // |-SomeTableScan
    //
    // Into
    // TopNodeParent
    // |
    // NewTopNode <-------- This was added
    // |
    // |----(current_top_node)
    // |    |-SubqueryNode2
    // |    |-SomeTableScan
    // |
    // |----(subquery_input_node)
    fn create_new_join_node_on_top<'a>(
        &'a mut self,
        subquery_alias: String,
        join_type: JoinType,
        current_top_node: &mut Node,
        subquery_input_node: Node,
        join_predicates: Vec<Expr>,
        post_join_predicates: Option<Expr>,
    ) -> Result<Node> {
        self.nodes
            .insert(subquery_input_node.id, subquery_input_node.clone());
        // Build the join node
        let mut right = LogicalPlanBuilder::new(subquery_input_node.plan.clone())
            .alias(subquery_alias)?
            .build()?;
        let alias_node = self.insert_node_and_links(
            right.clone(),
            0,
            None,
            vec![subquery_input_node.id],
        );
        let right_node_id = alias_node.id;
        // the left input does not matter, because later on the rewritting will happen using the pointers
        // from top node, following the children using Node.chilren field
        let mut builder = LogicalPlanBuilder::empty(false);

        builder = if join_predicates.is_empty() {
            builder.join_on(right, join_type, vec![lit(true)])?
        } else {
            builder.join_on(
                right,
                // TODO: join type based on filter condition
                join_type,
                join_predicates,
            )?
        };

        let join_node = builder.build()?;

        let upper_most_parent = current_top_node.parent.clone();
        let mut new_node = self.insert_node_and_links(
            join_node,
            current_top_node.id,
            upper_most_parent,
            vec![current_top_node.id, right_node_id],
        );
        current_top_node.parent = Some(new_node.id);

        let mut new_node_id = new_node.id;
        if let Some(expr) = post_join_predicates {
            let new_plan = LogicalPlanBuilder::new(new_node.plan.clone())
                .filter(expr)?
                .build()?;
            let new_node = self.insert_node_and_links(
                new_plan,
                new_node_id,
                upper_most_parent,
                vec![new_node_id],
            );
            new_node_id = new_node.id;
        }

        self.root = Some(new_node_id);

        Ok(self.nodes.swap_remove(&new_node_id).unwrap())
    }

    // insert a new node, if any link of parent, children is mentioned
    // also update the relationship in these remote nodes
    fn insert_node_and_links<'a>(
        &'a mut self,
        plan: LogicalPlan,
        // which node id in the parent should be replaced by this new node
        swapped_node_id: usize,
        parent: Option<usize>,
        children: Vec<usize>,
    ) -> &'a mut Node {
        self.current_id = self.current_id + 1;
        let node_id = self.current_id;

        // update parent
        if let Some(parent_id) = parent {
            for child_id in self.nodes.get_mut(&parent_id).unwrap().children.iter_mut() {
                if *child_id == swapped_node_id {
                    *child_id = node_id;
                }
            }
        }
        for child_id in children.iter() {
            if let Some(node) = self.nodes.get_mut(child_id) {
                node.parent = Some(node_id);
            }
        }

        let new_node = Node {
            id: node_id,
            plan,
            parent,
            is_subquery_node: false,
            is_dependent_join_node: false,
            children,
            access_tracker: IndexSet::new(),
            subquery_type: SubqueryType::None,
            correlated_relations: IndexSet::new(),
        };
        self.nodes.insert(node_id, new_node);
        self.nodes.get_mut(&node_id).unwrap()
    }

    // this function is aware that multiple subqueries may exist inside the filter predicate
    // and it tries it best to decorrelate all possible exprs, while leave the un-correlatable
    // expr untouched
    //
    // Example of such expression
    // `select * from outer_table where exists(select * from inner_table where ...) & col_b < complex_subquery`
    // the relationship tree looks like this
    //     [0] some parent node
    //     |
    //     -[1]dependent_join_node (filter exists(select * from inner_table where ...) & col_b < complex_subquery)
    //      |
    //      |- [2]simple_subquery
    //      |- [3]complex_subquery
    //      |- [4]outer_table scan
    // After decorrelation, the relationship tree may be translated using 2 approaches
    // Approach 1: Replace the left side of the join using the new input
    //     [0] some parent node
    //     |
    //     -[1]dependent_join_node (filter col_b < complex_subquery)
    //      |
    //      |- [2]REMOVED
    //      |- [3]complex_subquery
    //      |- [4]markjoin <-------- This was modified
    //          |-outer_table scan
    //          |-inner_table scan
    //
    // Approach 2: Keep everything except for the decorrelated expressions,
    // and add a new join above the original dependent join
    //     [0] some parent node
    //     |
    //     -[NEW_NODE_ID] markjoin <----------------- This was added
    //      |
    //      |-inner_table scan
    //      |-[1]dependent_join_node (filter col_b < complex_subquery)
    //        |
    //        |- [2]REMOVED
    //        |- [3]complex_subquery
    //        |- [4]outer_table scan
    // The following uses approach 2
    //
    // If decorrelation happen, this function will returns a new Node object that is supposed to be the new root of the tree
    fn build_join_from_simple_decorrelation_result_filter(
        &mut self,
        mut dependent_join_node: Node,
        outer_relations: &[String],
        ret: &mut SimpleDecorrelationResult,
        mut filter: Filter,
    ) -> Result<(Node)> {
        let still_correlated_sq_ids: Vec<usize> = dependent_join_node
            .access_tracker
            .iter()
            .map(|ac| ac.stack[1])
            .unique()
            .collect();

        let decorrelated_sq_ids = self
            .get_children_subquery_ids(&dependent_join_node)
            .into_iter()
            .filter(|n| !still_correlated_sq_ids.contains(n));
        let subquery_node_alias_map: IndexMap<String, Node> = decorrelated_sq_ids
            .map(|id| {
                dependent_join_node
                    .children
                    .retain(|current_children| *current_children != id);

                let subquery_node = self.nodes.swap_remove(&id).unwrap();
                let subquery_alias = self
                    .alias_generator
                    .next(&subquery_node.subquery_type.prefix());
                (subquery_alias, subquery_node)
            })
            .collect();

        ret.rewrite_all_pulled_up_expr(&subquery_node_alias_map, &outer_relations)?;
        let mut pullup_projection_by_sq_id: IndexMap<usize, Vec<Expr>> = ret
            .pulled_up_projections
            .iter()
            .fold(IndexMap::<usize, Vec<Expr>>::new(), |mut acc, e| {
                acc.entry(e.subquery_node_id)
                    .or_default()
                    .push(e.expr.clone());
                acc
            });
        let mut pullup_predicate_by_sq_id: IndexMap<usize, Vec<Expr>> = ret
            .pulled_up_predicates
            .iter()
            .fold(IndexMap::<usize, Vec<Expr>>::new(), |mut acc, e| {
                acc.entry(e.subquery_node_id)
                    .or_default()
                    .push(e.expr.clone());
                acc
            });

        let dependent_join_node_id = dependent_join_node.id;
        let mut top_node = dependent_join_node;

        for (subquery_alias, subquery_node) in subquery_node_alias_map {
            let subquery_input_node = self
                .nodes
                .swap_remove(subquery_node.children.first().unwrap())
                .unwrap();
            // let subquery_input_plan = subquery_input_node.plan.clone();
            let mut join_predicates = vec![];
            let mut post_join_predicates = vec![]; // this loop heavily assume that all subqueries belong to the same `dependent_join_node`
            let mut remained_predicates = vec![];
            let sq_type = subquery_node.subquery_type;
            let subquery = if let LogicalPlan::Subquery(subquery) = &subquery_node.plan {
                Ok(subquery)
            } else {
                internal_err!(
                    "object construction error: subquery.plan is not with type Subquery"
                )
            }?;
            let mut join_type = sq_type.default_join_type();

            let predicate_expr = split_conjunction(&filter.predicate);

            let pulled_up_projections = pullup_projection_by_sq_id
                .swap_remove(&subquery_node.id)
                .unwrap_or(vec![]);
            let pulled_up_predicates = pullup_predicate_by_sq_id
                .swap_remove(&subquery_node.id)
                .unwrap_or(vec![]);

            for expr in predicate_expr.into_iter() {
                // exist query may not have any transformed expr
                // i.e where exists(suquery) => semi join
                let (transformed, maybe_join_predicate, maybe_post_join_predicate) =
                    extract_join_metadata_from_subquery(
                        expr,
                        &subquery,
                        &subquery_input_node.plan.expressions(),
                        &subquery_alias,
                        &outer_relations,
                    )?;

                if let Some(transformed) = maybe_join_predicate {
                    join_predicates.push(strip_outer_reference(transformed));
                }
                if let Some(post_join_expr) = maybe_post_join_predicate {
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
                    post_join_predicates.push(strip_outer_reference(post_join_expr))
                }
                if !transformed {
                    remained_predicates.push(expr.clone());
                }
            }

            join_predicates
                .extend(pulled_up_predicates.into_iter().map(strip_outer_reference));
            filter.predicate = conjunction(remained_predicates).unwrap();

            // building new join node
            // let left = top_node.plan.clone();
            let new_top_node = self.create_new_join_node_on_top(
                subquery_alias,
                join_type,
                &mut top_node,
                subquery_input_node,
                join_predicates,
                conjunction(post_join_predicates),
                // TODO: post join projection
            )?;
            self.nodes.insert(top_node.id, top_node);
            top_node = new_top_node;
        }
        self.nodes.insert(top_node.id, top_node);
        let mut dependent_join_node =
            self.nodes.swap_remove(&dependent_join_node_id).unwrap();
        dependent_join_node.plan = LogicalPlan::Filter((filter));

        Ok(dependent_join_node)
    }

    fn rewrite_node(&mut self, node_id: usize) -> Result<LogicalPlan> {
        let mut node = self.nodes.swap_remove(&node_id).unwrap();
        if node.is_subquery_node {
            println!("{} {}", node.id, node.plan);
        }
        assert!(
            !node.is_subquery_node,
            "calling on rewrite_node while still exists subquery in the tree"
        );
        if node.children.is_empty() {
            return Ok(node.plan);
        }
        let new_children = node
            .children
            .iter()
            .map(|c| self.rewrite_node(*c))
            .collect::<Result<Vec<LogicalPlan>>>()?;
        node.plan
            .with_new_exprs(node.plan.expressions(), new_children)
    }

    fn rewrite_from_root(&mut self) -> Result<LogicalPlan> {
        self.rewrite_node(self.root.unwrap())
    }

    fn build_join_from_simple_decorrelation_result(
        &mut self,
        mut dependent_join_node: Node,
        ret: &mut SimpleDecorrelationResult,
    ) -> Result<Node> {
        let outer_relations: Vec<String> = dependent_join_node
            .correlated_relations
            .iter()
            .cloned()
            .collect();

        match dependent_join_node.plan.clone() {
            LogicalPlan::Filter(filter) => self
                .build_join_from_simple_decorrelation_result_filter(
                    dependent_join_node,
                    &outer_relations,
                    ret,
                    filter,
                ),
            _ => {
                unimplemented!()
            }
        }
    }

    fn build_domain(&self, node: &Node, left: &Node) -> Result<LogicalPlan> {
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
            .aggregate(
                unique_outer_refs
                    .iter()
                    .map(|col| Expr::Column(col.clone())),
                Vec::<Expr>::new(),
            )?
            .build()?;
        return Ok(initial_domain);
    }

    fn dependent_join_elimination(
        &mut self,
        dependent_join_node_id: usize,
        unnesting: &UnnestingInfo,
        outer_refs_from_parent: &mut IndexSet<ColumnAccess>,
    ) -> Result<LogicalPlan> {
        let parent = unnesting.parent.clone();
        let mut dependent_join_node =
            self.nodes.swap_remove(&dependent_join_node_id).unwrap();

        assert!(dependent_join_node.is_dependent_join_node);

        let mut simple_unnesting = SimpleDecorrelationResult {
            pulled_up_predicates: vec![],
            pulled_up_projections: IndexSet::new(),
        };

        dependent_join_node =
            self.simple_decorrelation(dependent_join_node, &mut simple_unnesting)?;
        if dependent_join_node.access_tracker.is_empty() {
            if parent.is_some() {
                // for each projection of outer column moved up by simple_decorrelation
                // replace them with the expr store inside parent.replaces
                unimplemented!("simple dependent join not implemented for the case of recursive subquery");
                self.general_decorrelate(
                    &mut dependent_join_node,
                    &mut parent.unwrap(),
                    outer_refs_from_parent,
                )?;
                return Ok(dependent_join_node.plan.clone());
            }
            self.nodes
                .insert(dependent_join_node.id, dependent_join_node);
            return self.rewrite_from_root();
        } else {
            // TODO: some of the expr was removed and expect to be pulled up in a best effort fashion
            // (i.e partially decorrelate)
        }
        if self.get_children_subquery_ids(&dependent_join_node).len() > 1 {
            unimplemented!(
                "general decorrelation for multiple subqueries in the same node"
            )
        }

        // for children_offset in self.get_children_subquery_ids(&dependent_join_node) {
        let (original_subquery, _, subquery_type) =
            self.get_subquery_info(&dependent_join_node, 0)?;
        // let mut join = self.new_dependent_join(&root_node);
        // TODO: handle the case where one dependent join node contains multiple subqueries
        let mut left = self.left_owned(&dependent_join_node);
        let mut right = self.right_owned(&dependent_join_node);
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
            None => self.build_domain(&dependent_join_node, &left)?,
            Some(info) => {
                unimplemented!()
            }
        };

        let new_unnesting_info = UnnestingInfo {
            parent: parent.clone(),
            domain,
        };
        let mut unnesting = Unnesting {
            original_subquery,
            info: Arc::new(new_unnesting_info.clone()),
            equivalences: UnionFind {
                parent: IndexMap::new(),
                rank: IndexMap::new(),
            },
            replaces: IndexMap::new(),
            pulled_up_columns: vec![],
            pulled_up_predicates: vec![],
            count_exprs_detected: IndexSet::new(),
            need_handle_count_bug: true, // TODO
            subquery_type,
            decorrelated_subquery: None,
        };
        let mut accesses: IndexSet<ColumnAccess> =
            dependent_join_node.access_tracker.clone();
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
        let decorrelated_plan = self.build_join_from_general_unnesting_info(
            &mut dependent_join_node,
            &mut left,
            &mut right,
            unnesting,
        )?;
        return Ok(decorrelated_plan);
        // }

        // self.nodes.insert(left.id, left);
        // self.nodes.insert(right.id, right);
        // self.nodes.insert(node, root_node);

        unimplemented!("implement relacing right node");
        // join.replace_right(new_right, &new_unnesting_info, &unnesting.replaces);
        // for acc in new_unnesting_info.outer_refs{
        //     join.join_conditions.append(other);
        // }
    }

    fn build_join_from_general_unnesting_info(
        &self,
        dependent_join_node: &mut Node,
        left_node: &mut Node,
        decorrelated_right_node: &mut Node,
        mut unnesting: Unnesting,
    ) -> Result<LogicalPlan> {
        let subquery = unnesting.decorrelated_subquery.take().unwrap();
        let decorrelated_right = decorrelated_right_node.plan.clone();
        let subquery_type = unnesting.subquery_type;

        let alias = self.alias_generator.next(&subquery_type.prefix());
        let outer_relations: Vec<String> = dependent_join_node
            .correlated_relations
            .iter()
            .cloned()
            .collect();

        unnesting.rewrite_all_pulled_up_expr(&alias, &outer_relations)?;
        // TODO: do this on left instead of dependent_join_node directly, because with recursive
        // the left side can also be rewritten
        match dependent_join_node.plan {
            LogicalPlan::Filter(ref mut filter) => {
                let exprs = split_conjunction(&filter.predicate);
                let mut join_exprs = vec![];
                let mut kept_predicates = vec![];
                let right_expr: Vec<_> = decorrelated_right_node
                    .plan
                    .schema()
                    .columns()
                    .iter()
                    .map(|c| Expr::Column(c.clone()))
                    .collect();
                let mut join_type = subquery_type.default_join_type();
                for expr in exprs.into_iter() {
                    // exist query may not have any transformed expr
                    // i.e where exists(suquery) => semi join
                    let (transformed, maybe_transformed_expr, maybe_post_join_expr) =
                        extract_join_metadata_from_subquery(
                            expr,
                            &subquery,
                            &right_expr,
                            &alias,
                            &outer_relations,
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

                // TODO: some predicate is join predicate, some is just filter
                // kept_predicates.extend(new_predicates);
                // filter.predicate = conjunction(kept_predicates).unwrap();
                // left
                let mut builder = LogicalPlanBuilder::new(filter.input.deref().clone());

                builder = if join_exprs.is_empty() {
                    builder.join_on(decorrelated_right, join_type, vec![lit(true)])?
                } else {
                    builder.join_on(
                        decorrelated_right,
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
    fn get_node_uncheck(&self, node_id: &usize) -> Node {
        self.nodes.get(node_id).unwrap().clone()
    }

    // Decorrelate the current node using `simple` approach.
    // It will consume the node and returns a new node where the decorrelatoin should continue
    // using `general` approach, should `simple` approach is not sufficient.
    // Most of the time the same Node is returned, avoid using &mut Node because of borrow checker
    // Beware that after calling this function, the root node may be changed (as new join node being added to the top)
    fn simple_decorrelation(
        &mut self,
        mut node: Node,
        simple_unnesting: &mut SimpleDecorrelationResult,
    ) -> Result<Node> {
        let node_id = node.id;
        // the iteration should happen with the order of bottom up, so any node pull up won't
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
                &mut node,
                &mut descendent,
                &col_access,
                simple_unnesting,
            )?;
            // TODO: find a nicer way to do in-place update
            self.nodes.insert(col_access.node_id, descendent);
        }
        self.build_join_from_simple_decorrelation_result(node, simple_unnesting)
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
    fn conclude_lowest_dependent_join_node(
        &mut self,
        child_id: usize,
        col: &Column,
        tbl_name: &str,
    ) {
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
                node.correlated_relations.insert(tbl_name.to_string());
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
struct Node {
    id: usize,
    plan: LogicalPlan,
    parent: Option<usize>,

    // This field is only set if the node is dependent join node
    // it track which descendent nodes still accessing the outer columns provided by its
    // left child
    // the insertion order is top down
    access_tracker: IndexSet<ColumnAccess>,

    is_dependent_join_node: bool,
    is_subquery_node: bool,

    // note that for dependent join nodes, there can be more than 1
    // subquery children at a time, but always 1 outer-column-providing-child
    // which is at the last element
    children: Vec<usize>,
    subquery_type: SubqueryType,
    correlated_relations: IndexSet<String>,
}
#[derive(Debug, Clone, Copy)]
enum SubqueryType {
    None,
    In,
    Exists,
    Scalar,
}
impl SubqueryType {
    fn default_join_type(&self) -> JoinType {
        match self {
            SubqueryType::None => {
                panic!("not reached")
            }
            SubqueryType::In => JoinType::LeftSemi,
            SubqueryType::Exists => JoinType::LeftSemi,
            // TODO: in duckdb, they have JoinType::Single
            // where there is only at most one join partner entry on the LEFT
            SubqueryType::Scalar => JoinType::Left,
        }
    }
    fn prefix(&self) -> String {
        match self {
            SubqueryType::None => "",
            SubqueryType::In => "__in_sq",
            SubqueryType::Exists => "__exists_sq",
            SubqueryType::Scalar => "__scalar_sq",
        }
        .to_string()
    }
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

        let mut subquery_type = SubqueryType::None;
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
                    self.conclude_lowest_dependent_join_node(
                        self.current_id,
                        &col,
                        tbl_scan.table_name.table(),
                    );
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
                let parent = self.stack.last().unwrap();
                let parent_node = self.get_node_uncheck(parent);
                for expr in parent_node.plan.expressions() {
                    expr.exists(|e| {
                        let (found_sq, checking_type) = match e {
                            Expr::ScalarSubquery(sq) => {
                                (sq == subquery, SubqueryType::Scalar)
                            }
                            Expr::Exists(Exists { subquery: sq, .. }) => {
                                (sq == subquery, SubqueryType::Exists)
                            }
                            Expr::InSubquery(InSubquery { subquery: sq, .. }) => {
                                (sq == subquery, SubqueryType::In)
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
            Node {
                id: self.current_id,
                parent,
                plan: node.clone(),
                is_subquery_node,
                is_dependent_join_node,
                children: vec![],
                access_tracker: IndexSet::new(),
                subquery_type,
                correlated_relations: IndexSet::new(),
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
    use arrow::datatypes::DataType as ArrowDataType;
    #[test]
    fn simple_1_level_subquery_in_from_expr() -> Result<()> {
        unimplemented!()
    }
    #[test]
    fn simple_1_level_subquery_in_selection_expr() -> Result<()> {
        unimplemented!()
    }
    #[test]
    fn complex_1_level_decorrelate_2_subqueries_at_the_same_level() -> Result<()> {
        unimplemented!()
    }
    #[test]
    fn simple_1_level_decorrelate_2_subqueries_at_the_same_level() -> Result<()> {
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

        let input1 = LogicalPlanBuilder::from(outer_table.clone())
            .filter(
                col("outer_table.a")
                    .gt(lit(1))
                    .and(exists(exist_sq_level1))
                    .and(in_subquery(col("outer_table.b"), in_sq_level1)),
            )?
            .build()?;
        let mut index = DependentJoinTracker::new(Arc::new(AliasGenerator::new()));
        index.build(input1)?;
        println!("{:?}", index);
        let new_plan = index.root_dependent_join_elimination()?;
        println!("{}", new_plan);
        let expected = "\
        LeftSemi Join:  Filter: outer_table.b = __in_sq_2.a\
        \n  Filter: __exists_sq_1.mark\
        \n    LeftMark Join:  Filter: Boolean(true)\
        \n      Filter: outer_table.a > Int32(1)\
        \n        TableScan: outer_table\
        \n      SubqueryAlias: __exists_sq_1\
        \n        Filter: inner_table_lv1.a AND inner_table_lv1.b = Int32(1)\
        \n          TableScan: inner_table_lv1\
        \n  SubqueryAlias: __in_sq_2\
        \n    Projection: inner_table_lv1.a\
        \n      Filter: inner_table_lv1.c = Int32(2)\
        \n        TableScan: inner_table_lv1";
        assert_eq!(expected, format!("{new_plan}"));
        Ok(())
    }

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
        println!("{}", new_plan);
        let expected = "\
        Filter: outer_table.a > Int32(1)\
        \n  LeftSemi Join:  Filter: outer_table.c = count_a\
        \n    TableScan: outer_table\
        \n    Projection: count(inner_table_lv1.a) AS count_a, inner_table_lv1.a, inner_table_lv1.c, inner_table_lv1.b\
        \n      Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv1.a)]]\
        \n        Filter: inner_table_lv1.a = outer_ref(outer_table.a) AND outer_ref(outer_table.a) > inner_table_lv1.c AND inner_table_lv1.b = Int32(1) AND outer_ref(outer_table.b) = inner_table_lv1.b\
        \n          TableScan: inner_table_lv1";
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
        Filter: outer_table.a > Int32(1) AND __exists_sq_1.mark\
        \n  LeftMark Join:  Filter: __exists_sq_1.a = outer_table.a AND outer_table.a > __exists_sq_1.c AND outer_table.b = __exists_sq_1.b\
        \n    TableScan: outer_table\
        \n    SubqueryAlias: __exists_sq_1\
        \n      Filter: inner_table_lv1.b = Int32(1)\
        \n        TableScan: inner_table_lv1";
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
        Filter: outer_table.a > Int32(1) AND __exists_sq_1.mark\
        \n  LeftMark Join:  Filter: Boolean(true)\
        \n    TableScan: outer_table\
        \n    SubqueryAlias: __exists_sq_1\
        \n      Projection: inner_table_lv1.b, inner_table_lv1.a\
        \n        Filter: inner_table_lv1.b = Int32(1)\
        \n          TableScan: inner_table_lv1";
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
        \n  LeftSemi Join:  Filter: outer_table.c = __in_sq_1.b\
        \n    TableScan: outer_table\
        \n    SubqueryAlias: __in_sq_1\
        \n      Projection: inner_table_lv1.b\
        \n        Filter: inner_table_lv1.b = Int32(1)\
        \n          TableScan: inner_table_lv1";
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
        \n  LeftSemi Join:  Filter: outer_table.c = outer_table.b AS outer_b_alias AND __in_sq_1.a = outer_table.a AND outer_table.a > __in_sq_1.c AND outer_table.b = __in_sq_1.b\
        \n    TableScan: outer_table\
        \n    SubqueryAlias: __in_sq_1\
        \n      Filter: inner_table_lv1.b = Int32(1)\
        \n        TableScan: inner_table_lv1";
        assert_eq!(expected, format!("{new_plan}"));
        Ok(())
    }
}
