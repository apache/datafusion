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

use std::cell::RefCell;
use std::collections::{BTreeSet, HashSet};
use std::fmt;
use std::ops::Deref;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use crate::simplify_expressions::ExprSimplifier;
use crate::{ApplyOrder, OptimizerConfig, OptimizerRule};

use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeContainer, TreeNodeRecursion,
    TreeNodeRewriter, TreeNodeVisitor,
};
use datafusion_common::{internal_err, not_impl_err, Column, Result};
use datafusion_expr::{binary_expr, Expr, JoinType, LogicalPlan};
use datafusion_sql::unparser::Unparser;
use indexmap::map::Entry;
use indexmap::IndexMap;
use itertools::Itertools;
use log::Log;

pub struct AlgebraIndex {
    root: Option<usize>,
    current_id: usize,
    nodes: IndexMap<usize, Operator>, // column_
    // TODO: use a different identifier for a node, instead of the whole logical plan obj
    stack: Vec<usize>,
    accessed_columns: IndexMap<Column, Vec<ColumnAccess>>,
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
impl DependentJoin {
    fn replace_right(
        &mut self,
        plan: LogicalPlan,
        unnesting: &UnnestingInfo,
        replacements: &IndexMap<Column, Column>,
    ) {
        self.right.plan = plan;
        for col in unnesting.outer_refs.iter() {
            let replacement = replacements.get(col).unwrap();
            self.join_conditions.push(binary_expr(
                Expr::Column(col.clone()),
                datafusion_expr::Operator::IsNotDistinctFrom,
                Expr::Column(replacement.clone()),
            ));
        }
    }
    fn replace_left(
        &mut self,
        plan: LogicalPlan,
        column_replacements: &IndexMap<Column, Column>,
    ) {
        self.left.plan = plan
        // TODO:
        // - update join condition
        // - check if the relation with children should be removed
    }
}

#[derive(Clone)]
struct UnnestingInfo {
    join: DependentJoin,
    outer_refs: Vec<Column>,
    domain: Vec<Column>,
    parent: Option<Unnesting>,
}
#[derive(Clone)]
struct Unnesting {
    info: Arc<UnnestingInfo>, // cclasses: union find data structure of equivalent columns
    equivalences: UnionFind,
    replaces: IndexMap<Column, Column>,
    // mapping from outer ref column to new column, if any
    // i.e in some subquery (
    // ... where outer.column_c=inner.column_a
    // )
    // and through union find we have outer.column_c = some_other_expr
    // we can substitute the inner query with inner.column_a=some_other_expr
}

// impl Default for GeneralDecorrelation {
//     fn default() -> Self {
//         return GeneralDecorrelation {
//             index: AlgebraIndex::default(),
//         };
//     }
// }
impl AlgebraIndex {
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
    fn is_linear_path(&self, parent: &usize, child: &usize) -> bool {
        let mut current_node = *child;

        loop {
            let child_node = self.nodes.get(&current_node).unwrap();
            if !self.is_linear_operator(&child_node.plan) {
                return false;
            }
            if current_node == *parent {
                return true;
            }
            match child_node.parent {
                None => return true,
                Some(new_parent) => {
                    if new_parent == *parent {
                        return true;
                    }
                    current_node = new_parent;
                }
            };
        }
    }
    // decorrelate all children with simple unnesting
    // returns true if all children were eliminated
    // TODO(impl me)
    fn try_decorrelate_child(&self, root: &usize, child: &usize) -> Result<bool> {
        if !self.is_linear_path(root, child) {
            return Ok(false);
        }
        let child_node = self.nodes.get(child).unwrap();
        let root_node = self.nodes.get(root).unwrap();
        match &child_node.plan {
            LogicalPlan::Projection(proj) => {}
            LogicalPlan::Filter(filter) => {
                let accessed_from_child = &child_node.access_tracker;
                for col_access in accessed_from_child {
                    println!(
                        "checking if col {} can be merged into parent's join filter {}",
                        col_access.debug(),
                        root_node.plan
                    )
                }
            }
            _ => {}
        }
        Ok(false)
    }

    fn unnest(
        &mut self,
        node_id: usize,
        unnesting: &mut Unnesting,
        outer_refs_from_parent: HashSet<Column>,
    ) -> Result<LogicalPlan> {
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
    fn right(&self, node: &Operator) -> &Operator {
        assert_eq!(2, node.children.len());
        // during the building of the tree, the subquery (right node) is always traversed first
        let node_id = node.children.get(0).unwrap();
        return self.nodes.get(node_id).unwrap();
    }
    fn left(&self, node: &Operator) -> &Operator {
        assert_eq!(2, node.children.len());
        // during the building of the tree, the subquery (right node) is always traversed first
        let node_id = node.children.get(1).unwrap();
        return self.nodes.get(node_id).unwrap();
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
            join: DependentJoin {
                original_expr: node.plan.clone(),
                left: self.left(node).clone(),
                right: self.right(node).clone(),
                join_conditions: vec![],
            },
            domain: vec![],
            outer_refs: vec![],
        };
        // let unnesting = Unnesting {
        //     info: Arc::new(unnesting),
        //     equivalences: UnionFind::new(),
        //     replaces: IndexMap::new(),
        // };

        self.dependent_join_elimination(node.id, &unnesting_info, HashSet::new())
    }

    fn column_accesses(&self, node_id: usize) -> Vec<&ColumnAccess> {
        let node = self.nodes.get(&node_id).unwrap();
        node.access_tracker.iter().collect()
    }
    fn new_dependent_join(&self, node: &Operator) -> DependentJoin {
        DependentJoin {
            original_expr: node.plan.clone(),
            left: self.left(node).clone(),
            right: self.left(node).clone(),
            join_conditions: vec![],
        }
    }

    fn dependent_join_elimination(
        &mut self,
        node: usize,
        unnesting: &UnnestingInfo,
        outer_refs_from_parent: HashSet<Column>,
    ) -> Result<LogicalPlan> {
        let parent = unnesting.parent.clone();
        let operator = self.nodes.get(&node).unwrap();
        let plan = &operator.plan;
        let mut join = self.new_dependent_join(operator);
        // we have to do the reversed iter, because we know the subquery (right side of
        // the dependent join) is always the first child of the node, and we want to visit
        // the left side first

        let (dependent_join, finished) = self.simple_decorrelation(node)?;
        if finished {
            if parent.is_some() {
                // for each projection of outer column moved up by simple_decorrelation
                // replace them with the expr store inside parent.replaces
                unimplemented!("");
                return self.unnest(node, &mut parent.unwrap(), outer_refs_from_parent);
            }
            return Ok(dependent_join);
        }
        if parent.is_some() {
            // i.e exists (where inner.col_a = outer_col.b and other_nested_subquery...)

            let mut outer_ref_from_left = HashSet::new();
            let left = join.left.clone();
            for col_from_parent in outer_refs_from_parent.iter() {
                if left
                    .plan
                    .all_out_ref_exprs()
                    .contains(&Expr::Column(col_from_parent.clone()))
                {
                    outer_ref_from_left.insert(col_from_parent.clone());
                }
            }
            let mut parent_unnesting = parent.clone().unwrap();
            let new_left =
                self.unnest(left.id, &mut parent_unnesting, outer_ref_from_left)?;
            join.replace_left(new_left, &parent_unnesting.replaces);

            // TODO: after imple simple_decorrelation, rewrite the projection pushed up column as well
        }
        let new_unnesting_info = UnnestingInfo {
            parent: parent.clone(),
            join: join.clone(),
            domain: vec![],     // TODO: populate me
            outer_refs: vec![], // TODO: populate me
        };
        let mut unnesting = Unnesting {
            info: Arc::new(new_unnesting_info.clone()),
            equivalences: UnionFind {
                parent: IndexMap::new(),
                rank: IndexMap::new(),
            },
            replaces: IndexMap::new(),
        };
        let mut accesses: HashSet<Column> = self
            .column_accesses(node)
            .iter()
            .map(|a| a.col.clone())
            .collect();
        if parent.is_some() {
            for col_access in outer_refs_from_parent {
                if join
                    .right
                    .plan
                    .all_out_ref_exprs()
                    .contains(&Expr::Column(col_access.clone()))
                {
                    accesses.insert(col_access.clone());
                }
            }
            // add equivalences from join.condition to unnest.cclasses
        }

        let new_right = self.unnest(join.right.id, &mut unnesting, accesses)?;
        join.replace_right(new_right, &new_unnesting_info, &unnesting.replaces);
        // for acc in new_unnesting_info.outer_refs{
        //     join.join_conditions.append(other);
        // }

        unimplemented!()
    }
    fn rewrite_columns(expr: Expr, unnesting: Unnesting) {
        unimplemented!()
        // expr.apply(|expr| {
        //     if let Expr::OuterReferenceColumn(_, col) = expr {
        //         set.insert(col);
        //     }
        //     Ok(TreeNodeRecursion::Continue)
        // })
        // .expect("traversal is infallible");
    }

    fn simple_decorrelation(&mut self, node: usize) -> Result<(LogicalPlan, bool)> {
        let node = self.nodes.get(&node).unwrap();
        let mut all_eliminated = false;
        for child in node.children.iter() {
            let branch_all_eliminated = self.try_decorrelate_child(child, child)?;
            all_eliminated = all_eliminated || branch_all_eliminated;
        }
        Ok((node.plan.clone(), false))
    }
    fn build(&mut self, root: &LogicalPlan) -> Result<()> {
        self.build_algebra_index(root.clone())?;
        println!("{:?}", self);
        Ok(())
    }
}
impl fmt::Debug for AlgebraIndex {
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

impl AlgebraIndex {
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
        writeln!(f, "\x1b[33m [{}] {}\x1b[0m", node_id, lp.display())?;
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

        let accessing_string = op
            .potential_accesses
            .iter()
            .map(|c| c.debug())
            .collect::<Vec<_>>()
            .join(", ");
        let accessed_by_string = op
            .access_tracker
            .iter()
            .map(|c| c.debug())
            .collect::<Vec<_>>()
            .join(", ");
        // Now print the Operator details
        writeln!(
            f,
            "acccessing: {}, accessed_by: {}",
            accessing_string, accessed_by_string,
        )?;
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
    // function visit_with_subqueries always visit the subquery before visiting the other child
    // we can always infer the LCA inside this function, by getting the deepest common parent
    fn conclude_lca_for_column(&mut self, child_id: usize, col: &Column) {
        if let Some(accesses) = self.accessed_columns.get(col) {
            for access in accesses.iter() {
                let mut cur_stack = self.stack.clone();
                cur_stack.push(child_id);
                // this is a dependen join node
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
        // iter from bottom to top, the goal is to mark the independen_join node
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
    fn build_algebra_index(&mut self, plan: LogicalPlan) -> Result<()> {
        println!("======================================begin");
        // let mut index = AlgebraIndex::default();
        plan.visit_with_subqueries(self)?;
        println!("======================================end");
        Ok(())
    }
    fn create_child_relationship(&mut self, parent: usize, child: usize) {
        let operator = self.nodes.get_mut(&parent).unwrap();
        operator.children.push(child);
    }
}

impl Default for AlgebraIndex {
    fn default() -> Self {
        return AlgebraIndex {
            root: None,
            current_id: 0,
            nodes: IndexMap::new(),
            stack: vec![],
            accessed_columns: IndexMap::new(),
        };
    }
}

#[derive(Debug, Hash, PartialEq, PartialOrd, Eq, Clone)]
enum ColumnUsage {
    Own(Column),
    Outer(Column),
}
impl ColumnUsage {
    fn debug(&self) -> String {
        match self {
            ColumnUsage::Own(col) => format!("\x1b[34m{}\x1b[0m", col.flat_name()),
            ColumnUsage::Outer(col) => format!("\x1b[31m{}\x1b[0m", col.flat_name()),
        }
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
    // Note if the current node is a Subquery
    // at the first time this node is visited,
    // the set of accesses columns are not sufficient
    // (i.e) some where deep down the ast another recursive subquery
    // exists and also referencing some columns belongs to the outer part
    // of the subquery
    // Thus, on discovery of new subquery, we must
    // add the accesses columns to the ancestor nodes which are Subquery
    potential_accesses: HashSet<ColumnUsage>,
    provides: HashSet<ColumnUsage>,

    // This field is only set if the node is dependent join node
    // it track which child still accessing which column of
    access_tracker: HashSet<ColumnAccess>,

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

impl TreeNodeVisitor<'_> for AlgebraIndex {
    type Node = LogicalPlan;
    fn f_down(&mut self, node: &LogicalPlan) -> Result<TreeNodeRecursion> {
        self.current_id += 1;
        if self.root.is_none() {
            self.root = Some(self.current_id);
        }
        let mut is_subquery_node = false;
        let mut is_dependent_join_node = false;
        println!("{}\nnode {}", "----".repeat(self.stack.len()), node);
        // for each node, find which column it is accessing, which column it is providing
        // Set of columns current node access
        let (accesses, provides): (HashSet<ColumnUsage>, HashSet<ColumnUsage>) =
            match node {
                LogicalPlan::Filter(f) => {
                    if contains_subquery(&f.predicate) {
                        is_dependent_join_node = true;
                    }
                    let mut outer_col_refs: HashSet<ColumnUsage> = f
                        .predicate
                        .outer_column_refs()
                        .into_iter()
                        .map(|f| {
                            self.mark_column_access(self.current_id, f);
                            ColumnUsage::Outer(f.clone())
                        })
                        .collect();

                    (outer_col_refs, HashSet::new())
                }
                LogicalPlan::TableScan(tbl_scan) => {
                    let provided_columns: HashSet<ColumnUsage> = tbl_scan
                        .projected_schema
                        .columns()
                        .into_iter()
                        .map(|col| {
                            self.conclude_lca_for_column(self.current_id, &col);
                            ColumnUsage::Own(col)
                        })
                        .collect();
                    (HashSet::new(), provided_columns)
                }
                LogicalPlan::Aggregate(_) => (HashSet::new(), HashSet::new()),
                LogicalPlan::EmptyRelation(_) => (HashSet::new(), HashSet::new()),
                LogicalPlan::Limit(_) => (HashSet::new(), HashSet::new()),
                // TODO
                // 1.handle subquery inside projection
                // 2.projection also provide some new columns
                // 3.if within projection exists multiple subquery, how does this work
                LogicalPlan::Projection(proj) => {
                    for expr in &proj.expr {
                        if contains_subquery(expr) {
                            is_dependent_join_node = true;
                            break;
                        }
                    }
                    // proj.expr
                    // TODO: fix me
                    (HashSet::new(), HashSet::new())
                }
                LogicalPlan::Subquery(subquery) => {
                    is_subquery_node = true;
                    // TODO: once we detect the subquery
                    let accessed = subquery
                        .outer_ref_columns
                        .iter()
                        .filter_map(|f| match f {
                            Expr::Column(col) => Some(ColumnUsage::Outer(col.clone())),
                            Expr::OuterReferenceColumn(_, col) => {
                                Some(ColumnUsage::Outer(col.clone()))
                            }
                            _ => None,
                        })
                        .collect();
                    (accessed, HashSet::new())
                }
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
                potential_accesses: accesses,
                provides,
                is_subquery_node,
                is_dependent_join_node,
                children: vec![],
                access_tracker: HashSet::new(),
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

impl OptimizerRule for AlgebraIndex {
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

    use datafusion_common::{DFSchema, Result};
    use datafusion_expr::{
        expr_fn::{self, col},
        in_subquery, lit, out_ref_col, scalar_subquery, table_scan, CreateMemoryTable,
        EmptyRelation, Expr, LogicalPlan, LogicalPlanBuilder,
    };
    use datafusion_functions_aggregate::{count::count, sum::sum};
    use regex_syntax::ast::LiteralKind;

    use crate::test::{test_table_scan, test_table_scan_with_name};

    use super::AlgebraIndex;
    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType as ArrowDataType, Field, Fields, Schema},
    };

    #[test]
    fn play_unnest_simple_projection_pull_up() -> Result<()> {
        // let mut framework = GeneralDecorrelation::default();

        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;
        let sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1)
                .filter(
                    col("inner_table_lv1.a")
                        .eq(out_ref_col(ArrowDataType::UInt32, "outer_table.a")),
                )?
                .project(vec![out_ref_col(ArrowDataType::UInt32, "outer_table.b")])?
                .build()?,
        );

        let input1 = LogicalPlanBuilder::from(outer_table.clone())
            .filter(
                col("outer_table.a")
                    .gt(lit(1))
                    .and(in_subquery(col("outer_table.c"), sq_level1)),
            )?
            .build()?;
        let mut index = AlgebraIndex::default();
        index.build(&input1)?;
        let new_plan = index.root_dependent_join_elimination()?;
        println!("{}", new_plan);

        // let input2 = LogicalPlanBuilder::from(input.clone())
        //     .filter(col("int_col").gt(lit(1)))?
        //     .project(vec![col("string_col")])?
        //     .build()?;

        // let mut b = GeneralDecorrelation::default();
        // b.build_algebra_index(input2)?;

        Ok(())
    }
    #[test]
    fn play_unnest_simple_predicate_pull_up() -> Result<()> {
        // let mut framework = GeneralDecorrelation::default();

        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;
        // let inner_table_lv2 = test_table_scan_with_name("inner_table_lv2")?;
        // let sq_level2 = Arc::new(
        //     LogicalPlanBuilder::from(inner_table_lv2)
        //         .filter(
        //             out_ref_col(ArrowDataType::UInt32, "inner_table_lv1.b")
        //                 .eq(col("inner_table_lv2.b"))
        //                 .and(
        //                     out_ref_col(ArrowDataType::UInt32, "outer_table.c")
        //                         .eq(col("inner_table_lv2.c")),
        //                 ),
        //         )?
        //         .aggregate(Vec::<Expr>::new(), vec![count(col("inner_table_lv2.a"))])?
        //         .build()?,
        // );
        let sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1)
                .filter(
                    col("inner_table_lv1.a")
                        .eq(out_ref_col(ArrowDataType::UInt32, "outer_table.a"))
                        .and(
                            out_ref_col(ArrowDataType::UInt32, "outer_table.a")
                                .eq(lit(1)),
                        ),
                )?
                .aggregate(Vec::<Expr>::new(), vec![sum(col("inner_table_lv1.b"))])?
                .project(vec![sum(col("inner_table_lv1.b"))])?
                .build()?,
        );

        let input1 = LogicalPlanBuilder::from(outer_table.clone())
            .filter(
                col("outer_table.a")
                    .gt(lit(1))
                    .and(col("outer_table.b").gt(scalar_subquery(sq_level1))),
            )?
            .build()?;
        let mut index = AlgebraIndex::default();
        index.build(&input1)?;
        let new_plan = index.root_dependent_join_elimination()?;
        println!("{}", new_plan);

        // let input2 = LogicalPlanBuilder::from(input.clone())
        //     .filter(col("int_col").gt(lit(1)))?
        //     .project(vec![col("string_col")])?
        //     .build()?;

        // let mut b = GeneralDecorrelation::default();
        // b.build_algebra_index(input2)?;

        Ok(())
    }
    #[test]
    fn play_unnest() -> Result<()> {
        // let mut framework = GeneralDecorrelation::default();

        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;
        // let inner_table_lv2 = test_table_scan_with_name("inner_table_lv2")?;
        // let sq_level2 = Arc::new(
        //     LogicalPlanBuilder::from(inner_table_lv2)
        //         .filter(
        //             out_ref_col(ArrowDataType::UInt32, "inner_table_lv1.b")
        //                 .eq(col("inner_table_lv2.b"))
        //                 .and(
        //                     out_ref_col(ArrowDataType::UInt32, "outer_table.c")
        //                         .eq(col("inner_table_lv2.c")),
        //                 ),
        //         )?
        //         .aggregate(Vec::<Expr>::new(), vec![count(col("inner_table_lv2.a"))])?
        //         .build()?,
        // );
        let sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1)
                .filter(
                    col("inner_table_lv1.a")
                        .eq(out_ref_col(ArrowDataType::UInt32, "outer_table.a")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![sum(col("inner_table_lv1.b"))])?
                .project(vec![sum(col("inner_table_lv1.b"))])?
                .build()?,
        );

        let input1 = LogicalPlanBuilder::from(outer_table.clone())
            .filter(
                col("outer_table.a")
                    .gt(lit(1))
                    .and(col("outer_table.b").gt(scalar_subquery(sq_level1))),
            )?
            .build()?;
        let mut index = AlgebraIndex::default();
        index.build(&input1)?;
        let new_plan = index.root_dependent_join_elimination()?;
        println!("{}", new_plan);

        // let input2 = LogicalPlanBuilder::from(input.clone())
        //     .filter(col("int_col").gt(lit(1)))?
        //     .project(vec![col("string_col")])?
        //     .build()?;

        // let mut b = GeneralDecorrelation::default();
        // b.build_algebra_index(input2)?;

        Ok(())
    }

    // #[test]
    // fn todo() -> Result<()> {
    //     let mut framework = GeneralDecorrelation::default();

    //     let outer_table = test_table_scan_with_name("outer_table")?;
    //     let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;
    //     let inner_table_lv2 = test_table_scan_with_name("inner_table_lv2")?;
    //     let sq_level2 = Arc::new(
    //         LogicalPlanBuilder::from(inner_table_lv2)
    //             .filter(
    //                 out_ref_col(ArrowDataType::UInt32, "inner_table_lv1.b")
    //                     .eq(col("inner_table_lv2.b"))
    //                     .and(
    //                         out_ref_col(ArrowDataType::UInt32, "outer_table.c")
    //                             .eq(col("inner_table_lv2.c")),
    //                     ),
    //             )?
    //             .aggregate(Vec::<Expr>::new(), vec![count(col("inner_table_lv2.a"))])?
    //             .build()?,
    //     );
    //     let sq_level1 = Arc::new(
    //         LogicalPlanBuilder::from(inner_table_lv1)
    //             .filter(
    //                 col("inner_table_lv1.a")
    //                     .eq(out_ref_col(ArrowDataType::UInt32, "outer_table.a"))
    //                     .and(scalar_subquery(sq_level2).gt(lit(5))),
    //             )?
    //             .aggregate(Vec::<Expr>::new(), vec![sum(col("inner_table_lv1.b"))])?
    //             .project(vec![sum(col("inner_table_lv1.b"))])?
    //             .build()?,
    //     );

    //     let input1 = LogicalPlanBuilder::from(outer_table.clone())
    //         .filter(
    //             col("outer_table.a")
    //                 .gt(lit(1))
    //                 .and(col("outer_table.b").gt(scalar_subquery(sq_level1))),
    //         )?
    //         .build()?;
    //     framework.build(&input1)?;

    //     // let input2 = LogicalPlanBuilder::from(input.clone())
    //     //     .filter(col("int_col").gt(lit(1)))?
    //     //     .project(vec![col("string_col")])?
    //     //     .build()?;

    //     // let mut b = GeneralDecorrelation::default();
    //     // b.build_algebra_index(input2)?;

    //     Ok(())
    // }
}
