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

use std::any::Any;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use crate::analyzer::type_coercion::TypeCoercionRewriter;
use crate::decorrelate::UN_MATCHED_ROW_INDICATOR;
use crate::{ApplyOrder, OptimizerConfig, OptimizerRule};

use arrow::datatypes::DataType;
use datafusion_common::alias::AliasGenerator;
use datafusion_common::tree_node::{
    Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter, TreeNodeVisitor,
};
use datafusion_common::{internal_err, Column, HashMap, Result};
use datafusion_expr::expr::{self, Exists, InSubquery};
use datafusion_expr::expr_rewriter::{normalize_col, strip_outer_reference};
use datafusion_expr::select_expr::SelectExpr;
use datafusion_expr::utils::{conjunction, disjunction, split_conjunction};
use datafusion_expr::{
    binary_expr, col, expr_fn, lit, BinaryExpr, Cast, DependentJoin, Expr, Filter,
    JoinType, LogicalPlan, LogicalPlanBuilder, Operator as ExprOperator, Subquery,
};
use datafusion_expr::{in_list, out_ref_col};

use indexmap::map::Entry;
use indexmap::{IndexMap, IndexSet};
use itertools::Itertools;
use log::Log;

pub struct DependentJoinRewriter {
    // each logical plan traversal will assign it a integer id
    current_id: usize,
    subquery_depth: usize,
    // each newly visted operator is inserted inside this map for tracking
    nodes: IndexMap<usize, Node>,
    // all the node ids from root to the current node
    // this is used during traversal only
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
        let mut lca = None;

        let min_len = stack_with_table_provider
            .len()
            .min(stack_with_subquery.len());

        for i in 0..min_len {
            let ai = stack_with_subquery[i];
            let bi = stack_with_table_provider[i];

            if ai == bi {
                lca = Some((ai, stack_with_subquery[ai]));
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
        if let Some(accesses) = self.all_outer_ref_columns.get(col) {
            for access in accesses.iter() {
                let mut cur_stack = self.stack.clone();

                cur_stack.push(child_id);
                if col.name() == "outer_table.a" || col.name == "a" {
                    println!("{:?}", access);
                    println!("{:?}", cur_stack);
                }
                // this is a dependent join node
                let (dependent_join_node_id, subquery_node_id) =
                    Self::dependent_join_and_subquery_node_ids(&cur_stack, &access.stack);
                let node = self.nodes.get_mut(&dependent_join_node_id).unwrap();
                let accesses = node.access_tracker.entry(subquery_node_id).or_default();
                accesses.push(ColumnAccess {
                    col: col.clone(),
                    node_id: access.node_id,
                    stack: access.stack.clone(),
                    data_type: access.data_type.clone(),
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
        let mut stack = self.stack.clone();
        stack.push(child_id);
        self.all_outer_ref_columns
            .entry(col.clone())
            .or_default()
            .push(ColumnAccess {
                stack,
                node_id: child_id,
                col: col.clone(),
                data_type: data_type.clone(),
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
        return DependentJoinRewriter {
            alias_generator,
            current_id: 0,
            nodes: IndexMap::new(),
            stack: vec![],
            all_outer_ref_columns: IndexMap::new(),
            subquery_depth: 0,
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

    // This field is only meaningful if the node is dependent join node
    // it track which descendent nodes still accessing the outer columns provided by its
    // left child
    // the key of this map is node_id of the children subquery
    // and insertion order matters here, and thus we use IndexMap
    access_tracker: IndexMap<usize, Vec<ColumnAccess>>,

    is_dependent_join_node: bool,
    is_subquery_node: bool,

    // note that for dependent join nodes, there can be more than 1
    // subquery children at a time, but always 1 outer-column-providing-child
    // which is at the last element
    subquery_type: SubqueryType,
}
#[derive(Debug, Clone)]
enum SubqueryType {
    None,
    In(Expr),
    Exists(Expr),
    Scalar(Expr),
}

impl SubqueryType {
    fn unwrap_expr(&self) -> Expr {
        match self {
            SubqueryType::None => {
                panic!("not reached")
            }
            SubqueryType::In(e) | SubqueryType::Exists(e) | SubqueryType::Scalar(e) => {
                e.clone()
            }
        }
    }
    fn default_join_type(&self) -> JoinType {
        match self {
            SubqueryType::None => {
                panic!("not reached")
            }
            SubqueryType::In(_) => JoinType::LeftSemi,
            SubqueryType::Exists(_) => JoinType::LeftMark,
            // TODO: in duckdb, they have JoinType::Single
            // where there is only at most one join partner entry on the LEFT
            SubqueryType::Scalar(_) => JoinType::Left,
        }
    }
    fn prefix(&self) -> String {
        match self {
            SubqueryType::None => "",
            SubqueryType::In(_) => "__in_sq",
            SubqueryType::Exists(_) => "__exists_sq",
            SubqueryType::Scalar(_) => "__scalar_sq",
        }
        .to_string()
    }
}
fn unwrap_subquery_input_from_expr(expr: &Expr) -> Arc<LogicalPlan> {
    match expr {
        Expr::ScalarSubquery(sq) => sq.subquery.clone(),
        Expr::Exists(exists) => exists.subquery.subquery.clone(),
        Expr::InSubquery(in_sq) => in_sq.subquery.subquery.clone(),
        _ => unreachable!(),
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

impl TreeNodeRewriter for DependentJoinRewriter {
    type Node = LogicalPlan;
    fn f_down(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        self.current_id += 1;
        let mut is_subquery_node = false;
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
                            self.mark_outer_column_access(
                                self.current_id,
                                data_type,
                                col,
                            );
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })
                    .expect("traversal is infallible");
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
                for expr in &proj.expr {
                    if contains_subquery(expr) {
                        is_dependent_join_node = true;
                        break;
                    }
                    expr.apply(|expr| {
                        if let Expr::OuterReferenceColumn(data_type, col) = expr {
                            self.mark_outer_column_access(
                                self.current_id,
                                data_type,
                                col,
                            );
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })?;
                }
            }
            LogicalPlan::Subquery(subquery) => {
                is_subquery_node = true;
                let parent = self.stack.last().unwrap();
                let parent_node = self.nodes.get_mut(parent).unwrap();
                parent_node.access_tracker.insert(self.current_id, vec![]);
                for expr in parent_node.plan.expressions() {
                    expr.exists(|e| {
                        let (found_sq, checking_type) = match e {
                            Expr::ScalarSubquery(sq) => {
                                if sq == subquery {
                                    (true, SubqueryType::Scalar(e.clone()))
                                } else {
                                    (false, SubqueryType::None)
                                }
                            }
                            Expr::Exists(exist) => {
                                if &exist.subquery == subquery {
                                    (true, SubqueryType::Exists(e.clone()))
                                } else {
                                    (false, SubqueryType::None)
                                }
                            }
                            Expr::InSubquery(in_sq) => {
                                if &in_sq.subquery == subquery {
                                    (true, SubqueryType::In(e.clone()))
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
            LogicalPlan::Aggregate(_) => {}
            _ => {
                return internal_err!("impl f_down for node type {:?}", node);
            }
        };

        if is_dependent_join_node {
            self.subquery_depth += 1
        }
        self.stack.push(self.current_id);
        self.nodes.insert(
            self.current_id,
            Node {
                id: self.current_id,
                plan: node.clone(),
                is_subquery_node,
                is_dependent_join_node,
                access_tracker: IndexMap::new(),
                subquery_type,
            },
        );

        Ok(Transformed::no(node))
    }
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
        assert!(
            1 == node.inputs().len(),
            "a dependent join node cannot have more than 1 child"
        );

        let cloned_input = (**node.inputs().first().unwrap()).clone();
        let mut current_plan = LogicalPlanBuilder::new(cloned_input);
        let mut subquery_alias_by_offset = HashMap::new();
        // let mut subquery_alias_by_node_id = HashMap::new();
        let mut subquery_expr_by_offset = HashMap::new();
        for (subquery_offset, (subquery_id, column_accesses)) in
            node_info.access_tracker.iter().enumerate()
        {
            let subquery_node = self.nodes.get(subquery_id).unwrap();
            // let subquery_input = subquery_node.plan.inputs().first().unwrap();
            let alias = self
                .alias_generator
                .next(&subquery_node.subquery_type.prefix());
            subquery_alias_by_offset.insert(subquery_offset, alias);
        }

        match &node {
            LogicalPlan::Filter(filter) => {
                // everytime we meet a subquery during traversal, we increment this by 1
                // we can use this offset to lookup the original subquery info
                // in subquery_alias_by_offset
                // the reason why we cannot create a hashmap keyed by Subquery object
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
                            Expr::InSubquery(_) | Expr::Exists(_) => {
                                subquery_alias_by_offset.get(offset_ref).unwrap()
                            }
                            Expr::ScalarSubquery(ref s) => {
                                println!("inserting new expr {}", s.subquery);
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
                        Ok(Transformed::yes(col(format!("{}.output", alias))))
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
                    node_info.access_tracker.iter().enumerate()
                {
                    let alias = subquery_alias_by_offset.get(&subquery_offset).unwrap();
                    let subquery_expr =
                        subquery_expr_by_offset.get(&subquery_offset).unwrap();

                    let subquery_input = unwrap_subquery_input_from_expr(subquery_expr);

                    let correlated_columns = column_accesses
                        .iter()
                        .map(|ac| (ac.col.clone()))
                        .unique()
                        .collect();

                    current_plan = current_plan.dependent_join(
                        subquery_input.deref().clone(),
                        correlated_columns,
                        subquery_expr.clone(),
                        current_subquery_depth,
                        alias.clone(),
                    )?;
                }
                current_plan = current_plan
                    .filter(new_predicate.clone())?
                    .project(post_join_projections)?;
            }
            _ => {
                unimplemented!("implement more dependent join node creation")
            }
        }
        Ok(Transformed::yes(current_plan.build()?))
    }
}
#[derive(Debug)]
struct Decorrelation {}

impl OptimizerRule for Decorrelation {
    fn supports_rewrite(&self) -> bool {
        true
    }
    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let mut transformer =
            DependentJoinRewriter::new(config.alias_generator().clone());
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

    // The rewriter handle recursion
    // fn apply_order(&self) -> Option<ApplyOrder> {
    //    None
    // }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion_common::{alias::AliasGenerator, DFSchema, Result, ScalarValue};
    use datafusion_expr::{
        exists,
        expr_fn::{self, col, not},
        in_subquery, lit, out_ref_col, scalar_subquery, table_scan, CreateMemoryTable,
        EmptyRelation, Expr, LogicalPlan, LogicalPlanBuilder,
    };
    use datafusion_functions_aggregate::{count::count, sum::sum};
    use insta::assert_snapshot;
    use regex_syntax::ast::LiteralKind;

    use crate::{
        assert_optimized_plan_eq_display_indent_snapshot,
        test::{test_table_scan, test_table_scan_with_name},
    };

    use super::DependentJoinRewriter;
    use arrow::datatypes::DataType as ArrowDataType;

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
    fn simple_in_subquery_inside_from_expr() -> Result<()> {
        Ok(())
    }
    #[test]
    fn simple_in_subquery_inside_select_expr() -> Result<()> {
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

        let input1 = LogicalPlanBuilder::from(outer_table.clone())
            .filter(
                col("outer_table.a")
                    .gt(lit(1))
                    .and(scalar_subquery(scalar_sq_level1).eq(col("outer_table.a"))),
            )?
            .build()?;
        assert_dependent_join_rewrite!(input1,@r"
Projection: outer_table.a, outer_table.b, outer_table.c [a:UInt32, b:UInt32, c:UInt32]
  Filter: outer_table.a > Int32(1) AND __scalar_sq_2.output = outer_table.a [a:UInt32, b:UInt32, c:UInt32, output:Int64]
    DependentJoin on [outer_table.a, outer_table.c] with expr (<subquery>) depth 1 [a:UInt32, b:UInt32, c:UInt32, output:Int64]
      TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
      Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv1.a)]] [count(inner_table_lv1.a):Int64]
        Projection: inner_table_lv1.a, inner_table_lv1.b, inner_table_lv1.c [a:UInt32, b:UInt32, c:UInt32]
          Filter: inner_table_lv1.c = outer_ref(outer_table.c) AND __scalar_sq_1.output = Int32(1) [a:UInt32, b:UInt32, c:UInt32, output:Int64]
            DependentJoin on [inner_table_lv1.b] with expr (<subquery>) depth 2 [a:UInt32, b:UInt32, c:UInt32, output:Int64]
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

        let input1 = LogicalPlanBuilder::from(outer_table.clone())
            .filter(
                col("outer_table.a")
                    .gt(lit(1))
                    .and(exists(exist_sq_level1))
                    .and(in_subquery(col("outer_table.b"), in_sq_level1)),
            )?
            .build()?;
        assert_dependent_join_rewrite!(input1,@r"
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

        let input1 = LogicalPlanBuilder::from(outer_table.clone())
            .filter(
                col("outer_table.a")
                    .gt(lit(1))
                    .and(in_subquery(col("outer_table.c"), sq_level1)),
            )?
            .build()?;
        assert_dependent_join_rewrite!(input1,@r"
Projection: outer_table.a, outer_table.b, outer_table.c [a:UInt32, b:UInt32, c:UInt32]
  Filter: outer_table.a > Int32(1) AND __in_sq_1.output [a:UInt32, b:UInt32, c:UInt32, output:Boolean]
    DependentJoin on [outer_table.a, outer_table.b] with expr outer_table.c IN (<subquery>) depth 1 [a:UInt32, b:UInt32, c:UInt32, output:Boolean]
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

        let input1 = LogicalPlanBuilder::from(outer_table.clone())
            .filter(col("outer_table.a").gt(lit(1)).and(exists(sq_level1)))?
            .build()?;
        assert_dependent_join_rewrite!(input1,@r"
Projection: outer_table.a, outer_table.b, outer_table.c [a:UInt32, b:UInt32, c:UInt32]
  Filter: outer_table.a > Int32(1) AND __exists_sq_1.output [a:UInt32, b:UInt32, c:UInt32, output:Boolean]
    DependentJoin on [outer_table.a, outer_table.b] with expr EXISTS (<subquery>) depth 1 [a:UInt32, b:UInt32, c:UInt32, output:Boolean]
      TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
      Projection: outer_ref(outer_table.b) AS outer_b_alias [outer_b_alias:UInt32;N]
        Filter: inner_table_lv1.a = outer_ref(outer_table.a) AND outer_ref(outer_table.a) > inner_table_lv1.c AND inner_table_lv1.b = Int32(1) AND outer_ref(outer_table.b) = inner_table_lv1.b [a:UInt32, b:UInt32, c:UInt32]
          TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
        ");
        Ok(())
    }

    #[test]
    fn rewrite_exist_subquery_with_no_dependent_columns() -> Result<()> {
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

        assert_dependent_join_rewrite!(input1,@r"
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

        let input1 = LogicalPlanBuilder::from(outer_table.clone())
            .filter(
                col("outer_table.a")
                    .gt(lit(1))
                    .and(in_subquery(col("outer_table.c"), sq_level1)),
            )?
            .build()?;
        assert_dependent_join_rewrite!(input1,@r"
Projection: outer_table.a, outer_table.b, outer_table.c [a:UInt32, b:UInt32, c:UInt32]
  Filter: outer_table.a > Int32(1) AND __in_sq_1.output [a:UInt32, b:UInt32, c:UInt32, output:Boolean]
    DependentJoin on [] with expr outer_table.c IN (<subquery>) depth 1 [a:UInt32, b:UInt32, c:UInt32, output:Boolean]
      TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
      Projection: inner_table_lv1.b [b:Uint32] 
        Filter: inner_table_lv1.a = Int32(1) [a:UInt32, b:UInt32, c:UInt32]
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

        let input1 = LogicalPlanBuilder::from(outer_table.clone())
            .filter(
                col("outer_table.a")
                    .gt(lit(1))
                    .and(in_subquery(col("outer_table.c"), sq_level1)),
            )?
            .build()?;
        assert_dependent_join_rewrite!(input1,@r"
Projection: outer_table.a, outer_table.b, outer_table.c [a:UInt32, b:UInt32, c:UInt32]
  Filter: outer_table.a > Int32(1) AND __in_sq_1.output [a:UInt32, b:UInt32, c:UInt32, output:Boolean]
    DependentJoin on [outer_table.a, outer_table.b] with expr outer_table.c IN (<subquery>) depth 1 [a:UInt32, b:UInt32, c:UInt32, output:Boolean]
      TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
      Projection: outer_ref(outer_table.b) AS outer_b_alias [outer_b_alias:UInt32;N]
        Filter: inner_table_lv1.a = outer_ref(outer_table.a) AND outer_ref(outer_table.a) > inner_table_lv1.c AND inner_table_lv1.b = Int32(1) AND outer_ref(outer_table.b) = inner_table_lv1.b [a:UInt32, b:UInt32, c:UInt32]
          TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
        ");
        Ok(())
    }
}
