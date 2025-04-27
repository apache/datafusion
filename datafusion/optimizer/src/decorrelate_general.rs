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
use datafusion_common::{internal_err, Column, Result};
use datafusion_expr::{Expr, LogicalPlan};
use datafusion_sql::unparser::Unparser;
use indexmap::map::Entry;
use indexmap::IndexMap;
use log::Log;

pub struct GeneralDecorrelation {
    root: Option<LogicalPlan>,
    current_id: usize,
    nodes: IndexMap<LogicalPlan, Operator>, // column_
    // TODO: use a different identifier for a node, instead of the whole logical plan obj
    stack: Vec<LogicalPlan>,
}
impl fmt::Debug for GeneralDecorrelation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "GeneralDecorrelation Tree:")?;
        if let Some(root_op) = &self.root {
            self.fmt_operator(f, root_op, 0, false)?;
        } else {
            writeln!(f, "  <empty root>")?;
        }
        Ok(())
    }
}

impl GeneralDecorrelation {
    fn fmt_operator(
        &self,
        f: &mut fmt::Formatter<'_>,
        lp: &LogicalPlan,
        indent: usize,
        is_last: bool,
    ) -> fmt::Result {
        // Find the LogicalPlan corresponding to this Operator
        let op = self.nodes.get(lp).unwrap();

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
        writeln!(f, "\x1b[33m{}\x1b[0m", lp.display())?;
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

        let access_string = op
            .accesses
            .iter()
            .map(|c| c.debug())
            .collect::<Vec<_>>()
            .join(", ");
        let provide_string = op
            .provides
            .iter()
            .map(|c| c.debug())
            .collect::<Vec<_>>()
            .join(", ");
        // Now print the Operator details
        writeln!(
            f,
            "accesses: {}, provides: {}",
            access_string, provide_string,
        )?;
        let len = op.children.len();

        // Recursively print children if Operator has children
        for (i, child) in op.children.iter().enumerate() {
            let last = i + 1 == len;

            self.fmt_operator(f, child, indent + 1, last)?;
        }

        Ok(())
    }

    fn update_ancestor_node_accesses(&mut self, col: &Column) {
        // iter from bottom to top, the goal is to find the LCA only
        for node in self.stack.iter().rev() {
            let operator = self.nodes.get_mut(node).unwrap();
            let to_insert = ColumnUsage::Outer(col.clone());
            // This is the LCA between the current node and the outer column provider
            if operator.accesses.contains(&to_insert) {
                return;
            }
            operator.accesses.insert(to_insert);
        }
    }
    fn build_algebra_index(&mut self, plan: LogicalPlan) -> Result<()> {
        println!("======================================begin");
        plan.visit_with_subqueries(self)?;
        println!("======================================end");
        Ok(())
    }
    fn update_children(&mut self, parent: &LogicalPlan, child: &LogicalPlan) {
        let operator = self.nodes.get_mut(parent).unwrap();
        operator.children.push(child.clone());
    }
}

impl Default for GeneralDecorrelation {
    fn default() -> Self {
        return GeneralDecorrelation {
            root: None,
            current_id: 0,
            nodes: IndexMap::new(),
            stack: vec![],
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
#[derive(Debug)]
struct Operator {
    id: usize,
    plan: LogicalPlan,
    parent: Option<LogicalPlan>,
    // children: Vec<Rc<RefCell<Operator>>>,
    // Note if the current node is a Subquery
    // at the first time this node is visited,
    // the set of accesses columns are not sufficient
    // (i.e) some where deep down the ast another recursive subquery
    // exists and also referencing some columns belongs to the outer part
    // of the subquery
    // Thus, on discovery of new subquery, we must
    // add the accesses columns to the ancestor nodes which are Subquery
    accesses: HashSet<ColumnUsage>,
    provides: HashSet<ColumnUsage>,

    // for now only care about filter/projection with one of the expr is subquery
    is_dependent_join_node: bool,
    children: Vec<LogicalPlan>,
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

// struct ExtractScalarSubQuery<'a> {
//     sub_query_info: Vec<(Subquery, String)>,
//     in_sub_query_info: Vec<(InSubquery, String)>,
//     alias_gen: &'a Arc<AliasGenerator>,
// }

// impl TreeNodeRewriter for ExtractScalarSubQuery<'_> {
//     type Node = Expr;

//     fn f_down(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
//         match expr {
//             Expr::InSubquery(in_subquery) => {}
//             Expr::ScalarSubquery(subquery) => {
//                 let subqry_alias = self.alias_gen.next("__scalar_sq");
//                 self.sub_query_info
//                     .push((subquery.clone(), subqry_alias.clone()));
//                 let scalar_expr = subquery
//                     .subquery
//                     .head_output_expr()?
//                     .map_or(plan_err!("single expression required."), Ok)?;
//                 Ok(Transformed::new(
//                     Expr::Column(create_col_from_scalar_expr(
//                         &scalar_expr,
//                         subqry_alias,
//                     )?),
//                     true,
//                     TreeNodeRecursion::Jump,
//                 ))
//             }
//             _ => Ok(Transformed::no(expr)),
//         }
//     }
// }

fn print(a: &Expr) -> Result<()> {
    let unparser = Unparser::default();
    let round_trip_sql = unparser.expr_to_sql(a)?.to_string();
    println!("{}", round_trip_sql);
    Ok(())
}

impl TreeNodeVisitor<'_> for GeneralDecorrelation {
    type Node = LogicalPlan;
    fn f_down(&mut self, node: &LogicalPlan) -> Result<TreeNodeRecursion> {
        if self.root.is_none() {
            self.root = Some(node.clone());
        }
        let mut is_dependent_join_node = false;
        println!("{}\nnode {}", "----".repeat(self.stack.len()), node);
        // for each node, find which column it is accessing, which column it is providing
        // Set of columns current node access
        let (accesses, provides): (HashSet<ColumnUsage>, HashSet<ColumnUsage>) =
            match node {
                LogicalPlan::Filter(f) => {
                    if contains_subquery(&f.predicate) {
                        is_dependent_join_node = true;
                        print(&f.predicate);
                    }
                    let mut outer_col_refs: HashSet<ColumnUsage> = f
                        .predicate
                        .outer_column_refs()
                        .into_iter()
                        .map(|f| {
                            self.update_ancestor_node_accesses(f);
                            ColumnUsage::Outer(f.clone())
                        })
                        .collect();

                    outer_col_refs.extend(
                        f.predicate
                            .column_refs()
                            .into_iter()
                            .map(|f| ColumnUsage::Own(f.clone())),
                    );
                    (outer_col_refs, HashSet::new())
                }
                LogicalPlan::TableScan(tbl_scan) => {
                    let provided_columns: HashSet<ColumnUsage> = tbl_scan
                        .projected_schema
                        .columns()
                        .into_iter()
                        .map(|col| ColumnUsage::Own(col))
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
                        }
                    }
                    // proj.expr
                    // TODO: fix me
                    (HashSet::new(), HashSet::new())
                }
                LogicalPlan::Subquery(subquery) => {
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
            self.update_children(&previous_node, node);
            Some(self.stack.last().unwrap().to_owned())
        };

        self.stack.push(node.clone());
        self.nodes.insert(
            node.clone(),
            Operator {
                id: self.current_id,
                parent,
                plan: node.clone(),
                accesses,
                provides,
                is_dependent_join_node,
                children: vec![],
            },
        );
        // let operator = match self.nodes.entry(node.clone()) {
        //     Entry::Occupied(entry) => entry.into_mut(),
        //     Entry::Vacant(entry) => {
        //         let parent = if self.stack.len() == 0 {
        //             None
        //         } else {
        //             Some(self.stack.last().unwrap().to_owned())
        //         };
        //         entry.insert(Operator {
        //             id: self.current_id,
        //             parent,
        //             plan: node.clone(),
        //             accesses,
        //             provides,
        //         })
        //     }
        // };

        Ok(TreeNodeRecursion::Continue)
    }

    /// Invoked while traversing up the tree after children are visited. Default
    /// implementation continues the recursion.
    fn f_up(&mut self, _node: &Self::Node) -> Result<TreeNodeRecursion> {
        self.stack.pop();
        Ok(TreeNodeRecursion::Continue)
    }
}

impl OptimizerRule for GeneralDecorrelation {
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
        lit, out_ref_col, scalar_subquery, table_scan, CreateMemoryTable, EmptyRelation,
        Expr, LogicalPlan, LogicalPlanBuilder,
    };
    use datafusion_functions_aggregate::{count::count, sum::sum};
    use regex_syntax::ast::LiteralKind;

    use crate::test::{test_table_scan, test_table_scan_with_name};

    use super::GeneralDecorrelation;
    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType as ArrowDataType, Field, Fields, Schema},
    };

    #[test]
    fn todo() -> Result<()> {
        let mut a = GeneralDecorrelation::default();

        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;
        let inner_table_lv2 = test_table_scan_with_name("inner_table_lv2")?;
        let sq_level2 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv2)
                .filter(
                    out_ref_col(ArrowDataType::UInt32, "inner_table_lv1.b")
                        .eq(col("inner_table_lv2.b")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![count(col("inner_table_lv2.a"))])?
                .build()?,
        );
        let sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1)
                .filter(
                    col("inner_table_lv1.a")
                        .eq(out_ref_col(ArrowDataType::UInt32, "outer_table.a")),
                )?
                .filter(scalar_subquery(sq_level2).gt(lit(5)))?
                .aggregate(Vec::<Expr>::new(), vec![sum(col("inner_table_lv1.b"))])?
                .project(vec![sum(col("inner_table_lv1.b"))])?
                .build()?,
        );

        let input1 = LogicalPlanBuilder::from(outer_table.clone())
            .filter(col("outer_table.a").gt(lit(1)))?
            .filter(col("outer_table.b").gt(scalar_subquery(sq_level1)))?
            .build()?;
        a.build_algebra_index(input1.clone())?;
        println!("{:?}", a);

        // let input2 = LogicalPlanBuilder::from(input.clone())
        //     .filter(col("int_col").gt(lit(1)))?
        //     .project(vec![col("string_col")])?
        //     .build()?;

        // let mut b = GeneralDecorrelation::default();
        // b.build_algebra_index(input2)?;

        Ok(())
    }
}
