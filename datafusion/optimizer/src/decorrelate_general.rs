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
use indexmap::map::Entry;
use indexmap::IndexMap;

#[derive(Debug)]
pub struct GeneralDecorrelation {
    root: Option<Operator>,
    current_id: usize,
    nodes: IndexMap<LogicalPlan, Operator>, // column_
    stack: Vec<LogicalPlan>,
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

#[derive(Debug)]
struct Operator {
    id: usize,
    plan: LogicalPlan,
    parent: Option<LogicalPlan>,
    // children: Vec<Rc<RefCell<Operator>>>,
    accesses: HashSet<Column>,
    provides: HashSet<Column>,
}

impl GeneralDecorrelation {
    fn build_algebra_index(&mut self, plan: LogicalPlan) -> Result<()> {
        plan.visit(self)?;
        Ok(())
    }
}

impl TreeNodeVisitor<'_> for GeneralDecorrelation {
    type Node = LogicalPlan;
    fn f_down(&mut self, node: &LogicalPlan) -> Result<TreeNodeRecursion> {
        self.stack.push(node.clone());
        println!("+++node {:?}", node);
        // for each node, find which column it is accessing, which column it is providing
        // Set of columns current node access
        let (accesses, provides): (HashSet<Column>, HashSet<Column>) = match node {
            LogicalPlan::Filter(f) => (
                HashSet::new(),
                f.predicate
                    .column_refs()
                    .into_iter()
                    .map(|r| r.to_owned())
                    .collect(),
            ),
            LogicalPlan::TableScan(tbl_scan) => {
                let provided_columns: HashSet<Column> =
                    tbl_scan.projected_schema.columns().into_iter().collect();
                (provided_columns, HashSet::new())
            }
            LogicalPlan::Aggregate(_) => (HashSet::new(), HashSet::new()),
            LogicalPlan::EmptyRelation(_) => (HashSet::new(), HashSet::new()),
            LogicalPlan::Limit(_) => (HashSet::new(), HashSet::new()),
            LogicalPlan::Subquery(_) => (HashSet::new(), HashSet::new()),
            _ => {
                return internal_err!("impl scan for node type {:?}", node);
            }
        };

        let parent = if self.stack.is_empty() {
            None
        } else {
            Some(self.stack.last().unwrap().to_owned())
        };
        self.nodes.insert(
            node.clone(),
            Operator {
                id: self.current_id,
                parent,
                plan: node.clone(),
                accesses,
                provides,
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
    use datafusion_functions_aggregate::sum::sum;
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
        let inner_table = test_table_scan_with_name("inner_table")?;
        let sq = Arc::new(
            LogicalPlanBuilder::from(inner_table)
                .filter(
                    col("inner_table.a")
                        .eq(out_ref_col(ArrowDataType::UInt64, "outer_table.a")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![sum(col("inner_table.b"))])?
                .project(vec![sum(col("inner_table.b"))])?
                .build()?,
        );

        let input1 = LogicalPlanBuilder::from(outer_table.clone())
            .filter(col("outer_table.a").gt(lit(1)))?
            .filter(col("inner_table.b").gt(scalar_subquery(sq)))?
            .build()?;
        a.build_algebra_index(input1.clone())?;

        // let input2 = LogicalPlanBuilder::from(input.clone())
        //     .filter(col("int_col").gt(lit(1)))?
        //     .project(vec![col("string_col")])?
        //     .build()?;

        // let mut b = GeneralDecorrelation::default();
        // b.build_algebra_index(input2)?;

        Ok(())
    }
}
