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
use datafusion_expr::LogicalPlan;
use indexmap::IndexMap;

type ID = usize;

#[allow(dead_code)]
struct Node {
    plan: LogicalPlan,
    id: ID,
}

#[allow(dead_code)]
struct JoinWithDelimScan {
    // Join node under DelimCandidate.
    node: Node,
    depth: ID,
}

#[allow(dead_code)]
struct DelimCandidate {
    node: Node,
    joins: Vec<JoinWithDelimScan>,
    delim_scan_count: ID,
}

#[allow(dead_code)]
impl DelimCandidate {
    fn new(plan: LogicalPlan, id: ID) -> Self {
        Self {
            node: Node { plan, id },
            joins: vec![],
            delim_scan_count: 0,
        }
    }
}

#[allow(dead_code)]
struct DelimCandidateVisitor {
    nodes: IndexMap<ID, LogicalPlan>,
    candidates: Vec<DelimCandidate>,
}

#[allow(dead_code)]
impl DelimCandidateVisitor {
    fn new() -> Self {
        Self {
            nodes: IndexMap::new(),
            candidates: vec![],
        }
    }

    fn collect_nodes(&mut self, plan: &LogicalPlan) -> Result<()> {
        let mut cur_id = 0;
        plan.apply(|plan| {
            let new_id = cur_id;
            self.nodes.insert(new_id, plan.clone());

            cur_id += 1;

            Ok(TreeNodeRecursion::Continue)
        })?;

        Ok(())
    }
}

impl TreeNodeVisitor<'_> for DelimCandidateVisitor {
    type Node = LogicalPlan;

    fn f_down(&mut self, _plan: &LogicalPlan) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, _plan: &LogicalPlan) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
}

#[cfg(test)]
mod tests {
    use crate::decorrelate_dependent_join::DecorrelateDependentJoin;
    use crate::delim_candidates_collector::DelimCandidateVisitor;
    use crate::deliminator::Deliminator;
    use crate::test::test_table_scan_with_name;
    use crate::Optimizer;
    use crate::{
        assert_optimized_plan_eq_display_indent_snapshot, OptimizerConfig,
        OptimizerContext, OptimizerRule,
    };
    use arrow::datatypes::DataType as ArrowDataType;
    use datafusion_common::Result;
    use datafusion_expr::{
        exists, expr_fn::col, in_subquery, lit, out_ref_col, scalar_subquery, Expr,
        LogicalPlan, LogicalPlanBuilder,
    };
    use datafusion_functions_aggregate::count::count;
    use std::sync::Arc;
    #[test]
    fn test_collect_nodes() -> Result<()> {
        let table = test_table_scan_with_name("t1")?;
        let plan = LogicalPlanBuilder::from(table)
            .filter(col("t1.a").eq(lit(1)))?
            .project(vec![col("t1.a")])?
            .build()?;

        // Projection: t1.a
        //   Filter: t1.a = Int32(1)
        //     TableScan: t1

        let mut visitor = DelimCandidateVisitor::new();
        visitor.collect_nodes(&plan);

        assert_eq!(visitor.nodes.len(), 3);

        match visitor.nodes.get(&2) {
            Some(LogicalPlan::TableScan(_)) => (),
            _ => panic!("Expected TableScan at id 2"),
        }

        match visitor.nodes.get(&1) {
            Some(LogicalPlan::Filter(_)) => (),
            _ => panic!("Expected Filter at id 1"),
        }

        match visitor.nodes.get(&0) {
            Some(LogicalPlan::Projection(_)) => (),
            _ => panic!("Expected Projection at id 0"),
        }

        Ok(())
    }
}
