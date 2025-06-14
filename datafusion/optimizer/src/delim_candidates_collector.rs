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
use datafusion_common::{internal_datafusion_err, DataFusionError, Result};
use datafusion_expr::LogicalPlan;
use indexmap::IndexMap;

type ID = usize;
type SubPlanSize = usize;

#[allow(dead_code)]
struct Node {
    plan: LogicalPlan,
    id: ID,
    // subplan size of current node.
    sub_plan_size: SubPlanSize,
}

impl Node {
    fn new(plan: LogicalPlan, id: ID) -> Self {
        Self {
            plan,
            id,
            sub_plan_size: 0,
        }
    }
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
            node: Node::new(plan, id),
            joins: vec![],
            delim_scan_count: 0,
        }
    }
}

#[allow(dead_code)]
struct DelimCandidateVisitor {
    nodes: IndexMap<ID, Node>,
    candidates: Vec<DelimCandidate>,
    cur_id: ID,
    // all the node ids from root to the current node
    // this is mutated duri traversal
    stack: Vec<usize>,
}

#[allow(dead_code)]
impl DelimCandidateVisitor {
    fn new() -> Self {
        Self {
            nodes: IndexMap::new(),
            candidates: vec![],
            cur_id: 0,
            stack: vec![],
        }
    }

    fn collect_nodes(&mut self, plan: &LogicalPlan) -> Result<()> {
        plan.apply(|plan| {
            self.nodes
                .insert(self.cur_id, Node::new(plan.clone(), self.cur_id));
            self.cur_id += 1;

            Ok(TreeNodeRecursion::Continue)
        })?;

        // reset current id
        self.cur_id = 0;

        plan.visit(self)?;

        println!("\n=== Nodes after visit ===");
        for (id, node) in &self.nodes {
            println!(
                "Node ID: {}, Type: {:?}, SubPlan Size: {}",
                id,
                node.plan.display().to_string(),
                node.sub_plan_size
            );
        }
        println!("======================\n");

        Ok(())
    }
}

impl TreeNodeVisitor<'_> for DelimCandidateVisitor {
    type Node = LogicalPlan;

    fn f_down(&mut self, _plan: &LogicalPlan) -> Result<TreeNodeRecursion> {
        self.stack.push(self.cur_id);
        self.cur_id += 1;
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, plan: &LogicalPlan) -> Result<TreeNodeRecursion> {
        let cur_id = self.stack.pop().ok_or(internal_datafusion_err!(
            "stack cannot be empty during upward traversal"
        ))?;

        // Calculate subplan size: 1 (current node) + sum of children's subplan sizes.
        let mut subplan_size = 1;
        let mut child_id = cur_id + 1;
        for _ in plan.inputs() {
            if let Some(child_node) = self.nodes.get(&child_id) {
                subplan_size += child_node.sub_plan_size;
                child_id = child_id + child_node.sub_plan_size;
            }
        }

        // Store the subplan size for current node.
        self.nodes
            .get_mut(&cur_id)
            .ok_or_else(|| {
                DataFusionError::Plan(
                    "Node should exist when calculating subplan size".to_string(),
                )
            })?
            .sub_plan_size = subplan_size;

        Ok(TreeNodeRecursion::Continue)
    }
}

#[cfg(test)]
mod tests {
    use crate::delim_candidates_collector::DelimCandidateVisitor;
    use crate::test::test_table_scan_with_name;
    use datafusion_common::Result;
    use datafusion_expr::{expr_fn::col, lit, JoinType, LogicalPlan, LogicalPlanBuilder};

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
        visitor.collect_nodes(&plan)?;

        assert_eq!(visitor.nodes.len(), 3);

        match visitor.nodes.get(&2).unwrap().plan {
            LogicalPlan::TableScan(_) => (),
            _ => panic!("Expected TableScan at id 2"),
        }

        match visitor.nodes.get(&1).unwrap().plan {
            LogicalPlan::Filter(_) => (),
            _ => panic!("Expected Filter at id 1"),
        }

        match visitor.nodes.get(&0).unwrap().plan {
            LogicalPlan::Projection(_) => (),
            _ => panic!("Expected Projection at id 0"),
        }

        Ok(())
    }

    #[test]
    fn test_collect_nodes_with_subplan_size() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // Build left side: Filter -> TableScan t1
        let left = LogicalPlanBuilder::from(t1)
            .filter(col("t1.a").eq(lit(1)))?
            .build()?;

        // Build right side: Filter -> TableScan t2
        let right = LogicalPlanBuilder::from(t2)
            .filter(col("t2.a").eq(lit(2)))?
            .build()?;

        // Join them together
        let plan = LogicalPlanBuilder::from(left)
            .join(right, JoinType::Inner, (vec!["a"], vec!["a"]), None)?
            .project(vec![col("t1.a")])?
            .build()?;

        // Projection: t1.a
        //   Inner Join(ComparisonJoin): t1.a = t2.a
        //     Filter: t1.a = Int32(1)
        //       TableScan: t1
        //     Filter: t2.a = Int32(2)
        //       TableScan: t2

        let mut visitor = DelimCandidateVisitor::new();
        visitor.collect_nodes(&plan)?;

        // Verify nodes count
        assert_eq!(visitor.nodes.len(), 6);

        // Verify subplan sizes:
        // TableScan t1 (id: 5) - size 1 (just itself)
        assert_eq!(visitor.nodes.get(&5).unwrap().sub_plan_size, 1);

        // TableScan t2 (id: 3) - size 1 (just itself)
        assert_eq!(visitor.nodes.get(&3).unwrap().sub_plan_size, 1);

        // Filter t1 (id: 4) - size 2 (itself + TableScan)
        assert_eq!(visitor.nodes.get(&4).unwrap().sub_plan_size, 2);

        // Filter t2 (id: 2) - size 2 (itself + TableScan)
        assert_eq!(visitor.nodes.get(&2).unwrap().sub_plan_size, 2);

        // Join (id: 1) - size 5 (itself + both Filter subtrees)
        assert_eq!(visitor.nodes.get(&1).unwrap().sub_plan_size, 5);

        // Projection (id: 0) - size 6 (entire tree)
        assert_eq!(visitor.nodes.get(&0).unwrap().sub_plan_size, 6);

        Ok(())
    }

    #[test]
    fn test_complex_node_collection() -> Result<()> {
        // Build a complex plan:
        //                    Project
        //                      |
        //                    Join
        //              /             \
        //          Filter            Join
        //            |             /     \
        //        TableScan     Filter  TableScan
        //        (t1)           |       (t4)
        //                    Filter
        //                      |
        //                  TableScan
        //                    (t2)

        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let t4 = test_table_scan_with_name("t4")?;

        // Left branch: Filter -> TableScan t1
        let left = LogicalPlanBuilder::from(t1)
            .filter(col("t1.a").eq(lit(1)))?
            .build()?;

        // Right branch:
        // First build inner join
        let right_left = LogicalPlanBuilder::from(t2)
            .filter(col("t2.b").eq(lit(2)))?
            .filter(col("t2.c").eq(lit(3)))?
            .build()?;

        let right = LogicalPlanBuilder::from(right_left)
            .join(t4, JoinType::Inner, (vec!["b"], vec!["b"]), None)?
            .build()?;

        // Final plan: Join the branches and project
        let plan = LogicalPlanBuilder::from(left)
            .join(right, JoinType::Inner, (vec!["t1.a"], vec!["t2.a"]), None)?
            .project(vec![col("t1.a"), col("t2.b"), col("t4.c")])?
            .build()?;

        // Projection: t1.a, t2.b, t4.c
        //   Inner Join(ComparisonJoin): t1.a = t2.a
        //     Filter: t1.a = Int32(1)
        //       TableScan: t1
        //     Inner Join(ComparisonJoin): t2.b = t4.b
        //       Filter: t2.c = Int32(3)
        //         Filter: t2.b = Int32(2)
        //           TableScan: t2
        //       TableScan: t4

        let mut visitor = DelimCandidateVisitor::new();
        visitor.collect_nodes(&plan)?;

        // Add assertions to verify the structure
        assert_eq!(visitor.nodes.len(), 9); // Total number of nodes

        // Verify some key subplan sizes
        // Leaf nodes should have size 1
        assert_eq!(visitor.nodes.get(&8).unwrap().sub_plan_size, 1); // TableScan t1
        assert_eq!(visitor.nodes.get(&7).unwrap().sub_plan_size, 1); // TableScan t2
        assert_eq!(visitor.nodes.get(&3).unwrap().sub_plan_size, 1); // TableScan t4

        // Mid-level nodes
        assert_eq!(visitor.nodes.get(&2).unwrap().sub_plan_size, 2); // Filter -> t1
        assert_eq!(visitor.nodes.get(&6).unwrap().sub_plan_size, 2); // First Filter -> t2
        assert_eq!(visitor.nodes.get(&5).unwrap().sub_plan_size, 3); // Second Filter -> Filter -> t2
        assert_eq!(visitor.nodes.get(&4).unwrap().sub_plan_size, 5); // Join -> (Filter chain, t4)

        // Top-level nodes
        assert_eq!(visitor.nodes.get(&1).unwrap().sub_plan_size, 8); // Top Join
        assert_eq!(visitor.nodes.get(&0).unwrap().sub_plan_size, 9); // Project

        Ok(())
    }
}
