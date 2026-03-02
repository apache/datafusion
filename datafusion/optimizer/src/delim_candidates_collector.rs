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

use std::sync::Arc;

use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion_common::{internal_datafusion_err, DataFusionError, Result};
use datafusion_expr::{JoinKind, LogicalPlan};
use indexmap::IndexMap;

type ID = usize;

#[derive(Clone)]
pub struct Node {
    pub plan: LogicalPlan,
    pub id: ID,
    // subplan size of current node.
    pub sub_plan_size: usize,
}

impl Node {
    fn new(plan: LogicalPlan, id: ID, sub_plan_size: usize) -> Self {
        Self {
            plan,
            id,
            sub_plan_size,
        }
    }
}

#[derive(Clone)]
pub struct JoinWithDelimScan {
    // Join node under DelimCandidate.
    pub node: Node,
    pub depth: usize,
    pub can_be_eliminated: bool,
    pub is_filter_generated: bool,
    pub replacement_plan: Option<Arc<LogicalPlan>>,
}

impl JoinWithDelimScan {
    fn new(plan: LogicalPlan, id: ID, depth: usize, sub_plan_size: usize) -> Self {
        Self {
            node: Node::new(plan, id, sub_plan_size),
            depth,
            can_be_eliminated: false,
            is_filter_generated: false,
            replacement_plan: None,
        }
    }
}

pub struct DelimCandidate {
    pub node: Node,
    pub joins: Vec<JoinWithDelimScan>,
    pub delim_scan_count: usize,
    pub is_transformed: bool,
}

impl DelimCandidate {
    fn new(plan: LogicalPlan, id: ID, sub_plan_size: usize) -> Self {
        Self {
            node: Node::new(plan, id, sub_plan_size),
            joins: vec![],
            delim_scan_count: 0,
            is_transformed: false,
        }
    }
}

pub struct NodeVisitor {
    nodes: IndexMap<ID, Node>,
    cur_id: ID,
    // all the node ids from root to the current node
    stack: Vec<usize>,
}

impl NodeVisitor {
    pub fn new() -> Self {
        Self {
            nodes: IndexMap::new(),
            cur_id: 0,
            stack: vec![],
        }
    }

    pub fn collect_nodes(&mut self, plan: &LogicalPlan) -> Result<()> {
        plan.apply(|plan| {
            self.nodes
                .insert(self.cur_id, Node::new(plan.clone(), self.cur_id, 0));
            self.cur_id += 1;

            Ok(TreeNodeRecursion::Continue)
        })?;

        // reset current id
        self.cur_id = 0;

        plan.visit(self)?;

        // println!("\n=== Nodes after visit ===");
        // for (id, node) in &self.nodes {
        //     println!(
        //         "Node ID: {}, Type: {:?}, SubPlan Size: {}",
        //         id,
        //         node.plan.display().to_string(),
        //         node.sub_plan_size
        //     );
        // }
        // println!("======================\n");

        Ok(())
    }
}

impl TreeNodeVisitor<'_> for NodeVisitor {
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
        plan.apply_children(|_| {
            if let Some(child_node) = self.nodes.get(&child_id) {
                subplan_size += child_node.sub_plan_size;
                child_id = child_id + child_node.sub_plan_size;
            }

            Ok(TreeNodeRecursion::Continue)
        })?;

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

pub struct DelimCandidateVisitor {
    pub candidates: IndexMap<ID, DelimCandidate>,
    node_visitor: NodeVisitor,
    cur_id: ID,
    // all the node ids from root to the current node
    stack: Vec<usize>,
}

impl DelimCandidateVisitor {
    pub fn new(node_visitor: NodeVisitor) -> Self {
        Self {
            candidates: IndexMap::new(),
            node_visitor,
            cur_id: 0,
            stack: vec![],
        }
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

        if let LogicalPlan::Join(join) = plan {
            if join.join_kind == JoinKind::DelimJoin {
                let sub_plan_size = self
                    .node_visitor
                    .nodes
                    .get(&cur_id)
                    .ok_or_else(|| {
                        DataFusionError::Plan("current node should exist".to_string())
                    })?
                    .sub_plan_size;

                self.candidates.insert(
                    cur_id,
                    DelimCandidate::new(plan.clone(), cur_id, sub_plan_size),
                );

                let left_id = cur_id + 1;
                // We calculate the right child id from left child's subplan size.
                let right_id = self
                    .node_visitor
                    .nodes
                    .get(&left_id)
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "left id {} should exist in join",
                            left_id
                        ))
                    })?
                    .sub_plan_size
                    + left_id;

                let mut candidate = self
                    .candidates
                    .get_mut(&cur_id)
                    .ok_or_else(|| internal_datafusion_err!("Candidate should exist"))?;
                let right_plan = &self
                    .node_visitor
                    .nodes
                    .get(&right_id)
                    .ok_or_else(|| {
                        DataFusionError::Plan(
                            "right child should exist in join".to_string(),
                        )
                    })?
                    .plan;

                // DelimScan are in the RHS.
                let mut collector = DelimCandidatesCollector::new(
                    &self.node_visitor,
                    &mut candidate,
                    0,
                    right_id,
                );
                right_plan.visit(&mut collector)?;
            }
        }

        Ok(TreeNodeRecursion::Continue)
    }
}

struct DelimCandidatesCollector<'a> {
    node_visitor: &'a NodeVisitor,
    candidate: &'a mut DelimCandidate,
    depth: usize,
    cur_id: ID,
    // all the node ids from root to the current node
    stack: Vec<usize>,
}

impl<'a> DelimCandidatesCollector<'a> {
    fn new(
        node_visitor: &'a NodeVisitor,
        candidate: &'a mut DelimCandidate,
        depth: usize,
        cur_id: ID,
    ) -> Self {
        Self {
            node_visitor,
            candidate,
            depth,
            cur_id,
            stack: vec![],
        }
    }
}

impl<'n> TreeNodeVisitor<'n> for DelimCandidatesCollector<'_> {
    type Node = LogicalPlan;

    fn f_down(&mut self, _plan: &LogicalPlan) -> Result<TreeNodeRecursion> {
        self.stack.push(self.cur_id);
        self.cur_id += 1;

        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, plan: &LogicalPlan) -> Result<TreeNodeRecursion> {
        let mut recursion = TreeNodeRecursion::Continue;

        let cur_id = self.stack.pop().ok_or(internal_datafusion_err!(
            "stack cannot be empty during upward traversal"
        ))?;

        match plan {
            LogicalPlan::Join(join) => {
                if join.join_kind == JoinKind::DelimJoin {
                    // iterate left child
                    let left_child_id = cur_id + 1;
                    let left_plan = &self
                        .node_visitor
                        .nodes
                        .get(&left_child_id)
                        .ok_or_else(|| {
                            DataFusionError::Plan(
                                "left child should exist in join".to_string(),
                            )
                        })?
                        .plan;
                    let mut new_collector = DelimCandidatesCollector::new(
                        &self.node_visitor,
                        &mut self.candidate,
                        self.depth + 1,
                        cur_id + 1,
                    );
                    left_plan.visit(&mut new_collector)?;

                    recursion = TreeNodeRecursion::Stop;
                }
            }
            LogicalPlan::DelimGet(_) => {
                self.candidate.delim_scan_count += 1;
            }
            _ => {}
        }

        if let LogicalPlan::Join(join) = plan {
            if join.join_kind == JoinKind::ComparisonJoin
                && (plan_is_delim_scan(join.left.as_ref())
                    || plan_is_delim_scan(join.right.as_ref()))
            {
                let sub_plan_size = self
                    .node_visitor
                    .nodes
                    .get(&cur_id)
                    .ok_or_else(|| {
                        DataFusionError::Plan("current node should exist".to_string())
                    })?
                    .sub_plan_size;

                self.candidate.joins.push(JoinWithDelimScan::new(
                    plan.clone(),
                    cur_id,
                    self.depth,
                    sub_plan_size,
                ));
            }
        }

        self.depth += 1;

        Ok(recursion)
    }
}

fn plan_is_delim_scan(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Filter(filter) => {
            if let LogicalPlan::DelimGet(_) = filter.input.as_ref() {
                true
            } else {
                false
            }
        }
        LogicalPlan::DelimGet(_) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use crate::delim_candidates_collector::NodeVisitor;
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

        let mut visitor = NodeVisitor::new();
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

        let mut visitor = NodeVisitor::new();
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

        let mut visitor = NodeVisitor::new();
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

    // TODO: add test for candidate collector.
}
