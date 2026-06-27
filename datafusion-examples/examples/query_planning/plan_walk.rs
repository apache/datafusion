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

//! See `main.rs` for how to run it.

use std::collections::HashSet;

use arrow::datatypes::{DataType, Field, Schema};

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::tree_node::{TreeNodeRewriter, TreeNodeVisitor};
use datafusion::common::{Result, TableReference};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{col, lit, Filter, LogicalPlan};
use datafusion::prelude::*;

/// This example demonstrates how to walk (inspect) and manipulate
/// [`LogicalPlan`]s using the [`TreeNode`] API.
///
/// `LogicalPlan` implements the `TreeNode` trait, which provides a
/// uniform way to recursively walk and rewrite tree structures.
///
/// The code in this example shows how to:
/// 1. Use [`TreeNode::apply`] to inspect plans (collect table names)
/// 2. Use [`TreeNode::exists`] to check if a plan contains a specific node
/// 3. Use [`TreeNodeVisitor`] for structured inspection with state tracking
/// 4. Use [`TreeNode::transform`] to rewrite plans with a closure
/// 5. Use [`TreeNodeRewriter`] for structured rewrites with state tracking
pub async fn plan_walk() -> Result<()> {
    // Create a SessionContext and register a table for building plans
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("salary", DataType::Int32, false),
        Field::new("department", DataType::Utf8, false),
    ]);
    ctx.register_batch("employee", create_employee_batch(&schema)?)?;

    // Build a sample plan using the DataFrame API:
    //
    //   Projection(name, salary)
    //     Filter(salary > 1000 AND department = 'engineering')
    //       TableScan(employee)
    let df = ctx
        .table("employee")
        .await?
        .filter(
            col("salary")
                .gt(lit(1000))
                .and(col("department").eq(lit("engineering"))),
        )?
        .select(vec![col("name"), col("salary")])?;
    let plan = df.logical_plan().clone();
    println!("Original plan:\n{}\n", plan.display_indent());

    // Example 1: Use `apply` to collect all referenced table names
    apply_demo(&plan)?;

    // Example 2: Use `exists` to check for a specific node type
    exists_demo(&plan)?;

    // Example 3: Use `TreeNodeVisitor` for structured plan inspection
    visitor_demo(&plan)?;

    // Example 4: Use `transform` to rewrite a plan with a closure
    transform_demo(plan.clone())?;

    // Example 5: Use `TreeNodeRewriter` for structured plan rewrites
    rewriter_demo(plan)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Example 1: TreeNode::apply — Collect all referenced table names
// ---------------------------------------------------------------------------

/// Demonstrates using [`TreeNode::apply`] to walk a plan and collect
/// all table names referenced by `TableScan` nodes.
///
/// `apply` traverses the tree top-down, calling the closure on each
/// node with a borrowed reference (`&LogicalPlan`). This is useful
/// when you need to inspect a plan without modifying it.
fn apply_demo(plan: &LogicalPlan) -> Result<()> {
    println!("--- Example 1: TreeNode::apply ---");

    let mut table_names: HashSet<TableReference> = HashSet::new();

    plan.apply(|node| {
        // Check if this node is a TableScan
        if let LogicalPlan::TableScan(scan) = node {
            table_names.insert(scan.table_name.clone());
        }
        // Continue recursion to visit all nodes
        Ok(TreeNodeRecursion::Continue)
    })?;

    println!("Referenced tables: {:?}", table_names);
    assert!(table_names.contains(&TableReference::bare("employee")));
    println!();

    Ok(())
}

// ---------------------------------------------------------------------------
// Example 2: TreeNode::exists — Check for a specific node
// ---------------------------------------------------------------------------

/// Demonstrates using [`TreeNode::exists`] to check if the plan
/// contains a `Filter` node.
///
/// `exists` is a convenience method that stops recursion as soon as
/// a match is found, making it efficient for simple checks.
fn exists_demo(plan: &LogicalPlan) -> Result<()> {
    println!("--- Example 2: TreeNode::exists ---");

    let has_filter = plan.exists(|node| Ok(matches!(node, LogicalPlan::Filter(_))))?;
    println!("Plan contains a Filter: {has_filter}");
    assert!(has_filter);

    let has_join = plan.exists(|node| Ok(matches!(node, LogicalPlan::Join(_))))?;
    println!("Plan contains a Join: {has_join}");
    assert!(!has_join);

    println!();
    Ok(())
}

// ---------------------------------------------------------------------------
// Example 3: TreeNodeVisitor — Structured inspection with state
// ---------------------------------------------------------------------------

/// A visitor that collects information while walking the plan tree.
///
/// Implementing [`TreeNodeVisitor`] is useful when you need to track
/// state across the traversal (e.g., depth level, accumulated results).
///
/// The visitor's `f_down` is called before children are visited (top-down),
/// and `f_up` is called after children are visited (bottom-up).
struct PlanInspector {
    /// The depth of the current node in the tree
    depth: usize,
    /// Collected node descriptions with their depths
    node_descriptions: Vec<(usize, String)>,
}

impl PlanInspector {
    fn new() -> Self {
        Self {
            depth: 0,
            node_descriptions: Vec::new(),
        }
    }
}

impl<'n> TreeNodeVisitor<'n> for PlanInspector {
    type Node = LogicalPlan;

    /// Called before visiting children (top-down).
    /// We record the node and increase depth.
    fn f_down(&mut self, node: &'n LogicalPlan) -> Result<TreeNodeRecursion> {
        let description = match node {
            LogicalPlan::Projection(p) => {
                format!("Projection ({} expressions)", p.expr.len())
            }
            LogicalPlan::Filter(_) => "Filter".to_string(),
            LogicalPlan::TableScan(scan) => {
                format!("TableScan({})", scan.table_name)
            }
            other => format!("{}", other.display()),
        };
        self.node_descriptions
            .push((self.depth, description));
        self.depth += 1;
        Ok(TreeNodeRecursion::Continue)
    }

    /// Called after visiting children (bottom-up).
    /// We decrease depth.
    fn f_up(&mut self, _node: &'n LogicalPlan) -> Result<TreeNodeRecursion> {
        self.depth -= 1;
        Ok(TreeNodeRecursion::Continue)
    }
}

/// Demonstrates using a [`TreeNodeVisitor`] with [`TreeNode::visit`].
fn visitor_demo(plan: &LogicalPlan) -> Result<()> {
    println!("--- Example 3: TreeNodeVisitor ---");

    let mut inspector = PlanInspector::new();
    plan.visit(&mut inspector)?;

    println!("Plan structure:");
    for (depth, desc) in &inspector.node_descriptions {
        let indent = "  ".repeat(*depth);
        println!("{indent}{desc}");
    }
    assert_eq!(inspector.node_descriptions.len(), 3); // Projection, Filter, TableScan
    println!();

    Ok(())
}

// ---------------------------------------------------------------------------
// Example 4: TreeNode::transform — Rewrite plans with a closure
// ---------------------------------------------------------------------------

/// Demonstrates using [`TreeNode::transform`] to rewrite a plan.
///
/// `transform` visits nodes bottom-up and produces a new plan.
/// The closure receives owned nodes and returns possibly modified nodes.
/// Use `Transformed::yes(node)` to signal a node was changed, or
/// `Transformed::no(node)` to return it unchanged.
fn transform_demo(plan: LogicalPlan) -> Result<()> {
    println!("--- Example 4: TreeNode::transform ---");
    println!("Before: {}\n", plan.display_indent());

    // Rewrite: change "salary > 1000" to "salary > 5000" in any Filter
    let result = plan.transform(|node| {
        if let LogicalPlan::Filter(filter) = &node {
            // Build replacement predicate
            let new_predicate = col("salary")
                .gt(lit(5000))
                .and(col("department").eq(lit("engineering")));

            // Create a new Filter with the replaced predicate using
            // the Filter::try_new constructor
            let new_filter = Filter::try_new(new_predicate, filter.input.clone())
                .map_err(|e| DataFusionError::Plan(format!("Error creating filter: {e}")))?;
            return Ok(Transformed::yes(LogicalPlan::Filter(new_filter)));
        }
        Ok(Transformed::no(node))
    })?;

    println!("Plan was changed: {}", result.transformed);
    println!("After: {}\n", result.data.display_indent());
    assert!(result.transformed);

    Ok(())
}

// ---------------------------------------------------------------------------
// Example 5: TreeNodeRewriter — Structured rewrites with state
// ---------------------------------------------------------------------------

/// A rewriter that replaces column references in Filter predicates.
///
/// Implementing [`TreeNodeRewriter`] is useful when you need to track
/// state during the rewrite. Like `TreeNodeVisitor`, it has `f_down`
/// (pre-order) and `f_up` (post-order) methods.
struct SalaryThresholdRewriter {
    /// The new salary threshold to use in filters
    new_threshold: i32,
    /// Count of filters rewritten
    filters_rewritten: usize,
}

impl TreeNodeRewriter for SalaryThresholdRewriter {
    type Node = LogicalPlan;

    /// Called top-down (before children). We rewrite Filter nodes here.
    fn f_down(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        if let LogicalPlan::Filter(filter) = &node {
            // Replace any filter that references "salary" with a new threshold
            let new_predicate = col("salary").gt(lit(self.new_threshold));
            let new_filter = Filter::try_new(new_predicate, filter.input.clone())
                .map_err(|e| DataFusionError::Plan(format!("Error creating filter: {e}")))?;
            self.filters_rewritten += 1;
            return Ok(Transformed::yes(LogicalPlan::Filter(new_filter)));
        }
        Ok(Transformed::no(node))
    }
}

/// Demonstrates using a [`TreeNodeRewriter`] with [`TreeNode::rewrite`].
fn rewriter_demo(plan: LogicalPlan) -> Result<()> {
    println!("--- Example 5: TreeNodeRewriter ---");
    println!("Before: {}\n", plan.display_indent());

    let mut rewriter = SalaryThresholdRewriter {
        new_threshold: 9999,
        filters_rewritten: 0,
    };

    let result = plan.rewrite(&mut rewriter)?;

    println!("Filters rewritten: {}", rewriter.filters_rewritten);
    println!("After: {}\n", result.data.display_indent());
    assert_eq!(rewriter.filters_rewritten, 1);
    assert!(result.transformed);

    Ok(())
}

// ---------------------------------------------------------------------------
// Helper: create sample data
// ---------------------------------------------------------------------------

fn create_employee_batch(schema: &Schema) -> Result<arrow::record_batch::RecordBatch> {
    use arrow::array::{Int32Array, StringArray};
    Ok(arrow::record_batch::RecordBatch::try_new(
        std::sync::Arc::new(schema.clone()),
        vec![
            std::sync::Arc::new(Int32Array::from(vec![1, 2, 3])),
            std::sync::Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            std::sync::Arc::new(Int32Array::from(vec![2000, 500, 3000])),
            std::sync::Arc::new(StringArray::from(vec![
                "engineering",
                "sales",
                "engineering",
            ])),
        ],
    )?)
}
