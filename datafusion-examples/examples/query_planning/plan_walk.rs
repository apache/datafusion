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

//! Walk and rewrite logical plans, including subqueries embedded in expressions.
//! Run with `cargo run --example query_planning -- plan_walk`.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::Result;
use datafusion::common::tree_node::{
    Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
};
use datafusion::logical_expr::logical_plan::builder::table_scan_with_filters;
use datafusion::logical_expr::{Expr, LogicalPlan, col, in_subquery, lit, table_scan};

pub fn plan_walk() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("salary", DataType::Int32, false),
    ]);

    // table_scan returns a LogicalPlanBuilder. Build a tree of operators:
    // Projection(name, salary)
    //   Filter(salary > 1000)
    //     TableScan(employee)
    // Only a schema is needed to inspect these plans; no data is read.
    let plan = table_scan(Some("employee"), &schema, None)?
        .filter(col("salary").gt(lit(1000)))?
        .project(vec![col("name"), col("salary")])?
        .build()?;
    println!("Original plan:\n{}", plan.display_indent());

    // apply borrows the plan and visits each node before visiting its inputs.
    // Continue visits the inputs, Jump skips them, and Stop ends the walk.
    let mut tables = BTreeSet::new();
    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            tables.insert(scan.table_name.to_string());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    assert_eq!(tables, BTreeSet::from(["employee".to_string()]));
    println!("Referenced tables: {tables:?}");

    // rewrite consumes the plan. Clone it here to keep the original for comparison.
    // This illustrative rewrite changes query semantics by removing filters;
    // it is not an optimizer rule that preserves query results.
    let mut rewriter = RemoveFilters::default();
    let rewritten = plan.clone().rewrite(&mut rewriter)?;
    assert!(rewritten.transformed);
    assert_eq!(rewriter.removed, 1);
    assert_eq!(rewritten.data.schema(), plan.schema());
    assert_eq!(
        rewritten.data.display_indent().to_string(),
        "Projection: employee.name, employee.salary\n  TableScan: employee"
    );
    println!("Without Filter nodes:\n{}", rewritten.data.display_indent());

    // A subquery in an expression is not an ordinary plan input, so apply
    // and rewrite do not traverse it. Use their *_with_subqueries variants.
    // Include a scan filter to illustrate predicates that have been pushed down
    // into a table source. Here it is set explicitly rather than by an optimizer.
    let scan_predicate = col("eligible.salary").gt(lit(1000));
    let subquery = table_scan_with_filters(
        Some("eligible"),
        &schema,
        None,
        vec![scan_predicate.clone()],
    )?
    .filter(col("salary").lt(lit(2000)))?
    .project(vec![col("salary")])?
    .build()?;
    let plan = table_scan(Some("employee"), &schema, None)?
        .filter(col("salary").gt(lit(0)))?
        .project(vec![
            col("name"),
            in_subquery(col("salary"), Arc::new(subquery)).alias("is_eligible"),
        ])?
        .build()?;

    tables.clear();
    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            tables.insert(scan.table_name.to_string());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    assert_eq!(tables, BTreeSet::from(["employee".to_string()]));

    let summary = summarize(&plan)?;
    assert_eq!(
        summary.tables,
        BTreeSet::from(["eligible".to_string(), "employee".to_string()])
    );
    // The outer Filter, the subquery's Filter, and the scan's pushed-down filter.
    assert_eq!(summary.filters.len(), 3);
    assert!(summary.filters.contains(&scan_predicate));
    println!("Tables including subqueries: {:?}", summary.tables);
    for predicate in &summary.filters {
        println!("Filter predicate: {predicate}");
    }

    let mut rewriter = RemoveFilters::default();
    let rewritten = plan.rewrite_with_subqueries(&mut rewriter)?;
    assert!(rewritten.transformed);
    assert_eq!(rewriter.removed, 2);
    // Both Filter nodes were removed, but the subquery in the Projection and
    // its pushed-down scan filter remain. Rewriting Filter nodes does not
    // modify TableScan.filters.
    let summary = summarize(&rewritten.data)?;
    assert_eq!(
        summary.tables,
        BTreeSet::from(["eligible".to_string(), "employee".to_string()])
    );
    assert_eq!(summary.filters, vec![scan_predicate]);

    Ok(())
}

#[derive(Debug, Default)]
struct PlanSummary {
    tables: BTreeSet<String>,
    filters: Vec<Expr>,
}

fn summarize(plan: &LogicalPlan) -> Result<PlanSummary> {
    let mut summary = PlanSummary::default();
    plan.apply_with_subqueries(|node| {
        match node {
            LogicalPlan::TableScan(scan) => {
                summary.tables.insert(scan.table_name.to_string());
                summary.filters.extend(scan.filters.iter().cloned());
            }
            LogicalPlan::Filter(filter) => {
                summary.filters.push(filter.predicate.clone());
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    // Filter pushdown support determines whether predicates appear in Filter
    // nodes, TableScan.filters, or both. This list may therefore have duplicates.
    // Other predicates, such as join conditions, are not collected here.
    Ok(summary)
}

// A rewriter can keep state across nodes, such as the number of removed filters.
#[derive(Default)]
struct RemoveFilters {
    removed: usize,
}

impl TreeNodeRewriter for RemoveFilters {
    type Node = LogicalPlan;

    // f_up runs after the inputs (and, with rewrite_with_subqueries, embedded
    // subqueries) have been rewritten. Returning the rewritten input also works
    // when several Filter nodes are nested.
    // For a stateless bottom-up rewrite, transform_up accepts a closure instead.
    fn f_up(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        match node {
            LogicalPlan::Filter(filter) => {
                self.removed += 1;
                Ok(Transformed::yes(filter.input.as_ref().clone()))
            }
            _ => Ok(Transformed::no(node)),
        }
    }
}
