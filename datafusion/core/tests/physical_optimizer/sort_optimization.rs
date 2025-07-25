// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

//! Tests for sort optimization that chooses PartialSortExec over SortExec
//! when the input has a compatible sort order prefix

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use std::sync::Arc;

use datafusion::physical_plan::sorts::partial_sort::PartialSortExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;

use datafusion_physical_plan::ExecutionPlanProperties;

#[tokio::test]
async fn test_sort_with_prefix_optimization() -> Result<()> {
    use datafusion::logical_expr::col;
    use datafusion_expr::SortExpr;

    let ctx = SessionContext::new();

    // Create a mock table with pre-sorted data on columns (a, b)
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
        Field::new("c", DataType::Int32, false),
    ]));

    // Create test data that's already sorted on (a, b)
    let data = vec![RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 1, 2, 2])),
            Arc::new(Int32Array::from(vec![1, 2, 1, 2])),
            Arc::new(Int32Array::from(vec![4, 3, 2, 1])),
        ],
    )?];

    // Create sort expressions for (a, b) ordering using logical expressions
    let sort_exprs = vec![
        SortExpr::new(col("a"), true, false), // ascending, nulls last
        SortExpr::new(col("b"), true, false), // ascending, nulls last
    ];

    // Create a table source with declared ordering on (a, b)
    let table = MemTable::try_new(Arc::clone(&schema), vec![data])?
        .with_sort_order(vec![sort_exprs]);
    ctx.register_table("test_table", Arc::new(table))?;

    // Query that requests sorting on (a, b, c) - should use PartialSortExec
    let df = ctx.sql("SELECT * FROM test_table ORDER BY a, b, c").await?;
    let physical_plan = df.create_physical_plan().await?;

    // DEBUG: Check if this fixes the ordering
    println!("Top-level plan type: {}", physical_plan.name());
    for (i, child) in physical_plan.children().iter().enumerate() {
        println!("Child {} ordering: {:?}", i, child.output_ordering());
    }

    // Now the assertions should pass
    assert!(
        contains_partial_sort(&physical_plan),
        "Expected PartialSortExec to be used for prefix-compatible sort"
    );
    assert!(
        !contains_full_sort(&physical_plan),
        "Expected SortExec NOT to be used when PartialSortExec is applicable"
    );

    Ok(())
}

#[tokio::test]
async fn test_sort_without_prefix_uses_full_sort() -> Result<()> {
    let ctx = SessionContext::new();

    // Create the same table as above
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
        Field::new("c", DataType::Int32, false),
    ]));

    let data = vec![RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 1, 2, 2])),
            Arc::new(Int32Array::from(vec![1, 2, 1, 2])),
            Arc::new(Int32Array::from(vec![4, 3, 2, 1])),
        ],
    )?];

    let table = MemTable::try_new(Arc::clone(&schema), vec![data])?;
    ctx.register_table("test_table", Arc::new(table))?;

    // Query that requests sorting on incompatible columns - should use SortExec
    let df = ctx.sql("SELECT * FROM test_table ORDER BY c, a, b").await?;
    let physical_plan = df.create_physical_plan().await?;

    // Verify that SortExec is used (no optimization possible)
    assert!(
        contains_full_sort(&physical_plan),
        "Expected SortExec to be used for non-prefix-compatible sort"
    );
    assert!(
        !contains_partial_sort(&physical_plan),
        "Expected PartialSortExec NOT to be used when sort order is incompatible"
    );

    Ok(())
}

#[tokio::test]
async fn test_partial_sort_correctness() -> Result<()> {
    let ctx = SessionContext::new();

    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
        Field::new("c", DataType::Int32, false),
    ]));

    // Create test data that's already sorted on (a, b) but not on c
    let data = vec![RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 1, 1, 2, 2, 2])),
            Arc::new(Int32Array::from(vec![1, 1, 2, 1, 1, 2])),
            Arc::new(Int32Array::from(vec![9, 1, 5, 8, 2, 6])),
        ],
    )?];

    let table = MemTable::try_new(Arc::clone(&schema), vec![data])?;
    ctx.register_table("test_table", Arc::new(table))?;

    // Execute the query and verify results are correctly sorted
    let df = ctx.sql("SELECT * FROM test_table ORDER BY a, b, c").await?;
    let result = df.collect().await?;

    // Verify the result is properly sorted on all three columns
    let expected = vec![RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 1, 1, 2, 2, 2])),
            Arc::new(Int32Array::from(vec![1, 1, 2, 1, 1, 2])),
            Arc::new(Int32Array::from(vec![1, 9, 5, 2, 8, 6])),
        ],
    )?];

    assert_eq!(
        result, expected,
        "PartialSortExec should produce correctly sorted results"
    );

    Ok(())
}

// Helper functions to check for specific execution plan types
fn contains_partial_sort(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if plan.as_any().downcast_ref::<PartialSortExec>().is_some() {
        return true;
    }
    plan.children()
        .iter()
        .any(|child| contains_partial_sort(child))
}

fn contains_full_sort(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if plan.as_any().downcast_ref::<SortExec>().is_some() {
        return true;
    }
    plan.children()
        .iter()
        .any(|child| contains_full_sort(child))
}
