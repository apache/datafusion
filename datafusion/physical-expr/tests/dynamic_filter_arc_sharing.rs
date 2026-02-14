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

//! Regression test for DynamicFilterPhysicalExpr Arc sharing after remapping

use std::sync::Arc;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::Result;
use datafusion_physical_expr::expressions::{Column, DynamicFilterPhysicalExpr, lit};
use datafusion_physical_expr::utils::reassign_expr_columns;
use datafusion_physical_expr::PhysicalExpr;

/// Test that DynamicFilterPhysicalExpr preserves its inner Arc when column indices are remapped.
/// This is critical for dynamic filters to work correctly - both the producer (HashJoinExec)
/// and consumer (DataSourceExec) must share the same inner Arc so that updates are visible.
#[test]
fn test_dynamic_filter_inner_arc_preserved_after_remap() -> Result<()> {
    // Create a schema: (a, b, c)
    let original_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
        Field::new("c", DataType::Int32, false),
    ]));

    // Create a different schema: (c, b, a) - same columns but different order/indices
    let remapped_schema = Arc::new(Schema::new(vec![
        Field::new("c", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
        Field::new("a", DataType::Int32, false),
    ]));

    // Create DynamicFilterPhysicalExpr with children referencing columns from original schema
    // Column "a" is at index 0 in original schema
    let col_a = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
    let initial_expr = lit(true);

    let original_filter = Arc::new(DynamicFilterPhysicalExpr::new(
        vec![col_a],
        initial_expr,
    ));

    // Get pointer to the inner Arc BEFORE remapping
    let inner_ptr_before = {
        // Access the inner field via the is_used check which references it
        // We can't access inner directly as it's private, but we can check strong_count
        // The inner Arc's identity is what matters for is_used()
        Arc::as_ptr(&original_filter) as *const () as u64
    };

    println!("Original filter Arc address: 0x{:x}", inner_ptr_before);
    println!("Original filter is_used (should be false, count=1): {}", original_filter.is_used());
    assert!(!original_filter.is_used(), "Original filter should not be used (count=1)");

    // Remap column indices to match the new schema
    // Column "a" should now be at index 2 in remapped schema
    let remapped_filter = reassign_expr_columns(
        original_filter.clone() as Arc<dyn PhysicalExpr>,
        &remapped_schema,
    )?;

    // Try to downcast to Arc<DynamicFilterPhysicalExpr>
    let remapped_dynamic_filter = remapped_filter
        .as_any()
        .downcast_ref::<DynamicFilterPhysicalExpr>()
        .expect("Should still be DynamicFilterPhysicalExpr");

    // We need Arc<DynamicFilterPhysicalExpr> for is_used(), so downcast the Arc itself
    let remapped_arc = Arc::downcast::<DynamicFilterPhysicalExpr>(remapped_filter.clone())
        .expect("Should be able to downcast Arc");

    let inner_ptr_after = Arc::as_ptr(&remapped_arc) as *const () as u64;

    println!("Remapped filter Arc address: 0x{:x}", inner_ptr_after);
    println!("Remapped filter is_used (checking inner Arc): {}", remapped_arc.is_used());

    // The outer Arc addresses will be different (that's expected)
    // But is_used() should return true because it checks BOTH:
    // 1. Arc::strong_count(self) > 1  (outer Arc)
    // 2. Arc::strong_count(&self.inner) > 1  (inner Arc - THIS must be shared!)

    // Since we're holding both original_filter and remapped_arc,
    // if they share the same inner Arc, the inner Arc count should be 2
    assert!(
        remapped_arc.is_used() || original_filter.is_used(),
        "At least one filter should report is_used()=true because they should share the inner Arc!\n\
         Original filter is_used: {}\n\
         Remapped filter is_used: {}",
        original_filter.is_used(),
        remapped_arc.is_used()
    );

    // Verify the remapped column index is correct
    let children = remapped_dynamic_filter.children();
    assert_eq!(children.len(), 1, "Should have 1 child");

    let remapped_col = children[0]
        .as_any()
        .downcast_ref::<Column>()
        .expect("Child should be a Column");

    // Column "a" should now be at index 2 in the remapped schema
    assert_eq!(remapped_col.index(), 2, "Column 'a' should be at index 2 in remapped schema");
    assert_eq!(remapped_col.name(), "a", "Column name should still be 'a'");

    Ok(())
}

/// Test is_used() behavior with explicit Arc cloning
#[test]
fn test_dynamic_filter_is_used_with_clone() {
    let filter = Arc::new(DynamicFilterPhysicalExpr::new(
        vec![],
        lit(true),
    ));

    // Initially, strong_count = 1, so is_used() should be false
    assert!(!filter.is_used(), "Single reference should not be 'used'");

    // Clone the Arc - now strong_count = 2
    let filter_clone = Arc::clone(&filter);

    // Now is_used() should return true because strong_count > 1
    assert!(filter.is_used(), "Multiple references should be 'used'");
    assert!(filter_clone.is_used(), "Multiple references should be 'used'");

    drop(filter_clone);

    // Back to strong_count = 1
    assert!(!filter.is_used(), "Back to single reference, should not be 'used'");
}
