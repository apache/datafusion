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

//! Tests for sort pushdown optimizer rule (Phase 1)
//!
//! Phase 1 tests verify that:
//! 1. Reverse scan is enabled (reverse_row_groups=true)
//! 2. SortExec is kept (because ordering is inexact)
//! 3. output_ordering remains unchanged
//! 4. Early termination is enabled for TopK queries
//! 5. Prefix matching works correctly

use datafusion_physical_expr::expressions;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_optimizer::pushdown_sort::PushdownSort;
use std::sync::Arc;

use crate::physical_optimizer::test_utils::{
    OptimizationTest, coalesce_partitions_exec, parquet_exec, parquet_exec_with_sort,
    projection_exec, projection_exec_with_alias, repartition_exec, schema,
    simple_projection_exec, sort_exec, sort_exec_with_fetch, sort_expr, sort_expr_named,
    test_scan_with_ordering,
};

#[test]
fn test_sort_pushdown_disabled() {
    // When pushdown is disabled, plan should remain unchanged
    let schema = schema();
    let source = parquet_exec(schema.clone());
    let sort_exprs = LexOrdering::new(vec![sort_expr("a", &schema)]).unwrap();
    let plan = sort_exec(sort_exprs, source);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), false),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
    "
    );
}

#[test]
fn test_sort_pushdown_basic_phase1() {
    // Phase 1: Reverse scan enabled, Sort kept, output_ordering unchanged
    let schema = schema();

    // Source has ASC NULLS LAST ordering (default)
    let a = sort_expr("a", &schema);
    let source_ordering = LexOrdering::new(vec![a.clone()]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // Request DESC NULLS LAST ordering (exact reverse)
    let desc_ordering = LexOrdering::new(vec![a.reverse()]).unwrap();
    let plan = sort_exec(desc_ordering, source);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_row_groups=true
    "
    );
}

#[test]
fn test_sort_with_limit_phase1() {
    // Phase 1: Sort with fetch enables early termination but keeps Sort
    let schema = schema();

    // Source has ASC ordering
    let a = sort_expr("a", &schema);
    let source_ordering = LexOrdering::new(vec![a.clone()]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // Request DESC ordering with limit
    let desc_ordering = LexOrdering::new(vec![a.reverse()]).unwrap();
    let plan = sort_exec_with_fetch(desc_ordering, Some(10), source);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: TopK(fetch=10), expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - SortExec: TopK(fetch=10), expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_row_groups=true
    "
    );
}

#[test]
fn test_sort_multiple_columns_phase1() {
    // Phase 1: Sort on multiple columns - reverse multi-column ordering
    let schema = schema();

    // Source has [a DESC NULLS LAST, b ASC] ordering
    let a = sort_expr("a", &schema);
    let b = sort_expr("b", &schema);
    let source_ordering = LexOrdering::new(vec![a.clone().reverse(), b.clone()]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // Request [a ASC NULLS FIRST, b DESC] ordering (exact reverse)
    let reverse_ordering =
        LexOrdering::new(vec![a.clone().asc().nulls_first(), b.reverse()]).unwrap();
    let plan = sort_exec(reverse_ordering, source);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 ASC, b@1 DESC NULLS LAST], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 DESC NULLS LAST, b@1 ASC], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[a@0 ASC, b@1 DESC NULLS LAST], preserve_partitioning=[false]
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_row_groups=true
    "
    );
}

// ============================================================================
// PREFIX MATCHING TESTS
// ============================================================================

#[test]
fn test_prefix_match_single_column() {
    // Test prefix matching: source has [a DESC, b ASC], query needs [a ASC]
    // After reverse: [a ASC, b DESC] which satisfies [a ASC] prefix
    let schema = schema();

    // Source has [a DESC NULLS LAST, b ASC NULLS LAST] ordering
    let a = sort_expr("a", &schema);
    let b = sort_expr("b", &schema);
    let source_ordering = LexOrdering::new(vec![a.clone().reverse(), b]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // Request only [a ASC NULLS FIRST] - a prefix of the reversed ordering
    let prefix_ordering = LexOrdering::new(vec![a.clone().asc().nulls_first()]).unwrap();
    let plan = sort_exec(prefix_ordering, source);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 DESC NULLS LAST, b@1 ASC], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_row_groups=true
    "
    );
}

#[test]
fn test_prefix_match_with_limit() {
    // Test prefix matching with LIMIT - important for TopK optimization
    let schema = schema();

    // Source has [a ASC, b DESC, c ASC] ordering
    let a = sort_expr("a", &schema);
    let b = sort_expr("b", &schema);
    let c = sort_expr("c", &schema);
    let source_ordering =
        LexOrdering::new(vec![a.clone(), b.clone().reverse(), c]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // Request [a DESC NULLS LAST, b ASC NULLS FIRST] with LIMIT 100
    // This is a prefix (2 columns) of the reversed 3-column ordering
    let prefix_ordering =
        LexOrdering::new(vec![a.reverse(), b.clone().asc().nulls_first()]).unwrap();
    let plan = sort_exec_with_fetch(prefix_ordering, Some(100), source);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: TopK(fetch=100), expr=[a@0 DESC NULLS LAST, b@1 ASC], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC, b@1 DESC NULLS LAST, c@2 ASC], file_type=parquet
      output:
        Ok:
          - SortExec: TopK(fetch=100), expr=[a@0 DESC NULLS LAST, b@1 ASC], preserve_partitioning=[false]
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_row_groups=true
    "
    );
}

#[test]
fn test_prefix_match_through_transparent_nodes() {
    // Test prefix matching works through transparent nodes
    let schema = schema();

    // Source has [a DESC NULLS LAST, b ASC, c DESC] ordering
    let a = sort_expr("a", &schema);
    let b = sort_expr("b", &schema);
    let c = sort_expr("c", &schema);
    let source_ordering =
        LexOrdering::new(vec![a.clone().reverse(), b, c.reverse()]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);
    let repartition = repartition_exec(source);

    // Request only [a ASC NULLS FIRST] - prefix of reversed ordering
    let prefix_ordering = LexOrdering::new(vec![a.clone().asc().nulls_first()]).unwrap();
    let plan = sort_exec(prefix_ordering, repartition);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
        -   RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, maintains_sort_order=true
        -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 DESC NULLS LAST, b@1 ASC, c@2 DESC NULLS LAST], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
          -   RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
          -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_row_groups=true
    "
    );
}

#[test]
fn test_no_prefix_match_wrong_direction() {
    // Test that prefix matching does NOT work if the direction is wrong
    let schema = schema();

    // Source has [a DESC, b ASC] ordering
    let a = sort_expr("a", &schema);
    let b = sort_expr("b", &schema);
    let source_ordering = LexOrdering::new(vec![a.clone().reverse(), b]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // Request [a DESC] - same direction as source, NOT a reverse prefix
    let same_direction = LexOrdering::new(vec![a.clone().reverse()]).unwrap();
    let plan = sort_exec(same_direction, source);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 DESC NULLS LAST, b@1 ASC], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 DESC NULLS LAST, b@1 ASC], file_type=parquet
    "
    );
}

#[test]
fn test_no_prefix_match_longer_than_source() {
    // Test that prefix matching does NOT work if requested is longer than source
    let schema = schema();

    // Source has [a DESC] ordering (single column)
    let a = sort_expr("a", &schema);
    let b = sort_expr("b", &schema);
    let source_ordering = LexOrdering::new(vec![a.clone().reverse()]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // Request [a ASC, b DESC] - longer than source, can't be a prefix
    let longer_ordering =
        LexOrdering::new(vec![a.clone().asc().nulls_first(), b.reverse()]).unwrap();
    let plan = sort_exec(longer_ordering, source);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 ASC, b@1 DESC NULLS LAST], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 DESC NULLS LAST], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[a@0 ASC, b@1 DESC NULLS LAST], preserve_partitioning=[false]
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 DESC NULLS LAST], file_type=parquet
    "
    );
}

// ============================================================================
// ORIGINAL TESTS
// ============================================================================

#[test]
fn test_sort_through_repartition() {
    // Sort should push through RepartitionExec
    let schema = schema();
    let a = sort_expr("a", &schema);
    let source_ordering = LexOrdering::new(vec![a.clone()]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);
    let repartition = repartition_exec(source);

    let desc_ordering = LexOrdering::new(vec![a.reverse()]).unwrap();
    let plan = sort_exec(desc_ordering, repartition);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
        -   RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, maintains_sort_order=true
        -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
          -   RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
          -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_row_groups=true
    "
    );
}

#[test]
fn test_nested_sorts() {
    // Nested sort operations - only innermost can be optimized
    let schema = schema();
    let a = sort_expr("a", &schema);
    let b = sort_expr("b", &schema);
    let source_ordering = LexOrdering::new(vec![a.clone()]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    let desc_ordering = LexOrdering::new(vec![a.reverse()]).unwrap();
    let inner_sort = sort_exec(desc_ordering, source);

    let sort_exprs2 = LexOrdering::new(vec![b]).unwrap();
    let plan = sort_exec(sort_exprs2, inner_sort);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[b@1 ASC], preserve_partitioning=[false]
        -   SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
        -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[b@1 ASC], preserve_partitioning=[false]
          -   SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
          -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_row_groups=true
    "
    );
}

#[test]
fn test_non_sort_plans_unchanged() {
    // Plans without SortExec should pass through unchanged
    let schema = schema();
    let plan = parquet_exec(schema.clone());

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      output:
        Ok:
          - DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
    "
    );
}

#[test]
fn test_optimizer_properties() {
    // Test optimizer metadata
    let optimizer = PushdownSort::new();

    assert_eq!(optimizer.name(), "PushdownSort");
    assert!(optimizer.schema_check());
}

#[test]
fn test_sort_through_coalesce_partitions() {
    // Sort should push through CoalescePartitionsExec
    let schema = schema();
    let a = sort_expr("a", &schema);
    let source_ordering = LexOrdering::new(vec![a.clone()]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);
    let repartition = repartition_exec(source);
    let coalesce_parts = coalesce_partitions_exec(repartition);

    let desc_ordering = LexOrdering::new(vec![a.reverse()]).unwrap();
    let plan = sort_exec(desc_ordering, coalesce_parts);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
        -   CoalescePartitionsExec
        -     RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, maintains_sort_order=true
        -       DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
          -   CoalescePartitionsExec
          -     RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
          -       DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_row_groups=true
    "
    );
}

#[test]
fn test_complex_plan_with_multiple_operators() {
    // Test a complex plan with multiple operators between sort and source
    let schema = schema();
    let a = sort_expr("a", &schema);
    let source_ordering = LexOrdering::new(vec![a.clone()]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);
    let repartition = repartition_exec(source);
    let coalesce_parts = coalesce_partitions_exec(repartition);

    let desc_ordering = LexOrdering::new(vec![a.reverse()]).unwrap();
    let plan = sort_exec(desc_ordering, coalesce_parts);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
        -   CoalescePartitionsExec
        -     RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, maintains_sort_order=true
        -       DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
          -   CoalescePartitionsExec
          -     RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
          -       DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_row_groups=true
    "
    );
}

#[test]
fn test_multiple_sorts_different_columns() {
    // Test nested sorts on different columns - only innermost can optimize
    let schema = schema();
    let a = sort_expr("a", &schema);
    let c = sort_expr("c", &schema);
    let source_ordering = LexOrdering::new(vec![a.clone()]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // First sort by column 'a' DESC (reverse of source)
    let desc_ordering = LexOrdering::new(vec![a.reverse()]).unwrap();
    let sort1 = sort_exec(desc_ordering, source);

    // Then sort by column 'c' (different column, can't optimize)
    let sort_exprs2 = LexOrdering::new(vec![c]).unwrap();
    let plan = sort_exec(sort_exprs2, sort1);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[c@2 ASC], preserve_partitioning=[false]
        -   SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
        -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[c@2 ASC], preserve_partitioning=[false]
          -   SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
          -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_row_groups=true
    "
    );
}

#[test]
fn test_no_pushdown_for_unordered_source() {
    // Verify pushdown does NOT happen for sources without ordering
    let schema = schema();
    let source = parquet_exec(schema.clone()); // No output_ordering
    let sort_exprs = LexOrdering::new(vec![sort_expr("a", &schema)]).unwrap();
    let plan = sort_exec(sort_exprs, source);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
    "
    );
}

#[test]
fn test_no_pushdown_for_non_reverse_sort() {
    // Verify pushdown does NOT happen when sort doesn't reverse source ordering
    let schema = schema();

    // Source sorted by 'a' ASC
    let a = sort_expr("a", &schema);
    let b = sort_expr("b", &schema);
    let source_ordering = LexOrdering::new(vec![a]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // Request sort by 'b' (different column)
    let sort_exprs = LexOrdering::new(vec![b]).unwrap();
    let plan = sort_exec(sort_exprs, source);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[b@1 ASC], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[b@1 ASC], preserve_partitioning=[false]
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
    "
    );
}

#[test]
fn test_pushdown_through_blocking_node() {
    // Test that pushdown works for inner sort even when outer sort is blocked
    // Structure: Sort -> Aggregate (blocks pushdown) -> Sort -> Scan
    // The outer sort can't push through aggregate, but the inner sort should still optimize
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_plan::aggregates::{
        AggregateExec, AggregateMode, PhysicalGroupBy,
    };
    use std::sync::Arc;

    let schema = schema();

    // Bottom: DataSource with [a ASC NULLS LAST] ordering
    let a = sort_expr("a", &schema);
    let source_ordering = LexOrdering::new(vec![a.clone()]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // Inner Sort: [a DESC NULLS FIRST] - exact reverse, CAN push down to source
    let inner_sort_ordering = LexOrdering::new(vec![a.clone().reverse()]).unwrap();
    let inner_sort = sort_exec(inner_sort_ordering, source);

    // Middle: Aggregate (blocks pushdown from outer sort)
    // GROUP BY a, COUNT(b)
    let group_by = PhysicalGroupBy::new_single(vec![(
        Arc::new(expressions::Column::new("a", 0)) as _,
        "a".to_string(),
    )]);

    let count_expr = Arc::new(
        AggregateExprBuilder::new(
            count_udaf(),
            vec![Arc::new(expressions::Column::new("b", 1)) as _],
        )
        .schema(Arc::clone(&schema))
        .alias("COUNT(b)")
        .build()
        .unwrap(),
    );

    let aggregate = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Final,
            group_by,
            vec![count_expr],
            vec![None],
            inner_sort,
            Arc::clone(&schema),
        )
        .unwrap(),
    );

    // Outer Sort: [a ASC] - this CANNOT push down through aggregate
    let outer_sort_ordering = LexOrdering::new(vec![a.clone()]).unwrap();
    let plan = sort_exec(outer_sort_ordering, aggregate);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
        -   AggregateExec: mode=Final, gby=[a@0 as a], aggr=[COUNT(b)], ordering_mode=Sorted
        -     SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
        -       DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
          -   AggregateExec: mode=Final, gby=[a@0 as a], aggr=[COUNT(b)], ordering_mode=Sorted
          -     SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
          -       DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_row_groups=true
    "
    );
}

// ============================================================================
// PROJECTION TESTS
// ============================================================================

#[test]
fn test_sort_pushdown_through_simple_projection() {
    // Sort pushes through projection with simple column references
    let schema = schema();

    // Source has [a ASC] ordering
    let a = sort_expr("a", &schema);
    let source_ordering = LexOrdering::new(vec![a.clone()]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // Projection: SELECT a, b (simple column references)
    let projection = simple_projection_exec(source, vec![0, 1]); // columns a, b

    // Request [a DESC] - should push through projection to source
    let desc_ordering = LexOrdering::new(vec![a.reverse()]).unwrap();
    let plan = sort_exec(desc_ordering, projection);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
        -   ProjectionExec: expr=[a@0 as a, b@1 as b]
        -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
          -   ProjectionExec: expr=[a@0 as a, b@1 as b]
          -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_row_groups=true
    "
    );
}

#[test]
fn test_sort_pushdown_through_projection_with_alias() {
    // Sort pushes through projection with column aliases
    let schema = schema();

    // Source has [a ASC] ordering
    let a = sort_expr("a", &schema);
    let source_ordering = LexOrdering::new(vec![a.clone()]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // Projection: SELECT a AS id, b AS value
    let projection = projection_exec_with_alias(source, vec![(0, "id"), (1, "value")]);

    // Request [id DESC] - should map to [a DESC] and push down
    let id_expr = sort_expr_named("id", 0);
    let desc_ordering = LexOrdering::new(vec![id_expr.reverse()]).unwrap();
    let plan = sort_exec(desc_ordering, projection);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[id@0 DESC NULLS LAST], preserve_partitioning=[false]
        -   ProjectionExec: expr=[a@0 as id, b@1 as value]
        -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[id@0 DESC NULLS LAST], preserve_partitioning=[false]
          -   ProjectionExec: expr=[a@0 as id, b@1 as value]
          -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_row_groups=true
    "
    );
}

#[test]
fn test_no_sort_pushdown_through_computed_projection() {
    use datafusion_expr::Operator;

    // Sort should NOT push through projection with computed columns
    let schema = schema();

    // Source has [a ASC] ordering
    let a = sort_expr("a", &schema);
    let source_ordering = LexOrdering::new(vec![a.clone()]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // Projection: SELECT a+b as sum, c
    let projection = projection_exec(
        vec![
            (
                Arc::new(expressions::BinaryExpr::new(
                    Arc::new(expressions::Column::new("a", 0)),
                    Operator::Plus,
                    Arc::new(expressions::Column::new("b", 1)),
                )) as Arc<dyn PhysicalExpr>,
                "sum".to_string(),
            ),
            (
                Arc::new(expressions::Column::new("c", 2)) as Arc<dyn PhysicalExpr>,
                "c".to_string(),
            ),
        ],
        source,
    )
    .unwrap();

    // Request [sum DESC] - should NOT push down (sum is computed)
    let sum_expr = sort_expr_named("sum", 0);
    let desc_ordering = LexOrdering::new(vec![sum_expr.reverse()]).unwrap();
    let plan = sort_exec(desc_ordering, projection);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[sum@0 DESC NULLS LAST], preserve_partitioning=[false]
        -   ProjectionExec: expr=[a@0 + b@1 as sum, c@2 as c]
        -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[sum@0 DESC NULLS LAST], preserve_partitioning=[false]
          -   ProjectionExec: expr=[a@0 + b@1 as sum, c@2 as c]
          -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
    "
    );
}

#[test]
fn test_sort_pushdown_projection_reordered_columns() {
    // Sort pushes through projection that reorders columns
    let schema = schema();

    // Source has [a ASC] ordering
    let a = sort_expr("a", &schema);
    let source_ordering = LexOrdering::new(vec![a.clone()]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // Projection: SELECT c, b, a (columns reordered)
    let projection = simple_projection_exec(source, vec![2, 1, 0]); // c, b, a

    // Request [a DESC] where a is now at index 2 in projection output
    let a_expr_at_2 = sort_expr_named("a", 2);
    let desc_ordering = LexOrdering::new(vec![a_expr_at_2.reverse()]).unwrap();
    let plan = sort_exec(desc_ordering, projection);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[a@2 DESC NULLS LAST], preserve_partitioning=[false]
        -   ProjectionExec: expr=[c@2 as c, b@1 as b, a@0 as a]
        -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[a@2 DESC NULLS LAST], preserve_partitioning=[false]
          -   ProjectionExec: expr=[c@2 as c, b@1 as b, a@0 as a]
          -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_row_groups=true
    "
    );
}

#[test]
fn test_sort_pushdown_projection_with_limit() {
    // Sort with LIMIT pushes through simple projection
    let schema = schema();

    // Source has [a ASC] ordering
    let a = sort_expr("a", &schema);
    let source_ordering = LexOrdering::new(vec![a.clone()]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // Projection: SELECT a, b
    let projection = simple_projection_exec(source, vec![0, 1]);

    // Request [a DESC] with LIMIT 10
    let desc_ordering = LexOrdering::new(vec![a.reverse()]).unwrap();
    let plan = sort_exec_with_fetch(desc_ordering, Some(10), projection);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: TopK(fetch=10), expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
        -   ProjectionExec: expr=[a@0 as a, b@1 as b]
        -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - SortExec: TopK(fetch=10), expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
          -   ProjectionExec: expr=[a@0 as a, b@1 as b]
          -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_row_groups=true
    "
    );
}

#[test]
fn test_sort_pushdown_through_projection() {
    // Sort pushes through both projection and coalesce batches
    let schema = schema();

    // Source has [a ASC] ordering
    let a = sort_expr("a", &schema);
    let source_ordering = LexOrdering::new(vec![a.clone()]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // Projection: SELECT a, b
    let projection = simple_projection_exec(source, vec![0, 1]);

    // Request [a DESC]
    let desc_ordering = LexOrdering::new(vec![a.reverse()]).unwrap();
    let plan = sort_exec(desc_ordering, projection);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
        -   ProjectionExec: expr=[a@0 as a, b@1 as b]
        -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
          -   ProjectionExec: expr=[a@0 as a, b@1 as b]
          -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_row_groups=true
    "
    );
}

#[test]
fn test_sort_pushdown_projection_subset_of_columns() {
    // Sort pushes through projection that selects subset of columns
    let schema = schema();

    // Source has [a ASC, b ASC] ordering
    let a = sort_expr("a", &schema);
    let b = sort_expr("b", &schema);
    let source_ordering = LexOrdering::new(vec![a.clone(), b.clone()]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // Projection: SELECT a (subset of columns)
    let projection = simple_projection_exec(source, vec![0]);

    // Request [a DESC]
    let desc_ordering = LexOrdering::new(vec![a.reverse()]).unwrap();
    let plan = sort_exec(desc_ordering, projection);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
        -   ProjectionExec: expr=[a@0 as a]
        -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC, b@1 ASC], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
          -   ProjectionExec: expr=[a@0 as a]
          -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_row_groups=true
    "
    );
}

// ============================================================================
// TESTSCAN DEMONSTRATION TESTS
// ============================================================================
// These tests use TestScan to demonstrate how sort pushdown works more clearly
// than ParquetExec. TestScan can accept ANY ordering (not just reverse) and
// displays the requested ordering explicitly in the output.

#[test]
fn test_sort_pushdown_with_test_scan_basic() {
    // Demonstrates TestScan showing requested ordering clearly
    let schema = schema();

    // Source has [a ASC] ordering
    let a = sort_expr("a", &schema);
    let source_ordering = LexOrdering::new(vec![a.clone()]).unwrap();
    let source = test_scan_with_ordering(schema.clone(), source_ordering);

    // Request [a DESC] ordering
    let desc_ordering = LexOrdering::new(vec![a.reverse()]).unwrap();
    let plan = sort_exec(desc_ordering, source);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
        -   TestScan: output_ordering=[a@0 ASC]
      output:
        Ok:
          - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
          -   TestScan: output_ordering=[a@0 ASC], requested_ordering=[a@0 DESC NULLS LAST]
    "
    );
}

#[test]
fn test_sort_pushdown_with_test_scan_multi_column() {
    // Demonstrates TestScan with multi-column ordering
    let schema = schema();

    // Source has [a ASC, b DESC] ordering
    let a = sort_expr("a", &schema);
    let b = sort_expr("b", &schema);
    let source_ordering = LexOrdering::new(vec![a.clone(), b.clone().reverse()]).unwrap();
    let source = test_scan_with_ordering(schema.clone(), source_ordering);

    // Request [a DESC, b ASC] ordering (reverse of source)
    let reverse_ordering = LexOrdering::new(vec![a.reverse(), b]).unwrap();
    let plan = sort_exec(reverse_ordering, source);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 DESC NULLS LAST, b@1 ASC], preserve_partitioning=[false]
        -   TestScan: output_ordering=[a@0 ASC, b@1 DESC NULLS LAST]
      output:
        Ok:
          - SortExec: expr=[a@0 DESC NULLS LAST, b@1 ASC], preserve_partitioning=[false]
          -   TestScan: output_ordering=[a@0 ASC, b@1 DESC NULLS LAST], requested_ordering=[a@0 DESC NULLS LAST, b@1 ASC]
    "
    );
}

#[test]
fn test_sort_pushdown_with_test_scan_arbitrary_ordering() {
    // Demonstrates that TestScan can accept ANY ordering (not just reverse)
    // This is different from ParquetExec which only supports reverse scans
    let schema = schema();

    // Source has [a ASC, b ASC] ordering
    let a = sort_expr("a", &schema);
    let b = sort_expr("b", &schema);
    let source_ordering = LexOrdering::new(vec![a.clone(), b.clone()]).unwrap();
    let source = test_scan_with_ordering(schema.clone(), source_ordering);

    // Request [a ASC, b DESC] - NOT a simple reverse, but TestScan accepts it
    let mixed_ordering = LexOrdering::new(vec![a, b.reverse()]).unwrap();
    let plan = sort_exec(mixed_ordering, source);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 ASC, b@1 DESC NULLS LAST], preserve_partitioning=[false]
        -   TestScan: output_ordering=[a@0 ASC, b@1 ASC]
      output:
        Ok:
          - SortExec: expr=[a@0 ASC, b@1 DESC NULLS LAST], preserve_partitioning=[false]
          -   TestScan: output_ordering=[a@0 ASC, b@1 ASC], requested_ordering=[a@0 ASC, b@1 DESC NULLS LAST]
    "
    );
}
