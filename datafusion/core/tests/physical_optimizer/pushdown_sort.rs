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
//! 1. Reverse scan is enabled (reverse_scan_inexact=true)
//! 2. SortExec is kept (because ordering is inexact)
//! 3. output_ordering remains unchanged
//! 4. Early termination is enabled for TopK queries
//! 5. Prefix matching works correctly

use arrow::compute::SortOptions;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_optimizer::pushdown_sort::PushdownSort;
use datafusion_physical_optimizer::PhysicalOptimizerRule;

use crate::physical_optimizer::test_utils::{
    coalesce_batches_exec, coalesce_partitions_exec, parquet_exec,
    parquet_exec_with_sort, repartition_exec, schema, sort_exec, sort_exec_with_fetch,
    sort_expr, sort_expr_options, OptimizationTest,
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
        @r###"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
    "###
    );
}

#[test]
fn test_sort_pushdown_basic_phase1() {
    // Phase 1: Reverse scan enabled, Sort kept, output_ordering unchanged
    let schema = schema();

    // Source has ASC NULLS LAST ordering (default)
    let source_ordering = LexOrdering::new(vec![sort_expr("a", &schema)]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // Request DESC NULLS LAST ordering (exact reverse)
    let desc_ordering = LexOrdering::new(vec![sort_expr_options(
        "a",
        &schema,
        SortOptions {
            descending: true,
            nulls_first: false,
        },
    )])
    .unwrap();
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
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_scan_inexact=true
    "
    );
}

#[test]
fn test_sort_with_limit_phase1() {
    // Phase 1: Sort with fetch enables early termination but keeps Sort
    let schema = schema();

    // Source has ASC ordering
    let source_ordering = LexOrdering::new(vec![sort_expr("a", &schema)]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // Request DESC ordering with limit
    let desc_ordering = LexOrdering::new(vec![sort_expr_options(
        "a",
        &schema,
        SortOptions {
            descending: true,
            nulls_first: false,
        },
    )])
    .unwrap();
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
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_scan_inexact=true
    "
    );
}

#[test]
fn test_sort_multiple_columns_phase1() {
    // Phase 1: Sort on multiple columns - reverse multi-column ordering
    let schema = schema();

    // Source has [a DESC NULLS LAST, b ASC] ordering
    let source_ordering = LexOrdering::new(vec![
        sort_expr_options(
            "a",
            &schema,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        ),
        sort_expr("b", &schema),
    ])
    .unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // Request [a ASC NULLS FIRST, b DESC] ordering (exact reverse)
    let reverse_ordering = LexOrdering::new(vec![
        sort_expr_options(
            "a",
            &schema,
            SortOptions {
                descending: false,
                nulls_first: true,
            },
        ),
        sort_expr_options(
            "b",
            &schema,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        ),
    ])
    .unwrap();
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
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_scan_inexact=true
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
    let source_ordering = LexOrdering::new(vec![
        sort_expr_options(
            "a",
            &schema,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        ),
        sort_expr("b", &schema),
    ])
    .unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // Request only [a ASC NULLS FIRST] - a prefix of the reversed ordering
    let prefix_ordering = LexOrdering::new(vec![sort_expr_options(
        "a",
        &schema,
        SortOptions {
            descending: false,
            nulls_first: true,
        },
    )])
    .unwrap();
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
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_scan_inexact=true
    "
    );
}

#[test]
fn test_prefix_match_with_limit() {
    // Test prefix matching with LIMIT - important for TopK optimization
    let schema = schema();

    // Source has [a ASC, b DESC, c ASC] ordering
    let source_ordering = LexOrdering::new(vec![
        sort_expr("a", &schema),
        sort_expr_options(
            "b",
            &schema,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        ),
        sort_expr("c", &schema),
    ])
    .unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // Request [a DESC NULLS LAST, b ASC NULLS FIRST] with LIMIT 100
    // This is a prefix (2 columns) of the reversed 3-column ordering
    let prefix_ordering = LexOrdering::new(vec![
        sort_expr_options(
            "a",
            &schema,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        ),
        sort_expr_options(
            "b",
            &schema,
            SortOptions {
                descending: false,
                nulls_first: true,
            },
        ),
    ])
    .unwrap();
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
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_scan_inexact=true
    "
    );
}

#[test]
fn test_prefix_match_through_transparent_nodes() {
    // Test prefix matching works through transparent nodes
    let schema = schema();

    // Source has [a DESC NULLS LAST, b ASC, c DESC] ordering
    let source_ordering = LexOrdering::new(vec![
        sort_expr_options(
            "a",
            &schema,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        ),
        sort_expr("b", &schema),
        sort_expr_options(
            "c",
            &schema,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        ),
    ])
    .unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);
    let coalesce = coalesce_batches_exec(source, 1024);
    let repartition = repartition_exec(coalesce);

    // Request only [a ASC NULLS FIRST] - prefix of reversed ordering
    let prefix_ordering = LexOrdering::new(vec![sort_expr_options(
        "a",
        &schema,
        SortOptions {
            descending: false,
            nulls_first: true,
        },
    )])
    .unwrap();
    let plan = sort_exec(prefix_ordering, repartition);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
        -   RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, maintains_sort_order=true
        -     CoalesceBatchesExec: target_batch_size=1024
        -       DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 DESC NULLS LAST, b@1 ASC, c@2 DESC NULLS LAST], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
          -   RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
          -     CoalesceBatchesExec: target_batch_size=1024
          -       DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_scan_inexact=true
    "
    );
}

#[test]
fn test_no_prefix_match_wrong_direction() {
    // Test that prefix matching does NOT work if the direction is wrong
    let schema = schema();

    // Source has [a DESC, b ASC] ordering
    let source_ordering = LexOrdering::new(vec![
        sort_expr_options(
            "a",
            &schema,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        ),
        sort_expr("b", &schema),
    ])
    .unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // Request [a DESC] - same direction as source, NOT a reverse prefix
    let same_direction = LexOrdering::new(vec![sort_expr_options(
        "a",
        &schema,
        SortOptions {
            descending: true,
            nulls_first: false,
        },
    )])
    .unwrap();
    let plan = sort_exec(same_direction, source);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r###"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 DESC NULLS LAST, b@1 ASC], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 DESC NULLS LAST, b@1 ASC], file_type=parquet
    "###
    );
}

#[test]
fn test_no_prefix_match_longer_than_source() {
    // Test that prefix matching does NOT work if requested is longer than source
    let schema = schema();

    // Source has [a DESC] ordering (single column)
    let source_ordering = LexOrdering::new(vec![sort_expr_options(
        "a",
        &schema,
        SortOptions {
            descending: true,
            nulls_first: false,
        },
    )])
    .unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // Request [a ASC, b DESC] - longer than source, can't be a prefix
    let longer_ordering = LexOrdering::new(vec![
        sort_expr_options(
            "a",
            &schema,
            SortOptions {
                descending: false,
                nulls_first: true,
            },
        ),
        sort_expr_options(
            "b",
            &schema,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        ),
    ])
    .unwrap();
    let plan = sort_exec(longer_ordering, source);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r###"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 ASC, b@1 DESC NULLS LAST], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 DESC NULLS LAST], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[a@0 ASC, b@1 DESC NULLS LAST], preserve_partitioning=[false]
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 DESC NULLS LAST], file_type=parquet
    "###
    );
}

// ============================================================================
// ORIGINAL TESTS
// ============================================================================

#[test]
fn test_sort_through_coalesce_batches() {
    // Sort pushes through CoalesceBatchesExec
    let schema = schema();
    let source_ordering = LexOrdering::new(vec![sort_expr("a", &schema)]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);
    let coalesce = coalesce_batches_exec(source, 1024);

    let desc_ordering = LexOrdering::new(vec![sort_expr_options(
        "a",
        &schema,
        SortOptions {
            descending: true,
            nulls_first: false,
        },
    )])
    .unwrap();
    let plan = sort_exec(desc_ordering, coalesce);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
        -   CoalesceBatchesExec: target_batch_size=1024
        -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
          -   CoalesceBatchesExec: target_batch_size=1024
          -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_scan_inexact=true
    "
    );
}

#[test]
fn test_sort_through_repartition() {
    // Sort should push through RepartitionExec
    let schema = schema();
    let source_ordering = LexOrdering::new(vec![sort_expr("a", &schema)]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);
    let repartition = repartition_exec(source);

    let desc_ordering = LexOrdering::new(vec![sort_expr_options(
        "a",
        &schema,
        SortOptions {
            descending: true,
            nulls_first: false,
        },
    )])
    .unwrap();
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
          -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_scan_inexact=true
    "
    );
}

#[test]
fn test_nested_sorts() {
    // Nested sort operations - only innermost can be optimized
    let schema = schema();
    let source_ordering = LexOrdering::new(vec![sort_expr("a", &schema)]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    let desc_ordering = LexOrdering::new(vec![sort_expr_options(
        "a",
        &schema,
        SortOptions {
            descending: true,
            nulls_first: false,
        },
    )])
    .unwrap();
    let inner_sort = sort_exec(desc_ordering, source);

    let sort_exprs2 = LexOrdering::new(vec![sort_expr("b", &schema)]).unwrap();
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
          -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_scan_inexact=true
    "
    );
}

#[test]
fn test_non_sort_plans_unchanged() {
    // Plans without SortExec should pass through unchanged
    let schema = schema();
    let source = parquet_exec(schema.clone());
    let plan = coalesce_batches_exec(source, 1024);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r###"
    OptimizationTest:
      input:
        - CoalesceBatchesExec: target_batch_size=1024
        -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      output:
        Ok:
          - CoalesceBatchesExec: target_batch_size=1024
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
    "###
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
    let source_ordering = LexOrdering::new(vec![sort_expr("a", &schema)]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);
    let repartition = repartition_exec(source);
    let coalesce_parts = coalesce_partitions_exec(repartition);

    let desc_ordering = LexOrdering::new(vec![sort_expr_options(
        "a",
        &schema,
        SortOptions {
            descending: true,
            nulls_first: false,
        },
    )])
    .unwrap();
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
          -       DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_scan_inexact=true
    "
    );
}

#[test]
fn test_complex_plan_with_multiple_operators() {
    // Test a complex plan with multiple operators between sort and source
    let schema = schema();
    let source_ordering = LexOrdering::new(vec![sort_expr("a", &schema)]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);
    let coalesce_batches = coalesce_batches_exec(source, 1024);
    let repartition = repartition_exec(coalesce_batches);
    let coalesce_parts = coalesce_partitions_exec(repartition);

    let desc_ordering = LexOrdering::new(vec![sort_expr_options(
        "a",
        &schema,
        SortOptions {
            descending: true,
            nulls_first: false,
        },
    )])
    .unwrap();
    let plan = sort_exec(desc_ordering, coalesce_parts);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
        -   CoalescePartitionsExec
        -     RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, maintains_sort_order=true
        -       CoalesceBatchesExec: target_batch_size=1024
        -         DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
          -   CoalescePartitionsExec
          -     RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
          -       CoalesceBatchesExec: target_batch_size=1024
          -         DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_scan_inexact=true
    "
    );
}

#[test]
fn test_multiple_sorts_different_columns() {
    // Test nested sorts on different columns - only innermost can optimize
    let schema = schema();
    let source_ordering = LexOrdering::new(vec![sort_expr("a", &schema)]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // First sort by column 'a' DESC (reverse of source)
    let desc_ordering = LexOrdering::new(vec![sort_expr_options(
        "a",
        &schema,
        SortOptions {
            descending: true,
            nulls_first: false,
        },
    )])
    .unwrap();
    let sort1 = sort_exec(desc_ordering, source);

    // Then sort by column 'c' (different column, can't optimize)
    let sort_exprs2 = LexOrdering::new(vec![sort_expr("c", &schema)]).unwrap();
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
          -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet, reverse_scan_inexact=true
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
        @r###"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
    "###
    );
}

#[test]
fn test_no_pushdown_for_non_reverse_sort() {
    // Verify pushdown does NOT happen when sort doesn't reverse source ordering
    let schema = schema();

    // Source sorted by 'a' ASC
    let source_ordering = LexOrdering::new(vec![sort_expr("a", &schema)]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // Request sort by 'b' (different column)
    let sort_exprs = LexOrdering::new(vec![sort_expr("b", &schema)]).unwrap();
    let plan = sort_exec(sort_exprs, source);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r###"
    OptimizationTest:
      input:
        - SortExec: expr=[b@1 ASC], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[b@1 ASC], preserve_partitioning=[false]
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
    "###
    );
}
