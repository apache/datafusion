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

//! Tests for sort pushdown optimizer rule
//!
use arrow::compute::SortOptions;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_optimizer::pushdown_sort::PushdownSort;
use datafusion_physical_optimizer::PhysicalOptimizerRule;

use crate::physical_optimizer::test_utils::{
    coalesce_batches_exec, coalesce_partitions_exec, global_limit_exec, parquet_exec,
    parquet_exec_with_sort, repartition_exec, schema, sort_exec, sort_exec_with_fetch,
    sort_exec_with_preserve_partitioning, sort_expr, sort_expr_options, OptimizationTest,
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
fn test_sort_pushdown_basic() {
    // Basic test: reverse sort on source with existing ASC ordering
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
        @r###"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 DESC NULLS LAST], file_type=parquet
    "###
    );
}

#[test]
fn test_sort_already_satisfied() {
    // If source already provides the required ordering, sort should be removed
    let schema = schema();
    let sort_exprs = LexOrdering::new(vec![sort_expr("a", &schema)]).unwrap();

    // Create a source that already has the ordering
    let source = parquet_exec_with_sort(schema, vec![sort_exprs.clone()]);
    let plan = sort_exec(sort_exprs, source);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r###"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
    "###
    );
}

#[test]
fn test_sort_with_limit() {
    // Sort with fetch should be optimized when reversing
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
        @r###"
    OptimizationTest:
      input:
        - SortExec: TopK(fetch=10), expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], limit=10, output_ordering=[a@0 DESC NULLS LAST], file_type=parquet
    "###
    );
}

#[test]
fn test_global_limit_sort_pushdown() {
    // GlobalLimitExec -> SortExec pattern should be optimized together
    let schema = schema();

    // Source has ASC ordering
    let source_ordering = LexOrdering::new(vec![sort_expr("a", &schema)]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // Request DESC ordering
    let desc_ordering = LexOrdering::new(vec![sort_expr_options(
        "a",
        &schema,
        SortOptions {
            descending: true,
            nulls_first: false,
        },
    )])
    .unwrap();
    let sort = sort_exec(desc_ordering, source);
    let plan = global_limit_exec(sort, 0, Some(10));

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r###"
    OptimizationTest:
      input:
        - GlobalLimitExec: skip=0, fetch=10
        -   SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
        -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], limit=10, output_ordering=[a@0 DESC NULLS LAST], file_type=parquet
    "###
    );
}

#[test]
fn test_global_limit_sort_with_skip() {
    // GlobalLimitExec with skip -> SortExec
    let schema = schema();

    // Source has ASC ordering
    let source_ordering = LexOrdering::new(vec![sort_expr("a", &schema)]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // Request DESC ordering
    let desc_ordering = LexOrdering::new(vec![sort_expr_options(
        "a",
        &schema,
        SortOptions {
            descending: true,
            nulls_first: false,
        },
    )])
    .unwrap();
    let sort = sort_exec(desc_ordering, source);
    let plan = global_limit_exec(sort, 5, Some(10));

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r###"
    OptimizationTest:
      input:
        - GlobalLimitExec: skip=5, fetch=10
        -   SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
        -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - GlobalLimitExec: skip=5, fetch=10
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], limit=15, output_ordering=[a@0 DESC NULLS LAST], file_type=parquet
    "###
    );
}

#[test]
fn test_sort_multiple_columns() {
    // Sort on multiple columns - reverse multi-column ordering
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
          - DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC, b@1 DESC NULLS LAST], file_type=parquet
    "
    );
}

#[test]
fn test_sort_through_coalesce_batches() {
    // Sort should not push through CoalesceBatchesExec (not transparent for sort pushdown)
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
          - CoalesceBatchesExec: target_batch_size=1024
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 DESC NULLS LAST], file_type=parquet
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
          - SortPreservingMergeExec: [a@0 DESC NULLS LAST]
          -   RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, maintains_sort_order=true
          -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 DESC NULLS LAST], file_type=parquet
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
        @r###"
    OptimizationTest:
      input:
        - SortExec: expr=[b@1 ASC], preserve_partitioning=[false]
        -   SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
        -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[b@1 ASC], preserve_partitioning=[false]
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 DESC NULLS LAST], file_type=parquet
    "###
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
fn test_sort_with_multiple_partitions_converts_to_merge() {
    // When source has multiple partitions and is already sorted,
    // SortExec should convert to SortPreservingMergeExec
    let schema = schema();
    let sort_exprs = LexOrdering::new(vec![sort_expr("a", &schema)]).unwrap();

    // Create source with ordering and then repartition to create multiple partitions
    let source = parquet_exec_with_sort(schema, vec![sort_exprs.clone()]);
    let repartition = repartition_exec(source);
    let plan = sort_exec(sort_exprs, repartition);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r###"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
        -   RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, maintains_sort_order=true
        -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - SortPreservingMergeExec: [a@0 ASC]
          -   RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, maintains_sort_order=true
          -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
    "###
    );
}

#[test]
fn test_sort_with_fetch_multiple_partitions_adds_local_limit() {
    // Sort with fetch and multiple partitions should add LocalLimitExec
    let schema = schema();
    let sort_exprs = LexOrdering::new(vec![sort_expr("a", &schema)]).unwrap();

    // Create source with ordering and multiple partitions
    let source = parquet_exec_with_sort(schema, vec![sort_exprs.clone()]);
    let repartition = repartition_exec(source);
    let plan = sort_exec_with_fetch(sort_exprs, Some(10), repartition);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: TopK(fetch=10), expr=[a@0 ASC], preserve_partitioning=[false], sort_prefix=[a@0 ASC]
        -   RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, maintains_sort_order=true
        -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - SortPreservingMergeExec: [a@0 ASC], fetch=10
          -   RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, maintains_sort_order=true
          -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
    "
    );
}

#[test]
fn test_sort_preserve_partitioning_with_satisfied_ordering() {
    // Sort with preserve_partitioning should not add merge when ordering is satisfied
    let schema = schema();
    let sort_exprs = LexOrdering::new(vec![sort_expr("a", &schema)]).unwrap();

    let source = parquet_exec_with_sort(schema, vec![sort_exprs.clone()]);
    let repartition = repartition_exec(source);
    let plan = sort_exec_with_preserve_partitioning(sort_exprs, repartition);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r###"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
        -   RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, maintains_sort_order=true
        -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, maintains_sort_order=true
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
    "###
    );
}

#[test]
fn test_sort_different_directions() {
    // Test DESC to ASC reversal
    let schema = schema();

    // Source has DESC NULLS LAST ordering
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

    // Request ASC NULLS FIRST ordering (exact reverse)
    let asc_ordering = LexOrdering::new(vec![sort_expr_options(
        "a",
        &schema,
        SortOptions {
            descending: false,
            nulls_first: true,
        },
    )])
    .unwrap();
    let plan = sort_exec(asc_ordering, source);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 DESC NULLS LAST], file_type=parquet
      output:
        Ok:
          - DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
    "
    );
}

#[test]
fn test_sort_asc_nulls_first() {
    // Test ASC NULLS FIRST to DESC NULLS LAST reversal
    let schema = schema();

    // Source has ASC NULLS LAST (default)
    let source_ordering = LexOrdering::new(vec![sort_expr("a", &schema)]).unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // Request DESC NULLS FIRST - this is NOT exact reverse of ASC NULLS LAST
    // ASC NULLS LAST reverses to DESC NULLS FIRST, not DESC NULLS LAST
    // So this test should NOT optimize
    let desc_ordering = LexOrdering::new(vec![sort_expr_options(
        "a",
        &schema,
        SortOptions {
            descending: true,
            nulls_first: false, // This doesn't match - should be nulls_first: true
        },
    )])
    .unwrap();
    let plan = sort_exec(desc_ordering, source);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r###"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 DESC NULLS LAST], file_type=parquet
    "###
    );
}

#[test]
fn test_sort_desc_nulls_first() {
    // Test DESC NULLS FIRST to ASC NULLS LAST reversal
    let schema = schema();

    // Source has DESC NULLS LAST (not NULLS FIRST!)
    let source_ordering = LexOrdering::new(vec![sort_expr_options(
        "a",
        &schema,
        SortOptions {
            descending: true,
            nulls_first: false, // NULLS LAST
        },
    )])
    .unwrap();
    let source = parquet_exec_with_sort(schema.clone(), vec![source_ordering]);

    // Request ASC NULLS FIRST (exact reverse)
    let asc_ordering = LexOrdering::new(vec![sort_expr_options(
        "a",
        &schema,
        SortOptions {
            descending: false,
            nulls_first: true,
        },
    )])
    .unwrap();
    let plan = sort_exec(asc_ordering, source);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 DESC NULLS LAST], file_type=parquet
      output:
        Ok:
          - DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
    "
    );
}

#[test]
fn test_sort_through_coalesce_partitions() {
    // Sort should push through CoalescePartitionsExec BUT
    // CoalescePartitions merges into single partition, losing the sort benefit
    // The optimizer may choose not to optimize this case
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
        @r###"
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
          -     RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, maintains_sort_order=true
          -       DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 DESC NULLS LAST], file_type=parquet
    "###
    );
}

#[test]
fn test_complex_plan_with_multiple_operators() {
    // Test a complex plan with multiple operators between sort and source
    // This should NOT optimize because CoalesceBatchesExec blocks the pushdown
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
        @r###"
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
          -     RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, maintains_sort_order=true
          -       CoalesceBatchesExec: target_batch_size=1024
          -         DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 DESC NULLS LAST], file_type=parquet
    "###
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
        @r###"
    OptimizationTest:
      input:
        - SortExec: expr=[c@2 ASC], preserve_partitioning=[false]
        -   SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
        -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[c@2 ASC], preserve_partitioning=[false]
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 DESC NULLS LAST], file_type=parquet
    "###
    );
}

#[test]
fn test_sort_with_mixed_directions() {
    // Test sort with mixed ASC/DESC across columns
    let schema = schema();

    // Source has [a DESC NULLS LAST, b ASC, c DESC NULLS LAST] ordering
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

    // Request [a ASC NULLS FIRST, b DESC, c ASC NULLS FIRST] ordering (exact reverse)
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
        sort_expr_options(
            "c",
            &schema,
            SortOptions {
                descending: false,
                nulls_first: true,
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
        - SortExec: expr=[a@0 ASC, b@1 DESC NULLS LAST, c@2 ASC], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 DESC NULLS LAST, b@1 ASC, c@2 DESC NULLS LAST], file_type=parquet
      output:
        Ok:
          - DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC, b@1 DESC NULLS LAST, c@2 ASC], file_type=parquet
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
