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
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
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
    // Basic test: sort gets pushed into the source if supported
    let schema = schema();
    let source = parquet_exec(schema.clone());
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
          - DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
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
    // Sort with fetch should be optimized
    let schema = schema();
    let source = parquet_exec(schema.clone());
    let sort_exprs = LexOrdering::new(vec![sort_expr("a", &schema)]).unwrap();
    let plan = sort_exec_with_fetch(sort_exprs, Some(10), source);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r###"
    OptimizationTest:
      input:
        - SortExec: TopK(fetch=10), expr=[a@0 ASC], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      output:
        Ok:
          - GlobalLimitExec: skip=0, fetch=10
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
    "###
    );
}

#[test]
fn test_global_limit_sort_pushdown() {
    // GlobalLimitExec -> SortExec pattern should be optimized together
    let schema = schema();
    let source = parquet_exec(schema.clone());
    let sort_exprs = LexOrdering::new(vec![sort_expr("a", &schema)]).unwrap();
    let sort = sort_exec(sort_exprs, source);
    let plan = global_limit_exec(sort, 0, Some(10));

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r###"
    OptimizationTest:
      input:
        - GlobalLimitExec: skip=0, fetch=10
        -   SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
        -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      output:
        Ok:
          - GlobalLimitExec: skip=0, fetch=10
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
    "###
    );
}

#[test]
fn test_global_limit_sort_with_skip() {
    // GlobalLimitExec with skip -> SortExec
    let schema = schema();
    let source = parquet_exec(schema.clone());
    let sort_exprs = LexOrdering::new(vec![sort_expr("a", &schema)]).unwrap();
    let sort = sort_exec(sort_exprs, source);
    let plan = global_limit_exec(sort, 5, Some(10));

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r###"
    OptimizationTest:
      input:
        - GlobalLimitExec: skip=5, fetch=10
        -   SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
        -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      output:
        Ok:
          - GlobalLimitExec: skip=5, fetch=10
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], fetch=15, file_type=parquet
    "###
    );
}

#[test]
fn test_sort_multiple_columns() {
    // Sort on multiple columns
    let schema = schema();
    let source = parquet_exec(schema.clone());

    let sort_exprs = LexOrdering::new(vec![
        sort_expr("a", &schema),
        PhysicalSortExpr {
            expr: datafusion_physical_expr::expressions::col("b", &schema).unwrap(),
            options: SortOptions {
                descending: true,
                nulls_first: true,
            },
        },
    ])
    .unwrap();

    let plan = sort_exec(sort_exprs, source);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r###"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 ASC, b@1 DESC NULLS FIRST], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      output:
        Ok:
          - DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC, b@1 DESC NULLS FIRST], file_type=parquet
    "###
    );
}

#[test]
fn test_sort_through_coalesce_batches() {
    // Sort should push through CoalesceBatchesExec
    let schema = schema();
    let source = parquet_exec(schema.clone());
    let coalesce = coalesce_batches_exec(source, 1024);
    let sort_exprs = LexOrdering::new(vec![sort_expr("a", &schema)]).unwrap();
    let plan = sort_exec(sort_exprs, coalesce);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r###"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
        -   CoalesceBatchesExec: target_batch_size=1024
        -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      output:
        Ok:
          - CoalesceBatchesExec: target_batch_size=1024
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
    "###
    );
}

#[test]
fn test_sort_through_repartition() {
    // Sort should push through RepartitionExec
    let schema = schema();
    let source = parquet_exec(schema.clone());
    let repartition = repartition_exec(source);
    let sort_exprs = LexOrdering::new(vec![sort_expr("a", &schema)]).unwrap();
    let plan = sort_exec(sort_exprs, repartition);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r###"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
        -   RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
        -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      output:
        Ok:
          - RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
    "###
    );
}

#[test]
fn test_nested_sorts() {
    // Nested sort operations should each be optimized
    let schema = schema();
    let source = parquet_exec(schema.clone());

    let sort_exprs1 = LexOrdering::new(vec![sort_expr("a", &schema)]).unwrap();
    let inner_sort = sort_exec(sort_exprs1, source);

    let sort_exprs2 = LexOrdering::new(vec![sort_expr("b", &schema)]).unwrap();
    let plan = sort_exec(sort_exprs2, inner_sort);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r###"
    OptimizationTest:
      input:
        - SortExec: expr=[b@1 ASC], preserve_partitioning=[false]
        -   SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
        -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[b@1 ASC], preserve_partitioning=[false]
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
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
        -   RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
        -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - SortPreservingMergeExec: [a@0 ASC]
          -   RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
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
        @r###"
    OptimizationTest:
      input:
        - SortExec: TopK(fetch=10), expr=[a@0 ASC], preserve_partitioning=[false]
        -   RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
        -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - LocalLimitExec: fetch=10
          -   RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
          -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
    "###
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
        -   RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
        -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
      output:
        Ok:
          - RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
    "###
    );
}

#[test]
fn test_sort_different_directions() {
    // Test various sort direction combinations
    let schema = schema();

    // Test DESC NULLS LAST
    let source = parquet_exec(schema.clone());
    let sort_exprs = LexOrdering::new(vec![sort_expr_options(
        "a",
        &schema,
        SortOptions {
            descending: true,
            nulls_first: false,
        },
    )])
    .unwrap();
    let plan = sort_exec(sort_exprs, source);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r###"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 DESC], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      output:
        Ok:
          - DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 DESC], file_type=parquet
    "###
    );
}

#[test]
fn test_sort_asc_nulls_first() {
    // Test ASC NULLS FIRST
    let schema = schema();
    let source = parquet_exec(schema.clone());
    let sort_exprs = LexOrdering::new(vec![sort_expr_options(
        "a",
        &schema,
        SortOptions {
            descending: false,
            nulls_first: true,
        },
    )])
    .unwrap();
    let plan = sort_exec(sort_exprs, source);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r###"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 ASC NULLS FIRST], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      output:
        Ok:
          - DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC NULLS FIRST], file_type=parquet
    "###
    );
}

#[test]
fn test_sort_desc_nulls_first() {
    // Test DESC NULLS FIRST
    let schema = schema();
    let source = parquet_exec(schema.clone());
    let sort_exprs = LexOrdering::new(vec![sort_expr_options(
        "a",
        &schema,
        SortOptions {
            descending: true,
            nulls_first: true,
        },
    )])
    .unwrap();
    let plan = sort_exec(sort_exprs, source);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r###"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 DESC NULLS FIRST], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      output:
        Ok:
          - DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 DESC NULLS FIRST], file_type=parquet
    "###
    );
}

#[test]
fn test_sort_through_coalesce_partitions() {
    // Sort should push through CoalescePartitionsExec
    let schema = schema();
    let source = parquet_exec(schema.clone());
    let repartition = repartition_exec(source);
    let coalesce_parts = coalesce_partitions_exec(repartition);
    let sort_exprs = LexOrdering::new(vec![sort_expr("a", &schema)]).unwrap();
    let plan = sort_exec(sort_exprs, coalesce_parts);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r###"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
        -   CoalescePartitionsExec
        -     RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
        -       DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      output:
        Ok:
          - CoalescePartitionsExec
          -   RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
          -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
    "###
    );
}

#[test]
fn test_complex_plan_with_multiple_operators() {
    // Test a complex plan with multiple operators between sort and source
    let schema = schema();
    let source = parquet_exec(schema.clone());
    let coalesce_batches = coalesce_batches_exec(source, 1024);
    let repartition = repartition_exec(coalesce_batches);
    let coalesce_parts = coalesce_partitions_exec(repartition);
    let sort_exprs = LexOrdering::new(vec![sort_expr("a", &schema)]).unwrap();
    let plan = sort_exec(sort_exprs, coalesce_parts);

    // NOTE: Current implementation limitation - sort pushdown cannot traverse
    // through multiple layers of transparent nodes (CoalescePartitions -> Repartition -> CoalesceBatches)
    // The sort remains at the top and is not optimized. This is a known limitation
    // that could be addressed in a future enhancement with recursive traversal.
    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r###"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
        -   CoalescePartitionsExec
        -     RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
        -       CoalesceBatchesExec: target_batch_size=1024
        -         DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
          -   CoalescePartitionsExec
          -     RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
          -       CoalesceBatchesExec: target_batch_size=1024
          -         DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
    "###
    );
}

#[test]
fn test_multiple_sorts_different_columns() {
    // Test nested sorts on different columns
    let schema = schema();
    let source = parquet_exec(schema.clone());

    // First sort by column 'a'
    let sort_exprs1 = LexOrdering::new(vec![sort_expr("a", &schema)]).unwrap();
    let sort1 = sort_exec(sort_exprs1, source);

    // Then sort by column 'c'
    let sort_exprs2 = LexOrdering::new(vec![sort_expr("c", &schema)]).unwrap();
    let plan = sort_exec(sort_exprs2, sort1);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r###"
    OptimizationTest:
      input:
        - SortExec: expr=[c@2 ASC], preserve_partitioning=[false]
        -   SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
        -     DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      output:
        Ok:
          - SortExec: expr=[c@2 ASC], preserve_partitioning=[false]
          -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
    "###
    );
}

#[test]
fn test_sort_with_mixed_directions() {
    // Test sort with mixed ASC/DESC across columns
    let schema = schema();
    let source = parquet_exec(schema.clone());

    let sort_exprs = LexOrdering::new(vec![
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
                nulls_first: false,
            },
        ),
    ])
    .unwrap();

    let plan = sort_exec(sort_exprs, source);

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownSort::new(), true),
        @r###"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 ASC NULLS FIRST, b@1 DESC, c@2 ASC], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      output:
        Ok:
          - DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC NULLS FIRST, b@1 DESC, c@2 ASC], file_type=parquet
    "###
    );
}
