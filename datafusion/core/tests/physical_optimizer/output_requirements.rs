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

use crate::physical_optimizer::test_utils::{parquet_exec, schema, sort_exec, sort_expr};

use arrow::array::{Int32Array, cast::AsArray, types::Int32Type};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::prelude::SessionContext;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::execution_props::{ScalarSubqueryResults, SubqueryIndex};
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion_physical_optimizer::output_requirements::OutputRequirements;
use datafusion_physical_plan::scalar_subquery::{ScalarSubqueryExec, ScalarSubqueryLink};
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::{ExecutionPlan, collect, displayable, get_plan_string};

/// `OutputRequirements::new_add_mode()` must be idempotent: re-applying it to
/// its own output must not stack additional `OutputRequirementExec` wrappers.
///
/// AQE (datafusion-ballista#1359) re-runs the optimizer chain after every
/// completed stage; without this guarantee, every replan adds another wrapper.
#[test]
fn add_mode_is_idempotent_on_bare_scan() {
    // Exercises the path where `require_top_ordering_helper` returns
    // `is_changed = false` and the rule adds a default (empty-requirement)
    // wrapper.
    assert_add_mode_idempotent(parquet_exec(schema()));
}

#[test]
fn add_mode_is_idempotent_on_sorted_plan() {
    // Exercises the path where the helper recognizes a top-level `SortExec`
    // and produces a wrapper carrying that ordering requirement
    // (`is_changed = true` branch).
    let s = schema();
    let ordering: LexOrdering = [sort_expr("a", &s)].into();
    let plan = sort_exec(ordering, parquet_exec(Arc::clone(&s)));
    assert_add_mode_idempotent(plan);
}

fn assert_add_mode_idempotent(plan: Arc<dyn ExecutionPlan>) {
    let config = ConfigOptions::new();
    let rule = OutputRequirements::new_add_mode();

    let once = rule.optimize(plan, &config).unwrap();
    let twice = rule.optimize(Arc::clone(&once), &config).unwrap();

    assert_eq!(
        get_plan_string(&once),
        get_plan_string(&twice),
        "second invocation of OutputRequirements::new_add_mode mutated the plan",
    );
}

/// For a `ScalarSubqueryExec` root, `new_add_mode()` descends through the main
/// input (child 0) and wraps the global `SortExec` with an `OutputRequirementExec`
/// carrying its ordering, leaving the subquery child untouched. Without this, the
/// multi-child root is skipped and the query's global ORDER BY requirement is lost.
#[test]
fn add_mode_descends_through_scalar_subquery() {
    let s = schema();
    let ordering: LexOrdering = [sort_expr("a", &s)].into();
    let sort = sort_exec(ordering, parquet_exec(Arc::clone(&s)));

    // A subquery child makes `children.len() == 2`, exercising the multi-child path.
    let subqueries = vec![ScalarSubqueryLink {
        plan: parquet_exec(Arc::clone(&s)),
        index: SubqueryIndex::new(0),
    }];
    let plan = Arc::new(ScalarSubqueryExec::new(
        sort,
        subqueries,
        ScalarSubqueryResults::new(1),
    )) as Arc<dyn ExecutionPlan>;

    let optimized = OutputRequirements::new_add_mode()
        .optimize(plan, &ConfigOptions::new())
        .unwrap();

    insta::assert_snapshot!(
        displayable(optimized.as_ref()).indent(true).to_string(),
        @r"
    ScalarSubqueryExec: subqueries=1
      OutputRequirementExec: order_by=[(a@0, asc)], dist_by=SinglePartition
        SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
    ");
}

/// End-to-end wrong-results guard: run the full default physical optimizer
/// pipeline over a `ScalarSubqueryExec` root whose global ordering is provided
/// by a `SortPreservingMergeExec` over a multi-partition ordered source — the
/// shape federated/custom planners hand to the optimizer, with no `SortExec`
/// backstop. If `OutputRequirements` fails to record the ordering under the
/// multi-child root, the empty root requirement (no ordering, unspecified
/// distribution) lets the pipeline eliminate the merge, and the query returns
/// rows in partition-interleaved order — actually incorrect results, not
/// merely a different plan.
#[tokio::test]
async fn full_pipeline_keeps_row_order_under_scalar_subquery_root() {
    // Two partitions, each sorted on `a`; only an order-preserving merge
    // yields the global order.
    let s = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let partitions: Vec<Vec<RecordBatch>> = [vec![1, 3, 5, 7], vec![2, 4, 6, 8]]
        .into_iter()
        .map(|v| {
            vec![
                RecordBatch::try_new(Arc::clone(&s), vec![Arc::new(Int32Array::from(v))])
                    .unwrap(),
            ]
        })
        .collect();
    let ordering: LexOrdering = [sort_expr("a", &s)].into();
    let source = DataSourceExec::from_data_source(
        MemorySourceConfig::try_new(&partitions, Arc::clone(&s), None)
            .unwrap()
            .try_with_sort_information(vec![ordering.clone()])
            .unwrap(),
    );
    let merge = Arc::new(SortPreservingMergeExec::new(ordering, source));

    // A scalar subquery child must produce exactly one column and one row.
    let sq_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
    let sq_batch = RecordBatch::try_new(
        Arc::clone(&sq_schema),
        vec![Arc::new(Int32Array::from(vec![42]))],
    )
    .unwrap();
    let sq_plan =
        MemorySourceConfig::try_new_exec(&[vec![sq_batch]], sq_schema, None).unwrap();

    let plan = Arc::new(ScalarSubqueryExec::new(
        merge,
        vec![ScalarSubqueryLink {
            plan: sq_plan,
            index: SubqueryIndex::new(0),
        }],
        ScalarSubqueryResults::new(1),
    )) as Arc<dyn ExecutionPlan>;

    let mut config = ConfigOptions::new();
    config.execution.target_partitions = 4;
    // Keep the plan focused on the merge-vs-coalesce decision rather than on
    // added parallelism.
    config.optimizer.enable_round_robin_repartition = false;
    config.optimizer.prefer_existing_sort = true;

    let mut optimized = plan;
    for rule in PhysicalOptimizer::new().rules {
        optimized = rule.optimize(optimized, &config).unwrap();
    }

    // The order-preserving merge must survive the pipeline; the failure mode
    // replaces it with a `CoalescePartitionsExec`.
    let display = displayable(optimized.as_ref()).indent(true).to_string();
    assert!(
        display.contains("SortPreservingMergeExec")
            && !display.contains("CoalescePartitionsExec"),
        "order-preserving merge was optimized away:\n{display}"
    );

    // And the rows must actually come back globally ordered.
    let ctx = SessionContext::new();
    let batches = collect(optimized, ctx.task_ctx()).await.unwrap();
    let values: Vec<i32> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_primitive::<Int32Type>()
                .values()
                .iter()
                .copied()
        })
        .collect();
    assert_eq!(values, vec![1, 2, 3, 4, 5, 6, 7, 8]);
}
