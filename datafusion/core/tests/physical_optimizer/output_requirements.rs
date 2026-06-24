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

use datafusion_common::config::ConfigOptions;
use datafusion_expr::execution_props::{ScalarSubqueryResults, SubqueryIndex};
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_optimizer::output_requirements::OutputRequirements;
use datafusion_physical_plan::scalar_subquery::{ScalarSubqueryExec, ScalarSubqueryLink};
use datafusion_physical_plan::{ExecutionPlan, displayable, get_plan_string};

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
