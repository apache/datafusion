// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use itertools::izip;
use std::sync::Arc;

use crate::error::Result;
use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::ExecutionPlan;

use datafusion_common::config::ConfigOptions;
use datafusion_common::plan_err;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_plan::ExecutionPlanProperties;

#[derive(Default)]
pub struct SanityCheckPlan {}

impl SanityCheckPlan {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for SanityCheckPlan {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|p| check_plan_sanity(p)).data()
    }

    fn name(&self) -> &str {
        "SanityCheckPlan"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Ensures that the plan is pipeline friendly and the order and
/// distribution requirements from its children are satisfied.
pub fn check_plan_sanity(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if !plan.execution_mode().pipeline_friendly() {
        return plan_err!("Plan {:?} is not pipeline friendly.", plan);
    }

    for (child, child_sort_req, child_dist_req) in izip!(
        plan.children().iter(),
        plan.required_input_ordering().iter(),
        plan.required_input_distribution().iter()
    ) {
        let child_eq_props = child.equivalence_properties();
        match child_sort_req {
            None => (),
            Some(child_sort_req) => {
                if !child_eq_props.ordering_satisfy_requirement(&child_sort_req) {
                    return plan_err!(
                        "Child: {:?} does not satisfy parent order requirements",
                        child
                    );
                }
            }
        };

        if !child
            .output_partitioning()
            .satisfy(&child_dist_req, child_eq_props)
        {
            return plan_err!(
                "Child: {:?} does not satisfy parent distribution requirements",
                child
            );
        }
    }

    Ok(Transformed::no(plan))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::physical_optimizer::test_utils::{
        bounded_window_exec, global_limit_exec, local_limit_exec, memory_exec,
        repartition_exec, sort_exec, sort_expr_options,
    };
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::Result;
    use datafusion_physical_plan::displayable;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("c9", DataType::Int32, true)]))
    }

    fn assert_sanity_check(plan: &Arc<dyn ExecutionPlan>, is_sane: bool) {
        let sanity_checker = SanityCheckPlan::new();
        let opts = ConfigOptions::default();
        assert_eq!(
            sanity_checker.optimize(plan.clone(), &opts).is_ok(),
            is_sane
        );
    }

    /// Check if the plan we created is as expected by comparing the plan
    /// formatted as a string.
    fn assert_plan(plan: &dyn ExecutionPlan, expected_lines: Vec<&str>) {
        let plan_str = displayable(plan).indent(true).to_string();
        let actual_lines: Vec<&str> = plan_str.trim().lines().collect();
        assert_eq!(actual_lines, expected_lines);
    }

    #[tokio::test]
    /// Tests that plan is valid when the sort requirements are satisfied.
    async fn test_bounded_window_agg_sort_requirement() -> Result<()> {
        let schema = create_test_schema();
        let source = memory_exec(&schema);
        let sort_exprs = vec![sort_expr_options(
            "c9",
            &source.schema(),
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        )];
        let sort = sort_exec(sort_exprs.clone(), source);
        let bw = bounded_window_exec("c9", sort_exprs, sort);
        assert_plan(bw.as_ref(), vec![
            "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
            "  SortExec: expr=[c9@0 ASC NULLS LAST], preserve_partitioning=[false]",
            "    MemoryExec: partitions=1, partition_sizes=[0]"
        ]);
        assert_sanity_check(&bw, true);
        Ok(())
    }

    #[tokio::test]
    /// Tests that plan is invalid when the sort requirements are not satisfied.
    async fn test_bounded_window_agg_no_sort_requirement() -> Result<()> {
        let schema = create_test_schema();
        let source = memory_exec(&schema);
        let sort_exprs = vec![sort_expr_options(
            "c9",
            &source.schema(),
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        )];
        let bw = bounded_window_exec("c9", sort_exprs, source);
        assert_plan(bw.as_ref(), vec![
            "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
            "  MemoryExec: partitions=1, partition_sizes=[0]"
        ]);
        assert_sanity_check(&bw, false);
        Ok(())
    }

    #[tokio::test]
    /// Tests that plan is valid when a single partition requirement
    /// is satisfied.
    async fn test_global_limit_single_partition() -> Result<()> {
        let schema = create_test_schema();
        let source = memory_exec(&schema);
        let limit = global_limit_exec(source);

        assert_plan(
            limit.as_ref(),
            vec![
                "GlobalLimitExec: skip=0, fetch=100",
                "  MemoryExec: partitions=1, partition_sizes=[0]",
            ],
        );
        assert_sanity_check(&limit, true);
        Ok(())
    }

    #[tokio::test]
    /// Tests that plan is invalid when a single partition requirement
    /// is not satisfied.
    async fn test_global_limit_multi_partition() -> Result<()> {
        let schema = create_test_schema();
        let source = memory_exec(&schema);
        let limit = global_limit_exec(repartition_exec(source));

        assert_plan(
            limit.as_ref(),
            vec![
                "GlobalLimitExec: skip=0, fetch=100",
                "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                "    MemoryExec: partitions=1, partition_sizes=[0]",
            ],
        );
        assert_sanity_check(&limit, false);
        Ok(())
    }

    #[tokio::test]
    /// Tests that when a plan has no requirements it is valid.
    async fn test_local_limit() -> Result<()> {
        let schema = create_test_schema();
        let source = memory_exec(&schema);
        let limit = local_limit_exec(source);

        assert_plan(
            limit.as_ref(),
            vec![
                "LocalLimitExec: fetch=100",
                "  MemoryExec: partitions=1, partition_sizes=[0]",
            ],
        );
        assert_sanity_check(&limit, true);
        Ok(())
    }
}
