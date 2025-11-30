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

//! The [SanityCheckPlan] rule ensures that a given plan can
//! accommodate its infinite sources, if there are any. It will reject
//! non-runnable query plans that use pipeline-breaking operators on
//! infinite input(s). In addition, it will check if all order and
//! distribution requirements of a plan are satisfied by its children.

use std::sync::Arc;

use datafusion_common::Result;
use datafusion_physical_plan::ExecutionPlan;

use datafusion_common::config::OptimizerOptions;
use datafusion_common::plan_err;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_expr::intervals::utils::{check_support, is_datatype_supported};
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::joins::SymmetricHashJoinExec;
use datafusion_physical_plan::{get_plan_string, ExecutionPlanProperties};

use crate::{OptimizerContext, PhysicalOptimizerRule};
use datafusion_physical_expr_common::sort_expr::format_physical_sort_requirement_list;
use itertools::izip;

/// The SanityCheckPlan rule rejects the following query plans:
/// 1. Invalid plans containing nodes whose order and/or distribution requirements
///    are not satisfied by their children.
/// 2. Plans that use pipeline-breaking operators on infinite input(s),
///    it is impossible to execute such queries (they will never generate output nor finish)
#[derive(Default, Debug)]
pub struct SanityCheckPlan {}

impl SanityCheckPlan {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for SanityCheckPlan {
    fn optimize_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &OptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let config = context.session_config().options();
        plan.transform_up(|p| check_plan_sanity(p, &config.optimizer))
            .data()
    }

    fn name(&self) -> &str {
        "SanityCheckPlan"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// This function propagates finiteness information and rejects any plan with
/// pipeline-breaking operators acting on infinite inputs.
pub fn check_finiteness_requirements(
    input: Arc<dyn ExecutionPlan>,
    optimizer_options: &OptimizerOptions,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if let Some(exec) = input.as_any().downcast_ref::<SymmetricHashJoinExec>() {
        if !(optimizer_options.allow_symmetric_joins_without_pruning
            || (exec.check_if_order_information_available()? && is_prunable(exec)))
        {
            return plan_err!("Join operation cannot operate on a non-prunable stream without enabling \
                              the 'allow_symmetric_joins_without_pruning' configuration flag");
        }
    }

    if matches!(
        input.boundedness(),
        Boundedness::Unbounded {
            requires_infinite_memory: true
        }
    ) || (input.boundedness().is_unbounded()
        && input.pipeline_behavior() == EmissionType::Final)
    {
        plan_err!(
            "Cannot execute pipeline breaking queries, operator: {:?}",
            input
        )
    } else {
        Ok(Transformed::no(input))
    }
}

/// This function returns whether a given symmetric hash join is amenable to
/// data pruning. For this to be possible, it needs to have a filter where
/// all involved [`PhysicalExpr`]s, [`Operator`]s and data types support
/// interval calculations.
///
/// [`PhysicalExpr`]: datafusion_physical_plan::PhysicalExpr
/// [`Operator`]: datafusion_expr::Operator
fn is_prunable(join: &SymmetricHashJoinExec) -> bool {
    join.filter().is_some_and(|filter| {
        check_support(filter.expression(), &join.schema())
            && filter
                .schema()
                .fields()
                .iter()
                .all(|f| is_datatype_supported(f.data_type()))
    })
}

/// Ensures that the plan is pipeline friendly and the order and
/// distribution requirements from its children are satisfied.
pub fn check_plan_sanity(
    plan: Arc<dyn ExecutionPlan>,
    optimizer_options: &OptimizerOptions,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    check_finiteness_requirements(Arc::clone(&plan), optimizer_options)?;

    for ((idx, child), sort_req, dist_req) in izip!(
        plan.children().into_iter().enumerate(),
        plan.required_input_ordering(),
        plan.required_input_distribution(),
    ) {
        let child_eq_props = child.equivalence_properties();
        if let Some(sort_req) = sort_req {
            let sort_req = sort_req.into_single();
            if !child_eq_props.ordering_satisfy_requirement(sort_req.clone())? {
                let plan_str = get_plan_string(&plan);
                return plan_err!(
                    "Plan: {:?} does not satisfy order requirements: {}. Child-{} order: {}",
                    plan_str,
                    format_physical_sort_requirement_list(&sort_req),
                    idx,
                    child_eq_props.oeq_class()
                );
            }
        }

        if !child
            .output_partitioning()
            .satisfy(&dist_req, child_eq_props)
        {
            let plan_str = get_plan_string(&plan);
            return plan_err!(
                "Plan: {:?} does not satisfy distribution requirements: {}. Child-{} output partitioning: {}",
                plan_str,
                dist_req,
                idx,
                child.output_partitioning()
            );
        }
    }

    Ok(Transformed::no(plan))
}

// See tests in datafusion/core/tests/physical_optimizer
