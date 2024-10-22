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

//! Pushdown the dynamic join filters down to scan execution if there is any

use std::sync::Arc;

use crate::{config::ConfigOptions, error::Result, physical_plan::joins::HashJoinExec};

use crate::datasource::physical_plan::ParquetExec;
use crate::physical_plan::ExecutionPlan;
use datafusion_common::tree_node::{Transformed, TransformedResult};
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::joins::DynamicFilterInfo;

/// this rule used for pushing the build side statistic down to probe phase
#[derive(Default, Debug)]
pub struct JoinFilterPushdown {}

impl JoinFilterPushdown {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for JoinFilterPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !config.optimizer.dynamic_join_pushdown {
            return Ok(plan);
        }
        optimize_impl(plan, &mut None).data()
    }

    fn name(&self) -> &str {
        "JoinFilterPushdown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn optimize_impl(
    plan: Arc<dyn ExecutionPlan>,
    join_filters: &mut Option<Arc<DynamicFilterInfo>>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if let Some(hashjoin_exec) = plan.as_any().downcast_ref::<HashJoinExec>() {
        join_filters.clone_from(&hashjoin_exec.dynamic_filters_pushdown);
        let new_right = optimize_impl(hashjoin_exec.right.clone(), join_filters)?;
        if new_right.transformed {
            let new_hash_join = HashJoinExec::try_new(
                hashjoin_exec.left().clone(),
                new_right.data,
                hashjoin_exec.on.clone(),
                hashjoin_exec.filter().cloned(),
                hashjoin_exec.join_type(),
                hashjoin_exec.projection.clone(),
                *hashjoin_exec.partition_mode(),
                hashjoin_exec.null_equals_null(),
            )?
            .with_dynamic_filter_info(hashjoin_exec.dynamic_filters_pushdown.clone());
            return Ok(Transformed::yes(Arc::new(new_hash_join)));
        }
        Ok(Transformed::no(plan))
    } else if let Some(parquet_exec) = plan.as_any().downcast_ref::<ParquetExec>() {
        if let Some(dynamic_filters) = join_filters {
            let final_exec = parquet_exec
                .clone()
                .with_dynamic_filter(Some(dynamic_filters.clone()));
            return Ok(Transformed::yes(Arc::new(final_exec)));
        }
        Ok(Transformed::no(plan))
    } else {
        let children = plan.children();
        let mut new_children = Vec::with_capacity(children.len());
        let mut transformed = false;

        for child in children {
            let new_child = optimize_impl(child.clone(), join_filters)?;
            if new_child.transformed {
                transformed = true;
            }
            new_children.push(new_child.data);
        }

        if transformed {
            let new_plan = plan.with_new_children(new_children)?;
            Ok(Transformed::yes(new_plan))
        } else {
            Ok(Transformed::no(plan))
        }
    }
}
