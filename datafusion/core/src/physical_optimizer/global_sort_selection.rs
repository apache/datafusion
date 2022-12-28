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

//! Select the efficient global sort implementation based on sort details.

use std::sync::Arc;

use crate::config::ConfigOptions;
use crate::error::Result;
use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::rewrite::TreeNodeRewritable;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use crate::physical_plan::ExecutionPlan;

/// Currently for a sort operator, if
/// - there are more than one input partitions
/// - and there's some limit which can be pushed down to each of its input partitions
/// then [SortPreservingMergeExec] with local sort with a limit pushed down will be preferred;
/// Otherwise, the normal global sort [SortExec] will be used.
/// Later more intelligent statistics-based decision can also be introduced.
/// For example, for a small data set, the global sort may be efficient enough
#[derive(Default)]
pub struct GlobalSortSelection {}

impl GlobalSortSelection {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for GlobalSortSelection {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(&|plan| {
            Ok(plan
                .as_any()
                .downcast_ref::<SortExec>()
                .and_then(|sort_exec| {
                    if sort_exec.input().output_partitioning().partition_count() > 1
                        && sort_exec.fetch().is_some()
                        // It's already preserving the partitioning so that it can be regarded as a local sort
                        && !sort_exec.preserve_partitioning()
                    {
                        let sort = SortExec::new_with_partitioning(
                            sort_exec.expr().to_vec(),
                            sort_exec.input().clone(),
                            true,
                            sort_exec.fetch(),
                        );
                        let global_sort: Arc<dyn ExecutionPlan> =
                            Arc::new(SortPreservingMergeExec::new(
                                sort_exec.expr().to_vec(),
                                Arc::new(sort),
                            ));
                        Some(global_sort)
                    } else {
                        None
                    }
                }))
        })
    }

    fn name(&self) -> &str {
        "global_sort_selection"
    }

    fn schema_check(&self) -> bool {
        false
    }
}
