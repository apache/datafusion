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

//! Skeleton operator that will eventually range-partition its input on a
//! single order-key into N output partitions, with halo overlap for
//! bounded RANGE-frame window functions sitting above it.
//!
//! This commit is the bare scaffold:
//! - one instance, one cached `PlanProperties` inherited from the input
//! - on the first call to `execute()`, spawn one tokio task per input
//!   partition that awaits `child.runtime_statistics(i)` and logs the
//!   result. This is the connective tissue that the eventual operator
//!   will sit on top of.
//! - `execute(i)` is a pass-through: it forwards to `child.execute(i)`
//!   verbatim, so inserting this operator into a plan is a no-op for
//!   correctness today.

use std::sync::{Arc, Once};

use datafusion_common::Result;
use datafusion_execution::TaskContext;
use log::{info, warn};

use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};

#[derive(Debug)]
pub struct RangeRepartitionExec {
    input: Arc<dyn ExecutionPlan>,
    cache: Arc<PlanProperties>,
    init: Arc<Once>,
}

impl RangeRepartitionExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let cache = Arc::clone(input.properties());
        Self {
            input,
            cache,
            init: Arc::new(Once::new()),
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for RangeRepartitionExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "RangeRepartitionExec")
    }
}

impl ExecutionPlan for RangeRepartitionExec {
    fn name(&self) -> &'static str {
        "RangeRepartitionExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(children.swap_remove(0))))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let n_in = self.input.output_partitioning().partition_count();
        let input = Arc::clone(&self.input);
        self.init.call_once(|| {
            for i in 0..n_in {
                let input_for_task = Arc::clone(&input);
                tokio::spawn(async move {
                    match input_for_task.runtime_statistics(i).await {
                        Ok(stats) => {
                            let col_stats = &stats.column_statistics;
                            info!(
                                "RangeRepartitionExec: input partition {i} runtime stats: \
                                 rows={:?} cols={:?}",
                                stats.num_rows,
                                col_stats
                                    .iter()
                                    .map(|c| (c.min_value.clone(), c.max_value.clone()))
                                    .collect::<Vec<_>>()
                            );
                        }
                        Err(e) => warn!(
                            "RangeRepartitionExec: input partition {i} runtime stats error: {e}"
                        ),
                    }
                });
            }
        });
        self.input.execute(partition, context)
    }
}
