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

//! Adaptive query execution primitive: a root-of-plan wrapper that runs
//! `RuntimeRule`s against the live plan once `PipelineBreakerBuffer`
//! children signal they have absorbed their first batch from every
//! partition.
//!
//! Stub: currently a passthrough. Real coordinator behavior — wait for
//! buffers, run rules, mutate adaptive operators in place via their
//! typed methods, then release the buffers — lands in follow-up commits.
//! Exists today to anchor the plan shape so the insertion rule has
//! somewhere to put a wrapper.
//!
//! Design notes:
//! - `RuntimeOptimizerExec` sits at the root of the plan tree. It owns
//!   the entire subplan and naturally receives every `poll()`. No
//!   ownership inversion, no back-pointers, no plan-tree mutation.
//! - Adaptive optimizations are expressed as `RuntimeRule`s that mutate
//!   operator internal state via typed methods (e.g.
//!   `HashJoinExec::flip_sides`). The plan tree shape stays static
//!   after planning; only operator config gets late-finalized.
//! - Buffer coordination uses two flags: `is_ready` (mechanical, set by
//!   the buffer when all input partitions have produced ≥1 batch) and
//!   `go_ahead` (semantic, set by `RuntimeOptimizerExec` after rules
//!   have run). Default is permissive — buffers release once ready
//!   unless a rule explicitly holds them back.

use std::sync::Arc;

use datafusion_common::Result;
use datafusion_execution::TaskContext;

use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};

#[derive(Debug)]
pub struct RuntimeOptimizerExec {
    input: Arc<dyn ExecutionPlan>,
    cache: Arc<PlanProperties>,
}

impl RuntimeOptimizerExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let cache = Arc::clone(input.properties());
        Self { input, cache }
    }
}

impl DisplayAs for RuntimeOptimizerExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "RuntimeOptimizerExec")
    }
}

impl ExecutionPlan for RuntimeOptimizerExec {
    fn name(&self) -> &'static str {
        "RuntimeOptimizerExec"
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

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Passthrough stub. Real adaptive body lands in follow-up commits.
        self.input.execute(partition, context)
    }
}
