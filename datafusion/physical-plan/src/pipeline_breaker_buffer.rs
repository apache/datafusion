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

//! Synchronization wrapper above a pipeline-breaking operator. Reports
//! when the breaker has finished its barrier (one batch absorbed from
//! every input partition) and holds its output until [`RuntimeOptimizerExec`]
//! signals `go_ahead`.
//!
//! Stub: currently a passthrough. The two-flag synchronization protocol
//! (`is_ready` set mechanically when all input partitions have produced
//! ≥1 batch; `go_ahead` set semantically by the root coordinator after
//! `Vec<RuntimeRule>` has run) lands in a follow-up commit. Exists today
//! to anchor the plan shape so the insertion rule has a stable wrapper
//! to put above each breaker.

use std::sync::Arc;

use datafusion_common::Result;
use datafusion_execution::TaskContext;

use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};

#[derive(Debug)]
pub struct PipelineBreakerBuffer {
    input: Arc<dyn ExecutionPlan>,
    cache: Arc<PlanProperties>,
}

impl PipelineBreakerBuffer {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let cache = Arc::clone(input.properties());
        Self { input, cache }
    }
}

impl DisplayAs for PipelineBreakerBuffer {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "PipelineBreakerBuffer")
    }
}

impl ExecutionPlan for PipelineBreakerBuffer {
    fn name(&self) -> &'static str {
        "PipelineBreakerBuffer"
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
        // Passthrough stub. Real two-flag synchronization lands next.
        self.input.execute(partition, context)
    }
}
