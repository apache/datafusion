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

//! A [`StageGraph`] is a physical plan split into [`QueryStage`]s at shuffle
//! boundaries. Each stage other than the final one is rooted at an
//! `ExchangeSinkExec` (its "producer" tasks push IPC frames into the exchange);
//! the final stage is the plan that produces the query's overall result and is
//! not wrapped in a sink. A stage's `input_stage_ids` name the upstream stages
//! whose exchange output it reads via `ExchangeSourceExec` leaves.

use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;

/// Identifies a [`QueryStage`] within a [`StageGraph`]. Stable only within a
/// single `StageGraph` — not a global identifier.
pub type StageId = usize;

/// One stage of a [`StageGraph`]: a subtree of the original physical plan,
/// plus the bookkeeping needed to schedule and connect it to its inputs.
pub struct QueryStage {
    /// This stage's id, unique within its `StageGraph`.
    pub id: StageId,
    /// The stage's root plan. For every stage but the final one, this is an
    /// `ExchangeSinkExec`; the final stage's plan is the rewritten original
    /// plan with no sink wrapping.
    pub plan: Arc<dyn ExecutionPlan>,
    /// Number of output partitions this stage produces (i.e. how a
    /// downstream `ExchangeSourceExec` should be sized). For the final stage
    /// this is simply `plan`'s own output partition count.
    pub output_partition_count: usize,
    /// Ids of upstream stages this stage reads from via `ExchangeSourceExec`
    /// leaves in `plan`.
    #[cfg_attr(not(test), expect(dead_code))]
    pub input_stage_ids: Vec<StageId>,
}

/// A physical plan split into [`QueryStage`]s at shuffle boundaries.
///
/// Stages are ordered so that a producer's id is always lower than the ids
/// of the consumer stages that read from it.
pub struct StageGraph {
    pub stages: Vec<QueryStage>,
    /// Id of the stage that produces the overall query result.
    pub final_stage_id: StageId,
}
