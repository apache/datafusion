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
use std::time::Instant;

use async_trait::async_trait;
use log::{debug, error, info, warn};
use tokio::sync::RwLock;

use ballista_core::error::{BallistaError, Result};
use ballista_core::event_loop::{EventAction, EventSender};
use ballista_core::serde::protobuf::{
    job_status, JobStatus, PartitionId, RunningJob, TaskStatus,
};
use ballista_core::serde::{AsExecutionPlan, AsLogicalPlan};
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::ExecutionContext;

use crate::planner::DistributedPlanner;
use crate::scheduler_server::event_loop::SchedulerServerEvent;
use crate::state::SchedulerState;

#[derive(Clone)]
pub enum QueryStageSchedulerEvent {
    JobSubmitted(String, Box<LogicalPlan>),
}

pub(crate) struct QueryStageScheduler<
    T: 'static + AsLogicalPlan,
    U: 'static + AsExecutionPlan,
> {
    ctx: Arc<RwLock<ExecutionContext>>,
    state: Arc<SchedulerState<T, U>>,
    event_sender: Option<EventSender<SchedulerServerEvent>>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> QueryStageScheduler<T, U> {
    pub(crate) fn new(
        ctx: Arc<RwLock<ExecutionContext>>,
        state: Arc<SchedulerState<T, U>>,
        event_sender: Option<EventSender<SchedulerServerEvent>>,
    ) -> Self {
        Self {
            ctx,
            state,
            event_sender,
        }
    }

    async fn create_physical_plan(
        &self,
        plan: Box<LogicalPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let start = Instant::now();

        let ctx = self.ctx.read().await.clone();
        let optimized_plan = ctx.optimize(plan.as_ref()).map_err(|e| {
            let msg = format!("Could not create optimized logical plan: {}", e);
            error!("{}", msg);
            BallistaError::General(msg)
        })?;

        debug!("Calculated optimized plan: {:?}", optimized_plan);

        let plan = ctx
            .create_physical_plan(&optimized_plan)
            .await
            .map_err(|e| {
                let msg = format!("Could not create physical plan: {}", e);
                error!("{}", msg);
                BallistaError::General(msg)
            });

        info!(
            "DataFusion created physical plan in {} milliseconds",
            start.elapsed().as_millis()
        );

        plan
    }

    async fn generate_stages(
        &self,
        job_id: &str,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<()> {
        let mut planner = DistributedPlanner::new();
        let stages = planner.plan_query_stages(job_id, plan).await.map_err(|e| {
            let msg = format!("Could not plan query stages: {}", e);
            error!("{}", msg);
            BallistaError::General(msg)
        })?;

        // save stages into state
        for shuffle_writer in stages {
            self.state
                .save_stage_plan(
                    job_id,
                    shuffle_writer.stage_id(),
                    shuffle_writer.clone(),
                )
                .await
                .map_err(|e| {
                    let msg = format!("Could not save stage plan: {}", e);
                    error!("{}", msg);
                    BallistaError::General(msg)
                })?;
            let num_partitions = shuffle_writer.output_partitioning().partition_count();
            for partition_id in 0..num_partitions {
                let pending_status = TaskStatus {
                    task_id: Some(PartitionId {
                        job_id: job_id.to_owned(),
                        stage_id: shuffle_writer.stage_id() as u32,
                        partition_id: partition_id as u32,
                    }),
                    status: None,
                };
                self.state
                    .save_task_status(&pending_status)
                    .await
                    .map_err(|e| {
                        let msg = format!("Could not save task status: {}", e);
                        error!("{}", msg);
                        BallistaError::General(msg)
                    })?;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>
    EventAction<QueryStageSchedulerEvent> for QueryStageScheduler<T, U>
{
    // TODO
    fn on_start(&self) {}

    // TODO
    fn on_stop(&self) {}

    async fn on_receive(
        &self,
        event: QueryStageSchedulerEvent,
    ) -> Result<Option<QueryStageSchedulerEvent>> {
        match event {
            QueryStageSchedulerEvent::JobSubmitted(job_id, plan) => {
                info!("Job {} submitted", job_id);
                let plan = self.create_physical_plan(plan).await?;
                if let Err(e) = self
                    .state
                    .save_job_metadata(
                        &job_id,
                        &JobStatus {
                            status: Some(job_status::Status::Running(RunningJob {})),
                        },
                    )
                    .await
                {
                    warn!("Could not update job {} status to running: {}", job_id, e);
                }
                self.generate_stages(&job_id, plan).await?;

                if let Some(event_sender) = self.event_sender.as_ref() {
                    // Send job_id to the scheduler channel
                    event_sender
                        .post_event(SchedulerServerEvent::JobSubmitted(job_id))
                        .await?;
                };
            }
        }
        Ok(None)
    }

    // TODO
    fn on_error(&self, _error: BallistaError) {}
}
