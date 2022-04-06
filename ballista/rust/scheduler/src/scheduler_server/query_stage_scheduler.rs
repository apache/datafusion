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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_recursion::async_recursion;
use async_trait::async_trait;
use log::{debug, error, info, warn};

use ballista_core::error::{BallistaError, Result};

use ballista_core::event_loop::{EventAction, EventSender};
use ballista_core::execution_plans::UnresolvedShuffleExec;
use ballista_core::serde::protobuf::{
    job_status, task_status, CompletedJob, CompletedTask, FailedJob, FailedTask,
    JobStatus, RunningJob, TaskStatus,
};
use ballista_core::serde::scheduler::{ExecutorMetadata, PartitionStats};
use ballista_core::serde::{protobuf, AsExecutionPlan, AsLogicalPlan};
use datafusion::physical_plan::ExecutionPlan;

use crate::planner::{
    find_unresolved_shuffles, remove_unresolved_shuffles, DistributedPlanner,
};
use crate::scheduler_server::event::{QueryStageSchedulerEvent, SchedulerServerEvent};
use crate::state::SchedulerState;

pub(crate) struct QueryStageScheduler<
    T: 'static + AsLogicalPlan,
    U: 'static + AsExecutionPlan,
> {
    state: Arc<SchedulerState<T, U>>,
    event_sender: Option<EventSender<SchedulerServerEvent>>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> QueryStageScheduler<T, U> {
    pub(crate) fn new(
        state: Arc<SchedulerState<T, U>>,
        event_sender: Option<EventSender<SchedulerServerEvent>>,
    ) -> Self {
        Self {
            state,
            event_sender,
        }
    }

    async fn generate_stages(
        &self,
        job_id: &str,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<()> {
        let mut planner = DistributedPlanner::new();
        // The last one is the final stage
        let stages = planner.plan_query_stages(job_id, plan).await.map_err(|e| {
            let msg = format!("Could not plan query stages: {}", e);
            error!("{}", msg);
            BallistaError::General(msg)
        })?;

        let mut stages_dependency: HashMap<u32, HashSet<u32>> = HashMap::new();
        // save stages into state
        for shuffle_writer in stages.iter() {
            let stage_id = shuffle_writer.stage_id();
            let stage_plan: Arc<dyn ExecutionPlan> = shuffle_writer.clone();
            self.state
                .save_stage_plan(job_id, stage_id, stage_plan.clone())
                .await
                .map_err(|e| {
                    let msg = format!("Could not save stage plan: {}", e);
                    error!("{}", msg);
                    BallistaError::General(msg)
                })?;

            for child in find_unresolved_shuffles(&stage_plan)? {
                stages_dependency
                    .entry(child.stage_id as u32)
                    .or_insert_with(HashSet::new)
                    .insert(stage_id as u32);
            }
        }

        self.state
            .stage_manager
            .add_stages_dependency(job_id, stages_dependency);

        let final_stage_id = stages.last().as_ref().unwrap().stage_id();
        self.state
            .stage_manager
            .add_final_stage(job_id, final_stage_id as u32);
        self.submit_stage(job_id, final_stage_id).await?;

        Ok(())
    }

    async fn submit_pending_stages(&self, job_id: &str, stage_id: usize) -> Result<()> {
        if let Some(parent_stages) = self
            .state
            .stage_manager
            .get_parent_stages(job_id, stage_id as u32)
        {
            self.state
                .stage_manager
                .remove_pending_stage(job_id, &parent_stages);
            for parent_stage in parent_stages {
                self.submit_stage(job_id, parent_stage as usize).await?;
            }
        }

        Ok(())
    }

    #[async_recursion]
    async fn submit_stage(&self, job_id: &str, stage_id: usize) -> Result<()> {
        {
            if self
                .state
                .stage_manager
                .is_running_stage(job_id, stage_id as u32)
            {
                debug!("stage {}/{} has already been submitted", job_id, stage_id);
                return Ok(());
            }
            if self
                .state
                .stage_manager
                .is_pending_stage(job_id, stage_id as u32)
            {
                debug!(
                    "stage {}/{} has already been added to the pending list",
                    job_id, stage_id
                );
                return Ok(());
            }
        }
        if let Some(stage_plan) = self.state.get_stage_plan(job_id, stage_id) {
            if let Some(incomplete_unresolved_shuffles) = self
                .try_resolve_stage(job_id, stage_id, stage_plan.clone())
                .await?
            {
                assert!(
                    !incomplete_unresolved_shuffles.is_empty(),
                    "there are no incomplete unresolved shuffles"
                );
                for incomplete_unresolved_shuffle in incomplete_unresolved_shuffles {
                    self.submit_stage(job_id, incomplete_unresolved_shuffle.stage_id)
                        .await?;
                }
                self.state
                    .stage_manager
                    .add_pending_stage(job_id, stage_id as u32);
            } else {
                self.state.stage_manager.add_running_stage(
                    job_id,
                    stage_id as u32,
                    stage_plan.output_partitioning().partition_count() as u32,
                );
            }
        } else {
            return Err(BallistaError::General(format!(
                "Fail to find stage plan for {}/{}",
                job_id, stage_id
            )));
        }
        Ok(())
    }

    /// Try to resolve a stage if all of the unresolved shuffles are completed.
    /// Return the unresolved shuffles which are incomplete
    async fn try_resolve_stage(
        &self,
        job_id: &str,
        stage_id: usize,
        stage_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Option<Vec<UnresolvedShuffleExec>>> {
        // Find all of the unresolved shuffles
        let unresolved_shuffles = find_unresolved_shuffles(&stage_plan)?;

        // If no dependent shuffles
        if unresolved_shuffles.is_empty() {
            return Ok(None);
        }

        // Find all of the incomplete unresolved shuffles
        let (incomplete_unresolved_shuffles, unresolved_shuffles): (
            Vec<UnresolvedShuffleExec>,
            Vec<UnresolvedShuffleExec>,
        ) = unresolved_shuffles.into_iter().partition(|s| {
            !self
                .state
                .stage_manager
                .is_completed_stage(job_id, s.stage_id as u32)
        });

        if !incomplete_unresolved_shuffles.is_empty() {
            return Ok(Some(incomplete_unresolved_shuffles));
        }

        // All of the unresolved shuffles are completed, update the stage plan
        {
            let mut partition_locations: HashMap<
                usize, // input stage id
                HashMap<
                    usize,                                                   // task id of this stage
                    Vec<ballista_core::serde::scheduler::PartitionLocation>, // shuffle partitions
                >,
            > = HashMap::new();
            for unresolved_shuffle in unresolved_shuffles.iter() {
                let input_stage_id = unresolved_shuffle.stage_id;
                let stage_shuffle_partition_locations = partition_locations
                    .entry(input_stage_id)
                    .or_insert_with(HashMap::new);
                if let Some(input_stage_tasks) = self
                    .state
                    .stage_manager
                    .get_stage_tasks(job_id, input_stage_id as u32)
                {
                    // each input partition can produce multiple output partitions
                    for (shuffle_input_partition_id, task_status) in
                        input_stage_tasks.iter().enumerate()
                    {
                        match &task_status.status {
                            Some(task_status::Status::Completed(CompletedTask {
                                executor_id,
                                partitions,
                            })) => {
                                debug!(
                                    "Task for unresolved shuffle input partition {} completed and produced these shuffle partitions:\n\t{}",
                                    shuffle_input_partition_id,
                                    partitions.iter().map(|p| format!("{}={}", p.partition_id, &p.path)).collect::<Vec<_>>().join("\n\t")
                                );

                                for shuffle_write_partition in partitions {
                                    let temp = stage_shuffle_partition_locations
                                        .entry(
                                            shuffle_write_partition.partition_id as usize,
                                        )
                                        .or_insert(Vec::new());
                                    let executor_meta = self
                                        .state
                                        .get_executor_metadata(executor_id)
                                        .ok_or_else(|| {
                                            BallistaError::General(format!(
                                                "Fail to find executor metadata for {}",
                                                &executor_id
                                            ))
                                        })?;
                                    let partition_location =
                                        ballista_core::serde::scheduler::PartitionLocation {
                                            partition_id:
                                            ballista_core::serde::scheduler::PartitionId {
                                                job_id: job_id.to_owned(),
                                                stage_id: unresolved_shuffle.stage_id,
                                                partition_id: shuffle_write_partition
                                                    .partition_id
                                                    as usize,
                                            },
                                            executor_meta,
                                            partition_stats: PartitionStats::new(
                                                Some(shuffle_write_partition.num_rows),
                                                Some(shuffle_write_partition.num_batches),
                                                Some(shuffle_write_partition.num_bytes),
                                            ),
                                            path: shuffle_write_partition.path.clone(),
                                        };
                                    debug!(
                                            "Scheduler storing stage {} output partition {} path: {}",
                                            unresolved_shuffle.stage_id,
                                            partition_location.partition_id.partition_id,
                                            partition_location.path
                                        );
                                    temp.push(partition_location);
                                }
                            }
                            _ => {
                                debug!(
                                    "Stage {} input partition {} has not completed yet",
                                    unresolved_shuffle.stage_id,
                                    shuffle_input_partition_id
                                );
                                // TODO task error handling
                            }
                        }
                    }
                } else {
                    return Err(BallistaError::General(format!(
                        "Fail to find completed stage for {}/{}",
                        job_id, stage_id
                    )));
                }
            }

            let plan = remove_unresolved_shuffles(stage_plan, &partition_locations)?;
            self.state.save_stage_plan(job_id, stage_id, plan).await?;
        }

        Ok(None)
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
                match self.generate_stages(&job_id, plan).await {
                    Err(e) => {
                        let msg = format!("Job {} failed due to {}", job_id, e);
                        warn!("{}", msg);
                        self.state
                            .save_job_metadata(
                                &job_id,
                                &JobStatus {
                                    status: Some(job_status::Status::Failed(FailedJob {
                                        error: msg.to_string(),
                                    })),
                                },
                            )
                            .await?;
                    }
                    Ok(()) => {
                        if let Err(e) = self
                            .state
                            .save_job_metadata(
                                &job_id,
                                &JobStatus {
                                    status: Some(job_status::Status::Running(
                                        RunningJob {},
                                    )),
                                },
                            )
                            .await
                        {
                            warn!(
                                "Could not update job {} status to running: {}",
                                job_id, e
                            );
                        }
                    }
                }
            }
            QueryStageSchedulerEvent::StageFinished(job_id, stage_id) => {
                info!("Job stage {}/{} finished", job_id, stage_id);
                self.submit_pending_stages(&job_id, stage_id as usize)
                    .await?;
            }
            QueryStageSchedulerEvent::JobFinished(job_id) => {
                info!("Job {} finished", job_id);
                let tasks_for_complete_final_stage = self
                    .state
                    .stage_manager
                    .get_tasks_for_complete_final_stage(&job_id)?;
                let executors: HashMap<String, ExecutorMetadata> = self
                    .state
                    .get_executors_metadata()
                    .await?
                    .into_iter()
                    .map(|(meta, _)| (meta.id.to_string(), meta))
                    .collect();
                let job_status = get_job_status_from_tasks(
                    &tasks_for_complete_final_stage,
                    &executors,
                );
                self.state.save_job_metadata(&job_id, &job_status).await?;
            }
            QueryStageSchedulerEvent::JobFailed(job_id, stage_id, fail_message) => {
                error!(
                    "Job stage {}/{} failed due to {}",
                    &job_id, stage_id, fail_message
                );
                let job_status = JobStatus {
                    status: Some(job_status::Status::Failed(FailedJob {
                        error: fail_message,
                    })),
                };
                self.state.save_job_metadata(&job_id, &job_status).await?;
            }
        }

        if let Some(event_sender) = self.event_sender.as_ref() {
            // The stage event must triggerred with releasing some resources. Therefore, revive offers for the scheduler
            event_sender
                .post_event(SchedulerServerEvent::ReviveOffers(1))
                .await?;
        };
        Ok(None)
    }

    // TODO
    fn on_error(&self, _error: BallistaError) {}
}

fn get_job_status_from_tasks(
    tasks: &[Arc<TaskStatus>],
    executors: &HashMap<String, ExecutorMetadata>,
) -> JobStatus {
    let mut job_status = tasks
        .iter()
        .map(|task| match &task.status {
            Some(task_status::Status::Completed(CompletedTask {
                executor_id,
                partitions,
            })) => Ok((task, executor_id, partitions)),
            _ => Err(BallistaError::General("Task not completed".to_string())),
        })
        .collect::<Result<Vec<_>>>()
        .ok()
        .map(|info| {
            let mut partition_location = vec![];
            for (status, executor_id, partitions) in info {
                let input_partition_id = status.task_id.as_ref().unwrap(); // TODO unwrap
                let executor_meta = executors.get(executor_id).map(|e| e.clone().into());
                for shuffle_write_partition in partitions {
                    let shuffle_input_partition_id = Some(protobuf::PartitionId {
                        job_id: input_partition_id.job_id.clone(),
                        stage_id: input_partition_id.stage_id,
                        partition_id: input_partition_id.partition_id,
                    });
                    partition_location.push(protobuf::PartitionLocation {
                        partition_id: shuffle_input_partition_id.clone(),
                        executor_meta: executor_meta.clone(),
                        partition_stats: Some(protobuf::PartitionStats {
                            num_batches: shuffle_write_partition.num_batches as i64,
                            num_rows: shuffle_write_partition.num_rows as i64,
                            num_bytes: shuffle_write_partition.num_bytes as i64,
                            column_stats: vec![],
                        }),
                        path: shuffle_write_partition.path.clone(),
                    });
                }
            }
            job_status::Status::Completed(CompletedJob { partition_location })
        });

    if job_status.is_none() {
        // Update other statuses
        for task in tasks.iter() {
            if let Some(task_status::Status::Failed(FailedTask { error })) = &task.status
            {
                let error = error.clone();
                job_status = Some(job_status::Status::Failed(FailedJob { error }));
                break;
            }
        }
    }

    JobStatus {
        status: Some(job_status.unwrap_or(job_status::Status::Running(RunningJob {}))),
    }
}
