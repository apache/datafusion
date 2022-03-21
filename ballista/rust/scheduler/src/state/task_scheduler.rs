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

use crate::state::stage_manager::StageKey;
use crate::state::SchedulerState;
use async_trait::async_trait;
use ballista_core::error::BallistaError;
use ballista_core::execution_plans::ShuffleWriterExec;
use ballista_core::serde::protobuf::{
    job_status, task_status, FailedJob, RunningTask, TaskDefinition, TaskStatus,
};
use ballista_core::serde::scheduler::to_proto::hash_partitioning_to_proto;
use ballista_core::serde::scheduler::{ExecutorData, PartitionId};
use ballista_core::serde::{AsExecutionPlan, AsLogicalPlan};
use log::{debug, info};

#[async_trait]
pub trait TaskScheduler {
    // For each round, it will fetch tasks from one stage
    async fn fetch_schedulable_tasks(
        &self,
        available_executors: &mut [ExecutorData],
        n_round: u32,
    ) -> Result<(Vec<Vec<TaskDefinition>>, usize), BallistaError>;
}

pub trait StageScheduler {
    fn fetch_schedulable_stage<F>(&self, cond: F) -> Option<StageKey>
    where
        F: Fn(&StageKey) -> bool;
}

#[async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> TaskScheduler
    for SchedulerState<T, U>
{
    async fn fetch_schedulable_tasks(
        &self,
        available_executors: &mut [ExecutorData],
        n_round: u32,
    ) -> Result<(Vec<Vec<TaskDefinition>>, usize), BallistaError> {
        let mut ret: Vec<Vec<TaskDefinition>> =
            Vec::with_capacity(available_executors.len());
        let mut max_task_num = 0u32;
        for executor in available_executors.iter() {
            ret.push(Vec::new());
            max_task_num += executor.available_task_slots;
        }

        let mut tasks_status = vec![];
        let mut has_resources = true;
        for i in 0..n_round {
            if !has_resources {
                break;
            }
            let mut num_tasks = 0;
            // For each round, it will fetch tasks from one stage
            if let Some((job_id, stage_id, tasks)) =
                self.stage_manager.fetch_pending_tasks(
                    max_task_num as usize - tasks_status.len(),
                    |stage_key| {
                        // Don't scheduler stages for jobs with error status
                        if let Some(job_meta) = self.get_job_metadata(&stage_key.0) {
                            if !matches!(
                                &job_meta.status,
                                Some(job_status::Status::Failed(FailedJob { error: _ }))
                            ) {
                                true
                            } else {
                                info!("Stage {}/{} not to be scheduled due to its job failed", stage_key.0, stage_key.1);
                                false
                            }
                        } else {
                            false
                        }
                    },
                )
            {
                let plan =
                    self.get_stage_plan(&job_id, stage_id as usize)
                        .ok_or_else(|| {
                            BallistaError::General(format!(
                                "Fail to find execution plan for stage {}/{}",
                                job_id, stage_id
                            ))
                        })?;
                loop {
                    debug!("Go inside fetching task loop for stage {}/{}", job_id, stage_id);

                    let mut has_tasks = true;
                    for (idx, executor) in available_executors.iter_mut().enumerate() {
                        if executor.available_task_slots == 0 {
                            has_resources = false;
                            break;
                        }

                        if num_tasks >= tasks.len() {
                            has_tasks = false;
                            break;
                        }

                        let task_id = PartitionId {
                            job_id: job_id.clone(),
                            stage_id: stage_id as usize,
                            partition_id: tasks[num_tasks] as usize,
                        };

                        let task_id = Some(task_id.into());
                        let running_task = TaskStatus {
                            task_id: task_id.clone(),
                            status: Some(task_status::Status::Running(RunningTask {
                                executor_id: executor.executor_id.to_owned(),
                            })),
                        };
                        tasks_status.push(running_task);

                        let plan_clone = plan.clone();
                        let output_partitioning = if let Some(shuffle_writer) =
                            plan_clone.as_any().downcast_ref::<ShuffleWriterExec>()
                        {
                            shuffle_writer.shuffle_output_partitioning()
                        } else {
                            return Err(BallistaError::General(format!(
                                "Task root plan was not a ShuffleWriterExec: {:?}",
                                plan_clone
                            )));
                        };

                        let mut buf: Vec<u8> = vec![];
                        U::try_from_physical_plan(
                            plan.clone(),
                            self.get_codec().physical_extension_codec(),
                        )
                        .and_then(|m| m.try_encode(&mut buf))
                        .map_err(|e| {
                            tonic::Status::internal(format!(
                                "error serializing execution plan: {:?}",
                                e
                            ))
                        })?;

                        ret[idx].push(TaskDefinition {
                            plan: buf,
                            task_id,
                            output_partitioning: hash_partitioning_to_proto(
                                output_partitioning,
                            )
                            .map_err(|_| tonic::Status::internal("TBD".to_string()))?,
                        });
                        executor.available_task_slots -= 1;
                        num_tasks += 1;
                    }
                    if !has_tasks {
                        break;
                    }
                    if !has_resources {
                        break;
                    }
                }
            }
            if !has_resources {
                info!(
                    "Not enough resource for task running. Stopped at round {}",
                    i
                );
                break;
            }
        }

        let total_task_num = tasks_status.len();
        info!("{} tasks to be scheduled", total_task_num);

        // No need to deal with the stage event, since the task status is changing from pending to running
        self.stage_manager.update_tasks_status(tasks_status);

        Ok((ret, total_task_num))
    }
}
