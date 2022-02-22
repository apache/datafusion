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

use crate::state::SchedulerState;
use async_trait::async_trait;
use ballista_core::error::BallistaError;
use ballista_core::execution_plans::ShuffleWriterExec;
use ballista_core::serde::protobuf::TaskDefinition;
use ballista_core::serde::scheduler::to_proto::hash_partitioning_to_proto;
use ballista_core::serde::scheduler::ExecutorData;
use ballista_core::serde::{AsExecutionPlan, AsLogicalPlan};
use log::{error, info};
use tonic::Status;

#[async_trait]
pub trait TaskScheduler {
    async fn fetch_tasks(
        &self,
        available_executors: &mut [ExecutorData],
        job_id: &str,
    ) -> Result<(Vec<Vec<TaskDefinition>>, usize), BallistaError>;
}

#[async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> TaskScheduler
    for SchedulerState<T, U>
{
    async fn fetch_tasks(
        &self,
        available_executors: &mut [ExecutorData],
        job_id: &str,
    ) -> Result<(Vec<Vec<TaskDefinition>>, usize), BallistaError> {
        let mut ret: Vec<Vec<TaskDefinition>> =
            Vec::with_capacity(available_executors.len());
        for _idx in 0..available_executors.len() {
            ret.push(Vec::new());
        }
        let mut num_tasks = 0;
        loop {
            info!("Go inside fetching task loop");
            let mut has_tasks = true;
            for (idx, executor) in available_executors.iter_mut().enumerate() {
                if executor.available_task_slots == 0 {
                    break;
                }
                let plan = self
                    .assign_next_schedulable_job_task(&executor.executor_id, job_id)
                    .await
                    .map_err(|e| {
                        let msg = format!("Error finding next assignable task: {}", e);
                        error!("{}", msg);
                        tonic::Status::internal(msg)
                    })?;
                if let Some((task, _plan)) = &plan {
                    let task_id = task.task_id.as_ref().unwrap();
                    info!(
                        "Sending new task to {}: {}/{}/{}",
                        executor.executor_id,
                        task_id.job_id,
                        task_id.stage_id,
                        task_id.partition_id
                    );
                }
                match plan {
                    Some((status, plan)) => {
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
                            plan,
                            self.get_codec().physical_extension_codec(),
                        )
                        .and_then(|m| m.try_encode(&mut buf))
                        .map_err(|e| {
                            Status::internal(format!(
                                "error serializing execution plan: {:?}",
                                e
                            ))
                        })?;

                        ret[idx].push(TaskDefinition {
                            plan: buf,
                            task_id: status.task_id,
                            output_partitioning: hash_partitioning_to_proto(
                                output_partitioning,
                            )
                            .map_err(|_| Status::internal("TBD".to_string()))?,
                        });
                        executor.available_task_slots -= 1;
                        num_tasks += 1;
                    }
                    _ => {
                        // Indicate there's no more tasks to be scheduled
                        has_tasks = false;
                        break;
                    }
                }
            }
            if !has_tasks {
                break;
            }
            let has_executors =
                available_executors.get(0).unwrap().available_task_slots > 0;
            if !has_executors {
                break;
            }
        }
        Ok((ret, num_tasks))
    }
}
