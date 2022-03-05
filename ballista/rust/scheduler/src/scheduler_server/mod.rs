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

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use log::{debug, error, info, warn};
use tonic::Status;

use crate::state::{ConfigBackendClient, SchedulerState};
use ballista_core::config::{BallistaConfig, TaskSchedulingPolicy};
use ballista_core::error::BallistaError;
use ballista_core::execution_plans::ShuffleWriterExec;
use ballista_core::serde::protobuf::executor_grpc_client::ExecutorGrpcClient;
use ballista_core::serde::protobuf::{LaunchTaskParams, TaskDefinition};
use ballista_core::serde::scheduler::to_proto::hash_partitioning_to_proto;
use ballista_core::serde::scheduler::ExecutorData;
use ballista_core::serde::{AsExecutionPlan, AsLogicalPlan, BallistaCodec};
use datafusion::prelude::{ExecutionConfig, ExecutionContext};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, RwLock};
use tonic::transport::Channel;

// include the generated protobuf source as a submodule
#[allow(clippy::all)]
pub mod externalscaler {
    include!(concat!(env!("OUT_DIR"), "/externalscaler.rs"));
}

mod external_scaler;
mod grpc;

type ExecutorsClient = Arc<RwLock<HashMap<String, ExecutorGrpcClient<Channel>>>>;

#[derive(Clone)]
pub struct SchedulerServer<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    pub(crate) state: Arc<SchedulerState<T, U>>,
    pub start_time: u128,
    policy: TaskSchedulingPolicy,
    scheduler_env: Option<SchedulerEnv>,
    executors_client: Option<ExecutorsClient>,
    ctx: Arc<RwLock<ExecutionContext>>,
    codec: BallistaCodec<T, U>,
}

#[derive(Clone)]
pub struct SchedulerEnv {
    pub tx_job: mpsc::Sender<String>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> SchedulerServer<T, U> {
    pub fn new(
        config: Arc<dyn ConfigBackendClient>,
        namespace: String,
        ctx: Arc<RwLock<ExecutionContext>>,
        codec: BallistaCodec<T, U>,
    ) -> Self {
        SchedulerServer::new_with_policy(
            config,
            namespace,
            TaskSchedulingPolicy::PullStaged,
            None,
            ctx,
            codec,
        )
    }

    pub fn new_with_policy(
        config: Arc<dyn ConfigBackendClient>,
        namespace: String,
        policy: TaskSchedulingPolicy,
        scheduler_env: Option<SchedulerEnv>,
        ctx: Arc<RwLock<ExecutionContext>>,
        codec: BallistaCodec<T, U>,
    ) -> Self {
        let state = Arc::new(SchedulerState::new(config, namespace, codec.clone()));

        let executors_client = if matches!(policy, TaskSchedulingPolicy::PushStaged) {
            Some(Arc::new(RwLock::new(HashMap::new())))
        } else {
            None
        };
        Self {
            state,
            start_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            policy,
            scheduler_env,
            executors_client,
            ctx,
            codec,
        }
    }

    pub async fn init(&self) -> Result<(), BallistaError> {
        let ctx = self.ctx.read().await;
        self.state.init(&ctx).await?;

        Ok(())
    }

    async fn schedule_job(&self, job_id: String) -> Result<(), BallistaError> {
        let mut available_executors = self.state.get_available_executors_data();

        // In case of there's no enough resources, reschedule the tasks of the job
        if available_executors.is_empty() {
            let tx_job = self.scheduler_env.as_ref().unwrap().tx_job.clone();
            // TODO Maybe it's better to use an exclusive runtime for this kind task scheduling
            warn!("Not enough available executors for task running");
            tokio::time::sleep(Duration::from_millis(100)).await;
            tx_job.send(job_id).await.unwrap();
            return Ok(());
        }

        let (tasks_assigment, num_tasks) =
            self.fetch_tasks(&mut available_executors, &job_id).await?;
        if num_tasks > 0 {
            for (idx_executor, tasks) in tasks_assigment.into_iter().enumerate() {
                if !tasks.is_empty() {
                    let executor_data = &available_executors[idx_executor];
                    debug!(
                        "Start to launch tasks {:?} to executor {:?}",
                        tasks, executor_data.executor_id
                    );
                    let mut client = {
                        let clients =
                            self.executors_client.as_ref().unwrap().read().await;
                        info!("Size of executor clients: {:?}", clients.len());
                        clients.get(&executor_data.executor_id).unwrap().clone()
                    };
                    // Update the resources first
                    self.state.save_executor_data(executor_data.clone());
                    // TODO check whether launching task is successful or not
                    client.launch_task(LaunchTaskParams { task: tasks }).await?;
                } else {
                    // Since the task assignment policy is round robin,
                    // if find tasks for one executor is empty, just break fast
                    break;
                }
            }
            return Ok(());
        }

        Ok(())
    }

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
                    .state
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
                            self.codec.physical_extension_codec(),
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

pub struct TaskScheduler<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    scheduler_server: Arc<SchedulerServer<T, U>>,
    plan_repr: PhantomData<T>,
    exec_repr: PhantomData<U>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> TaskScheduler<T, U> {
    pub fn new(scheduler_server: Arc<SchedulerServer<T, U>>) -> Self {
        Self {
            scheduler_server,
            plan_repr: PhantomData,
            exec_repr: PhantomData,
        }
    }

    pub fn start(&self, mut rx_job: mpsc::Receiver<String>) {
        let scheduler_server = self.scheduler_server.clone();
        tokio::spawn(async move {
            info!("Starting the task scheduler");
            loop {
                let job_id = rx_job.recv().await.unwrap();
                info!("Fetch job {:?} to be scheduled", job_id.clone());

                let server = scheduler_server.clone();
                server.schedule_job(job_id).await.unwrap();
            }
        });
    }
}

/// Create a DataFusion context that is compatible with Ballista
pub fn create_datafusion_context(config: &BallistaConfig) -> ExecutionContext {
    let config = ExecutionConfig::new()
        .with_target_partitions(config.default_shuffle_partitions());
    ExecutionContext::with_config(config)
}
