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

use std::convert::TryInto;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

use log::{debug, info};
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status};

use ballista_core::error::BallistaError;
use ballista_core::serde::physical_plan::from_proto::parse_protobuf_hash_partitioning;
use ballista_core::serde::protobuf::executor_grpc_server::{
    ExecutorGrpc, ExecutorGrpcServer,
};
use ballista_core::serde::protobuf::executor_registration::OptionalHost;
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use ballista_core::serde::protobuf::{
    ExecutorRegistration, LaunchTaskParams, LaunchTaskResult, RegisterExecutorParams,
    SendHeartBeatParams, StopExecutorParams, StopExecutorResult, TaskDefinition,
    UpdateTaskStatusParams,
};
use ballista_core::serde::scheduler::{ExecutorSpecification, ExecutorState};
use datafusion::physical_plan::ExecutionPlan;

use crate::as_task_status;
use crate::executor::Executor;

pub async fn startup(
    mut scheduler: SchedulerGrpcClient<Channel>,
    executor: Arc<Executor>,
    executor_meta: ExecutorRegistration,
) {
    // TODO make the buffer size configurable
    let (tx_task, rx_task) = mpsc::channel::<TaskDefinition>(1000);

    let executor_server = ExecutorServer::new(
        scheduler.clone(),
        executor.clone(),
        executor_meta.clone(),
        ExecutorEnv { tx_task },
    );

    // 1. Start executor grpc service
    {
        let executor_meta = executor_meta.clone();
        let addr = format!(
            "{}:{}",
            executor_meta
                .optional_host
                .map(|h| match h {
                    OptionalHost::Host(host) => host,
                })
                .unwrap_or_else(|| String::from("127.0.0.1")),
            executor_meta.grpc_port
        );
        let addr = addr.parse().unwrap();
        info!("Setup executor grpc service for {:?}", addr);

        let server = ExecutorGrpcServer::new(executor_server.clone());
        let grpc_server_future = Server::builder().add_service(server).serve(addr);
        tokio::spawn(async move { grpc_server_future.await });
    }

    let executor_server = Arc::new(executor_server);

    // 2. Do executor registration
    match register_executor(&mut scheduler, &executor_meta, &executor.specification).await
    {
        Ok(_) => {
            info!("Executor registration succeed");
        }
        Err(error) => {
            panic!("Executor registration failed due to: {}", error);
        }
    };

    // 3. Start Heartbeater
    {
        let heartbeater = Heartbeater::new(executor_server.clone());
        heartbeater.start().await;
    }

    // 4. Start TaskRunnerPool
    {
        let task_runner_pool = TaskRunnerPool::new(executor_server.clone());
        task_runner_pool.start(rx_task).await;
    }
}

#[allow(clippy::clone_on_copy)]
async fn register_executor(
    scheduler: &mut SchedulerGrpcClient<Channel>,
    executor_meta: &ExecutorRegistration,
    specification: &ExecutorSpecification,
) -> Result<(), BallistaError> {
    let result = scheduler
        .register_executor(RegisterExecutorParams {
            metadata: Some(executor_meta.clone()),
            specification: Some(specification.clone().into()),
        })
        .await?;
    if result.into_inner().success {
        Ok(())
    } else {
        Err(BallistaError::General(
            "Executor registration failed!!!".to_owned(),
        ))
    }
}

#[derive(Clone)]
pub struct ExecutorServer {
    _start_time: u128,
    executor: Arc<Executor>,
    executor_meta: ExecutorRegistration,
    scheduler: SchedulerGrpcClient<Channel>,
    executor_env: ExecutorEnv,
}

#[derive(Clone)]
struct ExecutorEnv {
    tx_task: mpsc::Sender<TaskDefinition>,
}

unsafe impl Sync for ExecutorEnv {}

impl ExecutorServer {
    fn new(
        scheduler: SchedulerGrpcClient<Channel>,
        executor: Arc<Executor>,
        executor_meta: ExecutorRegistration,
        executor_env: ExecutorEnv,
    ) -> Self {
        Self {
            _start_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            executor,
            executor_meta,
            scheduler,
            executor_env,
        }
    }

    async fn heartbeat(&self) {
        // TODO Error handling
        self.scheduler
            .clone()
            .send_heart_beat(SendHeartBeatParams {
                metadata: Some(self.executor_meta.clone()),
                state: Some(self.get_executor_state().await.into()),
            })
            .await
            .unwrap();
    }

    async fn run_task(&self, task: TaskDefinition) -> Result<(), BallistaError> {
        let task_id = task.task_id.unwrap();
        let task_id_log = format!(
            "{}/{}/{}",
            task_id.job_id, task_id.stage_id, task_id.partition_id
        );
        info!("Start to run task {}", task_id_log);

        let plan: Arc<dyn ExecutionPlan> = (&task.plan.unwrap()).try_into().unwrap();
        let shuffle_output_partitioning =
            parse_protobuf_hash_partitioning(task.output_partitioning.as_ref())?;

        let execution_result = self
            .executor
            .execute_shuffle_write(
                task_id.job_id.clone(),
                task_id.stage_id as usize,
                task_id.partition_id as usize,
                plan,
                shuffle_output_partitioning,
            )
            .await;
        info!("Done with task {}", task_id_log);
        debug!("Statistics: {:?}", execution_result);

        // TODO use another channel to update the status of a task set
        self.scheduler
            .clone()
            .update_task_status(UpdateTaskStatusParams {
                metadata: Some(self.executor_meta.clone()),
                task_status: vec![as_task_status(
                    execution_result,
                    self.executor_meta.id.clone(),
                    task_id,
                )],
            })
            .await?;

        Ok(())
    }

    // TODO with real state
    async fn get_executor_state(&self) -> ExecutorState {
        ExecutorState {
            available_memory_size: u64::MAX,
        }
    }
}

struct Heartbeater {
    executor_server: Arc<ExecutorServer>,
}

impl Heartbeater {
    fn new(executor_server: Arc<ExecutorServer>) -> Self {
        Self { executor_server }
    }

    async fn start(&self) {
        let executor_server = self.executor_server.clone();
        tokio::spawn(async move {
            info!("Starting heartbeater to send heartbeat the scheduler periodically");
            loop {
                executor_server.heartbeat().await;
                tokio::time::sleep(Duration::from_millis(60000)).await;
            }
        });
    }
}

struct TaskRunnerPool {
    executor_server: Arc<ExecutorServer>,
}

impl TaskRunnerPool {
    fn new(executor_server: Arc<ExecutorServer>) -> Self {
        Self { executor_server }
    }

    async fn start(&self, mut rx_task: mpsc::Receiver<TaskDefinition>) {
        let executor_server = self.executor_server.clone();
        tokio::spawn(async move {
            info!("Starting the task runner pool");
            loop {
                let task = rx_task.recv().await.unwrap();
                info!("Received task {:?}", task);

                let server = executor_server.clone();
                tokio::spawn(async move {
                    server.run_task(task).await.unwrap();
                });
            }
        });
    }
}

#[tonic::async_trait]
impl ExecutorGrpc for ExecutorServer {
    async fn launch_task(
        &self,
        request: Request<LaunchTaskParams>,
    ) -> Result<Response<LaunchTaskResult>, Status> {
        let tasks = request.into_inner().task;
        let task_sender = self.executor_env.tx_task.clone();
        for task in tasks {
            task_sender.send(task).await.unwrap();
        }
        Ok(Response::new(LaunchTaskResult { success: true }))
    }

    async fn stop_executor(
        &self,
        _request: Request<StopExecutorParams>,
    ) -> Result<Response<StopExecutorResult>, Status> {
        todo!()
    }
}
