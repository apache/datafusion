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

use anyhow::Context;
use ballista_core::config::{BallistaConfig, TaskSchedulingPolicy};
use ballista_core::error::BallistaError;
use ballista_core::serde::protobuf::execute_query_params::{OptionalSessionId, Query};
use ballista_core::serde::protobuf::executor_grpc_client::ExecutorGrpcClient;
use ballista_core::serde::protobuf::executor_registration::OptionalHost;
use ballista_core::serde::protobuf::scheduler_grpc_server::SchedulerGrpc;
use ballista_core::serde::protobuf::{
    job_status, ExecuteQueryParams, ExecuteQueryResult, ExecutorHeartbeat, FailedJob,
    FileType, GetFileMetadataParams, GetFileMetadataResult, GetJobStatusParams,
    GetJobStatusResult, HeartBeatParams, HeartBeatResult, JobStatus, PollWorkParams,
    PollWorkResult, QueuedJob, RegisterExecutorParams, RegisterExecutorResult,
    UpdateTaskStatusParams, UpdateTaskStatusResult,
};
use ballista_core::serde::scheduler::{
    ExecutorData, ExecutorDataChange, ExecutorMetadata,
};
use ballista_core::serde::{AsExecutionPlan, AsLogicalPlan};
use datafusion::datafusion_data_access::object_store::{
    local::LocalFileSystem, ObjectStore,
};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use futures::StreamExt;
use log::{debug, error, info, trace, warn};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use std::convert::TryInto;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Instant;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::{Request, Response, Status};

use crate::scheduler_server::event::QueryStageSchedulerEvent;
use crate::scheduler_server::{
    create_datafusion_context, update_datafusion_context, SchedulerServer,
};
use crate::state::task_scheduler::TaskScheduler;

#[tonic::async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> SchedulerGrpc
    for SchedulerServer<T, U>
{
    async fn poll_work(
        &self,
        request: Request<PollWorkParams>,
    ) -> std::result::Result<Response<PollWorkResult>, tonic::Status> {
        if let TaskSchedulingPolicy::PushStaged = self.policy {
            error!("Poll work interface is not supported for push-based task scheduling");
            return Err(tonic::Status::failed_precondition(
                "Bad request because poll work is not supported for push-based task scheduling",
            ));
        }
        let remote_addr = request.remote_addr();
        if let PollWorkParams {
            metadata: Some(metadata),
            can_accept_task,
            task_status,
        } = request.into_inner()
        {
            debug!("Received poll_work request for {:?}", metadata);
            let metadata = ExecutorMetadata {
                id: metadata.id,
                host: metadata
                    .optional_host
                    .map(|h| match h {
                        OptionalHost::Host(host) => host,
                    })
                    .unwrap_or_else(|| remote_addr.unwrap().ip().to_string()),
                port: metadata.port as u16,
                grpc_port: metadata.grpc_port as u16,
                specification: metadata.specification.unwrap().into(),
            };
            let executor_heartbeat = ExecutorHeartbeat {
                executor_id: metadata.id.clone(),
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_secs(),
                state: None,
            };
            // In case that it's the first time to poll work, do registration
            if self.state.get_executor_metadata(&metadata.id).is_none() {
                self.state
                    .save_executor_metadata(metadata.clone())
                    .await
                    .map_err(|e| {
                        let msg = format!("Could not save executor metadata: {}", e);
                        error!("{}", msg);
                        tonic::Status::internal(msg)
                    })?;
            }
            self.state
                .executor_manager
                .save_executor_heartbeat(executor_heartbeat);
            self.update_task_status(task_status).await.map_err(|e| {
                let msg = format!(
                    "Fail to update tasks status from executor {:?} due to {:?}",
                    &metadata.id, e
                );
                error!("{}", msg);
                tonic::Status::internal(msg)
            })?;
            let task: Result<Option<_>, Status> = if can_accept_task {
                let mut executors_data = vec![ExecutorData {
                    executor_id: metadata.id.clone(),
                    total_task_slots: 1,
                    available_task_slots: 1,
                }];
                let (mut tasks, num_tasks) = self
                    .state
                    .fetch_schedulable_tasks(&mut executors_data, 1)
                    .await
                    .map_err(|e| {
                        let msg = format!("Error finding next assignable task: {}", e);
                        error!("{}", msg);
                        tonic::Status::internal(msg)
                    })?;
                if num_tasks == 0 {
                    Ok(None)
                } else {
                    assert_eq!(tasks.len(), 1);
                    let mut task = tasks.pop().unwrap();
                    assert_eq!(task.len(), 1);
                    let task = task.pop().unwrap();
                    Ok(Some(task))
                }
            } else {
                Ok(None)
            };
            Ok(Response::new(PollWorkResult { task: task? }))
        } else {
            warn!("Received invalid executor poll_work request");
            Err(tonic::Status::invalid_argument(
                "Missing metadata in request",
            ))
        }
    }

    async fn register_executor(
        &self,
        request: Request<RegisterExecutorParams>,
    ) -> Result<Response<RegisterExecutorResult>, Status> {
        let remote_addr = request.remote_addr();
        if let RegisterExecutorParams {
            metadata: Some(metadata),
        } = request.into_inner()
        {
            info!("Received register executor request for {:?}", metadata);
            let metadata = ExecutorMetadata {
                id: metadata.id,
                host: metadata
                    .optional_host
                    .map(|h| match h {
                        OptionalHost::Host(host) => host,
                    })
                    .unwrap_or_else(|| remote_addr.unwrap().ip().to_string()),
                port: metadata.port as u16,
                grpc_port: metadata.grpc_port as u16,
                specification: metadata.specification.unwrap().into(),
            };
            // Check whether the executor starts the grpc service
            {
                let executor_url =
                    format!("http://{}:{}", metadata.host, metadata.grpc_port);
                info!("Connect to executor {:?}", executor_url);
                let executor_client = ExecutorGrpcClient::connect(executor_url)
                    .await
                    .context("Could not connect to executor")
                    .map_err(|e| tonic::Status::internal(format!("{:?}", e)))?;
                let mut clients = self.executors_client.as_ref().unwrap().write().await;
                // TODO check duplicated registration
                clients.insert(metadata.id.clone(), executor_client);
                info!("Size of executor clients: {:?}", clients.len());
            }
            self.state
                .save_executor_metadata(metadata.clone())
                .await
                .map_err(|e| {
                    let msg = format!("Could not save executor metadata: {}", e);
                    error!("{}", msg);
                    tonic::Status::internal(msg)
                })?;
            let executor_data = ExecutorData {
                executor_id: metadata.id.clone(),
                total_task_slots: metadata.specification.task_slots,
                available_task_slots: metadata.specification.task_slots,
            };
            self.state
                .executor_manager
                .save_executor_data(executor_data);
            Ok(Response::new(RegisterExecutorResult { success: true }))
        } else {
            warn!("Received invalid register executor request");
            Err(tonic::Status::invalid_argument(
                "Missing metadata in request",
            ))
        }
    }

    async fn heart_beat_from_executor(
        &self,
        request: Request<HeartBeatParams>,
    ) -> Result<Response<HeartBeatResult>, Status> {
        let HeartBeatParams { executor_id, state } = request.into_inner();

        debug!("Received heart beat request for {:?}", executor_id);
        trace!("Related executor state is {:?}", state);
        let executor_heartbeat = ExecutorHeartbeat {
            executor_id,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs(),
            state,
        };
        self.state
            .executor_manager
            .save_executor_heartbeat(executor_heartbeat);
        Ok(Response::new(HeartBeatResult { reregister: false }))
    }

    async fn update_task_status(
        &self,
        request: Request<UpdateTaskStatusParams>,
    ) -> Result<Response<UpdateTaskStatusResult>, Status> {
        let UpdateTaskStatusParams {
            executor_id,
            task_status,
        } = request.into_inner();

        debug!(
            "Received task status update request for executor {:?}",
            executor_id
        );
        let num_tasks = task_status.len();
        if let Some(executor_data) =
            self.state.executor_manager.get_executor_data(&executor_id)
        {
            self.state
                .executor_manager
                .update_executor_data(&ExecutorDataChange {
                    executor_id: executor_data.executor_id,
                    task_slots: num_tasks as i32,
                });
        } else {
            error!("Fail to get executor data for {:?}", &executor_id);
        }

        self.update_task_status(task_status).await.map_err(|e| {
            let msg = format!(
                "Fail to update tasks status from executor {:?} due to {:?}",
                &executor_id, e
            );
            error!("{}", msg);
            tonic::Status::internal(msg)
        })?;

        Ok(Response::new(UpdateTaskStatusResult { success: true }))
    }

    async fn get_file_metadata(
        &self,
        request: Request<GetFileMetadataParams>,
    ) -> std::result::Result<Response<GetFileMetadataResult>, tonic::Status> {
        // TODO support multiple object stores
        let obj_store = LocalFileSystem {};
        // TODO shouldn't this take a ListingOption object as input?

        let GetFileMetadataParams { path, file_type } = request.into_inner();

        let file_type: FileType = file_type.try_into().map_err(|e| {
            let msg = format!("Error reading request: {}", e);
            error!("{}", msg);
            tonic::Status::internal(msg)
        })?;

        let file_format: Arc<dyn FileFormat> = match file_type {
            FileType::Parquet => Ok(Arc::new(ParquetFormat::default())),
            // TODO implement for CSV
            _ => Err(tonic::Status::unimplemented(
                "get_file_metadata unsupported file type",
            )),
        }?;

        let file_metas = obj_store.list_file(&path).await.map_err(|e| {
            let msg = format!("Error listing files: {}", e);
            error!("{}", msg);
            tonic::Status::internal(msg)
        })?;

        let obj_readers = file_metas.map(move |f| obj_store.file_reader(f?.sized_file));

        let schema = file_format
            .infer_schema(Box::pin(obj_readers))
            .await
            .map_err(|e| {
                let msg = format!("Error infering schema: {}", e);
                error!("{}", msg);
                tonic::Status::internal(msg)
            })?;

        Ok(Response::new(GetFileMetadataResult {
            schema: Some(schema.as_ref().into()),
        }))
    }

    async fn execute_query(
        &self,
        request: Request<ExecuteQueryParams>,
    ) -> std::result::Result<Response<ExecuteQueryResult>, tonic::Status> {
        let query_params = request.into_inner();
        if let ExecuteQueryParams {
            query: Some(query),
            settings,
            optional_session_id,
        } = query_params
        {
            // parse config
            let mut config_builder = BallistaConfig::builder();
            for kv_pair in &settings {
                config_builder = config_builder.set(&kv_pair.key, &kv_pair.value);
            }
            let config = config_builder.build().map_err(|e| {
                let msg = format!("Could not parse configs: {}", e);
                error!("{}", msg);
                tonic::Status::internal(msg)
            })?;

            let df_session = match optional_session_id {
                Some(OptionalSessionId::SessionId(session_id)) => {
                    let session_ctx = self
                        .state
                        .session_registry()
                        .lookup_session(session_id.as_str())
                        .await
                        .expect(
                            "SessionContext does not exist in SessionContextRegistry.",
                        );
                    update_datafusion_context(session_ctx, &config)
                }
                _ => {
                    let df_session =
                        create_datafusion_context(&config, self.session_builder);
                    self.state
                        .session_registry()
                        .register_session(df_session.clone())
                        .await;
                    df_session
                }
            };

            let plan = match query {
                Query::LogicalPlan(message) => T::try_decode(message.as_slice())
                    .and_then(|m| {
                        m.try_into_logical_plan(
                            df_session.deref(),
                            self.codec.logical_extension_codec(),
                        )
                    })
                    .map_err(|e| {
                        let msg = format!("Could not parse logical plan protobuf: {}", e);
                        error!("{}", msg);
                        tonic::Status::internal(msg)
                    })?,
                Query::Sql(sql) => df_session
                    .sql(&sql)
                    .await
                    .and_then(|df| df.to_logical_plan())
                    .map_err(|e| {
                        let msg = format!("Error parsing SQL: {}", e);
                        error!("{}", msg);
                        tonic::Status::internal(msg)
                    })?,
            };
            debug!("Received plan for execution: {:?}", plan);

            // Generate job id.
            // TODO Maybe the format will be changed in the future
            let job_id = generate_job_id();
            let session_id = df_session.session_id();
            let state = self.state.clone();
            let query_stage_event_sender =
                self.query_stage_event_loop.get_sender().map_err(|e| {
                    tonic::Status::internal(format!(
                        "Could not get query stage event sender due to: {}",
                        e
                    ))
                })?;

            // Save placeholder job metadata
            state
                .save_job_metadata(
                    &job_id,
                    &JobStatus {
                        status: Some(job_status::Status::Queued(QueuedJob {})),
                    },
                )
                .await
                .map_err(|e| {
                    tonic::Status::internal(format!("Could not save job metadata: {}", e))
                })?;

            state
                .save_job_session(&job_id, &session_id)
                .await
                .map_err(|e| {
                    tonic::Status::internal(format!(
                        "Could not save job session mapping: {}",
                        e
                    ))
                })?;

            let job_id_spawn = job_id.clone();
            let ctx = df_session.clone();
            tokio::spawn(async move {
                if let Err(e) = async {
                    // create physical plan
                    let start = Instant::now();
                    let plan = async {
                        let optimized_plan = ctx.optimize(&plan).map_err(|e| {
                            let msg =
                                format!("Could not create optimized logical plan: {}", e);
                            error!("{}", msg);

                            BallistaError::General(msg)
                        })?;

                        debug!("Calculated optimized plan: {:?}", optimized_plan);

                        ctx.create_physical_plan(&optimized_plan)
                            .await
                            .map_err(|e| {
                                let msg =
                                    format!("Could not create physical plan: {}", e);
                                error!("{}", msg);

                                BallistaError::General(msg)
                            })
                    }
                    .await?;
                    info!(
                        "DataFusion created physical plan in {} milliseconds",
                        start.elapsed().as_millis()
                    );

                    query_stage_event_sender
                        .post_event(QueryStageSchedulerEvent::JobSubmitted(
                            job_id_spawn.clone(),
                            plan,
                        ))
                        .await?;

                    Ok::<(), BallistaError>(())
                }
                .await
                {
                    let msg = format!("Job {} failed due to {}", job_id_spawn, e);
                    warn!("{}", msg);
                    state
                        .save_job_metadata(
                            &job_id_spawn,
                            &JobStatus {
                                status: Some(job_status::Status::Failed(FailedJob {
                                    error: msg.to_string(),
                                })),
                            },
                        )
                        .await
                        .unwrap_or_else(|_| {
                            panic!(
                                "Fail to update job status to failed for {}",
                                job_id_spawn
                            )
                        });
                }
            });

            Ok(Response::new(ExecuteQueryResult { job_id, session_id }))
        } else if let ExecuteQueryParams {
            query: None,
            settings,
            optional_session_id: None,
        } = query_params
        {
            // parse config for new session
            let mut config_builder = BallistaConfig::builder();
            for kv_pair in &settings {
                config_builder = config_builder.set(&kv_pair.key, &kv_pair.value);
            }
            let config = config_builder.build().map_err(|e| {
                let msg = format!("Could not parse configs: {}", e);
                error!("{}", msg);
                tonic::Status::internal(msg)
            })?;
            let df_session = create_datafusion_context(&config, self.session_builder);
            self.state
                .session_registry()
                .register_session(df_session.clone())
                .await;
            Ok(Response::new(ExecuteQueryResult {
                job_id: "NA".to_owned(),
                session_id: df_session.session_id(),
            }))
        } else {
            Err(tonic::Status::internal("Error parsing request"))
        }
    }

    async fn get_job_status(
        &self,
        request: Request<GetJobStatusParams>,
    ) -> std::result::Result<Response<GetJobStatusResult>, tonic::Status> {
        let job_id = request.into_inner().job_id;
        debug!("Received get_job_status request for job {}", job_id);
        let job_meta = self.state.get_job_metadata(&job_id).unwrap();
        Ok(Response::new(GetJobStatusResult {
            status: Some(job_meta),
        }))
    }
}

fn generate_job_id() -> String {
    let mut rng = thread_rng();
    std::iter::repeat(())
        .map(|()| rng.sample(Alphanumeric))
        .map(char::from)
        .take(7)
        .collect()
}

#[cfg(all(test, feature = "sled"))]
mod test {
    use std::sync::Arc;

    use tonic::Request;

    use crate::state::{backend::standalone::StandaloneClient, SchedulerState};
    use ballista_core::error::BallistaError;
    use ballista_core::serde::protobuf::{
        executor_registration::OptionalHost, ExecutorRegistration, LogicalPlanNode,
        PhysicalPlanNode, PollWorkParams,
    };
    use ballista_core::serde::scheduler::ExecutorSpecification;
    use ballista_core::serde::BallistaCodec;

    use super::{SchedulerGrpc, SchedulerServer};

    #[tokio::test]
    async fn test_poll_work() -> Result<(), BallistaError> {
        let state_storage = Arc::new(StandaloneClient::try_new_temporary()?);
        let namespace = "default";
        let scheduler: SchedulerServer<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerServer::new(
                state_storage.clone(),
                namespace.to_owned(),
                BallistaCodec::default(),
            );
        let exec_meta = ExecutorRegistration {
            id: "abc".to_owned(),
            optional_host: Some(OptionalHost::Host("".to_owned())),
            port: 0,
            grpc_port: 0,
            specification: Some(ExecutorSpecification { task_slots: 2 }.into()),
        };
        let request: Request<PollWorkParams> = Request::new(PollWorkParams {
            metadata: Some(exec_meta.clone()),
            can_accept_task: false,
            task_status: vec![],
        });
        let response = scheduler
            .poll_work(request)
            .await
            .expect("Received error response")
            .into_inner();
        // no response task since we told the scheduler we didn't want to accept one
        assert!(response.task.is_none());
        let state: SchedulerState<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerState::new(
                state_storage.clone(),
                namespace.to_string(),
                BallistaCodec::default(),
            );
        state.init().await?;
        // executor should be registered
        assert_eq!(state.get_executors_metadata().await.unwrap().len(), 1);

        let request: Request<PollWorkParams> = Request::new(PollWorkParams {
            metadata: Some(exec_meta.clone()),
            can_accept_task: true,
            task_status: vec![],
        });
        let response = scheduler
            .poll_work(request)
            .await
            .expect("Received error response")
            .into_inner();
        // still no response task since there are no tasks in the scheduelr
        assert!(response.task.is_none());
        let state: SchedulerState<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerState::new(
                state_storage.clone(),
                namespace.to_string(),
                BallistaCodec::default(),
            );
        state.init().await?;
        // executor should be registered
        assert_eq!(state.get_executors_metadata().await.unwrap().len(), 1);
        Ok(())
    }
}
