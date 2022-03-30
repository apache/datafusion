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
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::RwLock;
use tonic::transport::Channel;

use ballista_core::config::{BallistaConfig, TaskSchedulingPolicy};
use ballista_core::error::Result;
use ballista_core::event_loop::EventLoop;
use ballista_core::serde::protobuf::executor_grpc_client::ExecutorGrpcClient;
use ballista_core::serde::protobuf::TaskStatus;
use ballista_core::serde::{AsExecutionPlan, AsLogicalPlan, BallistaCodec};
use datafusion::execution::context::{default_session_builder, SessionState};
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::scheduler_server::event::{QueryStageSchedulerEvent, SchedulerServerEvent};
use crate::scheduler_server::event_loop::SchedulerServerEventAction;
use crate::scheduler_server::query_stage_scheduler::QueryStageScheduler;
use crate::state::backend::StateBackendClient;
use crate::state::SchedulerState;

// include the generated protobuf source as a submodule
#[allow(clippy::all)]
pub mod externalscaler {
    include!(concat!(env!("OUT_DIR"), "/externalscaler.rs"));
}

pub mod event;
mod event_loop;
mod external_scaler;
mod grpc;
mod query_stage_scheduler;

type ExecutorsClient = Arc<RwLock<HashMap<String, ExecutorGrpcClient<Channel>>>>;
type SessionBuilder = fn(SessionConfig) -> SessionState;

#[derive(Clone)]
pub struct SchedulerServer<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    pub(crate) state: Arc<SchedulerState<T, U>>,
    pub start_time: u128,
    policy: TaskSchedulingPolicy,
    executors_client: Option<ExecutorsClient>,
    event_loop: Option<EventLoop<SchedulerServerEvent>>,
    query_stage_event_loop: EventLoop<QueryStageSchedulerEvent>,
    codec: BallistaCodec<T, U>,
    /// SessionState Builder
    session_builder: SessionBuilder,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> SchedulerServer<T, U> {
    pub fn new(
        config: Arc<dyn StateBackendClient>,
        namespace: String,
        codec: BallistaCodec<T, U>,
    ) -> Self {
        SchedulerServer::new_with_policy(
            config,
            namespace,
            TaskSchedulingPolicy::PullStaged,
            codec,
            default_session_builder,
        )
    }

    pub fn new_with_builder(
        config: Arc<dyn StateBackendClient>,
        namespace: String,
        codec: BallistaCodec<T, U>,
        session_builder: SessionBuilder,
    ) -> Self {
        SchedulerServer::new_with_policy(
            config,
            namespace,
            TaskSchedulingPolicy::PullStaged,
            codec,
            session_builder,
        )
    }

    pub fn new_with_policy(
        config: Arc<dyn StateBackendClient>,
        namespace: String,
        policy: TaskSchedulingPolicy,
        codec: BallistaCodec<T, U>,
        session_builder: SessionBuilder,
    ) -> Self {
        let state = Arc::new(SchedulerState::new(config, namespace, codec.clone()));

        let (executors_client, event_loop) =
            if matches!(policy, TaskSchedulingPolicy::PushStaged) {
                let executors_client = Arc::new(RwLock::new(HashMap::new()));
                let event_action: Arc<SchedulerServerEventAction<T, U>> =
                    Arc::new(SchedulerServerEventAction::new(
                        state.clone(),
                        executors_client.clone(),
                    ));
                let event_loop =
                    EventLoop::new("scheduler".to_owned(), 10000, event_action);
                (Some(executors_client), Some(event_loop))
            } else {
                (None, None)
            };
        let query_stage_scheduler =
            Arc::new(QueryStageScheduler::new(state.clone(), None));
        let query_stage_event_loop =
            EventLoop::new("query_stage".to_owned(), 10000, query_stage_scheduler);
        Self {
            state,
            start_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            policy,
            executors_client,
            event_loop,
            query_stage_event_loop,
            codec,
            session_builder,
        }
    }

    pub async fn init(&mut self) -> Result<()> {
        {
            // initialize state
            self.state.init().await?;
        }

        {
            if let Some(event_loop) = self.event_loop.as_mut() {
                event_loop.start()?;

                let query_stage_scheduler = Arc::new(QueryStageScheduler::new(
                    self.state.clone(),
                    Some(event_loop.get_sender()?),
                ));
                let query_stage_event_loop = EventLoop::new(
                    self.query_stage_event_loop.name.clone(),
                    self.query_stage_event_loop.buffer_size,
                    query_stage_scheduler,
                );
                self.query_stage_event_loop = query_stage_event_loop;
            }

            self.query_stage_event_loop.start()?;
        }

        Ok(())
    }

    pub(crate) async fn update_task_status(
        &self,
        tasks_status: Vec<TaskStatus>,
    ) -> Result<()> {
        let num_tasks_status = tasks_status.len() as u32;
        let stage_events = self.state.stage_manager.update_tasks_status(tasks_status);
        if stage_events.is_empty() {
            if let Some(event_loop) = self.event_loop.as_ref() {
                event_loop
                    .get_sender()?
                    .post_event(SchedulerServerEvent::ReviveOffers(num_tasks_status))
                    .await?;
            }
        } else {
            for stage_event in stage_events {
                self.post_stage_event(stage_event).await?;
            }
        }

        Ok(())
    }

    async fn post_stage_event(&self, event: QueryStageSchedulerEvent) -> Result<()> {
        self.query_stage_event_loop
            .get_sender()?
            .post_event(event)
            .await
    }
}

/// Create a DataFusion session context that is compatible with Ballista Configuration
pub fn create_datafusion_context(
    config: &BallistaConfig,
    session_builder: SessionBuilder,
) -> Arc<SessionContext> {
    let config = SessionConfig::new()
        .with_target_partitions(config.default_shuffle_partitions())
        .with_batch_size(config.default_batch_size())
        .with_repartition_joins(config.repartition_joins())
        .with_repartition_aggregations(config.repartition_aggregations())
        .with_repartition_windows(config.repartition_windows())
        .with_parquet_pruning(config.parquet_pruning());
    let session_state = session_builder(config);
    Arc::new(SessionContext::with_state(session_state))
}

/// Update the existing DataFusion session context with Ballista Configuration
pub fn update_datafusion_context(
    session_ctx: Arc<SessionContext>,
    config: &BallistaConfig,
) -> Arc<SessionContext> {
    {
        let mut mut_state = session_ctx.state.write();
        mut_state.config.target_partitions = config.default_shuffle_partitions();
        mut_state.config.batch_size = config.default_batch_size();
        mut_state.config.repartition_joins = config.repartition_joins();
        mut_state.config.repartition_aggregations = config.repartition_aggregations();
        mut_state.config.repartition_windows = config.repartition_windows();
        mut_state.config.parquet_pruning = config.parquet_pruning();
    }
    session_ctx
}

/// A Registry holds all the datafusion session contexts
pub struct SessionContextRegistry {
    /// A map from session_id to SessionContext
    pub running_sessions: RwLock<HashMap<String, Arc<SessionContext>>>,
}

impl Default for SessionContextRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionContextRegistry {
    /// Create the registry that object stores can registered into.
    /// ['LocalFileSystem'] store is registered in by default to support read local files natively.
    pub fn new() -> Self {
        Self {
            running_sessions: RwLock::new(HashMap::new()),
        }
    }

    /// Adds a new session to this registry.
    pub async fn register_session(
        &self,
        session_ctx: Arc<SessionContext>,
    ) -> Option<Arc<SessionContext>> {
        let session_id = session_ctx.session_id();
        let mut sessions = self.running_sessions.write().await;
        sessions.insert(session_id, session_ctx)
    }

    /// Lookup the session context registered
    pub async fn lookup_session(&self, session_id: &str) -> Option<Arc<SessionContext>> {
        let sessions = self.running_sessions.read().await;
        sessions.get(session_id).cloned()
    }

    /// Remove a session from this registry.
    pub async fn unregister_session(
        &self,
        session_id: &str,
    ) -> Option<Arc<SessionContext>> {
        let mut sessions = self.running_sessions.write().await;
        sessions.remove(session_id)
    }
}
#[cfg(all(test, feature = "sled"))]
mod test {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use ballista_core::config::TaskSchedulingPolicy;
    use ballista_core::error::{BallistaError, Result};
    use ballista_core::execution_plans::ShuffleWriterExec;
    use ballista_core::serde::protobuf::{
        job_status, task_status, CompletedTask, LogicalPlanNode, PartitionId,
        PhysicalPlanNode, TaskStatus,
    };
    use ballista_core::serde::scheduler::ExecutorData;
    use ballista_core::serde::BallistaCodec;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::context::default_session_builder;
    use datafusion::logical_plan::{col, sum, LogicalPlan, LogicalPlanBuilder};
    use datafusion::prelude::{SessionConfig, SessionContext};

    use crate::scheduler_server::event::QueryStageSchedulerEvent;
    use crate::scheduler_server::SchedulerServer;
    use crate::state::backend::standalone::StandaloneClient;
    use crate::state::task_scheduler::TaskScheduler;

    #[tokio::test]
    async fn test_pull_based_task_scheduling() -> Result<()> {
        let now = Instant::now();
        test_task_scheduling(TaskSchedulingPolicy::PullStaged, test_plan(), 4).await?;
        println!(
            "pull-based task scheduling cost {}ms",
            now.elapsed().as_millis()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_push_based_task_scheduling() -> Result<()> {
        let now = Instant::now();
        test_task_scheduling(TaskSchedulingPolicy::PushStaged, test_plan(), 4).await?;
        println!(
            "push-based task scheduling cost {}ms",
            now.elapsed().as_millis()
        );

        Ok(())
    }

    async fn test_task_scheduling(
        policy: TaskSchedulingPolicy,
        plan_of_linear_stages: LogicalPlan,
        total_available_task_slots: usize,
    ) -> Result<()> {
        let scheduler = test_scheduler(policy).await?;
        if matches!(policy, TaskSchedulingPolicy::PushStaged) {
            let executors = test_executors(total_available_task_slots);
            for executor_data in executors {
                scheduler
                    .state
                    .executor_manager
                    .save_executor_data(executor_data);
            }
        }
        let config =
            SessionConfig::new().with_target_partitions(total_available_task_slots);
        let ctx = Arc::new(SessionContext::with_config(config));
        let plan = async {
            let optimized_plan = ctx.optimize(&plan_of_linear_stages).map_err(|e| {
                BallistaError::General(format!(
                    "Could not create optimized logical plan: {}",
                    e
                ))
            })?;

            ctx.create_physical_plan(&optimized_plan)
                .await
                .map_err(|e| {
                    BallistaError::General(format!(
                        "Could not create physical plan: {}",
                        e
                    ))
                })
        }
        .await?;

        let job_id = "job";
        scheduler
            .state
            .session_registry()
            .register_session(ctx.clone())
            .await;
        scheduler
            .state
            .save_job_session(job_id, ctx.session_id().as_str())
            .await?;
        {
            // verify job submit
            scheduler
                .post_stage_event(QueryStageSchedulerEvent::JobSubmitted(
                    job_id.to_owned(),
                    plan,
                ))
                .await?;

            let waiting_time_ms =
                test_waiting_async(|| scheduler.state.get_job_metadata(job_id).is_some())
                    .await;
            let job_status = scheduler.state.get_job_metadata(job_id);
            assert!(
                job_status.is_some(),
                "Fail to receive JobSubmitted event within {}ms",
                waiting_time_ms
            );
        }

        let stage_task_num = test_get_job_stage_task_num(&scheduler, job_id);
        let first_stage_id = 1u32;
        let final_stage_id = stage_task_num.len() as u32 - 1;
        assert!(scheduler
            .state
            .stage_manager
            .is_final_stage(job_id, final_stage_id));

        if matches!(policy, TaskSchedulingPolicy::PullStaged) {
            assert!(!scheduler.state.stage_manager.has_running_tasks());
            assert!(scheduler
                .state
                .stage_manager
                .is_running_stage(job_id, first_stage_id));
            if first_stage_id != final_stage_id {
                assert!(scheduler
                    .state
                    .stage_manager
                    .is_pending_stage(job_id, final_stage_id));
            }
        }

        // complete stage one by one
        for stage_id in first_stage_id..final_stage_id {
            let next_stage_id = stage_id + 1;
            let num_tasks = stage_task_num[stage_id as usize] as usize;
            if matches!(policy, TaskSchedulingPolicy::PullStaged) {
                let mut executors = test_executors(total_available_task_slots);
                let _fet_tasks = scheduler
                    .state
                    .fetch_schedulable_tasks(&mut executors, 1)
                    .await?;
            }
            assert!(scheduler.state.stage_manager.has_running_tasks());
            assert!(scheduler
                .state
                .stage_manager
                .is_running_stage(job_id, stage_id));
            assert!(scheduler
                .state
                .stage_manager
                .is_pending_stage(job_id, next_stage_id));

            test_complete_stage(&scheduler, job_id, 1, num_tasks).await?;
            assert!(!scheduler.state.stage_manager.has_running_tasks());
            assert!(!scheduler
                .state
                .stage_manager
                .is_running_stage(job_id, stage_id));
            assert!(scheduler
                .state
                .stage_manager
                .is_completed_stage(job_id, stage_id));
            let waiting_time_ms = test_waiting_async(|| {
                !scheduler
                    .state
                    .stage_manager
                    .is_pending_stage(job_id, next_stage_id)
            })
            .await;
            assert!(
                !scheduler
                    .state
                    .stage_manager
                    .is_pending_stage(job_id, next_stage_id),
                "Fail to update stage state machine within {}ms",
                waiting_time_ms
            );
            assert!(scheduler
                .state
                .stage_manager
                .is_running_stage(job_id, next_stage_id));
        }

        // complete the final stage
        {
            let num_tasks = stage_task_num[final_stage_id as usize] as usize;
            if matches!(policy, TaskSchedulingPolicy::PullStaged) {
                let mut executors = test_executors(total_available_task_slots);
                let _fet_tasks = scheduler
                    .state
                    .fetch_schedulable_tasks(&mut executors, 1)
                    .await?;
            }
            assert!(scheduler.state.stage_manager.has_running_tasks());

            test_complete_stage(&scheduler, job_id, final_stage_id, num_tasks).await?;
            assert!(!scheduler.state.stage_manager.has_running_tasks());
            assert!(!scheduler
                .state
                .stage_manager
                .is_running_stage(job_id, final_stage_id));
            assert!(scheduler
                .state
                .stage_manager
                .is_completed_stage(job_id, final_stage_id));
            let waiting_time_ms = test_waiting_async(|| {
                let job_status = scheduler.state.get_job_metadata(job_id).unwrap();
                matches!(job_status.status, Some(job_status::Status::Completed(_)))
            })
            .await;

            let job_status = scheduler.state.get_job_metadata(job_id).unwrap();
            assert!(
                matches!(job_status.status, Some(job_status::Status::Completed(_))),
                "Fail to update job state machine within {}ms",
                waiting_time_ms
            );
        }

        Ok(())
    }

    async fn test_waiting_async<F>(cond: F) -> u64
    where
        F: Fn() -> bool,
    {
        let round_waiting_time = 10;
        let num_round = 5;
        for _i in 0..num_round {
            if cond() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(round_waiting_time)).await;
        }

        round_waiting_time * num_round
    }

    async fn test_complete_stage(
        scheduler: &SchedulerServer<LogicalPlanNode, PhysicalPlanNode>,
        job_id: &str,
        stage_id: u32,
        num_tasks: usize,
    ) -> Result<()> {
        let tasks_status: Vec<TaskStatus> = (0..num_tasks as u32)
            .into_iter()
            .map(|task_id| TaskStatus {
                status: Some(task_status::Status::Completed(CompletedTask {
                    executor_id: "localhost".to_owned(),
                    partitions: Vec::new(),
                })),
                task_id: Some(PartitionId {
                    job_id: job_id.to_owned(),
                    stage_id,
                    partition_id: task_id,
                }),
            })
            .collect();
        scheduler.update_task_status(tasks_status).await
    }

    async fn test_scheduler(
        policy: TaskSchedulingPolicy,
    ) -> Result<SchedulerServer<LogicalPlanNode, PhysicalPlanNode>> {
        let state_storage = Arc::new(StandaloneClient::try_new_temporary()?);
        let mut scheduler: SchedulerServer<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerServer::new_with_policy(
                state_storage.clone(),
                "default".to_owned(),
                policy,
                BallistaCodec::default(),
                default_session_builder,
            );
        scheduler.init().await?;

        Ok(scheduler)
    }

    fn test_executors(num_partitions: usize) -> Vec<ExecutorData> {
        let task_slots = (num_partitions as u32 + 1) / 2;

        vec![
            ExecutorData {
                executor_id: "localhost1".to_owned(),
                total_task_slots: task_slots,
                available_task_slots: task_slots,
            },
            ExecutorData {
                executor_id: "localhost2".to_owned(),
                total_task_slots: num_partitions as u32 - task_slots,
                available_task_slots: num_partitions as u32 - task_slots,
            },
        ]
    }

    fn test_get_job_stage_task_num(
        scheduler: &SchedulerServer<LogicalPlanNode, PhysicalPlanNode>,
        job_id: &str,
    ) -> Vec<u32> {
        let mut ret = vec![0, 1];
        let mut stage_id = 1;
        while let Some(stage_plan) = scheduler.state.get_stage_plan(job_id, stage_id) {
            if let Some(shuffle_writer) =
                stage_plan.as_any().downcast_ref::<ShuffleWriterExec>()
            {
                if let Some(partitions) = shuffle_writer.shuffle_output_partitioning() {
                    ret.push(partitions.partition_count() as u32)
                }
            }
            stage_id += 1;
        }

        ret
    }

    fn test_plan() -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("gmv", DataType::UInt64, false),
        ]);

        LogicalPlanBuilder::scan_empty(None, &schema, Some(vec![0, 1]))
            .unwrap()
            .aggregate(vec![col("id")], vec![sum(col("gmv"))])
            .unwrap()
            .build()
            .unwrap()
    }
}
