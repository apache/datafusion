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

use ballista_core::serde::{AsExecutionPlan, AsLogicalPlan, BallistaCodec};
use datafusion::execution::context::{default_session_builder, SessionState};
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::scheduler_server::event_loop::{
    SchedulerServerEvent, SchedulerServerEventAction,
};
use crate::state::{ConfigBackendClient, SchedulerState};

// include the generated protobuf source as a submodule
#[allow(clippy::all)]
pub mod externalscaler {
    include!(concat!(env!("OUT_DIR"), "/externalscaler.rs"));
}

mod event_loop;
mod external_scaler;
mod grpc;
mod task_scheduler;

type ExecutorsClient = Arc<RwLock<HashMap<String, ExecutorGrpcClient<Channel>>>>;
type SessionBuilder = fn(SessionConfig) -> SessionState;

#[derive(Clone)]
pub struct SchedulerServer<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    pub(crate) state: Arc<SchedulerState<T, U>>,
    pub start_time: u128,
    policy: TaskSchedulingPolicy,
    executors_client: Option<ExecutorsClient>,
    event_loop: Option<EventLoop<SchedulerServerEvent>>,
    codec: BallistaCodec<T, U>,
    /// SessionState Builder
    session_builder: SessionBuilder,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> SchedulerServer<T, U> {
    pub fn new(
        config: Arc<dyn ConfigBackendClient>,
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
        config: Arc<dyn ConfigBackendClient>,
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
        config: Arc<dyn ConfigBackendClient>,
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
        Self {
            state,
            start_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            policy,
            executors_client,
            event_loop,
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
            }
        }

        Ok(())
    }
}

/// Create a new DataFusion session context from Ballista Configuration
pub fn create_datafusion_session_context(
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
pub fn update_datafusion_session_context(
    session_ctx: Arc<SessionContext>,
    config: &BallistaConfig,
) -> Arc<SessionContext> {
    let session_config = session_ctx.state.lock().clone().config;
    let mut mut_config = session_config.lock();
    mut_config.target_partitions = config.default_shuffle_partitions();
    mut_config.batch_size = config.default_batch_size();
    mut_config.repartition_joins = config.repartition_joins();
    mut_config.repartition_aggregations = config.repartition_aggregations();
    mut_config.repartition_windows = config.repartition_windows();
    mut_config.parquet_pruning = config.parquet_pruning();
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
        session_id: String,
        session_ctx: Arc<SessionContext>,
    ) -> Option<Arc<SessionContext>> {
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
