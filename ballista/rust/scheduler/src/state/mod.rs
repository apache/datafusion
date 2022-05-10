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
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use datafusion::physical_plan::ExecutionPlan;

use ballista_core::error::Result;

use crate::scheduler_server::{SessionBuilder, SessionContextRegistry};
use ballista_core::serde::protobuf::{ExecutorHeartbeat, JobStatus, KeyValuePair};
use ballista_core::serde::scheduler::ExecutorMetadata;
use ballista_core::serde::{AsExecutionPlan, AsLogicalPlan, BallistaCodec};

use crate::state::backend::StateBackendClient;
use crate::state::executor_manager::ExecutorManager;
use crate::state::persistent_state::PersistentSchedulerState;
use crate::state::stage_manager::StageManager;

pub mod backend;
mod executor_manager;
mod persistent_state;
mod stage_manager;
pub mod task_scheduler;

#[derive(Clone)]
pub(super) struct SchedulerState<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>
{
    persistent_state: PersistentSchedulerState<T, U>,
    pub executor_manager: ExecutorManager,
    pub stage_manager: StageManager,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> SchedulerState<T, U> {
    pub fn new(
        config_client: Arc<dyn StateBackendClient>,
        namespace: String,
        session_builder: SessionBuilder,
        codec: BallistaCodec<T, U>,
    ) -> Self {
        Self {
            persistent_state: PersistentSchedulerState::new(
                config_client,
                namespace,
                session_builder,
                codec,
            ),
            executor_manager: ExecutorManager::new(),
            stage_manager: StageManager::new(),
        }
    }

    pub async fn init(&self) -> Result<()> {
        self.persistent_state.init().await?;

        Ok(())
    }

    pub fn get_codec(&self) -> &BallistaCodec<T, U> {
        &self.persistent_state.codec
    }

    pub async fn get_executors_metadata(
        &self,
    ) -> Result<Vec<(ExecutorMetadata, Duration)>> {
        let mut result = vec![];

        let executors_heartbeat = self
            .executor_manager
            .get_executors_heartbeat()
            .into_iter()
            .map(|heartbeat| (heartbeat.executor_id.clone(), heartbeat))
            .collect::<HashMap<String, ExecutorHeartbeat>>();

        let executors_metadata = self.persistent_state.get_executors_metadata();

        let now_epoch_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        for meta in executors_metadata.into_iter() {
            // If there's no heartbeat info for an executor, regard its heartbeat timestamp as 0
            // so that it will always be excluded when requesting alive executors
            let ts = executors_heartbeat
                .get(&meta.id)
                .map(|heartbeat| Duration::from_secs(heartbeat.timestamp))
                .unwrap_or_else(|| Duration::from_secs(0));
            let time_since_last_seen = now_epoch_ts
                .checked_sub(ts)
                .unwrap_or_else(|| Duration::from_secs(0));
            result.push((meta, time_since_last_seen));
        }
        Ok(result)
    }

    pub fn get_executor_metadata(&self, executor_id: &str) -> Option<ExecutorMetadata> {
        self.persistent_state.get_executor_metadata(executor_id)
    }

    pub async fn save_executor_metadata(
        &self,
        executor_meta: ExecutorMetadata,
    ) -> Result<()> {
        self.persistent_state
            .save_executor_metadata(executor_meta)
            .await
    }

    pub async fn save_job_session(
        &self,
        job_id: &str,
        session_id: &str,
        configs: Vec<KeyValuePair>,
    ) -> Result<()> {
        self.persistent_state
            .save_job_session(job_id, session_id, configs)
            .await
    }

    pub fn get_session_from_job(&self, job_id: &str) -> Option<String> {
        self.persistent_state.get_session_from_job(job_id)
    }

    pub async fn save_job_metadata(
        &self,
        job_id: &str,
        status: &JobStatus,
    ) -> Result<()> {
        self.persistent_state
            .save_job_metadata(job_id, status)
            .await
    }

    pub fn get_job_metadata(&self, job_id: &str) -> Option<JobStatus> {
        self.persistent_state.get_job_metadata(job_id)
    }

    pub async fn save_stage_plan(
        &self,
        job_id: &str,
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<()> {
        self.persistent_state
            .save_stage_plan(job_id, stage_id, plan)
            .await
    }

    pub fn get_stage_plan(
        &self,
        job_id: &str,
        stage_id: usize,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        self.persistent_state.get_stage_plan(job_id, stage_id)
    }

    pub fn session_registry(&self) -> Arc<SessionContextRegistry> {
        self.persistent_state.session_registry()
    }
}

#[cfg(all(test, feature = "sled"))]
mod test {
    use std::sync::Arc;

    use ballista_core::error::BallistaError;
    use ballista_core::serde::protobuf::{
        job_status, JobStatus, LogicalPlanNode, PhysicalPlanNode, QueuedJob,
    };
    use ballista_core::serde::scheduler::{ExecutorMetadata, ExecutorSpecification};
    use ballista_core::serde::BallistaCodec;
    use datafusion::execution::context::default_session_builder;

    use super::{backend::standalone::StandaloneClient, SchedulerState};

    #[tokio::test]
    async fn executor_metadata() -> Result<(), BallistaError> {
        let state: SchedulerState<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerState::new(
                Arc::new(StandaloneClient::try_new_temporary()?),
                "test".to_string(),
                default_session_builder,
                BallistaCodec::default(),
            );
        let meta = ExecutorMetadata {
            id: "123".to_owned(),
            host: "localhost".to_owned(),
            port: 123,
            grpc_port: 124,
            specification: ExecutorSpecification { task_slots: 2 },
        };
        state.save_executor_metadata(meta.clone()).await?;
        let result: Vec<_> = state
            .get_executors_metadata()
            .await?
            .into_iter()
            .map(|(meta, _)| meta)
            .collect();
        assert_eq!(vec![meta], result);
        Ok(())
    }

    #[tokio::test]
    async fn job_metadata() -> Result<(), BallistaError> {
        let state: SchedulerState<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerState::new(
                Arc::new(StandaloneClient::try_new_temporary()?),
                "test".to_string(),
                default_session_builder,
                BallistaCodec::default(),
            );
        let meta = JobStatus {
            status: Some(job_status::Status::Queued(QueuedJob {})),
        };
        state.save_job_metadata("job", &meta).await?;
        let result = state.get_job_metadata("job").unwrap();
        assert!(result.status.is_some());
        match result.status.unwrap() {
            job_status::Status::Queued(_) => (),
            _ => panic!("Unexpected status"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn job_metadata_non_existant() -> Result<(), BallistaError> {
        let state: SchedulerState<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerState::new(
                Arc::new(StandaloneClient::try_new_temporary()?),
                "test".to_string(),
                default_session_builder,
                BallistaCodec::default(),
            );
        let meta = JobStatus {
            status: Some(job_status::Status::Queued(QueuedJob {})),
        };
        state.save_job_metadata("job", &meta).await?;
        let result = state.get_job_metadata("job2");
        assert!(result.is_none());
        Ok(())
    }
}
