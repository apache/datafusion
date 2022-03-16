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

use log::debug;
use parking_lot::RwLock;
use prost::Message;
use std::any::type_name;
use std::collections::HashMap;
use std::sync::Arc;

use ballista_core::error::{BallistaError, Result};

use ballista_core::serde::protobuf::JobStatus;

use crate::state::backend::StateBackendClient;
use ballista_core::serde::scheduler::ExecutorMetadata;
use ballista_core::serde::{protobuf, AsExecutionPlan, AsLogicalPlan, BallistaCodec};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

type StageKey = (String, u32);

#[derive(Clone)]
pub(crate) struct PersistentSchedulerState<
    T: 'static + AsLogicalPlan,
    U: 'static + AsExecutionPlan,
> {
    // for db
    config_client: Arc<dyn StateBackendClient>,
    namespace: String,
    pub(crate) codec: BallistaCodec<T, U>,

    // for in-memory cache
    executors_metadata: Arc<RwLock<HashMap<String, ExecutorMetadata>>>,

    jobs: Arc<RwLock<HashMap<String, JobStatus>>>,
    stages: Arc<RwLock<HashMap<StageKey, Arc<dyn ExecutionPlan>>>>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>
    PersistentSchedulerState<T, U>
{
    pub(crate) fn new(
        config_client: Arc<dyn StateBackendClient>,
        namespace: String,
        codec: BallistaCodec<T, U>,
    ) -> Self {
        Self {
            config_client,
            namespace,
            codec,
            executors_metadata: Arc::new(RwLock::new(HashMap::new())),
            jobs: Arc::new(RwLock::new(HashMap::new())),
            stages: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Load the state stored in storage into memory
    pub(crate) async fn init(&self, ctx: &SessionContext) -> Result<()> {
        self.init_executors_metadata_from_storage().await?;
        self.init_jobs_from_storage().await?;
        self.init_stages_from_storage(ctx).await?;

        Ok(())
    }

    async fn init_executors_metadata_from_storage(&self) -> Result<()> {
        let entries = self
            .config_client
            .get_from_prefix(&get_executors_metadata_prefix(&self.namespace))
            .await?;

        let mut executors_metadata = self.executors_metadata.write();
        for (_key, entry) in entries {
            let meta: protobuf::ExecutorMetadata = decode_protobuf(&entry)?;
            executors_metadata.insert(meta.id.clone(), meta.into());
        }

        Ok(())
    }

    async fn init_jobs_from_storage(&self) -> Result<()> {
        let entries = self
            .config_client
            .get_from_prefix(&get_job_prefix(&self.namespace))
            .await?;

        let mut jobs = self.jobs.write();
        for (key, entry) in entries {
            let job: JobStatus = decode_protobuf(&entry)?;
            let job_id = extract_job_id_from_job_key(&key)
                .map(|job_id| job_id.to_string())
                .unwrap();
            jobs.insert(job_id, job);
        }

        Ok(())
    }

    async fn init_stages_from_storage(&self, ctx: &SessionContext) -> Result<()> {
        let entries = self
            .config_client
            .get_from_prefix(&get_stage_prefix(&self.namespace))
            .await?;

        let mut stages = self.stages.write();
        for (key, entry) in entries {
            let (job_id, stage_id) = extract_stage_id_from_stage_key(&key).unwrap();
            let value = U::try_decode(&entry)?;
            let plan = value
                .try_into_physical_plan(ctx, self.codec.physical_extension_codec())?;

            stages.insert((job_id, stage_id), plan);
        }

        Ok(())
    }

    pub(crate) async fn save_executor_metadata(
        &self,
        executor_meta: ExecutorMetadata,
    ) -> Result<()> {
        {
            // Save in db
            let key = get_executor_metadata_key(&self.namespace, &executor_meta.id);
            let value = {
                let executor_meta: protobuf::ExecutorMetadata =
                    executor_meta.clone().into();
                encode_protobuf(&executor_meta)?
            };
            self.synchronize_save(key, value).await?;
        }

        {
            // Save in memory
            let mut executors_metadata = self.executors_metadata.write();
            executors_metadata.insert(executor_meta.id.clone(), executor_meta);
        }

        Ok(())
    }

    pub(crate) fn get_executor_metadata(
        &self,
        executor_id: &str,
    ) -> Option<ExecutorMetadata> {
        let executors_metadata = self.executors_metadata.read();
        executors_metadata.get(executor_id).cloned()
    }

    pub(crate) fn get_executors_metadata(&self) -> Vec<ExecutorMetadata> {
        let executors_metadata = self.executors_metadata.read();
        executors_metadata.values().cloned().collect()
    }

    pub(crate) async fn save_job_metadata(
        &self,
        job_id: &str,
        status: &JobStatus,
    ) -> Result<()> {
        debug!("Saving job metadata: {:?}", status);
        {
            // Save in db
            let key = get_job_key(&self.namespace, job_id);
            let value = encode_protobuf(status)?;
            self.synchronize_save(key, value).await?;
        }

        {
            // Save in memory
            let mut jobs = self.jobs.write();
            jobs.insert(job_id.to_string(), status.clone());
        }

        Ok(())
    }

    pub(crate) fn get_job_metadata(&self, job_id: &str) -> Option<JobStatus> {
        let jobs = self.jobs.read();
        jobs.get(job_id).cloned()
    }

    pub(crate) async fn save_stage_plan(
        &self,
        job_id: &str,
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<()> {
        {
            // Save in db
            let key = get_stage_plan_key(&self.namespace, job_id, stage_id as u32);
            let value = {
                let mut buf: Vec<u8> = vec![];
                let proto = U::try_from_physical_plan(
                    plan.clone(),
                    self.codec.physical_extension_codec(),
                )?;
                proto.try_encode(&mut buf)?;

                buf
            };
            self.synchronize_save(key, value).await?;
        }

        {
            // Save in memory
            let mut stages = self.stages.write();
            stages.insert((job_id.to_string(), stage_id as u32), plan);
        }

        Ok(())
    }

    pub(crate) fn get_stage_plan(
        &self,
        job_id: &str,
        stage_id: usize,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        let stages = self.stages.read();
        let key = (job_id.to_string(), stage_id as u32);
        stages.get(&key).cloned()
    }

    async fn synchronize_save(&self, key: String, value: Vec<u8>) -> Result<()> {
        let mut lock = self.config_client.lock().await?;
        self.config_client.put(key, value).await?;
        lock.unlock().await;

        Ok(())
    }
}

fn get_executors_metadata_prefix(namespace: &str) -> String {
    format!("/ballista/{}/executor_metadata", namespace)
}

fn get_executor_metadata_key(namespace: &str, id: &str) -> String {
    format!("{}/{}", get_executors_metadata_prefix(namespace), id)
}

fn get_job_prefix(namespace: &str) -> String {
    format!("/ballista/{}/jobs", namespace)
}

fn get_job_key(namespace: &str, id: &str) -> String {
    format!("{}/{}", get_job_prefix(namespace), id)
}

fn get_stage_prefix(namespace: &str) -> String {
    format!("/ballista/{}/stages", namespace,)
}

fn get_stage_plan_key(namespace: &str, job_id: &str, stage_id: u32) -> String {
    format!("{}/{}/{}", get_stage_prefix(namespace), job_id, stage_id,)
}

fn extract_job_id_from_job_key(job_key: &str) -> Result<&str> {
    job_key.split('/').nth(2).ok_or_else(|| {
        BallistaError::Internal(format!("Unexpected task key: {}", job_key))
    })
}

fn extract_stage_id_from_stage_key(stage_key: &str) -> Result<StageKey> {
    let splits: Vec<&str> = stage_key.split('/').collect();
    if splits.len() < 4 {
        Err(BallistaError::Internal(format!(
            "Unexpected stage key: {}",
            stage_key
        )))
    } else {
        Ok((
            splits.get(2).unwrap().to_string(),
            splits.get(3).unwrap().parse::<u32>().unwrap(),
        ))
    }
}

fn decode_protobuf<T: Message + Default>(bytes: &[u8]) -> Result<T> {
    T::decode(bytes).map_err(|e| {
        BallistaError::Internal(format!(
            "Could not deserialize {}: {}",
            type_name::<T>(),
            e
        ))
    })
}

fn encode_protobuf<T: Message + Default>(msg: &T) -> Result<Vec<u8>> {
    let mut value: Vec<u8> = Vec::with_capacity(msg.encoded_len());
    msg.encode(&mut value).map_err(|e| {
        BallistaError::Internal(format!(
            "Could not serialize {}: {}",
            type_name::<T>(),
            e
        ))
    })?;
    Ok(value)
}
