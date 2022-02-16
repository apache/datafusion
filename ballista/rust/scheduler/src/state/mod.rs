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

use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{any::type_name, collections::HashMap, sync::Arc, time::Duration};

use parking_lot::RwLock;

use datafusion::physical_plan::ExecutionPlan;
use futures::Stream;
use log::{debug, error, info, warn};
use prost::Message;
use tokio::sync::{mpsc, OwnedMutexGuard};

use ballista_core::error::{BallistaError, Result};
use ballista_core::execution_plans::UnresolvedShuffleExec;
use ballista_core::serde::protobuf::{
    self, job_status, task_status, CompletedJob, CompletedTask, ExecutorHeartbeat,
    FailedJob, FailedTask, JobStatus, RunningJob, RunningTask, TaskStatus,
};
use ballista_core::serde::scheduler::{
    ExecutorData, ExecutorMetadata, PartitionId, PartitionStats,
};
use ballista_core::serde::{AsExecutionPlan, AsLogicalPlan, BallistaCodec};
use datafusion::prelude::ExecutionContext;

use super::planner::remove_unresolved_shuffles;

#[cfg(feature = "etcd")]
mod etcd;
#[cfg(feature = "sled")]
mod standalone;

#[cfg(feature = "etcd")]
pub use etcd::EtcdClient;
#[cfg(feature = "sled")]
pub use standalone::StandaloneClient;

/// A trait that contains the necessary methods to save and retrieve the state and configuration of a cluster.
#[tonic::async_trait]
pub trait ConfigBackendClient: Send + Sync {
    /// Retrieve the data associated with a specific key.
    ///
    /// An empty vec is returned if the key does not exist.
    async fn get(&self, key: &str) -> Result<Vec<u8>>;

    /// Retrieve all data associated with a specific key.
    async fn get_from_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>>;

    /// Saves the value into the provided key, overriding any previous data that might have been associated to that key.
    async fn put(&self, key: String, value: Vec<u8>) -> Result<()>;

    async fn lock(&self) -> Result<Box<dyn Lock>>;

    /// Watch all events that happen on a specific prefix.
    async fn watch(&self, prefix: String) -> Result<Box<dyn Watch>>;
}

/// A Watch is a cancelable stream of put or delete events in the [ConfigBackendClient]
#[tonic::async_trait]
pub trait Watch: Stream<Item = WatchEvent> + Send + Unpin {
    async fn cancel(&mut self) -> Result<()>;
}

#[derive(Debug, PartialEq)]
pub enum WatchEvent {
    /// Contains the inserted or updated key and the new value
    Put(String, Vec<u8>),

    /// Contains the deleted key
    Delete(String),
}

type JobTasks = HashMap<u32, HashMap<u32, TaskStatus>>;

#[derive(Clone)]
struct VolatileSchedulerState {
    executors_heartbeat: Arc<RwLock<HashMap<String, ExecutorHeartbeat>>>,
    executors_data: Arc<RwLock<HashMap<String, ExecutorData>>>,

    // job -> stage -> partition
    tasks: Arc<RwLock<HashMap<String, JobTasks>>>,
}

/// For in-memory state, we don't use async to provide related services
impl VolatileSchedulerState {
    fn new() -> Self {
        Self {
            executors_heartbeat: Arc::new(RwLock::new(HashMap::new())),
            executors_data: Arc::new(RwLock::new(HashMap::new())),
            tasks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn save_executor_heartbeat(&self, heartbeat: ExecutorHeartbeat) {
        let mut executors_heartbeat = self.executors_heartbeat.write();
        executors_heartbeat.insert(heartbeat.executor_id.clone(), heartbeat);
    }

    fn get_executors_heartbeat(&self) -> Vec<ExecutorHeartbeat> {
        let executors_heartbeat = self.executors_heartbeat.read();
        executors_heartbeat
            .iter()
            .map(|(_exec, heartbeat)| heartbeat.clone())
            .collect()
    }

    /// last_seen_ts_threshold is in seconds
    fn get_alive_executors(&self, last_seen_ts_threshold: u64) -> HashSet<String> {
        let executors_heartbeat = self.executors_heartbeat.read();
        executors_heartbeat
            .iter()
            .filter_map(|(exec, heartbeat)| {
                (heartbeat.timestamp > last_seen_ts_threshold).then(|| exec.clone())
            })
            .collect()
    }

    fn get_alive_executors_within_one_minute(&self) -> HashSet<String> {
        let now_epoch_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let last_seen_threshold = now_epoch_ts
            .checked_sub(Duration::from_secs(60))
            .unwrap_or_else(|| Duration::from_secs(0));
        self.get_alive_executors(last_seen_threshold.as_secs())
    }

    fn save_executor_data(&self, executor_data: ExecutorData) {
        let mut executors_data = self.executors_data.write();
        executors_data.insert(executor_data.executor_id.clone(), executor_data);
    }

    fn get_executor_data(&self, executor_id: &str) -> Option<ExecutorData> {
        let executors_data = self.executors_data.read();
        executors_data.get(executor_id).cloned()
    }

    /// There are two checks:
    /// 1. firstly alive
    /// 2. secondly available task slots > 0
    fn get_available_executors_data(&self) -> Vec<ExecutorData> {
        let mut res = {
            let alive_executors = self.get_alive_executors_within_one_minute();
            let executors_data = self.executors_data.read();
            executors_data
                .iter()
                .filter_map(|(exec, data)| {
                    alive_executors.contains(exec).then(|| data.clone())
                })
                .collect::<Vec<ExecutorData>>()
        };
        res.sort_by(|a, b| Ord::cmp(&b.available_task_slots, &a.available_task_slots));
        res
    }

    fn save_task_status(&self, status: &TaskStatus) {
        let task_id = status.task_id.as_ref().unwrap();
        let mut tasks = self.tasks.write();
        let job_tasks = tasks
            .entry(task_id.job_id.clone())
            .or_insert_with(HashMap::new);
        let stage_tasks = job_tasks
            .entry(task_id.stage_id)
            .or_insert_with(HashMap::new);
        stage_tasks.insert(task_id.partition_id, status.clone());
    }

    fn _get_task(
        &self,
        job_id: &str,
        stage_id: usize,
        partition_id: usize,
    ) -> Option<TaskStatus> {
        let tasks = self.tasks.read();
        let job_tasks = tasks.get(job_id);
        if let Some(job_tasks) = job_tasks {
            let stage_id = stage_id as u32;
            let stage_tasks = job_tasks.get(&stage_id);
            if let Some(stage_tasks) = stage_tasks {
                let partition_id = partition_id as u32;
                stage_tasks.get(&partition_id).cloned()
            } else {
                None
            }
        } else {
            None
        }
    }

    fn get_job_tasks(&self, job_id: &str) -> Option<Vec<TaskStatus>> {
        let tasks = self.tasks.read();
        let job_tasks = tasks.get(job_id);

        if let Some(job_tasks) = job_tasks {
            let mut res = vec![];
            VolatileSchedulerState::fill_job_tasks(&mut res, job_tasks);
            Some(res)
        } else {
            None
        }
    }

    fn get_tasks(&self) -> Vec<TaskStatus> {
        let mut res = vec![];

        let tasks = self.tasks.read();
        for (_job_id, job_tasks) in tasks.iter() {
            VolatileSchedulerState::fill_job_tasks(&mut res, job_tasks);
        }

        res
    }

    fn fill_job_tasks(
        res: &mut Vec<TaskStatus>,
        job_tasks: &HashMap<u32, HashMap<u32, TaskStatus>>,
    ) {
        for stage_tasks in job_tasks.values() {
            for task_status in stage_tasks.values() {
                res.push(task_status.clone());
            }
        }
    }
}

type StageKey = (String, u32);

#[derive(Clone)]
struct StableSchedulerState<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    // for db
    config_client: Arc<dyn ConfigBackendClient>,
    namespace: String,
    codec: BallistaCodec<T, U>,

    // for in-memory cache
    executors_metadata: Arc<RwLock<HashMap<String, ExecutorMetadata>>>,

    jobs: Arc<RwLock<HashMap<String, JobStatus>>>,
    stages: Arc<RwLock<HashMap<StageKey, Arc<dyn ExecutionPlan>>>>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>
    StableSchedulerState<T, U>
{
    fn new(
        config_client: Arc<dyn ConfigBackendClient>,
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
    async fn init(&self, ctx: &ExecutionContext) -> Result<()> {
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

    async fn init_stages_from_storage(&self, ctx: &ExecutionContext) -> Result<()> {
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

    pub async fn save_executor_metadata(
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

    fn get_executor_metadata(&self, executor_id: &str) -> Option<ExecutorMetadata> {
        let executors_metadata = self.executors_metadata.read();
        executors_metadata.get(executor_id).cloned()
    }

    fn get_executors_metadata(&self) -> Vec<ExecutorMetadata> {
        let executors_metadata = self.executors_metadata.read();
        executors_metadata.values().cloned().collect()
    }

    async fn save_job_metadata(&self, job_id: &str, status: &JobStatus) -> Result<()> {
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

    fn get_job_metadata(&self, job_id: &str) -> Option<JobStatus> {
        let jobs = self.jobs.read();
        jobs.get(job_id).cloned()
    }

    async fn save_stage_plan(
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

    fn get_stage_plan(
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

#[derive(Clone)]
struct SchedulerStateWatcher {
    tx_task: mpsc::Sender<TaskStatus>,
}

impl SchedulerStateWatcher {
    async fn watch(&self, task_status: TaskStatus) -> Result<()> {
        self.tx_task.send(task_status).await.map_err(|e| {
            BallistaError::Internal(format!(
                "Fail to send task status event to channel due to {:?}",
                e
            ))
        })?;

        Ok(())
    }

    fn synchronize_job_status_loop<
        T: 'static + AsLogicalPlan,
        U: 'static + AsExecutionPlan,
    >(
        &self,
        scheduler_state: SchedulerState<T, U>,
        mut rx_task: mpsc::Receiver<TaskStatus>,
    ) {
        tokio::spawn(async move {
            info!("Starting the scheduler state watcher");
            loop {
                if let Some(task_status) = rx_task.recv().await {
                    debug!("Watch on task status {:?}", task_status);
                    if let Some(task_id) = task_status.task_id {
                        scheduler_state
                            .synchronize_job_status(&task_id.job_id)
                            .await
                            .unwrap_or_else(|e| {
                                error!(
                                    "Fail to synchronize the status for job {:?} due to {:?}",
                                    task_id.job_id, e
                                );
                            });
                    } else {
                        warn!(
                            "There's no PartitionId in the task status {:?}",
                            task_status
                        );
                    }
                } else {
                    info!("Channel is closed and will exit the loop");
                    return;
                };
            }
        });
    }
}

#[derive(Clone)]
pub(super) struct SchedulerState<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>
{
    stable_state: StableSchedulerState<T, U>,
    volatile_state: VolatileSchedulerState,
    listener: SchedulerStateWatcher,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> SchedulerState<T, U> {
    pub fn new(
        config_client: Arc<dyn ConfigBackendClient>,
        namespace: String,
        codec: BallistaCodec<T, U>,
    ) -> Self {
        // TODO Make the buffer size configurable
        let (tx_task, rx_task) = mpsc::channel::<TaskStatus>(1000);
        let ret = Self {
            stable_state: StableSchedulerState::new(config_client, namespace, codec),
            volatile_state: VolatileSchedulerState::new(),
            listener: SchedulerStateWatcher { tx_task },
        };
        ret.listener
            .synchronize_job_status_loop(ret.clone(), rx_task);

        ret
    }

    pub async fn init(&self, ctx: &ExecutionContext) -> Result<()> {
        self.stable_state.init(ctx).await?;

        Ok(())
    }

    pub async fn get_executors_metadata(
        &self,
    ) -> Result<Vec<(ExecutorMetadata, Duration)>> {
        let mut result = vec![];

        let executors_heartbeat = self
            .volatile_state
            .get_executors_heartbeat()
            .into_iter()
            .map(|heartbeat| (heartbeat.executor_id.clone(), heartbeat))
            .collect::<HashMap<String, ExecutorHeartbeat>>();

        let executors_metadata = self.stable_state.get_executors_metadata();

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

    pub async fn get_alive_executors_metadata(
        &self,
        last_seen_threshold: Duration,
    ) -> Result<Vec<ExecutorMetadata>> {
        let mut result = vec![];

        let now_epoch_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let last_seen_ts_threshold = now_epoch_ts
            .checked_sub(last_seen_threshold)
            .unwrap_or_else(|| Duration::from_secs(0));
        let alive_executors = self
            .volatile_state
            .get_alive_executors(last_seen_ts_threshold.as_secs());
        for executor_id in alive_executors {
            let meta = self.get_executor_metadata(&executor_id);
            if meta.is_none() {
                return Err(BallistaError::General(format!(
                    "No executor metadata found for {}",
                    executor_id
                )));
            }
            result.push(meta.unwrap());
        }

        Ok(result)
    }

    pub fn get_executor_metadata(&self, executor_id: &str) -> Option<ExecutorMetadata> {
        self.stable_state.get_executor_metadata(executor_id)
    }

    pub async fn save_executor_metadata(
        &self,
        executor_meta: ExecutorMetadata,
    ) -> Result<()> {
        self.stable_state
            .save_executor_metadata(executor_meta)
            .await
    }

    pub fn save_executor_heartbeat(&self, heartbeat: ExecutorHeartbeat) {
        self.volatile_state.save_executor_heartbeat(heartbeat);
    }

    pub fn save_executor_data(&self, executor_data: ExecutorData) {
        self.volatile_state.save_executor_data(executor_data);
    }

    pub fn get_available_executors_data(&self) -> Vec<ExecutorData> {
        self.volatile_state.get_available_executors_data()
    }

    pub fn get_executor_data(&self, executor_id: &str) -> Option<ExecutorData> {
        self.volatile_state.get_executor_data(executor_id)
    }

    pub async fn save_job_metadata(
        &self,
        job_id: &str,
        status: &JobStatus,
    ) -> Result<()> {
        self.stable_state.save_job_metadata(job_id, status).await
    }

    pub fn get_job_metadata(&self, job_id: &str) -> Option<JobStatus> {
        self.stable_state.get_job_metadata(job_id)
    }

    pub async fn save_stage_plan(
        &self,
        job_id: &str,
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<()> {
        self.stable_state
            .save_stage_plan(job_id, stage_id, plan)
            .await
    }

    pub fn get_stage_plan(
        &self,
        job_id: &str,
        stage_id: usize,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        self.stable_state.get_stage_plan(job_id, stage_id)
    }

    pub async fn save_task_status(&self, status: &TaskStatus) -> Result<()> {
        self.volatile_state.save_task_status(status);
        self.listener.watch(status.clone()).await?;

        Ok(())
    }

    pub fn _get_task_status(
        &self,
        job_id: &str,
        stage_id: usize,
        partition_id: usize,
    ) -> Option<TaskStatus> {
        self.volatile_state
            ._get_task(job_id, stage_id, partition_id)
    }

    pub fn get_job_tasks(&self, job_id: &str) -> Option<Vec<TaskStatus>> {
        self.volatile_state.get_job_tasks(job_id)
    }

    pub fn get_all_tasks(&self) -> Vec<TaskStatus> {
        self.volatile_state.get_tasks()
    }

    /// This function ensures that the task wasn't assigned to an executor that died.
    /// If that is the case, then the task is re-scheduled.
    /// Returns true if the task was dead, false otherwise.
    async fn reschedule_dead_task(
        &self,
        task_status: &TaskStatus,
        executors: &[ExecutorMetadata],
    ) -> Result<bool> {
        let executor_id: &str = match &task_status.status {
            Some(task_status::Status::Completed(CompletedTask {
                executor_id, ..
            })) => executor_id,
            Some(task_status::Status::Running(RunningTask { executor_id })) => {
                executor_id
            }
            _ => return Ok(false),
        };
        let executor_meta = executors.iter().find(|exec| exec.id == executor_id);
        let task_is_dead = executor_meta.is_none();
        if task_is_dead {
            info!(
                "Executor {} isn't alive. Rescheduling task {:?}",
                executor_id,
                task_status.task_id.as_ref().unwrap()
            );
            // Task was handled in an executor that isn't alive anymore, so we can't resolve it
            // We mark the task as pending again and continue
            let mut task_status = task_status.clone();
            task_status.status = None;
            self.save_task_status(&task_status).await?;
        }
        Ok(task_is_dead)
    }

    pub async fn assign_next_schedulable_task(
        &self,
        executor_id: &str,
    ) -> Result<Option<(TaskStatus, Arc<dyn ExecutionPlan>)>> {
        let tasks = self.get_all_tasks();
        self.assign_next_schedulable_task_inner(executor_id, tasks)
            .await
    }

    pub async fn assign_next_schedulable_job_task(
        &self,
        executor_id: &str,
        job_id: &str,
    ) -> Result<Option<(TaskStatus, Arc<dyn ExecutionPlan>)>> {
        let job_tasks = self.get_job_tasks(job_id);
        if job_tasks.is_some() {
            self.assign_next_schedulable_task_inner(executor_id, job_tasks.unwrap())
                .await
        } else {
            Ok(None)
        }
    }

    async fn assign_next_schedulable_task_inner(
        &self,
        executor_id: &str,
        tasks: Vec<TaskStatus>,
    ) -> Result<Option<(TaskStatus, Arc<dyn ExecutionPlan>)>> {
        match self.get_next_schedulable_task(tasks).await? {
            Some((status, plan)) => {
                let mut status = status.clone();
                status.status = Some(task_status::Status::Running(RunningTask {
                    executor_id: executor_id.to_owned(),
                }));
                self.save_task_status(&status).await?;
                Ok(Some((status, plan)))
            }
            _ => Ok(None),
        }
    }

    async fn get_next_schedulable_task(
        &self,
        tasks: Vec<TaskStatus>,
    ) -> Result<Option<(TaskStatus, Arc<dyn ExecutionPlan>)>> {
        let tasks = tasks
            .into_iter()
            .map(|task| {
                let task_id = task.task_id.as_ref().unwrap();
                (
                    PartitionId::new(
                        &task_id.job_id,
                        task_id.stage_id as usize,
                        task_id.partition_id as usize,
                    ),
                    task,
                )
            })
            .collect::<HashMap<PartitionId, TaskStatus>>();
        // TODO: Make the duration a configurable parameter
        let executors = self
            .get_alive_executors_metadata(Duration::from_secs(60))
            .await?;
        'tasks: for (_key, status) in tasks.iter() {
            if status.status.is_none() {
                let task_id = status.task_id.as_ref().unwrap();
                let plan = self
                    .get_stage_plan(&task_id.job_id, task_id.stage_id as usize)
                    .unwrap();

                // Let's try to resolve any unresolved shuffles we find
                let unresolved_shuffles = find_unresolved_shuffles(&plan)?;
                let mut partition_locations: HashMap<
                    usize, // stage id
                    HashMap<
                        usize, // shuffle output partition id
                        Vec<ballista_core::serde::scheduler::PartitionLocation>, // shuffle partitions
                    >,
                > = HashMap::new();
                for unresolved_shuffle in unresolved_shuffles {
                    // we schedule one task per *input* partition and each input partition
                    // can produce multiple output partitions
                    for shuffle_input_partition_id in
                        0..unresolved_shuffle.input_partition_count
                    {
                        let partition_id = PartitionId {
                            job_id: task_id.job_id.clone(),
                            stage_id: unresolved_shuffle.stage_id,
                            partition_id: shuffle_input_partition_id,
                        };
                        let referenced_task = tasks.get(&partition_id).unwrap();
                        let task_is_dead = self
                            .reschedule_dead_task(referenced_task, &executors)
                            .await?;
                        if task_is_dead {
                            continue 'tasks;
                        }
                        match &referenced_task.status {
                            Some(task_status::Status::Completed(CompletedTask {
                                executor_id,
                                partitions,
                            })) => {
                                debug!("Task for unresolved shuffle input partition {} completed and produced these shuffle partitions:\n\t{}",
                                    shuffle_input_partition_id,
                                    partitions.iter().map(|p| format!("{}={}", p.partition_id, &p.path)).collect::<Vec<_>>().join("\n\t")
                                );
                                let stage_shuffle_partition_locations =
                                    partition_locations
                                        .entry(unresolved_shuffle.stage_id)
                                        .or_insert_with(HashMap::new);
                                let executor_meta = executors
                                    .iter()
                                    .find(|exec| exec.id == *executor_id)
                                    .unwrap()
                                    .clone();

                                for shuffle_write_partition in partitions {
                                    let temp = stage_shuffle_partition_locations
                                        .entry(
                                            shuffle_write_partition.partition_id as usize,
                                        )
                                        .or_insert_with(Vec::new);
                                    let executor_meta = executor_meta.clone();
                                    let partition_location =
                                        ballista_core::serde::scheduler::PartitionLocation {
                                            partition_id:
                                            ballista_core::serde::scheduler::PartitionId {
                                                job_id: task_id.job_id.clone(),
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
                            Some(task_status::Status::Failed(FailedTask { error })) => {
                                // A task should fail when its referenced_task fails
                                let mut status = status.clone();
                                let err_msg = error.to_string();
                                status.status =
                                    Some(task_status::Status::Failed(FailedTask {
                                        error: err_msg,
                                    }));
                                self.save_task_status(&status).await?;
                                continue 'tasks;
                            }
                            _ => {
                                debug!(
                                    "Stage {} input partition {} has not completed yet",
                                    unresolved_shuffle.stage_id,
                                    shuffle_input_partition_id,
                                );
                                continue 'tasks;
                            }
                        };
                    }
                }

                let plan =
                    remove_unresolved_shuffles(plan.as_ref(), &partition_locations)?;

                // If we get here, there are no more unresolved shuffled and the task can be run
                return Ok(Some((status.clone(), plan)));
            }
        }
        Ok(None)
    }

    async fn synchronize_job_status(&self, job_id: &str) -> Result<()> {
        let executors: HashMap<String, ExecutorMetadata> = self
            .get_executors_metadata()
            .await?
            .into_iter()
            .map(|(meta, _)| (meta.id.to_string(), meta))
            .collect();
        let status: JobStatus = self.stable_state.get_job_metadata(job_id).unwrap();
        let new_status = self.get_job_status_from_tasks(job_id, &executors).await?;
        if let Some(new_status) = new_status {
            if status != new_status {
                info!(
                    "Changing status for job {} to {:?}",
                    job_id, new_status.status
                );
                debug!("Old status: {:?}", status);
                debug!("New status: {:?}", new_status);
                self.save_job_metadata(job_id, &new_status).await?;
            }
        }
        Ok(())
    }

    async fn get_job_status_from_tasks(
        &self,
        job_id: &str,
        executors: &HashMap<String, ExecutorMetadata>,
    ) -> Result<Option<JobStatus>> {
        let statuses = self.volatile_state.get_job_tasks(job_id);
        if statuses.is_none() {
            return Ok(None);
        }
        let statuses = statuses.unwrap();
        if statuses.is_empty() {
            return Ok(None);
        }

        // Check for job completion
        let last_stage = statuses
            .iter()
            .map(|task| task.task_id.as_ref().unwrap().stage_id)
            .max()
            .unwrap();
        let statuses: Vec<_> = statuses
            .into_iter()
            .filter(|task| task.task_id.as_ref().unwrap().stage_id == last_stage)
            .collect();
        let mut job_status = statuses
            .iter()
            .map(|status| match &status.status {
                Some(task_status::Status::Completed(CompletedTask {
                    executor_id,
                    partitions,
                })) => Ok((status, executor_id, partitions)),
                _ => Err(BallistaError::General("Task not completed".to_string())),
            })
            .collect::<Result<Vec<_>>>()
            .ok()
            .map(|info| {
                let mut partition_location = vec![];
                for (status, executor_id, partitions) in info {
                    let input_partition_id = status.task_id.as_ref().unwrap(); //TODO unwrap
                    let executor_meta =
                        executors.get(executor_id).map(|e| e.clone().into());
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
            for status in statuses {
                match status.status {
                    Some(task_status::Status::Failed(FailedTask { error })) => {
                        job_status =
                            Some(job_status::Status::Failed(FailedJob { error }));
                        break;
                    }
                    Some(task_status::Status::Running(_)) if job_status == None => {
                        job_status = Some(job_status::Status::Running(RunningJob {}));
                    }
                    _ => (),
                }
            }
        }
        Ok(job_status.map(|status| JobStatus {
            status: Some(status),
        }))
    }
}

#[tonic::async_trait]
pub trait Lock: Send + Sync {
    async fn unlock(&mut self);
}

#[tonic::async_trait]
impl<T: Send + Sync> Lock for OwnedMutexGuard<T> {
    async fn unlock(&mut self) {}
}

/// Returns the unresolved shuffles in the execution plan
fn find_unresolved_shuffles(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Vec<UnresolvedShuffleExec>> {
    if let Some(unresolved_shuffle) =
        plan.as_any().downcast_ref::<UnresolvedShuffleExec>()
    {
        Ok(vec![unresolved_shuffle.clone()])
    } else {
        Ok(plan
            .children()
            .iter()
            .map(find_unresolved_shuffles)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect())
    }
}

#[cfg(all(test, feature = "sled"))]
mod test {
    use std::sync::Arc;

    use ballista_core::error::BallistaError;
    use ballista_core::serde::protobuf::{
        job_status, task_status, CompletedTask, FailedTask, JobStatus, LogicalPlanNode,
        PartitionId, PhysicalPlanNode, QueuedJob, RunningJob, RunningTask, TaskStatus,
    };
    use ballista_core::serde::scheduler::{ExecutorMetadata, ExecutorSpecification};
    use ballista_core::serde::BallistaCodec;

    use super::{SchedulerState, StandaloneClient};

    #[tokio::test]
    async fn executor_metadata() -> Result<(), BallistaError> {
        let state: SchedulerState<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerState::new(
                Arc::new(StandaloneClient::try_new_temporary()?),
                "test".to_string(),
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

    #[tokio::test]
    async fn task_status() -> Result<(), BallistaError> {
        let state: SchedulerState<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerState::new(
                Arc::new(StandaloneClient::try_new_temporary()?),
                "test".to_string(),
                BallistaCodec::default(),
            );
        let meta = TaskStatus {
            status: Some(task_status::Status::Failed(FailedTask {
                error: "error".to_owned(),
            })),
            task_id: Some(PartitionId {
                job_id: "job".to_owned(),
                stage_id: 1,
                partition_id: 2,
            }),
        };
        state.save_task_status(&meta).await?;
        let result = state._get_task_status("job", 1, 2);
        assert!(result.is_some());
        match result.unwrap().status.unwrap() {
            task_status::Status::Failed(_) => (),
            _ => panic!("Unexpected status"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn task_status_non_existant() -> Result<(), BallistaError> {
        let state: SchedulerState<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerState::new(
                Arc::new(StandaloneClient::try_new_temporary()?),
                "test".to_string(),
                BallistaCodec::default(),
            );
        let meta = TaskStatus {
            status: Some(task_status::Status::Failed(FailedTask {
                error: "error".to_owned(),
            })),
            task_id: Some(PartitionId {
                job_id: "job".to_owned(),
                stage_id: 1,
                partition_id: 2,
            }),
        };
        state.save_task_status(&meta).await?;
        let result = state._get_task_status("job", 25, 2);
        assert!(result.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn task_synchronize_job_status_queued() -> Result<(), BallistaError> {
        let state: SchedulerState<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerState::new(
                Arc::new(StandaloneClient::try_new_temporary()?),
                "test".to_string(),
                BallistaCodec::default(),
            );
        let job_id = "job";
        let job_status = JobStatus {
            status: Some(job_status::Status::Queued(QueuedJob {})),
        };
        state.save_job_metadata(job_id, &job_status).await?;
        // Call it explicitly to achieve fast synchronization
        state.synchronize_job_status(job_id).await?;
        let result = state.get_job_metadata(job_id).unwrap();
        assert_eq!(result, job_status);
        Ok(())
    }

    #[tokio::test]
    async fn task_synchronize_job_status_running() -> Result<(), BallistaError> {
        let state: SchedulerState<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerState::new(
                Arc::new(StandaloneClient::try_new_temporary()?),
                "test".to_string(),
                BallistaCodec::default(),
            );
        let job_id = "job";
        let job_status = JobStatus {
            status: Some(job_status::Status::Running(RunningJob {})),
        };
        state.save_job_metadata(job_id, &job_status).await?;
        let meta = TaskStatus {
            status: Some(task_status::Status::Completed(CompletedTask {
                executor_id: "".to_owned(),
                partitions: vec![],
            })),
            task_id: Some(PartitionId {
                job_id: job_id.to_owned(),
                stage_id: 0,
                partition_id: 0,
            }),
        };
        state.save_task_status(&meta).await?;
        let meta = TaskStatus {
            status: Some(task_status::Status::Running(RunningTask {
                executor_id: "".to_owned(),
            })),
            task_id: Some(PartitionId {
                job_id: job_id.to_owned(),
                stage_id: 0,
                partition_id: 1,
            }),
        };
        state.save_task_status(&meta).await?;
        // Call it explicitly to achieve fast synchronization
        state.synchronize_job_status(job_id).await?;
        let result = state.get_job_metadata(job_id).unwrap();
        assert_eq!(result, job_status);
        Ok(())
    }

    #[tokio::test]
    async fn task_synchronize_job_status_running2() -> Result<(), BallistaError> {
        let state: SchedulerState<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerState::new(
                Arc::new(StandaloneClient::try_new_temporary()?),
                "test".to_string(),
                BallistaCodec::default(),
            );
        let job_id = "job";
        let job_status = JobStatus {
            status: Some(job_status::Status::Running(RunningJob {})),
        };
        state.save_job_metadata(job_id, &job_status).await?;
        let meta = TaskStatus {
            status: Some(task_status::Status::Completed(CompletedTask {
                executor_id: "".to_owned(),
                partitions: vec![],
            })),
            task_id: Some(PartitionId {
                job_id: job_id.to_owned(),
                stage_id: 0,
                partition_id: 0,
            }),
        };
        state.save_task_status(&meta).await?;
        let meta = TaskStatus {
            status: None,
            task_id: Some(PartitionId {
                job_id: job_id.to_owned(),
                stage_id: 0,
                partition_id: 1,
            }),
        };
        state.save_task_status(&meta).await?;
        // Call it explicitly to achieve fast synchronization
        state.synchronize_job_status(job_id).await?;
        let result = state.get_job_metadata(job_id).unwrap();
        assert_eq!(result, job_status);
        Ok(())
    }

    #[tokio::test]
    async fn task_synchronize_job_status_completed() -> Result<(), BallistaError> {
        let state: SchedulerState<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerState::new(
                Arc::new(StandaloneClient::try_new_temporary()?),
                "test".to_string(),
                BallistaCodec::default(),
            );
        let job_id = "job";
        let job_status = JobStatus {
            status: Some(job_status::Status::Running(RunningJob {})),
        };
        state.save_job_metadata(job_id, &job_status).await?;
        let meta = TaskStatus {
            status: Some(task_status::Status::Completed(CompletedTask {
                executor_id: "".to_owned(),
                partitions: vec![],
            })),
            task_id: Some(PartitionId {
                job_id: job_id.to_owned(),
                stage_id: 0,
                partition_id: 0,
            }),
        };
        state.save_task_status(&meta).await?;
        let meta = TaskStatus {
            status: Some(task_status::Status::Completed(CompletedTask {
                executor_id: "".to_owned(),
                partitions: vec![],
            })),
            task_id: Some(PartitionId {
                job_id: job_id.to_owned(),
                stage_id: 0,
                partition_id: 1,
            }),
        };
        state.save_task_status(&meta).await?;
        // Call it explicitly to achieve fast synchronization
        state.synchronize_job_status(job_id).await?;
        let result = state.get_job_metadata(job_id).unwrap();
        match result.status.unwrap() {
            job_status::Status::Completed(_) => (),
            status => panic!("Received status: {:?}", status),
        }
        Ok(())
    }

    #[tokio::test]
    async fn task_synchronize_job_status_completed2() -> Result<(), BallistaError> {
        let state: SchedulerState<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerState::new(
                Arc::new(StandaloneClient::try_new_temporary()?),
                "test".to_string(),
                BallistaCodec::default(),
            );
        let job_id = "job";
        let job_status = JobStatus {
            status: Some(job_status::Status::Queued(QueuedJob {})),
        };
        state.save_job_metadata(job_id, &job_status).await?;
        let meta = TaskStatus {
            status: Some(task_status::Status::Completed(CompletedTask {
                executor_id: "".to_owned(),
                partitions: vec![],
            })),
            task_id: Some(PartitionId {
                job_id: job_id.to_owned(),
                stage_id: 0,
                partition_id: 0,
            }),
        };
        state.save_task_status(&meta).await?;
        let meta = TaskStatus {
            status: Some(task_status::Status::Completed(CompletedTask {
                executor_id: "".to_owned(),
                partitions: vec![],
            })),
            task_id: Some(PartitionId {
                job_id: job_id.to_owned(),
                stage_id: 0,
                partition_id: 1,
            }),
        };
        state.save_task_status(&meta).await?;
        // Call it explicitly to achieve fast synchronization
        state.synchronize_job_status(job_id).await?;
        let result = state.get_job_metadata(job_id).unwrap();
        match result.status.unwrap() {
            job_status::Status::Completed(_) => (),
            status => panic!("Received status: {:?}", status),
        }
        Ok(())
    }

    #[tokio::test]
    async fn task_synchronize_job_status_failed() -> Result<(), BallistaError> {
        let state: SchedulerState<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerState::new(
                Arc::new(StandaloneClient::try_new_temporary()?),
                "test".to_string(),
                BallistaCodec::default(),
            );
        let job_id = "job";
        let job_status = JobStatus {
            status: Some(job_status::Status::Running(RunningJob {})),
        };
        state.save_job_metadata(job_id, &job_status).await?;
        let meta = TaskStatus {
            status: Some(task_status::Status::Completed(CompletedTask {
                executor_id: "".to_owned(),
                partitions: vec![],
            })),
            task_id: Some(PartitionId {
                job_id: job_id.to_owned(),
                stage_id: 0,
                partition_id: 0,
            }),
        };
        state.save_task_status(&meta).await?;
        let meta = TaskStatus {
            status: Some(task_status::Status::Failed(FailedTask {
                error: "".to_owned(),
            })),
            task_id: Some(PartitionId {
                job_id: job_id.to_owned(),
                stage_id: 0,
                partition_id: 1,
            }),
        };
        state.save_task_status(&meta).await?;
        let meta = TaskStatus {
            status: None,
            task_id: Some(PartitionId {
                job_id: job_id.to_owned(),
                stage_id: 0,
                partition_id: 2,
            }),
        };
        state.save_task_status(&meta).await?;
        // Call it explicitly to achieve fast synchronization
        state.synchronize_job_status(job_id).await?;
        let result = state.get_job_metadata(job_id).unwrap();
        match result.status.unwrap() {
            job_status::Status::Failed(_) => (),
            status => panic!("Received status: {:?}", status),
        }
        Ok(())
    }
}
