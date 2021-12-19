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

use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    any::type_name, collections::HashMap, convert::TryInto, sync::Arc, time::Duration,
};

use datafusion::physical_plan::ExecutionPlan;
use futures::{Stream, StreamExt};
use log::{debug, error, info};
use prost::Message;
use tokio::sync::OwnedMutexGuard;

use ballista_core::serde::protobuf::{
    self, job_status, task_status, CompletedJob, CompletedTask, ExecutorHeartbeat,
    ExecutorMetadata, FailedJob, FailedTask, JobStatus, PhysicalPlanNode, RunningJob,
    RunningTask, TaskStatus,
};
use ballista_core::serde::scheduler::PartitionStats;
use ballista_core::{error::BallistaError, serde::scheduler::ExecutorMeta};
use ballista_core::{error::Result, execution_plans::UnresolvedShuffleExec};

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

#[derive(Clone)]
pub(super) struct SchedulerState {
    config_client: Arc<dyn ConfigBackendClient>,
    namespace: String,
}

impl SchedulerState {
    pub fn new(config_client: Arc<dyn ConfigBackendClient>, namespace: String) -> Self {
        Self {
            config_client,
            namespace,
        }
    }

    pub async fn get_executors_metadata(&self) -> Result<Vec<(ExecutorMeta, Duration)>> {
        let mut result = vec![];

        let entries = self
            .config_client
            .get_from_prefix(&get_executors_prefix(&self.namespace))
            .await?;
        let now_epoch_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        for (_key, entry) in entries {
            let heartbeat: ExecutorHeartbeat = decode_protobuf(&entry)?;
            let meta = heartbeat.meta.unwrap();
            let ts = Duration::from_secs(heartbeat.timestamp);
            let time_since_last_seen = now_epoch_ts
                .checked_sub(ts)
                .unwrap_or_else(|| Duration::from_secs(0));
            result.push((meta.into(), time_since_last_seen));
        }
        Ok(result)
    }

    pub async fn get_alive_executors_metadata(
        &self,
        last_seen_threshold: Duration,
    ) -> Result<Vec<ExecutorMeta>> {
        Ok(self
            .get_executors_metadata()
            .await?
            .into_iter()
            .filter_map(|(exec, last_seen)| {
                (last_seen < last_seen_threshold).then(|| exec)
            })
            .collect())
    }

    pub async fn save_executor_metadata(&self, meta: ExecutorMeta) -> Result<()> {
        let key = get_executor_key(&self.namespace, &meta.id);
        let meta: ExecutorMetadata = meta.into();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        let heartbeat = ExecutorHeartbeat {
            meta: Some(meta),
            timestamp,
        };
        let value: Vec<u8> = encode_protobuf(&heartbeat)?;
        self.config_client.put(key, value).await
    }

    pub async fn save_job_metadata(
        &self,
        job_id: &str,
        status: &JobStatus,
    ) -> Result<()> {
        debug!("Saving job metadata: {:?}", status);
        let key = get_job_key(&self.namespace, job_id);
        let value = encode_protobuf(status)?;
        self.config_client.put(key, value).await
    }

    pub async fn get_job_metadata(&self, job_id: &str) -> Result<JobStatus> {
        let key = get_job_key(&self.namespace, job_id);
        let value = &self.config_client.get(&key).await?;
        if value.is_empty() {
            return Err(BallistaError::General(format!(
                "No job metadata found for {}",
                key
            )));
        }
        let value: JobStatus = decode_protobuf(value)?;
        Ok(value)
    }

    pub async fn save_task_status(&self, status: &TaskStatus) -> Result<()> {
        let partition_id = status.partition_id.as_ref().unwrap();
        let key = get_task_status_key(
            &self.namespace,
            &partition_id.job_id,
            partition_id.stage_id as usize,
            partition_id.partition_id as usize,
        );
        let value = encode_protobuf(status)?;
        self.config_client.put(key, value).await
    }

    pub async fn _get_task_status(
        &self,
        job_id: &str,
        stage_id: usize,
        partition_id: usize,
    ) -> Result<TaskStatus> {
        let key = get_task_status_key(&self.namespace, job_id, stage_id, partition_id);
        let value = &self.config_client.clone().get(&key).await?;
        if value.is_empty() {
            return Err(BallistaError::General(format!(
                "No task status found for {}",
                key
            )));
        }
        let value: TaskStatus = decode_protobuf(value)?;
        Ok(value)
    }

    // "Unnecessary" lifetime syntax due to https://github.com/rust-lang/rust/issues/63033
    pub async fn save_stage_plan<'a>(
        &'a self,
        job_id: &'a str,
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<()> {
        let key = get_stage_plan_key(&self.namespace, job_id, stage_id);
        let value = {
            let proto: PhysicalPlanNode = plan.try_into()?;
            encode_protobuf(&proto)?
        };
        self.config_client.clone().put(key, value).await
    }

    pub async fn get_stage_plan(
        &self,
        job_id: &str,
        stage_id: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let key = get_stage_plan_key(&self.namespace, job_id, stage_id);
        let value = &self.config_client.get(&key).await?;
        if value.is_empty() {
            return Err(BallistaError::General(format!(
                "No stage plan found for {}",
                key
            )));
        }
        let value: PhysicalPlanNode = decode_protobuf(value)?;
        Ok((&value).try_into()?)
    }

    pub async fn get_all_tasks(&self) -> Result<HashMap<String, TaskStatus>> {
        self.config_client
            .get_from_prefix(&get_task_prefix(&self.namespace))
            .await?
            .into_iter()
            .map(|(key, bytes)| Ok((key, decode_protobuf(&bytes)?)))
            .collect()
    }

    /// This function ensures that the task wasn't assigned to an executor that died.
    /// If that is the case, then the task is re-scheduled.
    /// Returns true if the task was dead, false otherwise.
    async fn reschedule_dead_task(
        &self,
        task_status: &TaskStatus,
        executors: &[ExecutorMeta],
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
                task_status.partition_id.as_ref().unwrap()
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
        let tasks = self.get_all_tasks().await?;
        // TODO: Make the duration a configurable parameter
        let executors = self
            .get_alive_executors_metadata(Duration::from_secs(60))
            .await?;
        'tasks: for (_key, status) in tasks.iter() {
            if status.status.is_none() {
                let partition = status.partition_id.as_ref().unwrap();
                let plan = self
                    .get_stage_plan(&partition.job_id, partition.stage_id as usize)
                    .await?;

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
                        let referenced_task = tasks
                            .get(&get_task_status_key(
                                &self.namespace,
                                &partition.job_id,
                                unresolved_shuffle.stage_id,
                                shuffle_input_partition_id,
                            ))
                            .unwrap();
                        let task_is_dead = self
                            .reschedule_dead_task(referenced_task, &executors)
                            .await?;
                        if task_is_dead {
                            continue 'tasks;
                        } else if let Some(task_status::Status::Completed(
                            CompletedTask {
                                executor_id,
                                partitions,
                            },
                        )) = &referenced_task.status
                        {
                            debug!("Task for unresolved shuffle input partition {} completed and produced these shuffle partitions:\n\t{}",
                                shuffle_input_partition_id,
                                partitions.iter().map(|p| format!("{}={}", p.partition_id, &p.path)).collect::<Vec<_>>().join("\n\t")
                            );
                            let stage_shuffle_partition_locations = partition_locations
                                .entry(unresolved_shuffle.stage_id)
                                .or_insert_with(HashMap::new);
                            let executor_meta = executors
                                .iter()
                                .find(|exec| exec.id == *executor_id)
                                .unwrap()
                                .clone();

                            for shuffle_write_partition in partitions {
                                let temp = stage_shuffle_partition_locations
                                    .entry(shuffle_write_partition.partition_id as usize)
                                    .or_insert_with(Vec::new);
                                let executor_meta = executor_meta.clone();
                                let partition_location =
                                    ballista_core::serde::scheduler::PartitionLocation {
                                        partition_id:
                                            ballista_core::serde::scheduler::PartitionId {
                                                job_id: partition.job_id.clone(),
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
                        } else {
                            debug!(
                                "Stage {} input partition {} has not completed yet",
                                unresolved_shuffle.stage_id, shuffle_input_partition_id,
                            );
                            continue 'tasks;
                        }
                    }
                }

                let plan =
                    remove_unresolved_shuffles(plan.as_ref(), &partition_locations)?;

                // If we get here, there are no more unresolved shuffled and the task can be run
                let mut status = status.clone();
                status.status = Some(task_status::Status::Running(RunningTask {
                    executor_id: executor_id.to_owned(),
                }));
                self.save_task_status(&status).await?;
                return Ok(Some((status, plan)));
            }
        }
        Ok(None)
    }

    // Global lock for the state. We should get rid of this to be able to scale.
    pub async fn lock(&self) -> Result<Box<dyn Lock>> {
        self.config_client.lock().await
    }

    /// This function starts a watch over the task keys. Whenever a task changes, it re-evaluates
    /// the status for the parent job and updates it accordingly.
    ///
    /// The future returned by this function never returns (unless an error happens), so it is wise
    /// to [tokio::spawn] calls to this method.
    pub async fn synchronize_job_status_loop(&self) -> Result<()> {
        let watch = self
            .config_client
            .watch(get_task_prefix(&self.namespace))
            .await?;
        watch.for_each(|event: WatchEvent| async {
            let key = match event {
                WatchEvent::Put(key, _value) => key,
                WatchEvent::Delete(key) => key
            };
            let job_id = extract_job_id_from_task_key(&key).unwrap();
            match self.lock().await {
                Ok(mut lock) => {
                    if let Err(e) = self.synchronize_job_status(job_id).await {
                        error!("Could not update job status for {}. This job might be stuck forever. Error: {}", job_id, e);
                    }
                    lock.unlock().await;
                },
                Err(e) => error!("Could not lock config backend. Job {} will have an unsynchronized status and might be stuck forever. Error: {}", job_id, e)
            }
        }).await;

        Ok(())
    }

    async fn synchronize_job_status(&self, job_id: &str) -> Result<()> {
        let value = self
            .config_client
            .get(&get_job_key(&self.namespace, job_id))
            .await?;
        let executors: HashMap<String, ExecutorMeta> = self
            .get_executors_metadata()
            .await?
            .into_iter()
            .map(|(meta, _)| (meta.id.to_string(), meta))
            .collect();
        let status: JobStatus = decode_protobuf(&value)?;
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
        executors: &HashMap<String, ExecutorMeta>,
    ) -> Result<Option<JobStatus>> {
        let statuses = self
            .config_client
            .get_from_prefix(&get_task_prefix_for_job(&self.namespace, job_id))
            .await?
            .into_iter()
            .map(|(_k, v)| decode_protobuf::<TaskStatus>(&v))
            .collect::<Result<Vec<_>>>()?;
        if statuses.is_empty() {
            return Ok(None);
        }

        // Check for job completion
        let last_stage = statuses
            .iter()
            .map(|task| task.partition_id.as_ref().unwrap().stage_id)
            .max()
            .unwrap();
        let statuses: Vec<_> = statuses
            .into_iter()
            .filter(|task| task.partition_id.as_ref().unwrap().stage_id == last_stage)
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
                    let input_partition_id = status.partition_id.as_ref().unwrap(); //TODO unwrap
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

fn get_executors_prefix(namespace: &str) -> String {
    format!("/ballista/{}/executors", namespace)
}

fn get_executor_key(namespace: &str, id: &str) -> String {
    format!("{}/{}", get_executors_prefix(namespace), id)
}

fn get_job_prefix(namespace: &str) -> String {
    format!("/ballista/{}/jobs", namespace)
}

fn get_job_key(namespace: &str, id: &str) -> String {
    format!("{}/{}", get_job_prefix(namespace), id)
}

fn get_task_prefix(namespace: &str) -> String {
    format!("/ballista/{}/tasks", namespace)
}

fn get_task_prefix_for_job(namespace: &str, job_id: &str) -> String {
    format!("{}/{}", get_task_prefix(namespace), job_id)
}

fn get_task_status_key(
    namespace: &str,
    job_id: &str,
    stage_id: usize,
    partition_id: usize,
) -> String {
    format!(
        "{}/{}/{}",
        get_task_prefix_for_job(namespace, job_id),
        stage_id,
        partition_id,
    )
}

fn extract_job_id_from_task_key(job_key: &str) -> Result<&str> {
    job_key.split('/').nth(4).ok_or_else(|| {
        BallistaError::Internal(format!("Unexpected task key: {}", job_key))
    })
}

fn get_stage_plan_key(namespace: &str, job_id: &str, stage_id: usize) -> String {
    format!("/ballista/{}/stages/{}/{}", namespace, job_id, stage_id,)
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

#[cfg(all(test, feature = "sled"))]
mod test {
    use std::sync::Arc;

    use ballista_core::serde::protobuf::{
        job_status, task_status, CompletedTask, FailedTask, JobStatus, PartitionId,
        QueuedJob, RunningJob, RunningTask, TaskStatus,
    };
    use ballista_core::{error::BallistaError, serde::scheduler::ExecutorMeta};

    use super::{
        extract_job_id_from_task_key, get_task_status_key, SchedulerState,
        StandaloneClient,
    };

    #[tokio::test]
    async fn executor_metadata() -> Result<(), BallistaError> {
        let state = SchedulerState::new(
            Arc::new(StandaloneClient::try_new_temporary()?),
            "test".to_string(),
        );
        let meta = ExecutorMeta {
            id: "123".to_owned(),
            host: "localhost".to_owned(),
            port: 123,
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
        let state = SchedulerState::new(
            Arc::new(StandaloneClient::try_new_temporary()?),
            "test".to_string(),
        );
        let meta = JobStatus {
            status: Some(job_status::Status::Queued(QueuedJob {})),
        };
        state.save_job_metadata("job", &meta).await?;
        let result = state.get_job_metadata("job").await?;
        assert!(result.status.is_some());
        match result.status.unwrap() {
            job_status::Status::Queued(_) => (),
            _ => panic!("Unexpected status"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn job_metadata_non_existant() -> Result<(), BallistaError> {
        let state = SchedulerState::new(
            Arc::new(StandaloneClient::try_new_temporary()?),
            "test".to_string(),
        );
        let meta = JobStatus {
            status: Some(job_status::Status::Queued(QueuedJob {})),
        };
        state.save_job_metadata("job", &meta).await?;
        let result = state.get_job_metadata("job2").await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn task_status() -> Result<(), BallistaError> {
        let state = SchedulerState::new(
            Arc::new(StandaloneClient::try_new_temporary()?),
            "test".to_string(),
        );
        let meta = TaskStatus {
            status: Some(task_status::Status::Failed(FailedTask {
                error: "error".to_owned(),
            })),
            partition_id: Some(PartitionId {
                job_id: "job".to_owned(),
                stage_id: 1,
                partition_id: 2,
            }),
        };
        state.save_task_status(&meta).await?;
        let result = state._get_task_status("job", 1, 2).await?;
        assert!(result.status.is_some());
        match result.status.unwrap() {
            task_status::Status::Failed(_) => (),
            _ => panic!("Unexpected status"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn task_status_non_existant() -> Result<(), BallistaError> {
        let state = SchedulerState::new(
            Arc::new(StandaloneClient::try_new_temporary()?),
            "test".to_string(),
        );
        let meta = TaskStatus {
            status: Some(task_status::Status::Failed(FailedTask {
                error: "error".to_owned(),
            })),
            partition_id: Some(PartitionId {
                job_id: "job".to_owned(),
                stage_id: 1,
                partition_id: 2,
            }),
        };
        state.save_task_status(&meta).await?;
        let result = state._get_task_status("job", 25, 2).await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn task_synchronize_job_status_queued() -> Result<(), BallistaError> {
        let state = SchedulerState::new(
            Arc::new(StandaloneClient::try_new_temporary()?),
            "test".to_string(),
        );
        let job_id = "job";
        let job_status = JobStatus {
            status: Some(job_status::Status::Queued(QueuedJob {})),
        };
        state.save_job_metadata(job_id, &job_status).await?;
        state.synchronize_job_status(job_id).await?;
        let result = state.get_job_metadata(job_id).await?;
        assert_eq!(result, job_status);
        Ok(())
    }

    #[tokio::test]
    async fn task_synchronize_job_status_running() -> Result<(), BallistaError> {
        let state = SchedulerState::new(
            Arc::new(StandaloneClient::try_new_temporary()?),
            "test".to_string(),
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
            partition_id: Some(PartitionId {
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
            partition_id: Some(PartitionId {
                job_id: job_id.to_owned(),
                stage_id: 0,
                partition_id: 1,
            }),
        };
        state.save_task_status(&meta).await?;
        state.synchronize_job_status(job_id).await?;
        let result = state.get_job_metadata(job_id).await?;
        assert_eq!(result, job_status);
        Ok(())
    }

    #[tokio::test]
    async fn task_synchronize_job_status_running2() -> Result<(), BallistaError> {
        let state = SchedulerState::new(
            Arc::new(StandaloneClient::try_new_temporary()?),
            "test".to_string(),
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
            partition_id: Some(PartitionId {
                job_id: job_id.to_owned(),
                stage_id: 0,
                partition_id: 0,
            }),
        };
        state.save_task_status(&meta).await?;
        let meta = TaskStatus {
            status: None,
            partition_id: Some(PartitionId {
                job_id: job_id.to_owned(),
                stage_id: 0,
                partition_id: 1,
            }),
        };
        state.save_task_status(&meta).await?;
        state.synchronize_job_status(job_id).await?;
        let result = state.get_job_metadata(job_id).await?;
        assert_eq!(result, job_status);
        Ok(())
    }

    #[tokio::test]
    async fn task_synchronize_job_status_completed() -> Result<(), BallistaError> {
        let state = SchedulerState::new(
            Arc::new(StandaloneClient::try_new_temporary()?),
            "test".to_string(),
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
            partition_id: Some(PartitionId {
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
            partition_id: Some(PartitionId {
                job_id: job_id.to_owned(),
                stage_id: 0,
                partition_id: 1,
            }),
        };
        state.save_task_status(&meta).await?;
        state.synchronize_job_status(job_id).await?;
        let result = state.get_job_metadata(job_id).await?;
        match result.status.unwrap() {
            job_status::Status::Completed(_) => (),
            status => panic!("Received status: {:?}", status),
        }
        Ok(())
    }

    #[tokio::test]
    async fn task_synchronize_job_status_completed2() -> Result<(), BallistaError> {
        let state = SchedulerState::new(
            Arc::new(StandaloneClient::try_new_temporary()?),
            "test".to_string(),
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
            partition_id: Some(PartitionId {
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
            partition_id: Some(PartitionId {
                job_id: job_id.to_owned(),
                stage_id: 0,
                partition_id: 1,
            }),
        };
        state.save_task_status(&meta).await?;
        state.synchronize_job_status(job_id).await?;
        let result = state.get_job_metadata(job_id).await?;
        match result.status.unwrap() {
            job_status::Status::Completed(_) => (),
            status => panic!("Received status: {:?}", status),
        }
        Ok(())
    }

    #[tokio::test]
    async fn task_synchronize_job_status_failed() -> Result<(), BallistaError> {
        let state = SchedulerState::new(
            Arc::new(StandaloneClient::try_new_temporary()?),
            "test".to_string(),
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
            partition_id: Some(PartitionId {
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
            partition_id: Some(PartitionId {
                job_id: job_id.to_owned(),
                stage_id: 0,
                partition_id: 1,
            }),
        };
        state.save_task_status(&meta).await?;
        let meta = TaskStatus {
            status: None,
            partition_id: Some(PartitionId {
                job_id: job_id.to_owned(),
                stage_id: 0,
                partition_id: 2,
            }),
        };
        state.save_task_status(&meta).await?;
        state.synchronize_job_status(job_id).await?;
        let result = state.get_job_metadata(job_id).await?;
        match result.status.unwrap() {
            job_status::Status::Failed(_) => (),
            status => panic!("Received status: {:?}", status),
        }
        Ok(())
    }

    #[test]
    fn task_extract_job_id_from_task_key() {
        let job_id = "foo";
        assert_eq!(
            extract_job_id_from_task_key(&get_task_status_key("namespace", job_id, 0, 1))
                .unwrap(),
            job_id
        );
    }
}
