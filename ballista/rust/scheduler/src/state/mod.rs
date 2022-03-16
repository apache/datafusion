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

use log::{debug, error, info, warn};
use tokio::sync::mpsc;

use ballista_core::error::{BallistaError, Result};
use ballista_core::execution_plans::UnresolvedShuffleExec;

use ballista_core::serde::protobuf::{
    job_status, task_status, CompletedJob, CompletedTask, ExecutorHeartbeat, FailedJob,
    FailedTask, JobStatus, RunningJob, RunningTask, TaskStatus,
};
use ballista_core::serde::scheduler::{
    ExecutorData, ExecutorDataChange, ExecutorMetadata, PartitionId, PartitionStats,
};
use ballista_core::serde::{protobuf, AsExecutionPlan, AsLogicalPlan, BallistaCodec};
use datafusion::prelude::ExecutionContext;

use super::planner::remove_unresolved_shuffles;

use crate::state::backend::StateBackendClient;
use crate::state::in_memory_state::InMemorySchedulerState;
use crate::state::persistent_state::PersistentSchedulerState;

pub mod backend;
mod in_memory_state;
mod persistent_state;
pub mod task_scheduler;

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
    persistent_state: PersistentSchedulerState<T, U>,
    in_memory_state: InMemorySchedulerState,
    listener: SchedulerStateWatcher,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> SchedulerState<T, U> {
    pub fn new(
        config_client: Arc<dyn StateBackendClient>,
        namespace: String,
        codec: BallistaCodec<T, U>,
    ) -> Self {
        // TODO Make the buffer size configurable
        let (tx_task, rx_task) = mpsc::channel::<TaskStatus>(1000);
        let ret = Self {
            persistent_state: PersistentSchedulerState::new(
                config_client,
                namespace,
                codec,
            ),
            in_memory_state: InMemorySchedulerState::new(),
            listener: SchedulerStateWatcher { tx_task },
        };
        ret.listener
            .synchronize_job_status_loop(ret.clone(), rx_task);

        ret
    }

    pub async fn init(&self, ctx: &ExecutionContext) -> Result<()> {
        self.persistent_state.init(ctx).await?;

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
            .in_memory_state
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
            .in_memory_state
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

    pub fn save_executor_heartbeat(&self, heartbeat: ExecutorHeartbeat) {
        self.in_memory_state.save_executor_heartbeat(heartbeat);
    }

    pub fn save_executor_data(&self, executor_data: ExecutorData) {
        self.in_memory_state.save_executor_data(executor_data);
    }

    pub fn update_executor_data(&self, executor_data_change: &ExecutorDataChange) {
        self.in_memory_state
            .update_executor_data(executor_data_change);
    }

    pub fn get_available_executors_data(&self) -> Vec<ExecutorData> {
        self.in_memory_state.get_available_executors_data()
    }

    pub fn get_executor_data(&self, executor_id: &str) -> Option<ExecutorData> {
        self.in_memory_state.get_executor_data(executor_id)
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

    pub async fn save_task_status(&self, status: &TaskStatus) -> Result<()> {
        self.in_memory_state.save_task_status(status);
        self.listener.watch(status.clone()).await?;

        Ok(())
    }

    pub fn _get_task_status(
        &self,
        job_id: &str,
        stage_id: usize,
        partition_id: usize,
    ) -> Option<TaskStatus> {
        self.in_memory_state
            ._get_task(job_id, stage_id, partition_id)
    }

    pub fn get_job_tasks(&self, job_id: &str) -> Option<Vec<TaskStatus>> {
        self.in_memory_state.get_job_tasks(job_id)
    }

    pub fn get_all_tasks(&self) -> Vec<TaskStatus> {
        self.in_memory_state.get_tasks()
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
        let status: JobStatus = self.persistent_state.get_job_metadata(job_id).unwrap();
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
        let statuses = self.in_memory_state.get_job_tasks(job_id);
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

    use super::{backend::standalone::StandaloneClient, SchedulerState};

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
