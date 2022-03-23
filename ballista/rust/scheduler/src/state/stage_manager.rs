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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use log::{debug, error, warn};
use parking_lot::RwLock;
use rand::Rng;

use crate::scheduler_server::event::QueryStageSchedulerEvent;
use crate::state::task_scheduler::StageScheduler;
use ballista_core::error::{BallistaError, Result};
use ballista_core::serde::protobuf;
use ballista_core::serde::protobuf::{task_status, FailedTask, TaskStatus};

pub type StageKey = (String, u32);

#[derive(Clone)]
pub struct StageManager {
    stage_distribution: Arc<RwLock<StageDistribution>>,

    // The final stage id for jobs
    final_stages: Arc<RwLock<HashMap<String, u32>>>,

    // (job_id, stage_id) -> stage set in which each one depends on (job_id, stage_id)
    stages_dependency: Arc<RwLock<HashMap<StageKey, HashSet<u32>>>>,

    // job_id -> pending stages
    pending_stages: Arc<RwLock<HashMap<String, HashSet<u32>>>>,
}

impl StageManager {
    pub fn new() -> Self {
        Self {
            stage_distribution: Arc::new(RwLock::new(StageDistribution::new())),
            final_stages: Arc::new(RwLock::new(HashMap::new())),
            stages_dependency: Arc::new(RwLock::new(HashMap::new())),
            pending_stages: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn add_final_stage(&self, job_id: &str, stage_id: u32) {
        let mut final_stages = self.final_stages.write();
        final_stages.insert(job_id.to_owned(), stage_id);
    }

    pub fn is_final_stage(&self, job_id: &str, stage_id: u32) -> bool {
        self.get_final_stage_id(job_id)
            .map(|final_stage_id| final_stage_id == stage_id)
            .unwrap_or(false)
    }

    fn get_final_stage_id(&self, job_id: &str) -> Option<u32> {
        let final_stages = self.final_stages.read();
        final_stages.get(job_id).cloned()
    }

    pub fn get_tasks_for_complete_final_stage(
        &self,
        job_id: &str,
    ) -> Result<Vec<Arc<TaskStatus>>> {
        let final_stage_id = self.get_final_stage_id(job_id).ok_or_else(|| {
            BallistaError::General(format!(
                "Fail to find final stage id for job {}",
                job_id
            ))
        })?;

        let stage_key = (job_id.to_owned(), final_stage_id);
        let stage_distribution = self.stage_distribution.read();

        if let Some(stage) = stage_distribution.stages_completed.get(&stage_key) {
            Ok(stage.tasks.clone())
        } else {
            Err(BallistaError::General(format!(
                "The final stage id {} has not been completed yet",
                final_stage_id
            )))
        }
    }

    pub fn add_pending_stage(&self, job_id: &str, stage_id: u32) {
        let mut pending_stages = self.pending_stages.write();
        pending_stages
            .entry(job_id.to_owned())
            .or_insert_with(HashSet::new)
            .insert(stage_id);
    }

    pub fn is_pending_stage(&self, job_id: &str, stage_id: u32) -> bool {
        let pending_stages = self.pending_stages.read();
        if let Some(pending_stages) = pending_stages.get(job_id) {
            pending_stages.contains(&stage_id)
        } else {
            false
        }
    }

    pub fn remove_pending_stage(
        &self,
        job_id: &str,
        stages_remove: &HashSet<u32>,
    ) -> bool {
        let mut pending_stages = self.pending_stages.write();
        let mut is_stages_empty = false;
        let ret = if let Some(stages) = pending_stages.get_mut(job_id) {
            let len_before_remove = stages.len();
            for stage_id in stages_remove {
                stages.remove(stage_id);
            }
            is_stages_empty = stages.is_empty();
            stages.len() != len_before_remove
        } else {
            false
        };

        if is_stages_empty {
            pending_stages.remove(job_id);
        }

        ret
    }

    pub fn add_stages_dependency(
        &self,
        job_id: &str,
        dependencies: HashMap<u32, HashSet<u32>>,
    ) {
        let mut stages_dependency = self.stages_dependency.write();
        for (stage_id, parent_stages) in dependencies.into_iter() {
            stages_dependency.insert((job_id.to_owned(), stage_id), parent_stages);
        }
    }

    pub fn get_parent_stages(&self, job_id: &str, stage_id: u32) -> Option<HashSet<u32>> {
        let stage_key = (job_id.to_owned(), stage_id);
        let stages_dependency = self.stages_dependency.read();
        stages_dependency.get(&stage_key).cloned()
    }

    pub fn add_running_stage(&self, job_id: &str, stage_id: u32, num_partitions: u32) {
        let stage = Stage::new(job_id, stage_id, num_partitions);

        let mut stage_distribution = self.stage_distribution.write();
        stage_distribution
            .stages_running
            .insert((job_id.to_string(), stage_id), stage);
    }

    pub fn is_running_stage(&self, job_id: &str, stage_id: u32) -> bool {
        let stage_key = (job_id.to_owned(), stage_id);
        let stage_distribution = self.stage_distribution.read();
        stage_distribution.stages_running.get(&stage_key).is_some()
    }

    pub fn is_completed_stage(&self, job_id: &str, stage_id: u32) -> bool {
        let stage_key = (job_id.to_owned(), stage_id);
        let stage_distribution = self.stage_distribution.read();
        stage_distribution
            .stages_completed
            .get(&stage_key)
            .is_some()
    }

    pub(crate) fn get_stage_tasks(
        &self,
        job_id: &str,
        stage_id: u32,
    ) -> Option<Vec<Arc<TaskStatus>>> {
        let stage_key = (job_id.to_owned(), stage_id);
        let stage_distribution = self.stage_distribution.read();
        if let Some(stage) = stage_distribution.stages_running.get(&stage_key) {
            Some(stage.tasks.clone())
        } else {
            stage_distribution
                .stages_completed
                .get(&stage_key)
                .map(|task| task.tasks.clone())
        }
    }

    pub(crate) fn update_tasks_status(
        &self,
        tasks_status: Vec<TaskStatus>,
    ) -> Vec<QueryStageSchedulerEvent> {
        let mut all_tasks_status: HashMap<StageKey, Vec<TaskStatus>> = HashMap::new();
        for task_status in tasks_status {
            if let Some(task_id) = task_status.task_id.as_ref() {
                let stage_tasks_status = all_tasks_status
                    .entry((task_id.job_id.clone(), task_id.stage_id))
                    .or_insert_with(Vec::new);
                stage_tasks_status.push(task_status);
            } else {
                error!("There's no task id when updating status");
            }
        }

        let mut ret = vec![];
        let mut stage_distribution = self.stage_distribution.write();
        for (stage_key, stage_tasks_status) in all_tasks_status.into_iter() {
            if let Some(stage) = stage_distribution.stages_running.get_mut(&stage_key) {
                for task_status in &stage_tasks_status {
                    stage.update_task_status(task_status);
                }
                if let Some(fail_message) = stage.get_fail_message() {
                    ret.push(QueryStageSchedulerEvent::JobFailed(
                        stage_key.0.clone(),
                        stage_key.1,
                        fail_message,
                    ));
                } else if stage.is_completed() {
                    stage_distribution.complete_stage(stage_key.clone());
                    if self.is_final_stage(&stage_key.0, stage_key.1) {
                        ret.push(QueryStageSchedulerEvent::JobFinished(
                            stage_key.0.clone(),
                        ));
                    } else {
                        ret.push(QueryStageSchedulerEvent::StageFinished(
                            stage_key.0.clone(),
                            stage_key.1,
                        ));
                    }
                }
            } else {
                error!("Fail to find stage for {:?}/{}", &stage_key.0, stage_key.1);
            }
        }

        ret
    }

    pub fn fetch_pending_tasks<F>(
        &self,
        max_num: usize,
        cond: F,
    ) -> Option<(String, u32, Vec<u32>)>
    where
        F: Fn(&StageKey) -> bool,
    {
        if let Some(next_stage) = self.fetch_schedulable_stage(cond) {
            if let Some(next_tasks) =
                self.find_stage_pending_tasks(&next_stage.0, next_stage.1, max_num)
            {
                Some((next_stage.0.to_owned(), next_stage.1, next_tasks))
            } else {
                warn!(
                    "Fail to find pending tasks for stage {}/{}",
                    next_stage.0, next_stage.1
                );
                None
            }
        } else {
            None
        }
    }

    fn find_stage_pending_tasks(
        &self,
        job_id: &str,
        stage_id: u32,
        max_num: usize,
    ) -> Option<Vec<u32>> {
        let stage_key = (job_id.to_owned(), stage_id);
        let stage_distribution = self.stage_distribution.read();
        stage_distribution
            .stages_running
            .get(&stage_key)
            .map(|stage| stage.find_pending_tasks(max_num))
    }

    pub fn has_running_tasks(&self) -> bool {
        let stage_distribution = self.stage_distribution.read();
        for stage in stage_distribution.stages_running.values() {
            if !stage.get_running_tasks().is_empty() {
                return true;
            }
        }

        false
    }
}

// TODO Currently, it will randomly choose a stage. In the future, we can add more sophisticated stage choose algorithm here, like priority, etc.
impl StageScheduler for StageManager {
    fn fetch_schedulable_stage<F>(&self, cond: F) -> Option<StageKey>
    where
        F: Fn(&StageKey) -> bool,
    {
        let mut rng = rand::thread_rng();
        let stage_distribution = self.stage_distribution.read();
        let stages_running = &stage_distribution.stages_running;
        if stages_running.is_empty() {
            debug!("There's no running stages");
            return None;
        }
        let stages = stages_running
            .iter()
            .filter(|entry| entry.1.is_schedulable() && cond(entry.0))
            .map(|entry| entry.0)
            .collect::<Vec<&StageKey>>();
        if stages.is_empty() {
            None
        } else {
            let n_th = rng.gen_range(0..stages.len());
            Some(stages[n_th].clone())
        }
    }
}

struct StageDistribution {
    // The key is (job_id, stage_id)
    stages_running: HashMap<StageKey, Stage>,
    stages_completed: HashMap<StageKey, Stage>,
}

impl StageDistribution {
    fn new() -> Self {
        Self {
            stages_running: HashMap::new(),
            stages_completed: HashMap::new(),
        }
    }

    fn complete_stage(&mut self, stage_key: StageKey) {
        if let Some(stage) = self.stages_running.remove(&stage_key) {
            assert!(
                stage.is_completed(),
                "Stage {}/{} is not completed",
                stage_key.0,
                stage_key.1
            );
            self.stages_completed.insert(stage_key, stage);
        } else {
            warn!(
                "Fail to find running stage {:?}/{}",
                stage_key.0, stage_key.1
            );
        }
    }
}

pub struct Stage {
    pub stage_id: u32,
    tasks: Vec<Arc<TaskStatus>>,

    tasks_distribution: TaskStatusDistribution,
}

impl Stage {
    fn new(job_id: &str, stage_id: u32, num_partitions: u32) -> Self {
        let mut tasks = vec![];
        for partition_id in 0..num_partitions {
            let pending_status = Arc::new(TaskStatus {
                task_id: Some(protobuf::PartitionId {
                    job_id: job_id.to_owned(),
                    stage_id,
                    partition_id,
                }),
                status: None,
            });

            tasks.push(pending_status);
        }

        Stage {
            stage_id,
            tasks,
            tasks_distribution: TaskStatusDistribution::new(num_partitions as usize),
        }
    }

    // If error happens for updating some task status, just quietly print the error message
    fn update_task_status(&mut self, task: &TaskStatus) {
        if let Some(task_id) = &task.task_id {
            let task_idx = task_id.partition_id as usize;
            if task_idx < self.tasks.len() {
                let existing_task_status = self.tasks[task_idx].clone();
                if self.tasks_distribution.update(
                    task_idx,
                    &existing_task_status.status,
                    &task.status,
                ) {
                    self.tasks[task_idx] = Arc::new(task.clone());
                } else {
                    error!(
                        "Fail to update status from {:?} to {:?} for task: {:?}/{:?}/{:?}", &existing_task_status.status, &task.status,
                        &task_id.job_id, &task_id.stage_id, task_idx
                    )
                }
            } else {
                error!(
                    "Fail to find existing task: {:?}/{:?}/{:?}",
                    &task_id.job_id, &task_id.stage_id, task_idx
                )
            }
        } else {
            error!("Fail to update task status due to no task id");
        }
    }

    fn is_schedulable(&self) -> bool {
        self.tasks_distribution.is_schedulable()
    }

    fn is_completed(&self) -> bool {
        self.tasks_distribution.is_completed()
    }

    // If return None, means no failed tasks
    fn get_fail_message(&self) -> Option<String> {
        if self.tasks_distribution.is_failed() {
            let task_idx = self.tasks_distribution.sample_failed_index();
            if let Some(task) = self.tasks.get(task_idx) {
                if let Some(task_status::Status::Failed(FailedTask { error })) =
                    &task.status
                {
                    Some(error.clone())
                } else {
                    warn!("task {:?} is not failed", task);
                    None
                }
            } else {
                warn!("Could not find error tasks");
                None
            }
        } else {
            None
        }
    }

    pub fn find_pending_tasks(&self, max_num: usize) -> Vec<u32> {
        self.tasks_distribution.find_pending_indicators(max_num)
    }

    fn get_running_tasks(&self) -> Vec<Arc<TaskStatus>> {
        self.tasks_distribution
            .running_indicator
            .indicator
            .iter()
            .enumerate()
            .filter(|(_i, is_running)| **is_running)
            .map(|(i, _is_running)| self.tasks[i].clone())
            .collect()
    }
}

#[derive(Clone)]
struct TaskStatusDistribution {
    len: usize,
    pending_indicator: TaskStatusIndicator,
    running_indicator: TaskStatusIndicator,
    failed_indicator: TaskStatusIndicator,
    completed_indicator: TaskStatusIndicator,
}

impl TaskStatusDistribution {
    fn new(len: usize) -> Self {
        Self {
            len,
            pending_indicator: TaskStatusIndicator {
                indicator: (0..len).map(|_| true).collect::<Vec<bool>>(),
                n_of_true: len,
            },
            running_indicator: TaskStatusIndicator {
                indicator: (0..len).map(|_| false).collect::<Vec<bool>>(),
                n_of_true: 0,
            },
            failed_indicator: TaskStatusIndicator {
                indicator: (0..len).map(|_| false).collect::<Vec<bool>>(),
                n_of_true: 0,
            },
            completed_indicator: TaskStatusIndicator {
                indicator: (0..len).map(|_| false).collect::<Vec<bool>>(),
                n_of_true: 0,
            },
        }
    }

    fn is_schedulable(&self) -> bool {
        self.pending_indicator.n_of_true != 0
    }

    fn is_completed(&self) -> bool {
        self.completed_indicator.n_of_true == self.len
    }

    fn is_failed(&self) -> bool {
        self.failed_indicator.n_of_true != 0
    }

    fn sample_failed_index(&self) -> usize {
        for i in 0..self.len {
            if self.failed_indicator.indicator[i] {
                return i;
            }
        }

        self.len
    }

    fn find_pending_indicators(&self, max_num: usize) -> Vec<u32> {
        let mut ret = vec![];
        if max_num < 1 {
            return ret;
        }

        let len = std::cmp::min(max_num, self.len);
        for idx in 0..self.len {
            if self.pending_indicator.indicator[idx] {
                ret.push(idx as u32);
                if ret.len() >= len {
                    break;
                }
            }
        }

        ret
    }

    fn update(
        &mut self,
        idx: usize,
        from: &Option<task_status::Status>,
        to: &Option<task_status::Status>,
    ) -> bool {
        assert!(
            idx < self.len,
            "task index {} should be smaller than {}",
            idx,
            self.len
        );

        match (from, to) {
            (Some(from), Some(to)) => match (from, to) {
                (task_status::Status::Running(_), task_status::Status::Failed(_)) => {
                    self.running_indicator.set_false(idx);
                    self.failed_indicator.set_true(idx);
                }
                (task_status::Status::Running(_), task_status::Status::Completed(_)) => {
                    self.running_indicator.set_false(idx);
                    self.completed_indicator.set_true(idx);
                }
                _ => {
                    return false;
                }
            },
            (None, Some(task_status::Status::Running(_))) => {
                self.pending_indicator.set_false(idx);
                self.running_indicator.set_true(idx);
            }
            (Some(from), None) => match from {
                task_status::Status::Failed(_) => {
                    self.failed_indicator.set_false(idx);
                    self.pending_indicator.set_true(idx);
                }
                task_status::Status::Completed(_) => {
                    self.completed_indicator.set_false(idx);
                    self.pending_indicator.set_true(idx);
                }
                _ => {
                    return false;
                }
            },
            _ => {
                return false;
            }
        }

        true
    }
}

#[derive(Clone)]
struct TaskStatusIndicator {
    indicator: Vec<bool>,
    n_of_true: usize,
}

impl TaskStatusIndicator {
    fn set_false(&mut self, idx: usize) {
        self.indicator[idx] = false;
        self.n_of_true -= 1;
    }

    fn set_true(&mut self, idx: usize) {
        self.indicator[idx] = true;
        self.n_of_true += 1;
    }
}

#[cfg(test)]
mod test {
    use crate::state::stage_manager::StageManager;
    use ballista_core::error::Result;
    use ballista_core::serde::protobuf::{
        task_status, CompletedTask, FailedTask, PartitionId, RunningTask, TaskStatus,
    };

    #[tokio::test]
    async fn test_task_status_state_machine_failed() -> Result<()> {
        let stage_manager = StageManager::new();

        let num_partitions = 3;
        let job_id = "job";
        let stage_id = 1u32;

        stage_manager.add_running_stage(job_id, stage_id, num_partitions);

        let task_id = PartitionId {
            job_id: job_id.to_owned(),
            stage_id,
            partition_id: 2,
        };

        {
            // Invalid transformation from Pending to Failed
            stage_manager.update_tasks_status(vec![TaskStatus {
                status: Some(task_status::Status::Failed(FailedTask {
                    error: "error".to_owned(),
                })),
                task_id: Some(task_id.clone()),
            }]);
            let ret = stage_manager.get_stage_tasks(job_id, stage_id);
            assert!(ret.is_some());
            assert!(ret
                .unwrap()
                .get(task_id.partition_id as usize)
                .unwrap()
                .status
                .is_none());
        }

        {
            // Valid transformation from Pending to Running to Failed
            stage_manager.update_tasks_status(vec![TaskStatus {
                status: Some(task_status::Status::Running(RunningTask {
                    executor_id: "localhost".to_owned(),
                })),
                task_id: Some(task_id.clone()),
            }]);
            stage_manager.update_tasks_status(vec![TaskStatus {
                status: Some(task_status::Status::Failed(FailedTask {
                    error: "error".to_owned(),
                })),
                task_id: Some(task_id.clone()),
            }]);
            let ret = stage_manager.get_stage_tasks(job_id, stage_id);
            assert!(ret.is_some());
            match ret
                .unwrap()
                .get(task_id.partition_id as usize)
                .unwrap()
                .status
                .as_ref()
                .unwrap()
            {
                task_status::Status::Failed(_) => (),
                _ => panic!("Unexpected status"),
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_task_status_state_machine_completed() -> Result<()> {
        let stage_manager = StageManager::new();

        let num_partitions = 3;
        let job_id = "job";
        let stage_id = 1u32;

        stage_manager.add_running_stage(job_id, stage_id, num_partitions);

        let task_id = PartitionId {
            job_id: job_id.to_owned(),
            stage_id,
            partition_id: 2,
        };

        // Valid transformation from Pending to Running to Completed to Pending
        task_from_pending_to_completed(&stage_manager, &task_id);
        let ret = stage_manager.get_stage_tasks(job_id, stage_id);
        assert!(ret.is_some());
        match ret
            .unwrap()
            .get(task_id.partition_id as usize)
            .unwrap()
            .status
            .as_ref()
            .unwrap()
        {
            task_status::Status::Completed(_) => (),
            _ => panic!("Unexpected status"),
        }
        stage_manager.update_tasks_status(vec![TaskStatus {
            status: None,
            task_id: Some(task_id.clone()),
        }]);
        let ret = stage_manager.get_stage_tasks(job_id, stage_id);
        assert!(ret.is_some());
        assert!(ret
            .unwrap()
            .get(task_id.partition_id as usize)
            .unwrap()
            .status
            .is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_stage_state_machine_completed() -> Result<()> {
        let stage_manager = StageManager::new();

        let num_partitions = 3;
        let job_id = "job";
        let stage_id = 1u32;

        // Valid transformation from Running to Completed
        stage_manager.add_running_stage(job_id, stage_id, num_partitions);
        assert!(stage_manager.is_running_stage(job_id, stage_id));
        for partition_id in 0..num_partitions {
            task_from_pending_to_completed(
                &stage_manager,
                &PartitionId {
                    job_id: job_id.to_owned(),
                    stage_id,
                    partition_id,
                },
            );
        }
        assert!(stage_manager.is_completed_stage(job_id, stage_id));

        // Valid transformation from Completed to Running
        stage_manager.update_tasks_status(vec![TaskStatus {
            status: None,
            task_id: Some(PartitionId {
                job_id: job_id.to_owned(),
                stage_id,
                partition_id: 0,
            }),
        }]);
        assert!(!stage_manager.is_running_stage(job_id, stage_id));

        Ok(())
    }

    fn task_from_pending_to_completed(
        stage_manager: &StageManager,
        task_id: &PartitionId,
    ) {
        stage_manager.update_tasks_status(vec![TaskStatus {
            status: Some(task_status::Status::Running(RunningTask {
                executor_id: "localhost".to_owned(),
            })),
            task_id: Some(task_id.clone()),
        }]);
        stage_manager.update_tasks_status(vec![TaskStatus {
            status: Some(task_status::Status::Completed(CompletedTask {
                executor_id: "localhost".to_owned(),
                partitions: Vec::new(),
            })),
            task_id: Some(task_id.clone()),
        }]);
    }
}
