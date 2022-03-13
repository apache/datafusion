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

use ballista_core::serde::protobuf::{ExecutorHeartbeat, TaskStatus};
use ballista_core::serde::scheduler::ExecutorData;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

type JobTasks = HashMap<u32, HashMap<u32, TaskStatus>>;

#[derive(Clone)]
pub(crate) struct InMemorySchedulerState {
    executors_heartbeat: Arc<RwLock<HashMap<String, ExecutorHeartbeat>>>,
    executors_data: Arc<RwLock<HashMap<String, ExecutorData>>>,

    // job -> stage -> partition
    tasks: Arc<RwLock<HashMap<String, JobTasks>>>,
}

/// For in-memory state, we don't use async to provide related services
impl InMemorySchedulerState {
    pub(crate) fn new() -> Self {
        Self {
            executors_heartbeat: Arc::new(RwLock::new(HashMap::new())),
            executors_data: Arc::new(RwLock::new(HashMap::new())),
            tasks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub(crate) fn save_executor_heartbeat(&self, heartbeat: ExecutorHeartbeat) {
        let mut executors_heartbeat = self.executors_heartbeat.write();
        executors_heartbeat.insert(heartbeat.executor_id.clone(), heartbeat);
    }

    pub(crate) fn get_executors_heartbeat(&self) -> Vec<ExecutorHeartbeat> {
        let executors_heartbeat = self.executors_heartbeat.read();
        executors_heartbeat
            .iter()
            .map(|(_exec, heartbeat)| heartbeat.clone())
            .collect()
    }

    /// last_seen_ts_threshold is in seconds
    pub(crate) fn get_alive_executors(
        &self,
        last_seen_ts_threshold: u64,
    ) -> HashSet<String> {
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

    pub(crate) fn save_executor_data(&self, executor_data: ExecutorData) {
        let mut executors_data = self.executors_data.write();
        executors_data.insert(executor_data.executor_id.clone(), executor_data);
    }

    pub(crate) fn get_executor_data(&self, executor_id: &str) -> Option<ExecutorData> {
        let executors_data = self.executors_data.read();
        executors_data.get(executor_id).cloned()
    }

    /// There are two checks:
    /// 1. firstly alive
    /// 2. secondly available task slots > 0
    pub(crate) fn get_available_executors_data(&self) -> Vec<ExecutorData> {
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

    pub(crate) fn save_task_status(&self, status: &TaskStatus) {
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

    pub(crate) fn _get_task(
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

    pub(crate) fn get_job_tasks(&self, job_id: &str) -> Option<Vec<TaskStatus>> {
        let tasks = self.tasks.read();
        let job_tasks = tasks.get(job_id);

        if let Some(job_tasks) = job_tasks {
            let mut res = vec![];
            fill_job_tasks(&mut res, job_tasks);
            Some(res)
        } else {
            None
        }
    }

    pub(crate) fn get_tasks(&self) -> Vec<TaskStatus> {
        let mut res = vec![];

        let tasks = self.tasks.read();
        for (_job_id, job_tasks) in tasks.iter() {
            fill_job_tasks(&mut res, job_tasks);
        }

        res
    }
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
