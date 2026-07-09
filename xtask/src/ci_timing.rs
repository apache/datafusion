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

use std::cmp::Reverse;
use std::time::Duration;

use crate::ci_jobs::JobInfo;
use crate::ci_workflows::WorkflowInfo;
use crate::steps::StepInfo;

pub(crate) struct JobTiming {
    job_info: &'static JobInfo,
    /// step_name, execution time
    per_step_timing: Vec<(&'static StepInfo, Duration)>,
}

impl JobTiming {
    pub(crate) fn new(job: &'static JobInfo) -> Self {
        Self {
            job_info: job,
            per_step_timing: Vec::new(),
        }
    }

    pub(crate) fn add_step_timing(&mut self, step: &'static StepInfo, timing: Duration) {
        self.per_step_timing.push((step, timing));
    }

    fn total_time(&self) -> Duration {
        self.per_step_timing.iter().map(|pair| pair.1).sum()
    }

    /// Sort each step's execution time, and print like
    ///
    /// Job finished in 47s:
    ///     step b: 30s
    ///     step a: 20s
    ///     step f: 1s
    ///     ...
    pub(crate) fn display(&self) {
        let mut sorted_by_duration = self.per_step_timing.clone();
        sorted_by_duration.sort_by_key(|step| Reverse(step.1));

        println!(
            "\nci job `{}` total execution time: {:?}",
            self.job_info.name,
            self.total_time()
        );

        for (step, timing) in sorted_by_duration {
            println!("    ci step `{}`: {timing:?}", step.command);
        }
    }
}

pub(crate) struct WorkflowTiming {
    workflow_info: &'static WorkflowInfo,
    /// job, execution time
    per_job_timing: Vec<JobTiming>,
}

impl WorkflowTiming {
    pub(crate) fn new(workflow: &'static WorkflowInfo) -> Self {
        Self {
            workflow_info: workflow,
            per_job_timing: Vec::new(),
        }
    }

    pub(crate) fn add_job_timing(&mut self, job_timing: JobTiming) {
        self.per_job_timing.push(job_timing);
    }

    fn total_time(&self) -> Duration {
        self.per_job_timing.iter().map(JobTiming::total_time).sum()
    }

    pub(crate) fn display(&self) {
        let mut sorted_by_duration = self.per_job_timing.iter().collect::<Vec<_>>();
        sorted_by_duration.sort_by_key(|job_timing| Reverse(job_timing.total_time()));

        println!(
            "\nci workflow `{}` total execution time: {:?}",
            self.workflow_info.name,
            self.total_time()
        );

        for job_timing in sorted_by_duration {
            println!(
                "    ci job `{}`: {:?}",
                job_timing.job_info.name,
                job_timing.total_time()
            );
        }
    }
}

pub(crate) struct WorkflowsTiming {
    per_workflow_timing: Vec<WorkflowTiming>,
}

impl WorkflowsTiming {
    pub(crate) fn new() -> Self {
        Self {
            per_workflow_timing: Vec::new(),
        }
    }

    pub(crate) fn add_workflow_timing(&mut self, workflow_timing: WorkflowTiming) {
        self.per_workflow_timing.push(workflow_timing);
    }

    fn total_time(&self) -> Duration {
        self.per_workflow_timing
            .iter()
            .map(WorkflowTiming::total_time)
            .sum()
    }

    pub(crate) fn display(&self) {
        let mut sorted_by_duration = self.per_workflow_timing.iter().collect::<Vec<_>>();
        sorted_by_duration
            .sort_by_key(|workflow_timing| Reverse(workflow_timing.total_time()));

        println!(
            "\nci workflows total execution time: {:?}",
            self.total_time()
        );

        for workflow_timing in sorted_by_duration {
            println!(
                "    ci workflow `{}`: {:?}",
                workflow_timing.workflow_info.name,
                workflow_timing.total_time()
            );
        }
    }
}
