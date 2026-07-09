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

use crate::ci_jobs::{JOBS, JobInfo};
use crate::ci_workflows::WorkflowInfo;

const VERIFY_CLEAN_JOB_NAME: &str = "verify-clean";
const SKIPPED_JOBS: &[&str] = &[
    // TODO: Enable these after local setup is fixed.
    "linux-wasm-pack",
    "verify-benchmark-results",
    "sqllogictest-postgres",
];

pub(crate) const WORKFLOW_RUST: WorkflowInfo = WorkflowInfo {
    name: "rust",
    help_usage: "ci workflow rust",
    help_description: "Run Rust CI jobs locally",
    jobs,
};

fn jobs() -> Vec<&'static JobInfo> {
    let verify_clean = JOBS
        .iter()
        .find(|job| job.name == VERIFY_CLEAN_JOB_NAME)
        .unwrap_or_else(|| panic!("missing `{VERIFY_CLEAN_JOB_NAME}` job"));
    let mut jobs = vec![verify_clean];
    jobs.extend(JOBS.iter().filter(|job| !SKIPPED_JOBS.contains(&job.name)));
    jobs
}
