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

//! Ballista executor logic

use std::sync::Arc;

use ballista_core::error::BallistaError;
use ballista_core::execution_plans::ShuffleWriterExec;
use ballista_core::serde::protobuf;
use ballista_core::serde::scheduler::ExecutorSpecification;
use datafusion::error::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion::prelude::ExecutionConfig;

/// Ballista executor
pub struct Executor {
    /// Directory for storing partial results
    work_dir: String,

    /// Specification like total task slots
    pub specification: ExecutorSpecification,
}

impl Executor {
    /// Create a new executor instance
    pub fn new(work_dir: &str) -> Self {
        Executor::new_with_specification(
            work_dir,
            ExecutorSpecification { task_slots: 4 },
        )
    }

    pub fn new_with_specification(
        work_dir: &str,
        specification: ExecutorSpecification,
    ) -> Self {
        Self {
            work_dir: work_dir.to_owned(),
            specification,
        }
    }
}

impl Executor {
    /// Execute one partition of a query stage and persist the result to disk in IPC format. On
    /// success, return a RecordBatch containing metadata about the results, including path
    /// and statistics.
    pub async fn execute_shuffle_write(
        &self,
        job_id: String,
        stage_id: usize,
        part: usize,
        plan: Arc<dyn ExecutionPlan>,
        _shuffle_output_partitioning: Option<Partitioning>,
    ) -> Result<Vec<protobuf::ShuffleWritePartition>, BallistaError> {
        let exec = if let Some(shuffle_writer) =
            plan.as_any().downcast_ref::<ShuffleWriterExec>()
        {
            // recreate the shuffle writer with the correct working directory
            ShuffleWriterExec::try_new(
                job_id.clone(),
                stage_id,
                plan.children()[0].clone(),
                self.work_dir.clone(),
                shuffle_writer.shuffle_output_partitioning().cloned(),
            )
        } else {
            Err(DataFusionError::Internal(
                "Plan passed to execute_shuffle_write is not a ShuffleWriterExec"
                    .to_string(),
            ))
        }?;

        let config = ExecutionConfig::new().with_temp_file_path(self.work_dir.clone());
        let runtime = Arc::new(RuntimeEnv::new(config.runtime)?);

        let partitions = exec.execute_shuffle_write(part, runtime).await?;

        println!(
            "=== [{}/{}/{}] Physical plan with metrics ===\n{}\n",
            job_id,
            stage_id,
            part,
            DisplayableExecutionPlan::with_metrics(&exec).indent()
        );

        Ok(partitions)
    }

    pub fn work_dir(&self) -> &str {
        &self.work_dir
    }
}
