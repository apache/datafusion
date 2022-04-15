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

use std::collections::HashMap;
use std::sync::Arc;

use crate::metrics::ExecutorMetricsCollector;
use ballista_core::error::BallistaError;
use ballista_core::execution_plans::ShuffleWriterExec;
use ballista_core::serde::protobuf;
use ballista_core::serde::protobuf::ExecutorRegistration;
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::execution::runtime_env::RuntimeEnv;

use datafusion::physical_plan::udaf::AggregateUDF;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};

/// Ballista executor
pub struct Executor {
    /// Metadata
    pub metadata: ExecutorRegistration,

    /// Directory for storing partial results
    pub work_dir: String,

    /// Scalar functions that are registered in the Executor
    pub scalar_functions: HashMap<String, Arc<ScalarUDF>>,

    /// Aggregate functions registered in the Executor
    pub aggregate_functions: HashMap<String, Arc<AggregateUDF>>,

    /// Runtime environment for Executor
    pub runtime: Arc<RuntimeEnv>,

    /// Collector for runtime execution metrics
    pub metrics_collector: Arc<dyn ExecutorMetricsCollector>,
}

impl Executor {
    /// Create a new executor instance
    pub fn new(
        metadata: ExecutorRegistration,
        work_dir: &str,
        runtime: Arc<RuntimeEnv>,
        metrics_collector: Arc<dyn ExecutorMetricsCollector>,
    ) -> Self {
        Self {
            metadata,
            work_dir: work_dir.to_owned(),
            // TODO add logic to dynamically load UDF/UDAFs libs from files
            scalar_functions: HashMap::new(),
            aggregate_functions: HashMap::new(),
            runtime,
            metrics_collector,
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
        task_ctx: Arc<TaskContext>,
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

        let partitions = exec.execute_shuffle_write(part, task_ctx).await?;

        self.metrics_collector
            .record_stage(&job_id, stage_id, part, exec);

        Ok(partitions)
    }

    pub fn work_dir(&self) -> &str {
        &self.work_dir
    }
}
