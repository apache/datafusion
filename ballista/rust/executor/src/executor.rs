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

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use ballista_core::error::BallistaError;
use ballista_core::utils;
use datafusion::arrow::array::{ArrayRef, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::ExecutionPlan;
use log::info;

/// Ballista executor
pub struct Executor {
    /// Directory for storing partial results
    work_dir: String,
}

impl Executor {
    /// Create a new executor instance
    pub fn new(work_dir: &str) -> Self {
        Self {
            work_dir: work_dir.to_owned(),
        }
    }
}

impl Executor {
    /// Execute one partition of a query stage and persist the result to disk in IPC format. On
    /// success, return a RecordBatch containing metadata about the results, including path
    /// and statistics.
    pub async fn execute_partition(
        &self,
        job_id: String,
        stage_id: usize,
        part: usize,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<RecordBatch, BallistaError> {
        let mut path = PathBuf::from(&self.work_dir);
        path.push(&job_id);
        path.push(&format!("{}", stage_id));
        path.push(&format!("{}", part));
        std::fs::create_dir_all(&path)?;

        path.push("data.arrow");
        let path = path.to_str().unwrap();
        info!("Writing results to {}", path);

        let now = Instant::now();

        // execute the query partition
        let mut stream = plan.execute(part).await?;

        // stream results to disk
        let stats = utils::write_stream_to_disk(&mut stream, &path).await?;

        info!(
            "Executed partition {} in {} seconds. Statistics: {:?}",
            part,
            now.elapsed().as_secs(),
            stats
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("path", DataType::Utf8, false),
            stats.arrow_struct_repr(),
        ]));

        // build result set with summary of the partition execution status
        let mut c0 = StringBuilder::new(1);
        c0.append_value(&path).unwrap();
        let path: ArrayRef = Arc::new(c0.finish());

        let stats: ArrayRef = stats.to_arrow_arrayref()?;
        RecordBatch::try_new(schema, vec![path, stats]).map_err(BallistaError::ArrowError)
    }

    pub fn work_dir(&self) -> &str {
        &self.work_dir
    }
}
