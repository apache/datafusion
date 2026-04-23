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
use std::sync::{Arc, Mutex};
use std::{path::PathBuf, time::Duration};

use super::{DFSqlLogicTestError, error::Result, normalize};
use crate::engines::currently_executed_sql::CurrentlyExecutingSqlTracker;
use crate::engines::output::{DFColumnType, DFOutput};
use crate::is_spark_path;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::SessionContext;
use indicatif::ProgressBar;
use log::Level::{Debug, Info};
use log::{debug, log_enabled, warn};
use sqllogictest::DBOutput;
use tokio::time::Instant;

pub struct DataFusion {
    ctx: SessionContext,
    relative_path: PathBuf,
    pb: ProgressBar,
    currently_executing_sql_tracker: CurrentlyExecutingSqlTracker,
    default_config: HashMap<String, Option<String>>,
    config_change_errors: Option<Arc<Mutex<Vec<String>>>>,
}

impl DataFusion {
    pub fn new(ctx: SessionContext, relative_path: PathBuf, pb: ProgressBar) -> Self {
        let default_config = ctx
            .state()
            .config()
            .options()
            .entries()
            .iter()
            .map(|e| (e.key.clone(), e.value.clone()))
            .collect();

        Self {
            ctx,
            relative_path,
            pb,
            currently_executing_sql_tracker: CurrentlyExecutingSqlTracker::default(),
            default_config,
            config_change_errors: None,
        }
    }

    /// Add a tracker that will track the currently executed SQL statement.
    ///
    /// This is useful for logging and debugging purposes.
    pub fn with_currently_executing_sql_tracker(
        mut self,
        currently_executing_sql_tracker: CurrentlyExecutingSqlTracker,
    ) -> Self {
        self.currently_executing_sql_tracker = currently_executing_sql_tracker;
        self
    }

    pub fn with_config_change_errors(
        mut self,
        config_change_errors: Arc<Mutex<Vec<String>>>,
    ) -> Self {
        self.config_change_errors = Some(config_change_errors);
        self
    }

    fn update_slow_count(&self) {
        let msg = self.pb.message();
        let split: Vec<&str> = msg.split(" ").collect();
        let mut current_count = 0;

        if split.len() > 2 {
            // third match will be current slow count
            current_count = split[2].parse::<i32>().unwrap();
        }

        current_count += 1;

        self.pb
            .set_message(format!("{} - {} took > 500 ms", split[0], current_count));
    }

    pub fn validate_config_unchanged(&mut self) -> Result<()> {
        let mut changed = false;
        let mut message = format!(
            "SLT file {} left modified configuration",
            self.relative_path.display()
        );

        for entry in self.ctx.state().config().options().entries() {
            let default_entry = self.default_config.remove(&entry.key);

            if let Some(default_entry) = default_entry
                && default_entry.as_ref() != entry.value.as_ref()
            {
                changed = true;

                let default = default_entry.as_deref().unwrap_or("NULL");
                let current = entry.value.as_deref().unwrap_or("NULL");

                message
                    .push_str(&format!("\n  {}: {} -> {}", entry.key, default, current));
            }
        }

        for (key, value) in &self.default_config {
            changed = true;

            let default = value.as_deref().unwrap_or("NULL");
            message.push_str(&format!("\n  {key}: {default} -> NULL"));
        }

        if changed {
            Err(DFSqlLogicTestError::Other(message))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl sqllogictest::AsyncDB for DataFusion {
    type Error = DFSqlLogicTestError;
    type ColumnType = DFColumnType;

    async fn run(&mut self, sql: &str) -> Result<DFOutput> {
        if log_enabled!(Debug) {
            debug!(
                "[{}] Running query: \"{}\"",
                self.relative_path.display(),
                sql
            );
        }

        let tracked_sql = self.currently_executing_sql_tracker.set_sql(sql);

        let start = Instant::now();
        let result = run_query(&self.ctx, is_spark_path(&self.relative_path), sql).await;
        let duration = start.elapsed();

        self.currently_executing_sql_tracker.remove_sql(tracked_sql);

        if duration.gt(&Duration::from_millis(500)) {
            self.update_slow_count();
        }

        self.pb.inc(1);

        if log_enabled!(Info) && duration.gt(&Duration::from_secs(2)) {
            warn!(
                "[{}] Running query took more than 2 sec ({duration:?}): \"{sql}\"",
                self.relative_path.display()
            );
        }

        result
    }

    /// Engine name of current database.
    fn engine_name(&self) -> &str {
        "DataFusion"
    }

    /// [`DataFusion`] calls this function to perform sleep.
    ///
    /// The default implementation is `std::thread::sleep`, which is universal to any async runtime
    /// but would block the current thread. If you are running in tokio runtime, you should override
    /// this by `tokio::time::sleep`.
    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }

    /// Shutdown and check no DataFusion configuration has changed during test
    async fn shutdown(&mut self) {
        if let Some(config_change_errors) = self.config_change_errors.clone()
            && let Err(error) = self.validate_config_unchanged()
        {
            config_change_errors.lock().unwrap().push(error.to_string());
        }
    }
}

async fn run_query(
    ctx: &SessionContext,
    is_spark_path: bool,
    sql: impl Into<String>,
) -> Result<DFOutput> {
    let df = ctx.sql(sql.into().as_str()).await?;
    let task_ctx = Arc::new(df.task_ctx());
    let plan = df.create_physical_plan().await?;
    let schema = plan.schema();

    let stream = execute_stream(plan, task_ctx)?;
    let types = normalize::convert_schema_to_types(stream.schema().fields());
    let results: Vec<RecordBatch> = collect(stream).await?;
    let rows = normalize::convert_batches(&schema, results, is_spark_path)?;

    if rows.is_empty() && types.is_empty() {
        Ok(DBOutput::StatementComplete(0))
    } else {
        Ok(DBOutput::Rows { types, rows })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqllogictest::AsyncDB;

    #[tokio::test]
    async fn validate_config_unchanged_detects_modified_config() {
        let ctx = SessionContext::new();
        let default_batch_size = ctx.state().config().options().execution.batch_size;
        let mut runner =
            DataFusion::new(ctx, PathBuf::from("test.slt"), ProgressBar::hidden());

        <DataFusion as AsyncDB>::run(
            &mut runner,
            "SET datafusion.execution.batch_size = 2048",
        )
        .await
        .unwrap();

        let error = runner.validate_config_unchanged().unwrap_err();
        let message = error.to_string();

        assert!(message.contains("test.slt left modified configuration"));
        assert!(message.contains("datafusion.execution.batch_size"));
        assert!(message.contains(&format!("{default_batch_size} -> 2048")));
    }
}
