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

use std::sync::Arc;
use std::{path::PathBuf, time::Duration};

use crate::engines::currently_executed_sql::CurrentlyExecutingSqlTracker;
use crate::engines::datafusion_engine::Result;
use crate::engines::output::{DFColumnType, DFOutput};
use crate::{convert_batches, convert_schema_to_types, DFSqlLogicTestError};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::SessionContext;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use indicatif::ProgressBar;
use log::Level::{Debug, Info};
use log::{debug, log_enabled, warn};
use sqllogictest::DBOutput;
use tokio::time::Instant;

pub struct DataFusionSubstraitRoundTrip {
    ctx: SessionContext,
    relative_path: PathBuf,
    pb: ProgressBar,
    currently_executing_sql_tracker: CurrentlyExecutingSqlTracker,
}

impl DataFusionSubstraitRoundTrip {
    pub fn new(ctx: SessionContext, relative_path: PathBuf, pb: ProgressBar) -> Self {
        Self {
            ctx,
            relative_path,
            pb,
            currently_executing_sql_tracker: CurrentlyExecutingSqlTracker::default(),
        }
    }

    /// Add a tracker that will track the currently executed SQL statement.
    ///
    /// This is useful for logging and debugging purposes.
    pub fn with_currently_executing_sql_tracker(
        self,
        currently_executing_sql_tracker: CurrentlyExecutingSqlTracker,
    ) -> Self {
        Self {
            currently_executing_sql_tracker,
            ..self
        }
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
}

#[async_trait]
impl sqllogictest::AsyncDB for DataFusionSubstraitRoundTrip {
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
        let result = run_query_substrait_round_trip(&self.ctx, sql).await;
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
        "DataFusionSubstraitRoundTrip"
    }

    /// `DataFusion` calls this function to perform sleep.
    ///
    /// The default implementation is `std::thread::sleep`, which is universal to any async runtime
    /// but would block the current thread. If you are running in tokio runtime, you should override
    /// this by `tokio::time::sleep`.
    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }

    async fn shutdown(&mut self) {}
}

async fn run_query_substrait_round_trip(
    ctx: &SessionContext,
    sql: impl Into<String>,
) -> Result<DFOutput> {
    let df = ctx.sql(sql.into().as_str()).await?;
    let task_ctx = Arc::new(df.task_ctx());

    let state = ctx.state();
    let round_tripped_plan = match df.logical_plan() {
        // Substrait does not handle these plans
        LogicalPlan::Ddl(_)
        | LogicalPlan::Explain(_)
        | LogicalPlan::Dml(_)
        | LogicalPlan::Copy(_)
        | LogicalPlan::DescribeTable(_)
        | LogicalPlan::Statement(_) => df.logical_plan().clone(),
        // For any other plan, convert to Substrait
        logical_plan => {
            let plan = to_substrait_plan(logical_plan, &state)?;
            from_substrait_plan(&state, &plan).await?
        }
    };

    let physical_plan = state.create_physical_plan(&round_tripped_plan).await?;
    let schema = physical_plan.schema();
    let stream = execute_stream(physical_plan, task_ctx)?;
    let types = convert_schema_to_types(stream.schema().fields());
    let results: Vec<RecordBatch> = collect(stream).await?;
    let rows = convert_batches(&schema, results, false)?;

    if rows.is_empty() && types.is_empty() {
        Ok(DBOutput::StatementComplete(0))
    } else {
        Ok(DBOutput::Rows { types, rows })
    }
}
