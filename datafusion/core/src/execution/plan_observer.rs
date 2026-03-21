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

//! Provides callbacks to keep track of planned and executed queries.

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use std::{fmt::Debug, fs};

use arrow::{array::RecordBatch, util::pretty::pretty_format_batches};
use datafusion_common::error::Result;
use datafusion_expr::LogicalPlan;
use datafusion_physical_plan::ExecutionPlan;
use parking_lot::Mutex;

#[cfg(feature = "sql")]
use datafusion_sql::unparser::Unparser;

/// Provides callbacks to keep track of planned and executed queries.
pub trait PlanObserver: Send + Sync + 'static + Debug {
    /// Called after the physical plan has been created but before it has been executed.
    /// Receives the logical and physical plans, as well as a unique identifier.
    fn plan_created(
        &self,
        id: &str,
        logical_plan: &LogicalPlan,
        physical_plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<()>;

    /// Called after the physical plan has been executed.
    /// Receives the identifier, the EXPLAIN ANALYZE output, and the duration.
    fn plan_executed(
        &self,
        id: &str,
        explain_result: RecordBatch,
        duration: Duration,
    ) -> Result<()>;
}

/// Default implementation of [`PlanObserver`].
/// Outputs the result in the following format:
/// ```txt
/// QUERY: <sql query> (requires the `sql` feature; outputs `(unavailable)` otherwise)
/// DURATION: <query duration in milliseconds>
/// EXPLAIN: <explain analyze output>
/// ```
#[derive(Debug)]
pub struct DefaultPlanObserver {
    output: String,
    min_duration_ms: usize,
    /// stores a SQL representation of the logical plan, if the `sql` feature is enabled.
    queries: Arc<Mutex<HashMap<String, String>>>,
}

impl DefaultPlanObserver {
    /// Creates a new `DefaultPlanObserver`.
    /// * `output`: where to write the output.
    ///   Possible values:
    ///   - `log::error`
    ///   - `log::warn`
    ///   - `log::info`
    ///   - `log::debug`
    ///   - `log::trace`
    ///   - a file path: creates the file if it does not exist, or appends to it if it does.
    /// * `min_duration_ms`: only outputs the result if the execution duration is greater than or
    ///   equal to this value.
    pub fn new(output: String, min_duration_ms: usize) -> Self {
        Self {
            output,
            min_duration_ms,
            queries: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for DefaultPlanObserver {
    fn default() -> Self {
        Self {
            output: "log::info".to_owned(),
            min_duration_ms: 0,
            queries: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl PlanObserver for DefaultPlanObserver {
    fn plan_created(
        &self,
        id: &str,
        logical_plan: &LogicalPlan,
        _physical_plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<()> {
        #[cfg(feature = "sql")]
        {
            if let Ok(sql) = Unparser::default().plan_to_sql(logical_plan) {
                let mut queries = self.queries.lock();
                queries.insert(id.to_owned(), sql.to_string());
            }
        }
        Ok(())
    }

    fn plan_executed(
        &self,
        id: &str,
        explain_result: RecordBatch,
        duration: Duration,
    ) -> Result<()> {
        let mut queries = self.queries.lock();
        let sql = if let Some(sql) = queries.remove(id) {
            sql
        } else {
            "(unavailable)".to_string()
        };

        let duration_ms = duration.as_secs_f64() * 1000.0;
        if duration_ms < self.min_duration_ms as f64 {
            return Ok(());
        }

        let analyze = pretty_format_batches(&[explain_result])?;
        let message =
            format!("QUERY: {sql}\nDURATION: {duration_ms:.3}ms\nEXPLAIN:\n{analyze}");

        match self.output.as_str() {
            "log::error" => log::error!("{message}"),
            "log::warn" => log::warn!("{message}"),
            "log::info" => log::info!("{message}"),
            "log::debug" => log::debug!("{message}"),
            "log::trace" => log::trace!("{message}"),
            _ => {
                let fd = &mut fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.output)?;
                writeln!(fd, "{message}")?;
            }
        }

        Ok(())
    }
}
