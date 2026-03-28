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
use datafusion_physical_plan::analyze::AnalyzeObserver;
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
    /// Receives the identifier, the EXPLAIN ANALYZE output (annotated plan), and the duration.
    fn plan_executed(
        &self,
        id: &str,
        annotated_plan: RecordBatch,
        duration: Duration,
    ) -> Result<()>;
}

/// States where the plans will be outputted.
/// `LogError`, `LogWarn`, `LogInfo`, `LogDebug`, and `LogTrace` use the respective macros from the
/// [`log`] crate. `LogToFile(file)` writes the plans to the provided file.
#[derive(Debug)]
pub enum PlanOutput {
    /// Writes to log::error!
    LogError,
    /// Writes to log::warn!
    LogWarn,
    /// Writes to log::info!
    LogInfo,
    /// Writes to log::debug!
    LogDebug,
    /// Writes to log::trace!
    LogTrace,
    /// Writes to the provided file path
    LogToFile(String),
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
    output: PlanOutput,
    min_duration_ms: usize,
    /// stores a SQL representation of the logical plan, if the `sql` feature is enabled.
    queries: Arc<Mutex<HashMap<String, String>>>,
}

impl DefaultPlanObserver {
    /// Creates a new `DefaultPlanObserver`.
    /// * `output`: where to write the output. If `PlanOutput::LogToFile` is provided and the file
    ///   does not exist, it will be created (otherwise it appends to it).
    /// * `min_duration_ms`: only outputs the result if the execution duration is greater than or
    ///   equal to this value.
    pub fn new(output: PlanOutput, min_duration_ms: usize) -> Self {
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
            output: PlanOutput::LogInfo,
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
        annotated_plan: RecordBatch,
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

        let analyze = pretty_format_batches(&[annotated_plan])?;
        let message =
            format!("QUERY: {sql}\nDURATION: {duration_ms:.3}ms\nEXPLAIN:\n{analyze}");

        match self.output {
            PlanOutput::LogError => log::error!("{message}"),
            PlanOutput::LogWarn => log::warn!("{message}"),
            PlanOutput::LogInfo => log::info!("{message}"),
            PlanOutput::LogDebug => log::debug!("{message}"),
            PlanOutput::LogTrace => log::trace!("{message}"),
            PlanOutput::LogToFile(ref file) => {
                let fd = &mut fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(file)?;
                writeln!(fd, "{message}")?;
            }
        }

        Ok(())
    }
}

/// Implementation that bridges [`PlanObserver`] with the [`AnalyzeObserver`].
#[derive(Debug)]
pub struct AnalyzeObserverImpl {
    plan_observer: Arc<dyn PlanObserver>,
    id: String,
}

impl AnalyzeObserverImpl {
    /// Creates a new `AnalyzeObserverImpl`, receiving the `plan_observer` and an identifier for the
    /// query being executed.
    pub fn new(plan_observer: Arc<dyn PlanObserver>, id: String) -> Self {
        Self { plan_observer, id }
    }
}

impl AnalyzeObserver for AnalyzeObserverImpl {
    fn analyze_result_callback(
        &self,
        result: RecordBatch,
        duration: Duration,
    ) -> Result<()> {
        self.plan_observer.plan_executed(&self.id, result, duration)
    }
}
