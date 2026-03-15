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

use std::io::Write;
use std::{fmt::Debug, fs};

use arrow::{array::RecordBatch, util::pretty::pretty_format_batches};
use dashmap::DashMap;
use datafusion_common::error::Result;

/// Used to implement the auto_explain feature.
pub trait PlanObserver: Send + Sync + 'static + Debug {
    /// Called after the physical plan has been created but before it has been executed.
    /// Receives an identifier and a SQL representation of the query.
    /// The unparsing of some logical operators might not be implemented yet, hence why `sql` is an
    /// `Option`.
    fn plan_created(&self, id: String, sql: Option<String>) -> Result<()>;

    /// Called after the physical plan has been executed.
    /// Receives the identifier, the EXPLAIN ANALYZE output, and the duration.
    fn plan_executed(
        &self,
        id: String,
        explain_result: RecordBatch,
        duration_nanos: u128,
    ) -> Result<()>;
}

#[derive(Debug)]
pub struct DefaultPlanObserver {
    output: String,
    min_duration_ms: usize,
    queries: DashMap<String, String>,
}

impl DefaultPlanObserver {
    /// Creates a new `DefaultPlanObserver`.
    /// * `output`: where to store the output of `auto_explain`, if enabled.
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
            queries: DashMap::new(),
        }
    }
}

impl Default for DefaultPlanObserver {
    fn default() -> Self {
        Self {
            output: "log::info".to_owned(),
            min_duration_ms: 0,
            queries: DashMap::new(),
        }
    }
}

impl PlanObserver for DefaultPlanObserver {
    fn plan_created(&self, id: String, sql: Option<String>) -> Result<()> {
        if let Some(sql) = sql {
            self.queries.insert(id, sql);
        }
        Ok(())
    }

    fn plan_executed(
        &self,
        id: String,
        explain_result: RecordBatch,
        duration_nanos: u128,
    ) -> Result<()> {
        let sql = if let Some((_, sql)) = self.queries.remove(&id) {
            sql
        } else {
            "-".to_string()
        };

        let duration_ms = (duration_nanos as f64) / 1e6;
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
