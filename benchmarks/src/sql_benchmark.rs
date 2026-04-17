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

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::*;
use arrow::error::ArrowError;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::MemTable;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::{CsvReadOptions, DataFrame, SessionContext};
use datafusion_common::config::CsvOptions;
use datafusion_common::{DataFusionError, Result, exec_datafusion_err};
use futures::StreamExt;
use log::{debug, info, trace};
use regex::Regex;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read};
use std::path::PathBuf;
use std::sync::Arc;

/// A collection of benchmark configurations and state used by the DataFusion
/// sql test harness.  Each benchmark is defined by a file that can contain
/// directives such as `load`, `run`, `assert`, `result`, etc.  The
/// `SqlBenchmark` struct holds the parsed data from that file and
/// the impl provides methods to run, assert, persist, verify and cleanup benchmark
/// results.
#[derive(Debug, Clone)]
pub struct SqlBenchmark {
    /// Human‑readable name of the benchmark.
    name: String,
    /// Top‑level group name (derived from the file path or defined in a benchmark).
    group: String,
    /// Subgroup name, often a logical grouping.
    subgroup: String,
    /// Full path to the benchmark file.
    benchmark_path: PathBuf,
    /// Mapping of placeholder keys to concrete values (e.g. `"BENCHMARK_DIR"`).
    replacement_mapping: HashMap<String, String>,
    /// Expected string that must appear in the physical plan of the queries.
    expect: Vec<String>,
    /// All SQL queries grouped by directive (`load`, `run`, etc.).
    queries: HashMap<String, Vec<String>>,
    /// Queries whose results are persisted to disk for later comparison.
    result_queries: Vec<BenchmarkQuery>,
    /// Queries whose results are asserted against an expected table.
    assert_queries: Vec<BenchmarkQuery>,
    /// Flag indicating whether the benchmark has been fully loaded
    is_loaded: bool,
    /// Stores the last run results if needed so they can be compared or persisted.
    last_results: Option<Vec<RecordBatch>>,
    /// echo statements
    echo: Vec<String>,
}

impl SqlBenchmark {
    pub async fn new(
        ctx: &SessionContext,
        full_path: &str,
        benchmark_directory: &str,
    ) -> Self {
        let group_name = parse_group_from_path(full_path, benchmark_directory);
        let mut bm = Self {
            name: String::new(),
            group: group_name,
            subgroup: String::new(),
            benchmark_path: PathBuf::from(full_path),
            replacement_mapping: HashMap::new(),
            expect: Vec::new(),
            queries: HashMap::new(),
            result_queries: Vec::new(),
            assert_queries: Vec::new(),
            is_loaded: false,
            last_results: None,
            echo: vec![],
        };
        bm.replacement_mapping
            .insert("BENCHMARK_DIR".to_string(), benchmark_directory.to_string());

        let path = bm.benchmark_path.to_string_lossy().into_owned();
        bm.process_file(ctx, &path)
            .await
            .expect("processing failed");

        bm
    }

    /// Initializes the benchmark by executing `load` and `init` queries.
    ///
    /// Registers any required tables or sets up state in the provided
    /// `SessionContext` before running queries.  This method is idempotent:
    /// calling it multiple times on the same instance returns
    /// immediately after the first successful initialization.
    ///
    /// # Errors
    /// Returns an error if any `load` or `init` query fails, or if the
    /// benchmark file does not contain a `run` query.
    pub async fn initialize(&mut self, ctx: &SessionContext) -> Result<()> {
        if self.is_loaded {
            return Ok(());
        }

        let path = self.benchmark_path.to_string_lossy().into_owned();

        // validate there was a run query
        if !self.queries.contains_key("run") {
            return Err(exec_datafusion_err!(
                "Invalid benchmark file: no \"run\" query specified: {path}"
            ));
        }

        // display any echo's
        self.echo.iter().for_each(|txt| println!("{txt}"));

        let load_queries = self.queries.get("load");

        if let Some(queries) = load_queries {
            for query in queries {
                debug!("Executing load query {query}");
                ctx.sql(query).await?.collect().await?;
            }
        }

        let init_queries = self.queries.get("init");

        if let Some(queries) = init_queries {
            for query in queries {
                debug!("Executing init query {query}");
                ctx.sql(query).await?.collect().await?;
            }
        }

        self.is_loaded = true;

        Ok(())
    }

    /// Executes the `assert` queries and compares actual results against
    /// expected values.
    ///
    /// Each `assert` query must be followed by a result table (separated by
    /// `----`) in the benchmark file.  The assertion passes only if the
    /// returned record batches exactly match the expected rows.
    ///
    /// # Errors
    /// Returns an error if any `assert` query fails, or if the actual and
    /// expected results differ in row count or cell values.
    pub async fn assert(&mut self, ctx: &SessionContext) -> Result<()> {
        info!("Running assertions...");

        for assert_query in &self.assert_queries {
            let query = &assert_query.query;

            info!("Executing assert query ${query}");

            let result = ctx.sql(query).await?.collect().await?;
            let formatted_actual_results = format_record_batches(result)?;

            Self::compare_results(
                assert_query,
                &formatted_actual_results,
                &assert_query.expected_result,
            )?;
        }

        Ok(())
    }

    /// Executes the `run` queries, optionally saving results for later
    /// verification.
    ///
    /// When `save_results` is `true`, it collects `SELECT`/`WITH` query
    /// results and stores them in `last_results`.
    ///
    /// When `save_results` is `false`, it streams results and counts rows
    /// without buffering them.
    ///
    /// If an 'expect' string is defined this method also validates that
    /// the physical plan contains that string.
    ///
    /// # Errors
    /// Returns an error if a `run` query fails or if expected plan strings
    /// are not found.
    pub async fn run(&mut self, ctx: &SessionContext, save_results: bool) -> Result<()> {
        let run_queries = self
            .queries
            .get("run")
            .ok_or_else(|| exec_datafusion_err!("Run query should be loaded by now"))?;

        let mut result_count = 0;

        let result: Vec<RecordBatch> = {
            let mut local_result = vec![];

            for query in run_queries {
                match save_results {
                    true => {
                        debug!(
                            "Running query (saving results) {}-{}: {query}",
                            self.group, self.subgroup
                        );

                        let df = ctx.sql(query).await?;
                        if !self.expect.is_empty() {
                            let physical_plan = df.create_physical_plan().await?;
                            let plan_string = format!("{physical_plan:#?}");

                            for exp_str in self.expect.iter() {
                                if !plan_string.contains(exp_str) {
                                    return Err(exec_datafusion_err!(
                                        "The query physical plan does not contain the expected string '{exp_str}'.  Physical plan: {plan_string}"
                                    ));
                                }
                            }
                        }

                        let batches = df.collect().await;
                        let batches = match batches {
                            Ok(batches) => batches,
                            Err(e) => return Err(exec_datafusion_err!("{e:?}")),
                        };
                        let lc_query = query.to_lowercase();
                        let trimmed = lc_query.trim();

                        // save the output for select/with queries
                        if trimmed.starts_with("select") || trimmed.starts_with("with") {
                            debug!("Persisting {} batches...", batches.len());

                            result_count += batches.len();
                            local_result = batches;
                        }
                    }
                    false => {
                        debug!(
                            "Running query (ignoring results) {}-{}: {query}",
                            self.group, self.subgroup
                        );

                        result_count += self
                            .execute_sql_without_result_buffering(query, ctx)
                            .await?;
                    }
                }
            }

            Ok::<Vec<RecordBatch>, DataFusionError>(local_result)
        }?;

        debug!("Results have {result_count} rows");

        // Store results for verification
        self.last_results = Some(result);

        Ok(())
    }

    /// Calls run and persists results to disk as a CSV file.
    ///
    /// Requires that the benchmark defines a `result` or `result_query`.
    /// Registers the results in a memory table and writes them to disk with
    /// pipe delimiters and a header row.
    ///
    /// # Errors
    /// Returns an error if no results are available or if writing to the
    /// target path fails.
    pub async fn persist(&mut self, ctx: &SessionContext) -> Result<()> {
        self.run(ctx, true).await?;

        // Check if we have result queries to persist for
        if self.result_queries.is_empty() {
            info!("No result paths to persist");
            return Ok(());
        }

        // Get the stored results from the last run
        let Some(results) = self.last_results.clone() else {
            return Err(exec_datafusion_err!(
                "No results available for verification - does the benchmark return no results?"
            ));
        };

        let query = &self.result_queries[0];
        let path = query.path.as_ref().ok_or_else(|| {
            exec_datafusion_err!(
                "Unable to persist results from query '{}', no result specified",
                query.query
            )
        })?;

        info!("Persisting results for query to {path}");

        let first_batch = results
            .first()
            .ok_or_else(|| exec_datafusion_err!("Results should be loaded"))?;

        let schema = first_batch.schema();
        let provider = MemTable::try_new(schema, vec![results])?;

        ctx.register_table("persist_data", Arc::new(provider))?;

        let df = ctx.table("persist_data").await?;
        df.write_csv(
            path,
            DataFrameWriteOptions::new(),
            Some(
                CsvOptions::default()
                    .with_delimiter(b'|')
                    .with_has_header(true),
            ),
        )
        .await?;

        Ok(())
    }

    /// Verifies persisted results against expected values.
    ///
    /// Executes the `result_query` or uses the stored last run results, then
    /// compares actual output rows to the expected values defined in the
    /// benchmark file.
    ///
    /// # Errors
    /// Returns an error if no results are available or if the actual and
    /// expected results differ in count or content.
    pub async fn verify(&mut self, ctx: &SessionContext) -> Result<()> {
        // Check if we have result queries to verify
        if self.result_queries.is_empty() {
            return Ok(());
        }

        // Get the stored results from the last run
        let Some(actual_results) = self.last_results.clone() else {
            return Err(exec_datafusion_err!(
                "No results available for verification. Run the benchmark first."
            ));
        };

        info!("Verifying results...");

        // Get the first result query (assuming only one for now)
        let query = &self.result_queries[0];
        let results = if !query.query.trim().is_empty() {
            ctx.sql(&query.query).await?.collect().await
        } else {
            Ok(actual_results)
        }?;

        let formatted_actual_results = format_record_batches(results)?;

        Self::compare_results(query, &formatted_actual_results, &query.expected_result)
    }

    /// Runs `cleanup` queries to reset state after the benchmark.
    ///
    /// Executes any `cleanup` SQL defined in the benchmark file, ignoring
    /// failures so that partial cleanup does not abort the overall process.
    pub async fn cleanup(&mut self, ctx: &SessionContext) -> Result<()> {
        info!("Running cleanup...");

        let cleanup_queries = self.queries.get("cleanup");

        if let Some(queries) = cleanup_queries {
            for query in queries {
                let r = ctx.sql(query).await;
                match r {
                    Ok(df) => df.collect().await,
                    Err(e) => Err(e),
                }?;
            }
        }

        Ok(())
    }

    fn compare_results(
        query: &BenchmarkQuery,
        actual_results: &[Vec<String>],
        expected_results: &[Vec<String>],
    ) -> Result<()> {
        if actual_results.is_empty() && expected_results.is_empty() {
            return Ok(());
        }

        // Compare row count
        if actual_results.len() != expected_results.len() {
            return Err(exec_datafusion_err!(
                "Error in result: expected {} rows but got {} for query {}",
                expected_results.len(),
                actual_results.len(),
                query.query
            ));
        }

        // Compare values
        let zipped = actual_results
            .iter()
            .enumerate()
            .zip(expected_results.iter());

        for ((row_idx, actual), expected) in zipped {
            trace!(
                "row {}\nactual: {actual:?}\nexpected: {expected:?}",
                row_idx + 1
            );

            // Compare column count
            if actual.len() != expected.len() {
                return Err(exec_datafusion_err!(
                    "Error in result: expected {} columns but got {} for query {}",
                    expected.len(),
                    actual.len(),
                    query.query
                ));
            }

            for (col_idx, expected_val) in
                expected.iter().enumerate().take(query.column_count)
            {
                let Some(actual_val) = actual.get(col_idx) else {
                    return Err(exec_datafusion_err!(
                        "Error in result on row {} running query {}: expected a value at index {col_idx} in row {actual:?}",
                        row_idx + 1,
                        query.query
                    ));
                };

                trace!("actual_val = {actual_val:?}\nexpected_val = {expected_val:?}");

                if (expected_val == "NULL" && actual_val.is_empty())
                    || (expected_val == actual_val)
                    || (expected_val == "(empty)"
                        && (actual_val.is_empty() || actual_val == "NULL"))
                {
                    continue;
                }

                return Err(exec_datafusion_err!(
                    "Error in result on row {}, column {} running query \"{}\": expected value \
                    \"{expected_val}\" but got value \"{actual_val}\" in row: {actual:?}",
                    row_idx + 1,
                    col_idx + 1,
                    query.query
                ));
            }
        }

        Ok(())
    }

    async fn process_file(&mut self, ctx: &SessionContext, path: &str) -> Result<()> {
        debug!("   Processing file {path}");

        let mut replacement_mapping = self.replacement_mapping.clone();
        replacement_mapping.insert("FILE_PATH".to_string(), path.to_string());

        let mut reader = BenchmarkFileReader::new(path, replacement_mapping)?;
        let mut line = String::with_capacity(1024);
        let mut reader_result = reader.read_line(&mut line);

        while let Some(result) = reader_result {
            match result {
                Ok(_) => {
                    if !line.is_empty()
                        && !line.starts_with('#')
                        && !line.starts_with("--")
                    {
                        // boxing required because of recursion
                        Box::pin(self.process_line(ctx, &mut reader, &mut line)).await?;
                    }
                }
                Err(e) => return Err(e),
            }

            // Clear the line buffer for the next iteration.
            line.clear();
            reader_result = reader.read_line(&mut line);
        }

        Ok(())
    }

    async fn process_line(
        &mut self,
        ctx: &SessionContext,
        reader: &mut BenchmarkFileReader,
        line: &mut String,
    ) -> Result<()> {
        // Split the line into lower‑cased tokens.
        let cloned_line = line.clone();
        let splits: Vec<&str> = cloned_line.split_whitespace().collect();

        BenchmarkQualifier::select(reader, splits[0])?
            .process(ctx, self, reader, line, splits)
            .await
    }

    fn process_query(&mut self, splits: &[&str], mut query: String) -> Result<()> {
        debug!("processing query {query}");

        // Trim and validate.
        query = query.trim().to_string();
        if query.is_empty() {
            return Ok(());
        }

        // remove comments
        query = query
            .lines()
            .filter(|line| !line.starts_with('#') && !line.starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");

        if query.trim().is_empty() {
            return Ok(());
        }

        query = process_replacements(&query, self.replacement_mapping())?;

        let mut v = self.queries.get(splits[0]).cloned().unwrap_or_default();
        v.push(query.clone());

        self.queries.insert(splits[0].to_string(), v);

        Ok(())
    }

    async fn execute_sql_without_result_buffering(
        &self,
        sql: &str,
        ctx: &SessionContext,
    ) -> Result<usize> {
        let mut row_count = 0;

        let df = ctx.sql(sql).await?;
        let physical_plan = df.create_physical_plan().await?;

        if !self.expect.is_empty() {
            let plan_string = format!("{physical_plan:#?}");

            for exp_str in self.expect.iter() {
                if !plan_string.contains(exp_str) {
                    return Err(exec_datafusion_err!(
                        "Query's physical plan does not contain the expected string '{exp_str}'. Physical plan: {plan_string}"
                    ));
                }
            }
        }
        let mut stream = execute_stream(physical_plan, ctx.task_ctx())?;

        while let Some(batch) = stream.next().await {
            row_count += batch?.num_rows();

            // Evaluate the result and do nothing, the result will be dropped
            // to reduce memory pressure
        }

        Ok(row_count)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn group(&self) -> &str {
        &self.group
    }

    pub fn subgroup(&self) -> &str {
        &self.subgroup
    }

    pub fn benchmark_path(&self) -> &PathBuf {
        &self.benchmark_path
    }

    pub fn replacement_mapping(&self) -> &HashMap<String, String> {
        &self.replacement_mapping
    }

    pub fn queries(&self) -> &HashMap<String, Vec<String>> {
        &self.queries
    }

    pub fn result_queries(&self) -> &Vec<BenchmarkQuery> {
        &self.result_queries
    }

    pub fn assert_queries(&self) -> &Vec<BenchmarkQuery> {
        &self.assert_queries
    }

    pub fn is_loaded(&self) -> bool {
        self.is_loaded
    }
}

/// A trait for processing individual lines from benchmark files.
///
/// The `LineProcessor` trait defines the interface for handlers that process
/// specific directives (like `load`, `run`, `assert`, etc.) found in benchmark
/// configuration files. Each implementation of this trait handles a particular
/// type of directive and performs the appropriate actions to configure and
/// execute benchmarks.
///
/// # Implementors
///
/// The trait is implemented by several specialized handler types:
///
/// - `LoadRunInitCleanup`: Handles `load`, `run`, `init`, and `cleanup` directives
///   for SQL queries and file operations
/// - `Name`: Processes the `name` directive to set the benchmark's display name
/// - `Group`: Processes the `group` directive to set the benchmark's group name
/// - `Subgroup`: Processes the `subgroup` directive for logical grouping
/// - `Expect`: Processes the `expect_plan` directive to specify expected strings
///   in the physical query plan
/// - `Assert`: Processes the `assert` directive to define result validation tests
/// - `Results`: Handles `result` directives for persisting and verifying query results
/// - `ResultQuery`: Handles `result_query` directives for persisting and verifying query results
/// - `Template`: Processes the `template` directive to include and expand
///   template files with parameters
/// - `Include`: Processes the `include` directive to include other benchmark files
/// - `Echo`: Processes the `echo` directive to print messages during execution
///
/// # Error Handling
///
/// Each processor returns a `Result<()>`. Errors typically indicate:
/// - Invalid directive syntax
/// - Missing required parameters
/// - File I/O failures
/// - Validation failures (e.g., mismatched result counts)
trait LineProcessor {
    async fn process(
        &self,
        ctx: &SessionContext,
        bench: &mut SqlBenchmark,
        reader: &mut BenchmarkFileReader,
        line: &mut String,
        splits: Vec<&str>,
    ) -> Result<()>;
}

#[derive(Debug, Default)]
struct LoadRunInitCleanup;
#[derive(Debug, Default)]
struct Name;
#[derive(Debug, Default)]
struct Group;
#[derive(Debug, Default)]
struct Subgroup;
#[derive(Debug, Default)]
struct Expect;
#[derive(Debug, Default)]
struct Assert;
#[derive(Debug, Default)]
struct Results;
#[derive(Debug, Default)]
struct ResultQuery;
#[derive(Debug, Default)]
struct Template;
#[derive(Debug, Default)]
struct Include;
#[derive(Debug, Default)]
struct Echo;

impl LineProcessor for LoadRunInitCleanup {
    async fn process(
        &self,
        _ctx: &SessionContext,
        bench: &mut SqlBenchmark,
        reader: &mut BenchmarkFileReader,
        line: &mut String,
        splits: Vec<&str>,
    ) -> Result<()> {
        trace!("-- handling {}", splits[0]);

        if bench.queries.contains_key(splits[0]) {
            return Err(exec_datafusion_err!(
                "Multiple calls to {} in the same benchmark file",
                splits[0]
            ));
        }

        line.clear();

        // Read the query body until a blank line or EOF.
        let mut query = String::new();
        let mut reader_result = reader.read_line(line);

        loop {
            match reader_result {
                Some(Ok(_)) => {
                    if line.starts_with('#') || line.starts_with("--") {
                        // comment, ignore
                    } else if line.is_empty() {
                        break;
                    } else {
                        query.push_str(line);
                        query.push(' ');
                    }
                }
                Some(Err(e)) => return Err(e),
                None => break,
            }

            // Clear the line buffer for the next iteration.
            line.clear();
            reader_result = reader.read_line(line);
        }

        // Optional file parameter.
        if splits.len() > 1 && !splits[1].is_empty() {
            debug!("Processing {} file: {}", splits[0], splits[1]);

            let Ok(mut file) = File::open(splits[1]) else {
                return Err(exec_datafusion_err!(
                    "Failed to open query file {}",
                    splits[1]
                ));
            };
            let mut buf = Vec::new();

            let Ok(_) = file.read_to_end(&mut buf) else {
                return Err(exec_datafusion_err!(
                    "Failed to read query file {}",
                    splits[1]
                ));
            };

            let str = String::from_utf8_lossy(&buf);
            let split: Vec<&str> = str.split("\n\n").collect();
            let mut queries = vec![];

            // some files have multiple queries, split apart
            split.iter().for_each(|&query| {
                let s: Vec<&str> = query.split(";\n").collect();
                for query in s {
                    queries.push(query);
                }
            });

            for query in queries {
                bench.process_query(&splits, query.to_string())?;
            }
        } else {
            bench.process_query(&splits, query)?;
        }

        Ok(())
    }
}

impl LineProcessor for Name {
    async fn process(
        &self,
        _ctx: &SessionContext,
        bench: &mut SqlBenchmark,
        _reader: &mut BenchmarkFileReader,
        line: &mut String,
        splits: Vec<&str>,
    ) -> Result<()> {
        trace!("-- handling {}", splits[0]);

        // Extract the display value from the line
        let result = line[splits[0].len() + 1..].trim().to_string();
        bench.name = result.to_string();

        bench
            .replacement_mapping
            .insert("BENCH_NAME".to_string(), result);

        Ok(())
    }
}

impl LineProcessor for Group {
    async fn process(
        &self,
        _ctx: &SessionContext,
        bench: &mut SqlBenchmark,
        _reader: &mut BenchmarkFileReader,
        line: &mut String,
        splits: Vec<&str>,
    ) -> Result<()> {
        trace!("-- handling {}", splits[0]);

        // Extract the display value from the line
        let result = line[splits[0].len() + 1..].trim().to_string();
        bench.group.clone_from(&result);

        bench
            .replacement_mapping
            .insert("BENCH_GROUP".to_string(), result);

        Ok(())
    }
}

impl LineProcessor for Subgroup {
    async fn process(
        &self,
        _ctx: &SessionContext,
        bench: &mut SqlBenchmark,
        _reader: &mut BenchmarkFileReader,
        line: &mut String,
        splits: Vec<&str>,
    ) -> Result<()> {
        trace!("-- handling {}", splits[0]);

        // Extract the display value from the line
        let result = line[splits[0].len() + 1..].trim().to_string();
        bench.subgroup.clone_from(&result);

        bench
            .replacement_mapping
            .insert("BENCH_SUBGROUP".to_string(), result);

        Ok(())
    }
}

impl LineProcessor for Expect {
    async fn process(
        &self,
        _ctx: &SessionContext,
        bench: &mut SqlBenchmark,
        reader: &mut BenchmarkFileReader,
        _line: &mut String,
        splits: Vec<&str>,
    ) -> Result<()> {
        trace!("-- handling {}", splits[0]);

        if splits.len() <= 1 || splits[1].is_empty() {
            return Err(exec_datafusion_err!(
                "{}",
                reader.format_exception(
                    "expect_plan must be followed by a string to search in the physical plan"
                )
            ));
        }

        bench.expect.push(splits[1..].join(" ").to_string());

        Ok(())
    }
}

impl LineProcessor for Assert {
    async fn process(
        &self,
        _ctx: &SessionContext,
        bench: &mut SqlBenchmark,
        reader: &mut BenchmarkFileReader,
        line: &mut String,
        splits: Vec<&str>,
    ) -> Result<()> {
        trace!("-- handling {}", splits[0]);

        // count the amount of columns
        if splits.len() <= 1 || splits[1].is_empty() {
            return Err(exec_datafusion_err!(
                "{}",
                reader.format_exception(
                    "assert must be followed by a column count (e.g. assert III)"
                )
            ));
        }

        line.clear();

        // read the actual query
        let mut found_break = false;
        let mut sql = String::new();
        let mut reader_result = reader.read_line(line);

        loop {
            match reader_result {
                Some(Ok(_)) => {
                    if line == "----" {
                        found_break = true;
                        break;
                    }
                    sql.push('\n');
                    sql.push_str(line);
                }
                Some(Err(e)) => return Err(e),
                None => break,
            }

            // Clear the line buffer for the next iteration.
            line.clear();
            reader_result = reader.read_line(line);
        }

        if !found_break {
            return Err(exec_datafusion_err!(
                "{}",
                reader.format_exception(
                    "assert must be followed by a query and a result (separated by ----)"
                )
            ));
        }

        bench
            .assert_queries
            .push(read_query_from_reader(reader, &sql, splits[1])?);

        Ok(())
    }
}

impl LineProcessor for Results {
    async fn process(
        &self,
        ctx: &SessionContext,
        bench: &mut SqlBenchmark,
        reader: &mut BenchmarkFileReader,
        _line: &mut String,
        splits: Vec<&str>,
    ) -> Result<()> {
        trace!("-- handling {}", splits[0]);

        if splits.len() <= 1 || splits[1].is_empty() {
            return Err(exec_datafusion_err!(
                "{}",
                reader.format_exception(
                    "result must be followed by a path to a result file"
                )
            ));
        }

        let bq = read_query_from_file(ctx, splits[1], &bench.replacement_mapping).await?;

        if !bench.result_queries.is_empty() {
            return Err(exec_datafusion_err!(
                "{}",
                reader.format_exception("multiple results found")
            ));
        }

        bench.result_queries.push(bq);

        Ok(())
    }
}

impl LineProcessor for ResultQuery {
    async fn process(
        &self,
        _ctx: &SessionContext,
        bench: &mut SqlBenchmark,
        reader: &mut BenchmarkFileReader,
        line: &mut String,
        splits: Vec<&str>,
    ) -> Result<()> {
        trace!("-- handling {}", splits[0]);

        if splits.len() <= 1 || splits[1].is_empty() {
            return Err(exec_datafusion_err!(
                "{}",
                reader.format_exception(
                    "result_query must be followed by a column count (e.g. result_query III)"
                )
            ));
        }

        line.clear();

        let mut sql = String::new();
        let mut found_break = false;
        let mut reader_result = reader.read_line(line);

        loop {
            match reader_result {
                Some(Ok(_)) => {
                    if line.trim() == "----" {
                        found_break = true;
                        break;
                    }
                    sql.push_str(line);
                    sql.push('\n');
                }
                Some(Err(e)) => return Err(e),
                None => break,
            }

            // Clear the line buffer for the next iteration.
            line.clear();
            reader_result = reader.read_line(line);
        }

        if !found_break {
            return Err(exec_datafusion_err!("{}", reader.format_exception(
                        "result_query must be followed by a query and a result (separated by ----)"
                    )));
        }

        let result_check = read_query_from_reader(reader, &sql, splits[1])?;

        if !bench.result_queries.is_empty() {
            return Err(exec_datafusion_err!(
                "{}",
                reader.format_exception("multiple results found")
            ));
        }
        bench.result_queries.push(result_check);

        Ok(())
    }
}

impl LineProcessor for Template {
    async fn process(
        &self,
        ctx: &SessionContext,
        bench: &mut SqlBenchmark,
        reader: &mut BenchmarkFileReader,
        line: &mut String,
        splits: Vec<&str>,
    ) -> Result<()> {
        trace!("-- handling {}", splits[0]);

        // template: update the path to read
        bench.benchmark_path = PathBuf::from(splits[1]);

        line.clear();

        // now read parameters
        let mut reader_result = reader.read_line(line);

        loop {
            match reader_result {
                Some(Ok(_)) => {
                    if line.starts_with('#') || line.starts_with("--") {
                        // Clear the line buffer for the next iteration.
                        line.clear();
                        reader_result = reader.read_line(line);
                        continue;
                    }
                    if line.is_empty() {
                        break;
                    }

                    let parameters: Vec<&str> = line.split("=").collect();
                    if parameters.len() != 2 {
                        return Err(exec_datafusion_err!(
                            "{}",
                            reader.format_exception(
                                "Expected a template parameter in the form of X=Y"
                            )
                        ));
                    }
                    bench
                        .replacement_mapping
                        .insert(parameters[0].to_string(), parameters[1].to_string());
                }
                Some(Err(e)) => return Err(e),
                None => break,
            }

            // Clear the line buffer for the next iteration.
            line.clear();
            reader_result = reader.read_line(line);
        }

        // restart the load from the template file
        bench.process_file(ctx, splits[1]).await
    }
}

impl LineProcessor for Include {
    async fn process(
        &self,
        ctx: &SessionContext,
        bench: &mut SqlBenchmark,
        reader: &mut BenchmarkFileReader,
        _line: &mut String,
        splits: Vec<&str>,
    ) -> Result<()> {
        trace!("-- handling {}", splits[0]);

        if splits.len() != 2 {
            return Err(exec_datafusion_err!(
                "{}",
                reader.format_exception("include requires a single argument")
            ));
        }
        bench.process_file(ctx, splits[1]).await
    }
}

impl LineProcessor for Echo {
    async fn process(
        &self,
        _ctx: &SessionContext,
        bench: &mut SqlBenchmark,
        reader: &mut BenchmarkFileReader,
        _line: &mut String,
        splits: Vec<&str>,
    ) -> Result<()> {
        trace!("-- handling {}", splits[0]);

        if splits.len() < 2 {
            return Err(exec_datafusion_err!(
                "{}",
                reader.format_exception("Echo requires an argument")
            ));
        }

        bench.echo.push(splits[1..].join(" "));

        Ok(())
    }
}

enum BenchmarkQualifier {
    Load(LoadRunInitCleanup),
    Run(LoadRunInitCleanup),
    Init(LoadRunInitCleanup),
    Cleanup(LoadRunInitCleanup),
    Name(Name),
    Group(Group),
    Subgroup(Subgroup),
    Expect(Expect),
    Assert(Assert),
    ResultQuery(ResultQuery),
    Results(Results),
    Template(Template),
    Include(Include),
    Echo(Echo),
}

impl BenchmarkQualifier {
    fn select(reader: &mut BenchmarkFileReader, str: &str) -> Result<BenchmarkQualifier> {
        match str.to_lowercase().as_str() {
            "load" => Ok(BenchmarkQualifier::Load(LoadRunInitCleanup)),
            "run" => Ok(BenchmarkQualifier::Run(LoadRunInitCleanup)),
            "init" => Ok(BenchmarkQualifier::Init(LoadRunInitCleanup)),
            "cleanup" => Ok(BenchmarkQualifier::Cleanup(LoadRunInitCleanup)),
            "name" => Ok(BenchmarkQualifier::Name(Name)),
            "group" => Ok(BenchmarkQualifier::Group(Group)),
            "subgroup" => Ok(BenchmarkQualifier::Subgroup(Subgroup)),
            "expect_plan" => Ok(BenchmarkQualifier::Expect(Expect)),
            "assert" => Ok(BenchmarkQualifier::Assert(Assert)),
            "result_query" => Ok(BenchmarkQualifier::ResultQuery(ResultQuery)),
            "result" => Ok(BenchmarkQualifier::Results(Results)),
            "template" => Ok(BenchmarkQualifier::Template(Template)),
            "include" => Ok(BenchmarkQualifier::Include(Include)),
            "echo" => Ok(BenchmarkQualifier::Echo(Echo)),
            _ =>
            // Unknown command
            {
                Err(exec_datafusion_err!(
                    "{}",
                    reader.format_exception(&format!("Unrecognized command: {str}"))
                ))
            }
        }
    }
}

impl LineProcessor for BenchmarkQualifier {
    async fn process(
        &self,
        ctx: &SessionContext,
        bench: &mut SqlBenchmark,
        reader: &mut BenchmarkFileReader,
        line: &mut String,
        splits: Vec<&str>,
    ) -> Result<()> {
        match self {
            BenchmarkQualifier::Load(l) => {
                l.process(ctx, bench, reader, line, splits).await
            }
            BenchmarkQualifier::Run(l) => {
                l.process(ctx, bench, reader, line, splits).await
            }
            BenchmarkQualifier::Init(l) => {
                l.process(ctx, bench, reader, line, splits).await
            }
            BenchmarkQualifier::Cleanup(l) => {
                l.process(ctx, bench, reader, line, splits).await
            }
            BenchmarkQualifier::Name(l) => {
                l.process(ctx, bench, reader, line, splits).await
            }
            BenchmarkQualifier::Group(l) => {
                l.process(ctx, bench, reader, line, splits).await
            }
            BenchmarkQualifier::Subgroup(l) => {
                l.process(ctx, bench, reader, line, splits).await
            }
            BenchmarkQualifier::Expect(l) => {
                l.process(ctx, bench, reader, line, splits).await
            }
            BenchmarkQualifier::Assert(l) => {
                l.process(ctx, bench, reader, line, splits).await
            }
            BenchmarkQualifier::ResultQuery(l) => {
                l.process(ctx, bench, reader, line, splits).await
            }
            BenchmarkQualifier::Results(l) => {
                l.process(ctx, bench, reader, line, splits).await
            }
            BenchmarkQualifier::Template(l) => {
                l.process(ctx, bench, reader, line, splits).await
            }
            BenchmarkQualifier::Include(l) => {
                l.process(ctx, bench, reader, line, splits).await
            }
            BenchmarkQualifier::Echo(l) => {
                l.process(ctx, bench, reader, line, splits).await
            }
        }
    }
}

fn parse_group_from_path(path: &str, benchmark_directory: &str) -> String {
    let mut group_name: String = String::new();

    let path_buf = PathBuf::from(path);
    let mut parent = path_buf.parent();

    while let Some(p) = parent {
        let display = p.display().to_string().to_lowercase();
        let x = display.trim_end_matches("/");

        if x.ends_with(&benchmark_directory.to_lowercase()) {
            break;
        } else {
            let dir_name = p
                .file_name()
                .unwrap_or_else(|| OsStr::new(""))
                .display()
                .to_string();
            group_name = dir_name;
        }
        parent = p.parent();
    }

    group_name
}

fn replace_all<E>(
    re: &Regex,
    haystack: &str,
    replacement: impl Fn(&regex::Captures) -> Result<String, E>,
) -> Result<String, E> {
    let mut new = String::with_capacity(haystack.len());
    let mut last_match = 0;

    for caps in re.captures_iter(haystack) {
        let m = caps.get(0).unwrap();

        new.push_str(&haystack[last_match..m.start()]);
        new.push_str(&replacement(&caps)?);

        last_match = m.end();
    }

    new.push_str(&haystack[last_match..]);

    Ok(new)
}

/// Replace all `${KEY}` or `${KEY:-default}` placeholders in a string according to the mapping.
/// Also handles `${KEY|True value|false value}` syntax.
fn process_replacements(
    str: &str,
    replacement_map: &HashMap<String, String>,
) -> Result<String> {
    debug!("processing replacements for line '{str}'");

    // handle ${VAR|true value|false value} syntax
    let re =
        Regex::new(r"\$\{(\w+)\|([^|]+)\|([^}]+)}").expect("Regex failed to compile");
    let replacement = |caps: &regex::Captures| -> Result<String> {
        let key = &caps[1];
        let true_val = &caps[2];
        let false_val = &caps[3];

        let e = std::env::var(key.to_uppercase());
        if let Ok(v) = e
            && v.eq_ignore_ascii_case("true")
        {
            Ok(true_val.to_string())
        } else {
            Ok(false_val.to_string())
        }
    };
    let str =
        &replace_all(&re, str, replacement).expect("process replacement should succeed");

    // handle ${KEY} and ${KEY:-default}`
    let re = Regex::new(r"\$\{(\w+)(?::-([^}]+))?}").expect("Regex failed to compile");
    let replacement = |caps: &regex::Captures| -> Result<String> {
        let key = &caps[1];
        let default = caps.get(2);

        // search replacement map for key
        for (k, v) in replacement_map {
            if key.eq_ignore_ascii_case(k) {
                return Ok(v.to_string());
            }
        }

        // look in env variables
        let e = std::env::var(key.to_uppercase());
        if let Ok(v) = e {
            return Ok(v.to_string());
        }

        // use default if it was set
        if let Some(def) = default {
            Ok(def.as_str().to_string())
        } else {
            Err(exec_datafusion_err!("Missing value for key '{key}'"))
        }
    };

    Ok(replace_all(&re, str, replacement).expect("process replacement should succeed"))
}

fn read_query_from_reader(
    reader: &mut BenchmarkFileReader,
    sql: &str,
    header: &str,
) -> Result<BenchmarkQuery> {
    let column_count = header.len();
    let mut expected_result = Vec::new();
    let mut line = String::new();
    let mut reader_result = reader.read_line(&mut line);

    loop {
        match reader_result {
            Some(Ok(_)) => {
                if line.starts_with('#') || line.starts_with("--") {
                    // comment, ignore
                } else if line.is_empty() {
                    break;
                } else {
                    let result_splits: Vec<&str> = line.split(['\t', '|']).collect();

                    if result_splits.len() != column_count {
                        return Err(exec_datafusion_err!(
                            "{} {line}",
                            reader.format_exception(&format!(
                                "expected {} values but got {}",
                                column_count,
                                result_splits.len(),
                            ))
                        ));
                    }

                    expected_result
                        .push(result_splits.into_iter().map(|s| s.to_string()).collect());
                }
            }
            Some(Err(e)) => return Err(e),
            None => break,
        }

        // Clear the line buffer for the next iteration.
        line.clear();
        reader_result = reader.read_line(&mut line);
    }

    Ok(BenchmarkQuery {
        path: None,
        query: sql.to_string(),
        column_count,
        expected_result,
    })
}

async fn read_query_from_file(
    ctx: &SessionContext,
    path: &str,
    replacement_mapping: &HashMap<String, String>,
) -> Result<BenchmarkQuery> {
    // Process replacements in file path
    let path = process_replacements(path, replacement_mapping)?;
    let df: DataFrame = ctx
        .read_csv(
            path.clone(),
            CsvReadOptions::new()
                .has_header(true)
                .delimiter(b'|')
                .null_regex(Some("NULL".to_string()))
                // we only want string values, we do not want to infer the schema
                .schema_infer_max_records(0),
        )
        .await?;

    // Get schema to determine column count
    let schema = df.schema();
    let column_count = schema.fields().len();
    // Execute and collect results
    let batches = df.collect().await?;
    // Convert record batches to string vectors
    let expected_result = format_record_batches(batches)?;

    Ok(BenchmarkQuery {
        path: Some(path),
        query: String::new(),
        column_count,
        expected_result,
    })
}

fn format_record_batches(
    batches: Vec<RecordBatch>,
) -> Result<Vec<Vec<String>>, DataFusionError> {
    let mut expected_result = vec![];
    let arrow_format_options = FormatOptions::default()
        .with_null("NULL")
        .with_display_error(true);

    for batch in batches {
        let schema = batch.schema_ref();

        // Could be a custom schema that was provided.
        if batch.columns().len() != schema.fields().len() {
            return Err(exec_datafusion_err!(
                "{}",
                format!(
                    "Expected the same number of columns in a record batch ({}) as the number of fields ({}) in the schema",
                    batch.columns().len(),
                    schema.fields.len()
                )
            ));
        }

        let formatters = batch
            .columns()
            .iter()
            .zip(schema.fields().iter())
            .map(|(c, field)| make_array_formatter(c, &arrow_format_options, Some(field)))
            .collect::<std::result::Result<Vec<_>, ArrowError>>()?;

        for row in 0..batch.num_rows() {
            let mut cells = Vec::new();
            for formatter in &formatters {
                cells.push(formatter.value(row).to_string());
            }
            expected_result.push(cells);
        }
    }

    Ok(expected_result)
}

fn make_array_formatter<'a>(
    array: &'a dyn Array,
    options: &FormatOptions<'a>,
    field: Option<&'a Field>,
) -> std::result::Result<ArrayFormatter<'a>, ArrowError> {
    match options.formatter_factory() {
        None => ArrayFormatter::try_new(array, options),
        Some(formatters) => formatters
            .create_array_formatter(array, options, field)
            .transpose()
            .unwrap_or_else(|| ArrayFormatter::try_new(array, options)),
    }
}

struct BenchmarkFileReader {
    path: PathBuf,
    reader: BufReader<File>,
    line_nr: usize,
    replacements: HashMap<String, String>,
}

impl BenchmarkFileReader {
    fn new<P: Into<PathBuf>>(
        path: P,
        replacements: HashMap<String, String>,
    ) -> Result<Self> {
        let path = path.into();
        let file = OpenOptions::new().read(true).open(&path)?;

        Ok(Self {
            path,
            reader: BufReader::new(file),
            line_nr: 0,
            replacements,
        })
    }

    /// Read the next line, applying replacements and trimming.
    fn read_line(&mut self, line: &mut String) -> Option<Result<()>> {
        match self.reader.read_line(line) {
            Ok(0) => None,
            Ok(_) => {
                self.line_nr += 1;

                // Trim newline and carriage return
                let trimmed = line.trim_end_matches(['\n', '\r']);
                if trimmed != line.as_str() {
                    *line = trimmed.to_string();
                }

                match process_replacements(line, &self.replacements) {
                    Ok(l) => {
                        *line = l.trim().to_string();
                        Some(Ok(()))
                    }
                    Err(error) => Some(Err(error)),
                }
            }
            Err(e) => Some(Err(e.into())),
        }
    }

    fn format_exception(&self, msg: &str) -> String {
        format!("{}:{} - {}", self.path.display(), self.line_nr, msg)
    }
}

#[derive(Debug, Clone)]
pub struct BenchmarkQuery {
    path: Option<String>,
    query: String,
    column_count: usize,
    expected_result: Vec<Vec<String>>,
}

impl BenchmarkQuery {}
