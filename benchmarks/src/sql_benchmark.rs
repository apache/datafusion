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
use log::{debug, info, trace, warn};
use regex::Regex;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

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
    queries: HashMap<QueryDirective, Vec<String>>,
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
        full_path: impl AsRef<Path>,
        benchmark_directory: impl AsRef<Path>,
    ) -> Result<Self> {
        let full_path = full_path.as_ref();
        let benchmark_directory = benchmark_directory.as_ref();
        let group_name = parse_group_from_path(full_path, benchmark_directory);
        let mut bm = Self {
            name: String::new(),
            group: group_name,
            subgroup: String::new(),
            benchmark_path: full_path.to_path_buf(),
            replacement_mapping: HashMap::new(),
            expect: vec![],
            queries: HashMap::new(),
            result_queries: vec![],
            assert_queries: vec![],
            is_loaded: false,
            last_results: None,
            echo: vec![],
        };
        insert_replacement(
            &mut bm.replacement_mapping,
            "BENCHMARK_DIR",
            benchmark_directory.to_string_lossy().into_owned(),
        );

        let path = bm.benchmark_path.clone();
        bm.process_file(ctx, &path).await?;

        Ok(bm)
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
        if !self.queries.contains_key(&QueryDirective::Run) {
            return Err(exec_datafusion_err!(
                "Invalid benchmark file: no \"run\" query specified: {path}"
            ));
        }

        // display any echo's
        self.echo.iter().for_each(|txt| println!("{txt}"));

        let load_queries = self.queries.get(&QueryDirective::Load);

        if let Some(queries) = load_queries {
            for query in queries {
                debug!("Executing load query {query}");
                ctx.sql(query).await?.collect().await?;
            }
        }

        let init_queries = self.queries.get(&QueryDirective::Init);

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

            info!("Executing assert query {query}");

            let result = ctx.sql(query).await?.collect().await?;
            let formatted_actual_results = format_record_batches(&result)?;

            Self::compare_results(
                assert_query,
                &formatted_actual_results,
                &assert_query.expected_result,
            )?;
        }

        Ok(())
    }

    /// Executes the `run` queries, optionally saving results for later
    /// verification. If there are multiple queries only the results for
    /// the last query are saved.
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
            .get(&QueryDirective::Run)
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
                            self.validate_expected_plan(&physical_plan)?;
                        }

                        let result_schema = Arc::new(df.schema().as_arrow().clone());
                        let mut batches = df.collect().await?;
                        let trimmed = query.trim_start();

                        // save the output for select/with queries
                        if starts_with_ignore_ascii_case(trimmed, "select")
                            || starts_with_ignore_ascii_case(trimmed, "with")
                        {
                            if batches.is_empty() {
                                batches.push(RecordBatch::new_empty(result_schema));
                            }
                            let row_count_for_query =
                                batches.iter().map(RecordBatch::num_rows).sum::<usize>();
                            debug!(
                                "Persisting {} batches ({} rows)...",
                                batches.len(),
                                row_count_for_query
                            );

                            result_count = row_count_for_query;
                            local_result = batches;
                        }
                    }
                    false => {
                        debug!(
                            "Running query (ignoring results) {}-{}: {query}",
                            self.group, self.subgroup
                        );

                        result_count = self
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

        let results = self
            .last_results
            .as_ref()
            .expect("run should store last_results after successful execution");

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
        let provider = MemTable::try_new(schema, vec![results.clone()])?;

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

        ctx.deregister_table("persist_data")?;

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

        if self.last_results.is_none() {
            return Err(exec_datafusion_err!(
                "No results available for verification. Run the benchmark first."
            ));
        }

        info!("Verifying results...");

        self.load_expected_result_files(ctx).await?;

        // Get the first result query (assuming only one for now)
        let query = &self.result_queries[0];
        let formatted_actual_results = if !query.query.trim().is_empty() {
            let results = ctx.sql(&query.query).await?.collect().await?;
            format_record_batches(&results)
        } else {
            let actual_results = self
                .last_results
                .as_ref()
                .expect("last_results should be present after successful run");
            format_record_batches(actual_results)
        }?;

        Self::compare_results(query, &formatted_actual_results, &query.expected_result)
    }

    /// Runs `cleanup` queries to reset state after the benchmark run.
    pub async fn cleanup(&mut self, ctx: &SessionContext) -> Result<()> {
        info!("Running cleanup...");

        let cleanup_queries = self.queries.get(&QueryDirective::Cleanup);

        if let Some(queries) = cleanup_queries {
            for query in queries {
                let _ = ctx.sql(query).await?.collect().await?;
            }
        }

        Ok(())
    }

    async fn load_expected_result_files(&mut self, ctx: &SessionContext) -> Result<()> {
        for query in &mut self.result_queries {
            if query.query.trim().is_empty() {
                let Some(path) = query.path.clone() else {
                    continue;
                };

                let loaded_query =
                    read_query_from_file(ctx, path, &HashMap::new()).await?;
                query.column_count = loaded_query.column_count;
                query.expected_result = loaded_query.expected_result;
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
                // The row-width check above guarantees this index exists.
                let actual_val = &actual[col_idx];

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

    async fn process_file(&mut self, ctx: &SessionContext, path: &Path) -> Result<()> {
        debug!("Processing file {}", path.display());

        let mut replacement_mapping = self.replacement_mapping.clone();
        insert_replacement(
            &mut replacement_mapping,
            "FILE_PATH",
            path.to_string_lossy().into_owned(),
        );

        let mut reader = BenchmarkFileReader::new(path, replacement_mapping)?;
        let mut line = String::with_capacity(1024);
        let mut reader_result = reader.read_line(&mut line);

        while let Some(result) = reader_result {
            match result {
                Ok(_) => {
                    if !is_blank_or_comment_line(&line) {
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
        // Split the line into directive and arguments.
        let cloned_line = line.trim_start().to_string();
        let splits: Vec<&str> = cloned_line.split_whitespace().collect();

        BenchmarkDirective::select(reader, splits[0])?
            .process(ctx, self, reader, line, &splits)
            .await
    }

    fn process_query(&mut self, splits: &[&str], mut query: String) -> Result<()> {
        debug!("Processing query {query}");

        // Trim and validate.
        query = query.trim().to_string();
        if query.is_empty() {
            return Ok(());
        }

        // remove comments
        query = query
            .lines()
            .filter(|line| !is_comment_line(line))
            .collect::<Vec<_>>()
            .join("\n");

        if query.trim().is_empty() {
            return Ok(());
        }

        query = process_replacements(&query, self.replacement_mapping())?;

        let directive = QueryDirective::parse(splits[0]).ok_or_else(|| {
            exec_datafusion_err!("Invalid query directive: {}", splits[0])
        })?;

        self.queries.entry(directive).or_default().push(query);

        Ok(())
    }

    fn validate_expected_plan(&self, physical_plan: &impl Debug) -> Result<()> {
        if self.expect.is_empty() {
            return Ok(());
        }

        let plan_string = format!("{physical_plan:#?}");

        for exp_str in &self.expect {
            if !plan_string.contains(exp_str) {
                return Err(exec_datafusion_err!(
                    "The query physical plan does not contain the expected string '{exp_str}'. Physical plan: {plan_string}"
                ));
            }
        }

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

        self.validate_expected_plan(&physical_plan)?;
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

    pub fn benchmark_path(&self) -> &Path {
        &self.benchmark_path
    }

    pub fn replacement_mapping(&self) -> &HashMap<String, String> {
        &self.replacement_mapping
    }

    pub fn queries(&self) -> &HashMap<QueryDirective, Vec<String>> {
        &self.queries
    }

    pub fn result_queries(&self) -> &[BenchmarkQuery] {
        &self.result_queries
    }

    pub fn assert_queries(&self) -> &[BenchmarkQuery] {
        &self.assert_queries
    }

    pub fn is_loaded(&self) -> bool {
        self.is_loaded
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueryDirective {
    Load,
    Run,
    Init,
    Cleanup,
}

impl QueryDirective {
    fn parse(value: &str) -> Option<Self> {
        if value.eq_ignore_ascii_case("load") {
            Some(Self::Load)
        } else if value.eq_ignore_ascii_case("init") {
            Some(Self::Init)
        } else if value.eq_ignore_ascii_case("run") {
            Some(Self::Run)
        } else if value.eq_ignore_ascii_case("cleanup") {
            Some(Self::Cleanup)
        } else {
            None
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Load => "load",
            Self::Run => "run",
            Self::Init => "init",
            Self::Cleanup => "cleanup",
        }
    }
}

enum BenchmarkDirective {
    Load,
    Run,
    Init,
    Cleanup,
    Name,
    Group,
    Subgroup,
    Expect,
    Assert,
    ResultQuery,
    Results,
    Template,
    Include,
    Echo,
}

impl BenchmarkDirective {
    fn select(
        reader: &BenchmarkFileReader,
        directive: &str,
    ) -> Result<BenchmarkDirective> {
        if directive.eq_ignore_ascii_case("load") {
            Ok(BenchmarkDirective::Load)
        } else if directive.eq_ignore_ascii_case("run") {
            Ok(BenchmarkDirective::Run)
        } else if directive.eq_ignore_ascii_case("init") {
            Ok(BenchmarkDirective::Init)
        } else if directive.eq_ignore_ascii_case("cleanup") {
            Ok(BenchmarkDirective::Cleanup)
        } else if directive.eq_ignore_ascii_case("name") {
            Ok(BenchmarkDirective::Name)
        } else if directive.eq_ignore_ascii_case("group") {
            Ok(BenchmarkDirective::Group)
        } else if directive.eq_ignore_ascii_case("subgroup") {
            Ok(BenchmarkDirective::Subgroup)
        } else if directive.eq_ignore_ascii_case("expect_plan") {
            Ok(BenchmarkDirective::Expect)
        } else if directive.eq_ignore_ascii_case("assert") {
            Ok(BenchmarkDirective::Assert)
        } else if directive.eq_ignore_ascii_case("result_query") {
            Ok(BenchmarkDirective::ResultQuery)
        } else if directive.eq_ignore_ascii_case("result") {
            Ok(BenchmarkDirective::Results)
        } else if directive.eq_ignore_ascii_case("template") {
            Ok(BenchmarkDirective::Template)
        } else if directive.eq_ignore_ascii_case("include") {
            Ok(BenchmarkDirective::Include)
        } else if directive.eq_ignore_ascii_case("echo") {
            Ok(BenchmarkDirective::Echo)
        } else {
            Err(exec_datafusion_err!(
                "{}",
                reader.format_exception(&format!("Unrecognized command: {directive}"))
            ))
        }
    }

    async fn process(
        &self,
        ctx: &SessionContext,
        bench: &mut SqlBenchmark,
        reader: &mut BenchmarkFileReader,
        line: &mut String,
        splits: &[&str],
    ) -> Result<()> {
        trace!("-- handling {}", splits[0]);

        match self {
            BenchmarkDirective::Load
            | BenchmarkDirective::Run
            | BenchmarkDirective::Init
            | BenchmarkDirective::Cleanup => {
                Self::process_query_directive(bench, reader, line, splits)
            }
            BenchmarkDirective::Name => Self::process_metadata_value(
                bench,
                reader,
                line,
                "name",
                "BENCH_NAME",
                "name must be followed by a value",
            ),
            BenchmarkDirective::Group => Self::process_metadata_value(
                bench,
                reader,
                line,
                "group",
                "BENCH_GROUP",
                "group must be followed by a value",
            ),
            BenchmarkDirective::Subgroup => Self::process_metadata_value(
                bench,
                reader,
                line,
                "subgroup",
                "BENCH_SUBGROUP",
                "subgroup must be followed by a value",
            ),
            BenchmarkDirective::Expect => Self::process_expect(bench, reader, splits),
            BenchmarkDirective::Assert => {
                Self::process_assert(bench, reader, line, splits)
            }
            BenchmarkDirective::ResultQuery => {
                Self::process_result_query(bench, reader, line, splits)
            }
            BenchmarkDirective::Results => Self::process_results(bench, reader, splits),
            BenchmarkDirective::Template => {
                Self::process_template(ctx, bench, reader, line, splits).await
            }
            BenchmarkDirective::Include => {
                Self::process_include(ctx, bench, reader, splits).await
            }
            BenchmarkDirective::Echo => Self::process_echo(bench, reader, splits),
        }
    }

    fn process_query_directive(
        bench: &mut SqlBenchmark,
        reader: &mut BenchmarkFileReader,
        line: &mut String,
        splits: &[&str],
    ) -> Result<()> {
        let directive = QueryDirective::parse(splits[0]).ok_or_else(|| {
            exec_datafusion_err!("Invalid query directive: {}", splits[0])
        })?;

        if directive == QueryDirective::Run && bench.queries.contains_key(&directive) {
            return Err(exec_datafusion_err!(
                "Multiple calls to run in the same benchmark file"
            ));
        }

        line.clear();

        // Read the query body until a blank line or EOF.
        let mut query = String::new();
        let mut reader_result = reader.read_line(line);

        loop {
            match reader_result {
                Some(Ok(_)) => {
                    if is_comment_line(line) {
                        // comment, ignore
                    } else if is_blank_line(line) {
                        break;
                    } else {
                        query.push_str(line);
                        query.push('\n');
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
            if !query.trim().is_empty() {
                return Err(exec_datafusion_err!(
                    "{}",
                    reader.format_exception(&format!(
                        "{} directive must use either a query file or inline SQL, not both",
                        directive.as_str()
                    ))
                ));
            }

            debug!("Processing {} file: {}", splits[0], splits[1]);

            let query_file = fs::read_to_string(splits[1]).map_err(|e| {
                exec_datafusion_err!("Failed to read query file {}: {e}", splits[1])
            })?;
            let query_file = query_file.replace("\r\n", "\n");

            // some files have multiple queries, split apart
            for query in split_query_statements(&query_file) {
                bench.process_query(splits, query.to_string())?;
            }
        } else if directive == QueryDirective::Run {
            for query in split_query_statements(&query) {
                bench.process_query(splits, query.to_string())?;
            }
        } else {
            bench.process_query(splits, query)?;
        }

        Ok(())
    }

    fn process_metadata_value(
        bench: &mut SqlBenchmark,
        reader: &mut BenchmarkFileReader,
        line: &str,
        directive: &str,
        replacement_key: &str,
        message: &str,
    ) -> Result<()> {
        let value =
            directive_value(reader, line.trim_start(), directive, message)?.to_string();

        match directive {
            "name" => bench.name.clone_from(&value),
            "group" => bench.group.clone_from(&value),
            "subgroup" => bench.subgroup.clone_from(&value),
            _ => unreachable!("unsupported metadata directive: {directive}"),
        }

        insert_replacement(
            &mut bench.replacement_mapping,
            replacement_key,
            value.clone(),
        );
        insert_replacement(&mut reader.replacements, replacement_key, value);

        Ok(())
    }

    fn process_expect(
        bench: &mut SqlBenchmark,
        reader: &BenchmarkFileReader,
        splits: &[&str],
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

    fn process_assert(
        bench: &mut SqlBenchmark,
        reader: &mut BenchmarkFileReader,
        line: &mut String,
        splits: &[&str],
    ) -> Result<()> {
        // count the amount of columns based on character count. The actual
        // character used is irrelevant.
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
                    if line.trim() == "----" {
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

    fn process_results(
        bench: &mut SqlBenchmark,
        reader: &BenchmarkFileReader,
        splits: &[&str],
    ) -> Result<()> {
        if splits.len() <= 1 || splits[1].is_empty() {
            return Err(exec_datafusion_err!(
                "{}",
                reader.format_exception(
                    "result must be followed by a path to a result file"
                )
            ));
        }

        if !bench.result_queries.is_empty() {
            return Err(exec_datafusion_err!(
                "{}",
                reader.format_exception("multiple results found")
            ));
        }

        let path = process_replacements(splits[1], &bench.replacement_mapping)?;

        bench.result_queries.push(BenchmarkQuery {
            path: Some(path),
            query: String::new(),
            column_count: 0,
            expected_result: vec![],
        });

        Ok(())
    }

    fn process_result_query(
        bench: &mut SqlBenchmark,
        reader: &mut BenchmarkFileReader,
        line: &mut String,
        splits: &[&str],
    ) -> Result<()> {
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
            return Err(exec_datafusion_err!(
                "{}",
                reader.format_exception(
                    "result_query must be followed by a query and a result (separated by ----)"
                )
            ));
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

    async fn process_template(
        ctx: &SessionContext,
        bench: &mut SqlBenchmark,
        reader: &mut BenchmarkFileReader,
        line: &mut String,
        splits: &[&str],
    ) -> Result<()> {
        if splits.len() != 2 || splits[1].is_empty() {
            return Err(exec_datafusion_err!(
                "{}",
                reader.format_exception("template requires a single template path")
            ));
        }

        // template: update the path to read
        bench.benchmark_path = PathBuf::from(splits[1]);

        line.clear();

        // now read parameters
        let mut reader_result = reader.read_line(line);

        loop {
            match reader_result {
                Some(Ok(_)) => {
                    if is_comment_line(line) {
                        // Clear the line buffer for the next iteration.
                        line.clear();
                        reader_result = reader.read_line(line);
                        continue;
                    }
                    if is_blank_line(line) {
                        break;
                    }

                    let Some((key, value)) = line.trim_start().split_once('=') else {
                        return Err(exec_datafusion_err!(
                            "{}",
                            reader.format_exception(
                                "Expected a template parameter in the form of X=Y"
                            )
                        ));
                    };
                    insert_replacement(
                        &mut bench.replacement_mapping,
                        key.trim(),
                        value.trim().to_string(),
                    );
                }
                Some(Err(e)) => return Err(e),
                None => break,
            }

            // Clear the line buffer for the next iteration.
            line.clear();
            reader_result = reader.read_line(line);
        }

        // restart the load from the template file
        Box::pin(bench.process_file(ctx, Path::new(splits[1]))).await
    }

    async fn process_include(
        ctx: &SessionContext,
        bench: &mut SqlBenchmark,
        reader: &BenchmarkFileReader,
        splits: &[&str],
    ) -> Result<()> {
        if splits.len() != 2 || splits[1].is_empty() {
            return Err(exec_datafusion_err!(
                "{}",
                reader.format_exception("include requires a single argument")
            ));
        }

        Box::pin(bench.process_file(ctx, Path::new(splits[1]))).await
    }

    fn process_echo(
        bench: &mut SqlBenchmark,
        reader: &BenchmarkFileReader,
        splits: &[&str],
    ) -> Result<()> {
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

    /// Read the next line, applying replacements and removing line terminators.
    fn read_line(&mut self, line: &mut String) -> Option<Result<()>> {
        match self.reader.read_line(line) {
            Ok(0) => None,
            Ok(_) => {
                self.line_nr += 1;

                // Trim newline and carriage return without changing other content.
                let trimmed_len = line.trim_end_matches(['\n', '\r']).len();
                line.truncate(trimmed_len);

                match process_replacements(line, &self.replacements) {
                    Ok(l) => {
                        *line = l;
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

// ---- utility function below

fn directive_value<'a>(
    reader: &BenchmarkFileReader,
    line: &'a str,
    directive: &str,
    message: &str,
) -> Result<&'a str> {
    let value = line
        .get(..directive.len())
        .filter(|prefix| prefix.eq_ignore_ascii_case(directive))
        .and_then(|_| line.get(directive.len()..))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| exec_datafusion_err!("{}", reader.format_exception(message)))?;

    Ok(value)
}

fn parse_group_from_path(path: &Path, benchmark_directory: &Path) -> String {
    let mut group_name = String::new();
    let mut parent = path.parent();

    while let Some(p) = parent {
        if path_ends_with_ignore_ascii_case(p, benchmark_directory) {
            break;
        }

        if let Some(dir_name) = p.file_name() {
            group_name = dir_name.to_string_lossy().into_owned();
        }

        parent = p.parent();
    }

    if group_name.is_empty() {
        warn!("Unable to find group name in path: {}", path.display());
    }

    group_name
}

fn path_ends_with_ignore_ascii_case(path: &Path, suffix: &Path) -> bool {
    let mut path_components = path.components().rev();

    for suffix_component in suffix.components().rev() {
        let Some(path_component) = path_components.next() else {
            return false;
        };

        if !path_component
            .as_os_str()
            .to_string_lossy()
            .eq_ignore_ascii_case(&suffix_component.as_os_str().to_string_lossy())
        {
            return false;
        }
    }

    true
}

fn starts_with_ignore_ascii_case(input: &str, prefix: &str) -> bool {
    input
        .get(..prefix.len())
        .is_some_and(|value| value.eq_ignore_ascii_case(prefix))
}

fn split_query_statements(sql: &str) -> impl Iterator<Item = &str> {
    sql.split("\n\n")
        .flat_map(|query| {
            query
                .split_inclusive(";\n")
                .map(|part| part.trim_end_matches('\n'))
        })
        .filter(|query| !query.trim().is_empty())
}

fn is_blank_line(line: &str) -> bool {
    line.trim().is_empty()
}

fn is_comment_line(line: &str) -> bool {
    let line = line.trim_start();
    line.starts_with('#') || line.starts_with("--")
}

fn is_blank_or_comment_line(line: &str) -> bool {
    is_blank_line(line) || is_comment_line(line)
}

fn insert_replacement(
    replacement_map: &mut HashMap<String, String>,
    key: &str,
    value: String,
) {
    replacement_map.insert(key.to_lowercase(), value);
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

static TRUE_FALSE_REPLACEMENT_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\$\{(\w+)(?::-([^|}]+))?\|([^|]+)\|([^}]+)}")
        .expect("Regex failed to compile")
});

static VARIABLE_REPLACEMENT_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\$\{(\w+)(?::-([^}]+))?}").expect("Regex failed to compile")
});

/// Replace all `${KEY}` or `${KEY:-default}` placeholders in a string according to the mapping.
/// Also handles `${KEY:-default|True value|false value}` syntax.
fn process_replacements(
    input: &str,
    replacement_map: &HashMap<String, String>,
) -> Result<String> {
    process_replacements_with_env(input, replacement_map, |key| std::env::var(key).ok())
}

fn process_replacements_with_env(
    input: &str,
    replacement_map: &HashMap<String, String>,
    get_env: impl Fn(&str) -> Option<String>,
) -> Result<String> {
    debug!("processing replacements for line '{input}'");

    // handle ${VAR:-default|true value|false value} syntax
    let replacement = |caps: &regex::Captures| -> Result<String> {
        let key = &caps[1];
        let default = caps.get(2).map(|m| m.as_str().to_string());
        let true_val = &caps[3];
        let false_val = &caps[4];

        let value = lookup_replacement_value(key, replacement_map, &get_env).or(default);

        match value {
            Some(v) if v.eq_ignore_ascii_case("true") => Ok(true_val.to_string()),
            Some(_) => Ok(false_val.to_string()),
            None => Err(exec_datafusion_err!("Missing value for key '{key}'")),
        }
    };
    let input = replace_all(&TRUE_FALSE_REPLACEMENT_RE, input, replacement)?;

    // handle ${KEY} and ${KEY:-default}`
    let replacement = |caps: &regex::Captures| -> Result<String> {
        let key = &caps[1];
        let default = caps.get(2);

        if let Some(v) = lookup_replacement_value(key, replacement_map, &get_env) {
            return Ok(v.to_string());
        }

        // use default if it was set
        if let Some(def) = default {
            Ok(def.as_str().to_string())
        } else {
            Err(exec_datafusion_err!("Missing value for key '{key}'"))
        }
    };

    replace_all(&VARIABLE_REPLACEMENT_RE, &input, replacement)
}

fn lookup_replacement_value(
    key: &str,
    replacement_map: &HashMap<String, String>,
    get_env: &impl Fn(&str) -> Option<String>,
) -> Option<String> {
    if let Some(v) = replacement_map.get(&key.to_lowercase()) {
        return Some(v.to_string());
    }

    // look in env variables
    get_env(&key.to_uppercase())
}

fn read_query_from_reader(
    reader: &mut BenchmarkFileReader,
    sql: &str,
    header: &str,
) -> Result<BenchmarkQuery> {
    let column_count = header.len();
    let mut expected_result = vec![];
    let mut line = String::new();
    let mut reader_result = reader.read_line(&mut line);

    loop {
        match reader_result {
            Some(Ok(_)) => {
                if is_comment_line(&line) {
                    // comment, ignore
                } else if is_blank_line(&line) {
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
    path: impl AsRef<Path>,
    replacement_mapping: &HashMap<String, String>,
) -> Result<BenchmarkQuery> {
    // Process replacements in file path
    let path = path.as_ref().to_string_lossy();
    let path = process_replacements(&path, replacement_mapping)?;
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

    if column_count == 0 {
        return Err(exec_datafusion_err!(
            "Result file {path} did not contain any columns"
        ));
    }

    // Execute and collect results
    let batches = df.collect().await?;
    // Convert record batches to string vectors
    let expected_result = format_record_batches(&batches)?;

    Ok(BenchmarkQuery {
        path: Some(path),
        query: String::new(),
        column_count,
        expected_result,
    })
}

fn format_record_batches(
    batches: &[RecordBatch],
) -> Result<Vec<Vec<String>>, DataFusionError> {
    let mut expected_result = vec![];
    let arrow_format_options = FormatOptions::default()
        .with_null("NULL")
        .with_display_error(true);

    for batch in batches {
        let schema = batch.schema_ref();

        let formatters = batch
            .columns()
            .iter()
            .zip(schema.fields().iter())
            .map(|(c, field)| make_array_formatter(c, &arrow_format_options, Some(field)))
            .collect::<Result<Vec<_>, ArrowError>>()?;

        for row in 0..batch.num_rows() {
            let mut cells = vec![];
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
) -> Result<ArrayFormatter<'a>, ArrowError> {
    match options.formatter_factory() {
        None => ArrayFormatter::try_new(array, options),
        Some(formatters) => formatters
            .create_array_formatter(array, options, field)
            .transpose()
            .unwrap_or_else(|| ArrayFormatter::try_new(array, options)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use std::fs;
    use std::path::{Path, PathBuf};
    use tempfile::{TempDir, tempdir};

    fn write_test_file(temp_dir: &TempDir, name: &str, contents: &str) -> PathBuf {
        let path = temp_dir.path().join(name);
        fs::write(&path, contents).expect("failed to write benchmark test file");
        path
    }

    async fn parse_benchmark_file(path: &Path) -> Result<SqlBenchmark> {
        let ctx = SessionContext::new();
        let path_string = path.to_string_lossy().into_owned();
        SqlBenchmark::new(&ctx, &path_string, "/tmp").await
    }

    async fn parse_benchmark(contents: &str) -> Result<SqlBenchmark> {
        let temp_dir = tempdir().expect("failed to create benchmark test directory");
        let path = write_test_file(&temp_dir, "parser.benchmark", contents);

        parse_benchmark_file(&path).await
    }

    async fn assert_parse_error(contents: &str, expected_message: &str) {
        let error = parse_benchmark(contents)
            .await
            .expect_err("benchmark parsing should fail");

        let message = error.to_string();
        assert!(
            message.contains(expected_message),
            "expected error containing {expected_message:?}, got {message:?}"
        );
    }

    fn assert_result_error_contains<T: Debug>(result: Result<T>, expected_message: &str) {
        let error = result.expect_err("operation should fail");
        let message = error.to_string();
        assert!(
            message.contains(expected_message),
            "expected error containing {expected_message:?}, got {message:?}"
        );
    }

    fn formatted_last_results(benchmark: &SqlBenchmark) -> Vec<Vec<String>> {
        format_record_batches(
            benchmark
                .last_results
                .as_ref()
                .expect("last results should be set"),
        )
        .expect("results should format")
    }

    fn read_all_files_in_dir(path: &Path) -> String {
        let mut entries = fs::read_dir(path)
            .expect("directory should be readable")
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.is_file())
            .collect::<Vec<_>>();
        entries.sort();

        let mut contents = String::new();
        for path in entries {
            contents
                .push_str(&fs::read_to_string(path).expect("file should be readable"));
        }
        contents
    }

    fn replacement_map(entries: &[(&str, &str)]) -> HashMap<String, String> {
        let mut replacements = HashMap::new();
        for (key, value) in entries {
            insert_replacement(&mut replacements, key, value.to_string());
        }
        replacements
    }

    fn env_map(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    // Replacement tests cover benchmark variable expansion syntax.

    #[test]
    fn process_replacements_replaces_map_values_case_insensitively() {
        let replacements = replacement_map(&[
            ("BENCH_NAME", "tpch"),
            ("QUERY_NUMBER_PADDED", "01"),
            ("format_1", "parquet"),
        ]);

        let actual = process_replacements_with_env(
            "${bench_name}/q${query_number_padded}.${FORMAT_1}",
            &replacements,
            |_| None,
        )
        .expect("replacement should succeed");

        assert_eq!(actual, "tpch/q01.parquet");
    }

    #[test]
    fn process_replacements_uses_env_when_map_value_is_missing() {
        let replacements = HashMap::new();
        let env = env_map(&[("DATA_DIR", "/tmp/data")]);

        let actual = process_replacements_with_env(
            "${data_dir}/lineitem.parquet",
            &replacements,
            |key| env.get(key).cloned(),
        )
        .expect("replacement should succeed");

        assert_eq!(actual, "/tmp/data/lineitem.parquet");
    }

    #[test]
    fn process_replacements_prefers_map_over_env() {
        let replacements = replacement_map(&[("BENCH_SIZE", "10")]);
        let env = env_map(&[("BENCH_SIZE", "100")]);

        let actual =
            process_replacements_with_env("sf${BENCH_SIZE}", &replacements, |key| {
                env.get(key).cloned()
            })
            .expect("replacement should succeed");

        assert_eq!(actual, "sf10");
    }

    #[test]
    fn process_replacements_uses_default_for_missing_variable() {
        let replacements = HashMap::new();

        let actual = process_replacements_with_env(
            "load_${BENCH_SUBGROUP:-groupby}_${FILE_TYPE:-csv}.sql",
            &replacements,
            |_| None,
        )
        .expect("replacement should succeed");

        assert_eq!(actual, "load_groupby_csv.sql");
    }

    #[test]
    fn process_replacements_reports_missing_variable_without_default() {
        let replacements = HashMap::new();

        let error = process_replacements_with_env("${MISSING}", &replacements, |_| None)
            .expect_err("replacement should fail");

        assert!(
            error
                .to_string()
                .contains("Missing value for key 'MISSING'"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn process_replacements_applies_true_false_true_branch() {
        let replacements = HashMap::new();
        let env = env_map(&[("USE_PARQUET", "TrUe")]);

        let actual = process_replacements_with_env(
            "load_${USE_PARQUET:-false|parquet|csv}.sql",
            &replacements,
            |key| env.get(key).cloned(),
        )
        .expect("replacement should succeed");

        assert_eq!(actual, "load_parquet.sql");
    }

    #[test]
    fn process_replacements_applies_true_false_false_branch() {
        let replacements = HashMap::new();
        let env = env_map(&[("USE_PARQUET", "false")]);

        let actual = process_replacements_with_env(
            "load_${USE_PARQUET:-true|parquet|csv}.sql",
            &replacements,
            |key| env.get(key).cloned(),
        )
        .expect("replacement should succeed");

        assert_eq!(actual, "load_csv.sql");
    }

    #[test]
    fn process_replacements_uses_map_for_true_false_branch() {
        let replacements = replacement_map(&[("USE_PARQUET", "true")]);

        let actual = process_replacements_with_env(
            "load_${USE_PARQUET:-false|parquet|csv}.sql",
            &replacements,
            |_| None,
        )
        .expect("replacement should succeed");

        assert_eq!(actual, "load_parquet.sql");
    }

    #[test]
    fn process_replacements_prefers_map_over_env_for_true_false_branch() {
        let replacements = replacement_map(&[("USE_PARQUET", "false")]);
        let env = env_map(&[("USE_PARQUET", "true")]);

        let actual = process_replacements_with_env(
            "load_${USE_PARQUET:-true|parquet|csv}.sql",
            &replacements,
            |key| env.get(key).cloned(),
        )
        .expect("replacement should succeed");

        assert_eq!(actual, "load_csv.sql");
    }

    #[test]
    fn process_replacements_uses_true_false_default_for_missing_true_value() {
        let replacements = HashMap::new();

        let actual = process_replacements_with_env(
            "load_${USE_PARQUET:-true|parquet|csv}.sql",
            &replacements,
            |_| None,
        )
        .expect("replacement should succeed");

        assert_eq!(actual, "load_parquet.sql");
    }

    #[test]
    fn process_replacements_uses_true_false_default_for_missing_false_value() {
        let replacements = HashMap::new();

        let actual = process_replacements_with_env(
            "load_${USE_PARQUET:-false|parquet|csv}.sql",
            &replacements,
            |_| None,
        )
        .expect("replacement should succeed");

        assert_eq!(actual, "load_csv.sql");
    }

    #[test]
    fn process_replacements_reports_missing_true_false_variable_without_default() {
        let replacements = HashMap::new();

        let error = process_replacements_with_env(
            "load_${USE_PARQUET|parquet|csv}.sql",
            &replacements,
            |_| None,
        )
        .expect_err("replacement should fail");

        assert!(
            error
                .to_string()
                .contains("Missing value for key 'USE_PARQUET'"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn process_replacements_resolves_variables_after_true_false_replacement() {
        let replacements = replacement_map(&[("FILE_TYPE", "parquet")]);
        let env = env_map(&[("USE_TYPED_PATH", "true")]);

        let actual = process_replacements_with_env(
            "${USE_TYPED_PATH:-false|data.${FILE_TYPE}|data.csv}",
            &replacements,
            |key| env.get(key).cloned(),
        )
        .expect("replacement should succeed");

        assert_eq!(actual, "data.parquet");
    }

    #[test]
    fn process_replacements_leaves_unsupported_placeholder_syntax_unchanged() {
        let replacements = HashMap::new();

        let actual =
            process_replacements_with_env("${BAD-KEY:-fallback}", &replacements, |_| {
                None
            })
            .expect("unsupported placeholder should not match replacement regex");

        assert_eq!(actual, "${BAD-KEY:-fallback}");
    }

    // Parser tests cover benchmark directives and parse-time validation.

    #[tokio::test]
    async fn parser_accepts_metadata_expect_echo_and_sql_sections() {
        let benchmark = parse_benchmark(
            r#"
# top-level comments are ignored
name Parser Success
group Parser Group
subgroup Parser Subgroup
expect_plan ProjectionExec with details
echo hello from parser

load
-- query comments are ignored
CREATE TABLE t AS VALUES (1);

init
CREATE VIEW v AS SELECT * FROM t;

run
SELECT * FROM v;

cleanup
DROP VIEW v;
"#,
        )
        .await
        .expect("benchmark should parse");

        assert_eq!(benchmark.name(), "Parser Success");
        assert_eq!(benchmark.group(), "Parser Group");
        assert_eq!(benchmark.subgroup(), "Parser Subgroup");
        assert_eq!(benchmark.expect, vec!["ProjectionExec with details"]);
        assert_eq!(benchmark.echo, vec!["hello from parser"]);
        assert_eq!(
            benchmark
                .queries()
                .get(&QueryDirective::Load)
                .expect("load query"),
            &vec!["CREATE TABLE t AS VALUES (1);".to_string()]
        );
        assert_eq!(
            benchmark
                .queries()
                .get(&QueryDirective::Init)
                .expect("init query"),
            &vec!["CREATE VIEW v AS SELECT * FROM t;".to_string()]
        );
        assert_eq!(
            benchmark
                .queries()
                .get(&QueryDirective::Run)
                .expect("run query"),
            &vec!["SELECT * FROM v;".to_string()]
        );
        assert_eq!(
            benchmark
                .queries()
                .get(&QueryDirective::Cleanup)
                .expect("cleanup query"),
            &vec!["DROP VIEW v;".to_string()]
        );
    }

    #[tokio::test]
    async fn parser_splits_inline_run_block_on_semicolon_newline() {
        let benchmark = parse_benchmark(
            r#"
run
CREATE TABLE t AS SELECT 1 AS value;
SELECT value + 1 AS value FROM t;
DROP TABLE t;
"#,
        )
        .await
        .expect("benchmark should parse");

        assert_eq!(
            benchmark
                .queries()
                .get(&QueryDirective::Run)
                .expect("run query"),
            &vec![
                "CREATE TABLE t AS SELECT 1 AS value;".to_string(),
                "SELECT value + 1 AS value FROM t;".to_string(),
                "DROP TABLE t;".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn parser_accepts_assert_with_expected_rows() {
        let benchmark = parse_benchmark(
            r#"
assert II
select 1, 'one'
----
1|one
2	two
"#,
        )
        .await
        .expect("benchmark should parse");

        let query = benchmark
            .assert_queries()
            .first()
            .expect("assert query should be parsed");

        assert_eq!(query.column_count, 2);
        assert!(query.query.contains("select 1, 'one'"));
        assert_eq!(
            query.expected_result,
            vec![
                vec!["1".to_string(), "one".to_string()],
                vec!["2".to_string(), "two".to_string()]
            ]
        );
    }

    #[tokio::test]
    async fn parser_accepts_result_query_with_expected_rows() {
        let benchmark = parse_benchmark(
            r#"
result_query II
select 1, 'one'
----
1|one
NULL|(empty)
"#,
        )
        .await
        .expect("benchmark should parse");

        let query = benchmark
            .result_queries()
            .first()
            .expect("result query should be parsed");

        assert_eq!(query.path, None);
        assert_eq!(query.column_count, 2);
        assert!(query.query.contains("select 1, 'one'"));
        assert_eq!(
            query.expected_result,
            vec![
                vec!["1".to_string(), "one".to_string()],
                vec!["NULL".to_string(), "(empty)".to_string()]
            ]
        );
    }

    #[tokio::test]
    async fn parser_records_result_file_without_parsing_contents() {
        let temp_dir = tempdir().expect("failed to create benchmark test directory");
        let result_path =
            write_test_file(&temp_dir, "result.csv", "col_a|col_b\n1|one\nNULL|two\n");
        let benchmark_path = write_test_file(
            &temp_dir,
            "result.benchmark",
            &format!("result {}\n", result_path.display()),
        );

        let benchmark = parse_benchmark_file(&benchmark_path)
            .await
            .expect("benchmark should parse");

        let query = benchmark
            .result_queries()
            .first()
            .expect("result file should be parsed");

        assert_eq!(query.path, Some(result_path.to_string_lossy().into_owned()));
        assert_eq!(query.column_count, 0);
        assert!(query.expected_result.is_empty());
    }

    #[tokio::test]
    async fn parser_accepts_include_file() {
        let temp_dir = tempdir().expect("failed to create benchmark test directory");
        let include_path =
            write_test_file(&temp_dir, "include.benchmark", "run\nselect 1\n");

        let benchmark_path = write_test_file(
            &temp_dir,
            "include_driver.benchmark",
            &format!("include {}\n", include_path.display()),
        );

        let result = parse_benchmark_file(&benchmark_path).await;

        let benchmark = result.expect("benchmark should parse");
        assert_eq!(
            benchmark
                .queries()
                .get(&QueryDirective::Run)
                .expect("run query"),
            &vec!["select 1".to_string()]
        );
    }

    #[tokio::test]
    async fn parser_accepts_template_file_with_parameters() {
        let temp_dir = tempdir().expect("failed to create benchmark test directory");
        let template_path = write_test_file(
            &temp_dir,
            "template_success.benchmark",
            "# template comments are ignored\nrun\n-- query comments are ignored\nselect '${TABLE_NAME}', '${BENCHMARK_DIR}'\n",
        );

        let benchmark_path = write_test_file(
            &temp_dir,
            "template_success_driver.benchmark",
            &format!(
                "template {}\n# parameter comments are ignored\nTABLE_NAME=orders\n",
                template_path.display()
            ),
        );

        let result = parse_benchmark_file(&benchmark_path).await;

        let benchmark = result.expect("benchmark should parse");
        assert_eq!(benchmark.benchmark_path(), template_path.as_path());
        assert_eq!(
            benchmark
                .queries()
                .get(&QueryDirective::Run)
                .expect("run query"),
            &vec!["select 'orders', '/tmp'".to_string()]
        );
    }

    #[tokio::test]
    async fn parser_trims_template_parameter_keys_and_values() {
        let temp_dir = tempdir().expect("failed to create benchmark test directory");
        let template_path = write_test_file(
            &temp_dir,
            "template_trim.benchmark",
            "run\nselect '${TABLE_NAME}'\n",
        );

        let benchmark_path = write_test_file(
            &temp_dir,
            "template_trim_driver.benchmark",
            &format!(
                "template {}\n  TABLE_NAME = orders  \n",
                template_path.display()
            ),
        );

        let benchmark = parse_benchmark_file(&benchmark_path)
            .await
            .expect("benchmark should parse");

        assert_eq!(
            benchmark
                .queries()
                .get(&QueryDirective::Run)
                .expect("run query"),
            &vec!["select 'orders'".to_string()]
        );
        assert_eq!(
            benchmark.replacement_mapping().get("table_name"),
            Some(&"orders".to_string())
        );
    }

    #[tokio::test]
    async fn parser_preserves_expected_result_cell_whitespace() {
        let benchmark = parse_benchmark("assert I\nselect '  x  '\n----\n  x  \n")
            .await
            .expect("benchmark should parse");

        let query = benchmark
            .assert_queries()
            .first()
            .expect("assert query should be parsed");

        assert_eq!(query.expected_result, vec![vec!["  x  ".to_string()]]);
    }

    #[tokio::test]
    async fn parser_accepts_indented_comments_and_blank_lines() {
        let benchmark =
            parse_benchmark("  # comment\n  -- comment\n  run\n  select 1\n   \n")
                .await
                .expect("benchmark should parse");

        assert_eq!(
            benchmark
                .queries()
                .get(&QueryDirective::Run)
                .expect("run query"),
            &vec!["select 1".to_string()]
        );
    }

    #[tokio::test]
    async fn parser_accepts_case_insensitive_query_directives() {
        let benchmark = parse_benchmark("RUN\nselect 1\n")
            .await
            .expect("benchmark should parse");

        assert_eq!(
            benchmark
                .queries()
                .get(&QueryDirective::Run)
                .expect("run query"),
            &vec!["select 1".to_string()]
        );
    }

    #[tokio::test]
    async fn parser_accepts_query_file_and_splits_statements() {
        let temp_dir = tempdir().expect("failed to create benchmark test directory");
        let query_path = write_test_file(
            &temp_dir,
            "queries.sql",
            "-- leading comment\nSELECT 1 AS value;\nSELECT 2 AS value;\n\n# another comment\nWITH t AS (SELECT 3 AS value) SELECT * FROM t;\n",
        );
        let benchmark_path = write_test_file(
            &temp_dir,
            "query_file.benchmark",
            &format!("run {}\n", query_path.display()),
        );

        let benchmark = parse_benchmark_file(&benchmark_path)
            .await
            .expect("benchmark should parse");

        assert_eq!(
            benchmark
                .queries()
                .get(&QueryDirective::Run)
                .expect("run queries"),
            &vec![
                "SELECT 1 AS value;".to_string(),
                "SELECT 2 AS value;".to_string(),
                "WITH t AS (SELECT 3 AS value) SELECT * FROM t;".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn parser_accepts_replacements_in_query_file_path() {
        let temp_dir = tempdir().expect("failed to create benchmark test directory");
        let query_path =
            write_test_file(&temp_dir, "queries.sql", "SELECT 5 AS value;\n");
        let template_path = write_test_file(
            &temp_dir,
            "query_file_path_template.benchmark",
            "run ${QUERY_PATH}\n",
        );
        let benchmark_path = write_test_file(
            &temp_dir,
            "query_file_path_driver.benchmark",
            &format!(
                "template {}\nQUERY_PATH={}\n",
                template_path.display(),
                query_path.display()
            ),
        );

        let benchmark = parse_benchmark_file(&benchmark_path)
            .await
            .expect("benchmark should parse");

        assert_eq!(
            benchmark
                .queries()
                .get(&QueryDirective::Run)
                .expect("run query"),
            &vec!["SELECT 5 AS value;".to_string()]
        );
    }

    #[tokio::test]
    async fn parser_rejects_inline_sql_when_query_file_is_provided() {
        let temp_dir = tempdir().expect("failed to create benchmark test directory");
        let query_path =
            write_test_file(&temp_dir, "queries.sql", "SELECT 1 AS value;\n");
        let benchmark_path = write_test_file(
            &temp_dir,
            "query_file_with_inline_body.benchmark",
            &format!("run {}\nSELECT 999 AS value;\n", query_path.display()),
        );

        let result = parse_benchmark_file(&benchmark_path).await;

        assert_result_error_contains(
            result,
            "run directive must use either a query file or inline SQL, not both",
        );
    }

    #[tokio::test]
    async fn parser_rejects_inline_sql_when_load_file_is_provided() {
        let temp_dir = tempdir().expect("failed to create benchmark test directory");
        let query_path = write_test_file(
            &temp_dir,
            "load.sql",
            "CREATE TABLE t AS SELECT 1 AS value;\n",
        );
        let benchmark_path = write_test_file(
            &temp_dir,
            "load_file_with_inline_body.benchmark",
            &format!(
                "load {}\nCREATE TABLE u AS SELECT 2 AS value;\n",
                query_path.display()
            ),
        );

        let result = parse_benchmark_file(&benchmark_path).await;

        assert_result_error_contains(
            result,
            "load directive must use either a query file or inline SQL, not both",
        );
    }

    #[tokio::test]
    async fn parser_rejects_inline_sql_when_init_file_is_provided() {
        let temp_dir = tempdir().expect("failed to create benchmark test directory");
        let query_path = write_test_file(
            &temp_dir,
            "init.sql",
            "CREATE VIEW v AS SELECT 1 AS value;\n",
        );
        let benchmark_path = write_test_file(
            &temp_dir,
            "init_file_with_inline_body.benchmark",
            &format!(
                "init {}\nCREATE VIEW w AS SELECT 2 AS value;\n",
                query_path.display()
            ),
        );

        let result = parse_benchmark_file(&benchmark_path).await;

        assert_result_error_contains(
            result,
            "init directive must use either a query file or inline SQL, not both",
        );
    }

    #[tokio::test]
    async fn parser_rejects_inline_sql_when_cleanup_file_is_provided() {
        let temp_dir = tempdir().expect("failed to create benchmark test directory");
        let query_path = write_test_file(&temp_dir, "cleanup.sql", "DROP TABLE t;\n");
        let benchmark_path = write_test_file(
            &temp_dir,
            "cleanup_file_with_inline_body.benchmark",
            &format!("cleanup {}\nDROP TABLE u;\n", query_path.display()),
        );

        let result = parse_benchmark_file(&benchmark_path).await;

        assert_result_error_contains(
            result,
            "cleanup directive must use either a query file or inline SQL, not both",
        );
    }

    #[tokio::test]
    async fn parser_ignores_query_file_with_only_comments_and_blank_lines() {
        let temp_dir = tempdir().expect("failed to create benchmark test directory");
        let query_path = write_test_file(
            &temp_dir,
            "queries.sql",
            "# comment\n\n-- another comment\n\n",
        );
        let benchmark_path = write_test_file(
            &temp_dir,
            "empty_query_file.benchmark",
            &format!("run {}\n", query_path.display()),
        );

        let benchmark = parse_benchmark_file(&benchmark_path)
            .await
            .expect("benchmark should parse");

        assert!(!benchmark.queries().contains_key(&QueryDirective::Run));
    }

    #[tokio::test]
    async fn parser_splits_query_file_with_windows_line_endings() {
        let temp_dir = tempdir().expect("failed to create benchmark test directory");
        let query_path = write_test_file(
            &temp_dir,
            "queries.sql",
            "SELECT 1 AS value;\r\nSELECT 2 AS value;\r\n",
        );
        let benchmark_path = write_test_file(
            &temp_dir,
            "windows_query_file.benchmark",
            &format!("run {}\n", query_path.display()),
        );

        let benchmark = parse_benchmark_file(&benchmark_path)
            .await
            .expect("benchmark should parse");

        assert_eq!(
            benchmark
                .queries()
                .get(&QueryDirective::Run)
                .expect("run queries"),
            &vec![
                "SELECT 1 AS value;".to_string(),
                "SELECT 2 AS value;".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn parser_rejects_unknown_command() {
        assert_parse_error("wat\n", "Unrecognized command: wat").await;
    }

    #[tokio::test]
    async fn parser_rejects_assert_without_column_count() {
        assert_parse_error(
            "assert\nselect 1\n----\n1\n",
            "assert must be followed by a column count",
        )
        .await;
    }

    #[tokio::test]
    async fn parser_rejects_assert_without_result_separator() {
        assert_parse_error(
            "assert I\nselect 1\n1\n",
            "assert must be followed by a query and a result (separated by ----)",
        )
        .await;
    }

    #[tokio::test]
    async fn parser_rejects_result_query_without_separator() {
        assert_parse_error(
            "result_query I\nselect 1\n1\n",
            "result_query must be followed by a query and a result (separated by ----)",
        )
        .await;
    }

    #[tokio::test]
    async fn parser_rejects_result_query_with_wrong_column_count() {
        assert_parse_error(
            "result_query II\nselect 1\n----\n1\n",
            "expected 2 values but got 1",
        )
        .await;
    }

    #[tokio::test]
    async fn parser_rejects_multiple_result_queries() {
        assert_parse_error(
            "result_query I\nselect 1\n----\n1\n\nresult_query I\nselect 2\n----\n2\n",
            "multiple results found",
        )
        .await;
    }

    #[tokio::test]
    async fn parser_rejects_duplicate_run_directives() {
        assert_parse_error("run\nselect 1\n\nrun\nselect 2\n", "Multiple calls to run")
            .await;
    }

    #[tokio::test]
    async fn parser_accepts_multiple_load_directives() {
        let benchmark = parse_benchmark(
            "load\nCREATE TABLE t AS SELECT 1;\n\nload\nCREATE TABLE u AS SELECT 2;\n",
        )
        .await
        .expect("benchmark should parse");

        assert_eq!(
            benchmark
                .queries()
                .get(&QueryDirective::Load)
                .expect("load queries"),
            &vec![
                "CREATE TABLE t AS SELECT 1;".to_string(),
                "CREATE TABLE u AS SELECT 2;".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn parser_accepts_multiple_init_directives() {
        let benchmark = parse_benchmark(
            "init\nCREATE VIEW v AS SELECT 1;\n\ninit\nCREATE VIEW w AS SELECT 2;\n",
        )
        .await;

        let benchmark = benchmark.expect("benchmark should parse");
        assert_eq!(
            benchmark
                .queries()
                .get(&QueryDirective::Init)
                .expect("init queries"),
            &vec![
                "CREATE VIEW v AS SELECT 1;".to_string(),
                "CREATE VIEW w AS SELECT 2;".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn parser_accepts_multiple_cleanup_directives() {
        let benchmark =
            parse_benchmark("cleanup\nDROP TABLE t;\n\ncleanup\nDROP TABLE u;\n")
                .await
                .expect("benchmark should parse");

        assert_eq!(
            benchmark
                .queries()
                .get(&QueryDirective::Cleanup)
                .expect("cleanup queries"),
            &vec!["DROP TABLE t;".to_string(), "DROP TABLE u;".to_string(),]
        );
    }

    #[tokio::test]
    async fn parser_rejects_missing_query_file() {
        let temp_dir = tempdir().expect("failed to create benchmark test directory");
        let missing_path = temp_dir.path().join("missing.sql");
        let benchmark_path = write_test_file(
            &temp_dir,
            "missing_query_file.benchmark",
            &format!("run {}\n", missing_path.display()),
        );

        let result = parse_benchmark_file(&benchmark_path).await;

        assert_result_error_contains(result, "Failed to read query file");
    }

    #[tokio::test]
    async fn parser_rejects_template_with_invalid_parameter_assignment() {
        let temp_dir = tempdir().expect("failed to create benchmark test directory");
        let template_path =
            write_test_file(&temp_dir, "template.benchmark", "run\nselect 1\n");

        let benchmark_path = write_test_file(
            &temp_dir,
            "template_driver.benchmark",
            &format!("template {}\nINVALID\n", template_path.display()),
        );

        let ctx = SessionContext::new();
        let benchmark_path_string = benchmark_path.to_string_lossy().into_owned();
        let result = SqlBenchmark::new(&ctx, &benchmark_path_string, "/tmp").await;

        let error = result.expect_err("benchmark parsing should fail");
        let message = error.to_string();
        assert!(
            message.contains("Expected a template parameter in the form of X=Y"),
            "expected template parameter error, got {message:?}"
        );
    }

    #[tokio::test]
    async fn parser_rejects_metadata_and_result_directives_without_values() {
        assert_parse_error("name\n", "name must be followed by a value").await;
        assert_parse_error("group\n", "group must be followed by a value").await;
        assert_parse_error("subgroup\n", "subgroup must be followed by a value").await;
        assert_parse_error(
            "expect_plan\n",
            "expect_plan must be followed by a string to search in the physical plan",
        )
        .await;
        assert_parse_error("echo\n", "Echo requires an argument").await;
        assert_parse_error(
            "result\n",
            "result must be followed by a path to a result file",
        )
        .await;
        assert_parse_error("include\n", "include requires a single argument").await;
        assert_parse_error("template\n", "template requires a single template path")
            .await;
    }

    #[tokio::test]
    async fn parser_rejects_include_and_template_with_too_many_arguments() {
        assert_parse_error("include a b\n", "include requires a single argument").await;
        assert_parse_error("template a b\n", "template requires a single template path")
            .await;
    }

    #[tokio::test]
    async fn parser_rejects_missing_include_file() {
        let temp_dir = tempdir().expect("failed to create benchmark test directory");
        let missing_path = temp_dir.path().join("missing_include.benchmark");
        let benchmark_path = write_test_file(
            &temp_dir,
            "missing_include_driver.benchmark",
            &format!("include {}\n", missing_path.display()),
        );

        let result = parse_benchmark_file(&benchmark_path).await;

        assert_result_error_contains(result, "No such file");
    }

    #[tokio::test]
    async fn parser_rejects_missing_template_file() {
        let temp_dir = tempdir().expect("failed to create benchmark test directory");
        let missing_path = temp_dir.path().join("missing_template.benchmark");
        let benchmark_path = write_test_file(
            &temp_dir,
            "missing_template_driver.benchmark",
            &format!("template {}\n", missing_path.display()),
        );

        let result = parse_benchmark_file(&benchmark_path).await;

        assert_result_error_contains(result, "No such file");
    }

    #[tokio::test]
    async fn parser_uses_metadata_values_as_replacements() {
        let benchmark = parse_benchmark(
            r#"
name Q01
group tpch
subgroup sf1

run
SELECT '${BENCH_NAME}', '${BENCH_GROUP}', '${BENCH_SUBGROUP}'
"#,
        )
        .await
        .expect("benchmark should parse");

        assert_eq!(
            benchmark
                .queries()
                .get(&QueryDirective::Run)
                .expect("run query"),
            &vec!["SELECT 'Q01', 'tpch', 'sf1'".to_string()]
        );
    }

    #[tokio::test]
    async fn parser_accepts_replacement_in_result_file_path() {
        let temp_dir = tempdir().expect("failed to create benchmark test directory");
        let result_path = write_test_file(&temp_dir, "result.csv", "value\n1\n");
        let template_path = write_test_file(
            &temp_dir,
            "result_path_template.benchmark",
            "result ${RESULT_PATH}\n",
        );
        let benchmark_path = write_test_file(
            &temp_dir,
            "result_path_driver.benchmark",
            &format!(
                "template {}\nRESULT_PATH={}\n",
                template_path.display(),
                result_path.display()
            ),
        );

        let benchmark = parse_benchmark_file(&benchmark_path)
            .await
            .expect("benchmark should parse");

        let query = benchmark
            .result_queries()
            .first()
            .expect("result query should be parsed");
        assert_eq!(query.path, Some(result_path.to_string_lossy().into_owned()));
        assert_eq!(query.column_count, 0);
        assert!(query.expected_result.is_empty());
    }

    #[tokio::test]
    async fn parser_rejects_missing_replacement_in_result_file_path() {
        assert_parse_error("result ${MISSING_RESULT_PATH}\n", "Missing value for key")
            .await;
    }

    #[tokio::test]
    async fn parser_accepts_missing_result_file() {
        let temp_dir = tempdir().expect("failed to create benchmark test directory");
        let missing_path = temp_dir.path().join("missing_result.csv");
        let benchmark_path = write_test_file(
            &temp_dir,
            "missing_result_file.benchmark",
            &format!("result {}\n", missing_path.display()),
        );

        let benchmark = parse_benchmark_file(&benchmark_path)
            .await
            .expect("benchmark should parse");

        let query = benchmark
            .result_queries()
            .first()
            .expect("result file should be parsed");
        assert_eq!(
            query.path,
            Some(missing_path.to_string_lossy().into_owned())
        );
        assert!(query.expected_result.is_empty());
    }

    #[tokio::test]
    async fn parser_accepts_malformed_result_file() {
        let temp_dir = tempdir().expect("failed to create benchmark test directory");
        let result_path = temp_dir.path().join("malformed_result.csv");
        fs::write(&result_path, [0xff]).expect("failed to write malformed result file");
        let benchmark_path = write_test_file(
            &temp_dir,
            "malformed_result_file.benchmark",
            &format!("result {}\n", result_path.display()),
        );

        let benchmark = parse_benchmark_file(&benchmark_path)
            .await
            .expect("benchmark should parse");

        let query = benchmark
            .result_queries()
            .first()
            .expect("result file should be parsed");
        assert_eq!(query.path, Some(result_path.to_string_lossy().into_owned()));
        assert!(query.expected_result.is_empty());
    }

    // Lifecycle tests cover initialization, assertions, and cleanup execution.

    #[tokio::test]
    async fn initialize_executes_load_before_init_and_is_idempotent() {
        let mut benchmark = parse_benchmark(
            r#"
load
CREATE TABLE t AS SELECT 1 AS value;

load
CREATE TABLE u AS SELECT value + 1 AS value FROM t;

init
CREATE TABLE v AS SELECT value + 1 AS value FROM u;

init
CREATE TABLE initialized AS SELECT value + 1 AS value FROM v;

run
SELECT value FROM initialized;
"#,
        )
        .await
        .expect("benchmark should parse");
        let ctx = SessionContext::new();

        benchmark
            .initialize(&ctx)
            .await
            .expect("initialize should succeed");
        benchmark
            .initialize(&ctx)
            .await
            .expect("second initialize should be a no-op");

        assert!(benchmark.is_loaded());

        let rows = ctx
            .sql("SELECT value FROM initialized")
            .await
            .expect("query should plan")
            .collect()
            .await
            .expect("query should run");

        assert_eq!(format_record_batches(&rows).unwrap(), vec![vec!["4"]]);
    }

    #[tokio::test]
    async fn initialize_rejects_benchmark_without_run_query() {
        let mut benchmark = parse_benchmark(
            r#"
load
CREATE TABLE t AS SELECT 1 AS value;
"#,
        )
        .await
        .expect("benchmark should parse");
        let ctx = SessionContext::new();

        assert_result_error_contains(
            benchmark.initialize(&ctx).await,
            "Invalid benchmark file: no \"run\" query specified",
        );
    }

    #[tokio::test]
    async fn initialize_propagates_load_query_failures() {
        let mut benchmark = parse_benchmark(
            r#"
load
CREATE TABLE t AS SELECT * FROM missing_load_table;

run
SELECT 1;
"#,
        )
        .await
        .expect("benchmark should parse");
        let ctx = SessionContext::new();

        assert_result_error_contains(
            benchmark.initialize(&ctx).await,
            "missing_load_table",
        );
    }

    #[tokio::test]
    async fn initialize_propagates_init_query_failures() {
        let mut benchmark = parse_benchmark(
            r#"
init
CREATE TABLE t AS SELECT * FROM missing_init_table;

run
SELECT 1;
"#,
        )
        .await
        .expect("benchmark should parse");
        let ctx = SessionContext::new();

        assert_result_error_contains(
            benchmark.initialize(&ctx).await,
            "missing_init_table",
        );
    }

    #[tokio::test]
    async fn cleanup_executes_cleanup_queries() {
        let mut benchmark = parse_benchmark(
            r#"
run
SELECT 1;

cleanup
CREATE TABLE cleanup_marker_a AS SELECT 7 AS value;

cleanup
CREATE TABLE cleanup_marker_b AS SELECT value + 1 AS value FROM cleanup_marker_a;
"#,
        )
        .await
        .expect("benchmark should parse");
        let ctx = SessionContext::new();

        benchmark.cleanup(&ctx).await.expect("cleanup should run");

        let rows = ctx
            .sql("SELECT value FROM cleanup_marker_b")
            .await
            .expect("query should plan")
            .collect()
            .await
            .expect("query should run");
        assert_eq!(format_record_batches(&rows).unwrap(), vec![vec!["8"]]);
    }

    #[tokio::test]
    async fn cleanup_propagates_query_failures() {
        let mut benchmark = parse_benchmark(
            r#"
run
SELECT 1;

cleanup
SELECT * FROM missing_cleanup_table;
"#,
        )
        .await
        .expect("benchmark should parse");
        let ctx = SessionContext::new();

        assert_result_error_contains(
            benchmark.cleanup(&ctx).await,
            "missing_cleanup_table",
        );
    }

    #[tokio::test]
    async fn assert_executes_assert_queries_successfully() {
        let mut benchmark = parse_benchmark(
            r#"
assert I
SELECT 1 AS value
----
1

run
SELECT 1;
"#,
        )
        .await
        .expect("benchmark should parse");
        let ctx = SessionContext::new();

        benchmark.assert(&ctx).await.expect("assert should pass");
    }

    #[tokio::test]
    async fn assert_accepts_null_expected_for_empty_actual() {
        let mut benchmark = parse_benchmark(
            r#"
assert I
SELECT '' AS value
----
NULL

run
SELECT 1;
"#,
        )
        .await
        .expect("benchmark should parse");
        let ctx = SessionContext::new();

        benchmark.assert(&ctx).await.expect("assert should pass");
    }

    #[tokio::test]
    async fn assert_accepts_empty_marker_for_empty_actual() {
        let mut benchmark = parse_benchmark(
            r#"
assert I
SELECT '' AS value
----
(empty)

run
SELECT 1;
"#,
        )
        .await
        .expect("benchmark should parse");
        let ctx = SessionContext::new();

        benchmark.assert(&ctx).await.expect("assert should pass");
    }

    #[tokio::test]
    async fn assert_accepts_empty_marker_for_null_actual() {
        let mut benchmark = parse_benchmark(
            r#"
assert I
SELECT CAST(NULL AS VARCHAR) AS value
----
(empty)

run
SELECT 1;
"#,
        )
        .await
        .expect("benchmark should parse");
        let ctx = SessionContext::new();

        benchmark.assert(&ctx).await.expect("assert should pass");
    }

    #[tokio::test]
    async fn assert_succeeds_with_zero_actual_and_expected_rows() {
        let mut benchmark = parse_benchmark(
            r#"
assert I
SELECT 1 AS value WHERE false
----

run
SELECT 1;
"#,
        )
        .await
        .expect("benchmark should parse");
        let ctx = SessionContext::new();

        benchmark.assert(&ctx).await.expect("assert should pass");
    }

    #[tokio::test]
    async fn assert_propagates_query_failures() {
        let mut benchmark = parse_benchmark(
            r#"
assert I
SELECT * FROM missing_assert_table
----
1

run
SELECT 1;
"#,
        )
        .await
        .expect("benchmark should parse");
        let ctx = SessionContext::new();

        assert_result_error_contains(
            benchmark.assert(&ctx).await,
            "missing_assert_table",
        );
    }

    #[tokio::test]
    async fn assert_reports_row_count_mismatch() {
        let mut benchmark = parse_benchmark(
            r#"
assert I
SELECT 1 AS value
----
1
2

run
SELECT 1;
"#,
        )
        .await
        .expect("benchmark should parse");
        let ctx = SessionContext::new();

        assert_result_error_contains(
            benchmark.assert(&ctx).await,
            "expected 2 rows but got 1",
        );
    }

    #[tokio::test]
    async fn assert_reports_column_count_mismatch() {
        let mut benchmark = parse_benchmark(
            r#"
assert I
SELECT 1 AS a, 2 AS b
----
1

run
SELECT 1;
"#,
        )
        .await
        .expect("benchmark should parse");
        let ctx = SessionContext::new();

        assert_result_error_contains(
            benchmark.assert(&ctx).await,
            "expected 1 columns but got 2",
        );
    }

    #[tokio::test]
    async fn assert_reports_value_mismatch() {
        let mut benchmark = parse_benchmark(
            r#"
assert I
SELECT 1 AS value
----
2

run
SELECT 1;
"#,
        )
        .await
        .expect("benchmark should parse");
        let ctx = SessionContext::new();

        assert_result_error_contains(
            benchmark.assert(&ctx).await,
            "expected value \"2\" but got value \"1\"",
        );
    }

    // Run tests cover result buffering and physical-plan expectations.

    #[tokio::test]
    async fn run_saves_uppercase_select_results() {
        let mut benchmark = parse_benchmark("run\nSELECT 1 AS value\n")
            .await
            .expect("benchmark should parse");
        let ctx = SessionContext::new();

        benchmark.run(&ctx, true).await.expect("run should succeed");

        assert_eq!(formatted_last_results(&benchmark), vec![vec!["1"]]);
    }

    #[tokio::test]
    async fn run_saves_with_query_results() {
        let mut benchmark =
            parse_benchmark("run\nWITH t AS (SELECT 3 AS value) SELECT value FROM t\n")
                .await
                .expect("benchmark should parse");
        let ctx = SessionContext::new();

        benchmark.run(&ctx, true).await.expect("run should succeed");

        assert_eq!(formatted_last_results(&benchmark), vec![vec!["3"]]);
    }

    #[tokio::test]
    async fn run_only_keeps_last_select_or_with_result() {
        let temp_dir = tempdir().expect("failed to create benchmark test directory");
        let query_path = write_test_file(
            &temp_dir,
            "queries.sql",
            "SELECT 1 AS value;\nSELECT 2 AS value;\nWITH t AS (SELECT 3 AS value) SELECT value FROM t;\n",
        );
        let benchmark_path = write_test_file(
            &temp_dir,
            "run_file.benchmark",
            &format!("run {}\n", query_path.display()),
        );
        let mut benchmark = parse_benchmark_file(&benchmark_path)
            .await
            .expect("benchmark should parse");
        let ctx = SessionContext::new();

        benchmark.run(&ctx, true).await.expect("run should succeed");

        assert_eq!(formatted_last_results(&benchmark), vec![vec!["3"]]);
    }

    #[tokio::test]
    async fn run_inline_multi_statement_only_keeps_last_select_or_with_result() {
        let mut benchmark = parse_benchmark(
            "run\nCREATE TABLE t AS SELECT 1 AS value;\nSELECT 2 AS value;\nWITH u AS (SELECT 3 AS value) SELECT value FROM u;\n",
        )
        .await
        .expect("benchmark should parse");
        let ctx = SessionContext::new();

        benchmark.run(&ctx, true).await.expect("run should succeed");

        assert_eq!(formatted_last_results(&benchmark), vec![vec!["3"]]);
    }

    #[tokio::test]
    async fn run_does_not_save_results_for_non_select_statement() {
        let mut benchmark =
            parse_benchmark("run\nCREATE TABLE run_created AS SELECT 1 AS value;\n")
                .await
                .expect("benchmark should parse");
        let ctx = SessionContext::new();

        benchmark.run(&ctx, true).await.expect("run should succeed");

        assert!(
            benchmark
                .last_results
                .as_ref()
                .expect("last results should be set")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn run_propagates_query_failures_when_buffering_results() {
        let mut benchmark = parse_benchmark("run\nSELECT * FROM missing_run_table\n")
            .await
            .expect("benchmark should parse");
        let ctx = SessionContext::new();

        assert_result_error_contains(
            benchmark.run(&ctx, true).await,
            "missing_run_table",
        );
    }

    #[tokio::test]
    async fn run_propagates_query_failures_when_streaming_results() {
        let mut benchmark = parse_benchmark("run\nSELECT * FROM missing_stream_table\n")
            .await
            .expect("benchmark should parse");
        let ctx = SessionContext::new();

        assert_result_error_contains(
            benchmark.run(&ctx, false).await,
            "missing_stream_table",
        );
    }

    #[tokio::test]
    async fn run_rejects_missing_expect_plan_for_buffered_and_streaming_modes() {
        let ctx = SessionContext::new();
        let benchmark_text = "expect_plan definitely_not_in_plan\nrun\nSELECT 1\n";

        let mut buffered = parse_benchmark(benchmark_text)
            .await
            .expect("benchmark should parse");
        assert_result_error_contains(
            buffered.run(&ctx, true).await,
            "does not contain the expected string 'definitely_not_in_plan'",
        );

        let mut streaming = parse_benchmark(benchmark_text)
            .await
            .expect("benchmark should parse");
        assert_result_error_contains(
            streaming.run(&ctx, false).await,
            "does not contain the expected string 'definitely_not_in_plan'",
        );
    }

    #[tokio::test]
    async fn run_accepts_matching_expect_plan_for_buffered_and_streaming_modes() {
        let ctx = SessionContext::new();
        let benchmark_text = "expect_plan PlaceholderRowExec\nrun\nSELECT 1\n";

        let mut buffered = parse_benchmark(benchmark_text)
            .await
            .expect("benchmark should parse");
        buffered
            .run(&ctx, true)
            .await
            .expect("buffered run should accept matching plan");
        assert_eq!(formatted_last_results(&buffered), vec![vec!["1"]]);

        let mut streaming = parse_benchmark(benchmark_text)
            .await
            .expect("benchmark should parse");
        streaming
            .run(&ctx, false)
            .await
            .expect("streaming run should accept matching plan");
    }

    // Verification tests cover result_query and persisted-result comparison paths.

    #[tokio::test]
    async fn verify_without_result_query_returns_ok() {
        let mut benchmark = parse_benchmark("run\nSELECT 1 AS value\n")
            .await
            .expect("benchmark should parse");
        let ctx = SessionContext::new();

        benchmark.verify(&ctx).await.expect("verify should pass");
    }

    #[tokio::test]
    async fn verify_errors_when_benchmark_has_not_run() {
        let mut benchmark = parse_benchmark(
            r#"
result_query I
SELECT 1 AS value
----
1

run
SELECT 1;
"#,
        )
        .await
        .expect("benchmark should parse");
        let ctx = SessionContext::new();

        assert_result_error_contains(
            benchmark.verify(&ctx).await,
            "No results available for verification. Run the benchmark first.",
        );
    }

    #[tokio::test]
    async fn verify_uses_last_results_for_result_file_entries() {
        let temp_dir = tempdir().expect("failed to create benchmark test directory");
        let result_path = write_test_file(&temp_dir, "result.csv", "value\n1\n");
        let mut benchmark = parse_benchmark(&format!(
            "result {}\n\nrun\nSELECT 1 AS value\n",
            result_path.display()
        ))
        .await
        .expect("benchmark should parse");
        let ctx = SessionContext::new();

        benchmark.run(&ctx, true).await.expect("run should succeed");
        benchmark.verify(&ctx).await.expect("verify should pass");
    }

    #[tokio::test]
    async fn verify_uses_last_results_for_zero_row_result_file_entries() {
        let temp_dir = tempdir().expect("failed to create benchmark test directory");
        let result_path = write_test_file(&temp_dir, "result.csv", "value\n");
        let mut benchmark = parse_benchmark(&format!(
            "result {}\n\nrun\nSELECT 1 AS value WHERE false\n",
            result_path.display()
        ))
        .await
        .expect("benchmark should parse");
        let ctx = SessionContext::new();

        benchmark.run(&ctx, true).await.expect("run should succeed");
        benchmark.verify(&ctx).await.expect("verify should pass");
    }

    #[tokio::test]
    async fn verify_rejects_missing_result_file() {
        let temp_dir = tempdir().expect("failed to create benchmark test directory");
        let missing_path = temp_dir.path().join("missing_result.csv");
        let mut benchmark = parse_benchmark(&format!(
            "result {}\n\nrun\nSELECT 1 AS value\n",
            missing_path.display()
        ))
        .await
        .expect("benchmark should parse");
        let ctx = SessionContext::new();

        benchmark.run(&ctx, true).await.expect("run should succeed");

        assert_result_error_contains(benchmark.verify(&ctx).await, "missing_result.csv");
    }

    #[tokio::test]
    async fn verify_rejects_malformed_result_file() {
        let temp_dir = tempdir().expect("failed to create benchmark test directory");
        let result_path = temp_dir.path().join("malformed_result.csv");
        fs::write(&result_path, [0xff]).expect("failed to write malformed result file");
        let mut benchmark = parse_benchmark(&format!(
            "result {}\n\nrun\nSELECT 1 AS value\n",
            result_path.display()
        ))
        .await
        .expect("benchmark should parse");
        let ctx = SessionContext::new();

        benchmark.run(&ctx, true).await.expect("run should succeed");

        assert_result_error_contains(benchmark.verify(&ctx).await, "CSV");
    }

    #[tokio::test]
    async fn verify_executes_result_query_instead_of_last_results() {
        let mut benchmark = parse_benchmark(
            r#"
run
SELECT 100 AS value

result_query I
SELECT 1 AS value
----
1
"#,
        )
        .await
        .expect("benchmark should parse");
        let ctx = SessionContext::new();

        benchmark.run(&ctx, true).await.expect("run should succeed");
        benchmark.verify(&ctx).await.expect("verify should pass");
    }

    #[tokio::test]
    async fn verify_propagates_result_query_failures() {
        let mut benchmark = parse_benchmark(
            r#"
run
SELECT 1 AS value

result_query I
SELECT * FROM missing_verify_table
----
1
"#,
        )
        .await
        .expect("benchmark should parse");
        let ctx = SessionContext::new();

        benchmark.run(&ctx, true).await.expect("run should succeed");

        assert_result_error_contains(
            benchmark.verify(&ctx).await,
            "missing_verify_table",
        );
    }

    #[tokio::test]
    async fn verify_reports_result_mismatch_context() {
        let mut benchmark = parse_benchmark(
            r#"
run
SELECT 1 AS value

result_query I
SELECT 1 AS value
----
2
"#,
        )
        .await
        .expect("benchmark should parse");
        let ctx = SessionContext::new();

        benchmark.run(&ctx, true).await.expect("run should succeed");

        let error = benchmark
            .verify(&ctx)
            .await
            .expect_err("verify should fail");
        let message = error.to_string();
        assert!(
            message.contains("row 1, column 1")
                && message.contains("expected value \"2\"")
                && message.contains("got value \"1\""),
            "unexpected error: {message}"
        );
    }

    // Persistence tests cover CSV writing and persist-time error paths.

    #[tokio::test]
    async fn persist_without_result_query_returns_ok() {
        let mut benchmark = parse_benchmark("run\nSELECT 1 AS value\n")
            .await
            .expect("benchmark should parse");
        let ctx = SessionContext::new();

        benchmark.persist(&ctx).await.expect("persist should pass");
    }

    #[tokio::test]
    async fn persist_rejects_result_query_without_file_path() {
        let mut benchmark = parse_benchmark(
            r#"
run
SELECT 1 AS value

result_query I
SELECT 1 AS value
----
1
"#,
        )
        .await
        .expect("benchmark should parse");
        let ctx = SessionContext::new();

        assert_result_error_contains(
            benchmark.persist(&ctx).await,
            "Unable to persist results from query",
        );
    }

    #[tokio::test]
    async fn persist_rejects_run_without_saved_result_batches() {
        let temp_dir = tempdir().expect("failed to create benchmark test directory");
        let output_path = temp_dir.path().join("persisted");
        let mut benchmark =
            parse_benchmark("run\nCREATE TABLE persist_source AS SELECT 1 AS value;\n")
                .await
                .expect("benchmark should parse");
        benchmark.result_queries.push(BenchmarkQuery {
            path: Some(output_path.to_string_lossy().into_owned()),
            query: String::new(),
            column_count: 1,
            expected_result: vec![],
        });
        let ctx = SessionContext::new();

        assert_result_error_contains(
            benchmark.persist(&ctx).await,
            "Results should be loaded",
        );
    }

    #[tokio::test]
    async fn persist_writes_header_and_pipe_delimited_rows() {
        let temp_dir = tempdir().expect("failed to create benchmark test directory");
        let output_path = temp_dir.path().join("persisted");
        let mut benchmark = parse_benchmark("run\nSELECT 1 AS a, 'one' AS b\n")
            .await
            .expect("benchmark should parse");
        benchmark.result_queries.push(BenchmarkQuery {
            path: Some(output_path.to_string_lossy().into_owned()),
            query: String::new(),
            column_count: 2,
            expected_result: vec![],
        });
        let ctx = SessionContext::new();

        benchmark.persist(&ctx).await.expect("persist should pass");

        let contents = read_all_files_in_dir(&output_path);
        assert!(
            contents.contains("a|b\n") && contents.contains("1|one\n"),
            "unexpected persisted contents: {contents:?}"
        );
    }

    #[tokio::test]
    async fn persist_writes_header_for_zero_row_select_results() {
        let temp_dir = tempdir().expect("failed to create benchmark test directory");
        let output_path = temp_dir.path().join("persisted_empty");
        let mut benchmark = parse_benchmark("run\nSELECT 1 AS value WHERE false\n")
            .await
            .expect("benchmark should parse");
        benchmark.result_queries.push(BenchmarkQuery {
            path: Some(output_path.to_string_lossy().into_owned()),
            query: String::new(),
            column_count: 1,
            expected_result: vec![],
        });
        let ctx = SessionContext::new();

        benchmark.persist(&ctx).await.expect("persist should pass");

        let contents = read_all_files_in_dir(&output_path);
        assert!(
            contents.contains("value\n"),
            "unexpected persisted contents: {contents:?}"
        );
    }

    // Path helper tests cover group derivation from benchmark file paths.

    #[test]
    fn parse_group_from_path_returns_group_under_benchmark_directory() {
        let group = parse_group_from_path(
            Path::new("sql_benchmarks/tpch/benchmarks/q01.benchmark"),
            Path::new("sql_benchmarks"),
        );

        assert_eq!(group, "tpch");
    }

    #[test]
    fn parse_group_from_path_matches_benchmark_directory_case_insensitively() {
        let group = parse_group_from_path(
            Path::new("/tmp/SQL_BENCHMARKS/Tpch/benchmarks/q01.benchmark"),
            Path::new("sql_benchmarks"),
        );

        assert_eq!(group, "Tpch");
    }

    #[test]
    fn parse_group_from_path_handles_relative_and_absolute_paths() {
        let relative = parse_group_from_path(
            Path::new("sql_benchmarks/h2o/q01.benchmark"),
            Path::new("sql_benchmarks"),
        );
        let absolute = parse_group_from_path(
            Path::new("/tmp/sql_benchmarks/imdb/q01.benchmark"),
            Path::new("sql_benchmarks"),
        );

        assert_eq!(relative, "h2o");
        assert_eq!(absolute, "imdb");
    }

    #[test]
    fn parse_group_from_path_pins_fallback_for_paths_outside_benchmark_directory() {
        let group = parse_group_from_path(
            Path::new("outside/group/q01.benchmark"),
            Path::new("sql_benchmarks"),
        );

        assert_eq!(group, "outside");
    }

    #[test]
    fn path_ends_with_ignore_ascii_case_matches_component_suffixes() {
        assert!(path_ends_with_ignore_ascii_case(
            Path::new("/tmp/SQL_BENCHMARKS"),
            Path::new("sql_benchmarks")
        ));
        assert!(!path_ends_with_ignore_ascii_case(
            Path::new("/tmp/sql_benchmarks_extra"),
            Path::new("sql_benchmarks")
        ));
    }
}
