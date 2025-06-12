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

//! Execution functions

use crate::cli_context::CliSessionContext;
use crate::helper::split_from_semicolon;
use crate::print_format::PrintFormat;
use crate::{
    command::{Command, OutputFormat},
    helper::CliHelper,
    object_storage::get_object_store,
    print_options::{MaxRows, PrintOptions},
};
use futures::StreamExt;
use glob::Pattern;
use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::sync::Arc;
use url::Url;

use datafusion::common::instant::Instant;
use datafusion::common::{plan_datafusion_err, plan_err};
use datafusion::config::ConfigFileType;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::file_format::{csv::CsvFormat, json::JsonFormat as NdJsonFormat, parquet::ParquetFormat, FileFormat};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{DdlStatement, LogicalPlan, EmptyRelation, CreateExternalTable};
use datafusion::prelude::SessionContext;
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::{execute_stream, ExecutionPlanProperties};
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::sqlparser::dialect::dialect_from_str;

use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::physical_plan::spill::get_record_batch_memory_size;
use datafusion::sql::sqlparser;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use tokio::signal;

/// run and execute SQL statements and commands, against a context with the given print options
pub async fn exec_from_commands(
    ctx: &dyn CliSessionContext,
    commands: Vec<String>,
    print_options: &PrintOptions,
) -> Result<()> {
    for sql in commands {
        exec_and_print(ctx, print_options, sql).await?;
    }

    Ok(())
}

/// run and execute SQL statements and commands from a file, against a context with the given print options
pub async fn exec_from_lines(
    ctx: &dyn CliSessionContext,
    reader: &mut BufReader<File>,
    print_options: &PrintOptions,
) -> Result<()> {
    let mut query = "".to_owned();

    for line in reader.lines() {
        match line {
            Ok(line) if line.starts_with("#!") => {
                continue;
            }
            Ok(line) if line.starts_with("--") => {
                continue;
            }
            Ok(line) => {
                let line = line.trim_end();
                query.push_str(line);
                if line.ends_with(';') {
                    match exec_and_print(ctx, print_options, query).await {
                        Ok(_) => {}
                        Err(err) => eprintln!("{err}"),
                    }
                    query = "".to_string();
                } else {
                    query.push('\n');
                }
            }
            _ => {
                break;
            }
        }
    }

    // run the left over query if the last statement doesn't contain ‘;’
    // ignore if it only consists of '\n'
    if query.contains(|c| c != '\n') {
        exec_and_print(ctx, print_options, query).await?;
    }

    Ok(())
}

pub async fn exec_from_files(
    ctx: &dyn CliSessionContext,
    files: Vec<String>,
    print_options: &PrintOptions,
) -> Result<()> {
    let files = files
        .into_iter()
        .map(|file_path| File::open(file_path).unwrap())
        .collect::<Vec<_>>();

    for file in files {
        let mut reader = BufReader::new(file);
        exec_from_lines(ctx, &mut reader, print_options).await?;
    }

    Ok(())
}

/// run and execute SQL statements and commands against a context with the given print options
pub async fn exec_from_repl(
    ctx: &dyn CliSessionContext,
    print_options: &mut PrintOptions,
) -> rustyline::Result<()> {
    let mut rl = Editor::new()?;
    rl.set_helper(Some(CliHelper::new(
        &ctx.task_ctx().session_config().options().sql_parser.dialect,
        print_options.color,
    )));
    rl.load_history(".history").ok();

    loop {
        match rl.readline("> ") {
            Ok(line) if line.starts_with('\\') => {
                rl.add_history_entry(line.trim_end())?;
                let command = line.split_whitespace().collect::<Vec<_>>().join(" ");
                if let Ok(cmd) = &command[1..].parse::<Command>() {
                    match cmd {
                        Command::Quit => break,
                        Command::OutputFormat(subcommand) => {
                            if let Some(subcommand) = subcommand {
                                if let Ok(command) = subcommand.parse::<OutputFormat>() {
                                    if let Err(e) = command.execute(print_options).await {
                                        eprintln!("{e}")
                                    }
                                } else {
                                    eprintln!(
                                        "'\\{}' is not a valid command",
                                        &line[1..]
                                    );
                                }
                            } else {
                                println!("Output format is {:?}.", print_options.format);
                            }
                        }
                        _ => {
                            if let Err(e) = cmd.execute(ctx, print_options).await {
                                eprintln!("{e}")
                            }
                        }
                    }
                } else {
                    eprintln!("'\\{}' is not a valid command", &line[1..]);
                }
            }
            Ok(line) => {
                let lines = split_from_semicolon(&line);
                for line in lines {
                    rl.add_history_entry(line.trim_end())?;
                    tokio::select! {
                        res = exec_and_print(ctx, print_options, line) => match res {
                            Ok(_) => {}
                            Err(err) => eprintln!("{err}"),
                        },
                        _ = signal::ctrl_c() => {
                            println!("^C");
                            continue
                        },
                    }
                    // dialect might have changed
                    rl.helper_mut().unwrap().set_dialect(
                        &ctx.task_ctx().session_config().options().sql_parser.dialect,
                    );
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!("\\q");
                break;
            }
            Err(err) => {
                eprintln!("Unknown error happened {err:?}");
                break;
            }
        }
    }

    rl.save_history(".history")
}

pub(super) async fn exec_and_print(
    ctx: &dyn CliSessionContext,
    print_options: &PrintOptions,
    sql: String,
) -> Result<()> {
    let now = Instant::now();
    let task_ctx = ctx.task_ctx();
    let options = task_ctx.session_config().options();
    let dialect = &options.sql_parser.dialect;
    let dialect = dialect_from_str(dialect).ok_or_else(|| {
        plan_datafusion_err!(
            "Unsupported SQL dialect: {dialect}. Available dialects: \
                 Generic, MySQL, PostgreSQL, Hive, SQLite, Snowflake, Redshift, \
                 MsSQL, ClickHouse, BigQuery, Ansi, DuckDB, Databricks."
        )
    })?;

    let statements = DFParser::parse_sql_with_dialect(&sql, dialect.as_ref())?;
    for statement in statements {
        let adjusted =
            AdjustedPrintOptions::new(print_options.clone()).with_statement(&statement);

        let plan = create_plan(ctx, statement).await?;
        let adjusted = adjusted.with_plan(&plan);

        let df = ctx.execute_logical_plan(plan).await?;
        let physical_plan = df.create_physical_plan().await?;

        // Track memory usage for the query result if it's bounded
        let mut reservation =
            MemoryConsumer::new("DataFusion-Cli").register(task_ctx.memory_pool());

        if physical_plan.boundedness().is_unbounded() {
            if physical_plan.pipeline_behavior() == EmissionType::Final {
                return plan_err!(
                    "The given query can generate a valid result only once \
                    the source finishes, but the source is unbounded"
                );
            }
            // As the input stream comes, we can generate results.
            // However, memory safety is not guaranteed.
            let stream = execute_stream(physical_plan, task_ctx.clone())?;
            print_options
                .print_stream(stream, now, &options.format)
                .await?;
        } else {
            // Bounded stream; collected results size is limited by the maxrows option
            let schema = physical_plan.schema();
            let mut stream = execute_stream(physical_plan, task_ctx.clone())?;
            let mut results = vec![];
            let mut row_count = 0_usize;
            let max_rows = match print_options.maxrows {
                MaxRows::Unlimited => usize::MAX,
                MaxRows::Limited(n) => n,
            };
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                let curr_num_rows = batch.num_rows();
                // Stop collecting results if the number of rows exceeds the limit
                // results batch should include the last batch that exceeds the limit
                if row_count < max_rows + curr_num_rows {
                    // Try to grow the reservation to accommodate the batch in memory
                    reservation.try_grow(get_record_batch_memory_size(&batch))?;
                    results.push(batch);
                }
                row_count += curr_num_rows;
            }
            adjusted.into_inner().print_batches(
                schema,
                &results,
                now,
                row_count,
                &options.format,
            )?;
            reservation.free();
        }
    }

    Ok(())
}

/// Track adjustments to the print options based on the plan / statement being executed
#[derive(Debug)]
struct AdjustedPrintOptions {
    inner: PrintOptions,
}

impl AdjustedPrintOptions {
    fn new(inner: PrintOptions) -> Self {
        Self { inner }
    }
    /// Adjust print options based on any statement specific requirements
    fn with_statement(mut self, statement: &Statement) -> Self {
        if let Statement::Statement(sql_stmt) = statement {
            // SHOW / SHOW ALL
            if let sqlparser::ast::Statement::ShowVariable { .. } = sql_stmt.as_ref() {
                self.inner.maxrows = MaxRows::Unlimited
            }
        }
        self
    }

    /// Adjust print options based on any plan specific requirements
    fn with_plan(mut self, plan: &LogicalPlan) -> Self {
        // For plans like `Explain` ignore `MaxRows` option and always display
        // all rows
        if matches!(
            plan,
            LogicalPlan::Explain(_)
                | LogicalPlan::DescribeTable(_)
                | LogicalPlan::Analyze(_)
        ) {
            self.inner.maxrows = MaxRows::Unlimited;
        }
        self
    }

    /// Finalize and return the inner `PrintOptions`
    fn into_inner(mut self) -> PrintOptions {
        if self.inner.format == PrintFormat::Automatic {
            self.inner.format = PrintFormat::Table;
        }

        self.inner
    }
}

fn config_file_type_from_str(ext: &str) -> Option<ConfigFileType> {
    match ext.to_lowercase().as_str() {
        "csv" => Some(ConfigFileType::CSV),
        "json" => Some(ConfigFileType::JSON),
        "parquet" => Some(ConfigFileType::PARQUET),
        _ => None,
    }
}

fn file_format_from_config_type(config_type: ConfigFileType) -> (&'static str, Arc<dyn FileFormat>) {
    match config_type {
        ConfigFileType::PARQUET => (".parquet", Arc::new(ParquetFormat::default())),
        ConfigFileType::CSV => (".csv", Arc::new(CsvFormat::default().with_has_header(true))),
        ConfigFileType::JSON => (".json", Arc::new(NdJsonFormat::default())),
    }
}

/// Detects if a location string contains both a URL scheme and glob patterns
fn is_glob_pattern_with_scheme(location: &str) -> bool {
    location.find("://").map_or(false, |pos| pos > 0 && location.contains(['*', '?', '[']))
}

/// Splits a location string containing a glob pattern into (base_path, glob_part)
/// Returns None if no glob pattern is found.
fn split_glob_base(location: &str) -> Option<(&str, &str)> {
    let glob_pos = location.find(['*', '?', '['])?;
    let split_pos = location[..glob_pos].rfind('/')? + 1;
    Some(location.split_at(split_pos))
}

/// Handles CREATE EXTERNAL TABLE commands with glob patterns in URLs
///
/// Returns `Ok(Some(plan))` if glob pattern was handled, `Ok(None)` if not a glob pattern,
/// or `Err` if glob pattern handling failed.
async fn handle_glob_pattern_table_creation(
    ctx: &dyn CliSessionContext,
    cmd: &mut CreateExternalTable,
    format: Option<ConfigFileType>,
) -> Result<Option<LogicalPlan>, DataFusionError> {
    if !is_glob_pattern_with_scheme(&cmd.location) {
        return Ok(None);
    }

    // Register object store for the base path first
    let base_path = extract_base_path_for_object_store(&cmd.location);
    register_object_store_and_config_extensions(ctx, &base_path, &cmd.options, format.clone()).await?;
    
    // Create and register the table provider
    let table_provider = create_glob_listing_table(ctx, cmd, format).await?;
    let session_ctx = ctx.as_any().downcast_ref::<SessionContext>()
        .ok_or_else(|| plan_datafusion_err!("Failed to downcast CliSessionContext to SessionContext"))?;
    
    session_ctx.register_table(&cmd.name.to_string(), table_provider)?;
    
    // Return empty plan since table is already registered
    Ok(Some(LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(datafusion::common::DFSchema::empty()),
    })))
}

async fn create_plan(
    ctx: &dyn CliSessionContext,
    statement: Statement,
) -> Result<LogicalPlan, DataFusionError> {
    let mut plan = ctx.session_state().statement_to_plan(statement).await?;

    // Note that cmd is a mutable reference so that create_external_table function can remove all
    // datafusion-cli specific options before passing through to datafusion. Otherwise, datafusion
    // will raise Configuration errors.
    if let LogicalPlan::Ddl(DdlStatement::CreateExternalTable(cmd)) = &mut plan {
        // To support custom formats, treat error as None
        let format = config_file_type_from_str(&cmd.file_type);

        // Handle glob patterns by creating the table directly in CLI
        if let Some(glob_plan) = handle_glob_pattern_table_creation(ctx, cmd, format.clone()).await? {
            return Ok(glob_plan);
        }

        register_object_store_and_config_extensions(
            ctx,
            &cmd.location,
            &cmd.options,
            format,
        )
        .await?;
    }

    if let LogicalPlan::Copy(copy_to) = &mut plan {
        let format = config_file_type_from_str(&copy_to.file_type.get_ext());

        register_object_store_and_config_extensions(
            ctx,
            &copy_to.output_url,
            &copy_to.options,
            format,
        )
        .await?;
    }
    Ok(plan)
}

/// Asynchronously registers an object store and its configuration extensions
/// to the session context.
///
/// This function dynamically registers a cloud object store based on the given
/// location and options. It first parses the location to determine the scheme
/// and constructs the URL accordingly. Depending on the scheme, it also registers
/// relevant options. The function then alters the default table options with the
/// given custom options. Finally, it retrieves and registers the object store
/// in the session context.
///
/// # Parameters
///
/// * `ctx`: A reference to the `SessionContext` for registering the object store.
/// * `location`: A string reference representing the location of the object store.
/// * `options`: A reference to a hash map containing configuration options for
///   the object store.
///
/// # Returns
///
/// A `Result<()>` which is an Ok value indicating successful registration, or
/// an error upon failure.
///
/// # Errors
///
/// This function can return an error if the location parsing fails, options
/// alteration fails, or if the object store cannot be retrieved and registered
/// successfully.
pub(crate) async fn register_object_store_and_config_extensions(
    ctx: &dyn CliSessionContext,
    location: &String,
    options: &HashMap<String, String>,
    format: Option<ConfigFileType>,
) -> Result<()> {
    // Parse the location - for glob patterns, we need to register the base path for object store
    let base_path = extract_base_path_for_object_store(location);
    let table_path = ListingTableUrl::parse(&base_path)?;

    // Extract the scheme (e.g., "s3", "gcs") from the parsed URL
    let scheme = table_path.scheme();

    // Obtain a reference to the URL
    let url = table_path.as_ref();

    // Register the options based on the scheme extracted from the location
    ctx.register_table_options_extension_from_scheme(scheme);

    // Clone and modify the default table options based on the provided options
    let mut table_options = ctx.session_state().default_table_options();
    if let Some(format) = format {
        table_options.set_config_format(format);
    }
    table_options.alter_with_string_hash_map(options)?;

    // Retrieve the appropriate object store based on the scheme, URL, and modified table options
    let store =
        get_object_store(&ctx.session_state(), scheme, url, &table_options).await?;

    // Register the retrieved object store in the session context's runtime environment
    ctx.register_object_store(url, store);

    Ok(())
}

/// Extract the base path from a location with glob patterns for object store registration
fn extract_base_path_for_object_store(location: &str) -> String {
    if let Some((base_path, _glob_part)) = split_glob_base(location) {
        base_path.to_string()
    } else {
        location.to_string()
    }
}

/// Create a ListingTable for glob patterns
async fn create_glob_listing_table(
    ctx: &dyn CliSessionContext,
    cmd: &CreateExternalTable,
    format: Option<ConfigFileType>,
) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
    let location = &cmd.location;
    let url = create_listing_table_url(location)?;
    // Determine file format for the table configuration
    let format = format.ok_or_else(|| plan_datafusion_err!("glob(): file format must be specified"))?;
    let (file_extension, file_format) = file_format_from_config_type(format);

    // Create the listing table
    let listing_opts = ListingOptions::new(file_format).with_file_extension(file_extension);
    let schema = listing_opts.infer_schema(&ctx.session_state(), &url).await?;
    let config = ListingTableConfig::new(url)
        .with_listing_options(listing_opts)
        .with_schema(schema);
    let table = ListingTable::try_new(config)?;

    Ok(Arc::new(table))
}

/// Create a ListingTableUrl from a location string, handling glob patterns properly
fn create_listing_table_url(location: &str) -> Result<ListingTableUrl> {
    if location.contains("://") && location.contains(['*', '?', '[']) {
        if let Some((base_path, glob_part)) = split_glob_base(location) {
            let base_url = Url::parse(&format!("{}/", base_path.trim_end_matches('/')))
                .map_err(|e| plan_datafusion_err!("Invalid base URL: {}", e))?;
            let glob = Pattern::new(glob_part)
                .map_err(|e| plan_datafusion_err!("Invalid glob pattern: {}", e))?;
            ListingTableUrl::try_new(base_url, Some(glob))
        } else {
            plan_err!("Failed to split glob pattern from location: {}", location)
        }
    } else {
        // Local path or URL without globs - parse() handles this correctly
        ListingTableUrl::parse(location)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::common::plan_err;

    use datafusion::prelude::SessionContext;
    use url::Url;

    async fn create_external_table_test(location: &str, sql: &str) -> Result<()> {
        let ctx = SessionContext::new();
        let plan = ctx.state().create_logical_plan(sql).await?;

        if let LogicalPlan::Ddl(DdlStatement::CreateExternalTable(cmd)) = &plan {
            let format = config_file_type_from_str(&cmd.file_type);
            register_object_store_and_config_extensions(
                &ctx,
                &cmd.location,
                &cmd.options,
                format,
            )
            .await?;
        } else {
            return plan_err!("LogicalPlan is not a CreateExternalTable");
        }

        // Ensure the URL is supported by the object store
        ctx.runtime_env()
            .object_store(ListingTableUrl::parse(location)?)?;

        Ok(())
    }

    async fn copy_to_table_test(location: &str, sql: &str) -> Result<()> {
        let ctx = SessionContext::new();
        // AWS CONFIG register.

        let plan = ctx.state().create_logical_plan(sql).await?;

        if let LogicalPlan::Copy(cmd) = &plan {
            let format = config_file_type_from_str(&cmd.file_type.get_ext());
            register_object_store_and_config_extensions(
                &ctx,
                &cmd.output_url,
                &cmd.options,
                format,
            )
            .await?;
        } else {
            return plan_err!("LogicalPlan is not a CreateExternalTable");
        }

        // Ensure the URL is supported by the object store
        ctx.runtime_env()
            .object_store(ListingTableUrl::parse(location)?)?;

        Ok(())
    }

    #[tokio::test]
    async fn create_object_store_table_http() -> Result<()> {
        // Should be OK
        let location = "http://example.com/file.parquet";
        let sql =
            format!("CREATE EXTERNAL TABLE test STORED AS PARQUET LOCATION '{location}'");
        create_external_table_test(location, &sql).await?;

        Ok(())
    }
    #[tokio::test]
    async fn copy_to_external_object_store_test() -> Result<()> {
        let locations = vec![
            "s3://bucket/path/file.parquet",
            "oss://bucket/path/file.parquet",
            "cos://bucket/path/file.parquet",
            "gcs://bucket/path/file.parquet",
        ];
        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let dialect = &task_ctx.session_config().options().sql_parser.dialect;
        let dialect = dialect_from_str(dialect).ok_or_else(|| {
            plan_datafusion_err!(
                "Unsupported SQL dialect: {dialect}. Available dialects: \
                 Generic, MySQL, PostgreSQL, Hive, SQLite, Snowflake, Redshift, \
                 MsSQL, ClickHouse, BigQuery, Ansi, DuckDB, Databricks."
            )
        })?;
        for location in locations {
            let sql = format!("copy (values (1,2)) to '{location}' STORED AS PARQUET;");
            let statements = DFParser::parse_sql_with_dialect(&sql, dialect.as_ref())?;
            for statement in statements {
                //Should not fail
                let mut plan = create_plan(&ctx, statement).await?;
                if let LogicalPlan::Copy(copy_to) = &mut plan {
                    assert_eq!(copy_to.output_url, location);
                    assert_eq!(copy_to.file_type.get_ext(), "parquet".to_string());
                    ctx.runtime_env()
                        .object_store_registry
                        .get_store(&Url::parse(&copy_to.output_url).unwrap())?;
                } else {
                    return plan_err!("LogicalPlan is not a CopyTo");
                }
            }
        }
        
        Ok(())
    }

    #[tokio::test]
    async fn test_is_glob_pattern_with_scheme() {
        // Should detect glob patterns with schemes
        assert!(is_glob_pattern_with_scheme("s3://bucket/file-*.csv"));
        assert!(is_glob_pattern_with_scheme("https://example.com/file-?.json"));
        assert!(is_glob_pattern_with_scheme("gcs://bucket/data-[abc].parquet"));
        assert!(is_glob_pattern_with_scheme("file:///tmp/test-*.csv"));
        
        // Should NOT detect patterns without schemes
        assert!(!is_glob_pattern_with_scheme("file-*.csv"));
        assert!(!is_glob_pattern_with_scheme("/tmp/test-?.json"));
        assert!(!is_glob_pattern_with_scheme("*.parquet"));
        
        // Should NOT detect regular files with schemes
        assert!(!is_glob_pattern_with_scheme("s3://bucket/file.csv"));
        assert!(!is_glob_pattern_with_scheme("https://example.com/data.json"));
        assert!(!is_glob_pattern_with_scheme("gcs://bucket/data.parquet"));
        
        // Edge cases
        assert!(!is_glob_pattern_with_scheme(""));
        assert!(!is_glob_pattern_with_scheme("://"));
        assert!(!is_glob_pattern_with_scheme("*"));
        assert!(!is_glob_pattern_with_scheme("scheme://"));
        assert!(!is_glob_pattern_with_scheme("://path/*"));
    }

    #[test]
    fn test_split_glob_base() {
        // Test glob pattern splitting
        assert_eq!(
            split_glob_base("s3://bucket/path/file-*.csv"),
            Some(("s3://bucket/path/", "file-*.csv"))
        );
        
        assert_eq!(
            split_glob_base("s3://bucket/data/file-?.json"),
            Some(("s3://bucket/data/", "file-?.json"))
        );
        
        assert_eq!(
            split_glob_base("s3://bucket/file-[abc].parquet"),
            Some(("s3://bucket/", "file-[abc].parquet"))
        );
        
        // No glob pattern
        assert_eq!(split_glob_base("s3://bucket/file.csv"), None);
        
        // Local path with glob
        assert_eq!(
            split_glob_base("/tmp/test-*.csv"),
            Some(("/tmp/", "test-*.csv"))
        );

        // Edge cases
        assert_eq!(split_glob_base(""), None);
        assert_eq!(split_glob_base("*"), None); // No slash before glob
        assert_eq!(
            split_glob_base("path/to/file-*.txt"),
            Some(("path/to/", "file-*.txt"))
        );
    }

    #[test]
    fn test_extract_base_path_for_object_store() {
        assert_eq!(
            extract_base_path_for_object_store("s3://bucket/path/file-*.csv"),
            "s3://bucket/path/"
        );
        
        assert_eq!(
            extract_base_path_for_object_store("s3://bucket/file.csv"),
            "s3://bucket/file.csv"
        );
        
        assert_eq!(
            extract_base_path_for_object_store("/tmp/test-*.csv"),
            "/tmp/"
        );

        // Additional test cases
        assert_eq!(
            extract_base_path_for_object_store("gcs://bucket/data/logs-?.json"),
            "gcs://bucket/data/"
        );
        
        assert_eq!(
            extract_base_path_for_object_store("https://example.com/api/v1/data-[abc].parquet"),
            "https://example.com/api/v1/"
        );

        // No glob pattern
        assert_eq!(
            extract_base_path_for_object_store("s3://bucket/single-file.csv"),
            "s3://bucket/single-file.csv"
        );
    }

    #[test]
    fn test_scheme_extraction_from_various_urls() {
        use datafusion::datasource::listing::ListingTableUrl;
        
        let test_cases = vec![
            ("s3://bucket/data.csv", "s3"),
            ("gcs://bucket/logs.json", "gcs"),
            ("https://example.com/files.parquet", "https"),
            ("file:///tmp/test.csv", "file"),
        ];
        
        for (location, expected_scheme) in test_cases {
            let url = ListingTableUrl::parse(location).unwrap();
            assert_eq!(url.scheme(), expected_scheme, "Failed for location: {}", location);
        }
    }

    #[tokio::test]
    async fn test_handle_glob_pattern_table_creation_not_glob() {
        use datafusion::common::{DFSchema, Constraints};
        
        let ctx = SessionContext::new();
        let mut cmd = CreateExternalTable {
            schema: Arc::new(DFSchema::empty()),
            name: "test_table".into(),
            location: "s3://bucket/data.csv".to_string(),
            file_type: "csv".to_string(),
            table_partition_cols: vec![],
            if_not_exists: false,
            temporary: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::new(),
            constraints: Constraints::default(),
            column_defaults: HashMap::new(),
        };
        
        let result = handle_glob_pattern_table_creation(&ctx, &mut cmd, Some(ConfigFileType::CSV)).await;
        
        // Should return None since it's not a glob pattern
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_handle_glob_pattern_table_creation_local_glob() {
        use datafusion::common::{DFSchema, Constraints};
        
        let ctx = SessionContext::new();
        let mut cmd = CreateExternalTable {
            schema: Arc::new(DFSchema::empty()),
            name: "test_table".into(),
            location: "/tmp/data-*.csv".to_string(), // Local path with glob - should not be handled
            file_type: "csv".to_string(),
            table_partition_cols: vec![],
            if_not_exists: false,
            temporary: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::new(),
            constraints: Constraints::default(),
            column_defaults: HashMap::new(),
        };
        
        let result = handle_glob_pattern_table_creation(&ctx, &mut cmd, Some(ConfigFileType::CSV)).await;
        
        // Should return None since it doesn't have a scheme
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_glob_pattern_combinations() {
        // Test various glob pattern combinations
        let test_cases = vec![
            // Valid glob patterns with schemes
            ("s3://bucket/data-*.csv", true),
            ("s3://bucket/data-?.json", true),
            ("s3://bucket/data-[abc].parquet", true),
            ("gcs://bucket/logs-*.txt", true),
            ("https://api.com/files-*.xml", true),
            
            // Invalid - no scheme
            ("data-*.csv", false),
            ("/tmp/files-?.json", false),
            ("./data-[abc].parquet", false),
            
            // Invalid - no glob
            ("s3://bucket/data.csv", false),
            ("gcs://bucket/logs.json", false),
            ("https://api.com/file.xml", false),
            
            // Edge cases
            ("", false),
            ("*", false),
            ("://", false),
            ("scheme://", false),
            ("://path/*", false), // No valid scheme before ://, so it should be false
        ];
        
        for (location, expected) in test_cases {
            assert_eq!(
                is_glob_pattern_with_scheme(location),
                expected,
                "Failed for location: '{}'",
                location
            );
        }
    }

    #[tokio::test]
    async fn copy_to_object_store_table_s3() -> Result<()> {
        let access_key_id = "fake_access_key_id";
        let secret_access_key = "fake_secret_access_key";
        let location = "s3://bucket/path/file.parquet";

        // Missing region, use object_store defaults
        let sql = format!("COPY (values (1,2)) TO '{location}' STORED AS PARQUET
            OPTIONS ('aws.access_key_id' '{access_key_id}', 'aws.secret_access_key' '{secret_access_key}')");
        copy_to_table_test(location, &sql).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_object_store_table_s3() -> Result<()> {
        let access_key_id = "fake_access_key_id";
        let secret_access_key = "fake_secret_access_key";
        let region = "fake_us-east-2";
        let session_token = "fake_session_token";
        let location = "s3://bucket/path/file.parquet";

        // Missing region, use object_store defaults
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('aws.access_key_id' '{access_key_id}', 'aws.secret_access_key' '{secret_access_key}') LOCATION '{location}'");
        create_external_table_test(location, &sql).await?;

        // Should be OK
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('aws.access_key_id' '{access_key_id}', 'aws.secret_access_key' '{secret_access_key}', 'aws.region' '{region}', 'aws.session_token' '{session_token}') LOCATION '{location}'");
        create_external_table_test(location, &sql).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_object_store_table_oss() -> Result<()> {
        let access_key_id = "fake_access_key_id";
        let secret_access_key = "fake_secret_access_key";
        let endpoint = "fake_endpoint";
        let location = "oss://bucket/path/file.parquet";

        // Should be OK
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('aws.access_key_id' '{access_key_id}', 'aws.secret_access_key' '{secret_access_key}', 'aws.oss.endpoint' '{endpoint}') LOCATION '{location}'");
        create_external_table_test(location, &sql).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_object_store_table_cos() -> Result<()> {
        let access_key_id = "fake_access_key_id";
        let secret_access_key = "fake_secret_access_key";
        let endpoint = "fake_endpoint";
        let location = "cos://bucket/path/file.parquet";

        // Should be OK
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('aws.access_key_id' '{access_key_id}', 'aws.secret_access_key' '{secret_access_key}', 'aws.cos.endpoint' '{endpoint}') LOCATION '{location}'");
        create_external_table_test(location, &sql).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_object_store_table_gcs() -> Result<()> {
        let service_account_path = "fake_service_account_path";
        let service_account_key =
            "{\"private_key\": \"fake_private_key.pem\",\"client_email\":\"fake_client_email\", \"private_key_id\":\"id\"}";
        let application_credentials_path = "fake_application_credentials_path";
        let location = "gcs://bucket/path/file.parquet";

        // for service_account_path
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('gcp.service_account_path' '{service_account_path}') LOCATION '{location}'");
        let err = create_external_table_test(location, &sql)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("os error 2"));

        // for service_account_key
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET OPTIONS('gcp.service_account_key' '{service_account_key}') LOCATION '{location}'");
        let err = create_external_table_test(location, &sql)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("No RSA key found in pem file"), "{err}");

        // for application_credentials_path
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('gcp.application_credentials_path' '{application_credentials_path}') LOCATION '{location}'");
        let err = create_external_table_test(location, &sql)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("os error 2"));

        Ok(())
    }

    #[tokio::test]
    async fn create_external_table_local_file() -> Result<()> {
        let location = "path/to/file.parquet";

        // Ensure that local files are also registered
        let sql =
            format!("CREATE EXTERNAL TABLE test STORED AS PARQUET LOCATION '{location}'");
        create_external_table_test(location, &sql).await.unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn create_external_table_format_option() -> Result<()> {
        let location = "path/to/file.cvs";

        // Test with format options
        let sql =
            format!("CREATE EXTERNAL TABLE test STORED AS CSV LOCATION '{location}' OPTIONS('format.has_header' 'true')");
        create_external_table_test(location, &sql).await.unwrap();

        Ok(())
    }
}
