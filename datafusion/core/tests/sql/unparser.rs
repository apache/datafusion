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

//! SQL Unparser Roundtrip Integration Tests
//!
//! This module tests the [`Unparser`] by running queries through a complete roundtrip:
//! the original SQL is parsed into a logical plan, unparsed back to SQL, then that
//! generated SQL is parsed and executed. The results are compared to verify semantic
//! equivalence.
//!
//! ## Test Strategy
//!
//! Uses real-world benchmark queries (TPC-H and Clickbench) to validate that:
//! 1. The unparser produces syntactically valid SQL
//! 2. The unparsed SQL is semantically equivalent (produces identical results)
//!
//! ## Query Suites
//!
//! - **TPC-H**: Standard decision-support benchmark with 22 complex analytical queries
//! - **Clickbench**: Web analytics benchmark with 43 queries against a denormalized schema
//!
//! [`Unparser`]: datafusion_sql::unparser::Unparser

use std::fs::ReadDir;
use std::future::Future;

use arrow::array::RecordBatch;
use datafusion::common::Result;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_common::Column;
use datafusion_expr::Expr;
use datafusion_physical_plan::ExecutionPlanProperties;
use datafusion_sql::unparser::Unparser;
use datafusion_sql::unparser::dialect::DefaultDialect;
use itertools::Itertools;

/// Paths to benchmark query files (supports running from repo root or different working directories).
const BENCHMARK_PATHS: &[&str] = &["../../benchmarks/", "./benchmarks/"];

/// Reads all `.sql` files from a directory and converts them to test queries.
///
/// Skips files that:
/// - Are not regular files
/// - Don't have a `.sql` extension
/// - Contain multiple SQL statements (indicated by `;\n`)
///
/// Multi-statement files are skipped because the unparser doesn't support
/// DML statements like `CREATE VIEW` that appear in multi-statement Clickbench queries.
fn iterate_queries(dir: ReadDir) -> Vec<TestQuery> {
    let mut queries = vec![];
    for entry in dir.flatten() {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_file() {
            continue;
        }
        let path = entry.path();
        let Some(ext) = path.extension() else {
            continue;
        };
        if ext != "sql" {
            continue;
        }
        let name = path.file_stem().unwrap().to_string_lossy().to_string();
        if let Ok(mut contents) = std::fs::read_to_string(entry.path()) {
            // If the query contains ;\n it has DML statements like CREATE VIEW which the unparser doesn't support; skip it
            contents = contents.trim().to_string();
            if contents.contains(";\n") {
                println!("Skipping query with multiple statements: {name}");
                continue;
            }
            queries.push(TestQuery {
                sql: contents,
                name,
            });
        }
    }
    queries
}

/// A SQL query loaded from a benchmark file for roundtrip testing.
///
/// Each query is identified by its filename (without extension) and contains
/// the full SQL text to be tested.
struct TestQuery {
    /// The SQL query text to test.
    sql: String,
    /// The query identifier (typically the filename without .sql extension).
    name: String,
}

/// Collect SQL for Clickbench queries.
fn clickbench_queries() -> Vec<TestQuery> {
    let mut queries = vec![];
    for path in BENCHMARK_PATHS {
        let dir = format!("{path}queries/clickbench/queries/");
        println!("Reading Clickbench queries from {dir}");
        if let Ok(dir) = std::fs::read_dir(dir) {
            let read = iterate_queries(dir);
            println!("Found {} Clickbench queries", read.len());
            queries.extend(read);
        }
    }
    queries.sort_unstable_by_key(|q| {
        q.name
            .split('q')
            .next_back()
            .and_then(|num| num.parse::<u32>().ok())
    });
    queries
}

/// Collect SQL for TPC-H queries.
fn tpch_queries() -> Vec<TestQuery> {
    let mut queries = vec![];
    for path in BENCHMARK_PATHS {
        let dir = format!("{path}queries/");
        println!("Reading TPC-H queries from {dir}");
        if let Ok(dir) = std::fs::read_dir(dir) {
            let read = iterate_queries(dir);
            queries.extend(read);
        }
    }
    println!("Total TPC-H queries found: {}", queries.len());
    queries.sort_unstable_by_key(|q| q.name.clone());
    queries
}

/// Create a new SessionContext for testing that has all Clickbench tables registered.
async fn clickbench_test_context() -> Result<SessionContext> {
    let ctx = SessionContext::new();
    ctx.register_parquet(
        "hits",
        "tests/data/clickbench_hits_10.parquet",
        ParquetReadOptions::default(),
    )
    .await?;
    // Sanity check we found the table by querying it's schema, it should not be empty
    // Otherwise if the path is wrong the tests will all fail in confusing ways
    let df = ctx.sql("SELECT * FROM hits LIMIT 1").await?;
    assert!(
        !df.schema().fields().is_empty(),
        "Clickbench 'hits' table not registered correctly"
    );
    Ok(ctx)
}

/// Create a new SessionContext for testing that has all TPC-H tables registered.
async fn tpch_test_context() -> Result<SessionContext> {
    let ctx = SessionContext::new();
    let data_dir = "tests/data/";
    // All tables have the pattern "tpch_<table_name>_small.parquet"
    for table in [
        "customer", "lineitem", "nation", "orders", "part", "partsupp", "region",
        "supplier",
    ] {
        let path = format!("{data_dir}tpch_{table}_small.parquet");
        ctx.register_parquet(table, &path, ParquetReadOptions::default())
            .await?;
        // Sanity check we found the table by querying it's schema, it should not be empty
        // Otherwise if the path is wrong the tests will all fail in confusing ways
        let df = ctx.sql(&format!("SELECT * FROM {table} LIMIT 1")).await?;
        assert!(
            !df.schema().fields().is_empty(),
            "TPC-H '{table}' table not registered correctly"
        );
    }
    Ok(ctx)
}

/// Sorts record batches by all columns for deterministic comparison.
///
/// When comparing query results, we need a canonical ordering so that
/// semantically equivalent results compare as equal. This function sorts
/// by all columns in the schema to achieve that.
async fn sort_batches(
    ctx: &SessionContext,
    batches: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>> {
    let mut df = ctx.read_batches(batches)?;
    let schema = df.schema().as_arrow().clone();
    let sort_exprs = schema
        .fields()
        .iter()
        // Use Column directly, col() causes the column names to be normalized to lowercase
        .map(|f| {
            Expr::Column(Column::new_unqualified(f.name().to_string())).sort(true, false)
        })
        .collect_vec();
    if !sort_exprs.is_empty() {
        df = df.sort(sort_exprs)?;
    }
    df.collect().await
}

/// The outcome of running a single roundtrip test.
///
/// A successful test produces [`TestCaseResult::Success`].
/// All other variants capture different failure modes with enough context to diagnose the issue.
enum TestCaseResult {
    /// The unparsed SQL produced identical results to the original.
    Success,

    /// Both queries executed but produced different results.
    ///
    /// This indicates a semantic bug in the unparser where the generated SQL
    /// has different meaning than the original.
    ResultsMismatch { original: String, unparsed: String },

    /// The unparser failed to convert the logical plan to SQL.
    ///
    /// This may indicate an unsupported SQL feature or a bug in the unparser.
    UnparseError { original: String, error: String },

    /// The original SQL failed to execute.
    ///
    /// This indicates a problem with the test setup (missing tables,
    /// invalid test data) rather than an unparser issue.
    ExecutionError { original: String, error: String },

    /// The unparsed SQL failed to execute, even though the original succeeded.
    ///
    /// This indicates the unparser generated syntactically invalid SQL or SQL
    /// that references non-existent columns/tables.
    UnparsedExecutionError {
        original: String,
        unparsed: String,
        error: String,
    },
}

impl TestCaseResult {
    /// Returns true if the test case represents a failure
    /// (anything other than [`TestCaseResult::Success`]).
    fn is_failure(&self) -> bool {
        !matches!(self, TestCaseResult::Success)
    }

    /// Formats a detailed error message for the test case into a string.
    fn format_error(&self, name: &str) -> String {
        match self {
            TestCaseResult::Success => String::new(),
            TestCaseResult::ResultsMismatch { original, unparsed } => {
                format!(
                    "Results mismatch for {name}.\nOriginal SQL:\n{original}\n\nUnparsed SQL:\n{unparsed}"
                )
            }
            TestCaseResult::UnparseError { original, error } => {
                format!("Unparse error for {name}: {error}\nOriginal SQL:\n{original}")
            }
            TestCaseResult::ExecutionError { original, error } => {
                format!("Execution error for {name}: {error}\nOriginal SQL:\n{original}")
            }
            TestCaseResult::UnparsedExecutionError {
                original,
                unparsed,
                error,
            } => {
                format!(
                    "Unparsed execution error for {name}: {error}\nOriginal SQL:\n{original}\n\nUnparsed SQL:\n{unparsed}"
                )
            }
        }
    }
}

/// Executes a roundtrip test for a single SQL query.
///
/// This is the core test logic that:
/// 1. Parses the original SQL and creates a logical plan
/// 2. Unparses the logical plan back to SQL
/// 3. Executes both the original and unparsed queries
/// 4. Compares the results (sorting if the query has no ORDER BY)
///
/// This always uses [`DefaultDialect`] for unparsing.
///
/// # Arguments
///
/// * `ctx` - Session context with tables registered
/// * `original` - The original SQL query to test
///
/// # Returns
///
/// A [`TestCaseResult`] indicating success or the specific failure mode.
async fn collect_results(ctx: &SessionContext, original: &str) -> TestCaseResult {
    let unparser = Unparser::new(&DefaultDialect {});

    // Parse and create logical plan from original SQL
    let df = match ctx.sql(original).await {
        Ok(df) => df,
        Err(e) => {
            return TestCaseResult::ExecutionError {
                original: original.to_string(),
                error: e.to_string(),
            };
        }
    };

    // Unparse the logical plan back to SQL
    let unparsed = match unparser.plan_to_sql(df.logical_plan()) {
        Ok(sql) => format!("{sql:#}"),
        Err(e) => {
            return TestCaseResult::UnparseError {
                original: original.to_string(),
                error: e.to_string(),
            };
        }
    };

    let is_sorted = match ctx.state().create_physical_plan(df.logical_plan()).await {
        Ok(plan) => plan.equivalence_properties().output_ordering().is_some(),
        Err(e) => {
            return TestCaseResult::ExecutionError {
                original: original.to_string(),
                error: e.to_string(),
            };
        }
    };

    // Collect results from original query
    let mut expected = match df.collect().await {
        Ok(batches) => batches,
        Err(e) => {
            return TestCaseResult::ExecutionError {
                original: original.to_string(),
                error: e.to_string(),
            };
        }
    };

    // Parse and execute the unparsed SQL
    let actual_df = match ctx.sql(&unparsed).await {
        Ok(df) => df,
        Err(e) => {
            return TestCaseResult::UnparsedExecutionError {
                original: original.to_string(),
                unparsed,
                error: e.to_string(),
            };
        }
    };

    // Collect results from unparsed query
    let mut actual = match actual_df.collect().await {
        Ok(batches) => batches,
        Err(e) => {
            return TestCaseResult::UnparsedExecutionError {
                original: original.to_string(),
                unparsed,
                error: e.to_string(),
            };
        }
    };

    // Sort if needed for comparison
    if !is_sorted {
        expected = match sort_batches(ctx, expected).await {
            Ok(batches) => batches,
            Err(e) => {
                return TestCaseResult::ExecutionError {
                    original: original.to_string(),
                    error: format!("Failed to sort expected results: {e}"),
                };
            }
        };
        actual = match sort_batches(ctx, actual).await {
            Ok(batches) => batches,
            Err(e) => {
                return TestCaseResult::UnparsedExecutionError {
                    original: original.to_string(),
                    unparsed,
                    error: format!("Failed to sort actual results: {e}"),
                };
            }
        };
    }

    if expected != actual {
        TestCaseResult::ResultsMismatch {
            original: original.to_string(),
            unparsed,
        }
    } else {
        TestCaseResult::Success
    }
}

/// Runs roundtrip tests for a collection of queries and reports results.
///
/// Iterates through all queries, running each through [`collect_results`].
/// Prints colored status (green checkmark for success, red X for failure)
/// and panics at the end if any tests failed, with detailed error messages.
///
/// # Type Parameters
///
/// * `F` - Factory function that creates fresh session contexts
/// * `Fut` - Future type returned by the context factory
///
/// # Panics
///
/// Panics if any query fails the roundtrip test, displaying all failures.
async fn run_roundtrip_tests<F, Fut>(
    suite_name: &str,
    queries: Vec<TestQuery>,
    create_context: F,
) where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<SessionContext>>,
{
    let mut errors: Vec<String> = vec![];
    for sql in queries {
        let ctx = match create_context().await {
            Ok(ctx) => ctx,
            Err(e) => {
                println!("\x1b[31m✗\x1b[0m {} query: {}", suite_name, sql.name);
                errors.push(format!("Failed to create context for {}: {}", sql.name, e));
                continue;
            }
        };
        let result = collect_results(&ctx, &sql.sql).await;
        if result.is_failure() {
            println!("\x1b[31m✗\x1b[0m {} query: {}", suite_name, sql.name);
            errors.push(result.format_error(&sql.name));
        } else {
            println!("\x1b[32m✓\x1b[0m {} query: {}", suite_name, sql.name);
        }
    }
    if !errors.is_empty() {
        panic!(
            "{} {} test(s) failed:\n\n{}",
            errors.len(),
            suite_name,
            errors.join("\n\n---\n\n")
        );
    }
}

#[tokio::test]
async fn test_clickbench_unparser_roundtrip() {
    run_roundtrip_tests("Clickbench", clickbench_queries(), clickbench_test_context)
        .await;
}

#[tokio::test]
async fn test_tpch_unparser_roundtrip() {
    run_roundtrip_tests("TPC-H", tpch_queries(), tpch_test_context).await;
}
