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

//! Reports planning statistics alongside runtime metrics for benchmark queries.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, LazyLock};

use clap::Args;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::operator_statistics::StatisticsRegistry;
use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion::sql::parser::{DFParserBuilder, Statement};
use datafusion::sql::sqlparser::dialect::dialect_from_str;
use datafusion_common::config::{Dialect, SqlParserOptions};
use datafusion_common::config_err;
use datafusion_common::stats::Precision;
use regex::Regex;
use serde::{Deserialize, Serialize};

/// Generate reports that compare planning statistics with runtime metrics.
///
/// Parser options are captured when the run starts, so `SET` statements do not
/// affect parsing in later query files.
#[derive(Debug, Args)]
#[command(verbatim_doc_comment)]
pub struct RunOpt {
    /// Query filename stem. If not specified, runs every `.sql` file.
    #[arg(short, long)]
    query: Option<String>,

    /// Branch whose results should be compared. Defaults to the previous run on this branch.
    #[arg(long)]
    compare: Option<String>,

    /// Path to Parquet data. Top-level files and directories are registered as tables.
    #[arg(required = true, short = 'p', long)]
    path: PathBuf,

    /// Path to a SQL file or directory of SQL query files.
    #[arg(required = true, short = 'Q', long = "query_path")]
    query_path: PathBuf,
}

impl RunOpt {
    pub async fn run(self) -> Result<()> {
        let mut config = SessionConfig::from_env()?.with_collect_statistics(true);
        config.options_mut().optimizer.prefer_hash_join = true;
        let ctx = SessionContext::new_with_config(config);
        let sql_parser_options = ctx.state().config_options().sql_parser.clone();
        register_parquet_files(&ctx, &self.path).await?;

        let branch = current_branch_name();
        let result_path = self.report_path(&branch);
        let comparison_branch = self
            .compare
            .as_deref()
            .map_or(branch.as_str(), |branch| branch);
        let comparison_path = self.report_path(comparison_branch);
        let previous = load_comparison_report(&comparison_path)?;
        backup_previous_report(&result_path)?;
        let comparison_description = self.compare.as_ref().map_or_else(
            || format!("previous run on branch '{branch}'"),
            |branch| format!("branch '{branch}'"),
        );

        let mut reports = vec![];
        for query_path in query_files(&self.query_path, self.query.as_deref())? {
            let query = query_path
                .file_stem()
                .expect("query file has a filename")
                .to_string_lossy()
                .to_string();
            let sql = fs::read_to_string(query_path)?;
            let statements = match sql_statements(&sql, &sql_parser_options) {
                Ok(statements) => statements,
                Err(error) => {
                    let report = QueryReport {
                        query: query.clone(),
                        statement: 1,
                        operators: vec![],
                        success: false,
                        error: Some(error.to_string()),
                    };
                    print_query_report(&report, previous.as_deref());
                    reports.push(report);
                    continue;
                }
            };
            for (statement, sql) in statements.into_iter().enumerate() {
                let statement = statement + 1;
                let report = match self.report_statement(&ctx, sql).await {
                    Ok(operators) => QueryReport {
                        query: query.clone(),
                        statement,
                        operators,
                        success: true,
                        error: None,
                    },
                    Err(error) => QueryReport {
                        query: query.clone(),
                        statement,
                        operators: vec![],
                        success: false,
                        error: Some(error.to_string()),
                    },
                };
                print_query_report(&report, previous.as_deref());
                reports.push(report);
            }
        }
        store_report(&result_path, &reports)?;
        print_q_error_summary(
            &reports,
            previous.as_deref(),
            &branch,
            &comparison_description,
        );
        Ok(())
    }

    fn report_path(&self, branch: &str) -> PathBuf {
        let report_name = self.query.as_ref().map_or_else(
            || "statistics.json".to_string(),
            |query| format!("statistics-{query}.json"),
        );
        PathBuf::from("target/dfbench/statistics")
            .join(normalize_branch_name(branch))
            .join(report_name)
    }

    async fn report_statement(
        &self,
        ctx: &SessionContext,
        statement: Statement,
    ) -> Result<Vec<OperatorReport>> {
        let state = ctx.state();
        let logical_plan = state.statement_to_plan(statement).await?;
        if matches!(
            logical_plan,
            LogicalPlan::Ddl(_) | LogicalPlan::Statement(_)
        ) {
            ctx.execute_logical_plan(logical_plan)
                .await?
                .collect()
                .await?;
            return Ok(vec![]);
        }
        let logical_plan = state.optimize(&logical_plan)?;
        let physical_plan = state.create_physical_plan(&logical_plan).await?;

        let statistics = capture_statistics(physical_plan.as_ref())?;
        collect(Arc::clone(&physical_plan), state.task_ctx()).await?;

        let mut report = Vec::with_capacity(statistics.len());
        append_runtime_metrics(physical_plan.as_ref(), &statistics, &mut report);
        Ok(report)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct QueryReport {
    query: String,
    statement: usize,
    operators: Vec<OperatorReport>,
    #[serde(default = "default_success")]
    success: bool,
    #[serde(default)]
    error: Option<String>,
}

fn default_success() -> bool {
    true
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct OperatorReport {
    path: String,
    name: String,
    estimated_rows: StatisticValue,
    estimated_bytes: StatisticValue,
    runtime_output_rows: Option<usize>,
    runtime_output_bytes: Option<usize>,
    q_error: Option<QError>,
}

#[derive(Debug, Serialize)]
struct CapturedStatistics {
    path: String,
    name: String,
    estimated_rows: StatisticValue,
    estimated_bytes: StatisticValue,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(tag = "precision", content = "value", rename_all = "snake_case")]
enum StatisticValue {
    Exact(usize),
    Inexact(usize),
    Absent,
}

/// Symmetric multiplicative error between an estimated and actual row count.
///
/// The q-error metric is infinite when exactly one value is zero. A zero
/// estimate for an actual zero result is reported separately as an exact
/// estimate and is excluded from q-error percentiles.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
enum QError {
    Finite(f64),
    Infinite,
    ExactZero,
}

fn capture_statistics(plan: &dyn ExecutionPlan) -> Result<Vec<CapturedStatistics>> {
    let statistics_context = StatisticsRegistry::default_with_builtin_providers();
    let mut result = vec![];
    capture_statistics_inner(plan, &statistics_context, "0", &mut result)?;
    Ok(result)
}

fn capture_statistics_inner(
    plan: &dyn ExecutionPlan,
    statistics_context: &StatisticsRegistry,
    path: &str,
    result: &mut Vec<CapturedStatistics>,
) -> Result<()> {
    let statistics = StatisticsContext::new_with_registry(statistics_context.clone())
        .compute(plan, &StatisticsArgs::new())?;
    result.push(CapturedStatistics {
        path: path.to_string(),
        name: plan.name().to_string(),
        estimated_rows: statistic_value(statistics.num_rows),
        estimated_bytes: statistic_value(statistics.total_byte_size),
    });
    for (child_index, child) in plan.children().iter().enumerate() {
        capture_statistics_inner(
            child.as_ref(),
            statistics_context,
            &format!("{path}.{child_index}"),
            result,
        )?;
    }
    Ok(())
}

fn append_runtime_metrics(
    plan: &dyn ExecutionPlan,
    statistics: &[CapturedStatistics],
    report: &mut Vec<OperatorReport>,
) {
    let statistics_entry = &statistics[report.len()];
    let (runtime_output_rows, runtime_output_bytes) =
        plan.metrics().map_or((None, None), |m| {
            (
                m.output_rows(),
                m.sum(|metric| matches!(metric.value(), MetricValue::OutputBytes(_)))
                    .map(|metric| metric.as_usize()),
            )
        });
    report.push(OperatorReport {
        path: statistics_entry.path.clone(),
        name: statistics_entry.name.clone(),
        estimated_rows: statistics_entry.estimated_rows.clone(),
        estimated_bytes: statistics_entry.estimated_bytes.clone(),
        runtime_output_rows,
        runtime_output_bytes,
        q_error: q_error(&statistics_entry.estimated_rows, runtime_output_rows),
    });
    for child in plan.children() {
        append_runtime_metrics(child.as_ref(), statistics, report);
    }
}

fn q_error(estimated: &StatisticValue, actual: Option<usize>) -> Option<QError> {
    let estimated = match estimated {
        StatisticValue::Exact(value) | StatisticValue::Inexact(value) => *value,
        StatisticValue::Absent => return None,
    };
    let actual = actual?;
    if estimated == 0 && actual == 0 {
        Some(QError::ExactZero)
    } else if estimated == 0 || actual == 0 {
        Some(QError::Infinite)
    } else {
        Some(QError::Finite(
            estimated.max(actual) as f64 / estimated.min(actual) as f64,
        ))
    }
}

fn statistic_value(value: Precision<usize>) -> StatisticValue {
    match value {
        Precision::Exact(value) => StatisticValue::Exact(value),
        Precision::Inexact(value) => StatisticValue::Inexact(value),
        Precision::Absent => StatisticValue::Absent,
    }
}

fn load_comparison_report(comparison_path: &Path) -> Result<Option<Vec<QueryReport>>> {
    if comparison_path.exists() {
        let previous = fs::read_to_string(comparison_path)?;
        let previous: Vec<QueryReport> = serde_json::from_str(&previous)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        Ok(Some(previous))
    } else {
        eprintln!(
            "No comparison report found at {}",
            comparison_path.display()
        );
        Ok(None)
    }
}

fn backup_previous_report(result_path: &Path) -> Result<()> {
    if result_path.exists() {
        let previous_path = result_path.with_extension("previous.json");
        fs::copy(result_path, previous_path)?;
    }
    Ok(())
}

fn store_report(result_path: &Path, report: &[QueryReport]) -> Result<()> {
    let parent = result_path.parent().expect("result path has a parent");
    fs::create_dir_all(parent)?;
    let temporary_path = result_path.with_extension("tmp");
    fs::write(&temporary_path, serialize_report(report)?)?;
    fs::rename(temporary_path, result_path)?;
    Ok(())
}

fn current_branch_name() -> String {
    Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|branch| branch.trim().to_string())
        .filter(|branch| branch != "HEAD")
        .unwrap_or_else(|| "detached".to_string())
}

fn normalize_branch_name(branch: &str) -> String {
    branch
        .chars()
        .map(|character| match character {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' => character,
            _ => '_',
        })
        .collect()
}

fn print_query_report(query: &QueryReport, previous: Option<&[QueryReport]>) {
    if !query.success {
        println!(
            "=== {} (statement {}) FAILED ===\n{}",
            query.query,
            query.statement,
            query.error.as_deref().unwrap_or("unknown error")
        );
        return;
    }
    let previous = previous.map(operator_reports);
    println!("=== {} ===", query.query);
    for operator in &query.operators {
        let identifier = OperatorIdentifier::new(query, operator);
        let previous_q_error = previous
            .as_ref()
            .and_then(|reports| reports.get(&identifier))
            .and_then(|operator| operator.q_error.as_ref());
        let depth = operator.path.matches('.').count();
        println!(
            "{:indent$}{}: rows={} vs {}, q-error: previous={}, current={}, change={}",
            "",
            operator.name,
            display_statistic(&operator.estimated_rows),
            display_option(operator.runtime_output_rows),
            display_q_error(previous_q_error),
            display_q_error(operator.q_error.as_ref()),
            display_improvement(previous_q_error, operator.q_error.as_ref()),
            indent = depth * 2,
        );
    }
}

fn display_option(value: Option<usize>) -> String {
    value.map_or_else(|| "?".to_string(), |value| value.to_string())
}

fn display_statistic(value: &StatisticValue) -> String {
    match value {
        StatisticValue::Exact(value) => format!("Exact({value})"),
        StatisticValue::Inexact(value) => format!("Inexact({value})"),
        StatisticValue::Absent => "Absent".to_string(),
    }
}

fn display_q_error(value: Option<&QError>) -> String {
    match value {
        Some(QError::Finite(value)) => format!("{value:.2}x"),
        Some(QError::Infinite) => "infinite".to_string(),
        Some(QError::ExactZero) => "exact zero".to_string(),
        None => "?".to_string(),
    }
}

fn display_improvement(previous: Option<&QError>, current: Option<&QError>) -> String {
    match (previous, current) {
        (Some(QError::Finite(previous)), Some(QError::Finite(current))) => {
            let improvement = (previous - current) / previous * 100.0;
            if improvement >= 20.0 {
                format!("✅ {improvement:.1}%")
            } else if improvement <= -20.0 {
                format!("❌ {improvement:.1}%")
            } else {
                format!("{improvement:.1}%")
            }
        }
        (Some(QError::Infinite), Some(QError::Infinite)) => "0.0%".to_string(),
        (Some(QError::ExactZero), Some(QError::ExactZero)) => "0.0%".to_string(),
        (Some(QError::Infinite | QError::Finite(_)), Some(QError::ExactZero)) => {
            "✅ exact zero".to_string()
        }
        (Some(QError::ExactZero), Some(QError::Infinite | QError::Finite(_))) => {
            "❌ no longer exact zero".to_string()
        }
        (Some(QError::Infinite), Some(QError::Finite(_))) => "✅ resolved".to_string(),
        (Some(QError::Finite(_)), Some(QError::Infinite)) => "❌ infinite".to_string(),
        _ => "?".to_string(),
    }
}

fn print_q_error_summary(
    reports: &[QueryReport],
    previous: Option<&[QueryReport]>,
    branch: &str,
    comparison_description: &str,
) {
    let q_errors = sorted_finite_q_errors(reports);
    let previous_q_errors = previous.map(sorted_finite_q_errors);
    let q_error_counts = q_error_counts(reports);
    let total = reports
        .iter()
        .map(|query| query.operators.len())
        .sum::<usize>();

    println!("=== q-error summary ===");
    println!("current: branch '{branch}'");
    println!("comparison: {comparison_description}");
    let failed = reports.iter().filter(|query| !query.success).count();
    println!(
        "queries: {} succeeded, {failed} failed",
        reports.len() - failed
    );
    println!(
        "evaluated operators: {}/{}",
        q_error_counts.evaluated, total
    );
    println!(
        "finite q-errors: {}, exact-zero estimates: {}, infinite q-errors: {}",
        q_error_counts.finite, q_error_counts.exact_zero, q_error_counts.infinite
    );
    for percentile in [50, 75, 95, 99] {
        let current = percentile_q_error(&q_errors, percentile);
        let previous = previous_q_errors
            .as_ref()
            .and_then(|q_errors| percentile_q_error(q_errors, percentile));
        println!(
            "p{percentile}: previous={}, current={}, change={}",
            display_q_error(previous),
            display_q_error(current),
            display_improvement(previous, current),
        );
    }
}

struct QErrorCounts {
    evaluated: usize,
    finite: usize,
    exact_zero: usize,
    infinite: usize,
}

fn q_error_counts(reports: &[QueryReport]) -> QErrorCounts {
    let mut counts = QErrorCounts {
        evaluated: 0,
        finite: 0,
        exact_zero: 0,
        infinite: 0,
    };
    for q_error in reports
        .iter()
        .flat_map(|query| &query.operators)
        .filter_map(|operator| operator.q_error.as_ref())
    {
        counts.evaluated += 1;
        match q_error {
            QError::Finite(_) => counts.finite += 1,
            QError::ExactZero => counts.exact_zero += 1,
            QError::Infinite => counts.infinite += 1,
        }
    }
    counts
}

fn sorted_finite_q_errors(reports: &[QueryReport]) -> Vec<&QError> {
    let mut q_errors = reports
        .iter()
        .flat_map(|query| &query.operators)
        .filter_map(|operator| match operator.q_error.as_ref() {
            Some(QError::Finite(_)) => operator.q_error.as_ref(),
            Some(QError::ExactZero | QError::Infinite) | None => None,
        })
        .collect::<Vec<_>>();
    q_errors.sort_by(|left, right| q_error_value(left).total_cmp(&q_error_value(right)));
    q_errors
}

fn percentile_q_error<'a>(
    q_errors: &[&'a QError],
    percentile: usize,
) -> Option<&'a QError> {
    if q_errors.is_empty() {
        return None;
    }
    let index = (percentile * q_errors.len())
        .div_ceil(100)
        .saturating_sub(1);
    q_errors.get(index).copied()
}

fn q_error_value(q_error: &QError) -> f64 {
    match q_error {
        QError::Finite(value) => *value,
        QError::Infinite => f64::INFINITY,
        QError::ExactZero => 1.0,
    }
}

fn operator_reports(
    reports: &[QueryReport],
) -> HashMap<OperatorIdentifier<'_>, &OperatorReport> {
    reports
        .iter()
        .flat_map(|query| {
            query
                .operators
                .iter()
                .map(move |operator| (OperatorIdentifier::new(query, operator), operator))
        })
        .collect()
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct OperatorIdentifier<'a> {
    query: &'a str,
    statement: usize,
    path: &'a str,
    name: &'a str,
}

impl<'a> OperatorIdentifier<'a> {
    fn new(query: &'a QueryReport, operator: &'a OperatorReport) -> Self {
        Self {
            query: query.query.as_str(),
            statement: query.statement,
            path: operator.path.as_str(),
            name: operator.name.as_str(),
        }
    }
}

fn serialize_report(report: &[QueryReport]) -> Result<String> {
    serde_json::to_string_pretty(report)
        .map_err(|error| DataFusionError::External(Box::new(error)))
}

fn sql_statements(sql: &str, options: &SqlParserOptions) -> Result<VecDeque<Statement>> {
    let dialect = dialect_from_str(options.dialect).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "Unsupported SQL dialect: {}. Available dialects: {}.",
            options.dialect,
            Dialect::available()
        ))
    })?;

    DFParserBuilder::new(sql)
        .with_dialect(dialect.as_ref())
        .with_recursion_limit(options.recursion_limit.get())
        .build()?
        .parse_statements()
}

fn query_files(path: &Path, query: Option<&str>) -> Result<Vec<PathBuf>> {
    if path.is_file() {
        return Ok(vec![path.to_path_buf()]);
    }
    let mut files = fs::read_dir(path)?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().is_some_and(|extension| extension == "sql"))
        .filter(|path| {
            query.is_none_or(|query| path.file_stem().is_some_and(|name| name == query))
        })
        .collect::<Vec<_>>();
    files.sort_by(|left, right| {
        query_number(left)
            .cmp(&query_number(right))
            .then_with(|| left.cmp(right))
    });
    Ok(files)
}

fn query_number(path: &Path) -> Option<usize> {
    static QUERY_NUMBER: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\d+").unwrap());
    let filename = path.file_stem()?.to_str()?;
    QUERY_NUMBER.find(filename)?.as_str().parse().ok()
}

async fn register_parquet_files(ctx: &SessionContext, path: &Path) -> Result<()> {
    let mut files = vec![];
    collect_parquet_files(path, &mut files)?;

    let mut tables = BTreeMap::<String, PathBuf>::new();
    for file in files {
        let parent = file.parent().expect("Parquet file has a parent directory");
        let relative_parent = parent
            .strip_prefix(path)
            .expect("Parquet file is within the data path");
        let (table, table_path) = if relative_parent.as_os_str().is_empty() {
            (
                file.file_stem()
                    .expect("Parquet file has a filename")
                    .to_string_lossy()
                    .to_string(),
                file,
            )
        } else {
            let table = relative_parent
                .components()
                .next()
                .expect("relative table directory has a component")
                .as_os_str()
                .to_string_lossy()
                .to_string();
            (table.clone(), path.join(table))
        };
        if let Some(existing_path) = tables.get(&table) {
            if existing_path != &table_path {
                return config_err!(
                    "Tried to register duplicate table {table} from '{}' and '{}'",
                    existing_path.display(),
                    table_path.display()
                );
            }
        } else {
            tables.insert(table, table_path);
        }
    }

    for (table, table_path) in tables {
        ctx.register_parquet(
            table,
            table_path.to_string_lossy(),
            ParquetReadOptions::default(),
        )
        .await?;
    }
    Ok(())
}

fn collect_parquet_files(path: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(path)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_parquet_files(&path, files)?;
        } else if path
            .extension()
            .is_some_and(|extension| extension == "parquet")
        {
            files.push(path);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn parses_statements_without_losing_external_table_details() {
        let statements = sql_statements(
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV \
             PARTITIONED BY (p1, p2) LOCATION 'foo.csv' \
             OPTIONS (format.delimiter '|'); SELECT ';'",
            &SessionConfig::new().options().sql_parser,
        )
        .unwrap();

        assert_eq!(statements.len(), 2);
        let Statement::CreateExternalTable(table) = &statements[0] else {
            panic!("expected CREATE EXTERNAL TABLE");
        };
        assert_eq!(table.columns.len(), 1);
        assert_eq!(table.table_partition_cols, ["p1", "p2"]);
        assert_eq!(table.options.len(), 1);
    }

    #[test]
    fn parses_statements_with_session_sql_dialect() {
        let sql = "# MySQL comment\nSELECT 1";
        assert!(sql_statements(sql, &SessionConfig::new().options().sql_parser).is_err());

        let mut config = SessionConfig::new();
        config.options_mut().sql_parser.dialect = Dialect::MySQL;

        let statements = sql_statements(sql, &config.options().sql_parser).unwrap();

        assert_eq!(statements.len(), 1);
    }

    #[tokio::test]
    async fn applies_session_changing_statements_before_reporting_queries() {
        let directory = tempdir().unwrap();
        let options = RunOpt {
            query: None,
            compare: None,
            path: directory.path().to_path_buf(),
            query_path: directory.path().to_path_buf(),
        };
        let ctx = SessionContext::new();
        let mut statements = sql_statements(
            "CREATE TABLE x AS VALUES (1); SELECT * FROM x",
            &SessionConfig::new().options().sql_parser,
        )
        .unwrap();

        let reports = options
            .report_statement(&ctx, statements.pop_front().unwrap())
            .await
            .unwrap();
        assert!(reports.is_empty());

        let reports = options
            .report_statement(&ctx, statements.pop_front().unwrap())
            .await
            .unwrap();
        assert!(!reports.is_empty());
    }

    #[test]
    fn persists_failed_reports() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("statistics.json");
        let reports = vec![QueryReport {
            query: "q1".to_string(),
            statement: 1,
            operators: vec![],
            success: false,
            error: Some("expected failure".to_string()),
        }];

        store_report(&path, &reports).unwrap();
        assert_eq!(load_comparison_report(&path).unwrap(), Some(reports));
    }

    #[tokio::test]
    async fn rejects_duplicate_table_names() {
        let directory = tempdir().unwrap();
        let path = directory.path();
        fs::write(path.join("foo.parquet"), b"").unwrap();
        fs::create_dir(path.join("foo")).unwrap();
        fs::write(path.join("foo/part.parquet"), b"").unwrap();

        let error = register_parquet_files(&SessionContext::new(), path)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("duplicate table foo"));
    }
}
