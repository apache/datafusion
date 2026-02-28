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

use clap::{ColorChoice, Parser, ValueEnum};
use datafusion::common::instant::Instant;
use datafusion::common::utils::get_available_parallelism;
use datafusion::common::{DataFusionError, Result, exec_datafusion_err, exec_err};
use datafusion_sqllogictest::{
    CurrentlyExecutingSqlTracker, DataFusion, DataFusionSubstraitRoundTrip, Filter,
    TestContext, df_value_validator, read_dir_recursive, setup_scratch_dir,
    should_skip_file, should_skip_record, value_normalizer,
};
use futures::stream::StreamExt;
use indicatif::{
    HumanDuration, MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle,
};
use itertools::Itertools;
use log::Level::Info;
use log::{info, log_enabled};
use sqllogictest::{
    AsyncDB, Condition, MakeConnection, Normalizer, Record, Validator, parse_file,
    strict_column_validator,
};

#[cfg(feature = "postgres")]
use crate::postgres_container::{
    initialize_postgres_container, terminate_postgres_container,
};
use datafusion::common::runtime::SpawnedTask;
use futures::FutureExt;
use std::ffi::OsStr;
use std::fs;
use std::io::{IsTerminal, stderr, stdout};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[cfg(feature = "postgres")]
mod postgres_container;

const TEST_DIRECTORY: &str = "test_files/";
const DATAFUSION_TESTING_TEST_DIRECTORY: &str = "../../datafusion-testing/data/";
const PG_COMPAT_FILE_PREFIX: &str = "pg_compat_";
const SQLITE_PREFIX: &str = "sqlite";
const ERRS_PER_FILE_LIMIT: usize = 10;
const TIMING_DEBUG_SLOW_FILES_ENV: &str = "SLT_TIMING_DEBUG_SLOW_FILES";

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum TimingSummaryMode {
    Auto,
    Off,
    Top,
    Full,
}

#[derive(Debug)]
struct FileTiming {
    relative_path: PathBuf,
    elapsed: Duration,
}

pub fn main() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run_tests())
}

fn sqlite_value_validator(
    normalizer: Normalizer,
    actual: &[Vec<String>],
    expected: &[String],
) -> bool {
    let normalized_expected = expected.iter().map(normalizer).collect::<Vec<_>>();
    let normalized_actual = actual
        .iter()
        .map(|strs| strs.iter().map(normalizer).join(" "))
        .collect_vec();

    if log_enabled!(Info) && normalized_actual != normalized_expected {
        info!("sqlite validation failed. actual vs expected:");
        for i in 0..normalized_actual.len() {
            info!("[{i}] {}<eol>", normalized_actual[i]);
            info!(
                "[{i}] {}<eol>",
                if normalized_expected.len() >= i {
                    &normalized_expected[i]
                } else {
                    "No more results"
                }
            );
        }
    }

    normalized_actual == normalized_expected
}

async fn run_tests() -> Result<()> {
    // Enable logging (e.g. set RUST_LOG=debug to see debug logs)
    env_logger::init();

    let options: Options = Parser::parse();
    let timing_debug_slow_files = is_env_truthy(TIMING_DEBUG_SLOW_FILES_ENV);
    if options.list {
        // nextest parses stdout, so print messages to stderr
        eprintln!("NOTICE: --list option unsupported, quitting");
        // return Ok, not error so that tools like nextest which are listing all
        // workspace tests (by running `cargo test ... --list --format terse`)
        // do not fail when they encounter this binary. Instead, print nothing
        // to stdout and return OK so they can continue listing other tests.
        return Ok(());
    }

    options.warn_on_ignored();

    // Print parallelism info for debugging CI performance
    eprintln!(
        "Running with {} test threads (available parallelism: {})",
        options.test_threads,
        get_available_parallelism()
    );

    #[cfg(feature = "postgres")]
    initialize_postgres_container(&options).await?;

    // Run all tests in parallel, reporting failures at the end
    //
    // Doing so is safe because each slt file runs with its own
    // `SessionContext` and should not have side effects (like
    // modifying shared state like `/tmp/`)
    let m = MultiProgress::with_draw_target(ProgressDrawTarget::stderr_with_hz(1));
    let m_style = ProgressStyle::with_template(
        "[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}",
    )
    .unwrap()
    .progress_chars("##-");

    let colored_output = options.is_colored();

    let start = Instant::now();

    let test_files = read_test_files(&options)?;

    // Perform scratch file sanity check
    let scratch_errors = scratch_file_check(&test_files)?;
    if !scratch_errors.is_empty() {
        eprintln!("Scratch file sanity check failed:");
        for error in &scratch_errors {
            eprintln!("  {error}");
        }

        eprintln!(
            "\nTemporary file check failed. Please ensure that within each test file, any scratch file created is placed under a folder with the same name as the test file (without extension).\nExample: inside `join.slt`, temporary files must be created under `.../scratch/join/`\n"
        );

        return exec_err!("sqllogictests scratch file check failed");
    }

    let num_tests = test_files.len();
    // For CI environments without TTY, print progress periodically
    let is_ci = !stderr().is_terminal();
    let completed_count = Arc::new(AtomicUsize::new(0));

    let file_results: Vec<_> = futures::stream::iter(test_files)
        .map(|test_file| {
            let validator = if options.include_sqlite
                && test_file.relative_path.starts_with(SQLITE_PREFIX)
            {
                sqlite_value_validator
            } else {
                df_value_validator
            };

            let m_clone = m.clone();
            let m_style_clone = m_style.clone();
            let filters = options.filters.clone();

            let relative_path = test_file.relative_path.clone();
            let relative_path_for_timing = test_file.relative_path.clone();

            let currently_running_sql_tracker = CurrentlyExecutingSqlTracker::new();
            let currently_running_sql_tracker_clone =
                currently_running_sql_tracker.clone();
            let file_start = Instant::now();
            SpawnedTask::spawn(async move {
                let result = match (
                    options.postgres_runner,
                    options.complete,
                    options.substrait_round_trip,
                ) {
                    (_, _, true) => {
                        run_test_file_substrait_round_trip(
                            test_file,
                            validator,
                            m_clone,
                            m_style_clone,
                            filters.as_ref(),
                            currently_running_sql_tracker_clone,
                            colored_output,
                        )
                        .await
                    }
                    (false, false, _) => {
                        run_test_file(
                            test_file,
                            validator,
                            m_clone,
                            m_style_clone,
                            filters.as_ref(),
                            currently_running_sql_tracker_clone,
                            colored_output,
                        )
                        .await
                    }
                    (false, true, _) => {
                        run_complete_file(
                            test_file,
                            validator,
                            m_clone,
                            m_style_clone,
                            currently_running_sql_tracker_clone,
                        )
                        .await
                    }
                    (true, false, _) => {
                        run_test_file_with_postgres(
                            test_file,
                            validator,
                            m_clone,
                            m_style_clone,
                            filters.as_ref(),
                            currently_running_sql_tracker_clone,
                        )
                        .await
                    }
                    (true, true, _) => {
                        run_complete_file_with_postgres(
                            test_file,
                            validator,
                            m_clone,
                            m_style_clone,
                            currently_running_sql_tracker_clone,
                        )
                        .await
                    }
                };

                let elapsed = file_start.elapsed();
                if timing_debug_slow_files && elapsed.as_secs() > 30 {
                    eprintln!(
                        "Slow file: {} took {:.1}s",
                        relative_path_for_timing.display(),
                        elapsed.as_secs_f64()
                    );
                }

                (result, elapsed)
            })
            .join()
            .map(move |result| {
                let elapsed = match &result {
                    Ok((_, elapsed)) => *elapsed,
                    Err(_) => Duration::ZERO,
                };

                (
                    result.map(|(thread_result, _)| thread_result),
                    relative_path,
                    currently_running_sql_tracker,
                    elapsed,
                )
            })
        })
        // run up to num_cpus streams in parallel
        .buffer_unordered(options.test_threads)
        .inspect({
            let completed_count = Arc::clone(&completed_count);
            move |_| {
                let completed = completed_count.fetch_add(1, Ordering::Relaxed) + 1;
                // In CI (no TTY), print progress every 10% or every 50 files
                if is_ci && (completed.is_multiple_of(50) || completed == num_tests) {
                    eprintln!(
                        "Progress: {}/{} files completed ({:.0}%)",
                        completed,
                        num_tests,
                        (completed as f64 / num_tests as f64) * 100.0
                    );
                }
            }
        })
        .collect()
        .await;

    let mut file_timings: Vec<FileTiming> = file_results
        .iter()
        .map(|(_, path, _, elapsed)| FileTiming {
            relative_path: path.clone(),
            elapsed: *elapsed,
        })
        .collect();

    file_timings.sort_by(|a, b| {
        b.elapsed
            .cmp(&a.elapsed)
            .then_with(|| a.relative_path.cmp(&b.relative_path))
    });

    print_timing_summary(&options, &m, is_ci, &file_timings)?;

    let errors: Vec<_> = file_results
        .into_iter()
        .filter_map(|(result, test_file_path, current_sql, _)| {
            // Filter out any Ok() leaving only the DataFusionErrors
            match result {
                Err(e) => {
                    let error = DataFusionError::External(Box::new(e));
                    let current_sql = current_sql.get_currently_running_sqls();

                    if current_sql.is_empty() {
                        Some(error.context(format!(
                            "failure in {} with no currently running sql tracked",
                            test_file_path.display()
                        )))
                    } else if current_sql.len() == 1 {
                        let sql = &current_sql[0];
                        Some(error.context(format!(
                            "failure in {} for sql {sql}",
                            test_file_path.display()
                        )))
                    } else {
                        let sqls = current_sql
                            .iter()
                            .enumerate()
                            .map(|(i, sql)| format!("\n[{}]: {}", i + 1, sql))
                            .collect::<String>();
                        Some(error.context(format!(
                            "failure in {} for multiple currently running sqls: {}",
                            test_file_path.display(),
                            sqls
                        )))
                    }
                }
                Ok(thread_result) => thread_result.err(),
            }
        })
        .collect();

    m.println(format!(
        "Completed {} test files in {}",
        num_tests,
        HumanDuration(start.elapsed())
    ))?;

    #[cfg(feature = "postgres")]
    terminate_postgres_container().await?;

    // report on any errors
    if !errors.is_empty() {
        for e in &errors {
            println!("{e}");
        }
        exec_err!("{} failures", errors.len())
    } else {
        Ok(())
    }
}

fn print_timing_summary(
    options: &Options,
    progress: &MultiProgress,
    is_ci: bool,
    file_timings: &[FileTiming],
) -> Result<()> {
    let mode = options.timing_summary_mode(is_ci);
    if mode == TimingSummaryMode::Off || file_timings.is_empty() {
        return Ok(());
    }

    let top_n = options.timing_top_n;
    debug_assert!(matches!(
        mode,
        TimingSummaryMode::Top | TimingSummaryMode::Full
    ));
    let count = if mode == TimingSummaryMode::Full {
        file_timings.len()
    } else {
        top_n
    };

    progress.println("Per-file elapsed summary (deterministic):")?;
    for (idx, timing) in file_timings.iter().take(count).enumerate() {
        progress.println(format!(
            "{:>3}. {:>8.3}s  {}",
            idx + 1,
            timing.elapsed.as_secs_f64(),
            timing.relative_path.display()
        ))?;
    }

    if mode != TimingSummaryMode::Full && file_timings.len() > count {
        progress.println(format!(
            "... {} more files omitted (use --timing-summary full to show all)",
            file_timings.len() - count
        ))?;
    }

    Ok(())
}

fn is_env_truthy(name: &str) -> bool {
    std::env::var_os(name)
        .and_then(|value| value.into_string().ok())
        .is_some_and(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
}

fn parse_timing_top_n(arg: &str) -> std::result::Result<usize, String> {
    let parsed = arg
        .parse::<usize>()
        .map_err(|error| format!("invalid value '{arg}': {error}"))?;
    if parsed == 0 {
        return Err("must be >= 1".to_string());
    }
    Ok(parsed)
}

async fn run_test_file_substrait_round_trip(
    test_file: TestFile,
    validator: Validator,
    mp: MultiProgress,
    mp_style: ProgressStyle,
    filters: &[Filter],
    currently_executing_sql_tracker: CurrentlyExecutingSqlTracker,
    colored_output: bool,
) -> Result<()> {
    let TestFile {
        path,
        relative_path,
    } = test_file;
    let Some(test_ctx) = TestContext::try_new_for_test_file(&relative_path).await else {
        info!("Skipping: {}", path.display());
        return Ok(());
    };
    setup_scratch_dir(&relative_path)?;

    let count: u64 = get_record_count(&path, "DatafusionSubstraitRoundTrip".to_string());
    let pb = mp.add(ProgressBar::new(count));

    pb.set_style(mp_style);
    pb.set_message(format!("{:?}", &relative_path));

    let mut runner = sqllogictest::Runner::new(|| async {
        Ok(DataFusionSubstraitRoundTrip::new(
            test_ctx.session_ctx().clone(),
            relative_path.clone(),
            pb.clone(),
        )
        .with_currently_executing_sql_tracker(currently_executing_sql_tracker.clone()))
    });
    runner.add_label("DatafusionSubstraitRoundTrip");
    runner.with_column_validator(strict_column_validator);
    runner.with_normalizer(value_normalizer);
    runner.with_validator(validator);
    let res = run_file_in_runner(path, runner, filters, colored_output).await;
    pb.finish_and_clear();
    res
}

async fn run_test_file(
    test_file: TestFile,
    validator: Validator,
    mp: MultiProgress,
    mp_style: ProgressStyle,
    filters: &[Filter],
    currently_executing_sql_tracker: CurrentlyExecutingSqlTracker,
    colored_output: bool,
) -> Result<()> {
    let TestFile {
        path,
        relative_path,
    } = test_file;
    let Some(test_ctx) = TestContext::try_new_for_test_file(&relative_path).await else {
        info!("Skipping: {}", path.display());
        return Ok(());
    };
    setup_scratch_dir(&relative_path)?;

    let count: u64 = get_record_count(&path, "Datafusion".to_string());
    let pb = mp.add(ProgressBar::new(count));

    pb.set_style(mp_style);
    pb.set_message(format!("{:?}", &relative_path));

    let mut runner = sqllogictest::Runner::new(|| async {
        Ok(DataFusion::new(
            test_ctx.session_ctx().clone(),
            relative_path.clone(),
            pb.clone(),
        )
        .with_currently_executing_sql_tracker(currently_executing_sql_tracker.clone()))
    });
    runner.add_label("Datafusion");
    runner.with_column_validator(strict_column_validator);
    runner.with_normalizer(value_normalizer);
    runner.with_validator(validator);
    let result = run_file_in_runner(path, runner, filters, colored_output).await;
    pb.finish_and_clear();
    result
}

async fn run_file_in_runner<D: AsyncDB, M: MakeConnection<Conn = D>>(
    path: PathBuf,
    mut runner: sqllogictest::Runner<D, M>,
    filters: &[Filter],
    colored_output: bool,
) -> Result<()> {
    let path = path.canonicalize()?;
    let records =
        parse_file(&path).map_err(|e| DataFusionError::External(Box::new(e)))?;
    let mut errs = vec![];
    for record in records.into_iter() {
        if let Record::Halt { .. } = record {
            break;
        }
        if should_skip_record::<D>(&record, filters) {
            continue;
        }
        if let Err(err) = runner.run_async(record).await {
            if colored_output {
                errs.push(format!("{}", err.display(true)));
            } else {
                errs.push(format!("{err}"));
            }
        }
    }

    if !errs.is_empty() {
        let mut msg = format!("{} errors in file {}\n\n", errs.len(), path.display());
        for (i, err) in errs.iter().enumerate() {
            if i >= ERRS_PER_FILE_LIMIT {
                msg.push_str(&format!(
                    "... other {} errors in {} not shown ...\n\n",
                    errs.len() - ERRS_PER_FILE_LIMIT,
                    path.display()
                ));
                break;
            }
            msg.push_str(&format!("{}. {err}\n\n", i + 1));
        }
        return Err(DataFusionError::External(msg.into()));
    }

    Ok(())
}

#[expect(clippy::needless_pass_by_value)]
fn get_record_count(path: &PathBuf, label: String) -> u64 {
    let records: Vec<Record<<DataFusion as AsyncDB>::ColumnType>> =
        parse_file(path).unwrap();
    let mut count: u64 = 0;

    records.iter().for_each(|rec| match rec {
        Record::Query { conditions, .. } => {
            if conditions.is_empty()
                || !conditions.contains(&Condition::SkipIf {
                    label: label.clone(),
                })
                || conditions.contains(&Condition::OnlyIf {
                    label: label.clone(),
                })
            {
                count += 1;
            }
        }
        Record::Statement { conditions, .. } => {
            if conditions.is_empty()
                || !conditions.contains(&Condition::SkipIf {
                    label: label.clone(),
                })
                || conditions.contains(&Condition::OnlyIf {
                    label: label.clone(),
                })
            {
                count += 1;
            }
        }
        _ => {}
    });

    count
}

#[cfg(feature = "postgres")]
async fn run_test_file_with_postgres(
    test_file: TestFile,
    validator: Validator,
    mp: MultiProgress,
    mp_style: ProgressStyle,
    filters: &[Filter],
    currently_executing_sql_tracker: CurrentlyExecutingSqlTracker,
) -> Result<()> {
    use datafusion_sqllogictest::Postgres;
    let TestFile {
        path,
        relative_path,
    } = test_file;
    setup_scratch_dir(&relative_path)?;

    let count: u64 = get_record_count(&path, "postgresql".to_string());
    let pb = mp.add(ProgressBar::new(count));

    pb.set_style(mp_style);
    pb.set_message(format!("{:?}", &relative_path));

    let mut runner = sqllogictest::Runner::new(|| {
        Postgres::connect_with_tracked_sql(
            relative_path.clone(),
            pb.clone(),
            currently_executing_sql_tracker.clone(),
        )
    });
    runner.add_label("postgres");
    runner.with_column_validator(strict_column_validator);
    runner.with_normalizer(value_normalizer);
    runner.with_validator(validator);
    let result = run_file_in_runner(path, runner, filters, false).await;
    pb.finish_and_clear();
    result
}

#[cfg(not(feature = "postgres"))]
async fn run_test_file_with_postgres(
    _test_file: TestFile,
    _validator: Validator,
    _mp: MultiProgress,
    _mp_style: ProgressStyle,
    _filters: &[Filter],
    _currently_executing_sql_tracker: CurrentlyExecutingSqlTracker,
) -> Result<()> {
    use datafusion::common::plan_err;
    plan_err!("Can not run with postgres as postgres feature is not enabled")
}

async fn run_complete_file(
    test_file: TestFile,
    validator: Validator,
    mp: MultiProgress,
    mp_style: ProgressStyle,
    currently_executing_sql_tracker: CurrentlyExecutingSqlTracker,
) -> Result<()> {
    let TestFile {
        path,
        relative_path,
    } = test_file;

    info!("Using complete mode to complete: {}", path.display());

    let Some(test_ctx) = TestContext::try_new_for_test_file(&relative_path).await else {
        info!("Skipping: {}", path.display());
        return Ok(());
    };
    setup_scratch_dir(&relative_path)?;

    let count: u64 = get_record_count(&path, "Datafusion".to_string());
    let pb = mp.add(ProgressBar::new(count));

    pb.set_style(mp_style);
    pb.set_message(format!("{:?}", &relative_path));

    let mut runner = sqllogictest::Runner::new(|| async {
        Ok(DataFusion::new(
            test_ctx.session_ctx().clone(),
            relative_path.clone(),
            pb.clone(),
        )
        .with_currently_executing_sql_tracker(currently_executing_sql_tracker.clone()))
    });

    let col_separator = " ";
    let res = runner
        .update_test_file(
            path,
            col_separator,
            validator,
            value_normalizer,
            strict_column_validator,
        )
        .await
        // Can't use e directly because it isn't marked Send, so turn it into a string.
        .map_err(|e| exec_datafusion_err!("Error completing {relative_path:?}: {e}"));

    pb.finish_and_clear();

    res
}

#[cfg(feature = "postgres")]
async fn run_complete_file_with_postgres(
    test_file: TestFile,
    validator: Validator,
    mp: MultiProgress,
    mp_style: ProgressStyle,
    currently_executing_sql_tracker: CurrentlyExecutingSqlTracker,
) -> Result<()> {
    use datafusion_sqllogictest::Postgres;
    let TestFile {
        path,
        relative_path,
    } = test_file;
    info!(
        "Using complete mode to complete with Postgres runner: {}",
        path.display()
    );
    setup_scratch_dir(&relative_path)?;

    let count: u64 = get_record_count(&path, "postgresql".to_string());
    let pb = mp.add(ProgressBar::new(count));

    pb.set_style(mp_style);
    pb.set_message(format!("{:?}", &relative_path));

    let mut runner = sqllogictest::Runner::new(|| {
        Postgres::connect_with_tracked_sql(
            relative_path.clone(),
            pb.clone(),
            currently_executing_sql_tracker.clone(),
        )
    });
    runner.add_label("postgres");
    runner.with_column_validator(strict_column_validator);
    runner.with_normalizer(value_normalizer);
    runner.with_validator(validator);

    let col_separator = " ";
    let res = runner
        .update_test_file(
            path,
            col_separator,
            validator,
            value_normalizer,
            strict_column_validator,
        )
        .await
        // Can't use e directly because it isn't marked Send, so turn it into a string.
        .map_err(|e| exec_datafusion_err!("Error completing {relative_path:?}: {e}"));

    pb.finish_and_clear();

    res
}

#[cfg(not(feature = "postgres"))]
async fn run_complete_file_with_postgres(
    _test_file: TestFile,
    _validator: Validator,
    _mp: MultiProgress,
    _mp_style: ProgressStyle,
    _currently_executing_sql_tracker: CurrentlyExecutingSqlTracker,
) -> Result<()> {
    use datafusion::common::plan_err;
    plan_err!("Can not run with postgres as postgres feature is not enabled")
}

/// Represents a parsed test file
#[derive(Debug)]
struct TestFile {
    /// The absolute path to the file
    pub path: PathBuf,
    /// The relative path of the file (used for display)
    pub relative_path: PathBuf,
}

impl TestFile {
    fn new(path: PathBuf) -> Self {
        let p = path.to_string_lossy();
        let relative_path = PathBuf::from(if p.starts_with(TEST_DIRECTORY) {
            p.strip_prefix(TEST_DIRECTORY).unwrap()
        } else if p.starts_with(DATAFUSION_TESTING_TEST_DIRECTORY) {
            p.strip_prefix(DATAFUSION_TESTING_TEST_DIRECTORY).unwrap()
        } else {
            ""
        });

        Self {
            path,
            relative_path,
        }
    }

    fn is_slt_file(&self) -> bool {
        self.path.extension() == Some(OsStr::new("slt"))
    }

    fn check_sqlite(&self, options: &Options) -> bool {
        if !self.relative_path.starts_with(SQLITE_PREFIX) {
            return true;
        }

        options.include_sqlite
    }

    fn check_tpch(&self, options: &Options) -> bool {
        if !self.relative_path.starts_with("tpch") {
            return true;
        }

        options.include_tpch
    }
}

fn read_test_files(options: &Options) -> Result<Vec<TestFile>> {
    let mut paths = read_dir_recursive(TEST_DIRECTORY)?
        .into_iter()
        .map(TestFile::new)
        .filter(|f| options.check_test_file(&f.path))
        .filter(|f| f.is_slt_file())
        .filter(|f| f.check_tpch(options))
        .filter(|f| f.check_sqlite(options))
        .filter(|f| options.check_pg_compat_file(f.path.as_path()))
        .collect::<Vec<_>>();
    if options.include_sqlite {
        let mut sqlite_paths = read_dir_recursive(DATAFUSION_TESTING_TEST_DIRECTORY)?
            .into_iter()
            .map(TestFile::new)
            .filter(|f| options.check_test_file(&f.path))
            .filter(|f| f.is_slt_file())
            .filter(|f| f.check_sqlite(options))
            .filter(|f| options.check_pg_compat_file(f.path.as_path()))
            .collect::<Vec<_>>();

        paths.append(&mut sqlite_paths)
    }

    Ok(paths)
}

/// Parsed command line options
///
/// This structure attempts to mimic the command line options of the built-in rust test runner
/// accepted by IDEs such as CLion that pass arguments
///
/// See <https://github.com/apache/datafusion/issues/8287> for more details
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about= None)]
struct Options {
    #[clap(long, help = "Auto complete mode to fill out expected results")]
    complete: bool,

    #[clap(
        long,
        env = "PG_COMPAT",
        help = "Run Postgres compatibility tests with Postgres runner"
    )]
    postgres_runner: bool,

    #[clap(
        long,
        conflicts_with = "complete",
        conflicts_with = "postgres_runner",
        help = "Before executing each query, convert its logical plan to Substrait and from Substrait back to its logical plan"
    )]
    substrait_round_trip: bool,

    #[clap(long, env = "INCLUDE_SQLITE", help = "Include sqlite files")]
    include_sqlite: bool,

    #[clap(long, env = "INCLUDE_TPCH", help = "Include tpch files")]
    include_tpch: bool,

    #[clap(
        action,
        help = "test filter (substring match on filenames with optional :{line_number} suffix)"
    )]
    filters: Vec<Filter>,

    #[clap(
        long,
        help = "IGNORED (for compatibility with built in rust test runner)"
    )]
    format: Option<String>,

    #[clap(
        short = 'Z',
        long,
        help = "IGNORED (for compatibility with built in rust test runner)"
    )]
    z_options: Option<String>,

    #[clap(
        long,
        help = "IGNORED (for compatibility with built in rust test runner)"
    )]
    show_output: bool,

    #[clap(
        long,
        help = "Quits immediately, not listing anything (for compatibility with built-in rust test runner)"
    )]
    list: bool,

    #[clap(
        long,
        help = "IGNORED (for compatibility with built-in rust test runner)"
    )]
    ignored: bool,

    #[clap(
        long,
        help = "IGNORED (for compatibility with built-in rust test runner)"
    )]
    nocapture: bool,

    #[clap(
        long,
        help = "Number of threads used for running tests in parallel",
        default_value_t = get_available_parallelism()
    )]
    test_threads: usize,

    #[clap(
        long,
        env = "SLT_TIMING_SUMMARY",
        value_enum,
        default_value_t = TimingSummaryMode::Auto,
        help = "Per-file timing summary mode: auto|off|top|full"
    )]
    timing_summary: TimingSummaryMode,

    #[clap(
        long,
        env = "SLT_TIMING_TOP_N",
        default_value_t = 10,
        value_parser = parse_timing_top_n,
        help = "Number of files to show when timing summary mode is auto/top (must be >= 1)"
    )]
    timing_top_n: usize,

    #[clap(
        long,
        value_name = "MODE",
        help = "Control colored output",
        default_value_t = ColorChoice::Auto
    )]
    color: ColorChoice,
}

impl Options {
    fn timing_summary_mode(&self, is_ci: bool) -> TimingSummaryMode {
        match self.timing_summary {
            TimingSummaryMode::Auto => {
                if is_ci {
                    TimingSummaryMode::Top
                } else {
                    TimingSummaryMode::Off
                }
            }
            mode => mode,
        }
    }

    /// Because this test can be run as a cargo test, commands like
    ///
    /// ```shell
    /// cargo test foo
    /// ```
    ///
    /// Will end up passing `foo` as a command line argument.
    ///
    /// To be compatible with this, treat the command line arguments as a
    /// filter and that does a substring match on each input.  returns
    /// true f this path should be run
    fn check_test_file(&self, path: &Path) -> bool {
        !should_skip_file(path, &self.filters)
    }

    /// Postgres runner executes only tests in files with specific names or in
    /// specific folders
    fn check_pg_compat_file(&self, path: &Path) -> bool {
        let file_name = path.file_name().unwrap().to_str().unwrap().to_string();
        !self.postgres_runner
            || file_name.starts_with(PG_COMPAT_FILE_PREFIX)
            || (self.include_sqlite && path.to_string_lossy().contains(SQLITE_PREFIX))
    }

    /// Logs warning messages to stdout if any ignored options are passed
    fn warn_on_ignored(&self) {
        if self.format.is_some() {
            eprintln!("WARNING: Ignoring `--format` compatibility option");
        }

        if self.z_options.is_some() {
            eprintln!("WARNING: Ignoring `-Z` compatibility option");
        }

        if self.show_output {
            eprintln!("WARNING: Ignoring `--show-output` compatibility option");
        }
    }

    /// Determine if colour output should be enabled, respecting --color, NO_COLOR, CARGO_TERM_COLOR, and terminal detection
    fn is_colored(&self) -> bool {
        // NO_COLOR takes precedence
        if std::env::var_os("NO_COLOR").is_some() {
            return false;
        }

        match self.color {
            ColorChoice::Always => true,
            ColorChoice::Never => false,
            ColorChoice::Auto => {
                // CARGO_TERM_COLOR takes precedence over auto-detection
                let cargo_term_color = <ColorChoice as FromStr>::from_str(
                    &std::env::var("CARGO_TERM_COLOR")
                        .unwrap_or_else(|_| "auto".to_string()),
                )
                .unwrap_or(ColorChoice::Auto);
                match cargo_term_color {
                    ColorChoice::Always => true,
                    ColorChoice::Never => false,
                    ColorChoice::Auto => {
                        // Auto for both CLI argument and CARGO_TERM_COLOR,
                        // then use colors by default for non-dumb terminals
                        stdout().is_terminal()
                            && std::env::var("TERM").unwrap_or_default() != "dumb"
                    }
                }
            }
        }
    }
}

/// Performs scratch file check for all test files.
///
/// Scratch file rule: In each .slt test file, the temporary file created must
/// be under a folder that is has the same name as the test file.
/// e.g. In `join.slt`, temporary files must be created under `.../scratch/join/`
///
/// See: <https://github.com/apache/datafusion/tree/main/datafusion/sqllogictest#running-tests-scratchdir>
///
/// This function searches for `scratch/[target]/...` patterns and verifies
/// that the target matches the file name.
///
/// Returns a vector of error strings for incorrectly created scratch files.
fn scratch_file_check(test_files: &[TestFile]) -> Result<Vec<String>> {
    let mut errors = Vec::new();

    // Search for any scratch/[target]/... patterns and check if they match the file name
    let scratch_pattern = regex::Regex::new(r"scratch/([^/]+)/").unwrap();

    for test_file in test_files {
        // Get the file content
        let content = match fs::read_to_string(&test_file.path) {
            Ok(content) => content,
            Err(e) => {
                errors.push(format!(
                    "Failed to read file {}: {}",
                    test_file.path.display(),
                    e
                ));
                continue;
            }
        };

        // Get the expected target name (file name without extension)
        let expected_target = match test_file.path.file_stem() {
            Some(stem) => stem.to_string_lossy().to_string(),
            None => {
                errors.push(format!("File {} has no stem", test_file.path.display()));
                continue;
            }
        };

        let lines: Vec<&str> = content.lines().collect();

        for (line_num, line) in lines.iter().enumerate() {
            if let Some(captures) = scratch_pattern.captures(line)
                && let Some(found_target) = captures.get(1)
            {
                let found_target = found_target.as_str();
                if found_target != expected_target {
                    errors.push(format!(
                        "File {}:{}: scratch target '{}' does not match file name '{}'",
                        test_file.path.display(),
                        line_num + 1,
                        found_target,
                        expected_target
                    ));
                }
            }
        }
    }

    Ok(errors)
}
