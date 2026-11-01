// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! CI steps that can be inspected or run through `cargo xtask`.

use crate::Result;
use crate::command::{CommandSpec, EnvReader, system_env};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

type StepResult<T> = std::result::Result<T, StepError>;
type StepRunner = fn(&StepContext, &[String]) -> StepResult<CommandSpec>;

const CI_COMMAND: &str = "cargo xtask ci step";
const CI_SHORTCUT: &str = "cargo ci-step";
const TPCH_DATA_DIR: &str = "datafusion/sqllogictest/test_files/tpch/data";
/// The `datafusion-physical-plan` unit test that the `spill-pool-fuzz` step runs.
const SPILL_POOL_FUZZ_TEST: &str = "spill::spill_pool::tests::spill_pool_scenario_fuzz";

/// Metadata and implementation for one CI step.
///
/// Adding an entry to `CI_STEPS` makes the step dispatchable and includes its
/// usage and description in the generated help text.
struct StepInfo {
    command: &'static str,
    help_usage: &'static str,
    help_examples: &'static [&'static str],
    help_description: &'static str,
    runner: StepRunner,
}

static CI_STEPS: &[StepInfo] = &[
    StepInfo {
        command: "check",
        help_usage: "check <workspace|package> [default|no-default|feature] [--explain]",
        help_examples: &["check workspace", "check datafusion default"],
        help_description: "Check workspace or package compilation",
        runner: StepContext::run_check,
    },
    StepInfo {
        command: "test",
        help_usage: "test <workspace|slt-coverage|cli|doctest|ffi|microbenchmarks|benchmark-plan|benchmark-sqllogic|postgres|substrait|substrait-optimized|extended|spill-pool-fuzz|hash-collisions|sqlite> [--explain]",
        help_examples: &["test cli", "test workspace"],
        help_description: "Run a CI test suite",
        runner: StepContext::run_test,
    },
];

pub(crate) fn help() -> String {
    let mut help = format!(
        "DataFusion CI commands\n\nUsage:\n  {CI_COMMAND} <step-name> [args] [--explain]\n\nExamples:\n",
    );
    for step in CI_STEPS {
        if let Some(example) = step.help_examples.first() {
            writeln!(help, "  {CI_COMMAND} {example}").ok();
        }
    }
    writeln!(help, "  {CI_COMMAND} test workspace --explain").ok();

    let command_width = CI_STEPS
        .iter()
        .map(|step| step.command.len())
        .max()
        .unwrap_or_default();
    help.push_str("\nAvailable steps:\n");
    for step in CI_STEPS {
        writeln!(
            help,
            "  {:command_width$}  {}",
            step.command, step.help_description
        )
        .ok();
    }

    write!(help,
        "\nShortcut:\n  # `{CI_SHORTCUT}` is short for `{CI_COMMAND}`.\n  {CI_SHORTCUT} check workspace\n\nFor more details:\n  {CI_COMMAND} check --help\n"
    ).ok();
    help
}

fn step_help(step: &StepInfo) -> String {
    let mut help = format!(
        "DataFusion CI command: {}\n\n{}\n\nUsage:\n  {CI_COMMAND} {}\n\nExamples:\n",
        step.command, step.help_description, step.help_usage,
    );
    for example in step.help_examples {
        writeln!(help, "  {CI_COMMAND} {example}").ok();
    }
    if let Some(example) = step.help_examples.first() {
        write!(help,
            "\nUse '--explain' to show the full command:\n  {CI_COMMAND} {example} --explain\n"
        ).ok();
    }
    help
}

fn find_step(command: &str) -> Result<&'static StepInfo> {
    CI_STEPS
        .iter()
        .find(|step| step.command == command)
        .ok_or_else(|| format!("unknown CI step `{command}`"))
}

pub(crate) fn is_help_arg(arg: &str) -> bool {
    matches!(arg, "help" | "-h" | "--help")
}

pub(crate) fn run(root: &Path, args: &[String]) -> Result<()> {
    StepContext::new(root).run(args)
}

struct StepContext {
    root: PathBuf,
    env_reader: EnvReader,
}

impl StepContext {
    fn new(root: &Path) -> Self {
        Self {
            root: root.to_path_buf(),
            env_reader: system_env,
        }
    }

    fn run(&self, args: &[String]) -> Result<()> {
        match args {
            [] => {
                print!("{}", help());
                Ok(())
            }
            [help_arg] if is_help_arg(help_arg) => {
                print!("{}", help());
                Ok(())
            }
            [step_name, help_arg] if is_help_arg(help_arg) => {
                print!("{}", step_help(find_step(step_name)?));
                Ok(())
            }
            step_args => {
                let (action, command) = self.ci_step(step_args)?;
                match action {
                    StepAction::Execute => command.execute(),
                    StepAction::Explain => {
                        command.explain();
                        Ok(())
                    }
                }
            }
        }
    }

    /// Parses a CI step into an action and its complete command description.
    fn ci_step(&self, args: &[String]) -> Result<(StepAction, CommandSpec)> {
        let (action, args) = match args.split_last() {
            Some((arg, args)) if arg == "--explain" => (StepAction::Explain, args),
            _ => (StepAction::Execute, args),
        };
        let Some((step, args)) = args.split_first() else {
            return Err(format!("missing CI step\n\n{}", help()));
        };

        let step = find_step(step)?;
        let command = (step.runner)(self, args).map_err(|error| error.render(step))?;
        Ok((action, command))
    }

    /// Implementation for `cargo xtask ci step check [args]`
    fn run_check(&self, args: &[String]) -> StepResult<CommandSpec> {
        let args = args.iter().map(String::as_str).collect::<Vec<_>>();
        let mut builder = CommandSpec::builder(self.env_reader);
        builder
            .current_dir(&self.root)
            .failure_message("CI check step failed");

        match args.as_slice() {
            ["workspace"] => {
                builder.program("cargo").args([
                    "check",
                    "--profile",
                    "ci",
                    "--workspace",
                    "--all-targets",
                    "--features",
                    "integration-tests",
                    "--locked",
                ]);
            }
            [package, "default"] => {
                builder.program("cargo").args([
                    "check",
                    "--profile",
                    "ci",
                    "--all-targets",
                    "-p",
                    package,
                ]);
            }
            [package, "no-default"] => {
                builder.program("cargo").args([
                    "check",
                    "--profile",
                    "ci",
                    "--no-default-features",
                    "-p",
                    package,
                ]);
            }
            [package, feature] => {
                builder.program("cargo").args([
                    "check",
                    "--profile",
                    "ci",
                    "--no-default-features",
                    "-p",
                    package,
                    "--features",
                    feature,
                ]);
            }
            _ => return Err(StepError::Usage),
        }

        builder.build().map_err(StepError::InvalidCommand)
    }

    /// Implementation for `cargo xtask ci step test [args]`
    fn run_test(&self, args: &[String]) -> StepResult<CommandSpec> {
        let [variant] = args else {
            return Err(StepError::Usage);
        };

        let mut cmd_builder = CommandSpec::builder(self.env_reader);
        cmd_builder
            .current_dir(&self.root)
            .failure_message("CI test step failed");

        match variant.as_str() {
            "workspace" | "slt-coverage" => {
                cmd_builder.program("cargo").args([
                    "llvm-cov",
                    "--profile",
                    "ci",
                    "--exclude",
                    "datafusion-examples",
                    "--exclude",
                    "ffi_example_table_provider",
                    "--exclude",
                    "datafusion-cli",
                    "--workspace",
                ]);
                let output_path = if variant == "slt-coverage" {
                    cmd_builder.args(["--test", "sqllogictests"]);
                    "target/codecov-slt.json"
                } else {
                    cmd_builder.args(["--lib", "--tests", "--bins"]);
                    "target/codecov.json"
                };
                cmd_builder
                    .args([
                        "--features",
                        "serde,avro,json,backtrace,integration-tests,parquet_encryption,substrait",
                        "--codecov",
                        "--output-path",
                        output_path,
                    ])
                    .env("RUST_BACKTRACE", "1")
                    .requires_executable(
                        "cargo-llvm-cov",
                        "install it with `cargo install cargo-llvm-cov --locked`",
                    );
            }
            "cli" => {
                cmd_builder.doc(
                    "Run CLI tests with storage integration enabled. The AWS credentials\n\
                     shown below are fixed test values, not secrets.",
                );
                cmd_builder
                    .program("cargo")
                    .args([
                        "test",
                        "--features",
                        "backtrace",
                        "--profile",
                        "ci",
                        "-p",
                        "datafusion-cli",
                        "--lib",
                        "--tests",
                        "--bins",
                    ])
                    .env("RUST_BACKTRACE", "1")
                    .env("AWS_ENDPOINT", "http://127.0.0.1:9000")
                    .env("AWS_ACCESS_KEY_ID", "TEST-DataFusionLogin")
                    .env("AWS_SECRET_ACCESS_KEY", "TEST-DataFusionPassword")
                    .env("TEST_STORAGE_INTEGRATION", "1")
                    .env("AWS_ALLOW_HTTP", "true");
            }
            "doctest" => {
                cmd_builder.program("cargo").args([
                    "test",
                    "--profile",
                    "ci",
                    "--doc",
                    "--features",
                    "avro,json",
                ]);
            }
            "ffi" => {
                cmd_builder.program("cargo").args([
                    "test",
                    "--profile",
                    "ci",
                    "-p",
                    "datafusion-ffi",
                    "-p",
                    "datafusion-ffi-integration-tests",
                    "--lib",
                    "--tests",
                    "--features",
                    "integration-tests",
                ]);
            }
            "microbenchmarks" => {
                cmd_builder.doc(
                    "Run each Criterion benchmark once without timing it, selecting only\n\
                     bench targets (not lib/bin unit tests). Keep debug assertions and\n\
                     overflow checks enabled while optimizing execution for larger datasets.\n\
                     The SQL benchmark harness needs separately generated datasets and\n\
                     is covered by the benchmark result verification job.",
                );
                cmd_builder.env(
                    "CLICKBENCH_DATA_PATH",
                    self.root
                        .join("datafusion/core/tests/data/clickbench_hits_10.parquet"),
                );
                cmd_builder.program("cargo").args([
                    "test",
                    "--profile",
                    "ci-bench",
                    "--no-fail-fast",
                    "--workspace",
                    "--exclude",
                    "datafusion-examples",
                    "--exclude",
                    "datafusion-benchmarks",
                    "--exclude",
                    "datafusion-cli",
                    "--bench",
                    "*",
                    "--features",
                    "datafusion-physical-plan/test_utils",
                    "--",
                    "--test",
                ]);
            }
            "benchmark-plan" => {
                let tpch_data = self.root.join(TPCH_DATA_DIR);
                cmd_builder
                    .program("cargo")
                    .args([
                        "test",
                        "plan_q",
                        "--package",
                        "datafusion-benchmarks",
                        "--profile",
                        "ci",
                        "--features=ci",
                        "--",
                        "--test-threads=1",
                    ])
                    .env("RUST_MIN_STACK", "20971520")
                    .env("TPCH_DATA", tpch_data.as_os_str())
                    .requires_path(
                        tpch_data,
                        "generate the TPC-H fixture data before running this step",
                    );
            }
            "benchmark-sqllogic" => {
                let tpch_data = self.root.join(TPCH_DATA_DIR);
                cmd_builder
                    .program("cargo")
                    .args([
                        "test",
                        "--features",
                        "backtrace,parquet_encryption,substrait",
                        "--profile",
                        "ci",
                        "--package",
                        "datafusion-sqllogictest",
                        "--test",
                        "sqllogictests",
                    ])
                    .env("RUST_MIN_STACK", "20971520")
                    .env("TPCH_DATA", tpch_data.as_os_str())
                    .env("INCLUDE_TPCH", "true")
                    .requires_path(
                        tpch_data,
                        "generate the TPC-H fixture data before running this step",
                    );
            }
            "postgres" => {
                cmd_builder
                    .program("cargo")
                    .args([
                        "test",
                        "--features",
                        "backtrace",
                        "--profile",
                        "ci",
                        "--features=postgres",
                        "--test",
                        "sqllogictests",
                    ])
                    .current_dir(self.root.join("datafusion/sqllogictest"))
                    .env("PG_COMPAT", "true")
                    .env_template(
                        "PG_URI",
                        "postgresql://postgres:postgres@{POSTGRES_HOST}:{POSTGRES_PORT}/db_test",
                    );
            }
            "substrait" | "substrait-optimized" => {
                cmd_builder.program("cargo").args([
                    "test",
                    "-p",
                    "datafusion-sqllogictest",
                    "--test",
                    "sqllogictests",
                    "--features",
                    "substrait",
                    "--",
                    "--substrait-round-trip",
                ]);
                if variant == "substrait-optimized" {
                    cmd_builder.args(["--substrait-optimize", "joins_conditionless.slt"]);
                } else {
                    cmd_builder.args(["limit.slt"]);
                }
            }
            "extended" => {
                cmd_builder.doc(
                    "Run tests behind the `extended_tests` feature. The `workspace`\n\
                     step runs the remaining tests.",
                );
                cmd_builder
                    .program("cargo")
                    .args([
                        "test",
                        "--profile",
                        "ci",
                        "-p",
                        "datafusion",
                        "--test",
                        "extended_tests",
                        "--features",
                        "avro,backtrace,extended_tests,parquet_encryption",
                    ])
                    .env("RUST_BACKTRACE", "1");
            }
            "spill-pool-fuzz" => {
                cmd_builder.doc(
                    "Run more random spill pool fuzzer scenarios than the `workspace`\n\
                     step. This takes about a minute.",
                );
                cmd_builder
                    .program("cargo")
                    .args([
                        "test",
                        "--profile",
                        "ci",
                        "-p",
                        "datafusion-physical-plan",
                        "--lib",
                        "--",
                        "--exact",
                        SPILL_POOL_FUZZ_TEST,
                    ])
                    .env("RUST_BACKTRACE", "1")
                    .env("DATAFUSION_SPILL_POOL_FUZZ_ITERATIONS", "1000");
            }
            "hash-collisions" => {
                cmd_builder.doc(
                    "Check that results stay correct when hash values collide.\n\
                     Run from the `datafusion` directory with `RUST_BACKTRACE=1`.",
                );
                cmd_builder
                    .program("cargo")
                    .args([
                        "test",
                        "--profile",
                        "ci",
                        "--exclude",
                        "datafusion-examples",
                        "--exclude",
                        "datafusion-benchmarks",
                        "--exclude",
                        "datafusion-sqllogictest",
                        "--exclude",
                        "datafusion-cli",
                        "--workspace",
                        "--lib",
                        "--tests",
                        "--features=force_hash_collisions,avro",
                    ])
                    .current_dir(self.root.join("datafusion"))
                    .env("RUST_BACKTRACE", "1");
            }
            "sqlite" => {
                cmd_builder.doc(
                    "Run SQL logic tests including the SQLite suite. This step does not\n\
                     set `RUST_BACKTRACE` because backtraces make the suite expensive.",
                );
                cmd_builder.program("cargo").args([
                    "test",
                    "--features",
                    "backtrace,parquet_encryption",
                    "--profile",
                    "ci-optimized",
                    "--test",
                    "sqllogictests",
                    "--",
                    "--include-sqlite",
                ]);
            }
            _ => return Err(StepError::Usage),
        }

        cmd_builder.build().map_err(StepError::InvalidCommand)
    }
}

#[derive(Debug, PartialEq)]
enum StepAction {
    Execute,
    Explain,
}

#[derive(Debug)]
enum StepError {
    Usage,
    InvalidCommand(String),
}

impl StepError {
    fn render(self, step: &StepInfo) -> String {
        match self {
            Self::Usage => format!("usage: {CI_COMMAND} {}", step.help_usage),
            Self::InvalidCommand(message) => {
                format!("invalid CI command definition: {message}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsStr;

    const TEST_USAGE: &str = "usage: cargo xtask ci step test <workspace|slt-coverage|cli|doctest|ffi|microbenchmarks|benchmark-plan|benchmark-sqllogic|postgres|substrait|substrait-optimized|extended|spill-pool-fuzz|hash-collisions|sqlite> [--explain]";

    fn args(args: &[&str]) -> Vec<String> {
        args.iter().map(|arg| (*arg).to_string()).collect()
    }

    fn test_env(name: &str) -> Result<String> {
        match name {
            "POSTGRES_HOST" => Ok("postgres.example".to_string()),
            "POSTGRES_PORT" => Ok("15432".to_string()),
            _ => Err(format!("unexpected environment variable `{name}`")),
        }
    }

    fn context() -> StepContext {
        StepContext {
            root: PathBuf::from("/workspace"),
            env_reader: test_env,
        }
    }

    #[test]
    fn explain_builds_the_same_command_as_execute() {
        let context = context();
        let (execute_action, execute_command) = context
            .ci_step(&args(&["check", "datafusion", "default"]))
            .unwrap();
        let (explain_action, explain_command) = context
            .ci_step(&args(&["check", "datafusion", "default", "--explain"]))
            .unwrap();

        assert_eq!(execute_action, StepAction::Execute);
        assert_eq!(explain_action, StepAction::Explain);
        assert_eq!(
            execute_command.full_command(),
            explain_command.full_command()
        );
    }

    #[test]
    fn explain_flag_must_be_last() {
        let error = context()
            .ci_step(&args(&["test", "--explain", "workspace"]))
            .err()
            .unwrap();

        assert_eq!(error, TEST_USAGE);
    }

    /// A filter that matches no test passes without running anything, so a
    /// renamed or moved spill pool fuzzer would silently stop running in CI.
    #[test]
    fn spill_pool_fuzz_test_exists() {
        let (module, test) = SPILL_POOL_FUZZ_TEST
            .rsplit_once("::tests::")
            .expect("test path has a `tests` module");
        let file = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../datafusion/physical-plan/src")
            .join(format!("{}.rs", module.replace("::", "/")));
        let source = std::fs::read_to_string(&file)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", file.display()));
        assert!(
            source.contains("mod tests {") && source.contains(&format!("fn {test}(")),
            "{} has no test `{test}` in `mod tests`; update SPILL_POOL_FUZZ_TEST",
            file.display()
        );
    }

    /// The `extended` step runs only the `extended_tests` test target, which
    /// requires the `extended_tests` feature. A test gated on the feature with a
    /// `cfg` in any other target would never run in CI.
    #[test]
    fn extended_tests_feature_only_gates_whole_targets() {
        fn visit(dir: &Path, found: &mut Vec<PathBuf>) {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    visit(&path, found);
                } else if path.extension() == Some(OsStr::new("rs"))
                    && std::fs::read_to_string(&path)
                        .unwrap()
                        .contains("feature = \"extended_tests\"")
                {
                    found.push(path);
                }
            }
        }
        let mut found = vec![];
        visit(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("../datafusion"),
            &mut found,
        );
        assert!(
            found.is_empty(),
            "put tests behind `extended_tests` in the `extended_tests` test target \
             (see datafusion/core/Cargo.toml), not behind a `cfg` in {found:?}"
        );
    }

    #[test]
    fn unknown_test_variant_is_rejected() {
        let error = context()
            .ci_step(&args(&["test", "extended_tests", "--explain"]))
            .err()
            .unwrap();

        assert_eq!(error, TEST_USAGE);
    }

    // Test all available commands, ensure their executed command is expected
    #[test]
    fn explain_all_available_ci_commands() {
        struct Cmd {
            // Actual command to run
            command: &'static [&'static str],
            // Full explain output
            expected: fn(&str),
        }

        let commands = [
            Cmd {
                command: &["check", "workspace"],
                expected: |actual| {
                    insta::assert_snapshot!(actual, @r"
cd /workspace && \
cargo check \
--profile ci \
--workspace \
--all-targets \
--features integration-tests \
--locked
")
                },
            },
            Cmd {
                command: &["check", "datafusion", "default"],
                expected: |actual| {
                    insta::assert_snapshot!(actual, @r"
cd /workspace && \
cargo check \
--profile ci \
--all-targets \
-p datafusion
")
                },
            },
            Cmd {
                command: &["check", "datafusion", "no-default"],
                expected: |actual| {
                    insta::assert_snapshot!(actual, @r"
cd /workspace && \
cargo check \
--profile ci \
--no-default-features \
-p datafusion
")
                },
            },
            Cmd {
                command: &["check", "datafusion", "parquet"],
                expected: |actual| {
                    insta::assert_snapshot!(actual, @r"
cd /workspace && \
cargo check \
--profile ci \
--no-default-features \
-p datafusion \
--features parquet
")
                },
            },
            Cmd {
                command: &["test", "workspace"],
                expected: |actual| {
                    insta::assert_snapshot!(actual, @r"
# Requires executable `cargo-llvm-cov` (install it with `cargo install cargo-llvm-cov --locked`)
cd /workspace && \
RUST_BACKTRACE=1 \
cargo llvm-cov \
--profile ci \
--exclude datafusion-examples \
--exclude ffi_example_table_provider \
--exclude datafusion-cli \
--workspace \
--lib \
--tests \
--bins \
--features serde,avro,json,backtrace,integration-tests,parquet_encryption,substrait \
--codecov \
--output-path target/codecov.json
")
                },
            },
            Cmd {
                command: &["test", "slt-coverage"],
                expected: |actual| {
                    insta::assert_snapshot!(actual, @r"
# Requires executable `cargo-llvm-cov` (install it with `cargo install cargo-llvm-cov --locked`)
cd /workspace && \
RUST_BACKTRACE=1 \
cargo llvm-cov \
--profile ci \
--exclude datafusion-examples \
--exclude ffi_example_table_provider \
--exclude datafusion-cli \
--workspace \
--test sqllogictests \
--features serde,avro,json,backtrace,integration-tests,parquet_encryption,substrait \
--codecov \
--output-path target/codecov-slt.json
")
                },
            },
            Cmd {
                command: &["test", "cli"],
                expected: |actual| {
                    insta::assert_snapshot!(actual, @r"
# Run CLI tests with storage integration enabled. The AWS credentials
# shown below are fixed test values, not secrets.

cd /workspace && \
AWS_ACCESS_KEY_ID=TEST-DataFusionLogin \
AWS_ALLOW_HTTP=true \
AWS_ENDPOINT=http://127.0.0.1:9000 \
AWS_SECRET_ACCESS_KEY=TEST-DataFusionPassword \
RUST_BACKTRACE=1 \
TEST_STORAGE_INTEGRATION=1 \
cargo test \
--features backtrace \
--profile ci \
-p datafusion-cli \
--lib \
--tests \
--bins
")
                },
            },
            Cmd {
                command: &["test", "doctest"],
                expected: |actual| {
                    insta::assert_snapshot!(actual, @r"
cd /workspace && \
cargo test \
--profile ci \
--doc \
--features avro,json
")
                },
            },
            Cmd {
                command: &["test", "ffi"],
                expected: |actual| {
                    insta::assert_snapshot!(actual, @r"
cd /workspace && \
cargo test \
--profile ci \
-p datafusion-ffi \
-p datafusion-ffi-integration-tests \
--lib \
--tests \
--features integration-tests
")
                },
            },
            Cmd {
                command: &["test", "microbenchmarks"],
                expected: |actual| {
                    insta::assert_snapshot!(actual, @r"
# Run each Criterion benchmark once without timing it, selecting only
# bench targets (not lib/bin unit tests). Keep debug assertions and
# overflow checks enabled while optimizing execution for larger datasets.
# The SQL benchmark harness needs separately generated datasets and
# is covered by the benchmark result verification job.

cd /workspace && \
CLICKBENCH_DATA_PATH=/workspace/datafusion/core/tests/data/clickbench_hits_10.parquet \
cargo test \
--profile ci-bench \
--no-fail-fast \
--workspace \
--exclude datafusion-examples \
--exclude datafusion-benchmarks \
--exclude datafusion-cli \
--bench '*' \
--features datafusion-physical-plan/test_utils \
-- --test
")
                },
            },
            Cmd {
                command: &["test", "benchmark-plan"],
                expected: |actual| {
                    insta::assert_snapshot!(actual, @r"
# Requires path `/workspace/datafusion/sqllogictest/test_files/tpch/data` (generate the TPC-H fixture data before running this step)
cd /workspace && \
RUST_MIN_STACK=20971520 \
TPCH_DATA=/workspace/datafusion/sqllogictest/test_files/tpch/data \
cargo test plan_q \
--package datafusion-benchmarks \
--profile ci \
--features=ci \
-- --test-threads=1
")
                },
            },
            Cmd {
                command: &["test", "benchmark-sqllogic"],
                expected: |actual| {
                    insta::assert_snapshot!(actual, @r"
# Requires path `/workspace/datafusion/sqllogictest/test_files/tpch/data` (generate the TPC-H fixture data before running this step)
cd /workspace && \
INCLUDE_TPCH=true \
RUST_MIN_STACK=20971520 \
TPCH_DATA=/workspace/datafusion/sqllogictest/test_files/tpch/data \
cargo test \
--features backtrace,parquet_encryption,substrait \
--profile ci \
--package datafusion-sqllogictest \
--test sqllogictests
")
                },
            },
            Cmd {
                command: &["test", "postgres"],
                expected: |actual| {
                    insta::assert_snapshot!(actual, @r"
# Requires environment variable `POSTGRES_HOST`
# Requires environment variable `POSTGRES_PORT`
cd /workspace/datafusion/sqllogictest && \
PG_COMPAT=true \
PG_URI=postgresql://postgres:postgres@postgres.example:15432/db_test \
cargo test \
--features backtrace \
--profile ci \
--features=postgres \
--test sqllogictests
")
                },
            },
            Cmd {
                command: &["test", "substrait"],
                expected: |actual| {
                    insta::assert_snapshot!(actual, @r"
cd /workspace && \
cargo test \
-p datafusion-sqllogictest \
--test sqllogictests \
--features substrait \
-- --substrait-round-trip limit.slt
")
                },
            },
            Cmd {
                command: &["test", "substrait-optimized"],
                expected: |actual| {
                    insta::assert_snapshot!(actual, @r"
cd /workspace && \
cargo test \
-p datafusion-sqllogictest \
--test sqllogictests \
--features substrait \
-- --substrait-round-trip --substrait-optimize joins_conditionless.slt
")
                },
            },
            Cmd {
                command: &["test", "extended"],
                expected: |actual| {
                    insta::assert_snapshot!(actual, @r"
# Run tests behind the `extended_tests` feature. The `workspace`
# step runs the remaining tests.

cd /workspace && \
RUST_BACKTRACE=1 \
cargo test \
--profile ci \
-p datafusion \
--test extended_tests \
--features avro,backtrace,extended_tests,parquet_encryption
")
                },
            },
            Cmd {
                command: &["test", "spill-pool-fuzz"],
                expected: |actual| {
                    insta::assert_snapshot!(actual, @r"
# Run more random spill pool fuzzer scenarios than the `workspace`
# step. This takes about a minute.

cd /workspace && \
DATAFUSION_SPILL_POOL_FUZZ_ITERATIONS=1000 \
RUST_BACKTRACE=1 \
cargo test \
--profile ci \
-p datafusion-physical-plan \
--lib \
-- --exact spill::spill_pool::tests::spill_pool_scenario_fuzz
")
                },
            },
            Cmd {
                command: &["test", "hash-collisions"],
                expected: |actual| {
                    insta::assert_snapshot!(actual, @r"
# Check that results stay correct when hash values collide.
# Run from the `datafusion` directory with `RUST_BACKTRACE=1`.

cd /workspace/datafusion && \
RUST_BACKTRACE=1 \
cargo test \
--profile ci \
--exclude datafusion-examples \
--exclude datafusion-benchmarks \
--exclude datafusion-sqllogictest \
--exclude datafusion-cli \
--workspace \
--lib \
--tests \
--features=force_hash_collisions,avro
")
                },
            },
            Cmd {
                command: &["test", "sqlite"],
                expected: |actual| {
                    insta::assert_snapshot!(actual, @r"
# Run SQL logic tests including the SQLite suite. This step does not
# set `RUST_BACKTRACE` because backtraces make the suite expensive.

cd /workspace && \
cargo test \
--features backtrace,parquet_encryption \
--profile ci-optimized \
--test sqllogictests \
-- --include-sqlite
")
                },
            },
        ];

        let context = context();
        for Cmd { command, expected } in commands {
            let explain_args = command
                .iter()
                .copied()
                .chain(std::iter::once("--explain"))
                .collect::<Vec<_>>();
            let (action, actual) = context.ci_step(&args(&explain_args)).unwrap();

            assert_eq!(action, StepAction::Explain);
            expected(&actual.explanation());
        }
    }
}
