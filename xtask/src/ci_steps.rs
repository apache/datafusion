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

//! CI steps that can be inspected or run through `cargo xtask`.

use crate::Result;
use std::env;
use std::ffi::OsStr;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

type StepResult<T> = std::result::Result<T, StepError>;
type StepRunner = fn(&StepContext, &[String]) -> StepResult<CiCommand>;

const CI_COMMAND: &str = "cargo xtask ci step";
const CI_SHORTCUT: &str = "cargo ci-step";
const TPCH_DATA_DIR: &str = "datafusion/sqllogictest/test_files/tpch/data";

/// Metadata and implementation for one CI step.
///
/// Adding an entry to `CI_STEPS` makes the step dispatchable and includes its
/// usage and description in the generated help text.
struct StepInfo {
    command: &'static str,
    help_usage: &'static str,
    help_examples: &'static [&'static str],
    help_description: &'static str,
    error_message: &'static str,
    runner: StepRunner,
}

static CI_STEPS: &[StepInfo] = &[
    StepInfo {
        command: "check",
        help_usage: "check <workspace|package> [default|no-default|feature] [--explain]",
        help_examples: &["check workspace", "check datafusion default"],
        help_description: "Check workspace or package compilation",
        error_message: "Cargo check step failed",
        runner: StepContext::run_check,
    },
    StepInfo {
        command: "test",
        help_usage: "test <workspace|cli|doctest|ffi|benchmark-plan|benchmark-sqllogic|postgres|substrait> [--explain]",
        help_examples: &["test cli", "test workspace"],
        help_description: "Run a CI test suite",
        error_message: "Cargo test step failed",
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
    env_reader: fn(&str) -> Result<String>,
}

impl StepContext {
    fn new(root: &Path) -> Self {
        Self {
            root: root.to_path_buf(),
            env_reader: required_env,
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
                let (action, step, command) = self.ci_step(step_args)?;
                match action {
                    StepAction::Execute => command.execute(step.error_message),
                    StepAction::Explain => {
                        command.explain();
                        Ok(())
                    }
                }
            }
        }
    }

    /// Parses a CI step into an action and its complete command description.
    fn ci_step(
        &self,
        args: &[String],
    ) -> Result<(StepAction, &'static StepInfo, CiCommand)> {
        let (action, args) = match args.split_last() {
            Some((arg, args)) if arg == "--explain" => (StepAction::Explain, args),
            _ => (StepAction::Execute, args),
        };
        let Some((step, args)) = args.split_first() else {
            return Err(format!("missing CI step\n\n{}", help()));
        };

        let step = find_step(step)?;
        let command = (step.runner)(self, args).map_err(|error| error.render(step))?;
        Ok((action, step, command))
    }

    /// Implementation for `cargo xtask ci step check [args]`
    fn run_check(&self, args: &[String]) -> StepResult<CiCommand> {
        let args = args.iter().map(String::as_str).collect::<Vec<_>>();
        let mut command = self.cargo();
        command.args(["check", "--profile", "ci"]);

        match args.as_slice() {
            ["workspace"] => {
                command.args([
                    "--workspace",
                    "--all-targets",
                    "--features",
                    "integration-tests",
                    "--locked",
                ]);
            }
            [package, "default"] => {
                command.args(["--all-targets", "-p", package]);
            }
            [package, "no-default"] => {
                command.args(["--no-default-features", "-p", package]);
            }
            [package, feature] => {
                command.args([
                    "--no-default-features",
                    "-p",
                    package,
                    "--features",
                    feature,
                ]);
            }
            _ => return Err(StepError::Usage),
        }

        Ok(command)
    }

    /// Implementation for `cargo xtask ci step test [args]`
    fn run_test(&self, args: &[String]) -> StepResult<CiCommand> {
        let [variant] = args else {
            return Err(StepError::Usage);
        };

        let mut command = self.cargo();
        match variant.as_str() {
            "workspace" => {
                command.args([
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
                    "--lib",
                    "--tests",
                    "--bins",
                    "--features",
                    "serde,avro,json,backtrace,integration-tests,parquet_encryption,substrait",
                    "--codecov",
                    "--output-path",
                    "target/codecov.json",
                ]);
            }
            "cli" => {
                command.args([
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
                ]);
            }
            "doctest" => {
                command.args([
                    "test",
                    "--profile",
                    "ci",
                    "--doc",
                    "--features",
                    "avro,json",
                ]);
            }
            "ffi" => {
                command.args([
                    "test",
                    "--profile",
                    "ci",
                    "-p",
                    "datafusion-ffi",
                    "--lib",
                    "--tests",
                    "--features",
                    "integration-tests",
                ]);
            }
            "benchmark-plan" => {
                command
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
                    .env("TPCH_DATA", self.root.join(TPCH_DATA_DIR));
            }
            "benchmark-sqllogic" => {
                command
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
                    .env("TPCH_DATA", self.root.join(TPCH_DATA_DIR))
                    .env("INCLUDE_TPCH", "true");
            }
            "postgres" => {
                let host = self
                    .required_env("POSTGRES_HOST")
                    .map_err(StepError::Message)?;
                let port = self
                    .required_env("POSTGRES_PORT")
                    .map_err(StepError::Message)?;
                let uri = format!("postgresql://postgres:postgres@{host}:{port}/db_test");
                command
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
                    .env("PG_URI", uri);
            }
            "substrait" => {
                command.args([
                    "test",
                    "-p",
                    "datafusion-sqllogictest",
                    "--test",
                    "sqllogictests",
                    "--features",
                    "substrait",
                    "--",
                    "--substrait-round-trip",
                    "limit.slt",
                ]);
            }
            _ => return Err(StepError::Usage),
        }

        Ok(command)
    }

    fn cargo(&self) -> CiCommand {
        CiCommand::new("cargo", &self.root)
    }

    fn required_env(&self, name: &str) -> Result<String> {
        (self.env_reader)(name)
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
    Message(String),
}

impl StepError {
    fn render(self, step: &StepInfo) -> String {
        match self {
            Self::Usage => format!("usage: {CI_COMMAND} {}", step.help_usage),
            Self::Message(message) => message,
        }
    }
}

/// Command shared by invocation and `--explain`
struct CiCommand {
    command: Command,
}

impl CiCommand {
    fn new(program: impl AsRef<OsStr>, current_dir: impl AsRef<Path>) -> Self {
        let mut command = Command::new(program);
        command.current_dir(current_dir);
        Self { command }
    }

    fn args<I, S>(&mut self, args: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.command.args(args);
        self
    }

    fn env<K, V>(&mut self, key: K, value: V) -> &mut Self
    where
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        self.command.env(key, value);
        self
    }

    fn current_dir(&mut self, directory: impl AsRef<Path>) -> &mut Self {
        self.command.current_dir(directory);
        self
    }

    fn explain(&self) {
        println!("{}", self.full_command());
    }

    fn execute(mut self, error_message: &str) -> Result<()> {
        println!("+ {}", self.full_command());
        let status = self
            .command
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .map_err(|error| format!("{error_message}: {error}"))?;

        if status.success() {
            Ok(())
        } else {
            Err(format!("{error_message}: {status}"))
        }
    }

    fn full_command(&self) -> String {
        let mut lines = Vec::new();

        if let Some(current_dir) = self.command.get_current_dir() {
            lines.push(format!("cd {} &&", shell_quote(current_dir.as_os_str())));
        }

        for (key, value) in self.command.get_envs() {
            let Some(value) = value else {
                continue;
            };
            lines.push(format!("{}={}", shell_quote(key), shell_quote(value)));
        }

        lines.extend(shell_command_lines(
            self.command.get_program(),
            self.command.get_args(),
        ));
        lines.join(" \\\n")
    }
}

/// Groups option-value pairs so the result reads like a hand-written command.
fn shell_command_lines<'a>(
    program: &OsStr,
    args: impl Iterator<Item = &'a OsStr>,
) -> Vec<String> {
    let args = args.collect::<Vec<_>>();
    let mut lines = vec![shell_quote(program)];
    let mut index = 0;

    // Keep command names and leading positional arguments together, such as
    // `cargo check` and `cargo test plan_q`.
    while index < args.len() && !is_option(args[index]) {
        lines[0].push(' ');
        lines[0].push_str(&shell_quote(args[index]));
        index += 1;
    }

    while index < args.len() {
        let arg = args[index];
        let mut line = shell_quote(arg);
        index += 1;

        if arg == "--" {
            while index < args.len() {
                line.push(' ');
                line.push_str(&shell_quote(args[index]));
                index += 1;
            }
        } else if !arg.to_string_lossy().contains('=')
            && index < args.len()
            && !is_option(args[index])
        {
            line.push(' ');
            line.push_str(&shell_quote(args[index]));
            index += 1;
        }

        lines.push(line);
    }

    lines
}

fn is_option(value: &OsStr) -> bool {
    value.to_string_lossy().starts_with('-')
}

/// Quotes one shell word when it contains characters that need protection.
fn shell_quote(value: &OsStr) -> String {
    let value = value.to_string_lossy();
    if !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'_' | b'@' | b'%' | b'+' | b'=' | b':' | b',' | b'.' | b'/' | b'-'
                )
        })
    {
        value.into_owned()
    } else {
        format!("'{}'", value.replace('\'', "'\"'\"'"))
    }
}

fn required_env(name: &str) -> Result<String> {
    env::var(name)
        .map_err(|_| format!("required environment variable `{name}` is not set"))
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let (execute_action, _, execute_command) = context
            .ci_step(&args(&["check", "datafusion", "default"]))
            .unwrap();
        let (explain_action, _, explain_command) = context
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

        assert_eq!(
            error,
            "usage: cargo xtask ci step test <workspace|cli|doctest|ffi|benchmark-plan|benchmark-sqllogic|postgres|substrait> [--explain]"
        );
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
cd /workspace && \
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
                command: &["test", "cli"],
                expected: |actual| {
                    insta::assert_snapshot!(actual, @r"
cd /workspace && \
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
--lib \
--tests \
--features integration-tests
")
                },
            },
            Cmd {
                command: &["test", "benchmark-plan"],
                expected: |actual| {
                    insta::assert_snapshot!(actual, @r"
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
        ];

        let context = context();
        for Cmd { command, expected } in commands {
            let explain_args = command
                .iter()
                .copied()
                .chain(std::iter::once("--explain"))
                .collect::<Vec<_>>();
            let (action, _, actual) = context.ci_step(&args(&explain_args)).unwrap();

            assert_eq!(action, StepAction::Explain);
            expected(&actual.full_command());
        }
    }
}
