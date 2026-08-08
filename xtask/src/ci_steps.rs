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
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

type StepResult<T> = std::result::Result<T, StepError>;
type StepRunner = fn(&StepContext, &[String]) -> StepResult<CiCommand>;

const CI_COMMAND: &str = "cargo xtask ci step";
const CI_SHORTCUT: &str = "cargo ci-step";

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
        help_usage: "check <workspace|package> [default|no-default|feature]",
        help_examples: &["check workspace", "check datafusion default"],
        help_description: "Check workspace or package compilation",
        error_message: "Cargo check step failed",
        runner: StepContext::run_check,
    },
    StepInfo {
        command: "test",
        help_usage: "test <workspace|cli|doctest|ffi|benchmark-plan|benchmark-sqllogic|postgres|substrait>",
        help_examples: &["test cli", "test workspace"],
        help_description: "Run a CI test suite",
        error_message: "Cargo test step failed",
        runner: StepContext::run_test,
    },
];

pub(crate) fn help() -> String {
    let mut help = format!(
        "DataFusion CI commands\n\nUsage:\n  {CI_COMMAND} <step-name> [args]\n\nExamples:\n",
    );
    for step in CI_STEPS {
        if let Some(example) = step.help_examples.first() {
            help.push_str(&format!("  {CI_COMMAND} {example}\n"));
        }
    }
    help.push_str(&format!("  {CI_COMMAND} explain test workspace\n"));

    let command_width = CI_STEPS
        .iter()
        .map(|step| step.command.len())
        .max()
        .unwrap_or_default();
    help.push_str("\nAvailable steps:\n");
    for step in CI_STEPS {
        help.push_str(&format!(
            "  {:command_width$}  {}\n",
            step.command, step.help_description
        ));
    }

    help.push_str(&format!(
        "\nShortcut:\n  # `{CI_SHORTCUT}` is short for `{CI_COMMAND}`.\n  {CI_SHORTCUT} check workspace\n\nFor more details:\n  {CI_COMMAND} check --help\n"
    ));
    help
}

fn step_help(step: &StepInfo) -> String {
    let mut help = format!(
        "DataFusion CI command: {}\n\n{}\n\nUsage:\n  {CI_COMMAND} {}\n\nExamples:\n",
        step.command, step.help_description, step.help_usage,
    );
    for example in step.help_examples {
        help.push_str(&format!("  {CI_COMMAND} {example}\n"));
    }
    if let Some(example) = step.help_examples.first() {
        help.push_str(&format!(
            "\nUse 'explain' to show the full command:\n  {CI_COMMAND} explain {example}\n"
        ));
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
}

impl StepContext {
    fn new(root: &Path) -> Self {
        Self {
            root: root.to_path_buf(),
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
    /// Keeping execution out of this method guarantees `explain` and execution
    /// use exactly the same program, arguments, environment, and directory.
    fn ci_step(
        &self,
        args: &[String],
    ) -> Result<(StepAction, &'static StepInfo, CiCommand)> {
        let (action, args) = match args.split_first() {
            Some((arg, args)) if arg == "explain" => (StepAction::Explain, args),
            _ => (StepAction::Execute, args),
        };
        let Some((step, args)) = args.split_first() else {
            return Err(format!("missing CI step\n\n{}", help()));
        };

        let step = find_step(step)?;
        let command = (step.runner)(self, args).map_err(|error| error.render(step))?;
        Ok((action, step, command))
    }

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
                    .env("TPCH_DATA", self.tpch_data_dir());
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
                    .env("TPCH_DATA", self.tpch_data_dir())
                    .env("INCLUDE_TPCH", "true");
            }
            "postgres" => {
                let host = required_env("POSTGRES_HOST").map_err(StepError::Message)?;
                let port = required_env("POSTGRES_PORT").map_err(StepError::Message)?;
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

    fn tpch_data_dir(&self) -> PathBuf {
        self.root
            .join("datafusion/sqllogictest/test_files/tpch/data")
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

/// A complete process description shared by explanation and execution.
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
        let current_dir = self.command.get_current_dir();
        let mut output = String::new();

        if let Some(current_dir) = current_dir {
            push_shell_line(
                &mut output,
                "",
                &format!("cd {} &&", shell_quote(current_dir.as_os_str())),
                true,
            );
        }

        for (key, value) in self.command.get_envs() {
            let Some(value) = value else {
                continue;
            };
            push_shell_line(
                &mut output,
                "",
                &format!("{}={}", shell_quote(key), shell_quote(value)),
                true,
            );
        }

        let command_lines =
            shell_command_lines(self.command.get_program(), self.command.get_args());
        let last_line = command_lines.len() - 1;
        for (index, line) in command_lines.into_iter().enumerate() {
            push_shell_line(&mut output, "", &line, index != last_line);
        }

        output.pop();
        output
    }
}

fn push_shell_line(output: &mut String, indent: &str, content: &str, continued: bool) {
    output.push_str(indent);
    output.push_str(content);
    if continued {
        output.push_str(" \\");
    }
    output.push('\n');
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

    fn command_args(command: &CiCommand) -> Vec<String> {
        command
            .command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect()
    }

    fn context() -> StepContext {
        StepContext {
            root: PathBuf::from("/workspace"),
        }
    }

    #[test]
    fn check_variants_build_complete_commands() {
        let context = context();
        let cases = [
            (
                args(&["workspace"]),
                vec![
                    "check",
                    "--profile",
                    "ci",
                    "--workspace",
                    "--all-targets",
                    "--features",
                    "integration-tests",
                    "--locked",
                ],
            ),
            (
                args(&["datafusion", "default"]),
                vec![
                    "check",
                    "--profile",
                    "ci",
                    "--all-targets",
                    "-p",
                    "datafusion",
                ],
            ),
            (
                args(&["datafusion", "no-default"]),
                vec![
                    "check",
                    "--profile",
                    "ci",
                    "--no-default-features",
                    "-p",
                    "datafusion",
                ],
            ),
            (
                args(&["datafusion", "parquet"]),
                vec![
                    "check",
                    "--profile",
                    "ci",
                    "--no-default-features",
                    "-p",
                    "datafusion",
                    "--features",
                    "parquet",
                ],
            ),
        ];

        for (args, expected) in cases {
            let command = context.run_check(&args).unwrap();
            assert_eq!(command_args(&command), expected);
        }
    }

    #[test]
    fn explain_uses_the_same_ci_command() {
        let context = context();
        let (action, step, command) = context
            .ci_step(&args(&["explain", "test", "benchmark-plan"]))
            .unwrap();

        assert_eq!(action, StepAction::Explain);
        assert_eq!(step.command, "test");
        assert_eq!(command.command.get_program(), "cargo");
        assert_eq!(
            command.command.get_current_dir(),
            Some(Path::new("/workspace"))
        );
        assert!(command.full_command().starts_with("cd /workspace && \\\n"));
        assert!(
            command
                .full_command()
                .contains("RUST_MIN_STACK=20971520 \\\n")
        );
        assert!(command.full_command().contains("cargo test plan_q \\\n"));
        assert!(command.full_command().ends_with("-- --test-threads=1"));
    }

    #[test]
    fn full_command_is_formatted_for_copy_and_paste() {
        let command = context()
            .run_check(&args(&["datafusion", "default"]))
            .unwrap();

        assert_eq!(
            command.full_command(),
            concat!(
                "cd /workspace && \\\n",
                "cargo check \\\n",
                "--profile ci \\\n",
                "--all-targets \\\n",
                "-p datafusion"
            )
        );
        assert_eq!(shell_quote(OsStr::new("a b'c")), "'a b'\"'\"'c'");
    }

    #[test]
    fn only_check_and_test_are_ci_steps() {
        let error = context()
            .ci_step(&args(&["fmt"]))
            .err()
            .expect("fmt must remain outside the CI step interface");
        assert_eq!(error, "unknown CI step `fmt`");
    }

    #[test]
    fn help_and_usage_are_generated_from_step_info() {
        let help = help();
        assert!(help.starts_with(
            "DataFusion CI commands\n\nUsage:\n  cargo xtask ci step <step-name> [args]"
        ));
        assert!(help.contains("  cargo xtask ci step explain test workspace\n"));
        assert!(help.contains("\nAvailable steps:\n"));
        assert!(help.contains(
            "Shortcut:\n  # `cargo ci-step` is short for `cargo xtask ci step`.\n  cargo ci-step check workspace\n"
        ));
        assert!(
            help.ends_with("For more details:\n  cargo xtask ci step check --help\n")
        );

        for step in CI_STEPS {
            assert!(help.contains(step.help_description));
            let details = step_help(step);
            assert!(details.contains(&format!("{CI_COMMAND} {}", step.help_usage)));
            assert!(details.contains(step.help_description));
            for example in step.help_examples {
                assert!(details.contains(&format!("{CI_COMMAND} {example}")));
            }
        }

        let error = context()
            .ci_step(&args(&["check"]))
            .err()
            .expect("missing check arguments must show generated usage");
        assert_eq!(
            error,
            "usage: cargo xtask ci step check <workspace|package> [default|no-default|feature]"
        );
    }
}
