// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Complete, inspectable definitions of external commands.
//!
//! CI code only describes what a command needs: program, arguments, working
//! directory, non-secret environment, prerequisites, and failure context.
//! This module owns the mechanics of resolving that definition, validating it,
//! rendering it for `--explain`, and executing it. Keeping those paths together
//! ensures that local and CI invocations cannot drift from the command being shown.

use crate::Result;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::{OsStr, OsString};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

pub(crate) type EnvReader = fn(&str) -> Result<String>;

/// Access to environment inputs required while resolving a command.
struct HostEnvironment {
    reader: EnvReader,
}

impl HostEnvironment {
    fn new(reader: EnvReader) -> Self {
        Self { reader }
    }

    fn required(&self, name: &str) -> Result<String> {
        (self.reader)(name)
    }
}

enum EnvTemplatePart {
    Literal(&'static str),
    Required(&'static str),
}

struct EnvTemplate {
    parts: Vec<EnvTemplatePart>,
}

impl EnvTemplate {
    fn parse(mut template: &'static str) -> Result<Self> {
        let original = template;
        let mut parts = Vec::new();
        while let Some(start) = template.find('{') {
            let (literal, after_literal) = template.split_at(start);
            if literal.contains('}') {
                return Err(format!("invalid environment template `{original}`"));
            }
            if !literal.is_empty() {
                parts.push(EnvTemplatePart::Literal(literal));
            }
            let after_open = &after_literal[1..];
            let Some(end) = after_open.find('}') else {
                return Err(format!("invalid environment template `{original}`"));
            };
            let (name, after_name) = after_open.split_at(end);
            if name.is_empty()
                || !name.bytes().all(|byte| {
                    byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_'
                })
            {
                return Err(format!("invalid environment name in template `{original}`"));
            }
            parts.push(EnvTemplatePart::Required(name));
            template = &after_name[1..];
        }
        if template.contains('}') {
            return Err(format!("invalid environment template `{original}`"));
        }
        if !template.is_empty() {
            parts.push(EnvTemplatePart::Literal(template));
        }
        Ok(Self { parts })
    }

    fn resolve(&self, environment: &HostEnvironment) -> Result<OsString> {
        let mut value = String::new();
        for part in &self.parts {
            match part {
                EnvTemplatePart::Literal(literal) => value.push_str(literal),
                EnvTemplatePart::Required(name) => {
                    value.push_str(&environment.required(name)?)
                }
            }
        }
        Ok(value.into())
    }

    fn display(&self, environment: &HostEnvironment) -> OsString {
        let mut value = String::new();
        for part in &self.parts {
            match part {
                EnvTemplatePart::Literal(literal) => value.push_str(literal),
                EnvTemplatePart::Required(name) => match environment.required(name) {
                    Ok(resolved) => value.push_str(&resolved),
                    Err(_) => {
                        write!(value, "<{name}>").ok();
                    }
                },
            }
        }
        value.into()
    }

    fn required_env(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.parts.iter().filter_map(|part| match part {
            EnvTemplatePart::Required(name) => Some(*name),
            EnvTemplatePart::Literal(_) => None,
        })
    }
}

enum EnvValue {
    Literal(OsString),
    Template(EnvTemplate),
}

enum PendingEnvValue {
    Literal(OsString),
    Template(&'static str),
}

impl EnvValue {
    fn resolve(&self, environment: &HostEnvironment) -> Result<OsString> {
        match self {
            Self::Literal(value) => Ok(value.clone()),
            Self::Template(template) => template.resolve(environment),
        }
    }

    fn display(&self, environment: &HostEnvironment) -> OsString {
        match self {
            Self::Literal(value) => value.clone(),
            Self::Template(template) => template.display(environment),
        }
    }
}

/// A tool or path that must exist before a command can run.
///
/// Execution checks each prerequisite before starting the process.
/// If one is missing, it returns an error containing the `help` hint.
///
/// # Example
/// A coverage command needs `cargo-llvm-cov` in `PATH`.
/// Its `help` hint tells the user how to install it:
///
/// ```text
/// cargo install cargo-llvm-cov --locked
/// ```
///
/// `--explain` lists this prerequisite even when the tool is missing.
enum Requirement {
    Executable { name: OsString, help: &'static str },
    Path { path: PathBuf, help: &'static str },
}

impl Requirement {
    fn explain(&self) -> String {
        match self {
            Self::Executable { name, help } => {
                format!("executable `{}` ({help})", name.to_string_lossy())
            }
            Self::Path { path, help } => {
                format!("path `{}` ({help})", path.display())
            }
        }
    }

    fn validate(&self) -> Result<()> {
        match self {
            Self::Executable { name, help } if !executable_exists(name) => Err(format!(
                "required executable `{}` was not found in PATH; {help}",
                name.to_string_lossy()
            )),
            Self::Path { path, help } if !path.exists() => Err(format!(
                "required path `{}` does not exist; {help}",
                path.display()
            )),
            _ => Ok(()),
        }
    }
}

/// A complete recipe for an external command, shared by `--explain` and execution.
///
/// Example coverage command, run from the repository root:
///
/// ```text
/// RUST_BACKTRACE=1 cargo llvm-cov
/// ```
///
/// Prerequisite: `cargo-llvm-cov` must be available in `PATH`.
///
/// - `--explain` prints the documentation, prerequisites, and command.
/// - Execution checks the prerequisites, then launches the process.
///   Failures are reported with this step's context.
pub(crate) struct CommandSpec {
    /// Optional explanation, printed as shell comments before the command.
    doc: Option<&'static str>,
    /// Executable to launch, e.g. `cargo`.
    program: OsString,
    /// Arguments in order, e.g. `["llvm-cov"]`.
    args: Vec<OsString>,
    /// Working directory, e.g. the repository root.
    current_dir: PathBuf,
    /// Child process environment overrides, e.g. `RUST_BACKTRACE=1`.
    env: BTreeMap<OsString, EnvValue>,
    /// Prerequisites checked before launch, e.g. `cargo-llvm-cov` in `PATH`.
    requirements: Vec<Requirement>,
    /// Reads host variables used by templates in `env`.
    environment: HostEnvironment,
    /// Error prefix identifying the step, e.g. `CI test step failed`.
    failure_message: &'static str,
}

/// Collects a command definition before validating its required fields.
pub(crate) struct CommandSpecBuilder {
    doc: Option<&'static str>,
    program: Option<OsString>,
    args: Vec<OsString>,
    current_dir: Option<PathBuf>,
    env: BTreeMap<OsString, PendingEnvValue>,
    requirements: Vec<Requirement>,
    environment: HostEnvironment,
    failure_message: Option<&'static str>,
}

impl CommandSpec {
    pub(crate) fn builder(env_reader: EnvReader) -> CommandSpecBuilder {
        CommandSpecBuilder {
            doc: None,
            program: None,
            args: Vec::new(),
            current_dir: None,
            env: BTreeMap::new(),
            requirements: Vec::new(),
            environment: HostEnvironment::new(env_reader),
            failure_message: None,
        }
    }
}

impl CommandSpecBuilder {
    pub(crate) fn doc(&mut self, doc: &'static str) -> &mut Self {
        self.doc = Some(doc);
        self
    }

    pub(crate) fn program(&mut self, program: impl AsRef<OsStr>) -> &mut Self {
        self.program = Some(program.as_ref().to_owned());
        self
    }

    pub(crate) fn args<I, S>(&mut self, args: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.args
            .extend(args.into_iter().map(|arg| arg.as_ref().to_owned()));
        self
    }

    pub(crate) fn env<K, V>(&mut self, key: K, value: V) -> &mut Self
    where
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        // Command definitions must not pass secrets here: values are displayed
        // by both `--explain` and the execution log.
        self.env.insert(
            key.as_ref().to_owned(),
            PendingEnvValue::Literal(value.as_ref().to_owned()),
        );
        self
    }

    pub(crate) fn env_template<K>(&mut self, key: K, template: &'static str) -> &mut Self
    where
        K: AsRef<OsStr>,
    {
        // Templates are inspectable command semantics, so resolved values are
        // subject to the same non-secret restriction as literal values.
        self.env
            .insert(key.as_ref().to_owned(), PendingEnvValue::Template(template));
        self
    }

    pub(crate) fn requires_executable(
        &mut self,
        name: impl AsRef<OsStr>,
        help: &'static str,
    ) -> &mut Self {
        self.requirements.push(Requirement::Executable {
            name: name.as_ref().to_owned(),
            help,
        });
        self
    }

    pub(crate) fn requires_path(
        &mut self,
        path: impl AsRef<Path>,
        help: &'static str,
    ) -> &mut Self {
        self.requirements.push(Requirement::Path {
            path: path.as_ref().to_path_buf(),
            help,
        });
        self
    }

    pub(crate) fn current_dir(&mut self, directory: impl AsRef<Path>) -> &mut Self {
        self.current_dir = Some(directory.as_ref().to_path_buf());
        self
    }

    pub(crate) fn failure_message(&mut self, message: &'static str) -> &mut Self {
        self.failure_message = Some(message);
        self
    }

    /// Finalizes the command only after its structural definition is complete.
    pub(crate) fn build(self) -> Result<CommandSpec> {
        let program = self
            .program
            .filter(|program| !program.is_empty())
            .ok_or_else(|| "command program is required".to_string())?;
        let current_dir = self
            .current_dir
            .filter(|directory| !directory.as_os_str().is_empty())
            .ok_or_else(|| "command working directory is required".to_string())?;
        let failure_message = self
            .failure_message
            .filter(|message| !message.trim().is_empty())
            .ok_or_else(|| "command failure message is required".to_string())?;

        let env =
            self.env
                .into_iter()
                .map(|(key, value)| {
                    if key.is_empty() {
                        return Err("command environment variable name cannot be empty"
                            .to_string());
                    }
                    let value = match value {
                        PendingEnvValue::Literal(value) => EnvValue::Literal(value),
                        PendingEnvValue::Template(template) => {
                            EnvValue::Template(EnvTemplate::parse(template)?)
                        }
                    };
                    Ok((key, value))
                })
                .collect::<Result<_>>()?;

        Ok(CommandSpec {
            doc: self.doc,
            program,
            args: self.args,
            current_dir,
            env,
            requirements: self.requirements,
            environment: self.environment,
            failure_message,
        })
    }
}

impl CommandSpec {
    pub(crate) fn explain(&self) {
        println!("{}", self.explanation());
    }

    pub(crate) fn explanation(&self) -> String {
        let mut lines = Vec::new();
        if let Some(doc) = self.doc {
            lines.extend(doc.lines().map(|line| format!("# {line}")));
            lines.push(String::new());
        }
        lines.extend(
            self.requirement_descriptions()
                .into_iter()
                .map(|requirement| format!("# Requires {requirement}")),
        );
        lines.push(self.full_command());
        lines.join("\n")
    }

    pub(crate) fn execute(self) -> Result<()> {
        self.validate_requirements()
            .map_err(|error| format!("{}: {error}", self.failure_message))?;
        let env = self
            .resolve_env()
            .map_err(|error| format!("{}: {error}", self.failure_message))?;
        println!("+ {}", self.render_command(&env));

        let status = Command::new(&self.program)
            .args(&self.args)
            .current_dir(&self.current_dir)
            .envs(env)
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .map_err(|error| format!("{}: {error}", self.failure_message))?;

        if status.success() {
            Ok(())
        } else {
            Err(format!("{}: {status}", self.failure_message))
        }
    }

    pub(crate) fn full_command(&self) -> String {
        let env = self
            .env
            .iter()
            .map(|(key, value)| (key.clone(), value.display(&self.environment)))
            .collect();
        self.render_command(&env)
    }

    fn requirement_descriptions(&self) -> Vec<String> {
        let mut descriptions = self
            .requirements
            .iter()
            .map(Requirement::explain)
            .collect::<Vec<_>>();
        let mut required_env = BTreeSet::new();
        for value in self.env.values() {
            if let EnvValue::Template(template) = value {
                required_env.extend(template.required_env());
            }
        }
        descriptions.extend(
            required_env
                .into_iter()
                .map(|name| format!("environment variable `{name}`")),
        );
        descriptions
    }

    fn validate_requirements(&self) -> Result<()> {
        for requirement in &self.requirements {
            requirement.validate()?;
        }
        Ok(())
    }

    fn resolve_env(&self) -> Result<BTreeMap<OsString, OsString>> {
        self.env
            .iter()
            .map(|(key, value)| {
                value
                    .resolve(&self.environment)
                    .map(|value| (key.clone(), value))
            })
            .collect()
    }

    fn render_command(&self, env: &BTreeMap<OsString, OsString>) -> String {
        let mut lines = vec![format!(
            "cd {} &&",
            shell_quote(self.current_dir.as_os_str())
        )];

        lines.extend(
            env.iter().map(|(key, value)| {
                format!("{}={}", shell_quote(key), shell_quote(value))
            }),
        );
        lines.extend(shell_command_lines(&self.program, self.args.iter()));
        lines.join(" \\\n")
    }
}

pub(crate) fn system_env(name: &str) -> Result<String> {
    env::var(name)
        .map_err(|_| format!("required environment variable `{name}` is not set"))
}

fn executable_exists(name: &OsStr) -> bool {
    let path = Path::new(name);
    if path.components().count() > 1 {
        return is_executable_file(path);
    }

    let Some(search_path) = env::var_os("PATH") else {
        return false;
    };

    env::split_paths(&search_path).any(|directory| executable_in(&directory, name))
}

fn executable_in(directory: &Path, name: &OsStr) -> bool {
    let candidate = directory.join(name);
    if is_executable_file(&candidate) {
        return true;
    }

    #[cfg(windows)]
    {
        if Path::new(name).extension().is_some() {
            return false;
        }
        let extensions = env::var_os("PATHEXT")
            .unwrap_or_else(|| OsString::from(".COM;.EXE;.BAT;.CMD"));
        return extensions.to_string_lossy().split(';').any(|extension| {
            is_executable_file(
                &candidate.with_extension(extension.trim_start_matches('.')),
            )
        });
    }

    #[cfg(not(windows))]
    false
}

fn is_executable_file(path: &Path) -> bool {
    let Ok(metadata) = path.metadata() else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }

    #[cfg(not(unix))]
    true
}

/// Groups option-value pairs so the result reads like a hand-written command.
fn shell_command_lines<'a>(
    program: &OsStr,
    args: impl Iterator<Item = &'a OsString>,
) -> Vec<String> {
    let args = args.map(OsString::as_os_str).collect::<Vec<_>>();
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

#[cfg(test)]
mod tests {
    use super::*;

    fn missing_env(name: &str) -> Result<String> {
        Err(format!("missing `{name}`"))
    }

    fn command_builder() -> CommandSpecBuilder {
        let mut builder = CommandSpec::builder(missing_env);
        builder
            .program("tool")
            .current_dir("/workspace")
            .failure_message("tool failed");
        builder
    }

    #[test]
    fn explain_lists_prerequisites_without_resolving_environment() {
        let mut builder = command_builder();
        builder
            .env_template("ENDPOINT", "{HOST}:{PORT}")
            .requires_path(
                "/workspace/input",
                "generate the input before running this command",
            );
        let command = builder.build().unwrap();

        insta::assert_snapshot!(command.explanation(), @r"
# Requires path `/workspace/input` (generate the input before running this command)
# Requires environment variable `HOST`
# Requires environment variable `PORT`
cd /workspace && \
ENDPOINT='<HOST>:<PORT>' \
tool
");
    }

    #[test]
    fn missing_prerequisite_is_reported_with_failure_context() {
        let mut builder = command_builder();
        builder.requires_executable(
            "datafusion-xtask-command-that-does-not-exist",
            "install the test tool",
        );
        let command = builder.build().unwrap();

        assert_eq!(
            command.execute().unwrap_err(),
            "tool failed: required executable `datafusion-xtask-command-that-does-not-exist` was not found in PATH; install the test tool"
        );
    }

    #[test]
    fn missing_environment_is_reported_with_failure_context() {
        let mut builder = command_builder();
        builder.env_template("ENDPOINT", "{HOST}:{PORT}");
        let command = builder.build().unwrap();

        assert_eq!(
            command.execute().unwrap_err(),
            "tool failed: missing `HOST`"
        );
    }

    #[test]
    fn build_validates_required_fields_and_templates() {
        let error = CommandSpec::builder(missing_env).build().err().unwrap();
        assert_eq!(error, "command program is required");

        let mut builder = command_builder();
        builder.env_template("ENDPOINT", "{lowercase}");
        let error = builder.build().err().unwrap();
        assert_eq!(error, "invalid environment name in template `{lowercase}`");
    }
}
