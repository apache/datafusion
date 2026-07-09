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

use crate::{Result, Xtask};
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

impl Xtask {
    pub(crate) fn script(&self, relative_path: &str) -> XtaskCommand {
        self.command(self.root.join(relative_path))
    }

    pub(crate) fn command(&self, program: impl Into<OsString>) -> XtaskCommand {
        XtaskCommand::new(program, &self.root)
    }

    pub(crate) fn ensure_tool(&self, tool: &str, installer: XtaskCommand) -> Result<()> {
        let installed = Command::new(tool)
            .arg("--version")
            .current_dir(&self.root)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false);

        if installed {
            return Ok(());
        }

        installer.run()
    }
}

/// Builder for commands run from xtask.
///
/// Keeps command setup in one fluent path instead of adding helper methods for
/// every combination of args, environment, and working directory.
///
/// ```no_run
/// self.command("cargo")
///     .args(["test", "--profile", "ci"])
///     .env("RUST_BACKTRACE", "1")
///     .current_dir(...)
///     .run()
/// ```
pub(crate) struct XtaskCommand {
    program: OsString,
    args: Vec<OsString>,
    current_dir: PathBuf,
    envs: Vec<(OsString, OsString)>,
}

impl XtaskCommand {
    fn new(program: impl Into<OsString>, current_dir: impl AsRef<Path>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            current_dir: current_dir.as_ref().to_path_buf(),
            envs: Vec::new(),
        }
    }

    pub(crate) fn arg(mut self, arg: impl Into<OsString>) -> Self {
        self.args.push(arg.into());
        self
    }

    pub(crate) fn args(
        mut self,
        args: impl IntoIterator<Item = impl Into<OsString>>,
    ) -> Self {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }

    pub(crate) fn current_dir(mut self, current_dir: impl AsRef<Path>) -> Self {
        self.current_dir = current_dir.as_ref().to_path_buf();
        self
    }

    pub(crate) fn env(
        mut self,
        name: impl Into<OsString>,
        value: impl Into<OsString>,
    ) -> Self {
        self.envs.push((name.into(), value.into()));
        self
    }

    pub(crate) fn run(self) -> Result<()> {
        let display = command_display(&self.program, &self.args);
        println!("+ {display}");

        let mut command = Command::new(&self.program);
        command
            .args(&self.args)
            .current_dir(&self.current_dir)
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());

        for (name, value) in self.envs {
            command.env(name, value);
        }

        let status = command.status().map_err(|error| {
            format!(
                "failed to run `{display}` in {}: {error}",
                self.current_dir.display()
            )
        })?;

        if status.success() {
            Ok(())
        } else {
            Err(format!("`{display}` failed with {status}"))
        }
    }
}

fn command_display(program: &OsStr, args: &[OsString]) -> String {
    let mut display = program.to_string_lossy().into_owned();
    for arg in args {
        display.push(' ');
        display.push_str(&arg.to_string_lossy());
    }
    display
}
