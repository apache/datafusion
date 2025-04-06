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

use crate::print_options::PrintOptions;
use rustyline::error::ReadlineError;

/// Assume that a pager command will look something like this:
///  `/usr/bin/less -S -MM`
/// see also locate_pager
fn pager_parts(pager_cmd: &str) -> (&str, Vec<&str>) {
    let mut parts = pager_cmd
        .split_whitespace()
        .collect::<std::collections::VecDeque<_>>();
    let exec_path = parts.pop_front().unwrap();
    let args = parts.into_iter().collect::<Vec<_>>();
    (exec_path, args)
}

/// Create a child process of the pager command from print_options, ready to ready input from its stdin.
pub(crate) fn build_pager_process(
    print_options: &PrintOptions,
) -> Result<std::process::Child, Box<dyn std::error::Error>> {
    if cfg!(target_os = "windows") {
        Err("pager not supported on windows".into())
    } else {
        use std::process::Stdio;

        if let Some(pager_cmd) = &print_options.pager {
            let (exec_path, args) = pager_parts(&pager_cmd);

            let pager = std::process::Command::new(exec_path)
                .args(&args)
                .stdout(Stdio::inherit())
                .stdin(Stdio::piped())
                .spawn()?;
            Ok(pager)
        } else {
            Err("no pager specified".into())
        }
    }
}

/// Find the default pager from the env var PAGER, or default to "less"
pub fn default_pager() -> String {
    std::env::var("PAGER").unwrap_or("less".into())
}

pub fn os_default_pager() -> Option<String> {
    if cfg!(target_os = "windows") {
        None
    } else {
        Some(default_pager())
    }
}

#[derive(Debug)]
pub struct PagerError(String);

impl std::fmt::Display for PagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::convert::From<PagerError> for ReadlineError {
    fn from(pager_error: PagerError) -> Self {
        ReadlineError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("pager command {} not found on path", pager_error),
        ))
    }
}

impl std::convert::From<PagerError> for datafusion::error::DataFusionError {
    fn from(pager_error: PagerError) -> Self {
        use datafusion::error::DataFusionError;
        DataFusionError::External(
            format!("pager command {} not found on path", pager_error).into(),
        )
    }
}

impl std::convert::From<std::env::VarError> for PagerError {
    fn from(value: std::env::VarError) -> Self {
        PagerError(format!("{}", value))
    }
}

impl std::convert::From<String> for PagerError {
    fn from(value: String) -> Self {
        PagerError(value)
    }
}

/// Search in all parts of PATH to resolve pager_cmd to its full path
/// So it will convert less -S -MM to /usr/bin/less -S -MM
fn locate_pager(pager_cmd: &str) -> Result<String, PagerError> {
    if &pager_cmd[0..1] == "/" {
        // absolute path so just use as-is
        let (first_part, _rest) = crate::split_on_first_space(pager_cmd);

        let path = std::path::PathBuf::from(first_part);
        if path.exists() {
            return Ok(pager_cmd.into());
        }
    } else {
        // non-absolute path, so find it in PATH
        let (first_part, rest) = crate::split_on_first_space(pager_cmd);
        let paths = std::env::var("PATH")?;
        let paths = paths.split(':');
        for path in paths {
            let path = std::path::PathBuf::from(path).join(first_part);
            if path.exists() {
                return Ok(format!("{} {}", path.display(), rest.unwrap_or("")));
            }
        }
    }

    Err(format!("pager command error {}", pager_cmd).into())
}

/// Parse and process pager command option
pub fn parse_pset_pager(maybe_pager: &str) -> Result<Option<String>, PagerError> {
    let maybe_pager = match maybe_pager {
        "yes" | "Y" | "y" | "true" | "default" => {
            // locate "less" or "more" or some other pager
            Some(locate_pager("less")?)
        }
        "no" | "N" | "n" | "false" | "none" => None,
        other => Some(locate_pager(other)?),
    };
    Ok(maybe_pager)
}

/// Return a String suitable for PrintOptions pager, or none if we're in a
/// non-pager OS, or not outputting to a tty, or something else goes wrong.
pub fn main_default_pager() -> Option<String> {
    use std::io::IsTerminal;

    // never use a pager if stdout is piped to something, only if it's a terminal.
    if !std::io::stdout().is_terminal() {
        return None;
    }

    os_default_pager().and_then(|pgr| {
        match parse_pset_pager(&pgr) {
            Ok(None) => {
                eprintln!("error determining default pager from {pgr}");
                None
            }
            Ok(pgr) => pgr,
            Err(e) => {
                eprintln!("error determining default pager from {pgr} - {e:?}");
                // keep going
                None
            }
        }
    })
}
