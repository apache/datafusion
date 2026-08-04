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

use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion_substrait::function_yaml::generate_function_extension;
use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::Path;

const DATAFUSION_FUNCTIONS_PATH: &str =
    "datafusion/substrait/extensions/functions_datafusion.yaml";

fn main() -> Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    let contents = generated_yaml()?;

    match args.as_slice() {
        [] => print_file(DATAFUSION_FUNCTIONS_PATH, &contents),
        [flag] if flag == "--check" => check_file(DATAFUSION_FUNCTIONS_PATH, &contents),
        [flag] if flag == "--write" => write_file(DATAFUSION_FUNCTIONS_PATH, &contents),
        _ => exec_err!("Usage: print_substrait_function_yaml [--check|--write]"),
    }
}

fn generated_yaml() -> Result<String> {
    serde_yaml::to_string(&generate_function_extension()?)
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

fn print_file(path: &str, contents: &str) -> Result<()> {
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "# {path}")?;
    write!(stdout, "{contents}")?;
    Ok(())
}

fn check_file(path: &str, contents: &str) -> Result<()> {
    match fs::read_to_string(path) {
        Ok(actual) if actual == contents => Ok(()),
        Ok(_) => exec_err!(
            "generated Substrait function YAML is out of date: {}. Run `cargo run -p datafusion-substrait --bin print_substrait_function_yaml -- --write`",
            path
        ),
        Err(e) => exec_err!("failed to read {path}: {e}"),
    }
}

fn write_file(path: &str, contents: &str) -> Result<()> {
    let path = Path::new(path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, contents)?;
    Ok(())
}
