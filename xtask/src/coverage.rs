// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Workspace coverage with one SLT run and separate SLT-only and combined reports.

use crate::Result;
use crate::command::{CommandSpec, system_env};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

const FEATURES: &str =
    "serde,avro,json,backtrace,integration-tests,parquet_encryption,substrait";
const EXCLUDES: &[&str] = &[
    "datafusion-examples",
    "ffi_example_table_provider",
    "datafusion-cli",
];

fn package_args() -> Vec<String> {
    let mut args = vec!["--workspace".to_string()];
    for package in EXCLUDES {
        args.extend(["--exclude".to_string(), (*package).to_string()]);
    }
    args.extend(["--features".to_string(), FEATURES.to_string()]);
    args
}

pub(crate) fn test_args() -> Vec<String> {
    let mut args = vec!["--profile".to_string(), "ci".to_string()];
    args.extend(package_args());
    args
}

fn cargo(root: &Path, args: &[String]) -> Result<CommandSpec> {
    let mut builder = CommandSpec::builder(system_env);
    builder
        .program("cargo")
        .args(args)
        .current_dir(root)
        .env("RUST_BACKTRACE", "1")
        .failure_message("Workspace coverage failed");
    if args.first().is_some_and(|arg| arg == "llvm-cov") {
        builder.requires_executable(
            "cargo-llvm-cov",
            "install it with `cargo install cargo-llvm-cov --locked`",
        );
    }
    builder.build()
}

fn llvm_cov(root: &Path, args: &[String]) -> Result<CommandSpec> {
    let mut command = vec!["llvm-cov".to_string()];
    command.extend_from_slice(args);
    cargo(root, &command)
}

fn workspace_tests(
    root: &Path,
    selection: &[String],
) -> Result<(Vec<String>, BTreeSet<String>)> {
    let metadata = cargo(
        root,
        &strings(&["metadata", "--no-deps", "--format-version", "1", "--locked"]),
    )?
    .output()?;
    let metadata: Value = serde_json::from_slice(&metadata)
        .map_err(|error| format!("invalid Cargo metadata: {error}"))?;

    // Unlike metadata's workspace-wide resolve graph, tree honors --exclude.
    let mut args = strings(&["tree"]);
    args.extend_from_slice(selection);
    args.extend(strings(&[
        "--locked",
        "--depth",
        "0",
        "--prefix",
        "none",
        "--no-dedupe",
        "--color",
        "never",
        "--format",
        "{p}|{f}",
    ]));
    let tree = String::from_utf8(cargo(root, &args)?.output()?)
        .map_err(|error| format!("invalid Cargo tree output: {error}"))?;
    let mut features = BTreeMap::<String, BTreeSet<String>>::new();
    for line in tree.lines().filter(|line| !line.is_empty()) {
        let (package, enabled) = line
            .rsplit_once('|')
            .ok_or_else(|| format!("unexpected Cargo tree entry: {line}"))?;
        let name = package
            .split_whitespace()
            .next()
            .ok_or_else(|| format!("missing package name: {line}"))?;
        features.insert(
            name.to_string(),
            enabled
                .split(',')
                .filter(|f| !f.is_empty())
                .map(String::from)
                .collect(),
        );
    }
    let mut packages = Vec::new();
    let mut tests = BTreeSet::new();
    for package in array(&metadata["packages"])? {
        let name = string(&package["name"])?;
        let Some(enabled) = features.get(name) else {
            continue;
        };
        packages.push(name.to_string());
        for target in array(&package["targets"])? {
            let is_test = array(&target["kind"])?.iter().any(|kind| kind == "test");
            let required = target
                .get("required-features")
                .map(array)
                .transpose()?
                .unwrap_or_default();
            if is_test
                && required
                    .iter()
                    .all(|feature| feature.as_str().is_some_and(|f| enabled.contains(f)))
            {
                tests.insert(string(&target["name"])?.to_string());
            }
        }
    }
    if packages.len() != features.len() {
        return Err("Cargo tree selected packages absent from Cargo metadata".to_string());
    }
    if !tests.contains("sqllogictests") {
        return Err("Cargo selection must include the sqllogictests target".to_string());
    }
    Ok((packages, tests))
}

fn commands(
    root: &Path,
    selection: &[String],
    packages: &[String],
    tests: &BTreeSet<String>,
) -> Result<Vec<CommandSpec>> {
    let mut report = strings(&["report", "--profile", "ci", "--codecov"]);
    for package in packages {
        report.extend(["--package".to_string(), package.clone()]);
    }
    let write_report = |path: &str| {
        let mut args = report.clone();
        args.extend(strings(&["--output-path", path]));
        llvm_cov(root, &args)
    };

    let mut commands = vec![llvm_cov(root, &strings(&["clean", "--workspace"]))?];
    let mut common = strings(&["--profile", "ci"]);
    common.extend_from_slice(selection);
    let mut args = common.clone();
    args.extend(strings(&["--no-report", "--test", "sqllogictests"]));
    commands.push(llvm_cov(root, &args)?);
    commands.push(write_report("target/codecov-slt.json")?);

    let mut args = common;
    args.extend(strings(&["--no-report", "--lib", "--bins"]));
    for test in tests {
        if test != "sqllogictests" {
            args.extend(["--test".to_string(), test.clone()]);
        }
    }
    commands.push(llvm_cov(root, &args)?);
    commands.push(write_report("target/codecov.json")?);
    Ok(commands)
}

pub(crate) fn run(root: &Path) -> Result<()> {
    let selection = package_args();
    let (packages, tests) = workspace_tests(root, &selection)?;
    for command in commands(root, &selection, &packages, &tests)? {
        command.execute()?;
    }
    Ok(())
}

pub(crate) fn explanation(root: &Path) -> Result<String> {
    let commands = commands(
        root,
        &package_args(),
        &["<selected-package>".to_string()],
        &BTreeSet::from(["<enabled-integration-target-except-SLT>".to_string()]),
    )?;
    Ok(format!(
        "# Package and integration-target placeholders are resolved by Cargo at execution time.\n{}",
        commands
            .iter()
            .map(CommandSpec::explanation)
            .collect::<Vec<_>>()
            .join("\n\n")
    ))
}

fn array(value: &Value) -> Result<&[Value]> {
    value
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| "expected an array in Cargo metadata".to_string())
}

fn string(value: &Value) -> Result<&str> {
    value
        .as_str()
        .ok_or_else(|| "expected a string in Cargo metadata".to_string())
}

fn strings(args: &[&str]) -> Vec<String> {
    args.iter().map(|arg| (*arg).to_string()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn excluded_member_does_not_enable_an_integration_target() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        for package in ["core", "examples"] {
            std::fs::create_dir_all(root.join(package).join("src")).unwrap();
            std::fs::write(root.join(package).join("src/lib.rs"), "").unwrap();
        }
        std::fs::write(
            root.join("Cargo.toml"),
            "[workspace]\nmembers = ['core', 'examples']\nresolver = '3'\n",
        )
        .unwrap();
        std::fs::write(
            root.join("core/Cargo.toml"),
            r#"
[package]
name = "coverage-core"
version = "0.1.0"
edition = "2024"
[features]
enabled = []
excluded-only = []
[[test]]
name = "sqllogictests"
path = "src/lib.rs"
[[test]]
name = "enabled"
path = "src/lib.rs"
required-features = ["enabled"]
[[test]]
name = "excluded"
path = "src/lib.rs"
required-features = ["excluded-only"]
"#,
        )
        .unwrap();
        std::fs::write(
            root.join("examples/Cargo.toml"),
            r#"
[package]
name = "datafusion-examples"
version = "0.1.0"
edition = "2024"
[dependencies]
coverage-core = { path = "../core", features = ["excluded-only"] }
"#,
        )
        .unwrap();
        cargo(root, &strings(&["generate-lockfile", "--offline"]))?.execute()?;
        let selection = strings(&[
            "--workspace",
            "--exclude",
            "datafusion-examples",
            "--features",
            "enabled",
        ]);
        let (packages, tests) = workspace_tests(root, &selection)?;
        assert_eq!(packages, ["coverage-core"]);
        assert_eq!(
            tests,
            BTreeSet::from(["enabled".to_string(), "sqllogictests".to_string()])
        );
        let commands = commands(root, &selection, &packages, &tests)?;
        let rendered = commands
            .iter()
            .map(CommandSpec::full_command)
            .collect::<Vec<_>>();
        let rendered = rendered
            .join("\n\n")
            .replace(root.to_str().unwrap(), "/workspace");
        insta::assert_snapshot!("workspace_coverage_commands", rendered);
        let (_, tests) =
            workspace_tests(root, &strings(&["--workspace", "--features", "enabled"]))?;
        assert!(tests.contains("excluded"));
        Ok(())
    }
}
