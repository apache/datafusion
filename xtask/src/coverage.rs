// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Workspace coverage with one SLT run and separate SLT-only and combined reports.

use crate::Result;
use crate::command::{CommandSpec, EnvReader};
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

fn cargo(root: &Path, env_reader: EnvReader, args: &[String]) -> Result<CommandSpec> {
    let mut builder = CommandSpec::builder(env_reader);
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

fn llvm_cov(root: &Path, env_reader: EnvReader, args: &[String]) -> Result<CommandSpec> {
    let mut command = vec!["llvm-cov".to_string()];
    command.extend_from_slice(args);
    cargo(root, env_reader, &command)
}

struct TestTarget {
    package: String,
    name: String,
    features: BTreeSet<String>,
    enabled: bool,
}

fn workspace_tests(
    root: &Path,
    env_reader: EnvReader,
    selection: &[String],
) -> Result<(Vec<String>, Vec<TestTarget>)> {
    let metadata = cargo(
        root,
        env_reader,
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
    let tree = String::from_utf8(cargo(root, env_reader, &args)?.output()?)
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
    let mut tests = Vec::new();
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
            if is_test {
                let enabled_target = required
                    .iter()
                    .all(|feature| feature.as_str().is_some_and(|f| enabled.contains(f)));
                tests.push(TestTarget {
                    package: name.to_string(),
                    name: string(&target["name"])?.to_string(),
                    features: enabled.clone(),
                    enabled: enabled_target,
                });
            }
        }
    }
    if packages.len() != features.len() {
        return Err("Cargo tree selected packages absent from Cargo metadata".to_string());
    }
    let slt = tests
        .iter()
        .filter(|test| test.name == "sqllogictests")
        .collect::<Vec<_>>();
    if slt.len() != 1 || !slt[0].enabled {
        return Err(
            "Cargo selection must include exactly one enabled sqllogictests target"
                .to_string(),
        );
    }
    Ok((packages, tests))
}

fn commands(
    root: &Path,
    env_reader: EnvReader,
    selection: &[String],
    packages: &[String],
    tests: &[TestTarget],
) -> Result<Vec<CommandSpec>> {
    let mut report = strings(&["report", "--profile", "ci", "--codecov"]);
    for package in packages {
        report.extend(["--package".to_string(), package.clone()]);
    }
    let write_report = |path: &str| {
        let mut args = report.clone();
        args.extend(strings(&["--output-path", path]));
        llvm_cov(root, env_reader, &args)
    };

    let mut commands = vec![llvm_cov(
        root,
        env_reader,
        &strings(&["clean", "--workspace"]),
    )?];
    let mut common = strings(&["--profile", "ci"]);
    common.extend_from_slice(selection);
    let mut args = common.clone();
    args.extend(strings(&["--no-report", "--test", "sqllogictests"]));
    // Report before other tests add their profiles to the SLT-only coverage.
    commands.push(llvm_cov(root, env_reader, &args)?);
    commands.push(write_report("target/codecov-slt.json")?);

    for args in remaining_test_args(selection, tests) {
        commands.push(llvm_cov(root, env_reader, &args)?);
    }
    commands.push(write_report("target/codecov.json")?);
    Ok(commands)
}

fn remaining_test_args(selection: &[String], tests: &[TestTarget]) -> Vec<Vec<String>> {
    let disabled = tests
        .iter()
        .filter(|test| !test.enabled)
        .map(|test| &test.name)
        .collect::<BTreeSet<_>>();
    // --tests would run SLT twice. Explicit --test names must also avoid
    // matching a feature-disabled target with the same name in another package.
    let mut workspace = strings(&["--profile", "ci"]);
    workspace.extend_from_slice(selection);
    workspace.extend(strings(&["--no-report", "--lib", "--bins"]));
    let mut separate = Vec::new();
    let mut workspace_names = BTreeSet::new();
    for test in tests
        .iter()
        .filter(|test| test.enabled && test.name != "sqllogictests")
    {
        if !disabled.contains(&test.name) {
            workspace_names.insert(&test.name);
        } else {
            let mut args = strings(&[
                "--profile",
                "ci",
                "--no-report",
                "--no-default-features",
                "--package",
            ]);
            args.push(test.package.clone());
            if !test.features.is_empty() {
                args.extend([
                    "--features".to_string(),
                    test.features.iter().cloned().collect::<Vec<_>>().join(","),
                ]);
            }
            args.extend(["--test".to_string(), test.name.clone()]);
            separate.push(args);
        }
    }
    for name in workspace_names {
        workspace.extend(["--test".to_string(), name.clone()]);
    }
    let mut commands = vec![workspace];
    commands.extend(separate);
    commands
}

pub(crate) fn run(root: &Path, env_reader: EnvReader) -> Result<()> {
    let selection = package_args();
    let (packages, tests) = workspace_tests(root, env_reader, &selection)?;
    for command in commands(root, env_reader, &selection, &packages, &tests)? {
        command.execute()?;
    }
    Ok(())
}

pub(crate) fn explanation(root: &Path, env_reader: EnvReader) -> Result<String> {
    let mut builder = CommandSpec::builder(env_reader);
    builder
        .program("cargo")
        .args(["xtask", "ci", "step", "test", "workspace"])
        .current_dir(root)
        .failure_message("Workspace coverage failed")
        .doc("Resolve coverage targets at runtime; run SLT once and write both reports.");
    Ok(builder.build()?.explanation())
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
    use crate::command::system_env;

    fn fixture() -> tempfile::TempDir {
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
        std::fs::create_dir_all(root.join("core/tests")).unwrap();
        for target in ["sqllogictests", "enabled", "excluded"] {
            std::fs::write(root.join("core/tests").join(format!("{target}.rs")), "")
                .unwrap();
        }
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
path = "tests/sqllogictests.rs"
[[test]]
name = "enabled"
path = "tests/enabled.rs"
required-features = ["enabled"]
[[test]]
name = "excluded"
path = "tests/excluded.rs"
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
        dir
    }

    #[test]
    fn excluded_member_does_not_enable_an_integration_target() -> Result<()> {
        let dir = fixture();
        let root = dir.path();
        cargo(
            root,
            system_env,
            &strings(&["generate-lockfile", "--offline"]),
        )?
        .execute()?;
        let selection = strings(&[
            "--workspace",
            "--exclude",
            "datafusion-examples",
            "--features",
            "enabled",
        ]);
        let (packages, tests) = workspace_tests(root, system_env, &selection)?;
        assert_eq!(packages, ["coverage-core"]);
        assert_eq!(
            tests
                .iter()
                .filter(|test| test.enabled)
                .map(|test| test.name.clone())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["enabled".to_string(), "sqllogictests".to_string()])
        );
        let commands = commands(root, system_env, &selection, &packages, &tests)?;
        let rendered = commands
            .iter()
            .map(CommandSpec::full_command)
            .collect::<Vec<_>>();
        let rendered = rendered
            .join("\n\n")
            .replace(root.to_str().unwrap(), "/workspace");
        insta::assert_snapshot!(rendered, @r"
cd /workspace && \
RUST_BACKTRACE=1 \
cargo llvm-cov clean \
--workspace

cd /workspace && \
RUST_BACKTRACE=1 \
cargo llvm-cov \
--profile ci \
--workspace \
--exclude datafusion-examples \
--features enabled \
--no-report \
--test sqllogictests

cd /workspace && \
RUST_BACKTRACE=1 \
cargo llvm-cov report \
--profile ci \
--codecov \
--package coverage-core \
--output-path target/codecov-slt.json

cd /workspace && \
RUST_BACKTRACE=1 \
cargo llvm-cov \
--profile ci \
--workspace \
--exclude datafusion-examples \
--features enabled \
--no-report \
--lib \
--bins \
--test enabled

cd /workspace && \
RUST_BACKTRACE=1 \
cargo llvm-cov report \
--profile ci \
--codecov \
--package coverage-core \
--output-path target/codecov.json
");
        let (_, tests) = workspace_tests(
            root,
            system_env,
            &strings(&["--workspace", "--features", "enabled"]),
        )?;
        assert!(
            tests
                .iter()
                .any(|test| test.enabled && test.name == "excluded")
        );
        Ok(())
    }

    #[test]
    fn same_named_targets_keep_package_feature_requirements() -> Result<()> {
        let dir = fixture();
        let root = dir.path();
        std::fs::write(root.join("Cargo.toml"), "[workspace]\nmembers = ['core', 'examples', 'other']\nresolver = '3'\n[profile.ci]\ninherits = 'dev'\n").unwrap();
        std::fs::create_dir_all(root.join("other/src")).unwrap();
        std::fs::write(root.join("other/src/lib.rs"), "").unwrap();
        std::fs::create_dir_all(root.join("other/tests")).unwrap();
        std::fs::write(root.join("other/tests/enabled.rs"), "").unwrap();
        std::fs::write(
            root.join("other/Cargo.toml"),
            r#"
[package]
name = "coverage-other"
version = "0.1.0"
edition = "2024"
[features]
disabled = []
[[test]]
name = "enabled"
path = "tests/enabled.rs"
required-features = ["disabled"]
"#,
        )
        .unwrap();
        cargo(
            root,
            system_env,
            &strings(&["generate-lockfile", "--offline"]),
        )?
        .execute()?;
        let selection = strings(&[
            "--workspace",
            "--exclude",
            "datafusion-examples",
            "--features",
            "enabled",
        ]);
        let (_, tests) = workspace_tests(root, system_env, &selection)?;
        assert!(tests.iter().any(|test| test.package == "coverage-core"
            && test.name == "enabled"
            && test.enabled));
        assert!(tests.iter().any(|test| test.package == "coverage-other"
            && test.name == "enabled"
            && !test.enabled));

        let mut built_tests = BTreeSet::new();
        for args in remaining_test_args(&selection, &tests) {
            let mut command =
                strings(&["test", "--offline", "--no-run", "--message-format=json"]);
            command.extend(args.into_iter().filter(|arg| arg != "--no-report"));
            command.extend([
                "--target-dir".to_string(),
                root.join("target").display().to_string(),
            ]);
            let output = cargo(root, system_env, &command)?.output()?;
            for artifact in
                serde_json::Deserializer::from_slice(&output).into_iter::<Value>()
            {
                let artifact = artifact.map_err(|error| error.to_string())?;
                if artifact["reason"] == "compiler-artifact"
                    && artifact["target"]["kind"]
                        .as_array()
                        .is_some_and(|kind| kind.iter().any(|kind| kind == "test"))
                {
                    let manifest = Path::new(string(&artifact["manifest_path"])?);
                    built_tests.insert((
                        manifest
                            .parent()
                            .unwrap()
                            .file_name()
                            .unwrap()
                            .to_string_lossy()
                            .into_owned(),
                        string(&artifact["target"]["name"])?.to_string(),
                    ));
                }
            }
        }
        assert_eq!(
            built_tests,
            BTreeSet::from([("core".to_string(), "enabled".to_string())])
        );
        Ok(())
    }
}
