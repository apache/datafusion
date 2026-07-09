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
use std::env;
use std::ffi::OsStr;
use std::fs;
use std::path::Path;
use std::process::Command;

// ---------------------------------------------
// Leaf step runner implementations and utilities.
// ---------------------------------------------

impl Xtask {
    // Step runner entry point for `cargo xtask ci step fmt ...`.
    // See the corresponding `fmt` StepInfo for supported args.
    pub(crate) fn run_fmt(&self, args: &[String]) -> Result<()> {
        self.script("ci/scripts/rust_fmt.sh")
            .args(args.iter().map(String::as_str))
            .run()
    }

    // Step runner entry point for `cargo xtask ci step clippy ...`.
    // See the corresponding `clippy` StepInfo for supported args.
    pub(crate) fn run_clippy(&self, args: &[String]) -> Result<()> {
        self.command("rustup")
            .args(["component", "add", "clippy"])
            .run()?;
        self.script("ci/scripts/rust_clippy.sh")
            .args(args.iter().map(String::as_str))
            .run()
    }

    // Step runner entry point for `cargo xtask ci step rust-check ...`.
    // See the corresponding `rust-check` StepInfo for supported args.
    pub(crate) fn run_rust_check(&self, args: &[String]) -> Result<()> {
        let target = args.first().map(String::as_str).unwrap_or("workspace");
        if args.len() > 1 {
            return Err("usage: cargo xtask ci step rust-check [target]".to_string());
        }

        match target {
            "workspace" => self
                .command("cargo")
                .args([
                    "check",
                    "--profile",
                    "ci",
                    "--workspace",
                    "--all-targets",
                    "--features",
                    "integration-tests",
                    "--locked",
                ])
                .run(),
            "datafusion-common" => {
                self.command("cargo")
                    .args([
                        "check",
                        "--profile",
                        "ci",
                        "--all-targets",
                        "-p",
                        "datafusion-common",
                    ])
                    .run()?;
                self.command("cargo")
                    .args([
                        "check",
                        "--profile",
                        "ci",
                        "--no-default-features",
                        "-p",
                        "datafusion-common",
                    ])
                    .run()
            }
            "datafusion-substrait" => {
                self.command("cargo")
                    .args([
                        "check",
                        "--profile",
                        "ci",
                        "--all-targets",
                        "-p",
                        "datafusion-substrait",
                    ])
                    .run()?;
                self.command("cargo")
                    .args([
                        "check",
                        "--profile",
                        "ci",
                        "--no-default-features",
                        "-p",
                        "datafusion-substrait",
                    ])
                    .run()?;
                self.command("cargo")
                    .args([
                        "check",
                        "--profile",
                        "ci",
                        "--no-default-features",
                        "-p",
                        "datafusion-substrait",
                        "--features=physical",
                    ])
                    .run()?;
                self.command("cargo")
                    .args([
                        "check",
                        "--profile",
                        "ci",
                        "--no-default-features",
                        "-p",
                        "datafusion-substrait",
                        "--features=protoc",
                    ])
                    .run()
            }
            "datafusion-proto" => {
                self.command("cargo")
                    .args([
                        "check",
                        "--profile",
                        "ci",
                        "--all-targets",
                        "-p",
                        "datafusion-proto",
                    ])
                    .run()?;
                self.command("cargo")
                    .args([
                        "check",
                        "--profile",
                        "ci",
                        "--no-default-features",
                        "-p",
                        "datafusion-proto",
                    ])
                    .run()?;
                for feature in ["json", "parquet", "avro"] {
                    self.command("cargo")
                        .args([
                            "check",
                            "--profile",
                            "ci",
                            "--no-default-features",
                            "-p",
                            "datafusion-proto",
                        ])
                        .arg(format!("--features={feature}"))
                        .run()?;
                }
                Ok(())
            }
            "datafusion" => {
                self.command("cargo")
                    .args([
                        "check",
                        "--profile",
                        "ci",
                        "--all-targets",
                        "-p",
                        "datafusion",
                    ])
                    .run()?;
                self.command("cargo")
                    .args([
                        "check",
                        "--profile",
                        "ci",
                        "--no-default-features",
                        "-p",
                        "datafusion",
                    ])
                    .run()?;
                for feature in [
                    "nested_expressions",
                    "array_expressions",
                    "avro",
                    "backtrace",
                    "compression",
                    "crypto_expressions",
                    "datetime_expressions",
                    "encoding_expressions",
                    "force_hash_collisions",
                    "math_expressions",
                    "parquet",
                    "regex_expressions",
                    "recursive_protection",
                    "serde",
                    "sql",
                    "string_expressions",
                    "unicode_expressions",
                    "parquet_encryption",
                ] {
                    self.command("cargo")
                        .args([
                            "check",
                            "--profile",
                            "ci",
                            "--no-default-features",
                            "-p",
                            "datafusion",
                        ])
                        .arg(format!("--features={feature}"))
                        .run()?;
                }
                Ok(())
            }
            "datafusion-functions" => {
                self.command("cargo")
                    .args([
                        "check",
                        "--profile",
                        "ci",
                        "--all-targets",
                        "-p",
                        "datafusion-functions",
                    ])
                    .run()?;
                self.command("cargo")
                    .args([
                        "check",
                        "--profile",
                        "ci",
                        "--no-default-features",
                        "-p",
                        "datafusion-functions",
                    ])
                    .run()?;
                for feature in [
                    "crypto_expressions",
                    "datetime_expressions",
                    "encoding_expressions",
                    "math_expressions",
                    "regex_expressions",
                    "string_expressions",
                    "unicode_expressions",
                ] {
                    self.command("cargo")
                        .args([
                            "check",
                            "--profile",
                            "ci",
                            "--no-default-features",
                            "-p",
                            "datafusion-functions",
                        ])
                        .arg(format!("--features={feature}"))
                        .run()?;
                }
                Ok(())
            }
            unknown => Err(format!("unknown rust-check target `{unknown}`")),
        }
    }

    // Step runner entry point for `cargo xtask ci step rust-test ...`.
    // See the corresponding `rust-test` StepInfo for supported args.
    pub(crate) fn run_rust_test(&self, args: &[String]) -> Result<()> {
        let target = args.first().map(String::as_str).unwrap_or("workspace");
        if args.len() > 1 {
            return Err(
                "usage: cargo xtask ci step rust-test [workspace|datafusion-cli|datafusion-ffi]"
                    .to_string(),
            );
        }

        match target {
            "workspace" => self
                .command("cargo")
                .args([
                    "test",
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
                ])
                .env("RUST_BACKTRACE", "1")
                .run(),
            "datafusion-cli" => self
                .command("cargo")
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
                .env("AWS_ALLOW_HTTP", "true")
                .run(),
            "datafusion-ffi" => self
                .command("cargo")
                .args([
                    "test",
                    "--profile",
                    "ci",
                    "-p",
                    "datafusion-ffi",
                    "--lib",
                    "--tests",
                    "--features",
                    "integration-tests",
                ])
                .run(),
            unknown => Err(format!("unknown rust-test target `{unknown}`")),
        }
    }

    // Step runner entry point for `cargo xtask ci step rust-doctest ...`.
    // See the corresponding `rust-doctest` StepInfo for supported args.
    pub(crate) fn run_rust_doctest(&self, _args: &[String]) -> Result<()> {
        self.command("cargo")
            .args([
                "test",
                "--profile",
                "ci",
                "--doc",
                "--features",
                "avro,json",
            ])
            .run()
    }

    // Step runner entry point for `cargo xtask ci step rust-doc ...`.
    // See the corresponding `rust-doc` StepInfo for supported args.
    pub(crate) fn run_rust_doc(&self, _args: &[String]) -> Result<()> {
        self.script("ci/scripts/rust_docs.sh").run()
    }

    // Step runner entry point for `cargo xtask ci step rust-examples ...`.
    // See the corresponding `rust-examples` StepInfo for supported args.
    pub(crate) fn run_rust_examples(&self, _args: &[String]) -> Result<()> {
        self.command("cargo")
            .args(["run", "--profile", "ci", "--example", "sql"])
            .run()?;
        self.script("ci/scripts/rust_example.sh").run()
    }

    // Step runner entry point for `cargo xtask ci step rust-wasm ...`.
    // See the corresponding `rust-wasm` StepInfo for supported args.
    pub(crate) fn run_rust_wasm(&self, _args: &[String]) -> Result<()> {
        let wasm_dir = self.root.join("datafusion/wasmtest");
        let rustflags = r#"--cfg getrandom_backend="wasm_js" -C debuginfo=none"#;
        self.command("wasm-pack")
            .args(["test", "--headless", "--firefox"])
            .current_dir(&wasm_dir)
            .env("RUSTFLAGS", rustflags)
            .run()?;

        let chrome_driver = format!(
            "{}/chromedriver",
            env::var("CHROMEWEBDRIVER").unwrap_or_default()
        );
        self.command("wasm-pack")
            .args([
                "test",
                "--headless",
                "--chrome",
                "--chromedriver",
                chrome_driver.as_str(),
            ])
            .current_dir(&wasm_dir)
            .env("RUSTFLAGS", rustflags)
            .run()
    }

    // Step runner entry point for `cargo xtask ci step rust-benchmark-results ...`.
    // See the corresponding `rust-benchmark-results` StepInfo for supported args.
    //
    // Generate TPCH data, then run checks that require `TPCH_DATA`:
    // - benchmark Rust tests for TPCH plan round-trip coverage
    // - sqllogictests for TPCH plans and query answers
    pub(crate) fn run_rust_benchmark_results(&self, _args: &[String]) -> Result<()> {
        let tpch_data = self
            .root
            .join("datafusion/sqllogictest/test_files/tpch/data");
        fs::create_dir_all(&tpch_data).map_err(|error| {
            format!("failed to create {}: {error}", tpch_data.display())
        })?;

        self.command("git")
            .args(["clone", "https://github.com/databricks/tpch-dbgen.git"])
            .run()?;

        let tpch_dbgen = self.root.join("tpch-dbgen");
        self.command("make").current_dir(&tpch_dbgen).run()?;
        self.command("./dbgen")
            .args(["-f", "-s", "0.1"])
            .current_dir(&tpch_dbgen)
            .run()?;
        self.move_tbl_files(&tpch_dbgen, &tpch_data)?;

        let tpch_data = fs::canonicalize(&tpch_data).map_err(|error| {
            format!("failed to canonicalize {}: {error}", tpch_data.display())
        })?;
        let tpch_data = tpch_data
            .to_str()
            .ok_or_else(|| format!("{} is not valid UTF-8", tpch_data.display()))?;

        self.command("cargo")
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
            .env("TPCH_DATA", tpch_data)
            .run()?;
        self.command("cargo")
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
            .env("TPCH_DATA", tpch_data)
            .env("INCLUDE_TPCH", "true")
            .run()
    }

    // Step runner entry point for `cargo xtask ci step rust-sqllogictest ...`.
    //
    // Supported targets:
    // - postgres: reference tests for applicable SLTs against Postgres
    // - substrait: Substrait round-trip tests for applicable SLTs
    pub(crate) fn run_rust_sqllogictest(&self, args: &[String]) -> Result<()> {
        match args {
            [target] if target == "postgres" => {
                let (host, port) = if matches!(
                    env::var("GITHUB_ACTIONS").as_deref(),
                    Ok("true")
                ) {
                    // GitHub container jobs reach services by label.
                    (
                        env::var("POSTGRES_HOST")
                            .unwrap_or_else(|_| "postgres".to_string()),
                        env::var("POSTGRES_PORT").unwrap_or_else(|_| "5432".to_string()),
                    )
                } else {
                    (
                        env::var("POSTGRES_HOST").map_err(|_| {
                            "POSTGRES_HOST must be set for local Postgres sqllogictest runs"
                                .to_string()
                        })?,
                        env::var("POSTGRES_PORT").map_err(|_| {
                            "POSTGRES_PORT must be set for local Postgres sqllogictest runs"
                                .to_string()
                        })?,
                    )
                };
                let uri = format!("postgresql://postgres:postgres@{host}:{port}/db_test");
                self.command("cargo")
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
                    .env("PG_URI", uri.as_str())
                    .run()
            }
            [target] if target == "substrait" => self
                .command("cargo")
                .args([
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
                ])
                .run(),
            _ => Err(
                "usage: cargo xtask ci step rust-sqllogictest [postgres|substrait]"
                    .to_string(),
            ),
        }
    }

    // Step runner entry point for `cargo xtask ci step rust-vendor ...`.
    // See the corresponding `rust-vendor` StepInfo for supported args.
    pub(crate) fn run_rust_vendor(&self, _args: &[String]) -> Result<()> {
        self.command("./regen.sh")
            .current_dir(self.root.join("datafusion/proto"))
            .run()?;
        self.run_verify_clean(&[])
    }

    // Step runner entry point for `cargo xtask ci step rust-toml-format ...`.
    // See the corresponding `rust-toml-format` StepInfo for supported args.
    pub(crate) fn run_rust_toml_format(&self, args: &[String]) -> Result<()> {
        self.ensure_tool(
            "taplo",
            self.command("cargo").args([
                "+stable",
                "install",
                "taplo-cli",
                "--version",
                "^0.9",
                "--locked",
            ]),
        )?;
        self.script("ci/scripts/rust_toml_fmt.sh")
            .args(args.iter().map(String::as_str))
            .run()
    }

    // Step runner entry point for `cargo xtask ci step rust-config-docs ...`.
    // See the corresponding `rust-config-docs` StepInfo for supported args.
    pub(crate) fn run_rust_config_docs(&self, _args: &[String]) -> Result<()> {
        self.command("./dev/update_config_docs.sh").run()?;
        self.run_verify_clean(&[])?;
        self.command("./dev/update_function_docs.sh").run()?;
        self.run_verify_clean(&[])
    }

    // Step runner entry point for `cargo xtask ci step rust-examples-docs ...`.
    // See the corresponding `rust-examples-docs` StepInfo for supported args.
    pub(crate) fn run_rust_examples_docs(&self, _args: &[String]) -> Result<()> {
        self.command("bash")
            .args(["ci/scripts/check_examples_docs.sh"])
            .run()
    }

    // Step runner entry point for `cargo xtask ci step rust-msrv ...`.
    // See the corresponding `rust-msrv` StepInfo for supported args.
    pub(crate) fn run_rust_msrv(&self, args: &[String]) -> Result<()> {
        let target = args.first().map(String::as_str).unwrap_or("all");
        if args.len() > 1 {
            return Err("usage: cargo xtask ci step rust-msrv [all|datafusion|datafusion-substrait|datafusion-proto]".to_string());
        }

        match target {
            "all" => {
                for target in ["datafusion", "datafusion-substrait", "datafusion-proto"] {
                    self.run_rust_msrv(&[target.to_string()])?;
                }
                Ok(())
            }
            "datafusion" => self.cargo_msrv("datafusion/core"),
            "datafusion-substrait" => self.cargo_msrv("datafusion/substrait"),
            "datafusion-proto" => self.cargo_msrv("datafusion/proto"),
            unknown => Err(format!("unknown rust-msrv target `{unknown}`")),
        }
    }

    // Step runner entry point for `cargo xtask ci step verify-clean ...`.
    // See the corresponding `verify-clean` StepInfo for supported args.
    pub(crate) fn run_verify_clean(&self, args: &[String]) -> Result<()> {
        let include_untracked = match args {
            [] => false,
            [flag] if flag == "--untracked" => true,
            _ => {
                return Err(
                    "usage: cargo xtask ci step verify-clean [--untracked]".to_string()
                );
            }
        };

        self.command("git").args(["diff", "--exit-code"]).run()?;

        if include_untracked {
            let output = Command::new("git")
                .args(["status", "--porcelain"])
                .current_dir(&self.root)
                .output()
                .map_err(|error| {
                    format!("failed to run `git status --porcelain`: {error}")
                })?;

            if !output.status.success() {
                return Err(format!(
                    "`git status --porcelain` failed with {}",
                    output.status
                ));
            }

            let status = String::from_utf8(output.stdout)
                .map_err(|error| format!("git status output was not UTF-8: {error}"))?;
            let unexpected = status
                .lines()
                .filter(|line| *line != "?? false/" && *line != "?? false")
                .collect::<Vec<_>>();

            if !unexpected.is_empty() {
                for line in unexpected {
                    println!("{line}");
                }
                return Err("working directory contains untracked files".to_string());
            }
        }

        Ok(())
    }

    fn cargo_msrv(&self, relative_dir: &str) -> Result<()> {
        self.command("cargo")
            .args([
                "msrv",
                "--output-format",
                "json",
                "--log-target",
                "stdout",
                "verify",
                // Avoid the default rust-changelog source, which is subject to GitHub rate limits.
                "--release-source",
                "rust-dist",
            ])
            .current_dir(self.root.join(relative_dir))
            .run()
    }

    fn move_tbl_files(&self, source_dir: &Path, destination_dir: &Path) -> Result<()> {
        let mut moved_files = 0;
        for entry in fs::read_dir(source_dir).map_err(|error| {
            format!("failed to read {}: {error}", source_dir.display())
        })? {
            let entry = entry
                .map_err(|error| format!("failed to read directory entry: {error}"))?;
            let source = entry.path();
            if source.extension() == Some(OsStr::new("tbl")) {
                let destination = destination_dir.join(entry.file_name());
                fs::rename(&source, &destination).map_err(|error| {
                    format!(
                        "failed to move {} to {}: {error}",
                        source.display(),
                        destination.display()
                    )
                })?;
                moved_files += 1;
            }
        }

        if moved_files == 0 {
            return Err(format!("no .tbl files found in {}", source_dir.display()));
        }

        Ok(())
    }
}
