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

pub(crate) type StepRunner = fn(&Xtask, &[String]) -> Result<()>;

/// Definition of each leaf xtask step.
///
/// Steps are the commands GitHub Actions should schedule directly. Higher
/// levels such as `ci job linux-test` and `ci workflow rust` compose these
/// definitions with concrete arguments.
///
/// # Example
///
/// StepInfo {
///     command: "fmt",
///     help_usage: "fmt [--write] [--allow-dirty]",
///     help_description: "Check or fix Rust formatting",
///     error_message: "Rust formatting failed",
///     runner: Xtask::run_fmt,
/// }
///
/// After executing `cargo xtask ci step fmt`, the `runner` is called. If it
/// fails, `error_message` is displayed with guidance for fixing the failure.
///
/// `help_usage` and `help_description` are displayed in `cargo xtask help`.
pub(crate) struct StepInfo {
    pub(crate) command: &'static str,
    pub(crate) runner: StepRunner,
    pub(crate) error_message: &'static str,
    pub(crate) help_usage: &'static str,
    pub(crate) help_description: &'static str,
}

/// An invocation of a step.
///
/// * command: the step command, resolved through `STEPS`
/// * args: the arguments for this invocation
pub(crate) struct StepCall {
    pub(crate) command: &'static str,
    pub(crate) args: &'static [&'static str],
}

impl StepCall {
    pub(crate) fn step(&self) -> Result<&'static StepInfo> {
        find_step(self.command).ok_or_else(|| {
            format!("unknown ci step `{}` in job definition", self.command)
        })
    }

    pub(crate) fn command_line(&self) -> String {
        let mut command_line = self.command.to_string();
        for arg in self.args {
            command_line.push(' ');
            command_line.push_str(arg);
        }
        command_line
    }
}

pub(crate) fn find_step(command: &str) -> Option<&'static StepInfo> {
    STEPS.iter().find(|step| step.command == command)
}

pub(crate) const STEPS: &[StepInfo] = &[
    StepInfo {
        command: "fmt",
        help_usage: "fmt [--write] [--allow-dirty]",
        help_description: "Check or fix Rust formatting",
        error_message: "Rust formatting failed. Run `cargo xtask ci step fmt --write --allow-dirty` and commit the result.",
        runner: Xtask::run_fmt,
    },
    StepInfo {
        command: "clippy",
        help_usage: "clippy [--write] [--allow-dirty]",
        help_description: "Run the CI clippy suite",
        error_message: "Rust clippy failed. Run `cargo xtask ci step clippy` locally and fix all warnings.",
        runner: Xtask::run_clippy,
    },
    // GitHub uses this command for the base workspace cargo check and for the
    // package feature-check steps. The workspace target also verifies
    // `Cargo.lock` with `--locked`; the datafusion-common target intentionally
    // checks only its limited standalone feature set.
    StepInfo {
        command: "rust-check",
        help_usage: "rust-check [workspace|datafusion|datafusion-common|datafusion-functions|datafusion-proto|datafusion-substrait]",
        help_description: "Run CI cargo check variants",
        error_message: "Rust check failed. For workspace lockfile failures, regenerate and commit Cargo.lock; otherwise run the same `cargo xtask ci step rust-check ...` target locally and fix the reported compile error.",
        runner: Xtask::run_rust_check,
    },
    // Covers the Linux workspace test step, the datafusion-cli test step, and the
    // macOS datafusion-ffi test step. macOS is narrowed to `datafusion-ffi`
    // because amd64 cannot reproduce FFI cdylib loading differences (`.dylib`
    // vs `.so` resolution in datafusion/ffi/src/tests/utils.rs). Other
    // historically macOS-only failures came from datafusion-benchmarks (now
    // covered on amd64) or flaky sqllogictest metrics.
    StepInfo {
        command: "rust-test",
        help_usage: "rust-test [workspace|datafusion-cli|datafusion-ffi]",
        help_description: "Run CI Rust test variants (defaults to workspace)",
        error_message: "Rust tests failed. Re-run the same `cargo xtask ci step rust-test ...` target locally and fix the failing test.",
        runner: Xtask::run_rust_test,
    },
    // Runs documentation examples through `cargo test --doc`.
    StepInfo {
        command: "rust-doctest",
        help_usage: "rust-doctest",
        help_description: "Run Rust doctests",
        error_message: "Rust doctests failed. Run `cargo xtask ci step rust-doctest` locally and fix the failing documentation example.",
        runner: Xtask::run_rust_doctest,
    },
    // Builds Rust documentation to ensure rustdoc is clean.
    StepInfo {
        command: "rust-doc",
        help_usage: "rust-doc",
        help_description: "Build Rust documentation with warnings denied",
        error_message: "Rust docs failed. Run `cargo xtask ci step rust-doc` locally and fix rustdoc warnings or errors.",
        runner: Xtask::run_rust_doc,
    },
    StepInfo {
        command: "rust-examples",
        help_usage: "rust-examples",
        help_description: "Run Rust examples checks",
        error_message: "Rust examples failed. Run `cargo xtask ci step rust-examples` locally and fix the failing example.",
        runner: Xtask::run_rust_examples,
    },
    StepInfo {
        command: "rust-wasm",
        help_usage: "rust-wasm",
        help_description: "Run wasm-pack tests",
        error_message: "Rust wasm tests failed. Run `cargo xtask ci step rust-wasm` locally with wasm-pack installed and fix the wasm test failure.",
        runner: Xtask::run_rust_wasm,
    },
    // Verifies that the benchmark queries return the expected results.
    StepInfo {
        command: "rust-benchmark-results",
        help_usage: "rust-benchmark-results",
        help_description: "Generate TPC-H data (sf=0.1) and verify benchmark results",
        error_message: "Rust benchmark result verification failed. Run `cargo xtask ci step rust-benchmark-results` locally and update or fix the affected benchmark expectation.",
        runner: Xtask::run_rust_benchmark_results,
    },
    // Specialized sqllogictest variants:
    // - postgres: reference tests for applicable SLTs against Postgres
    // - substrait: Substrait round-trip tests for applicable SLTs
    //
    // The Substrait round-trip variant is intentionally limited to `limit.slt`
    // until https://github.com/apache/datafusion/issues/16248 is addressed.
    StepInfo {
        command: "rust-sqllogictest",
        help_usage: "rust-sqllogictest [postgres|substrait]",
        help_description: "Run specialized sqllogictests. (PostgreSQL reference test, or substrait round-trip test)",
        error_message: "Rust sqllogictest failed. Re-run the same `cargo xtask ci step rust-sqllogictest ...` target locally and fix the failing sqllogictest.",
        runner: Xtask::run_rust_sqllogictest,
    },
    StepInfo {
        command: "rust-vendor",
        help_usage: "rust-vendor",
        help_description: "Verify vendored generated code",
        error_message: concat!(
            "Rust vendor verification failed. Verify the workspace is clean.\n",
            "If this fails, run `./datafusion/proto/regen.sh` and check in the results."
        ),
        runner: Xtask::run_rust_vendor,
    },
    StepInfo {
        command: "rust-toml-format",
        help_usage: "rust-toml-format [--write] [--allow-dirty]",
        help_description: "Check or fix Cargo.toml formatting",
        error_message: "Cargo.toml formatting failed. Run `cargo xtask ci step rust-toml-format --write --allow-dirty`, then commit the result.",
        runner: Xtask::run_rust_toml_format,
    },
    StepInfo {
        command: "rust-config-docs",
        help_usage: "rust-config-docs",
        help_description: "Verify generated config and function docs",
        error_message: "Config or function docs verification failed. Run `cargo xtask ci step rust-config-docs` and commit the generated docs.",
        runner: Xtask::run_rust_config_docs,
    },
    // Keeps `datafusion-examples/README.md` in sync with the Rust examples by
    // regenerating the README, formatting it with Prettier, and checking it
    // against the committed file.
    StepInfo {
        command: "rust-examples-docs",
        help_usage: "rust-examples-docs",
        help_description: "Verify examples README generation",
        error_message: "Examples docs verification failed. Run `cargo xtask ci step rust-examples-docs` and commit the updated datafusion-examples/README.md.",
        runner: Xtask::run_rust_examples_docs,
    },
    // Verifies MSRV for crates directly used by downstream projects:
    // datafusion, datafusion-substrait, and datafusion-proto.
    StepInfo {
        command: "rust-msrv",
        help_usage: "rust-msrv [all|datafusion|datafusion-substrait|datafusion-proto]",
        help_description: "Run cargo-msrv verification",
        error_message: "Rust MSRV verification failed. Some code or dependency may require a newer Rust version; review the DataFusion Rust Version Compatibility Policy before updating Cargo.toml.",
        runner: Xtask::run_rust_msrv,
    },
    // Used after tests and code generation to ensure CI did not leave temporary
    // files or generated diffs. The untracked mode ignores rust-cache's `false/`
    // marker directory.
    StepInfo {
        command: "verify-clean",
        help_usage: "verify-clean [--untracked]",
        help_description: "Verify the git worktree is clean",
        error_message: concat!(
            "Worktree cleanliness verification failed.\n",
            "  - For local runs, ensure all expected changes are committed before running this check.\n",
            "  - Next, try `git diff` and `git status --porcelain`; then remove temporary files or commit expected generated output."
        ),
        runner: Xtask::run_verify_clean,
    },
];
