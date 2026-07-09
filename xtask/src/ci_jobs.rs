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

use crate::steps::StepCall;

/// One GitHub Actions job expressed as an ordered list of local xtask steps.
pub(crate) struct JobInfo {
    pub(crate) name: &'static str,
    pub(crate) help_description: &'static str,
    pub(crate) steps: &'static [StepCall],
}

pub(crate) const JOBS: &[JobInfo] = &[
    JobInfo {
        name: "verify-clean",
        help_description: "verify working tree is clean",
        steps: &[
            StepCall {
                command: "verify-clean",
                args: &[],
            },
            StepCall {
                command: "verify-clean",
                args: &["--untracked"],
            },
        ],
    },
    JobInfo {
        name: "linux-build-lib",
        help_description: "linux build test",
        steps: &[StepCall {
            command: "rust-check",
            args: &["workspace"],
        }],
    },
    JobInfo {
        name: "linux-datafusion-common-features",
        help_description: "cargo check datafusion-common features",
        steps: &[StepCall {
            command: "rust-check",
            args: &["datafusion-common"],
        }],
    },
    JobInfo {
        name: "linux-datafusion-substrait-features",
        help_description: "cargo check datafusion-substrait features",
        steps: &[StepCall {
            command: "rust-check",
            args: &["datafusion-substrait"],
        }],
    },
    JobInfo {
        name: "linux-datafusion-proto-features",
        help_description: "cargo check datafusion-proto features",
        steps: &[StepCall {
            command: "rust-check",
            args: &["datafusion-proto"],
        }],
    },
    JobInfo {
        name: "linux-cargo-check-datafusion",
        help_description: "cargo check datafusion features",
        steps: &[StepCall {
            command: "rust-check",
            args: &["datafusion"],
        }],
    },
    JobInfo {
        name: "linux-cargo-check-datafusion-functions",
        help_description: "cargo check datafusion-functions features",
        steps: &[StepCall {
            command: "rust-check",
            args: &["datafusion-functions"],
        }],
    },
    JobInfo {
        name: "linux-test",
        help_description: "cargo test (amd64)",
        steps: &[
            StepCall {
                command: "rust-test",
                args: &[],
            },
            StepCall {
                command: "verify-clean",
                args: &[],
            },
            StepCall {
                command: "verify-clean",
                args: &["--untracked"],
            },
        ],
    },
    JobInfo {
        name: "linux-test-datafusion-cli",
        help_description: "cargo test datafusion-cli (amd64)",
        steps: &[
            StepCall {
                command: "rust-test",
                args: &["datafusion-cli"],
            },
            StepCall {
                command: "verify-clean",
                args: &[],
            },
        ],
    },
    JobInfo {
        name: "linux-test-example",
        help_description: "cargo examples (amd64)",
        steps: &[
            StepCall {
                command: "rust-examples",
                args: &[],
            },
            StepCall {
                command: "verify-clean",
                args: &[],
            },
        ],
    },
    JobInfo {
        name: "linux-test-doc",
        help_description: "cargo test doc (amd64)",
        steps: &[
            StepCall {
                command: "rust-doctest",
                args: &[],
            },
            StepCall {
                command: "verify-clean",
                args: &[],
            },
        ],
    },
    JobInfo {
        name: "linux-rustdoc",
        help_description: "cargo doc",
        steps: &[StepCall {
            command: "rust-doc",
            args: &[],
        }],
    },
    JobInfo {
        name: "linux-wasm-pack",
        help_description: "build and run with wasm-pack",
        steps: &[StepCall {
            command: "rust-wasm",
            args: &[],
        }],
    },
    JobInfo {
        name: "verify-benchmark-results",
        help_description: "verify benchmark results (amd64)",
        steps: &[
            StepCall {
                command: "rust-benchmark-results",
                args: &[],
            },
            StepCall {
                command: "verify-clean",
                args: &[],
            },
        ],
    },
    JobInfo {
        name: "sqllogictest-postgres",
        help_description: "Run sqllogictest with Postgres runner",
        steps: &[StepCall {
            command: "rust-sqllogictest",
            args: &["postgres"],
        }],
    },
    JobInfo {
        name: "sqllogictest-substrait",
        help_description: "Run sqllogictest in Substrait round-trip mode",
        steps: &[StepCall {
            command: "rust-sqllogictest",
            args: &["substrait"],
        }],
    },
    JobInfo {
        name: "macos-aarch64",
        help_description: "cargo test (macos-aarch64)",
        steps: &[StepCall {
            command: "rust-test",
            args: &["datafusion-ffi"],
        }],
    },
    JobInfo {
        name: "vendor",
        help_description: "Verify Vendored Code",
        steps: &[StepCall {
            command: "rust-vendor",
            args: &[],
        }],
    },
    JobInfo {
        name: "check-fmt",
        help_description: "Check cargo fmt",
        steps: &[StepCall {
            command: "fmt",
            args: &[],
        }],
    },
    JobInfo {
        name: "clippy",
        help_description: "clippy",
        steps: &[StepCall {
            command: "clippy",
            args: &[],
        }],
    },
    JobInfo {
        name: "cargo-toml-formatting-checks",
        help_description: "check Cargo.toml formatting",
        steps: &[StepCall {
            command: "rust-toml-format",
            args: &[],
        }],
    },
    JobInfo {
        name: "config-docs-check",
        help_description: "check configs.md and ***_functions.md is up-to-date",
        steps: &[StepCall {
            command: "rust-config-docs",
            args: &[],
        }],
    },
    JobInfo {
        name: "examples-docs-check",
        help_description: "check example README is up-to-date",
        steps: &[StepCall {
            command: "rust-examples-docs",
            args: &[],
        }],
    },
    JobInfo {
        name: "msrv",
        help_description: "Verify MSRV (Min Supported Rust Version)",
        steps: &[
            StepCall {
                command: "rust-msrv",
                args: &["datafusion"],
            },
            StepCall {
                command: "rust-msrv",
                args: &["datafusion-substrait"],
            },
            StepCall {
                command: "rust-msrv",
                args: &["datafusion-proto"],
            },
        ],
    },
];
