<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# Non-Functional Tests

This page covers checks for CI workflows, documentation, repository maintenance, and benchmarks. For tests of DataFusion's query behavior, see [Testing](testing.md).

## CI Workflow Tests

The Rust workflow validator and fork status bridge have regression tests.
These tests cover skip conditions, fork run recovery, polling frequency, and
concurrency groups for fork pushes and merge queue runs.
They run in the `asf-yaml-check` CI job and in `./dev/rust_lint.sh`.
Run them locally with Python, PyYAML, and Node.js 24:

```shell
python3 ci/scripts/test_rust_workflow_skips.py
node --test ci/scripts/test_fork_ci_status.cjs
```

The validator tests remove guards and add an invalid job dependency in memory.
The bridge tests use stubbed GitHub API responses and make no network requests.

## Documentation Examples

We use Rust [doctest] to verify examples from the documentation are correct and
up-to-date. These tests are run as part of our CI and you can run them
locally with the following command:

```shell
cargo test --doc
```

### API Documentation Examples

As with other Rust projects, examples in doc comments in `.rs` files are
automatically checked to ensure they work and evolve along with the code.

Prefix a line with `# ` (a `#` followed by a space) to keep it compiled and
run, but hidden from the rendered documentation. This is most useful for
`use` statements, since readers care about the API being demonstrated, not
its import boilerplate:

````rust
/// ```
/// # use std::sync::Arc;
/// let value = Arc::new(42);
/// ```
````

Only the `let value = ...` line renders.

### User Guide Documentation

Rust example code from the user guide (anything marked with \`\`\`rust) is also
tested in the same way using the [doc_comment] crate. See the end of
[core/src/lib.rs] for more details.

[doctest]: https://doc.rust-lang.org/rust-by-example/testing/doc_testing.html
[doc_comment]: https://docs.rs/doc-comment/latest/doc_comment
[core/src/lib.rs]: https://github.com/apache/datafusion/blob/main/datafusion/core/src/lib.rs#L583

## Documentation Link Checks

`./dev/rust_lint.sh` runs the internal markdown link check. If `lychee` is
missing, the script installs the version pinned in
`ci/scripts/utils/tool_versions.sh`. It uses an existing installation as is,
even if the version differs from the pin.

To run the check on its own:

```shell
source ci/scripts/utils/tool_versions.sh
cargo install lychee --locked --version "${LYCHEE_VERSION}"
bash ci/scripts/markdown_link_check.sh
```

Notes:

- The script is run with `bash` and is compatible with the default Bash on macOS (no `mapfile` dependency).
- The CI configuration currently checks internal markdown links only. External `http(s)` and `mailto` links are excluded to avoid flaky failures.
- The check only reports broken links. `./dev/rust_lint.sh --write` does not change them.

When a link is broken, lychee prints the file and URL/path that failed. For example:

```text
[docs/source/user-guide/cli/overview.md]:
  [ERROR] file:///.../docs/source/user-guide/cli/missing-page.md | Cannot find file: File not found. Check if file exists and path is correct
```

Rust doc comments are validated by rustdoc in CI and can be checked locally with:

```shell
bash ci/scripts/rust_docs.sh
```

## ASF Status Check Validation

`ci/scripts/check_asf_yaml_status_checks.py` checks that every required status
check in `.asf.yaml` matches a job in `.github/workflows`.
`ci/scripts/check_rust_workflow_skips.py` checks that `rust.yml` skips only its
listed jobs on pushes to `main` and on pull requests from forks.
`./dev/rust_lint.sh` runs both and needs `python3` with [PyYAML]. The [uv]
workspace provides both:

```shell
uv run ./dev/rust_lint.sh
```

To run the checks on their own:

```shell
uv run python3 ci/scripts/check_asf_yaml_status_checks.py
uv run python3 ci/scripts/check_rust_workflow_skips.py
```

[pyyaml]: https://pypi.org/project/PyYAML/
[uv]: https://docs.astral.sh/uv/

## Security Audit

`ci/scripts/security_audit.sh` runs `cargo audit` on the root `Cargo.lock` with
the advisory exceptions that CI uses. `./dev/rust_lint.sh` runs it and installs
[cargo-audit] if it is missing. To run the audit on its own:

```shell
./ci/scripts/security_audit.sh
```

The audit fetches the RustSec advisory database. A new advisory or a different
`cargo-audit` version can change the result without any change to the repository.

[cargo-audit]: https://github.com/rustsec/rustsec/blob/main/cargo-audit/README.md

## Large File Check

`ci/scripts/check_large_files.sh` fails if any file committed between a base
ref and a head ref is larger than 1.5 MB, the same check the "Large files PR
check" workflow runs on pull requests. `./dev/rust_lint.sh` runs it against
the merge base of `HEAD` and `main` on the remote that points at
`apache/datafusion`, or on `origin` when there is no such remote. To run the
check on its own, or against a different range:

```shell
./ci/scripts/check_large_files.sh
./ci/scripts/check_large_files.sh --base upstream/main --head my-branch
```

Only committed files are checked. Commit a change before running it.

## Dependency Checks

CI runs two dependency checks, and `./dev/rust_lint.sh` runs both:

- `ci/scripts/check_circular_dependencies.sh` fails on dependency cycles
  between DataFusion crates, including one through a dev-dependency, which
  Cargo allows when building but which prevents publishing the crates. It runs
  `cargo publish --workspace --dry-run --no-verify`, which packages every crate
  that does not set `publish = false` without building or uploading anything,
  so it also fails on any other problem that would stop the crates from being
  published to crates.io. It reads the crates.io index and accepts uncommitted
  changes.
- `ci/scripts/check_unused_dependencies.sh` runs `cargo machete --with-metadata`
  from the repository root. The lint suite installs [cargo-machete] with the
  version in `ci/scripts/utils/tool_versions.sh` if it is missing.

To run either check on its own:

```shell
./ci/scripts/check_circular_dependencies.sh
./ci/scripts/check_unused_dependencies.sh
```

[cargo-machete]: https://github.com/bnjbvr/cargo-machete

## Examples README Check

`datafusion-examples/README.md` is generated from the documentation comments in
`datafusion-examples/examples/<group>/main.rs`. `ci/scripts/check_examples_docs.sh`
regenerates it and fails if the committed file differs. `./dev/rust_lint.sh`
runs it and needs `cargo` and `npx`. To run the check on its own, or to update
the README:

```shell
./ci/scripts/check_examples_docs.sh
./ci/scripts/check_examples_docs.sh --write
```

## Config and Function Docs Check

[`configs.md`], [`aggregate_functions.md`], [`scalar_functions.md`], and
[`window_functions.md`] are generated by [`dev/update_config_docs.sh`] and
[`dev/update_function_docs.sh`]. To check that they are up to date, run the
[`ci/scripts/check_generated_docs.sh`] script, which also runs as part of
`./dev/rust_lint.sh`. Run it with `--write` to update the pages.

[`configs.md`]: https://github.com/apache/datafusion/blob/main/docs/source/user-guide/configs.md
[`aggregate_functions.md`]: https://github.com/apache/datafusion/blob/main/docs/source/user-guide/sql/aggregate_functions.md
[`scalar_functions.md`]: https://github.com/apache/datafusion/blob/main/docs/source/user-guide/sql/scalar_functions.md
[`window_functions.md`]: https://github.com/apache/datafusion/blob/main/docs/source/user-guide/sql/window_functions.md
[`dev/update_config_docs.sh`]: https://github.com/apache/datafusion/blob/main/dev/update_config_docs.sh
[`dev/update_function_docs.sh`]: https://github.com/apache/datafusion/blob/main/dev/update_function_docs.sh
[`ci/scripts/check_generated_docs.sh`]: https://github.com/apache/datafusion/blob/main/ci/scripts/check_generated_docs.sh

## Documentation HTML Build

[`ci/scripts/check_docs_html.sh`] builds the documentation website, the same
build the "Test doc build" job runs. Sphinx builds with `-W`, so one warning
fails the check. `./dev/rust_lint.sh` runs it. Run the script with `--help`
for the tools it needs and the files it writes.

```shell
./ci/scripts/check_docs_html.sh
```

[`ci/scripts/check_docs_html.sh`]: https://github.com/apache/datafusion/blob/main/ci/scripts/check_docs_html.sh

## Benchmarks

### Criterion Benchmarks

[Criterion](https://docs.rs/criterion/latest/criterion/index.html) is a statistics-driven micro-benchmarking framework used by DataFusion for evaluating the performance of specific code-paths. In particular, the criterion benchmarks help to both guide optimisation efforts, and prevent performance regressions within DataFusion.

Criterion integrates with Cargo's built-in [benchmark support](https://doc.rust-lang.org/cargo/commands/cargo-bench.html) and a given benchmark can be run with

```
cargo bench --bench BENCHMARK_NAME
```

A full list of benchmarks can be found [here](https://github.com/apache/datafusion/tree/main/datafusion/core/benches).

_[cargo-criterion](https://github.com/bheisler/cargo-criterion) may also be used for more advanced reporting._

### Microbenchmark health checks (local-only)

Runs each microbenchmark case once to catch runtime panics, without measuring performance:

```shell
cargo xtask ci step test microbenchmarks
```

The suite took about 10 minutes in CI, so execution is local-only, including on
release branches; no hosted workflow or required check is provided. Runtime failures
may go unnoticed until a local run. When changing benchmark fixtures, setup, or APIs,
run the affected benchmarks and include the commands and results in your PR.

### Parquet SQL Benchmarks

The parquet SQL benchmarks can be run with

```
 cargo bench --bench parquet_query_sql
```

These randomly generate a parquet file, and then benchmark queries sourced from [parquet_query_sql.sql](../../../datafusion/core/benches/parquet_query_sql.sql) against it. This can therefore be a quick way to add coverage of particular query and/or data paths.

If the environment variable `PARQUET_FILE` is set, the benchmark will run queries against this file instead of a randomly generated one. This can be useful for performing multiple runs, potentially with different code, against the same source data, or for testing against a custom dataset.

The benchmark will automatically remove any generated parquet file on exit, however, if interrupted (e.g. by CTRL+C) it will not. This can be useful for analysing the particular file after the fact, or preserving it to use with `PARQUET_FILE` in subsequent runs.

### Comparing Baselines

By default, Criterion.rs will compare the measurements against the previous run (if any). Sometimes it's useful to keep a set of measurements around for several runs. For example, you might want to make multiple changes to the code while comparing against the master branch. For this situation, Criterion.rs supports custom baselines.

```
 git checkout main
 cargo bench --bench sql_planner -- --save-baseline main
 git checkout YOUR_BRANCH
 cargo bench --bench sql_planner --  --baseline main
```

Note: For MacOS it may be required to run `cargo bench` with `sudo`

```
sudo cargo bench ...
```

More information on [Baselines](https://bheisler.github.io/criterion.rs/book/user_guide/command_line_options.html#baselines)

### Upstream Benchmark Suites

Instructions and tooling for running upstream benchmark suites against DataFusion can be found in [benchmarks](https://github.com/apache/datafusion/tree/main/benchmarks).

These are valuable for comparative evaluation against alternative Arrow implementations and query engines.
