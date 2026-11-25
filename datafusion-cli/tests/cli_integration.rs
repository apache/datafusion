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

use std::process::Command;

use rstest::rstest;

use async_trait::async_trait;
use futures::TryStreamExt;
use insta::internals::SettingsBindDropGuard;
use insta::{Settings, glob};
use insta_cmd::{assert_cmd_snapshot, get_cargo_bin};
use object_store::{
    ObjectStore, ObjectStoreExt, aws::AmazonS3Builder, local::LocalFileSystem,
};
use std::path::PathBuf;
use std::{env, fs};
use testcontainers_modules::testcontainers::core::{
    CmdWaitFor, ExecCommand, IntoContainerPort,
};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{
    ContainerAsync, GenericImage, ImageExt, TestcontainersError,
};

fn cli() -> Command {
    Command::new(get_cargo_bin("datafusion-cli"))
}

fn make_settings() -> Settings {
    let mut settings = Settings::clone_current();
    settings.set_prepend_module_to_snapshot(false);
    settings.add_filter(r"Elapsed .* seconds\.", "[ELAPSED]");
    settings.add_filter(r"DataFusion CLI v.*", "[CLI_VERSION]");
    settings.add_filter(r"(?s)backtrace:.*?\n\n\n", "");
    settings
}

const RUSTFS_ACCESS_KEY: &str = "TEST-DataFusionLogin";
const RUSTFS_SECRET_KEY: &str = "TEST-DataFusionPassword";

const RUSTFS_IMAGE_NAME: &str = "docker.io/rustfs/rustfs";
const RUSTFS_IMAGE_TAG: &str = "1.0.0";

/// Starts a RustFS container preloaded with the test data.
async fn setup_rustfs_container() -> Result<ContainerAsync<GenericImage>, String> {
    let container = start_rustfs_container().await?;
    provision_rustfs_container(&container).await?;
    Ok(container)
}

async fn start_rustfs_container() -> Result<ContainerAsync<GenericImage>, String> {
    GenericImage::new(RUSTFS_IMAGE_NAME, RUSTFS_IMAGE_TAG)
        .with_exposed_port(9000.tcp())
        .with_env_var("RUSTFS_ACCESS_KEY", RUSTFS_ACCESS_KEY)
        .with_env_var("RUSTFS_SECRET_KEY", RUSTFS_SECRET_KEY)
        .with_env_var("RUSTFS_CONSOLE_ENABLE", "false")
        .start()
        .await
        .map_err(|e| match e {
            TestcontainersError::Client(e) => format!(
                "Failed to start RustFS container. Ensure Docker is running and accessible: {e}"
            ),
            e => format!("Failed to start RustFS container: {e}"),
        })
}

/// Waits for RustFS to be healthy and uploads the test files.
///
/// Use the image's `curl` to check readiness and create the bucket, then upload
/// the fixtures through `object_store`.
async fn provision_rustfs_container(
    container: &ContainerAsync<GenericImage>,
) -> Result<(), String> {
    let credentials = format!("{RUSTFS_ACCESS_KEY}:{RUSTFS_SECRET_KEY}");
    let commands = [
        ExecCommand::new([
            "curl",
            "--fail",
            "--silent",
            "--show-error",
            "--retry",
            "60",
            "--retry-delay",
            "1",
            "--retry-all-errors",
            "--max-time",
            "5",
            "http://localhost:9000/health/ready",
        ]),
        ExecCommand::new([
            "curl",
            "--fail",
            "--silent",
            "--show-error",
            "--aws-sigv4",
            "aws:amz:us-east-1:s3",
            "--user",
            &credentials,
            "--request",
            "PUT",
            "http://localhost:9000/data",
        ]),
    ];

    for command in commands {
        let command =
            command.with_cmd_ready_condition(CmdWaitFor::Exit { code: Some(0) });

        let cmd_ref = format!("{command:?}");

        if let Err(e) = container.exec(command).await {
            let stdout = container.stdout_to_vec().await.unwrap_or_default();
            let stderr = container.stderr_to_vec().await.unwrap_or_default();

            return Err(format!(
                "Failed to execute command: {}\nError: {}\nStdout: {:?}\nStderr: {:?}",
                cmd_ref,
                e,
                String::from_utf8_lossy(&stdout),
                String::from_utf8_lossy(&stderr)
            ));
        }
    }

    let port = container
        .get_host_port_ipv4(9000)
        .await
        .map_err(|e| e.to_string())?;
    let store = AmazonS3Builder::new()
        .with_bucket_name("data")
        .with_region("us-east-1")
        .with_access_key_id(RUSTFS_ACCESS_KEY)
        .with_secret_access_key(RUSTFS_SECRET_KEY)
        .with_endpoint(format!("http://localhost:{port}"))
        .with_allow_http(true)
        .build()
        .map_err(|e| e.to_string())?;
    let data_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../datafusion/core/tests/data");
    let source =
        LocalFileSystem::new_with_prefix(data_path).map_err(|e| e.to_string())?;
    let mut files = source.list(None);
    while let Some(file) = files.try_next().await.map_err(|e| e.to_string())? {
        let data = source
            .get(&file.location)
            .await
            .map_err(|e| e.to_string())?
            .bytes()
            .await
            .map_err(|e| e.to_string())?;
        store
            .put(&file.location, data.into())
            .await
            .map_err(|e| e.to_string())?;
    }

    Ok(())
}

#[cfg(test)]
#[ctor::ctor(unsafe)]
fn init() {
    // Enable RUST_LOG logging configuration for tests
    let _ = env_logger::try_init();
}

#[rstest]
#[case::exec_multiple_statements(
    "statements",
    ["--command", "select 1; select 2;", "-q"],
)]
#[case::exec_backslash(
    "backslash",
    ["--file", "tests/sql/backslash.sql", "--format", "json", "-q"],
)]
#[case::exec_from_files(
    "files",
    ["--file", "tests/sql/select.sql", "-q"],
)]
#[case::set_batch_size(
    "batch_size",
    ["--command", "show datafusion.execution.batch_size", "-q", "-b", "1"],
)]
#[case::default_explain_plan(
    "default_explain_plan",
    // default explain format should be tree
    ["--command", "EXPLAIN SELECT 123"],
)]
#[case::can_see_indent_format(
    "can_see_indent_format",
    // can choose the old explain format too
    ["--command", "EXPLAIN FORMAT indent SELECT 123"],
)]
#[case::change_format_version(
    "change_format_version",
    ["--file", "tests/sql/types_format.sql", "-q"],
)]
#[test]
fn cli_quick_test<'a>(
    #[case] snapshot_name: &'a str,
    #[case] args: impl IntoIterator<Item = &'a str>,
) {
    let mut settings = make_settings();
    settings.set_snapshot_suffix(snapshot_name);
    let _bound = settings.bind_to_scope();

    let mut cmd = cli();
    cmd.args(args);

    assert_cmd_snapshot!(cmd);
}

#[test]
fn cli_explain_environment_overrides() {
    let mut settings = make_settings();
    settings.set_snapshot_suffix("explain_plan_environment_overrides");
    let _bound = settings.bind_to_scope();

    let mut cmd = cli();

    // should use the environment variable to override the default explain plan
    cmd.env("DATAFUSION_EXPLAIN_FORMAT", "pgjson")
        .args(["--command", "EXPLAIN SELECT 123"]);

    assert_cmd_snapshot!(cmd);
}

#[rstest]
#[case("csv")]
#[case("tsv")]
#[case("table")]
#[case("json")]
#[case("nd-json")]
#[case("automatic")]
#[test]
fn test_cli_format<'a>(#[case] format: &'a str) {
    let mut settings = make_settings();
    settings.set_snapshot_suffix(format);
    let _bound = settings.bind_to_scope();

    let mut cmd = cli();
    cmd.args(["--command", "select 1", "-q", "--format", format]);

    assert_cmd_snapshot!(cmd);
}

#[rstest]
#[case("no_track", ["--top-memory-consumers", "0"])]
#[case("top2", ["--top-memory-consumers", "2"])]
#[case("top3_default", [])]
#[test]
fn test_cli_top_memory_consumers<'a>(
    #[case] snapshot_name: &str,
    #[case] top_memory_consumers: impl IntoIterator<Item = &'a str>,
) {
    let _bound = bind_to_settings(snapshot_name);

    let mut cmd = cli();
    let sql = "select * from generate_series(1,500000) as t1(v1) order by v1 desc;";
    cmd.args(["--memory-limit", "10M", "--command", sql]);
    cmd.args(top_memory_consumers);

    assert_cmd_snapshot!(cmd);
}

#[rstest]
#[case("no_track", ["--top-memory-consumers", "0"])]
#[case("top2", ["--top-memory-consumers", "2"])]
#[test]
fn test_cli_top_memory_consumers_with_mem_pool_type<'a>(
    #[case] snapshot_name: &str,
    #[case] top_memory_consumers: impl IntoIterator<Item = &'a str>,
) {
    let _bound = bind_to_settings(snapshot_name);

    let mut cmd = cli();
    let sql = "select * from generate_series(1,500000) as t1(v1) order by v1 desc;";
    cmd.args([
        "--memory-limit",
        "10M",
        "--mem-pool-type",
        "fair",
        "--command",
        sql,
    ]);
    cmd.args(top_memory_consumers);

    assert_cmd_snapshot!(cmd);
}

fn bind_to_settings(snapshot_name: &str) -> SettingsBindDropGuard {
    let mut settings = make_settings();

    settings.set_snapshot_suffix(snapshot_name);

    settings.add_filter(
        r"[^\s]+\#\d+\(can spill: (true|false)\) consumed .*?B, peak .*?B",
        "Consumer(can spill: bool) consumed XB, peak XB",
    );
    settings.add_filter(
        r"Error: Failed to allocate additional .*? for .*? with .*? already allocated for this reservation - .*? remain available for the total memory pool: '.*?'",
        "Error: Failed to allocate ",
    );
    settings.add_filter(
        r"Resources exhausted: Failed to allocate additional .*? for .*? with .*? already allocated for this reservation - .*? remain available for the total memory pool: '.*?'",
        "Resources exhausted: Failed to allocate",
    );

    settings.bind_to_scope()
}

#[test]
fn test_cli_with_unbounded_memory_pool() {
    let mut settings = make_settings();

    settings.set_snapshot_suffix("default");

    let _bound = settings.bind_to_scope();

    let mut cmd = cli();
    let sql = "select * from generate_series(1,500000) as t1(v1) order by v1;";
    cmd.args(["--maxrows", "10", "--command", sql]);

    assert_cmd_snapshot!(cmd);
}

#[test]
fn test_cli_wide_result_set_no_crash() {
    let mut settings = make_settings();

    settings.set_snapshot_suffix("wide_result_set");

    let _bound = settings.bind_to_scope();

    let mut cmd = cli();
    let sql = "SELECT v1 as c0, v1+1 as c1, v1+2 as c2, v1+3 as c3, v1+4 as c4, \
               v1+5 as c5, v1+6 as c6, v1+7 as c7, v1+8 as c8, v1+9 as c9 \
               FROM generate_series(1, 100) as t1(v1);";
    cmd.args(["--maxrows", "5", "--command", sql]);

    assert_cmd_snapshot!(cmd);
}

#[tokio::test]
async fn test_cli() {
    if env::var("TEST_STORAGE_INTEGRATION").is_err() {
        eprintln!("Skipping external storages integration tests");
        return;
    }

    let container = match setup_rustfs_container().await {
        Ok(c) => c,
        Err(e) if e.contains("toomanyrequests") => {
            eprintln!("Skipping test: Docker pull rate limit reached: {e}");
            return;
        }
        e @ Err(_) => e.unwrap(),
    };

    let settings = make_settings();
    let _bound = settings.bind_to_scope();

    let port = container.get_host_port_ipv4(9000).await.unwrap();

    glob!("sql/integration/*.sql", |path| {
        let input = fs::read_to_string(path).unwrap();
        assert_cmd_snapshot!(
            cli()
                .env_clear()
                .env("AWS_ACCESS_KEY_ID", "TEST-DataFusionLogin")
                .env("AWS_SECRET_ACCESS_KEY", "TEST-DataFusionPassword")
                .env("AWS_ENDPOINT", format!("http://localhost:{port}"))
                .env("AWS_ALLOW_HTTP", "true")
                .pass_stdin(input)
        )
    });
}

#[tokio::test]
async fn test_aws_options() {
    // Separate test is needed to pass aws as options in sql and not via env

    if env::var("TEST_STORAGE_INTEGRATION").is_err() {
        eprintln!("Skipping external storages integration tests");
        return;
    }

    let settings = make_settings();
    let _bound = settings.bind_to_scope();

    let container = match setup_rustfs_container().await {
        Ok(c) => c,
        Err(e) if e.contains("toomanyrequests") => {
            eprintln!("Skipping test: Docker pull rate limit reached: {e}");
            return;
        }
        e @ Err(_) => e.unwrap(),
    };
    let port = container.get_host_port_ipv4(9000).await.unwrap();

    let input = format!(
        r#"CREATE EXTERNAL TABLE CARS
STORED AS CSV
LOCATION 's3://data/cars.csv'
OPTIONS(
    'aws.access_key_id' 'TEST-DataFusionLogin',
    'aws.secret_access_key' 'TEST-DataFusionPassword',
    'aws.endpoint' 'http://localhost:{port}',
    'aws.allow_http' 'true'
);

SELECT * FROM CARS limit 1;
"#
    );

    assert_cmd_snapshot!(cli().env_clear().pass_stdin(input));
}

#[tokio::test]
async fn test_aws_region_auto_resolution() {
    if env::var("TEST_STORAGE_INTEGRATION").is_err() {
        eprintln!("Skipping external storages integration tests");
        return;
    }

    let mut settings = make_settings();
    settings.add_filter(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", "[TIME]");
    let _bound = settings.bind_to_scope();

    let bucket = "s3://clickhouse-public-datasets/hits_compatible/athena_partitioned/hits_1.parquet";
    let region = "us-east-1";

    let input = format!(
        r#"CREATE EXTERNAL TABLE hits
STORED AS PARQUET
LOCATION '{bucket}'
OPTIONS(
    'aws.region' '{region}',
    'aws.skip_signature' true
);

SELECT COUNT(*) FROM hits;
"#
    );

    assert_cmd_snapshot!(
        cli()
            .env("RUST_LOG", "warn")
            .env_remove("AWS_ENDPOINT")
            .pass_stdin(input)
    );
}

/// Ensure backtrace will be printed, if executing `datafusion-cli` with a query
/// that triggers error.
/// Example:
///     RUST_BACKTRACE=1 cargo run --features backtrace -- -c 'select pow(1,'foo');'
#[rstest]
#[case("SELECT pow(1,'foo')")]
#[case("SELECT CAST('not_a_number' AS INTEGER);")]
#[cfg(feature = "backtrace")]
fn test_backtrace_output(#[case] query: &str) {
    let mut cmd = cli();
    // Use a command that will cause an error and trigger backtrace
    cmd.args(["--command", query, "-q"])
        .env("RUST_BACKTRACE", "1"); // Enable backtrace

    let output = cmd.output().expect("Failed to execute command");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined_output = format!("{stdout}{stderr}");

    // Assert that the output includes literal 'backtrace'
    assert!(
        combined_output.to_lowercase().contains("backtrace"),
        "Expected output to contain 'backtrace', but got stdout: '{stdout}' stderr: '{stderr}'"
    );
}

#[tokio::test]
async fn test_s3_url_fallback() {
    if env::var("TEST_STORAGE_INTEGRATION").is_err() {
        eprintln!("Skipping external storages integration tests");
        return;
    }

    let container = match setup_rustfs_container().await {
        Ok(c) => c,
        Err(e) if e.contains("toomanyrequests") => {
            eprintln!("Skipping test: Docker pull rate limit reached: {e}");
            return;
        }
        e @ Err(_) => e.unwrap(),
    };

    let mut settings = make_settings();
    settings.set_snapshot_suffix("s3_url_fallback");
    let _bound = settings.bind_to_scope();

    // Create a table using a prefix path (without trailing slash)
    // This should trigger the fallback logic where head() fails on the prefix
    // and list() is used to discover the actual files
    let input = r#"CREATE EXTERNAL TABLE partitioned_data
STORED AS CSV
LOCATION 's3://data/partitioned_csv'
OPTIONS (
    'format.has_header' 'false'
);

SELECT * FROM partitioned_data ORDER BY column_1, column_2 LIMIT 5;
"#;

    assert_cmd_snapshot!(cli().with_rustfs(&container).await.pass_stdin(input));
}

/// Validate object store profiling output
#[tokio::test]
async fn test_object_store_profiling() {
    if env::var("TEST_STORAGE_INTEGRATION").is_err() {
        eprintln!("Skipping external storages integration tests");
        return;
    }

    let container = match setup_rustfs_container().await {
        Ok(c) => c,
        Err(e) if e.contains("toomanyrequests") => {
            eprintln!("Skipping test: Docker pull rate limit reached: {e}");
            return;
        }
        e @ Err(_) => e.unwrap(),
    };
    let mut settings = make_settings();

    // as the object store profiling contains timestamps and durations, we must
    // filter them out to have stable snapshots
    //
    // Example line to filter:
    // 2025-10-11T12:02:59.722646+00:00 operation=Get duration=0.001495s size=1006 path=cars.csv
    // Output:
    // <TIMESTAMP> operation=Get duration=[DURATION] size=1006 path=cars.csv
    settings.add_filter(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?[+-]\d{2}:\d{2} operation=(Get|Put|Delete|List|Head) duration=\d+\.\d{6}s (size=\d+\s+)?path=(.*)",
        "<TIMESTAMP> operation=$1 duration=[DURATION] ${2}path=$3",
    );

    // We also need to filter out the summary statistics (anything with an 's' at the end)
    // Example line(s) to filter:
    // | Get       | duration | 5.000000s | 5.000000s | 5.000000s |           | 1         |
    settings.add_filter(
        r"\| (Get|Put|Delete|List|Head)( +)\| duration \| .*? \| .*? \| .*? \| .*? \| (.*?) \|",
        "| $1$2 | duration | ...NORMALIZED...| $3 |",
    );

    let _bound = settings.bind_to_scope();

    let input = r#"
    CREATE EXTERNAL TABLE CARS
STORED AS CSV
LOCATION 's3://data/cars.csv';

-- Initial query should not show any profiling as the object store is not instrumented yet
SELECT * from CARS LIMIT 1;
\object_store_profiling trace
-- Query again to see the full profiling output
SELECT * from CARS LIMIT 1;
\object_store_profiling summary
-- Query again to see the summarized profiling output
SELECT * from CARS LIMIT 1;
\object_store_profiling disabled
-- Final query should not show any profiling as we disabled it again
SELECT * from CARS LIMIT 1;
"#;

    assert_cmd_snapshot!(cli().with_rustfs(&container).await.pass_stdin(input));
}

/// Add the RustFS connection information to a Command.
#[async_trait]
trait RustfsCommandExt {
    async fn with_rustfs(
        &mut self,
        container: &ContainerAsync<GenericImage>,
    ) -> &mut Self;
}

#[async_trait]
impl RustfsCommandExt for Command {
    async fn with_rustfs(
        &mut self,
        container: &ContainerAsync<GenericImage>,
    ) -> &mut Self {
        let port = container.get_host_port_ipv4(9000).await.unwrap();

        self.env_clear()
            .env("AWS_ACCESS_KEY_ID", RUSTFS_ACCESS_KEY)
            .env("AWS_SECRET_ACCESS_KEY", RUSTFS_SECRET_KEY)
            .env("AWS_ENDPOINT", format!("http://localhost:{port}"))
            .env("AWS_ALLOW_HTTP", "true")
    }
}
