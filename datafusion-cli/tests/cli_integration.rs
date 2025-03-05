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

use insta::{glob, Settings};
use insta_cmd::{assert_cmd_snapshot, get_cargo_bin};
use std::{env, fs};

fn cli() -> Command {
    Command::new(get_cargo_bin("datafusion-cli"))
}

fn make_settings() -> Settings {
    let mut settings = Settings::clone_current();
    settings.set_prepend_module_to_snapshot(false);
    settings.add_filter(r"Elapsed .* seconds\.", "[ELAPSED]");
    settings.add_filter(r"DataFusion CLI v.*", "[CLI_VERSION]");
    settings
}

#[cfg(test)]
#[ctor::ctor]
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

#[tokio::test]
async fn test_cli() {
    if env::var("TEST_STORAGE_INTEGRATION").is_err() {
        eprintln!("Skipping external storages integration tests");
        return;
    }

    let settings = make_settings();
    let _bound = settings.bind_to_scope();

    glob!("sql/integration/*.sql", |path| {
        let input = fs::read_to_string(path).unwrap();
        assert_cmd_snapshot!(cli().pass_stdin(input))
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

    let access_key_id =
        env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID is not set");
    let secret_access_key =
        env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_SECRET_ACCESS_KEY is not set");
    let endpoint_url = env::var("AWS_ENDPOINT").expect("AWS_ENDPOINT is not set");

    let input = format!(
        r#"CREATE EXTERNAL TABLE CARS
STORED AS CSV
LOCATION 's3://data/cars.csv'
OPTIONS(
    'aws.access_key_id' '{}',
    'aws.secret_access_key' '{}',
    'aws.endpoint' '{}',
    'aws.allow_http' 'true'
);

SELECT * FROM CARS limit 1;
"#,
        access_key_id, secret_access_key, endpoint_url
    );

    assert_cmd_snapshot!(cli().env_clear().pass_stdin(input));
}
