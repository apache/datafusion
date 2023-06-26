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

use assert_cmd::prelude::{CommandCargoExt, OutputAssertExt};
use predicates::prelude::predicate;
use rstest::rstest;

#[cfg(test)]
#[ctor::ctor]
fn init() {
    // Enable RUST_LOG logging configuration for tests
    let _ = env_logger::try_init();
}

#[rstest]
#[case::exec_from_commands(
    ["--command", "select 1", "--format", "json", "-q"],
    "[{\"Int64(1)\":1}]\n"
)]
#[case::exec_from_files(
    ["--file", "tests/data/sql.txt", "--format", "json", "-q"],
    "[{\"Int64(1)\":1}]\n"
)]
#[case::set_batch_size(
    ["--command", "show datafusion.execution.batch_size", "--format", "json", "-q", "-b", "1"],
    "[{\"name\":\"datafusion.execution.batch_size\",\"setting\":\"1\"}]\n"
)]
#[test]
fn cli_quick_test<'a>(
    #[case] args: impl IntoIterator<Item = &'a str>,
    #[case] expected: &str,
) {
    let mut cmd = Command::cargo_bin("datafusion-cli").unwrap();
    cmd.args(args);
    cmd.assert().stdout(predicate::eq(expected));
}
