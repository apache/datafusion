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

// Disabled due to https://github.com/apache/datafusion/issues/10793
#[cfg(not(target_family = "windows"))]
#[rstest]
#[case::exec_from_commands(
    ["--command", "select 1", "--format", "json", "-q"],
    "[{\"Int64(1)\":1}]\n"
)]
#[case::exec_multiple_statements(
    ["--command", "select 1; select 2;", "--format", "json", "-q"],
    "[{\"Int64(1)\":1}]\n[{\"Int64(2)\":2}]\n"
)]
#[case::exec_backslash(
    ["--file", "tests/data/backslash.txt", "--format", "json", "-q"],
    "[{\"Utf8(\\\"\\\\\\\")\":\"\\\\\",\"Utf8(\\\"\\\\\\\\\\\")\":\"\\\\\\\\\",\"Utf8(\\\"\\\\\\\\\\\\\\\\\\\\\\\")\":\"\\\\\\\\\\\\\\\\\\\\\",\"Utf8(\\\"dsdsds\\\\\\\\\\\\\\\\\\\")\":\"dsdsds\\\\\\\\\\\\\\\\\",\"Utf8(\\\"\\\\t\\\")\":\"\\\\t\",\"Utf8(\\\"\\\\0\\\")\":\"\\\\0\",\"Utf8(\\\"\\\\n\\\")\":\"\\\\n\"}]\n"
)]
#[case::exec_from_files(
    ["--file", "tests/data/sql.txt", "--format", "json", "-q"],
    "[{\"Int64(1)\":1}]\n"
)]
#[case::set_batch_size(
    ["--command", "show datafusion.execution.batch_size", "--format", "json", "-q", "-b", "1"],
    "[{\"name\":\"datafusion.execution.batch_size\",\"value\":\"1\"}]\n"
)]

/// Add case fixed issue: https://github.com/apache/datafusion/issues/14920
#[case::exec_from_commands(
    [
        "--command", "SELECT * FROM generate_series(1, 5) t1(v1) ORDER BY v1 DESC;",
        "--format", "table",
        "-q"
    ],
    "+----+\n\
     | v1 |\n\
     +----+\n\
     | 5  |\n\
     | 4  |\n\
     | 3  |\n\
     | 2  |\n\
     | 1  |\n\
     +----+\n"
)]

/// Add case for unlimited the number of rows to be printed for table format
#[case::exec_from_commands(
    [
        "--command", "SELECT * FROM generate_series(1, 5) t1(v1) ORDER BY v1 DESC;",
        "--format", "table",
        "--maxrows", "inf",
        "-q"
    ],
    "+----+\n\
     | v1 |\n\
     +----+\n\
     | 5  |\n\
     | 4  |\n\
     | 3  |\n\
     | 2  |\n\
     | 1  |\n\
     +----+\n"
)]

/// Add case for limiting the number of rows to be printed for table format
#[case::exec_from_commands(
    [
        "--command", "SELECT * FROM generate_series(1, 5) t1(v1) ORDER BY v1 DESC;",
        "--format", "table",
        "--maxrows", "3",
        "-q"
    ],
    "+----+\n\
     | v1 |\n\
     +----+\n\
     | 5  |\n\
     | 4  |\n\
     | 3  |\n\
     | .  |\n\
     | .  |\n\
     | .  |\n\
     +----+\n"
)]

/// Add case for limiting the number to 0 of rows to be printed for table format
#[case::exec_from_commands(
    [
        "--command", "SELECT * FROM generate_series(1, 5) t1(v1) ORDER BY v1 DESC;",
        "--format", "table",
        "--maxrows", "0",
        "-q"
    ],
    "+----+\n\
     | v1 |\n\
     +----+\n\
     +----+\n"
)]

/// Add case for limiting the number of rows to be printed for csv format
#[case::exec_from_commands(
    [
        "--command", "SELECT * FROM generate_series(1, 5) t1(v1) ORDER BY v1 DESC;",
        "--format", "csv",
        "--maxrows", "3",
        "-q"
    ],
    "v1\n5\n4\n3\n"
)]

/// Add case for explain table format printing
#[case::exec_explain_simple(
    ["--command", "explain select 1;", "--format", "table", "-q"],
    "+---------------+--------------------------------------+\n\
     | plan_type     | plan                                 |\n\
     +---------------+--------------------------------------+\n\
     | logical_plan  | Projection: Int64(1)                 |\n\
     |               |   EmptyRelation                      |\n\
     | physical_plan | ProjectionExec: expr=[1 as Int64(1)] |\n\
     |               |   PlaceholderRowExec                 |\n\
     +---------------+--------------------------------------+\n"
)]

/// Add case for printing empty result set for table format
#[case::exec_select_empty(
    [
        "--command",
        "select * from (values (1)) as t(col) where false;",
        "--format",
        "table",
        "-q"
    ],
    "+-----+\n\
     | col |\n\
     +-----+\n\
     +-----+\n"
)]

/// Add case for printing empty result set for json format
#[case::exec_select_empty_json(
    [
        "--command",
        "select * from (values (1)) as t(col) where false;",
        "--format",
        "json",
        "-q"
    ],
    ""
)]

/// Add case for printing empty result set for csv format
#[case::exec_select_empty_csv(
    [
        "--command",
        "select * from (values (1)) as t(col) where false;",
        "--format",
        "csv",
        "-q"
    ],
    ""
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
