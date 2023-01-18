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

use testcontainers::core::WaitFor;
use testcontainers::images::generic::GenericImage;

pub const PG_USER: &str = "postgres";
pub const PG_PASSWORD: &str = "postgres";
pub const PG_DB: &str = "test";
pub const PG_PORT: u16 = 5432;

pub fn postgres_docker_image() -> GenericImage {
    let postgres_test_data = match datafusion::test_util::get_data_dir(
        "POSTGRES_TEST_DATA",
        "tests/sqllogictests/postgres",
    ) {
        Ok(pb) => pb.display().to_string(),
        Err(err) => panic!("failed to get arrow data dir: {err}"),
    };
    GenericImage::new("postgres", "15")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_DB", PG_DB)
        .with_env_var("POSTGRES_USER", PG_USER)
        .with_env_var("POSTGRES_PASSWORD", PG_PASSWORD)
        .with_env_var(
            "POSTGRES_INITDB_ARGS",
            "--encoding=UTF-8 --lc-collate=C --lc-ctype=C",
        )
        .with_exposed_port(PG_PORT)
        .with_volume(
            format!(
                "{0}/csv/aggregate_test_100.csv",
                datafusion::test_util::arrow_test_data()
            ),
            "/opt/data/csv/aggregate_test_100.csv",
        )
        .with_volume(
            format!("{0}/postgres_create_table.sql", postgres_test_data),
            "/docker-entrypoint-initdb.d/0_create_table.sql",
        )
}
