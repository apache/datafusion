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

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use log::info;
use sqllogictest::{ColumnType, DBOutput};
use testcontainers::core::WaitFor;
use testcontainers::images::generic::GenericImage;
use tokio::task::JoinHandle;

pub struct Postgres {
    client: Arc<tokio_postgres::Client>,
    join_handle: JoinHandle<()>,
}

pub const PG_USER: &str = "postgres";
pub const PG_PASSWORD: &str = "postgres";
pub const PG_DB: &str = "test";
pub const PG_PORT: u16 = 5432;

impl Postgres {
    pub fn postgres_docker_image() -> GenericImage {
        let postgres_test_data = match datafusion::test_util::get_data_dir("POSTGRES_TEST_DATA", "tests/sqllogictests/postgres") {
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
            .with_env_var("POSTGRES_INITDB_ARGS", "--encoding=UTF-8 --lc-collate=C --lc-ctype=C")
            .with_exposed_port(PG_PORT)
            .with_volume(format!("{0}/csv/aggregate_test_100.csv", datafusion::test_util::arrow_test_data()), "/opt/data/csv/aggregate_test_100.csv")
            .with_volume(format!("{0}/postgres_create_table.sql", postgres_test_data), "/docker-entrypoint-initdb.d/0_create_table.sql")
    }

    pub async fn connect_with_retry(host: &str,
                                    port: u16,
                                    db: &str,
                                    user: &str,
                                    pass: &str) -> Result<Self, tokio_postgres::error::Error> {
        let mut retry = 0;
        loop {
            let connection_result = Postgres::connect(host, port, db, user, pass).await;
            match connection_result {
                Err(e) if retry <= 3 => {
                    info!("Retrying connection error '{:?}'", e);
                    retry += 1;
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                result => break result
            }
        }
    }

    async fn connect(host: &str,
                     port: u16,
                     db: &str,
                     user: &str,
                     pass: &str) -> Result<Self, tokio_postgres::error::Error> {
        let (client, connection) = tokio_postgres::Config::new()
            .host(host)
            .port(port)
            .dbname(db)
            .user(user)
            .password(pass)
            .connect(tokio_postgres::NoTls)
            .await?;

        let join_handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                log::error!("Postgres connection error: {:?}", e);
            }
        });

        Ok(Self {
            client: Arc::new(client),
            join_handle,
        })
    }
}


impl Drop for Postgres {
    fn drop(&mut self) {
        self.join_handle.abort()
    }
}

#[async_trait]
impl sqllogictest::AsyncDB for Postgres {
    type Error = tokio_postgres::error::Error;

    async fn run(&mut self, sql: &str) -> Result<DBOutput, Self::Error> {
        let mut output = vec![];

        let is_query_sql = {
            let lower_sql = sql.trim_start().to_ascii_lowercase();
            lower_sql.starts_with("select")
                || lower_sql.starts_with("values")
                || lower_sql.starts_with("show")
                || lower_sql.starts_with("with")
                || lower_sql.starts_with("describe")
        };

        // NOTE:
        // We use `simple_query` API which returns the query results as strings.
        // This means that we can not reformat values based on their type,
        // and we have to follow the format given by the specific database (pg).
        // For example, postgres will output `t` as true and `f` as false,
        // thus we have to write `t`/`f` in the expected results.
        let rows = self.client.simple_query(sql).await?;
        for row in rows {
            let mut row_vec = vec![];
            match row {
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    for i in 0..row.len() {
                        match row.get(i) {
                            Some(v) => {
                                if v.is_empty() {
                                    row_vec.push("(empty)".to_string());
                                } else {
                                    row_vec.push(v.to_string());
                                }
                            }
                            None => row_vec.push("NULL".to_string()),
                        }
                    }
                }
                tokio_postgres::SimpleQueryMessage::CommandComplete(cnt) => {
                    if is_query_sql {
                        break;
                    } else {
                        return Ok(DBOutput::StatementComplete(cnt));
                    }
                }
                _ => unreachable!(),
            }
            output.push(row_vec);
        }

        if output.is_empty() {
            let stmt = self.client.prepare(sql).await?;
            Ok(DBOutput::Rows {
                types: vec![ColumnType::Any; stmt.columns().len()],
                rows: vec![],
            })
        } else {
            Ok(DBOutput::Rows {
                types: vec![ColumnType::Any; output[0].len()],
                rows: output,
            })
        }
    }

    fn engine_name(&self) -> &str {
        "postgres"
    }
}
