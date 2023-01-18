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
use log::debug;
use sqllogictest::{ColumnType, DBOutput};
use tokio::task::JoinHandle;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use postgres_types::Type;
use tokio_postgres::{Column, Row};
use rust_decimal::Decimal;
use super::conversion::*;

pub mod image;

pub struct Postgres {
    client: Arc<tokio_postgres::Client>,
    join_handle: JoinHandle<()>,
    file_name: String,
}

impl Postgres {
    pub async fn connect_with_retry(
        file_name: String,
        host: &str,
        port: u16,
        db: &str,
        user: &str,
        pass: &str,
    ) -> Result<Self, tokio_postgres::error::Error> {
        let mut retry = 0;
        loop {
            let connection_result = Postgres::connect(file_name.clone(), host, port, db, user, pass).await;
            match connection_result {
                Err(e) if retry <= 3 => {
                    debug!("Retrying connection error '{:?}'", e);
                    retry += 1;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                result => break result,
            }
        }
    }

    async fn connect(file_name: String,
                     host: &str,
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
            file_name,
        })
    }
}

impl Drop for Postgres {
    fn drop(&mut self) {
        self.join_handle.abort()
    }
}

macro_rules! make_string {
    ($row:ident, $idx:ident, $t:ty) => {{
        let value:Option<$t> = $row.get($idx);
         match value {
            Some(value) => value.to_string(),
            None => NULL_STR.to_string()
         }
    }};
    ($row:ident, $idx:ident, $t:ty, $convert:ident) => {{
        let value: Option<$t> = $row.get($idx);
        match value {
            Some(value) => $convert(value).to_string(),
            None => NULL_STR.to_string()
         }
    }};
}

fn cell_to_string(row: &Row, column: &Column, idx: usize) -> String {
    match column.type_().clone() {
        Type::CHAR => make_string!(row, idx, i8),
        Type::INT2 => make_string!(row, idx, i16),
        Type::INT4 => make_string!(row, idx, i32),
        Type::INT8 => make_string!(row, idx, i64),
        Type::NUMERIC => make_string!(row, idx, Decimal, decimal_to_str),
        Type::DATE => make_string!(row, idx, NaiveDate),
        Type::TIME => make_string!(row, idx, NaiveTime),
        Type::TIMESTAMP => {
            let value: Option<NaiveDateTime> = row.get(idx);
            value
                .map(|d| format!("{:?}", d))
                .unwrap_or_else(|| "NULL".to_string())
        }
        Type::BOOL => make_string!(row, idx, bool, bool_to_str),
        Type::BPCHAR | Type::VARCHAR | Type::TEXT => make_string!(row, idx, &str, varchar_to_str),
        Type::FLOAT4 => make_string!(row, idx, f32, f32_to_str),
        Type::FLOAT8 => make_string!(row, idx, f64, f64_to_str),
        _ => todo!("Unsupported type: {}", column.type_().name())
    }
}

#[async_trait]
impl sqllogictest::AsyncDB for Postgres {
    type Error = tokio_postgres::error::Error;

    async fn run(&mut self, sql: &str) -> Result<DBOutput, Self::Error> {
        println!("[{}] Running query: \"{}\"", self.file_name, sql);

        let is_query_sql = {
            let lower_sql = sql.trim_start().to_ascii_lowercase();
            lower_sql.starts_with("select")
                || lower_sql.starts_with("values")
                || lower_sql.starts_with("show")
                || lower_sql.starts_with("with")
                || lower_sql.starts_with("describe")
        };
        if !is_query_sql {
            self.client.execute(sql, &[]).await?;
            return Ok(DBOutput::StatementComplete(0));
        }
        let rows = self.client.query(sql, &[]).await?;
        let output = rows.iter().map(|row| {
            row.columns().iter().enumerate()
                .map(|(idx, column)| {
                    cell_to_string(&row, &column, idx)
                })
                .collect::<Vec<String>>()
        }).collect::<Vec<_>>();

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
