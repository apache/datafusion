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
use std::fmt::Write;

use async_trait::async_trait;
use log::info;
use sqllogictest::{ColumnType, DBOutput};
use testcontainers::core::WaitFor;
use testcontainers::images::generic::GenericImage;
use tokio::task::JoinHandle;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use postgres_types::Type;
use tokio_postgres::{Column, Row};
use rust_decimal::Decimal;
use super::conversion::*;

pub mod image;

pub struct Postgres {
    client: Arc<tokio_postgres::Client>,
    join_handle: JoinHandle<()>,
}

impl Postgres {
    pub async fn connect_with_retry(
        host: &str,
        port: u16,
        db: &str,
        user: &str,
        pass: &str,
    ) -> Result<Self, tokio_postgres::error::Error> {
        let mut retry = 0;
        loop {
            let connection_result = Postgres::connect(host, port, db, user, pass).await;
            match connection_result {
                Err(e) if retry <= 3 => {
                    info!("Retrying connection error '{:?}'", e);
                    retry += 1;
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                result => break result,
            }
        }
    }

    pub async fn connect(host: &str,
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

macro_rules! array_process {
    ($row:ident, $row_vec:ident, $idx:ident, $t:ty) => {
        let value: Option<Vec<Option<$t>>> = $row.get($idx);
        match value {
            Some(value) => {
                let mut output = String::new();
                write!(output, "{{").unwrap();
                for (i, v) in value.iter().enumerate() {
                    match v {
                        Some(v) => {
                            write!(output, "{}", v).unwrap();
                        }
                        None => {
                            write!(output, "NULL").unwrap();
                        }
                    }
                    if i < value.len() - 1 {
                        write!(output, ",").unwrap();
                    }
                }
                write!(output, "}}").unwrap();
                $row_vec.push(output);
            }
            None => {
                $row_vec.push("NULL".to_string());
            }
        }
    };
    ($row:ident, $row_vec:ident, $idx:ident, $t:ty, $convert:ident) => {
        let value: Option<Vec<Option<$t>>> = $row.get($idx);
        match value {
            Some(value) => {
                let mut output = String::new();
                write!(output, "{{").unwrap();
                for (i, v) in value.iter().enumerate() {
                    match v {
                        Some(v) => {
                            write!(output, "{}", $convert(v)).unwrap();
                        }
                        None => {
                            write!(output, "NULL").unwrap();
                        }
                    }
                    if i < value.len() - 1 {
                        write!(output, ",").unwrap();
                    }
                }
                write!(output, "}}").unwrap();
                $row_vec.push(output);
            }
            None => {
                $row_vec.push("NULL".to_string());
            }
        }
    };
    // ($self:ident, $row:ident, $row_vec:ident, $idx:ident, $t:ty, $ty_name:expr) => {
    //     let value: Option<Vec<Option<$t>>> = $row.get($idx);
    //     match value {
    //         Some(value) => {
    //             let mut output = String::new();
    //             write!(output, "{{").unwrap();
    //             for (i, v) in value.iter().enumerate() {
    //                 match v {
    //                     Some(v) => {
    //                         let sql = format!("select ($1::{})::varchar", stringify!($ty_name));
    //                         let tmp_rows = $self.client.query(&sql, &[&v]).await.unwrap();
    //                         let value: &str = tmp_rows.get(0).unwrap().get(0);
    //                         assert!(value.len() > 0);
    //                         write!(output, "{}", value).unwrap();
    //                     }
    //                     None => {
    //                         write!(output, "NULL").unwrap();
    //                     }
    //                 }
    //                 if i < value.len() - 1 {
    //                     write!(output, ",").unwrap();
    //                 }
    //             }
    //             write!(output, "}}").unwrap();
    //             $row_vec.push(output);
    //         }
    //         None => {
    //             $row_vec.push("NULL".to_string());
    //         }
    //     }
    // };
}

macro_rules! make_string {
    ($row:ident, $idx:ident, $t:ty) => {{
        let value:Option<$t> = $row.get($idx);
        value.unwrap().to_string()
    }};
    ($row:ident, $idx:ident, $t:ty, $convert:ident) => {{
        let value: Option<$t> = $row.get($idx);
        $convert(value.unwrap()).to_string()
    }};
}

fn cell_to_string(row: &Row, column: &Column, idx: usize) -> String {
    if idx >= row.columns().len() {
        return "NULL".to_string();
    }
    match column.type_().clone() {
        Type::INT2 => make_string!(row, idx, i16),
        Type::INT4 => make_string!(row, idx, i32),
        Type::INT8 => make_string!(row, idx, i64),
        Type::NUMERIC => make_string!(row, idx, Decimal, decimal_to_str),
        Type::DATE => make_string!(row, idx, NaiveDate),
        Type::TIME => make_string!(row, idx, NaiveTime),
        Type::TIMESTAMP => make_string!(row, idx, NaiveDateTime),
        Type::BOOL => make_string!(row, idx, bool, bool_to_str),
        // Type::INT2_ARRAY => array_process!(row, row_vec, idx, i16),
        // Type::INT4_ARRAY => array_process!(row, row_vec, idx, i32),
        // Type::INT8_ARRAY => array_process!(row, row_vec, idx, i64),
        // Type::BOOL_ARRAY => array_process!(row, row_vec, idx, bool, bool_to_str),
        // Type::FLOAT4_ARRAY => array_process!(row, row_vec, idx, f32, float4_to_str),
        // Type::FLOAT8_ARRAY => array_process!(row, row_vec, idx, f64, float8_to_str),
        // Type::NUMERIC_ARRAY => array_process!(row, row_vec, idx, Decimal),
        // Type::DATE_ARRAY => array_process!(row, row_vec, idx, NaiveDate),
        // Type::TIME_ARRAY => array_process!(row, row_vec, idx, NaiveTime),
        // Type::TIMESTAMP_ARRAY => array_process!(row, row_vec, idx, NaiveDateTime),
        // Type::VARCHAR_ARRAY | Type::TEXT_ARRAY => array_process!(row, row_vec, idx, String, varchar_to_str),
        Type::VARCHAR | Type::TEXT => make_string!(row, idx, &str, varchar_to_str),
        Type::FLOAT4 => make_string!(row, idx, f32, float4_to_str),
        Type::FLOAT8 => make_string!(row, idx, f64, float8_to_str),
        // Type::INTERVAL => {
        //     single_process!(self, row, row_vec, idx, Interval, INTERVAL);
        // }
        // Type::TIMESTAMPTZ => {
        //     single_process!(
        //                     self,
        //                     row,
        //                     row_vec,
        //                     idx,
        //                     DateTime<chrono::Utc>,
        //                     TIMESTAMPTZ
        //                 );
        // }
        // Type::INTERVAL_ARRAY => {
        //     array_process!(self, row, row_vec, idx, Interval, INTERVAL);
        // }
        // Type::TIMESTAMPTZ_ARRAY => {
        //     array_process!(self, row, row_vec, idx, DateTime<chrono::Utc>, TIMESTAMPTZ);
        // }
        _ => {
            todo!("Unsupported type: {}", column.type_().name())
        }
    }
}

#[async_trait]
impl sqllogictest::AsyncDB for Postgres {
    type Error = tokio_postgres::error::Error;

    async fn run(&mut self, sql: &str) -> Result<DBOutput, Self::Error> {
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
