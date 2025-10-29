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

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::common::runtime::SpawnedTask;
use futures::{SinkExt, StreamExt};
use log::{debug, info};
use sqllogictest::DBOutput;
/// Postgres engine implementation for sqllogictest.
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use super::conversion::*;
use crate::engines::output::{DFColumnType, DFOutput};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use indicatif::ProgressBar;
use postgres_types::Type;
use rust_decimal::Decimal;
use tokio::time::Instant;
use tokio_postgres::{Column, Row};
use types::PgRegtype;

mod types;

// default connect string, can be overridden by the `PG_URL` environment variable
const PG_URI: &str = "postgresql://postgres@127.0.0.1/test";

/// DataFusion sql-logicaltest error
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Postgres error: {0}")]
    Postgres(#[from] tokio_postgres::error::Error),
    #[error("Error handling copy command: {0}")]
    Copy(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Postgres {
    // None means the connection has been shutdown
    client: Option<tokio_postgres::Client>,
    spawned_task: Option<SpawnedTask<()>>,
    /// Relative test file path
    relative_path: PathBuf,
    pb: ProgressBar,
}

impl Postgres {
    /// Creates a runner for executing queries against an existing postgres connection.
    /// `relative_path` is used for display output and to create a postgres schema.
    ///
    /// The database connection details can be overridden by the
    /// `PG_URI` environment variable.
    ///
    /// This defaults to
    ///
    /// ```text
    /// PG_URI="postgresql://postgres@127.0.0.1/test"
    /// ```
    ///
    /// See https://docs.rs/tokio-postgres/latest/tokio_postgres/config/struct.Config.html#url for format
    pub async fn connect(relative_path: PathBuf, pb: ProgressBar) -> Result<Self> {
        let uri =
            std::env::var("PG_URI").map_or(PG_URI.to_string(), std::convert::identity);

        info!("Using postgres connection string: {uri}");

        let config = tokio_postgres::Config::from_str(&uri)?;

        // hint to user what the connection string was
        let res = config.connect(tokio_postgres::NoTls).await;
        if res.is_err() {
            eprintln!("Error connecting to postgres using PG_URI={uri}");
        };

        let (client, connection) = res?;

        let spawned_task = SpawnedTask::spawn(async move {
            if let Err(e) = connection.await {
                log::error!("Postgres connection error: {e:?}");
            }
        });

        let schema = schema_name(&relative_path);

        // create a new clean schema for running the test
        debug!("Creating new empty schema '{schema}'");
        client
            .execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"), &[])
            .await?;

        client
            .execute(&format!("CREATE SCHEMA {schema}"), &[])
            .await?;

        client
            .execute(&format!("SET search_path TO {schema}"), &[])
            .await?;

        Ok(Self {
            client: Some(client),
            spawned_task: Some(spawned_task),
            relative_path,
            pb,
        })
    }

    fn get_client(&mut self) -> &mut tokio_postgres::Client {
        self.client.as_mut().expect("client is shutdown")
    }

    /// Special COPY command support. "COPY 'filename'" requires the
    /// server to read the file which may not be possible (maybe it is
    /// remote or running in some other docker container).
    ///
    /// Thus, we rewrite  sql statements like
    ///
    /// ```sql
    /// COPY ... FROM 'filename' ...
    /// ```
    ///
    /// Into
    ///
    /// ```sql
    /// COPY ... FROM STDIN ...
    /// ```
    ///
    /// And read the file locally.
    async fn run_copy_command(&mut self, sql: &str) -> Result<DFOutput> {
        let canonical_sql = sql.trim_start().to_ascii_lowercase();

        debug!("Handling COPY command: {sql}");

        // Hacky way to  find the 'filename' in the statement
        let mut tokens = canonical_sql.split_whitespace().peekable();
        let mut filename = None;

        // COPY FROM '/opt/data/csv/aggregate_test_100.csv' ...
        //
        // into
        //
        // COPY FROM STDIN ...

        let mut new_sql = vec![];
        while let Some(tok) = tokens.next() {
            new_sql.push(tok);
            // rewrite FROM <file> to FROM STDIN
            if tok == "from" {
                filename = tokens.next();
                new_sql.push("STDIN");
            }
        }

        let filename = filename.map(no_quotes).ok_or_else(|| {
            Error::Copy(format!("Can not find filename in COPY: {sql}"))
        })?;

        let new_sql = new_sql.join(" ");
        debug!("Copying data from file {filename} using sql: {new_sql}");

        // start the COPY command and get location to write data to
        let tx = self.get_client().transaction().await?;
        let sink = tx.copy_in(&new_sql).await?;
        let mut sink = Box::pin(sink);

        // read the input file as a string ans feed it to the copy command
        let data = std::fs::read_to_string(filename)
            .map_err(|e| Error::Copy(format!("Error reading {filename}: {e}")))?;

        let mut data_stream = futures::stream::iter(vec![Ok(Bytes::from(data))]).boxed();

        sink.send_all(&mut data_stream).await?;
        sink.close().await?;
        tx.commit().await?;
        Ok(DBOutput::StatementComplete(0))
    }

    fn update_slow_count(&self) {
        let msg = self.pb.message();
        let split: Vec<&str> = msg.split(" ").collect();
        let mut current_count = 0;

        if split.len() > 2 {
            // second match will be current slow count
            current_count += split[2].parse::<i32>().unwrap();
        }

        current_count += 1;

        self.pb
            .set_message(format!("{} - {} took > 500 ms", split[0], current_count));
    }
}

/// remove single quotes from the start and end of the string
///
/// 'filename' --> filename
fn no_quotes(t: &str) -> &str {
    t.trim_start_matches('\'').trim_end_matches('\'')
}

/// Given a file name like pg_compat_foo.slt
/// return a schema name
fn schema_name(relative_path: &Path) -> String {
    relative_path
        .to_string_lossy()
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
        .trim_start_matches("pg_")
        .to_string()
}

#[async_trait]
impl sqllogictest::AsyncDB for Postgres {
    type Error = Error;
    type ColumnType = DFColumnType;

    async fn run(
        &mut self,
        sql: &str,
    ) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        debug!(
            "[{}] Running query: \"{}\"",
            self.relative_path.display(),
            sql
        );

        let lower_sql = sql.trim_start().to_ascii_lowercase();

        let is_query_sql = {
            lower_sql.starts_with("select")
                || lower_sql.starts_with("values")
                || lower_sql.starts_with("show")
                || lower_sql.starts_with("with")
                || lower_sql.starts_with("describe")
                || ((lower_sql.starts_with("insert")
                    || lower_sql.starts_with("update")
                    || lower_sql.starts_with("delete"))
                    && lower_sql.contains("returning"))
        };

        if lower_sql.starts_with("copy") {
            self.pb.inc(1);
            return self.run_copy_command(sql).await;
        }

        if !is_query_sql {
            self.get_client().execute(sql, &[]).await?;
            self.pb.inc(1);
            return Ok(DBOutput::StatementComplete(0));
        }
        let start = Instant::now();
        let rows = self.get_client().query(sql, &[]).await?;
        let duration = start.elapsed();

        if duration.gt(&Duration::from_millis(500)) {
            self.update_slow_count();
        }

        self.pb.inc(1);

        let types: Vec<Type> = if rows.is_empty() {
            self.get_client()
                .prepare(sql)
                .await?
                .columns()
                .iter()
                .map(|c| c.type_().clone())
                .collect()
        } else {
            rows[0]
                .columns()
                .iter()
                .map(|c| c.type_().clone())
                .collect()
        };

        if rows.is_empty() && types.is_empty() {
            Ok(DBOutput::StatementComplete(0))
        } else {
            Ok(DBOutput::Rows {
                types: convert_types(types),
                rows: convert_rows(rows),
            })
        }
    }

    fn engine_name(&self) -> &str {
        "postgres"
    }

    async fn shutdown(&mut self) {
        if let Some(client) = self.client.take() {
            drop(client);
        }
        if let Some(spawned_task) = self.spawned_task.take() {
            spawned_task.join().await.ok();
        }
    }
}

fn convert_rows(rows: Vec<Row>) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| {
            row.columns()
                .iter()
                .enumerate()
                .map(|(idx, column)| cell_to_string(row, column, idx))
                .collect::<Vec<String>>()
        })
        .collect::<Vec<_>>()
}

macro_rules! make_string {
    ($row:ident, $idx:ident, $t:ty) => {{
        let value: Option<$t> = $row.get($idx);
        match value {
            Some(value) => value.to_string(),
            None => NULL_STR.to_string(),
        }
    }};
    ($row:ident, $idx:ident, $t:ty, $convert:ident) => {{
        let value: Option<$t> = $row.get($idx);
        match value {
            Some(value) => $convert(value).to_string(),
            None => NULL_STR.to_string(),
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
                .map(|d| format!("{d:?}"))
                .unwrap_or_else(|| "NULL".to_string())
        }
        Type::BOOL => make_string!(row, idx, bool, bool_to_str),
        Type::BPCHAR | Type::VARCHAR | Type::TEXT => {
            make_string!(row, idx, &str, varchar_to_str)
        }
        Type::FLOAT4 => make_string!(row, idx, f32, f32_to_str),
        Type::FLOAT8 => make_string!(row, idx, f64, f64_to_str),
        Type::REGTYPE => make_string!(row, idx, PgRegtype),
        _ => unimplemented!("Unsupported type: {}", column.type_().name()),
    }
}

fn convert_types(types: Vec<Type>) -> Vec<DFColumnType> {
    types
        .into_iter()
        .map(|t| match t {
            Type::BOOL => DFColumnType::Boolean,
            Type::INT2 | Type::INT4 | Type::INT8 => DFColumnType::Integer,
            Type::BPCHAR | Type::VARCHAR | Type::TEXT => DFColumnType::Text,
            Type::FLOAT4 | Type::FLOAT8 | Type::NUMERIC => DFColumnType::Float,
            Type::DATE | Type::TIME => DFColumnType::DateTime,
            Type::TIMESTAMP => DFColumnType::Timestamp,
            _ => DFColumnType::Another,
        })
        .collect()
}
