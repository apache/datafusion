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

//! Distributed execution context.

use log::info;
use parking_lot::Mutex;
use sqlparser::ast::Statement;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use ballista_core::config::BallistaConfig;
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use ballista_core::serde::protobuf::{ExecuteQueryParams, KeyValuePair, LogicalPlanNode};
use ballista_core::utils::create_df_ctx_with_ballista_query_planner;

use datafusion::catalog::TableReference;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_plan::{CreateExternalTable, LogicalPlan, TableScan};
use datafusion::prelude::{
    AvroReadOptions, CsvReadOptions, ParquetReadOptions, SessionConfig, SessionContext,
};
use datafusion::sql::parser::{DFParser, FileType, Statement as DFStatement};

struct BallistaContextState {
    /// Ballista configuration
    config: BallistaConfig,
    /// Scheduler host
    scheduler_host: String,
    /// Scheduler port
    scheduler_port: u16,
    /// Tables that have been registered with this context
    tables: HashMap<String, Arc<dyn TableProvider>>,
}

impl BallistaContextState {
    pub fn new(
        scheduler_host: String,
        scheduler_port: u16,
        config: &BallistaConfig,
    ) -> Self {
        Self {
            config: config.clone(),
            scheduler_host,
            scheduler_port,
            tables: HashMap::new(),
        }
    }

    pub fn config(&self) -> &BallistaConfig {
        &self.config
    }
}

pub struct BallistaContext {
    state: Arc<Mutex<BallistaContextState>>,
    context: Arc<SessionContext>,
}

impl BallistaContext {
    /// Create a context for executing queries against a remote Ballista scheduler instance
    pub async fn remote(
        host: &str,
        port: u16,
        config: &BallistaConfig,
    ) -> ballista_core::error::Result<Self> {
        let state = BallistaContextState::new(host.to_owned(), port, config);

        let scheduler_url =
            format!("http://{}:{}", &state.scheduler_host, state.scheduler_port);
        info!(
            "Connecting to Ballista scheduler at {}",
            scheduler_url.clone()
        );
        let mut scheduler = SchedulerGrpcClient::connect(scheduler_url.clone())
            .await
            .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;

        let remote_session_id = scheduler
            .execute_query(ExecuteQueryParams {
                query: None,
                settings: config
                    .settings()
                    .iter()
                    .map(|(k, v)| KeyValuePair {
                        key: k.to_owned(),
                        value: v.to_owned(),
                    })
                    .collect::<Vec<_>>(),
                optional_session_id: None,
            })
            .await
            .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?
            .into_inner()
            .session_id;

        info!(
            "Server side SessionContext created with session id: {}",
            remote_session_id
        );

        let ctx = {
            create_df_ctx_with_ballista_query_planner::<LogicalPlanNode>(
                scheduler_url,
                remote_session_id,
                state.config(),
            )
        };

        Ok(Self {
            state: Arc::new(Mutex::new(state)),
            context: Arc::new(ctx),
        })
    }

    #[cfg(feature = "standalone")]
    pub async fn standalone(
        config: &BallistaConfig,
        concurrent_tasks: usize,
    ) -> ballista_core::error::Result<Self> {
        use ballista_core::serde::protobuf::PhysicalPlanNode;
        use ballista_core::serde::BallistaCodec;

        log::info!("Running in local mode. Scheduler will be run in-proc");

        let addr = ballista_scheduler::standalone::new_standalone_scheduler().await?;
        let scheduler_url = format!("http://localhost:{}", addr.port());
        let mut scheduler = loop {
            match SchedulerGrpcClient::connect(scheduler_url.clone()).await {
                Err(_) => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    log::info!("Attempting to connect to in-proc scheduler...");
                }
                Ok(scheduler) => break scheduler,
            }
        };

        let remote_session_id = scheduler
            .execute_query(ExecuteQueryParams {
                query: None,
                settings: config
                    .settings()
                    .iter()
                    .map(|(k, v)| KeyValuePair {
                        key: k.to_owned(),
                        value: v.to_owned(),
                    })
                    .collect::<Vec<_>>(),
                optional_session_id: None,
            })
            .await
            .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?
            .into_inner()
            .session_id;

        info!(
            "Server side SessionContext created with session id: {}",
            remote_session_id
        );

        let ctx = {
            create_df_ctx_with_ballista_query_planner::<LogicalPlanNode>(
                scheduler_url,
                remote_session_id,
                config,
            )
        };

        let default_codec: BallistaCodec<LogicalPlanNode, PhysicalPlanNode> =
            BallistaCodec::default();

        ballista_executor::new_standalone_executor(
            scheduler,
            concurrent_tasks,
            default_codec,
        )
        .await?;

        let state =
            BallistaContextState::new("localhost".to_string(), addr.port(), config);

        Ok(Self {
            state: Arc::new(Mutex::new(state)),
            context: Arc::new(ctx),
        })
    }

    /// Create a DataFrame representing an Avro table scan
    /// TODO fetch schema from scheduler instead of resolving locally
    pub async fn read_avro(
        &self,
        path: &str,
        options: AvroReadOptions<'_>,
    ) -> Result<Arc<DataFrame>> {
        // convert to absolute path because the executor likely has a different working directory
        let path = PathBuf::from(path);
        let path = fs::canonicalize(&path)?;

        let ctx = self.context.clone();
        let df = ctx.read_avro(path.to_str().unwrap(), options).await?;
        Ok(df)
    }

    /// Create a DataFrame representing a Parquet table scan
    /// TODO fetch schema from scheduler instead of resolving locally
    pub async fn read_parquet(
        &self,
        path: &str,
        options: ParquetReadOptions<'_>,
    ) -> Result<Arc<DataFrame>> {
        // convert to absolute path because the executor likely has a different working directory
        let path = PathBuf::from(path);
        let path = fs::canonicalize(&path)?;

        let ctx = self.context.clone();
        let df = ctx.read_parquet(path.to_str().unwrap(), options).await?;
        Ok(df)
    }

    /// Create a DataFrame representing a CSV table scan
    /// TODO fetch schema from scheduler instead of resolving locally
    pub async fn read_csv(
        &self,
        path: &str,
        options: CsvReadOptions<'_>,
    ) -> Result<Arc<DataFrame>> {
        // convert to absolute path because the executor likely has a different working directory
        let path = PathBuf::from(path);
        let path = fs::canonicalize(&path)?;

        let ctx = self.context.clone();
        let df = ctx.read_csv(path.to_str().unwrap(), options).await?;
        Ok(df)
    }

    /// Register a DataFrame as a table that can be referenced from a SQL query
    pub fn register_table(
        &self,
        name: &str,
        table: Arc<dyn TableProvider>,
    ) -> Result<()> {
        let mut state = self.state.lock();
        state.tables.insert(name.to_owned(), table);
        Ok(())
    }

    pub async fn register_csv(
        &self,
        name: &str,
        path: &str,
        options: CsvReadOptions<'_>,
    ) -> Result<()> {
        match self.read_csv(path, options).await?.to_logical_plan() {
            LogicalPlan::TableScan(TableScan { source, .. }) => {
                self.register_table(name, source)
            }
            _ => Err(DataFusionError::Internal("Expected tables scan".to_owned())),
        }
    }

    pub async fn register_parquet(
        &self,
        name: &str,
        path: &str,
        options: ParquetReadOptions<'_>,
    ) -> Result<()> {
        match self.read_parquet(path, options).await?.to_logical_plan() {
            LogicalPlan::TableScan(TableScan { source, .. }) => {
                self.register_table(name, source)
            }
            _ => Err(DataFusionError::Internal("Expected tables scan".to_owned())),
        }
    }

    pub async fn register_avro(
        &self,
        name: &str,
        path: &str,
        options: AvroReadOptions<'_>,
    ) -> Result<()> {
        match self.read_avro(path, options).await?.to_logical_plan() {
            LogicalPlan::TableScan(TableScan { source, .. }) => {
                self.register_table(name, source)
            }
            _ => Err(DataFusionError::Internal("Expected tables scan".to_owned())),
        }
    }

    /// is a 'show *' sql
    pub async fn is_show_statement(&self, sql: &str) -> Result<bool> {
        let mut is_show_variable: bool = false;
        let statements = DFParser::parse_sql(sql)?;

        if statements.len() != 1 {
            return Err(DataFusionError::NotImplemented(
                "The context currently only supports a single SQL statement".to_string(),
            ));
        }

        if let DFStatement::Statement(s) = &statements[0] {
            let st: &Statement = s;
            match st {
                Statement::ShowVariable { .. } => {
                    is_show_variable = true;
                }
                Statement::ShowColumns { .. } => {
                    is_show_variable = true;
                }
                _ => {
                    is_show_variable = false;
                }
            }
        };

        Ok(is_show_variable)
    }

    /// Create a DataFrame from a SQL statement.
    ///
    /// This method is `async` because queries of type `CREATE EXTERNAL TABLE`
    /// might require the schema to be inferred.
    pub async fn sql(&self, sql: &str) -> Result<Arc<DataFrame>> {
        let mut ctx = self.context.clone();

        let is_show = self.is_show_statement(sql).await?;
        // the show tables、 show columns sql can not run at scheduler because the tables is store at client
        if is_show {
            let state = self.state.lock();
            ctx = Arc::new(SessionContext::with_config(
                SessionConfig::new().with_information_schema(
                    state.config.default_with_information_schema(),
                ),
            ));
        }

        // register tables with DataFusion context
        {
            let state = self.state.lock();
            for (name, prov) in &state.tables {
                // ctx is shared between queries, check table exists or not before register
                let table_ref = TableReference::Bare { table: name };
                if !ctx.table_exist(table_ref)? {
                    ctx.register_table(
                        TableReference::Bare { table: name },
                        Arc::clone(prov),
                    )?;
                }
            }
        }

        let plan = ctx.create_logical_plan(sql)?;

        match plan {
            LogicalPlan::CreateExternalTable(CreateExternalTable {
                ref schema,
                ref name,
                ref location,
                ref file_type,
                ref has_header,
                ref delimiter,
                ref table_partition_cols,
                ref if_not_exists,
            }) => {
                let table_exists = ctx.table_exist(name.as_str())?;

                match (if_not_exists, table_exists) {
                    (_, false) => match file_type {
                        FileType::CSV => {
                            self.register_csv(
                                name,
                                location,
                                CsvReadOptions::new()
                                    .schema(&schema.as_ref().to_owned().into())
                                    .has_header(*has_header)
                                    .delimiter(*delimiter as u8)
                                    .table_partition_cols(table_partition_cols.to_vec()),
                            )
                            .await?;
                            Ok(Arc::new(DataFrame::new(ctx.state.clone(), &plan)))
                        }
                        FileType::Parquet => {
                            self.register_parquet(
                                name,
                                location,
                                ParquetReadOptions::default()
                                    .table_partition_cols(table_partition_cols.to_vec()),
                            )
                            .await?;
                            Ok(Arc::new(DataFrame::new(ctx.state.clone(), &plan)))
                        }
                        FileType::Avro => {
                            self.register_avro(
                                name,
                                location,
                                AvroReadOptions::default()
                                    .table_partition_cols(table_partition_cols.to_vec()),
                            )
                            .await?;
                            Ok(Arc::new(DataFrame::new(ctx.state.clone(), &plan)))
                        }
                        _ => Err(DataFusionError::NotImplemented(format!(
                            "Unsupported file type {:?}.",
                            file_type
                        ))),
                    },
                    (true, true) => {
                        Ok(Arc::new(DataFrame::new(ctx.state.clone(), &plan)))
                    }
                    (false, true) => Err(DataFusionError::Execution(format!(
                        "Table '{:?}' already exists",
                        name
                    ))),
                }
            }
            _ => ctx.sql(sql).await,
        }
    }
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    #[cfg(feature = "standalone")]
    async fn test_standalone_mode() {
        use super::*;
        let context = BallistaContext::standalone(&BallistaConfig::new().unwrap(), 1)
            .await
            .unwrap();
        let df = context.sql("SELECT 1;").await.unwrap();
        df.collect().await.unwrap();
    }

    #[tokio::test]
    #[cfg(feature = "standalone")]
    async fn test_ballista_show_tables() {
        use super::*;
        use std::fs::File;
        use std::io::Write;
        use tempfile::TempDir;
        let context = BallistaContext::standalone(&BallistaConfig::new().unwrap(), 1)
            .await
            .unwrap();

        let data = "Jorge,2018-12-13T12:12:10.011Z\n\
                    Andrew,2018-11-13T17:11:10.011Z";

        let tmp_dir = TempDir::new().unwrap();
        let file_path = tmp_dir.path().join("timestamps.csv");

        // scope to ensure the file is closed and written
        {
            File::create(&file_path)
                .expect("creating temp file")
                .write_all(data.as_bytes())
                .expect("writing data");
        }

        let sql = format!(
            "CREATE EXTERNAL TABLE csv_with_timestamps (
                  name VARCHAR,
                  ts TIMESTAMP
              )
              STORED AS CSV
              LOCATION '{}'
              ",
            file_path.to_str().expect("path is utf8")
        );

        context.sql(sql.as_str()).await.unwrap();

        let df = context.sql("show columns from csv_with_timestamps;").await;

        assert!(df.is_err());
    }

    #[tokio::test]
    #[cfg(feature = "standalone")]
    async fn test_show_tables_not_with_information_schema() {
        use super::*;
        use ballista_core::config::{
            BallistaConfigBuilder, BALLISTA_WITH_INFORMATION_SCHEMA,
        };
        use std::fs::File;
        use std::io::Write;
        use tempfile::TempDir;
        let config = BallistaConfigBuilder::default()
            .set(BALLISTA_WITH_INFORMATION_SCHEMA, "true")
            .build()
            .unwrap();
        let context = BallistaContext::standalone(&config, 1).await.unwrap();

        let data = "Jorge,2018-12-13T12:12:10.011Z\n\
                    Andrew,2018-11-13T17:11:10.011Z";

        let tmp_dir = TempDir::new().unwrap();
        let file_path = tmp_dir.path().join("timestamps.csv");

        // scope to ensure the file is closed and written
        {
            File::create(&file_path)
                .expect("creating temp file")
                .write_all(data.as_bytes())
                .expect("writing data");
        }

        let sql = format!(
            "CREATE EXTERNAL TABLE csv_with_timestamps (
                  name VARCHAR,
                  ts TIMESTAMP
              )
              STORED AS CSV
              LOCATION '{}'
              ",
            file_path.to_str().expect("path is utf8")
        );

        context.sql(sql.as_str()).await.unwrap();
        let df = context.sql("show tables;").await;
        assert!(df.is_ok());
    }

    #[tokio::test]
    #[cfg(feature = "standalone")]
    #[ignore]
    // Tracking: https://github.com/apache/arrow-datafusion/issues/1840
    async fn test_task_stuck_when_referenced_task_failed() {
        use super::*;
        use datafusion::arrow::datatypes::Schema;
        use datafusion::arrow::util::pretty;
        use datafusion::datasource::file_format::csv::CsvFormat;
        use datafusion::datasource::listing::{
            ListingOptions, ListingTable, ListingTableConfig,
        };

        use ballista_core::config::{
            BallistaConfigBuilder, BALLISTA_WITH_INFORMATION_SCHEMA,
        };
        let config = BallistaConfigBuilder::default()
            .set(BALLISTA_WITH_INFORMATION_SCHEMA, "true")
            .build()
            .unwrap();
        let context = BallistaContext::standalone(&config, 1).await.unwrap();

        let testdata = datafusion::test_util::parquet_test_data();
        context
            .register_parquet(
                "single_nan",
                &format!("{}/single_nan.parquet", testdata),
                ParquetReadOptions::default(),
            )
            .await
            .unwrap();

        {
            let mut guard = context.state.lock();
            let csv_table = guard.tables.get("single_nan");

            if let Some(table_provide) = csv_table {
                if let Some(listing_table) = table_provide
                    .clone()
                    .as_any()
                    .downcast_ref::<ListingTable>()
                {
                    let x = listing_table.options();
                    let error_options = ListingOptions {
                        file_extension: x.file_extension.clone(),
                        format: Arc::new(CsvFormat::default()),
                        table_partition_cols: x.table_partition_cols.clone(),
                        collect_stat: x.collect_stat,
                        target_partitions: x.target_partitions,
                    };

                    let config = ListingTableConfig::new(
                        listing_table.object_store().clone(),
                        listing_table.table_path().to_string(),
                    )
                    .with_schema(Arc::new(Schema::new(vec![])))
                    .with_listing_options(error_options);

                    let error_table = ListingTable::try_new(config).unwrap();

                    // change the table to an error table
                    guard
                        .tables
                        .insert("single_nan".to_string(), Arc::new(error_table));
                }
            }
        }

        let df = context
            .sql("select count(1) from single_nan;")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        pretty::print_batches(&results);
    }

    #[tokio::test]
    #[cfg(feature = "standalone")]
    async fn test_empty_exec_with_one_row() {
        use crate::context::BallistaContext;
        use ballista_core::config::{
            BallistaConfigBuilder, BALLISTA_WITH_INFORMATION_SCHEMA,
        };

        let config = BallistaConfigBuilder::default()
            .set(BALLISTA_WITH_INFORMATION_SCHEMA, "true")
            .build()
            .unwrap();
        let context = BallistaContext::standalone(&config, 1).await.unwrap();

        let sql = "select EXTRACT(year FROM to_timestamp('2020-09-08T12:13:14+00:00'));";

        let df = context.sql(sql).await.unwrap();
        assert!(!df.collect().await.unwrap().is_empty());
    }

    #[tokio::test]
    #[cfg(feature = "standalone")]
    async fn test_union_and_union_all() {
        use super::*;
        use ballista_core::config::{
            BallistaConfigBuilder, BALLISTA_WITH_INFORMATION_SCHEMA,
        };
        use datafusion::arrow::util::pretty::pretty_format_batches;
        use datafusion::assert_batches_eq;
        let config = BallistaConfigBuilder::default()
            .set(BALLISTA_WITH_INFORMATION_SCHEMA, "true")
            .build()
            .unwrap();
        let context = BallistaContext::standalone(&config, 1).await.unwrap();

        let df = context
            .sql("SELECT 1 as NUMBER union SELECT 1 as NUMBER;")
            .await
            .unwrap();
        let res1 = df.collect().await.unwrap();
        let expected1 = vec![
            "+--------+",
            "| number |",
            "+--------+",
            "| 1      |",
            "+--------+",
        ];
        assert_eq!(
            expected1,
            pretty_format_batches(&*res1)
                .unwrap()
                .to_string()
                .trim()
                .lines()
                .collect::<Vec<&str>>()
        );
        let expected2 = vec![
            "+--------+",
            "| number |",
            "+--------+",
            "| 1      |",
            "| 1      |",
            "+--------+",
        ];
        let df = context
            .sql("SELECT 1 as NUMBER union all SELECT 1 as NUMBER;")
            .await
            .unwrap();
        let res2 = df.collect().await.unwrap();
        assert_eq!(
            expected2,
            pretty_format_batches(&*res2)
                .unwrap()
                .to_string()
                .trim()
                .lines()
                .collect::<Vec<&str>>()
        );
    }
}
