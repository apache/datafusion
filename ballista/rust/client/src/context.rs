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

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use ballista_core::config::BallistaConfig;
use ballista_core::utils::create_df_ctx_with_ballista_query_planner;

use datafusion::catalog::TableReference;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::dataframe_impl::DataFrameImpl;
use datafusion::logical_plan::{CreateExternalTable, LogicalPlan, TableScan};
use datafusion::prelude::{AvroReadOptions, CsvReadOptions};
use datafusion::sql::parser::FileType;

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

    #[cfg(feature = "standalone")]
    pub async fn new_standalone(
        config: &BallistaConfig,
        concurrent_tasks: usize,
    ) -> ballista_core::error::Result<Self> {
        use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;

        log::info!("Running in local mode. Scheduler will be run in-proc");

        let addr = ballista_scheduler::new_standalone_scheduler().await?;

        let scheduler = loop {
            match SchedulerGrpcClient::connect(format!(
                "http://localhost:{}",
                addr.port()
            ))
            .await
            {
                Err(_) => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    log::info!("Attempting to connect to in-proc scheduler...");
                }
                Ok(scheduler) => break scheduler,
            }
        };

        ballista_executor::new_standalone_executor(scheduler, concurrent_tasks).await?;
        Ok(Self {
            config: config.clone(),
            scheduler_host: "localhost".to_string(),
            scheduler_port: addr.port(),
            tables: HashMap::new(),
        })
    }

    pub fn config(&self) -> &BallistaConfig {
        &self.config
    }
}

pub struct BallistaContext {
    state: Arc<Mutex<BallistaContextState>>,
}

impl BallistaContext {
    /// Create a context for executing queries against a remote Ballista scheduler instance
    pub fn remote(host: &str, port: u16, config: &BallistaConfig) -> Self {
        let state = BallistaContextState::new(host.to_owned(), port, config);

        Self {
            state: Arc::new(Mutex::new(state)),
        }
    }

    #[cfg(feature = "standalone")]
    pub async fn standalone(
        config: &BallistaConfig,
        concurrent_tasks: usize,
    ) -> ballista_core::error::Result<Self> {
        let state =
            BallistaContextState::new_standalone(config, concurrent_tasks).await?;

        Ok(Self {
            state: Arc::new(Mutex::new(state)),
        })
    }

    /// Create a DataFrame representing an Avro table scan
    /// TODO fetch schema from scheduler instead of resolving locally
    pub async fn read_avro(
        &self,
        path: &str,
        options: AvroReadOptions<'_>,
    ) -> Result<Arc<dyn DataFrame>> {
        // convert to absolute path because the executor likely has a different working directory
        let path = PathBuf::from(path);
        let path = fs::canonicalize(&path)?;

        // use local DataFusion context for now but later this might call the scheduler
        let mut ctx = {
            let guard = self.state.lock().unwrap();
            create_df_ctx_with_ballista_query_planner(
                &guard.scheduler_host,
                guard.scheduler_port,
                guard.config(),
            )
        };
        let df = ctx.read_avro(path.to_str().unwrap(), options).await?;
        Ok(df)
    }

    /// Create a DataFrame representing a Parquet table scan
    /// TODO fetch schema from scheduler instead of resolving locally
    pub async fn read_parquet(&self, path: &str) -> Result<Arc<dyn DataFrame>> {
        // convert to absolute path because the executor likely has a different working directory
        let path = PathBuf::from(path);
        let path = fs::canonicalize(&path)?;

        // use local DataFusion context for now but later this might call the scheduler
        let mut ctx = {
            let guard = self.state.lock().unwrap();
            create_df_ctx_with_ballista_query_planner(
                &guard.scheduler_host,
                guard.scheduler_port,
                guard.config(),
            )
        };
        let df = ctx.read_parquet(path.to_str().unwrap()).await?;
        Ok(df)
    }

    /// Create a DataFrame representing a CSV table scan
    /// TODO fetch schema from scheduler instead of resolving locally
    pub async fn read_csv(
        &self,
        path: &str,
        options: CsvReadOptions<'_>,
    ) -> Result<Arc<dyn DataFrame>> {
        // convert to absolute path because the executor likely has a different working directory
        let path = PathBuf::from(path);
        let path = fs::canonicalize(&path)?;

        // use local DataFusion context for now but later this might call the scheduler
        let mut ctx = {
            let guard = self.state.lock().unwrap();
            create_df_ctx_with_ballista_query_planner(
                &guard.scheduler_host,
                guard.scheduler_port,
                guard.config(),
            )
        };
        let df = ctx.read_csv(path.to_str().unwrap(), options).await?;
        Ok(df)
    }

    /// Register a DataFrame as a table that can be referenced from a SQL query
    pub fn register_table(
        &self,
        name: &str,
        table: Arc<dyn TableProvider>,
    ) -> Result<()> {
        let mut state = self.state.lock().unwrap();
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

    pub async fn register_parquet(&self, name: &str, path: &str) -> Result<()> {
        match self.read_parquet(path).await?.to_logical_plan() {
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

    /// Create a DataFrame from a SQL statement.
    ///
    /// This method is `async` because queries of type `CREATE EXTERNAL TABLE`
    /// might require the schema to be inferred.
    pub async fn sql(&self, sql: &str) -> Result<Arc<dyn DataFrame>> {
        let mut ctx = {
            let state = self.state.lock().unwrap();
            create_df_ctx_with_ballista_query_planner(
                &state.scheduler_host,
                state.scheduler_port,
                state.config(),
            )
        };

        // register tables with DataFusion context
        {
            let state = self.state.lock().unwrap();
            for (name, prov) in &state.tables {
                ctx.register_table(
                    TableReference::Bare { table: name },
                    Arc::clone(prov),
                )?;
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
            }) => match file_type {
                FileType::CSV => {
                    self.register_csv(
                        name,
                        location,
                        CsvReadOptions::new()
                            .schema(&schema.as_ref().to_owned().into())
                            .has_header(*has_header),
                    )
                    .await?;
                    Ok(Arc::new(DataFrameImpl::new(ctx.state, &plan)))
                }
                FileType::Parquet => {
                    self.register_parquet(name, location).await?;
                    Ok(Arc::new(DataFrameImpl::new(ctx.state, &plan)))
                }
                FileType::Avro => {
                    self.register_avro(name, location, AvroReadOptions::default())
                        .await?;
                    Ok(Arc::new(DataFrameImpl::new(ctx.state, &plan)))
                }
                _ => Err(DataFusionError::NotImplemented(format!(
                    "Unsupported file type {:?}.",
                    file_type
                ))),
            },

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
}
