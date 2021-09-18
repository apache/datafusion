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
use ballista_core::{
    datasource::DfTableAdapter, utils::create_df_ctx_with_ballista_query_planner,
};

use datafusion::catalog::TableReference;
use datafusion::dataframe::DataFrame;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::dataframe_impl::DataFrameImpl;
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::avro::AvroReadOptions;
use datafusion::physical_plan::csv::CsvReadOptions;
use datafusion::sql::parser::FileType;

struct BallistaContextState {
    /// Ballista configuration
    config: BallistaConfig,
    /// Scheduler host
    scheduler_host: String,
    /// Scheduler port
    scheduler_port: u16,
    /// Tables that have been registered with this context
    tables: HashMap<String, LogicalPlan>,
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
        info!("Running in local mode. Scheduler will be run in-proc");

        let addr = ballista_scheduler::new_standalone_scheduler().await?;

        let scheduler = loop {
            match SchedulerGrpcClient::connect(format!(
                "http://localhost:{}",
                addr.port()
            ))
            .await
            {
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    info!("Attempting to connect to in-proc scheduler...");
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

    pub fn read_avro(
        &self,
        path: &str,
        options: AvroReadOptions,
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
        let df = ctx.read_avro(path.to_str().unwrap(), options)?;
        Ok(df)
    }

    /// Create a DataFrame representing a Parquet table scan

    pub fn read_parquet(&self, path: &str) -> Result<Arc<dyn DataFrame>> {
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
        let df = ctx.read_parquet(path.to_str().unwrap())?;
        Ok(df)
    }

    /// Create a DataFrame representing a CSV table scan

    pub fn read_csv(
        &self,
        path: &str,
        options: CsvReadOptions,
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
        let df = ctx.read_csv(path.to_str().unwrap(), options)?;
        Ok(df)
    }

    /// Register a DataFrame as a table that can be referenced from a SQL query
    pub fn register_table(&self, name: &str, table: &dyn DataFrame) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state
            .tables
            .insert(name.to_owned(), table.to_logical_plan());
        Ok(())
    }

    pub fn register_csv(
        &self,
        name: &str,
        path: &str,
        options: CsvReadOptions,
    ) -> Result<()> {
        let df = self.read_csv(path, options)?;
        self.register_table(name, df.as_ref())
    }

    pub fn register_parquet(&self, name: &str, path: &str) -> Result<()> {
        let df = self.read_parquet(path)?;
        self.register_table(name, df.as_ref())
    }

    pub fn register_avro(
        &self,
        name: &str,
        path: &str,
        options: AvroReadOptions,
    ) -> Result<()> {
        let df = self.read_avro(path, options)?;
        self.register_table(name, df.as_ref())?;
        Ok(())
    }

    /// Create a DataFrame from a SQL statement
    pub fn sql(&self, sql: &str) -> Result<Arc<dyn DataFrame>> {
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
            for (name, plan) in &state.tables {
                let plan = ctx.optimize(plan)?;
                let execution_plan = ctx.create_physical_plan(&plan)?;
                ctx.register_table(
                    TableReference::Bare { table: name },
                    Arc::new(DfTableAdapter::new(plan, execution_plan)),
                )?;
            }
        }

        let plan = ctx.create_logical_plan(sql)?;
        match plan {
            LogicalPlan::CreateExternalTable {
                ref schema,
                ref name,
                ref location,
                ref file_type,
                ref has_header,
            } => match file_type {
                FileType::CSV => {
                    self.register_csv(
                        name,
                        location,
                        CsvReadOptions::new()
                            .schema(&schema.as_ref().to_owned().into())
                            .has_header(*has_header),
                    )?;
                    Ok(Arc::new(DataFrameImpl::new(ctx.state, &plan)))
                }
                FileType::Parquet => {
                    self.register_parquet(name, location)?;
                    Ok(Arc::new(DataFrameImpl::new(ctx.state, &plan)))
                }
                FileType::Avro => {
                    self.register_avro(name, location, AvroReadOptions::default())?;
                    Ok(Arc::new(DataFrameImpl::new(ctx.state, &plan)))
                }
                _ => Err(DataFusionError::NotImplemented(format!(
                    "Unsupported file type {:?}.",
                    file_type
                ))),
            },

            _ => ctx.sql(sql),
        }
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    #[cfg(feature = "standalone")]
    async fn test_standalone_mode() {
        use super::*;
        let context = BallistaContext::standalone(1).await.unwrap();
        let df = context.sql("SELECT 1;").unwrap();
        context.collect(&df.to_logical_plan()).await.unwrap();
    }
}
