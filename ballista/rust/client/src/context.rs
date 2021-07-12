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

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::{collections::HashMap, convert::TryInto};
use std::{fs, time::Duration};

use ballista_core::config::BallistaConfig;
use ballista_core::serde::protobuf::{
    execute_query_params::Query, job_status, scheduler_grpc_client::SchedulerGrpcClient,
    ExecuteQueryParams, GetJobStatusParams, GetJobStatusResult, KeyValuePair,
    PartitionLocation,
};
use ballista_core::{
    client::BallistaClient, datasource::DfTableAdapter, utils::create_datafusion_context,
    utils::WrappedStream,
};

use datafusion::arrow::datatypes::Schema;
use datafusion::catalog::TableReference;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::csv::CsvReadOptions;
use datafusion::{dataframe::DataFrame, physical_plan::RecordBatchStream};
use futures::future;
use futures::StreamExt;
use log::{error, info};

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

    /// Create a DataFrame representing a Parquet table scan

    pub fn read_parquet(&self, path: &str) -> Result<Arc<dyn DataFrame>> {
        // convert to absolute path because the executor likely has a different working directory
        let path = PathBuf::from(path);
        let path = fs::canonicalize(&path)?;

        // use local DataFusion context for now but later this might call the scheduler
        let mut ctx = create_datafusion_context(&self.state.lock().unwrap().config());
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
        let mut ctx = create_datafusion_context(&self.state.lock().unwrap().config());
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

    /// Create a DataFrame from a SQL statement
    pub fn sql(&self, sql: &str) -> Result<Arc<dyn DataFrame>> {
        // use local DataFusion context for now but later this might call the scheduler
        // register tables
        let state = self.state.lock().unwrap();
        let mut ctx = create_datafusion_context(&state.config());
        for (name, plan) in &state.tables {
            let plan = ctx.optimize(plan)?;
            let execution_plan = ctx.create_physical_plan(&plan)?;
            ctx.register_table(
                TableReference::Bare { table: name },
                Arc::new(DfTableAdapter::new(plan, execution_plan)),
            )?;
        }
        ctx.sql(sql)
    }

    async fn fetch_partition(
        location: PartitionLocation,
    ) -> Result<Pin<Box<dyn RecordBatchStream + Send + Sync>>> {
        let metadata = location.executor_meta.ok_or_else(|| {
            DataFusionError::Internal("Received empty executor metadata".to_owned())
        })?;
        let partition_id = location.partition_id.ok_or_else(|| {
            DataFusionError::Internal("Received empty partition id".to_owned())
        })?;
        let mut ballista_client =
            BallistaClient::try_new(metadata.host.as_str(), metadata.port as u16)
                .await
                .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;
        Ok(ballista_client
            .fetch_partition(
                &partition_id.job_id,
                partition_id.stage_id as usize,
                partition_id.partition_id as usize,
            )
            .await
            .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?)
    }

    pub async fn collect(
        &self,
        plan: &LogicalPlan,
    ) -> Result<Pin<Box<dyn RecordBatchStream + Send + Sync>>> {
        let (scheduler_url, config) = {
            let state = self.state.lock().unwrap();
            let scheduler_url =
                format!("http://{}:{}", state.scheduler_host, state.scheduler_port);
            (scheduler_url, state.config.clone())
        };

        info!("Connecting to Ballista scheduler at {}", scheduler_url);

        let mut scheduler = SchedulerGrpcClient::connect(scheduler_url)
            .await
            .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;

        let schema: Schema = plan.schema().as_ref().clone().into();

        let job_id = scheduler
            .execute_query(ExecuteQueryParams {
                query: Some(Query::LogicalPlan(
                    (plan)
                        .try_into()
                        .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?,
                )),
                settings: config
                    .settings()
                    .iter()
                    .map(|(k, v)| KeyValuePair {
                        key: k.to_owned(),
                        value: v.to_owned(),
                    })
                    .collect::<Vec<_>>(),
            })
            .await
            .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?
            .into_inner()
            .job_id;

        let mut prev_status: Option<job_status::Status> = None;

        loop {
            let GetJobStatusResult { status } = scheduler
                .get_job_status(GetJobStatusParams {
                    job_id: job_id.clone(),
                })
                .await
                .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?
                .into_inner();
            let status = status.and_then(|s| s.status).ok_or_else(|| {
                DataFusionError::Internal("Received empty status message".to_owned())
            })?;
            let wait_future = tokio::time::sleep(Duration::from_millis(100));
            let has_status_change = prev_status.map(|x| x != status).unwrap_or(true);
            match status {
                job_status::Status::Queued(_) => {
                    if has_status_change {
                        info!("Job {} still queued...", job_id);
                    }
                    wait_future.await;
                    prev_status = Some(status);
                }
                job_status::Status::Running(_) => {
                    if has_status_change {
                        info!("Job {} is running...", job_id);
                    }
                    wait_future.await;
                    prev_status = Some(status);
                }
                job_status::Status::Failed(err) => {
                    let msg = format!("Job {} failed: {}", job_id, err.error);
                    error!("{}", msg);
                    break Err(DataFusionError::Execution(msg));
                }
                job_status::Status::Completed(completed) => {
                    let result = future::join_all(
                        completed
                            .partition_location
                            .into_iter()
                            .map(BallistaContext::fetch_partition),
                    )
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>>>()?;

                    let result = WrappedStream::new(
                        Box::pin(futures::stream::iter(result).flatten()),
                        Arc::new(schema),
                    );
                    break Ok(Box::pin(result));
                }
            };
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
