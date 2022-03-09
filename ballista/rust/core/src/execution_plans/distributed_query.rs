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

use std::any::Any;

use std::fmt::Debug;
use std::marker::PhantomData;

use std::sync::Arc;
use std::time::Duration;

use crate::client::BallistaClient;
use crate::config::BallistaConfig;
use crate::serde::protobuf::{
    execute_query_params::Query, job_status, scheduler_grpc_client::SchedulerGrpcClient,
    ExecuteQueryParams, GetJobStatusParams, GetJobStatusResult, KeyValuePair,
    PartitionLocation,
};
use crate::utils::WrappedStream;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};

use crate::serde::{AsLogicalPlan, DefaultLogicalExtensionCodec, LogicalExtensionCodec};
use async_trait::async_trait;
use datafusion::execution::runtime_env::RuntimeEnv;
use futures::future;
use futures::StreamExt;
use log::{error, info};

/// This operator sends a logial plan to a Ballista scheduler for execution and
/// polls the scheduler until the query is complete and then fetches the resulting
/// batches directly from the executors that hold the results from the final
/// query stage.
#[derive(Debug, Clone)]
pub struct DistributedQueryExec<T: 'static + AsLogicalPlan> {
    /// Ballista scheduler URL
    scheduler_url: String,
    /// Ballista configuration
    config: BallistaConfig,
    /// Logical plan to execute
    plan: LogicalPlan,
    /// Codec for LogicalPlan extensions
    extension_codec: Arc<dyn LogicalExtensionCodec>,
    /// Phantom data for serializable plan message
    plan_repr: PhantomData<T>,
}

impl<T: 'static + AsLogicalPlan> DistributedQueryExec<T> {
    pub fn new(scheduler_url: String, config: BallistaConfig, plan: LogicalPlan) -> Self {
        Self {
            scheduler_url,
            config,
            plan,
            extension_codec: Arc::new(DefaultLogicalExtensionCodec {}),
            plan_repr: PhantomData,
        }
    }

    pub fn with_extension(
        scheduler_url: String,
        config: BallistaConfig,
        plan: LogicalPlan,
        extension_codec: Arc<dyn LogicalExtensionCodec>,
    ) -> Self {
        Self {
            scheduler_url,
            config,
            plan,
            extension_codec,
            plan_repr: PhantomData,
        }
    }

    pub fn with_repr(
        scheduler_url: String,
        config: BallistaConfig,
        plan: LogicalPlan,
        extension_codec: Arc<dyn LogicalExtensionCodec>,
        plan_repr: PhantomData<T>,
    ) -> Self {
        Self {
            scheduler_url,
            config,
            plan,
            extension_codec,
            plan_repr,
        }
    }
}

#[async_trait]
impl<T: 'static + AsLogicalPlan> ExecutionPlan for DistributedQueryExec<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.plan.schema().as_ref().clone().into()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn relies_on_input_order(&self) -> bool {
        false
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DistributedQueryExec {
            scheduler_url: self.scheduler_url.clone(),
            config: self.config.clone(),
            plan: self.plan.clone(),
            extension_codec: self.extension_codec.clone(),
            plan_repr: self.plan_repr,
        }))
    }

    async fn execute(
        &self,
        partition: usize,
        _runtime: Arc<RuntimeEnv>,
    ) -> Result<SendableRecordBatchStream> {
        assert_eq!(0, partition);

        info!("Connecting to Ballista scheduler at {}", self.scheduler_url);

        let mut scheduler = SchedulerGrpcClient::connect(self.scheduler_url.clone())
            .await
            .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;

        let schema: Schema = self.plan.schema().as_ref().clone().into();

        let mut buf: Vec<u8> = vec![];
        let plan_message =
            T::try_from_logical_plan(&self.plan, self.extension_codec.as_ref()).map_err(
                |e| {
                    DataFusionError::Internal(format!(
                        "failed to serialize logical plan: {:?}",
                        e
                    ))
                },
            )?;
        plan_message.try_encode(&mut buf).map_err(|e| {
            DataFusionError::Execution(format!("failed to encode logical plan: {:?}", e))
        })?;

        let job_id = scheduler
            .execute_query(ExecuteQueryParams {
                query: Some(Query::LogicalPlan(buf)),
                settings: self
                    .config
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
                            .map(fetch_partition),
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

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "DistributedQueryExec: scheduler_url={}",
                    self.scheduler_url
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        // This execution plan sends the logical plan to the scheduler without
        // performing the node by node conversion to a full physical plan.
        // This implies that we cannot infer the statistics at this stage.
        Statistics::default()
    }
}

async fn fetch_partition(
    location: PartitionLocation,
) -> Result<SendableRecordBatchStream> {
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
    ballista_client
        .fetch_partition(
            &partition_id.job_id,
            partition_id.stage_id as usize,
            partition_id.partition_id as usize,
            &location.path,
        )
        .await
        .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))
}
