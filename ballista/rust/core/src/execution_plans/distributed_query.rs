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

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};

use crate::serde::protobuf::execute_query_params::OptionalSessionId;
use crate::serde::{AsLogicalPlan, DefaultLogicalExtensionCodec, LogicalExtensionCodec};
use datafusion::arrow::error::{ArrowError, Result as ArrowResult};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt};
use log::{error, info};

/// This operator sends a logical plan to a Ballista scheduler for execution and
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
    /// Session id
    session_id: String,
}

impl<T: 'static + AsLogicalPlan> DistributedQueryExec<T> {
    pub fn new(
        scheduler_url: String,
        config: BallistaConfig,
        plan: LogicalPlan,
        session_id: String,
    ) -> Self {
        Self {
            scheduler_url,
            config,
            plan,
            extension_codec: Arc::new(DefaultLogicalExtensionCodec {}),
            plan_repr: PhantomData,
            session_id,
        }
    }

    pub fn with_extension(
        scheduler_url: String,
        config: BallistaConfig,
        plan: LogicalPlan,
        extension_codec: Arc<dyn LogicalExtensionCodec>,
        session_id: String,
    ) -> Self {
        Self {
            scheduler_url,
            config,
            plan,
            extension_codec,
            plan_repr: PhantomData,
            session_id,
        }
    }

    pub fn with_repr(
        scheduler_url: String,
        config: BallistaConfig,
        plan: LogicalPlan,
        extension_codec: Arc<dyn LogicalExtensionCodec>,
        plan_repr: PhantomData<T>,
        session_id: String,
    ) -> Self {
        Self {
            scheduler_url,
            config,
            plan,
            extension_codec,
            plan_repr,
            session_id,
        }
    }
}

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
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DistributedQueryExec {
            scheduler_url: self.scheduler_url.clone(),
            config: self.config.clone(),
            plan: self.plan.clone(),
            extension_codec: self.extension_codec.clone(),
            plan_repr: self.plan_repr,
            session_id: self.session_id.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        assert_eq!(0, partition);

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

        let query = ExecuteQueryParams {
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
            optional_session_id: Some(OptionalSessionId::SessionId(
                self.session_id.clone(),
            )),
        };

        let stream = futures::stream::once(
            execute_query(self.scheduler_url.clone(), self.session_id.clone(), query)
                .map_err(|e| ArrowError::ExternalError(Box::new(e))),
        )
        .try_flatten();

        let schema = self.schema();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
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

async fn execute_query(
    scheduler_url: String,
    session_id: String,
    query: ExecuteQueryParams,
) -> Result<impl Stream<Item = ArrowResult<RecordBatch>> + Send> {
    info!("Connecting to Ballista scheduler at {}", scheduler_url);
    // TODO reuse the scheduler to avoid connecting to the Ballista scheduler again and again

    let mut scheduler = SchedulerGrpcClient::connect(scheduler_url.clone())
        .await
        .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;

    let query_result = scheduler
        .execute_query(query)
        .await
        .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?
        .into_inner();

    assert_eq!(
        session_id, query_result.session_id,
        "Session id inconsistent between Client and Server side in DistributedQueryExec."
    );

    let job_id = query_result.job_id;
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
                let streams = completed.partition_location.into_iter().map(|p| {
                    let f = fetch_partition(p)
                        .map_err(|e| ArrowError::ExternalError(Box::new(e)));

                    futures::stream::once(f).try_flatten()
                });

                break Ok(futures::stream::iter(streams).flatten());
            }
        };
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
