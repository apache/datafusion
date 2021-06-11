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

use std::fmt::Formatter;
use std::sync::Arc;
use std::{any::Any, pin::Pin};

use crate::client::BallistaClient;
use crate::memory_stream::MemoryStream;
use crate::serde::scheduler::PartitionLocation;

use crate::utils::WrappedStream;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{DisplayFormatType, ExecutionPlan, Partitioning};
use datafusion::{
    error::{DataFusionError, Result},
    physical_plan::RecordBatchStream,
};
use futures::{future, Stream, StreamExt};
use log::info;

/// ShuffleReaderExec reads partitions that have already been materialized by a query stage
/// being executed by an executor
#[derive(Debug, Clone)]
pub struct ShuffleReaderExec {
    /// Each partition of a shuffle can read data from multiple locations
    pub(crate) partition: Vec<Vec<PartitionLocation>>,
    pub(crate) schema: SchemaRef,
}

impl ShuffleReaderExec {
    /// Create a new ShuffleReaderExec
    pub fn try_new(
        partition: Vec<Vec<PartitionLocation>>,
        schema: SchemaRef,
    ) -> Result<Self> {
        Ok(Self { partition, schema })
    }
}

#[async_trait]
impl ExecutionPlan for ShuffleReaderExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partition.len())
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "Ballista ShuffleReaderExec does not support with_new_children()".to_owned(),
        ))
    }

    async fn execute(
        &self,
        partition: usize,
    ) -> Result<Pin<Box<dyn RecordBatchStream + Send + Sync>>> {
        info!("ShuffleReaderExec::execute({})", partition);

        let partition_locations = &self.partition[partition];
        let result = future::join_all(partition_locations.iter().map(fetch_partition))
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        let result = WrappedStream::new(
            Box::pin(futures::stream::iter(result).flatten()),
            Arc::new(self.schema.as_ref().clone()),
        );
        Ok(Box::pin(result))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                let loc_str = self
                    .partition
                    .iter()
                    .map(|x| {
                        x.iter()
                            .map(|l| {
                                format!(
                                    "[executor={} part={}:{}:{} stats={:?}]",
                                    l.executor_meta.id,
                                    l.partition_id.job_id,
                                    l.partition_id.stage_id,
                                    l.partition_id.partition_id,
                                    l.partition_stats
                                )
                            })
                            .collect::<Vec<String>>()
                            .join(",")
                    })
                    .collect::<Vec<String>>()
                    .join("\n");
                write!(f, "ShuffleReaderExec: partition_locations={}", loc_str)
            }
        }
    }
}

async fn fetch_partition(
    location: &PartitionLocation,
) -> Result<Pin<Box<dyn RecordBatchStream + Send + Sync>>> {
    let metadata = &location.executor_meta;
    let partition_id = &location.partition_id;
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
