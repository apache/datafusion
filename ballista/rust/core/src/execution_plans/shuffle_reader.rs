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
use crate::serde::scheduler::{PartitionLocation, PartitionStats};

use crate::utils::WrappedStream;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::metrics::{
    ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Metric, Partitioning, Statistics,
};
use datafusion::{
    error::{DataFusionError, Result},
    physical_plan::RecordBatchStream,
};
use futures::{future, Stream, StreamExt};
use hashbrown::HashMap;
use log::info;
use std::time::Instant;

/// ShuffleReaderExec reads partitions that have already been materialized by a ShuffleWriterExec
/// being executed by an executor
#[derive(Debug, Clone)]
pub struct ShuffleReaderExec {
    /// Each partition of a shuffle can read data from multiple locations
    pub(crate) partition: Vec<Vec<PartitionLocation>>,
    pub(crate) schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl ShuffleReaderExec {
    /// Create a new ShuffleReaderExec
    pub fn try_new(
        partition: Vec<Vec<PartitionLocation>>,
        schema: SchemaRef,
    ) -> Result<Self> {
        Ok(Self {
            partition,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
        })
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
        // TODO partitioning may be known and could be populated here
        // see https://github.com/apache/arrow-datafusion/issues/758
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

        let fetch_time =
            MetricBuilder::new(&self.metrics).subset_time("fetch_time", partition);
        let timer = fetch_time.timer();

        let partition_locations = &self.partition[partition];
        let result = future::join_all(partition_locations.iter().map(fetch_partition))
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        timer.done();

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
                    .enumerate()
                    .map(|(partition_id, locations)| {
                        format!(
                            "[partition={} paths={}]",
                            partition_id,
                            locations
                                .iter()
                                .map(|l| l.path.clone())
                                .collect::<Vec<String>>()
                                .join(",")
                        )
                    })
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(
                    f,
                    "ShuffleReaderExec: partition_locations({})={}",
                    self.partition.len(),
                    loc_str
                )
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        stats_for_partitions(
            self.partition
                .iter()
                .flatten()
                .map(|loc| loc.partition_stats),
        )
    }
}

fn stats_for_partitions(
    partition_stats: impl Iterator<Item = PartitionStats>,
) -> Statistics {
    // TODO stats: add column statistics to PartitionStats
    partition_stats.fold(
        Statistics {
            is_exact: true,
            num_rows: Some(0),
            total_byte_size: Some(0),
            column_statistics: None,
        },
        |mut acc, part| {
            // if any statistic is unkown it makes the entire statistic unkown
            acc.num_rows = acc.num_rows.zip(part.num_rows).map(|(a, b)| a + b as usize);
            acc.total_byte_size = acc
                .total_byte_size
                .zip(part.num_bytes)
                .map(|(a, b)| a + b as usize);
            acc
        },
    )
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
            &location.path,
        )
        .await
        .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_stats_for_partitions_empty() {
        let result = stats_for_partitions(std::iter::empty());

        let exptected = Statistics {
            is_exact: true,
            num_rows: Some(0),
            total_byte_size: Some(0),
            column_statistics: None,
        };

        assert_eq!(result, exptected);
    }

    #[tokio::test]
    async fn test_stats_for_partitions_full() {
        let part_stats = vec![
            PartitionStats {
                num_rows: Some(10),
                num_bytes: Some(84),
                num_batches: Some(1),
            },
            PartitionStats {
                num_rows: Some(4),
                num_bytes: Some(65),
                num_batches: None,
            },
        ];

        let result = stats_for_partitions(part_stats.into_iter());

        let exptected = Statistics {
            is_exact: true,
            num_rows: Some(14),
            total_byte_size: Some(149),
            column_statistics: None,
        };

        assert_eq!(result, exptected);
    }

    #[tokio::test]
    async fn test_stats_for_partitions_missing() {
        let part_stats = vec![
            PartitionStats {
                num_rows: Some(10),
                num_bytes: Some(84),
                num_batches: Some(1),
            },
            PartitionStats {
                num_rows: None,
                num_bytes: None,
                num_batches: None,
            },
        ];

        let result = stats_for_partitions(part_stats.into_iter());

        let exptected = Statistics {
            is_exact: true,
            num_rows: None,
            total_byte_size: None,
            column_statistics: None,
        };

        assert_eq!(result, exptected);
    }
}
