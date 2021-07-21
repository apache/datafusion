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

//! Client API for sending requests to executors.

use std::sync::Arc;
use std::{collections::HashMap, pin::Pin};
use std::{
    convert::{TryFrom, TryInto},
    task::{Context, Poll},
};

use crate::error::{ballista_error, BallistaError, Result};
use crate::memory_stream::MemoryStream;
use crate::serde::protobuf::{self};
use crate::serde::scheduler::{
    Action, ExecutePartition, ExecutePartitionResult, PartitionId, PartitionStats,
};

use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::Ticket;
use arrow_flight::{flight_service_client::FlightServiceClient, FlightData};
use datafusion::arrow::{
    array::{StringArray, StructArray},
    datatypes::{Schema, SchemaRef},
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion::{logical_plan::LogicalPlan, physical_plan::RecordBatchStream};
use futures::{Stream, StreamExt};
use log::debug;
use prost::Message;
use tonic::Streaming;
use uuid::Uuid;

/// Client for interacting with Ballista executors.
#[derive(Clone)]
pub struct BallistaClient {
    flight_client: FlightServiceClient<tonic::transport::channel::Channel>,
}

impl BallistaClient {
    /// Create a new BallistaClient to connect to the executor listening on the specified
    /// host and port
    pub async fn try_new(host: &str, port: u16) -> Result<Self> {
        let addr = format!("http://{}:{}", host, port);
        debug!("BallistaClient connecting to {}", addr);
        let flight_client =
            FlightServiceClient::connect(addr.clone())
                .await
                .map_err(|e| {
                    BallistaError::General(format!(
                        "Error connecting to Ballista scheduler or executor at {}: {:?}",
                        addr, e
                    ))
                })?;
        debug!("BallistaClient connected OK");

        Ok(Self { flight_client })
    }

    /// Fetch a partition from an executor
    pub async fn fetch_partition(
        &mut self,
        job_id: &str,
        stage_id: usize,
        partition_id: usize,
        path: &str,
    ) -> Result<SendableRecordBatchStream> {
        let action = Action::FetchPartition {
            job_id: job_id.to_string(),
            stage_id,
            partition_id,
            path: path.to_owned(),
        };
        self.execute_action(&action).await
    }

    /// Execute an action and retrieve the results
    pub async fn execute_action(
        &mut self,
        action: &Action,
    ) -> Result<SendableRecordBatchStream> {
        let serialized_action: protobuf::Action = action.to_owned().try_into()?;

        let mut buf: Vec<u8> = Vec::with_capacity(serialized_action.encoded_len());

        serialized_action
            .encode(&mut buf)
            .map_err(|e| BallistaError::General(format!("{:?}", e)))?;

        let request = tonic::Request::new(Ticket { ticket: buf });

        let mut stream = self
            .flight_client
            .do_get(request)
            .await
            .map_err(|e| BallistaError::General(format!("{:?}", e)))?
            .into_inner();

        // the schema should be the first message returned, else client should error
        match stream
            .message()
            .await
            .map_err(|e| BallistaError::General(format!("{:?}", e)))?
        {
            Some(flight_data) => {
                // convert FlightData to a stream
                let schema = Arc::new(Schema::try_from(&flight_data)?);

                // all the remaining stream messages should be dictionary and record batches
                Ok(Box::pin(FlightDataStream::new(stream, schema)))
            }
            None => Err(ballista_error(
                "Did not receive schema batch from flight server",
            )),
        }
    }
}

struct FlightDataStream {
    stream: Streaming<FlightData>,
    schema: SchemaRef,
}

impl FlightDataStream {
    pub fn new(stream: Streaming<FlightData>, schema: SchemaRef) -> Self {
        Self { stream, schema }
    }
}

impl Stream for FlightDataStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx).map(|x| match x {
            Some(flight_data_chunk_result) => {
                let converted_chunk = flight_data_chunk_result
                    .map_err(|e| ArrowError::from_external_error(Box::new(e)))
                    .and_then(|flight_data_chunk| {
                        flight_data_to_arrow_batch(
                            &flight_data_chunk,
                            self.schema.clone(),
                            &[],
                        )
                    });
                Some(converted_chunk)
            }
            None => None,
        })
    }
}

impl RecordBatchStream for FlightDataStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
