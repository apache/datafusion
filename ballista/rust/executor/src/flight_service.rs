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

//! Implementation of the Apache Arrow Flight protocol that wraps an executor.

use std::fs::File;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use crate::executor::Executor;
use ballista_core::error::BallistaError;
use ballista_core::serde::decode_protobuf;
use ballista_core::serde::scheduler::{Action as BallistaAction, PartitionStats};

use arrow_flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty,
    FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse,
    PutResult, SchemaResult, Ticket,
};
use datafusion::arrow::{
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    ipc::reader::FileReader,
    ipc::writer::IpcWriteOptions,
    record_batch::RecordBatch,
};
use datafusion::physical_plan::displayable;
use futures::{Stream, StreamExt};
use log::{info, warn};
use std::io::{Read, Seek};
use tokio::sync::mpsc::channel;
use tokio::task::JoinHandle;
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

type FlightDataSender = Sender<Result<FlightData, Status>>;
type FlightDataReceiver = Receiver<Result<FlightData, Status>>;

/// Service implementing the Apache Arrow Flight Protocol
#[derive(Clone)]
pub struct BallistaFlightService {
    /// Executor
    executor: Arc<Executor>,
}

impl BallistaFlightService {
    pub fn new(executor: Arc<Executor>) -> Self {
        Self { executor }
    }
}

type BoxedFlightStream<T> =
    Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + Sync + 'static>>;

#[tonic::async_trait]
impl FlightService for BallistaFlightService {
    type DoActionStream = BoxedFlightStream<arrow_flight::Result>;
    type DoExchangeStream = BoxedFlightStream<FlightData>;
    type DoGetStream = BoxedFlightStream<FlightData>;
    type DoPutStream = BoxedFlightStream<PutResult>;
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;
    type ListActionsStream = BoxedFlightStream<ActionType>;
    type ListFlightsStream = BoxedFlightStream<FlightInfo>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        info!("Received do_get request");

        let action =
            decode_protobuf(&ticket.ticket).map_err(|e| from_ballista_err(&e))?;

        match &action {
            BallistaAction::ExecutePartition(partition) => {
                info!(
                    "ExecutePartition: job={}, stage={}, partition={:?}\n{}",
                    partition.job_id,
                    partition.stage_id,
                    partition.partition_id,
                    displayable(partition.plan.as_ref()).indent().to_string()
                );

                let mut tasks: Vec<JoinHandle<Result<_, BallistaError>>> = vec![];
                for &part in &partition.partition_id {
                    let partition = partition.clone();
                    let executor = self.executor.clone();
                    tasks.push(tokio::spawn(async move {
                        let results = executor
                            .execute_partition(
                                partition.job_id.clone(),
                                partition.stage_id,
                                part,
                                partition.plan.clone(),
                            )
                            .await?;
                        let results = vec![results];

                        let mut flights: Vec<Result<FlightData, Status>> = vec![];
                        let options = arrow::ipc::writer::IpcWriteOptions::default();

                        let mut batches: Vec<Result<FlightData, Status>> = results
                            .iter()
                            .flat_map(|batch| create_flight_iter(batch, &options))
                            .collect();

                        // append batch vector to schema vector, so that the first message sent is the schema
                        flights.append(&mut batches);

                        Ok(flights)
                    }));
                }

                // wait for all partitions to complete
                let results = futures::future::join_all(tasks).await;

                // get results
                let mut flights: Vec<Result<FlightData, Status>> = vec![];

                // add an initial FlightData message that sends schema
                let options = arrow::ipc::writer::IpcWriteOptions::default();
                let stats = PartitionStats::default();
                let schema = Arc::new(Schema::new(vec![
                    Field::new("path", DataType::Utf8, false),
                    stats.arrow_struct_repr(),
                ]));
                let schema_flight_data =
                    arrow_flight::utils::flight_data_from_arrow_schema(
                        schema.as_ref(),
                        &options,
                    );
                flights.push(Ok(schema_flight_data));

                // collect statistics from each executed partition
                for result in results {
                    let result = result.map_err(|e| {
                        Status::internal(format!("Ballista Error: {:?}", e))
                    })?;
                    let batches = result.map_err(|e| {
                        Status::internal(format!("Ballista Error: {:?}", e))
                    })?;
                    flights.extend_from_slice(&batches);
                }

                let output = futures::stream::iter(flights);
                Ok(Response::new(Box::pin(output) as Self::DoGetStream))
            }
            BallistaAction::FetchPartition(partition_id) => {
                // fetch a partition that was previously executed by this executor
                info!("FetchPartition {:?}", partition_id);

                let mut path = PathBuf::from(self.executor.work_dir());
                path.push(&partition_id.job_id);
                path.push(&format!("{}", partition_id.stage_id));
                path.push(&format!("{}", partition_id.partition_id));
                path.push("data.arrow");
                let path = path.to_str().unwrap();

                info!("FetchPartition {:?} reading {}", partition_id, path);
                let file = File::open(&path)
                    .map_err(|e| {
                        BallistaError::General(format!(
                            "Failed to open partition file at {}: {:?}",
                            path, e
                        ))
                    })
                    .map_err(|e| from_ballista_err(&e))?;
                let reader = FileReader::try_new(file).map_err(|e| from_arrow_err(&e))?;

                let (tx, rx): (FlightDataSender, FlightDataReceiver) = channel(2);

                // Arrow IPC reader does not implement Sync + Send so we need to use a channel
                // to communicate
                task::spawn(async move {
                    if let Err(e) = stream_flight_data(reader, tx).await {
                        warn!("Error streaming results: {:?}", e);
                    }
                });

                Ok(Response::new(
                    Box::pin(ReceiverStream::new(rx)) as Self::DoGetStream
                ))
            }
        }
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut request = request.into_inner();

        while let Some(data) = request.next().await {
            let _data = data?;
        }

        Err(Status::unimplemented("do_put"))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();

        let _action =
            decode_protobuf(&action.body.to_vec()).map_err(|e| from_ballista_err(&e))?;

        Err(Status::unimplemented("do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }
}

/// Convert a single RecordBatch into an iterator of FlightData (containing
/// dictionaries and batches)
fn create_flight_iter(
    batch: &RecordBatch,
    options: &IpcWriteOptions,
) -> Box<dyn Iterator<Item = Result<FlightData, Status>>> {
    let (flight_dictionaries, flight_batch) =
        arrow_flight::utils::flight_data_from_arrow_batch(batch, options);
    Box::new(
        flight_dictionaries
            .into_iter()
            .chain(std::iter::once(flight_batch))
            .map(Ok),
    )
}

async fn stream_flight_data<T>(
    reader: FileReader<T>,
    tx: FlightDataSender,
) -> Result<(), Status>
where
    T: Read + Seek,
{
    let options = arrow::ipc::writer::IpcWriteOptions::default();
    let schema_flight_data = arrow_flight::utils::flight_data_from_arrow_schema(
        reader.schema().as_ref(),
        &options,
    );
    send_response(&tx, Ok(schema_flight_data)).await?;

    for batch in reader {
        let batch_flight_data: Vec<_> = batch
            .map(|b| create_flight_iter(&b, &options).collect())
            .map_err(|e| from_arrow_err(&e))?;
        for batch in &batch_flight_data {
            send_response(&tx, batch.clone()).await?;
        }
    }
    Ok(())
}

async fn send_response(
    tx: &FlightDataSender,
    data: Result<FlightData, Status>,
) -> Result<(), Status> {
    tx.send(data)
        .await
        .map_err(|e| Status::internal(format!("{:?}", e)))
}

fn from_arrow_err(e: &ArrowError) -> Status {
    Status::internal(format!("ArrowError: {:?}", e))
}

fn from_ballista_err(e: &ballista_core::error::BallistaError) -> Status {
    Status::internal(format!("Ballista Error: {:?}", e))
}
