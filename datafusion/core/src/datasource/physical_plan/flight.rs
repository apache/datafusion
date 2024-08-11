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

//! Execution plan for reading flights from Arrow Flight services

use std::any::Any;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Formatter;
use std::sync::Arc;

use arrow_flight::error::FlightError;
use arrow_flight::{FlightClient, FlightEndpoint, Ticket};
use arrow_schema::SchemaRef;
use futures::TryStreamExt;
use tonic::metadata::{MetadataKey, MetadataMap, MetadataValue};
use tonic::transport::Channel;

use datafusion_common::DataFusionError;
use datafusion_common::Result;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};

use crate::datasource::flight::FlightMetadata;

/// Arrow Flight physical plan that maps flight endpoints to partitions
#[derive(Clone, Debug)]
pub struct FlightExec {
    /// Visible for proto serialization
    pub partitions: Vec<Arc<FlightPartition>>,
    /// Visible for proto serialization
    pub plan_properties: Arc<PlanProperties>,
    /// Visible for proto serialization
    pub grpc_metadata: Arc<MetadataMap>,
}

/// The minimum information required for fetching a flight stream.
#[derive(Clone, Debug)]
pub struct FlightPartition {
    /// Visible for proto serialization
    pub locations: Vec<String>,
    /// Visible for proto serialization
    pub ticket: Vec<u8>,
}

impl FlightPartition {
    fn new(endpoint: &FlightEndpoint, fallback_location: String) -> Self {
        let locations = if endpoint.location.is_empty() {
            vec![fallback_location]
        } else {
            endpoint
                .location
                .iter()
                .map(|loc| {
                    if loc.uri.starts_with("arrow-flight-reuse-connection://") {
                        fallback_location.clone()
                    } else {
                        loc.uri.clone()
                    }
                })
                .collect()
        };
        Self {
            locations,
            ticket: endpoint
                .ticket
                .clone()
                .expect("No flight ticket")
                .ticket
                .to_vec(),
        }
    }

    /// Primarily used for proto deserialization
    pub fn restore(locations: Vec<String>, ticket: Vec<u8>) -> Self {
        Self { locations, ticket }
    }
}

impl FlightExec {
    /// Creates a FlightExec with the provided [FlightMetadata]
    /// and origin URL (used as fallback location as per the protocol spec).
    pub fn new(metadata: &FlightMetadata, origin: &str) -> Self {
        let partitions = metadata
            .flight_info
            .endpoint
            .iter()
            .map(|endpoint| FlightPartition::new(endpoint, origin.to_string()))
            .map(Arc::new)
            .collect();
        Self {
            partitions,
            plan_properties: metadata.plan_properties.clone(),
            grpc_metadata: metadata.grpc_metadata.clone(),
        }
    }

    /// Primarily used for proto deserialization
    pub fn restore(
        partitions: Vec<FlightPartition>,
        plan_properties: PlanProperties,
        grpc_headers: &HashMap<String, Vec<u8>>,
    ) -> Self {
        let mut grpc_metadata = MetadataMap::new();
        for (key, value) in grpc_headers {
            let text_value = String::from_utf8(value.clone());
            if text_value.is_ok() {
                let text_key = MetadataKey::from_bytes(key.as_bytes()).unwrap();
                grpc_metadata.insert(text_key, text_value.unwrap().parse().unwrap());
            } else {
                let binary_key = MetadataKey::from_bytes(key.as_bytes()).unwrap();
                grpc_metadata.insert_bin(binary_key, MetadataValue::from_bytes(value));
            }
        }
        Self {
            partitions: partitions.into_iter().map(Arc::new).collect(),
            plan_properties: Arc::new(plan_properties),
            grpc_metadata: Arc::new(grpc_metadata),
        }
    }
}

async fn flight_stream(
    partition: Arc<FlightPartition>,
    schema: SchemaRef,
    grpc_metadata: Arc<MetadataMap>,
) -> Result<SendableRecordBatchStream> {
    let mut errors: Vec<Box<dyn Error + Send + Sync>> = vec![];
    for loc in &partition.locations {
        match try_fetch_stream(
            loc,
            partition.ticket.clone(),
            schema.clone(),
            grpc_metadata.clone(),
        )
        .await
        {
            Ok(stream) => return Ok(stream),
            Err(e) => errors.push(Box::new(e)),
        }
    }
    let err = errors.into_iter().last().unwrap_or_else(|| {
        Box::new(FlightError::ProtocolError(format!(
            "No available location for endpoint {:?}",
            partition.locations
        )))
    });
    Err(DataFusionError::External(err))
}

async fn try_fetch_stream(
    source: impl Into<String>,
    ticket: Vec<u8>,
    schema: SchemaRef,
    grpc: Arc<MetadataMap>,
) -> arrow_flight::error::Result<SendableRecordBatchStream> {
    let ticket = Ticket::new(ticket);
    let dest = Channel::from_shared(source.into())
        .map_err(|e| FlightError::ExternalError(Box::new(e)))?;
    let channel = dest
        .connect()
        .await
        .map_err(|e| FlightError::ExternalError(Box::new(e)))?;
    let mut client = FlightClient::new(channel);
    client.metadata_mut().clone_from(grpc.as_ref());
    let stream = client.do_get(ticket).await?;
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        stream.schema().map(Arc::to_owned).unwrap_or(schema),
        stream.map_err(|e| DataFusionError::External(Box::new(e))),
    )))
}

impl DisplayAs for FlightExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => f.write_str("FlightExec"),
            DisplayFormatType::Verbose => write!(f, "FlightExec {:?}", self.partitions),
        }
    }
}

impl ExecutionPlan for FlightExec {
    fn name(&self) -> &str {
        "FlightExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.plan_properties.as_ref()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let future_stream = flight_stream(
            self.partitions[partition].clone(),
            self.schema(),
            self.grpc_metadata.clone(),
        );
        let stream = futures::stream::once(future_stream).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}
