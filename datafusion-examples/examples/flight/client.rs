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

//! See `main.rs` for how to run it.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_flight::flight_descriptor;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::{FlightDescriptor, Ticket};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::util::pretty;
use datafusion::prelude::SessionContext;
use datafusion_examples::utils::{datasets::ExampleDataset, write_csv_to_parquet};
use tonic::transport::Endpoint;

/// This example shows how to wrap DataFusion with `FlightService` to support looking up schema information for
/// Parquet files and executing SQL queries against them on a remote server.
/// This example is run along-side the example `flight_server`.
pub async fn client() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();

    // Convert the CSV input into a temporary Parquet directory for querying
    let dataset = ExampleDataset::Cars;
    let parquet_temp = write_csv_to_parquet(&ctx, &dataset.path()).await?;

    // Create Flight client
    let endpoint = Endpoint::new("http://localhost:50051")?;
    let channel = endpoint.connect().await?;
    let mut client = FlightServiceClient::new(channel);

    // Call get_schema to get the schema of a Parquet file
    let request = tonic::Request::new(FlightDescriptor {
        r#type: flight_descriptor::DescriptorType::Path as i32,
        cmd: Default::default(),
        path: vec![format!("{}", parquet_temp.path_str()?)],
    });

    let schema_result = client.get_schema(request).await?.into_inner();
    let schema = Schema::try_from(&schema_result)?;
    println!("Schema: {schema:?}");

    // Call do_get to execute a SQL query and receive results
    let request = tonic::Request::new(Ticket {
        ticket: "SELECT car FROM cars".into(),
    });

    let mut stream = client.do_get(request).await?.into_inner();

    // the schema should be the first message returned, else client should error
    let flight_data = stream.message().await?.unwrap();
    // convert FlightData to a stream
    let schema = Arc::new(Schema::try_from(&flight_data)?);
    println!("Schema: {schema:?}");

    // all the remaining stream messages should be dictionary and record batches
    let mut results = vec![];
    let dictionaries_by_field = HashMap::new();
    while let Some(flight_data) = stream.message().await? {
        let record_batch = flight_data_to_arrow_batch(
            &flight_data,
            schema.clone(),
            &dictionaries_by_field,
        )?;
        results.push(record_batch);
    }

    // print the results
    pretty::print_batches(&results)?;

    Ok(())
}
