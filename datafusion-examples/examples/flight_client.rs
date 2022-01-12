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

use std::sync::Arc;

use arrow::io::flight::deserialize_schemas;
use arrow_format::flight::data::{flight_descriptor, FlightDescriptor, Ticket};
use arrow_format::flight::service::flight_service_client::FlightServiceClient;
use datafusion::arrow::io::print;
use std::collections::HashMap;

/// This example shows how to wrap DataFusion with `FlightService` to support looking up schema information for
/// Parquet files and executing SQL queries against them on a remote server.
/// This example is run along-side the example `flight_server`.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let testdata = datafusion::test_util::parquet_test_data();

    // Create Flight client
    let mut client = FlightServiceClient::connect("http://localhost:50051").await?;

    // Call get_schema to get the schema of a Parquet file
    let request = tonic::Request::new(FlightDescriptor {
        r#type: flight_descriptor::DescriptorType::Path as i32,
        cmd: vec![],
        path: vec![format!("{}/alltypes_plain.parquet", testdata)],
    });

    let schema_result = client.get_schema(request).await?.into_inner();
    let (schema, _) = deserialize_schemas(schema_result.schema.as_slice()).unwrap();
    let schema = Arc::new(schema);
    println!("Schema: {:?}", schema);

    // Call do_get to execute a SQL query and receive results
    let request = tonic::Request::new(Ticket {
        ticket: "SELECT id FROM alltypes_plain".into(),
    });

    let mut stream = client.do_get(request).await?.into_inner();

    // the schema should be the first message returned, else client should error
    let flight_data = stream.message().await?.unwrap();
    // convert FlightData to a stream
    let (schema, ipc_schema) =
        deserialize_schemas(flight_data.data_body.as_slice()).unwrap();
    let schema = Arc::new(schema);
    println!("Schema: {:?}", schema);

    // all the remaining stream messages should be dictionary and record batches
    let mut results = vec![];
    let dictionaries_by_field = HashMap::new();
    while let Some(flight_data) = stream.message().await? {
        let record_batch = arrow::io::flight::deserialize_batch(
            &flight_data,
            schema.clone(),
            &ipc_schema,
            &dictionaries_by_field,
        )?;
        results.push(record_batch);
    }

    // print the results
    print::print(&results);

    Ok(())
}
