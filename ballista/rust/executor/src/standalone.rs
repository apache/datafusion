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

use arrow_flight::flight_service_server::FlightServiceServer;
use ballista_core::{
    error::Result,
    serde::protobuf::executor_registration::OptionalHost,
    serde::protobuf::{scheduler_grpc_client::SchedulerGrpcClient, ExecutorRegistration},
    BALLISTA_VERSION,
};
use log::info;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tonic::transport::{Channel, Server};
use uuid::Uuid;

use crate::{execution_loop, executor::Executor, flight_service::BallistaFlightService};

pub async fn new_standalone_executor(
    scheduler: SchedulerGrpcClient<Channel>,
    concurrent_tasks: usize,
) -> Result<()> {
    let work_dir = TempDir::new()?
        .into_path()
        .into_os_string()
        .into_string()
        .unwrap();
    let executor = Arc::new(Executor::new(&work_dir));

    let service = BallistaFlightService::new(executor.clone());

    let server = FlightServiceServer::new(service);
    // Let the OS assign a random, free port
    let listener = TcpListener::bind("localhost:0").await?;
    let addr = listener.local_addr()?;
    info!(
        "Ballista v{} Rust Executor listening on {:?}",
        BALLISTA_VERSION, addr
    );
    tokio::spawn(
        Server::builder().add_service(server).serve_with_incoming(
            tokio_stream::wrappers::TcpListenerStream::new(listener),
        ),
    );
    let executor_meta = ExecutorRegistration {
        id: Uuid::new_v4().to_string(), // assign this executor a unique ID
        optional_host: Some(OptionalHost::Host("localhost".to_string())),
        port: addr.port() as u32,
    };
    tokio::spawn(execution_loop::poll_loop(
        scheduler,
        executor,
        executor_meta,
        concurrent_tasks,
    ));
    Ok(())
}
