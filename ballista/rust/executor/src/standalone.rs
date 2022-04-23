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

use ballista_core::serde::scheduler::ExecutorSpecification;
use ballista_core::serde::{AsExecutionPlan, AsLogicalPlan, BallistaCodec};
use ballista_core::{
    error::Result,
    serde::protobuf::executor_registration::OptionalHost,
    serde::protobuf::{scheduler_grpc_client::SchedulerGrpcClient, ExecutorRegistration},
    BALLISTA_VERSION,
};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use log::info;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tonic::transport::{Channel, Server};
use uuid::Uuid;

use crate::metrics::LoggingMetricsCollector;
use crate::{execution_loop, executor::Executor, flight_service::BallistaFlightService};

pub async fn new_standalone_executor<
    T: 'static + AsLogicalPlan,
    U: 'static + AsExecutionPlan,
>(
    scheduler: SchedulerGrpcClient<Channel>,
    concurrent_tasks: usize,
    codec: BallistaCodec<T, U>,
) -> Result<()> {
    // Let the OS assign a random, free port
    let listener = TcpListener::bind("localhost:0").await?;
    let addr = listener.local_addr()?;
    info!(
        "Ballista v{} Rust Executor listening on {:?}",
        BALLISTA_VERSION, addr
    );

    let executor_meta = ExecutorRegistration {
        id: Uuid::new_v4().to_string(), // assign this executor a unique ID
        optional_host: Some(OptionalHost::Host("localhost".to_string())),
        port: addr.port() as u32,
        // TODO Make it configurable
        grpc_port: 50020,
        specification: Some(
            ExecutorSpecification {
                task_slots: concurrent_tasks as u32,
            }
            .into(),
        ),
    };
    let work_dir = TempDir::new()?
        .into_path()
        .into_os_string()
        .into_string()
        .unwrap();
    info!("work_dir: {}", work_dir);

    let config = RuntimeConfig::new().with_temp_file_path(work_dir.clone());

    let executor = Arc::new(Executor::new(
        executor_meta,
        &work_dir,
        Arc::new(RuntimeEnv::new(config).unwrap()),
        Arc::new(LoggingMetricsCollector::default()),
    ));

    let service = BallistaFlightService::new(executor.clone());
    let server = FlightServiceServer::new(service);
    tokio::spawn(
        Server::builder().add_service(server).serve_with_incoming(
            tokio_stream::wrappers::TcpListenerStream::new(listener),
        ),
    );

    tokio::spawn(execution_loop::poll_loop(scheduler, executor, codec));
    Ok(())
}
