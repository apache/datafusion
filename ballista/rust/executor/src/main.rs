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

//! Ballista Rust executor binary.

use std::{
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
};

use anyhow::{Context, Result};
use arrow_flight::flight_service_server::FlightServiceServer;
use futures::future::MaybeDone;
use log::info;
use tempfile::TempDir;
use tonic::transport::Server;
use uuid::Uuid;

use ballista_core::{
    client::BallistaClient,
    serde::protobuf::{
        executor_registration, scheduler_grpc_client::SchedulerGrpcClient,
        ExecutorRegistration,
    },
};
use ballista_core::{
    print_version, serde::protobuf::scheduler_grpc_server::SchedulerGrpcServer,
    BALLISTA_VERSION,
};
use ballista_executor::flight_service::BallistaFlightService;
use ballista_scheduler::{state::StandaloneClient, SchedulerServer};
use config::prelude::*;

mod execution_loop;

#[macro_use]
extern crate configure_me;

#[allow(clippy::all, warnings)]
mod config {
    // Ideally we would use the include_config macro from configure_me, but then we cannot use
    // #[allow(clippy::all)] to silence clippy warnings from the generated code
    include!(concat!(env!("OUT_DIR"), "/executor_configure_me_config.rs"));
}

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    // parse command-line arguments
    let (opt, _remaining_args) =
        Config::including_optional_config_files(&["/etc/ballista/executor.toml"])
            .unwrap_or_exit();

    if opt.version {
        print_version();
        std::process::exit(0);
    }

    let external_host = opt.external_host;
    let bind_host = opt.bind_host;
    let port = opt.port;

    let addr = format!("{}:{}", bind_host, port);
    let addr = addr
        .parse()
        .with_context(|| format!("Could not parse address: {}", addr))?;

    let scheduler_host = if opt.local {
        "localhost".to_string()
    } else {
        opt.scheduler_host
    };
    let scheduler_port = opt.scheduler_port;
    let scheduler_url = format!("http://{}:{}", scheduler_host, scheduler_port);

    let work_dir = opt.work_dir.unwrap_or(
        TempDir::new()?
            .into_path()
            .into_os_string()
            .into_string()
            .unwrap(),
    );
    info!("Running with config:");
    info!("work_dir: {}", work_dir);
    info!("concurrent_tasks: {}", opt.concurrent_tasks);

    let executor_meta = ExecutorRegistration {
        id: Uuid::new_v4().to_string(), // assign this executor a unique ID
        optional_host: external_host
            .clone()
            .map(executor_registration::OptionalHost::Host),
        port: port as u32,
    };

    if opt.local {
        info!("Running in local mode. Scheduler will be run in-proc");

        let client = match opt.scheduler_data_path {
            Some(v) => StandaloneClient::try_new(v)
                .context("Could not create standalone config backend")?,
            None => StandaloneClient::try_new_temporary()
                .context("Could not create standalone config backend")?,
        };

        let server = SchedulerGrpcServer::new(SchedulerServer::new(
            Arc::new(client),
            "ballista".to_string(),
            IpAddr::V4(Ipv4Addr::LOCALHOST),
        ));
        let addr = format!("localhost:{}", scheduler_port);
        let addr = addr
            .parse()
            .with_context(|| format!("Could not parse {}", addr))?;
        info!(
            "Ballista v{} Rust Scheduler listening on {:?}",
            BALLISTA_VERSION, addr
        );
        let scheduler_future =
            tokio::spawn(Server::builder().add_service(server).serve(addr));
        let mut scheduler_result = futures::future::maybe_done(scheduler_future);

        // Ensure scheduler is ready to receive connections
        while SchedulerGrpcClient::connect(scheduler_url.clone())
            .await
            .is_err()
        {
            let scheduler_future = match scheduler_result {
                MaybeDone::Future(f) => f,
                MaybeDone::Done(Err(e)) => return Err(e).context("Tokio error"),
                MaybeDone::Done(Ok(Err(e))) => {
                    return Err(e).context("Scheduler failed to initialize correctly")
                }
                MaybeDone::Done(Ok(Ok(()))) => {
                    return Err(anyhow::format_err!(
                        "Scheduler unexpectedly finished successfully"
                    ))
                }
                MaybeDone::Gone => {
                    panic!("Received Gone from recently created MaybeDone")
                }
            };
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            scheduler_result = futures::future::maybe_done(scheduler_future);
        }
    }

    let scheduler = SchedulerGrpcClient::connect(scheduler_url)
        .await
        .context("Could not connect to scheduler")?;
    let service = BallistaFlightService::new(work_dir);

    let server = FlightServiceServer::new(service);
    info!(
        "Ballista v{} Rust Executor listening on {:?}",
        BALLISTA_VERSION, addr
    );
    let server_future = tokio::spawn(Server::builder().add_service(server).serve(addr));
    let client_host = external_host.as_deref().unwrap_or_else(|| {
        if bind_host == "0.0.0.0" {
            // If the executor is being bound to "0.0.0.0" (which means use all ips in all eth devices)
            // then use "localhost" to connect to itself through the BallistaClient
            "localhost"
        } else {
            &bind_host
        }
    });
    let client = BallistaClient::try_new(client_host, port).await?;
    tokio::spawn(execution_loop::poll_loop(
        scheduler,
        client,
        executor_meta,
        opt.concurrent_tasks,
    ));

    server_future
        .await
        .context("Tokio error")?
        .context("Could not start executor server")?;
    Ok(())
}
