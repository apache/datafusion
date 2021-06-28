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

use ballista_core::{
    error::Result, serde::protobuf::scheduler_grpc_server::SchedulerGrpcServer,
    BALLISTA_VERSION,
};
use log::info;
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};
use tokio::net::TcpListener;
use tonic::transport::Server;

use crate::{state::StandaloneClient, SchedulerServer};

pub async fn new_standalone_scheduler() -> Result<SocketAddr> {
    let client = StandaloneClient::try_new_temporary()?;

    let server = SchedulerGrpcServer::new(SchedulerServer::new(
        Arc::new(client),
        "ballista".to_string(),
        IpAddr::V4(Ipv4Addr::LOCALHOST),
    ));
    // Let the OS assign a random, free port
    let listener = TcpListener::bind("localhost:0").await?;
    let addr = listener.local_addr()?;
    info!(
        "Ballista v{} Rust Scheduler listening on {:?}",
        BALLISTA_VERSION, addr
    );
    tokio::spawn(
        Server::builder().add_service(server).serve_with_incoming(
            tokio_stream::wrappers::TcpListenerStream::new(listener),
        ),
    );

    Ok(addr)
}
