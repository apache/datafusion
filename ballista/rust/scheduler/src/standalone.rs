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

#[cfg(feature = "sled")]
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
