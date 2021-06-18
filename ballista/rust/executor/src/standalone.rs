use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use ballista_core::{
    error::Result,
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
        optional_host: None,
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
