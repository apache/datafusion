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

use chrono::{DateTime, Duration, Utc};
use std::sync::Arc;
use std::time::Duration as Core_Duration;

use anyhow::{Context, Result};
use arrow_flight::flight_service_server::FlightServiceServer;
use ballista_executor::{execution_loop, executor_server};
use log::{error, info};
use tempfile::TempDir;
use tokio::fs::ReadDir;
use tokio::{fs, time};
use tonic::transport::Server;
use uuid::Uuid;

use ballista_core::config::TaskSchedulingPolicy;
use ballista_core::serde::protobuf::{
    executor_registration, scheduler_grpc_client::SchedulerGrpcClient,
    ExecutorRegistration, LogicalPlanNode, PhysicalPlanNode,
};
use ballista_core::serde::scheduler::ExecutorSpecification;
use ballista_core::serde::BallistaCodec;
use ballista_core::{print_version, BALLISTA_VERSION};
use ballista_executor::executor::Executor;
use ballista_executor::flight_service::BallistaFlightService;
use config::prelude::*;
use datafusion::prelude::SessionContext;

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
    let port = opt.bind_port;
    let grpc_port = opt.bind_grpc_port;

    let addr = format!("{}:{}", bind_host, port);
    let addr = addr
        .parse()
        .with_context(|| format!("Could not parse address: {}", addr))?;

    let scheduler_host = opt.scheduler_host;
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
        grpc_port: grpc_port as u32,
        specification: Some(
            ExecutorSpecification {
                task_slots: opt.concurrent_tasks as u32,
            }
            .into(),
        ),
    };
    let executor = Arc::new(Executor::new(
        executor_meta,
        &work_dir,
        Arc::new(SessionContext::new()),
    ));

    let scheduler = SchedulerGrpcClient::connect(scheduler_url)
        .await
        .context("Could not connect to scheduler")?;

    let default_codec: BallistaCodec<LogicalPlanNode, PhysicalPlanNode> =
        BallistaCodec::default();

    let scheduler_policy = opt.task_scheduling_policy;
    let cleanup_ttl = opt.executor_cleanup_ttl;

    if opt.executor_cleanup_enable {
        let mut interval_time =
            time::interval(Core_Duration::from_secs(opt.executor_cleanup_interval));
        tokio::spawn(async move {
            loop {
                interval_time.tick().await;
                if let Err(e) =
                    clean_shuffle_data_loop(&work_dir, cleanup_ttl as i64).await
                {
                    error!("Ballista executor fail to clean_shuffle_data {:?}", e)
                }
            }
        });
    }

    match scheduler_policy {
        TaskSchedulingPolicy::PushStaged => {
            tokio::spawn(executor_server::startup(
                scheduler,
                executor.clone(),
                default_codec,
            ));
        }
        _ => {
            tokio::spawn(execution_loop::poll_loop(
                scheduler,
                executor.clone(),
                default_codec,
            ));
        }
    }

    // Arrow flight service
    {
        let service = BallistaFlightService::new(executor.clone());
        let server = FlightServiceServer::new(service);
        info!(
            "Ballista v{} Rust Executor listening on {:?}",
            BALLISTA_VERSION, addr
        );
        let server_future =
            tokio::spawn(Server::builder().add_service(server).serve(addr));
        server_future
            .await
            .context("Tokio error")?
            .context("Could not start executor server")?;
    }

    Ok(())
}

/// This function will scheduled periodically for cleanup executor.
/// Will only clean the dir under work_dir not include file
async fn clean_shuffle_data_loop(work_dir: &str, seconds: i64) -> Result<()> {
    let mut dir = fs::read_dir(work_dir).await?;
    let mut to_deleted = Vec::new();
    let mut need_delete_dir;
    while let Some(child) = dir.next_entry().await? {
        if let Ok(metadata) = child.metadata().await {
            // only delete the job dir
            if metadata.is_dir() {
                let dir = fs::read_dir(child.path()).await?;
                match check_modified_time_in_dirs(vec![dir], seconds).await {
                    Ok(x) => match x {
                        true => {
                            need_delete_dir = child.path().into_os_string();
                            to_deleted.push(need_delete_dir)
                        }
                        false => {}
                    },
                    Err(e) => {
                        error!("Fail in clean_shuffle_data_loop {:?}", e)
                    }
                }
            }
        } else {
            error!("Can not get metadata from file: {:?}", child)
        }
    }
    info!(
        "The work_dir {:?} that have not been modified for {:?} seconds will be deleted",
        &to_deleted, seconds
    );
    for del in to_deleted {
        fs::remove_dir_all(del).await?;
    }
    Ok(())
}

/// Determines if a directory all files are older than cutoff seconds.
async fn check_modified_time_in_dirs(
    mut vec: Vec<ReadDir>,
    ttl_seconds: i64,
) -> Result<bool> {
    let cutoff = Utc::now() - Duration::seconds(ttl_seconds);

    while !vec.is_empty() {
        let mut dir = vec.pop().unwrap();
        while let Some(child) = dir.next_entry().await? {
            let meta = child.metadata().await?;
            if meta.is_dir() {
                let dir = fs::read_dir(child.path()).await?;
                // check in next loop
                vec.push(dir);
            } else {
                let modified_time: DateTime<Utc> =
                    meta.modified().map(chrono::DateTime::from)?;
                if modified_time > cutoff {
                    // if one file has been modified in ttl we won't delete the whole dir
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use crate::clean_shuffle_data_loop;
    use std::fs;
    use std::fs::File;
    use std::io::Write;
    use std::time::Duration;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_executor_clean_up() {
        let work_dir = TempDir::new().unwrap().into_path();
        let job_dir = work_dir.as_path().join("job_id");
        let file_path = job_dir.as_path().join("tmp.csv");
        let data = "Jorge,2018-12-13T12:12:10.011Z\n\
                    Andrew,2018-11-13T17:11:10.011Z";
        fs::create_dir(job_dir).unwrap();
        File::create(&file_path)
            .expect("creating temp file")
            .write_all(data.as_bytes())
            .expect("writing data");

        let work_dir_clone = work_dir.clone();

        let count1 = fs::read_dir(work_dir.clone()).unwrap().count();
        assert_eq!(count1, 1);
        let mut handles = vec![];
        handles.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            clean_shuffle_data_loop(work_dir_clone.to_str().unwrap(), 1)
                .await
                .unwrap();
        }));
        futures::future::join_all(handles).await;
        let count2 = fs::read_dir(work_dir.clone()).unwrap().count();
        assert_eq!(count2, 0);
    }
}
