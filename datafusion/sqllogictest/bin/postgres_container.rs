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

use crate::Options;
use ContainerCommands::{FetchHost, FetchPort};
use datafusion::common::Result;
use log::info;
use std::env::set_var;
use std::future::Future;
use std::sync::LazyLock;
use std::{env, thread};
use testcontainers::ImageExt;
use testcontainers::core::IntoContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{Mutex, mpsc};

#[derive(Debug)]
pub enum ContainerCommands {
    FetchHost,
    FetchPort,
    Stop,
}

pub struct Channel<T> {
    pub tx: UnboundedSender<T>,
    pub rx: Mutex<UnboundedReceiver<T>>,
}

pub fn channel<T>() -> Channel<T> {
    let (tx, rx) = mpsc::unbounded_channel();
    Channel {
        tx,
        rx: Mutex::new(rx),
    }
}

pub fn execute_blocking<F: Future>(f: F) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(f);
}

static POSTGRES_IN: LazyLock<Channel<ContainerCommands>> = LazyLock::new(channel);
static POSTGRES_HOST: LazyLock<Channel<String>> = LazyLock::new(channel);
static POSTGRES_PORT: LazyLock<Channel<u16>> = LazyLock::new(channel);
static POSTGRES_STOPPED: LazyLock<Channel<()>> = LazyLock::new(channel);

pub async fn initialize_postgres_container(options: &Options) -> Result<()> {
    let start_pg_database = options.postgres_runner && !is_pg_uri_set();
    if start_pg_database {
        info!("Starting postgres db ...");

        thread::spawn(|| {
            execute_blocking(start_postgres(
                &POSTGRES_IN,
                &POSTGRES_HOST,
                &POSTGRES_PORT,
                &POSTGRES_STOPPED,
            ))
        });

        POSTGRES_IN.tx.send(FetchHost).unwrap();
        let db_host = POSTGRES_HOST.rx.lock().await.recv().await.unwrap();

        POSTGRES_IN.tx.send(FetchPort).unwrap();
        let db_port = POSTGRES_PORT.rx.lock().await.recv().await.unwrap();

        let pg_uri = format!("postgresql://postgres:postgres@{db_host}:{db_port}/test");
        info!("Postgres uri is {pg_uri}");

        unsafe {
            set_var("PG_URI", pg_uri);
        }
    } else {
        // close receiver
        POSTGRES_IN.rx.lock().await.close();
    }

    Ok(())
}

pub async fn terminate_postgres_container() -> Result<()> {
    if !POSTGRES_IN.tx.is_closed() {
        println!("Stopping postgres db ...");
        POSTGRES_IN.tx.send(ContainerCommands::Stop).unwrap_or(());
        POSTGRES_STOPPED.rx.lock().await.recv().await;
    }

    Ok(())
}

async fn start_postgres(
    in_channel: &Channel<ContainerCommands>,
    host_channel: &Channel<String>,
    port_channel: &Channel<u16>,
    stopped_channel: &Channel<()>,
) {
    info!("Starting postgres test container with user postgres/postgres and db test");

    let container = postgres::Postgres::default()
        .with_user("postgres")
        .with_password("postgres")
        .with_db_name("test")
        .with_mapped_port(16432, 5432.tcp())
        .with_tag("17-alpine")
        .start()
        .await
        .unwrap();
    // uncomment this if you are running docker in docker
    // let host = "host.docker.internal".to_string();
    let host = container.get_host().await.unwrap().to_string();
    let port = container.get_host_port_ipv4(5432).await.unwrap();

    let mut rx = in_channel.rx.lock().await;
    while let Some(command) = rx.recv().await {
        match command {
            FetchHost => host_channel.tx.send(host.clone()).unwrap(),
            FetchPort => port_channel.tx.send(port).unwrap(),
            ContainerCommands::Stop => {
                container.stop().await.unwrap();
                stopped_channel.tx.send(()).unwrap();
                rx.close();
            }
        }
    }
}

fn is_pg_uri_set() -> bool {
    env::var("PG_URI").is_ok()
}
